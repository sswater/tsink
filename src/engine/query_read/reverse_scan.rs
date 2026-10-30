use std::sync::Arc;

use parking_lot::RwLockReadGuard;

use crate::engine::encoder::{Encoder, TimestampSearchIndex, TIMESTAMP_SEARCH_BLOCK_POINTS};
use crate::validation::{validate_labels, validate_metric};
use crate::{DataPoint, Label, Result, TsinkError};

use super::super::state::ActivePartitionSnapshot;
use super::*;

/// Relative freshness of a chunk source. Active in-memory heads are always newer
/// than the sealed copy of the same chunk, which is newer than persisted copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum SourceRecency {
    Persisted,
    Sealed,
    Active,
}

/// One candidate chunk (or active partition head) overlapping the query window.
///
/// `max_ts` is the candidate's **latest in-window timestamp**, not merely the
/// chunk header maximum: when a chunk straddles `end`, collection peeks at the
/// tail block to find the actual newest point below `end`. This keeps the global
/// ordering honest without decoding any values. Point lookups skip that peek and
/// keep the header maximum instead.
pub(super) struct ChunkCandidate {
    pub min_ts: i64,
    pub max_ts: i64,
    pub recency: SourceRecency,
    pub(super) sequence: u64,
    peeked: bool,
    pub(super) source: CandidateSource,
}

pub(super) enum CandidateSource {
    Persisted(PersistedChunkRef),
    Sealed(Arc<Chunk>),
    Active(ActivePartitionSnapshot),
}

/// Honest work counters for a reverse scan. `points_decoded` counts every point
/// whose value was materialized (before any output truncation), not only the
/// points that survive `limit`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReverseScanStats {
    pub chunks_touched: usize,
    pub points_decoded: usize,
}

/// Candidate snapshot plus the persisted-index read guard that keeps captured
/// chunk refs and their lazily built timestamp indexes alive for the whole scan.
pub(super) struct CandidateSnapshot<'a> {
    pub(super) candidates: Vec<ChunkCandidate>,
    pub(super) persisted_index: RwLockReadGuard<'a, PersistedIndexState>,
    peeked_chunks: usize,
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn select_desc_api(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        limit: usize,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        validate_metric(metric)?;
        validate_labels(labels)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        if limit == 0 {
            return Ok(Vec::new());
        }

        let plan = self.query_tier_plan(start, end);
        let (points, _stats) =
            self.select_desc_with_plan(metric, labels, start, end, limit, plan)?;
        Ok(points)
    }

    pub(super) fn select_desc_with_plan(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        limit: usize,
        plan: TieredQueryPlan,
    ) -> Result<(Vec<DataPoint>, ReverseScanStats)> {
        let Some(series_id) = self
            .catalog
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            return Ok((Vec::new(), ReverseScanStats::default()));
        };

        self.select_desc_for_series_with_plan(series_id, start, end, limit, plan)
    }

    pub(super) fn select_desc_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        limit: usize,
        plan: TieredQueryPlan,
    ) -> Result<(Vec<DataPoint>, ReverseScanStats)> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        if limit == 0 {
            return Ok((Vec::new(), ReverseScanStats::default()));
        }

        let snapshot = self.collect_candidates(series_id, start, end, plan)?;
        if snapshot.candidates.is_empty() {
            return Ok((Vec::new(), ReverseScanStats::default()));
        }

        let mut out = Vec::new();
        let mut stats = ReverseScanStats {
            // Candidates already peeked during ordering were touched once there.
            chunks_touched: snapshot.peeked_chunks,
            points_decoded: 0,
        };
        let retention_cutoff = self.active_retention_cutoff();
        let mut seen_timestamps = HashSet::with_capacity(limit.min(4096));
        self.tombstone_read_context().with_series_tombstone_ranges(
            series_id,
            |tombstone_ranges| -> Result<()> {
                'candidates: for candidate in &snapshot.candidates {
                    let remaining = limit.saturating_sub(out.len());
                    if remaining == 0 {
                        break;
                    }

                    let points = self.decode_candidate_tail(
                        &snapshot, candidate, start, end, remaining, &mut stats,
                    )?;

                    for point in points {
                        if retention_cutoff.is_some_and(|cutoff| point.timestamp < cutoff) {
                            continue;
                        }
                        if tombstone_ranges.is_some_and(|ranges| {
                            tombstone::timestamp_is_tombstoned(point.timestamp, ranges)
                        }) {
                            continue;
                        }
                        if !seen_timestamps.insert(point.timestamp) {
                            continue;
                        }
                        out.push(point);
                        if out.len() >= limit {
                            break 'candidates;
                        }
                    }
                }
                Ok(())
            },
        )?;

        Ok((out, stats))
    }

    /// Collects every persisted, sealed, and active source overlapping `[start, end)`
    /// and orders them by `(latest-in-window ts desc, recency desc, sequence desc)`.
    ///
    /// Enumeration itself is header/metadata based: a wide window may visit every
    /// overlapping chunk to build the candidate list. Spec §5.1 accepts this cost
    /// (metadata enumeration is parked); INV-1 bounds the number of chunks and
    /// values actually decoded, which stays independent of window width.
    ///
    /// The persisted-index read guard is captured in the snapshot so the lazily
    /// cached timestamp index cannot disappear between collection and decode while
    /// captured segment payloads are still readable.
    pub(super) fn collect_candidates(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<CandidateSnapshot<'_>> {
        self.collect_candidates_with_mode(series_id, start, end, plan, true)
    }

    /// Collects candidates for point lookups, using chunk header min/max as-is
    /// and never peeking at a tail block. Enumeration therefore fetches no chunk
    /// payload at all, leaving at most one payload fetch per touched group to the
    /// point scan itself (INV-2).
    pub(super) fn collect_candidates_without_tail_peek(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<CandidateSnapshot<'_>> {
        self.collect_candidates_with_mode(series_id, start, end, plan, false)
    }

    fn collect_candidates_with_mode(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        peek_tail: bool,
    ) -> Result<CandidateSnapshot<'_>> {
        let context = self.query_snapshot_context();
        let visibility_guard = context.visibility_read_fence();
        let mut candidates = Vec::new();
        let mut peeked_chunks = 0usize;

        {
            let sealed = context.chunks.sealed_shard(series_id).read();
            if let Some(chunks) = sealed.get(&series_id) {
                let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                for (key, chunk) in chunks.range(..end_bound) {
                    if chunk.header.max_ts < start {
                        continue;
                    }
                    let (min_ts, max_ts, peeked) = if !peek_tail || chunk.header.max_ts < end {
                        (chunk.header.min_ts, chunk.header.max_ts, false)
                    } else {
                        let Some(max_ts) = self.latest_sealed_in_window_ts(chunk, start, end)?
                        else {
                            continue;
                        };
                        peeked_chunks = peeked_chunks.saturating_add(1);
                        (chunk.header.min_ts, max_ts, true)
                    };
                    candidates.push(ChunkCandidate {
                        min_ts,
                        max_ts,
                        recency: SourceRecency::Sealed,
                        sequence: key.sequence,
                        peeked,
                        source: CandidateSource::Sealed(Arc::clone(chunk)),
                    });
                }
            }
        }

        {
            let active = context.chunks.active_shard(series_id).read();
            if let Some(state) = active.get(&series_id) {
                let partition_window = context.partition_window.max(1);
                let start_partition = partition_id_for_timestamp(start, partition_window);
                let end_partition =
                    partition_id_for_timestamp(end.saturating_sub(1), partition_window);
                for head in state
                    .partition_heads
                    .range(start_partition..=end_partition)
                    .map(|(_, head)| head)
                {
                    let points = head.builder.snapshot_in_range(start, end);
                    if points.is_empty() {
                        continue;
                    }

                    let mut min_ts = i64::MAX;
                    let mut max_ts = i64::MIN;
                    for point in points.iter_points() {
                        min_ts = min_ts.min(point.ts);
                        max_ts = max_ts.max(point.ts);
                    }

                    candidates.push(ChunkCandidate {
                        min_ts,
                        max_ts,
                        recency: SourceRecency::Active,
                        sequence: u64::MAX,
                        peeked: false,
                        source: CandidateSource::Active(ActivePartitionSnapshot::from_points(
                            points,
                        )),
                    });
                }
            }
        }

        let persisted_index = context.persisted_index.read();
        if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
            let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
            for chunk_ref in &chunks[..end_idx] {
                if chunk_ref.max_ts < start {
                    continue;
                }
                let tier = persisted_index
                    .segment_tiers
                    .get(&chunk_ref.segment_slot)
                    .copied()
                    .unwrap_or(PersistedSegmentTier::Hot);
                if !ChunkStorage::plan_includes_persisted_tier(plan, tier) {
                    continue;
                }

                let (max_ts, peeked) = if !peek_tail || chunk_ref.max_ts < end {
                    (chunk_ref.max_ts, false)
                } else {
                    let Some(max_ts) = self.latest_persisted_in_window_ts(
                        &persisted_index,
                        chunk_ref,
                        start,
                        end,
                    )?
                    else {
                        continue;
                    };
                    peeked_chunks = peeked_chunks.saturating_add(1);
                    (max_ts, true)
                };
                candidates.push(ChunkCandidate {
                    min_ts: chunk_ref.min_ts,
                    max_ts,
                    recency: SourceRecency::Persisted,
                    sequence: chunk_ref.sequence,
                    peeked,
                    source: CandidateSource::Persisted(*chunk_ref),
                });
            }
        }
        drop(visibility_guard);

        candidates.sort_by(|left, right| {
            right
                .max_ts
                .cmp(&left.max_ts)
                .then_with(|| right.recency.cmp(&left.recency))
                .then_with(|| right.sequence.cmp(&left.sequence))
        });

        Ok(CandidateSnapshot {
            candidates,
            persisted_index,
            peeked_chunks,
        })
    }

    fn latest_sealed_in_window_ts(
        &self,
        chunk: &Chunk,
        start: i64,
        end: i64,
    ) -> Result<Option<i64>> {
        if chunk.encoded_payload.is_empty() {
            return Ok(chunk
                .points
                .iter()
                .filter(|point| point.ts >= start && point.ts < end)
                .map(|point| point.ts)
                .max());
        }

        let descriptor = EncodedChunkDescriptor {
            lane: chunk.header.lane,
            ts_codec: chunk.header.ts_codec,
            value_codec: chunk.header.value_codec,
            point_count: chunk.header.point_count as usize,
        };
        let search_index = Encoder::build_timestamp_search_index_from_payload(
            chunk.header.ts_codec,
            descriptor.point_count,
            &chunk.encoded_payload,
        )?;
        latest_in_window_timestamp(
            descriptor,
            &chunk.encoded_payload,
            &search_index,
            start,
            end,
        )
    }

    fn latest_persisted_in_window_ts(
        &self,
        persisted_index: &PersistedIndexState,
        chunk_ref: &PersistedChunkRef,
        start: i64,
        end: i64,
    ) -> Result<Option<i64>> {
        let search_index =
            self.persisted_chunk_timestamp_search_index(persisted_index, chunk_ref)?;
        let payload = persisted_chunk_payload(&persisted_index.segment_maps, chunk_ref)?;
        let descriptor = EncodedChunkDescriptor {
            lane: chunk_ref.lane,
            ts_codec: chunk_ref.ts_codec,
            value_codec: chunk_ref.value_codec,
            point_count: chunk_ref.point_count as usize,
        };
        latest_in_window_timestamp(descriptor, payload.as_ref(), search_index, start, end)
    }

    fn decode_candidate_tail(
        &self,
        snapshot: &CandidateSnapshot<'_>,
        candidate: &ChunkCandidate,
        start: i64,
        end: i64,
        max_points: usize,
        stats: &mut ReverseScanStats,
    ) -> Result<Vec<DataPoint>> {
        if !candidate.peeked {
            stats.chunks_touched = stats.chunks_touched.saturating_add(1);
        }

        match &candidate.source {
            CandidateSource::Active(points) => {
                let decoded = tail_points(
                    points
                        .iter_points()
                        .filter(|point| point.ts >= start && point.ts < end),
                    max_points,
                );
                stats.points_decoded = stats.points_decoded.saturating_add(decoded.len());
                Ok(decoded)
            }
            CandidateSource::Sealed(chunk) => {
                if chunk.encoded_payload.is_empty() {
                    let decoded = tail_points(
                        chunk
                            .points
                            .iter()
                            .filter(|point| point.ts >= start && point.ts < end),
                        max_points,
                    );
                    stats.points_decoded = stats.points_decoded.saturating_add(decoded.len());
                    return Ok(decoded);
                }

                let descriptor = EncodedChunkDescriptor {
                    lane: chunk.header.lane,
                    ts_codec: chunk.header.ts_codec,
                    value_codec: chunk.header.value_codec,
                    point_count: chunk.header.point_count as usize,
                };
                let search_index = Encoder::build_timestamp_search_index_from_payload(
                    chunk.header.ts_codec,
                    descriptor.point_count,
                    &chunk.encoded_payload,
                )?;
                decode_encoded_tail(
                    descriptor,
                    &chunk.encoded_payload,
                    &search_index,
                    start,
                    end,
                    max_points,
                    stats,
                )
            }
            CandidateSource::Persisted(chunk_ref) => {
                let search_index = self
                    .persisted_chunk_timestamp_search_index(&snapshot.persisted_index, chunk_ref)?;
                let payload =
                    persisted_chunk_payload(&snapshot.persisted_index.segment_maps, chunk_ref)?;
                let descriptor = EncodedChunkDescriptor {
                    lane: chunk_ref.lane,
                    ts_codec: chunk_ref.ts_codec,
                    value_codec: chunk_ref.value_codec,
                    point_count: chunk_ref.point_count as usize,
                };
                decode_encoded_tail(
                    descriptor,
                    payload.as_ref(),
                    search_index,
                    start,
                    end,
                    max_points,
                    stats,
                )
            }
        }
    }
}

/// Returns the newest `max_points` in-memory points as descending `DataPoint`s.
///
/// Only the selected tail values are cloned; the rest of the snapshot is never
/// materialized as values, so `points_decoded` remains tail-bounded.
fn tail_points<'a>(
    points: impl Iterator<Item = &'a ChunkPoint>,
    max_points: usize,
) -> Vec<DataPoint> {
    let mut selected = points.collect::<Vec<_>>();
    selected.sort_by_key(|point| point.ts);
    selected.reverse();
    selected.truncate(max_points);
    selected
        .into_iter()
        .map(|point| DataPoint::new(point.ts, point.value.clone()))
        .collect()
}

/// Finds the newest timestamp in `[start, end)` for a chunk whose header maximum
/// crosses `end`, using the anchor index to decode only the candidate tail block.
fn latest_in_window_timestamp(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    search_index: &TimestampSearchIndex,
    start: i64,
    end: i64,
) -> Result<Option<i64>> {
    if descriptor.point_count == 0 {
        return Ok(None);
    }

    let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload)?;
    let Some(block_idx) =
        search_index.candidate_block_for_ceiling(descriptor.point_count, end.saturating_sub(1))
    else {
        return Ok(None);
    };
    let block_timestamps =
        search_index.decode_block(descriptor.point_count, ts_payload, block_idx)?;
    Ok(block_timestamps
        .iter()
        .rev()
        .find(|timestamp| **timestamp < end)
        .copied()
        .filter(|timestamp| *timestamp >= start))
}

/// Decodes only the descending tail of one encoded chunk needed to fill `max_points`.
///
/// The timestamp search index locates the block containing the last point below
/// `end`; at most one additional block is decoded to find the starting index.
/// Values are then decoded for the selected index range only. Chunks with an
/// unsorted timestamp stream fall back to a full decode followed by an explicit
/// descending sort, and count every decoded value honestly.
fn decode_encoded_tail(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    search_index: &TimestampSearchIndex,
    start: i64,
    end: i64,
    max_points: usize,
    stats: &mut ReverseScanStats,
) -> Result<Vec<DataPoint>> {
    if max_points == 0 || start >= end || descriptor.point_count == 0 {
        return Ok(Vec::new());
    }

    let timestamps = Encoder::decode_timestamps_from_payload(
        descriptor.ts_codec,
        descriptor.point_count,
        payload,
    )?;
    if !timestamps.windows(2).all(|window| window[0] <= window[1]) {
        let decoded = Encoder::decode_chunk_points_from_payload(
            descriptor.lane,
            descriptor.ts_codec,
            descriptor.value_codec,
            descriptor.point_count,
            payload,
        )?;
        stats.points_decoded = stats.points_decoded.saturating_add(decoded.len());

        let mut points = decoded
            .into_iter()
            .filter(|point| point.ts >= start && point.ts < end)
            .map(|point| DataPoint::new(point.ts, point.value))
            .collect::<Vec<_>>();
        points.sort_by_key(|point| point.timestamp);
        points.reverse();
        points.truncate(max_points);
        return Ok(points);
    }

    let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload)?;
    let Some(block_idx) =
        search_index.candidate_block_for_ceiling(descriptor.point_count, end.saturating_sub(1))
    else {
        return Ok(Vec::new());
    };

    let block_timestamps =
        search_index.decode_block(descriptor.point_count, ts_payload, block_idx)?;
    let block_start_idx = block_idx.saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS);
    let Some(last_pos) = block_timestamps
        .iter()
        .rposition(|timestamp| *timestamp < end)
    else {
        return Ok(Vec::new());
    };
    let last_idx = block_start_idx.saturating_add(last_pos);
    if block_timestamps[last_pos] < start {
        return Ok(Vec::new());
    }

    let tail_len = last_idx.saturating_add(1);
    let decode_start_idx = tail_len.saturating_sub(max_points.min(tail_len));
    let decode_start_ts = timestamp_at_index(
        descriptor.point_count,
        ts_payload,
        search_index,
        block_idx,
        block_start_idx,
        &block_timestamps,
        decode_start_idx,
    )?;
    let range_start = decode_start_ts.max(start);

    let decoded = Encoder::decode_chunk_points_from_payload_in_range(
        descriptor.lane,
        descriptor.ts_codec,
        descriptor.value_codec,
        descriptor.point_count,
        payload,
        range_start,
        end,
    )?;
    stats.points_decoded = stats.points_decoded.saturating_add(decoded.len());

    Ok(decoded
        .into_iter()
        .rev()
        .map(|point| DataPoint::new(point.ts, point.value))
        .collect())
}

fn timestamp_at_index(
    point_count: usize,
    ts_payload: &[u8],
    search_index: &TimestampSearchIndex,
    block_idx: usize,
    block_start_idx: usize,
    block_timestamps: &[i64],
    target_idx: usize,
) -> Result<i64> {
    if target_idx >= block_start_idx && target_idx < block_start_idx + block_timestamps.len() {
        return Ok(block_timestamps[target_idx - block_start_idx]);
    }

    let target_block = target_idx / TIMESTAMP_SEARCH_BLOCK_POINTS;
    if target_block == block_idx {
        return Err(TsinkError::DataCorruption(format!(
            "timestamp index {target_idx} not present in decoded block {block_idx}"
        )));
    }
    let timestamps = search_index.decode_block(point_count, ts_payload, target_block)?;
    timestamps
        .get(target_idx % TIMESTAMP_SEARCH_BLOCK_POINTS)
        .copied()
        .ok_or_else(|| {
            TsinkError::DataCorruption(format!(
                "timestamp index {target_idx} is missing from block {target_block}"
            ))
        })
}
