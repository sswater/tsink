use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::engine::encoder::{Encoder, TimestampSearchIndex};
use crate::validation::{validate_labels, validate_metric};
use crate::{DataPoint, Label, Result, TsinkError};

use super::super::state::ActivePartitionSnapshot;
use super::*;

const TIMESTAMP_SEARCH_BLOCK_POINTS: usize = 64;

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
/// Bound metadata comes from chunk headers / active snapshots, so ordering the
/// candidates never decodes a chunk.
pub(super) struct ChunkCandidate {
    pub max_ts: i64,
    pub recency: SourceRecency,
    sequence: u64,
    pub(super) source: CandidateSource,
}

pub(super) enum CandidateSource {
    Persisted(PersistedChunkRef),
    Sealed(Arc<Chunk>),
    Active(ActivePartitionSnapshot),
}

/// Honest work counters for a reverse scan. `points_decoded` counts only points
/// whose values were actually materialized, not candidates that were only ordered.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReverseScanStats {
    pub chunks_touched: usize,
    pub points_decoded: usize,
}

#[derive(Default)]
pub(super) struct DescScanSnapshot {
    pub(super) candidates: Vec<ChunkCandidate>,
    pub(super) segment_maps: HashMap<usize, Arc<PlatformMmap>>,
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
        let mut out = Vec::new();
        let mut stats = ReverseScanStats::default();
        if limit == 0 || start >= end {
            return Ok((out, stats));
        }

        let snapshot = self.collect_candidates(series_id, start, end, plan)?;
        if snapshot.candidates.is_empty() {
            return Ok((out, stats));
        }

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
    /// and orders them by `(max_ts desc, recency desc, sequence desc)`.
    pub(super) fn collect_candidates(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
    ) -> Result<DescScanSnapshot> {
        let context = self.query_snapshot_context();
        let _visibility_guard = context.visibility_read_fence();
        let mut snapshot = DescScanSnapshot::default();

        {
            let persisted_index = context.persisted_index.read();
            let mut segment_slots = BTreeSet::<usize>::new();
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

                    segment_slots.insert(chunk_ref.segment_slot);
                    snapshot.candidates.push(ChunkCandidate {
                        max_ts: chunk_ref.max_ts,
                        recency: SourceRecency::Persisted,
                        sequence: chunk_ref.sequence,
                        source: CandidateSource::Persisted(*chunk_ref),
                    });
                }
            }

            for slot in segment_slots {
                let segment_map = persisted_index.segment_maps.get(&slot).ok_or_else(|| {
                    TsinkError::DataCorruption(format!("missing mapped segment slot {slot}"))
                })?;
                snapshot.segment_maps.insert(slot, Arc::clone(segment_map));
            }
        }

        {
            let sealed = context.chunks.sealed_shard(series_id).read();
            if let Some(chunks) = sealed.get(&series_id) {
                let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                for (key, chunk) in chunks.range(..end_bound) {
                    if chunk.header.max_ts < start {
                        continue;
                    }
                    snapshot.candidates.push(ChunkCandidate {
                        max_ts: chunk.header.max_ts,
                        recency: SourceRecency::Sealed,
                        sequence: key.sequence,
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

                    let mut max_ts = i64::MIN;
                    for point in points.iter_points() {
                        max_ts = max_ts.max(point.ts);
                    }

                    snapshot.candidates.push(ChunkCandidate {
                        max_ts,
                        recency: SourceRecency::Active,
                        sequence: u64::MAX,
                        source: CandidateSource::Active(ActivePartitionSnapshot::from_points(
                            points,
                        )),
                    });
                }
            }
        }

        snapshot.candidates.sort_by(|left, right| {
            right
                .max_ts
                .cmp(&left.max_ts)
                .then_with(|| right.recency.cmp(&left.recency))
                .then_with(|| right.sequence.cmp(&left.sequence))
        });
        Ok(snapshot)
    }

    fn decode_candidate_tail(
        &self,
        snapshot: &DescScanSnapshot,
        candidate: &ChunkCandidate,
        start: i64,
        end: i64,
        max_points: usize,
        stats: &mut ReverseScanStats,
    ) -> Result<Vec<DataPoint>> {
        stats.chunks_touched = stats.chunks_touched.saturating_add(1);

        match &candidate.source {
            CandidateSource::Active(points) => {
                let mut decoded = points
                    .iter_points()
                    .filter(|point| point.ts >= start && point.ts < end)
                    .map(|point| DataPoint::new(point.ts, point.value.clone()))
                    .collect::<Vec<_>>();
                decoded.sort_by_key(|point| point.timestamp);
                decoded.reverse();
                decoded.truncate(max_points);
                stats.points_decoded = stats.points_decoded.saturating_add(decoded.len());
                Ok(decoded)
            }
            CandidateSource::Sealed(chunk) => {
                if chunk.encoded_payload.is_empty() {
                    let mut decoded = chunk
                        .points
                        .iter()
                        .filter(|point| point.ts >= start && point.ts < end)
                        .map(|point| DataPoint::new(point.ts, point.value.clone()))
                        .collect::<Vec<_>>();
                    decoded.sort_by_key(|point| point.timestamp);
                    decoded.reverse();
                    decoded.truncate(max_points);
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
                let persisted_index = self.persisted.persisted_index.read();
                let search_index =
                    self.persisted_chunk_timestamp_search_index(&persisted_index, chunk_ref)?;
                let payload = persisted_chunk_payload(&snapshot.segment_maps, chunk_ref)?;
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

    let mut points = Vec::with_capacity(decoded.len());
    for point in decoded.into_iter().rev() {
        points.push(DataPoint::new(point.ts, point.value));
    }
    points.truncate(max_points);
    stats.points_decoded = stats.points_decoded.saturating_add(points.len());
    Ok(points)
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
