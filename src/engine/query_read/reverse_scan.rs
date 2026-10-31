use std::borrow::Cow;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::RwLockReadGuard;

use crate::engine::encoder::{Encoder, TimestampSearchIndex, TIMESTAMP_SEARCH_BLOCK_POINTS};
use crate::validation::{validate_labels, validate_metric};
use crate::{DataPoint, Label, Result, TsinkError, ValuePredicate};

use super::super::{elapsed_nanos_u64, saturating_u64_from_usize};
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
/// tail block to find the actual newest point below `end`. This keeps the merge
/// ordering honest without decoding any values. Point lookups skip that peek and
/// keep the header maximum instead.
pub(super) struct ChunkCandidate {
    pub min_ts: i64,
    pub max_ts: i64,
    pub recency: SourceRecency,
    pub(super) sequence: u64,
    pub(super) peeked: bool,
    pub(super) source: CandidateSource,
}

pub(super) enum CandidateSource {
    Persisted(PersistedChunkRef),
    Sealed(Arc<Chunk>),
    Active(ActivePartitionSnapshot),
}

/// Honest work counters for a reverse scan.
///
/// `points_decoded` counts every point whose **value** was materialized, before
/// any output filtering or truncation. Timestamps decoded by cursor batches are
/// not counted; batches are aligned to search blocks, so each cursor step decodes
/// at most one block (64 timestamps) per chunk.
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
    pub(super) peeked_chunks: usize,
}

/// Scan direction for candidate collection and merge ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanDirection {
    Asc,
    Desc,
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn select_desc_api(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        limit: usize,
        predicate: Option<&ValuePredicate>,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_desc_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = std::time::Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            self.ensure_open()?;
            validate_metric(metric)?;
            validate_labels(labels)?;
            self.request_background_persisted_refresh_if_needed();
            if start >= end {
                return Err(TsinkError::InvalidTimeRange { start, end });
            }
            if limit == 0 {
                return Ok(Vec::new());
            }

            let plan = self.query_tier_plan(start, end);
            let (points, _stats) =
                self.select_desc_with_plan(metric, labels, start, end, limit, predicate, plan)?;
            Ok(points)
        })();

        self.observability
            .query
            .select_desc_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_desc_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_desc_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_desc_with_plan(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        limit: usize,
        predicate: Option<&ValuePredicate>,
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

        self.select_desc_for_series_with_plan(series_id, start, end, limit, predicate, plan)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_desc_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        limit: usize,
        predicate: Option<&ValuePredicate>,
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

        // One descending cursor per candidate, merged by point timestamp. Each
        // cursor decodes in batches of at most one anchor block, so the merge
        // never materializes more than the current batch per active cursor.
        let mut cursors = snapshot
            .candidates
            .iter()
            .map(|_| CursorState::default())
            .collect::<Vec<_>>();
        let mut heap = BinaryHeap::with_capacity(snapshot.candidates.len());
        for (index, candidate) in snapshot.candidates.iter().enumerate() {
            heap.push(HeapEntry {
                timestamp: candidate.max_ts,
                recency: candidate.recency,
                sequence: candidate.sequence,
                candidate_index: index,
            });
        }

        self.tombstone_read_context().with_series_tombstone_ranges(
            series_id,
            |tombstone_ranges| -> Result<()> {
                while out.len() < limit {
                    let Some(entry) = heap.pop() else {
                        break;
                    };
                    let candidate_index = entry.candidate_index;
                    let Some(point) = cursors[candidate_index].next_point(
                        self,
                        &snapshot,
                        candidate_index,
                        start,
                        end,
                        &mut stats,
                    )?
                    else {
                        continue;
                    };

                    // Re-queue the cursor for its next point before filtering.
                    // Filtered or duplicate-dropped points advance the same
                    // cursor, so a chunk refills itself instead of moving on.
                    if let Some(next_timestamp) = cursors[candidate_index].head_timestamp() {
                        heap.push(HeapEntry {
                            timestamp: next_timestamp,
                            recency: entry.recency,
                            sequence: entry.sequence,
                            candidate_index,
                        });
                    }

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
                    if let Some(predicate) = predicate {
                        if !point.value.matches_predicate(predicate) {
                            // The cursor was already re-queued above, so this
                            // chunk keeps refilling the heap with its next
                            // point until `limit` matches are collected.
                            continue;
                        }
                    }
                    out.push(point);
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
        self.collect_candidates_with_mode(series_id, start, end, plan, true, ScanDirection::Desc)
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
        self.collect_candidates_with_mode(
            series_id,
            start,
            end,
            plan,
            false,
            ScanDirection::Desc,
        )
    }

    pub(super) fn collect_candidates_with_mode(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        peek: bool,
        direction: ScanDirection,
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
                    let (min_ts, max_ts, peeked) = match direction {
                        ScanDirection::Desc => {
                            if !peek || chunk.header.max_ts < end {
                                (chunk.header.min_ts, chunk.header.max_ts, false)
                            } else {
                                let Some(max_ts) =
                                    self.latest_sealed_in_window_ts(chunk, start, end)?
                                else {
                                    continue;
                                };
                                peeked_chunks = peeked_chunks.saturating_add(1);
                                (chunk.header.min_ts, max_ts, true)
                            }
                        }
                        ScanDirection::Asc => {
                            if !peek || chunk.header.min_ts >= start {
                                (chunk.header.min_ts, chunk.header.max_ts, false)
                            } else {
                                let Some(min_ts) =
                                    self.earliest_sealed_in_window_ts(chunk, start, end)?
                                else {
                                    continue;
                                };
                                peeked_chunks = peeked_chunks.saturating_add(1);
                                (min_ts, chunk.header.max_ts, true)
                            }
                        }
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

                let (min_ts, max_ts, peeked) = match direction {
                    ScanDirection::Desc => {
                        if !peek || chunk_ref.max_ts < end {
                            (chunk_ref.min_ts, chunk_ref.max_ts, false)
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
                            (chunk_ref.min_ts, max_ts, true)
                        }
                    }
                    ScanDirection::Asc => {
                        if !peek || chunk_ref.min_ts >= start {
                            (chunk_ref.min_ts, chunk_ref.max_ts, false)
                        } else {
                            let Some(min_ts) = self.earliest_persisted_in_window_ts(
                                &persisted_index,
                                chunk_ref,
                                start,
                                end,
                            )?
                            else {
                                continue;
                            };
                            peeked_chunks = peeked_chunks.saturating_add(1);
                            (min_ts, chunk_ref.max_ts, true)
                        }
                    }
                };
                candidates.push(ChunkCandidate {
                    min_ts,
                    max_ts,
                    recency: SourceRecency::Persisted,
                    sequence: chunk_ref.sequence,
                    peeked,
                    source: CandidateSource::Persisted(*chunk_ref),
                });
            }
        }
        drop(visibility_guard);

        match direction {
            ScanDirection::Desc => {
                candidates.sort_by(|left, right| {
                    right
                        .max_ts
                        .cmp(&left.max_ts)
                        .then_with(|| right.recency.cmp(&left.recency))
                        .then_with(|| right.sequence.cmp(&left.sequence))
                });
            }
            ScanDirection::Asc => {
                candidates.sort_by(|left, right| {
                    left.min_ts
                        .cmp(&right.min_ts)
                        .then_with(|| right.recency.cmp(&left.recency))
                        .then_with(|| right.sequence.cmp(&left.sequence))
                });
            }
        }

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

    fn earliest_sealed_in_window_ts(
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
                .min());
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
        earliest_in_window_timestamp(
            descriptor,
            &chunk.encoded_payload,
            &search_index,
            start,
            end,
        )
    }

    fn earliest_persisted_in_window_ts(
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
        earliest_in_window_timestamp(descriptor, payload.as_ref(), search_index, start, end)
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
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct HeapEntry {
    timestamp: i64,
    recency: SourceRecency,
    sequence: u64,
    candidate_index: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.recency.cmp(&other.recency))
            .then_with(|| self.sequence.cmp(&other.sequence))
            .then_with(|| self.candidate_index.cmp(&other.candidate_index))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct CursorState<'a> {
    data: Option<CursorData<'a>>,
    initialized: bool,
}

impl<'a> CursorState<'a> {
    fn next_point(
        &mut self,
        storage: &ChunkStorage,
        snapshot: &'a CandidateSnapshot<'_>,
        candidate_index: usize,
        start: i64,
        end: i64,
        stats: &mut ReverseScanStats,
    ) -> Result<Option<DataPoint>> {
        if !self.initialized {
            self.initialized = true;
            let candidate = &snapshot.candidates[candidate_index];
            if !candidate.peeked {
                stats.chunks_touched = stats.chunks_touched.saturating_add(1);
            }
            self.data = Some(CursorData::new(
                storage, snapshot, candidate, start, end, stats,
            )?);
        }

        self.data
            .as_mut()
            .map_or(Ok(None), |data| data.next_point(stats))
    }

    fn head_timestamp(&self) -> Option<i64> {
        self.data.as_ref().and_then(CursorData::head_timestamp)
    }
}

enum CursorData<'a> {
    /// In-memory active/partition or payload-less sealed points, ordered descending.
    Refs {
        points: Vec<&'a ChunkPoint>,
        pos: usize,
    },
    /// Full fallback decode for an inconsistent (unsorted) encoded chunk.
    Buffered { points: Vec<DataPoint>, pos: usize },
    /// Anchor-backed encoded chunk cursor.
    Encoded(EncodedCursor<'a>),
}

impl<'a> CursorData<'a> {
    fn new(
        storage: &ChunkStorage,
        snapshot: &'a CandidateSnapshot<'_>,
        candidate: &'a ChunkCandidate,
        start: i64,
        end: i64,
        stats: &mut ReverseScanStats,
    ) -> Result<Self> {
        match &candidate.source {
            CandidateSource::Active(points) => Ok(Self::from_refs(
                points
                    .iter_points()
                    .filter(|point| point.ts >= start && point.ts < end),
            )),
            CandidateSource::Sealed(chunk) => {
                if chunk.encoded_payload.is_empty() {
                    return Ok(Self::from_refs(
                        chunk
                            .points
                            .iter()
                            .filter(|point| point.ts >= start && point.ts < end),
                    ));
                }

                let descriptor = EncodedChunkDescriptor {
                    lane: chunk.header.lane,
                    ts_codec: chunk.header.ts_codec,
                    value_codec: chunk.header.value_codec,
                    point_count: chunk.header.point_count as usize,
                };
                let search_index = Cow::Owned(Encoder::build_timestamp_search_index_from_payload(
                    descriptor.ts_codec,
                    descriptor.point_count,
                    &chunk.encoded_payload,
                )?);
                Self::new_encoded(
                    descriptor,
                    Cow::Borrowed(chunk.encoded_payload.as_slice()),
                    search_index,
                    start,
                    end,
                    stats,
                )
            }
            CandidateSource::Persisted(chunk_ref) => {
                let search_index = Cow::Borrowed(storage.persisted_chunk_timestamp_search_index(
                    &snapshot.persisted_index,
                    chunk_ref,
                )?);
                let payload =
                    persisted_chunk_payload(&snapshot.persisted_index.segment_maps, chunk_ref)?;
                let descriptor = EncodedChunkDescriptor {
                    lane: chunk_ref.lane,
                    ts_codec: chunk_ref.ts_codec,
                    value_codec: chunk_ref.value_codec,
                    point_count: chunk_ref.point_count as usize,
                };
                Self::new_encoded(descriptor, payload, search_index, start, end, stats)
            }
        }
    }

    fn from_refs(points: impl Iterator<Item = &'a ChunkPoint>) -> Self {
        let mut points = points.collect::<Vec<_>>();
        points.sort_by_key(|point| point.ts);
        points.reverse();
        Self::Refs { points, pos: 0 }
    }

    fn new_encoded(
        descriptor: EncodedChunkDescriptor,
        payload: Cow<'a, [u8]>,
        search_index: Cow<'a, TimestampSearchIndex>,
        start: i64,
        end: i64,
        stats: &mut ReverseScanStats,
    ) -> Result<Self> {
        if descriptor.point_count == 0 {
            return Ok(Self::Buffered {
                points: Vec::new(),
                pos: 0,
            });
        }

        let Some(last_index) =
            last_in_window_index(descriptor, payload.as_ref(), &search_index, end)
        else {
            // Anchor miss where the candidate said data exists: fall back to the
            // full decode path for this chunk only.
            return Self::full_fallback(descriptor, payload.as_ref(), start, end, None, stats);
        };
        let first_index =
            first_in_window_index(descriptor, payload.as_ref(), &search_index, start)?;
        if first_index > last_index {
            return Self::full_fallback(descriptor, payload.as_ref(), start, end, None, stats);
        }

        let encoded = EncodedCursor {
            descriptor,
            payload,
            search_index,
            first_index,
            next_index: Some(last_index),
            buffer: Vec::new(),
            previous_batch_min_ts: None,
            start,
            end,
            last_emitted_ts: None,
        };
        Ok(Self::Encoded(encoded))
    }

    fn full_fallback(
        descriptor: EncodedChunkDescriptor,
        payload: &[u8],
        start: i64,
        end: i64,
        last_emitted_ts: Option<i64>,
        stats: &mut ReverseScanStats,
    ) -> Result<Self> {
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
            .filter(|point| {
                point.ts >= start
                    && point.ts < end
                    && last_emitted_ts.is_none_or(|floor| point.ts < floor)
            })
            .map(|point| DataPoint::new(point.ts, point.value))
            .collect::<Vec<_>>();
        points.sort_by_key(|point| point.timestamp);
        points.reverse();
        Ok(Self::Buffered { points, pos: 0 })
    }

    fn next_point(&mut self, stats: &mut ReverseScanStats) -> Result<Option<DataPoint>> {
        let fallback = if let Self::Encoded(cursor) = self {
            cursor.buffer.is_empty() && cursor.next_index.is_some()
        } else {
            false
        };
        if fallback {
            let inconsistent = {
                let Self::Encoded(cursor) = self else {
                    unreachable!("checked encoded cursor above");
                };
                cursor.load_batch(stats)?
            };
            if inconsistent {
                let replacement = {
                    let Self::Encoded(cursor) = self else {
                        unreachable!("checked encoded cursor above");
                    };
                    Self::full_fallback(
                        cursor.descriptor,
                        cursor.payload.as_ref(),
                        cursor.start,
                        cursor.end,
                        cursor.last_emitted_ts,
                        stats,
                    )?
                };
                *self = replacement;
                return self.next_point(stats);
            }
        }

        match self {
            Self::Refs { points, pos } => {
                let Some(point) = points.get(*pos) else {
                    return Ok(None);
                };
                *pos = pos.saturating_add(1);
                Ok(Some(DataPoint::new(point.ts, point.value.clone())))
            }
            Self::Buffered { points, pos } => {
                let Some(point) = points.get(*pos) else {
                    return Ok(None);
                };
                *pos = pos.saturating_add(1);
                Ok(Some(point.clone()))
            }
            Self::Encoded(cursor) => {
                let point = cursor.buffer.pop();
                if let Some(point) = &point {
                    cursor.last_emitted_ts = Some(point.timestamp);
                }

                // Pull the next batch eagerly so the heap head key is known
                // without a separate timestamp lookahead decode.
                let mut replacement = None;
                if cursor.buffer.is_empty()
                    && cursor.next_index.is_some()
                    && cursor.load_batch(stats)?
                {
                    replacement = Some(Self::full_fallback(
                        cursor.descriptor,
                        cursor.payload.as_ref(),
                        cursor.start,
                        cursor.end,
                        cursor.last_emitted_ts,
                        stats,
                    )?);
                }
                if let Some(replacement) = replacement {
                    *self = replacement;
                }
                Ok(point)
            }
        }
    }

    fn head_timestamp(&self) -> Option<i64> {
        match self {
            Self::Refs { points, pos } => points.get(*pos).map(|point| point.ts),
            Self::Buffered { points, pos } => points.get(*pos).map(|point| point.timestamp),
            Self::Encoded(cursor) => cursor.buffer.last().map(|point| point.timestamp),
        }
    }
}

struct EncodedCursor<'a> {
    descriptor: EncodedChunkDescriptor,
    payload: Cow<'a, [u8]>,
    search_index: Cow<'a, TimestampSearchIndex>,
    first_index: usize,
    next_index: Option<usize>,
    /// Decoded batch in ascending order; `pop()` yields the newest point first.
    buffer: Vec<DataPoint>,
    previous_batch_min_ts: Option<i64>,
    start: i64,
    end: i64,
    last_emitted_ts: Option<i64>,
}

impl<'a> EncodedCursor<'a> {
    /// Loads the next block-aligned batch and returns `true` when the timestamp
    /// stream is inconsistent (non-ascending within a batch or across a batch
    /// boundary).
    fn load_batch(&mut self, stats: &mut ReverseScanStats) -> Result<bool> {
        let Some(next_index) = self.next_index else {
            return Ok(false);
        };
        let block_start = (next_index / TIMESTAMP_SEARCH_BLOCK_POINTS)
            .saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS);
        let batch_start = block_start.max(self.first_index);
        let timestamps = timestamps_in_index_range(
            self.descriptor,
            self.payload.as_ref(),
            &self.search_index,
            batch_start,
            next_index,
        )?;

        let ascending = timestamps.windows(2).all(|window| window[0] <= window[1]);
        let boundary_ok = match (self.previous_batch_min_ts, timestamps.last()) {
            (Some(previous_min), Some(batch_max)) => *batch_max <= previous_min,
            _ => true,
        };
        if !ascending || !boundary_ok {
            return Ok(true);
        }
        self.previous_batch_min_ts = timestamps.first().copied();

        let indexes = (batch_start..=next_index).collect::<Vec<_>>();
        let values = Encoder::decode_chunk_values_at_indexes(
            self.descriptor.lane,
            self.descriptor.value_codec,
            self.descriptor.point_count,
            self.payload.as_ref(),
            &indexes,
        )?;
        if values.len() != timestamps.len() {
            return Err(TsinkError::DataCorruption(
                "timestamp/value count mismatch while decoding reverse batch".to_string(),
            ));
        }
        stats.points_decoded = stats.points_decoded.saturating_add(values.len());

        self.buffer = timestamps
            .into_iter()
            .zip(values)
            .map(|(timestamp, value)| DataPoint::new(timestamp, value))
            .collect();
        self.next_index = if batch_start > self.first_index {
            Some(batch_start - 1)
        } else {
            None
        };
        Ok(false)
    }
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

/// Finds the earliest timestamp in `[start, end)` for a chunk whose header
/// minimum falls below `start`, using the anchor index to decode only the
/// candidate head block.
fn earliest_in_window_timestamp(
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
    let Some(block_idx) = search_index.candidate_block_for_ceiling(descriptor.point_count, start)
    else {
        return Ok(None);
    };
    let block_timestamps =
        search_index.decode_block(descriptor.point_count, ts_payload, block_idx)?;
    Ok(block_timestamps
        .iter()
        .find(|timestamp| **timestamp >= start)
        .copied()
        .filter(|timestamp| *timestamp < end))
}

/// Returns the highest point index whose timestamp is below `end`.
pub(super) fn last_in_window_index(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    search_index: &TimestampSearchIndex,
    end: i64,
) -> Option<usize> {
    if descriptor.point_count == 0 {
        return None;
    }
    let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload).ok()?;
    let block_idx =
        search_index.candidate_block_for_ceiling(descriptor.point_count, end.saturating_sub(1))?;
    let block_timestamps = search_index
        .decode_block(descriptor.point_count, ts_payload, block_idx)
        .ok()?;
    let position = block_timestamps
        .iter()
        .rposition(|timestamp| *timestamp < end)?;
    Some(
        block_idx
            .saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS)
            .saturating_add(position),
    )
}

/// Returns the lowest point index whose timestamp is at or above `start`.
pub(super) fn first_in_window_index(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    search_index: &TimestampSearchIndex,
    start: i64,
) -> Result<usize> {
    let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload)?;
    let Some(block_idx) =
        search_index.candidate_block_for_ceiling(descriptor.point_count, start.saturating_sub(1))
    else {
        return Ok(0);
    };
    let block_timestamps =
        search_index.decode_block(descriptor.point_count, ts_payload, block_idx)?;
    let position = block_timestamps
        .iter()
        .rposition(|timestamp| *timestamp < start)
        .map_or(0, |position| position.saturating_add(1));
    Ok(block_idx
        .saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS)
        .saturating_add(position))
}

pub(super) fn timestamps_in_index_range(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    search_index: &TimestampSearchIndex,
    from: usize,
    to_inclusive: usize,
) -> Result<Vec<i64>> {
    let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload)?;
    let start_block = from / TIMESTAMP_SEARCH_BLOCK_POINTS;
    let end_block = to_inclusive / TIMESTAMP_SEARCH_BLOCK_POINTS;
    let mut out = Vec::with_capacity(to_inclusive.saturating_sub(from).saturating_add(1));
    for block_idx in start_block..=end_block {
        let block = search_index.decode_block(descriptor.point_count, ts_payload, block_idx)?;
        let block_start = block_idx.saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS);
        let lo = from.saturating_sub(block_start).min(block.len());
        let hi = (to_inclusive.saturating_sub(block_start).saturating_add(1)).min(block.len());
        if lo < hi {
            out.extend_from_slice(&block[lo..hi]);
        }
    }
    Ok(out)
}
