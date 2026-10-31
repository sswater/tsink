//! Forward (timestamp-ascending) streaming tail scan, mirroring
//! `reverse_scan.rs`: one ascending cursor per candidate chunk, merged by
//! point timestamp through a binary heap, with the scan stopping as soon as
//! `limit` points have been collected. Chunks never reached by the merge are
//! never decompressed.
//!
//! Merge ordering is NOT a reversal of the descending `HeapEntry` order:
//! among candidates sharing a timestamp the *newest version* must surface
//! first (so that first-seen dedupe keeps it), i.e. minimal timestamp wins,
//! and ties break towards maximal recency and sequence.

use std::borrow::Cow;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::atomic::Ordering;

use crate::engine::encoder::{Encoder, TimestampSearchIndex, TIMESTAMP_SEARCH_BLOCK_POINTS};
use crate::validation::{validate_labels, validate_metric};
use crate::{DataPoint, Label, Result, TsinkError, ValuePredicate};

use super::super::{elapsed_nanos_u64, saturating_u64_from_usize};
use super::reverse_scan::{
    first_in_window_index, last_in_window_index, timestamps_in_index_range, CandidateSnapshot,
    CandidateSource, ChunkCandidate, ScanDirection, SourceRecency,
};
use super::*;

/// Honest work counters for a forward scan, mirroring [`ReverseScanStats`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ForwardScanStats {
    pub chunks_touched: usize,
    pub points_decoded: usize,
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn select_asc_api(
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
            .select_asc_calls_total
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
            self.record_query_tier_plan(plan);
            let (points, _stats) =
                self.select_asc_with_plan(metric, labels, start, end, limit, predicate, plan)?;
            Ok(points)
        })();

        self.observability
            .query
            .select_asc_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_asc_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_asc_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_asc_with_plan(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        limit: usize,
        predicate: Option<&ValuePredicate>,
        plan: TieredQueryPlan,
    ) -> Result<(Vec<DataPoint>, ForwardScanStats)> {
        let Some(series_id) = self
            .catalog
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            return Ok((Vec::new(), ForwardScanStats::default()));
        };

        self.select_asc_for_series_with_plan(series_id, start, end, limit, predicate, plan)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_asc_for_series_with_plan(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        limit: usize,
        predicate: Option<&ValuePredicate>,
        plan: TieredQueryPlan,
    ) -> Result<(Vec<DataPoint>, ForwardScanStats)> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        if limit == 0 {
            return Ok((Vec::new(), ForwardScanStats::default()));
        }

        let snapshot = self.collect_candidates_with_mode(
            series_id,
            start,
            end,
            plan,
            true,
            ScanDirection::Asc,
        )?;
        if snapshot.candidates.is_empty() {
            return Ok((Vec::new(), ForwardScanStats::default()));
        }

        let mut out = Vec::new();
        let mut stats = ForwardScanStats {
            // Candidates already peeked during ordering were touched once there.
            chunks_touched: snapshot.peeked_chunks,
            points_decoded: 0,
        };
        let retention_cutoff = self.active_retention_cutoff();
        let mut seen_timestamps = HashSet::with_capacity(limit.min(4096));

        // One ascending cursor per candidate, merged by point timestamp. Each
        // cursor decodes in batches of at most one anchor block, so the merge
        // never materializes more than the current batch per active cursor.
        let mut cursors = snapshot
            .candidates
            .iter()
            .map(|_| AscCursorState::default())
            .collect::<Vec<_>>();
        let mut heap = BinaryHeap::with_capacity(snapshot.candidates.len());
        for (index, candidate) in snapshot.candidates.iter().enumerate() {
            heap.push(AscHeapEntry {
                timestamp: candidate.min_ts,
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
                        heap.push(AscHeapEntry {
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
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct AscHeapEntry {
    timestamp: i64,
    recency: SourceRecency,
    sequence: u64,
    candidate_index: usize,
}

impl Ord for AscHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap pops the largest element; the ascending merge wants the
        // smallest timestamp first, but among equal timestamps the NEWEST
        // version (maximal recency, then sequence) must surface first so that
        // first-seen dedupe keeps it.
        other
            .timestamp
            .cmp(&self.timestamp)
            .then_with(|| self.recency.cmp(&other.recency))
            .then_with(|| self.sequence.cmp(&other.sequence))
            .then_with(|| self.candidate_index.cmp(&other.candidate_index))
    }
}

impl PartialOrd for AscHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct AscCursorState<'a> {
    data: Option<AscCursorData<'a>>,
    initialized: bool,
}

impl<'a> AscCursorState<'a> {
    fn next_point(
        &mut self,
        storage: &ChunkStorage,
        snapshot: &'a CandidateSnapshot<'_>,
        candidate_index: usize,
        start: i64,
        end: i64,
        stats: &mut ForwardScanStats,
    ) -> Result<Option<DataPoint>> {
        if !self.initialized {
            self.initialized = true;
            let candidate = &snapshot.candidates[candidate_index];
            if !candidate.peeked {
                stats.chunks_touched = stats.chunks_touched.saturating_add(1);
            }
            self.data = Some(AscCursorData::new(
                storage, snapshot, candidate, start, end, stats,
            )?);
        }

        self.data
            .as_mut()
            .map_or(Ok(None), |data| data.next_point(stats))
    }

    fn head_timestamp(&self) -> Option<i64> {
        self.data.as_ref().and_then(AscCursorData::head_timestamp)
    }
}

enum AscCursorData<'a> {
    /// In-memory active/partition or payload-less sealed points, ordered ascending.
    Refs {
        points: Vec<&'a ChunkPoint>,
        pos: usize,
    },
    /// Full fallback decode for an inconsistent (unsorted) encoded chunk.
    Buffered { points: Vec<DataPoint>, pos: usize },
    /// Anchor-backed encoded chunk cursor.
    Encoded(AscEncodedCursor<'a>),
}

impl<'a> AscCursorData<'a> {
    fn new(
        storage: &ChunkStorage,
        snapshot: &'a CandidateSnapshot<'_>,
        candidate: &'a ChunkCandidate,
        start: i64,
        end: i64,
        stats: &mut ForwardScanStats,
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
        // Mirror of desc's `sort + reverse`: keep the newest inserted version
        // first within each equal-timestamp run so first-seen dedupe keeps it.
        points.reverse();
        points.sort_by_key(|point| point.ts);
        Self::Refs { points, pos: 0 }
    }

    fn new_encoded(
        descriptor: EncodedChunkDescriptor,
        payload: Cow<'a, [u8]>,
        search_index: Cow<'a, TimestampSearchIndex>,
        start: i64,
        end: i64,
        stats: &mut ForwardScanStats,
    ) -> Result<Self> {
        if descriptor.point_count == 0 {
            return Ok(Self::Buffered {
                points: Vec::new(),
                pos: 0,
            });
        }

        let first_index =
            first_in_window_index(descriptor, payload.as_ref(), &search_index, start)?;
        let Some(last_index) =
            last_in_window_index(descriptor, payload.as_ref(), &search_index, end)
        else {
            return Self::full_fallback(descriptor, payload.as_ref(), start, end, None, stats);
        };
        if first_index > last_index {
            return Self::full_fallback(descriptor, payload.as_ref(), start, end, None, stats);
        }
        if Self::has_duplicate_timestamp_at_block_boundary(
            descriptor,
            payload.as_ref(),
            &search_index,
            first_index,
            last_index,
        )? {
            // A duplicate timestamp straddles a block boundary: ascending
            // batches would emit the older index first and first-seen dedupe
            // would keep the stale value. The full fallback's single sorted
            // vector orders newest-first within equal timestamps.
            return Self::full_fallback(descriptor, payload.as_ref(), start, end, None, stats);
        }

        let encoded = AscEncodedCursor {
            descriptor,
            payload,
            search_index,
            last_index,
            next_index: Some(first_index),
            buffer: VecDeque::new(),
            previous_batch_max_ts: None,
            start,
            end,
            last_emitted_ts: None,
        };
        Ok(Self::Encoded(encoded))
    }

    /// Detects a duplicate timestamp that straddles a search-block boundary
    /// inside the window `[first_index, last_index]`: the last index of one
    /// block and the first index of the next carry the same timestamp.
    /// Timestamps-only decode, at most one probe per block boundary.
    fn has_duplicate_timestamp_at_block_boundary(
        descriptor: EncodedChunkDescriptor,
        payload: &[u8],
        search_index: &TimestampSearchIndex,
        first_index: usize,
        last_index: usize,
    ) -> Result<bool> {
        let first_block = first_index.div_ceil(TIMESTAMP_SEARCH_BLOCK_POINTS);
        let last_block = last_index / TIMESTAMP_SEARCH_BLOCK_POINTS;
        for block in first_block..=last_block {
            let boundary = block * TIMESTAMP_SEARCH_BLOCK_POINTS;
            if boundary == 0 || boundary > last_index {
                continue;
            }
            let left = timestamps_in_index_range(
                descriptor,
                payload,
                search_index,
                boundary - 1,
                boundary - 1,
            )?;
            let right =
                timestamps_in_index_range(descriptor, payload, search_index, boundary, boundary)?;
            if let (Some(before), Some(after)) = (left.first(), right.first()) {
                if before == after {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn full_fallback(
        descriptor: EncodedChunkDescriptor,
        payload: &[u8],
        start: i64,
        end: i64,
        last_emitted_ts: Option<i64>,
        stats: &mut ForwardScanStats,
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
                    && last_emitted_ts.is_none_or(|ceiling| point.ts > ceiling)
            })
            .map(|point| DataPoint::new(point.ts, point.value))
            .collect::<Vec<_>>();
        // Mirror of desc's `sort + reverse` (see `from_refs`).
        points.reverse();
        points.sort_by_key(|point| point.timestamp);
        Ok(Self::Buffered { points, pos: 0 })
    }

    fn next_point(&mut self, stats: &mut ForwardScanStats) -> Result<Option<DataPoint>> {
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
                let point = cursor.buffer.pop_front();
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
            Self::Encoded(cursor) => cursor.buffer.front().map(|point| point.timestamp),
        }
    }
}

struct AscEncodedCursor<'a> {
    descriptor: EncodedChunkDescriptor,
    payload: Cow<'a, [u8]>,
    search_index: Cow<'a, TimestampSearchIndex>,
    last_index: usize,
    next_index: Option<usize>,
    /// Decoded batch in ascending order; `pop_front()` yields the oldest
    /// unconsumed point first.
    buffer: VecDeque<DataPoint>,
    previous_batch_max_ts: Option<i64>,
    start: i64,
    end: i64,
    last_emitted_ts: Option<i64>,
}

impl<'a> AscEncodedCursor<'a> {
    /// Loads the next block-aligned batch and returns `true` when the timestamp
    /// stream is inconsistent (non-ascending within a batch or across a batch
    /// boundary).
    fn load_batch(&mut self, stats: &mut ForwardScanStats) -> Result<bool> {
        let Some(next_index) = self.next_index else {
            return Ok(false);
        };
        let block_start = (next_index / TIMESTAMP_SEARCH_BLOCK_POINTS)
            .saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS);
        let batch_end = (block_start + TIMESTAMP_SEARCH_BLOCK_POINTS - 1).min(self.last_index);
        let timestamps = timestamps_in_index_range(
            self.descriptor,
            self.payload.as_ref(),
            &self.search_index,
            next_index,
            batch_end,
        )?;

        let ascending = timestamps.windows(2).all(|window| window[0] <= window[1]);
        let boundary_ok = match (self.previous_batch_max_ts, timestamps.first()) {
            (Some(previous_max), Some(batch_min)) => *batch_min >= previous_max,
            _ => true,
        };
        if !ascending || !boundary_ok {
            return Ok(true);
        }
        self.previous_batch_max_ts = timestamps.last().copied();

        let indexes = (next_index..=batch_end).collect::<Vec<_>>();
        let values = Encoder::decode_chunk_values_at_indexes(
            self.descriptor.lane,
            self.descriptor.value_codec,
            self.descriptor.point_count,
            self.payload.as_ref(),
            &indexes,
        )?;
        if values.len() != timestamps.len() {
            return Err(TsinkError::DataCorruption(
                "timestamp/value count mismatch while decoding forward batch".to_string(),
            ));
        }
        stats.points_decoded = stats.points_decoded.saturating_add(values.len());

        let mut batch = timestamps
            .into_iter()
            .zip(values)
            .map(|(timestamp, value)| DataPoint::new(timestamp, value))
            .collect::<Vec<_>>();
        // Mirror of desc's tail-popping batch (see `from_refs`): newest
        // inserted version first within each equal-timestamp run.
        batch.reverse();
        batch.sort_by_key(|point| point.timestamp);
        self.buffer = batch.into_iter().collect();
        self.next_index = if batch_end < self.last_index {
            Some(batch_end + 1)
        } else {
            None
        };
        Ok(false)
    }
}
