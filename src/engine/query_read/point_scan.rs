use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::engine::encoder::{Encoder, TimestampSearchIndex, TIMESTAMP_SEARCH_BLOCK_POINTS};
use crate::validation::{validate_labels, validate_metric};
use crate::{DataPoint, Label, Result, Value};

use super::reverse_scan::{CandidateSnapshot, CandidateSource};
use super::*;

/// Honest work counters for a point scan.
///
/// `chunk_groups_touched` counts the distinct `(source layer, chunk)` groups
/// whose payload was fetched and decoded. In-memory active/sealed groups are
/// counted too, so this is an upper bound on the number of zstd decompressions
/// (which equals the number of touched persisted groups), independent of how
/// many timestamps were requested.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct PointScanStats {
    pub chunk_groups_touched: usize,
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn select_at_timestamps_api(
        &self,
        metric: &str,
        labels: &[Label],
        timestamps: &[i64],
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        validate_metric(metric)?;
        validate_labels(labels)?;
        if timestamps.is_empty() {
            return Ok(Vec::new());
        }

        let Some(series_id) = self
            .catalog
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            return Ok(Vec::new());
        };

        let (points, _stats) = self.select_at_timestamps_for_series(series_id, timestamps)?;
        Ok(points)
    }

    pub(super) fn select_at_timestamps_for_series(
        &self,
        series_id: SeriesId,
        timestamps: &[i64],
    ) -> Result<(Vec<DataPoint>, PointScanStats)> {
        let mut targets = timestamps.to_vec();
        targets.sort_unstable();
        targets.dedup();
        if targets.is_empty() {
            return Ok((Vec::new(), PointScanStats::default()));
        }

        let first_ts = targets[0];
        let last_ts = targets[targets.len() - 1];
        // The point list is a set of discrete timestamps, so the smallest window
        // covering all of them is [first_ts, last_ts + 1).
        let end = last_ts.saturating_add(1);
        if first_ts >= end {
            return Ok((Vec::new(), PointScanStats::default()));
        }

        let plan = self.query_tier_plan(first_ts, end);
        // Point lookups never peek at chunk tails: enumeration must not fetch a
        // payload, otherwise the same chunk could be decompressed twice (INV-2).
        let snapshot = self.collect_candidates_without_tail_peek(series_id, first_ts, end, plan)?;
        if snapshot.candidates.is_empty() {
            return Ok((Vec::new(), PointScanStats::default()));
        }

        // Newest source first (active > sealed > persisted), breaking ties with
        // the newer chunk sequence. Only a candidate whose header min/max range
        // contains the timestamp is considered.
        let mut order = (0..snapshot.candidates.len()).collect::<Vec<_>>();
        order.sort_by(|&left, &right| {
            let left = &snapshot.candidates[left];
            let right = &snapshot.candidates[right];
            right
                .recency
                .cmp(&left.recency)
                .then_with(|| right.sequence.cmp(&left.sequence))
                .then_with(|| right.max_ts.cmp(&left.max_ts))
        });

        let mut stats = PointScanStats::default();
        // Candidate index -> points found for every requested timestamp inside
        // that candidate's range. Populated on first use, so each group is
        // fetched and decoded at most once.
        let mut decoded_groups: HashMap<usize, Vec<(i64, Value)>> = HashMap::new();
        // Timestamp -> winning value. Iterating timestamps ascending and
        // candidates newest-first means the first hit is the newest version.
        let mut resolved: BTreeMap<i64, Value> = BTreeMap::new();

        for &timestamp in &targets {
            for &candidate_index in &order {
                let candidate = &snapshot.candidates[candidate_index];
                if timestamp < candidate.min_ts || timestamp > candidate.max_ts {
                    continue;
                }

                if let std::collections::hash_map::Entry::Vacant(entry) =
                    decoded_groups.entry(candidate_index)
                {
                    let decoded = self.decode_candidate_points(
                        &snapshot,
                        candidate_index,
                        &targets,
                        &mut stats,
                    )?;
                    entry.insert(decoded);
                }

                let group = &decoded_groups[&candidate_index];
                if let Ok(position) = group.binary_search_by_key(&timestamp, |(ts, _)| *ts) {
                    resolved.insert(timestamp, group[position].1.clone());
                    break;
                }
            }
        }

        let retention_cutoff = self.active_retention_cutoff();
        let mut out = Vec::with_capacity(resolved.len());
        self.tombstone_read_context()
            .with_series_tombstone_ranges(series_id, |tombstone_ranges| {
                for (timestamp, value) in resolved {
                    if retention_cutoff.is_some_and(|cutoff| timestamp < cutoff) {
                        continue;
                    }
                    if tombstone_ranges
                        .is_some_and(|ranges| tombstone::timestamp_is_tombstoned(timestamp, ranges))
                    {
                        continue;
                    }
                    out.push(DataPoint::new(timestamp, value));
                }
            });

        Ok((out, stats))
    }

    /// Fetches and decodes one candidate group exactly once, returning every
    /// requested timestamp that the group actually contains.
    fn decode_candidate_points(
        &self,
        snapshot: &CandidateSnapshot<'_>,
        candidate_index: usize,
        targets: &[i64],
        stats: &mut PointScanStats,
    ) -> Result<Vec<(i64, Value)>> {
        let candidate = &snapshot.candidates[candidate_index];
        stats.chunk_groups_touched = stats.chunk_groups_touched.saturating_add(1);

        let wanted = targets_in_range(targets, candidate.min_ts, candidate.max_ts);
        if wanted.is_empty() {
            return Ok(Vec::new());
        }

        match &candidate.source {
            CandidateSource::Active(points) => Ok(points_from_in_memory_snapshot(
                points.iter_points().map(|point| (point.ts, &point.value)),
                &wanted,
            )),
            CandidateSource::Sealed(chunk) => {
                // Mirror the reverse scan's preference: a finalized sealed chunk
                // always carries its encoded payload, so use it first and fall
                // back to raw in-memory points only for payload-less chunks.
                if chunk.encoded_payload.is_empty() {
                    if chunk.points.is_empty() {
                        return Ok(Vec::new());
                    }
                    return Ok(points_from_in_memory_snapshot(
                        chunk.points.iter().map(|point| (point.ts, &point.value)),
                        &wanted,
                    ));
                }

                let descriptor = EncodedChunkDescriptor {
                    lane: chunk.header.lane,
                    ts_codec: chunk.header.ts_codec,
                    value_codec: chunk.header.value_codec,
                    point_count: chunk.header.point_count as usize,
                };
                let search_index = Encoder::build_timestamp_search_index_from_payload(
                    descriptor.ts_codec,
                    descriptor.point_count,
                    &chunk.encoded_payload,
                )?;
                decode_matched_points(
                    descriptor,
                    &chunk.encoded_payload,
                    index_payload_matches(
                        descriptor,
                        &chunk.encoded_payload,
                        &search_index,
                        &wanted,
                    )?,
                )
            }
            CandidateSource::Persisted(chunk_ref) => {
                let payload =
                    persisted_chunk_payload(&snapshot.persisted_index.segment_maps, chunk_ref)?;
                let descriptor = EncodedChunkDescriptor {
                    lane: chunk_ref.lane,
                    ts_codec: chunk_ref.ts_codec,
                    value_codec: chunk_ref.value_codec,
                    point_count: chunk_ref.point_count as usize,
                };

                match payload {
                    Cow::Borrowed(payload) => {
                        // Uncompressed segment: jump to the anchor blocks that can
                        // contain the requested timestamps.
                        let search_index = self.build_persisted_chunk_timestamp_search_index(
                            &snapshot.persisted_index,
                            chunk_ref,
                            payload,
                        )?;
                        let matched =
                            index_payload_matches(descriptor, payload, search_index, &wanted)?;
                        decode_matched_points(descriptor, payload, matched)
                    }
                    Cow::Owned(payload) => {
                        // zstd chunk: the single decompression already materialized
                        // the timestamp stream, so binary search it directly.
                        let matched = locate_timestamps(
                            Encoder::decode_timestamps_from_payload(
                                descriptor.ts_codec,
                                descriptor.point_count,
                                &payload,
                            )?,
                            &wanted,
                        );
                        decode_matched_points(descriptor, &payload, matched)
                    }
                }
            }
        }
    }
}

/// Returns the requested timestamps that fall inside `[min_ts, max_ts]`.
fn targets_in_range(targets: &[i64], min_ts: i64, max_ts: i64) -> Vec<i64> {
    if targets.is_empty() || min_ts > max_ts {
        return Vec::new();
    }
    let start = targets.partition_point(|timestamp| *timestamp < min_ts);
    let end = targets.partition_point(|timestamp| *timestamp <= max_ts);
    if start >= end {
        return Vec::new();
    }
    targets[start..end].to_vec()
}

/// Selects matching points from an ascending in-memory point stream. Later
/// duplicates win, matching the "newest version" rule for repeated timestamps.
fn points_from_in_memory_snapshot<'a>(
    points: impl Iterator<Item = (i64, &'a Value)>,
    wanted: &[i64],
) -> Vec<(i64, Value)> {
    let wanted = wanted.iter().copied().collect::<HashSet<_>>();
    let mut found = BTreeMap::new();
    for (timestamp, value) in points {
        if wanted.contains(&timestamp) {
            found.insert(timestamp, value.clone());
        }
    }
    found.into_iter().collect()
}

/// Locates the last index of each requested timestamp in an ascending timestamp
/// stream, returning `(index, timestamp)` pairs for the hits.
fn locate_timestamps(timestamps: Vec<i64>, wanted: &[i64]) -> Vec<(usize, i64)> {
    let mut matched = Vec::with_capacity(wanted.len());
    for &timestamp in wanted {
        let end = timestamps.partition_point(|candidate| *candidate <= timestamp);
        if end == 0 || timestamps[end - 1] != timestamp {
            continue;
        }
        matched.push((end - 1, timestamp));
    }
    matched
}

/// Locates the requested timestamps by decoding only the anchor blocks that can
/// contain them, then decodes values for the matched point indexes only.
fn index_payload_matches(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    search_index: &TimestampSearchIndex,
    wanted: &[i64],
) -> Result<Vec<(usize, i64)>> {
    if descriptor.point_count == 0 || wanted.is_empty() {
        return Ok(Vec::new());
    }

    let ts_payload = Encoder::timestamp_payload_from_chunk_payload(payload)?;
    let mut matched = Vec::with_capacity(wanted.len());
    let mut block_cache: HashMap<usize, Vec<i64>> = HashMap::new();

    for &timestamp in wanted {
        let Some(block_idx) =
            search_index.candidate_block_for_ceiling(descriptor.point_count, timestamp)
        else {
            continue;
        };
        if let std::collections::hash_map::Entry::Vacant(entry) = block_cache.entry(block_idx) {
            entry.insert(search_index.decode_block(
                descriptor.point_count,
                ts_payload,
                block_idx,
            )?);
        }
        let block = &block_cache[&block_idx];

        let end = block.partition_point(|candidate| *candidate <= timestamp);
        if end == 0 || block[end - 1] != timestamp {
            continue;
        }
        let point_index = block_idx
            .saturating_mul(TIMESTAMP_SEARCH_BLOCK_POINTS)
            .saturating_add(end - 1);
        matched.push((point_index, timestamp));
    }

    Ok(matched)
}

fn decode_matched_points(
    descriptor: EncodedChunkDescriptor,
    payload: &[u8],
    mut matched: Vec<(usize, i64)>,
) -> Result<Vec<(i64, Value)>> {
    if matched.is_empty() || descriptor.point_count == 0 {
        return Ok(Vec::new());
    }

    matched.sort_unstable_by_key(|(point_index, _)| *point_index);
    matched.dedup_by_key(|(point_index, _)| *point_index);

    let indexes = matched
        .iter()
        .map(|(point_index, _)| *point_index)
        .collect::<Vec<_>>();
    let values = Encoder::decode_chunk_values_at_indexes(
        descriptor.lane,
        descriptor.value_codec,
        descriptor.point_count,
        payload,
        &indexes,
    )?;

    Ok(matched
        .into_iter()
        .map(|(_, timestamp)| timestamp)
        .zip(values)
        .collect())
}
