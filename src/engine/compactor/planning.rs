use super::*;
use crate::engine::segment::SegmentManifest;
use crate::engine::storage_engine::tiering::PersistedSegmentTier;
use tracing::warn;

/// One compaction source as planning sees it: the inventory snapshot's view of
/// a segment (root + manifest), never its payload.
///
/// Planning — counting, overlap detection, window selection — only needs the
/// manifest, so it runs entirely on the snapshot. Only the members of the
/// selected window are loaded from disk.
#[derive(Debug, Clone)]
pub(in crate::engine) struct PlanningCandidate {
    pub root: PathBuf,
    pub manifest: SegmentManifest,
}

impl Compactor {
    pub(in crate::engine) fn compact_once_with_changes(&self) -> Result<CompactionOutcome> {
        // Skip before any disk access: the background compaction thread wakes up
        // before the inventory provider is injected, and must not fall back to an
        // O(N) scan in that window.
        let Some(inventory) = self.planning_inventory()? else {
            return Ok(CompactionOutcome::default());
        };

        finalize_pending_compaction_replacements(&self.data_path)?;
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L0,
            CompactionLevel::L1,
            self.l0_trigger,
            &inventory,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        if let Some(outcome) = self.try_compact_level(
            CompactionLevel::L1,
            CompactionLevel::L2,
            self.l1_trigger,
            &inventory,
            &tombstones,
        )? {
            return Ok(outcome);
        }

        Ok(CompactionOutcome::default())
    }

    /// Planning candidates for `level`, taken from the inventory snapshot.
    ///
    /// Only this lane's hot segments take part: segments tiered away to
    /// warm/cold are no longer compacted from the lane root. Ordering matches
    /// the previous disk-driven planning (ascending `segment_id`).
    fn planning_candidates(
        &self,
        inventory: &SegmentInventory,
        level: u8,
    ) -> Vec<PlanningCandidate> {
        let mut candidates = inventory
            .entries()
            .iter()
            .filter(|entry| {
                entry.root.starts_with(&self.data_path)
                    && entry.manifest.level == level
                    && entry.tier == PersistedSegmentTier::Hot
            })
            .map(|entry| PlanningCandidate {
                root: entry.root.clone(),
                manifest: entry.manifest.clone(),
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| candidate.manifest.segment_id);
        candidates
    }

    fn try_compact_level(
        &self,
        source: CompactionLevel,
        target: CompactionLevel,
        count_trigger: usize,
        inventory: &SegmentInventory,
        tombstones: &TombstoneMap,
    ) -> Result<Option<CompactionOutcome>> {
        let source_level = level_to_u8(source);
        let target_level = level_to_u8(target);

        let candidates = self.planning_candidates(inventory, source_level);
        if candidates.len() < 2 {
            return Ok(None);
        }

        let candidate_refs = candidates.iter().collect::<Vec<_>>();
        let should_compact = candidates.len() >= count_trigger || has_time_overlap(&candidate_refs);
        if !should_compact {
            return Ok(None);
        }

        let window =
            select_compaction_window(&candidates, count_trigger, DEFAULT_SOURCE_WINDOW_SEGMENTS);
        if window.len() < 2 {
            return Ok(None);
        }

        let (segments, retired_roots) = load_window_members(&window)?;
        if !retired_roots.is_empty() {
            warn!(
                roots = ?retired_roots,
                "Compaction window members vanished before load; dropping them from this window"
            );
        }
        if segments.len() < 2 {
            return Ok(None);
        }

        let segment_refs = segments.iter().collect::<Vec<_>>();
        let mut outcome = self.compact_segments(target_level, &segment_refs, tombstones)?;
        outcome.stats.compacted = true;
        outcome.stats.windows_merged = 1;
        outcome.stats.source_level = Some(source_level);
        outcome.stats.target_level = Some(target_level);
        Ok(Some(outcome))
    }
}

/// Loads the selected window from disk, root by root.
///
/// Roots whose segment vanished mid-removal are returned separately so the
/// caller can retire them from its view; the survivors are merged.
fn load_window_members(
    window: &[&PlanningCandidate],
) -> Result<(Vec<LoadedSegment>, Vec<PathBuf>)> {
    let mut segments = Vec::with_capacity(window.len());
    let mut retired_roots = Vec::new();

    for candidate in window {
        match load_segment_root_for_compaction(&candidate.root)? {
            Some(segment) => segments.push(segment),
            None => retired_roots.push(candidate.root.clone()),
        }
    }

    Ok((segments, retired_roots))
}

/// Counts the segments of `level` under `base`.
///
/// Kept for the disk fallback path (standalone compactor / reconciliation
/// scan), which only needs a count; planning itself now counts the inventory
/// snapshot's candidates and never rescans the level.
#[allow(dead_code)]
pub(super) fn count_level_segments_for_compaction(base: &Path, level: u8) -> Result<usize> {
    let expected_level_dir = format!("L{level}");
    let mut count = 0usize;

    for root in list_segment_dirs(base)? {
        if root
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            != Some(expected_level_dir.as_str())
        {
            continue;
        }

        match read_segment_manifest(&root) {
            Ok(manifest) => {
                if manifest.level != level {
                    return Err(segment_validation_error(
                        &root,
                        SegmentValidationContext::Compaction,
                        &format!(
                            "segment directory level mismatch: stored under L{level}, manifest says L{}",
                            manifest.level
                        ),
                    ));
                }
                count = count.saturating_add(1);
            }
            Err(err) if is_vanished_source_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory is mid-removal during compaction planning; skipping"
                );
                continue;
            }
            Err(TsinkError::DataCorruption(details)) => {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &details,
                ));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(count)
}

pub(super) fn has_time_overlap(candidates: &[&PlanningCandidate]) -> bool {
    let mut ranges = candidates
        .iter()
        .filter_map(|candidate| {
            Some((
                candidate.manifest.min_ts?,
                candidate.manifest.max_ts?,
                candidate.manifest.segment_id,
            ))
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return false;
    }

    ranges.sort_by_key(|(min_ts, _, segment_id)| (*min_ts, *segment_id));

    let mut current_max = ranges[0].1;
    for (min_ts, max_ts, _) in ranges.into_iter().skip(1) {
        if min_ts <= current_max {
            return true;
        }
        current_max = current_max.max(max_ts);
    }

    false
}

pub(super) fn select_compaction_window(
    candidates: &[PlanningCandidate],
    count_trigger: usize,
    max_segments: usize,
) -> Vec<&PlanningCandidate> {
    let max_segments = max_segments.max(2);
    if let Some(indexes) = overlapping_window_indexes(candidates, max_segments) {
        return indexes
            .into_iter()
            .filter_map(|index| candidates.get(index))
            .collect();
    }

    let window_len = count_trigger.max(2).min(max_segments).min(candidates.len());
    candidates.iter().take(window_len).collect()
}

#[derive(Debug, Clone, Copy)]
struct SegmentTimeRange {
    index: usize,
    segment_id: u64,
    min_ts: i64,
    max_ts: i64,
}

fn overlapping_window_indexes(
    candidates: &[PlanningCandidate],
    max_segments: usize,
) -> Option<Vec<usize>> {
    let mut ranges = candidates
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            Some(SegmentTimeRange {
                index,
                segment_id: candidate.manifest.segment_id,
                min_ts: candidate.manifest.min_ts?,
                max_ts: candidate.manifest.max_ts?,
            })
        })
        .collect::<Vec<_>>();

    if ranges.len() < 2 {
        return None;
    }

    ranges.sort_by_key(|range| (range.min_ts, range.segment_id));

    let mut cluster_start = 0usize;
    let mut cluster_max = ranges[0].max_ts;

    for idx in 1..ranges.len() {
        if ranges[idx].min_ts <= cluster_max {
            cluster_max = cluster_max.max(ranges[idx].max_ts);
            continue;
        }

        if idx.saturating_sub(cluster_start) >= 2 {
            return Some(select_cluster_indexes(
                &ranges[cluster_start..idx],
                max_segments,
            ));
        }

        cluster_start = idx;
        cluster_max = ranges[idx].max_ts;
    }

    if ranges.len().saturating_sub(cluster_start) >= 2 {
        return Some(select_cluster_indexes(
            &ranges[cluster_start..],
            max_segments,
        ));
    }

    None
}

fn select_cluster_indexes(cluster: &[SegmentTimeRange], max_segments: usize) -> Vec<usize> {
    let mut indexes = cluster.iter().map(|range| range.index).collect::<Vec<_>>();
    indexes.sort_unstable();
    indexes.truncate(max_segments.max(2));
    indexes
}

pub(super) fn level_to_u8(level: CompactionLevel) -> u8 {
    match level {
        CompactionLevel::L0 => 0,
        CompactionLevel::L1 => 1,
        CompactionLevel::L2 => 2,
    }
}
