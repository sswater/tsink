use std::time::Instant;

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

/// What one drain-loop step decided, i.e. whether the pass should keep going.
enum WindowStep {
    /// No source level is at or beyond its trigger: the pass has drained.
    Converged,
    /// One window's effects on the persisted catalog are complete on disk: a
    /// merged window's replacement is published and its sources are gone, a
    /// window of vanished members is retired from the working copy, or both.
    ///
    /// The diff is returned instead of being accumulated for the end of the
    /// round, so the caller can merge it immediately: a later window's error
    /// must not be able to drop an already-published window's diff.
    Published(PendingPersistedSegmentDiff),
    /// The window produced no output segments because every point was
    /// tombstoned, so its sources are still on disk and must not be absorbed.
    /// The caller skips those candidates for the rest of the pass (they keep
    /// counting toward `source_above_trigger`) so the drain loop moves on to
    /// other windows; the next pass retries them.
    ///
    /// The compaction path runs without a retention cutoff, so only the
    /// tombstone elimination can reach this state; the cutoff half belongs to
    /// `stage_segment_rewrite_with_retention`.
    NoOutput,
}

/// Mutable per-level view of the inventory snapshot for a single pass.
///
/// Every merged window is absorbed here: its sources are removed and its
/// outputs are inserted at the target level. That is what lets the same pass
/// see its own L1 outputs when the L1→L2 stage runs, and what keeps a vanished
/// member from being selected again once it has been retired.
///
/// Windows that produced no output are moved into `skipped_*`: they are out of
/// the selection for the rest of this pass — each unmergeable window is
/// attempted at most once — but they still count as sources above the trigger
/// line, so the next pass runs soon and retries them.
struct WorkingInventory {
    l0: Vec<PlanningCandidate>,
    l1: Vec<PlanningCandidate>,
    skipped_l0: Vec<PathBuf>,
    skipped_l1: Vec<PathBuf>,
}

impl WorkingInventory {
    fn from_inventory(compactor: &Compactor, inventory: &SegmentInventory) -> Self {
        Self {
            l0: compactor.planning_candidates(inventory, level_to_u8(CompactionLevel::L0)),
            l1: compactor.planning_candidates(inventory, level_to_u8(CompactionLevel::L1)),
            skipped_l0: Vec::new(),
            skipped_l1: Vec::new(),
        }
    }

    fn candidates(&self, level: CompactionLevel) -> &[PlanningCandidate] {
        match level {
            CompactionLevel::L0 => &self.l0,
            CompactionLevel::L1 => &self.l1,
            // L2 is never a source level: there is no L3 to compact into.
            CompactionLevel::L2 => &[],
        }
    }

    fn count(&self, level: CompactionLevel) -> usize {
        self.candidates(level).len()
    }

    fn skipped_count(&self, level: CompactionLevel) -> usize {
        match level {
            CompactionLevel::L0 => self.skipped_l0.len(),
            CompactionLevel::L1 => self.skipped_l1.len(),
            CompactionLevel::L2 => 0,
        }
    }

    /// Sources still at or above the trigger, counting the windows this pass
    /// could not merge.
    fn above_trigger(&self, level: CompactionLevel, trigger: usize) -> bool {
        self.count(level) + self.skipped_count(level) >= trigger
    }

    fn remove_roots(&mut self, level: CompactionLevel, roots: &[PathBuf]) {
        let candidates = match level {
            CompactionLevel::L0 => &mut self.l0,
            CompactionLevel::L1 => &mut self.l1,
            CompactionLevel::L2 => return,
        };
        candidates.retain(|candidate| !roots.contains(&candidate.root));
    }

    /// Removes a window that produced no output from this pass's selection and
    /// records it as skipped, so it keeps counting toward the trigger line.
    fn skip_roots(&mut self, level: CompactionLevel, roots: &[PathBuf]) {
        self.remove_roots(level, roots);
        let skipped = match level {
            CompactionLevel::L0 => &mut self.skipped_l0,
            CompactionLevel::L1 => &mut self.skipped_l1,
            CompactionLevel::L2 => return,
        };
        skipped.extend(roots.iter().cloned());
    }

    /// Inserts a merged window's outputs at `target`.
    ///
    /// L2 outputs are not tracked because nothing in this pass plans from L2.
    /// Each L1 output costs one manifest read — bounded by the window size,
    /// never by the inventory size.
    fn absorb_outputs(&mut self, target: CompactionLevel, output_roots: &[PathBuf]) -> Result<()> {
        if !matches!(target, CompactionLevel::L1) {
            return Ok(());
        }

        for root in output_roots {
            self.l1.push(PlanningCandidate {
                root: root.clone(),
                manifest: read_segment_manifest(root)?,
            });
        }
        self.l1
            .sort_by_key(|candidate| candidate.manifest.segment_id);
        Ok(())
    }
}

impl Compactor {
    /// Drains the source levels of one inventory snapshot.
    ///
    /// Loop contract:
    /// `while count(source) >= trigger && (budget remains || no window merged yet)`.
    ///
    /// The wall-clock budget only gates *opening* a window: a window that has
    /// started always runs to completion, and the first window of a round
    /// always opens, so a budget shorter than one window cannot stall the
    /// drain. L0 is drained before L1 in every iteration, which is what makes
    /// this pass's own L1 outputs eligible for L1→L2 in the same pass.
    ///
    /// Every window step holds the storage compaction gate for exactly that
    /// step (load + merge + replacement marker + source removal) and releases
    /// it before the next one, so flush, retention, snapshot, and catalog
    /// refresh can interleave between windows instead of waiting for a full
    /// round.
    ///
    /// `on_window` is the partial-failure contract: it runs once per window
    /// whose roots changed on disk, immediately after that window is published
    /// and with the gate already released. The caller merges the diff into its
    /// pending catalog diff there, so a window that fails later in the round
    /// (`?`) cannot drop the diff of the windows that already succeeded. A
    /// window that only retires vanished members also reports a diff (removed
    /// roots, no output): those roots are gone from disk but must still leave
    /// the catalog. A round that publishes nothing calls back zero times.
    pub(in crate::engine) fn compact_round_with_changes(
        &self,
        on_window: &mut dyn FnMut(PendingPersistedSegmentDiff),
    ) -> Result<CompactionRunStats> {
        // Skip before any disk access: the background compaction thread wakes up
        // before the inventory provider is injected, and must not fall back to an
        // O(N) scan in that window.
        let Some(inventory) = self.planning_inventory()? else {
            return Ok(CompactionRunStats::default());
        };

        finalize_pending_compaction_replacements(&self.data_path)?;
        let tombstones = load_tombstones(&self.data_path.join(TOMBSTONES_FILE_NAME))?;

        let mut working = WorkingInventory::from_inventory(self, &inventory);
        let deadline = Instant::now() + self.pass_wall_budget;
        let mut stats = CompactionRunStats::default();

        loop {
            if stats.windows_merged > 0 && Instant::now() >= deadline {
                break;
            }

            // `parking_lot::Mutex` is not reentrant: no caller may hold this
            // gate around the pass (see the background worker and shutdown
            // call sites). The guard is dropped before the step's diff is
            // handed over, so `on_window` never runs under the gate either.
            let step = {
                let _guard = self.gate.as_ref().map(|gate| gate.lock());
                self.merge_one_window(&mut working, &tombstones, &mut stats)?
            };

            match step {
                WindowStep::Published(diff) => on_window(diff),
                WindowStep::NoOutput => {}
                WindowStep::Converged => break,
            }
        }

        // A level that is still at or above its trigger after the loop (budget
        // exhausted, or a window that could not be merged) needs the next pass
        // to run immediately rather than park a full interval. Windows skipped
        // for producing no output still count: their sources are still on disk.
        stats.source_above_trigger = working.above_trigger(CompactionLevel::L0, self.l0_trigger)
            || working.above_trigger(CompactionLevel::L1, self.l1_trigger);

        Ok(stats)
    }

    /// Picks the next source level to drain: L0 first, then L1.
    fn select_source_level(
        &self,
        working: &WorkingInventory,
    ) -> Option<(CompactionLevel, CompactionLevel, usize)> {
        [
            (CompactionLevel::L0, CompactionLevel::L1, self.l0_trigger),
            (CompactionLevel::L1, CompactionLevel::L2, self.l1_trigger),
        ]
        .into_iter()
        .find(|(source, _, trigger)| should_compact_level(working.candidates(*source), *trigger))
    }

    /// Merges at most one window of the selected source level.
    ///
    /// Every step either publishes a window (strictly shrinking the selectable
    /// source level), retires vanished roots (also strictly shrinking it), or
    /// skips a zero-output window out of this pass's selection (again strictly
    /// shrinking it), so the loop cannot revisit the same state.
    fn merge_one_window(
        &self,
        working: &mut WorkingInventory,
        tombstones: &TombstoneMap,
        stats: &mut CompactionRunStats,
    ) -> Result<WindowStep> {
        let Some((source, target, trigger)) = self.select_source_level(working) else {
            return Ok(WindowStep::Converged);
        };

        // Cloned out of the working copy: absorbing the merged window mutates
        // that same copy, so the selection cannot stay borrowed. The clone is
        // bounded by DEFAULT_SOURCE_WINDOW_SEGMENTS, and
        // `select_compaction_window` always yields at least two members (an
        // overlap cluster of ≥ 2, or `count_trigger.max(2)` candidates), so no
        // "window too small to merge" guard is needed here.
        let window = select_compaction_window(
            working.candidates(source),
            trigger,
            DEFAULT_SOURCE_WINDOW_SEGMENTS,
        )
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

        let (segments, mut retired_roots) = load_window_members(&window)?;
        if !retired_roots.is_empty() {
            warn!(
                roots = ?retired_roots,
                "Compaction window members vanished before load; retiring them from the planning inventory"
            );
            working.remove_roots(source, &retired_roots);
        }

        if segments.len() < 2 {
            // Only ghosts were left in this window. Their eviction already
            // shrank the source level, so the next iteration re-selects from
            // the surviving candidates instead of merging this window.
            if retired_roots.is_empty() {
                // Selection always yields at least two members and every member
                // either loads or is retired, so reaching here means the
                // invariant broke. Fail loudly instead of spinning the drain
                // loop over the same unmergeable window forever.
                return Err(TsinkError::DataCorruption(format!(
                    "compaction window of {} candidates yielded {} loadable segments \
                     and no retired roots",
                    window.len(),
                    segments.len()
                )));
            }
            // Already gone from disk, but they still have to leave the
            // inventory and the catalog, otherwise every pass re-selects them.
            return Ok(WindowStep::Published(retired_roots_diff(retired_roots)));
        }

        let segment_refs = segments.iter().collect::<Vec<_>>();
        let window_outcome =
            self.compact_segments(level_to_u8(target), &segment_refs, tombstones)?;
        if !window_outcome.stats.compacted {
            // Zero output: every point of the window was tombstoned. The
            // sources stay on disk (there is nothing to publish in their
            // place), so they cannot be absorbed — but the pass must not
            // re-select the same window either. Skip them for the rest of this
            // pass so the remaining windows drain; they keep counting toward
            // `source_above_trigger`, and the next pass retries them.
            working.skip_roots(source, &window_outcome.source_roots);
            if retired_roots.is_empty() {
                return Ok(WindowStep::NoOutput);
            }
            // The window produced no replacement, but its vanished members are
            // still gone: report them so the catalog drops them now.
            return Ok(WindowStep::Published(retired_roots_diff(retired_roots)));
        }

        working.remove_roots(source, &window_outcome.source_roots);
        working.absorb_outputs(target, &window_outcome.output_roots)?;

        stats.compacted = true;
        stats.source_level.get_or_insert(level_to_u8(source));
        stats.target_level = Some(level_to_u8(target));
        stats.source_segments += window_outcome.stats.source_segments;
        stats.output_segments += window_outcome.stats.output_segments;
        stats.source_chunks += window_outcome.stats.source_chunks;
        stats.output_chunks += window_outcome.stats.output_chunks;
        stats.source_points += window_outcome.stats.source_points;
        stats.output_points += window_outcome.stats.output_points;
        stats.windows_merged = stats.windows_merged.saturating_add(1);

        // `record_changes` adds before it removes, so a root this window both
        // published and consumed (the same L1 output in a later window) ends up
        // on the removed side. That ordering is what lets L0→L1→L2 ride one
        // accumulating diff: the intermediate L1 roots are added by their L0
        // window and removed by the L1→L2 window.
        let mut diff = PendingPersistedSegmentDiff::default();
        let mut removed_roots = window_outcome.source_roots;
        removed_roots.append(&mut retired_roots);
        diff.record_changes(window_outcome.output_roots, removed_roots);
        Ok(WindowStep::Published(diff))
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
}

/// A level is due for compaction when it has something to merge and is either
/// at/above its count trigger or holds time-overlapping segments.
fn should_compact_level(candidates: &[PlanningCandidate], count_trigger: usize) -> bool {
    candidates.len() >= 2 && (candidates.len() >= count_trigger || has_time_overlap(candidates))
}

/// Diff for a window whose only effect was retiring vanished members.
///
/// Nothing was published, but those roots are already gone from disk and must
/// still leave the inventory and the catalog, otherwise every pass re-selects
/// the same ghosts.
fn retired_roots_diff(retired_roots: Vec<PathBuf>) -> PendingPersistedSegmentDiff {
    let mut diff = PendingPersistedSegmentDiff::default();
    diff.record_changes(std::iter::empty::<PathBuf>(), retired_roots);
    diff
}

/// Loads the selected window from disk, root by root.
///
/// Roots whose segment vanished mid-removal are returned separately so the
/// caller can retire them from its working inventory copy and report them as
/// retired roots. The survivors are merged.
fn load_window_members(window: &[PlanningCandidate]) -> Result<(Vec<LoadedSegment>, Vec<PathBuf>)> {
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

pub(super) fn has_time_overlap(candidates: &[PlanningCandidate]) -> bool {
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

/// Selects the next window of `candidates` to merge.
///
/// Callers must pass at least two candidates, and the returned window then
/// always holds at least two members: either an overlap cluster or the first
/// `max(count_trigger, 2)` candidates. A smaller window can never be merged,
/// so callers do not need a "window too small" guard.
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
