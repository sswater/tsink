//! Root-keyed cache of the series every persisted segment contributes.
//!
//! The persisted registry catalog carries a fingerprint over the union of all
//! persisted segment series metadata. Re-deriving that union by reading
//! `series.bin` for every segment on every catalog write was the dominant cost
//! of catalog production; this cache keeps one [`SeriesRun`] per segment root so
//! the union is a pure in-memory merge whose cost is O(distinct series).
//!
//! Authority: runs are built ONLY from segment series metadata — the same bytes
//! written to `series.bin`. They are never re-derived from the series registry,
//! which knows strictly more (unflushed in-memory series). A run mirrors an
//! immutable segment root, so it never needs invalidation: segment roots are
//! unique per (level, segment id) and are never rewritten in place.
//!
//! Footprint: O(Σ per-segment series), i.e. it can exceed the distinct series
//! count while the same series lives in several levels. Callers that notice the
//! skew (see [`super::registry_catalog`]) bound it by falling back to the disk
//! path for that write instead of trusting an oversized cache.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::warn;

use super::registry_catalog::{fingerprint_series_fields, PersistedRegistrySeriesFingerprint};
use super::{ChunkStorage, Result, TsinkError};
use crate::engine::segment::{load_segment_series_metadata, IndexedSegment, PersistedSeries};
use crate::engine::series::SeriesId;
use crate::Label;

/// One series identity contributed by a persisted segment: its id plus the
/// `Arc`-shared `(metric, labels)` key.
pub(in crate::engine::storage_engine) type SeriesContribution =
    (SeriesId, Arc<(String, Vec<Label>)>);

/// One persisted segment's series keys, sorted by [`SeriesId`].
///
/// The key body is `Arc`-shared: merging the runs of a catalog write only clones
/// the `Arc`, never the metric/label payload.
#[derive(Debug, Default, Clone)]
pub(in crate::engine::storage_engine) struct SeriesRun(
    pub(in crate::engine::storage_engine) Vec<SeriesContribution>,
);

impl SeriesRun {
    /// Builds a run from segment series metadata (`series.bin`). `value_family`
    /// is deliberately dropped: the catalog fingerprint only covers identity.
    pub(in crate::engine::storage_engine) fn from_series(series: &[PersistedSeries]) -> Self {
        let mut entries = series
            .iter()
            .map(|series| {
                (
                    series.series_id,
                    Arc::new((series.metric.clone(), series.labels.clone())),
                )
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(series_id, _)| *series_id);
        Self(entries)
    }

    pub(in crate::engine::storage_engine) fn len(&self) -> usize {
        self.0.len()
    }
}

/// Root-keyed contribution cache. Cheap to share by reference; all mutation
/// goes through one short-lived mutex.
#[derive(Default)]
pub(in crate::engine::storage_engine) struct RegistryContributions {
    inner: Mutex<ContributionsInner>,
}

#[derive(Default)]
struct ContributionsInner {
    runs: HashMap<PathBuf, Arc<SeriesRun>>,
    /// Σ per-segment series entries, kept in step with `runs` so replacement and
    /// eviction stay exact without walking the map.
    entry_count: usize,
}

impl RegistryContributions {
    pub(in crate::engine::storage_engine) fn new() -> Self {
        Self::default()
    }

    /// Installs runs discovered outside the publish path (bootstrap priming).
    pub(in crate::engine::storage_engine) fn prime(&self, entries: &[(PathBuf, SeriesRun)]) {
        for (root, run) in entries {
            self.replace_root(root, run.clone());
        }
    }

    /// Records (or replaces) one segment root's contribution.
    pub(in crate::engine::storage_engine) fn replace_root(&self, root: &Path, run: SeriesRun) {
        self.replace_root_shared(root, Arc::new(run));
    }

    pub(in crate::engine::storage_engine) fn replace_root_shared(
        &self,
        root: &Path,
        run: Arc<SeriesRun>,
    ) {
        let mut inner = self.inner.lock();
        let run_len = run.len();
        let previous = inner.runs.insert(root.to_path_buf(), run);
        inner.entry_count = inner
            .entry_count
            .saturating_add(run_len)
            .saturating_sub(previous.map_or(0, |previous| previous.len()));
    }

    /// Evicts a retired root so a long-lived engine's footprint tracks the
    /// visible segment set instead of every segment it ever saw.
    pub(in crate::engine::storage_engine) fn remove_root(&self, root: &Path) {
        let mut inner = self.inner.lock();
        if let Some(previous) = inner.runs.remove(root) {
            inner.entry_count = inner.entry_count.saturating_sub(previous.len());
        }
    }

    /// Drops every run. Used when the persisted index is rebuilt wholesale, so
    /// the cache can never describe roots the index no longer contains.
    pub(in crate::engine::storage_engine) fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.runs.clear();
        inner.entry_count = 0;
    }

    pub(in crate::engine::storage_engine) fn run_for_root(
        &self,
        root: &Path,
    ) -> Option<Arc<SeriesRun>> {
        self.inner.lock().runs.get(root).cloned()
    }

    /// Σ per-segment series entries currently held.
    pub(in crate::engine::storage_engine) fn entry_count(&self) -> usize {
        self.inner.lock().entry_count
    }
}

/// K-way merges `SeriesId`-sorted runs into the catalog series fingerprint.
///
/// The merge keeps the ordering and hashing of the disk path exactly (`Xxh64`
/// over the union of distinct series in `SeriesId` order) and preserves both
/// corruption checks: one id bound to two different keys, and one key bound to
/// two different ids.
pub(in crate::engine::storage_engine) fn merge_series_runs(
    runs: &[&SeriesRun],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut cursors = BinaryHeap::<Reverse<(SeriesId, usize, usize)>>::new();
    for (run_index, run) in runs.iter().enumerate() {
        if let Some((series_id, _)) = run.0.first() {
            cursors.push(Reverse((*series_id, run_index, 0)));
        }
    }

    let mut merged: Vec<SeriesContribution> = Vec::new();
    let mut series_id_by_key = HashMap::<Arc<(String, Vec<Label>)>, SeriesId>::new();

    while let Some(Reverse((series_id, run_index, position))) = cursors.pop() {
        let run = runs[run_index];
        let (_, key) = &run.0[position];

        match merged.last() {
            Some((last_id, last_key)) if *last_id == series_id => {
                if last_key.as_ref() != key.as_ref() {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts across persisted segment metadata",
                        series_id
                    )));
                }
            }
            _ => {
                if let Some(bound_id) = series_id_by_key.get(key) {
                    if *bound_id != series_id {
                        return Err(TsinkError::DataCorruption(format!(
                            "series key already bound to id {}, persisted segment metadata tried to bind {}",
                            bound_id, series_id
                        )));
                    }
                } else {
                    series_id_by_key.insert(Arc::clone(key), series_id);
                }
                merged.push((series_id, Arc::clone(key)));
            }
        }

        if let Some((next_id, _)) = run.0.get(position + 1) {
            cursors.push(Reverse((*next_id, run_index, position + 1)));
        }
    }

    Ok(fingerprint_series_fields(merged.iter().map(
        |(series_id, key)| (*series_id, key.0.as_str(), key.1.as_slice()),
    )))
}

impl ChunkStorage {
    /// Fills contribution gaps left by segment indexes that were loaded without
    /// their series payload (startup with a valid registry catalog).
    ///
    /// Runs before the background flush/refresh threads start, so it never
    /// competes with a catalog write, and it never fails the startup: a root
    /// whose `series.bin` cannot be parsed stays unprimed and the next catalog
    /// build reads that root from disk.
    pub(in crate::engine::storage_engine) fn prime_registry_contributions_from_persisted_index(
        &self,
    ) {
        // Only the registry catalog consumes contributions; without a registry
        // snapshot path there is no catalog to build, so do not pay the reads.
        if self.persisted.series_index_path.is_none() {
            return;
        }

        // Take the roots, then drop the index guard: priming does disk I/O and
        // must never run under the `persisted_index` lock.
        let roots = self
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        let mut primed = Vec::new();
        for root in roots {
            if self
                .persisted
                .registry_contributions
                .run_for_root(&root)
                .is_some()
            {
                continue;
            }
            match load_segment_series_metadata(&root) {
                Ok(series) => {
                    primed.push((root, SeriesRun::from_series(&series)));
                }
                Err(err) => warn!(
                    path = %root.display(),
                    error = %err,
                    "Failed to prime registry contributions for persisted segment; \
                     the catalog build will read it on demand"
                ),
            }
        }
        self.persisted.registry_contributions.prime(&primed);
    }

    /// Records contributions for segments that were just loaded with their
    /// series payload (flush verification, publish, compaction diff apply).
    pub(in crate::engine::storage_engine) fn prime_registry_contributions_for_segments(
        &self,
        segments: &[IndexedSegment],
    ) {
        for segment in segments {
            // `load_segment_index` skips the series payload when startup only
            // needs the indexes; a partial run would silently shrink the
            // fingerprint, so leave such roots for the disk fallback.
            if segment.series.len() != segment.manifest.series_count {
                continue;
            }
            self.persisted
                .registry_contributions
                .replace_root(&segment.root, SeriesRun::from_series(&segment.series));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::series::SeriesValueFamily;

    fn series(series_id: SeriesId, metric: &str, labels: &[(&str, &str)]) -> PersistedSeries {
        PersistedSeries {
            series_id,
            metric: metric.to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect(),
            value_family: Some(SeriesValueFamily::F64),
        }
    }

    fn run(series: &[PersistedSeries]) -> SeriesRun {
        SeriesRun::from_series(series)
    }

    /// The merged fingerprint must equal the disk path's fingerprint over the
    /// union of the same series metadata — same order, same hash bytes.
    #[test]
    fn merge_fingerprint_matches_disk_union_fingerprint() {
        let first = vec![
            series(7, "cpu", &[("host", "a")]),
            series(2, "cpu", &[("host", "b")]),
        ];
        let second = vec![
            series(2, "cpu", &[("host", "b")]),
            series(9, "mem", &[("host", "a"), ("zone", "z")]),
        ];

        let contributions = RegistryContributions::new();
        contributions.replace_root(Path::new("/seg/one"), run(&first));
        contributions.replace_root(Path::new("/seg/two"), run(&second));

        let union = [
            series(2, "cpu", &[("host", "b")]),
            series(7, "cpu", &[("host", "a")]),
            series(9, "mem", &[("host", "a"), ("zone", "z")]),
        ];
        let expected = fingerprint_series_fields(union.iter().map(|series| {
            (
                series.series_id,
                series.metric.as_str(),
                series.labels.as_slice(),
            )
        }));

        let first = run(&first);
        let second = run(&second);
        let merged = merge_series_runs(&[&first, &second]).unwrap();
        assert_eq!(merged, expected);
        assert_eq!(expected.series_count, 3);
        assert_eq!(contributions.entry_count(), 4, "footprint is Σ per-segment");
    }

    #[test]
    fn merge_rejects_one_id_bound_to_two_keys() {
        let first = run(&[series(1, "cpu", &[])]);
        let second = run(&[series(1, "mem", &[])]);

        let err = merge_series_runs(&[&first, &second]).unwrap_err();
        assert!(
            matches!(
                &err,
                TsinkError::DataCorruption(message)
                    if message == "series id 1 conflicts across persisted segment metadata"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn merge_rejects_one_key_bound_to_two_ids() {
        let first = run(&[series(1, "cpu", &[("host", "a")])]);
        let second = run(&[series(4, "cpu", &[("host", "a")])]);

        let err = merge_series_runs(&[&first, &second]).unwrap_err();
        assert!(
            matches!(
                &err,
                TsinkError::DataCorruption(message)
                    if message
                        == "series key already bound to id 1, persisted segment metadata tried to bind 4"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn remove_root_evicts_its_contribution() {
        let contributions = RegistryContributions::new();
        contributions.replace_root(Path::new("/seg/one"), run(&[series(1, "cpu", &[])]));
        contributions.replace_root(
            Path::new("/seg/two"),
            run(&[series(1, "cpu", &[]), series(2, "mem", &[])]),
        );
        assert_eq!(contributions.entry_count(), 3);

        contributions.remove_root(Path::new("/seg/two"));
        assert!(contributions.run_for_root(Path::new("/seg/two")).is_none());
        assert_eq!(contributions.entry_count(), 1);
        let remaining = contributions.run_for_root(Path::new("/seg/one")).unwrap();
        assert_eq!(merge_series_runs(&[&remaining]).unwrap().series_count, 1);

        contributions.remove_root(Path::new("/seg/two"));
        assert_eq!(contributions.entry_count(), 1, "eviction is idempotent");
    }

    /// A run must not grow when the same root is republished: the flush path
    /// primes the new root before the catalog write and publishes it again when
    /// the persisted index absorbs it.
    #[test]
    fn replace_root_replaces_instead_of_appending() {
        let contributions = RegistryContributions::new();
        let root = Path::new("/seg/one");
        contributions.replace_root(root, run(&[series(1, "cpu", &[]), series(2, "mem", &[])]));
        contributions.replace_root(root, run(&[series(1, "cpu", &[])]));

        assert_eq!(contributions.entry_count(), 1);
        assert_eq!(
            contributions.run_for_root(root).unwrap().len(),
            1,
            "the newest run wins"
        );
    }

    #[test]
    fn empty_cache_merges_to_the_empty_union_fingerprint() {
        let contributions = RegistryContributions::new();
        assert_eq!(contributions.entry_count(), 0);
        let fingerprint = merge_series_runs(&[]).unwrap();
        assert_eq!(fingerprint.series_count, 0);
        assert_eq!(
            fingerprint,
            fingerprint_series_fields(std::iter::empty::<(SeriesId, &str, &[Label])>())
        );
    }
}
