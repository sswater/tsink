use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::Xxh64;

use super::registry_contributions::{merge_series_runs, RegistryContributions, SeriesRun};
use super::tiering::SegmentLaneFamily;
use super::*;
use crate::engine::fs_utils::{path_exists_no_follow, write_file_atomically_and_sync_parent};
use crate::engine::segment::{
    is_vanished_source_error, load_segment_series_metadata, read_segment_manifest_fingerprint,
    IndexedSegment, ManifestFileEntry, SegmentManifest, MANIFEST_FILE_ENTRY_COUNT,
};
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::Label;
use tracing::warn;

const REGISTRY_CATALOG_FILE_NAME: &str = "series_index.catalog.json";
const REGISTRY_CATALOG_VERSION: u32 = 2;

/// Absolute budget for the series-entry set one catalog write may merge from the
/// contribution cache.
///
/// The cache now interns `(metric, labels)` payloads, so its remaining footprint
/// is the per-segment `SeriesId` entries. A write whose merged runs exceed this
/// many entries falls back to the disk path instead of paying an unbounded
/// in-memory merge (and holding the maintenance gate longer). The budget is an
/// absolute count, not a ratio to the distinct series count: a ratio fires on
/// the normal multi-level skew and silently reintroduces O(N) `series.bin`
/// reads on every debounced write.
pub(super) const DEFAULT_CONTRIBUTION_ENTRY_BUDGET: usize = 2_000_000;

/// One persisted segment the catalog must describe.
///
/// `manifest`/`files` carry what the inventory (or a just-verified publish)
/// already knows, so a catalog write does not re-read the segment. Sources that
/// only know lane + root (e.g. a shared catalog file) leave them `None` and pay
/// exactly one bounded manifest re-read.
#[derive(Debug, Clone)]
pub(super) struct PersistedRegistryCatalogSource {
    pub(super) lane: SegmentLaneFamily,
    pub(super) root: PathBuf,
    pub(super) manifest: Option<SegmentManifest>,
    pub(super) files: Option<[ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedRegistryCatalogFile {
    version: u32,
    segments: Vec<PersistedRegistryCatalogEntry>,
    #[serde(default)]
    series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedRegistryCatalogEntry {
    lane: SegmentLaneFamily,
    level: u8,
    segment_id: u64,
    chunk_count: usize,
    point_count: usize,
    series_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    wal_highwater_segment: u64,
    wal_highwater_frame: u64,
    chunks_len: u64,
    chunks_hash64: u64,
    chunk_index_len: u64,
    chunk_index_hash64: u64,
    series_len: u64,
    series_hash64: u64,
    postings_len: u64,
    postings_hash64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedRegistrySeriesFingerprint {
    pub(in crate::engine::storage_engine) series_count: usize,
    pub(in crate::engine::storage_engine) series_hash64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ValidatedRegistryCatalog {
    pub(super) series_fingerprint: Option<PersistedRegistrySeriesFingerprint>,
}

pub(super) fn catalog_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path
        .parent()
        .map(|parent| parent.join(REGISTRY_CATALOG_FILE_NAME))
        .unwrap_or_else(|| PathBuf::from(REGISTRY_CATALOG_FILE_NAME))
}

/// Compares the catalog sidecar against the expected segment entries.
///
/// The expectation is inventory-driven: sources that carry their manifest
/// `files` are compared from memory, so validation costs no segment reads for
/// segments the startup scan already parsed.
pub(super) fn validate_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Option<ValidatedRegistryCatalog>> {
    let path = catalog_path(snapshot_path);
    if !path_exists_no_follow(&path)? {
        return Ok(None);
    }

    let expected_segments = build_catalog_entries(sources)?;
    let actual = serde_json::from_slice::<PersistedRegistryCatalogFile>(&std::fs::read(&path)?)?;
    if actual.segments != expected_segments {
        return Ok(None);
    }

    Ok(Some(ValidatedRegistryCatalog {
        series_fingerprint: actual.series_fingerprint,
    }))
}

/// Writes the catalog from the in-memory inventory + contribution cache.
pub(super) fn persist_registry_catalog(
    snapshot_path: &Path,
    sources: &[PersistedRegistryCatalogSource],
    contributions: &RegistryContributions,
) -> Result<()> {
    write_registry_catalog(
        snapshot_path,
        &build_catalog_from_memory(sources, contributions)?,
    )
}

/// Writes the catalog from an inventory snapshot, with no segment reads: the
/// entries come straight from the inventory and the series fingerprint from the
/// contribution cache.
///
/// This is the refresh thread's debounced write path (C-2): by the time it runs
/// the persisted index already reflects the applied segment diff, so the
/// catalog describes the post-diff inventory.
pub(super) fn persist_registry_catalog_from_inventory(
    snapshot_path: &Path,
    inventory: &super::tiering::SegmentInventory,
    contributions: &RegistryContributions,
) -> Result<()> {
    let sources = inventory_sources(inventory);
    write_registry_catalog(
        snapshot_path,
        &PersistedRegistryCatalogFile {
            version: REGISTRY_CATALOG_VERSION,
            segments: build_catalog_entries_from_inventory(inventory, contributions)?,
            series_fingerprint: Some(build_series_fingerprint_from_contributions(
                &sources,
                contributions,
            )?),
        },
    )
}

fn write_registry_catalog(snapshot_path: &Path, file: &PersistedRegistryCatalogFile) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(file)?;
    write_file_atomically_and_sync_parent(&catalog_path(snapshot_path), &bytes)
}

pub(super) fn inventory_sources(
    inventory: &super::tiering::SegmentInventory,
) -> Vec<PersistedRegistryCatalogSource> {
    inventory
        .entries()
        .iter()
        .map(|entry| PersistedRegistryCatalogSource {
            lane: entry.lane,
            root: entry.root.clone(),
            manifest: Some(entry.manifest.clone()),
            files: entry.files,
        })
        .collect()
}

pub(super) fn validate_registry_snapshot(
    registry: &SeriesRegistry,
    indexed_segments: &[IndexedSegment],
    expected: &PersistedRegistrySeriesFingerprint,
) -> Result<()> {
    let series_ids = indexed_segment_series_ids(indexed_segments);
    let actual = fingerprint_registry_series(registry, &series_ids)?;
    if actual == *expected {
        return Ok(());
    }

    Err(TsinkError::DataCorruption(
        "persisted registry snapshot conflicts with persisted segment series metadata".to_string(),
    ))
}

/// Memory catalog build: entries come from the source inventory, the series
/// fingerprint from the contribution cache. Output must be byte-identical to
/// [`build_catalog_from_disk`] for the same segment set.
pub(super) fn build_catalog_from_memory(
    sources: &[PersistedRegistryCatalogSource],
    contributions: &RegistryContributions,
) -> Result<PersistedRegistryCatalogFile> {
    build_catalog_from_memory_with_budget(sources, contributions, DEFAULT_CONTRIBUTION_ENTRY_BUDGET)
}

/// Budget-parameterized memory build; tests lower the budget to pin the disk
/// fallback without allocating millions of entries.
pub(super) fn build_catalog_from_memory_with_budget(
    sources: &[PersistedRegistryCatalogSource],
    contributions: &RegistryContributions,
    entry_budget: usize,
) -> Result<PersistedRegistryCatalogFile> {
    Ok(PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: build_catalog_entries(sources)?,
        series_fingerprint: Some(build_series_fingerprint_from_contributions_with_budget(
            sources,
            contributions,
            entry_budget,
        )?),
    })
}

/// Disk catalog build: every field is re-read from the segment roots.
///
/// Kept as the byte-identical reference for the memory build in tests; the
/// production fallback is bounded to the series fingerprint
/// ([`build_series_fingerprint_from_sources`]), because entries never need a
/// full re-read beyond their per-source manifest fallback.
#[cfg(test)]
pub(super) fn build_catalog_from_disk(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<PersistedRegistryCatalogFile> {
    let disk_sources = sources
        .iter()
        .map(|source| PersistedRegistryCatalogSource {
            manifest: None,
            files: None,
            ..source.clone()
        })
        .collect::<Vec<_>>();
    Ok(PersistedRegistryCatalogFile {
        version: REGISTRY_CATALOG_VERSION,
        segments: build_catalog_entries(&disk_sources)?,
        series_fingerprint: Some(build_series_fingerprint_from_sources(&disk_sources)?),
    })
}

/// Catalog entries for an inventory snapshot.
///
/// The `contributions` handle is part of the in-memory build contract: the
/// entries and the fingerprint halves of one catalog are always produced from
/// the same inventory/contributions pair. Entries themselves are fully
/// inventory-driven and only fall back to one manifest read per entry whose
/// inventory source lacks the recorded file fingerprints.
pub(super) fn build_catalog_entries_from_inventory(
    inventory: &super::tiering::SegmentInventory,
    _contributions: &RegistryContributions,
) -> Result<Vec<PersistedRegistryCatalogEntry>> {
    build_catalog_entries(&inventory_sources(inventory))
}

/// A catalog source that vanished (root directory removed, or its required
/// files already unlinked) between the inventory snapshot and this read is a
/// segment mid-removal: compaction or retention retired it concurrently. The
/// catalog describes surviving segments, so skip the source instead of
/// failing — surfacing the error would fence the engine on a transient state.
fn is_vanished_catalog_source_error(err: &TsinkError) -> bool {
    is_vanished_source_error(err)
}

fn build_catalog_entries(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<Vec<PersistedRegistryCatalogEntry>> {
    let mut segments = Vec::with_capacity(sources.len());
    for source in sources {
        match build_catalog_entry(source) {
            Ok(entry) => segments.push(entry),
            Err(err) if is_vanished_catalog_source_error(&err) => {
                warn!(
                    path = %source.root.display(),
                    error = %err,
                    "Registry catalog source vanished mid-removal; skipping"
                );
            }
            Err(err) => return Err(err),
        }
    }
    segments.sort_by_key(|entry| (entry.lane, entry.level, entry.segment_id));
    Ok(segments)
}

fn build_catalog_entry(
    source: &PersistedRegistryCatalogSource,
) -> Result<PersistedRegistryCatalogEntry> {
    match (&source.manifest, source.files) {
        (Some(manifest), Some(files)) => Ok(catalog_entry(source.lane, manifest, &files)),
        _ => {
            // Bounded fallback: a source whose inventory only knows the root
            // (shared catalog file) or a legacy/test constructor.
            let fingerprint = read_segment_manifest_fingerprint(&source.root)?;
            Ok(catalog_entry(
                source.lane,
                &fingerprint.manifest,
                &fingerprint.files,
            ))
        }
    }
}

fn catalog_entry(
    lane: SegmentLaneFamily,
    manifest: &SegmentManifest,
    files: &[ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT],
) -> PersistedRegistryCatalogEntry {
    let [chunks, chunk_index, series, postings] = files;
    PersistedRegistryCatalogEntry {
        lane,
        level: manifest.level,
        segment_id: manifest.segment_id,
        chunk_count: manifest.chunk_count,
        point_count: manifest.point_count,
        series_count: manifest.series_count,
        min_ts: manifest.min_ts,
        max_ts: manifest.max_ts,
        wal_highwater_segment: manifest.wal_highwater.segment,
        wal_highwater_frame: manifest.wal_highwater.frame,
        chunks_len: chunks.file_len,
        chunks_hash64: chunks.hash64,
        chunk_index_len: chunk_index.file_len,
        chunk_index_hash64: chunk_index.hash64,
        series_len: series.file_len,
        series_hash64: series.hash64,
        postings_len: postings.file_len,
        postings_hash64: postings.hash64,
    }
}

/// Series fingerprint from the contribution cache.
///
/// Roots without a cached run (bootstrap priming failed, or a source that was
/// added without its series payload) are read from `series.bin` once and cached
/// for the following writes; a root that vanished mid-removal is skipped exactly
/// like the disk path skips it.
pub(super) fn build_series_fingerprint_from_contributions(
    sources: &[PersistedRegistryCatalogSource],
    contributions: &RegistryContributions,
) -> Result<PersistedRegistrySeriesFingerprint> {
    build_series_fingerprint_from_contributions_with_budget(
        sources,
        contributions,
        DEFAULT_CONTRIBUTION_ENTRY_BUDGET,
    )
}

pub(super) fn build_series_fingerprint_from_contributions_with_budget(
    sources: &[PersistedRegistryCatalogSource],
    contributions: &RegistryContributions,
    entry_budget: usize,
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut runs = Vec::<Arc<SeriesRun>>::with_capacity(sources.len());

    for source in sources {
        match contributions.run_for_root(&source.root) {
            Some(run) => runs.push(run),
            None => match load_segment_series_metadata(&source.root) {
                Ok(series) => {
                    let run = Arc::new(contributions.series_run_from_series(&series));
                    contributions.replace_root_shared(&source.root, Arc::clone(&run));
                    runs.push(run);
                }
                Err(err) if is_vanished_catalog_source_error(&err) => {
                    warn!(
                        path = %source.root.display(),
                        error = %err,
                        "Registry catalog source vanished mid-removal; skipping"
                    );
                }
                Err(err) => return Err(err),
            },
        }
    }

    let contribution_count = runs.iter().map(|run| run.len()).sum::<usize>();

    // Bounded degradation: never price an unbounded in-memory merge; re-derive
    // the fingerprint from the segments instead so the output is still exact.
    if contribution_count > entry_budget {
        warn!(
            contributions = contribution_count,
            cache_entries = contributions.entry_count(),
            entry_budget,
            "Registry contribution cache exceeds its entry budget; re-reading segment series for this catalog write"
        );
        return build_series_fingerprint_from_sources(sources);
    }

    let borrowed = runs.iter().map(|run| run.as_ref()).collect::<Vec<_>>();
    merge_series_runs(&borrowed)
}

pub(super) fn build_series_fingerprint_from_sources(
    sources: &[PersistedRegistryCatalogSource],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series_by_id = BTreeMap::<SeriesId, crate::engine::segment::PersistedSeries>::new();
    let mut series_id_by_key = BTreeMap::<(String, Vec<Label>), SeriesId>::new();

    for source in sources {
        let series_metadata = match load_segment_series_metadata(&source.root) {
            Ok(metadata) => metadata,
            Err(err) if is_vanished_catalog_source_error(&err) => {
                warn!(
                    path = %source.root.display(),
                    error = %err,
                    "Registry catalog source vanished mid-removal; skipping"
                );
                continue;
            }
            Err(err) => return Err(err),
        };
        for series in series_metadata {
            match series_by_id.get(&series.series_id) {
                Some(existing)
                    if existing.metric == series.metric && existing.labels == series.labels => {}
                Some(_) => {
                    return Err(TsinkError::DataCorruption(format!(
                        "series id {} conflicts across persisted segment metadata",
                        series.series_id
                    )));
                }
                None => {
                    let key = (series.metric.clone(), series.labels.clone());
                    if let Some(existing_id) = series_id_by_key.get(&key) {
                        if *existing_id != series.series_id {
                            return Err(TsinkError::DataCorruption(format!(
                                "series key already bound to id {}, persisted segment metadata tried to bind {}",
                                existing_id, series.series_id
                            )));
                        }
                    } else {
                        series_id_by_key.insert(key, series.series_id);
                    }
                    series_by_id.insert(series.series_id, series);
                }
            }
        }
    }

    Ok(fingerprint_series(series_by_id.values()))
}

fn indexed_segment_series_ids(indexed_segments: &[IndexedSegment]) -> Vec<SeriesId> {
    let mut series_ids = BTreeSet::new();
    for segment in indexed_segments {
        for entry in &segment.chunk_index.entries {
            series_ids.insert(entry.series_id);
        }
    }
    series_ids.into_iter().collect()
}

fn fingerprint_registry_series(
    registry: &SeriesRegistry,
    series_ids: &[SeriesId],
) -> Result<PersistedRegistrySeriesFingerprint> {
    let mut series = Vec::with_capacity(series_ids.len());
    for series_id in series_ids {
        let Some(series_key) = registry.decode_series_key(*series_id) else {
            return Err(TsinkError::DataCorruption(format!(
                "persisted registry snapshot is missing series id {} required by persisted segments",
                series_id
            )));
        };
        series.push(crate::engine::segment::PersistedSeries {
            series_id: *series_id,
            metric: series_key.metric,
            labels: series_key.labels,
            value_family: None,
        });
    }
    Ok(fingerprint_series(series.iter()))
}

fn fingerprint_series<'a>(
    series: impl IntoIterator<Item = &'a crate::engine::segment::PersistedSeries>,
) -> PersistedRegistrySeriesFingerprint {
    fingerprint_series_fields(series.into_iter().map(|series| {
        (
            series.series_id,
            series.metric.as_str(),
            series.labels.as_slice(),
        )
    }))
}

/// The single fingerprint algorithm: `Xxh64` over the series identity in
/// `SeriesId` order. Both the disk path and the in-memory merge feed this, so the
/// catalog hash bytes cannot drift between them.
pub(super) fn fingerprint_series_fields<'a>(
    series: impl IntoIterator<Item = (SeriesId, &'a str, &'a [Label])>,
) -> PersistedRegistrySeriesFingerprint {
    let mut count = 0usize;
    let mut hasher = Xxh64::new(0);
    for (series_id, metric, labels) in series {
        count = count.saturating_add(1);
        hasher.update(&series_id.to_le_bytes());
        update_len_prefixed_bytes(&mut hasher, metric.as_bytes());
        hasher.update(&(labels.len() as u64).to_le_bytes());
        for label in labels {
            update_len_prefixed_bytes(&mut hasher, label.name.as_bytes());
            update_len_prefixed_bytes(&mut hasher, label.value.as_bytes());
        }
    }
    PersistedRegistrySeriesFingerprint {
        series_count: count,
        series_hash64: hasher.digest(),
    }
}

fn update_len_prefixed_bytes(hasher: &mut Xxh64, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn disk_only_source(root: PathBuf) -> PersistedRegistryCatalogSource {
        PersistedRegistryCatalogSource {
            lane: SegmentLaneFamily::Numeric,
            root,
            manifest: None,
            files: None,
        }
    }

    #[test]
    fn catalog_build_skips_sources_that_vanish_mid_removal() {
        let temp_dir = TempDir::new().unwrap();
        let ghost = disk_only_source(temp_dir.path().join("seg-00000000000000ff"));

        // The root directory does not exist. Before the vanished-source
        // tolerance this surfaced as a bare ENOENT from the manifest
        // fingerprint read and fenced the flush worker while it persisted
        // the registry catalog (production backtrace, 2026-11-01).
        let entries = build_catalog_entries(std::slice::from_ref(&ghost)).unwrap();
        assert!(entries.is_empty());

        let fingerprint =
            build_series_fingerprint_from_sources(std::slice::from_ref(&ghost)).unwrap();
        assert_eq!(fingerprint.series_count, 0);

        // The memory path skips the same vanished root from either half.
        let contributions = RegistryContributions::new();
        let fingerprint = build_series_fingerprint_from_contributions(
            std::slice::from_ref(&ghost),
            &contributions,
        )
        .unwrap();
        assert_eq!(fingerprint.series_count, 0);
        assert!(
            contributions.run_for_root(&ghost.root).is_none(),
            "a vanished root must stay unprimed"
        );
    }

    /// The fingerprint hash is part of the on-disk contract; the in-memory path
    /// must reproduce it exactly, including the label ordering.
    #[test]
    fn fingerprint_series_fields_hashes_series_id_then_metric_then_labels() {
        let labels = vec![Label::new("host", "a"), Label::new("zone", "z")];
        let fields = fingerprint_series_fields([(42u64, "cpu", labels.as_slice())]);

        let persisted = crate::engine::segment::PersistedSeries {
            series_id: 42,
            metric: "cpu".to_string(),
            labels: labels.clone(),
            value_family: None,
        };
        assert_eq!(fields, fingerprint_series([&persisted]));
    }
}
