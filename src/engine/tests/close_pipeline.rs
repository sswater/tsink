//! Close-pipeline closure semantics.
//!
//! `close()` is the last chance to publish durable state, so a maintenance
//! failure it *can* survive must be deferred (recorded + skipped) instead of
//! aborting the remaining steps. These tests pin that contract.

use super::*;
use crate::engine::tombstone::{TombstoneRange, TOMBSTONES_FILE_NAME};
use std::path::PathBuf;

fn write_l0_numeric_segment(
    lane_path: &Path,
    registry: &SeriesRegistry,
    series_id: crate::engine::series::SeriesId,
    segment_id: u64,
    point: (i64, f64),
) -> PathBuf {
    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, &[point])],
    );
    let writer = SegmentWriter::new(lane_path, 0, segment_id).unwrap();
    writer.write_segment(registry, &chunks).unwrap();
    writer.layout().root.clone()
}

struct CloseCompactionFixture {
    storage: ChunkStorage,
    roots: Vec<PathBuf>,
    series_id: crate::engine::series::SeriesId,
}

/// Eight disjoint L0 segments: the L0 trigger (4) publishes the first window
/// and only then reaches the second one. Any corruption planted in the last
/// segment therefore exercises the partial-failure shape the close deferral has
/// to survive — window one is already merged into the pending diff when window
/// two fails.
fn close_compaction_fixture(data_path: &Path) -> CloseCompactionFixture {
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("close_compaction_deferral", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut roots = Vec::new();
    for segment_id in 1..=8u64 {
        roots.push(write_l0_numeric_segment(
            &lane_path,
            &registry,
            series_id,
            segment_id,
            (segment_id as i64, segment_id as f64),
        ));
    }

    let mut storage = open_raw_numeric_storage_from_data_path(data_path);
    storage
        .persisted
        .numeric_compactor
        .as_mut()
        .unwrap()
        .set_pass_wall_budget(Duration::from_secs(60));

    CloseCompactionFixture {
        storage,
        roots,
        series_id,
    }
}

#[test]
fn close_with_failing_compaction_still_persists_refresh_and_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let lane_path = data_path.join(NUMERIC_LANE_ROOT);
    let checkpoint_path = data_path.join(SERIES_INDEX_FILE_NAME);
    let CloseCompactionFixture {
        storage,
        roots,
        series_id,
    } = close_compaction_fixture(data_path);

    // Seed a tombstone that no earlier step persists: `persist_tombstones_index`
    // is the close step whose continuity this test pins, so the store must not
    // exist until close() runs it.
    storage
        .visibility
        .tombstones
        .write()
        .entry(series_id)
        .or_default()
        .push(TombstoneRange { start: 8, end: 9 });
    let tombstones_path = lane_path.join(TOMBSTONES_FILE_NAME);
    assert!(
        !tombstones_path.exists(),
        "fixture invariant: the tombstone store must be unwritten before close"
    );

    // Payload hash mismatch in the last window: compaction loads the member and
    // rejects it ("failed compaction validation"), it is not a vanished source.
    let corrupted_root = roots[7].clone();
    let corrupted_chunks_path = corrupted_root.join("chunks.bin");
    let original_chunks = std::fs::read(&corrupted_chunks_path).unwrap();
    let mut corrupted_chunks = original_chunks.clone();
    let last_byte = corrupted_chunks.len() - 1;
    corrupted_chunks[last_byte] ^= 0xff;
    std::fs::write(&corrupted_chunks_path, corrupted_chunks).unwrap();

    let close_result = storage.close();

    // Restore the fixture bytes before asserting, so a failure message describes
    // the real bug rather than the planted corruption.
    std::fs::write(&corrupted_chunks_path, original_chunks).unwrap();

    close_result.expect(
        "close must defer a compaction error: refresh, tombstones and the registry \
         checkpoint all still have to run",
    );

    let health = storage.observability_snapshot().health;
    let maintenance_error = health
        .last_maintenance_error
        .clone()
        .unwrap_or_else(|| "close did not record the deferred compaction error".to_string());
    assert_eq!(
        health.maintenance_errors_total, 1,
        "the deferred compaction error must be recorded exactly once: {maintenance_error}"
    );
    assert!(
        maintenance_error.contains("close compaction"),
        "expected the deferral to name the close compaction step, got: {maintenance_error}"
    );
    assert!(
        maintenance_error.contains("failed compaction validation"),
        "expected the recorded deferral to carry the compaction error, got: {maintenance_error}"
    );

    // The first window published before the failure: its L1 outputs are merged
    // and the close refresh applied them, while the failed window's sources are
    // still visible for restart discovery to retry.
    let persisted_index = storage.persisted.persisted_index.read();
    let levels = persisted_index
        .segments_by_root
        .values()
        .map(|segment| segment.manifest.level)
        .collect::<Vec<_>>();
    assert!(
        levels.contains(&1),
        "the published L1 window must survive the deferral and land in the index: {levels:?}"
    );
    assert!(
        persisted_index
            .segments_by_root
            .contains_key(&corrupted_root),
        "the failed window's sources stay visible until a later compaction retires them"
    );

    assert!(
        tombstones_path.exists(),
        "tombstone persistence must still run after a deferred compaction error"
    );
    let persisted_tombstones = crate::engine::tombstone::load_tombstones(&tombstones_path).unwrap();
    assert_eq!(
        persisted_tombstones.get(&series_id),
        Some(&vec![TombstoneRange { start: 8, end: 9 }]),
        "close must persist the pending tombstones unchanged after a deferred compaction error"
    );
    assert!(
        checkpoint_path.exists(),
        "the registry checkpoint must still be written after a deferred compaction error"
    );
}

#[test]
fn close_defers_compaction_when_a_visible_manifest_is_unreadable() {
    // A manifest-level corruption fails compaction *and* the registry catalog
    // index build (both parse the manifest). Deferring only the compaction error
    // while keeping the strict catalog policy would still abort close at the
    // checkpoint, so this test pins that a deferred close also tolerates the
    // catalog index it can no longer rebuild: the registry snapshot is written,
    // the derived catalog sidecar is skipped, and restart rebuilds it from its
    // disk walk.
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let checkpoint_path = data_path.join(SERIES_INDEX_FILE_NAME);
    let CloseCompactionFixture { storage, roots, .. } = close_compaction_fixture(data_path);

    let corrupted_manifest_path = roots[7].join("manifest.bin");
    let original_manifest = std::fs::read(&corrupted_manifest_path).unwrap();
    let mut corrupted_manifest = original_manifest.clone();
    corrupted_manifest[0] ^= 0xff;
    std::fs::write(&corrupted_manifest_path, corrupted_manifest).unwrap();

    // The contribution cache holds this root's series identity from before the
    // corruption, so the in-memory fingerprint would otherwise be derivable and
    // the catalog writable. Evict it to model a root that was never primed: its
    // fingerprint has to read the (now corrupt) manifest, which is exactly the
    // "catalog cannot be rebuilt" case this test pins.
    storage
        .persisted
        .registry_contributions
        .remove_root(&roots[7]);

    let close_result = storage.close();

    std::fs::write(&corrupted_manifest_path, original_manifest).unwrap();

    close_result.expect(
        "close must satisfy the deferral promise even when the visible catalog can no \
         longer be rebuilt: the registry checkpoint is still written",
    );
    assert!(
        checkpoint_path.exists(),
        "the registry checkpoint must still be written when the catalog index is skipped"
    );

    // Task 12: the catalog rewrite is debounced off the flush path, but close
    // still writes it synchronously. This close cannot rebuild it (the catalog
    // fingerprint needs the series payload the corrupted manifest gates), so
    // the deferred path leaves the sidecar absent instead of failing close —
    // restart rebuilds it from its disk walk.
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    assert!(
        !catalog_path.exists(),
        "a deferred close must not leave a catalog sidecar it could not rebuild"
    );
}
