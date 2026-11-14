use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;

use super::{
    compaction_park_interval, execution, finalize_pending_compaction_replacements,
    scan_inventory_from_disk, CompactionInventorySource, CompactionRunStats, Compactor,
    COMPACTION_BACKLOG_PARK,
};
use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::fs_utils::{
    count_segment_file_reads_under, set_segment_file_read_hook, CountingSegmentReads,
};
use crate::engine::segment::{
    load_segment_root_for_compaction, load_segments, load_segments_for_level,
    read_segment_manifest_with_files, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
use crate::engine::storage_engine::tiering::{
    PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily,
};
use crate::engine::storage_engine::PendingPersistedSegmentDiff;
use crate::engine::tombstone::{persist_tombstones, TombstoneRange, TOMBSTONES_FILE_NAME};
use crate::{
    HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram, Value,
};

fn sample_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -1,
            length: 1,
        }],
        negative_deltas: vec![2],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![3, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.25, 0.5],
    }
}

#[test]
fn compacts_overlapping_l0_segments_into_l1() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert!(l0.is_empty());
    assert_eq!(l1.len(), 1);

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].header.point_count, 4);
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    assert_eq!(decoded[0].ts, 10);
    assert_eq!(decoded[3].ts, 30);
}

#[test]
fn compaction_preserves_histogram_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("rpc_duration_seconds", &[Label::new("job", "backend")])
        .unwrap()
        .series_id;

    let first_chunk = make_histogram_chunk(series_id, &[10, 20]);
    let second_chunk = make_histogram_chunk(series_id, &[30, 40]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = chunks
        .iter()
        .flat_map(|chunk| execution::decode_chunk_points_for_compaction(chunk).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded.len(), 4);
    for (point, ts) in decoded.iter().zip([10, 20, 30, 40]) {
        assert_eq!(point.ts, ts);
        assert_eq!(point.value, Value::from(sample_histogram()));
    }
}

#[test]
fn compactor_uses_shared_segment_id_allocator() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_chunk = make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)]);
    let second_chunk = make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)]);

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(series_id, vec![first_chunk]);

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(series_id, vec![second_chunk]);

    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let next_segment_id = Arc::new(AtomicU64::new(100));
    let compactor =
        Compactor::new_with_segment_id_allocator(temp_dir.path(), 8, Arc::clone(&next_segment_id));
    compactor.compact_once().unwrap();

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.segment_id, 100);
    assert_eq!(next_segment_id.load(Ordering::SeqCst), 101);
}

#[test]
fn compactor_limits_source_window_per_pass() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=6 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 8);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();

    assert_eq!(l0.len(), 2);
    assert_eq!(l1.len(), 1);
    assert_eq!(
        l0.iter()
            .map(|segment| segment.manifest.segment_id)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
}

#[test]
fn compactor_splits_large_output_into_multiple_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let start = (segment_id as i64 - 1) * 300;
        let points = (start..start + 300)
            .map(|ts| (ts, ts as f64))
            .collect::<Vec<_>>();

        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);
        SegmentWriter::new(temp_dir.path(), 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let compactor = Compactor::new(temp_dir.path(), 2);
    compactor.compact_once().unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert!(l0.is_empty());
    assert!(l1.len() >= 2);
    assert!(l1.iter().all(|segment| {
        segment.manifest.point_count <= 2 * super::DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER
    }));

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let total_points = chunks
        .iter()
        .map(|chunk| chunk.header.point_count as usize)
        .sum::<usize>();
    assert_eq!(total_points, 1200);
}

#[test]
fn compactor_purges_tombstoned_points() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut seg1_chunks = HashMap::new();
    seg1_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &seg1_chunks)
        .unwrap();

    let mut seg2_chunks = HashMap::new();
    seg2_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    SegmentWriter::new(temp_dir.path(), 0, 2)
        .unwrap()
        .write_segment(&registry, &seg2_chunks)
        .unwrap();

    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 15, end: 21 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    assert!(compactor.compact_once().unwrap());

    let loaded = load_segments(temp_dir.path()).unwrap();
    let chunks = loaded.chunks_by_series.get(&series_id).unwrap();
    let decoded = execution::decode_chunk_points_for_compaction(&chunks[0]).unwrap();
    let timestamps = decoded
        .into_iter()
        .map(|point| point.ts)
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![10, 30]);
}

#[test]
fn compaction_fails_when_any_source_segment_is_corrupted() {
    // Historical name, narrowed semantics: a pass only loads the members of the
    // selected *window* (plus, on the standalone disk-scan fallback used here,
    // the manifests it plans from). This test pins that a corrupt source fails
    // the round instead of being skipped like a vanished one; the injected
    // inventory path is covered by `corrupt_window_member_still_fails_the_round`.
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let first_writer = SegmentWriter::new(temp_dir.path(), 0, 1).unwrap();
    let mut first_chunks = HashMap::new();
    first_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(10, 1.0), (20, 2.0)])],
    );
    first_writer
        .write_segment(&registry, &first_chunks)
        .unwrap();

    let second_writer = SegmentWriter::new(temp_dir.path(), 0, 2).unwrap();
    let mut second_chunks = HashMap::new();
    second_chunks.insert(
        series_id,
        vec![make_numeric_chunk(series_id, &[(15, 3.0), (30, 4.0)])],
    );
    second_writer
        .write_segment(&registry, &second_chunks)
        .unwrap();

    let corrupt_root = first_writer.layout().root.clone();
    let mut manifest_bytes = fs::read(first_writer.layout().manifest_path.clone()).unwrap();
    manifest_bytes[0] ^= 0xff;
    fs::write(first_writer.layout().manifest_path.clone(), manifest_bytes).unwrap();

    let compactor = Compactor::new(temp_dir.path(), 8);
    let err = compactor.compact_once().unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("failed compaction validation"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("manifest crc32 mismatch"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&corrupt_root.display().to_string()),
        "unexpected error: {message}"
    );
    assert!(
        corrupt_root.exists(),
        "corrupt source root should stay in place"
    );
    assert!(
        second_writer.layout().root.exists(),
        "healthy source root should not be compacted away"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 1)
            .unwrap()
            .is_empty(),
        "compaction must not publish replacement output when any source root is corrupt"
    );
}

#[test]
fn finalize_pending_replacements_removes_marked_source_segments() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut l0_chunks = HashMap::new();
    l0_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 1.0)])]);
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &l0_chunks)
        .unwrap();

    let mut l1_chunks = HashMap::new();
    l1_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 2.0)])]);
    SegmentWriter::new(temp_dir.path(), 1, 2)
        .unwrap()
        .write_segment(&registry, &l1_chunks)
        .unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l1.len(), 1);

    let source_root = l0[0].root.clone();
    let output_root = l1[0].root.clone();
    let marker_path = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        std::slice::from_ref(&source_root),
        std::slice::from_ref(&output_root),
    )
    .unwrap();
    assert!(marker_path.exists());

    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();

    assert!(!source_root.exists(), "source segment should be removed");
    assert!(
        output_root.exists(),
        "replacement output should be preserved"
    );
    assert!(
        !marker_path.exists(),
        "replacement marker should be removed after apply"
    );
}

#[test]
fn finalize_pending_replacements_ignores_dot_prefixed_temp_files() {
    let temp_dir = TempDir::new().unwrap();
    let registry = SeriesRegistry::new();

    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut l0_chunks = HashMap::new();
    l0_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 1.0)])]);
    SegmentWriter::new(temp_dir.path(), 0, 1)
        .unwrap()
        .write_segment(&registry, &l0_chunks)
        .unwrap();

    let mut l1_chunks = HashMap::new();
    l1_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 2.0)])]);
    SegmentWriter::new(temp_dir.path(), 1, 2)
        .unwrap()
        .write_segment(&registry, &l1_chunks)
        .unwrap();

    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    let source_root = l0[0].root.clone();
    let output_root = l1[0].root.clone();
    let marker_path = execution::write_compaction_replacement_marker(
        temp_dir.path(),
        std::slice::from_ref(&source_root),
        std::slice::from_ref(&output_root),
    )
    .unwrap();

    // A partially written marker temp file lives in the same directory and is
    // dot-prefixed by construction; it must never be treated as a marker.
    let temp_marker = temp_dir
        .path()
        .join(".compaction-replacements")
        .join(".replace-deadbeef.tmp-1-0000000000000000");
    fs::write(&temp_marker, b"{ not a complete marker").unwrap();

    finalize_pending_compaction_replacements(temp_dir.path()).unwrap();

    assert!(
        !source_root.exists(),
        "the real marker must still be applied"
    );
    assert!(
        output_root.exists(),
        "replacement output should be preserved"
    );
    assert!(
        !marker_path.exists(),
        "the real marker should be removed after apply"
    );
    assert!(
        temp_marker.exists(),
        "a dot-prefixed temp file must be ignored, not consumed as a marker"
    );
}

#[test]
fn compactor_skips_round_until_inventory_source_is_set() {
    let temp_dir = TempDir::new().unwrap();
    write_disjoint_l0_segments(temp_dir.path(), 6);

    let compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        CompactionInventorySource::default(),
    );

    let _guard = CountingSegmentReads::install(temp_dir.path());
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(!stats.compacted);
    assert_eq!(stats.windows_merged, 0);
    assert_eq!(
        count_segment_file_reads_under(temp_dir.path()),
        0,
        "注入前不得扫盘"
    );
}

#[test]
fn concurrent_rounds_on_the_same_compactor_do_not_overlap() {
    use std::sync::mpsc;

    let temp_dir = TempDir::new().unwrap();
    write_non_overlapping_l0_segments(temp_dir.path(), 6);

    let scan_root = temp_dir.path().to_path_buf();
    let source = CompactionInventorySource::default();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Mutex::new(release_rx);
    source.set(Arc::new(move || {
        // Signal that the first round is inside its round, then hold it there
        // until the test has attempted the contended round.
        let _ = entered_tx.send(());
        let _ = release_rx.lock().unwrap().recv();
        scan_inventory_from_disk(&scan_root).unwrap()
    }));

    let compactor = Arc::new(Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    ));

    let holder = {
        let compactor = Arc::clone(&compactor);
        std::thread::spawn(move || compactor.compact_once_with_stats())
    };
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the first round must reach the inventory provider");

    let contended = compactor.compact_once_with_stats().unwrap();
    assert!(
        !contended.compacted,
        "the contended round must not run concurrently"
    );
    assert_eq!(
        contended.windows_merged, 0,
        "the contended round must return immediately while the other drains"
    );

    release_tx.send(()).unwrap();
    let winner = holder.join().unwrap().unwrap();
    assert!(winner.compacted, "the claiming round must do the work");
    assert_eq!(winner.windows_merged, 1);

    // Exactly one round's output: 6 disjoint L0, trigger 4 -> one window of 4.
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 0).unwrap().len(),
        2,
        "a duplicate round would have consumed the remaining segments"
    );
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 1).unwrap().len(),
        1,
        "a duplicate round would have published a second L1 output"
    );
}

#[test]
fn disk_scan_inventory_fills_lane_and_tier() {
    let temp_dir = TempDir::new().unwrap();
    write_disjoint_l0_segments(temp_dir.path(), 2);

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "b")])
        .unwrap()
        .series_id;
    let mut l1_chunks = HashMap::new();
    l1_chunks.insert(series_id, vec![make_numeric_chunk(series_id, &[(1, 1.0)])]);
    SegmentWriter::new(temp_dir.path(), 1, 3)
        .unwrap()
        .write_segment(&registry, &l1_chunks)
        .unwrap();

    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();
    let levels = inventory
        .entries()
        .iter()
        .map(|entry| entry.manifest.level)
        .collect::<Vec<_>>();
    assert_eq!(levels, vec![0, 0, 1]);
    assert!(inventory
        .entries()
        .iter()
        .all(|entry| entry.lane == SegmentLaneFamily::Numeric
            && entry.tier == PersistedSegmentTier::Hot));

    let blob_root = temp_dir.path().join("lane_blob");
    let mut blob_chunks = HashMap::new();
    blob_chunks.insert(series_id, vec![make_histogram_chunk(series_id, &[1, 2])]);
    SegmentWriter::new(&blob_root, 0, 1)
        .unwrap()
        .write_segment(&registry, &blob_chunks)
        .unwrap();

    let blob_inventory = scan_inventory_from_disk(&blob_root).unwrap();
    assert_eq!(blob_inventory.entries().len(), 1);
    assert_eq!(blob_inventory.entries()[0].lane, SegmentLaneFamily::Blob);
    assert_eq!(blob_inventory.entries()[0].tier, PersistedSegmentTier::Hot);
}

#[test]
fn injected_inventory_source_falls_through_to_existing_planning() {
    let temp_dir = TempDir::new().unwrap();
    write_disjoint_l0_segments(temp_dir.path(), 6);

    let source = CompactionInventorySource::default();
    let compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source.clone(),
    );
    let scan_root = temp_dir.path().to_path_buf();
    source.set(Arc::new(move || {
        scan_inventory_from_disk(scan_root.as_path()).unwrap()
    }));

    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(stats.compacted);
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 0).unwrap().len(),
        2
    );
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 1).unwrap().len(),
        1
    );
}

#[test]
fn inventory_driven_planning_matches_disk_driven_outcome() {
    let disk_dir = TempDir::new().unwrap();
    let inventory_dir = TempDir::new().unwrap();
    write_time_overlapping_l0_segments(disk_dir.path(), 5);
    write_time_overlapping_l0_segments(inventory_dir.path(), 5);

    let disk_outcome = RoundCapture::run(&Compactor::new(disk_dir.path(), 8)).unwrap();

    // Snapshot the inventory, then inject it: planning must reach the same
    // decision an on-disk scan would have reached.
    let inventory = scan_inventory_from_disk(inventory_dir.path()).unwrap();
    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let compactor = Compactor::new_with_inventory_source(
        inventory_dir.path(),
        8,
        Arc::new(AtomicU64::new(6)),
        source,
    );
    let inventory_outcome = RoundCapture::run(&compactor).unwrap();

    assert!(disk_outcome.stats.compacted);
    assert!(inventory_outcome.stats.compacted);
    assert_eq!(
        relative_roots(disk_dir.path(), &disk_outcome.removed_roots()),
        relative_roots(inventory_dir.path(), &inventory_outcome.removed_roots()),
        "same source window must be selected"
    );
    assert_eq!(
        relative_roots(disk_dir.path(), &disk_outcome.added_roots()),
        relative_roots(inventory_dir.path(), &inventory_outcome.added_roots()),
        "same outputs must be published"
    );
    assert_eq!(
        disk_outcome.stats.windows_merged,
        inventory_outcome.stats.windows_merged
    );
    assert_eq!(
        disk_outcome.stats.source_segments,
        inventory_outcome.stats.source_segments
    );
    assert_eq!(
        disk_outcome.stats.output_segments,
        inventory_outcome.stats.output_segments
    );
    assert_eq!(
        disk_outcome.stats.source_chunks,
        inventory_outcome.stats.source_chunks
    );
    assert_eq!(
        disk_outcome.stats.output_points,
        inventory_outcome.stats.output_points
    );
}

#[test]
fn vanished_window_member_is_skipped_and_survivors_compacted() {
    let temp_dir = TempDir::new().unwrap();
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 4);

    // The snapshot still lists all four; one is retired (manifest unlinked)
    // right before the window is loaded — the NFS mid-removal race.
    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();
    let vanished_root = roots[1].clone();
    fs::remove_file(vanished_root.join("manifest.bin")).unwrap();

    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );

    let outcome = RoundCapture::run(&compactor).unwrap();
    assert!(outcome.stats.compacted);
    assert_eq!(outcome.stats.windows_merged, 1);
    assert!(!outcome.added_roots().is_empty());
    assert_eq!(
        outcome.stats.source_segments, 3,
        "the three surviving window members must be merged"
    );
    let removed = outcome.removed_roots();
    assert_eq!(
        removed.len(),
        4,
        "the three merged sources plus the retired ghost must leave the catalog: {removed:?}"
    );
    assert!(
        removed.contains(&vanished_root),
        "a vanished member is already gone from disk, but must still be retired from the catalog"
    );

    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 3);
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 0).unwrap().len(),
        0,
        "only the vanished root stays behind, and it no longer loads"
    );
}

#[test]
fn corrupt_window_member_still_fails_the_round() {
    let temp_dir = TempDir::new().unwrap();
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 4);
    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();

    // A real corruption (payload hash mismatch, not a missing file) must keep
    // failing the pass: vanished-source tolerance is not damage tolerance.
    let corrupted_root = roots[1].clone();
    let chunks_path = corrupted_root.join("chunks.bin");
    let mut chunks_bytes = fs::read(&chunks_path).unwrap();
    let last = chunks_bytes.len() - 1;
    chunks_bytes[last] ^= 0xff;
    fs::write(&chunks_path, chunks_bytes).unwrap();

    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );

    let err = compactor
        .compact_round_with_changes(&mut |_| {})
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("failed compaction validation"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains(&corrupted_root.display().to_string()),
        "unexpected error: {message}"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 1)
            .unwrap()
            .is_empty(),
        "no replacement output may be published when a window member is corrupt"
    );
}

#[test]
fn planning_reads_scale_with_window_not_inventory_size() {
    let temp_dir = TempDir::new().unwrap();
    write_non_overlapping_l0_segments(temp_dir.path(), 12);
    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();

    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let mut compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );
    // One window per pass: this test measures what *planning a window* costs.
    // A full drain of the same 12 segments legitimately loads all 12 (see
    // `drain_loop_merges_until_below_trigger`), which is a different question.
    compactor.set_pass_wall_budget(Duration::ZERO);

    let _guard = CountingSegmentReads::install(temp_dir.path());
    let outcome = RoundCapture::run(&compactor).unwrap();
    let reads = count_segment_file_reads_under(temp_dir.path());

    assert!(outcome.stats.compacted);
    assert_eq!(outcome.stats.windows_merged, 1);
    assert_eq!(
        outcome.stats.source_segments, 4,
        "the trigger window is the first four candidates"
    );
    assert_eq!(
        reads,
        4 * 5 + 1,
        "only the four window members' five files plus the one absorbed output \
         manifest may be read; an inventory of 12 must not add scans (observed {reads})"
    );
}

#[test]
fn drain_loop_merges_until_below_trigger() {
    let temp_dir = TempDir::new().unwrap();
    // Twelve disjoint L0 segments: one 4-segment window is not enough to get
    // below the trigger line, so a single-window pass would leave a backlog.
    write_non_overlapping_l0_segments(temp_dir.path(), 12);

    // The wall-clock budget only exists to keep a pass from starving flush;
    // give these drain assertions room so they do not depend on machine speed.
    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::from_secs(60));
    let stats = compactor.compact_once_with_stats().unwrap();

    assert!(stats.compacted);
    assert!(
        stats.windows_merged >= 2,
        "a backlogged L0 must drain over several windows in one pass, merged {}",
        stats.windows_merged
    );
    let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap();
    assert!(
        l0.len() < 4,
        "the pass must end with L0 below the trigger line, {} segments left",
        l0.len()
    );
    assert!(
        !stats.source_above_trigger,
        "nothing was left above the trigger line"
    );
    assert!(
        load_segments_for_level(temp_dir.path(), 2)
            .unwrap()
            .is_empty(),
        "three L1 outputs are below the L1 trigger, so nothing may reach L2"
    );
}

#[test]
fn wall_budget_stops_new_windows_but_first_window_always_runs() {
    let temp_dir = TempDir::new().unwrap();
    write_non_overlapping_l0_segments(temp_dir.path(), 12);

    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::ZERO);
    let stats = compactor.compact_once_with_stats().unwrap();

    assert!(stats.compacted);
    assert_eq!(
        stats.windows_merged, 1,
        "an exhausted budget must still open the first window (min-1 guarantee)"
    );
    assert!(
        stats.source_above_trigger,
        "eight L0 segments are still above the trigger after one window"
    );
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 0).unwrap().len(),
        8,
        "exactly one window's worth of L0 segments may be consumed"
    );
}

#[test]
fn vanished_member_is_evicted_and_round_converges() {
    let temp_dir = TempDir::new().unwrap();
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 12);
    let ghost_root = roots[1].clone();

    // The planning inventory is the persisted index: it still lists the ghost
    // until this pass's diff retires it. The test stands in for that diff by
    // removing the reported roots from the inventory between rounds.
    let live_inventory = Arc::new(Mutex::new(
        scan_inventory_from_disk(temp_dir.path()).unwrap(),
    ));
    fs::remove_file(ghost_root.join("manifest.bin")).unwrap();

    let source = CompactionInventorySource::default();
    let provider_inventory = Arc::clone(&live_inventory);
    source.set(Arc::new(move || provider_inventory.lock().unwrap().clone()));
    let mut compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );
    // One window for round one, so the backlog survives into round two.
    compactor.set_pass_wall_budget(Duration::ZERO);

    let ghost_reads = Arc::new(AtomicUsize::new(0));
    let read_counter = Arc::clone(&ghost_reads);
    let ghost_prefix = ghost_root.clone();
    let _hook = set_segment_file_read_hook(Some(Arc::new(move |path: &Path| {
        if path.starts_with(&ghost_prefix) {
            read_counter.fetch_add(1, Ordering::SeqCst);
        }
    })));

    let first = RoundCapture::run(&compactor).unwrap();
    assert_eq!(first.stats.windows_merged, 1);
    assert!(
        first.removed_roots().contains(&ghost_root),
        "the vanished member must leave the catalog through the window diff: {:?}",
        first.removed_roots()
    );
    assert!(
        !first.added_roots().contains(&ghost_root),
        "a retired root must never be re-added"
    );
    assert!(
        first.stats.source_above_trigger,
        "the remaining L0 backlog still needs draining"
    );
    assert!(
        ghost_reads.load(Ordering::SeqCst) > 0,
        "round one had to try loading the ghost before it could retire it"
    );

    // The pass's diff flows out of the inventory: retired and merged roots
    // leave, output roots enter.
    let updated = apply_diff_to_inventory(
        live_inventory.lock().unwrap().clone(),
        &first.diff(),
        SegmentLaneFamily::Numeric,
    );
    *live_inventory.lock().unwrap() = updated;

    compactor.set_pass_wall_budget(Duration::from_secs(60));
    ghost_reads.store(0, Ordering::SeqCst);

    let second = RoundCapture::run(&compactor).unwrap();
    assert_eq!(
        ghost_reads.load(Ordering::SeqCst),
        0,
        "an already-retired ghost must not be touched again"
    );
    assert!(
        !second.removed_roots().contains(&ghost_root),
        "round two has no ghosts left: {:?}",
        second.removed_roots()
    );
    assert!(
        second.stats.windows_merged >= 1,
        "round two must drain the remaining L0 backlog"
    );
    assert!(!second.stats.source_above_trigger);
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 0).unwrap().len(),
        0,
        "the backlog is fully drained and the ghost no longer loads"
    );
}

#[test]
fn same_pass_l1_outputs_are_visible_to_l1_to_l2() {
    let temp_dir = TempDir::new().unwrap();
    // Four L0 windows of four segments each produce four L1 outputs, which is
    // exactly the L1 trigger: the same pass must then compact L1→L2 using the
    // outputs it just wrote.
    write_non_overlapping_l0_segments(temp_dir.path(), 16);

    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::from_secs(60));
    let stats = compactor.compact_once_with_stats().unwrap();

    assert_eq!(
        stats.windows_merged, 5,
        "four L0 windows plus one L1→L2 window over their outputs"
    );
    assert!(load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .is_empty());
    assert!(load_segments_for_level(temp_dir.path(), 1)
        .unwrap()
        .is_empty());
    let l2 = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert_eq!(l2.len(), 1);
    assert_eq!(l2[0].manifest.point_count, 16);
}

#[test]
fn flush_gate_wait_bounded_by_single_window() {
    let temp_dir = TempDir::new().unwrap();
    write_non_overlapping_l0_segments(temp_dir.path(), 4);

    let gate = Arc::new(parking_lot::Mutex::new(()));
    let compactor = Compactor::new(temp_dir.path(), 8).with_gate(Arc::clone(&gate));

    // The main thread stands in for a flush/snapshot pass holding the gate.
    let held = gate.lock();

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(compactor.compact_once());
    });

    // The pass must block on the window gate, not finish the whole round.
    match rx.recv_timeout(Duration::from_millis(200)) {
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        other => panic!("compaction finished while the gate was held: {other:?}"),
    }

    drop(held);
    let finished = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("released gate must let the pass finish within 2s");
    finished.unwrap();
    worker.join().unwrap();

    assert_eq!(
        load_segments_for_level(temp_dir.path(), 1).unwrap().len(),
        1,
        "the released pass must complete its one window"
    );
}

#[test]
fn compaction_park_interval_backlogged_vs_idle() {
    let interval = Duration::from_secs(5);

    // No-progress pass: full interval even with sources above the trigger. A
    // fully-tombstoned window produces no output, so parking 1ms here would
    // reload the same window in a hot loop.
    let mut stats = CompactionRunStats {
        source_above_trigger: true,
        windows_merged: 0,
        ..CompactionRunStats::default()
    };
    assert_eq!(compaction_park_interval(&stats, interval), interval);

    // Productive pass with a backlog still above the trigger: drain back-to-back.
    stats.windows_merged = 1;
    assert_eq!(
        compaction_park_interval(&stats, interval),
        COMPACTION_BACKLOG_PARK
    );

    // Productive pass that drained the backlog, and a fully idle pass: both park
    // the configured interval.
    stats.source_above_trigger = false;
    assert_eq!(compaction_park_interval(&stats, interval), interval);
    assert_eq!(
        compaction_park_interval(&CompactionRunStats::default(), interval),
        interval
    );
}

#[test]
fn no_output_head_window_is_skipped_and_later_windows_merge() {
    let temp_dir = TempDir::new().unwrap();
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 8);

    // The head window (segments 1..=4, ts 1..=4) is fully tombstoned, so it can
    // never produce output; the later four segments are still mergeable.
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 1, end: 5 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();
    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let mut compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );
    compactor.set_pass_wall_budget(Duration::from_secs(60));

    let head_reads = Arc::new(AtomicUsize::new(0));
    let read_counter = Arc::clone(&head_reads);
    let head_root = roots[0].clone();
    let hook_root = head_root.clone();
    let _hook = set_segment_file_read_hook(Some(Arc::new(move |path: &Path| {
        if path.starts_with(&hook_root) {
            read_counter.fetch_add(1, Ordering::SeqCst);
        }
    })));

    let outcome = RoundCapture::run(&compactor).unwrap();
    let pass_reads = head_reads.load(Ordering::SeqCst);

    assert_eq!(
        outcome.stats.windows_merged, 1,
        "the pass must skip the unmergeable head window and merge the next one"
    );
    assert!(outcome.stats.compacted);
    assert!(
        !outcome
            .removed_roots()
            .iter()
            .any(|root| roots[..4].contains(root)),
        "a zero-output window is not a retirement: {:?}",
        outcome.removed_roots()
    );
    assert_eq!(
        outcome.windows.len(),
        1,
        "only the merged window may report a diff"
    );
    assert!(
        outcome.stats.source_above_trigger,
        "the skipped head window still counts toward the trigger line"
    );
    for root in &roots[..4] {
        assert!(
            root.exists(),
            "an unmergeable window's sources must stay on disk: {}",
            root.display()
        );
    }
    for root in &roots[4..] {
        assert!(
            !root.exists(),
            "the mergeable window's sources must be replaced: {}",
            root.display()
        );
    }
    let l1 = load_segments_for_level(temp_dir.path(), 1).unwrap();
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].manifest.point_count, 4);

    // Exactly one attempt: one load of the head window touches the head root
    // once per file, so the pass's notification count must equal a single load.
    head_reads.store(0, Ordering::SeqCst);
    load_segment_root_for_compaction(&head_root)
        .unwrap()
        .unwrap();
    let single_load_reads = head_reads.load(Ordering::SeqCst);
    assert!(
        single_load_reads > 0,
        "the read hook must observe the loader's notifications"
    );
    assert_eq!(
        pass_reads, single_load_reads,
        "the skipped head window must be attempted exactly once per pass \
         (observed {pass_reads} notifications vs {single_load_reads} for one load)"
    );
}

#[test]
fn all_tombstoned_window_reports_no_output_and_parks_full_interval() {
    let temp_dir = TempDir::new().unwrap();
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 4);

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 1, end: 5 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::from_secs(60));
    let outcome = RoundCapture::run(&compactor).unwrap();

    assert!(!outcome.stats.compacted);
    assert_eq!(outcome.stats.windows_merged, 0);
    assert!(
        outcome.windows.is_empty(),
        "a zero-output round must not report any diff: {:?}",
        outcome.windows
    );
    for root in &roots {
        assert!(
            root.exists(),
            "no-output sources must stay on disk: {}",
            root.display()
        );
    }
    assert!(
        outcome.stats.source_above_trigger,
        "the untouched window is still above the trigger line"
    );

    let interval = Duration::from_secs(5);
    assert_eq!(
        compaction_park_interval(&outcome.stats, interval),
        interval,
        "a zero-output pass must park the full interval, not hot-loop"
    );
}

#[test]
fn ghost_member_is_evicted_once_with_full_budget() {
    let temp_dir = TempDir::new().unwrap();
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 12);
    let ghost_root = roots[1].clone();

    // The inventory still lists the ghost until this pass's diff retires it.
    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();
    fs::remove_file(ghost_root.join("manifest.bin")).unwrap();

    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let mut compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );
    // A full budget: the working copy itself must absorb the eviction, so the
    // ghost is never selected again within this pass.
    compactor.set_pass_wall_budget(Duration::from_secs(60));

    let _guard = CountingSegmentReads::install(temp_dir.path());
    let outcome = RoundCapture::run(&compactor).unwrap();

    let removed = outcome.removed_roots();
    assert_eq!(
        removed.iter().filter(|root| **root == ghost_root).count(),
        1,
        "the vanished member must be retired exactly once: {removed:?}"
    );
    assert_eq!(
        outcome.stats.windows_merged, 3,
        "twelve L0 segments drain over three windows"
    );
    assert!(!outcome.stats.source_above_trigger);
    assert_eq!(
        count_segment_file_reads_under(&ghost_root),
        1,
        "the ghost may only be touched by the single attempt that retires it"
    );
    assert!(load_segments_for_level(temp_dir.path(), 0)
        .unwrap()
        .is_empty());
}

#[test]
fn partial_failure_preserves_published_windows_diff() {
    let temp_dir = TempDir::new().unwrap();
    // Twelve disjoint L0 segments drain as three 4-segment windows; the middle
    // window's member is corrupt, so the round publishes window one and then
    // fails.
    let roots = write_non_overlapping_l0_segments(temp_dir.path(), 12);
    // The snapshot is the persisted index: it is taken while everything is
    // healthy, so only the window load can discover the damage (the standalone
    // disk-scan fallback would validate it during planning instead).
    let inventory = scan_inventory_from_disk(temp_dir.path()).unwrap();
    let corrupt_root = roots[5].clone();
    let chunks_path = corrupt_root.join("chunks.bin");
    let mut chunks_bytes = fs::read(&chunks_path).unwrap();
    let last = chunks_bytes.len() - 1;
    chunks_bytes[last] ^= 0xff;
    fs::write(&chunks_path, chunks_bytes).unwrap();

    let source = CompactionInventorySource::default();
    source.set(Arc::new(move || inventory.clone()));
    let mut compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );
    compactor.set_pass_wall_budget(Duration::from_secs(60));

    let mut windows = Vec::new();
    let err = compactor
        .compact_round_with_changes(&mut |window| windows.push(window))
        .unwrap_err();
    assert!(
        err.to_string().contains("failed compaction validation"),
        "the corrupt member must fail the round: {err}"
    );

    // The window that was already replaced on disk must still be handed to the
    // caller: dropping it on the error is what diverges disk and persisted index.
    assert_eq!(
        windows.len(),
        1,
        "the first window's diff must survive the second window's failure"
    );
    let published = &windows[0];
    assert_eq!(
        published.removed_roots,
        roots[..4].iter().cloned().collect(),
        "the first window's sources must be reported as removed"
    );
    assert_eq!(
        published.added_roots.len(),
        1,
        "the first window's L1 replacement must be reported as added: {:?}",
        published.added_roots
    );
    for root in &published.removed_roots {
        assert!(
            !root.exists(),
            "a reported source is already gone from disk: {}",
            root.display()
        );
    }
    for root in &published.added_roots {
        assert!(
            root.exists(),
            "a reported output is already published on disk: {}",
            root.display()
        );
    }
    for root in &roots[4..] {
        assert!(
            root.exists(),
            "windows that never published must not be reported: {}",
            root.display()
        );
    }
}

#[test]
fn same_pass_intermediate_l1_roots_are_removed_after_being_added() {
    let temp_dir = TempDir::new().unwrap();
    // Four L0 windows publish four L1 outputs; the L1 trigger then consumes
    // exactly those four in the same pass. Each L1 root is therefore both an
    // added root (its L0 window) and a removed root (the L1→L2 window).
    write_non_overlapping_l0_segments(temp_dir.path(), 16);

    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::from_secs(60));
    let capture = RoundCapture::run(&compactor).unwrap();

    assert_eq!(capture.stats.windows_merged, 5);
    assert_eq!(
        capture.windows.len(),
        5,
        "one callback per merged window, in publication order"
    );
    assert_eq!(
        capture.intermediate_roots.len(),
        4,
        "the four L1 outputs of this pass must come back as removals: {:?}",
        capture.intermediate_roots
    );

    // `record_changes` adds before it removes. If it were the other way round,
    // these intermediate L1 roots would stay on the added side of the
    // accumulating diff and the persisted index would keep catalog entries for
    // segments this pass already deleted.
    let diff = capture.diff();
    let l2 = load_segments_for_level(temp_dir.path(), 2).unwrap();
    assert_eq!(l2.len(), 1, "the four L1 outputs merge into one L2 segment");
    assert_eq!(
        diff.added_roots.len(),
        1,
        "only the L2 output may remain added: {:?}",
        diff.added_roots
    );
    assert!(diff.added_roots.contains(&l2[0].root));
    for root in &capture.intermediate_roots {
        assert!(
            !diff.added_roots.contains(root),
            "an intermediate L1 root must not stay added: {}",
            root.display()
        );
        assert!(
            diff.removed_roots.contains(root),
            "an intermediate L1 root must end up removed: {}",
            root.display()
        );
    }
    assert_eq!(
        diff.removed_roots.len(),
        20,
        "16 L0 sources plus the 4 intermediate L1 roots"
    );
}

#[test]
fn window_gate_blocks_only_the_next_window() {
    let temp_dir = TempDir::new().unwrap();
    // Eight disjoint L0 segments: exactly two windows of four.
    write_non_overlapping_l0_segments(temp_dir.path(), 8);

    let gate = Arc::new(parking_lot::Mutex::new(()));
    let mut compactor = Compactor::new(temp_dir.path(), 8).with_gate(Arc::clone(&gate));
    compactor.set_pass_wall_budget(Duration::from_secs(60));

    let (window_tx, window_rx) = std::sync::mpsc::channel::<usize>();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();

    let worker = std::thread::spawn(move || {
        let resume_rx = resume_rx;
        let mut windows = 0usize;
        let result = compactor.compact_round_with_changes(&mut |_diff| {
            windows += 1;
            let _ = window_tx.send(windows);
            if windows == 1 {
                // Hold the round between windows until the main thread has
                // taken the gate: that is the only point where the test can
                // prove the gate is released between windows.
                let _ = resume_rx.recv();
            }
        });
        (result, windows)
    });

    window_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("window 1 must publish within 2s");

    // `on_window` runs after the window's guard is dropped, so the gate is free
    // while the round sits between windows. Taking it here stands in for a
    // flush/snapshot pass.
    let held = gate
        .try_lock_for(Duration::from_secs(2))
        .expect("the per-window gate must be free while the callback runs");
    resume_tx.send(()).expect("worker must still be waiting");

    match window_rx.recv_timeout(Duration::from_millis(200)) {
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        other => panic!("window 2 ran while the gate was held: {other:?}"),
    }

    drop(held);
    assert_eq!(
        window_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("the released gate must let window 2 finish"),
        2
    );

    let (result, windows) = worker.join().unwrap();
    result.unwrap();
    assert_eq!(
        windows, 2,
        "eight L0 segments drain over exactly two windows"
    );
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 1).unwrap().len(),
        2,
        "both windows must leave their L1 output behind"
    );
}

#[test]
fn l1_to_l2_no_output_window_is_skipped_and_reported() {
    let temp_dir = TempDir::new().unwrap();
    // Four L1 segments is exactly the L1 trigger, and every point is
    // tombstoned, so the L1→L2 window can never produce output. L0 is empty:
    // this pins the L1 half of RULING 2 (`skipped_l1`).
    let roots = write_l1_segments(temp_dir.path(), 4);

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;
    let mut tombstones = HashMap::new();
    tombstones.insert(series_id, vec![TombstoneRange { start: 1, end: 5 }]);
    persist_tombstones(&temp_dir.path().join(TOMBSTONES_FILE_NAME), &tombstones).unwrap();

    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::from_secs(60));
    let capture = RoundCapture::run(&compactor).unwrap();

    assert!(!capture.stats.compacted);
    assert_eq!(capture.stats.windows_merged, 0);
    assert!(
        capture.windows.is_empty(),
        "a zero-output window reports no diff: {:?}",
        capture.windows
    );
    for root in &roots {
        assert!(
            root.exists(),
            "no-output L1 sources must stay on disk: {}",
            root.display()
        );
    }
    assert!(
        capture.stats.source_above_trigger,
        "the skipped L1 window still counts toward the trigger line"
    );
    assert_eq!(
        compaction_park_interval(&capture.stats, Duration::from_secs(5)),
        Duration::from_secs(5),
        "a zero-output pass must not hot-loop"
    );
}

#[test]
fn backlog_drain_keeps_up_with_write_triggered_production() {
    let temp_dir = TempDir::new().unwrap();
    // Stand-in for ingest: every round starts 20 freshly written L0 segments on
    // top of whatever the previous round left behind. No "N segments per second"
    // assumption is made here — the question is only whether a round drains the
    // backlog that exists when it starts.
    let mut compactor = Compactor::new(temp_dir.path(), 8);
    compactor.set_pass_wall_budget(Duration::from_secs(60));

    let mut next_id = 1u64;
    let mut observed_l0 = Vec::new();
    for _round in 0..3 {
        write_non_overlapping_l0_segments_from(temp_dir.path(), next_id, 20);
        next_id += 20;

        let stats = compactor.compact_round_with_changes(&mut |_| {}).unwrap();
        let l0 = load_segments_for_level(temp_dir.path(), 0).unwrap().len();
        observed_l0.push(l0);

        assert!(
            stats.windows_merged >= 5,
            "twenty fresh L0 segments must drain over at least five windows, got {}",
            stats.windows_merged
        );
        assert!(
            !stats.source_above_trigger,
            "the round must end with nothing left above the trigger line"
        );
        assert!(
            l0 < 4,
            "the round must drain below the L0 trigger instead of falling behind \
             (round {_round} left {l0} segments)"
        );
    }

    // A round with no new ingest converges the same way: the backlog does not
    // grow monotonically across rounds.
    let stats = compactor.compact_round_with_changes(&mut |_| {}).unwrap();
    assert_eq!(
        load_segments_for_level(temp_dir.path(), 0).unwrap().len(),
        0,
        "an idle round drains the last backlog: {observed_l0:?}"
    );
    assert_eq!(stats.windows_merged, 0, "nothing is left to merge");
}

#[test]
fn published_window_diff_survives_an_unreadable_absorbed_output() {
    let temp_dir = TempDir::new().unwrap();
    // Two windows of four: both publish, so both must report a diff.
    write_non_overlapping_l0_segments(temp_dir.path(), 8);

    // An explicit id allocator, like the storage path: the standalone
    // `Compactor::new` fallback would allocate the next id with a strict
    // full-lane load, which is a different (and here unwanted) way to notice
    // the deliberately broken output.
    let mut compactor =
        Compactor::new_with_segment_id_allocator(temp_dir.path(), 8, Arc::new(AtomicU64::new(9)));
    compactor.set_pass_wall_budget(Duration::from_secs(60));

    // The only fallible in-memory step between "published on disk" and the diff
    // callback is the planning view's read of a just-written output manifest.
    // The read hook fires *before* the read, so deleting the file there makes
    // that read fail exactly where production code has to stay best-effort.
    let deletions = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&deletions);
    let lane_root = temp_dir.path().to_path_buf();
    let _hook = set_segment_file_read_hook(Some(Arc::new(move |path: &Path| {
        let is_l1_manifest = path.starts_with(&lane_root)
            && path.to_string_lossy().contains("/L1/")
            && path.ends_with("manifest.bin");
        if is_l1_manifest && counter.swap(1, Ordering::SeqCst) == 0 {
            let _ = fs::remove_file(path);
        }
    })));

    let capture = RoundCapture::run(&compactor).unwrap();

    assert_eq!(
        deletions.load(Ordering::SeqCst),
        1,
        "the fixture must have broken exactly one absorbed output"
    );
    assert_eq!(
        capture.stats.windows_merged, 2,
        "an unreadable absorbed output must not fail the round"
    );
    assert_eq!(
        capture.windows.len(),
        2,
        "every published window must deliver its diff, even when absorbing its \
         output into the planning view fails"
    );
    for (index, window) in capture.windows.iter().enumerate() {
        assert_eq!(
            window.removed_roots.len(),
            4,
            "window {index} must still report its sources as removed"
        );
        assert_eq!(
            window.added_roots.len(),
            1,
            "window {index} must still report its replacement as added"
        );
    }
    assert!(
        load_segments_for_level(temp_dir.path(), 0)
            .unwrap()
            .is_empty(),
        "both windows really were published on disk"
    );
}

/// One round's outcome, rebuilt from the per-window callback.
///
/// The round API reports each published window as it happens, so the tests can
/// see exactly what the caller would merge: the flattened roots in callback
/// order, and the round-level stats.
#[derive(Debug, Default)]
struct RoundCapture {
    stats: CompactionRunStats,
    /// Diffs in publication order. Empty means the round published nothing.
    windows: Vec<PendingPersistedSegmentDiff>,
    /// Roots an earlier window added and a later window removed: the same-pass
    /// intermediate roots (an L1 output later consumed by L1→L2).
    intermediate_roots: Vec<PathBuf>,
}

impl RoundCapture {
    /// Runs one round, merging every window the way a real caller does.
    fn run(compactor: &Compactor) -> crate::Result<Self> {
        Self::run_observing(compactor, |_| {})
    }

    /// Runs one round and observes the cumulative diff after every window.
    fn run_observing(
        compactor: &Compactor,
        mut observe_window: impl FnMut(&PendingPersistedSegmentDiff),
    ) -> crate::Result<Self> {
        let mut capture = Self::default();
        let mut pending = PendingPersistedSegmentDiff::default();
        capture.stats = compactor.compact_round_with_changes(&mut |window| {
            for root in &window.removed_roots {
                if pending.added_roots.contains(root) {
                    capture.intermediate_roots.push(root.clone());
                }
            }
            pending.merge(window.clone());
            observe_window(&window);
            capture.windows.push(window);
        })?;
        Ok(capture)
    }

    /// The round's diff as one accumulating diff would see it.
    fn diff(&self) -> PendingPersistedSegmentDiff {
        let mut merged = PendingPersistedSegmentDiff::default();
        for window in &self.windows {
            merged.merge(window.clone());
        }
        merged
    }

    fn added_roots(&self) -> Vec<PathBuf> {
        self.windows
            .iter()
            .flat_map(|window| window.added_roots.iter().cloned())
            .collect()
    }

    fn removed_roots(&self) -> Vec<PathBuf> {
        self.windows
            .iter()
            .flat_map(|window| window.removed_roots.iter().cloned())
            .collect()
    }
}

/// Writes `count` disjoint one-point L0 segments (helpers that do not need
/// overlapping ranges; the name is disjoint because `overlap=false`).
fn write_disjoint_l0_segments(root: &std::path::Path, count: u64) {
    let _ = write_l0_segment_roots(root, count, false);
}

/// Applies a round's diff to an inventory the way the persisted index does.
fn apply_diff_to_inventory(
    inventory: SegmentInventory,
    diff: &PendingPersistedSegmentDiff,
    lane: SegmentLaneFamily,
) -> SegmentInventory {
    let mut entries = inventory
        .without_roots(&diff.removed_roots)
        .entries()
        .to_vec();
    for root in &diff.added_roots {
        let (manifest, files) = read_segment_manifest_with_files(root).unwrap();
        entries.push(SegmentInventoryEntry {
            lane,
            tier: PersistedSegmentTier::Hot,
            root: root.clone(),
            manifest,
            files: Some(files),
        });
    }
    SegmentInventory::from_entries(entries)
}

/// Writes `count` L0 segments whose time ranges all overlap.
fn write_time_overlapping_l0_segments(root: &std::path::Path, count: u64) {
    let _ = write_l0_segment_roots(root, count, true);
}

/// Writes `count` L0 segments with disjoint one-point time ranges and returns
/// their roots in segment-id order.
fn write_non_overlapping_l0_segments(root: &std::path::Path, count: u64) -> Vec<PathBuf> {
    write_l0_segment_roots(root, count, false)
}

/// Writes `count` L0 segments whose ids start at `start_id` (and whose one-point
/// timestamps do too), so successive rounds can stand in for fresh ingest.
fn write_non_overlapping_l0_segments_from(
    root: &std::path::Path,
    start_id: u64,
    count: u64,
) -> Vec<PathBuf> {
    write_segment_roots(root, 0, start_id..start_id + count, false)
}

/// Writes `count` disjoint one-point L1 segments (nothing to compact from L0).
fn write_l1_segments(root: &std::path::Path, count: u64) -> Vec<PathBuf> {
    write_segment_roots(root, 1, 1..=count, false)
}

fn write_l0_segment_roots(root: &std::path::Path, count: u64, overlap: bool) -> Vec<PathBuf> {
    write_segment_roots(root, 0, 1..=count, overlap)
}

fn write_segment_roots(
    root: &std::path::Path,
    level: u8,
    segment_ids: impl Iterator<Item = u64>,
    overlap: bool,
) -> Vec<PathBuf> {
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut roots = Vec::new();
    for segment_id in segment_ids {
        let points = if overlap {
            vec![(10, segment_id as f64), (20, segment_id as f64 + 0.5)]
        } else {
            vec![(segment_id as i64, segment_id as f64)]
        };

        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);

        let writer = SegmentWriter::new(root, level, segment_id).unwrap();
        writer.write_segment(&registry, &chunks).unwrap();
        roots.push(writer.layout().root.clone());
    }

    roots
}

fn relative_roots(base: &std::path::Path, roots: &[PathBuf]) -> Vec<String> {
    let mut relative = roots
        .iter()
        .map(|root| {
            root.strip_prefix(base)
                .expect("segment root must live under the comparison base")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    relative.sort();
    relative
}

fn make_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
    let points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Numeric).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}

fn make_histogram_chunk(series_id: u64, timestamps: &[i64]) -> Chunk {
    let histogram = sample_histogram();
    let points = timestamps
        .iter()
        .map(|ts| ChunkPoint {
            ts: *ts,
            value: Value::from(histogram.clone()),
        })
        .collect::<Vec<_>>();

    let encoded = Encoder::encode_chunk_points(&points, ValueLane::Blob).unwrap();

    Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Blob,
            value_family: Some(SeriesValueFamily::Histogram),
            point_count: points.len() as u16,
            min_ts: points.first().unwrap().ts,
            max_ts: points.last().unwrap().ts,
            ts_codec: encoded.ts_codec,
            value_codec: encoded.value_codec,
        },
        points,
        encoded_payload: encoded.payload,
        wal_highwater: WalHighWatermark::default(),
    }
}
