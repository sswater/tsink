use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use super::{
    execution, finalize_pending_compaction_replacements, scan_inventory_from_disk,
    CompactionInventorySource, Compactor,
};
use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::encoder::Encoder;
use crate::engine::fs_utils::{count_segment_file_reads_under, CountingSegmentReads};
use crate::engine::segment::{
    load_segments, load_segments_for_level, SegmentWriter, WalHighWatermark,
};
use crate::engine::series::{SeriesRegistry, SeriesValueFamily};
use crate::engine::storage_engine::tiering::{PersistedSegmentTier, SegmentLaneFamily};
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
fn compactor_skips_round_until_inventory_source_is_set() {
    let temp_dir = TempDir::new().unwrap();
    write_overlapping_l0_segments(temp_dir.path(), 6);

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
fn disk_scan_inventory_fills_lane_and_tier() {
    let temp_dir = TempDir::new().unwrap();
    write_overlapping_l0_segments(temp_dir.path(), 2);

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
    write_overlapping_l0_segments(temp_dir.path(), 6);

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

    let disk_outcome = Compactor::new(disk_dir.path(), 8)
        .compact_once_with_changes()
        .unwrap();

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
    let inventory_outcome = compactor.compact_once_with_changes().unwrap();

    assert!(disk_outcome.stats.compacted);
    assert!(inventory_outcome.stats.compacted);
    assert_eq!(
        relative_roots(disk_dir.path(), &disk_outcome.source_roots),
        relative_roots(inventory_dir.path(), &inventory_outcome.source_roots),
        "same source window must be selected"
    );
    assert_eq!(
        relative_roots(disk_dir.path(), &disk_outcome.output_roots),
        relative_roots(inventory_dir.path(), &inventory_outcome.output_roots),
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

    let outcome = compactor.compact_once_with_changes().unwrap();
    assert!(outcome.stats.compacted);
    assert_eq!(outcome.stats.windows_merged, 1);
    assert!(!outcome.output_roots.is_empty());
    assert_eq!(
        outcome.stats.source_segments, 3,
        "the three surviving window members must be merged"
    );
    assert_eq!(outcome.source_roots.len(), 3);
    assert!(
        !outcome.source_roots.contains(&vanished_root),
        "a vanished member must not be reported as a source"
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

    let err = compactor.compact_once_with_changes().unwrap_err();
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
    let compactor = Compactor::new_with_inventory_source(
        temp_dir.path(),
        8,
        Arc::new(AtomicU64::new(100)),
        source,
    );

    let _guard = CountingSegmentReads::install(temp_dir.path());
    let outcome = compactor.compact_once_with_changes().unwrap();
    let reads = count_segment_file_reads_under(temp_dir.path());

    assert!(outcome.stats.compacted);
    assert_eq!(
        outcome.stats.source_segments, 4,
        "the trigger window is the first four candidates"
    );
    assert_eq!(
        reads,
        4 * 5,
        "only the four window members' five files may be read; an inventory of \
         12 must not add scans (observed {reads})"
    );
}

fn write_overlapping_l0_segments(root: &std::path::Path, count: u64) {
    let _ = write_l0_segment_roots(root, count, false);
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

fn write_l0_segment_roots(root: &std::path::Path, count: u64, overlap: bool) -> Vec<PathBuf> {
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cpu", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    let mut roots = Vec::new();
    for segment_id in 1..=count {
        let points = if overlap {
            vec![(10, segment_id as f64), (20, segment_id as f64 + 0.5)]
        } else {
            vec![(segment_id as i64, segment_id as f64)]
        };

        let mut chunks = HashMap::new();
        chunks.insert(series_id, vec![make_numeric_chunk(series_id, &points)]);

        let writer = SegmentWriter::new(root, 0, segment_id).unwrap();
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
