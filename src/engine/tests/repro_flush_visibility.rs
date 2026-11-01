//! Reproduction: after a runtime flush (not close), does select_asc still see
//! the flushed points on the SAME live instance?

use super::*;

#[test]
fn repro_asc_visible_after_runtime_flush() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);

    let storage = ChunkStorage::new_with_data_path_and_options(
        4, // chunk cap: 4 points per chunk
        None,
        Some(lane_path.clone()),
        Some(blob_lane_path),
        1,
        base_storage_test_options(TimestampPrecision::Milliseconds, None),
    )
    .unwrap();

    let now: i64 = 1_793_500_000_000;
    let labels = vec![Label::new("d", "x")];

    // 12 i64 points -> 3 sealed chunks
    let mut rows = Vec::new();
    for i in 0..12 {
        rows.push(Row::with_labels(
            "repro.flush.i64",
            labels.clone(),
            DataPoint::new(now + i, Value::I64(i)),
        ));
    }
    storage.insert_rows(&rows).unwrap();

    let before = storage
        .select_asc("repro.flush.i64", &labels, 0, i64::MAX, 1000, None)
        .unwrap();
    println!("asc before flush: {}", before.len());
    assert_eq!(before.len(), 12);

    storage.flush().unwrap();

    let after = storage
        .select_asc("repro.flush.i64", &labels, 0, i64::MAX, 1000, None)
        .unwrap();
    println!("asc after flush: {}", after.len());

    let legacy = storage
        .select("repro.flush.i64", &labels, now, now + 100)
        .unwrap();
    println!("legacy after flush: {}", legacy.len());

    let desc = storage
        .select_desc("repro.flush.i64", &labels, 0, i64::MAX, 1000, None)
        .unwrap();
    println!("desc after flush: {}", desc.len());

    assert_eq!(after.len(), 12, "select_asc must see points after runtime flush");
    assert_eq!(legacy.len(), 12);
    assert_eq!(desc.len(), 12);
    storage.close().unwrap();
}

#[test]
fn repro_asc_visibility_with_background_threads() {
    use std::sync::Arc;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            4,
            None,
            Some(lane_path.clone()),
            Some(blob_lane_path.clone()),
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Milliseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Milliseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: 32 * 1024, // small budget: force sealed-chunk eviction after persist
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: Duration::from_millis(25),
                background_threads_enabled: true,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    storage.start_background_persisted_refresh_thread().unwrap();

    let now: i64 = 1_793_500_000_000;
    let labels = vec![Label::new("d", "x")];
    let mut rows = Vec::new();
    for i in 0..24 {
        rows.push(Row::with_labels(
            "repro.bg.i64",
            labels.clone(),
            DataPoint::new(now + i, Value::I64(i)),
        ));
    }
    storage.insert_rows(&rows).unwrap();
    storage.flush().unwrap();

    // poll visibility for up to 5s while background compaction+refresh run
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = 0usize;
    while Instant::now() < deadline {
        last = storage
            .select_asc("repro.bg.i64", &labels, 0, i64::MAX, 1000, None)
            .unwrap()
            .len();
        if last != 24 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let l0 = load_segments_for_level(&lane_path, 0).unwrap().len();
    println!(
        "final asc count: {} (l0 segments now: {})",
        last, l0
    );
    assert_eq!(last, 24, "select_asc lost points while background threads ran");
    storage.close().unwrap();
}

#[test]
fn repro_asc_visible_after_flush_and_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);

    {
        let storage = ChunkStorage::new_with_data_path_and_options(
            4,
            None,
            Some(lane_path.clone()),
            Some(blob_lane_path.clone()),
            1,
            base_storage_test_options(TimestampPrecision::Milliseconds, None),
        )
        .unwrap();
        let now: i64 = 1_793_500_000_000;
        let labels = vec![Label::new("d", "x")];
        let mut rows = Vec::new();
        for i in 0..12 {
            rows.push(Row::with_labels(
                "repro.reopen.i64",
                labels.clone(),
                DataPoint::new(now + i, Value::I64(i)),
            ));
        }
        storage.insert_rows(&rows).unwrap();
        storage.flush().unwrap();
        let after = storage
            .select_asc("repro.reopen.i64", &labels, 0, i64::MAX, 1000, None)
            .unwrap();
        println!("asc after flush (same instance): {}", after.len());
        storage.close().unwrap();
    }

    let storage = ChunkStorage::new_with_data_path_and_options(
        4,
        None,
        Some(lane_path.clone()),
        Some(blob_lane_path),
        1,
        base_storage_test_options(TimestampPrecision::Milliseconds, None),
    )
    .unwrap();
    storage.load_tombstones_index().unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(lane_path.clone()).unwrap(), false)
        .unwrap();
    storage.reconcile_live_metadata_indexes().unwrap();

    let labels = vec![Label::new("d", "x")];
    let asc = storage
        .select_asc("repro.reopen.i64", &labels, 0, i64::MAX, 1000, None)
        .unwrap();
    println!("asc after reopen: {}", asc.len());
    assert_eq!(asc.len(), 12, "select_asc must see flushed points after reopen");
    storage.close().unwrap();
}
