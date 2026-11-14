use super::*;
use std::sync::mpsc;

fn new_raw_numeric_storage(lane_path: std::path::PathBuf, next_segment_id: u64) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path),
        None,
        next_segment_id,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap()
}

fn write_numeric_segment_to_path(
    lane_path: &std::path::Path,
    registry: &SeriesRegistry,
    series_id: crate::engine::series::SeriesId,
    level: u8,
    segment_id: u64,
    points: &[(i64, f64)],
) -> std::path::PathBuf {
    let mut chunks = HashMap::new();
    chunks.insert(
        series_id,
        vec![make_persisted_numeric_chunk(series_id, points)],
    );
    let writer = SegmentWriter::new(lane_path, level, segment_id).unwrap();
    writer.write_segment(registry, &chunks).unwrap();
    writer.layout().root.clone()
}

fn incremental_segment_paths(snapshot_path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let dir_path = SeriesRegistry::incremental_dir(snapshot_path);
    let mut paths = match std::fs::read_dir(&dir_path) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                file_type.is_file().then_some(entry.path())
            })
            .collect::<Vec<_>>(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("failed to list incremental registry dir: {err}"),
    };
    paths.sort();
    paths
}

#[test]
fn background_compaction_reduces_l0_segments_while_storage_is_open() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("background_compaction", &labels)
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        5,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
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
    .unwrap();

    // `new_with_data_path_and_options` deliberately skips startup recovery
    // (public callers must use `StorageBuilder::build`, which discovers and
    // hydrates the inventory). Compaction now plans from that inventory, so
    // simulate the discovery a real startup would have done over the four
    // pre-existing segments.
    storage
        .persisted
        .persisted_index_dirty
        .store(true, std::sync::atomic::Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut compacted = false;

    while Instant::now() < deadline {
        let l0 = load_segments_for_level(&lane_path, 0).unwrap();
        let l1 = load_segments_for_level(&lane_path, 1).unwrap();
        if l0.len() < 4 && !l1.is_empty() {
            compacted = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    assert!(compacted, "background thread did not compact L0 into L1");
    storage.close().unwrap();
}

#[test]
fn background_compaction_refreshes_persisted_index_in_background() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("background_compaction_sync", &labels)
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            5,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Nanoseconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
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
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();
    storage.start_background_persisted_refresh_thread().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut compacted = false;
    while Instant::now() < deadline {
        let l0 = load_segments_for_level(&lane_path, 0).unwrap();
        let l1 = load_segments_for_level(&lane_path, 1).unwrap();
        if l0.len() < 4 && !l1.is_empty() {
            compacted = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        compacted,
        "background compaction did not produce compacted output"
    );
    storage.notify_persisted_refresh_thread();

    let refresh_deadline = Instant::now() + Duration::from_secs(2);
    let mut refreshed = false;
    while Instant::now() < refresh_deadline {
        let persisted_levels = storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .unwrap()
            .iter()
            .map(|chunk| chunk.level)
            .collect::<Vec<_>>();
        if !storage
            .persisted
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
            && persisted_levels.iter().all(|level| *level == 1)
        {
            refreshed = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        refreshed,
        "background refresh worker did not reconcile the compacted persisted index"
    );

    let points = storage
        .select("background_compaction_sync", &labels, 0, 10)
        .unwrap();
    assert_eq!(points.len(), 4);

    let persisted_levels = storage
        .persisted
        .persisted_index
        .read()
        .chunk_refs
        .get(&series_id)
        .unwrap()
        .iter()
        .map(|chunk| chunk.level)
        .collect::<Vec<_>>();
    assert!(
        persisted_levels.iter().all(|level| *level == 1),
        "persisted index should point at compacted L1 output after background refresh"
    );

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_reconciles_known_dirty_compaction_changes_before_checkpointing() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("flush_known_dirty_compaction", &labels)
        .unwrap()
        .series_id;

    for segment_id in 1..=4 {
        let mut chunks = HashMap::new();
        chunks.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.clone()),
        None,
        5,
        ChunkStorageOptions {
            retention_enforced: false,
            compaction_interval: Duration::from_millis(25),
            background_threads_enabled: true,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut compacted = false;
    while Instant::now() < deadline {
        let l1 = load_segments_for_level(&lane_path, 1).unwrap();
        if storage
            .persisted
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::SeqCst)
            && !l1.is_empty()
        {
            compacted = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        compacted,
        "background compaction did not leave dirty persisted changes for flush reconciliation"
    );

    storage
        .insert_rows(&[Row::with_labels(
            "flush_known_dirty_compaction",
            labels.clone(),
            DataPoint::new(10, 10.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .select("flush_known_dirty_compaction", &labels, 0, 20)
            .unwrap()
            .len(),
        5
    );

    storage.close().unwrap();
}

#[test]
fn background_flush_pipeline_refreshes_persisted_index_and_evicts_sealed_chunks_while_open() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    storage
        .start_background_flush_thread(Duration::from_millis(25))
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels("background_flush", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("background_flush", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("background_flush", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush", &labels)
        .unwrap()
        .series_id;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut flushed = false;
    while Instant::now() < deadline {
        let active_len = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count());
        let sealed_len = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let persisted_len = storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let l0 = load_segments_for_level(&lane_path, 0).unwrap();

        if active_len == 0 && sealed_len == 0 && persisted_len >= 2 && !l0.is_empty() {
            flushed = true;
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    assert!(
            flushed,
            "background flush pipeline did not refresh persisted indexes and evict flushed sealed chunks"
        );
    assert_eq!(
        storage
            .select("background_flush", &labels, 0, 10)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should seal the remaining current head once it becomes the freshness bottleneck",
    );

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_waits_for_inflight_tombstone_visibility_publication() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(temp_dir.path().join(NUMERIC_LANE_ROOT)),
            None,
            1,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Milliseconds,
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_delete_visibility_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "flush_delete_visibility_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "flush_delete_visibility_metric",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let (flush_published_tx, flush_published_rx) = mpsc::channel();
    storage.set_persist_post_publish_hook(move |_| {
        let _ = flush_published_tx.send(());
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage
            .delete_series(&SeriesSelection::new().with_metric("flush_delete_visibility_metric"))
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        let result = flush_storage.flush_pipeline_once();
        flush_tx.send(result).unwrap();
    });

    flush_published_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not reach the segment publication hook");
    let flush_blocked = flush_rx.recv_timeout(Duration::from_millis(200)).is_err();

    delete_release_tx.send(()).unwrap();
    assert!(
        flush_blocked,
        "flush should wait for the in-flight tombstone visibility publication",
    );

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert!(flush_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    flush_thread.join().unwrap();

    assert!(storage
        .select("flush_delete_visibility_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());

    storage.clear_persist_post_publish_hook();
    storage.clear_tombstone_post_swap_pre_visibility_hook();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_adopts_new_segments_without_full_inventory_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let historical_series_id = registry
        .resolve_or_insert("historical_flush_inventory", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=64 {
        let mut chunks = HashMap::new();
        chunks.insert(
            historical_series_id,
            vec![make_persisted_numeric_chunk(
                historical_series_id,
                &[(segment_id as i64, segment_id as f64)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, segment_id)
            .unwrap()
            .write_segment(&registry, &chunks)
            .unwrap();
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            65,
            ChunkStorageOptions {
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });
    storage
        .start_background_flush_thread(Duration::from_millis(25))
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "background_flush_incremental",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "background_flush_incremental",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "background_flush_incremental",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_incremental", &labels)
        .unwrap()
        .series_id;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut flushed = false;
    while Instant::now() < deadline {
        let active_len = storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count());
        let sealed_len = storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());
        let persisted_len = storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len());

        if active_len == 0 && sealed_len == 0 && persisted_len >= 2 {
            flushed = true;
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    assert!(
        flushed,
        "background flush did not persist the incremental series"
    );
    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "steady-state flush should not trigger a full persisted inventory scan",
    );
    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should seal the last current head once it is the only unpublished data",
    );

    storage.clear_full_inventory_scan_hook();
    drop(storage);
}

#[test]
fn background_flush_persists_current_partial_head_and_resets_wal() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_wal_guard",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_wal_guard",
            labels.clone(),
            DataPoint::new(2, 2.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_wal_guard",
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_wal_guard", &labels)
        .unwrap()
        .series_id;
    let wal = storage.persisted.wal.as_ref().unwrap();
    assert!(
        wal.current_highwater() > WalHighWatermark::default(),
        "writes should advance the WAL highwater before any flush path runs",
    );

    storage.background_flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should seal the current partial head once it is the last unpublished data",
    );
    assert_eq!(
        storage
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        0,
        "background flush should evict the sealed snapshot it just persisted",
    );
    assert_eq!(
        storage
            .persisted.persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        2,
        "background flush should persist both the previously sealed chunk and the current partial head",
    );
    assert_eq!(
        wal.total_size_bytes().unwrap(),
        0,
        "background flush should reset the WAL once it durably publishes the current head",
    );

    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "explicit flush should still drain the current head",
    );
    assert_eq!(
        storage
            .persisted.persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .map_or(0, |chunks| chunks.len()),
        2,
        "explicit flush should be a no-op once background flush has already published the current head",
    );
    assert_eq!(
        wal.total_size_bytes().unwrap(),
        0,
        "WAL should stay reset once the current head has already been durably published",
    );
    assert_eq!(
        storage
            .select("background_flush_wal_guard", &labels, 0, 10)
            .unwrap()
            .len(),
        3
    );

    storage.close().unwrap();
}

#[test]
fn background_flush_persists_sealed_and_live_series_together() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let target_labels = vec![Label::new("host", "sealed")];
    let blocker_labels = vec![Label::new("host", "live")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        Some(wal),
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "background_flush_live_blocker",
            blocker_labels.clone(),
            DataPoint::new(1, 10.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[
            Row::with_labels(
                "background_flush_sealed_target",
                target_labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "background_flush_sealed_target",
                target_labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();

    let blocker_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_live_blocker", &blocker_labels)
        .unwrap()
        .series_id;
    let target_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("background_flush_sealed_target", &target_labels)
        .unwrap()
        .series_id;

    storage.background_flush_pipeline_once().unwrap();

    assert_eq!(
        storage
            .active_shard(blocker_series_id)
            .read()
            .get(&blocker_series_id)
            .map_or(0, |state| state.point_count()),
        0,
        "background flush should also publish the unrelated live head once it is the only unpublished data for that series",
    );
    assert_eq!(
        storage
            .sealed_shard(target_series_id)
            .read()
            .get(&target_series_id)
            .map_or(0, |chunks| chunks.len()),
        0,
        "background flush should evict the sealed chunk once it is published",
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&target_series_id)
            .map_or(0, |chunks| chunks.len()),
        1,
        "background flush should publish the unrelated sealed series",
    );
    assert_eq!(
        storage
            .persisted
            .persisted_index
            .read()
            .chunk_refs
            .get(&blocker_series_id)
            .map_or(0, |chunks| chunks.len()),
        1,
        "background flush should publish the live blocker series once it seals the current head",
    );
    assert_eq!(
        load_segments_for_level(&lane_path, 0).unwrap().len(),
        1,
        "background flush should write a persisted segment without waiting for close",
    );
    assert_eq!(
        storage.persisted.wal.as_ref().unwrap().total_size_bytes().unwrap(),
        0,
        "background flush should reset the WAL once both the sealed series and the live head are durably published",
    );
    assert_eq!(
        storage
            .select("background_flush_sealed_target", &target_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
    );
    assert_eq!(
        storage
            .select("background_flush_live_blocker", &blocker_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 10.0)],
    );

    drop(storage);

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_retention_enforced(false)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened
            .select("background_flush_sealed_target", &target_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)],
        "reopen should recover the background-persisted sealed series without losing data",
    );
    assert_eq!(
        reopened
            .select("background_flush_live_blocker", &blocker_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 10.0)],
        "reopen should recover the live blocker from the background-persisted segment",
    );

    reopened.close().unwrap();
}

#[test]
fn background_flush_publishes_current_heads_to_compute_only_readers() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let metric = "background_flush_compute_only_visibility";

    let writer = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(data_dir.path().join(NUMERIC_LANE_ROOT)),
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(super::super::config::TieredStorageConfig {
                object_store_root: object_store_dir.path().to_path_buf(),
                segment_catalog_path: None,
                mirror_hot_segments: true,
                hot_retention_window: i64::MAX,
                warm_retention_window: i64::MAX,
            }),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    let reader = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        None,
        None,
        1,
        ChunkStorageOptions {
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ComputeOnly,
            background_threads_enabled: false,
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(super::super::config::TieredStorageConfig {
                object_store_root: object_store_dir.path().to_path_buf(),
                segment_catalog_path: None,
                mirror_hot_segments: false,
                hot_retention_window: i64::MAX,
                warm_retention_window: i64::MAX,
            }),
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();

    writer
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    assert!(
        reader.select(metric, &labels, 0, 10).unwrap().is_empty(),
        "compute-only readers should not see the current head before it is background-published",
    );

    writer.background_flush_pipeline_once().unwrap();

    assert_eq!(
        load_segments_for_level(
            object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT),
            0
        )
        .unwrap()
        .len(),
        1,
        "background flush should mirror the sealed current head into the object-store hot tier",
    );

    std::thread::sleep(Duration::from_millis(5));
    reader.sync_persisted_segments_from_disk_if_dirty().unwrap();

    assert_eq!(
        reader.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0)],
        "compute-only readers should observe current-head writes after the bounded background flush and refresh interval",
    );

    reader.close().unwrap();
    writer.close().unwrap();
}

#[test]
fn flush_pipeline_persists_large_sealed_snapshot_without_chunk_clones() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = new_raw_numeric_storage(lane_path.clone(), 1);
    let metric = "flush_snapshot_no_clone";
    let series_count = 64usize;
    let points_per_series = 16usize;
    let total_points = series_count * points_per_series;
    let total_chunks = total_points / 2;
    let sample_labels = vec![Label::new("host", "series-0000")];

    let rows = (0..series_count)
        .flat_map(|series_idx| {
            let labels = vec![Label::new("host", format!("series-{series_idx:04}"))];
            (0..points_per_series).map(move |point_idx| {
                Row::with_labels(
                    metric,
                    labels.clone(),
                    DataPoint::new(1_000 + point_idx as i64, point_idx as f64),
                )
            })
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    crate::engine::chunk::reset_chunk_clone_count();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        crate::engine::chunk::chunk_clone_count(),
        0,
        "flush should persist sealed chunks through shared references"
    );

    let l0 = load_segments_for_level(&lane_path, 0).unwrap();
    assert_eq!(l0.len(), 1);
    assert_eq!(l0[0].manifest.chunk_count, total_chunks);
    assert_eq!(l0[0].manifest.point_count, total_points);

    let selected = storage.select(metric, &sample_labels, 0, 10_000).unwrap();
    assert_eq!(selected.len(), points_per_series);

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_defers_unknown_dirty_reconcile_to_background_refresh() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_dirty_reconcile",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "flush_dirty_reconcile",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.flush_pipeline_once().unwrap();

    assert!(
        full_scans.load(Ordering::SeqCst) == 0,
        "dirty flush should not block on a full persisted inventory scan",
    );
    assert!(
        storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "flush should leave unknown dirty reconcile work pending for the background worker",
    );
    assert_eq!(
        storage
            .select("flush_dirty_reconcile", &labels, 0, 10)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        storage.list_metrics().unwrap(),
        vec![MetricSeries {
            name: "flush_dirty_reconcile".to_string(),
            labels: labels.clone(),
        }]
    );
    assert_eq!(
        storage
            .select_series(&SeriesSelection::new().with_metric("flush_dirty_reconcile"))
            .unwrap(),
        vec![MetricSeries {
            name: "flush_dirty_reconcile".to_string(),
            labels: labels.clone(),
        }]
    );
    assert!(
        full_scans.load(Ordering::SeqCst) == 0,
        "foreground reads should keep using the last published catalog without running a full scan",
    );

    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut refreshed = false;
    while Instant::now() < deadline {
        if full_scans.load(Ordering::SeqCst) > 0
            && !storage
                .persisted
                .persisted_index_dirty
                .load(Ordering::SeqCst)
        {
            refreshed = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        refreshed,
        "background refresh worker did not run the deferred full persisted inventory scan",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_full_refresh_keeps_concurrent_queries_running() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("dirty_refresh_query_metric", &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        0,
        1,
        &[(1, 1.0), (2, 2.0)],
    );

    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 2));
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let scan_started = Arc::new(AtomicBool::new(false));
    let release_scan = Arc::new(AtomicBool::new(false));
    let scan_count = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let scan_started = Arc::clone(&scan_started);
        let release_scan = Arc::clone(&release_scan);
        let scan_count = Arc::clone(&scan_count);
        move || {
            scan_count.fetch_add(1, Ordering::SeqCst);
            scan_started.store(true, Ordering::SeqCst);
            while !release_scan.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !scan_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        scan_started.load(Ordering::SeqCst),
        "background dirty refresh did not reach the full inventory scan hook",
    );

    let concurrent_storage = Arc::clone(&storage);
    let concurrent_labels = labels.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let concurrent_query = thread::spawn(move || {
        let result =
            concurrent_storage.select("dirty_refresh_query_metric", &concurrent_labels, 0, 10);
        query_tx.send(result).unwrap();
    });

    let concurrent_points = query_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("concurrent query should keep using the last visible catalog");
    assert_eq!(
        concurrent_points.unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    release_scan.store(true, Ordering::SeqCst);

    concurrent_query.join().unwrap();
    let refresh_deadline = Instant::now() + Duration::from_secs(2);
    while storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst)
        && Instant::now() < refresh_deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        scan_count.load(Ordering::SeqCst),
        1,
        "only the refresh owner should run the full inventory scan",
    );
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "successful dirty refresh should clear the dirty flag",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_known_diff_refresh_skips_full_inventory_scans_at_large_segment_counts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("dirty_known_diff_refresh_metric", &labels)
        .unwrap()
        .series_id;

    let mut removed_root = None;
    for segment_id in 1..=256 {
        let root = write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
        if segment_id == 1 {
            removed_root = Some(root);
        }
    }

    let storage = new_raw_numeric_storage(lane_path.clone(), 257);
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let removed_root = removed_root.expect("expected the first root to exist");
    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&removed_root).unwrap();
    let added_root = write_numeric_segment_to_path(
        &lane_path,
        &registry,
        series_id,
        0,
        257,
        &[(10_000, 10_000.0)],
    );

    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(
            std::iter::once(added_root.clone()),
            std::iter::once(removed_root.clone()),
        );
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "known add/remove refresh should not rescan the full segment tree",
    );
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "known dirty refresh should settle the dirty flag",
    );
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "known dirty refresh should drain the pending root diff",
    );
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&added_root));
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&removed_root));
    let points = storage
        .select("dirty_known_diff_refresh_metric", &labels, 0, 20_000)
        .unwrap();
    assert_eq!(points.len(), 256);
    assert!(!points.contains(&DataPoint::new(1, 1.0)));
    assert!(points.contains(&DataPoint::new(10_000, 10_000.0)));

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn stranded_known_diff_is_refreshed_without_dirty_flag() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("stranded_known_diff_refresh_metric", &labels)
        .unwrap()
        .series_id;

    write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 1, &[(1, 1.0)]);

    let storage = new_raw_numeric_storage(lane_path.clone(), 2);
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let added_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 2, &[(2, 2.0)]);

    // Reproduce the stranded state left behind when a producer merges a diff after
    // the refresh path has already claimed the known diff: the diff is pending but
    // the dirty flag is clear.
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::once(added_root.clone()), std::iter::empty());
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "the stranded state must have the dirty flag clear",
    );
    assert!(
        storage.has_known_persisted_segment_changes(),
        "the stranded state must carry a known pending diff",
    );

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert!(
        storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(&added_root),
        "a known diff pending without the dirty flag must still be applied",
    );
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "the stranded refresh should drain the pending root diff",
    );

    storage.close().unwrap();
}

#[test]
fn runtime_refresh_disappearance_is_transient_and_does_not_fence_writes() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "runtime_refresh_transient_metric";

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 1, &[(1, 1.0)]);

    // Fail-fast is on: `ChunkStorageOptions::default()` carries
    // `background_fail_fast: true`, exactly like `StorageBuilder`'s default.
    let storage = new_raw_numeric_storage(lane_path.clone(), 2);
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    // A write-triggered flush publishes a tiny L0 segment and records it as a
    // pending added root, arming the dirty flag, without applying it yet.
    let published_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 2, &[(2, 2.0)]);
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::once(published_root.clone()), std::iter::empty());
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    // A back-to-back compaction pass merges that micro-segment away before the
    // refresh worker loads it: the root is already gone from disk.
    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&published_root).unwrap();

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    // The vanished added root is retired, not failed and not restored: the
    // refresh converges even when the compaction's own removal has not landed
    // in the pending diff yet.
    let health = storage.observability_snapshot().health;
    assert!(
        health.fail_fast_enabled,
        "the fixture must run fail-fast on"
    );
    assert!(
        !health.fail_fast_triggered,
        "a transient disappearance must not fence new work"
    );
    assert_eq!(
        health.background_errors_total, 0,
        "a transient disappearance must not be counted as a background failure"
    );
    assert_eq!(
        health.transient_disappearances_total, 1,
        "the skip must be visible on the health gauge"
    );
    assert!(
        health.last_background_error.is_none(),
        "a transient disappearance must not be reported as the last background error"
    );
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "the skipped root is retired, not restored into the pending diff"
    );
    assert!(
        !storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(&published_root),
        "the deleted root must never enter the persisted index"
    );

    // Compaction's own removal can also land after the skip; the retried pass
    // applies it and settles.
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::empty(), std::iter::once(published_root.clone()));
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "the retried pass should drain the convergent merged diff"
    );
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "the retried pass should settle the dirty flag"
    );

    // Writes are not fenced: the transient race never poisoned the storage.
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap();
    let points = storage.select(metric, &labels, 0, 10).unwrap();
    assert!(
        points.contains(&DataPoint::new(3, 3.0)),
        "a write after the transient disappearance must succeed and be readable"
    );

    storage.close().unwrap();
}

#[test]
fn vanished_added_root_removal_landing_after_drain_is_not_resurrected() {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "reverse_order_diff_resurrection_metric";

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;

    write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 1, &[(1, 1.0)]);

    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 2));
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    // A flush publishes R and records it as pending added.
    let published_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 2, &[(2, 2.0)]);
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::once(published_root.clone()), std::iter::empty());
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    // Compaction removes R from disk before the refresh can load it.
    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&published_root).unwrap();

    // The reverse of the existing transient test's ordering: the removal lands
    // in pending AFTER the refresh drained the +R diff. On the unfixed code the
    // failed load restores (+R) and record_changes' add-before-remove ordering
    // cancels the pending -R, resurrecting a root that no longer exists.
    {
        let weak = Arc::downgrade(&storage);
        let removed_root = published_root.clone();
        storage.set_known_dirty_load_hook(move || {
            if let Some(storage) = weak.upgrade() {
                storage
                    .persisted
                    .pending_persisted_segment_diff
                    .lock()
                    .record_changes(std::iter::empty(), std::iter::once(removed_root.clone()));
            }
        });
    }

    let apply = storage.sync_persisted_segments_from_disk_if_dirty();
    storage.clear_known_dirty_load_hook();
    assert!(
        apply.is_ok(),
        "a vanished added root must not fail the pass: {apply:?}"
    );

    {
        let pending = storage.persisted.pending_persisted_segment_diff.lock();
        assert!(
            !pending.added_roots.contains(&published_root),
            "the vanished root must not be resurrected onto the added side: {:?}",
            pending.added_roots
        );
        assert!(
            pending.removed_roots.contains(&published_root),
            "the compacted removal must survive on the removed side: {:?}",
            pending.removed_roots
        );
    }

    let health = storage.observability_snapshot().health;
    assert!(
        !health.fail_fast_triggered,
        "a vanished added root is a transient retirement, not a fenced failure"
    );
    assert_eq!(
        health.background_errors_total, 0,
        "the vanished root must not be counted as a background failure"
    );

    // The next pass applies the removal and settles.
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(
        !storage.has_known_persisted_segment_changes(),
        "the retried pass must drain the pending removal"
    );
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "the retried pass must settle the dirty flag"
    );
    assert!(
        !storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(&published_root),
        "the deleted root must never enter the persisted index"
    );

    // Service is not fenced.
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(3, 3.0),
        )])
        .unwrap();
    let points = storage.select(metric, &labels, 0, 10).unwrap();
    assert!(
        points.contains(&DataPoint::new(3, 3.0)),
        "a write after the reverse-order race must succeed and be readable"
    );

    storage.close().unwrap();
}

#[test]
fn non_transient_runtime_refresh_errors_still_fence_writes_under_fail_fast() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = new_raw_numeric_storage(lane_path, 1);

    // Control: a real validation failure keeps the previous behavior — the
    // worker stops and fail-fast fences new work.
    let worker_would_exit = storage.handle_background_worker_result_for_tests(
        "persisted_refresh",
        Err(TsinkError::DataCorruption(
            "persisted segment failed runtime refresh validation: manifest crc32 mismatch"
                .to_string(),
        )),
    );
    assert!(
        worker_would_exit,
        "non-transient refresh errors must still stop the worker under fail-fast"
    );
    let health = storage.observability_snapshot().health;
    assert!(health.fail_fast_triggered);
    assert_eq!(health.background_errors_total, 1);

    let err = storage
        .insert_rows(&[Row::new(
            "runtime_refresh_corruption_metric",
            DataPoint::new(1, 1.0),
        )])
        .unwrap_err();
    assert!(matches!(err, TsinkError::StorageShuttingDown));

    storage.close().unwrap();
}

#[test]
fn runtime_refresh_load_of_mid_removal_segment_is_transient() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("runtime_refresh_mid_removal_metric", &labels)
        .unwrap()
        .series_id;

    // `remove_dir_all` unlinks a retiring segment's files before its root, so a
    // concurrent reader can see the root with a required file already gone.
    let mid_removal_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 1, &[(1, 1.0)]);
    std::fs::remove_file(mid_removal_root.join("chunks.bin")).unwrap();

    let err = match ChunkStorage::load_segment_index_for_runtime_refresh(&mid_removal_root) {
        Ok(_) => panic!("expected the mid-removal load to fail"),
        Err(err) => err,
    };
    assert!(
        crate::engine::segment::is_runtime_refresh_disappearance_error(&err),
        "a root whose required file is already unlinked must be reported as the \
         transient disappearance race, got: {err}"
    );

    // A genuinely unreadable segment must keep surfacing as validation failure.
    let corrupt_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 2, &[(2, 2.0)]);
    std::fs::write(corrupt_root.join("manifest.bin"), b"not a manifest").unwrap();

    let err = match ChunkStorage::load_segment_index_for_runtime_refresh(&corrupt_root) {
        Ok(_) => panic!("expected the corrupt manifest load to fail"),
        Err(err) => err,
    };
    assert!(
        !crate::engine::segment::is_runtime_refresh_disappearance_error(&err),
        "real corruption must not be classified as a transient disappearance: {err}"
    );
    assert!(
        matches!(
            &err,
            TsinkError::DataCorruption(message)
                if message.contains("failed runtime refresh validation")
        ),
        "real corruption must still surface as a runtime refresh validation error, got: {err}"
    );
}

#[test]
fn flush_publication_excludes_compaction_until_verification() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let storage = Arc::new(new_raw_numeric_storage(lane_path, 1));

    // The post-publish hook runs after the flush wrote and published its
    // segment roots, i.e. inside the publication→verification window.
    let gate_free_during_publish = Arc::new(AtomicBool::new(true));
    let weak = Arc::downgrade(&storage);
    storage.set_persist_post_publish_hook({
        let gate_free_during_publish = Arc::clone(&gate_free_during_publish);
        move |_roots| {
            if let Some(storage) = weak.upgrade() {
                gate_free_during_publish.store(
                    storage.compaction_gate_is_free_for_tests(),
                    Ordering::SeqCst,
                );
            }
        }
    });

    storage
        .insert_rows(&[Row::new(
            "flush_compaction_gate_metric",
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert!(
        !gate_free_during_publish.load(Ordering::SeqCst),
        "flush publication must exclude compaction until verification: a back-to-back \
         compaction pass could otherwise retire the just-published segment before the \
         flush verified it"
    );

    storage.clear_persist_post_publish_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_persisted_refresh_waits_for_inflight_tombstone_visibility_publication() {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 1));

    storage
        .insert_rows(&[
            Row::with_labels(
                "dirty_refresh_delete_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "dirty_refresh_delete_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let (delete_entered_tx, delete_entered_rx) = mpsc::channel();
    let (delete_release_tx, delete_release_rx) = mpsc::channel();
    let delete_release_rx = Arc::new(Mutex::new(delete_release_rx));
    storage.set_tombstone_post_swap_pre_visibility_hook(move || {
        delete_entered_tx.send(()).unwrap();
        delete_release_rx.lock().unwrap().recv().unwrap();
    });

    let (scan_started_tx, scan_started_rx) = mpsc::channel();
    storage.set_full_inventory_scan_hook(move || {
        let _ = scan_started_tx.send(());
    });

    let delete_storage = Arc::clone(&storage);
    let delete_thread = thread::spawn(move || {
        delete_storage
            .delete_series(&SeriesSelection::new().with_metric("dirty_refresh_delete_metric"))
    });

    delete_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("delete did not reach the tombstone publication hook");

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let refresh_storage = Arc::clone(&storage);
    let (refresh_tx, refresh_rx) = mpsc::channel();
    let refresh_thread = thread::spawn(move || {
        let result = refresh_storage.sync_persisted_segments_from_disk_if_dirty();
        refresh_tx.send(result).unwrap();
    });

    scan_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("refresh did not reach the full inventory scan hook");
    let refresh_blocked = refresh_rx.recv_timeout(Duration::from_millis(200)).is_err();

    delete_release_tx.send(()).unwrap();
    assert!(
        refresh_blocked,
        "dirty refresh should wait for the in-flight tombstone visibility publication",
    );

    let delete_result = delete_thread.join().unwrap().unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);
    assert!(refresh_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    refresh_thread.join().unwrap();

    if storage
        .persisted
        .persisted_index_dirty
        .load(Ordering::SeqCst)
    {
        storage
            .sync_persisted_segments_from_disk_if_dirty()
            .unwrap();
    }

    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a follow-up refresh should reconcile any invalidated scan after delete publication",
    );
    assert!(storage
        .select("dirty_refresh_delete_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());

    storage.clear_full_inventory_scan_hook();
    storage.clear_tombstone_post_swap_pre_visibility_hook();
    storage.close().unwrap();
}

#[test]
fn dirty_persisted_refresh_skips_stale_scanned_state_after_delete_visibility_change() {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(new_raw_numeric_storage(lane_path.clone(), 1));

    storage
        .insert_rows(&[
            Row::with_labels(
                "dirty_refresh_stale_visibility_metric",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "dirty_refresh_stale_visibility_metric",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let (scan_entered_tx, scan_entered_rx) = mpsc::channel();
    let (release_scan_tx, release_scan_rx) = mpsc::channel();
    let release_scan_rx = Arc::new(Mutex::new(release_scan_rx));
    storage.set_full_inventory_scan_hook({
        let release_scan_rx = Arc::clone(&release_scan_rx);
        move || {
            scan_entered_tx.send(()).unwrap();
            release_scan_rx.lock().unwrap().recv().unwrap();
        }
    });

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    let refresh_storage = Arc::clone(&storage);
    let (refresh_tx, refresh_rx) = mpsc::channel();
    let refresh_thread = thread::spawn(move || {
        let result = refresh_storage.sync_persisted_segments_from_disk_if_dirty();
        refresh_tx.send(result).unwrap();
    });

    scan_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("refresh did not reach the full inventory scan hook");

    let delete_result = storage
        .delete_series(&SeriesSelection::new().with_metric("dirty_refresh_stale_visibility_metric"))
        .unwrap();
    assert_eq!(delete_result.matched_series, 1);
    assert_eq!(delete_result.tombstones_applied, 1);

    release_scan_tx.send(()).unwrap();
    assert!(refresh_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    refresh_thread.join().unwrap();

    assert!(
        storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a scanned refresh prepared before a newer visibility publication should stay dirty for retry",
    );
    assert!(
        storage
            .select("dirty_refresh_stale_visibility_metric", &labels, 0, 10)
            .unwrap()
            .is_empty(),
        "stale scanned refresh state must not resurrect data hidden by a newer tombstone publication",
    );

    storage.clear_full_inventory_scan_hook();
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(
        !storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a follow-up refresh should clear the dirty flag once it snapshots the newer visibility generation",
    );
    assert!(storage
        .select("dirty_refresh_stale_visibility_metric", &labels, 0, 10)
        .unwrap()
        .is_empty());

    storage.close().unwrap();
}

#[test]
fn remote_catalog_refresh_keeps_concurrent_queries_running() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("remote_refresh_query_metric", &labels)
        .unwrap()
        .series_id;
    let segment_root =
        write_numeric_segment_to_path(&hot_lane, &registry, series_id, 0, 1, &[(1, 1.0), (2, 2.0)]);

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            None,
            None,
            2,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: i64::MAX,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: false,
                runtime_mode: StorageRuntimeMode::ComputeOnly,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_millis(1),
                tiered_storage: Some(super::super::config::TieredStorageConfig {
                    object_store_root: object_store_dir.path().to_path_buf(),
                    segment_catalog_path: None,
                    mirror_hot_segments: false,
                    hot_retention_window: 10,
                    warm_retention_window: 50,
                }),
                #[cfg(test)]
                current_time_override: None,
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(
            crate::engine::segment::load_segment_indexes_from_dirs_with_series(
                vec![segment_root],
                true,
            )
            .unwrap(),
            false,
        )
        .unwrap();
    storage.mark_remote_catalog_refresh_success();

    thread::sleep(Duration::from_millis(5));

    let scan_started = Arc::new(AtomicBool::new(false));
    let release_scan = Arc::new(AtomicBool::new(false));
    let scan_count = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let scan_started = Arc::clone(&scan_started);
        let release_scan = Arc::clone(&release_scan);
        let scan_count = Arc::clone(&scan_count);
        move || {
            scan_count.fetch_add(1, Ordering::SeqCst);
            scan_started.store(true, Ordering::SeqCst);
            while !release_scan.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !scan_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        scan_started.load(Ordering::SeqCst),
        "background remote refresh did not reach the full inventory scan hook",
    );

    let concurrent_storage = Arc::clone(&storage);
    let concurrent_labels = labels.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let concurrent_query = thread::spawn(move || {
        let result =
            concurrent_storage.select("remote_refresh_query_metric", &concurrent_labels, 0, 10);
        query_tx.send(result).unwrap();
    });

    let concurrent_points = query_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("concurrent query should not block on remote inventory refresh");
    assert_eq!(
        concurrent_points.unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    release_scan.store(true, Ordering::SeqCst);

    concurrent_query.join().unwrap();
    assert_eq!(
        scan_count.load(Ordering::SeqCst),
        1,
        "only one remote refresh should scan the object-store inventory",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn remote_catalog_refresh_uses_shared_catalog_without_full_inventory_scans() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = object_store_dir.path().join("hot").join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("remote_catalog_incremental_metric", &labels)
        .unwrap()
        .series_id;

    let mut initial_roots = Vec::new();
    for segment_id in 1..=256 {
        initial_roots.push(write_numeric_segment_to_path(
            &hot_lane,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        ));
    }

    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let shared_catalog_path = super::super::tiering::shared_segment_catalog_path(&tiered_storage);
    let initial_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_segment_catalog(&shared_catalog_path, &initial_inventory)
        .unwrap();

    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        None,
        None,
        257,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ComputeOnly,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_millis(1),
            tiered_storage: Some(tiered_storage.clone()),
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(
            crate::engine::segment::load_segment_indexes_from_dirs_with_series(
                initial_roots.clone(),
                true,
            )
            .unwrap(),
            false,
        )
        .unwrap();
    storage.mark_remote_catalog_refresh_success();
    std::thread::sleep(Duration::from_millis(5));

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let removed_root = initial_roots[0].clone();
    crate::engine::fs_utils::remove_path_if_exists_and_sync_parent(&removed_root).unwrap();
    let added_root = write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        257,
        &[(10_000, 10_000.0)],
    );
    let updated_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        None,
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    super::super::tiering::persist_segment_catalog(&shared_catalog_path, &updated_inventory)
        .unwrap();

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "remote refresh should consume the shared catalog instead of rescanning the object-store tree",
    );
    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&added_root));
    assert!(!storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&removed_root));
    let points = storage
        .select("remote_catalog_incremental_metric", &labels, 0, 20_000)
        .unwrap();
    assert_eq!(points.len(), 256);
    assert!(!points.contains(&DataPoint::new(1, 1.0)));
    assert!(points.contains(&DataPoint::new(10_000, 10_000.0)));

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_writes_incremental_registry_sidecar_without_rewriting_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels = vec![Label::new("host", "delta")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "flush_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "flush_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_before = std::fs::read(&checkpoint_path).unwrap();
    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    storage
        .insert_rows(&[
            Row::with_labels(
                "flush_registry_delta",
                delta_labels.clone(),
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "flush_registry_delta",
                delta_labels.clone(),
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(std::fs::read(&checkpoint_path).unwrap(), checkpoint_before);
    assert!(!delta_path.exists());
    assert!(delta_dir.exists());
    assert_eq!(incremental_segment_paths(&checkpoint_path).len(), 1);
    let delta_registry = SeriesRegistry::load_incremental_state(&checkpoint_path)
        .unwrap()
        .expect("incremental sidecar should load");
    assert_eq!(delta_registry.delta_series_count, 1);
    assert_eq!(delta_registry.registry.series_count(), 1);
    assert!(delta_registry
        .registry
        .resolve_existing("flush_registry_delta", &delta_labels)
        .is_some());

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("checkpoint plus sidecar should load");
    assert!(loaded_registry
        .registry
        .resolve_existing("flush_registry_checkpoint", &base_labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("flush_registry_delta", &delta_labels)
        .is_some());
    let visible_inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    // C-2: the publish only marks the catalog dirty; the refresh pass owns the
    // rewrite, so the sidecar still lags the published segment here.
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_none());
    assert!(storage
        .persisted
        .registry_catalog_dirty
        .load(std::sync::atomic::Ordering::SeqCst));

    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());
}

#[test]
fn dirty_reconcile_writes_incremental_registry_sidecar_without_rewriting_checkpoint() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels = vec![Label::new("host", "reconcile")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "reconcile_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "reconcile_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_before = std::fs::read(&checkpoint_path).unwrap();
    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    let delta_registry = SeriesRegistry::new();
    let delta_series_id = delta_registry
        .register_series_with_id(100, "reconcile_registry_delta", &delta_labels)
        .unwrap()
        .series_id;
    let mut chunks = HashMap::new();
    chunks.insert(
        delta_series_id,
        vec![make_persisted_numeric_chunk(delta_series_id, &[(10, 10.0)])],
    );
    SegmentWriter::new(&lane_path, 0, 100)
        .unwrap()
        .write_segment(&delta_registry, &chunks)
        .unwrap();

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();

    assert_eq!(std::fs::read(&checkpoint_path).unwrap(), checkpoint_before);
    assert!(!delta_path.exists());
    assert!(delta_dir.exists());
    assert_eq!(incremental_segment_paths(&checkpoint_path).len(), 1);
    let delta_registry = SeriesRegistry::load_incremental_state(&checkpoint_path)
        .unwrap()
        .expect("incremental sidecar should load");
    assert_eq!(delta_registry.delta_series_count, 1);
    assert_eq!(delta_registry.registry.series_count(), 1);
    assert!(delta_registry
        .registry
        .resolve_existing("reconcile_registry_delta", &delta_labels)
        .is_some());

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("checkpoint plus sidecar should load");
    assert!(loaded_registry
        .registry
        .resolve_existing("reconcile_registry_checkpoint", &base_labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("reconcile_registry_delta", &delta_labels)
        .is_some());
    let visible_inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());
}

#[test]
fn repeated_small_flushes_append_incremental_registry_segments_and_restart_recovers() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels_a = vec![Label::new("host", "delta-a")];
    let delta_labels_b = vec![Label::new("host", "delta-b")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "repeated_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "repeated_registry_checkpoint",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_before = std::fs::read(&checkpoint_path).unwrap();
    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    storage
        .insert_rows(&[
            Row::with_labels(
                "repeated_registry_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "repeated_registry_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let first_segment_paths = incremental_segment_paths(&checkpoint_path);
    assert_eq!(first_segment_paths.len(), 1);
    let first_segment_bytes = std::fs::read(&first_segment_paths[0]).unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "repeated_registry_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(5, 5.0),
            ),
            Row::with_labels(
                "repeated_registry_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(6, 6.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(std::fs::read(&checkpoint_path).unwrap(), checkpoint_before);
    let segment_paths = incremental_segment_paths(&checkpoint_path);
    assert_eq!(segment_paths.len(), 2);
    assert_eq!(
        std::fs::read(&segment_paths[0]).unwrap(),
        first_segment_bytes
    );

    let incremental = SeriesRegistry::load_incremental_state(&checkpoint_path)
        .unwrap()
        .expect("incremental segments should load");
    assert_eq!(incremental.delta_series_count, 2);
    assert!(incremental
        .registry
        .resolve_existing("repeated_registry_delta_a", &delta_labels_a)
        .is_some());
    assert!(incremental
        .registry
        .resolve_existing("repeated_registry_delta_b", &delta_labels_b)
        .is_some());

    let reopened = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    assert_eq!(
        reopened
            .select("repeated_registry_checkpoint", &base_labels, 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    assert_eq!(
        reopened
            .select("repeated_registry_delta_a", &delta_labels_a, 0, 10)
            .unwrap(),
        vec![DataPoint::new(3, 3.0), DataPoint::new(4, 4.0)]
    );
    assert_eq!(
        reopened
            .select("repeated_registry_delta_b", &delta_labels_b, 0, 10)
            .unwrap(),
        vec![DataPoint::new(5, 5.0), DataPoint::new(6, 6.0)]
    );
    reopened.close().unwrap();
    storage.close().unwrap();
}

#[test]
fn checkpoint_rollover_compacts_incremental_registry_segments() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let delta_path = SeriesRegistry::incremental_path(&checkpoint_path);
    let delta_dir = SeriesRegistry::incremental_dir(&checkpoint_path);
    let base_labels = vec![Label::new("host", "base")];
    let delta_labels_a = vec![Label::new("host", "delta-a")];
    let delta_labels_b = vec![Label::new("host", "delta-b")];

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "checkpoint_rollover_base",
                    base_labels.clone(),
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "checkpoint_rollover_base",
                    base_labels.clone(),
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let storage = open_raw_numeric_storage_with_registry_snapshot_from_data_path(temp_dir.path());
    storage
        .insert_rows(&[
            Row::with_labels(
                "checkpoint_rollover_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "checkpoint_rollover_delta_a",
                delta_labels_a.clone(),
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let checkpoint_before_rollover = std::fs::read(&checkpoint_path).unwrap();
    assert_eq!(incremental_segment_paths(&checkpoint_path).len(), 1);

    storage
        .catalog
        .delta_series_count
        .store(u64::MAX, Ordering::SeqCst);
    storage
        .insert_rows(&[
            Row::with_labels(
                "checkpoint_rollover_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(5, 5.0),
            ),
            Row::with_labels(
                "checkpoint_rollover_delta_b",
                delta_labels_b.clone(),
                DataPoint::new(6, 6.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_ne!(
        std::fs::read(&checkpoint_path).unwrap(),
        checkpoint_before_rollover
    );
    assert!(!delta_path.exists());
    assert!(!delta_dir.exists());

    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("checkpoint should include compacted incremental state");
    assert_eq!(loaded_registry.delta_series_count, 0);
    assert!(loaded_registry
        .registry
        .resolve_existing("checkpoint_rollover_base", &base_labels)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("checkpoint_rollover_delta_a", &delta_labels_a)
        .is_some());
    assert!(loaded_registry
        .registry
        .resolve_existing("checkpoint_rollover_delta_b", &delta_labels_b)
        .is_some());

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_completes_while_another_writer_permit_is_held() {
    use std::sync::mpsc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.clone()),
        None,
        1,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Nanoseconds,
            retention_window: i64::MAX,
            future_skew_window: default_future_skew_window(TimestampPrecision::Nanoseconds),
            retention_enforced: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: None,
            #[cfg(test)]
            current_time_override: None,
        },
    )
    .unwrap();

    storage
        .insert_rows(&[Row::with_labels(
            "flush_busy_writer",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("flush_busy_writer", &labels)
        .unwrap()
        .series_id;

    let active_before = storage
        .active_shard(series_id)
        .read()
        .get(&series_id)
        .map_or(0, |state| state.point_count());
    assert_eq!(active_before, 1);

    let held_permit = storage.runtime.write_limiter.acquire();
    let (flush_tx, flush_rx) = mpsc::channel();
    thread::scope(|scope| {
        scope.spawn(|| {
            flush_tx.send(storage.flush_pipeline_once()).unwrap();
        });
        let flush_result = flush_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("flush should not wait for an unrelated held writer permit");
        flush_result.unwrap();
        drop(held_permit);
    });

    let active_after = storage
        .active_shard(series_id)
        .read()
        .get(&series_id)
        .map_or(0, |state| state.point_count());
    assert_eq!(active_after, 0);
    assert_eq!(
        storage
            .select("flush_busy_writer", &labels, 0, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(
        !load_segments_for_level(&lane_path, 0).unwrap().is_empty(),
        "flush pipeline should persist while another writer permit is held"
    );

    storage.close().unwrap();
}

#[test]
fn flush_stage_keeps_staged_segments_and_wal_behind_visibility_publish_boundary() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[Row::with_labels(
            "flush_visibility_boundary",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "flush_visibility_boundary",
            labels,
            DataPoint::new(2, 2.0),
        )])
        .unwrap();

    let (staged_tx, staged_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_persist_post_publish_hook({
        let release_rx = Arc::clone(&release_rx);
        move |roots| {
            staged_tx.send(roots.to_vec()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        flush_tx.send(flush_storage.flush_pipeline_once()).unwrap();
    });

    // Invariant: staged flush roots and WAL bytes stay behind the visibility publish step.
    let staged_roots = staged_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush did not reach the staged segment hook");
    assert_eq!(staged_roots.len(), 1);
    let staged_root = staged_roots[0].clone();
    assert!(staged_root.exists());
    load_segment_index(&staged_root).expect("staged segment should already be readable");
    assert!(
        !storage
            .persisted
            .persisted_index
            .read()
            .segments_by_root
            .contains_key(&staged_root),
        "staged flush roots must stay hidden until the visibility swap publishes them",
    );
    assert!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .total_size_bytes()
            .unwrap()
            > 0,
        "WAL reset must wait until the flush visibility publication commits",
    );
    assert!(
        flush_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "flush should still be paused at the visibility publish boundary",
    );

    release_tx.send(()).unwrap();
    assert!(flush_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    flush_thread.join().unwrap();

    assert!(storage
        .persisted
        .persisted_index
        .read()
        .segments_by_root
        .contains_key(&staged_root));
    assert_eq!(
        storage
            .persisted
            .wal
            .as_ref()
            .unwrap()
            .total_size_bytes()
            .unwrap(),
        0,
        "WAL reset should only happen after the new persisted view is published",
    );

    storage.clear_persist_post_publish_hook();
    storage.close().unwrap();
}

#[test]
fn flush_snapshot_waits_for_active_to_sealed_publication() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path),
            None,
            1,
            options,
        )
        .unwrap(),
    );
    let metric = "active_to_sealed_flush_visibility";
    let labels = vec![Label::new("host", "flush")];

    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();

    let (publish_entered_tx, publish_entered_rx) = mpsc::channel();
    let (publish_release_tx, publish_release_rx) = mpsc::channel();
    let publish_release_rx = Arc::new(Mutex::new(publish_release_rx));
    storage.set_ingest_pre_sealed_chunk_publish_hook({
        let publish_entered_tx = publish_entered_tx.clone();
        let publish_release_rx = Arc::clone(&publish_release_rx);
        move || {
            publish_entered_tx.send(()).unwrap();
            publish_release_rx.lock().unwrap().recv().unwrap();
        }
    });

    let writer_storage = Arc::clone(&storage);
    let writer_labels = labels.clone();
    let writer_thread = thread::spawn(move || {
        writer_storage.insert_rows(&[Row::with_labels(
            metric,
            writer_labels,
            DataPoint::new(2, 2.0),
        )])
    });

    publish_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("write did not reach the active-to-sealed publish boundary");

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        flush_tx
            .send(flush_storage.persist_segment_with_outcome())
            .unwrap();
    });

    assert!(
        flush_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "flush snapshot should wait while the finalized chunk is between active mutation and sealed publication",
    );

    publish_release_tx.send(()).unwrap();

    writer_thread.join().unwrap().unwrap();
    let outcome = flush_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    flush_thread.join().unwrap();
    storage.clear_ingest_pre_sealed_chunk_publish_hook();

    assert!(outcome.persisted);
    assert_eq!(outcome.series, 1);
    assert_eq!(outcome.chunks, 1);
    assert_eq!(
        storage.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );

    storage.close().unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(2)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)]
    );
    reopened.close().unwrap();
}

#[test]
fn snapshot_fences_background_flush_before_copying_wal_state() {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let snapshot_path = temp_dir.path().join("snapshot");
    let restore_path = temp_dir.path().join("restore");

    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let mut options = base_storage_test_options(TimestampPrecision::Seconds, None);
    options.retention_enforced = false;
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            4_096,
            Some(wal),
            Some(lane_path),
            None,
            1,
            options,
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[
            Row::new("snapshot_background_flush", DataPoint::new(1, 11.0)),
            Row::new("snapshot_background_flush", DataPoint::new(2, 22.0)),
        ])
        .unwrap();

    let (flush_start_tx, flush_start_rx) = mpsc::channel();
    let (flush_attempt_tx, flush_attempt_rx) = mpsc::channel();
    let flush_attempt_rx = Arc::new(Mutex::new(flush_attempt_rx));
    let flush_storage = Arc::clone(&storage);
    let flush_thread = thread::spawn(move || {
        flush_start_rx.recv().unwrap();
        flush_attempt_tx.send(()).unwrap();
        let _background_maintenance_guard = flush_storage.background_maintenance_gate();
        flush_storage.background_flush_pipeline_once().unwrap();
    });

    storage.set_snapshot_pre_wal_copy_hook({
        let flush_attempt_rx = Arc::clone(&flush_attempt_rx);
        move || {
            flush_start_tx.send(()).unwrap();
            flush_attempt_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(1))
                .expect("background flush helper did not start");
            thread::sleep(Duration::from_millis(200));
        }
    });

    storage.snapshot(&snapshot_path).unwrap();
    storage.clear_snapshot_pre_wal_copy_hook();
    flush_thread.join().unwrap();

    let snapshot_wal =
        FramedWal::open(snapshot_path.join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
    assert!(
        snapshot_wal.total_size_bytes().unwrap() > 0,
        "snapshot should keep WAL-backed writes when a background flush races the staging copy",
    );
    drop(snapshot_wal);
    let _ = std::fs::remove_dir_all(snapshot_path.join(NUMERIC_LANE_ROOT));

    StorageBuilder::restore_from_snapshot(&snapshot_path, &restore_path).unwrap();
    let restored = StorageBuilder::new()
        .with_data_path(&restore_path)
        .with_chunk_points(4_096)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .build()
        .unwrap();
    assert_eq!(
        restored
            .select("snapshot_background_flush", &[], 0, 10)
            .unwrap(),
        vec![DataPoint::new(1, 11.0), DataPoint::new(2, 22.0)],
    );

    restored.close().unwrap();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_skips_wal_reset_when_a_new_write_commits_after_publish() {
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let wal_path = temp_dir.path().join(WAL_DIR_NAME);
    let labels = vec![Label::new("host", "a")];
    let wal = FramedWal::open(&wal_path, WalSyncMode::PerAppend).unwrap();
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            Some(wal),
            Some(lane_path.clone()),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[Row::with_labels(
            "flush_reset_race_metric",
            labels.clone(),
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "flush_reset_race_metric",
            labels.clone(),
            DataPoint::new(2, 2.0),
        )])
        .unwrap();

    let hook_storage = Arc::clone(&storage);
    let hook_labels = labels.clone();
    storage.set_persist_post_publish_hook(move |_| {
        hook_storage
            .insert_rows(&[Row::with_labels(
                "flush_reset_race_metric",
                hook_labels.clone(),
                DataPoint::new(3, 3.0),
            )])
            .unwrap();
    });

    storage.flush_pipeline_once().unwrap();

    let wal = storage.persisted.wal.as_ref().unwrap();
    assert!(
        wal.total_size_bytes().unwrap() > 0,
        "WAL reset should be skipped once a newer write lands after publish",
    );
    assert_eq!(
        storage
            .select("flush_reset_race_metric", &labels, 0, 10)
            .unwrap()
            .len(),
        3,
    );

    storage.clear_persist_post_publish_hook();
    storage.close().unwrap();
}

#[test]
fn flush_pipeline_runs_steady_state_post_flush_maintenance_without_full_inventory_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let historical_series_id = registry
        .resolve_or_insert("historical_post_flush_maintenance", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=64 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            historical_series_id,
            0,
            segment_id,
            &[(100, segment_id as f64)],
        );
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            65,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 10,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    storage
        .insert_rows(&[Row::with_labels(
            "steady_state_flush_maintenance",
            labels.clone(),
            DataPoint::new(100, 1.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    storage.set_current_time_override(101);
    storage
        .insert_rows(&[Row::with_labels(
            "steady_state_flush_maintenance",
            labels.clone(),
            DataPoint::new(101, 2.0),
        )])
        .unwrap();
    assert_eq!(
        storage
            .select("steady_state_flush_maintenance", &labels, 0, 200)
            .unwrap(),
        vec![DataPoint::new(100, 1.0), DataPoint::new(101, 2.0)],
        "foreground reads should keep serving published data while no-op maintenance settles"
    );

    assert!(
        !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst),
        "background maintenance did not settle the post-flush no-op work"
    );
    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "steady-state post-flush maintenance should reuse the persisted catalog instead of rescanning the segment tree",
    );

    storage.clear_full_inventory_scan_hook();
}

#[test]
fn background_post_flush_maintenance_applies_known_dirty_diff_before_inventory_scan() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];

    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("dirty_post_flush_maintenance", &labels)
        .unwrap()
        .series_id;
    for segment_id in 1..=128 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(100, segment_id as f64)],
        );
    }

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path.clone()),
            None,
            129,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 10,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: None,
                #[cfg(test)]
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&lane_path).unwrap(), false)
        .unwrap();

    let full_scans = Arc::new(AtomicUsize::new(0));
    storage.set_full_inventory_scan_hook({
        let full_scans = Arc::clone(&full_scans);
        move || {
            full_scans.fetch_add(1, Ordering::SeqCst);
        }
    });

    let added_root =
        write_numeric_segment_to_path(&lane_path, &registry, series_id, 0, 129, &[(100, 129.0)]);
    storage
        .persisted
        .pending_persisted_segment_diff
        .lock()
        .record_changes(std::iter::once(added_root.clone()), std::iter::empty());
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);

    storage.start_background_persisted_refresh_thread().unwrap();
    storage.schedule_post_flush_maintenance().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut settled = false;
    while Instant::now() < deadline {
        if !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst)
            && !storage
                .persisted
                .persisted_index_dirty
                .load(Ordering::SeqCst)
            && !storage.has_known_persisted_segment_changes()
            && storage
                .persisted
                .persisted_index
                .read()
                .segments_by_root
                .contains_key(&added_root)
        {
            settled = true;
            break;
        }

        thread::sleep(Duration::from_millis(10));
    }

    assert!(
        settled,
        "background post-flush maintenance did not settle the known dirty diff",
    );
    assert_eq!(
        full_scans.load(Ordering::SeqCst),
        0,
        "background post-flush maintenance should apply known dirty roots before scanning the full segment tree",
    );

    storage.clear_full_inventory_scan_hook();
    storage.close().unwrap();
}

#[test]
fn background_post_flush_maintenance_stage_does_not_block_queries() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let warm_root = object_store_dir
        .path()
        .join("warm")
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let labels = vec![Label::new("host", "a")];
    let metric = "background_post_flush_query_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(hot_lane.clone()),
            None,
            2,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 100,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: Some(super::super::config::TieredStorageConfig {
                    object_store_root: object_store_dir.path().to_path_buf(),
                    segment_catalog_path: None,
                    mirror_hot_segments: false,
                    hot_retention_window: 10,
                    warm_retention_window: 50,
                }),
                #[cfg(test)]
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();

    let stage_started = Arc::new(AtomicBool::new(false));
    let release_stage = Arc::new(AtomicBool::new(false));
    storage.set_post_flush_maintenance_stage_hook({
        let stage_started = Arc::clone(&stage_started);
        let release_stage = Arc::clone(&release_stage);
        move || {
            stage_started.store(true, Ordering::SeqCst);
            while !release_stage.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });

    storage.start_background_persisted_refresh_thread().unwrap();
    storage.schedule_post_flush_maintenance().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !stage_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        stage_started.load(Ordering::SeqCst),
        "background post-flush maintenance did not reach the staging hook",
    );

    let concurrent_storage = Arc::clone(&storage);
    let concurrent_labels = labels.clone();
    let (query_tx, query_rx) = mpsc::channel();
    let concurrent_query = thread::spawn(move || {
        let result = concurrent_storage.select(metric, &concurrent_labels, 0, 200);
        query_tx.send(result).unwrap();
    });

    let concurrent_points = query_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("concurrent query should not block on staged post-flush maintenance");
    assert_eq!(
        concurrent_points.unwrap(),
        vec![DataPoint::new(60, 60.0), DataPoint::new(61, 61.0)]
    );

    release_stage.store(true, Ordering::SeqCst);
    concurrent_query.join().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut settled = false;
    while Instant::now() < deadline {
        if !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst)
            && warm_root.exists()
            && !hot_lane
                .join("segments")
                .join("L0")
                .join("seg-0000000000000001")
                .exists()
        {
            settled = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        settled,
        "background post-flush maintenance did not finish the staged tier move"
    );

    storage.clear_post_flush_maintenance_stage_hook();
    storage.close().unwrap();
}

#[test]
fn background_post_flush_maintenance_syncs_registry_catalog_after_tier_move() {
    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = data_dir.path().join(SERIES_INDEX_FILE_NAME);
    let hot_root = hot_lane
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let warm_root = object_store_dir
        .path()
        .join("warm")
        .join(NUMERIC_LANE_ROOT)
        .join("segments")
        .join("L0")
        .join("seg-0000000000000001");
    let labels = vec![Label::new("host", "a")];
    let metric = "background_post_flush_catalog_sync_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let tiered_storage = super::super::config::TieredStorageConfig {
        object_store_root: object_store_dir.path().to_path_buf(),
        segment_catalog_path: None,
        mirror_hot_segments: false,
        hot_retention_window: 10,
        warm_retention_window: 50,
    };
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(hot_lane.clone()),
        None,
        2,
        ChunkStorageOptions {
            timestamp_precision: TimestampPrecision::Seconds,
            retention_window: 100,
            future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
            retention_enforced: true,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            partition_window: i64::MAX,
            max_active_partition_heads_per_series:
                crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            max_writers: 2,
            write_timeout: Duration::from_secs(1),
            memory_budget_bytes: u64::MAX,
            cardinality_limit: usize::MAX,
            wal_size_limit_bytes: u64::MAX,
            admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
            compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            background_threads_enabled: false,
            background_fail_fast: false,
            metadata_shard_count: None,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: Duration::from_secs(5),
            tiered_storage: Some(tiered_storage.clone()),
            #[cfg(test)]
            current_time_override: Some(100),
        },
    )
    .unwrap();
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();
    storage.checkpoint_series_registry_index().unwrap();

    storage.schedule_post_flush_maintenance().unwrap();

    assert!(!hot_root.exists());
    assert!(warm_root.exists());
    assert_eq!(
        storage.select(metric, &labels, 0, 200).unwrap(),
        vec![DataPoint::new(60, 60.0), DataPoint::new(61, 61.0)],
    );

    let visible_inventory = super::super::tiering::build_segment_inventory_runtime_strict(
        Some(&hot_lane),
        None,
        Some(&tiered_storage),
    )
    .unwrap();
    assert!(visible_inventory
        .entries()
        .iter()
        .any(|entry| entry.root == warm_root));
    assert!(super::super::registry_catalog::validate_registry_catalog(
        &checkpoint_path,
        &super::super::registry_catalog::inventory_sources(&visible_inventory),
    )
    .unwrap()
    .is_some());

    storage.close().unwrap();
}

#[test]
fn flush_pipeline_returns_while_background_post_flush_maintenance_is_staged() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    let data_dir = TempDir::new().unwrap();
    let object_store_dir = TempDir::new().unwrap();
    let hot_lane = data_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let metric = "background_post_flush_flush_metric";
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert(metric, &labels)
        .unwrap()
        .series_id;
    write_numeric_segment_to_path(
        &hot_lane,
        &registry,
        series_id,
        0,
        1,
        &[(60, 60.0), (61, 61.0)],
    );

    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(hot_lane.clone()),
            None,
            2,
            ChunkStorageOptions {
                timestamp_precision: TimestampPrecision::Seconds,
                retention_window: 100,
                future_skew_window: default_future_skew_window(TimestampPrecision::Seconds),
                retention_enforced: true,
                runtime_mode: StorageRuntimeMode::ReadWrite,
                partition_window: i64::MAX,
                max_active_partition_heads_per_series:
                    crate::storage::DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
                memory_budget_bytes: u64::MAX,
                cardinality_limit: usize::MAX,
                wal_size_limit_bytes: u64::MAX,
                admission_poll_interval: DEFAULT_ADMISSION_POLL_INTERVAL,
                compaction_interval: DEFAULT_COMPACTION_INTERVAL,
                background_threads_enabled: false,
                background_fail_fast: false,
                metadata_shard_count: None,
                remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
                remote_segment_refresh_interval: Duration::from_secs(5),
                tiered_storage: Some(super::super::config::TieredStorageConfig {
                    object_store_root: object_store_dir.path().to_path_buf(),
                    segment_catalog_path: None,
                    mirror_hot_segments: false,
                    hot_retention_window: 10,
                    warm_retention_window: 50,
                }),
                #[cfg(test)]
                current_time_override: Some(100),
            },
        )
        .unwrap(),
    );
    storage
        .apply_loaded_segment_indexes(load_segment_indexes(&hot_lane).unwrap(), false)
        .unwrap();

    let stage_started = Arc::new(AtomicBool::new(false));
    let release_stage = Arc::new(AtomicBool::new(false));
    storage.set_post_flush_maintenance_stage_hook({
        let stage_started = Arc::clone(&stage_started);
        let release_stage = Arc::clone(&release_stage);
        move || {
            stage_started.store(true, Ordering::SeqCst);
            while !release_stage.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });

    storage.start_background_persisted_refresh_thread().unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            metric,
            labels.clone(),
            DataPoint::new(100, 100.0),
        )])
        .unwrap();

    let flush_storage = Arc::clone(&storage);
    let (flush_tx, flush_rx) = mpsc::channel();
    let flush_thread = thread::spawn(move || {
        flush_tx.send(flush_storage.flush_pipeline_once()).unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    while !stage_started.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        stage_started.load(Ordering::SeqCst),
        "background post-flush maintenance did not reach the staging hook after flush",
    );

    let flush_result = flush_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("flush should return while background post-flush maintenance is staged");
    assert!(flush_result.is_ok());

    release_stage.store(true, Ordering::SeqCst);
    flush_thread.join().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while storage
        .coordination
        .post_flush_maintenance_pending
        .load(Ordering::SeqCst)
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !storage
            .coordination
            .post_flush_maintenance_pending
            .load(Ordering::SeqCst),
        "background post-flush maintenance did not settle after the staging hook released",
    );

    storage.clear_post_flush_maintenance_stage_hook();
    storage.close().unwrap();
}

#[test]
fn wal_disabled_persistent_storage_still_runs_background_flush() {
    use std::thread;
    use std::time::Instant;

    let temp_dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_retention_enforced(false)
        .with_wal_enabled(false)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    storage
        .insert_rows(&[
            Row::with_labels(
                "wal_disabled_background",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "wal_disabled_background",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels(
                "wal_disabled_background",
                labels.clone(),
                DataPoint::new(3, 3.0),
            ),
        ])
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut persisted = false;
    while Instant::now() < deadline {
        if !load_segments_for_level(&lane_path, 0).unwrap().is_empty() {
            persisted = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    assert!(
        persisted,
        "background flush should persist segments even when WAL is disabled"
    );
    storage.close().unwrap();
}

#[test]
fn close_waits_for_inflight_background_flush_before_final_persist() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[
            Row::with_labels(
                "close_background_flush",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "close_background_flush",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
            Row::with_labels("close_background_flush", labels, DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let (background_entered_tx, background_entered_rx) = mpsc::channel();
    let (close_persist_tx, close_persist_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_persist_post_publish_hook({
        let hook_calls = Arc::clone(&hook_calls);
        let release_rx = Arc::clone(&release_rx);
        move |_| match hook_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                background_entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
            1 => {
                close_persist_tx.send(()).unwrap();
            }
            _ => {}
        }
    });

    storage
        .start_background_flush_thread(Duration::from_secs(60))
        .unwrap();
    storage.notify_flush_thread();
    background_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("background flush did not reach the publish hook");

    let close_storage = Arc::clone(&storage);
    let (close_tx, close_rx) = mpsc::channel();
    let close_thread = thread::spawn(move || {
        close_tx.send(close_storage.close()).unwrap();
    });

    assert!(
        close_persist_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "close should not publish its final persisted segment while background flush is mid-pass",
    );

    release_tx.send(()).unwrap();
    close_persist_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close did not reach its final persist after the background flush finished");
    assert!(close_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    close_thread.join().unwrap();
    assert_eq!(hook_calls.load(Ordering::SeqCst), 2);
}

#[test]
fn close_waits_for_inflight_background_refresh_before_final_persist() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "a")];
    let storage = Arc::new(
        ChunkStorage::new_with_data_path_and_options(
            2,
            None,
            Some(lane_path),
            None,
            1,
            ChunkStorageOptions {
                retention_enforced: false,
                background_threads_enabled: false,
                ..ChunkStorageOptions::default()
            },
        )
        .unwrap(),
    );

    storage
        .insert_rows(&[
            Row::with_labels(
                "close_background_refresh",
                labels.clone(),
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "close_background_refresh",
                labels.clone(),
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage
        .insert_rows(&[Row::with_labels(
            "close_background_refresh",
            labels,
            DataPoint::new(3, 3.0),
        )])
        .unwrap();

    let scan_calls = Arc::new(AtomicUsize::new(0));
    let (background_entered_tx, background_entered_rx) = mpsc::channel();
    let (close_persist_tx, close_persist_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    storage.set_full_inventory_scan_hook({
        let scan_calls = Arc::clone(&scan_calls);
        let release_rx = Arc::clone(&release_rx);
        move || {
            if scan_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                background_entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        }
    });
    storage.set_persist_post_publish_hook(move |_| {
        close_persist_tx.send(()).unwrap();
    });

    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage.start_background_persisted_refresh_thread().unwrap();
    storage.notify_persisted_refresh_thread();
    background_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("background refresh did not reach the full inventory scan hook");

    let close_storage = Arc::clone(&storage);
    let (close_tx, close_rx) = mpsc::channel();
    let close_thread = thread::spawn(move || {
        close_tx.send(close_storage.close()).unwrap();
    });

    assert!(
        close_persist_rx
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "close should not publish its final persisted segment while background refresh is mid-pass",
    );

    release_tx.send(()).unwrap();
    close_persist_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("close did not reach its final persist after the background refresh finished");
    assert!(close_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_ok());
    close_thread.join().unwrap();
    assert!(scan_calls.load(Ordering::SeqCst) >= 1);
}

#[test]
fn window_diff_is_merged_into_pending_diff_immediately() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("window_diff_merge", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // Twelve disjoint L0 segments drain as three windows in a single round.
    for segment_id in 1..=12u64 {
        write_numeric_segment_to_path(
            &lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }

    let mut storage = new_raw_numeric_storage(lane_path, 13);
    // A real startup discovers the inventory and injects it; without it the
    // compactor refuses to plan (no on-disk fallback for the storage path).
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    storage
        .persisted
        .numeric_compactor
        .as_mut()
        .unwrap()
        .set_pass_wall_budget(Duration::from_secs(60));

    let mut added_after_each_window = Vec::new();
    let stats = ChunkStorage::compact_compactors_with_changes(
        storage.persisted.numeric_compactor.as_ref(),
        None,
        Some(storage.observability.as_ref()),
        &mut |window| {
            ChunkStorage::merge_published_window(
                storage.persisted.pending_persisted_segment_diff.as_ref(),
                storage.persisted.persisted_index_dirty.as_ref(),
                storage.observability.as_ref(),
                window,
            );
            // The contract the background worker and close path rely on: the
            // window's roots are already merged and the index is already dirty
            // when the callback returns — not at the end of the round.
            assert!(
                storage
                    .persisted
                    .persisted_index_dirty
                    .load(Ordering::SeqCst),
                "each published window must mark the persisted index dirty"
            );
            added_after_each_window.push(
                storage
                    .persisted
                    .pending_persisted_segment_diff
                    .lock()
                    .added_roots
                    .len(),
            );
        },
    )
    .unwrap();

    assert_eq!(
        added_after_each_window.len(),
        stats.windows_merged as usize,
        "one callback per merged window"
    );
    assert!(
        added_after_each_window.len() >= 2,
        "the fixture must produce a multi-window round, got {}",
        added_after_each_window.len()
    );
    assert!(
        added_after_each_window
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "the pending diff must grow window by window: {added_after_each_window:?}"
    );
    assert_eq!(
        storage
            .persisted
            .pending_persisted_segment_diff
            .lock()
            .added_roots
            .len(),
        3,
        "three L0 windows publish three L1 outputs, all pending"
    );
    assert_eq!(
        storage
            .persisted
            .pending_persisted_segment_diff
            .lock()
            .removed_roots
            .len(),
        12,
        "all twelve L0 sources must be pending removal"
    );

    // Observability: the round's window count and the live pending-diff gauge.
    let snapshot = storage.observability_snapshot();
    assert_eq!(
        snapshot.compaction.windows_merged_total,
        u64::from(stats.windows_merged),
        "windows_merged_total must track the round's stats"
    );
    assert_eq!(
        snapshot.health.pending_persisted_segment_diff_entries,
        3 + 12,
        "the pending-diff gauge must reflect added + removed roots"
    );

    // Apply the merged diff before closing: the close pipeline plans from the
    // same persisted index, and stale source entries would only be retired as
    // ghosts there.
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    storage.close().unwrap();
}

#[test]
fn blob_lane_failure_keeps_the_numeric_lane_windows_already_merged() {
    use std::sync::atomic::Ordering;

    let temp_dir = TempDir::new().unwrap();
    let numeric_lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);
    let registry = SeriesRegistry::new();
    let series_id = registry
        .resolve_or_insert("cross_lane_partial_failure", &[Label::new("host", "a")])
        .unwrap()
        .series_id;

    // Numeric: eight disjoint L0 segments, i.e. two windows that both publish.
    for segment_id in 1..=8u64 {
        write_numeric_segment_to_path(
            &numeric_lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        );
    }
    // Blob: four L0 segments is one window; the payload family is irrelevant for
    // the failure this test needs — the lane only has to reach its window load
    // and fail there.
    let mut blob_roots = Vec::new();
    for segment_id in 1..=4u64 {
        blob_roots.push(write_numeric_segment_to_path(
            &blob_lane_path,
            &registry,
            series_id,
            0,
            segment_id,
            &[(segment_id as i64, segment_id as f64)],
        ));
    }

    let mut storage = ChunkStorage::new_with_data_path_and_options(
        8,
        None,
        Some(numeric_lane_path.clone()),
        Some(blob_lane_path),
        9,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage
        .persisted
        .persisted_index_dirty
        .store(true, Ordering::SeqCst);
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    storage
        .persisted
        .numeric_compactor
        .as_mut()
        .unwrap()
        .set_pass_wall_budget(Duration::from_secs(60));
    storage
        .persisted
        .blob_compactor
        .as_mut()
        .unwrap()
        .set_pass_wall_budget(Duration::from_secs(60));

    // Corrupt one blob window member (payload hash mismatch, not a vanished
    // file) and keep the bytes so the fixture can be closed cleanly afterwards.
    let corrupt_chunks_path = blob_roots[1].join("chunks.bin");
    let original_chunks = std::fs::read(&corrupt_chunks_path).unwrap();
    let mut corrupted = original_chunks.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xff;
    std::fs::write(&corrupt_chunks_path, corrupted).unwrap();

    let mut merged_windows = 0usize;
    let err = ChunkStorage::compact_compactors_with_changes(
        storage.persisted.numeric_compactor.as_ref(),
        storage.persisted.blob_compactor.as_ref(),
        Some(storage.observability.as_ref()),
        &mut |window| {
            merged_windows += 1;
            ChunkStorage::merge_published_window(
                storage.persisted.pending_persisted_segment_diff.as_ref(),
                storage.persisted.persisted_index_dirty.as_ref(),
                storage.observability.as_ref(),
                window,
            );
        },
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("failed compaction validation"),
        "the corrupt blob member must fail the pass: {err}"
    );

    // The numeric lane ran before the blob lane. Its two published windows were
    // already merged, so the later lane's error cannot diverge disk from index.
    assert_eq!(
        merged_windows, 2,
        "both numeric windows must have been merged before the blob failure"
    );
    assert!(
        storage
            .persisted
            .persisted_index_dirty
            .load(Ordering::SeqCst),
        "a merged window must leave the persisted index dirty even when a later lane fails"
    );
    let pending = storage.persisted.pending_persisted_segment_diff.lock();
    assert_eq!(
        pending.added_roots.len(),
        2,
        "both numeric L1 outputs must be pending addition"
    );
    assert_eq!(
        pending.removed_roots.len(),
        8,
        "all eight numeric L0 sources must be pending removal"
    );
    assert!(
        pending
            .added_roots
            .iter()
            .all(|root| root.starts_with(&numeric_lane_path) && root.exists()),
        "the numeric lane's replacements are already on disk: {:?}",
        pending.added_roots
    );
    drop(pending);

    std::fs::write(&corrupt_chunks_path, original_chunks).unwrap();
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    storage.close().unwrap();
}

#[test]
fn persisted_state_carries_manifest_files() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let labels = vec![Label::new("host", "files")];

    let storage = new_raw_numeric_storage(lane_path, 1);
    storage
        .insert_rows(&[
            Row::with_labels("inventory_files", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("inventory_files", labels, DataPoint::new(2, 2.0)),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    let (root, persisted_manifest, persisted_files) = {
        let index = storage.persisted.persisted_index.read();
        assert_eq!(
            index.segments_by_root.len(),
            1,
            "the flush should have published exactly one segment"
        );
        let (root, state) = index
            .segments_by_root
            .iter()
            .next()
            .expect("flush must publish a segment root");
        (
            root.clone(),
            state.manifest.clone(),
            state
                .files
                .expect("published segments must carry manifest file entries"),
        )
    };

    let (manifest, files) =
        crate::engine::segment::read_segment_manifest_with_files(&root).unwrap();
    assert_eq!(manifest, persisted_manifest);
    assert_eq!(
        files.map(|entry| (entry.kind, entry.file_len, entry.hash64)),
        persisted_files.map(|entry| (entry.kind, entry.file_len, entry.hash64)),
        "persisted files must match a direct manifest read"
    );
    assert_eq!(
        files.map(|entry| entry.kind),
        [1, 2, 3, 4],
        "manifest entries stay ordered chunks, chunk_index, series, postings"
    );

    storage.close().unwrap();
}

/// Flushes one segment, then returns the single published root.
fn single_published_root(storage: &ChunkStorage) -> std::path::PathBuf {
    let index = storage.persisted.persisted_index.read();
    assert_eq!(
        index.segments_by_root.len(),
        1,
        "the flush should have published exactly one segment"
    );
    index
        .segments_by_root
        .keys()
        .next()
        .expect("flush must publish a segment root")
        .clone()
}

fn series_run_tuples(
    run: &super::super::registry_contributions::SeriesRun,
) -> Vec<(u64, String, Vec<Label>)> {
    let mut tuples = run
        .0
        .iter()
        .map(|(series_id, key)| (*series_id, key.0.clone(), key.1.clone()))
        .collect::<Vec<_>>();
    tuples.sort_by_key(|(series_id, _, _)| *series_id);
    tuples
}

fn disk_series_tuples(root: &std::path::Path) -> Vec<(u64, String, Vec<Label>)> {
    let mut tuples = crate::engine::segment::load_segment_series_metadata(root)
        .unwrap()
        .into_iter()
        .map(|series| (series.series_id, series.metric, series.labels))
        .collect::<Vec<_>>();
    tuples.sort_by_key(|(series_id, _, _)| *series_id);
    tuples
}

/// The in-memory catalog build must be byte-identical to the disk build for the
/// same segment set: the catalog schema and the series fingerprint hash are the
/// on-disk contract, not an implementation detail of the memory path.
#[test]
fn memory_built_catalog_matches_disk_built_catalog() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = new_raw_numeric_storage(lane_path.clone(), 1);
    storage
        .insert_rows(&[
            Row::with_labels(
                "catalog_memory",
                vec![Label::new("host", "a")],
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "catalog_memory",
                vec![Label::new("host", "b")],
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage
        .insert_rows(&[
            Row::with_labels(
                "catalog_memory",
                vec![Label::new("host", "a")],
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "catalog_memory_second",
                vec![Label::new("host", "c")],
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage.close().unwrap();

    let inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    assert_eq!(inventory.entries().len(), 2);
    assert!(
        inventory
            .entries()
            .iter()
            .all(|entry| entry.files.is_some()),
        "a disk scan must carry manifest file entries for the memory build"
    );

    let sources = super::super::registry_catalog::inventory_sources(&inventory);
    let contributions = super::super::registry_contributions::RegistryContributions::new();
    let primed = sources
        .iter()
        .map(|source| {
            let series =
                crate::engine::segment::load_segment_series_metadata(&source.root).unwrap();
            (
                source.root.clone(),
                contributions.series_run_from_series(&series),
            )
        })
        .collect::<Vec<_>>();
    contributions.prime(&primed);

    let disk = super::super::registry_catalog::build_catalog_from_disk(&sources).unwrap();
    let memory =
        super::super::registry_catalog::build_catalog_from_memory(&sources, &contributions)
            .unwrap();

    assert_eq!(
        serde_json::to_vec_pretty(&disk).unwrap(),
        serde_json::to_vec_pretty(&memory).unwrap(),
        "the memory-built catalog must be byte-identical to the disk-built catalog"
    );
}

/// Over the absolute entry budget, the write must degrade to the disk path —
/// never to wrong output — and produce the same catalog bytes.
#[test]
fn contribution_entry_budget_falls_back_to_disk_with_identical_catalog() {
    use crate::engine::fs_utils::{count_segment_file_reads_under, CountingSegmentReads};

    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = new_raw_numeric_storage(lane_path.clone(), 1);
    storage
        .insert_rows(&[
            Row::with_labels(
                "budget_fallback",
                vec![Label::new("host", "a")],
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "budget_fallback",
                vec![Label::new("host", "b")],
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage
        .insert_rows(&[
            Row::with_labels(
                "budget_fallback",
                vec![Label::new("host", "a")],
                DataPoint::new(3, 3.0),
            ),
            Row::with_labels(
                "budget_fallback_second",
                vec![Label::new("host", "c")],
                DataPoint::new(4, 4.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage.close().unwrap();

    let inventory =
        super::super::tiering::build_segment_inventory_runtime_strict(Some(&lane_path), None, None)
            .unwrap();
    let sources = super::super::registry_catalog::inventory_sources(&inventory);
    let contributions = super::super::registry_contributions::RegistryContributions::new();
    let primed = sources
        .iter()
        .map(|source| {
            let series =
                crate::engine::segment::load_segment_series_metadata(&source.root).unwrap();
            (
                source.root.clone(),
                contributions.series_run_from_series(&series),
            )
        })
        .collect::<Vec<_>>();
    contributions.prime(&primed);

    let disk = super::super::registry_catalog::build_catalog_from_disk(&sources).unwrap();
    let disk_bytes = serde_json::to_vec_pretty(&disk).unwrap();

    // Under budget the cache answers the whole write from memory.
    let guard = CountingSegmentReads::install(temp_dir.path());
    let cached = super::super::registry_catalog::build_catalog_from_memory_with_budget(
        &sources,
        &contributions,
        usize::MAX,
    )
    .unwrap();
    let cached_reads = count_segment_file_reads_under(temp_dir.path());
    drop(guard);
    assert_eq!(
        cached_reads, 0,
        "a primed cache under budget must not re-read segment files"
    );
    assert_eq!(serde_json::to_vec_pretty(&cached).unwrap(), disk_bytes);

    // Over budget the write re-derives the fingerprint from disk.
    let guard = CountingSegmentReads::install(temp_dir.path());
    let budgeted = super::super::registry_catalog::build_catalog_from_memory_with_budget(
        &sources,
        &contributions,
        1,
    )
    .unwrap();
    let budgeted_reads = count_segment_file_reads_under(temp_dir.path());
    drop(guard);
    assert!(
        budgeted_reads > 0,
        "over the entry budget the disk fallback must read series.bin"
    );
    assert_eq!(
        serde_json::to_vec_pretty(&budgeted).unwrap(),
        disk_bytes,
        "the budget fallback must still produce the exact catalog"
    );
}

/// A catalog write that follows a publish must not re-read any already known
/// segment; only the freshly published segment may need bounded disk work.
#[test]
fn catalog_write_after_publish_does_not_touch_segment_files() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = new_raw_numeric_storage(lane_path, 1);
    storage
        .insert_rows(&[Row::with_labels(
            "catalog_reads_old",
            vec![Label::new("host", "old")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    let old_root = single_published_root(&storage);

    storage
        .insert_rows(&[Row::with_labels(
            "catalog_reads_new",
            vec![Label::new("host", "new")],
            DataPoint::new(2, 2.0),
        )])
        .unwrap();

    let guard = crate::engine::fs_utils::CountingSegmentReads::install(temp_dir.path());
    storage.flush_pipeline_once().unwrap();
    let old_root_reads = crate::engine::fs_utils::count_segment_file_reads_under(&old_root);
    drop(guard);

    assert_eq!(
        old_root_reads,
        0,
        "catalog build after publish must not read old segment files: {}",
        old_root.display()
    );

    let roots = {
        let index = storage.persisted.persisted_index.read();
        index.segments_by_root.keys().cloned().collect::<Vec<_>>()
    };
    assert_eq!(roots.len(), 2, "the second flush publishes one new segment");
    for root in &roots {
        assert!(
            storage
                .persisted
                .registry_contributions
                .run_for_root(root)
                .is_some(),
            "every published root must contribute a series run: {}",
            root.display()
        );
    }

    // Task 12: the publish only marks the catalog dirty; the refresh pass writes
    // it from memory once the diff has been applied to the persisted index.
    let guard = crate::engine::fs_utils::CountingSegmentReads::install(temp_dir.path());
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    let refresh_reads = crate::engine::fs_utils::count_segment_file_reads_under(temp_dir.path());
    drop(guard);
    assert_eq!(
        refresh_reads, 0,
        "the debounced catalog write must be built from memory, not segment files"
    );

    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog = std::fs::read(super::super::registry_catalog::catalog_path(
        &checkpoint_path,
    ))
    .unwrap();
    let catalog: serde_json::Value = serde_json::from_slice(&catalog).unwrap();
    assert_eq!(
        catalog["segments"].as_array().unwrap().len(),
        2,
        "the debounced catalog write must describe both visible segments"
    );

    storage.close().unwrap();
}

/// Publishes one segment and lets the refresh pass materialize the catalog, so a
/// following flush has a baseline catalog file to leave untouched.
fn storage_with_materialized_registry_catalog(
    lane_path: &Path,
) -> (ChunkStorage, std::path::PathBuf) {
    let storage = new_raw_numeric_storage(lane_path.to_path_buf(), 1);
    storage
        .insert_rows(&[Row::with_labels(
            "catalog_debounce_base",
            vec![Label::new("host", "base")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .expect("the refresh pass must write the materialized catalog");
    let checkpoint_path = lane_path
        .parent()
        .expect("the lane path has a data path parent")
        .join(SERIES_INDEX_FILE_NAME);
    (storage, checkpoint_path)
}

/// C-2: the catalog sidecar rewrite is debounced off the flush critical path.
/// A publish that rewrote the multi-MB JSON + fsync inline starved compaction
/// windows under write-triggered flush.
#[test]
fn flush_publish_does_not_rewrite_catalog_inline() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let (storage, checkpoint_path) = storage_with_materialized_registry_catalog(&lane_path);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);

    let baseline_bytes = std::fs::read(&catalog_path).unwrap();
    let baseline_mtime = std::fs::metadata(&catalog_path)
        .unwrap()
        .modified()
        .unwrap();
    assert!(
        !storage
            .persisted
            .registry_catalog_dirty
            .load(std::sync::atomic::Ordering::SeqCst),
        "the materialized catalog starts clean"
    );

    storage
        .insert_rows(&[Row::with_labels(
            "catalog_debounce_publish",
            vec![Label::new("host", "publish")],
            DataPoint::new(2, 2.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();

    assert_eq!(
        std::fs::read(&catalog_path).unwrap(),
        baseline_bytes,
        "a flush publish must not rewrite the catalog sidecar inline"
    );
    assert_eq!(
        std::fs::metadata(&catalog_path)
            .unwrap()
            .modified()
            .unwrap(),
        baseline_mtime,
        "the catalog sidecar mtime must be untouched by the flush publish"
    );
    assert!(
        storage
            .persisted
            .registry_catalog_dirty
            .load(std::sync::atomic::Ordering::SeqCst),
        "the flush publish must mark the catalog dirty for the refresh pass"
    );

    storage.close().unwrap();
}

/// C-2: the refresh pass owns the debounced rewrite, after the diff is applied
/// (so the catalog describes the post-diff inventory) and from memory only.
#[test]
fn refresh_pass_writes_debounced_catalog_after_diff_application() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let (storage, checkpoint_path) = storage_with_materialized_registry_catalog(&lane_path);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);

    storage
        .insert_rows(&[Row::with_labels(
            "catalog_debounce_refresh",
            vec![Label::new("host", "refresh")],
            DataPoint::new(2, 2.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    assert!(
        storage
            .persisted
            .registry_catalog_dirty
            .load(std::sync::atomic::Ordering::SeqCst),
        "the publish must leave the catalog dirty until the refresh pass"
    );

    let guard = crate::engine::fs_utils::CountingSegmentReads::install(temp_dir.path());
    storage
        .sync_persisted_segments_from_disk_if_dirty()
        .unwrap();
    let segment_reads = crate::engine::fs_utils::count_segment_file_reads_under(temp_dir.path());
    drop(guard);

    assert_eq!(
        segment_reads, 0,
        "the debounced catalog write must be built from the in-memory inventory"
    );
    assert!(
        !storage
            .persisted
            .registry_catalog_dirty
            .load(std::sync::atomic::Ordering::SeqCst),
        "a successful debounced write clears the dirty marker"
    );

    let inventory = storage.persisted_segment_inventory();
    assert_eq!(
        inventory.entries().len(),
        2,
        "the catalog must describe the post-diff inventory"
    );
    let sources = super::super::registry_catalog::inventory_sources(&inventory);
    let validated =
        super::super::registry_catalog::validate_registry_catalog(&checkpoint_path, &sources)
            .unwrap()
            .expect("the debounced catalog must match the post-diff inventory");
    assert_eq!(
        validated.series_fingerprint.expect("catalog fingerprint"),
        super::super::registry_catalog::build_series_fingerprint_from_sources(&sources).unwrap(),
        "the memory-built fingerprint must equal the disk-derived one"
    );

    let expected = super::super::registry_catalog::build_catalog_from_memory(
        &sources,
        &storage.persisted.registry_contributions,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(&catalog_path).unwrap(),
        serde_json::to_vec_pretty(&expected).unwrap(),
        "the catalog file must contain exactly the in-memory build"
    );

    storage.close().unwrap();
}

/// C-2 trade-off: a catalog left stale by a crash between publish and the
/// debounced write must not lose data. Startup sees a fingerprint mismatch,
/// falls back to the full rebuild, and heals the sidecar.
#[test]
fn stale_catalog_falls_back_to_full_rebuild_on_startup() {
    let temp_dir = TempDir::new().unwrap();
    let metric = "stale_catalog_rebuild";
    let labels = vec![Label::new("host", "stale")];
    let expected_points = vec![DataPoint::new(1, 1.0), DataPoint::new(2, 2.0)];

    {
        let storage = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();
        storage
            .insert_rows(&[
                Row::with_labels(metric, labels.clone(), expected_points[0].clone()),
                Row::with_labels(metric, labels.clone(), expected_points[1].clone()),
            ])
            .unwrap();
        storage.close().unwrap();
    }

    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let catalog_path = super::super::registry_catalog::catalog_path(&checkpoint_path);
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    assert_eq!(
        catalog["segments"].as_array().unwrap().len(),
        1,
        "the closed store describes its one visible segment"
    );

    // Simulate the accepted C-2 crash window: the catalog lags the segments.
    catalog["segments"] = serde_json::Value::Array(Vec::new());
    std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();

    let reopened = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(2)
        .build()
        .unwrap();

    assert_eq!(
        reopened.select(metric, &labels, 0, 10).unwrap(),
        expected_points,
        "a stale catalog must fall back to the full rebuild without losing data"
    );
    assert_eq!(
        reopened
            .select_series(&SeriesSelection::new().with_metric(metric))
            .unwrap()
            .len(),
        1,
        "startup recovery must restore the complete registry"
    );

    let healed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    assert_eq!(
        healed["segments"].as_array().unwrap().len(),
        1,
        "startup recovery must heal the stale catalog sidecar"
    );

    reopened.close().unwrap();
}

/// Contributions come from segment series files only. The registry is allowed
/// to know strictly more (unflushed in-memory series) and that must never leak
/// into the cached run, otherwise the fingerprint no longer guards the catalog.
#[test]
fn contribution_is_sourced_from_segment_series_file() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = new_raw_numeric_storage(lane_path, 1);
    storage
        .insert_rows(&[
            Row::with_labels(
                "catalog_contrib",
                vec![Label::new("host", "a"), Label::new("zone", "z")],
                DataPoint::new(1, 1.0),
            ),
            Row::with_labels(
                "catalog_contrib",
                vec![Label::new("host", "b")],
                DataPoint::new(2, 2.0),
            ),
        ])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    let root = single_published_root(&storage);

    let run = storage
        .persisted
        .registry_contributions
        .run_for_root(&root)
        .expect("the published segment must contribute its series");
    assert_eq!(
        series_run_tuples(&run),
        disk_series_tuples(&root),
        "the cached run must equal a direct parse of series.bin"
    );

    // Registry-only series (written, not yet flushed) must not enter the cache.
    storage
        .insert_rows(&[Row::with_labels(
            "catalog_contrib_unflushed",
            vec![Label::new("host", "registry-only")],
            DataPoint::new(3, 3.0),
        )])
        .unwrap();
    let run_after = storage
        .persisted
        .registry_contributions
        .run_for_root(&root)
        .expect("an unflushed write must not evict the published run");
    assert!(
        series_run_tuples(&run_after)
            .iter()
            .all(|(_, metric, _)| metric != "catalog_contrib_unflushed"),
        "contributions must never be re-derived from the registry"
    );
    assert_eq!(series_run_tuples(&run_after), disk_series_tuples(&root));

    storage.close().unwrap();
}

/// E6: bootstrap priming must fill the contribution cache from `series.bin` for
/// roots the startup scan loaded without their series payload, so the first
/// catalog write after restart does not re-read segments.
#[test]
fn startup_primes_registry_contributions_from_segment_series_files() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let checkpoint_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);

    {
        let storage = new_raw_numeric_storage(lane_path.clone(), 1);
        storage
            .insert_rows(&[
                Row::with_labels(
                    "catalog_priming",
                    vec![Label::new("host", "a")],
                    DataPoint::new(1, 1.0),
                ),
                Row::with_labels(
                    "catalog_priming",
                    vec![Label::new("host", "b")],
                    DataPoint::new(2, 2.0),
                ),
            ])
            .unwrap();
        storage.flush_pipeline_once().unwrap();
        storage.close().unwrap();
    }

    // Reopen the way the fast startup path does: the registry snapshot is
    // restored, the segment indexes are loaded WITHOUT their series payload.
    let storage = ChunkStorage::new_with_data_path_and_options(
        2,
        None,
        Some(lane_path.clone()),
        None,
        2,
        ChunkStorageOptions {
            retention_enforced: false,
            background_threads_enabled: false,
            ..ChunkStorageOptions::default()
        },
    )
    .unwrap();
    storage.load_tombstones_index().unwrap();
    let loaded_registry = SeriesRegistry::load_persisted_state(&checkpoint_path)
        .unwrap()
        .expect("a closed store persists its registry snapshot");
    storage
        .replace_registry_from_persisted_state(
            loaded_registry.registry,
            loaded_registry.delta_series_count,
        )
        .unwrap();
    storage
        .apply_loaded_segment_indexes(
            crate::engine::segment::load_segment_indexes_with_series(&lane_path, false).unwrap(),
            true,
        )
        .unwrap();

    let roots = {
        let index = storage.persisted.persisted_index.read();
        index.segments_by_root.keys().cloned().collect::<Vec<_>>()
    };
    assert_eq!(roots.len(), 1);
    assert!(
        storage
            .persisted
            .registry_contributions
            .run_for_root(&roots[0])
            .is_none(),
        "an index loaded without series must not fake a contribution"
    );

    let guard = crate::engine::fs_utils::CountingSegmentReads::install(temp_dir.path());
    storage.prime_registry_contributions_from_persisted_index();
    let priming_reads = crate::engine::fs_utils::count_segment_file_reads_under(temp_dir.path());
    assert!(
        priming_reads > 0,
        "priming must read through the instrumented segment read path"
    );
    drop(guard);

    for root in &roots {
        let run = storage
            .persisted
            .registry_contributions
            .run_for_root(root)
            .expect("bootstrap priming must fill every persisted root");
        assert_eq!(series_run_tuples(&run), disk_series_tuples(root));
    }
    assert!(
        storage
            .persisted
            .registry_contributions
            .run_for_root(&roots[0])
            .is_some(),
        "a failed-free prime leaves the root cached"
    );
    assert_eq!(
        storage
            .observability_snapshot()
            .health
            .registry_contribution_entries,
        2,
        "the health snapshot must expose the contribution cache footprint"
    );

    // A catalog write after priming costs no segment reads: entries come from
    // the persisted index and the fingerprint from the cache.
    std::fs::remove_file(super::super::registry_catalog::catalog_path(
        &checkpoint_path,
    ))
    .unwrap();
    let guard = crate::engine::fs_utils::CountingSegmentReads::install(temp_dir.path());
    storage.checkpoint_series_registry_index().unwrap();
    let catalog_reads = crate::engine::fs_utils::count_segment_file_reads_under(temp_dir.path());
    drop(guard);
    assert_eq!(
        catalog_reads, 0,
        "a primed catalog write must not read any segment file"
    );
    assert!(super::super::registry_catalog::catalog_path(&checkpoint_path).exists());

    storage.close().unwrap();
}

/// The contribution cache is bounded: a footprint above the ceiling falls back
/// to the disk path for that write, so an over-priced cache can never produce a
/// fingerprint that disagrees with the segments.
#[test]
fn over_budget_contribution_falls_back_to_the_disk_fingerprint() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);

    let storage = new_raw_numeric_storage(lane_path, 1);
    storage
        .insert_rows(&[Row::with_labels(
            "catalog_footprint",
            vec![Label::new("host", "a")],
            DataPoint::new(1, 1.0),
        )])
        .unwrap();
    storage.flush_pipeline_once().unwrap();
    let root = single_published_root(&storage);
    let series_id = crate::engine::segment::load_segment_series_metadata(&root)
        .unwrap()
        .first()
        .expect("the published segment carries its series")
        .series_id;

    let source =
        super::super::registry_catalog::inventory_sources(&storage.persisted_segment_inventory())
            .into_iter()
            .next()
            .expect("the persisted index describes the published segment");

    let contributions = super::super::registry_contributions::RegistryContributions::new();
    // Poison the cached key so a cache-derived fingerprint is distinguishable
    // from the disk-derived one.
    contributions.replace_root(
        &root,
        super::super::registry_contributions::SeriesRun(vec![(
            series_id,
            Arc::new(("cache_only".to_string(), Vec::new())),
        )]),
    );

    // Four copies of the same source price four entries.
    let sources = vec![source.clone(); 4];
    let at_budget =
        super::super::registry_catalog::build_series_fingerprint_from_contributions_with_budget(
            &sources,
            &contributions,
            4,
        )
        .unwrap();
    let over_budget =
        super::super::registry_catalog::build_series_fingerprint_from_contributions_with_budget(
            &sources,
            &contributions,
            3,
        )
        .unwrap();

    let cache_only = super::super::registry_catalog::fingerprint_series_fields([(
        series_id,
        "cache_only",
        [].as_slice(),
    )]);
    assert_eq!(
        at_budget, cache_only,
        "at the entry budget the cache is trusted"
    );
    assert_eq!(
        over_budget,
        super::super::registry_catalog::build_series_fingerprint_from_sources(
            std::slice::from_ref(&source),
        )
        .unwrap(),
        "over the entry budget the write re-reads the segments"
    );
    assert_ne!(over_budget, cache_only);
}
