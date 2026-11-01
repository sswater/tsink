//! Multi-delta + checkpoint repro: mimic the server (several flushes, growing
//! series set, close=checkpoint merge), then verify registry consistency.

use super::*;

#[test]
fn repro_registry_multi_delta_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);

    let index_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);

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
        // 5 flush rounds, each with NEW metrics and series (like continuous kafka ingest)
        // Rows carry the server-managed tenant label like real server writes do.
        for round in 0..5 {
            let mut rows = Vec::new();
            for m in 0..4 {
                let metric = format!("repro.m{m}.r{round}");
                for s in 0..6 {
                    // alternate numeric / blob lanes like the real workload
                    let value = if m % 2 == 0 {
                        Value::I64(s as i64)
                    } else {
                        Value::Bytes(vec![0x41u8 + s as u8; 16])
                    };
                    rows.push(Row::with_labels(
                        metric.clone(),
                        vec![
                            Label::new("d", format!("D{s}")),
                            Label::new("__tsink_tenant__", "default"),
                        ],
                        DataPoint::new(
                            now + (round * 1000 + m * 100 + s) as i64,
                            value,
                        ),
                    ));
                }
            }
            storage.insert_rows(&rows).unwrap();
            storage.flush().unwrap();
            let deltas = std::fs::read_dir(temp_dir.path().join("series_index.delta.d"))
                .map(|d| d.count())
                .unwrap_or(0);
            println!("round {round}: delta files now {deltas}");
        }
        storage.close().unwrap();
    }

    // after close: delta dir should be merged into the checkpoint
    let has_delta_dir = temp_dir.path().join("series_index.delta.d").exists();
    println!("after close: delta dir exists = {has_delta_dir}");

    let loaded = SeriesRegistry::load_persisted_state(&index_path).unwrap();
    let loaded = loaded.expect("registry must be loadable after close");
    let registry = &loaded.registry;
    println!(
        "loaded: series_count={} delta_count={}",
        registry.series_count(),
        loaded.delta_series_count
    );

    let mut inconsistent = 0usize;
    let mut missing_metrics = Vec::new();
    for series_id in registry.all_series_ids() {
        let Some(key) = registry.decode_series_key(series_id) else {
            inconsistent += 1;
            continue;
        };
        if registry.metric_id(&key.metric).is_none() {
            inconsistent += 1;
            if missing_metrics.len() < 5 {
                missing_metrics.push(key.metric.clone());
            }
        }
    }
    println!(
        "inconsistent definitions: {inconsistent} missing_metrics={missing_metrics:?}"
    );

    // reopen through the library and read every series back
    let _ = registry;
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

    // restore registry the way hydrate does
    if let Some(loaded) = SeriesRegistry::load_persisted_state(&index_path).unwrap() {
        storage
            .replace_registry_from_persisted_state(loaded.registry, loaded.delta_series_count)
            .unwrap();
    }

    let labels = vec![Label::new("d", "D0")];
    for m in 0..5 {
        let metric = format!("repro.m{m}.r{}", m); // rounds 0..4 for m0..3, m4 never written
        let asc = storage.select_asc(&metric, &labels, 0, i64::MAX, 1000, None);
        println!("reopen select_asc {metric} = {:?}", asc.map(|p| p.len()));
    }
    storage.close().unwrap();

    assert_eq!(inconsistent, 0, "multi-delta checkpoint corrupted the registry");
}
