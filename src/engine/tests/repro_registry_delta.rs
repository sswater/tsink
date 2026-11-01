//! Minimal repro: does the registry DELTA (written during flush) round-trip
//! consistently through load_persisted_state?

use super::*;

#[test]
fn repro_registry_delta_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
    let blob_lane_path = temp_dir.path().join(BLOB_LANE_ROOT);

    let storage = ChunkStorage::new_with_data_path_and_options(
        4,
        None,
        Some(lane_path.clone()),
        Some(blob_lane_path),
        1,
        base_storage_test_options(TimestampPrecision::Milliseconds, None),
    )
    .unwrap();

    let now: i64 = 1_793_500_000_000;
    let mut rows = Vec::new();
    for i in 0..24 {
        rows.push(Row::with_labels(
            "repro.delta.metric",
            vec![Label::new("d", "x")],
            DataPoint::new(now + i, Value::I64(i)),
        ));
    }
    storage.insert_rows(&rows).unwrap();

    let index_path = temp_dir.path().join(SERIES_INDEX_FILE_NAME);
    let before = SeriesRegistry::load_persisted_state(&index_path).unwrap();
    println!(
        "before flush: persisted registry = {:?}",
        before.map(|l| l.registry.series_count())
    );

    storage.flush().unwrap();

    // delta dir contents
    let delta_dir = temp_dir.path().join("series_index.delta.d");
    if delta_dir.exists() {
        println!(
            "delta dir entries: {:?}",
            std::fs::read_dir(&delta_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect::<Vec<_>>()
        );
    } else {
        println!("no delta dir after flush");
    }
    let bin_size = std::fs::metadata(&index_path).map(|m| m.len()).ok();
    println!("series_index.bin size after flush: {bin_size:?}");

    let loaded = SeriesRegistry::load_persisted_state(&index_path).unwrap();
    if let Some(loaded) = loaded {
        let registry = &loaded.registry;
        println!(
            "after flush: loaded series_count={} metric_id(repro.delta.metric)={:?}",
            registry.series_count(),
            registry.metric_id("repro.delta.metric")
        );
        let resolve = registry.resolve_existing("repro.delta.metric", &[Label::new("d", "x")]);
        println!("resolve_existing = {resolve:?}");

        // internal consistency: every by_id definition must decode and re-resolve
        let mut inconsistent = 0usize;
        for series_id in registry.all_series_ids() {
            let Some(key) = registry.decode_series_key(series_id) else {
                inconsistent += 1;
                continue;
            };
            if registry.metric_id(&key.metric).is_none() {
                inconsistent += 1;
                if inconsistent <= 3 {
                    println!(
                        "INCONSISTENT: series_id={series_id} metric={:?} missing from metric dict",
                        key.metric
                    );
                }
            }
        }
        println!("inconsistent definitions: {inconsistent}");
        assert_eq!(inconsistent, 0, "registry delta round-trip is inconsistent");
    } else {
        println!("after flush: NO persisted registry loaded");
    }
    storage.close().unwrap();
}
