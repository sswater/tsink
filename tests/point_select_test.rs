use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tsink::{DataPoint, Row, Storage, StorageBuilder, TimestampPrecision, Value};

fn build_storage(temp_dir: &TempDir) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(4)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_partition_duration(Duration::from_secs(3600))
        .build()
        .unwrap()
}

#[test]
fn point_scan_returns_ascending_with_dedup_and_partial_hits() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir);
    storage
        .insert_rows(&[
            Row::new("point_scan_asc", DataPoint::new(100, Value::Bytes(vec![1]))),
            Row::new("point_scan_asc", DataPoint::new(200, Value::Bytes(vec![2]))),
            Row::new("point_scan_asc", DataPoint::new(300, Value::Bytes(vec![3]))),
        ])
        .unwrap();

    let points = storage
        .select_at_timestamps("point_scan_asc", &[], &[300, 100, 100, 999])
        .unwrap();

    assert_eq!(
        points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>(),
        vec![100, 300],
        "duplicate timestamps collapse, missing timestamps are omitted, output ascends"
    );
    assert_eq!(points[0].value, Value::Bytes(vec![1]));
    assert_eq!(points[1].value, Value::Bytes(vec![3]));
    storage.close().unwrap();
}

#[test]
fn point_scan_cross_layer_overlap_takes_newest_source() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = build_storage(&temp_dir);
        storage
            .insert_rows(&[Row::new(
                "point_scan_overlap",
                DataPoint::new(100, Value::Bytes(vec![1])),
            )])
            .unwrap();
        // Closing persists the first sample, simulating crash-replay overlap
        // where the same timestamp also exists in an older layer.
        storage.close().unwrap();
    }

    let storage = build_storage(&temp_dir);
    storage
        .insert_rows(&[Row::new(
            "point_scan_overlap",
            DataPoint::new(100, Value::Bytes(vec![2])),
        )])
        .unwrap();

    let points = storage
        .select_at_timestamps("point_scan_overlap", &[], &[100])
        .unwrap();

    assert_eq!(points.len(), 1, "duplicate timestamps must collapse to one");
    assert_eq!(points[0].timestamp, 100);
    assert_eq!(
        points[0].value,
        Value::Bytes(vec![2]),
        "the newest source (active) wins over the persisted copy"
    );
    storage.close().unwrap();
}

#[test]
fn point_scan_empty_ts_list_returns_empty() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir);
    storage
        .insert_rows(&[Row::new(
            "point_scan_empty",
            DataPoint::new(100, Value::Bytes(vec![1])),
        )])
        .unwrap();

    let points = storage
        .select_at_timestamps("point_scan_empty", &[], &[])
        .unwrap();

    assert!(points.is_empty());
    storage.close().unwrap();
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[test]
fn point_scan_applies_retention_filter() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(4)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_retention(Duration::from_secs(2))
        .build()
        .unwrap();

    let now = now_millis();
    let old = now - 1_200;
    storage
        .insert_rows(&[
            Row::new("point_scan_retention", DataPoint::new(old, 0.0)),
            Row::new("point_scan_retention", DataPoint::new(now - 200, 2.0)),
            Row::new("point_scan_retention", DataPoint::new(now - 100, 3.0)),
            Row::new("point_scan_retention", DataPoint::new(now, 4.0)),
        ])
        .unwrap();

    // Let the retention cutoff advance past the oldest point.
    std::thread::sleep(Duration::from_millis(1_200));

    let points = storage
        .select_at_timestamps(
            "point_scan_retention",
            &[],
            &[old, now - 200, now - 100, now],
        )
        .unwrap();

    assert_eq!(
        points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>(),
        vec![now - 200, now - 100, now],
        "retention-filtered timestamps are reported as missing"
    );
    storage.close().unwrap();
}

#[test]
fn point_scan_skips_tombstoned_timestamps() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir);

    let rows = (100..=110i64)
        .map(|ts| Row::new("point_scan_tombstone", DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let deleted = storage
        .delete_series(
            &tsink::SeriesSelection::new()
                .with_metric("point_scan_tombstone")
                .with_time_range(105, 108),
        )
        .unwrap();
    assert_eq!(deleted.matched_series, 1);

    let points = storage
        .select_at_timestamps("point_scan_tombstone", &[], &[104, 105, 106, 107, 108, 109])
        .unwrap();

    assert_eq!(
        points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>(),
        vec![104, 108, 109],
        "tombstoned timestamps are omitted instead of returned as placeholders"
    );
    storage.close().unwrap();
}
