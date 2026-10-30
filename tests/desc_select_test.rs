use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tsink::{DataPoint, Row, Storage, StorageBuilder, TimestampPrecision, Value};

fn build_storage(temp_dir: &TempDir, chunk_points: usize, partition: Duration) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(chunk_points)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_partition_duration(partition)
        .build()
        .unwrap()
}

fn timestamps(points: &[DataPoint]) -> Vec<i64> {
    points.iter().map(|point| point.timestamp).collect()
}

fn assert_strictly_descending(points: &[DataPoint]) {
    assert!(
        points
            .windows(2)
            .all(|window| window[0].timestamp > window[1].timestamp),
        "expected strictly descending timestamps, got {:?}",
        timestamps(points)
    );
}

#[test]
fn desc_returns_last_limit_points_descending() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    let rows = (100..=111i64)
        .map(|ts| Row::new("desc_metric", DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let points = storage.select_desc("desc_metric", &[], 0, 200, 5).unwrap();

    assert_eq!(timestamps(&points), vec![111, 110, 109, 108, 107]);
    assert_strictly_descending(&points);
    storage.close().unwrap();
}

#[test]
fn desc_across_chunk_boundary() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    let rows = (100..=119i64)
        .map(|ts| Row::new("desc_boundary", DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let points = storage
        .select_desc("desc_boundary", &[], 0, 500, 6)
        .unwrap();

    assert_eq!(timestamps(&points), vec![119, 118, 117, 116, 115, 114]);
    assert_strictly_descending(&points);
    storage.close().unwrap();
}

#[test]
fn desc_global_order_mixed_sources() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_millis(1000));

    // Older partition (p1) keeps an active head while the newer partition (p2)
    // fills a chunk and rotates it into sealed storage.
    storage
        .insert_rows(&[Row::new("desc_mixed", DataPoint::new(500, 500.0))])
        .unwrap();
    let newer = (2500..=2503i64)
        .map(|ts| Row::new("desc_mixed", DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&newer).unwrap();
    // Append to the still-active older partition after p2 has been sealed.
    storage
        .insert_rows(&[Row::new("desc_mixed", DataPoint::new(800, 800.0))])
        .unwrap();

    let points = storage
        .select_desc("desc_mixed", &[], 0, 4000, 100)
        .unwrap();

    assert_eq!(timestamps(&points), vec![2503, 2502, 2501, 2500, 800, 500]);
    assert_strictly_descending(&points);
    assert!(points.iter().any(|point| point.timestamp == 800));
    assert!(points.iter().any(|point| point.timestamp == 2503));
    storage.close().unwrap();
}

#[test]
fn duplicate_ts_collapsed_to_newest_source() {
    let temp_dir = TempDir::new().unwrap();

    {
        let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));
        storage
            .insert_rows(&[Row::new("desc_duplicate", DataPoint::new(100, 1.0))])
            .unwrap();
        // Closing persists the first sample, so the duplicate lives in the
        // persisted source when the storage reopens.
        storage.close().unwrap();
    }

    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));
    storage
        .insert_rows(&[Row::new("desc_duplicate", DataPoint::new(100, 2.0))])
        .unwrap();

    let points = storage
        .select_desc("desc_duplicate", &[], 0, 200, 10)
        .unwrap();

    assert_eq!(points.len(), 1, "duplicate timestamps must collapse");
    assert_eq!(points[0].timestamp, 100);
    assert_eq!(points[0].value, Value::F64(2.0));
    storage.close().unwrap();
}

#[test]
fn desc_respects_end_exclusive_and_empty_window() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    let rows = (100..=111i64)
        .map(|ts| Row::new("desc_window", DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let points = storage
        .select_desc("desc_window", &[], 0, 108, 100)
        .unwrap();
    assert_eq!(timestamps(&points), (100..108i64).rev().collect::<Vec<_>>());

    let before_data = storage
        .select_desc("desc_window", &[], 0, 100, 100)
        .unwrap();
    assert!(before_data.is_empty());

    let disjoint = storage
        .select_desc("desc_window", &[], 200, 300, 100)
        .unwrap();
    assert!(disjoint.is_empty());
    storage.close().unwrap();
}
