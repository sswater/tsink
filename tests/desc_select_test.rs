use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;
use tsink::{
    DataPoint, Label, Row, SeriesSelection, Storage, StorageBuilder, TimestampPrecision,
    TsinkError, Value,
};

fn build_storage(temp_dir: &TempDir, chunk_points: usize, partition: Duration) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(temp_dir.path())
        .with_chunk_points(chunk_points)
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_partition_duration(partition)
        .build()
        .unwrap()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
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

    let err = storage
        .select_desc("desc_window", &[], 111, 111, 10)
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::InvalidTimeRange {
            start: 111,
            end: 111
        }
    ));
    storage.close().unwrap();
}

#[test]
fn desc_orders_by_latest_in_window_ts_across_straddling_chunk() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    // The 4th append seals a chunk that spans the query end boundary; the 500
    // point that arrives afterwards stays in the active head.
    for ts in [100i64, 200, 300, 10_000] {
        storage
            .insert_rows(&[Row::new("desc_straddle", DataPoint::new(ts, ts as f64))])
            .unwrap();
    }
    storage
        .insert_rows(&[Row::new("desc_straddle", DataPoint::new(500, 500.0))])
        .unwrap();

    let points = storage
        .select_desc("desc_straddle", &[], 0, 600, 10)
        .unwrap();
    assert_eq!(timestamps(&points), vec![500, 300, 200, 100]);
    assert_strictly_descending(&points);

    let newest = storage
        .select_desc("desc_straddle", &[], 0, 600, 1)
        .unwrap();
    assert_eq!(timestamps(&newest), vec![500]);
    storage.close().unwrap();
}

#[test]
fn desc_applies_retention_filter_and_limits_visible_points() {
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
            Row::new("desc_retention", DataPoint::new(old, 0.0)),
            Row::new("desc_retention", DataPoint::new(now - 200, 2.0)),
            Row::new("desc_retention", DataPoint::new(now - 100, 3.0)),
            Row::new("desc_retention", DataPoint::new(now, 4.0)),
        ])
        .unwrap();

    // Let the retention cutoff advance past the oldest point.
    std::thread::sleep(Duration::from_millis(1_200));

    let points = storage
        .select_desc("desc_retention", &[], old - 10, now + 1_000, 10)
        .unwrap();
    assert_eq!(timestamps(&points), vec![now, now - 100, now - 200]);

    let limited = storage
        .select_desc("desc_retention", &[], old - 10, now + 1_000, 2)
        .unwrap();
    assert_eq!(timestamps(&limited), vec![now, now - 100]);
    storage.close().unwrap();
}

#[test]
fn desc_skips_tombstoned_range() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    let rows = (100..=110i64)
        .map(|ts| Row::new("desc_tombstone", DataPoint::new(ts, ts as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let deleted = storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("desc_tombstone")
                .with_time_range(105, 108),
        )
        .unwrap();
    assert_eq!(deleted.matched_series, 1);

    let points = storage
        .select_desc("desc_tombstone", &[], 0, 200, 100)
        .unwrap();
    assert_eq!(
        timestamps(&points),
        vec![110, 109, 108, 104, 103, 102, 101, 100]
    );
    storage.close().unwrap();
}

#[test]
fn desc_matches_exact_labels_only() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    storage
        .insert_rows(&[
            Row::with_labels(
                "desc_labels",
                vec![Label::new("host", "a")],
                DataPoint::new(100, 1.0),
            ),
            Row::with_labels(
                "desc_labels",
                vec![Label::new("host", "a")],
                DataPoint::new(101, 2.0),
            ),
            Row::with_labels(
                "desc_labels",
                vec![Label::new("host", "a")],
                DataPoint::new(102, 3.0),
            ),
            Row::with_labels(
                "desc_labels",
                vec![Label::new("host", "b")],
                DataPoint::new(200, 9.0),
            ),
        ])
        .unwrap();

    let points = storage
        .select_desc("desc_labels", &[Label::new("host", "a")], 0, 500, 100)
        .unwrap();
    assert_eq!(timestamps(&points), vec![102, 101, 100]);
    storage.close().unwrap();
}

#[test]
fn desc_overlapping_sealed_and_late_active_same_partition() {
    let temp_dir = TempDir::new().unwrap();
    let storage = build_storage(&temp_dir, 4, Duration::from_secs(3600));

    // 4th point seals [500,1000,1001,1002]; the late 900/950 writes land in the
    // same partition's active head and overlap the sealed chunk's range.
    storage
        .insert_rows(
            &[500i64, 1000, 1001, 1002]
                .into_iter()
                .map(|ts| Row::new("desc_overlap", DataPoint::new(ts, ts as f64)))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    storage
        .insert_rows(&[
            Row::new("desc_overlap", DataPoint::new(900, 900.0)),
            Row::new("desc_overlap", DataPoint::new(950, 950.0)),
        ])
        .unwrap();

    let points = storage
        .select_desc("desc_overlap", &[], 0, 2_000, 5)
        .unwrap();
    assert_eq!(timestamps(&points), vec![1002, 1001, 1000, 950, 900]);
    assert_strictly_descending(&points);

    let newest = storage
        .select_desc("desc_overlap", &[], 0, 2_000, 2)
        .unwrap();
    assert_eq!(timestamps(&newest), vec![1002, 1001]);
    storage.close().unwrap();
}

#[test]
fn desc_tombstoned_tail_refills_from_same_chunk() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = build_storage(&temp_dir, 256, Duration::from_secs(3600));
        let rows = (100..=200i64)
            .map(|ts| Row::new("desc_refill", DataPoint::new(ts, ts as f64)))
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = build_storage(&temp_dir, 256, Duration::from_secs(3600));
    storage
        .delete_series(
            &SeriesSelection::new()
                .with_metric("desc_refill")
                .with_time_range(196, 201),
        )
        .unwrap();

    let points = storage
        .select_desc("desc_refill", &[], 0, 1_000, 5)
        .unwrap();
    assert_eq!(timestamps(&points), vec![195, 194, 193, 192, 191]);
    storage.close().unwrap();
}

#[test]
fn desc_crash_replay_partial_overlap() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = build_storage(&temp_dir, 32, Duration::from_secs(3600));
        let rows = (100..=110i64)
            .map(|ts| Row::new("desc_replay", DataPoint::new(ts, ts as f64)))
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = build_storage(&temp_dir, 32, Duration::from_secs(3600));
    // Simulate crash replay re-publishing the newest tail in the active head.
    storage
        .insert_rows(
            &[108i64, 109, 110]
                .into_iter()
                .map(|ts| Row::new("desc_replay", DataPoint::new(ts, ts as f64)))
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let points = storage.select_desc("desc_replay", &[], 0, 200, 11).unwrap();
    assert_eq!(
        timestamps(&points),
        (100..=110i64).rev().collect::<Vec<_>>()
    );
    storage.close().unwrap();
}

#[test]
fn desc_persisted_straddling_end_peek() {
    let temp_dir = TempDir::new().unwrap();
    {
        let storage = build_storage(&temp_dir, 32, Duration::from_secs(3600));
        let rows = (100..=110i64)
            .map(|ts| Row::new("desc_peek", DataPoint::new(ts, ts as f64)))
            .collect::<Vec<_>>();
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = build_storage(&temp_dir, 32, Duration::from_secs(3600));
    let points = storage.select_desc("desc_peek", &[], 0, 105, 5).unwrap();
    assert_eq!(timestamps(&points), vec![104, 103, 102, 101, 100]);

    let newest = storage.select_desc("desc_peek", &[], 0, 105, 3).unwrap();
    assert_eq!(timestamps(&newest), vec![104, 103, 102]);
    storage.close().unwrap();
}
