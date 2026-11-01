//! Reproduction: after persist+reopen, do select_asc/select_desc see persisted
//! points? Compares against select_at_timestamps and the legacy select path.

use std::sync::Arc;

use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision, Value};

fn build_storage(temp_dir: &TempDir) -> Arc<dyn Storage> {
    StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_data_path(temp_dir.path().to_path_buf())
        .build()
        .unwrap()
}

fn bytes_value(tag: u8) -> Value {
    Value::Bytes(vec![tag; 8])
}

#[test]
fn repro_asc_desc_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let now: i64 = 1_793_500_000_000;

    {
        let storage = build_storage(&temp_dir);
        let mut rows = Vec::new();
        // bytes series
        for i in 0..10 {
            rows.push(Row::with_labels(
                "repro.bytes",
                vec![Label::new("d", "x")],
                DataPoint::new(now + i, bytes_value(0x10 + i as u8)),
            ));
        }
        // i64 series, multiple label values
        for i in 0..10 {
            rows.push(Row::with_labels(
                "repro.i64",
                vec![Label::new("d", format!("D{}", i % 3))],
                DataPoint::new(now + i, Value::I64(i as i64)),
            ));
        }
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }

    let storage = build_storage(&temp_dir);

    let asc = storage
        .select_asc("repro.bytes", &[Label::new("d", "x")], 0, i64::MAX, 1000, None)
        .unwrap();
    println!("select_asc bytes points: {}", asc.len());

    let desc = storage
        .select_desc("repro.bytes", &[Label::new("d", "x")], 0, i64::MAX, 1000, None)
        .unwrap();
    println!("select_desc bytes points: {}", desc.len());

    let legacy = storage
        .select("repro.bytes", &[Label::new("d", "x")], now, now + 100)
        .unwrap();
    println!("legacy select bytes points: {}", legacy.len());

    let at_ts = storage
        .select_at_timestamps("repro.bytes", &[Label::new("d", "x")], &[now, now + 5])
        .unwrap();
    println!("select_at_timestamps bytes points: {}", at_ts.len());

    let asc_i64 = storage
        .select_asc("repro.i64", &[Label::new("d", "D1")], 0, i64::MAX, 1000, None)
        .unwrap();
    println!("select_asc i64 D1 points: {}", asc_i64.len());

    let legacy_i64 = storage
        .select("repro.i64", &[Label::new("d", "D1")], now, now + 100)
        .unwrap();
    println!("legacy select i64 D1 points: {}", legacy_i64.len());

    assert_eq!(asc.len(), 10, "select_asc must see persisted bytes points");
    assert_eq!(desc.len(), 10, "select_desc must see persisted bytes points");
    assert_eq!(legacy.len(), 10, "legacy select must see persisted bytes points");
    assert_eq!(at_ts.len(), 2);
    assert_eq!(asc_i64.len(), 3, "select_asc must see persisted i64 points");
    assert_eq!(legacy_i64.len(), 3);
    storage.close().unwrap();
}
