//! Close-with-compaction repro: enough volume to force L0->L1 compaction at
//! close (compact_until_settled), mixed lanes, then reopen and read ALL data.

use std::sync::Arc;

use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision, Value};

#[test]
fn repro_close_compaction_volume() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    {
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_data_path(data_dir.clone())
            .with_chunk_points(4)
            .build()
            .unwrap();

        let now: i64 = 1_793_500_000_000;
        // 40 metrics x 5 series x 20 points = 4000 rows, mixed lanes
        for m in 0..40 {
            for s in 0..5 {
                let mut rows = Vec::new();
                for i in 0..20 {
                    let value = if m % 2 == 0 {
                        Value::I64((m * 1000 + s * 100 + i) as i64)
                    } else {
                        Value::Bytes(vec![0x60u8 + (i % 32) as u8; 24])
                    };
                    rows.push(Row::with_labels(
                        format!("vol.m{m:02}"),
                        vec![
                            Label::new("d", format!("D{s}")),
                            Label::new("__tsink_tenant__", "default"),
                        ],
                        DataPoint::new(now + (m * 1000 + s * 100 + i) as i64, value),
                    ));
                }
                storage.insert_rows(&rows).unwrap();
            }
        }
        storage.close().unwrap();
    }

    // reopen and read EVERYTHING back
    let storage: Arc<dyn Storage> = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_data_path(data_dir.clone())
        .with_chunk_points(4)
        .build()
        .unwrap();

    let mut missing = Vec::new();
    for m in 0..40 {
        for s in 0..5 {
            let metric = format!("vol.m{m:02}");
            let labels = vec![
                Label::new("d", format!("D{s}")),
                Label::new("__tsink_tenant__", "default"),
            ];
            let points = storage.select_asc(&metric, &labels, 0, i64::MAX, 1000, None).unwrap();
            if points.len() != 20 {
                missing.push(format!("{metric}/D{s}: {}", points.len()));
            }
        }
    }
    if missing.is_empty() {
        println!("ALL 200 series readable with 20 points each");
    } else {
        println!("MISSING/SHORT ({}): {:?}", missing.len(), missing);
    }
    assert!(missing.is_empty(), "data lost across close: {}", missing.len());
    storage.close().unwrap();
}
