//! Multi-generation repro: open (full hydrate) → read previous gens → write →
//! close, repeated. Data from earlier generations must stay readable.

use std::sync::Arc;

use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision, Value};

#[test]
fn repro_multi_generation_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    for gen in 0..4 {
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_data_path(data_dir.clone())
            .with_chunk_points(4)
            .build()
            .unwrap();

        // read all previous generations' data
        for prev in 0..gen {
            for m in 0..4 {
                let metric = format!("gen{prev}.m{m}");
                let points = storage
                    .select_asc(
                        &metric,
                        &[Label::new("d", "D0"), Label::new("__tsink_tenant__", "default")],
                        0,
                        i64::MAX,
                        1000,
                        None,
                    )
                    .unwrap();
                println!(
                    "gen {gen}: read {metric} -> {} points (expect 1)",
                    points.len()
                );
                assert_eq!(
                    points.len(),
                    1,
                    "gen {gen}: data from gen {prev} metric {metric} became unreadable"
                );
            }
        }

        let now: i64 = 1_793_500_000_000 + gen as i64 * 10_000_000;
        let mut rows = Vec::new();
        for m in 0..4 {
            for s in 0..6 {
                let value = if m % 2 == 0 {
                    Value::I64((gen * 100 + s) as i64)
                } else {
                    Value::Bytes(vec![0x50u8 + s as u8; 16])
                };
                rows.push(Row::with_labels(
                    format!("gen{gen}.m{m}"),
                    vec![
                        Label::new("d", format!("D{s}")),
                        Label::new("__tsink_tenant__", "default"),
                    ],
                    DataPoint::new(now + m as i64 * 100 + s as i64, value),
                ));
            }
        }
        storage.insert_rows(&rows).unwrap();
        storage.close().unwrap();
    }
}
