//! High-fidelity repro: public StorageBuilder (WAL on, data path), continuous
//! inserts from a writer thread, select_asc polled continuously. Catches the
//! moment visibility drops.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tempfile::TempDir;
use tsink::{DataPoint, Label, Row, StorageBuilder, TimestampPrecision, Value};

#[test]
fn repro_visibility_under_continuous_writes() {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .with_data_path(temp_dir.path().to_path_buf())
        .with_chunk_points(4)
        .with_retention(std::time::Duration::from_secs(14 * 24 * 3600))
        .build()
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_writer = Arc::clone(&stop);
    let storage_writer = Arc::clone(&storage);
    let writer = std::thread::spawn(move || {
        let mut i = 0i64;
        let base: i64 = 1_793_500_000_000;
        while !stop_writer.load(Ordering::Relaxed) {
            let mut rows = Vec::new();
            for d in 0..8 {
                rows.push(Row::with_labels(
                    "repro.stream",
                    vec![Label::new("d", format!("D{d}"))],
                    DataPoint::new(base + i, Value::I64(i)),
                ));
            }
            if storage_writer.insert_rows(&rows).is_err() {
                break;
            }
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        i
    });

    let start = std::time::Instant::now();
    let deadline = start + std::time::Duration::from_secs(45);
    let mut consecutive_zero = 0;
    let mut polls = 0;
    while std::time::Instant::now() < deadline {
        // scan one series with strict label match
        let points = storage
            .select_asc(
                "repro.stream",
                &[Label::new("d", "D0")],
                0,
                i64::MAX,
                100000,
                None,
            )
            .unwrap();
        polls += 1;
        if points.is_empty() {
            consecutive_zero += 1;
            println!(
                "t={:?} VISIBILITY LOST (poll #{polls}, consecutive={consecutive_zero})",
                start.elapsed()
            );
            if consecutive_zero >= 3 {
                break;
            }
        } else {
            if consecutive_zero > 0 || polls % 20 == 0 {
                println!(
                    "t={:?} poll #{polls}: {} points",
                    start.elapsed(),
                    points.len()
                );
            }
            consecutive_zero = 0;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    stop.store(true, Ordering::Relaxed);
    let written = writer.join().unwrap();
    println!("writer inserted {written} batches; polls={polls}");
    if consecutive_zero >= 3 {
        panic!("select_asc lost visibility under continuous writes");
    }
    storage.close().unwrap();
}
