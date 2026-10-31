use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use super::analysis::{analyze_series_read_sources, SeriesReadAnalysis};
use super::merge::{pop_next_point_from_sources, QueryMergeCursor, QueryMergeSourceRef};
use super::pagination::{RawSeriesPagination, SortedSeriesDedupeMode, SortedSeriesPageCollector};
use super::*;
use crate::engine::chunk::ChunkHeader;
use crate::{ValuePredicate, ValuePredicateOp, ValuePredicateTarget};

fn default_future_skew_window(precision: TimestampPrecision) -> i64 {
    super::super::duration_to_timestamp_units(
        super::super::DEFAULT_FUTURE_SKEW_ALLOWANCE,
        precision,
    )
}

fn test_options(current_time_override: Option<i64>) -> ChunkStorageOptions {
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
        write_timeout: Duration::from_secs(2),
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
        current_time_override,
    }
}

fn live_storage(chunk_point_cap: usize) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        None,
        None,
        1,
        test_options(None),
    )
    .unwrap()
}

fn persistent_numeric_storage(path: &std::path::Path, chunk_point_cap: usize) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(path.join(NUMERIC_LANE_ROOT)),
        None,
        1,
        test_options(None),
    )
    .unwrap()
}

fn unsorted_chunk(series_id: SeriesId, points: &[(i64, f64)]) -> Arc<Chunk> {
    let chunk_points = points
        .iter()
        .map(|(ts, value)| ChunkPoint {
            ts: *ts,
            value: Value::F64(*value),
        })
        .collect::<Vec<_>>();
    let min_ts = points.iter().map(|(ts, _)| *ts).min().unwrap_or(i64::MIN);
    let max_ts = points.iter().map(|(ts, _)| *ts).max().unwrap_or(i64::MAX);
    Arc::new(Chunk {
        header: ChunkHeader {
            series_id,
            lane: ValueLane::Numeric,
            value_family: Some(SeriesValueFamily::F64),
            point_count: u16::try_from(points.len()).unwrap_or(u16::MAX),
            min_ts,
            max_ts,
            ts_codec: crate::engine::chunk::TimestampCodecId::DeltaOfDeltaBitpack,
            value_codec: crate::engine::chunk::ValueCodecId::GorillaXorF64,
        },
        points: chunk_points,
        encoded_payload: Vec::new(),
        wal_highwater: WalHighWatermark::default(),
    })
}

struct FakeMergeCursor {
    points: Vec<DataPoint>,
    next_idx: usize,
}

impl FakeMergeCursor {
    fn new(points: Vec<DataPoint>) -> Self {
        Self {
            points,
            next_idx: 0,
        }
    }
}

impl QueryMergeCursor for FakeMergeCursor {
    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.points.get(self.next_idx).map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        let point = self.points.get(self.next_idx).cloned();
        if point.is_some() {
            self.next_idx = self.next_idx.saturating_add(1);
        }
        Ok(point)
    }
}

#[test]
fn analyze_series_read_sources_routes_unsorted_sealed_data_to_append_sort() {
    let analysis = analyze_series_read_sources(
        &[],
        &[unsorted_chunk(7, &[(2, 2.0), (1, 1.0)])],
        &ActiveSeriesSnapshot::default(),
        0,
        10,
    );

    assert_eq!(
        analysis,
        SeriesReadAnalysis {
            estimated_points: 2,
            has_overlap: false,
            requires_output_validation: true,
            requires_timestamp_dedupe: false,
            requires_exact_dedupe: false,
            persisted_source_sorted: true,
            sealed_source_sorted: true,
        }
    );
    assert!(!analysis.can_use_merge_path());
}

#[test]
fn pop_next_point_from_sources_preserves_source_precedence_on_timestamp_ties() {
    let mut persisted =
        FakeMergeCursor::new(vec![DataPoint::new(1, 10.0), DataPoint::new(5, 50.0)]);
    let mut sealed = FakeMergeCursor::new(vec![DataPoint::new(1, 20.0), DataPoint::new(4, 40.0)]);
    let mut active = FakeMergeCursor::new(vec![DataPoint::new(1, 30.0), DataPoint::new(3, 30.5)]);

    let mut sources = [
        QueryMergeSourceRef::new(&mut persisted),
        QueryMergeSourceRef::new(&mut sealed),
        QueryMergeSourceRef::new(&mut active),
    ];
    let mut merged = Vec::new();
    while let Some(point) = pop_next_point_from_sources(&mut sources).unwrap() {
        merged.push(point);
    }

    assert_eq!(
        merged,
        vec![
            DataPoint::new(1, 10.0),
            DataPoint::new(1, 20.0),
            DataPoint::new(1, 30.0),
            DataPoint::new(3, 30.5),
            DataPoint::new(4, 40.0),
            DataPoint::new(5, 50.0),
        ]
    );
}

#[test]
fn sorted_series_page_collector_applies_pagination_after_filters_and_dedupe() {
    let tombstones = [tombstone::TombstoneRange { start: 12, end: 13 }];
    let mut collector = SortedSeriesPageCollector::new(
        Some(11),
        Some(&tombstones),
        SortedSeriesDedupeMode::Timestamp,
        RawSeriesPagination::new(1, Some(1)),
    );

    assert!(!collector.push(DataPoint::new(10, 10.0)));
    assert!(!collector.push(DataPoint::new(11, 11.0)));
    assert!(!collector.push(DataPoint::new(11, 11.5)));
    assert!(!collector.push(DataPoint::new(12, 12.0)));
    assert!(!collector.push(DataPoint::new(13, 13.0)));
    collector.finish();

    assert_eq!(collector.final_rows_seen(), 2);
    assert_eq!(collector.into_points(), vec![DataPoint::new(13, 13.0)]);
}

#[test]
fn snapshot_in_memory_series_sources_release_shard_locks_without_select_wrapper() {
    let storage = Arc::new(live_storage(512));

    let query_metric = "snapshot_query_metric";
    let query_labels = vec![Label::new("host", "query")];
    let rows = (0..192i64)
        .map(|ts| {
            Row::with_labels(
                query_metric,
                query_labels.clone(),
                DataPoint::new(ts, ts as f64),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let query_series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing(query_metric, &query_labels)
        .unwrap()
        .series_id;
    let target_shard = ChunkStorage::series_shard_idx(query_series_id);

    let writer_metric = "snapshot_writer_metric";
    let writer_labels = (0..1024usize)
        .find_map(|idx| {
            let labels = vec![Label::new("host", format!("writer-{idx}"))];
            storage
                .insert_rows(&[Row::with_labels(
                    writer_metric,
                    labels.clone(),
                    DataPoint::new(10_000, 10_000.0),
                )])
                .unwrap();
            let series_id = storage
                .catalog
                .registry
                .read()
                .resolve_existing(writer_metric, &labels)
                .unwrap()
                .series_id;
            (ChunkStorage::series_shard_idx(series_id) == target_shard).then_some(labels)
        })
        .expect("expected same-shard writer series");

    let snapshot_storage = Arc::clone(&storage);
    let (snapshot_ready_tx, snapshot_ready_rx) = mpsc::channel();
    let (snapshot_release_tx, snapshot_release_rx) = mpsc::channel();
    let snapshot_thread = thread::spawn(move || {
        let (snapshot, stats) =
            snapshot_storage.snapshot_in_memory_series_sources(query_series_id, 0, 256);
        snapshot_ready_tx
            .send((snapshot.active_point_count(), stats.snapshot_count()))
            .unwrap();
        snapshot_release_rx.recv().unwrap();
    });

    let (point_count, snapshots) = snapshot_ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("snapshot did not complete");
    assert_eq!(point_count, 192);
    assert_eq!(snapshots, 1);

    let writer_storage = Arc::clone(&storage);
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_thread = thread::spawn(move || {
        let result = (|| -> Result<()> {
            for offset in 1..=32i64 {
                writer_storage.insert_rows(&[Row::with_labels(
                    writer_metric,
                    writer_labels.clone(),
                    DataPoint::new(10_000 + offset, (10_000 + offset) as f64),
                )])?;
            }
            Ok(())
        })();
        writer_tx.send(result).unwrap();
    });

    let writer_result = writer_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("writer should progress after snapshot creation");
    assert!(writer_result.is_ok());

    snapshot_release_tx.send(()).unwrap();
    writer_thread.join().unwrap();
    snapshot_thread.join().unwrap();
}

#[test]
fn merge_and_append_sort_paths_match_for_persisted_and_active_exact_duplicates() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_storage(temp_dir.path(), 4);
    let labels = vec![Label::new("host", "equivalent")];

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
        ])
        .unwrap();
    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment_with_outcome().unwrap().persisted);

    storage
        .insert_rows(&[
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
        ])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("cpu", &labels)
        .unwrap()
        .series_id;
    let plan = TieredQueryPlan::from_cutoffs(0, 10, None, None);

    let (merge_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    assert!(merge_snapshot.analysis.can_use_merge_path());
    let mut merge_points = Vec::new();
    storage
        .execute_series_read_merge_path(series_id, 0, 10, merge_snapshot, &mut merge_points)
        .unwrap();

    let (append_snapshot, _) = storage
        .snapshot_series_read_sources(series_id, 0, 10, plan)
        .unwrap();
    let mut append_sort_points = Vec::new();
    storage
        .execute_series_read_append_sort_path(
            series_id,
            0,
            10,
            append_snapshot,
            &mut append_sort_points,
        )
        .unwrap();

    assert_eq!(merge_points, append_sort_points);
    assert_eq!(
        merge_points,
        vec![
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
        ]
    );

    storage.close().unwrap();
}

#[test]
fn inv1_decoded_points_bounded() {
    let storage = live_storage(2048);
    let rows = (0..100_000i64)
        .map(|idx| Row::new("inv1_desc", DataPoint::new(idx * 10, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let limit = 100;
    let end = 100_000i64 * 10 + 1;
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("inv1_desc", &[])
        .unwrap()
        .series_id;

    let run = |start: i64| {
        let plan = storage.query_tier_plan(start, end);
        storage
            .select_desc_for_series_with_plan(series_id, start, end, limit, None, plan)
            .unwrap()
    };

    // Both windows share the same newest edge; only the window width changes.
    let (wide_points, wide_stats) = run(0);
    let (narrow_points, narrow_stats) = run(end - 500 * 10);

    assert_eq!(wide_points.len(), limit);
    assert_eq!(narrow_points, wide_points);
    assert!(wide_stats.chunks_touched > 0);
    assert_eq!(wide_stats.chunks_touched, narrow_stats.chunks_touched);

    let wide_bound = limit + 64 * wide_stats.chunks_touched + 64;
    let narrow_bound = limit + 64 * narrow_stats.chunks_touched + 64;
    assert!(
        wide_stats.points_decoded <= wide_bound,
        "wide scan decoded {} points across {} chunks (bound {})",
        wide_stats.points_decoded,
        wide_stats.chunks_touched,
        wide_bound
    );
    assert!(
        narrow_stats.points_decoded <= narrow_bound,
        "narrow scan decoded {} points across {} chunks (bound {})",
        narrow_stats.points_decoded,
        narrow_stats.chunks_touched,
        narrow_bound
    );

    storage.close().unwrap();
}

#[test]
fn desc_predicate_filters_points_and_stops_early() {
    // Cross-block data: 200 points across 4 chunks (cap 50). The predicate
    // matches a sparse tail; early stop must survive the predicate chain.
    let storage = live_storage(50);
    let rows = (0..200i64)
        .map(|idx| Row::new("desc_pred", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let predicate = ValuePredicate {
        op: ValuePredicateOp::Lt,
        target: ValuePredicateTarget::F64(150.0),
    };
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("desc_pred", &[])
        .unwrap()
        .series_id;
    let plan = storage.query_tier_plan(0, 200);
    let (points, stats) = storage
        .select_desc_for_series_with_plan(series_id, 0, 200, 5, Some(&predicate), plan)
        .unwrap();

    assert_eq!(
        points,
        vec![
            DataPoint::new(149, 149.0),
            DataPoint::new(148, 148.0),
            DataPoint::new(147, 147.0),
            DataPoint::new(146, 146.0),
            DataPoint::new(145, 145.0),
        ]
    );
    assert!(
        stats.points_decoded < 200,
        "predicate scan should stop early, decoded {}",
        stats.points_decoded
    );

    storage.close().unwrap();
}

#[test]
fn desc_predicate_none_matches_plain_desc() {
    let storage = live_storage(50);
    let rows = (0..80i64)
        .map(|idx| Row::new("desc_pred_none", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let with_none = storage
        .select_desc_api("desc_pred_none", &[], 0, 80, 100, None)
        .unwrap();
    let mut plain_asc = storage
        .select("desc_pred_none", &[], 0, 80)
        .unwrap();
    plain_asc.reverse();

    assert_eq!(with_none, plain_asc);

    storage.close().unwrap();
}

#[test]
fn desc_predicate_rejects_latest_version_drops_timestamp() {
    let storage = live_storage(50);
    storage
        .insert_rows(&[Row::new("desc_pred_versions", DataPoint::new(5, 1.0))])
        .unwrap();
    storage
        .insert_rows(&[Row::new("desc_pred_versions", DataPoint::new(5, 2.0))])
        .unwrap();

    // Latest version (2.0) matches: the timestamp survives with the new value.
    let matches_latest = ValuePredicate {
        op: ValuePredicateOp::Gt,
        target: ValuePredicateTarget::F64(1.5),
    };
    let points = storage
        .select_desc_api("desc_pred_versions", &[], 0, 10, 10, Some(&matches_latest))
        .unwrap();
    assert_eq!(points, vec![DataPoint::new(5, 2.0)]);

    // Latest version (2.0) rejected: the timestamp drops entirely — the older
    // matching version (1.0) must NOT fall back into the output.
    let rejects_latest = ValuePredicate {
        op: ValuePredicateOp::Gt,
        target: ValuePredicateTarget::F64(3.0),
    };
    let points = storage
        .select_desc_api("desc_pred_versions", &[], 0, 10, 10, Some(&rejects_latest))
        .unwrap();
    assert!(points.is_empty());

    storage.close().unwrap();
}

#[test]
fn desc_predicate_drops_wrong_typed_values() {
    let storage = live_storage(50);
    let rows = (0..10i64)
        .map(|idx| Row::new("desc_pred_types", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    // Points carry F64; an I64 predicate never matches them.
    let predicate = ValuePredicate {
        op: ValuePredicateOp::Gt,
        target: ValuePredicateTarget::I64(0),
    };
    let points = storage
        .select_desc_api("desc_pred_types", &[], 0, 10, 100, Some(&predicate))
        .unwrap();
    assert!(points.is_empty());

    storage.close().unwrap();
}

#[test]
fn asc_scan_returns_strict_ascending_with_limit() {
    // Cross-block data: 200 points across 4 chunks (cap 50). A limit of 5
    // must return the earliest 5 in-window points in strictly ascending order.
    let storage = live_storage(50);
    let rows = (0..200i64)
        .map(|idx| Row::new("asc_scan", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let points = storage
        .select_asc_api("asc_scan", &[], 0, 200, 5, None)
        .unwrap();

    assert_eq!(
        points,
        vec![
            DataPoint::new(0, 0.0),
            DataPoint::new(1, 1.0),
            DataPoint::new(2, 2.0),
            DataPoint::new(3, 3.0),
            DataPoint::new(4, 4.0),
        ]
    );

    storage.close().unwrap();
}

#[test]
fn asc_merge_prefers_newest_version_at_equal_timestamp() {
    // Same timestamp written twice: the newest version must win the merge
    // (active over sealed over persisted, higher sequence over lower).
    let storage = live_storage(50);
    storage
        .insert_rows(&[Row::new("asc_merge", DataPoint::new(5, 1.0))])
        .unwrap();
    storage
        .insert_rows(&[Row::new("asc_merge", DataPoint::new(5, 2.0))])
        .unwrap();
    storage
        .insert_rows(&[Row::new("asc_merge", DataPoint::new(6, 3.0))])
        .unwrap();

    let points = storage.select_asc_api("asc_merge", &[], 0, 10, 10, None).unwrap();
    assert_eq!(
        points,
        vec![DataPoint::new(5, 2.0), DataPoint::new(6, 3.0)]
    );

    storage.close().unwrap();
}

#[test]
fn asc_predicate_filters_points_and_stops_early() {
    // Mirror of the desc predicate test: 200 points across 4 chunks, the
    // predicate matches a sparse head; early stop must survive the chain.
    let storage = live_storage(50);
    let rows = (0..200i64)
        .map(|idx| Row::new("asc_pred", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let predicate = ValuePredicate {
        op: ValuePredicateOp::Gt,
        target: ValuePredicateTarget::F64(50.0),
    };
    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("asc_pred", &[])
        .unwrap()
        .series_id;
    let plan = storage.query_tier_plan(0, 200);
    let (points, stats) = storage
        .select_asc_for_series_with_plan(series_id, 0, 200, 5, Some(&predicate), plan)
        .unwrap();

    assert_eq!(
        points,
        vec![
            DataPoint::new(51, 51.0),
            DataPoint::new(52, 52.0),
            DataPoint::new(53, 53.0),
            DataPoint::new(54, 54.0),
            DataPoint::new(55, 55.0),
        ]
    );
    assert!(
        stats.points_decoded < 200,
        "predicate scan should stop early, decoded {}",
        stats.points_decoded
    );

    storage.close().unwrap();
}

#[test]
fn asc_predicate_none_matches_select_desc_reversed() {
    let storage = live_storage(50);
    let rows = (0..80i64)
        .map(|idx| Row::new("asc_parity", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let asc = storage
        .select_asc_api("asc_parity", &[], 0, 80, 100, None)
        .unwrap();
    let mut desc = storage
        .select_desc_api("asc_parity", &[], 0, 80, 100, None)
        .unwrap();
    desc.reverse();

    assert_eq!(asc, desc);

    storage.close().unwrap();
}

#[test]
fn asc_and_desc_stop_early_on_limit() {
    // limit=3 over 1000 points (cap 50): decoded points must stay far below
    // the full window for both directions.
    let storage = live_storage(50);
    let rows = (0..1000i64)
        .map(|idx| Row::new("asc_early", DataPoint::new(idx, idx as f64)))
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("asc_early", &[])
        .unwrap()
        .series_id;

    let asc_plan = storage.query_tier_plan(0, 1000);
    let (_, asc_stats) = storage
        .select_asc_for_series_with_plan(series_id, 0, 1000, 3, None, asc_plan)
        .unwrap();
    let desc_plan = storage.query_tier_plan(0, 1000);
    let (_, desc_stats) = storage
        .select_desc_for_series_with_plan(series_id, 0, 1000, 3, None, desc_plan)
        .unwrap();

    assert!(
        asc_stats.points_decoded <= 3 + 64 * (asc_stats.chunks_touched + 1),
        "asc decoded {} across {} chunks",
        asc_stats.points_decoded,
        asc_stats.chunks_touched
    );
    assert!(
        desc_stats.points_decoded <= 3 + 64 * (desc_stats.chunks_touched + 1),
        "desc decoded {} across {} chunks",
        desc_stats.points_decoded,
        desc_stats.chunks_touched
    );

    storage.close().unwrap();
}

fn persistent_numeric_and_blob_storage(
    path: &std::path::Path,
    chunk_point_cap: usize,
) -> ChunkStorage {
    ChunkStorage::new_with_data_path_and_options(
        chunk_point_cap,
        None,
        Some(path.join(NUMERIC_LANE_ROOT)),
        Some(path.join(BLOB_LANE_ROOT)),
        1,
        test_options(None),
    )
    .unwrap()
}

#[test]
fn point_scan_single_decompression_per_chunk_group() {
    let temp_dir = TempDir::new().unwrap();
    // 50 clustered timestamps fit in exactly one chunk. Each value carries its
    // timestamp in every byte: highly compressible (so the persisted chunk takes
    // the zstd path) but distinct per point, so misaligned index decoding would
    // produce the wrong value.
    let storage = persistent_numeric_and_blob_storage(temp_dir.path(), 50);
    let rows = (0..50i64)
        .map(|ts| {
            Row::new(
                "point_scan_inv2",
                DataPoint::new(ts, Value::Bytes(vec![ts as u8; 512])),
            )
        })
        .collect::<Vec<_>>();
    storage.insert_rows(&rows).unwrap();
    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment_with_outcome().unwrap().persisted);

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("point_scan_inv2", &[])
        .unwrap()
        .series_id;

    // The persisted chunk must actually take the zstd/decompress path.
    {
        let persisted_index = storage.persisted.persisted_index.read();
        let chunk_ref = persisted_index.chunk_refs.get(&series_id).unwrap()[0];
        let payload = persisted_chunk_payload(&persisted_index.segment_maps, &chunk_ref).unwrap();
        assert!(
            matches!(payload, std::borrow::Cow::Owned(_)),
            "test chunk should be persisted with zstd compression"
        );
    }

    let timestamps = (0..50i64).collect::<Vec<_>>();
    let (points, stats) = storage
        .select_at_timestamps_for_series(series_id, &timestamps)
        .unwrap();

    assert_eq!(points.len(), 50);
    assert!(points
        .windows(2)
        .all(|window| window[0].timestamp < window[1].timestamp));
    for ts in [0i64, 1, 17, 33, 49] {
        let point = points
            .iter()
            .find(|point| point.timestamp == ts)
            .unwrap_or_else(|| panic!("missing point at ts {ts}"));
        assert_eq!(
            point.value,
            Value::Bytes(vec![ts as u8; 512]),
            "indexed value decode returned the wrong value at ts {ts}"
        );
    }
    assert_eq!(
        stats.chunk_groups_touched, 1,
        "INV-2: 50 clustered timestamps in one chunk must decode the chunk once"
    );

    storage.close().unwrap();
}

#[test]
fn point_scan_cross_layer_overlap_decodes_only_newest_group() {
    let temp_dir = TempDir::new().unwrap();
    let storage = persistent_numeric_and_blob_storage(temp_dir.path(), 4);

    storage
        .insert_rows(&[Row::new(
            "point_scan_overlap_inv2",
            DataPoint::new(100, Value::Bytes(vec![1])),
        )])
        .unwrap();
    storage.flush_all_active().unwrap();
    assert!(storage.persist_segment_with_outcome().unwrap().persisted);

    // Crash-replay style overlap: the same timestamp now exists in the persisted
    // layer and in a newer active head.
    storage
        .insert_rows(&[Row::new(
            "point_scan_overlap_inv2",
            DataPoint::new(100, Value::Bytes(vec![2])),
        )])
        .unwrap();

    let series_id = storage
        .catalog
        .registry
        .read()
        .resolve_existing("point_scan_overlap_inv2", &[])
        .unwrap()
        .series_id;

    let (points, stats) = storage
        .select_at_timestamps_for_series(series_id, &[100])
        .unwrap();

    assert_eq!(points, vec![DataPoint::new(100, Value::Bytes(vec![2]))]);
    assert_eq!(
        stats.chunk_groups_touched, 1,
        "the resolved timestamp must belong to exactly one group (the active layer)"
    );

    storage.close().unwrap();
}
