use super::*;
use crate::engine::compactor::CompactionInventorySource;
use std::time::Instant;

pub(in crate::engine::storage_engine) struct StorageAssemblyResources {
    pub(in crate::engine::storage_engine) series_index_path: Option<PathBuf>,
    pub(in crate::engine::storage_engine) next_segment_id: Arc<AtomicU64>,
    pub(in crate::engine::storage_engine) numeric_compactor: Option<Compactor>,
    pub(in crate::engine::storage_engine) blob_compactor: Option<Compactor>,
    pub(in crate::engine::storage_engine) compaction_inventory_source: CompactionInventorySource,
    pub(in crate::engine::storage_engine) lifecycle: Arc<AtomicU8>,
    pub(in crate::engine::storage_engine) compaction_lock: Arc<Mutex<()>>,
    pub(in crate::engine::storage_engine) compaction_thread: Option<std::thread::JoinHandle<()>>,
    pub(in crate::engine::storage_engine) persisted_index_dirty: Arc<AtomicBool>,
    pub(in crate::engine::storage_engine) pending_persisted_segment_diff:
        Arc<Mutex<PendingPersistedSegmentDiff>>,
    pub(in crate::engine::storage_engine) observability: Arc<StorageObservabilityCounters>,
}

#[derive(Debug, Clone, Default)]
pub(in crate::engine::storage_engine) struct RemoteCatalogRefreshState {
    pub(in crate::engine::storage_engine) last_successful_refresh: Option<Instant>,
    pub(in crate::engine::storage_engine) consecutive_failures: u32,
    pub(in crate::engine::storage_engine) next_retry_at: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub(in crate::engine) struct PendingPersistedSegmentDiff {
    pub(in crate::engine) added_roots: BTreeSet<PathBuf>,
    pub(in crate::engine) removed_roots: BTreeSet<PathBuf>,
}

impl PendingPersistedSegmentDiff {
    pub(in crate::engine) fn is_empty(&self) -> bool {
        self.added_roots.is_empty() && self.removed_roots.is_empty()
    }

    pub(in crate::engine) fn record_changes<I, J>(&mut self, added_roots: I, removed_roots: J)
    where
        I: IntoIterator<Item = PathBuf>,
        J: IntoIterator<Item = PathBuf>,
    {
        for root in added_roots {
            self.removed_roots.remove(&root);
            self.added_roots.insert(root);
        }
        for root in removed_roots {
            self.added_roots.remove(&root);
            self.removed_roots.insert(root);
        }
    }

    pub(in crate::engine) fn merge(&mut self, other: Self) {
        self.record_changes(other.added_roots, other.removed_roots);
    }
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn prepare_construction_resources(
        chunk_point_cap: usize,
        numeric_lane_path: Option<&PathBuf>,
        blob_lane_path: Option<&PathBuf>,
        next_segment_id: u64,
        options: &ChunkStorageOptions,
    ) -> Result<StorageAssemblyResources> {
        let series_index_path = Self::series_index_path_for_lanes(
            numeric_lane_path.map(|path| path.as_path()),
            blob_lane_path.map(|path| path.as_path()),
        );
        let next_segment_id = Arc::new(AtomicU64::new(next_segment_id.max(1)));
        let compaction_inventory_source = CompactionInventorySource::default();
        let compaction_lock = Arc::new(Mutex::new(()));
        let numeric_compactor = numeric_lane_path.map(|path| {
            Compactor::new_with_inventory_source(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
                compaction_inventory_source.clone(),
            )
            .with_gate(Arc::clone(&compaction_lock))
        });
        let blob_compactor = blob_lane_path.map(|path| {
            Compactor::new_with_inventory_source(
                path,
                chunk_point_cap,
                Arc::clone(&next_segment_id),
                compaction_inventory_source.clone(),
            )
            .with_gate(Arc::clone(&compaction_lock))
        });
        let lifecycle = Arc::new(AtomicU8::new(STORAGE_OPEN));
        let persisted_index_dirty = Arc::new(AtomicBool::new(false));
        let pending_persisted_segment_diff =
            Arc::new(Mutex::new(PendingPersistedSegmentDiff::default()));
        let observability = Arc::new(StorageObservabilityCounters::default());
        let compaction_thread = if options.background_threads_enabled {
            Self::spawn_background_compaction_thread(
                Arc::downgrade(&lifecycle),
                numeric_compactor.clone(),
                blob_compactor.clone(),
                Arc::clone(&persisted_index_dirty),
                Arc::clone(&pending_persisted_segment_diff),
                options.compaction_interval,
                Arc::clone(&observability),
                options.background_fail_fast,
            )?
        } else {
            None
        };

        Ok(StorageAssemblyResources {
            series_index_path,
            next_segment_id,
            numeric_compactor,
            blob_compactor,
            compaction_inventory_source,
            lifecycle,
            compaction_lock,
            compaction_thread,
            persisted_index_dirty,
            pending_persisted_segment_diff,
            observability,
        })
    }

    fn series_index_path_for_lanes(
        numeric_lane_path: Option<&Path>,
        blob_lane_path: Option<&Path>,
    ) -> Option<PathBuf> {
        numeric_lane_path
            .and_then(|path| {
                path.parent()
                    .map(|parent| parent.join(SERIES_INDEX_FILE_NAME))
            })
            .or_else(|| {
                blob_lane_path.and_then(|path| {
                    path.parent()
                        .map(|parent| parent.join(SERIES_INDEX_FILE_NAME))
                })
            })
    }
}

#[cfg(test)]
mod pending_diff_tests {
    use super::*;

    const ROOT: &str = "lane_numeric/segments/L1/seg-0000000000000042";

    /// The same-pass L0→L1→L2 chain hands the intermediate L1 root to two
    /// different windows: the L0→L1 window adds it, the L1→L2 window removes it.
    ///
    /// The removed side must win, otherwise the persisted index keeps a
    /// catalog entry for a segment this pass already deleted.
    #[test]
    fn removal_wins_for_a_root_added_by_an_earlier_window() {
        let root = PathBuf::from(ROOT);
        let mut diff = PendingPersistedSegmentDiff::default();

        diff.record_changes([root.clone()], std::iter::empty());
        assert_eq!(diff.added_roots, BTreeSet::from([root.clone()]));

        diff.record_changes(std::iter::empty(), [root.clone()]);
        assert!(
            diff.added_roots.is_empty(),
            "a removed root must leave the added side: {:?}",
            diff.added_roots
        );
        assert_eq!(diff.removed_roots, BTreeSet::from([root]));
    }

    /// Within one call the ordering is explicit: every added root is inserted
    /// first, then every removed root, so a root named in both lists ends up
    /// removed.
    #[test]
    fn within_one_call_removed_wins_over_added_for_the_same_root() {
        let root = PathBuf::from(ROOT);
        let mut diff = PendingPersistedSegmentDiff::default();

        diff.record_changes([root.clone()], [root.clone()]);

        assert!(diff.added_roots.is_empty());
        assert_eq!(diff.removed_roots, BTreeSet::from([root]));
    }
}
