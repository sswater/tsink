mod context;

use super::super::*;

impl ChunkStorage {
    pub(in super::super) fn replace_registry_from_persisted_state(
        &self,
        registry: SeriesRegistry,
        delta_series_count: usize,
    ) -> Result<()> {
        self.registry_persistence_context()
            .replace_registry_from_persisted_state(
                registry,
                self.tombstone_read_context().max_tombstoned_series_id(),
                delta_series_count,
            )
    }

    /// Catalog sources from the published persisted index.
    ///
    /// Manifest fields and per-file fingerprints travel with each source, so
    /// catalog production does not re-read segments the inventory already
    /// parsed. Take the read guard only to snapshot; never do I/O under it.
    pub(super) fn persisted_registry_catalog_sources(
        &self,
    ) -> Vec<registry_catalog::PersistedRegistryCatalogSource> {
        let persisted_index = self.persisted.persisted_index.read();
        persisted_index
            .segments_by_root
            .iter()
            .map(
                |(root, segment)| registry_catalog::PersistedRegistryCatalogSource {
                    lane: segment.lane,
                    root: root.clone(),
                    manifest: Some(segment.manifest.clone()),
                    files: segment.files,
                },
            )
            .collect()
    }

    pub(in super::super) fn persisted_registry_catalog_sources_with_root_changes(
        &self,
        added_roots: &[PathBuf],
        removed_roots: &[PathBuf],
    ) -> Result<Vec<registry_catalog::PersistedRegistryCatalogSource>> {
        let mut sources = self
            .persisted_registry_catalog_sources()
            .into_iter()
            .map(|source| (source.root.clone(), source))
            .collect::<BTreeMap<_, _>>();
        for root in removed_roots {
            sources.remove(root);
        }
        for root in added_roots {
            let (lane, _tier) = self.persisted_segment_location_for_root(root)?;
            // A root published by this pass is not in the persisted index yet,
            // so it has no inventory manifest: the catalog build pays exactly
            // one bounded manifest read for it (its series contribution was
            // already primed from the in-memory loaded segment).
            sources.insert(
                root.clone(),
                registry_catalog::PersistedRegistryCatalogSource {
                    lane,
                    root: root.clone(),
                    manifest: None,
                    files: None,
                },
            );
        }
        Ok(sources.into_values().collect())
    }

    fn persist_series_registry_catalog_index_with_sources(
        &self,
        checkpoint_path: &Path,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        if matches!(
            registry_catalog::validate_registry_catalog(checkpoint_path, sources)?,
            Some(registry_catalog::ValidatedRegistryCatalog {
                series_fingerprint: Some(_),
            })
        ) {
            return Ok(());
        }
        registry_catalog::persist_registry_catalog(
            checkpoint_path,
            sources,
            &self.persisted.registry_contributions,
        )
    }

    fn persist_series_registry_catalog_index(&self, checkpoint_path: &Path) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_catalog_index_with_sources(checkpoint_path, &sources)
    }

    fn checkpoint_series_registry_index_with_policy(
        &self,
        allow_invalid_catalog: bool,
    ) -> Result<()> {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .checkpoint_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                allow_invalid_catalog,
                |checkpoint_path| self.persist_series_registry_catalog_index(checkpoint_path),
            )
    }

    pub(in super::super) fn checkpoint_series_registry_index(&self) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(false)
    }

    pub(in super::super) fn checkpoint_series_registry_index_allow_invalid_catalog(
        &self,
    ) -> Result<()> {
        self.checkpoint_series_registry_index_with_policy(true)
    }

    pub(in super::super) fn persist_series_registry_index(&self) -> Result<()> {
        let sources = self.persisted_registry_catalog_sources();
        self.persist_series_registry_index_with_catalog_sources(&sources)
    }

    pub(in super::super) fn persist_series_registry_index_with_catalog_sources(
        &self,
        sources: &[registry_catalog::PersistedRegistryCatalogSource],
    ) -> Result<()> {
        self.persist_series_registry_index_with_catalog_persist(|checkpoint_path| {
            self.persist_series_registry_catalog_index_with_sources(checkpoint_path, sources)
        })
    }

    /// Flush-path registry persistence: the incremental registry delta is
    /// written inline (it is crash-recovery metadata), while the derived catalog
    /// sidecar is only marked dirty.
    ///
    /// Rewriting the catalog here cost a multi-megabyte JSON serialization plus
    /// an atomic file swap and parent fsync per publish; under write-triggered
    /// flush that starved the compaction windows. The refresh thread rewrites it
    /// from the in-memory inventory once the published segments are in the
    /// persisted index (C-2).
    pub(in super::super) fn persist_series_registry_index_deferring_catalog(&self) -> Result<()> {
        self.persist_series_registry_index_with_catalog_persist(|_checkpoint_path| {
            self.mark_registry_catalog_dirty();
            Ok(())
        })
    }

    fn persist_series_registry_index_with_catalog_persist<PersistCatalog>(
        &self,
        persist_catalog_index: PersistCatalog,
    ) -> Result<()>
    where
        PersistCatalog: Fn(&Path) -> Result<()>,
    {
        let Some(checkpoint_path) = &self.persisted.series_index_path else {
            return Ok(());
        };
        let delta_path = SeriesRegistry::incremental_path(checkpoint_path);
        let delta_dir_path = SeriesRegistry::incremental_dir(checkpoint_path);
        self.registry_persistence_context()
            .persist_series_registry_index(
                checkpoint_path,
                &delta_path,
                &delta_dir_path,
                persist_catalog_index,
            )
    }

    /// Marks the derived registry catalog sidecar as needing a rewrite from the
    /// in-memory inventory.
    pub(in super::super) fn mark_registry_catalog_dirty(&self) {
        self.persisted
            .registry_catalog_dirty
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Claims the pending catalog rewrite: returns `true` exactly once per dirty
    /// epoch and clears the mark so a concurrent publish can re-arm it.
    pub(in super::super) fn take_registry_catalog_dirty(&self) -> bool {
        self.persisted
            .registry_catalog_dirty
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }
}
