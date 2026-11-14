use super::*;
use crate::engine::segment::{is_runtime_refresh_disappearance_error, is_vanished_source_error};
use std::path::Path;
use tracing::warn;

/// Loads one added root for a refresh, treating a vanished root as retired.
///
/// The only remover of a published root is compaction/retention. Its own diff
/// either is in the drained set (so the removal applies now) or lands in the
/// pending diff (so it applies next pass), and `record_changes` dedup keeps the
/// root on the removed side either way. Failing the whole pass — or restoring
/// the drained `+root` over that pending `-root` — would resurrect a root that
/// is already deleted on disk and never converge. Real corruption (hash
/// mismatch, parse failure) still propagates.
fn load_added_root_skipping_vanished(
    root: &Path,
    observability: &StorageObservabilityCounters,
) -> Result<Option<IndexedSegment>> {
    match ChunkStorage::load_segment_index_for_runtime_refresh(root) {
        Ok(segment) => Ok(Some(segment)),
        Err(err)
            if is_vanished_source_error(&err) || is_runtime_refresh_disappearance_error(&err) =>
        {
            observability.record_transient_disappearance();
            warn!(
                path = %root.display(),
                error = %err,
                "Catalog refresh segment vanished mid-removal; retiring it instead of failing the pass"
            );
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

impl<'a> CatalogRefreshContext<'a> {
    pub(super) fn load_known_dirty_catalog_refresh_segments_phase(
        self,
        diff: &PendingPersistedSegmentDiff,
    ) -> Result<Vec<IndexedSegment>> {
        #[cfg(test)]
        self.invoke_known_dirty_load_hook();

        let mut loaded_segments = Vec::with_capacity(diff.added_roots.len());
        for root in &diff.added_roots {
            if let Some(segment) =
                load_added_root_skipping_vanished(root, self.observability_counters())?
            {
                loaded_segments.push(segment);
            }
        }
        Ok(loaded_segments)
    }

    pub(super) fn plan_loaded_inventory_catalog_refresh_phase(
        self,
        loaded: LoadedInventoryCatalogRefresh,
    ) -> Result<PlannedPersistedCatalogRefresh> {
        let current_roots = loaded.inventory.root_set();
        let added_roots = current_roots
            .difference(&loaded.visible_roots)
            .cloned()
            .collect::<Vec<_>>();
        let removed_roots = loaded
            .visible_roots
            .difference(&current_roots)
            .cloned()
            .collect::<Vec<_>>();

        let mut loaded_segments = Vec::with_capacity(added_roots.len());
        for root in &added_roots {
            if let Some(segment) =
                load_added_root_skipping_vanished(root, self.observability_counters())?
            {
                loaded_segments.push(segment);
            }
        }

        Ok(PlannedPersistedCatalogRefresh::Inventory(
            PlannedInventoryCatalogRefresh {
                visibility_fence: loaded.visibility_fence,
                inventory: loaded.inventory,
                loaded_segments,
                removed_roots,
                tombstones: loaded.tombstones,
            },
        ))
    }
}

impl ChunkStorage {
    pub(in super::super) fn plan_persisted_catalog_transition_phase(
        &self,
        planned: PlannedPersistedCatalogRefresh,
    ) -> Result<PersistedCatalogTransition> {
        match planned {
            PlannedPersistedCatalogRefresh::KnownDirty(planned) => {
                let added_roots = planned.diff.added_roots.into_iter().collect::<Vec<_>>();
                let removed_roots = planned.diff.removed_roots.into_iter().collect::<Vec<_>>();
                let registry_catalog_sources = self
                    .persisted_registry_catalog_sources_with_root_changes(
                        &added_roots,
                        &removed_roots,
                    )?;
                Ok(PersistedCatalogTransition {
                    visibility_fence: None,
                    loaded_segments: planned.loaded_segments,
                    removed_roots,
                    publication: PersistedCatalogPublication::PersistedState {
                        published_segment_roots: added_roots,
                    },
                    registry_catalog_sources: Some(registry_catalog_sources),
                })
            }
            PlannedPersistedCatalogRefresh::Inventory(planned) => Ok(PersistedCatalogTransition {
                visibility_fence: Some(planned.visibility_fence),
                loaded_segments: planned.loaded_segments,
                removed_roots: planned.removed_roots,
                publication: PersistedCatalogPublication::Inventory {
                    inventory: planned.inventory.clone(),
                    tombstones: Some(planned.tombstones),
                },
                registry_catalog_sources: Some(registry_catalog::inventory_sources(
                    &planned.inventory,
                )),
            }),
        }
    }
}
