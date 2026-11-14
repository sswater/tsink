use std::fmt;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use tracing::warn;

use crate::engine::segment::{
    is_vanished_source_error, list_segment_dirs, read_segment_manifest_with_files,
    segment_validation_error, SegmentValidationContext,
};
use crate::engine::storage_engine::tiering::{
    PersistedSegmentTier, SegmentInventory, SegmentInventoryEntry, SegmentLaneFamily,
};
use crate::{Result, TsinkError};

/// Returns the in-memory persisted-segment inventory the compactor plans from.
///
/// The provider is injected after construction because the persisted index does
/// not exist yet when the background compaction thread is spawned.
pub(in crate::engine) type InventoryProvider = Arc<dyn Fn() -> SegmentInventory + Send + Sync>;

/// Arc-shared inventory injection point for [`super::Compactor`].
///
/// `Compactor` is cloned by value into the background compaction thread, so the
/// handle must be shared through an `Arc`: a clone taken before [`Self::set`]
/// still observes the provider afterwards. A per-instance `OnceLock` would
/// leave the thread's copy permanently empty.
#[derive(Clone, Default)]
pub(in crate::engine) struct CompactionInventorySource(Arc<OnceLock<InventoryProvider>>);

impl CompactionInventorySource {
    pub(in crate::engine) fn set(&self, provider: InventoryProvider) {
        let _ = self.0.set(provider);
    }

    pub(in crate::engine) fn snapshot(&self) -> Option<SegmentInventory> {
        self.0.get().map(|provider| provider())
    }
}

impl fmt::Debug for CompactionInventorySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionInventorySource")
            .field("injected", &self.0.get().is_some())
            .finish()
    }
}

/// Builds a one-shot inventory from the segment manifests under `lane_root`.
///
/// This is the standalone-compactor fallback and, later, the reconciliation
/// scan: it only reads manifest files, never chunk payloads.
pub(in crate::engine) fn scan_inventory_from_disk(lane_root: &Path) -> Result<SegmentInventory> {
    let lane = SegmentLaneFamily::for_lane_root(lane_root);
    let mut entries = Vec::new();

    for root in list_segment_dirs(lane_root)? {
        match read_segment_manifest_with_files(&root) {
            Ok((manifest, files)) => entries.push(SegmentInventoryEntry {
                lane,
                tier: PersistedSegmentTier::Hot,
                root,
                manifest,
                files: Some(files),
            }),
            Err(err) if is_vanished_source_error(&err) => {
                warn!(
                    path = %root.display(),
                    error = %err,
                    "Segment directory is mid-removal during compaction inventory scan; skipping"
                );
            }
            Err(TsinkError::DataCorruption(details)) => {
                return Err(segment_validation_error(
                    &root,
                    SegmentValidationContext::Compaction,
                    &details,
                ));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(SegmentInventory::from_entries(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_taken_before_set_observes_provider() {
        let source = CompactionInventorySource::default();
        let clone = source.clone();
        assert!(source.snapshot().is_none());

        clone.set(Arc::new(SegmentInventory::default));

        assert!(source.snapshot().is_some());
        assert!(clone.snapshot().is_some());
    }
}
