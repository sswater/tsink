use super::*;
use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
struct LifecycleShutdownContext<'a> {
    background_maintenance_lock: &'a Mutex<()>,
    write_limiter: &'a crate::concurrency::Semaphore,
    write_timeout: std::time::Duration,
    numeric_compactor: Option<&'a Compactor>,
    blob_compactor: Option<&'a Compactor>,
    pending_persisted_segment_diff: &'a Mutex<PendingPersistedSegmentDiff>,
    persisted_index_dirty: &'a AtomicBool,
    observability: &'a StorageObservabilityCounters,
}

impl<'a> LifecycleShutdownContext<'a> {
    fn background_maintenance_gate(self) -> MutexGuard<'a, ()> {
        self.background_maintenance_lock.lock()
    }

    fn acquire_close_write_permits(self) -> Result<Vec<crate::concurrency::SemaphoreGuard<'a>>> {
        self.write_limiter.acquire_all(self.write_timeout)
    }

    fn compact_until_settled(self, max_passes: usize) -> Result<usize> {
        // The compactor holds the compaction gate per window itself. Taking it
        // here as well would self-deadlock: `parking_lot::Mutex` is not
        // reentrant.
        let mut passes = 0usize;
        for _ in 0..max_passes.max(1) {
            let (changes, _stats) = ChunkStorage::compact_compactors_with_changes(
                self.numeric_compactor,
                self.blob_compactor,
                Some(self.observability),
            )?;
            if changes.is_empty() {
                break;
            }
            self.pending_persisted_segment_diff.lock().merge(changes);
            self.persisted_index_dirty.store(true, Ordering::SeqCst);
            passes = passes.saturating_add(1);
        }
        Ok(passes)
    }

    fn persisted_index_dirty(self) -> bool {
        self.persisted_index_dirty.load(Ordering::SeqCst)
    }

    fn record_deferred_dirty_refresh(self, err: &TsinkError) {
        self.observability
            .record_maintenance_error("close dirty persisted refresh", err);
    }
}

impl ChunkStorage {
    fn lifecycle_shutdown_context(&self) -> LifecycleShutdownContext<'_> {
        LifecycleShutdownContext {
            background_maintenance_lock: &self.coordination.background_maintenance_lock,
            write_limiter: &self.runtime.write_limiter,
            write_timeout: self.runtime.write_timeout,
            numeric_compactor: self.persisted.numeric_compactor.as_ref(),
            blob_compactor: self.persisted.blob_compactor.as_ref(),
            pending_persisted_segment_diff: &self.persisted.pending_persisted_segment_diff,
            persisted_index_dirty: self.persisted.persisted_index_dirty.as_ref(),
            observability: self.observability.as_ref(),
        }
    }

    pub(in super::super) fn compact_compactors_with_changes(
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<(PendingPersistedSegmentDiff, CompactionRunStats)> {
        let mut changes = PendingPersistedSegmentDiff::default();
        let mut stats = CompactionRunStats::default();
        if let Some(compactor) = numeric_compactor {
            let (lane_changes, lane_stats) = Self::run_compactor_once(compactor, observability)?;
            changes.merge(lane_changes);
            accumulate_run_stats(&mut stats, lane_stats);
        }
        if let Some(compactor) = blob_compactor {
            let (lane_changes, lane_stats) = Self::run_compactor_once(compactor, observability)?;
            changes.merge(lane_changes);
            accumulate_run_stats(&mut stats, lane_stats);
        }
        Ok((changes, stats))
    }

    fn run_compactor_once(
        compactor: &Compactor,
        observability: Option<&StorageObservabilityCounters>,
    ) -> Result<(PendingPersistedSegmentDiff, CompactionRunStats)> {
        let started = Instant::now();
        match compactor.compact_once_with_changes() {
            Ok(outcome) => {
                let stats = outcome.stats;
                if let Some(obs) = observability {
                    obs.record_compaction_result(stats, elapsed_nanos_u64(started));
                }
                let mut changes = PendingPersistedSegmentDiff::default();
                if stats.compacted || !outcome.retired_roots.is_empty() {
                    // Vanished window members are already gone from disk, but
                    // they must still leave the inventory and the catalog,
                    // otherwise every pass re-selects the same ghosts.
                    let mut removed = outcome.source_roots;
                    removed.extend(outcome.retired_roots);
                    changes.record_changes(outcome.output_roots, removed);
                }
                Ok((changes, stats))
            }
            Err(err) => {
                if let Some(obs) = observability {
                    obs.record_compaction_error(elapsed_nanos_u64(started));
                }
                Err(err)
            }
        }
    }

    fn close_should_defer_dirty_refresh_error(err: &TsinkError) -> bool {
        crate::engine::segment::segment_validation_error_message(err).is_some()
            || matches!(err, TsinkError::Other(message) if message.contains("during runtime refresh"))
    }

    fn execute_close_pipeline(&self) -> Result<()> {
        let shutdown = self.lifecycle_shutdown_context();
        let _background_maintenance_guard = shutdown.background_maintenance_gate();
        let mut deferred_dirty_refresh = false;
        let _write_permits = shutdown.acquire_close_write_permits()?;
        self.flush_all_active()?;
        self.persist_segment()?;
        self.sweep_expired_persisted_segments()?;
        if self.memory_budget_value() != usize::MAX {
            self.refresh_memory_usage();
        }
        shutdown.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES)?;
        if shutdown.persisted_index_dirty() || self.has_known_persisted_segment_changes() {
            if let Err(err) = self.refresh_dirty_persisted_segments_claimed() {
                if Self::close_should_defer_dirty_refresh_error(&err) {
                    shutdown.record_deferred_dirty_refresh(&err);
                    tracing::warn!(
                        error = %err,
                        "Close deferred dirty persisted refresh and kept the last visible catalog"
                    );
                    deferred_dirty_refresh = true;
                } else {
                    return Err(err);
                }
            }
        }
        self.persist_tombstones_index()?;
        if deferred_dirty_refresh {
            self.checkpoint_series_registry_index_allow_invalid_catalog()?;
        } else {
            self.checkpoint_series_registry_index()?;
        }
        Ok(())
    }

    pub(in super::super) fn close_impl(&self) -> Result<()> {
        self.start_close_transition()?;
        let close_result = self.execute_close_pipeline();
        self.finish_close_transition(close_result)
    }
}

/// Folds one lane's pass statistics into the pass total.
///
/// Only `windows_merged` and `source_above_trigger` drive the worker's park
/// decision, but the whole struct is summed so the aggregate stays meaningful
/// for future consumers.
fn accumulate_run_stats(total: &mut CompactionRunStats, lane: CompactionRunStats) {
    total.compacted |= lane.compacted;
    total.source_level = total.source_level.or(lane.source_level);
    total.target_level = total.target_level.or(lane.target_level);
    total.source_segments = total.source_segments.saturating_add(lane.source_segments);
    total.output_segments = total.output_segments.saturating_add(lane.output_segments);
    total.source_chunks = total.source_chunks.saturating_add(lane.source_chunks);
    total.output_chunks = total.output_chunks.saturating_add(lane.output_chunks);
    total.source_points = total.source_points.saturating_add(lane.source_points);
    total.output_points = total.output_points.saturating_add(lane.output_points);
    total.windows_merged = total.windows_merged.saturating_add(lane.windows_merged);
    total.source_above_trigger |= lane.source_above_trigger;
}
