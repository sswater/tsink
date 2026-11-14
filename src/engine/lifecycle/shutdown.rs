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
            let mut published = false;
            ChunkStorage::compact_compactors_with_changes(
                self.numeric_compactor,
                self.blob_compactor,
                Some(self.observability),
                &mut |window| {
                    published = true;
                    ChunkStorage::merge_published_window(
                        self.pending_persisted_segment_diff,
                        self.persisted_index_dirty,
                        window,
                    );
                },
            )?;
            if !published {
                break;
            }
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

    fn record_deferred_close_compaction(self, err: &TsinkError) {
        self.observability
            .record_maintenance_error("close compaction", err);
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

    /// Applies one published window's diff to the storage's catalog state.
    ///
    /// This is the partial-failure contract's sink: the merge happens as soon
    /// as the window is published, so a later window's error can no longer drop
    /// an already-published window's roots. Every caller must use it per window
    /// instead of merging a round-level diff at the end.
    pub(in super::super) fn merge_published_window(
        pending_persisted_segment_diff: &Mutex<PendingPersistedSegmentDiff>,
        persisted_index_dirty: &AtomicBool,
        window: PendingPersistedSegmentDiff,
    ) {
        pending_persisted_segment_diff.lock().merge(window);
        persisted_index_dirty.store(true, Ordering::SeqCst);
    }

    /// Runs one pass per lane, handing every published window's diff to
    /// `on_window` as soon as that window's publication is complete.
    ///
    /// The callback is `&mut dyn FnMut` so a lane can hand its windows over
    /// without buffering them: the diff of an already-published window survives
    /// even when a later window (of this lane or the other one) fails and the
    /// pass returns `Err`.
    pub(in super::super) fn compact_compactors_with_changes(
        numeric_compactor: Option<&Compactor>,
        blob_compactor: Option<&Compactor>,
        observability: Option<&StorageObservabilityCounters>,
        on_window: &mut dyn FnMut(PendingPersistedSegmentDiff),
    ) -> Result<CompactionRunStats> {
        let mut stats = CompactionRunStats::default();
        if let Some(compactor) = numeric_compactor {
            let lane_stats = Self::run_compactor_once(compactor, observability, on_window)?;
            accumulate_run_stats(&mut stats, lane_stats);
        }
        if let Some(compactor) = blob_compactor {
            let lane_stats = Self::run_compactor_once(compactor, observability, on_window)?;
            accumulate_run_stats(&mut stats, lane_stats);
        }
        Ok(stats)
    }

    fn run_compactor_once(
        compactor: &Compactor,
        observability: Option<&StorageObservabilityCounters>,
        on_window: &mut dyn FnMut(PendingPersistedSegmentDiff),
    ) -> Result<CompactionRunStats> {
        let started = Instant::now();
        // Observability is recorded once per lane-round with the round's window
        // sums (never per window, which is what a per-window callback would make
        // easy to get wrong); a failing round records one error and stops the
        // pass, so the two lanes never double-report an error.
        match compactor.compact_round_with_changes(on_window) {
            Ok(stats) => {
                if let Some(obs) = observability {
                    obs.record_compaction_result(stats, elapsed_nanos_u64(started));
                }
                Ok(stats)
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
        // Any deferred step means the visible catalog may no longer match what
        // compaction/refresh could verify, so the registry checkpoint must
        // tolerate a catalog index that cannot be rebuilt from it.
        let mut deferred_close_step = false;
        let _write_permits = shutdown.acquire_close_write_permits()?;
        self.flush_all_active()?;
        self.persist_segment()?;
        self.sweep_expired_persisted_segments()?;
        if self.memory_budget_value() != usize::MAX {
            self.refresh_memory_usage();
        }
        // Compaction is not load-bearing for close: every published window's
        // diff is already merged (per window, Task 8), the compaction marker
        // protocol keeps the disk consistent, and restart discovery re-reads
        // everything. So a failing round is deferred instead of aborting the
        // pipeline — refresh, tombstones and the registry checkpoint still run.
        if let Err(err) = shutdown.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES) {
            shutdown.record_deferred_close_compaction(&err);
            tracing::warn!(
                error = %err,
                "Close deferred compaction; dirty refresh, tombstone persistence and the registry checkpoint continue"
            );
            deferred_close_step = true;
        }
        if shutdown.persisted_index_dirty() || self.has_known_persisted_segment_changes() {
            if let Err(err) = self.refresh_dirty_persisted_segments_claimed() {
                if Self::close_should_defer_dirty_refresh_error(&err) {
                    shutdown.record_deferred_dirty_refresh(&err);
                    tracing::warn!(
                        error = %err,
                        "Close deferred dirty persisted refresh and kept the last visible catalog"
                    );
                    deferred_close_step = true;
                } else {
                    return Err(err);
                }
            }
        }
        self.persist_tombstones_index()?;
        if deferred_close_step {
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
/// `windows_merged` and `source_above_trigger` drive the worker's park decision;
/// they are summed and OR-ed respectively. The remaining counters are summed so
/// the aggregate stays meaningful for the observability snapshot.
///
/// `source_level` / `target_level` are *or-ed*, not summed: they name one
/// (source, target) pair, so the aggregate reports the first lane that had one
/// and keeps it even when the other lane compacted a different pair. Consumers
/// must treat them as "some pair", not as a per-lane list.
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
