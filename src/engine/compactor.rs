use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::chunk::{Chunk, ChunkHeader, ChunkPoint, ValueLane};
use crate::engine::durability::WalHighWatermark;
use crate::engine::encoder::Encoder;
use crate::engine::fs_utils::{remove_dir_if_exists, write_file_atomically_and_sync_parent};
use crate::engine::segment::{
    load_segment_root_for_compaction, load_segments_runtime_strict, read_segment_manifest,
    LoadedSegment, PersistedSeries, SegmentWriter,
};
use crate::engine::series::{SeriesId, SeriesRegistry};
use crate::engine::storage_engine::tiering::SegmentInventory;
use crate::engine::tombstone::{
    load_tombstones, timestamp_is_tombstoned, TombstoneMap, TombstoneRange, TOMBSTONES_FILE_NAME,
};
use crate::{Result, TsinkError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[path = "compactor/execution.rs"]
mod execution;
#[path = "compactor/inventory_source.rs"]
mod inventory_source;
#[path = "compactor/planning.rs"]
mod planning;

pub(in crate::engine) use self::execution::finalize_pending_compaction_replacements;
pub(in crate::engine) use self::inventory_source::{
    scan_inventory_from_disk, CompactionInventorySource,
};

const DEFAULT_L0_TRIGGER: usize = 4;
const DEFAULT_L1_TRIGGER: usize = 4;
const DEFAULT_SOURCE_WINDOW_SEGMENTS: usize = 8;
const DEFAULT_OUTPUT_SEGMENT_CHUNK_MULTIPLIER: usize = 512;
/// Wall-clock budget one compaction pass may spend opening new windows.
///
/// The budget is a flush-starvation guard, not a drain-rate bound: an open
/// window always runs to completion, and a backlogged source level always
/// drains at least one window per pass. Back-to-back passes (not a bigger
/// budget) are what keep the drain rate up.
const DEFAULT_COMPACTION_PASS_WALL_BUDGET: Duration = Duration::from_secs(1);
/// Park duration for the background compaction worker after a pass that made
/// progress while sources are still above their trigger line.
///
/// A backlogged, productive pass parks for 1ms so consecutive passes drain
/// back-to-back instead of waiting a full interval per window.
pub(in crate::engine) const COMPACTION_BACKLOG_PARK: Duration = Duration::from_millis(1);
const COMPACTION_REPLACEMENT_DIR: &str = ".compaction-replacements";
const COMPACTION_REPLACEMENT_VERSION: u16 = 1;
static COMPACTION_REPLACEMENT_COUNTER: AtomicU64 = AtomicU64::new(1);

type SeriesChunkRefs<'a> = HashMap<SeriesId, Vec<&'a Chunk>>;
type MergeSegmentsOutput<'a> = (Vec<PersistedSeries>, SeriesChunkRefs<'a>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactionReplacementMarker {
    version: u16,
    source_segments: Vec<String>,
    output_segments: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionRunStats {
    pub compacted: bool,
    pub source_level: Option<u8>,
    pub target_level: Option<u8>,
    pub source_segments: usize,
    pub output_segments: usize,
    pub source_chunks: usize,
    pub output_chunks: usize,
    pub source_points: usize,
    pub output_points: usize,
    pub windows_merged: u32,
    pub source_above_trigger: bool,
}

/// How long the background compaction worker parks after one pass.
///
/// The decision is keyed on *progress*, not on the backlog alone: a pass that
/// merged no window parks the full interval even when sources remain above
/// their trigger line. That is what keeps a window that can never produce
/// output (every point tombstoned) from becoming a 1ms hot loop that reloads
/// the same window forever — it is exactly today's idle tick for every
/// no-progress pass.
///
/// A productive pass with sources still above their trigger parks
/// [`COMPACTION_BACKLOG_PARK`] so passes run back-to-back and the drain keeps
/// up with ingest.
pub(in crate::engine) fn compaction_park_interval(
    stats: &CompactionRunStats,
    interval: Duration,
) -> Duration {
    if stats.windows_merged == 0 {
        interval
    } else if stats.source_above_trigger {
        COMPACTION_BACKLOG_PARK
    } else {
        interval
    }
}

#[derive(Debug, Default)]
pub(super) struct CompactionOutcome {
    pub(super) stats: CompactionRunStats,
    pub(super) source_roots: Vec<PathBuf>,
    pub(super) output_roots: Vec<PathBuf>,
    /// Roots the inventory listed but that vanished before they could be
    /// loaded. They are already gone from disk; they must still leave the
    /// planning inventory and the catalog, so the caller folds them into the
    /// pass's `removed_roots`.
    pub(super) retired_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionLevel {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone)]
pub struct Compactor {
    data_path: PathBuf,
    point_cap: usize,
    l0_trigger: usize,
    l1_trigger: usize,
    pass_wall_budget: Duration,
    next_segment_id: Option<Arc<AtomicU64>>,
    source: Option<CompactionInventorySource>,
    /// Storage-wide compaction gate, held for one window at a time by
    /// [`Self::compact_once_with_changes`].
    ///
    /// `None` for standalone compactors (tests, one-shot maintenance) where no
    /// other thread contends for the storage's cross-subsystem ordering.
    gate: Option<Arc<Mutex<()>>>,
}

impl Compactor {
    pub fn new(data_path: impl AsRef<Path>, point_cap: usize) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            pass_wall_budget: DEFAULT_COMPACTION_PASS_WALL_BUDGET,
            next_segment_id: None,
            source: None,
            gate: None,
        }
    }

    pub fn new_with_segment_id_allocator(
        data_path: impl AsRef<Path>,
        point_cap: usize,
        next_segment_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            pass_wall_budget: DEFAULT_COMPACTION_PASS_WALL_BUDGET,
            next_segment_id: Some(next_segment_id),
            source: None,
            gate: None,
        }
    }

    pub(in crate::engine) fn new_with_inventory_source(
        data_path: impl AsRef<Path>,
        point_cap: usize,
        next_segment_id: Arc<AtomicU64>,
        source: CompactionInventorySource,
    ) -> Self {
        Self {
            data_path: data_path.as_ref().to_path_buf(),
            point_cap: point_cap.clamp(1, u16::MAX as usize),
            l0_trigger: DEFAULT_L0_TRIGGER,
            l1_trigger: DEFAULT_L1_TRIGGER,
            pass_wall_budget: DEFAULT_COMPACTION_PASS_WALL_BUDGET,
            next_segment_id: Some(next_segment_id),
            source: Some(source),
            gate: None,
        }
    }

    /// Shares the storage's compaction gate with this compactor.
    ///
    /// The pass then holds the gate for exactly one window step at a time
    /// (load + merge + replacement marker + source removal) and releases it
    /// between windows, so flush/retention/snapshot can interleave instead of
    /// waiting for a whole round. `parking_lot::Mutex` is not reentrant, so a
    /// caller of [`Self::compact_once_with_changes`] must not hold this same
    /// gate itself.
    pub(in crate::engine) fn with_gate(mut self, gate: Arc<Mutex<()>>) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Overrides the per-pass wall-clock budget.
    ///
    /// Tests set [`Duration::ZERO`] to pin the min-1 guarantee: an exhausted
    /// budget must still let the first window of a backlogged level open.
    #[cfg(test)]
    pub(in crate::engine) fn set_pass_wall_budget(&mut self, budget: Duration) {
        self.pass_wall_budget = budget;
    }

    /// Inventory to plan this round from.
    ///
    /// `Ok(None)` means "no snapshot injected yet" and makes the pass skip
    /// before any disk access. Without a source (standalone compactor), the
    /// inventory is rebuilt from disk — the same scan used for reconciliation.
    fn planning_inventory(&self) -> Result<Option<SegmentInventory>> {
        match self.source.as_ref() {
            None => Ok(Some(scan_inventory_from_disk(&self.data_path)?)),
            Some(source) => Ok(source.snapshot()),
        }
    }

    pub fn compact_once(&self) -> Result<bool> {
        Ok(self.compact_once_with_stats()?.compacted)
    }

    pub fn compact_once_with_stats(&self) -> Result<CompactionRunStats> {
        Ok(self.compact_once_with_changes()?.stats)
    }
}

#[cfg(test)]
#[path = "compactor/tests.rs"]
mod tests;
