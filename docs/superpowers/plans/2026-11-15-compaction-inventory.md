# 压缩库存化 + vanished-skip 对齐 + registry catalog 内存化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除压缩/registry catalog 的 O(N) 逐段磁盘扫描，使压缩排空速率与积压规模无关、与写触发产生速率匹配，并统一"删除中目录"的 skip 策略。

**Architecture:** Compactor 从内存库存快照（`persisted_index`）做规划与窗口选择，磁盘只加载选中窗口；压缩 gate 从整轮持有改为按窗口持有；diff 按窗口即时合并；registry catalog 的生产从逐段磁盘读改为内存库存 + per-segment 贡献缓存，catalog 重写去抖出 flush 关键路径。

**Tech Stack:** Rust（parking_lot、bincode/serde、Xxh64），现有 tsink engine 内部结构。

**Spec:** `docs/superpowers/specs/2026-11-15-compaction-inventory-design.md`（v3）

## Global Constraints

- 崩溃安全机制不动：replacement marker 写入/应用协议、WAL、fsync/原子重命名模式。
- 不新增工作线程、不做并行压缩；gate 按窗口持有是缩短持锁区间，不是新增并发。
- **死锁规避规则**：任何代码不得在持有 `persisted_index` 读锁期间获取 compaction gate；库存快照必须是短临界区内 clone 出的值。
- 启动加载语义不变（`StartupRecoverable` 隔离保持现状）；运行时路径对 vanished 错误 warn+skip。
- 真实损坏（hash 不符、解析失败）在运行时仍然 FailOnInvalid；只有 vanished（NotFound / `missing ...`）跳过。
- 新增常量一律内部，不加配置面；catalog 文件 schema、fingerprint 哈希算法不变。
- 贡献（contribution）的唯一权威来源是段自身写入 `series.bin` 的集合，绝不由 registry 反推。
- 排空速率目标：写触发产生速率下 drain ≥ produce（本地盘）；flush 的 gate 等待上界 = 单窗口时长。

## Review Focus

1. **窗口源在加载瞬间消失**（retention/compaction 并发删除）→ 期望：该成员被跳过、计入 removed_roots、循环收敛，整轮不失败。测试：Task 6 步骤 1（`vanished_member_is_evicted_and_round_converges`）。
2. **压缩积压时 flush 被长时间阻塞** → 期望：flush 的 gate 等待 ≤ 单窗口时长。测试：Task 7 步骤 3（`flush_gate_wait_bounded_by_single_window`）。
3. **积压未清时压缩线程干等 5 秒间隔** → 期望：≥ 触发线时背靠背连跑，drain 跟上写触发产生。测试：Task 7 步骤 5（`compaction_park_interval_backlogged_vs_idle` + Task 8 仿真）。
4. **幽灵段**（已合并源仍在库存/catalog）→ 期望：经 removed_roots 流出库存与 catalog，不再被重选。测试：Task 6 步骤 3 + Task 8 步骤 3。
5. **贡献缓存权威性**（缓存被 registry 反推则 fingerprint 校验失效）→ 期望：贡献来自段文件；段文件与缓存不一致可被检出。测试：Task 11 步骤 5（`contribution_is_sourced_from_segment_series_file`）。

---

### Task 1: 共享 vanished 谓词（原始错误形态，包装前判定）

**Files:**
- Modify: `src/engine/segment/validation.rs`
- Modify: `src/engine/registry_catalog.rs:145`（`is_vanished_catalog_source_error` 改为委托）
- Modify: `src/engine/compactor/planning.rs:99-114`（`count_level_segments_for_compaction` 的两处 inline 判定改用谓词）
- Test: `src/engine/segment/validation.rs` 内新增 `#[cfg(test)] mod vanished_predicate_tests`

**Interfaces:**
- Produces: `pub(in crate::engine) fn is_vanished_source_error(err: &TsinkError) -> bool` —— `TsinkError::Io(NotFound)` 或 `TsinkError::DataCorruption(msg)` 且 `msg.starts_with("missing ")`。

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn vanished_source_error_matches_raw_missing_and_not_found() {
    assert!(is_vanished_source_error(&TsinkError::DataCorruption(
        "missing manifest.bin".to_string()
    )));
    assert!(is_vanished_source_error(&TsinkError::DataCorruption(
        "missing chunks.bin".to_string()
    )));
    assert!(is_vanished_source_error(&TsinkError::Io(
        std::io::Error::from(std::io::ErrorKind::NotFound)
    )));
    assert!(!is_vanished_source_error(&TsinkError::DataCorruption(
        "bad magic".to_string()
    )));
    assert!(!is_vanished_source_error(&TsinkError::Io(
        std::io::Error::from(std::io::ErrorKind::PermissionDenied)
    )));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib vanished_source_error -- --nocapture`
Expected: FAIL（函数不存在）

- [ ] **Step 3: Implement**

`validation.rs` 增加上述谓词；`is_vanished_catalog_source_error` 函数体改为 `is_vanished_source_error(err)`；`count_level_segments_for_compaction` 中 `Err(err) if is_not_found_error(&err)` 与 `Err(TsinkError::DataCorruption(details)) if details.starts_with("missing ")` 两个分支合并为 `Err(err) if is_vanished_source_error(&err)`（warn 文案保留）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib vanished_source_error && cargo test --lib count_level_segments`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/segment/validation.rs src/engine/registry_catalog.rs src/engine/compactor/planning.rs
git commit -m "feat(engine): shared vanished-source error predicate"
```

---

### Task 2: stranded-diff bug 修复

**Files:**
- Modify: `src/engine/maintenance/catalog_refresh.rs`（`sync_persisted_segments_from_disk_if_dirty` 的两处提前返回，约 :203-214）
- Test: `src/engine/tests/persistence_background.rs`（新增，setup 模仿 `dirty_known_diff_refresh_skips_full_inventory_scans_at_large_segment_counts`）

**Interfaces:**
- Consumes: `ctx.has_known_persisted_segment_changes()`（catalog_refresh/context.rs 已存在）。

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn stranded_known_diff_is_refreshed_without_dirty_flag() {
    // 模仿既有测试的存储构建；通过既有 helper 将 diff 合并进 pending_persisted_segment_diff
    // 但不置位 persisted_index_dirty（复现 clobber 后的 stranded 状态）。
    // 调用 storage.sync_persisted_segments_from_disk_if_dirty()。
    // 断言：known diff 被应用（persisted_index 包含 diff 的 added_roots）。
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib stranded_known_diff -- --nocapture`
Expected: FAIL（diff 未被应用）

- [ ] **Step 3: Implement**

`sync_persisted_segments_from_disk_if_dirty` 中两处 `if !ctx.persisted_index_dirty() { return Ok(()); }` 改为
`if !ctx.persisted_index_dirty() && !ctx.has_known_persisted_segment_changes() { return Ok(()); }`。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib stranded_known_diff && cargo test --lib persistence_background`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/maintenance/catalog_refresh.rs src/engine/tests/persistence_background.rs
git commit -m "fix(engine): stranded known-diff refreshed without dirty flag"
```

---

### Task 3: 段文件读取计数 hook（cfg(test)）

**Files:**
- Modify: `src/engine/fs_utils.rs`（按既有 `directory_sync_hook_slot` 模式新增）
- Modify: `src/engine/segment/loader.rs`（`read_required_segment_file`）、`src/engine/segment/format.rs`（`read_segment_manifest`、`read_segment_manifest_fingerprint`）、`src/engine/registry_catalog.rs`（`load_segment_series_metadata`）各插入一行 notify
- Test: `src/engine/fs_utils.rs` `#[cfg(test)] mod segment_read_hook_tests`

**Interfaces:**
- Produces: `#[cfg(test)] pub(crate) fn set_segment_file_read_hook(hook: Option<Arc<dyn Fn(&Path) + Send + Sync>>) -> ReadHookGuard`（RAII guard，测试结束自动清除，模仿 `fail_directory_sync_once` 的 guard 模式）与 `#[cfg(test)] pub(crate) fn count_segment_file_reads_under(prefix: &Path) -> usize`（内置计数器实现，guard 模式：`CountingSegmentReads::install(prefix) -> Guard` 同时装 hook 与计数）。

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn counting_guard_counts_reads_under_prefix() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("a.bin"), b"x").unwrap();
    let _guard = CountingSegmentReads::install(tmp.path());
    let _ = std::fs::read(tmp.path().join("a.bin")); // 经由被插桩的读取路径触发
    // 直接调用 notify（保证与插桩点解耦的最小验证）
    super::notify_segment_file_read(&tmp.path().join("manifest.bin"));
    assert_eq!(super::count_segment_file_reads_under(tmp.path()), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib counting_guard_counts -- --nocapture`
Expected: FAIL（函数不存在）

- [ ] **Step 3: Implement**

`fs_utils.rs` 增加槽位 + `notify_segment_file_read`（cfg(test)）；四处读取点各插入 `#[cfg(test)] notify_segment_file_read(path)`；guard/计数按接口块实现。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib segment_read_hook && cargo test --lib counting_guard`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/fs_utils.rs src/engine/segment/loader.rs src/engine/segment/format.rs src/engine/registry_catalog.rs
git commit -m "test(engine): segment file read counting hook"
```

---

### Task 4: CompactionInventorySource + Compactor 接线 + 注入前跳过

**Files:**
- Create: `src/engine/compactor/inventory_source.rs`
- Modify: `src/engine/compactor.rs`（字段 + 构造器 + 模块声明；`CompactionRunStats` 增加 `pub windows_merged: u32` 与 `pub source_above_trigger: bool`——本 Task 起始终为 0/false，Task 6/7 赋值）
- Modify: `src/engine/construction/resources.rs`（`prepare_construction_resources` 创建 source 并传入两个 Compactor；`StorageAssemblyResources` 增加字段返回）
- Modify: `src/engine/construction/assembly.rs`（storage 构建完成后 `source.set(provider)`；provider = `Arc::new(move || storage.persisted_segment_inventory())`）
- Test: `src/engine/compactor/tests.rs`

**Interfaces:**
- Produces:
  - `pub(in crate::engine) type InventoryProvider = std::sync::Arc<dyn Fn() -> SegmentInventory + Send + Sync>;`
  - `#[derive(Clone, Default)] pub(in crate::engine) struct CompactionInventorySource(Arc<OnceLock<InventoryProvider>>);` 带 `fn set(&self, provider: InventoryProvider)` 与 `fn snapshot(&self) -> Option<SegmentInventory>`（**Arc 共享：clone 后 set 对所有副本可见**，E5）。
  - `Compactor::new_with_inventory_source(data_path, point_cap, next_segment_id, source) -> Self`；`Compactor::new*` 既有构造器保持 `source: None` 语义。
  - `Compactor` 新增私有字段 `source: Option<CompactionInventorySource>`；内部 `fn planning_inventory(&self) -> Result<Option<SegmentInventory>>`：`None`（无 source）→ `Some(scan_inventory_from_disk(&self.data_path)?)`（回退，独立测试路径）；`Some(src)` 且 `snapshot() == None` → `Ok(None)`（注入前跳过信号）。
  - `fn scan_inventory_from_disk(lane_root: &Path) -> Result<SegmentInventory>`（复用 `collect_segment_dirs` + `read_segment_manifest`，lane 根下所有层；后续 Task 也作为独立回退/对账实现）。

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn compactor_skips_round_until_inventory_source_is_set() {
    // 在 temp_dir 构造 6 个 L0 segment（复用既有测试的 segment 构造 helper）。
    // Compactor::new_with_inventory_source(...)，不 set source。
    // CountingSegmentReads::install(temp_dir)。
    let stats = compactor.compact_once_with_stats().unwrap();
    assert!(!stats.compacted);
    assert_eq!(stats.windows_merged, 0);
    assert_eq!(count_segment_file_reads_under(temp_dir.path()), 0, "注入前不得扫盘");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib compactor_skips_round_until -- --nocapture`
Expected: FAIL（构造器不存在）

- [ ] **Step 3: Implement**

按 Interfaces 块实现；`compact_once_with_changes` 开头：`let Some(inventory) = self.planning_inventory()? else { return Ok(CompactionOutcome::default()) };`（本 Task 先保持单窗口行为不变，后续 Task 替换内部）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib compactor -- --nocapture`
Expected: PASS（含既有 8 个 `compact_once` 测试——它们走 `Compactor::new` 回退扫描路径）

- [ ] **Step 5: Commit**

```bash
git add src/engine/compactor/inventory_source.rs src/engine/compactor.rs src/engine/construction/resources.rs src/engine/construction/assembly.rs src/engine/compactor/tests.rs
git commit -m "feat(engine): Arc-shared compaction inventory source"
```

---

### Task 5: planning 从库存做计数与窗口选择 + 窗口成员按根加载（vanished skip）

**Files:**
- Modify: `src/engine/compactor/planning.rs`（`count_level_segments_for_compaction` 的库存版、`try_compact_level`、`has_time_overlap`/`select_compaction_window` 改为作用于 manifest 视图）
- Modify: `src/engine/segment/loader.rs`（新增 `load_segment_root_for_compaction`）
- Test: `src/engine/compactor/tests.rs`

**Interfaces:**
- Consumes: Task 1 谓词、Task 3 hook、Task 4 `planning_inventory`。
- Produces:
  - `pub(in crate::engine) struct PlanningCandidate { pub root: PathBuf, pub manifest: SegmentManifest }`（`has_time_overlap(&[&PlanningCandidate])`、`select_compaction_window(&[PlanningCandidate], trigger, max)` 重构为作用于此类型；`LoadedSegment` 版本删除或变薄包装）。
  - `pub(in crate::engine) fn load_segment_root_for_compaction(root: &Path) -> Result<Option<LoadedSegment>>`：内部 `load_segment(root)`，错误命中 `is_vanished_source_error` → warn + `Ok(None)`；其余错误原样传播（真实损坏仍失败）。
- 候选过滤：`entry.root.starts_with(&self.data_path)` && `entry.manifest.level == level` && `entry.tier == PersistedSegmentTier::Hot`。

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn inventory_driven_planning_matches_disk_driven_outcome() {
    // 构造 5 个时间重叠的 L0 segment；两个 compactor：一个 Compactor::new（回退扫描），
    // 一个 new_with_inventory_source + set(返回同一批段构建的 SegmentInventory)。
    // 各跑一轮 compact_once_with_stats，断言两者 source_roots/output_roots 集合一致。
}

#[test]
fn vanished_window_member_is_skipped_and_survivors_compacted() {
    // 构造 4 个 L0 segment（≥ 触发线），删除其中一个的 manifest.bin。
    // 一轮 compact_once_with_stats：Ok，windows_merged == 1，
    // output_roots 非空，且 3 个幸存者被合并（断言输出 manifest 的 source 数或读 hook 计数）。
}

#[test]
fn corrupt_window_member_still_fails_the_round() {
    // 既有语义保持：破坏某个成员 chunks.bin 字节（hash 失配，非缺失）→ compact_once_with_changes 返回 Err。
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib inventory_driven_planning vanished_window_member corrupt_window_member -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

- `try_compact_level`：从 `planning_inventory` 快照构建 `Vec<PlanningCandidate>`（过滤 + 按 `manifest.segment_id` 排序）；计数/触发/重叠/窗口选择全部作用于候选；选中窗口逐根 `load_segment_root_for_compaction`，`None` 成员记入待剔除列表；幸存者 < 2 → 记剔除并返回 `None`。
- `count_level_segments_for_compaction` 保留为 `scan_inventory_from_disk` 的内部组成（回退路径用）。
- 既有 `compaction_fails_when_any_source_segment_is_corrupted` 语义由幸存者加载错误传播保持。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib compactor -- --nocapture && cargo test --lib segment -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/compactor/planning.rs src/engine/segment/loader.rs src/engine/compactor/tests.rs
git commit -m "feat(engine): inventory-driven compaction planning with per-root window loads"
```

---

### Task 6: 多窗口排空循环 + 墙钟预算 + 收敛契约

**Files:**
- Modify: `src/engine/compactor.rs`（`CompactionOutcome` 增加 `retired_roots: Vec<PathBuf>`；`pass_wall_budget` 字段默认 `DEFAULT_COMPACTION_PASS_WALL_BUDGET: Duration = Duration::from_secs(1)`；`windows_merged`/`source_above_trigger` 于本 Task 开始赋值）
- Modify: `src/engine/compactor/planning.rs`（`compact_once_with_changes` 改为循环）
- Modify: `src/engine/lifecycle/shutdown.rs:70-105`（`run_compactor_once` 把 `retired_roots` 并入 `removed`）
- Test: `src/engine/compactor/tests.rs`

**Interfaces:**
- Produces:
  - `#[cfg(test)] pub(in crate::engine) fn set_pass_wall_budget(&mut self, budget: Duration)`（仅测试）。
  - 循环契约：`while count(source) >= trigger && (budget 剩余 || 本轮尚未合并任何窗口)`；每窗口后：吸收输出进工作副本、把 vanished 剔除根记入 `retired_roots` 并从工作副本移除。
  - `source_above_trigger`：循环结束时源层（L0，其次 L1）是否仍 ≥ 触发线。

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn drain_loop_merges_until_below_trigger() {
    // 构造 12 个 L0 segment；一轮 compact_once_with_stats：
    // assert stats.windows_merged >= 2；断言 L0 剩余 < 4（触发线）。
}

#[test]
fn wall_budget_stops_new_windows_but_first_window_always_runs() {
    // 构造 12 个 segment；set_pass_wall_budget(Duration::ZERO)；
    // 一轮：assert windows_merged == 1（min-1 保证）；source_above_trigger == true。
}

#[test]
fn vanished_member_is_evicted_and_round_converges() {
    // 构造 6 个 segment，其一 manifest.bin 缺失；
    // 连续两轮 compact_once_with_stats：
    // assert 第一轮 retired_roots 含幽灵根；第二轮不再触碰幽灵（读 hook 计数不增）且正常排空。
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib drain_loop wall_budget vanished_member_is_evicted -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

循环体 = Task 5 的单窗口逻辑；窗口内：`rewrite_segments` 成功 → 工作副本插入输出、移除源；`load_segment_root_for_compaction` 返回 `None` 的成员 → `retired_roots.push(root)` + 工作副本移除。预算检查在**开新窗口前**；已开窗口必须跑完。

- [ ] **Step 4: Run tests and audit existing expectations**

Run: `cargo test --lib compactor -- --nocapture`
Expected: PASS。**注意**：既有测试若断言"每轮只合并一个窗口"的剩余数量，属语义变更（spec 目标 2），按新语义修正断言，并在 commit message 注明。

- [ ] **Step 5: Commit**

```bash
git add src/engine/compactor.rs src/engine/compactor/planning.rs src/engine/lifecycle/shutdown.rs src/engine/compactor/tests.rs
git commit -m "feat(engine): multi-window drain loop with wall budget and convergence contract"
```

---

### Task 7: gate 按窗口持有 + 占空比修正

**Files:**
- Modify: `src/engine/compactor.rs`（`Compactor` 新增 `gate: Option<Arc<Mutex<()>>>`；`new_with_inventory_source` 增参或新增 `with_gate` builder）
- Modify: `src/engine/construction/resources.rs`（构造 Compactor 时传 `Some(Arc::clone(&compaction_lock))`）
- Modify: `src/engine/runtime.rs`（compaction 调用点 `acquire_guard` 改 `|| ()`；线程循环按 `source_above_trigger` 决定 park：`COMPACTION_BACKLOG_PARK: Duration = 1ms` vs `compaction_interval`）
- Modify: `src/engine/lifecycle/shutdown.rs:31`（`compact_until_settled` 删除外层 `let _compaction_guard`）
- Test: `src/engine/compactor/tests.rs` + `src/engine/runtime.rs` 内 `#[cfg(test)]`（纯函数）

**Interfaces:**
- Produces: `fn compaction_park_interval(stats: &CompactionRunStats, interval: Duration) -> Duration`（纯函数：`source_above_trigger` → `COMPACTION_BACKLOG_PARK`，否则 `interval`）。
- **死锁前提（必须同任务完成）**：`runtime.rs` 与 `shutdown.rs` 两处外层获取移除；获取点仅在 planning 循环窗口步骤内：`let _guard = self.gate.as_ref().map(|g| g.lock());`。`stage_segment_rewrite_with_retention` 不变（retention caller 已持 gate）。

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn flush_gate_wait_bounded_by_single_window() {
    // 主线程持有 compaction gate；子线程跑 compact_once_with_changes（有 ≥1 个待合并窗口）。
    // 断言 200ms 内未完成（被窗口 gate 阻塞）；释放 gate 后 2s 内完成（recv_timeout，避免挂死 CI）。
}

#[test]
fn compaction_park_interval_backlogged_vs_idle() {
    let mut stats = CompactionRunStats::default();
    stats.source_above_trigger = true;
    assert_eq!(compaction_park_interval(&stats, Duration::from_secs(5)), Duration::from_millis(1));
    stats.source_above_trigger = false;
    assert_eq!(compaction_park_interval(&stats, Duration::from_secs(5)), Duration::from_secs(5));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib flush_gate_wait compaction_park_interval -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

按 Interfaces 块。**执行顺序敏感**：同一 commit 内必须同时完成外层移除与内部获取，避免中间态死锁。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib compaction_park_interval flush_gate_wait && cargo test --lib compactor -- --nocapture && timeout 60 cargo test --lib persistence_background`
Expected: PASS（`timeout 60` 验证无挂死）

- [ ] **Step 5: Commit**

```bash
git add src/engine/compactor.rs src/engine/compactor/planning.rs src/engine/construction/resources.rs src/engine/runtime.rs src/engine/lifecycle/shutdown.rs src/engine/compactor/tests.rs
git commit -m "feat(engine): per-window compaction gate and backlog duty cycle"
```

---

### Task 8: 每窗口 diff 即时合并 + 部分失败契约 + 可观测性

**Files:**
- Modify: `src/engine/compactor/planning.rs`（新 API）
- Modify: `src/engine/lifecycle/shutdown.rs`（`run_compactor_once`/`compact_compactors_with_changes` 增加 per-window 回调参数）
- Modify: `src/engine/runtime.rs:274-287`（compaction 分支：闭包内 `changes.merge(window_diff)` + `persisted_index_dirty.store(true)` 逐窗口执行）
- Test: `src/engine/compactor/tests.rs` + `src/engine/tests/persistence_background.rs`

**Interfaces:**
- Produces:
  - `pub(in crate::engine) fn compact_round_with_changes(&self, on_window: &mut dyn FnMut(PendingPersistedSegmentDiff)) -> Result<CompactionRunStats>`；`compact_once_with_changes` 保留为薄包装（内部累积闭包，返回 `CompactionOutcome` 形状以兼容既有调用/测试——或在同一 commit 内迁移全部调用方并删除旧签名，builder 择一，倾向后者避免双路径）。
  - `compact_compactors_with_changes(numeric, blob, observability, on_window)`：回调参数贯穿两条 lane。
  - 可观测性：整轮 `record_compaction_result` 一次（窗口求和 + `windows_merged`）；错误按轮 `record_compaction_error` 一次。

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn partial_failure_preserves_published_windows_diff() {
    // 构造 ≥8 个 segment 使至少 2 个窗口；破坏第二个窗口的成员 chunks.bin。
    // 调用 compact_round_with_changes，on_window 闭包把每次回调的 diff 收进 Vec。
    // assert 返回 Err；Vec.len() >= 1（已发布窗口的 diff 已回调）；无"Err 丢 diff"。
}

#[test]
fn window_diff_is_merged_into_pending_diff_immediately() {
    // persistence_background 风格：跑一轮带多窗口的压缩（storage 路径），
    // 断言 pending_persisted_segment_diff 在轮内逐窗口增长（回调计数 == windows_merged），
    // 且 persisted_index_dirty 在每个窗口后为 true。
}

#[test]
fn backlog_drain_keeps_up_with_write_triggered_production() {
    // 仿真（无"4/秒"上界假设）：每轮之间向 L0 注入 20 个新 segment（写触发产生），
    // 背靠背调用 compact_round_with_changes 若干轮。
    // assert 每轮结束后 L0 计数有界（< 触发线 + 本轮注入量），最终收敛到触发线附近；
    // 若排空 < 产生，L0 将单调增长 → 测试失败。
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib partial_failure_preserves window_diff_is_merged -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

窗口成功发布（marker 写入 + 源删除处理完成）后立即构造 `PendingPersistedSegmentDiff::record_changes(output_roots, source_roots ∪ retired_roots)` 并调用 `on_window`；错误传播路径不变（`?`），已回调的 diff 天然保留。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib partial_failure_preserves window_diff_is_merged && cargo test --lib compactor && timeout 60 cargo test --lib persistence_background`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/compactor/planning.rs src/engine/lifecycle/shutdown.rs src/engine/runtime.rs src/engine/tests/persistence_background.rs
git commit -m "feat(engine): per-window diff merge and partial-failure contract"
```

---

### Task 9: close 路径压缩错误按 deferral 处理

**Files:**
- Modify: `src/engine/lifecycle/shutdown.rs`（`execute_close_pipeline` 中 `compact_until_settled` 的错误分支；`LifecycleShutdownContext` 增加 deferral 记录复用 `record_deferred_dirty_refresh` 的模式）
- Test: `src/engine/tests/`（close 场景；模仿既有 close 测试文件，若无为 `src/engine/tests/close_pipeline.rs` 新建）

**Interfaces:**
- Consumes: `close_should_defer_dirty_refresh_error` 的处理模式（shutdown.rs:105-108）。

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn close_with_failing_compaction_still_persists_refresh_and_tombstones() {
    // 构造含损坏段（hash 失配）的存储 → close()。
    // assert close 返回 Ok；observability 的 maintenance error 记录含 "close compaction"；
    // tombstones index 与 registry checkpoint 均已写出（文件存在）。
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib close_with_failing_compaction -- --nocapture`
Expected: FAIL（close 返回 Err）

- [ ] **Step 3: Implement**

```rust
if let Err(err) = shutdown.compact_until_settled(CLOSE_COMPACTION_MAX_PASSES) {
    shutdown.record_deferred_dirty_refresh(&err); // 或独立的 close compaction deferral 记录
    tracing::warn!(error = %err, "Close deferred compaction; refresh and tombstone persistence continue");
    // 继续执行后续 refresh/tombstone/checkpoint；compaction 残留由 marker + 重启 discovery 兜底
} else { /* 现有 dirty 刷新分支不变 */ }
```

注意：压缩失败时 `persisted_index_dirty` 可能已被逐窗口置位（Task 8），refresh 分支照常执行。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib close_with_failing_compaction && cargo test --lib close -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/lifecycle/shutdown.rs src/engine/tests/
git commit -m "feat(engine): close defers compaction errors instead of aborting pipeline"
```

---

### Task 10: files[] 贯通库存（catalog 内存化的数据基础）

**Files:**
- Modify: `src/engine/segment/format.rs`（新增 `read_segment_manifest_with_files(path) -> Result<(SegmentManifest, [ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT])>`，复用 `read_segment_manifest_fingerprint` 的解析内部）
- Modify: `src/engine/state/persisted.rs:116`（`PersistedSegmentState` 增加 `pub files: Option<[ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT]>`）
- Modify: `src/engine/lifecycle/persisted_state/install.rs:330`（insert 处填充：`segment_manifest` 来源若已含文件则直传；否则对 root 调 `read_segment_manifest_with_files` 补一次）
- Modify: `src/engine/tiering/inventory.rs:44`（`SegmentInventoryEntry` 增加 `pub files: Option<[ManifestFileEntry; MANIFEST_FILE_ENTRY_COUNT]>`；`persisted_segment_inventory()`（catalog_refresh/context.rs:165-179）与 `inventory_sources` 相关构造点同步拷贝）
- Test: `src/engine/segment/tests.rs` + `src/engine/tiering` 既有测试适配

**Interfaces:**
- Produces: `SegmentInventoryEntry.files`（Option；`None` 时 catalog 构建允许对 root 做一次有界 `read_segment_manifest_with_files` 回退）。

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn persisted_state_carries_manifest_files() {
    // 构造存储并 flush 发布一个 segment；
    // 读 persisted_index.segments_by_root[root].files：assert Some，且 4 项的 file_len/hash64
    // 与直接 read_segment_manifest_with_files(root) 的结果一致。
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib persisted_state_carries_manifest_files -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

按 Interfaces 块；`PersistedSegmentState` 的全部构造点（install、diff apply、discovery）补 files（各自来源：writer/manifest 解析/一次有界重读）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib persisted_state_carries_manifest_files && cargo test --lib tiering && cargo test --lib segment`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/segment/format.rs src/engine/state/persisted.rs src/engine/lifecycle/persisted_state/install.rs src/engine/tiering/inventory.rs src/engine/maintenance/catalog_refresh/context.rs
git commit -m "feat(engine): manifest file entries threaded through persisted inventory"
```

---

### Task 11: C-1 — registry catalog 从内存构建 + 贡献缓存 + bootstrap 预热

**Files:**
- Create: `src/engine/registry_contributions.rs`
- Modify: `src/engine/registry_catalog.rs`（`build_catalog_entries` 与 `validate_registry_catalog` 增加库存驱动变体；`build_series_fingerprint_from_sources` 保留为磁盘路径供回退/对照）
- Modify: `src/engine/maintenance/registry_persistence.rs`（catalog 相关调用点改为库存+贡献驱动）
- Modify: `src/engine/construction/assembly.rs`（bootstrap 预热调用：在 persisted index 构建完成、后台 flush/refresh 线程 spawn 之前；对无贡献缓存的 root 逐段读 `series.bin`（仅解析）填充缓存）
- Modify: `src/engine/state/persisted.rs`（`PersistedIndexState` 挂 `registry_contributions: RegistryContributions`，或经 `ChunkStorage` 字段持有——builder 按锁纪律择一：**不得在持有 persisted_index 读锁时做磁盘读**）
- Test: `src/engine/registry_contributions.rs` `#[cfg(test)]` + `src/engine/tests/persistence_background.rs`

**Interfaces:**
- Produces:
  - `pub(in crate::engine) struct RegistryContributions`：`fn prime(&self, entries: &[(PathBuf, SeriesRun)])`；`fn replace_root(&self, root: &Path, run: SeriesRun)`；`fn remove_root(&self, root: &Path)`；`fn merge_fingerprint(&self) -> Result<PersistedRegistrySeriesFingerprint>`（按 SeriesId 升序归并全部 run，含既有冲突检查：同 id 异 metric/labels、同 key 异 id → `DataCorruption`）。
  - `pub(in crate::engine) struct SeriesRun(pub Vec<(SeriesId, Arc<(String, Vec<Label>)>)>)`（按 SeriesId 升序；Arc 共享去重 key 内存）。
  - `pub(in crate::engine) fn build_catalog_entries_from_inventory(inventory: &SegmentInventory, contributions: &RegistryContributions) -> Result<Vec<PersistedRegistryCatalogEntry>>`：`files: None` 的条目回退一次 `read_segment_manifest_with_files`。
  - fingerprint 归并成本 O(distinct series)；缓存足迹 O(Σ per-segment series)，超界（builder 定阈值，如 > 4 × distinct）→ 该次写回退磁盘路径。

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn memory_built_catalog_matches_disk_built_catalog() {
    // 同一批段：磁盘路径 build_catalog(sources) vs 内存路径 build_catalog_entries_from_inventory
    // + contributions（prime 自各段 series.bin 解析结果）。
    // assert serde_json 序列化字节一致（schema 逐字节同构）。
}

#[test]
fn catalog_write_after_publish_does_not_touch_segment_files() {
    // CountingSegmentReads::install(data_root)；flush 发布一个新段后走一次 catalog 构建；
    // assert 旧段（非新段）的 series.bin / manifest.bin 读计数为 0（仅新段允许一次 manifest 读）。
}

#[test]
fn contribution_is_sourced_from_segment_series_file() {
    // flush 发布已知 series 集合的段 → 取该 root 的缓存 run，
    // 与直接解析磁盘 series.bin 的 (id, metric, labels) 集合逐一相等。
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib memory_built_catalog catalog_write_after_publish contribution_is_sourced -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

- `registry_contributions.rs`：按 Interfaces 实现（root-keyed `Mutex<HashMap<..>>`；root 移除逐出；`replace_root` 由 flush 发布与 compaction diff 应用调用）。
- `registry_catalog.rs`：库存驱动变体；既有磁盘路径保留（回退 + 对照测试）。
- `assembly.rs` 预热：在 `persisted_index` 组装完成后、flush/refresh 线程 spawn 前调用 `prime`（worker 未起，无 gate 争议）；预热读盘失败 → warn + 该 root 标记"未预热"，catalog 构建时对未预热 root 走磁盘回退（不 fail startup）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib registry_contributions memory_built_catalog catalog_write_after_publish contribution_is_sourced && cargo test --lib registry_catalog -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/registry_contributions.rs src/engine/registry_catalog.rs src/engine/maintenance/registry_persistence.rs src/engine/construction/assembly.rs src/engine/state/persisted.rs src/engine/tests/persistence_background.rs
git commit -m "feat(engine): registry catalog built from in-memory inventory with contribution cache"
```

---

### Task 12: C-2 — catalog 重写去抖出 flush 关键路径

**Files:**
- Modify: `src/engine/lifecycle/flush.rs:337-365`（`persist_flush_recovery_metadata_stage` 中传给 `persist_series_registry_index` 的 catalog 回调改为 `mark_registry_catalog_dirty()`；registry index delta 写保持内联——崩溃恢复关键）
- Modify: `src/engine/state/persisted.rs`（`registry_catalog_dirty: AtomicBool`）
- Modify: `src/engine/maintenance/catalog_refresh.rs`（`sync_persisted_segments_from_disk_if_dirty` 末尾：`if registry_catalog_dirty { 从内存写 catalog（Task 11 路径）; clear }`——在 diff 应用之后执行）
- Test: `src/engine/tests/persistence_background.rs`

**Interfaces:**
- Consumes: Task 11 的内存 catalog 构建。
- Produces: `fn mark_registry_catalog_dirty(&self)`、`fn take_registry_catalog_dirty(&self) -> bool`。
- 语义：close 路径 `checkpoint_series_registry_index` 保持同步写 catalog（现有行为不变，恢复最终一致）。

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn flush_publish_does_not_rewrite_catalog_inline() {
    // CountingSegmentReads::install(data_root) + 记录 catalog 文件 mtime；
    // flush 发布一个段；assert catalog 文件未变（mtime/字节不变）且 registry_catalog_dirty == true。
}

#[test]
fn refresh_pass_writes_debounced_catalog_after_diff_application() {
    // 接上：调用 sync_persisted_segments_from_disk_if_dirty()；
    // assert catalog 文件已更新且包含新段条目（字节与内存构建一致），dirty 标志清除。
}

#[test]
fn stale_catalog_falls_back_to_full_rebuild_on_startup() {
    // 手工破坏 catalog（删除某条目）→ 重建存储（startup）；
    // assert registry 完整（经既有 fingerprint 失配 → 全量重建路径），无数据丢失。
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib flush_publish_does_not_rewrite refresh_pass_writes_debounced stale_catalog_falls_back -- --nocapture`
Expected: FAIL

- [ ] **Step 3: Implement**

flush 路径回调替换 + 标志；refresh 路径在 diff 应用后按 Task 11 内存构建写 catalog（一次原子写 + fsync parent，沿用 `write_file_atomically_and_sync_parent`）。

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib flush_publish_does_not_rewrite refresh_pass_writes_debounced stale_catalog_falls_back && timeout 120 cargo test --lib persistence_background`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/lifecycle/flush.rs src/engine/state/persisted.rs src/engine/maintenance/catalog_refresh.rs src/engine/tests/persistence_background.rs
git commit -m "feat(engine): debounce registry catalog writes off the flush path"
```

---

### Task 13: 全量回归 + NFS 开发环境验证清单

**Files:**
- Modify: `docs/superpowers/plans/2026-11-15-compaction-inventory.md`（本文件，追加验证记录）
- 无产品代码（若回归发现缺陷，回到对应 Task 修复后重跑）

**Interfaces:**
- Consumes: Task 1-12 全部。

- [ ] **Step 1: 全量测试**

Run: `cargo test --lib && cargo test && cargo clippy --all-targets -- -D warnings`
Expected: 全部 PASS

- [ ] **Step 2: NFS 开发环境部署验证（手动，root@172.26.120.19）**

1. 备份 `/opt/data/tsink` 元数据文件（catalog/manifest 不可变，仅记录）。
2. 部署新构建替换 `iotp_tsink-server` 容器（现积压 L0 ≈ 9310/9237）。
3. 观察点（预期）：
   - 启动日志无新增 `Data corruption detected: ... missing manifest.bin`；
   - `Registry catalog source vanished mid-removal` 不再出现；
   - 压缩轮次间隔与 L0 规模无关（积压期背靠背，L0 递减至触发线附近）；
   - `find /opt/data/tsink -type f | wc -l` 逐日单调下降直至积压清空（find 用长超时）；
   - `Compaction source removal deferred` 仍可能出现（NFS silly-rename，非缺陷）。

- [ ] **Step 3: 记录验证结果并提交**

```bash
git add docs/superpowers/plans/2026-11-15-compaction-inventory.md
git commit -m "docs(plan): compaction inventory verification record"
```
