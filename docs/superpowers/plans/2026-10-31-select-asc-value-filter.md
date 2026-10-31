# select_asc + Value Predicate 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增流式正向扫描 `Storage::select_asc`（时间升序、凑足 limit 即停），并为 `select_asc`/`select_desc` 统一加上单值比较谓词（`ValuePredicate`）。

**Architecture:** 镜像现有 `reverse_scan.rs` 的 cursor + BinaryHeap 归并模型实现 `forward_scan.rs`；谓词插在两方向过滤链尾（retention → tombstone → dedupe → predicate）；trait 签名贯穿 engine → tenant → server HTTP 端点。

**Tech Stack:** Rust（无新依赖），server 端 serde JSON。

**Spec:** `docs/superpowers/specs/2026-10-31-select-asc-value-filter-design.md`

## Global Constraints

- 阶段性成果自动提交（语义化 commit message）；未 push 的小修复可 `git commit --amend`；**不自动 push**。
- 零新增依赖；谓词为纯标准库类型。
- `select_desc` 扫描语义与输出不变；允许的变化仅限：签名加谓词参数、API 入口补 refresh hook、新增查询计数器。
- 原生 `select` / `select_with_options` / `select_raw_series_page` 路径**行为不变**（含"保留重复时间戳"语义）。
- 谓词求值规则：类型严格匹配（`I64`/`U64`/`F64` 三者互斥），类型不符刷掉不报错；NaN 参与序比较一律不匹配；`Eq`/`Ne` 用库内 `f64_eq` 约定（`NaN == NaN` 为 true）。
- 多版本：谓词作用于去重后最新版本，不回退旧版本；asc 归并 tie-break：时间戳最小先出、同 ts 时 recency/sequence **最大**先出（不是 desc Ord 的简单反转）。
- offset：asc/desc HTTP 请求均拒绝。
- 集群：不改 `InternalSelectRequest`、不覆盖 `DistributedStorage::select_asc`、集群模式下端点继续 unsupported。

## Review Focus

1. **同 ts 多版本 + 谓词**：覆盖写入后，谓词刷掉最新版本时该 ts 必须完全消失（不得回退旧值）→ Task 2 测试 `desc_predicate_rejects_latest_version_drops_timestamp`；Task 4 对称测试 `asc_...`。
2. **asc 同 ts 版本裁决**：active/sealed/persisted 同 ts 并存时 asc 必须输出最新值 → Task 4 测试 `asc_merge_prefers_newest_version_at_equal_timestamp`。
3. **NaN 输入**：`F64(NaN)` 点遇 `Lt/Gt` 被刷、遇 `Eq(NaN)` 匹配、遇 `Ne(NaN)` 不匹配 → Task 1 单元测试钉死。
4. **谓词下提前终止仍成立**：谓词刷点时 cursor 必须推进同一 chunk（重排 heap），不卡死也不漏点 → Task 2/4 各一个"谓词过滤 + 跨块数据凑足 limit"测试。
5. **HTTP asc 行为变化**：`/api/v1/select` `order=asc` 从"保留重复 ts"变为"每 ts 最新值" → Task 5 测试 `select_asc_http_dedupes_duplicate_timestamps`。

---

### Task 1: `ValuePredicate` 类型与求值

**Files:**
- Modify: `src/value.rs`
- Modify: `src/lib.rs`（re-export）
- Test: `src/value.rs`（文件内 `#[cfg(test)]`，参照该文件既有测试风格）

**Interfaces:**
- Produces（Task 2-5 依赖的精确签名）:

```rust
// src/value.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValuePredicateOp { Lt, Lte, Gt, Gte, Eq, Ne }

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValuePredicateTarget { I64(i64), U64(u64), F64(f64) }

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ValuePredicate {
    pub op: ValuePredicateOp,
    pub target: ValuePredicateTarget,
}

impl Value {
    /// 类型严格匹配：`Value::I64` 只与 `ValuePredicateTarget::I64` 比较，
    /// 其余类型（U64/F64 互跨、Bool/Bytes/String/Histogram）一律 false。
    pub fn matches_predicate(&self, predicate: &ValuePredicate) -> bool
}
```

- `lib.rs` re-export：`ValuePredicate`、`ValuePredicateOp`、`ValuePredicateTarget`（加到现有 `Value` 的 re-export 位置）。

- [ ] **Step 1: 写失败测试**（`src/value.rs` 测试模块）

```rust
#[test]
fn predicate_strict_type_matching() {
    let p = ValuePredicate { op: ValuePredicateOp::Gt, target: ValuePredicateTarget::I64(42) };
    assert!(Value::I64(43).matches_predicate(&p));
    assert!(!Value::I64(42).matches_predicate(&p));      // Gt 严格大于
    assert!(!Value::U64(43).matches_predicate(&p));      // U64 不跨 I64 谓词
    assert!(!Value::F64(43.0).matches_predicate(&p));    // F64 不跨 I64 谓词
    assert!(!Value::Bool(true).matches_predicate(&p));   // 非数值类型刷掉
}

#[test]
fn predicate_nan_semantics() {
    let gt = ValuePredicate { op: ValuePredicateOp::Gt, target: ValuePredicateTarget::F64(1.0) };
    assert!(!Value::F64(f64::NAN).matches_predicate(&gt));   // NaN 序比较不匹配
    let eq_nan = ValuePredicate { op: ValuePredicateOp::Eq, target: ValuePredicateTarget::F64(f64::NAN) };
    assert!(Value::F64(f64::NAN).matches_predicate(&eq_nan));    // 库内约定 NaN==NaN
    let ne_nan = ValuePredicate { op: ValuePredicateOp::Ne, target: ValuePredicateTarget::F64(f64::NAN) };
    assert!(!Value::F64(f64::NAN).matches_predicate(&ne_nan));
}

#[test]
fn predicate_all_ops_on_f64_and_u64() {
    // Lte/Gt 边界值、Eq/Ne、U64 大值（> 2^53）路径各一断言
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test -p tsink --lib predicate_`，预期 FAIL（类型不存在）
- [ ] **Step 3: 实现** — `matches_predicate`：序比较用 `partial_cmp`，NaN 情形返回 false；`Eq`/`Ne` 的 F64 分支复用 `value.rs` 中现有 `f64_eq`（NaN==NaN 为 true）；I64/U64 直接整型比较
- [ ] **Step 4: 运行确认通过** — `cargo test -p tsink --lib predicate_`
- [ ] **Step 5: Commit** — `git commit -m "feat(value): add ValuePredicate with strict-type, NaN-aware evaluation"`

### Task 2: trait 签名贯穿 + desc 谓词过滤

**Files:**
- Modify: `src/storage.rs`（`Storage` trait，`select_desc` 约 649-662 行）
- Modify: `src/engine/query_read/reverse_scan.rs`（`select_desc_api` / `select_desc_with_plan` / `select_desc_for_series_with_plan` 及主循环）
- Modify: `src/engine/engine.rs:373-381`（trait 实现）
- Modify: `crates/tsink-server/src/tenant.rs:1672`（包装透传）
- Modify: `crates/tsink-server/src/server.rs:1665` 附近（测试 stub 跟随签名）
- Test: `src/engine/query_read/tests.rs`（参照既有 select_desc 测试，约 384 行处的构造模式）

**Interfaces:**
- Consumes: Task 1 的 `ValuePredicate` / `Value::matches_predicate`
- Produces:

```rust
// src/storage.rs — Storage trait（默认实现保持 UnsupportedOperation，operation: "select_desc"）
fn select_desc(
    &self, metric: &str, labels: &[Label], start: i64, end: i64,
    limit: usize, predicate: Option<&ValuePredicate>,
) -> Result<Vec<DataPoint>>;

// reverse_scan.rs 内部链（谓词一路透传到过滤链）
fn select_desc_api(&self, metric: &str, labels: &[Label], start: i64, end: i64,
    limit: usize, predicate: Option<&ValuePredicate>) -> Result<Vec<DataPoint>>;
```

- [ ] **Step 1: 写失败测试**（`src/engine/query_read/tests.rs`，用现有 select_desc 测试的 storage 构造方式）

```rust
#[test]
fn desc_predicate_filters_points_and_stops_early() {
    // 跨块数据：写 >64 点（≥2 个搜索块），谓词只匹配稀疏子集；
    // select_desc(..., limit, Some(&pred)) 返回按时间降序的前 limit 个匹配点，
    // 且 stats.points_decoded < 全窗口点数（提前终止未被谓词破坏）
}
#[test]
fn desc_predicate_none_matches_plain_desc() {
    // 同一数据集：predicate=None 的结果与改动前行为逐点相等（可用 select 全量倒排对照）
}
#[test]
fn desc_predicate_rejects_latest_version_drops_timestamp() {
    // 同 ts 写两次（v1 不匹配、v2 匹配）→ 返回 v2；
    // v2 不匹配、v1 匹配 → 该 ts 不出现（不回退）
}
#[test]
fn desc_predicate_drops_wrong_typed_values() {
    // series 值为 F64、谓词 I64 → 全部刷掉返回空；bytes/histogram 值同理
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test -p tsink --lib desc_predicate`，预期 FAIL（签名不存在）
- [ ] **Step 3: 实现**
  - trait `select_desc` 加第 6 参 `predicate: Option<&ValuePredicate>`；默认实现 `let _ = predicate;` 保持 `UnsupportedOperation`
  - `reverse_scan.rs` 三个函数签名透传；主循环在 `seen_timestamps` 检查之后、`out.push` 之前插入：

```rust
if let Some(predicate) = predicate {
    if !point.value.matches_predicate(predicate) {
        continue;   // cursor 已 re-queue，同一 chunk 继续补位
    }
}
```
  - `engine.rs` / `tenant.rs` / server 测试 stub 签名跟随（透传谓词，逻辑不变）
- [ ] **Step 4: 运行确认通过** — `cargo test -p tsink --lib desc_predicate && cargo test --workspace`（workspace 验证签名变更无遗漏实现者）
- [ ] **Step 5: Commit** — `git commit -m "feat(engine): thread value predicate through select_desc filter chain"`

### Task 3: desc/asc 查询计数器 + desc API 入口 refresh hook

**Files:**
- Modify: `src/engine/observability.rs`（counters struct + snapshot struct，参照 `select_with_options_*` 四件套模式）
- Modify: `src/engine/query_read/reverse_scan.rs:66-88`（`select_desc_api`）
- Test: `src/engine/observability.rs` 或既有 observability 测试位置（先 `grep -rn "select_with_options_calls_total" src/engine/ --include="*.rs" -l` 找测试落点）

**Interfaces:**
- Produces: `observability.query.select_asc_calls_total / select_asc_errors_total / select_asc_duration_nanos_total / select_asc_points_returned_total` 及 `select_desc_*` 同名四件套（Task 4 的 `select_asc_api` 使用）。

- [ ] **Step 1: 写失败测试** — 调一次 `select_desc_api` 后断言 `select_desc_calls_total` 增加 1（snapshot 读取方式参照现有计数器测试）
- [ ] **Step 2: 确认失败** — `cargo test -p tsink --lib select_desc_calls` 
- [ ] **Step 3: 实现** — observability.rs 两组四件套字段（counters + snapshot，照抄 `select_with_options_*` 的加载/曝光代码路径）；`select_desc_api` 加 `request_background_persisted_refresh_if_needed()` + 计数包裹（照抄 `select_api.rs:18-66` 的 calls/duration/points/errors 模式）
- [ ] **Step 4: 确认通过** — `cargo test -p tsink --lib observability`
- [ ] **Step 5: Commit** — `git commit -m "feat(engine): observability and persisted-refresh hook for desc/asc scans"`

### Task 4: `forward_scan.rs` — select_asc 流式实现

**Files:**
- Create: `src/engine/query_read/forward_scan.rs`
- Modify: `src/engine/query_read.rs`（模块注册，仿 `mod reverse_scan;`）
- Modify: `src/engine/query_read/reverse_scan.rs`（`collect_candidates_with_mode` 加方向参数；自由函数 `last_in_window_index` / `first_in_window_index` / `timestamps_in_index_range` 改 `pub(super)` 供 forward_scan 复用；`HeapEntry`/`CursorState`/`CursorData` 保持 desc 专用不动）
- Modify: `src/engine/engine.rs`（trait 实现 `select_asc` → `select_asc_api`）
- Modify: `crates/tsink-server/src/tenant.rs`（`select_asc` 包装，照抄 desc 的 scoped_labels + default-tenant fallback 模式）
- Test: `src/engine/query_read/tests.rs`

**Interfaces:**
- Consumes: Task 1 谓词、Task 2 签名模式、Task 3 计数器
- Produces:

```rust
// forward_scan.rs（镜像 reverse_scan.rs 结构）
pub(in crate::engine::storage_engine) fn select_asc_api(
    &self, metric: &str, labels: &[Label], start: i64, end: i64,
    limit: usize, predicate: Option<&ValuePredicate>) -> Result<Vec<DataPoint>>;
// 含：ensure_open/validate/limit==0→空/request_background_persisted_refresh_if_needed/
//     select_asc_* 计数包裹/query_tier_plan
pub(super) fn select_asc_with_plan(...) -> Result<(Vec<DataPoint>, ForwardScanStats)>;
pub(super) fn select_asc_for_series_with_plan(
    series_id: SeriesId, start: i64, end: i64, limit: usize,
    plan: TieredQueryPlan, predicate: Option<&ValuePredicate>) -> Result<(Vec<DataPoint>, ForwardScanStats)>;
```

关键镜像点（实现时对照 reverse_scan.rs）：
- **collect 方向化**：`collect_candidates_with_mode` 增加 `direction: ScanDirection`（`pub(super) enum ScanDirection { Asc, Desc }`，定义于 reverse_scan.rs）；`collect_candidates`/`collect_candidates_without_tail_peek` 传 `Desc`（调用点不变）。Asc 分支：sealed/persisted 枚举沿用现有 range 后过滤 `min_ts >= start`（下界优化 builder 酌情）；对 `min_ts < start` 的 chunk 做 **head-peek**——`first_in_window_index` 取窗口内最早点 index，再解该单点 ts 为 `min_ts`，无则跳过该 chunk；`peeked_chunks` 计数镜像。预排序键：`a.min_ts.cmp(&b.min_ts).then(b.recency.cmp(&a.recency)).then(b.sequence.cmp(&a.sequence))`。
- **AscHeapEntry Ord**（BinaryHeap 最大者先出 → 权重为 Reverse(ts) × max(recency, sequence)）：

```rust
other.timestamp.cmp(&self.timestamp)
    .then_with(|| self.recency.cmp(&other.recency))
    .then_with(|| self.sequence.cmp(&other.sequence))
    .then_with(|| self.candidate_index.cmp(&other.candidate_index))
```
- **AscCursorData**：`Refs`（sort_by_key 不 reverse）、`Buffered`（升序 fallback，过滤 `point.ts > ceiling`，ceiling 即 asc 版 last_emitted_ts）、`Encoded`（正向 batch：`next_index` 从 `first_index` 起，块对齐 batch_end ≤ `last_index`，`timestamps_in_index_range(next_index..=batch_end)`；ascending 检查同 desc；boundary 检查取 `batch_min >= previous_batch_max_ts`；`stats.points_decoded` 累加同 desc）
- **过滤链 + 主循环**：与 desc 完全同构（retention → tombstone → seen_timestamps → predicate → push），`while out.len() < limit`。

- [ ] **Step 1: 写失败测试**（`src/engine/query_read/tests.rs`，数据构造参照现有 desc 测试）

```rust
#[test]
fn asc_scan_returns_strict_ascending_with_limit() {
    // 跨 3 个 chunk 写 200 点，select_asc(..., 5, None) 返回窗口内最早 5 点、严格升序
}
#[test]
fn asc_merge_prefers_newest_version_at_equal_timestamp() {
    // 同 ts：persisted v1、active v2 → 返回 v2；含 sealed vs persisted 同 ts 用例
}
#[test]
fn asc_predicate_filters_points_and_stops_early() { /* 对称于 Task 2 desc 版 */ }
#[test]
fn asc_predicate_none_matches_select_desc_reversed() {
    // parity：asc(None) == desc(None).reverse()（同数据集逐点相等）
}
#[test]
fn asc_and_desc_stop_early_on_limit() {
    // stats（ForwardScanStats/ReverseScanStats）断言：limit=3 时 points_decoded 远小于全窗口
}
```

- [ ] **Step 2: 确认失败** — `cargo test -p tsink --lib asc_scan`，预期 FAIL（方法不存在）
- [ ] **Step 3: 实现** — 按"关键镜像点"清单实现 forward_scan.rs + 方向化 collect + engine.rs/tenant.rs 接线
- [ ] **Step 4: 确认通过** — `cargo test -p tsink --lib asc_scan && cargo test --workspace`
- [ ] **Step 5: Commit** — `git commit -m "feat(engine): add select_asc forward streaming scan with early limit stop"`

### Task 5: server `/api/v1/select` — value_filter + asc 切换

**Files:**
- Modify: `crates/tsink-server/src/handlers/public_api/select.rs`（`RawSelectRequest` 约 28-56 行、分发 `match query.order` 约 129-144 行、offset 校验 274 行、文件内测试约 592/1079 行处）
- Modify: `crates/tsink-server/src/server.rs:1635-1665`（测试 stub `select_desc` 跟随签名；stub 是否实现 `select_asc` 由 stub 用途定——仅当 asc HTTP 测试需要时实现）
- Test: `crates/tsink-server/src/handlers/public_api/select.rs`（文件内 HTTP 测试）

**Interfaces:**
- Consumes: Task 2/4 的 trait 方法
- Produces（HTTP 契约，写入 docs）：

```json
{ "order": "desc", "limit": 10,
  "value_filter": { "op": "gt", "type": "i64", "value": 42 } }
```
`op ∈ {lt,lte,gt,gte,eq,ne}`；`type ∈ {i64,u64,f64}`；`value` 为 JSON number 按 type 取值；`value_filter` 缺省 = 无谓词；`offset` 在 asc/desc 下均 400。

- [ ] **Step 1: 写失败测试**（照抄文件内既有 HTTP 测试模式，约 592 行 baseline 与 1079 行 tenant 隔离的构造方式）

```rust
// select_http_desc_applies_value_filter：i64 值 series，filter gt → 只回匹配点
// select_http_asc_applies_value_filter：同上 asc
// select_http_value_filter_rejects_wrong_typed_values：F64 series + i64 filter → 空结果
// select_http_rejects_offset_for_both_orders：asc/desc + offset → 400
// select_http_asc_dedupes_duplicate_timestamps：同 ts 两写 → 单条最新值
// select_http_rejects_malformed_value_filter：非法 op/type/value → 400
// select_http_tenant_isolation_with_value_filter：照抄 1079 行模式
```

- [ ] **Step 2: 确认失败** — `cargo test -p tsink-server select_http` 
- [ ] **Step 3: 实现**
  - `RawValueFilter { op, r#type, value }` 反序列化 + `TryInto<ValuePredicate>`（非法组合 400，错误信息含字段名）
  - `RawSelectRequest.value_filter: Option<RawValueFilter>`（`#[serde(default)]`）
  - offset 校验改为两方向均拒绝（复用现有 400 路径与文案风格）
  - 分发：`Asc => storage.select_asc(&query.metric, &query.labels, query.start, query.end, query.limit, predicate)`；`Desc => storage.select_desc(..., predicate)`；`predicate = query.value_filter.as_ref().map(|f| f.clone().into())`
  - server.rs 测试 stub 签名跟随
- [ ] **Step 4: 确认通过** — `cargo test -p tsink-server select_http && cargo test --workspace`
- [ ] **Step 5: Commit** — `git commit -m "feat(server): value_filter and select_asc wiring for /api/v1/select"`

### Task 6: 文档同步 + 全量验证

**Files:**
- Modify: `README.md`（查询特性一句话，参照 `19b5acf` 的加行位置）
- Modify: `docs/embedded-library.md`（约 244 行 `select_desc` 小节：补 `select_asc`、谓词参数、成本模型两句话）
- Modify: `docs/http-api.md`（约 328 行 `POST /api/v1/select` 小节：`value_filter` 字段表、asc 去重语义、offset 双向拒绝）

**Interfaces:** 无代码接口；文档描述以 Task 5 的 HTTP 契约和 spec 的"成本模型与配额"节为准。

- [ ] **Step 1: 更新三个文档**（内容要点：谓词语法、类型严格匹配与 NaN 行为、asc/desc 去重契约、谓词下最坏情况解码全窗口的成本说明、多租户放大面提示）
- [ ] **Step 2: 全量验证** — `cargo test --workspace && cargo clippy --workspace --all-targets`（clippy 若有存量警告，保证不新增）
- [ ] **Step 3: Commit** — `git commit -m "docs: document select_asc, value_filter, and asc dedup semantics"`
