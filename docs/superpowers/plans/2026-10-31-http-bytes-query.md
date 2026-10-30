# HTTP bytes API 与倒序/点查查询 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 tsink-server 暴露 bytes 写入与两类查询端点（asc/desc 范围查询、ts 列表点查），引擎新增 `select_desc`（倒序尾读）与 `select_at_timestamps`（分组点查）两条读路径。

**Architecture:** 两条新读路径作为 `Storage` trait 新方法（默认 `UnsupportedOperation`，`ChunkStorage` 覆写），实现放在 `query_read` 模块新文件中，复用 chunk 索引二分、64 点 anchor 块、索引区间解码三块现有设施；HTTP 层新增三个 handler，鉴权/RBAC/租户/admission 复用现有模式。升序查询管道零改动。

**Tech Stack:** Rust（workspace: `tsink` 核心库 + `tsink-server`）；serde/serde_json；base64（已有依赖）；既有自研 HTTP 框架（无 axum）。

**Spec:** `docs/superpowers/specs/2026-10-30-http-bytes-query-design.md`

## Global Constraints

- INV-1：desc 绝不允许升序读取全窗口再截尾；触达 chunk 数与解码点数与窗口宽度无关。
- INV-2：一次点查调用内每个被触达 chunk 至多解码/解压一次；解压次数 = 触达 zstd chunk 组数。
- 上限常量：单值 ≤ 64 KiB（`MAX_VALUE_BYTES = 64 * 1024`）；单请求 ≤ 10 000 行；body ≤ 32 MiB；ts 列表 ≤ 10 000；`limit` ∈ 1..=100 000（必填）。
- 区间语义 [start, end)，end 排他。
- 值编码：类型标签制（bytes=base64 标准字母表 STANDARD 含 padding / f64 / i64 / u64 / bool / string=UTF-8 原文）；histogram 写读均 400。
- 重复 ts：desc/点查折叠为 1 条（最新来源版本：active → sealed → persisted）；asc 保持既有行为。
- 集群模式（`cluster_context.is_some()`）三端点返回 501；单机多租户必须工作（`TenantScopedStorage` 转发新方法）。
- 新路由必须注册 RBAC path→permission 表；不得改动 codec/WAL/compaction/PromQL/升序合并管道。
- desc/点查必须镜像应用 retention 与 tombstone 过滤，limit 按可见点计数。
- 每个 Task 结束 `cargo test` 相关目标通过后 commit（不 push）。

## Review Focus

1. **重复 ts 分歧**：同 (series, ts) 双写后 desc/点查必须返回 1 条、asc 返回 2 条 → Task 5 `duplicate_ts_divergence_desc_vs_asc`。
2. **全局降序**：旧分区 active head + 更晚分区 sealed chunk 并存时 desc 输出严格降序 → Task 1 `desc_global_order_mixed_sources`。
3. **RBAC 漏注册 = 静默无鉴权** → Task 4 `new_endpoints_map_to_tenant_permissions`。
4. **集群静默本地读写** → Task 5 `cluster_mode_returns_501`。
5. **INV-2 重复解压** → Task 2 `point_scan_single_decompression_per_chunk_group`；INV-1 → Task 1 `inv1_decoded_points_bounded`。

---

### Task 1: 引擎 `select_desc` 倒序尾读（INV-1）

**Files:**
- Create: `src/engine/query_read/reverse_scan.rs`
- Modify: `src/engine/query_read.rs`（`mod reverse_scan;`，按字母序插入现有 `mod` 列表）、`src/storage.rs`（`trait Storage` 内、`select_with_options` 之后新增默认方法）、`src/engine/engine.rs`（`impl Storage for ChunkStorage` 内覆写，:330 起）
- Test: `tests/desc_select_test.rs`（新建，仓库根 tests/ 目录，模式参照 `tests/typed_value_behaviors_test.rs`）；基准断言用 `src/engine/query_read/tests.rs` 追加

**Interfaces:**
- Produces（Task 5 依赖）：
  ```rust
  // src/storage.rs，trait Storage 内：
  fn select_desc(
      &self,
      metric: &str,
      labels: &[Label],
      start: i64,
      end: i64,
      limit: usize,
  ) -> Result<Vec<DataPoint>> {
      Err(TsinkError::UnsupportedOperation {
          operation: "select_desc",
          reason: "not implemented for this storage backend".to_string(),
      })
  }
  ```
- Produces（Task 2 依赖的共享设施）：`reverse_scan.rs` 内
  ```rust
  pub(super) struct ReverseScanStats { pub chunks_touched: usize, pub points_decoded: usize }
  pub(super) struct ChunkCandidate { pub max_ts: i64, pub recency: SourceRecency, /* …chunk 句柄 */ }
  pub(super) enum SourceRecency { Active, Sealed, Persisted }
  ```
  及候选收集函数 `collect_candidates(...)`（供 `point_scan` 复用；具体参数签名按 `select_into_impl`（query_read.rs:147）的取数方式定，实现者自行对齐内部字段）。

- [ ] **Step 1: 写失败测试** `tests/desc_select_test.rs`

```rust
// 核心断言（构造方式：StorageBuilder::new().with_data_path(tmp)
//   .with_chunk_points(4).with_timestamp_precision(Milliseconds).build()）
#[test]
fn desc_returns_last_limit_points_descending() {
    // 写入 ts=100..=111 共 12 点（同 series），select_desc(metric, &[], 0, 200, 5)
    // 断言：len==5，值对应 ts = 111,110,109,108,107（严格降序）
}
#[test]
fn desc_across_chunk_boundary() {
    // 写入 20 点（chunk_points=4 → 5 个 chunk），limit=6
    // 断言：返回最新 6 点、跨 chunk、严格降序
}
#[test]
fn desc_global_order_mixed_sources() {
    // 构造"旧分区 active head + 更新 sealed/persisted chunk"：
    //   with_partition_duration(Duration::from_millis(1000))；
    //   先写 p1(ts=500) 与 p2(ts=2500..) 并把 p2 写满 4 点（rotate_full 封为 sealed），
    //   再向仍活跃的 p1 head 追加 ts=800；
    //   若 p1 head 已被逐出则改用"p2 数据 flush 落盘（persisted）+ p1 活跃"等价构造。
    // select_desc(..., 0, 4000, 100) 断言：所有点严格降序，且同时包含两个来源的点
}
#[test]
fn duplicate_ts_collapsed_to_newest_source() {
    // 同 ts 双写（值不同）：写 ts=100 v=1 → flush/persist → 再写 ts=100 v=2（active）
    // select_desc 返回 1 条且 v=2
}
#[test]
fn desc_respects_end_exclusive_and_empty_window() {
    // end=恰等于某点 ts：该点不返回；空窗口返回空 Vec
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test --test desc_select_test`
Expected: 编译失败（`select_desc` 不存在）

- [ ] **Step 3: 实现**

`src/storage.rs` 加 trait 默认方法（上面的签名）；`reverse_scan.rs` 实现算法（严格按序）：
1. 解析 series（精确 metric+labels）→ 不存在返回空。
2. 收集候选：persisted（`src/engine/index.rs` 索引按 (series_id, min_ts, max_ts) 二分取与 [start,end) 重叠项）/ sealed / active（分区 head `snapshot_in_range`）。min/max_ts 全部来自 header，零解码。
3. **全局排序** `(max_ts desc, 来源新近度 desc)`（`SourceRecency`: Active > Sealed > Persisted）。禁止分层迭代。
4. 逐 chunk：定位窗口内尾部点索引（anchor 二分，`encoder/timestamps.rs:297` `candidate_anchor_block_for_ceiling`；持久化 chunk 用 `core_impl.rs:236` 的懒加载搜索索引）→ 用 `decode_*_in_index_range` 只解尾部 1~2 块 → 块内反转输出；同 ts 已输出则跳过（seen-ts set）。
5. 解码前后应用 retention 与 tombstone 过滤（对齐 `query_read` 现有过滤调用），limit 按可见点计数；凑满即停。
6. 乱序 chunk 回退：整块解码后排序（保持 stats 计数诚实）。
`engine.rs` 覆写 `select_desc` 调用它。

- [ ] **Step 4: 测试通过 + INV-1 门禁**

Run: `cargo test --test desc_select_test && cargo test -p tsink --lib`
Expected: PASS

INV-1 门禁（追加到 `src/engine/query_read/tests.rs`）：

```rust
#[test]
fn inv1_decoded_points_bounded() {
    // 100k 点单 series（chunk_points 默认 2048），limit=100
    // 分别取窗口 [last-1天宽] 与 [全窗口] 两种 end：断言两次
    // stats.chunks_touched 相同、points_decoded <= limit + 64*chunks_touched + 64
    // （stats 经 reverse_scan_collect 的 pub(super) 返回值获取）
}
```
Run: `cargo test -p tsink --lib inv1 -- --release`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/query_read/reverse_scan.rs src/engine/query_read.rs src/engine/query_read/tests.rs src/storage.rs src/engine/engine.rs tests/desc_select_test.rs
git commit -m "feat(engine): add select_desc reverse tail-scan read path (INV-1)"
```

---

### Task 2: 引擎 `select_at_timestamps` 分组点查（INV-2）

**Files:**
- Create: `src/engine/query_read/point_scan.rs`
- Modify: `src/engine/query_read.rs`（`mod point_scan;`）、`src/storage.rs`（trait 默认方法）、`src/engine/engine.rs`（覆写）
- Test: `tests/point_select_test.rs`（新建）

**Interfaces:**
- Consumes: Task 1 的 `collect_candidates` / `SourceRecency`。
- Produces（Task 5 依赖）：

```rust
// src/storage.rs，trait Storage 内：
fn select_at_timestamps(
    &self,
    metric: &str,
    labels: &[Label],
    timestamps: &[i64],
) -> Result<Vec<DataPoint>> {
    Err(TsinkError::UnsupportedOperation {
        operation: "select_at_timestamps",
        reason: "not implemented for this storage backend".to_string(),
    })
}
```

```rust
// point_scan.rs 内：
pub(super) struct PointScanStats { pub chunk_groups_touched: usize }
```

- [ ] **Step 1: 写失败测试** `tests/point_select_test.rs`

```rust
#[test]
fn point_scan_returns_ascending_with_dedup_and_partial_hits() {
    // 写入 ts=100,200,300（值可区分）；查询 &[300,100,100,999]
    // 断言：返回 2 点（100,300，升序）；999 省略；重复 100 不重复返回
}
#[test]
fn point_scan_cross_layer_overlap_takes_newest_source() {
    // 写 ts=100 v=1 → flush 落盘 → 再写 ts=100 v=2（active）
    // select_at_timestamps(&[100]) 断言：1 条、v=2
}
#[test]
fn point_scan_single_decompression_per_chunk_group() {
    // 同一 zstd chunk 内 50 个聚簇 ts（写入 50 点后 flush）：
    // stats.chunk_groups_touched == 1（INV-2：解压次数 = 组数，由构造保证每组一次获取）
}
#[test]
fn point_scan_empty_ts_list_returns_empty() { /* &[] → 空 Vec，不报错 */ }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test --test point_select_test`
Expected: 编译失败

- [ ] **Step 3: 实现**（严格按序）

1. ts 排序去重 → 逐 ts 解析最新来源（active → sealed → persisted，用 chunk min/max_ts 与 anchor 判定归属）。
2. 按 (来源层, chunk 标识) 分组——**先解析再分组**，一 ts 只落一组。
3. 每组恰好一次 payload 获取：未压缩 `Cow::Borrowed` + anchor 块级解码命中索引；zstd 解压一次后按 ts 二分提取。
4. 应用 retention/tombstone 过滤后输出升序点集；缺失 ts 省略；统计 `chunk_groups_touched`。

- [ ] **Step 4: 测试通过**

Run: `cargo test --test point_select_test && cargo test -p tsink --lib`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine/query_read/point_scan.rs src/engine/query_read.rs src/storage.rs src/engine/engine.rs tests/point_select_test.rs
git commit -m "feat(engine): add select_at_timestamps chunk-grouped point query (INV-2)"
```

---

### Task 3: 服务器 — 类型化值 DTO 与常量

**Files:**
- Create: `crates/tsink-server/src/handlers/public_api/typed_value.rs`
- Modify: `crates/tsink-server/src/handlers/public_api/mod.rs`（`pub(crate) mod typed_value;` + re-export）

**Interfaces:**
- Produces（Task 4/5 依赖）：

```rust
pub(crate) const MAX_VALUE_BYTES: usize = 64 * 1024;
pub(crate) const MAX_ROWS_PER_REQUEST: usize = 10_000;
pub(crate) const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const MAX_TIMESTAMPS: usize = 10_000;
pub(crate) const MAX_LIMIT: usize = 100_000;

pub(crate) enum TypedValueDto { Bytes(Vec<u8>), F64(f64), I64(i64), U64(u64), Bool(bool), Str(String) }
// 手写 Serialize/Deserialize：{"type":"bytes","value":"<base64 STANDARD>"} /
// {"type":"f64","value":21.5} / …；Bytes 用 base64::engine::general_purpose::STANDARD
pub(crate) fn from_tsink_value(v: &tsink::Value) -> Result<TypedValueDto, &'static str>; // Histogram → Err
pub(crate) fn into_tsink_value(self) -> tsink::Value;                                    // 恒成功
```

- [ ] **Step 1: 写失败测试**（`typed_value.rs` 底部 `#[cfg(test)] mod tests`，模式参照 `handlers/internal_api/mod.rs:2775`）

```rust
#[test] fn typed_value_roundtrips_all_variants();      // 6 个变体 serde 往返相等
#[test] fn bytes_value_is_base64_not_number_array();   // Bytes([0xCA,0xFE]) 序列化含 "yv4="，不含 "[202"
#[test] fn utf8_string_passes_through_unescaped();     // Str("中文") 序列化含 UTF-8 原文字节
#[test] fn invalid_base64_rejected();                  // "not_base64!!" → Deserialize 错误
```

- [ ] **Step 2: 运行确认失败** — `cargo test -p tsink-server typed_value`，Expected: 编译失败
- [ ] **Step 3: 实现**（手写 serde impl；base64 STANDARD；`from_tsink_value` 对 `Value::Histogram` 返回 Err）
- [ ] **Step 4:** `cargo test -p tsink-server typed_value` — Expected: PASS
- [ ] **Step 5: Commit**

```bash
git add crates/tsink-server/src/handlers/public_api/typed_value.rs crates/tsink-server/src/handlers/public_api/mod.rs
git commit -m "feat(server): typed value DTO with base64 bytes and UTF-8 strings"
```

---

### Task 4: 服务器 — `POST /api/v1/rows` 写入端点

**Files:**
- Create: `crates/tsink-server/src/handlers/public_api/rows.rs`（底部 `#[cfg(test)] mod tests`）
- Modify: `crates/tsink-server/src/handlers/public_api/mod.rs`（re-export）、`crates/tsink-server/src/handlers/router.rs`（`("POST", "/api/v1/rows")` 臂，紧跟 `("/api/v1/write")` 臂之后）、`crates/tsink-server/src/server/auth.rs`（`request_permission_for` 的 Write 臂追加 `("POST", "/api/v1/rows")`）

**Interfaces:**
- Consumes: Task 3 全部；`admission::global_public_write_admission()`（`ingest.rs:12` 模式）；`tsink::Row::with_labels`。
- Produces: `pub(crate) async fn handle_rows_write(storage: &Arc<dyn Storage>, request: &HttpRequest, precision: TimestampPrecision, tenant_registry: Option<&tenant::TenantRegistry>, usage_accounting: Option<&UsageAccounting>) -> HttpResponse`

- [ ] **Step 1: 写失败测试**（`rows.rs` 底部 mod tests；harness = 直接构造 `StorageBuilder` + `HttpRequest{method,path,body,headers}` 调 handler，参照 `internal_api/mod.rs:2775`）

```rust
#[test] fn rows_write_roundtrip_and_mixed_families();  // bytes 行 + f64 行同批成功；
                                                       // storage.select 读回值一致
#[test] fn rows_write_same_series_mixed_family_rejected_400();
#[test] fn rows_write_limit_rejections();              // >10k 行 / 值 >64KiB / body >32MiB / 非法 base64 → 400
#[test] fn rows_write_honors_timestamp_precision();    // precision=Millis 时 ts 按 ms 解释
```

- [ ] **Step 2: 运行确认失败** — `cargo test -p tsink-server rows_write`，Expected: 编译失败
- [ ] **Step 3: 实现**：解析（上限校验先行）→ 组装 `Vec<Row>` → 走 `handle_remote_write` 同款 admission 入口（`ingest.rs:12-24` 模式）→ `insert_rows`；响应 `{"status":"success","data":{"inserted_rows":N}}`；错误按 `classify_storage_write_error` 分类。
- [ ] **Step 4:** `cargo test -p tsink-server rows_write` — Expected: PASS
- [ ] **Step 5: Commit**

```bash
git add crates/tsink-server/src/handlers/public_api/rows.rs crates/tsink-server/src/handlers/public_api/mod.rs crates/tsink-server/src/handlers/router.rs crates/tsink-server/src/server/auth.rs
git commit -m "feat(server): add POST /api/v1/rows typed batch write endpoint"
```

---

### Task 5: 服务器 — `POST /api/v1/select` + `POST /api/v1/select_points`

**Files:**
- Create: `crates/tsink-server/src/handlers/public_api/select.rs`（底部 `#[cfg(test)] mod tests`）
- Modify: `handlers/public_api/mod.rs`（re-export）、`handlers/router.rs`（两个 Read 臂）、`server/auth.rs`（`("POST", "/api/v1/select")`、`("POST", "/api/v1/select_points")` 追加进现有 Read 臂的 match 列表）、`crates/tsink-server/src/tenant.rs`（`impl Storage for TenantScopedStorage`，:1613 起，转发 `select_desc`/`select_at_timestamps`——透传参数与返回值，租户逻辑已由 inner 处理）

**Interfaces:**
- Consumes: `Storage::select_desc` / `select_at_timestamps`（Task 1/2）、`TypedValueDto::from_tsink_value`（Task 3）。
- Produces:
  ```rust
  pub(crate) async fn handle_rows_select(
      storage: &Arc<dyn Storage>, request: &HttpRequest, precision: TimestampPrecision,
      cluster_context: Option<&ClusterRequestContext>,
      public_read_context: &PublicReadContext) -> HttpResponse
  pub(crate) async fn handle_rows_select_points(
      storage: &Arc<dyn Storage>, request: &HttpRequest, precision: TimestampPrecision,
      cluster_context: Option<&ClusterRequestContext>) -> HttpResponse
  ```

- [ ] **Step 1: 写失败测试**（`select.rs` 底部 mod tests）

```rust
#[test] fn asc_parity_with_storage_select_and_offset_paging(); // 与 select() 结果一致；offset/limit 翻页
#[test] fn desc_returns_newest_limit_descending();             // 调 select_desc；严格降序
#[test] fn duplicate_ts_divergence_desc_vs_asc();              // 同 ts 双写：desc=1 条、asc=2 条
#[test] fn limit_is_required_and_bounded();                    // 缺失/0/100_001 → 400
#[test] fn cluster_mode_returns_501();                         // cluster_context=Some → 501（三端点）
#[test] fn select_points_contract();                           // 去重/部分命中/升序输出/>10k ts → 400
#[test] fn tenant_isolation_via_scoped_storage();              // tenant::scoped_storage 包裹后
                                                               // 租户 A 写、B 查为空
```

- [ ] **Step 2: 运行确认失败** — `cargo test -p tsink-server rows_select`，Expected: 编译失败
- [ ] **Step 3: 实现**：
  - 公共前置：`cluster_context.is_some()` → 501（text/plain，三端点一致）。
  - asc：`storage.select_with_options(metric, QueryOptions::new(start,end).with_labels(labels).with_pagination(offset, Some(limit)))`，`Aggregation::None`。
  - desc：`storage.select_desc(metric, labels, start, end, limit)`。
  - 点查：`storage.select_at_timestamps(...)`；响应点集经 `TypedValueDto::from_tsink_value`（histogram → 400）。
  - 响应：`{"status":"success","data":{"points":[{"timestamp":...,"value":{...}}]}}`；错误信封对齐 `public_api` 现有 JSON 错误分类。
- [ ] **Step 4:** `cargo test -p tsink-server rows_select` — Expected: PASS
- [ ] **Step 5: Commit**

```bash
git add crates/tsink-server/src/handlers/public_api/select.rs crates/tsink-server/src/handlers/public_api/mod.rs crates/tsink-server/src/handlers/router.rs crates/tsink-server/src/server/auth.rs crates/tsink-server/src/tenant.rs
git commit -m "feat(server): add /api/v1/select and /api/v1/select_points query endpoints"
```

---

### Task 6: 文档 + 全量验证

**Files:**
- Modify: `docs/http-api.md`（在 "Metadata queries" 前插入三节：`POST /api/v1/rows`、`POST /api/v1/select`、`POST /api/v1/select_points`——契约从 spec §4 摘录，含类型标签表、重复 ts 分歧条款、上限表）、`docs/data-types.md`（"bytes" 小节追加"HTTP 传输约定"：base64 规则、64 KiB 上限与 chunk_points 提示、string=UTF-8 原文）

- [ ] **Step 1: 写文档**（内容以 spec §4 为准，不改契约）
- [ ] **Step 2: 全量验证**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets 2>&1 | tail -5`
Expected: 全部 PASS；clippy 无新增 warning（存量 warning 不处理）

- [ ] **Step 3: INV 门禁复跑**

Run: `cargo test -p tsink --lib inv1 -- --release && cargo test --test point_select_test`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add docs/http-api.md docs/data-types.md
git commit -m "docs: document rows/select/select_points endpoints and bytes transport rules"
```

---

## Self-Review 记录

- **Spec 覆盖**：G1→Task 4；G2→Task 1+5；G3→Task 2+5；G4→Task 1+2；非目标（501/RBAC/tenant）→Task 4/5；§7 全部测试项已落位；§6 表格行与 Task 文件清单一一对应。
- **Step 扫描**：无 TBD 行；算法只给序号大纲（INV-1/INV-2 语义是 spec 固定的），签名/常量/测试名全部钉死。
- **类型一致性**：`select_desc`/`select_at_timestamps`/`TypedValueDto`/`ReverseScanStats`/`PointScanStats` 在 Task 1/2/3 定义、Task 4/5 引用，名称一致。
- **Review Focus**：5 项均有归属测试（Task 1×1、Task 2×1、Task 4×1、Task 5×2）。
- **比例**：计划 ≈ spec 体量，无代码转录。
