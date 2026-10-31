# 设计：select_asc（流式正向扫描）+ 双方向 value 谓词过滤

日期：2026-10-31
分支：`dev-bytes`
状态：待审批

## 意图与背景

单 series 查询需要 `WHERE value <op> x ORDER BY ts ASC/DESC LIMIT n` 风格的管线：
按时间序流式提取 chunk、逐点按值过滤、凑足 limit 立即停止（不再解压剩余 chunk）。

本分支已实现 `select_desc`（`src/engine/query_read/reverse_scan.rs`，commit `fcea2e4`）：
降序 cursor + BinaryHeap 归并 + `while out.len() < limit` 提前终止 + 诚实工作量统计。
但它没有 value 谓词；正向（asc）方向仍走旧 `select` 路径——全量物化后截断，无提前终止。

本设计新增 `select_asc`（镜像 `select_desc` 的流式模型），并为两个方向统一加上
单值比较谓词（value predicate）。

## 目标

1. 新增 `Storage::select_asc`：流式正向扫描，时间戳严格升序，凑足 limit 即停，
   成本模型与 `select_desc` 对称。
2. `select_desc` 与 `select_asc` 均支持可选的单值比较谓词 `ValuePredicate`。
3. 服务端 `/api/v1/select`：`order=asc` 分支切换到 `select_asc`；请求体新增
   `value_filter` 字段；`order=desc` 分支同样支持 `value_filter`。
4. 同步更新说明文档（README、docs/ 下 library/protocol 指南、API doc 注释）。
5. 两个方向的引擎级与 HTTP 级测试。

## 非目标（本期明确不做）

- **集群接线**：internal select RPC（`InternalSelectRequest`）不加 order/limit 字段；
  `DistributedStorage` 不覆盖 `select_asc`（`select_desc` 维持 trait 默认的
  `UnsupportedOperation`）；`/api/v1/select` 在集群模式下继续返回 unsupported。
  引擎层与集群分片模型（按 series 分片）天然兼容，后续接线只需传输层改动。
- 组合谓词（AND/OR）、多谓词列表。
- `Bytes` / `String` / `Histogram` / `Bool` 类型的谓词。
- 跨类型隐式数值转换（避免 u64 > 2^53 的 f64 精度坑）。
- uniffi / Python 绑定暴露（后续单独做）。
- 引擎内部读取路径（PromQL selector、rollup 回读等）迁移到新 API；原生
  `select` / `select_with_options` 保留原样、行为不变。
- `offset` 支持（镜像 `select_desc` 现状：desc 不支持 offset，asc 亦然）。
- 谓词查询的每查询解码点数上限 / 配额接入（见"成本模型与配额"）。

## 反目标（出现即为失败）

- 破坏现有 `select_desc` / `select` 的语义或性能特征。
- 为凑足 limit 而在过滤前预解码整个窗口（应保持逐块流式）。
- 谓词求值触发额外的值解码成本（谓词只在已物化值的点上求值）。

## 已定决策（对话中拍板）

### API 形态

```rust
pub enum ValuePredicateOp { Lt, Lte, Gt, Gte, Eq, Ne }

pub enum ValuePredicateTarget {
    I64(i64),
    U64(u64),
    F64(f64),
}

pub struct ValuePredicate {
    pub op: ValuePredicateOp,
    pub target: ValuePredicateTarget,
}

// Storage trait（本分支未发布，select_desc 直接改签名，不留旧五参版本）：
fn select_asc(
    &self,
    metric: &str,
    labels: &[Label],
    start: i64,
    end: i64,
    limit: usize,
    predicate: Option<&ValuePredicate>,
) -> Result<Vec<DataPoint>>;

fn select_desc(/* 同上，加 predicate 参数 */) -> Result<Vec<DataPoint>>;
```

### 求值规则

- 点值类型与谓词 target 类型**严格一致**才比较；不一致（含 `Bool`/`Bytes`/
  `String`/`Histogram`）的点**直接刷掉，不报错**。
- **NaN 语义（builder check 拍板）**：序比较（`Lt`/`Lte`/`Gt`/`Gte`）遇到 NaN
  点一律视为不匹配；`Eq`/`Ne` 遵循库内 `f64_eq` 约定（`NaN == NaN` 为 true，
  `src/value.rs`），与 `Value` 相等语义保持一致。
- 谓词为 `None` 时行为与无谓词完全一致。
- `limit == 0` 返回空；`start >= end` 返回 `InvalidTimeRange`（镜像现有校验）。

### 多版本语义

同 ts 多版本（如内存新点 + 旧 segment 旧值）时，谓词作用在**去重后的最新版本**
上：最新版本不匹配则该时间戳直接丢弃，**不回退旧版本**。理由：TSDB 语义是
"这个时间戳的值就是它"，值不满足即该时间戳无匹配数据。实现上对应过滤链位置：

```
retention → tombstone → 同 ts 去重（保留最新版本）→ value 谓词 → out.push
```

### 去重契约（用户拍板：与 desc 对称）

`select_asc` 与 `select_desc` 一致：**每个时间戳只输出一个点（最新版本）**。
这改变了现有 HTTP `order=asc` 分支（原走 `select_with_options`，故意保留同 ts
重复点，有测试钉死 `[1,2,3,3]`）的可见行为——切换后 HTTP asc 返回去重结果。
依赖"保留重复"语义的调用方继续使用库层原生 `select`（其行为不变）。相关既有
parity 测试按新契约更新，并在文档中明示该行为差异。

### 可观测性与读新鲜度（builder check 拍板）

- `select_asc_api`（镜像 `select_desc_api` 的 API 入口）需调用
  `request_background_persisted_refresh_if_needed()`，避免切换后 asc 查询
  读到陈旧的持久化索引（现有 asc 路径有此自愈行为，不回退）。
- 为 `select_asc` / `select_desc` 各自新增查询计数（calls/errors/duration/
  points_returned），避免 asc 请求从现有计数器中消失；具体计数器命名与是否
  在 `/metrics` 暴露 `points_decoded` 类统计由 builder 决定。

### 成本模型与配额（用户拍板：本期不设解码上限）

- 无谓词时，两方向解码成本与窗口宽度无关（INV-1）；有谓词时，最坏情况
  （选择性差）会解码整个窗口才能凑足 limit——这是预期行为，stats 如实计数，
  文档明示成本模型。
- 本期不为谓词查询增加每查询解码点数上限或配额接入（适合自托管/嵌入的可信
  调用方；多租户部署需在文档中提示该放大面）。`seen_timestamps` 去重集合
  在谓词存在时随扫描到的去重时间戳数增长，属预期内存特征，builder 可在实现
  时评估容量策略。

### 过滤扫描超时护栏（追加设计，2026-10-31 二轮）

谓词选择性差时扫描无上界，前端可能早已放弃而后端仍在解码。以**超时护栏**
兜底（decoded-points budget 记为未来接租户配额时的扩展，与本机制可共存）。

已定决策：

- **生效条件**：仅当 `predicate` 存在时启用。无谓词扫描成本由 limit 驱动
  且每点有产出（凑满即停），不存在白烧，不设超时。
- **配置形态**：`StorageBuilder::with_filtered_scan_timeout(Duration)` 落入
  引擎 options；**`Storage` trait 签名不动**（引擎从自身 options 读取）。
  默认 `None` = 不限制，不改变现有行为。
- **Server CLI**：`--filtered-scan-timeout <humantime>`（如 `30s`），映射到
  builder；默认未设置 = 不限制。
- **检查时机**：扫描主循环每迭代检查一次（`heap.pop` 前）。cursor 的 batch
  解压发生在循环体内，因此超时后最多多解码一个 64 点块即停。起算点为
  candidate 枚举之后、循环开始之前（metadata 枚举不计入）。
- **触发行为（用户拍板：方案 A）**：返回错误
  `TsinkError::ScanTimeout { scanned_points }`，**不带部分结果**——调用方
  不会把半页数据误当完整结果。HTTP 映射 `503`，`errorType: "scan_timeout"`，
  携带已扫描点数，客户端可缩小窗口或放宽谓词重试。
- decoded-points budget（按 `points_decoded` 上限）为未来扩展位，与超时
  检查可在同一循环共存，本期不实现。

### 流式模型（asc 镜像 desc，但 tie-break 不是简单反转）

- candidate 枚举复用 `collect_candidates_with_mode`；asc 需要 head-peek（窗口内
  最早 ts）替代 desc 的 tail-peek。
- **归并顺序（builder check 修正）**：时间戳最小者先出；**同 ts 时 recency /
  sequence 最大者先出**（即"最新版本先到"）。这不是对 desc 的 `HeapEntry::Ord`
  或 candidate 预排序的简单反转——简单反转会先弹出最旧版本，配合"先到先得"的
  `seen_timestamps` 去重会错误保留旧值，与多版本语义矛盾。
- 每个 candidate 一个正向 cursor，按块（≤64 时间戳）解码。
- 主循环 `while out.len() < limit`：凑足即退出，heap 中剩余 cursor 不再推进，
  未触及的 chunk 不解压。
- asc 输出严格升序；desc 维持严格降序。

### 诚实工作量统计

- 谓词引入后，`points_decoded` 的最坏情况变为整个窗口（过滤选择性差时凑不满
  limit），这是预期行为；统计继续如实计数（谓词过滤也计入 points_decoded）。
- INV-1 的表述随本设计更新：无谓词时解码成本与窗口宽度无关；有谓词时与
  选择性相关，由 stats 暴露真实成本。

### 服务端

- `/api/v1/select` 请求体新增（示意）：

  ```json
  { "order": "desc", "limit": 10,
    "value_filter": { "op": "gt", "type": "i64", "value": 42 } }
  ```

  `op ∈ {lt,lte,gt,gte,eq,ne}`；`type ∈ {i64,u64,f64}`；`type` 与引擎侧
  `ValuePredicateTarget` 一一对应。`value_filter` 缺省 = 无谓词。
- `order=asc` 时 limit/offset 语义与 desc 分支对齐（asc 同样不支持 offset，
  保持两分支一致；现有 asc 分支基于 `select_with_options` 的 offset 行为随之
  收紧，文档说明）。
- tenant 包装层（`tenant.rs`）透传谓词，维持租户隔离测试（含 desc 的租户隔离
  回归用例）。

## 约束

- `select_desc` 的扫描语义与输出不变；允许的变化仅限：签名加谓词参数、
  API 入口补 refresh hook、新增查询计数器。
- 复用现有过滤链组件（retention/tombstone/dedupe），不复制逻辑。
- Rust 版本、依赖零新增（谓词为纯标准库类型）。
- 文档同步：README 查询章节、`docs/` 中 library/protocol 指南
  （参照 commit `19b5acf` 覆盖过的文档位置）、`select_desc`/`select_asc` 的
  API doc 注释。

## 工作流约束（用户指定）

- 阶段性成果自动提交（语义化 commit message）。
- 未 push 的细微修复可 `git commit --amend`。
- 不自动 push。

## 留给 builder 的部分

- 模块/文件命名与组织（如 `forward_scan.rs` vs `asc_scan.rs`；stats 类型是否
  与 `ReverseScanStats` 统一改名）。
- 测试用例的具体划分与放置（参照 `reverse_scan` 既有测试风格）。
- server 端 JSON 字段命名细节（如上示意为准，可在实现时微调并同步文档）。
- trait 默认实现的落点（`select_asc` 默认实现应与 `select_desc` 默认一致地
  返回 `UnsupportedOperation`）。
