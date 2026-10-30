# tsink HTTP bytes API 与倒序/点查查询 — 设计文档（spec）

- 日期：2026-10-30
- 状态：待审批（brainstorming 产出，builder check 已通过）
- 分支：`dev-bytes`
- 范围：**仅 tsink 仓库修改**。应用层建模方案（如 type_seen 标记流）不在本文档范围。

---

## 1. 意图与背景

tsink 引擎原生支持二进制值（`Value::Bytes`），但 HTTP 服务层完全没有暴露：

- 所有公共写入协议（Prometheus Remote Write / OTLP / Influx Line / StatsD / Graphite）
  归一化为 f64 或 histogram，没有 bytes 通道；
- PromQL 查询把 bytes 转成 NaN（`src/promql/types.rs:65`）——series 能匹配到，payload 丢失；
- 内部集群 API（`/internal/v1/*`）受 token/mTLS 保护且只支持精确 label 集，不是面向用户的接口。

业务场景：海量设备间歇性上传 ~200 字节二进制 payload，需要按时间范围、
正序/倒序、限量查询原始字节。为什么是现在：引擎层已就绪（见 §3），
只差 HTTP 门面与两条读路径，属于低风险、边界清晰的增量。

**成功标准**：客户端通过 HTTP 完成 bytes 的写入、asc/desc 范围查询、ts 点查；
desc 尾读成本与窗口宽度无关；既有查询路径零回归。

## 2. 目标 / 非目标 / 反目标

### 目标

- **G1** HTTP bytes 批量写入（base64 编码值）。
- **G2** HTTP 范围查询：metric + labels + start/end + **order (asc/desc)** + limit。
  desc 走新增倒序尾读路径（不变式 INV-1，见 §5.1）。
- **G3** HTTP ts 列表点查：无 limit 参数、**不保证输出顺序**（按实现自然升序返回），
  按 chunk 分组解码（不变式 INV-2，见 §5.2）。
- **G4** 引擎新增 `Storage` trait 方法 `select_desc` 与 `select_at_timestamps`，
  由存储引擎实现；其余引擎（codec/WAL/compaction/PromQL/升序合并管道）零改动。

### 非目标

- PromQL、Remote Read 对 bytes 的支持（值仍为 NaN）——payload 获取只走新端点。
- Python 绑定（tsink-uniffi）同步更新。
- 内部集群 API、集群复制/合并逻辑改动（bytes 已可透传）。
- 应用层数据建模（设备/类型标记方案）——属于使用方项目方案。
- 既有升序查询路径的行为变化。
- **集群模式下的新端点**：v1 范围为单机部署（含多租户，`TenantScopedStorage` 转发新
  方法）；集群部署时三端点显式返回 **501**（响亮失败优于静默本地读写导致错 shard），
  并有测试锁定。

### 反目标（出现即为失败）

- **desc 实现按升序读取全部 chunk 再截尾**——7 天窗口的海量数据下不可承受（INV-1）。
- 为 bytes 值修改存储格式或现有 codec。
- 点查逐 ts 独立发起查询导致同一 chunk 重复解压。

## 3. 引擎现状（已源码核实，builder 可直接引用）

| 事实 | 位置 |
|---|---|
| `Value::Bytes(Vec<u8>)` 一等值类型 | `src/value.rs:105` |
| Blob lane + `BytesDeltaBlock` 编码（tag+uvarint len+raw，不压缩内容） | `src/engine/chunk.rs:13,34`、`src/engine/encoder/values.rs:282` |
| chunk header 含 min_ts/max_ts/point_count（排序定位零解码成本） | `src/engine/chunk.rs:38-47` |
| chunk 索引按 (series_id, min_ts, max_ts) 有序，支持二分 | `src/engine/index.rs:32-64` |
| 64 点 anchor 块（point_idx, timestamp, payload_offset），可流中跳读 | `src/engine/encoder/timestamps.rs:188-214, 297` |
| 持久化 chunk 搜索索引懒加载缓存（OnceCell） | `src/engine/core_impl.rs:236-267` |
| 按索引区间解码 ts/值 | `src/engine/encoder/values.rs:332`、`timestamps.rs` 各 `_range` |
| chunk 落盘 zstd level 1；**解压结果不缓存**，每次访问现场解压；未压缩走 mmap 零拷贝 | `src/engine/segment/format.rs:1515-1568` |
| 查询多路合并恒为升序；分页 offset/limit 已实现 | `src/engine/query_read/merge.rs`、`pagination.rs` |
| 同 (series, ts) 不去重；crash replay 可造成跨层同 ts 重叠 | 调研结论 |
| 每 series 最多 8 个活跃分区 head（BTreeMap 按 partition_id） | `src/engine/state/active.rs` |
| 服务器持有 `Arc<dyn Storage>`（同步 trait） | `crates/tsink-server/src/server.rs:363` |
| base64 crate 已在依赖中 | `crates/tsink-server/src/rbac.rs:1` |

## 4. HTTP API 设计

通用约定：

- 鉴权、多租户（`x-tsink-tenant`）、admission/配额、错误信封完全复用现有公共 API 模式
  （参照 `handlers/public_api/ingest.rs`）；**新路由必须注册进 RBAC path→permission 表**
  （`server/auth.rs`），并有"未授权 403"测试——漏注册意味着静默无鉴权。
- 时间戳为 i64，单位跟随服务器 `--timestamp-precision` 配置。
- 区间语义 [start, end)（与引擎一致，end 排他）。
- 响应信封对齐现有 `{"status":"success","data":...}`。
- **值编码（读写对称，类型标签制）**：

```json
{"type": "bytes",  "value": "yv4="}   // Bytes：value = base64 编码后的字符串本身
{"type": "f64",    "value": 21.5}              // 数值/布尔/字符串为 JSON 原生
{"type": "i64",    "value": 1}
{"type": "u64",    "value": 42}
{"type": "bool",   "value": true}
{"type": "string", "value": "text"}
```

  - Bytes 的 `value` 字段放的是**编码后的字符串本身**（无前缀、无包装）：
    原始字节 `[0xCA, 0xFE]` 编码后为 `"yv4="`。写入时客户端编码、服务器解码存储；
    查询时服务器编码返回、客户端解码还原。JSON 无法承载任意二进制
    （控制字节/非法 UTF-8 会破坏解析），base64 将其映射为纯 ASCII，体积 +33%。
  - **字符串编码**：`string` 类型的值直接用 UTF-8 原文（RFC 8259 强制 UTF-8 传输）；
    `\uXXXX` 转义是合法等价写法，服务器两侧（serde_json）均接受但**响应恒输出
    UTF-8 原文、不转义非 ASCII**。`Value::String` 恒为 UTF-8，存储无损往返。
    有含义的文本用 `string`；不透明二进制（含客户端自行编码的文本字节）用 `bytes`。

  - histogram 家族 v1 不支持：写入与查询均返回 400（Prometheus 协议已覆盖其读写）。
  - series 是单值家族的：查询响应内所有点 `type` 一致。

### 4.1 `POST /api/v1/rows` — 批量写入（bytes 为主，支持全部非 histogram 类型）

```json
{
  "rows": [
    {"metric": "device_data", "labels": [["device_id","A"]],
     "timestamp": 1730000000000,
     "value": {"type": "bytes", "value": "yv4="}},
    {"metric": "temperature", "labels": [["device_id","A"]],
     "timestamp": 1730000000000,
     "value": {"type": "f64", "value": 21.5}}
  ]
}
```

- `value.type` 省缺为 `"bytes"`（本端点的主要用途）。
- 同批次不同 series 可以是不同值家族；同一 series 混家族由引擎按
  `ValueTypeMismatch` 拒绝（HTTP 层 400）。
- 上限（默认值，可后续配置化）：**单值 ≤ 64 KiB**；单请求 ≤ 10 000 行；body ≤ 32 MiB；
  ts 数值范围校验遵循服务器精度配置。
  > 64 KiB 的由来：chunk 按 2048 点密封、无字节维度，1 MiB 值会产生 ~2 GiB chunk
  > （且逼近 `u32::MAX` 编码上限、读取时单次解压不可承受）。接近上限的使用方式
  > 应调小 `with_chunk_points`（如 256 → chunk ~16 MiB）。
- 校验失败 → 400；鉴权失败 → 401/403；admission 背压 → 复用现有写错误分类。
- 响应：`{"status":"success","data":{"inserted_rows":N}}`。

### 4.2 `POST /api/v1/select` — 范围查询（order + limit）

```json
{
  "metric": "device_data",
  "labels": [["device_id","A"]],
  "start": 1730000000000,
  "end": 1730086400000,
  "order": "desc",
  "limit": 100
}
```

- **`limit` 必填**（1..=100 000）——消除无界内存问题；更大的结果用区间调整分页获取。
- `order` 缺省 `asc`。
- **asc**：现有升序查询管道 + offset（可选）/limit 分页（引擎已支持，透传 `QueryOptions`）。
- **desc**：新增倒序尾读路径（§5.1）。无 offset 参数——继续翻页由客户端调整 `end`
  （上一页最老 ts，区间排他，天然无缝），不引入 cursor 参数。
- 精确 label 集匹配（不做匹配器；正则匹配器留作后续增强）。
- **重复 ts 语义（与 asc 的分歧，文档明示）**：desc 与点查把同 (series, ts) 折叠为
  一条（取最新来源版本）；asc 保持既有行为（可能返回多条）。理由：重复 ts 只源于
  重试/异常，重试场景值恒相同，折叠更干净且保住 INV-1 的尾读成本模型
  （无需为对齐 asc 而回溯更高优先级来源）。
- 响应：`{"status":"success","data":{"points":[{"timestamp":...,
  "value":{"type":"bytes","value":"yv4="}}]}}`。

### 4.3 `POST /api/v1/select_points` — ts 列表点查

```json
{
  "metric": "device_data",
  "labels": [["device_id","A"]],
  "timestamps": [1730000001000, 1730000060000, 1730000120000]
}
```

- **无 limit 参数**（硬上限 10 000 个 ts，超出 400）。
- **无输出顺序保证**：实现按升序自然返回，客户端按 ts 对齐（点查通常对应一页数据）。
- 重复 ts 去重；缺失的 ts 省略（不返回 null 占位）。
- 同 ts 多点取最新来源版本（同 §4.2 分歧条款）。
- 响应同 4.2。

## 5. 引擎设计

新增 `src/engine/query_read/reverse_scan.rs`（desc）与 `src/engine/query_read/point_scan.rs`
（点查），共享"候选 chunk 收集"设施。两者通过 `Storage` trait 新方法暴露
（默认实现返回 `UnsupportedOperation`，主引擎覆写；服务器持有同步 trait，无需 Async 包装）。

### 5.1 `select_desc(metric, labels, start, end, limit)` — 不变式 INV-1

> **INV-1（验收级）**：绝不允许"升序读取全窗口再截尾"。必须从最新 chunk 开始定位、
> 逐个向旧读取，凑满 limit 即停。**触达 chunk 数与解码点数必须与窗口宽度无关**
> （7 天与 70 天同价）。性能基准作为合入门禁（§7）。

- **候选收集**：persisted（chunk 索引二分，`index.rs`）/ sealed / active（分区 head），
  收集与 [start, end) 重叠的 chunk。min/max_ts 来自 header，排序零解码成本。
- **全局排序迭代**：排序键 `(max_ts desc, 来源新近度 desc)`，seen-ts 去重
  （同 ts 取最新来源版本）。**禁止分层迭代**（先 active 全部再 sealed…）——
  反例：补传场景旧分区 head 仍活跃、更晚分区 chunk 已 sealed，分层迭代破坏全局降序。
- **chunk 内**：升序解码可接受（chunk 有界 2048 点；**不需要逆向解码器**——codec 恒
  前向解码，尾部切片内存反转）。anchor 块使成本进一步降为"只解尾部 1~2 个块"，
  整 chunk 解码仅作回退。
- **排序假设**：anchor 二分要求 chunk 内时间有序（目标场景逐设备单调递增，天然满足）；
  乱序 chunk 回退为整块解码后排序（罕见，功能正确）。
- limit 为必填参数（§4.2），沿降序迭代凑满即停；无流式响应需求
  （HTTP 响应为整体缓冲，limit 上限即内存上界）。
- 成本模型：触达 chunk 数 ≈ ⌈limit / 每 chunk 命中点数⌉，典型 1-2；
  解码点数 ≤ limit + 64 × 触达 chunk 数。

### 5.2 `select_at_timestamps(metric, labels, &[i64])` — 不变式 INV-2

> **INV-2（验收级）**：一次调用内，**每个被触达的 chunk 至多解码/解压一次**，
> 解压次数 = 触达的 zstd chunk 组数，与 ts 数量无关。

- 流程：ts 排序去重 → **逐 ts 解析最新来源（active → sealed → persisted）** →
  按 (来源层, chunk 标识) 分组 → 每组恰好一次 payload 获取（未压缩：零拷贝 + anchor
  块级解码；zstd：解压一次后按 ts 提取）→ 输出升序点集。
- 先解析再分组：同一 ts 跨层重叠时只落入一组，去重与单次解压同时成立。
- 无顺序契约（§4.3）；缺失 ts 省略。

### 5.3 不做的事

- 不改升序合并管道、codec、WAL、compaction、PromQL。
- 不引入解压结果缓存（跨调用）；mmap 页热 + 点查 ts 聚簇已足够，
  如未来 profile 显示热点，可加内存预算内 LRU（另行提案）。

## 6. HTTP 层改动清单

| # | 内容 | 位置 | 规模 |
|---|---|---|---|
| 1 | 路由注册（3 端点）+ **RBAC path→permission 表注册** | `handlers/router.rs`、`server/auth.rs` | ~25 行 |
| 2 | 写入 handler（类型化值解析、上限校验） | 新 `handlers/public_api/rows.rs` | ~250-350 行 |
| 3 | 范围查询 handler（asc 透传 / desc 调 select_desc；集群模式 501 守卫） | 新 `handlers/public_api/select.rs` | ~180 行 |
| 4 | 点查 handler | 同上或新文件 | ~100 行 |
| 5 | 类型化值 serde helper + DTO + base64 | `handlers/public_api/mod.rs` 或新模块 | ~160 行 |
| 6 | 引擎 `select_desc`（含 tombstone/retention 过滤镜像） | `src/engine/query_read/reverse_scan.rs` + trait | ~250-400 行 |
| 7 | 引擎 `select_at_timestamps` | `src/engine/query_read/point_scan.rs` + trait | ~150-250 行 |
| 8 | `TenantScopedStorage` / 其他 `Storage` 实现转发新方法（集群 `DistributedStorageAdapter` 保持默认 Unsupported → 501） | `src/storage.rs`、`crates/tsink-server/src/tenant.rs` | ~60 行 |
| 9 | 测试（引擎 + 服务器集成，见 §7） | `tests/`、`src/engine/query_read/tests.rs` | ~550-700 行 |
| 10 | 文档：`docs/http-api.md`、`docs/data-types.md` | docs | ~120 行 |

合计 ≈ 1 200-1 500 行，预估 4-5 人日。里程碑：

- M1 引擎 `select_desc`（含基准）→ M2 引擎 `select_at_timestamps` →
  M3 HTTP 写入 → M4 HTTP 查询两端点 → M5 文档收尾。
  每个 M 完成即 commit（不 push）。

## 7. 测试与验收

**正确性**

- 写入 roundtrip：各类型（bytes/f64/i64/u64/bool/string）编码 → 写入 → 查询 → 解码一致
  （含空值、0/1 字节边界）。
- 同 series 混家族 → 400 `ValueTypeMismatch`；不同 series 异家族同批成功（HTTP 级）。
- histogram 家族：写入与查询均 400。
- asc 范围查询与既有 `select` 结果一致；offset/limit 翻页正确。
- desc：跨 chunk 边界、单 chunk 尾部、limit 小于单 chunk 命中数、limit 跨多 chunk、
  空窗口、end 排他语义。
- **重复 ts 分歧条款**：同 (series, ts) 双写后，desc/点查返回 1 条（最新来源版本），
  asc 返回 2 条——断言该分歧为文档化行为。
- **全局降序回归防御**：构造"旧分区 active head + 更晚分区 sealed chunk"并存，
  输出必须严格全局降序。
- 点查：乱序/重复 ts、部分命中省略、空列表、跨层重叠（模拟 crash replay）只取最新
  来源且只解一层。
- **INV-2 解压计数**：1 000 个聚簇 ts 的点查，zstd 解压次数 = 触达 chunk 组数。
- 重启恢复：bytes 与数值数据落盘重启后可查。
- 鉴权：无 token 401 / **RBAC 未授权 403（防路由漏注册静默放行）** / 租户隔离。
- **集群 501 守卫**：cluster_context 存在时三端点返回 501。
- 非法 base64 / 超上限（值/行数/ts 数/limit 范围）→ 400。

**性能门禁（INV-1 / INV-2）**

- 50 万点 series，limit 100：7 天 vs 70 天窗口，触达 chunk 数与解码点数同阶
  且 ≪ 窗口总点数（解码 ≤ limit + 64 × 触达 chunk 数）。
- （INV-2 计数断言见上，归入正确性即可。）

## 8. 遗留给 builder 的决定空间

- `reverse_scan.rs` / `point_scan.rs` 内部的具体数据结构与函数拆分。
- 候选 chunk 收集的共享设施抽取方式（避免两路径重复代码）。
- asc 路径复用现有管道的具体接线（`select_with_options` vs 新入口）。
- 服务器侧 DTO 的 serde 细节、错误码命名（对齐现有错误分类）。
- 基准测试的放置位置（`benches/` 或引擎 tests 内联断言）。

## 9. 明确不由 builder 决定的事项

- INV-1 / INV-2 的语义（本文档定义，测试为门禁）。
- 端点路径与 JSON 契约（§4）。
- desc 无 offset、分页靠区间调整（§4.2）。
- 非目标清单（§2）。
