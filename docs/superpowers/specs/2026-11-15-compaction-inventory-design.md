# 设计：压缩规划库存化（O(1)/轮）+ 运行时 vanished-skip 对齐 + registry catalog 内存化

日期：2026-11-15
分支：`dev-bytes`
状态：待评审（v3，已吸收两轮 builder-check 意见）

## 意图与背景

### 生产环境实测现象（NFSv3，阿里云 NAS，单设备，容器运行 12 天）

- `lane_numeric` L0 积压 **9,310 个 segment 目录（46,530 个文件）**，`lane_blob` L0 9,237 个；L1 各 ~228，L2 各 ~10。总文件数 ~10.8 万。
- 压缩触发阈值为 4（`DEFAULT_L0_TRIGGER`/`DEFAULT_L1_TRIGGER`），L0 却堆积到四位数——排空速率长期低于产生速率。
- 48 小时日志：`Compaction source removal deferred` 1,519 次；`Background worker execution failed worker="compaction" error=... "missing manifest.bin"` **1,062 次**；`Registry catalog source vanished mid-removal` 10,448 次；`Segment directory is mid-removal during compaction planning` 4,161 次。
- 实测压缩轮次间隔 ~7 分钟（默认应为 5 秒），与 L0 规模正相关。

### 根因链条（五个独立缺陷叠加成死亡螺旋）

1. **planning O(N) 全量扫盘且全量解析**：`compact_once_with_changes`（`src/engine/compactor/planning.rs`）每轮对每层调用
   `count_level_segments_for_compaction`（遍历目录树读 manifest）+
   `load_segments_for_level_runtime_strict`（`src/engine/segment/loader.rs:345`）。
   后者经 `load_segment_dir` 对**每个** segment 做全部 5 个文件的 read + hash64 校验 + 完整解析
   （含 chunks 数据），而 planning 实际只需要 manifest 字段（segment_id / level / min_ts / max_ts）。
   N 大时每轮分钟级，且 N 越大轮次越慢 → 排空越慢 → N 更大。
2. **运行时加载把"删除中"判为 DataCorruption 硬失败**：源目录删除是"先删文件、最后 rmdir"，
   期间目录以缺文件状态存在。`handle_segment_dir_load_error`（`src/engine/segment/validation.rs:126`）
   对 NotFound 跳过，但 `DataCorruption("missing ...")` 经 `segment_validation_error` 包装为
   `"persisted segment <p> failed compaction validation: missing manifest.bin"` 后落入
   `FailOnInvalid` 分支 → **整轮压缩失败白跑**。同一事实在 `count_level_segments_for_compaction`
   （planning.rs:99，warn+skip）与 `is_vanished_catalog_source_error`（registry_catalog.rs:145，
   匹配**未包装**错误）中已是 skip 惯例——三处策略不一致，且包装差异使谓词不可直接复用。
3. **registry catalog 每次写前对全量段做 3N 磁盘读**：catalog 写（每次 flush 发布、每次
   diff/inventory 应用、close 时都会发生）路径为 `validate_registry_catalog`（一遍 manifest 读）
   + `build_catalog_entries`（第二遍 manifest 读）+ `build_series_fingerprint_from_sources`
   （逐段 series.bin 读）。N=10.8k 时每次写 = ~3 万次磁盘读，全部在 compaction gate 内，
   且必然撞上删除中的目录（10,448 条 vanished warn 的直接来源）。
4. **排空速率上限低于产生速率上限，且无重入机制**：
   - planning 每轮只合并**一个窗口（≤8 个）**即返回（planning.rs:51-66 首个成功即 return）；
   - **产生侧是写触发而非节拍触发**：每笔提交写入调用 `notify_flush_thread`
     （ingest_pipeline.rs:108），显式唤醒不推进 `next_bounded_flush_at`
     （runtime.rs:127-136），持续写入/replay 时 flush 背靠背运行——
     产生速率可达**每秒数十个 segment/lane**，不存在"4 个/秒"的上界；
   - 压缩线程只在空闲时 park 5 秒，积压时**没有重新唤醒机制**
     （`notify_compaction_thread` 仅删除路径调用），预算轮跑完还要干等一个完整间隔。
   Kafka 积压回灌（升级/故障后重启消费，时间有序、持续数小时）时，排空结构性追不上产生。
5. **catalog 全量重写发生在每次 flush 发布的 gate 内**：`persist_flush_recovery_metadata_stage`
   → `persist_series_registry_index_with_catalog_sources`（flush.rs:341-359）持有 compaction gate
   期间重写整个 catalog 文件（N 条目 JSON + 原子写 + fsync，N=10.8k 时 ~2.7MB）。
   flush 背靠背时该写成为 gate 的主要占用者，直接挤压压缩窗口机会。

### 为什么本地盘也要修

1、2、3、5 是纯代码逻辑问题，本地盘同样存在，只是窗口从分钟级缩到毫秒级、概率降若干数量级：
- O(N) 扫描与 3N 读在 N 大时本地盘同样退化（backfill 场景可达数万目录）；
- retention sweep 与 compaction 并发删除同一目录的竞态真实存在
  （`remove_dir_all_with_retry` 注释自证），扫描器撞毫秒级窗口 → 浪费整轮 + 误导性
  "Data corruption" ERROR 告警；
- catalog 每次 flush 全量重写在本地盘也是纯开销，且 gate 内做 O(N) 工作挤压排空。

4 是排空速率设计上限问题，与文件系统无关。

### 顺带修复的既有 bug（本设计范围内）

**stranded-diff**：`apply_known_dirty_persisted_refresh_if_pending` 中
`set_persisted_index_dirty(has_known_changes())`（catalog_refresh.rs:141）可覆盖并发生产者
刚置位的 dirty；刷新线程提前返回只检查 `persisted_index_dirty()` 不检查 `has_known_changes()`
（catalog_refresh.rs:203-214）→ diff 已合并但永不应用，静默潜伏至下次压缩或重启。
修复：提前返回条件与置位逻辑补齐 has-known 检查。本设计"不做周期对账"的决策
以该修复为前提。

### 明确排除的根因（本设计不处理）

NFSv3 silly-rename（`.nfs*`）语义：引擎 mmap 持有已合并 segment 的文件时 unlink，
NFS 客户端转为 `.nfs*` 占位，重删 EBUSY，3 秒重试（`remove_dir_all_with_retry`，150×20ms）失效后
按瞬态推迟（deferred）。该机制**设计正确**（防误删、崩溃安全），本地盘上不存在此语义。
deferred 带来的空壳目录由 finalize 重试机制最终清理。本设计不改变 mmap 生命周期、
不缩短/加长重试窗口、不引入 janitor 线程。

## 目标

1. **A（vanished-skip 对齐 + 排空收敛契约）**：
   - 提取共享谓词 `is_vanished_source_error`（`Io NotFound` 或 `DataCorruption` 且消息以
     `missing ` 开头），语义与 `is_vanished_catalog_source_error` 一致。
   - **谓词必须在错误包装之前判定**（或在 `segment_validation_error` 包装形态上同时匹配）——
     现有 compaction 加载路径经 `handle_segment_dir_load_error` 包装后消息前缀已改变，
     直接套用谓词会静默失效（E3）。
   - 施加点：**窗口成员加载器**（B 之后 planning 唯一的磁盘加载点）与既有计数路径。
     窗口成员加载命中该谓词 → warn + 跳过该成员。
   - **收敛契约**：被跳过的成员（a）立即从工作库存副本中剔除（工作计数严格递减），
     （b）**计入本轮 diff 的 `removed_roots`**——幽灵必须流出库存与 catalog，
     否则每轮重选、永不收敛，且违反"每次库存变更必经 diff/dirty"；
     （c）窗口从剩余候选重选。
   - 启动加载路径维持现状（`StartupRecoverable` 隔离语义不变，builder-check 确认：
     启动并非严格失败，本设计不改变它）。
2. **B（planning 库存化 + 每轮多窗口排空 + gate 纪律 + 占空比修正）**：
   - planning 的 per-level 计数、窗口候选从内存 `SegmentInventory`（persisted state）读取，
     **每轮磁盘 I/O 与 N 无关**（摊销 O(变更段数)；内存窗口选择 O(N log N) 可接受）；
     只有选中窗口（≤ `DEFAULT_SOURCE_WINDOW_SEGMENTS` 个）才从磁盘加载。
   - 每轮内循环合并多窗口，直到源层低于触发线（L0→L1 与 L1→L2 同轮先后执行）。
     **工作库存副本必须吸收窗口输出**（含 L1 输出），同轮的 L1→L2 才能看到它们。
   - **gate 纪律（死锁前提）**：现有两处调用点（`runtime.rs` `run_background_worker_pass`
     对 compaction 的整轮获取、`shutdown.rs` `compact_until_settled`）**必须去掉外层获取**，
     改由 compactor 内部按窗口获取/释放——parking_lot Mutex 不可重入，
     保留外层获取 = 自死锁（E4）。窗口间 flush/retention/snapshot/Scanned 刷新可插入。
     **死锁规避规则**：任何地方不得在持有 `persisted_index` 读锁期间获取压缩 gate
     （快照必须是短临界区内 clone 出的值）。
   - **占空比修正（E1）**：源层 ≥ 触发线时，pass 结束后**不 park 满间隔**——背靠背连跑
     （gate 在窗口间的交接天然让 flush 插入节流）；低于触发线才 park 满 5 秒。
   - **墙钟预算**：开新窗口前检查每轮墙钟预算（内部常量，秒级）；进行中的窗口必须跑完；
     高于触发线时每轮至少排一个窗口。预算职责收窄为**flush 防饿守卫**（排空速率由
     背靠背连跑保证，不由预算保证）。
   - **每窗口 diff 合并（不是轮末）**：每窗口发布后立即把变更合并进
     `PendingPersistedSegmentDiff` 并更新 `persisted_index`——窗口间插入的 flush/catalog
     写看到的是已扣减源、已加输出的状态。此点与 C 的内存 catalog 组合是正确性关键：
     现状逐段读盘的 catalog 会跳过 vanished 源（磁盘为准），内存 catalog 若看见
     轮末前的旧库存就会**编码幽灵**（列已删源、漏新输出）。
     由此部分失败不再有"diff 丢失"问题（见目标 4）。
   - 库存候选过滤：lane + level + **root 前缀必须位于本 lane 的 data_path 之下**。
     builder 需核实 `mirror_hot_segments` 改写 hot root 后的路径归一化。
   - **库存句柄（E5）**：`Compactor` 是 Clone 且按值克隆进压缩线程——句柄必须 **Arc 共享**
     （跨 clone 同一实例），禁止 per-instance OnceLock。线程在注入前醒来（spawn 后 ~5s）
     → **跳过该轮 pass**（debug 日志），flush/WAL 保证数据安全，压缩晚几秒无害。
   - 库存漂移对账：**不新增周期对账**。自愈 = 启动 discovery 重建 + stranded-flag/stranded-diff
     路径（修复 bug 后可靠）。**接受的残余风险（我的取舍）**：库存缺失的段在重启前对压缩
     不可见；库存多出的幽灵段由 A 的收敛契约逐轮剔除（经 removed_roots 流出）。
     每一次库存变更都必须流经 diff/dirty 机制——无计划漂移即 bug。
   - **独立 Compactor 回退路径**：`Compactor::compact_once()`（8 个现有测试使用）保留——
     由磁盘扫描构建一次性库存，**与对账扫描共用同一实现**。
   - 现有 `finalize_pending_compaction_replacements`（marker 兜底）保持每轮开始执行；
     `load_tombstones` 保持原位（分片存储时最多 256 个 shard 文件，对 N 仍是 O(1)）。
3. **C（registry catalog 内存化 + 写去抖）**：
   - **C-1 读消除**：库存条目扩展携带 per-file `ManifestFileEntry`（flush publish 时 writer
     手里有 manifest_files；**compaction 输出在 diff 应用时手里只有 roots** —— 输出 manifest
     允许对每个新输出 root 重读一次 manifest.bin（≤ 窗口大小，有界），或由 outcome 直传，
     builder 择一）。`build_catalog_entries` 与 `validate_registry_catalog` 改为内存映射。
   - **C-2 写去抖**：flush 发布路径不再内联重写 catalog——标记 registry-catalog dirty，
     由现有 refresh 线程异步重写（合并逻辑纯内存 + 单文件原子写）。崩溃时 catalog 落后 →
     fingerprint 与段不符 → 启动走**既有的**全量重建兜底（bootstrap/recovery.rs 已有路径），
     正确性无损、重启变慢是可接受代价。
   - **series fingerprint**：保持现有顺序 Xxh64 语义与文件 schema（不改算法、不改格式）。
     **per-segment 贡献缓存**（独立 root-keyed 结构，**不放进库存条目**——Scanned 刷新
     构造全新条目会重置内嵌缓存；root 移除时逐出）在每次写 catalog 时内存重归并
     （O(distinct series) 归并；缓存足迹 O(Σ per-segment series)，builder 需用紧凑表示，
     足迹超界时回退逐段磁盘读路径）。
   - **权威性红线**：贡献唯一来源是**段自身写入 series.bin 的同一集合**，绝不由 registry
     反推——保住"段文件与 registry checkpoint 一致"这唯一独立校验
     （`bootstrap/recovery.rs` `should_load_segment_series` 依赖它跳过逐段读）。
   - **E6 修正**：贡献补读**不得**落在 gate 内的首次 flush——**bootstrap 阶段离线预热**：
     discovery 构建库存后、worker 开始服务前，对无缓存的段离线读 series.bin 补齐贡献
     （与现有启动读盘同量级；NFS 上启动已是分钟级，可接受）。归并时的冲突检查
     （series id / key 绑定冲突）与 vanished-source 容忍语义保留，排序按 SeriesId 升序不变。
4. **失败契约（多窗口）**：窗口内原子性不变（全有或全无，经 replacement marker）。
   每窗口 diff 即时合并后，第 k 个窗口命中真实 DataCorruption 时：停止循环 → 上报错误。
   **不再存在"Err 丢弃 diff"的状态分裂**（diff 已逐窗口合并）。
   - close 路径（`compact_until_settled`）：压缩错误**按 deferral 处理**
     （与 `close_should_defer_dirty_refresh_error` 同型）——继续执行 refresh/tombstone 持久化，
     不中止 close 管线；重启由 discovery 兜底。
   - fail-fast（`background_fail_fast`）：按现有 `handle_result` 控制流退出 worker——
     磁盘经 marker 保持一致，未应用的 diff 由重启 discovery 兜底。
   - 可观测性：整轮聚合一次 `record_compaction_result`（窗口求和 + 窗口数字段），
     错误按轮记一次。
5. 全部现有测试通过；新增测试覆盖：planning 不逐段触盘（新增磁盘读计数 hook）、
   排空循环收敛与预算、写触发产生速率下的占空比（drain ≥ produce 仿真）、
   窗口成员消失时的剔除收敛（含 removed_roots 流出）、多窗口部分失败 diff 保留、
   gate 死锁回归（外层获取移除断言）、stranded-diff 回归测试。

## 非目标（本期明确不做）

- NFS silly-rename 语义、mmap 生命周期、删除重试窗口、janitor 线程。
- 崩溃安全机制：replacement marker 写入/应用协议、WAL、fsync/原子重命名模式一律不动。
- fingerprint 哈希算法与 catalog 文件 schema 变更。
- 并发度提升：不做并行压缩、不增工作线程；跨任务竞态面不得扩大
  （gate 按窗口获取是**缩短**持锁区间，不是新增并发）。
- 查询路径、WAL、tiering 策略、保留策略行为不变。
- 对外配置面/API 不变（预算、连跑上限等一律内部常量）。

## 约束

- 排空必须满足：**写触发产生速率下排空速率 ≥ 产生速率**（本地盘；NFS 开发环境允许
  排空受 fsync 延迟限制而变慢，用户接受"整体偏慢"，必要时由消费端限速兜底）。
- flush 的最大 gate 卡顿 = 单窗口时长 + 单次 catalog 去抖写（后者移出 flush 路径后趋近于零）。
- 内存库存是"规划视图"，磁盘始终是加载时刻的唯一真值：窗口加载失败按 A 的策略处理，
  真实损坏（hash 不符、解析失败）仍 FailOnInvalid。
- 不引入新锁顺序；现有 maintenance gate / visibility fence 语义不变；
  压缩 gate 的外层获取点移除属于本设计（见目标 2）。

## 方案要点

### A：共享 vanished 谓词（包装前判定）+ 窗口加载器 skip + 收敛契约

谓词在 `segment_validation_error` 包装**之前**判定（或同时匹配包装形态）；窗口成员加载命中
→ warn + 跳过；被跳过成员从工作库存剔除**并计入本轮 removed_roots**；窗口从剩余候选重选。
计数路径与 registry catalog 的既有 skip 惯例统一引用同一谓词。

### B：Compactor 改为库存驱动 + 占空比修正

- `compact_once_with_changes` 接收 **Arc 共享**的 per-lane 库存句柄（接线自
  `maintenance/catalog_refresh` 的 `persisted_segment_inventory()`），替换两个全量扫盘点；
  `compact_segments` 仅加载窗口成员。
- 两处调用点移除 compaction gate 外层获取（死锁前提）；窗口级获取/释放；
  背靠背连跑（≥ 触发线）/满 park（空闲）的占空比；墙钟预算为 flush 防饿守卫。
- 每窗口 diff 合并 + 工作副本吸收输出与剔除。
- stranded-diff bug 修复（前置条件）。
- 独立 Compactor 回退与对账扫描共用同一实现。

### C：registry catalog 内存化 + 写去抖

- C-1：catalog 生产从内存（库存条目 + files[]；compaction 输出允许有界 manifest 重读）。
- C-2：catalog 重写去抖出 flush 关键路径（dirty 标记 + refresh 线程异步写）。
- fingerprint：顺序 Xxh64 语义不变；独立 root-keyed 贡献缓存（root 移除逐出；
  与 Scanned 刷新解耦）；bootstrap 离线预热；权威性红线见目标 3。

## 成功判据

- 新增磁盘读计数 hook：大 N 库存下 planning 磁盘读次数与 N 无关（只随变更段数增长）；
  catalog 写路径磁盘读次数与 N 无关。
- 排空循环把超阈值 L0 降到触发线以下且受墙钟预算约束；幽灵成员剔除后循环收敛
  （removed_roots 流出断言）。
- **占空比仿真**：写触发连续产生（无 4/秒上界假设）下 drain ≥ produce，L0 有界。
- flush 的 gate 等待上界 = 单窗口时长（时序断言或可观测性指标验证）；
  gate 死锁回归测试（外层获取移除）。
- 多窗口部分失败：diff 不丢失；单窗口损坏仍 Err（现有测试保持）；close 不被压缩错误中止。
- stranded-diff 回归：dirty 覆盖 + 提前返回场景下 diff 最终被应用。
- NFS 开发环境部署后（现积压 9,310/9,237）：`Data corruption detected: ... missing manifest.bin`
  与 `Registry catalog source vanished` 两类日志归零；文件数单调下降直至积压清空。
- 本地盘 backfill 模拟（持续高写入）排空速率 ≥ 产生速率，L0 稳定在触发线附近。

## 留给 builder 的决策点

1. 库存句柄的 Arc 共享结构与注入时序（`Compactor::new_with_inventory_handle` 形态）。
2. 排空循环中"窗口成员消失 → 剔除 → 重选"的具体实现形态（就地缩窗 vs 丢弃整窗重选）。
3. compaction 输出 manifest 进入库存的通道（outcome 直传 vs diff 应用时有界重读）。
4. per-segment 贡献缓存的紧凑表示与足迹上限（超界回退磁盘读）。
5. 磁盘读计数 hook 的落点（loader 层 vs FS 包装层）。
6. `mirror_hot_segments` 场景下 root 归一化核实。
7. 背靠背连跑的让步策略细节（连续 pass 上限 / 预算让步的参数化）。
