# Schema Evolution Registry (本地模式)

一个纯本地、零外部依赖的模式（Schema）注册与演进校验组件。用本地 JSON 快照文件
模拟多版本模式仓库与消费者确认位点，保证：

- 同一主题（topic）下版本号从 1 开始、连续且单调递增；
- 已发布版本的内容不可被静默覆盖，不同内容不能共享同一版本号；
- 并发/重复提交结果确定（恰好一个赢家，其余得到可区分的冲突错误）；
- 向后 / 向前 / 全兼容三种策略对新增、删除、改名、类型变化给出可区分、可解释的结论；
- 每个主题可配置兼容性**审查范围**（仅最新版 / 最近 N 版 / 全部历史版本），范围内任一版本
  不通过即整体拒绝，且拒绝原因指明卡在哪个版本、哪种变化；
- 注册前可**预检（试算）**：只读地列出候选模式相对每个相关历史版本的判定与理由，
  不发布版本、不动消费者位点、不写快照；
- 版本历史按版本号升序稳定返回并支持分页，落后消费者按落后程度稳定排序；
- 畸形模式一律显式报错，绝不静默放行或降级为兼容；
- 消费者确认位点持久化，进程重启后不丢失、不回退；
- 任何写入失败都不会留下部分更新的状态。

## 包结构

| 包 | 职责 |
| --- | --- |
| `schema` | 扁平 record 模式定义（字段名/类型/可选性）与内容指纹（规范化 JSON 的 SHA-256） |
| `compat` | 结构校验、字段级 diff（新增/删除/改名/类型变化）与三种策略的规则判定 |
| `storage` | `Store` 接口与原子 JSON 文件存储（临时文件 + fsync + rename） |
| `registry` | 本地注册中心：注册、范围审查、预检、按版本/按策略查询、历史分页、消费者确认、重启恢复、错误分类 |

## 模式模型

模式为扁平 record：字段列表，每个字段包含 `name`、`type`（`string|int|long|float|double|bool|bytes`）
与 `optional`。字段顺序是指纹的一部分。

结构非法（无字段、空字段名、重复字段名、未知类型）时，`Validate` 与 `Check` 返回
`MalformedError`，注册时归类为 `MALFORMED_SCHEMA`，不会被当作兼容或空结果放行。

## 兼容性规则

判定方向（与 Confluent Schema Registry 语义一致）：候选模式（待注册）相对于
已发布的最新模式进行评估。

| 变化 | BACKWARD（新读旧） | FORWARD（旧读新） | FULL（双向） |
| --- | --- | --- | --- |
| 新增字段 | 仅当新字段 **optional**；否则拒绝（旧数据没有该值） | 允许（旧 reader 忽略多余字段） | 仅当新字段 optional |
| 删除字段 | 允许（新 reader 忽略旧数据中多余字段） | 仅当被删字段在旧模式中 **optional**；否则拒绝（旧 reader 需要它） | 仅当被删字段 optional |
| 字段改名 | 拒绝 | 拒绝 | 拒绝 |
| 类型变化 | 仅允许**安全拓宽**（见下） | 仅允许**安全收窄** | 一律拒绝 |

改名的识别是确定性的：恰好 1 个删除 + 1 个新增且类型相同才判为改名；
任何"多删多增"的组合都按字面报告为多个新增/删除，避免猜测导致结论不稳定。

数值类型拓宽链：`int → long → float → double`（沿链正向为拓宽、反向为收窄）；
`string` 与 `bytes` 互相可拓宽（Avro 语义）。`bool` 与数值类型之间、任何不在链上
的组合均为"不可解析"，在 BACKWARD/FORWARD 下拒绝。

判定结论 `compat.Result` 同时给出 `Changes`（字段级变化枚举）与 `Reasons`
（拒绝依据的人类可读说明），注册被拒时这些依据会原样挂在错误上。

## 兼容性审查范围（Audit Scope）

判定方向不变（候选相对已发布版本），但"跟哪些历史版本比"可以按主题配置：

| 范围 | 语义 |
| --- | --- |
| `LATEST`（默认） | 只与最新发布版本比对，与历史行为一致 |
| `RECENT(N)` | 与最近 N 个发布版本逐一比对（N ≥ 1；历史不足 N 个时即全部） |
| `ALL` | 与全部历史版本逐一比对 |

- 用 `SetAuditScope(ctx, topic, scope)` 配置；主题不存在时以默认策略自动创建。
  非法范围（未知种类、`RECENT(0)` 等）返回 `INVALID_SCOPE`。
- 注册时范围内**任何一个**版本不通过即整体拒绝，错误为 `INCOMPATIBLE_SCHEMA`：
  `Message` 列出所有卡住的基线版本号，逐条 `Reasons` 以 `baseline vN: ...` 前缀
  指明是哪个版本、哪种字段级变化触发的拒绝。
- 范围配置存在 `TopicConfig.Scope` 中，与版本表、确认位点写入**同一份原子快照**，
  重启后原样恢复，不会悄悄退回默认。旧快照没有该字段时按零值处理，归一化为
  `LATEST`，行为与升级前完全一致。
- `TopicConfig(ctx, topic)` 可查询主题当前生效的策略与范围。

## 注册前预检（Precheck）

```go
report, err := r.Precheck(ctx, "orders", candidate, registry.PrecheckOptions{
    // 均可选：不传则用主题已配置的策略/范围
    Strategy: schema.CompatBackward,
    Scope:    &schema.AuditScope{Kind: schema.ScopeRecent, N: 3},
})
```

- 返回 `PrecheckReport`：生效的策略与范围、总体是否通过，以及按版本号升序排列的
  逐版本 `VersionVerdict`（各自的变化列表与拒绝理由）——提交前即可看到会卡在哪个版本。
- **严格只读**：不发布版本、不改消费者确认位点、不写快照文件；策略/范围覆盖只作用于
  本次调用，不会写回主题配置。
- 候选模式畸形（无字段、重名、未知类型等）返回 `MALFORMED_SCHEMA` 错误，
  既不会被当作兼容，也不会返回空报告；主题不存在返回 `TOPIC_NOT_FOUND`，
  非法策略/范围覆盖分别返回 `INVALID_STRATEGY` / `INVALID_SCOPE`。
- 策略为 `NONE` 或主题尚无版本时，没有可判定的基线，报告 `Compatible=true` 且无逐版本项。

## 查询顺序与分页约定

- `ListVersions(ctx, topic, offset, limit)`：版本历史按**版本号升序**稳定返回，
  同一数据反复查询顺序完全一致。`offset < 0` 按 0 处理、超出末尾则截断为空页；
  `limit <= 0` 表示从 offset 起取全部。返回的 `Total` 始终是历史全长，
  与分页参数无关，便于分页与断点续看。
- `ConsumersBehind(ctx, topic)`：落后消费者按**落后程度（latest - confirmed）降序**，
  并列时按消费者名字典序，顺序稳定，可安全地分页/续看。

### 错误分类（`registry.ErrorCode`）

| Code | 含义 |
| --- | --- |
| `MALFORMED_SCHEMA` | 模式结构畸形，无法判定 |
| `INVALID_STRATEGY` | 未知/不可用于查询的兼容策略 |
| `VERSION_CONFLICT` | 版本号已被**不同内容**占用 |
| `VERSION_OUT_OF_ORDER` | 显式指定的版本号不是当前期望值（乱序/跳号） |
| `VERSION_NOT_FOUND` | 版本不存在（含查询无兼容版本） |
| `INCOMPATIBLE_SCHEMA` | 兼容策略拒绝，错误中带逐条 `Reasons`（含卡住的基线版本号） |
| `INVALID_SCOPE` | 非法的审查范围（未知种类、`RECENT(0)` 等） |
| `TOPIC_NOT_FOUND` / `CONSUMER_NOT_FOUND` | 主题/消费者不存在 |
| `INVALID_CONFIRMATION` | 确认位点回退等非法确认 |
| `PERSISTENCE_FAILED` | 快照损坏无法恢复或落盘失败 |

## 并发与一致性语义

- 注册中心内部用互斥锁串行化所有**读改写**事务；整个新状态先在内存克隆中构造，
  再一次性原子落盘，落盘成功后才对后续请求可见。
- 自动注册：`Register` 分配 `nextVersion`；显式注册 `RegisterVersion` 必须恰好等于
  期望值，已占用→`VERSION_CONFLICT`，跳号→`VERSION_OUT_OF_ORDER`。
- 幂等：相同指纹（相同内容）的重复提交直接返回既有版本，`Reused=true`，不产生新版本。
- 竞态下多个不同内容抢同一版本：恰好 1 个成功，其余全部得到 `VERSION_CONFLICT`；
  已发布版本永远不会被另一个内容覆盖。
- 落盘：同目录临时文件写入 → `fsync` → 原子 `rename`（并尽力对目录 fsync）。
  写失败时内存状态保持为旧快照，调用方看到 `PERSISTENCE_FAILED`，不存在部分更新；
  快照文件损坏时启动直接报错，拒绝带病运行。
- 未设置 `Path` 时为纯内存模式（仅用于测试）。

## 消费者确认与重启恢复

- `Confirm(topic, consumer, version)` 记录位点；位点单调，重复确认同版本幂等，
  向回确认返回 `INVALID_CONFIRMATION`，确认未发布版本返回 `VERSION_NOT_FOUND`。
- 位点与版本表在同一份快照中原子写入，因此重启后：已确认位点不丢失、不回退，
  注册继续从 `nextVersion` 顺延。
- `ConsumersBehind(topic)` 返回落后于最新版本的消费者及其待追版本号列表；
  `GetConfirmation` 用于查询单个位点。

## 本地验证

```bash
# 全部单元/集成测试（含并发注册、兼容拒绝、范围审查、预检只读、
# 排序稳定、分页边界、重启恢复、存储原子性）
go test ./... -v

# 竞态检测，重复运行以压测并发确定性
go test -race -count=3 ./...

# 可运行示例：注册、冲突、拒绝、确认、重启恢复的完整演示
go run ./examples/basic
```

测试日志中会打印：分配/复用的**版本号**、兼容判定的**字段级变化与拒绝依据**
（`Changes`/`Reasons`，含卡住的基线版本号）、预检的**逐版本判定表**、
分页查询的**每页版本号**、落后消费者的**排序结果**，以及每个错误的**错误分类**
（`ErrorCode`）。

## 最小用法

```go
r, _ := registry.New(ctx, registry.Options{
    Path:            "./data/registry.json",
    DefaultStrategy: schema.CompatBackward,
})

res, err := r.Register(ctx, "orders", schema.Schema{Fields: []schema.Field{
    {Name: "id", Type: schema.KindLong},
}})
// res.Version == 1, res.Fingerprint 为内容指纹

_ = r.Confirm(ctx, "orders", "consumer-a", res.Version)

// 把审查范围扩到全部历史版本（随快照持久化，重启后不变）
_ = r.SetAuditScope(ctx, "orders", schema.AuditScope{Kind: schema.ScopeAll})

// 注册前预检：只读，逐版本列出判定与理由
report, _ := r.Precheck(ctx, "orders", candidate, registry.PrecheckOptions{})
if !report.Compatible {
    // report.Verdicts[i] 标明卡在哪个版本、哪种变化
}

// 按策略查询：返回与候选模式在指定策略下兼容的最新已发布版本
v, _ := r.QueryByStrategy(ctx, "orders", schema.CompatForward, candidate)

// 版本历史分页：升序稳定，Total 为全长
page, _ := r.ListVersions(ctx, "orders", 0, 50)
```
