# Schema Evolution Registry (本地模式)

一个纯本地、零外部依赖的模式（Schema）注册与演进校验组件。用本地 JSON 快照文件
模拟多版本模式仓库与消费者确认位点，保证：

- 同一主题（topic）下版本号从 1 开始、连续且单调递增；
- 已发布版本的内容不可被静默覆盖，不同内容不能共享同一版本号；
- 并发/重复提交结果确定（恰好一个赢家，其余得到可区分的冲突错误）；
- 向后 / 向前 / 全兼容三种策略对新增、删除、改名、类型变化给出可区分、可解释的结论；
- 兼容审查范围按主题配置（只看最新 / 最近 N 个 / 全部历史版本），范围随快照持久化；
- 注册前预检（dry run）只读、不改状态，逐版本给出判定与理由；
- 版本历史按版本号升序稳定分页返回，落后消费者按落后程度稳定排序；
- 畸形模式一律显式报错，绝不静默放行或降级为兼容；
- 消费者确认位点持久化，进程重启后不丢失、不回退；
- 任何写入失败都不会留下部分更新的状态。

## 包结构

| 包 | 职责 |
| --- | --- |
| `schema` | 扁平 record 模式定义（字段名/类型/可选性）与内容指纹（规范化 JSON 的 SHA-256） |
| `compat` | 结构校验、字段级 diff（新增/删除/改名/类型变化）与三种策略的规则判定 |
| `storage` | `Store` 接口与原子 JSON 文件存储（临时文件 + fsync + rename） |
| `registry` | 本地注册中心：注册、按版本/按策略查询、消费者确认、重启恢复、错误分类 |

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

## 兼容审查范围（Review Scope）

只跟最新版本比对会漏掉一类事故：候选模式与最新版兼容，但某个消费者还停在
更早的版本上，候选模式恰好会让它读挂。为此每个主题可以单独配置审查范围
（`registry.ConfigureScope(topic, scope)`），注册时按范围内**每个**历史版本
逐一判定，只要有一个版本不通过，整体拒绝。

| 范围 | 构造 | 语义 |
| --- | --- | --- |
| `LATEST`（默认） | `storage.LatestScope()` | 只审最新一个版本，即历史行为 |
| `RECENT(n)` | `storage.RecentScope(n)` | 只回溯最近 n 个版本；`n` 必须为正，否则 `INVALID_SCOPE`；`n` 大于版本数时等价于全量 |
| `ALL` | `storage.AllScope()` | 审全部已发布历史版本 |

- 窗口取"范围内版本"，按版本号**升序**逐个判定。拒绝错误 `registry.Error`
  上挂 `Blocking []VersionVerdict`：每个被卡住的版本都列出 `Version`、
  `Changes` 和 `Reasons`，错误消息也标明**第一个**（窗口内最老的）卡住版本号，
  例如 "first blocked at v1"。不会只甩一句"不兼容"。
- 范围配置是主题配置的一部分，和版本表、确认位点写在**同一份原子快照**里，
  重启后恢复的还是原来那个范围，不会悄悄退回默认。早于该字段写入的旧快照
  （没有 `scope`）恢复时补成 `LATEST`，保持历史语义。
- 主题策略为 `NONE` 时不做兼容判定（任何演进都放行），范围不产生影响。
- `GetTopic(topic)` 只读返回 `TopicInfo`（策略、范围、下一个版本号、最新版本号）。

## 注册前预检 / 试算（Preflight）

`Preflight(topic, candidate, opts)` 在**真正提交之前**模拟一次注册，全程只读：

- 不发布任何版本，不修改消费者确认位点，不触发快照写入；
- 返回 `PreflightReport`：`Compatible`、实际生效的 `Strategy`/`Scope`、
  `CheckedVersions`（窗口内版本号升序）以及逐版本的 `Verdicts`
  （每个版本的 `Compatible`/`Changes`/`Reasons`），`BlockingVersion` 指出
  第一个卡住的版本（通过为 0）；
- 候选内容与某个已发布版本指纹相同：`WouldRegister=false`、
  `ReusedVersion=<既有版本>`，表示真实注册会复用而不会新增版本；
- `opts.Strategy` / `opts.Scope` 可对这一次预检做**临时覆盖**，不落盘、不改主题配置；
  留空则使用主题自身配置；
- 候选模式畸形（无字段、空字段名、重复字段名、未知类型）直接返回
  `MALFORMED_SCHEMA`，**不会**被当成兼容放行，也不会返回空报告糊弄；
- 覆盖参数非法分别得到 `INVALID_STRATEGY` / `INVALID_SCOPE`；主题不存在为
  `TOPIC_NOT_FOUND`；主题策略为 `NONE` 或主题还没有版本时，合法候选直接
  得到"可注册"结论（没有基线可比，而不是返回空结果）。

注意：预检中"与某历史版本不兼容"是通过返回报告里的字段表达的（不是 Go error）；
Go error 只用于畸形输入、非法参数、主题不存在这类无法给出判定的情况。

## 版本历史与落后消费者查询

- `ListVersions(topic, cursor, limit)` 按版本号**从小到大**稳定返回
  `VersionPage`：
  - `cursor` 为"本页严格从哪个版本号之后开始"，首页传 `0`；取下一页时把上一页
    返回的 `NextCursor` 原样传回即可断点续看；`NextCursor==0` 表示没有更多；
  - `limit<=0` 使用默认页大小（20），超过上限（200）自动收敛；负数 cursor
    返回 `INVALID_PAGE`；
  - 同一份数据反复查询顺序完全一致。版本顺序在写入时就是不变量，加载快照时
    还会再做一次防御性升序排序，手工改过的快照也不影响顺序。
- `ConsumersBehind(topic)` 只返回落后于最新版的消费者，排序键固定：
  **确认版本越小（落后越多）越靠前；确认版本相同时按消费者名字升序**。
  重复查询返回顺序逐字节一致，方便分页和断点续看。`PendingVersions` 同样升序。


### 错误分类（`registry.ErrorCode`）

| Code | 含义 |
| --- | --- |
| `MALFORMED_SCHEMA` | 模式结构畸形，无法判定（注册与预检都报这个） |
| `INVALID_STRATEGY` | 未知/不可用于查询的兼容策略 |
| `INVALID_SCOPE` | 审查范围非法（如 `RECENT` 的 limit 非正） |
| `VERSION_CONFLICT` | 版本号已被**不同内容**占用 |
| `VERSION_OUT_OF_ORDER` | 显式指定的版本号不是当前期望值（乱序/跳号） |
| `VERSION_NOT_FOUND` | 版本不存在（含查询无兼容版本） |
| `INCOMPATIBLE_SCHEMA` | 兼容策略拒绝，错误中带逐条 `Reasons` 和 `Blocking` 逐版本判定 |
| `TOPIC_NOT_FOUND` / `CONSUMER_NOT_FOUND` | 主题/消费者不存在 |
| `INVALID_CONFIRMATION` | 确认位点回退等非法确认 |
| `INVALID_PAGE` | 分页参数非法（如负数 cursor） |
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
- `ConsumersBehind(topic)` 返回落后于最新版本的消费者及其待追版本号列表，
  顺序为"落后最多优先、同落后按消费者名升序"的稳定顺序；
  `GetConfirmation` 用于查询单个位点。
- `ListVersions(topic, cursor, limit)` 以升序游标分页拉取版本历史，避免版本
  很多时一次全量返回（详见上文"版本历史与落后消费者查询"）。

## 本地验证

```bash
# 全部单元/集成测试（含并发注册、兼容拒绝、范围审查、只读预检、
# 排序/分页稳定性、重启恢复、存储原子性与旧快照迁移）
go test ./... -v

# 竞态检测，重复运行以压测并发确定性
go test -race -count=3 ./...

# 只看新能力（审查范围/预检/分页/排序/重启持久化）
go test -v ./registry -run 'ReviewScope|Preflight|ListVersions|ConsumersBehind|PersistenceAcrossRestart'

# 可运行示例：注册、冲突、拒绝、确认、重启恢复的完整演示
go run ./examples/basic
```

测试日志中会打印：分配/复用的**版本号**、兼容判定的**字段级变化与拒绝依据**
（`Changes`/`Reasons`）、范围审查时**卡住的具体版本号**、预检每个版本的判定、
分页拼接与消费者排序结果，以及每个错误的**错误分类**（`ErrorCode`）。
`registry/scope_test.go` 开头会打印一行场景横幅，标明覆盖了哪些新场景。

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

// 按策略查询：返回与候选模式在指定策略下兼容的最新已发布版本
v, _ := r.QueryByStrategy(ctx, "orders", schema.CompatForward, candidate)

// 把审查范围放宽到全部历史版本，保护还停在老版本上的消费者
_ = r.ConfigureScope(ctx, "orders", storage.AllScope())

// 注册前只读试算：逐版本判定，先看清会不会过、卡在哪个版本
report, err := r.Preflight(ctx, "orders", candidate, registry.PreflightOptions{})
// report.Compatible / report.BlockingVersion / report.Verdicts[i].Reasons

// 稳定升序分页拉取历史版本（cursor 首页传 0，之后用 NextCursor 续看）
page, _ := r.ListVersions(ctx, "orders", 0, 50)
for page.NextCursor != 0 {
	page, _ = r.ListVersions(ctx, "orders", page.NextCursor, 50)
}
```
