# Schema Evolution Registry (本地模式)

一个纯本地、零外部依赖的 **模式（Schema）注册与演进校验** 组件。用单个本地
JSON 文件模拟多版本模式与消费者确认位点（checkpoint），保证：

- 版本号在同一主题（topic）下**单调递增**，一个版本只能被一份模式内容占用；
- 并发注册 / 重复提交的结果**确定**：不会出现两个内容共享版本、已发布版本被
  静默覆盖；
- 按 **向后 / 向前 / 全兼容** 三种策略给出**字段级、可解释**的判定结论；
- 结构畸形或无法判定的模式以**明确错误分类**返回，绝不静默放行；
- 消费者确认位点**持久化**，进程重启后不丢失、不回退；
- 任何写入失败都只通过「临时文件 + fsync + rename」原子落盘，**不留下部分
  更新**。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/schemadef` | 模式文档的严格解析、校验、规范化与内容指纹（SHA-256） |
| `internal/compat` | 字段级 diff（新增/删除/改名/类型变化）与三策略兼容性判定 |
| `internal/store` | 单文件原子持久化、快照隔离、重启加载 |
| `registry` | 对外门面：注册、按版本/按策略查询、消费者确认位点 |
| `cmd/registry-demo` | 可直接运行的本地演示程序 |

## 模式文档格式

模式为 JSON 记录（record）：一个 `name` 加一个 `fields` 数组，每个字段有
`name`、`type`，可选 `aliases` 与 `default`。

```json
{
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "email", "type": "string", "default": null},
    {"name": "full_name", "type": "string", "aliases": ["name"], "default": ""}
  ]
}
```

支持的基础类型：`string`、`int`、`long`、`float`、`double`、`bool`、`bytes`。

解析是**严格**的，以下情况都会返回 `MALFORMED_SCHEMA` 错误，而不会被忽略：

- 文档为空、JSON 非法、文档后有多余 token；
- 出现未知属性（顶层或字段级，严格模式）；
- 缺少 `name` / `fields`，字段名为空，字段名或别名重复/冲突；
- 使用不支持的类型；
- `default` 字面量与字段类型不匹配（如 `int` 给了 `"x"` 或 `1.5`）。

注意：**显式 `"default": null` 与完全没有 default 是两回事**，会产生不同的
内容指纹。

### 内容身份（Schema ID）

解析后字段按名字排序、别名排序，再以稳定 JSON 序列化并取 SHA-256。因此
「字段顺序不同、JSON 排版/键序不同、别名顺序不同」但语义相同的两份文档，其
Schema ID 相同；而类型、默认值不同的文档 ID 一定不同。

## 演进策略与兼容性规则

判定方向（旧版本 = 数据写方当时的模式，新版本 = 注册中的模式）：

- `BACKWARD`（默认）：**新消费者能读旧数据**（new reads old）。
- `FORWARD`：**旧消费者能读新数据**（old reads new）。
- `FULL`：两个方向都必须成立。

字段匹配顺序：先按**同名**匹配，再按 **alias** 确定性匹配改名，其余为新增或
删除。每一种差异都会产生一条可区分的 `Finding`：

| 差异类型 | BACKWARD（新读旧） | FORWARD（旧读新） |
| --- | --- | --- |
| `FIELD_ADDED` 新增字段 | 新字段**必须有 default**，旧数据无法填充 → 拒绝 | 旧读者忽略多余字段 → 通过 |
| `FIELD_REMOVED` 删除字段 | 新读者忽略旧数据中的多余字段 → 通过 | 被删的**旧字段必须有 default**，否则旧读者无法解析 → 拒绝 |
| `FIELD_RENAMED` 改名（alias） | 新读者通过 alias 投影旧名 → 通过 | 旧读者不认识新名；**旧字段需有 default** 才能解析 → 否则拒绝 |
| `TYPE_CHANGED` 类型变化 | 旧类型必须可**提升**为新类型 | 新类型必须可**提升**为旧类型（反向） |

类型提升表（单向，只能从左读到右）：

```
int    -> long, float, double
long   -> float, double
float  -> double
string -> bytes
```

相同类型视为兼容；反向收窄（如 `double -> int`）、`string -> int` 等都不兼容。
不存在「无法判定就当作兼容」的降级路径：未知策略、nil 模式均直接报错。

即使判定为兼容，差异本身仍会出现在 `Report.Findings` 中；真正阻断演进的差异
在 `Report.Violations` 里，并带 `Detail` 判定依据。

## 版本与并发语义

- `Register(topic, raw)`：自动分配下一个单调版本号（`latest+1`，首个为 1）。
  - 注册前按配置策略与「当前最新版本」做兼容性判定，拒绝则不写任何状态；
  - 相同内容重复注册是**幂等**的，返回既有版本号与 `alreadyExisted=true`，
    不产生新版本、不落盘。
- `RegisterVersion(topic, v, raw)`：显式版本号。
  - `v` 必须恰为 `latest+1`（不允许跳号）；
  - `v` 已被占用时：内容相同 → 幂等成功；内容不同 → `VERSION_EXISTS`
    （分类错误），已发布内容绝不被覆盖；
  - 低于最新版本且不存在的号 → `VERSION_OUT_OF_ORDER`。

所有状态转移在存储层的**单把互斥锁**内完成，并在提交前原子落盘，因此：

- 并发自动注册：每个版本恰好一个「写赢家」，其他相同内容提交者全部得到幂等
  结果；不同内容的版本竞争只有一个赢家（见
  `TestConcurrentRegisterDeterministic`、
  `TestConcurrentExplicitVersionConflict`，均在 `-race` 下验证）。
- 磁盘上版本序列始终连续为 `1..N`，每个版本只对应一个 Schema ID。

## 消费者确认位点（Checkpoint）

- `Ack(topic, consumer, version)`：记录消费者已确认到某版本。
  - 目标版本必须已注册（否则 `UNKNOWN_VERSION` / `UNKNOWN_TOPIC`）；
  - 位点**单调不减**：从 2 回退到 1 返回 `CHECKPOINT_INVALID`；
  - 重复确认同一版本幂等。
- `Checkpoint(topic, consumer)`：返回 `(acked, latest)`；从未确认时 `acked=0`。
- `Lagging(topic)`：返回所有 `acked < latest` 的消费者及位点差。落后消费者
  不会阻塞新注册；注册的兼容性只与「最新模式」有关。

## 持久化与恢复语义

整个注册表（各 topic 的版本 + 各 topic/消费者的位点）是一个 JSON 文档。
写入流程：

1. 在锁内对当前状态做**深拷贝**，在拷贝上执行业务变更；
2. 业务失败 → 直接返回，内存与磁盘都不变；
3. 成功 → 同目录写临时文件 → `fsync` 文件 → `rename` 覆盖目标文件 →
   尽力 `fsync` 目录；
4. 写入任何一步失败 → 删除临时文件，候选状态**不安装**，旧状态保持不变。

重启（用同一路径再次 `Open`）后：版本与位点原样恢复，位点不回退，后续注册
从正确的 `latest+1` 继续。状态文件缺失视为空库；文件存在但无法解码是
**明确的损坏错误**，不会静默重置。

## 错误分类

通过 `registry.ClassOf(err)` 获取，调用方与测试可据此区分拒绝原因：

| 分类 | 含义 |
| --- | --- |
| `MALFORMED_SCHEMA` | 模式文档结构畸形或默认值不合法 |
| `VERSION_EXISTS` | 该版本已被**不同内容**占用 |
| `VERSION_OUT_OF_ORDER` | 显式版本号非单调/跳号 |
| `INCOMPATIBLE` | 不满足所选演进策略（`Error.Report` 携带字段级依据） |
| `UNKNOWN_TOPIC` / `UNKNOWN_VERSION` | 查询/确认的主题或版本不存在 |
| `CHECKPOINT_INVALID` | 确认位点非法（如回退、非正数） |
| `STORAGE_FAILURE` | 落盘/读取等底层失败 |

## 本地验证

需要 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 编译 + 静态检查
go build ./...
go vet ./...

# 全部单元测试
go test ./...

# 带竞态检测（并发注册/显式版本竞争用例）
go test -race ./...

# 仅跑关键场景（并附带版本/判定依据/错误分类日志）
go test -race -v ./registry -run 'TestConcurrent|TestRestart|TestIncompatible'

# 可复现的端到端演示：注册 -> 兼容拒绝 -> 幂等 -> 位点 -> 模拟重启
go run ./cmd/registry-demo
```

测试覆盖：

- 严格解析与内容指纹（`internal/schemadef`）；
- 新增/删除/改名/类型变化在三种策略下的可区分结论（`internal/compat`）；
- 原子写、失败不留部分状态、损坏文件显式报错、快照隔离（`internal/store`）；
- 版本单调与幂等、冲突分类、落后消费者、跨重启位点恢复、并发确定性
  （`registry`）。
