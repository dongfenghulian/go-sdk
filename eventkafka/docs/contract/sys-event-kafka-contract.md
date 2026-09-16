# 系统日志事件契约（Kafka）

各服务/管道 → Kafka → 自建 Flink → `event_sys_di`（**贴源，无 ODS 中转**）。
本契约是全公司日志生产者与数仓之间的**唯一对接口径**，三端须逐字段一致：

- 生产者（flink/batch/renderer/umc-api/dq/alert/业务服务…）—— 按本 schema 产消息
- Flink 作业 —— 消费、北京时间日切、`fingerprint` 归并校验、体积截断、落库
- `event_sys_di`（消费方结果表）—— 字段口径见 `sql/dw_deployed/event_sys_di.sql` 文件头

**定位**：全公司**通用日志事件汇聚**，收 `ERROR` / `WARN` / `FATAL` 三档（`level` 区分），
非仅错误。靠 `fingerprint` 把「同一类错误的成千上万条」归并成「1 类 × N 次」——定长关键
维度上表头，不定长上下文塞 `context_json`，**切忌为每种错误加专列**。

## Topic

`sys.sys-event-v1`

`sys`=系统域；`-v1`=schema 版本。**不兼容变更**（删字段/改类型/改语义）时切 `-v2`，
老消费者不受影响；**兼容变更**（加可选字段）沿用 v1。分隔符用 `.`+`-`，避开 Kafka
`.`+`_` 命名冲突。

## 消息结构

**一条消息 = 一条日志事件**。`event_id` 全局唯一，作去重键（结果表 `UNIQUE KEY event_id`），
Kafka 至少一次语义下重投同 id 按版本列 `event_time` 后到覆盖，不翻倍。

```json
{
  "event_id":      "01HXAB...ULID",
  "event_time":    1725580800123,
  "source_system": "flink",
  "job_name":      "dw_ingest_all",
  "component":     "KafkaSourceReader",
  "env":           "prod",

  "level":         "ERROR",
  "event_code":    "SINK_TIMEOUT",
  "event_type":    "java.net.SocketTimeoutException",
  "message":       "write to bytehouse timeout after 30s",
  "stack_trace":   "java.net.SocketTimeoutException: ...\n\tat ...",
  "fingerprint":   "a1b2c3d4e5f6a7b8",

  "bid":           "example-business",
  "app_id":        12,
  "request_id":    "req-8f3a...",
  "entity_ref":    "AP20250825001",
  "context_json":  { "topic": "app.app_event", "partition": 3, "offset": 998877 },

  "host":          "flink-tm-7",
  "app_version":   "1.4.2"
}
```

## 字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `event_id` | string | ✓ | 单条唯一 ID（ULID/UUID），全局唯一去重键；重投同 id 后到覆盖 |
| `event_time` | int64 | ✓ | 事件发生毫秒时间戳（UTC）；**版本列** |
| `source_system` | string | ✓ | 来源系统：flink/batch/renderer/umc-api/dq/alert/业务服务… |
| `level` | string | ✓ | 级别：`ERROR` / `WARN` / `FATAL` |
| `job_name` | string | | 作业/管道/服务名，如 `dw_ingest_all` |
| `component` | string | | 更细模块/类名 |
| `env` | string | | 环境：`prod` / `staging` |
| `message` | string | ✓ | 摘要，日志可读主体；**硬上限 8KB**，超限 Flink 截断 |
| `event_code` | string | | 业务/系统码，可枚举聚合；框架级异常常无，与 `event_type` 至少有其一 |
| `event_type` | string | | 类型，如异常类名 `TimeoutException`；与 `event_code` 至少有其一 |
| `stack_trace` | string | | 堆栈；**软 32KB / 硬 128KB**，超限 Flink 截断 |
| `fingerprint` | string | | 归并键；缺省由 Flink 按统一算法兜底重算（见「fingerprint 归并算法」节） |
| `bid` | string | | 业务线；框架级错误可空 |
| `app_id` | int | | App ID；缺省 `0` |
| `request_id` | string | | 请求 ID |
| `entity_ref` | string | | 关联实体（application_no/user_id…），自由文本 |
| `context_json` | object / array | | 其余不定长上下文；**软 16KB / 硬 64KB**，Flink 序列化为 String 落库 |
| `host` | string | | 机器/容器主机 |
| `app_version` | string | | 版本，定位是否某次发布引入 |

## fingerprint 归并算法（全公司统一）

同一类错误无论出在哪个服务、哪台机、带什么动态参数，都须算出**同一个 16 位十六进制指纹**；
不同类不碰撞。核心是「**归一化后再哈希**」——抹掉易变部分，只留结构骨架。

**权威实现放 Flink**：生产者可自算 `fingerprint` 上报，但 Flink 用同一份归一化库**兜底重算**
（未上报时必算；上报了也可校验）。全公司一致不靠各服务自觉，由 Flink 单点保证。

**实现形态：Flink SQL 注册的标量 UDF**，而非 SQL 内置函数——因为下述归一化纯 SQL 表达不了：
`normalize(message)` 需按序串 7 条正则替换，`top_frame(stack_trace)` 需分行→取栈顶首帧→
剥行号，均是过程逻辑，`REGEXP_REPLACE` 等内置函数拼不出且跨语言一致性无保证。故：

```sql
-- Flink SQL 作业结构不变，只多一个 UDF 调用
SELECT ...,
       fingerprint(source_system, event_code, event_type, message, stack_trace) AS fingerprint
FROM source_kafka
```

`fingerprint()` 是注册的 Java UDF，归一化+哈希全套逻辑封装在内，是全公司唯一权威实现；
生产者若自算须与该 UDF 逐字节一致，否则以 UDF 重算为准。**不得用 SQL 内置函数拼凑替代。**

**输入五要素**（固定顺序拼接，`∥` 为分隔符 `\x1f`）：

```
source_system ∥ event_code ∥ event_type ∥ normalize(message) ∥ top_frame(stack_trace)
```

**`normalize(message)`**——抹除动态量，保留骨架：

| 规则 | 原文 | 归一后 |
|---|---|---|
| 数字（含小数/时长/字节） | `timeout after 30s` | `timeout after <NUM>s` |
| 十六进制/UUID/ULID | `id=01HXAB...` | `id=<ID>` |
| IP[:端口] | `192.0.2.1:19000` | `<IP>` |
| 引号内字面量 | `user 'ray'` | `user '<STR>'` |
| 路径/URL 尾段 | `/data/part_998877` | `/data/<PATH>` |
| 时间戳串 | `2026-09-06 12:00:00` | `<TS>` |
| 连续空白 | `a   b` | `a b` |

**`top_frame(stack_trace)`**：取栈顶首帧的 `类名.方法名`（**去行号**，行号随版本漂），
如 `KafkaSourceReader.pollNext`；无堆栈则空串。

**哈希**：`lower(hex(sipHash64(拼接串)))` → 16 位十六进制。逐字节敏感，拼接前统一 trim、
**不改大小写**（异常类名大小写有语义）。

**版本化**：指纹前置算法版本号 `fp_v1:`（如 `fp_v1:a1b2c3d4e5f6a7b8`）。归一化规则演进时
切 `fp_v2:`，新旧指纹不在同一告警组里割裂。

## 消费方派生（Flink 落，不上报）

| 结果表列 | 由谁算 |
|---|---|
| `beijing_date` | 按**北京时间**对 `event_time` 日切（分区键） |
| `beijing_slot_5m` | 按北京时间算 5 分钟槽 `[0,287]` |
| `beijing_time_str` | 格式化北京时间串 `YYYY-MM-DD HH:MM:SS.mmm`，展示用 |
| `fingerprint` | 生产者未给时由 Flink 按统一算法兜底重算（见「fingerprint 归并算法」节） |
| `_etl_time` | `now()` |

## 不变式

- **时间戳恒为 UTC 毫秒 int64**，不传本地时间、不传字符串。日切统一按**北京时间**
  （日志源多无 bid，不按 bid 时区），口径唯一，告警窗口不混时区。
- **`event_id` 全局唯一且稳定**——跨系统、跨 level 全局唯一。重投同 id 由结果表按
  `event_time` 后到覆盖去重。
- **`fingerprint` 是归并主轴**——同类错误须算出同一 `fingerprint`，否则告警按类计数会散。
  **全公司统一算法，权威实现在 Flink**（生产者可自算上报，Flink 兜底重算/校验），见
  「fingerprint 归并算法」节。
- **`context_json` 传原生 JSON object/array，不传字符串**——Flink `toJSONString` 转
  ClickHouse `String` 落库（列为 `String DEFAULT ''`，缺省落空串）。
- **体积上限（Flink 落库前治理）**：`message` 硬 8KB、`stack_trace` 软 32KB/硬 128KB、
  `context_json` 软 16KB/硬 64KB。超软上限告警治理；超硬上限 Flink 截断——
  `message`/`stack_trace` 尾部截并接 `'...[truncated]'`，`context_json` 替换为
  `{"_truncated":1,"orig_bytes":N}`，避免超大字符串拖累分区。
- **只读日志**：本表纯 append 事件流，不含「已认领/已消音/已解决」等处置状态；
  告警处置闭环由告警服务按 `fingerprint` 另表维护，数仓侧不回写。
- **不脱敏**（§8.3）：`entity_ref` / `context_json` 等原样透传，访问控制交由库表/列级权限。

## 已确认约定

1. **`fingerprint` 归并算法全公司统一**，权威实现在 Flink（见「fingerprint 归并算法」节）。
   跨 `source_system` 的同类错误按同一指纹合并计数；生产者自算须与该算法一致，否则以
   Flink 重算为准。
2. **`WARN` 全量上报**，不采样、不限流。体积由 `message`/`stack_trace`/`context_json` 的
   软/硬上限（Flink 落库前截断）与 30 天 TTL 控制。

