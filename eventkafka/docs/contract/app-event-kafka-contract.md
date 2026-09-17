# App 事件契约（Kafka）

应用事件 → Kafka → 自建 Flink → `event_app_di`（**贴源，无 ODS 中转**）。
本契约是上游生产者与数仓之间的**唯一对接口径**，三端须逐字段一致：

- 生产者 —— 按本 schema 产消息
- Flink 作业 —— 消费、时区日切、`payload_json` 序列化、落库
- `event_app_di`（消费方结果表）—— 字段口径见 `dw.event_app_di`

**定位**：`app.app-event-v1` 承接**应用事件**，不限于客户端埋点——服务端产生的业务事件
（如放款成功、还款到账、审批完成等）同样以此 schema 投递。

## Topic

`app.app-event-v1`

**不兼容变更**（删字段/改类型/改语义）时切新版本 topic，老消费者不受影响；
**兼容变更**（加可选字段）沿用。

## 消息结构

**一条消息 = 一个事件**。`event_id` 全局唯一，作去重键（结果表 `UNIQUE KEY event_id`）。

```json
{
  "event_id":       "01HXAB...ULID",
  "event_name":     "loan_apply_click",
  "event_time":     1725580800123,
  "event_time_app": 1725580799880,
  "sequence":       42,
  "request_id":     "req-8f3a...",
  "session_id":     "sess-2c9d...",
  "is_test":        0,

  "device_uuid":    "d4e5f6-uuid",
  "gaid_idfa":      "GAID-xxxx",
  "user_id":        10086,
  "group_user_id":  20086,
  "mobile":         "token_char28...",
  "id_number":      "3204...",

  "bid":            "id01",
  "app_id":         101,
  "app_version":    "3.12.0",
  "ip":             "10.1.2.3",

  "fi": { "amount": 500000, "term": 30 },
  "ff": { "apr": 0.36 },
  "fs": { "from_page": "home", "button": "apply" },

  "payload_json":   { "any": "object", "or": ["array", 1, 2] }
}
```

## 字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `event_id` | string | ✓ | 事件唯一 ID（ULID/UUID），全局唯一去重键；重投同 id 才能去重 |
| `event_name` | string | ✓ | 事件名 |
| `event_time` | int64 | ✓ | 事件服务端毫秒时间戳（UTC）；**版本列锚点** |
| `request_id` | string | ✓ | 上报请求 ID |
| `device_uuid` | string | ✓ | 安装实例 ID，主序键；重装/换 App 变 |
| `bid` | string | ✓ | 业务线；须存在于 `conf.business_line`，否则时区日切查不到（见不变式） |
| `app_id` | int | ✓ | 数字 app_id |
| `event_time_app` | int64 | | 事件客户端毫秒时间戳；仅供分析，非版本列 |
| `sequence` | int64 | | 设备内事件序号；配合去重 |
| `session_id` | string | | 会话 ID |
| `is_test` | int | | `1`=测试事件；缺省 `0` |
| `gaid_idfa` | string | | GAID/IDFA；iOS 未授权可空 |
| `user_id` | int64 | | 用户 ID；未登录/缺省 `0` |
| `group_user_id` | int64 | | 集团用户 ID；未映射/缺省 `0` |
| `mobile` | string | | 手机号，原样透传不脱敏 |
| `id_number` | string | | 证件号，原样透传不脱敏 |
| `app_version` | string | | App 版本名 |
| `ip` | string | | 上报 IP，原样透传不脱敏 |
| `fi` | object | | 整型自定义字段 `{key: int}` |
| `ff` | object | | 浮点自定义字段 `{key: float}` |
| `fs` | object | | 字符串自定义字段 `{key: string}` |
| `payload_json` | object | | 原始事件负载；**Flink 序列化为 String 落库**，原样透传 |
| `_src_ts` | int64 | | Kafka 消息毫秒时间；缺省由 Flink 取 `event_time` |

## 消费方派生（Flink 落，不上报）

| 结果表列 | 由谁算 |
|---|---|
| `event_local_date` | 按 `bid` 时区对 `event_time` 日切（分区键） |
| `event_slot_5m` | 按本地时区算 5 分钟槽 `[0,287]` |
| `beijing_time_str` | 格式化北京时间串，展示用 |
| `_source` | 固定 `'app_event'` |
| `_etl_time` | `now()` |

## 不变式

- **时间戳恒为 UTC 毫秒 int64**，不传本地时间、不传字符串。本地日切由数仓按
  `conf.business_line.timezone` 计算（§5.1），上游不负责。
- **`bid` 必须能在 `conf.business_line` 命中**——它是时区日切 lookup 的键。未登记的 bid
  会导致 `event_local_date` 算不出、该行落库失败或错分区。新业务线须先进 `conf`。
- **`event_id` 全局唯一且稳定**——跨设备、跨事件类型全局唯一。重投同 id 由结果表按
  `_src_ts` 后到覆盖去重。
- **`_src_ts` 由 Flink 注入**——生产者不必上报；缺省时 Flink 回退取 `event_time`。
- **`payload_json` 传原生 JSON object/array，不传字符串**——Flink 侧 `toJSONString` 转
  ClickHouse `String` 落库（列为 `String DEFAULT ''`，缺省落空串）。
- **`payload_json` 体积**：软上限 **64 KB**、硬上限 **256 KB**，由 **Flink 落库前治理**。超过软上限 Flink 产生一条错误事件(实现告警)。
  超软上限告警；超硬上限 Flink 截断并置 `fs['_payload_truncated']='1'`，避免超大字符串拖累主表。
- **不脱敏**（§8.3）：`mobile` / `gaid_idfa` / `ip` 等标识原样透传，访问控制交由库表/列级权限。
- **`bid` / `app_id` 上游直传**，数仓不再由 package 反查映射；消息不含 `package`。
- 自定义字段按值类型分投 `fi` / `ff` / `fs` 三桶，不为单 key 开列。


