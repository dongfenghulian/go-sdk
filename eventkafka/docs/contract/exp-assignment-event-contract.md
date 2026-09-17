# 实验分流事件契约（Kafka）

实验平台 → Kafka → 自建 Flink → `exp_assignment_di`（**桥接表，无 ODS 中转**）。
本契约是上游实验平台与数仓之间的**唯一对接口径**，三端须逐字段一致：

- 实验平台（生产者）—— 按本 schema 产消息
- Flink 作业 `sql/flink/job_exp_assignment.selfhosted.sql` —— 消费、落库
- `exp_assignment_di`（消费方结果表）—— 字段口径见 `sql/dw_deployed/exp_assignment_di.sql` 文件头

**定位**：本表是**跨表维度桥接表**——`experiment_id` / `variant` 不进任何业务 DWD 明细，
只在本表；指标计算时按 `subject_id` JOIN 挂上实验维度。故消息只承载「某主体在某实验里的
分组归属」，其余（`subject_grain` / `scene` 反查 `meta.experiment`，`bid` / `app_id` 由业务
DWD 主表自带）一律不上报。

## Topic

`dw.exp-assignment-v1`

`dw`=数仓域；`-v1`=schema 版本。**不兼容变更**（删字段/改类型/改语义）时切 `-v2`，
老消费者不受影响；**兼容变更**（加可选字段）沿用 v1。

## 消息结构

**一条消息 = 一个分流**，即「一个主体在一个实验里的分组」。一个主体同时命中多个正交实验时，
拆成多条消息（每实验一条）。去重键 `(experiment_id, subject_id)`，重投同键后到覆盖。

```json
{
  "experiment_id":    "risk_score_cutoff_0825",
  "subject_id":       "AP20250825001",
  "variant":          "treatment",
  "is_active":        1,
  "assigned_time_ms": 1690000000000,
  "event_id":         "evt_0825_abc123"
}
```

## 字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `experiment_id` | string | ✓ | 实验编码 → `meta.experiment.experiment_id` |
| `subject_id` | string | ✓ | 分流主体标识；**含义随实验 `subject_grain` 变**（见下表），不上报 grain |
| `variant` | string | ✓ | 分组：`control` / `treatment` / `v1`…；对照组名以 `meta.experiment.control_variant` 为准 |
| `is_active` | int | | `1`=有效（缺省）；`0`=平台显式移出整条。预留软失效，当前下游暂不据此过滤 |
| `assigned_time_ms` | int64 | ✓ | 分流命中毫秒时间戳（UTC）；**版本列**，重分流后到覆盖 |
| `event_id` | string | | 分流事件 id；仅供审计/幂等排查，缺省落空串。**去重不依赖它** |

### `subject_grain` ↔ `subject_id`

主体粒度由 `meta.experiment.subject_grain` 声明，本表不落 grain，按 `experiment_id` 反查即得：

| subject_grain | subject_id 装什么 | 典型实验 |
|---|---|---|
| `user` | user_id | 用户级（App 页面 A/B 多为此） |
| `group_user` | group_user_id | 自然人/群组级 |
| `application` | application_no | 进件/支用级（循环授信每次支用一行） |
| `loan` | loan_no | 贷后/借据级 |

## 消费方派生（Flink 落，不上报）

| 结果表列 | 由谁算 |
|---|---|
| `_etl_time` | `now()` |

其余不落表、按需反查/JOIN：`subject_grain` / `scene` 反查 `meta.experiment`；
`bid` / `app_id` 由业务 DWD 主表 JOIN 后自带；本表**不做时区日切**（无 `local_date` 列）。

## 不变式

- **时间戳恒为 UTC 毫秒 int64**，不传本地时间、不传字符串。
- **去重键 = `(experiment_id, subject_id)`**——重分流 = 同键再发一条，用更大的
  `assigned_time_ms`，结果表版本列后到覆盖，保留最新 variant。历史 variant 不留痕。
- **`experiment_id` 须存在于 `meta.experiment`**——否则下游反查 `subject_grain` / `scene` 落空。
- **JOIN 必 single_select 锁单实验**——业务表 JOIN 本表时必须带 `experiment_id = :exp`
  等值过滤，否则一主体命中多实验会被放大成多行→重复计数（见 DWD 文件头 §19.6）。
- **不脱敏**（§8.3）：`subject_id` 等标识原样透传，访问控制交由库表/列级权限。
- 值中不使用需转义的控制字符；`variant` / `experiment_id` 不 trim、不改大小写
  （下游进 dim_hash 逐字节敏感，见 dim-hash-contract.md）。

## 已确认约定

1. **分流失效机制**：已上报 `is_active`（默认 `1`），预留软失效。两类「剔除」分治：
   - **业务规则剔除**（测试账号、未走到实验入口、异常值）——业务 DWD 本身有数据，
     在 **JOIN / 分析期用业务条件过滤**，不写 `is_active`。
   - **实验平台显式移出整条**（非改 variant）——上游发 `is_active=0`。该场景尚未确认真实
     存在，当前下游暂不据此过滤；将来启用时约定下游 JOIN 补 `is_active=1` 过滤即可，无需改表。

   重分流（改分组）仍走版本列 `assigned_time_ms` 后到覆盖，保留最新 variant。
2. **主体粒度**：上游可稳定给到 `application` / `loan` 级，`subject_id` 按实验
   `subject_grain` 直接装对应业务键，业务 DWD 可直接 JOIN，无需 user→application→loan 映射。
3. **`event_id` 全局唯一**：跨实验、跨批全局唯一，审计可精确定位单条；去重仍靠业务键
   `(experiment_id, subject_id)`，不依赖 event_id。
