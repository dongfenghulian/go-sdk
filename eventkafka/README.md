# eventkafka

Go SDK，包名 `eventkafka`，基于 `github.com/segmentio/kafka-go`。

- `AppMessage` → `app.app-event-v1`，Kafka key 为 device_uuid。
- `SysMessage` → `sys.sys-event-v1`，Kafka key 为 event_id。
- 消息字段遵循 [App 契约](docs/contract/app-event-kafka-contract.md) 和 [Sys 契约](docs/contract/sys-event-kafka-contract.md)。

## 使用

导入路径：`github.com/dongfenghulian/go-sdk/eventkafka`。安装：`go get github.com/dongfenghulian/go-sdk@v0.1.5`。

```go
import (
    "context"
    eventkafka "github.com/dongfenghulian/go-sdk/eventkafka"
)

// 服务启动时调用一次，将返回的 Client 注入各业务处理器。
func newClient(brokersJSON string) (*eventkafka.Client, error) {
    brokers, err := eventkafka.ParseBrokers(brokersJSON)
    if err != nil { return nil, err }
    return eventkafka.New(eventkafka.Config{
        Brokers: brokers,
        SourceSystem: "example-service",
        JobName: "example-job",
        Environment: "prod",
        Version: "1.0.0",
    })
}

// 每次业务事件复用传入的 Client；这里不创建或关闭连接。
func send(ctx context.Context, client *eventkafka.Client) error {

    msg := eventkafka.NewAppMessage("approval_completed")
    msg.RequestID = "request-1"
    msg.DeviceUUID = "device-1"
    msg.BID = "example-business"
    msg.AppID = 101
    msg.PayloadJSON = map[string]any{"application_no": "app-1"}
    if err := client.SendApp(ctx, msg); err != nil { return err }

    sys := eventkafka.NewSysMessage(eventkafka.LevelWarn, "PROVIDER_SLOW", "provider response slow")
    sys.EntityRef = "app-1"
    sys.ContextJSON = map[string]any{"elapsed_ms": 2000}
    return client.SendSys(ctx, sys)
}
```

服务启动时调用一次 `newClient`，各处理器共享返回的 Client。
停止接收请求并等待业务任务结束后，在服务退出流程调用 `client.Close()`。

接入注意：

- 如有全局 Client 包装层，只在读取或替换指针时持锁；调用 SDK 发送、更新或关闭时释放状态锁。
- 初始化、配置更新和退出单独协调；退出后禁止重新创建 Client。遇到 `ErrClosed` 不自动重建。
- 数据查询及多条事件发送应共享调用方的耗时预算，避免每条消息重新获得完整超时时间。

## etcd 接入

业务服务负责读取和 watch `eventkafka.BrokersKey`：
`/config/rw/kafka/borker`（保留既有拼写），值为 `["broker.example.invalid:9092"]`。

启动时将 etcd 返回值交给 `ParseBrokers`，再传入 `New`。
watch 收到更新时调用 `ParseBrokers` 和 `client.UpdateBrokers`。
删除、空值或格式错误应在业务配置层记录错误并保留旧配置。
SDK 不依赖 etcd、Gin 或 CAS，配置来源可由其他项目自行选择。

更新先切换 Writer，再等待旧 Writer 的在途发送完成并关闭旧 Writer；配置不合法不切换。
配置更新之间串行执行，旧 Writer 的清理不阻塞新 Writer 的发送。
调用方应串行处理或合并配置通知，避免为每次更新无限创建 goroutine。
`UpdateBrokers` 和 `Close` 同步等待清理，没有硬性关闭时限；跨配置切换不保证消息顺序。
如果旧 Writer 关闭失败，UpdateBrokers 返回错误，但新配置已经生效。
New 不探测 broker 可达性，网络错误在发送时返回。

## 发送语义

- Client 支持并发发送；发送期间不要修改消息及其 map/slice。
- 默认同步发送、RequireAll、最多 3 次尝试，总发送超时 3 秒；
  可通过 SendTimeout、MaxAttempts 调整，调用方更短的 context deadline 优先。
- 发送超时从 SendApp/SendSys 入口开始计算；发送不会等待旧 Writer 清理。
  同步校验、JSON 编码（包括自定义 MarshalJSON）和运行时调度不能被 context 强制中断，
  因此不承诺任意调用方代码下严格的墙钟返回时限。
- 构造函数只生成一次 UUID 与 UTC 毫秒时间；重投应复用消息及 event_id。
- 失败返回 error，不递归发送 SysMessage，不内置落盘队列。
  进程崩溃或重试耗尽后的补发由调用方负责；超时结果可能不确定，允许重复投递。
- SysMessage 默认继承客户端的 source_system、job_name、env、host、app_version；
  单条消息显式值优先，SendSys 不修改原消息。
- WARN 全量发送。fingerprint 默认省略，由 Flink 权威算法生成。
- payload_json/context_json 必须为原生 object/array；可用 map、slice 或 json.RawMessage，
  不接受字符串、数字或显式 JSON null。无需字段时使用 nil。
- 不脱敏、不截断，不生成消费方派生字段；Flink 负责契约规定的体积治理。
  Kafka 自身消息大小限制仍可能导致发送失败。
- 调用方提供有效 bid、身份字段和 is_test；SDK 不查询业务数据库。
- 同设备使用相同 Kafka key 不等于跨并发请求的业务严格顺序。
- SDK 不自动创建 topic，部署前准备两个 topic。

## 验证

```sh
go test -race ./...
go vet ./...
```

单元测试使用模拟 Writer；真实 Kafka 连通性需要在部署环境验证。

## 配置保密

SDK 不内置账号、密码、真实连接地址或本机路径。示例域名使用保留的 .invalid 域，均非实际服务。连接信息由调用方从配置系统读取；不要将运行时配置或凭据提交到仓库。SDK 不主动输出日志；底层发送错误可能包含网络端点，调用方对外输出错误时应按需脱敏。

## 连接池生命周期（v0.1.2）

每个 Writer 使用 SDK 独立持有的 Transport，不使用进程共享的 DefaultTransport。
关闭和配置切换时，先等待 Writer 完成，再调用 CloseIdleConnections 释放旧连接池。
重复设置相同 brokers 列表不重建 Writer；列表顺序变化视为配置变化。

## 发送并发限制（v0.1.4）

Config.MaxConcurrentSends 控制每个 Client 同时执行的发送调用数量，默认 32，
正数可自定义，负数非法。SendApp 与 SendSys 共用额度，校验和序列化也在额度内。
超限立即返回 ErrBusy，不排队、不自动补发；调用方可用 errors.Is 判断。
这是发送调用并发限制，不是连接数、Kafka 分区数或进程全局限制。
发送结束（含错误、超时）释放额度；已经超时但仍由底层 Writer 处理的批次
不计入活跃调用数，因此这不是 Kafka 内部缓冲字节数的硬上限。
同一进程建议复用一个 Client。
