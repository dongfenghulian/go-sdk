# go-sdk

共享 Go SDK 仓库，使用一个 go.mod，按功能划分独立包。

模块路径：`github.com/dongfenghulian/go-sdk`

| 包 | 用途 |
| --- | --- |
| [eventkafka](eventkafka/README.md) | 向 Kafka 发送 AppMessage 和 SysMessage，支持契约校验、重试与连接更新 |

## 安装

安装：

```sh
go get github.com/dongfenghulian/go-sdk@v0.1.5
```

业务中按需导入：

```go
import "github.com/dongfenghulian/go-sdk/eventkafka"
```

所有包共用根目录依赖和版本，发布使用根级 vX.Y.Z 标签。
新增 SDK 时创建同级包目录，不新增 go.mod；包之间避免不必要的耦合。

## 开发检查

```sh
go test -race ./...
go vet ./...
```

## 配置保密

仓库不存放真实账号、密码、连接地址或运行环境配置。
运行时配置由调用方提供；示例仅使用占位值和保留域名。
