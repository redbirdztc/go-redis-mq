# go-redis-mq

基于 **Redis Streams** 的通用消息队列库：生产、消费、死信处理、本地消息表（Transactional Outbox）派发，一套搞定。

核心设计是 **零三方依赖**——库本身不引入任何 Redis 客户端、日志库、告警渠道、ORM，全部通过接口（`Client` / `Logger` / `Alerter` / `OutboxStore`）由调用方注入。于是：

- 不同业务可用不同 Redis 客户端（go-redis v8/v9、redigo、自研封装）
- 不同业务可对接不同告警通道（飞书 / 钉钉 / Slack / 邮件）
- 单测可注入 mock 实现

```
import "github.com/redbirdztc/go-redis-mq"  // package redisstream
```

> 要求 Go 1.18+。

---

## 三种使用形态

| 形态 | 入口 | 适用场景 |
|---|---|---|
| **直接生产** | `Manager.Publish` | 不需要事务一致性的事件：日志、通知、缓存失效 |
| **消费** | `Manager.Register` + `Manager.Run` | 注册消费者，库为每个 stream 启动 worker + reaper |
| **本地消息表** | `Dispatcher` + `OutboxStore` | 需要"业务写库 + 投递消息"原子性：下单发奖、回调外部系统 |

---

## 快速开始

### 1. 消费

```go
m := redisstream.NewManager(myClient, myLogger, myAlerter, "smartCooker:stream:")

_ = m.Register(redisstream.ConsumerSpec{
    Stream:   "order_pay_success",
    Group:    "order-workers",
    Consumer: "worker-0", // 多实例用 hostname / pod name 区分
    Handler: func(ctx context.Context, msg redisstream.Message) error {
        // 返回 nil → 自动 XAck；返回 error → 留 PEL 等 reaper 重投
        return doBusiness(ctx, msg)
    },
    // 其余字段留空走默认值（见下方配置表）
})

// 阻塞直到 ctx 取消；每个 stream 起 worker + reaper 两个 goroutine
go m.Run(ctx)
```

实际 Redis key = `keyPrefix + Stream`，例：`smartCooker:stream:order_pay_success`，死信流为 `...:dead`。

### 2. 直接生产

```go
id, err := m.Publish(ctx, "order_pay_success", map[string]interface{}{
    "order_no": "ORD20260617001",
    "user_id":  12345,
})
```

> ⚠️ `Publish` 是直接投递，**不保证与业务 DB 事务的原子性**。需要事务一致性请用下面的 Outbox。

### 3. 本地消息表（Transactional Outbox）

关键点：**本库不持有数据库连接、不管理事务**。原子性由你在自己的事务里达成——业务行与 outbox 行同一个 `*sql.Tx` commit：

```go
// 业务侧（不在本库职责内）：
tx := db.Begin()
tx.Exec("INSERT INTO `order` (...) VALUES (...)")
payload, _ := json.Marshal(map[string]interface{}{"order_no": "ORD..."})
tx.Exec("INSERT INTO outbox(stream, payload, state) VALUES (?, ?, 0)", "order_pay_success", payload)
tx.Commit() // 两条 INSERT 要么都成、要么都失败
```

commit 之后，`Dispatcher` 周期把 pending 行搬进 Redis Stream（异步补偿，at-least-once）：

```go
d, _ := redisstream.NewDispatcher(redisstream.DispatcherConfig{
    Client:       myClient,
    Store:        myOutboxStore, // 你实现的 OutboxStore
    KeyPrefix:    "smartCooker:stream:",
    MaxLenApprox: redisstream.DefaultMaxLen,
})
go d.Run(ctx) // FetchPending → XAdd → MarkPublished
```

> ⚠️ **启动顺序（outbox + 消费同一批 stream 时必看）**：`Dispatcher.Run` 一启动就立刻发一轮，
> 而消费组按 `"$"` 创建（只收建组后的新消息）。若 dispatcher 在消费组创建【之前】就把积压 XADD 进流，
> 这些消息会被 `"$"` 跳过、永不投递且不重试（静默丢失）。因此**务必先建组再启动 dispatcher**：
> ```go
> if err := m.EnsureGroups(ctx); err != nil { /* fail-closed：放弃启动 dispatcher */ return }
> go d.Run(ctx)   // 此时所有组已存在，积压 XADD 在建组之后 → 不丢
> go m.Run(ctx)   // Run 内部会再 ensure 一次（BUSYGROUP 幂等）
> ```
> `EnsureGroups` 保留 `"$"` 语义：后续真正新加入的消费组不会重放整条流的历史。

---

## 你需要实现的接口

| 接口 | 职责 | 备注 |
|---|---|---|
| `Client` | 封装 Redis Streams 命令 | 见 `client.go`，go-redis 用户注意 `Block=0` 是无限阻塞的坑 |
| `Logger` | 结构化日志 | 可从 ctx 取 trace_id；必须并发安全；不想要可传 `NopLogger{}` |
| `Alerter` | 告警渠道 | 死信 / 启动失败等触发；建议内部做频控；可传 `NopAlerter{}` |
| `OutboxStore` | 本地消息表读写 | 仅 Outbox 形态需要；见下 |

### OutboxStore 实现要点

```go
type OutboxStore interface {
    FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)
    MarkPublished(ctx context.Context, localID int64, streamID string) error
}
```

1. **`FetchPending` 必须多实例并发安全**：
   - MySQL 8.0+ / PostgreSQL：`SELECT ... FOR UPDATE SKIP LOCKED`
   - MySQL 5.7：`locked_by` / `locked_at` 乐观锁
2. **`MarkPublished` 必须幂等**：`XAdd` 成功但 `MarkPublished` 失败时，下一轮会重发 → 消费方需按 `OutboxRecord.LocalID` 去重。
3. **失败 / 重试 / 放弃由你在 `FetchPending` 的 SQL 里管**：dispatcher 不跟踪 attempts、不判 dead。例如 `WHERE attempts < 10` 过滤超限行。

推荐表结构见 `outbox.go` 顶部注释。

---

## 配置项与默认值（`ConsumerSpec`）

| 字段 | 默认值 | 说明 |
|---|---|---|
| `MaxDeliver` | 5 | 累计投递次数达到此值仍失败 → 转死信 |
| `ClaimMinIdle` | 60s | pending 多久无 ack 视为"卡住"，由 reaper 抢回重投 |
| `ReapInterval` | 30s | reaper 巡检周期 |
| `BatchSize` | 16 | XREADGROUP 单次读取条数 |
| `BlockTimeout` | 5s | XREADGROUP 阻塞超时，也是关停时 worker 退出的最大延迟 |
| `HandlerTimeout` | 30s | 单次 handler 执行超时，**强制夹到 < `ClaimMinIdle`**（见下） |

`DispatcherConfig` 默认值：`Interval=2s`、`BatchSize=100`、`MaxLenApprox=DefaultMaxLen(100000)`、`FetchFailAlertThreshold=5`。

---

## 语义保证与必读陷阱

- **at-least-once**：Stream 内消息至少投递一次，**`Handler` 必须幂等**。
- **Handler 必须并发安全**：handler 会被 worker（新消息）与 reaper（重投卡住消息）两个 goroutine 调用。
- **重投只走 reaper 的 `XClaim`**：失败消息留在 PEL，由 reaper 抢回重投（deliver count +1），达到 `MaxDeliver` 转 `<stream>:dead` 死信流并告警。失败消息**首次重试延迟约为 `ClaimMinIdle`**（默认 60s），不是热重试。

### ⚠️ `HandlerTimeout` 陷阱

`HandlerTimeout` 会被强制夹到**严格小于 `ClaimMinIdle`**。它的首要目的是给单次 handler 执行设上界——挂死（但尊重 ctx）的 handler 会被打断，不会永久冻结 reaper 扫描循环、架空死信升级。

- handler **必须尊重 ctx** 才能被超时打断；完全忽略 ctx 的阻塞调用无法被强制中止。
- 需要长耗时 handler（默认下 >54s）时，**必须同时调大 `ClaimMinIdle` 和 `HandlerTimeout`**，否则被默默夹小。
- `BatchSize=1` 时 `HandlerTimeout` 还能避免同一消息被 worker / reaper 并发重投；`BatchSize>1` 不提供该保证（一批消息同时投递、串行处理，靠后的可能在被处理前就 idle 超时被重投）——属 at-least-once 内的重复投递，由幂等兜底。

### 死信

`MaxDeliver` 次仍失败的消息搬到 `<stream>:dead`，附带 `orig_id` / `orig_stream` / `group` / `consumer` / `retry_count` / `dead_at_ms` / `payload` 等元信息，并触发一次告警。

#### 死信处理回调 `DeadLetterHandler`（可选）

如果不想自己再起一个消费 `<stream>:dead` 的服务，可在 `ConsumerSpec` 上挂一个**消费端死信回调**，在消息死亡时就地接管（落库 / 路由 / 自定义告警）：

```go
_ = m.Register(redisstream.ConsumerSpec{
    Stream:   "order_pay_success",
    Group:    "order-workers",
    Consumer: "worker-0",
    Handler:  bizHandler,
    DeadLetterHandler: func(ctx context.Context, msg redisstream.Message, info redisstream.DeadLetterInfo) error {
        // 例如落到 order_dead 表，或转投另一个补偿队列
        return saveDeadLetter(ctx, msg, info)
    },
})
```

语义（**与死信流并存，回调 best-effort**）：

- 消息死亡时，库**先**把它 `XAdd` 到 `<stream>:dead`（持久兜底、死信的真相之源，MAXLEN 自动裁剪），**再**调用回调。
- **无论回调成功还是失败，库都会 `XAck` 原消息并发死信告警**——消息已持久落在死信流里，不会丢。回调返回 `error` / panic 只额外记错误日志 + 发一条"回调失败"告警，**不阻塞 XAck、不触发库级重试**（避免毒消息每轮重写死信流、冲刷 `<stream>:dead` 的 MAXLEN 把别的死信挤掉）。
- 因此回调是**尽力而为的即时反应钩子**；要保证死信被可靠处理，请消费 `<stream>:dead`。
- 回调**必须幂等且并发安全**：XAck 失败会重试、且多实例下多个 reaper 会对同一 `OrigID` 并发调用——按 `info.OrigID` 去重。
- payload 已被 MAXLEN 裁剪掉的死信**不会**触发回调（无消息体可交付），库照常 XAck + 告警。
- 回调在 `HandlerTimeout` 子 ctx 内执行并带 panic 兜底，挂死 / panic 的回调不会冻结 reaper；回调应尊重 ctx。
- `DeadLetterHandler` 为 `nil` 时行为不变（仅写死信流 + 告警）。

`DeadLetterInfo` 携带 `Stream` / `Group` / `Consumer` / `OrigID`（原消息 ID，去重键）/ `RetryCount` / `Idle` / `DeadID`（写入死信流后分配的 ID）。

> 仍未内建的是死信流的**消费 / 重放服务**——`DeadLetterHandler` 是"死亡时就地反应"，若需要可靠/事后批量处理 `<stream>:dead`，仍由接入方消费该死信流。

---

## 多实例部署

- worker 用不同 `Consumer` 名（hostname / pod name）即可水平扩展，Streams 自动在 consumer 间分配新消息。
- 多个 reaper 可同时跑：`XClaim` 传 `minIdle=ClaimMinIdle`，被别的 reaper 抢过、idle 已重置的消息会静默落空，从而天然去重。
- 多实例 Dispatcher 靠 `OutboxStore.FetchPending` 的行锁互斥。

---

## 测试

```bash
go test -race ./...
go vet ./...
```
