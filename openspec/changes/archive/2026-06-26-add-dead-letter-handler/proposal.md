# 消费端死信处理回调（DeadLetterHandler）

## Why

当前消息累计投递达 `MaxDeliver` 仍失败时，库只把它 `XAdd` 到 `<stream>:dead` 死信流并告警，**只写不读**——接入方无法在死亡时刻对消息做路由/落库/自定义处理，死信流堆积无人消费。

死信是消费端语义（由 reaper 在消费端产生），应在消费端提供一个"消息死亡时"的处理钩子，让接入方决定如何接管，而不必额外起一个消费 `<stream>:dead` 的服务。

## What Changes

- 新增 `DeadLetterInfo` 结构（携带 stream/group/consumer/retryCount/idle/deadID 元信息）。
- 新增 `DeadLetterHandler` 回调类型，并作为可选字段加入 `ConsumerSpec`。
- `moveToDeadLetter` 改为**并存 + best-effort 回调**语义：
  1. 仍先 `XRangeN` 读 payload、`XAdd` 到 `<stream>:dead`（持久兜底、真相之源，MAXLEN 自动裁剪）；
  2. 若设置了 `DeadLetterHandler`，写入死信流之后调用它；
  3. **无论回调成败，库都 `XAck` 原消息并发死信告警**（消息已落死信流不会丢）；回调失败仅额外记错误日志 + 发一条回调失败告警，**不阻塞 XAck、不做库级重试**。
- 回调在 `HandlerTimeout` 子 ctx 内执行并带 panic 兜底，避免挂死/panic 的死信回调冻结 reaper 扫描循环。
- 回调为 nil 时完全保持现状（向后兼容）。
- 补充测试、`doc.go` 与 README。

> 设计修正（review 后）：早期设计是"回调失败 → 不 XAck → 下一轮重试"，但这会让毒消息把原消息永久卡在 PEL，并每轮重写一份死信流、冲刷 `<stream>:dead` 的 MAXLEN 挤掉别的死信。改为"持久写 + 总是 XAck/告警 + best-effort 回调"，由死信流本身作为可靠兜底。

## Impact

- Affected specs: `message-queue`
- Affected code: `consumer.go`（新增类型 + 字段）、`deadletter.go`（调用回调）、`doc.go`、`README.md`，新增 `deadletter_test.go`
- 向后兼容：`DeadLetterHandler` 为可选字段，不设置时行为不变。
- 接入方注意：回调必须幂等（失败重试会重复触发、死信流可能重复写），且应尊重 ctx 以便被 `HandlerTimeout` 打断。
