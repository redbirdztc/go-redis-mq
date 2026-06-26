# Tasks

## 1. worker 收敛为只读新消息
- [x] 1.1 `consumer.go`：`runConsumer` 移除 `readPEL` 调用，仅保留 `readNew`
- [x] 1.2 删除不再使用的 `readPEL` 方法，更新循环注释说明重投改由 reaper 驱动

## 2. reaper 驱动重投
- [x] 2.1 `reaper.go`：`XClaim` 接收返回的 `[]Message`，对每条调用 `handleOne` 重投
- [x] 2.2 `XClaim` 的 minIdle 由 `0` 改回 `spec.ClaimMinIdle`，更新相关注释
- [x] 2.3 校对 deliver count 升级到 `MaxDeliver` 后转死信的边界

## 2b. Handler 执行超时界限（review 追加）
- [x] 2b.1 `ConsumerSpec` 增加 `HandlerTimeout` 字段 + `DefaultHandlerTimeout`(30s)
- [x] 2b.2 `applyConsumerDefaults` 套默认并夹取 `HandlerTimeout < ClaimMinIdle`
- [x] 2b.3 `handleOne` 在 `HandlerTimeout` 子 ctx 内执行 handler
- [x] 2b.4 `reaper.go` XClaim 失败补 `ctx.Err()` 关停守卫
- [x] 2b.5 测试：挂死 handler 仍升级到死信、默认值夹取

## 3. 文档同步
- [x] 3.1 `consumer.go`/`doc.go`：handler 要求由"幂等"升级为"幂等且并发安全"
- [x] 3.2 说明首次重试延迟约为 `ClaimMinIdle`，重投不再热循环

## 4. 行为测试
- [x] 4.1 新增 `reaper_test.go`：in-memory fake `Client`（维护 PEL + deliver count + dead stream）
- [x] 4.2 用例：毒消息经多轮 `reapOnce` 后进死信、告警触发一次、deliver count 递增
- [x] 4.3 用例：PEL 中无人处理的老消息被 reaper claim 回并成功处理后 XAck
- [x] 4.4 用例：idle 未超时的 pending 被 reaper 跳过
- [x] 4.5 `go test ./... && go vet ./...` 通过
