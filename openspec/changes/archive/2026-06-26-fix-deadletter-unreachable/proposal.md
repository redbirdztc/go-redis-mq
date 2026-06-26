# 修复死信不可达缺陷（重投职责收归 reaper）

## Why

当前 worker 主循环每轮无条件 `readPEL(fromID="0")` 重读自身全部 pending 消息（`consumer.go:93,104`）。Redis 的 `XREADGROUP id=0` 会**重置 idle 但不增加 deliver count**，导致：

1. handler 持续失败的"毒消息"被 worker 每 ~`BlockTimeout`(5s) 热重试，idle 始终被压回 ~0；
2. reaper 永远判定 `p.Idle < ClaimMinIdle`（默认 60s）而 `continue`，永不 `XClaim`；
3. deliver count 永不增长，`RetryCount >= MaxDeliver` 永不成立；
4. 消息**永不转死信、永不告警**，与 `doc.go` / `consumer.go` 承诺的"MaxDeliver 次失败转死信"语义直接矛盾。

死信链路实际只在 consumer 进程崩溃（无人重置 idle）时才生效。这是定位为消息基建的库不可接受的可靠性缺口，且当前无任何行为测试覆盖该逻辑。

## What Changes

- **worker 不再自旋读 PEL**：`runConsumer` 只读新消息（`fromID=">"`），删除 `readPEL` 路径。失败/卡住消息留在 PEL，重投统一由 reaper 驱动。
- **重投职责收归 reaper**：`reapOnce` 在 `XClaim` 抢回卡住消息后，用其返回的消息体直接调用 `handleOne` 重投（此前返回值被丢弃）。只有 `XClaim` 会增加 deliver count，从而让 `MaxDeliver → 死信` 真正可达，并天然获得 `ClaimMinIdle` 节奏的退避。
- **`XClaim` 的 minIdle 改回 `spec.ClaimMinIdle`**（此前传 `0`）：worker 已不抢 PEL，唯一竞争是多 reaper 实例，交给 Redis 用 minIdle 做最终校验即可天然去重，使 `reaper.go` "多实例 XClaim 会去重"的注释名副其实。
- **新增 `HandlerTimeout`（review 追加）**：handler 在 `HandlerTimeout` 限定的子 ctx 内执行，默认 30s 且强制 < `ClaimMinIdle`。这关闭 reaper 同步执行 handler 引入的两个回归：① worker 慢 handler 仍在跑时被 reaper 并发重投同一条消息；② 挂死 handler 永久冻结 reaper 扫描循环、架空死信升级。
- **文档补充**：`handler` 现会被 worker 与 reaper 两个 goroutine 调用，要求由"幂等"升级为"幂等且并发安全"；说明首次重试延迟从 ~5s 变为 `ClaimMinIdle`。
- **补行为测试**：新增基于 in-memory fake `Client` 的 reaper/死信单测，覆盖毒消息升级、崩溃恢复、未超时跳过三类场景。

## Impact

- Affected specs: `message-queue`
- Affected code: `consumer.go`、`reaper.go`、`doc.go`，新增 `reaper_test.go`
- 行为变化：失败消息首次重试延迟变长（约 `ClaimMinIdle`），但换来死信/告警链路真正生效，且不再热重试打爆下游。要求接入方 handler 并发安全（多实例部署本就要求）。
