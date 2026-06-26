# message-queue Specification

## Purpose
TBD - created by archiving change fix-deadletter-unreachable. Update Purpose after archive.
## Requirements
### Requirement: 失败消息的重投与死信升级

消费失败（handler 返回 error 或 panic）的消息 SHALL 仅通过 reaper 的 `XClaim` 路径重投，使 deliver count 单调增长；当某条消息的 deliver count 达到 `MaxDeliver` 时，系统 SHALL 将其转入 `<stream>:dead` 死信流、`XACK` 原消息并触发一次告警。

worker 主循环 SHALL NOT 通过 `XREADGROUP id=0` 自行重读 PEL 重投，因为该路径会重置 idle 且不增加 deliver count，会使死信升级永不可达。

#### Scenario: 持续失败的毒消息最终进入死信

- **GIVEN** 一个 handler 对某条消息恒定返回 error
- **AND** `MaxDeliver = N`
- **WHEN** reaper 经过足够多轮巡检，对该消息累计 `XClaim` 使其 deliver count 达到 N
- **THEN** 系统将该消息写入 `<stream>:dead`
- **AND** `XACK` 原 stream 上的该消息
- **AND** 通过 Alerter 触发一次死信告警

#### Scenario: idle 未超时的 pending 不被重投

- **GIVEN** 一条 pending 消息的 idle 小于 `ClaimMinIdle`
- **WHEN** reaper 巡检
- **THEN** 该消息被跳过，本轮不 `XClaim`、deliver count 不变

#### Scenario: 消费者崩溃后遗留的 pending 被接管

- **GIVEN** PEL 中存在一条曾被投递但无人处理、idle 已超过 `ClaimMinIdle` 的消息
- **AND** 其 deliver count 尚未达到 `MaxDeliver`
- **WHEN** reaper 巡检
- **THEN** reaper 通过 `XClaim` 将其抢到当前 consumer 名下并调用 handler 处理
- **AND** 处理成功后 `XACK` 该消息

### Requirement: Handler 并发与幂等约束

`Handler` SHALL 由调用方实现为幂等且并发安全，因为同一进程内 worker（处理新消息）与 reaper（处理重投消息）两个 goroutine 可能并发调用 handler，且消息在 at-least-once 语义下可能被重复投递。

#### Scenario: worker 与 reaper 并发调用 handler

- **WHEN** worker 正在处理新到达的消息
- **AND** reaper 同时对另一条卡住的消息触发重投
- **THEN** handler 被两个 goroutine 并发调用，调用方实现必须能安全承受

### Requirement: Handler 执行超时界限

每次 handler 调用 SHALL 在 `HandlerTimeout` 限定的子 context 内执行，且 `HandlerTimeout` SHALL 严格小于 `ClaimMinIdle`（零值取默认值；显式越界值由默认逻辑夹回 `ClaimMinIdle` 之内）。该上界 SHALL 确保挂死（但尊重 ctx）的 handler 不会永久冻结 reaper 扫描循环、不会架空死信升级。

当 `BatchSize = 1` 时该约束 SHALL 额外保证 worker 处理完一条消息后它才可能变得可被 reaper `XClaim`，从而不被并发重投；`BatchSize > 1` 时不提供该并发保证——一批消息同时投递、串行处理，靠后的消息可能在被处理前就 idle 超时被 reaper 重投，此为 at-least-once 内的重复投递，由 handler 幂等且并发安全兜底。

#### Scenario: 挂死但尊重 ctx 的 handler 仍能升级到死信

- **GIVEN** 一个 handler 阻塞直到 ctx 取消才返回
- **AND** `HandlerTimeout` 远小于 `ClaimMinIdle`
- **WHEN** reaper 在重投时调用该 handler
- **THEN** handler 在 `HandlerTimeout` 后因 ctx 取消而返回（视为失败）
- **AND** reaper 扫描循环不被永久阻塞
- **AND** 该消息在 deliver count 达到 `MaxDeliver` 后照常进入死信

#### Scenario: HandlerTimeout 越界被夹回

- **GIVEN** 注册时 `HandlerTimeout >= ClaimMinIdle`
- **WHEN** 套用默认值
- **THEN** `HandlerTimeout` 被夹到严格小于 `ClaimMinIdle`

### Requirement: 消费端死信处理回调

`ConsumerSpec` SHALL 提供一个可选的 `DeadLetterHandler` 回调。当一条消息累计投递达到 `MaxDeliver` 仍失败而被判定死亡时，系统在将其写入 `<stream>:dead` 死信流之后 SHALL 调用该回调（若已设置），把原消息与 `DeadLetterInfo` 元信息交给接入方就地处理。

死信流写入与回调 SHALL 并存：即使设置了回调，消息仍 SHALL 先被写入 `<stream>:dead` 作为持久兜底（死信的真相之源）。

回调 SHALL 为 best-effort：无论回调成功、返回 error 还是 panic，系统都 SHALL `XACK` 原消息并发死信告警（消息已持久落在死信流，不会丢失）。回调失败 SHALL 仅额外记错误日志并发一条回调失败告警，SHALL NOT 阻塞 `XACK`、SHALL NOT 触发库级重试（避免毒消息每轮重写死信流、冲刷 `<stream>:dead` 的 MAXLEN）。

回调 SHALL 在 `HandlerTimeout` 限定的子 context 内执行并带 panic 兜底，使挂死或 panic 的死信回调不会冻结 reaper 扫描循环。`DeadLetterHandler` 为 nil 时系统行为 SHALL 与未引入该特性前一致（写死信流 + `XACK` + 告警）。

回调必须由接入方实现为幂等且并发安全：多实例部署下多个 reaper 可能对同一 `OrigID` 并发调用回调。

#### Scenario: 设置回调时在死信流写入后被调用并成功

- **GIVEN** 一个设置了 `DeadLetterHandler` 的 ConsumerSpec
- **AND** 某条消息累计投递达到 `MaxDeliver`
- **WHEN** reaper 处理该死信
- **THEN** 系统先将消息 `XAdd` 到 `<stream>:dead`
- **AND** 随后以原消息与 `DeadLetterInfo`（含 `OrigID` / `DeadID`）调用 `DeadLetterHandler`
- **AND** 回调返回 nil 时 `XACK` 原消息并触发一次死信告警

#### Scenario: 回调失败不丢消息也不卡住

- **GIVEN** `DeadLetterHandler` 返回 error 或 panic
- **WHEN** reaper 处理该死信
- **THEN** 系统仍 `XACK` 原消息（消息已持久落 `<stream>:dead`）
- **AND** 额外记错误日志并发一条回调失败告警
- **AND** 不触发库级重试、不重复重写死信流，reaper 扫描循环不被阻塞或崩溃

#### Scenario: 未设置回调时保持原行为

- **GIVEN** `DeadLetterHandler` 为 nil
- **WHEN** 消息死亡
- **THEN** 系统写入 `<stream>:dead`、`XACK` 原消息并告警，与引入该特性前一致

