# message-queue Spec Delta

## ADDED Requirements

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
