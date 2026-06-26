# Tasks

## 1. 公共 API
- [x] 1.1 `consumer.go`：新增 `DeadLetterInfo` 结构
- [x] 1.2 `consumer.go`：新增 `DeadLetterHandler` 回调类型
- [x] 1.3 `consumer.go`：`ConsumerSpec` 增加可选字段 `DeadLetterHandler`

## 2. reaper / 死信逻辑
- [x] 2.1 `deadletter.go`：写死信流后、XAck 前调用 `DeadLetterHandler`（设置时）
- [x] 2.2 回调在 `HandlerTimeout` 子 ctx 内执行 + panic 兜底
- [x] 2.3 回调返回 error/panic → 不 XAck，下一轮重试；返回 nil → XAck + 告警
- [x] 2.4 回调为 nil 时保持现状（向后兼容）

## 3. 文档同步
- [x] 3.1 `doc.go`：死信语义补充回调说明
- [x] 3.2 `README.md`：死信小节补充 `DeadLetterHandler` 用法与幂等/去重提示

## 4. 测试
- [x] 4.1 新增 `deadletter_test.go`：回调在死信流写入后被调用、success 后 XAck + 告警
- [x] 4.2 用例：回调返回 error → 不 XAck、下一轮重试、success 后才告警
- [x] 4.3 用例：回调 panic 不冻结 reaper、消息不丢
- [x] 4.4 用例：回调 nil → 保持现状（写死信流 + XAck + 告警）
- [x] 4.5 `go test -race ./... && go vet ./...` 通过
