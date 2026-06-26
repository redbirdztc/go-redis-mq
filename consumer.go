package redisstream

import (
	"context"
	"runtime/debug"
	"time"
)

// Handler 业务处理函数
//
// 返回 nil → Manager 自动 XAck 该消息
// 返回 error → 不 ack，留在 PEL，等 reaper 周期巡检
// reaper 视 idle 超时为"卡住"，会 XClaim 抢回（deliver count +1）并直接重投
// 累计 deliver count 达到 ConsumerSpec.MaxDeliver 自动转死信流
//
// handler 必须幂等且并发安全：
//   - 幂等：消息可能因 reaper 重投 / 进程崩溃 / XAck 失败被重复投递
//   - 并发安全：同进程内 worker（处理新消息）与 reaper（重投卡住消息）两个
//     goroutine 可能并发调用 handler
//
// 失败消息首次重试延迟约为 ClaimMinIdle（默认 60s），而非热重试；可按业务调小
type Handler func(ctx context.Context, msg Message) error

// DeadLetterInfo 一条死信消息的元信息，随 DeadLetterHandler 传给接入方
type DeadLetterInfo struct {
	// Stream 业务流名（不含 Manager.keyPrefix 前缀）
	Stream string
	// Group 消费者组名
	Group string
	// Consumer 死亡时该消息归属的 consumer 名
	Consumer string
	// OrigID 原 stream 上的消息 ID（= msg.ID），死信去重的幂等键
	OrigID string
	// RetryCount 死亡时的累计投递次数（deliver count）
	RetryCount int64
	// Idle 死亡时距上次 delivery 的时间
	Idle time.Duration
	// DeadID 消息写入 <stream>:dead 死信流后分配的 ID，便于审计与关联
	DeadID string
}

// DeadLetterHandler 死信处理回调（可选，挂在 ConsumerSpec.DeadLetterHandler）
//
// 当一条消息累计投递达到 MaxDeliver 仍失败、被判定死亡时，库会：
//  1. 先把它 XAdd 到 <stream>:dead 死信流（持久兜底，是死信的"真相之源"）；
//  2. 再调用本回调（best-effort 即时反应钩子），把原消息 msg 与 DeadLetterInfo
//     交给接入方就地处理（落库 / 路由 / 自定义告警等）；
//  3. 无论回调成功还是失败，库都会 XAck 原消息并发死信告警——因为消息已持久落在
//     死信流里，不会丢。回调返回 error / panic 只记错误日志 + 一条回调失败告警，
//     **不会**阻塞 XAck、也**不会**触发库级重试（避免毒消息无限重写死信流、
//     反复冲刷 <stream>:dead 的 MAXLEN 把别的死信挤掉）。
//
// 因此：
//   - 回调是"尽力而为"的即时处理；要保证死信被可靠处理，请消费 <stream>:dead。
//   - 回调可能因 XAck 失败重试、且多实例下多个 reaper 会对同一 orig_id 并发调用，
//     故回调必须幂等且并发安全，按 DeadLetterInfo.OrigID 去重。
//   - payload 已被 MAXLEN 裁剪掉的死信不会触发回调（无消息体可交付）。
//
// 回调在 HandlerTimeout 子 ctx 内执行并带 panic 兜底，挂死（尊重 ctx）或 panic 的
// 回调不会冻结 reaper 扫描循环；回调应尊重 ctx 才能被超时打断。
type DeadLetterHandler func(ctx context.Context, msg Message, info DeadLetterInfo) error

// ConsumerSpec 一个 stream 的消费规格，调用方注册到 Manager
type ConsumerSpec struct {
	// Stream 业务流名（不含 Manager.keyPrefix 前缀）
	// 例：keyPrefix="smartCooker:stream:", Stream="order_pay_success"
	//     实际 Redis key 为 "smartCooker:stream:order_pay_success"
	Stream string

	// Group 消费者组名
	// 不同 group 各自独立消费同一份消息（Streams 多组语义）
	Group string

	// Consumer 当前进程的 consumer 名
	// 单实例部署用固定值即可，例如 "worker-0"
	// 多实例时应该用 hostname / pod name 区分
	Consumer string

	// Handler 业务处理函数（必填）
	Handler Handler

	// DeadLetterHandler 死信处理回调（可选）
	// 消息累计投递达 MaxDeliver 死亡时，先写 <stream>:dead 再调用本回调。
	// 为 nil 时仅写死信流 + 告警（向后兼容）。详见 DeadLetterHandler 类型说明
	DeadLetterHandler DeadLetterHandler

	// 以下为可选项，零值时套用默认值（见 defaults.go）

	// MaxDeliver 最大投递次数，超过转死信
	MaxDeliver int64

	// ClaimMinIdle pending 多久没 ack 视为卡住，由 reaper 抢回
	ClaimMinIdle time.Duration

	// ReapInterval reaper 巡检周期
	ReapInterval time.Duration

	// BatchSize XREADGROUP 单次读取条数
	BatchSize int64

	// BlockTimeout XREADGROUP 阻塞超时
	// 进程关停时 worker 最多等 BlockTimeout 才能退出，因此不要设太长
	BlockTimeout time.Duration

	// HandlerTimeout 单条消息 handler 的执行超时（基于 ctx）
	//
	// 会被 applyConsumerDefaults 夹到严格小于 ClaimMinIdle。
	//
	// 首要目的：给单次 handler 执行设上界。handler 由 worker 与 reaper 两个 goroutine
	// 调用；一个挂死的 handler 若在 reaper 中无限阻塞，会冻结 reaper 扫描循环，使整个
	// stream 再也无法升级到死信。限制在 HandlerTimeout 内可避免这一点。
	//
	// 次要作用：当 BatchSize=1 时，它也保证 worker 处理完该消息后它才可能变得可被
	// reaper claim，避免同一消息被并发重投。BatchSize>1 时不提供该保证（一批消息同时
	// 投递、串行处理，靠后的消息可能在被处理前就 idle 超时被 reaper 重投）——这属于
	// at-least-once 内的重复投递，由 handler 幂等且并发安全兜底。
	//
	// 注意：超时通过 ctx 取消传递，handler 必须尊重 ctx 才能真正被打断；
	// 完全忽略 ctx 的阻塞调用无法被强制中止。
	HandlerTimeout time.Duration
}

// applyConsumerDefaults 把 ConsumerSpec 的零值字段填上默认值
func applyConsumerDefaults(spec *ConsumerSpec) {
	if spec.MaxDeliver <= 0 {
		spec.MaxDeliver = DefaultMaxDeliver
	}
	if spec.ClaimMinIdle <= 0 {
		spec.ClaimMinIdle = DefaultClaimMinIdle
	}
	if spec.ReapInterval <= 0 {
		spec.ReapInterval = DefaultReapInterval
	}
	if spec.BatchSize <= 0 {
		spec.BatchSize = DefaultBatchSize
	}
	if spec.BlockTimeout <= 0 {
		spec.BlockTimeout = DefaultBlockTimeout
	}
	if spec.HandlerTimeout <= 0 {
		spec.HandlerTimeout = DefaultHandlerTimeout
	}
	// HandlerTimeout 夹到严格小于 ClaimMinIdle。
	//
	// 首要目的是给单次 handler 执行设上界：挂死（但尊重 ctx）的 handler 会在
	// HandlerTimeout 后被打断，不会永久冻结 reaper 扫描循环或无限拖延死信升级。
	//
	// 次要作用：当 BatchSize=1 时，它也保证 worker 处理完一条消息后该消息才可能
	// 变得可被 reaper claim，从而不会被并发重投。注意当 BatchSize>1 时不提供这一
	// 保证——一批消息在同一时刻被投递（idle 同时归零）却串行处理，靠后的消息可能
	// 在 worker 尚未处理到它时 idle 就超过 ClaimMinIdle 而被 reaper 重投。这属于
	// at-least-once 语义内的重复投递，由"handler 幂等且并发安全"的约束兜底，并非错误。
	if spec.HandlerTimeout >= spec.ClaimMinIdle {
		margin := spec.ClaimMinIdle / 10
		if margin <= 0 {
			margin = 1
		}
		spec.HandlerTimeout = spec.ClaimMinIdle - margin
	}
}

// runConsumer 一个 stream 的 worker 主循环
//
// 只阻塞读新消息（fromID=">"）。处理失败不 ack，留在 PEL。
// 失败 / 卡住消息的重投统一由 reaper 通过 XClaim 驱动——只有 XClaim 会让
// deliver count 增长，这样 MaxDeliver → 死信 的升级链路才能真正生效。
//
// worker 刻意不再 XReadGroup id="0" 自行重读 PEL：那条路径会重置 idle 但不增加
// deliver count，会把 reaper 的 idle 超时判断和死信升级一起架空（毒消息永世热重试）。
func (m *Manager) runConsumer(ctx context.Context, spec ConsumerSpec) {
	streamKey := m.keyPrefix + spec.Stream

	for {
		select {
		case <-ctx.Done():
			m.logger.Infof(ctx, "[redisstream] consumer exit, stream=%s group=%s", spec.Stream, spec.Group)
			return
		default:
		}

		// 阻塞读新消息：fromID=">" 阻塞等待 BlockTimeout
		m.readNew(ctx, spec, streamKey)
	}
}

// readNew 阻塞读取新消息
func (m *Manager) readNew(ctx context.Context, spec ConsumerSpec, streamKey string) {
	defer m.recoverReadLoop(ctx, spec)

	msgs, err := m.client.XReadGroupBlock(ctx, spec.Group, spec.Consumer, streamKey, ">", spec.BatchSize, spec.BlockTimeout)
	m.afterRead(ctx, spec, streamKey, msgs, err, ">")
}

// recoverReadLoop 单次 XReadGroup 的 panic 兜底
func (m *Manager) recoverReadLoop(ctx context.Context, spec ConsumerSpec) {
	if r := recover(); r != nil {
		m.logger.Errorf(ctx,
			"[redisstream] consumer panic, stream=%s group=%s err=%v stack=%s",
			spec.Stream, spec.Group, r, debug.Stack(),
		)
	}
}

// afterRead 处理 XReadGroup 的返回，分发到 handler
func (m *Manager) afterRead(ctx context.Context, spec ConsumerSpec, streamKey string, msgs []Message, err error, fromID string) {
	if err != nil {
		// ctx 取消时 client 应返回 context 错误，不告警
		if ctx.Err() != nil {
			return
		}
		m.logger.Errorf(ctx,
			"[redisstream] XReadGroup failed, stream=%s group=%s from=%s err=%v",
			spec.Stream, spec.Group, fromID, err,
		)
		// 短暂 sleep 避免热循环；带 ctx 中断
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
		return
	}

	for _, msg := range msgs {
		m.handleOne(ctx, spec, streamKey, msg)
	}
}

// handleOne 调用 Handler 处理单条消息，根据返回值决定是否 XACK
//
// handler 在 HandlerTimeout 限定的子 ctx 内执行：超时即 ctx 取消，handler 应据此
// 返回（视为失败、不 ack、留 PEL 等下一轮）。这保证 worker 在消息变得可被 reaper
// claim 之前一定收手，并防止挂死的 handler 冻结 reaper 扫描循环。
func (m *Manager) handleOne(ctx context.Context, spec ConsumerSpec, streamKey string, msg Message) {
	hctx := ctx
	if spec.HandlerTimeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(ctx, spec.HandlerTimeout)
		defer cancel()
	}

	// 单条消息的 panic 不能扩散到 worker 循环
	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				m.logger.Errorf(ctx,
					"[redisstream] handler panic, stream=%s id=%s err=%v stack=%s",
					spec.Stream, msg.ID, r, debug.Stack(),
				)
				handlerErr = &handlerPanicError{value: r}
			}
		}()
		handlerErr = spec.Handler(hctx, msg)
	}()

	if handlerErr != nil {
		m.logger.Errorf(ctx,
			"[redisstream] handler failed, stream=%s id=%s err=%v",
			spec.Stream, msg.ID, handlerErr,
		)
		// 不 ack，等 reaper 处理
		return
	}

	if err := m.client.XAck(ctx, streamKey, spec.Group, msg.ID); err != nil {
		m.logger.Errorf(ctx,
			"[redisstream] XAck failed, stream=%s id=%s err=%v",
			spec.Stream, msg.ID, err,
		)
		// ack 失败：reaper 会兜底重投，handler 必须幂等
	}
}

// handlerPanicError 把 panic 的值包成 error
type handlerPanicError struct {
	value interface{}
}

func (e *handlerPanicError) Error() string {
	return "handler panic"
}
