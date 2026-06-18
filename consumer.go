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
// reaper 视 idle 超时为"卡住"，会 XClaim 重置（deliver count +1）
// 累计 deliver count 超过 ConsumerSpec.MaxDeliver 自动转死信流
//
// handler 必须幂等：消息可能因 reaper 重试 / 进程崩溃 / XAck 失败被重复投递
type Handler func(ctx context.Context, msg Message) error

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
}

// runConsumer 一个 stream 的 worker 主循环
//
// 每轮先非阻塞读 PEL 里的旧消息（reaper 重置过 idle 的）
// 再阻塞读新消息
// 处理失败不 ack，留给 reaper 处理
func (m *Manager) runConsumer(ctx context.Context, spec ConsumerSpec) {
	streamKey := m.keyPrefix + spec.Stream

	for {
		select {
		case <-ctx.Done():
			m.logger.Infof(ctx, "[redisstream] consumer exit, stream=%s group=%s", spec.Stream, spec.Group)
			return
		default:
		}

		// 1. 先读 PEL：fromID="0" 非阻塞读自己 PEL 里被 reaper 重置过的旧消息
		m.readPEL(ctx, spec, streamKey)

		// 2. 再读新消息：fromID=">" 阻塞等待 BlockTimeout
		m.readNew(ctx, spec, streamKey)
	}
}

// readPEL 非阻塞读取自己 PEL 里的旧消息（reaper 重置 idle 后回流到当前 consumer）
func (m *Manager) readPEL(ctx context.Context, spec ConsumerSpec, streamKey string) {
	defer m.recoverReadLoop(ctx, spec)

	msgs, err := m.client.XReadGroupNoBlock(ctx, spec.Group, spec.Consumer, streamKey, "0", spec.BatchSize)
	m.afterRead(ctx, spec, streamKey, msgs, err, "0")
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
func (m *Manager) handleOne(ctx context.Context, spec ConsumerSpec, streamKey string, msg Message) {
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
		handlerErr = spec.Handler(ctx, msg)
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
