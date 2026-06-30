package redisstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Manager 消息队列管理器
//
// 负责：
//   - 注册 ConsumerSpec
//   - 启动每个 stream 的 worker + reaper 协程
//   - 提供直接 Publish 接口
//
// 一个进程通常只需一个 Manager，但多个 Manager 共用同一 Client / KeyPrefix 也可。
//
// 并发安全：Register / Publish / Run 均可在多协程调用。
// Run 必须只调用一次（重复调用将 panic 由 sync.Once 触发）。
type Manager struct {
	client    Client
	logger    Logger
	alerter   Alerter
	keyPrefix string

	mu    sync.Mutex
	specs []ConsumerSpec

	runOnce sync.Once
	running bool
}

// NewManager 创建 Manager
//
// keyPrefix 会拼在 Stream 业务名前组成 Redis key，例如：
//   keyPrefix = "smartCooker:stream:", spec.Stream = "order_pay_success"
//   实际 Redis key 为 "smartCooker:stream:order_pay_success"
//   死信 key 为 "smartCooker:stream:order_pay_success:dead"
//
// 允许传 "" 不加前缀，但生产建议加上以避免与其它业务 key 冲突
func NewManager(client Client, logger Logger, alerter Alerter, keyPrefix string) *Manager {
	if logger == nil {
		logger = NopLogger{}
	}
	if alerter == nil {
		alerter = NopAlerter{}
	}
	return &Manager{
		client:    client,
		logger:    logger,
		alerter:   alerter,
		keyPrefix: keyPrefix,
	}
}

// Register 注册一个 stream 的消费规格
//
// 必须在 Run 调用前完成。重复注册同 (Stream, Group) 返回错误
func (m *Manager) Register(spec ConsumerSpec) error {
	if m.client == nil {
		return errors.New("redisstream: manager.client is nil")
	}
	if spec.Stream == "" {
		return errors.New("redisstream: spec.Stream is required")
	}
	if spec.Group == "" {
		return errors.New("redisstream: spec.Group is required")
	}
	if spec.Consumer == "" {
		return errors.New("redisstream: spec.Consumer is required")
	}
	if spec.Handler == nil {
		return errors.New("redisstream: spec.Handler is required")
	}
	applyConsumerDefaults(&spec)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return errors.New("redisstream: cannot Register after Run")
	}
	for _, existing := range m.specs {
		if existing.Stream == spec.Stream && existing.Group == spec.Group {
			return errors.New("redisstream: duplicate (stream, group) registration: " + spec.Stream + "/" + spec.Group)
		}
	}
	m.specs = append(m.specs, spec)
	return nil
}

// Publish 投递一条消息到指定 stream
//
// 自动加 keyPrefix 前缀，并用 DefaultMaxLen 近似裁剪
// 返回 Redis 分配的消息 ID
//
// 注意：此 Publish 是直接投递，不经过 outbox 表，不保证与业务 DB tx 的原子性。
// 需要 tx 一致性的场景请用 Dispatcher + OutboxStore
func (m *Manager) Publish(ctx context.Context, stream string, values map[string]interface{}) (string, error) {
	return m.PublishWithMaxLen(ctx, stream, values, DefaultMaxLen)
}

// PublishWithMaxLen 同 Publish，但可自定义 MAXLEN
// maxLen <= 0 表示不裁剪（不推荐）
func (m *Manager) PublishWithMaxLen(ctx context.Context, stream string, values map[string]interface{}, maxLen int64) (string, error) {
	if maxLen < 0 {
		maxLen = 0
	}
	return m.client.XAdd(ctx, m.keyPrefix+stream, maxLen, values)
}

// Run 启动所有已注册 stream 的 worker + reaper，阻塞直到 ctx 取消
//
// 每个 stream 起两个 goroutine：worker 主循环 + reaper 巡检
// ensureGroup 失败的 spec 跳过启动，避免持续 NOGROUP 报错
// 调用方必须保证只调用一次
func (m *Manager) Run(ctx context.Context) error {
	var runErr error
	called := false
	m.runOnce.Do(func() {
		called = true
		runErr = m.runInternal(ctx)
	})
	if !called {
		return errors.New("redisstream: Manager.Run called twice")
	}
	return runErr
}

func (m *Manager) runInternal(ctx context.Context) error {
	m.mu.Lock()
	specs := make([]ConsumerSpec, len(m.specs))
	copy(specs, m.specs)
	m.running = true
	m.mu.Unlock()

	if len(specs) == 0 {
		m.logger.Infof(ctx, "[redisstream] no consumer registered, exit")
		return nil
	}

	// 只为 ensureGroup 成功的 spec 启动 worker + reaper
	// 失败的 spec 跳过，避免 NOGROUP 错误持续刷日志，并发告警提醒
	readySpecs := make([]ConsumerSpec, 0, len(specs))
	for i := range specs {
		spec := specs[i]
		if err := m.ensureGroup(ctx, spec); err != nil {
			// ctx 取消导致的失败属于正常关停路径，不告警
			if ctx.Err() != nil {
				m.logger.Infof(ctx,
					"[redisstream] ensureGroup canceled by ctx, stream=%s group=%s",
					spec.Stream, spec.Group,
				)
				return nil
			}
			m.logger.Errorf(ctx,
				"[redisstream] ensureGroup failed, stream=%s group=%s err=%v (skip start)",
				spec.Stream, spec.Group, err,
			)
			// 静默少跑 consumer 比死信更危险，发一次告警
			m.alerter.Alert(ctx, AlertLevelError,
				"Redis Stream consumer 启动失败",
				fmt.Sprintf("stream: %s\ngroup: %s\nerr: %v\n该 consumer 已跳过启动，业务消息将堆积",
					spec.Stream, spec.Group, err),
			)
			continue
		}
		m.logger.Infof(ctx,
			"[redisstream] start consumer stream=%s group=%s consumer=%s",
			spec.Stream, spec.Group, spec.Consumer,
		)
		readySpecs = append(readySpecs, spec)
	}

	if len(readySpecs) == 0 {
		m.logger.Errorf(ctx, "[redisstream] no consumer started (all ensureGroup failed), exit")
		return errors.New("redisstream: no consumer started")
	}

	var wg sync.WaitGroup
	for i := range readySpecs {
		spec := readySpecs[i]
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.runConsumer(ctx, spec)
		}()
		go func() {
			defer wg.Done()
			m.runReaper(ctx, spec)
		}()
	}
	wg.Wait()
	m.logger.Infof(ctx, "[redisstream] all consumers exited")
	return nil
}

// EnsureGroups 预先创建所有已注册 spec 的 consumer group（幂等，BUSYGROUP 视为已存在）
//
// 用途：消除「Dispatcher 启动即发一轮」与「按 "$" 建组只收建组后消息」之间的启动竞态。
// 调用方应在【启动 Dispatcher 之前】先调用本方法，保证 group 先于任何 XADD 存在；
// 如此 "$" 既能消费随后被 Dispatcher 泵入的 outbox 积压（XADD 发生在建组之后），
// 又不会像 startID="0" 那样让后续新加入的 group 重放整条流的历史。
//
// 单进程内顺序调用即可保证有序；多实例下各 pod 都先 Ensure 再发，首个 pod 建组、其余 BUSYGROUP。
// 返回首个非 BUSYGROUP 的硬错误（如 Redis 不支持 Stream）；ctx 取消按关停处理返回 ctx.Err()。
// 失败时调用方应放弃启动 Dispatcher（fail-closed），避免消息 XADD 出去却无组消费而丢失。
func (m *Manager) EnsureGroups(ctx context.Context) error {
	m.mu.Lock()
	specs := make([]ConsumerSpec, len(m.specs))
	copy(specs, m.specs)
	m.mu.Unlock()

	for i := range specs {
		if err := m.ensureGroup(ctx, specs[i]); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("redisstream: ensure group %s/%s: %w", specs[i].Stream, specs[i].Group, err)
		}
	}
	return nil
}

// ensureGroup 保证 stream + group 存在
//
// 用 XGroupCreateMkStream（stream 不存在自动创建）
// 已存在的 group 通过 Client.IsBusyGroup 识别后忽略
//
// startID = "$" → 只消费建组后的新消息，新加入的 group 不重放历史。
// ⚠️ 启动竞态：Dispatcher 启动即发一轮，若它在本组创建【之前】就把 outbox 积压 XADD 进流，
// 按 "$" 建组会跳过这些先到消息 → 永不投递且不重试（静默丢消息）。
// 故调用方须在启动 Dispatcher 前先调用 EnsureGroups，保证组先于任何 XADD 存在。
func (m *Manager) ensureGroup(ctx context.Context, spec ConsumerSpec) error {
	streamKey := m.keyPrefix + spec.Stream
	err := m.client.XGroupCreateMkStream(ctx, streamKey, spec.Group, "$")
	if err != nil && !m.client.IsBusyGroup(err) {
		return err
	}
	return nil
}
