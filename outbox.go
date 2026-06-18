package redisstream

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// OutboxRecord 一条 outbox 表里待发布的消息
//
// 字段约定：
//   - LocalID：outbox 表主键，作为消费方的幂等键
//     （建议把 LocalID 也写进 Values 的某个固定字段，例如 "outbox_local_id"）
//   - Stream：目标业务流名，不含 Dispatcher 的 keyPrefix 前缀
//   - Values：XAdd 用的字段映射，业务自行决定 schema
//
// 注意：本结构刻意不带 Attempts / LastError 字段。
// 失败次数、放弃阈值、死信状态等属于业务侧的状态机，应在 FetchPending 的 SQL
// 里直接过滤（例如 WHERE attempts < 10 AND state = 'pending'），与本库解耦
type OutboxRecord struct {
	LocalID int64
	Stream  string
	Values  map[string]interface{}
}

// OutboxStore 本地消息表抽象，由调用方实现
//
// 调用方业务侧自行 INSERT outbox 行（INSERT INTO outbox(...) VALUES (...)），
// 必须与业务表写在同一个 *sql.Tx / gorm tx 内 commit，否则消息会丢/出现幽灵。
// 本库不提供 Append 方法正是为了不绑定 ORM 风格。
//
// 实现要求：
//
//  1. FetchPending 必须保证多实例 dispatcher 并发安全：
//     - PostgreSQL / MySQL 8.0+：SELECT ... FOR UPDATE SKIP LOCKED 是最佳实践
//     - MySQL 5.7：用乐观锁
//         UPDATE outbox SET locked_by=?, locked_at=NOW()
//           WHERE state='pending' AND (locked_at IS NULL OR locked_at < NOW() - INTERVAL 5 MINUTE)
//           ORDER BY id LIMIT ?
//         然后 SELECT WHERE locked_by=?
//
//  2. MarkPublished 必须幂等：
//     XAdd 成功但 MarkPublished 失败时，下一轮 FetchPending 会再返回这条消息，
//     dispatcher 会重新 XAdd，导致 Stream 出现重复。消费方必须基于 LocalID 去重。
//
//  3. 失败 / 重试 / 放弃由调用方在 FetchPending 的 SQL 里管：
//     dispatcher 本身不跟踪 attempts、不会判 dead。
//     例如调用方可以加 `WHERE attempts < 10` 让超限的不再被取出，
//     再用单独的清理 job 把超限行迁到 outbox_dead 表。
//
//  4. 推荐的 outbox 表结构：
//
//     CREATE TABLE outbox (
//       id           BIGINT       AUTO_INCREMENT PRIMARY KEY,
//       stream       VARCHAR(64)  NOT NULL,
//       payload      MEDIUMBLOB   NOT NULL,        -- 序列化后的 Values
//       state        TINYINT      NOT NULL DEFAULT 0, -- 0=pending 1=published
//       attempts     INT          NOT NULL DEFAULT 0, -- 业务侧自管
//       stream_id    VARCHAR(64)  NULL,           -- Redis Stream 分配的 ID（成功后回填）
//       locked_by    VARCHAR(64)  NULL,
//       locked_at    DATETIME     NULL,
//       create_time  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
//       update_time  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
//       INDEX idx_state_id (state, id)
//     );
type OutboxStore interface {
	// FetchPending 取出 limit 条待发布消息
	// 实现应对返回的记录做"占用"处理（行锁 / lockedBy 标记），避免多 dispatcher 重复处理
	// 也是调用方决定"哪些算 pending"的唯一入口：失败重试 / 放弃逻辑都靠这条 SQL 表达
	FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)

	// MarkPublished 标记一条消息已成功发布到 Stream
	// streamID 是 XAdd 返回的 Redis 消息 ID，便于审计与排查
	// 必须幂等：本方法失败会导致下一轮重发，但 Stream 端已有数据
	MarkPublished(ctx context.Context, localID int64, streamID string) error
}

// DispatcherConfig Dispatcher 的构造参数
//
// 提交给 NewDispatcher 后，Dispatcher 内部会拷贝并应用默认值，原 Config 可安全丢弃
type DispatcherConfig struct {
	// Client Redis 客户端抽象（必填）
	Client Client

	// Store 本地消息表实现（必填）
	Store OutboxStore

	// Logger 日志（nil 时用 NopLogger）
	Logger Logger

	// Alerter 告警（nil 时用 NopAlerter）
	// 仅在 FetchPending 持续失败 / dispatchOnce panic 等不可恢复场景触发
	// XAdd 失败因噪音大不告警
	Alerter Alerter

	// KeyPrefix Stream key 前缀，与 Manager 保持一致
	// 例："smartCooker:stream:"
	KeyPrefix string

	// Interval 轮询周期（零值用 DefaultDispatcherInterval = 2s）
	Interval time.Duration

	// BatchSize 单次拉取条数（零值用 DefaultDispatcherBatchSize = 100）
	BatchSize int

	// MaxLenApprox Stream 近似裁剪上限
	// <=0 表示不裁剪（不推荐，Redis 内存会持续增长）
	// 推荐显式设为 redisstream.DefaultMaxLen 或业务自估值
	MaxLenApprox int64

	// FetchFailAlertThreshold FetchPending 连续失败多少轮后触发一次告警
	// 零值用 DefaultFetchFailAlertThreshold = 5（约 5 × Interval ≈ 10s）
	// 设负数关闭该告警
	FetchFailAlertThreshold int
}

// Dispatcher 本地消息表派发器
//
// 周期从 OutboxStore 取 pending 行，XAdd 到对应 Stream，然后 MarkPublished
//
// 失败处理：
//   - XAdd 失败：库只打 warn 日志，下一轮 FetchPending 会再取这条
//   - MarkPublished 失败：库打 error 日志，下一轮重发 → 消费方需按 LocalID 去重
//   - FetchPending 连续失败超过 fetchFailAlertThreshold → 告警一次
//
// 单进程一个 Dispatcher 足够；多实例部署时通过 OutboxStore.FetchPending 的行锁
// 实现实例间互斥
//
// 并发约束：所有字段在 NewDispatcher 后只读，Run 期间不可修改
type Dispatcher struct {
	client                  Client
	store                   OutboxStore
	logger                  Logger
	alerter                 Alerter
	keyPrefix               string
	interval                time.Duration
	batchSize               int
	maxLenApprox            int64
	fetchFailAlertThreshold int

	// dispatchOnce 内部串行访问，不需要锁
	consecutiveFetchFails int
	fetchAlertSent        bool
}

// NewDispatcher 创建并初始化 Dispatcher
//
// cfg.Client / cfg.Store 必填；其它字段零值时套默认值
// 返回的 Dispatcher 内部字段已快照，cfg 可丢弃
func NewDispatcher(cfg DispatcherConfig) (*Dispatcher, error) {
	if cfg.Client == nil {
		return nil, errors.New("redisstream: DispatcherConfig.Client is nil")
	}
	if cfg.Store == nil {
		return nil, errors.New("redisstream: DispatcherConfig.Store is nil")
	}

	d := &Dispatcher{
		client:                  cfg.Client,
		store:                   cfg.Store,
		logger:                  cfg.Logger,
		alerter:                 cfg.Alerter,
		keyPrefix:               cfg.KeyPrefix,
		interval:                cfg.Interval,
		batchSize:               cfg.BatchSize,
		maxLenApprox:            cfg.MaxLenApprox,
		fetchFailAlertThreshold: cfg.FetchFailAlertThreshold,
	}
	if d.logger == nil {
		d.logger = NopLogger{}
	}
	if d.alerter == nil {
		d.alerter = NopAlerter{}
	}
	if d.interval <= 0 {
		d.interval = DefaultDispatcherInterval
	}
	if d.batchSize <= 0 {
		d.batchSize = DefaultDispatcherBatchSize
	}
	// maxLenApprox <= 0 在 XAdd 路径已经被处理为不裁剪，无需特殊化
	if d.fetchFailAlertThreshold == 0 {
		d.fetchFailAlertThreshold = DefaultFetchFailAlertThreshold
	}
	return d, nil
}

// Run 启动派发循环，阻塞直到 ctx 取消
func (d *Dispatcher) Run(ctx context.Context) error {
	d.logger.Infof(ctx, "[redisstream-outbox] dispatcher begin interval=%s batch=%d",
		d.interval, d.batchSize)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	// 启动时立刻跑一轮，避免冷启动时 outbox 已有积压等到第一次 tick
	d.dispatchOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			d.logger.Infof(ctx, "[redisstream-outbox] dispatcher exit")
			return nil
		case <-ticker.C:
			d.dispatchOnce(ctx)
		}
	}
}

// dispatchOnce 单轮派发
func (d *Dispatcher) dispatchOnce(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Errorf(ctx, "[redisstream-outbox] dispatcher panic err=%v stack=%s", r, debug.Stack())
			d.alerter.Alert(ctx, AlertLevelCritical, "Outbox dispatcher panic",
				fmt.Sprintf("err: %+v\nstack: %s", r, debug.Stack()))
		}
	}()

	records, err := d.store.FetchPending(ctx, d.batchSize)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		d.handleFetchFail(ctx, err)
		return
	}

	// 成功了清空连续失败计数
	if d.consecutiveFetchFails > 0 {
		d.logger.Infof(ctx, "[redisstream-outbox] FetchPending recovered after %d failures",
			d.consecutiveFetchFails)
		d.consecutiveFetchFails = 0
		d.fetchAlertSent = false
	}

	for _, r := range records {
		d.processOne(ctx, r)
	}
}

// handleFetchFail FetchPending 失败时累计连续失败次数，超过阈值告警一次
//
// 用 fetchAlertSent 防止持续失败时反复告警，恢复后才能再次告警
func (d *Dispatcher) handleFetchFail(ctx context.Context, err error) {
	d.consecutiveFetchFails++
	d.logger.Errorf(ctx, "[redisstream-outbox] FetchPending failed count=%d err=%v",
		d.consecutiveFetchFails, err)

	if d.fetchFailAlertThreshold > 0 &&
		d.consecutiveFetchFails >= d.fetchFailAlertThreshold &&
		!d.fetchAlertSent {
		content := fmt.Sprintf(
			"FetchPending 连续失败 %d 轮 (阈值 %d)\nlast_err: %v\nouter 积压可能正在累积，请尽快排查",
			d.consecutiveFetchFails, d.fetchFailAlertThreshold, err,
		)
		d.alerter.Alert(ctx, AlertLevelError, "Outbox FetchPending 持续失败", content)
		d.fetchAlertSent = true
	}
}

// processOne 处理一条 outbox 记录
//
// XAdd 失败 → 仅记日志；下一轮 FetchPending 仍会返回该记录（前提是调用方的
// SQL 没把它过滤掉），dispatcher 不主动跟踪 attempts
//
// MarkPublished 失败 → 记 error 日志；下一轮会再发一份，消费方需按 LocalID 去重
func (d *Dispatcher) processOne(ctx context.Context, r OutboxRecord) {
	streamKey := d.keyPrefix + r.Stream

	// maxLenApprox <= 0 时传 0 给 XAdd，Client 实现应理解 0 为"不裁剪"
	trim := d.maxLenApprox
	if trim < 0 {
		trim = 0
	}

	streamID, err := d.client.XAdd(ctx, streamKey, trim, r.Values)
	if err != nil {
		d.logger.Warnf(ctx, "[redisstream-outbox] XAdd failed local_id=%d stream=%s err=%v",
			r.LocalID, r.Stream, err)
		return
	}

	if err := d.store.MarkPublished(ctx, r.LocalID, streamID); err != nil {
		d.logger.Errorf(ctx, "[redisstream-outbox] MarkPublished failed local_id=%d stream_id=%s err=%v",
			r.LocalID, streamID, err)
		return
	}
}
