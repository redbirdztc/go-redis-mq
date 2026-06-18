package redisstream

import "time"

// 各 spec 的默认值，零值时套用
const (
	// DefaultMaxDeliver 默认最大投递次数（超过转死信）
	DefaultMaxDeliver int64 = 5

	// DefaultClaimMinIdle 默认 pending 多久无 ack 视为卡住
	DefaultClaimMinIdle time.Duration = 60 * time.Second

	// DefaultReapInterval 默认 reaper 巡检周期
	DefaultReapInterval time.Duration = 30 * time.Second

	// DefaultBatchSize 默认 XREADGROUP 单次读取条数
	DefaultBatchSize int64 = 16

	// DefaultBlockTimeout 默认 XREADGROUP 阻塞超时
	// 也是 worker 在 ctx 取消后退出的最大延迟，不要设太长
	DefaultBlockTimeout time.Duration = 5 * time.Second

	// DefaultMaxLen 默认 Stream 近似裁剪上限
	// 业务可按 "峰值 QPS × 最坏滞后秒数 × 2~3" 估算后用 PublishWithMaxLen 覆盖
	DefaultMaxLen int64 = 100000

	// DefaultDispatcherInterval 默认 Dispatcher 轮询周期
	DefaultDispatcherInterval time.Duration = 2 * time.Second

	// DefaultDispatcherBatchSize 默认 Dispatcher 单次拉取条数
	DefaultDispatcherBatchSize int = 100

	// DefaultFetchFailAlertThreshold FetchPending 连续失败多少轮告警一次
	DefaultFetchFailAlertThreshold int = 5
)

// reaperBatchSize reaper 单轮 XPendingExt 读取上限
// 没暴露成 spec 字段是因为它只影响吞吐，正常情况下 30s 内积压不会超
const reaperBatchSize int64 = 100
