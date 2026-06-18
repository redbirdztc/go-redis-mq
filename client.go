package redisstream

import (
	"context"
	"time"
)

// Message Stream 中的一条消息
type Message struct {
	// ID Stream 分配的 ID，形如 "1700000000000-0"
	ID string
	// Values 消息字段
	// 业务通常约定一个 "payload" 字段存 JSON，其它字段做索引
	Values map[string]interface{}
}

// PendingInfo 一条 pending 消息的元信息（来自 XPENDING 命令）
type PendingInfo struct {
	// ID 消息 ID
	ID string
	// Consumer 当前归属的 consumer 名
	Consumer string
	// Idle 距上次 delivery 的时间
	Idle time.Duration
	// RetryCount 已投递次数（deliver count）
	RetryCount int64
}

// Client 对 Redis Streams 命令的抽象，由调用方实现并注入到 Manager / Dispatcher
//
// 实现要点：
//   - 所有方法必须尊重 ctx 取消语义（go-redis v8 在 BLOCK 命令上不能可靠中断，
//     调用方需自行控制阻塞时长）
//   - 错误应原样返回，不要包装成 fmt.Errorf 后丢失原类型，否则 IsBusyGroup 判断会失效
//   - "无消息" 不应作为 error 返回，XReadGroup / XRangeN / XPendingExt 返回空切片即可
type Client interface {
	// XAdd 投递消息
	// maxLenApprox = 0 表示不裁剪；> 0 时用 MAXLEN ~ 近似裁剪
	XAdd(ctx context.Context, stream string, maxLenApprox int64, values map[string]interface{}) (id string, err error)

	// XReadGroupBlock 阻塞式读取消息，最长等待 block 时长
	// fromID = ">" 读新消息（未分配给任何 consumer 的）
	// fromID = "0" 读自己 PEL 里未 ack 的旧消息（一般用 XReadGroupNoBlock 即可）
	// block <= 0 是非法值，请用 XReadGroupNoBlock；实现可视为等价于 XReadGroupNoBlock
	// 空结果应返回 nil 错误 + 空切片
	//
	// 实现警告：go-redis v8 的 XReadGroupArgs.Block = 0 是 Redis BLOCK 0 无限阻塞，
	// 不是非阻塞，新手实现时容易踩坑。请确保 block <= 0 走 XReadGroupNoBlock 分支
	XReadGroupBlock(ctx context.Context, group, consumer, stream, fromID string, count int64, block time.Duration) ([]Message, error)

	// XReadGroupNoBlock 非阻塞读取消息（Redis 端 XREADGROUP 不带 BLOCK 子句）
	// 队列空时立即返回 nil 错误 + 空切片
	// 主要用于 worker "先读 PEL 里的重投消息" 路径
	//
	// 实现要点（go-redis v8.3.3）：
	//   args := &redis.XReadGroupArgs{Block: -1, ...}  // -1 让 go-redis 不追加 BLOCK
	// 不要用 0，0 是无限阻塞
	XReadGroupNoBlock(ctx context.Context, group, consumer, stream, fromID string, count int64) ([]Message, error)

	// XAck 确认消息处理完成
	XAck(ctx context.Context, stream, group, id string) error

	// XClaim 把 pending 消息归到 consumer 名下
	// 副作用：deliver count +1，idle 重置为 0
	// 适合"reaper 把卡住的消息重新交给 worker 重试"
	XClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, ids []string) ([]Message, error)

	// XPendingExt 扫描 group 的 pending 列表
	// start/end 用 "-" / "+" 取全部
	// 注意：go-redis v8.3.3 的 XPendingExt 没有 Idle filter，需要客户端侧过滤
	XPendingExt(ctx context.Context, stream, group, start, end string, count int64) ([]PendingInfo, error)

	// XRangeN 按 ID 区间读取消息内容（只读，不动 PEL / idle / deliver count）
	// 死信路径用它取 payload，避免 XClaim 的 idle 重置副作用
	XRangeN(ctx context.Context, stream, start, stop string, count int64) ([]Message, error)

	// XGroupCreateMkStream 创建 consumer group
	// stream 不存在时一并创建（MKSTREAM 子句）
	// startID = "$" 只消费创建后的新消息；"0" 从头消费
	// group 已存在时返回的错误应能被 IsBusyGroup 识别
	XGroupCreateMkStream(ctx context.Context, stream, group, startID string) error

	// IsBusyGroup 判断 XGroupCreateMkStream 返回的错误是否为 "group 已存在"
	// 实现示例：strings.Contains(err.Error(), "BUSYGROUP")
	IsBusyGroup(err error) bool
}
