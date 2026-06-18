package redisstream

import "context"

// AlertLevel 告警级别
type AlertLevel int

const (
	// AlertLevelInfo 普通通知，仅作记录
	AlertLevelInfo AlertLevel = iota
	// AlertLevelWarn 警告，需要关注但不紧急
	AlertLevelWarn
	// AlertLevelError 错误，需要及时处理
	AlertLevelError
	// AlertLevelCritical 严重故障，需要立即处理
	AlertLevelCritical
)

func (l AlertLevel) String() string {
	switch l {
	case AlertLevelInfo:
		return "INFO"
	case AlertLevelWarn:
		return "WARN"
	case AlertLevelError:
		return "ERROR"
	case AlertLevelCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// Alerter 告警渠道抽象
//
// 触发场景：
//   - 消息进入死信流
//   - dispatcher 投递超过 MaxAttempts 后放弃
//   - reaper 处理 pending 时遇到不可恢复错误
//
// 实现注意：
//   - 内部应做去重 / 频控（同一消息每分钟告警一次即可，避免风暴）
//   - 必须并发安全
type Alerter interface {
	Alert(ctx context.Context, level AlertLevel, title, content string)
}

// NopAlerter 不做任何动作的 Alerter，主要给测试用
type NopAlerter struct{}

func (NopAlerter) Alert(context.Context, AlertLevel, string, string) {}
