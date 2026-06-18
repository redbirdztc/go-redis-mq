package redisstream

import "context"

// Logger 日志接口，调用方注入到 Manager / Dispatcher
//
// 实现要点：
//   - 所有方法应当 fmt.Sprintf 格式化 args，与 log.Printf 一致
//   - 实现可从 ctx 中提取 trace_id / request_id 等并写入字段
//   - 实现必须并发安全（Manager 内多 goroutine 调用）
type Logger interface {
	Debugf(ctx context.Context, format string, args ...interface{})
	Infof(ctx context.Context, format string, args ...interface{})
	Warnf(ctx context.Context, format string, args ...interface{})
	Errorf(ctx context.Context, format string, args ...interface{})
}

// NopLogger 不做任何输出的 Logger，主要给测试用
type NopLogger struct{}

func (NopLogger) Debugf(context.Context, string, ...interface{}) {}
func (NopLogger) Infof(context.Context, string, ...interface{})  {}
func (NopLogger) Warnf(context.Context, string, ...interface{})  {}
func (NopLogger) Errorf(context.Context, string, ...interface{}) {}
