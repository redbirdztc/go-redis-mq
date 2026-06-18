package redisstream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"git.geebento.com/go/redisstream"
)

// 以下示例展示三种典型用法。所有 stub 实现都是空壳，仅为了让示例代码可以编译。
// 真实使用时请把 stub 替换成基于具体技术栈的适配：
//   - Client：包装 go-redis / redigo / golib redis
//   - Logger：包装 zap / qlog
//   - Alerter：包装 Lark / 钉钉 / Slack webhook
//   - OutboxStore：基于业务侧 ORM 实现 outbox 表的 CRUD

// stubClient 仅用于示例编译，所有方法返回零值
type stubClient struct{}

func (stubClient) XAdd(context.Context, string, int64, map[string]interface{}) (string, error) {
	return "", nil
}
func (stubClient) XReadGroupBlock(context.Context, string, string, string, string, int64, time.Duration) ([]redisstream.Message, error) {
	return nil, nil
}
func (stubClient) XReadGroupNoBlock(context.Context, string, string, string, string, int64) ([]redisstream.Message, error) {
	return nil, nil
}
func (stubClient) XAck(context.Context, string, string, string) error { return nil }
func (stubClient) XClaim(context.Context, string, string, string, time.Duration, []string) ([]redisstream.Message, error) {
	return nil, nil
}
func (stubClient) XPendingExt(context.Context, string, string, string, string, int64) ([]redisstream.PendingInfo, error) {
	return nil, nil
}
func (stubClient) XRangeN(context.Context, string, string, string, int64) ([]redisstream.Message, error) {
	return nil, nil
}
func (stubClient) XGroupCreateMkStream(context.Context, string, string, string) error { return nil }
func (stubClient) IsBusyGroup(error) bool                                             { return false }

// stubStore outbox 表 stub
type stubStore struct{}

func (stubStore) FetchPending(context.Context, int) ([]redisstream.OutboxRecord, error) {
	return nil, nil
}
func (stubStore) MarkPublished(context.Context, int64, string) error { return nil }

// ExampleManager_Publish 演示直接投递（无事务一致性需求时）
func ExampleManager_Publish() {
	m := redisstream.NewManager(stubClient{}, redisstream.NopLogger{}, redisstream.NopAlerter{}, "smartCooker:stream:")

	// 业务事件触发时直接 Publish
	_, _ = m.Publish(context.Background(), "order_pay_success", map[string]interface{}{
		"order_no": "ORD20260617001",
		"user_id":  12345,
	})

	fmt.Println("published")
	// Output: published
}

// ExampleManager_Run 演示注册消费者并启动
func ExampleManager_Run() {
	m := redisstream.NewManager(stubClient{}, redisstream.NopLogger{}, redisstream.NopAlerter{}, "smartCooker:stream:")

	_ = m.Register(redisstream.ConsumerSpec{
		Stream:   "order_pay_success",
		Group:    "order-workers",
		Consumer: "worker-0",
		Handler: func(ctx context.Context, msg redisstream.Message) error {
			// 业务处理。返回 nil → 自动 XAck；返回 error → 留 PEL 等 reaper 重试
			fmt.Printf("handle id=%s\n", msg.ID)
			return nil
		},
		// 其它字段留空走默认值
	})

	// 实际场景里这里阻塞到 ctx 取消
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = m.Run(ctx)

	fmt.Println("done")
	// Output: done
}

// ExampleDispatcher_Run 演示本地消息表派发
//
// 关键点：业务侧自己在 tx 里 INSERT outbox 行；Dispatcher 周期取出投递
func ExampleDispatcher_Run() {
	// 业务侧伪代码（不在本库职责内）：
	//
	//   tx := db.Begin()
	//   tx.Exec("INSERT INTO `order` (...) VALUES (...)")
	//   payload, _ := json.Marshal(map[string]interface{}{"order_no": "ORD..."})
	//   tx.Exec("INSERT INTO outbox(stream, payload, state) VALUES (?, ?, 0)",
	//       "order_pay_success", payload)
	//   tx.Commit()
	//
	// 这两条 INSERT 必须在同一个 tx 里，否则失去原子性

	d, err := redisstream.NewDispatcher(redisstream.DispatcherConfig{
		Client:       stubClient{},
		Store:        stubStore{},
		Logger:       redisstream.NopLogger{},
		Alerter:      redisstream.NopAlerter{},
		KeyPrefix:    "smartCooker:stream:",
		Interval:     2 * time.Second,
		BatchSize:    100,
		MaxLenApprox: redisstream.DefaultMaxLen,
	})
	if err != nil {
		fmt.Println("init failed:", err)
		return
	}

	// 实际场景里这里阻塞到 ctx 取消
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = d.Run(ctx)

	// 演示 OutboxRecord.Values 的常见结构：把 outbox.id 放进去做幂等键
	_ = json.RawMessage(`{"outbox_local_id": 42, "order_no": "ORD20260617001"}`)

	fmt.Println("done")
	// Output: done
}
