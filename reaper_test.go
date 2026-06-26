package redisstream

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakePending 一条 pending 消息在 fakeClient 里的状态
type fakePending struct {
	consumer     string
	idle         time.Duration
	deliverCount int64
}

// fakeClient 是只为 reaper / 死信行为测试服务的内存版 Client。
// 它把"主流内容"和"PEL 状态"分开存：XClaim / XRangeN 取主流消息体，
// XPendingExt 反映 PEL，XAck 从 PEL 移除，XAdd 收集死信流。
type fakeClient struct {
	mu     sync.Mutex
	stream map[string]map[string]interface{} // id -> values（主流内容）
	pel    map[string]*fakePending           // id -> pending 状态
	order  []string                          // 维持 pending 的稳定顺序
	dead   []map[string]interface{}          // 死信流内容
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		stream: map[string]map[string]interface{}{},
		pel:    map[string]*fakePending{},
	}
}

// seed 放入一条已投递（pending）消息
func (f *fakeClient) seed(id string, values map[string]interface{}, consumer string, idle time.Duration, deliverCount int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stream[id] = values
	f.pel[id] = &fakePending{consumer: consumer, idle: idle, deliverCount: deliverCount}
	f.order = append(f.order, id)
}

// advanceIdle 模拟时间流逝：把所有仍 pending 的消息 idle 抬到 d，
// 用于驱动多轮 reaper（真实环境里 idle 随时间自然增长）
func (f *fakeClient) advanceIdle(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.pel {
		p.idle = d
	}
}

func (f *fakeClient) pendingLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pel)
}

func (f *fakeClient) deliverCount(id string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.pel[id]; ok {
		return p.deliverCount
	}
	return -1
}

func (f *fakeClient) deadLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dead)
}

// --- Client 接口实现 ---

func (f *fakeClient) XAdd(_ context.Context, stream string, _ int64, values map[string]interface{}) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.HasSuffix(stream, deadStreamSuffix) {
		f.dead = append(f.dead, values)
		return "dead-1", nil
	}
	return "new-1", nil
}

func (f *fakeClient) XReadGroupBlock(_ context.Context, _, _, _, _ string, _ int64, _ time.Duration) ([]Message, error) {
	return nil, nil
}

func (f *fakeClient) XAck(_ context.Context, _, _, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pel, id)
	return nil
}

func (f *fakeClient) XClaim(_ context.Context, _, _, consumer string, minIdle time.Duration, ids []string) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Message
	for _, id := range ids {
		p, ok := f.pel[id]
		if !ok {
			continue
		}
		// Redis 端二次校验：idle < minIdle 时静默落空（模拟被别的 reaper 抢过）
		if p.idle < minIdle {
			continue
		}
		p.deliverCount++
		p.idle = 0
		p.consumer = consumer
		out = append(out, Message{ID: id, Values: f.stream[id]})
	}
	return out, nil
}

func (f *fakeClient) XPendingExt(_ context.Context, _, _, _, _ string, count int64) ([]PendingInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []PendingInfo
	for _, id := range f.order {
		p, ok := f.pel[id]
		if !ok {
			continue
		}
		out = append(out, PendingInfo{ID: id, Consumer: p.consumer, Idle: p.idle, RetryCount: p.deliverCount})
		if int64(len(out)) >= count {
			break
		}
	}
	return out, nil
}

func (f *fakeClient) XRangeN(_ context.Context, _, start, _ string, _ int64) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.stream[start]
	if !ok {
		return nil, nil
	}
	return []Message{{ID: start, Values: v}}, nil
}

func (f *fakeClient) XGroupCreateMkStream(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeClient) IsBusyGroup(error) bool                                       { return false }

// countingAlerter 记录告警次数
type countingAlerter struct {
	mu        sync.Mutex
	count     int
	lastTitle string
}

func (a *countingAlerter) Alert(_ context.Context, _ AlertLevel, title, _ string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.count++
	a.lastTitle = title
}

func newSpec(handler Handler, maxDeliver int64, minIdle time.Duration) ConsumerSpec {
	spec := ConsumerSpec{
		Stream:       "orders",
		Group:        "g",
		Consumer:     "worker-0",
		Handler:      handler,
		MaxDeliver:   maxDeliver,
		ClaimMinIdle: minIdle,
	}
	applyConsumerDefaults(&spec)
	return spec
}

// 核心回归：持续失败的毒消息必须经 reaper 多轮重投后进入死信并告警一次。
// 这正是旧实现（worker 自旋读 PEL 重置 idle）永远走不到的路径。
func TestReaper_PoisonMessageEscalatesToDeadLetter(t *testing.T) {
	fake := newFakeClient()
	fake.seed("1-0", map[string]interface{}{"payload": "x"}, "worker-0", 60*time.Second, 1)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	var calls int32
	spec := newSpec(func(_ context.Context, _ Message) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("boom")
	}, 3, 60*time.Second)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		m.reapOnce(ctx, spec)
		fake.advanceIdle(60 * time.Second) // 模拟下一个 ClaimMinIdle 窗口到来
	}

	if got := fake.deadLen(); got != 1 {
		t.Fatalf("死信流应有 1 条，实际 %d", got)
	}
	if al.count != 1 {
		t.Fatalf("死信告警应触发 1 次，实际 %d", al.count)
	}
	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("进死信后 PEL 应清空，实际仍有 %d", got)
	}
	// deliverCount 1→2→3，第三轮判 >=MaxDeliver 转死信不再调 handler，故 handler 被调 2 次
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("handler 应被重投调用 2 次，实际 %d", got)
	}
}

// 崩溃恢复：PEL 中无人处理、idle 已超时的老消息应被 reaper claim 回并成功处理后 XAck。
func TestReaper_RecoversCrashedPending(t *testing.T) {
	fake := newFakeClient()
	fake.seed("2-0", map[string]interface{}{"payload": "y"}, "dead-consumer", 90*time.Second, 1)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	var calls int32
	spec := newSpec(func(_ context.Context, _ Message) error {
		atomic.AddInt32(&calls, 1)
		return nil // 处理成功
	}, 5, 60*time.Second)
	spec.Consumer = "worker-1" // 新进程的 consumer 名

	m.reapOnce(context.Background(), spec)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("handler 应被调用 1 次，实际 %d", got)
	}
	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("成功处理后应 XAck 出 PEL，实际仍有 %d", got)
	}
	if got := fake.deadLen(); got != 0 {
		t.Fatalf("不应进死信，实际 %d", got)
	}
	if al.count != 0 {
		t.Fatalf("不应告警，实际 %d", al.count)
	}
}

// 未超时跳过：idle < ClaimMinIdle 的 pending 不应被 claim / 重投。
func TestReaper_SkipsNotYetIdle(t *testing.T) {
	fake := newFakeClient()
	fake.seed("3-0", map[string]interface{}{"payload": "z"}, "worker-0", 10*time.Second, 1)

	m := NewManager(fake, nil, &countingAlerter{}, "")

	var calls int32
	spec := newSpec(func(_ context.Context, _ Message) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}, 5, 60*time.Second)

	m.reapOnce(context.Background(), spec)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("未超时不应调用 handler，实际 %d", got)
	}
	if got := fake.deliverCount("3-0"); got != 1 {
		t.Fatalf("未超时不应 XClaim，deliverCount 应仍为 1，实际 %d", got)
	}
	if got := fake.pendingLen(); got != 1 {
		t.Fatalf("消息应仍 pending，实际 %d", got)
	}
}

// HandlerTimeout 回归：一个"挂死"但尊重 ctx 的 handler 在 reaper 中执行时，
// 应被 HandlerTimeout 打断而非永久阻塞 reaper 扫描循环，消息照常升级到死信。
// 这覆盖"reaper 同步执行 handler 可能被挂死 handler 冻结"的回归点。
func TestReaper_HungHandlerStillEscalates(t *testing.T) {
	fake := newFakeClient()
	fake.seed("4-0", map[string]interface{}{"payload": "w"}, "worker-0", 60*time.Second, 1)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	var calls int32
	spec := newSpec(func(ctx context.Context, _ Message) error {
		atomic.AddInt32(&calls, 1)
		<-ctx.Done() // 模拟挂死：只靠 ctx 超时解除
		return ctx.Err()
	}, 2, 60*time.Second)
	spec.HandlerTimeout = 50 * time.Millisecond // 远小于 ClaimMinIdle

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 4; i++ {
			m.reapOnce(ctx, spec)
			fake.advanceIdle(60 * time.Second)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reaper 被挂死 handler 冻结：reapOnce 未在限定时间内完成")
	}

	if got := fake.deadLen(); got != 1 {
		t.Fatalf("挂死 handler 也应最终升级到死信 1 条，实际 %d", got)
	}
	if al.count != 1 {
		t.Fatalf("死信告警应触发 1 次，实际 %d", al.count)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("MaxDeliver=2、seed count=1 时 handler 应被重投调用 1 次，实际 %d", got)
	}
}

// 默认值与夹取：HandlerTimeout 必须落在 ClaimMinIdle 之内，避免并发重投。
func TestApplyDefaults_HandlerTimeoutBoundedByClaimMinIdle(t *testing.T) {
	// 默认场景：零值 → DefaultHandlerTimeout，且 < DefaultClaimMinIdle
	def := ConsumerSpec{Stream: "s", Group: "g", Consumer: "c", Handler: func(context.Context, Message) error { return nil }}
	applyConsumerDefaults(&def)
	if def.HandlerTimeout != DefaultHandlerTimeout {
		t.Fatalf("默认 HandlerTimeout 应为 %s，实际 %s", DefaultHandlerTimeout, def.HandlerTimeout)
	}
	if def.HandlerTimeout >= def.ClaimMinIdle {
		t.Fatalf("默认 HandlerTimeout(%s) 必须 < ClaimMinIdle(%s)", def.HandlerTimeout, def.ClaimMinIdle)
	}

	// 越界场景：HandlerTimeout >= ClaimMinIdle 时应被夹到 ClaimMinIdle 之内
	over := ConsumerSpec{
		Stream: "s", Group: "g", Consumer: "c",
		Handler:        func(context.Context, Message) error { return nil },
		ClaimMinIdle:   10 * time.Second,
		HandlerTimeout: 30 * time.Second,
	}
	applyConsumerDefaults(&over)
	if over.HandlerTimeout >= over.ClaimMinIdle {
		t.Fatalf("越界 HandlerTimeout 应被夹到 < ClaimMinIdle，实际 HandlerTimeout=%s ClaimMinIdle=%s",
			over.HandlerTimeout, over.ClaimMinIdle)
	}
}
