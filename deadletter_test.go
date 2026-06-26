package redisstream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 设置回调时：消息先写入死信流，再调用回调；回调成功后 XAck 原消息并告警一次。
func TestDeadLetter_HandlerCalledAfterStreamWrite(t *testing.T) {
	fake := newFakeClient()
	// deliverCount 已达 MaxDeliver，reapOnce 直接走 moveToDeadLetter
	fake.seed("1-0", map[string]interface{}{"payload": "x"}, "worker-0", 60*time.Second, 2)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	var calls int32
	var deadLenAtCall int
	var gotInfo DeadLetterInfo
	spec := newSpec(func(_ context.Context, _ Message) error { return errors.New("boom") }, 2, 60*time.Second)
	spec.DeadLetterHandler = func(_ context.Context, msg Message, info DeadLetterInfo) error {
		atomic.AddInt32(&calls, 1)
		deadLenAtCall = fake.deadLen() // 回调被调时死信流应已写入
		gotInfo = info
		return nil
	}

	m.reapOnce(context.Background(), spec)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("DeadLetterHandler 应被调用 1 次，实际 %d", got)
	}
	if deadLenAtCall != 1 {
		t.Fatalf("回调被调用时死信流应已写入（并存语义），实际 deadLen=%d", deadLenAtCall)
	}
	if got := fake.deadLen(); got != 1 {
		t.Fatalf("死信流应有 1 条，实际 %d", got)
	}
	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("回调成功后应 XAck 出 PEL，实际仍有 %d", got)
	}
	if al.count != 1 {
		t.Fatalf("死信告警应触发 1 次，实际 %d", al.count)
	}
	if gotInfo.Stream != "orders" || gotInfo.Group != "g" || gotInfo.OrigID != "1-0" ||
		gotInfo.RetryCount != 2 || gotInfo.DeadID == "" {
		t.Fatalf("DeadLetterInfo 字段不对：%+v", gotInfo)
	}
}

// 回调失败也不丢、不卡：消息已落死信流，库照常 XAck 出 PEL，并多发一条回调失败告警。
func TestDeadLetter_HandlerErrorStillCompletes(t *testing.T) {
	fake := newFakeClient()
	fake.seed("2-0", map[string]interface{}{"payload": "y"}, "worker-0", 60*time.Second, 2)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	var calls int32
	spec := newSpec(func(_ context.Context, _ Message) error { return errors.New("boom") }, 2, 60*time.Second)
	spec.DeadLetterHandler = func(_ context.Context, _ Message, _ DeadLetterInfo) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("sink down")
	}

	m.reapOnce(context.Background(), spec)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("回调应被调用 1 次，实际 %d", got)
	}
	if got := fake.deadLen(); got != 1 {
		t.Fatalf("死信流应有 1 条（持久兜底），实际 %d", got)
	}
	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("回调失败也应 XAck 出 PEL（不丢不卡），实际仍有 %d", got)
	}
	// 死信告警 + 回调失败告警 = 2 次
	if al.count != 2 {
		t.Fatalf("应有死信告警 + 回调失败告警共 2 次，实际 %d", al.count)
	}
}

// 回调失败不会无限重写死信流：连续两轮各只写一份、各 XAck，不堆积。
func TestDeadLetter_HandlerErrorDoesNotLoop(t *testing.T) {
	fake := newFakeClient()
	fake.seed("a-0", map[string]interface{}{"payload": "1"}, "worker-0", 60*time.Second, 2)
	fake.seed("b-0", map[string]interface{}{"payload": "2"}, "worker-0", 60*time.Second, 2)

	m := NewManager(fake, nil, &countingAlerter{}, "")
	spec := newSpec(func(_ context.Context, _ Message) error { return errors.New("boom") }, 2, 60*time.Second)
	spec.DeadLetterHandler = func(_ context.Context, _ Message, _ DeadLetterInfo) error {
		return errors.New("sink down")
	}

	m.reapOnce(context.Background(), spec)
	// 两条消息各写一份死信、各 XAck；不会因回调失败而残留再重写
	if got := fake.deadLen(); got != 2 {
		t.Fatalf("两条死信各写一份，应为 2，实际 %d", got)
	}
	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("均应 XAck 出 PEL，实际仍有 %d", got)
	}

	// 再跑一轮：PEL 已空，不应再产生任何死信写入（验证无重写死信流的循环）
	m.reapOnce(context.Background(), spec)
	if got := fake.deadLen(); got != 2 {
		t.Fatalf("PEL 已空，死信流不应再增长，仍应为 2，实际 %d", got)
	}
}

// 回调 panic 不冻结 reaper、不丢消息；按失败处理（XAck + 告警）。
func TestDeadLetter_HandlerPanicDoesNotFreezeOrLose(t *testing.T) {
	fake := newFakeClient()
	fake.seed("3-0", map[string]interface{}{"payload": "z"}, "worker-0", 60*time.Second, 2)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	spec := newSpec(func(_ context.Context, _ Message) error { return errors.New("boom") }, 2, 60*time.Second)
	spec.DeadLetterHandler = func(_ context.Context, _ Message, _ DeadLetterInfo) error {
		panic("dead-letter sink panic")
	}

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		m.reapOnce(ctx, spec) // panic 应被兜底，不向上扩散
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reaper 被 panic 的死信回调冻结")
	}

	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("panic 按失败处理也应 XAck，PEL 应为 0，实际 %d", got)
	}
	if got := fake.deadLen(); got != 1 {
		t.Fatalf("死信流应有 1 条，实际 %d", got)
	}
	if al.count != 2 {
		t.Fatalf("应有死信告警 + 回调失败告警共 2 次，实际 %d", al.count)
	}
}

// 回调为 nil 时保持现状：写死信流 + XAck + 告警（仅 1 次）。
func TestDeadLetter_NilHandlerKeepsCurrentBehavior(t *testing.T) {
	fake := newFakeClient()
	fake.seed("4-0", map[string]interface{}{"payload": "w"}, "worker-0", 60*time.Second, 2)

	al := &countingAlerter{}
	m := NewManager(fake, nil, al, "")

	spec := newSpec(func(_ context.Context, _ Message) error { return errors.New("boom") }, 2, 60*time.Second)
	// 不设置 DeadLetterHandler

	m.reapOnce(context.Background(), spec)

	if got := fake.deadLen(); got != 1 {
		t.Fatalf("死信流应有 1 条，实际 %d", got)
	}
	if got := fake.pendingLen(); got != 0 {
		t.Fatalf("应 XAck 出 PEL，实际仍有 %d", got)
	}
	if al.count != 1 {
		t.Fatalf("应告警 1 次，实际 %d", al.count)
	}
}
