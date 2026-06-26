package redisstream

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"
)

// deadStreamSuffix 死信流名后缀，拼在原 stream key 之后
const deadStreamSuffix = ":dead"

// moveToDeadLetter 把一条卡死的 pending 消息搬到死信流，并 XACK 原消息 + 告警
//
// 步骤：
//  1. XRangeN 单条只读取 payload，不修改 PEL / idle / deliver count
//     （早期实现用 XClaim 取 payload，但 XClaim 会重置 idle 为 0；
//     若后续 XAdd 死信失败，下一轮 reaper 因 idle < ClaimMinIdle 跳过，
//     导致告警风暴 + 延迟。改用 XRange 后失败可由下一轮自然重试）
//  2. XAdd 到 <stream>:dead，附带原 ID / consumer / retry / dead_at 等元信息
//     （设置了 spec.DeadLetterHandler 时，在此之后、XACK 之前调用它——并存语义）
//  3. XACK 原 stream
//  4. 告警
//
// 任一步失败都记日志并放弃本轮，等下一轮 reaper 重试，保持幂等
func (m *Manager) moveToDeadLetter(ctx context.Context, spec ConsumerSpec, p PendingInfo) {
	streamKey := m.keyPrefix + spec.Stream
	deadKey := streamKey + deadStreamSuffix

	// 1. XRangeN 单条读取 payload（只读，不动 PEL）
	msgs, err := m.client.XRangeN(ctx, streamKey, p.ID, p.ID, 1)
	if err != nil {
		m.logger.Errorf(ctx, "[redisstream] dead-letter XRangeN failed, stream=%s id=%s err=%v",
			spec.Stream, p.ID, err)
		return
	}

	if len(msgs) == 0 {
		// 消息体已被裁剪（PEL 残留 ID，payload 已不存在）
		// 把残留 ID 清掉避免反复触发，告警提醒
		m.logger.Warnf(ctx, "[redisstream] dead-letter payload trimmed, stream=%s id=%s",
			spec.Stream, p.ID)
		if e := m.client.XAck(ctx, streamKey, spec.Group, p.ID); e != nil {
			m.logger.Errorf(ctx, "[redisstream] dead-letter XAck (trimmed) failed, stream=%s id=%s err=%v",
				spec.Stream, p.ID, e)
			return
		}
		m.alertDeadLetter(ctx, spec, p, nil)
		return
	}
	msg := msgs[0]

	// 2. 搬到死信流
	deadValues := map[string]interface{}{
		"orig_id":     msg.ID,
		"orig_stream": spec.Stream,
		"group":       spec.Group,
		"consumer":    p.Consumer,
		"retry_count": p.RetryCount,
		"dead_at_ms":  time.Now().UnixNano() / int64(time.Millisecond),
		"payload":     marshalPayload(msg.Values),
	}
	deadID, err := m.client.XAdd(ctx, deadKey, DefaultMaxLen, deadValues)
	if err != nil {
		m.logger.Errorf(ctx, "[redisstream] dead-letter XAdd failed, stream=%s id=%s err=%v",
			spec.Stream, p.ID, err)
		// 没改 idle，下一轮 reaper 看到 idle 仍超阈值会自然重试
		return
	}

	// 2.5 死信回调（可选，best-effort 即时反应钩子）：消息已持久落在死信流，
	// 故回调无论成败都不阻塞后续 XAck——失败只记日志 + 一条回调失败告警，
	// 不做库级重试（否则毒消息会每轮重写死信流、冲刷 <stream>:dead 的 MAXLEN）。
	// 要可靠处理死信请消费 <stream>:dead。
	if spec.DeadLetterHandler != nil {
		info := DeadLetterInfo{
			Stream:     spec.Stream,
			Group:      spec.Group,
			Consumer:   p.Consumer,
			OrigID:     msg.ID,
			RetryCount: p.RetryCount,
			Idle:       p.Idle,
			DeadID:     deadID,
		}
		if hErr := m.invokeDeadLetterHandler(ctx, spec, msg, info); hErr != nil {
			m.logger.Errorf(ctx, "[redisstream] dead-letter handler failed, stream=%s orig_id=%s err=%v",
				spec.Stream, msg.ID, hErr)
			m.alerter.Alert(ctx, AlertLevelError, "Redis Stream 死信回调失败",
				fmt.Sprintf("stream: %s\norig_id: %s\ndead_id: %s\nerr: %v\n消息已落 <stream>:dead，请消费该死信流兜底处理",
					spec.Stream, msg.ID, deadID, hErr))
		}
	}

	// 3. ACK 原消息（无论回调成败：消息已持久落死信流，不会丢）
	if err := m.client.XAck(ctx, streamKey, spec.Group, msg.ID); err != nil {
		m.logger.Errorf(ctx, "[redisstream] dead-letter XAck failed, stream=%s id=%s err=%v",
			spec.Stream, msg.ID, err)
		// 已写进死信流但 ACK 失败，下一轮会再写一份，业务侧按 orig_id 去重
		return
	}

	m.logger.Warnf(ctx, "[redisstream] message moved to dead letter, stream=%s orig_id=%s dead_id=%s retry=%d",
		spec.Stream, msg.ID, deadID, p.RetryCount)

	// 4. 告警
	m.alertDeadLetter(ctx, spec, p, msg.Values)
}

// alertDeadLetter 发死信告警
func (m *Manager) alertDeadLetter(ctx context.Context, spec ConsumerSpec, p PendingInfo, values map[string]interface{}) {
	payloadStr := "<payload missing>"
	if values != nil {
		payloadStr = marshalPayload(values)
	}
	content := fmt.Sprintf(
		"stream: %s\ngroup: %s\norig_id: %s\nconsumer: %s\nretry_count: %d\nidle: %s\npayload: %s",
		spec.Stream, spec.Group, p.ID, p.Consumer, p.RetryCount, p.Idle, payloadStr,
	)
	m.alerter.Alert(ctx, AlertLevelError, "Redis Stream 消息进入死信", content)
}

// invokeDeadLetterHandler 在 HandlerTimeout 子 ctx 内、带 panic 兜底地调用死信回调
//
// 与 handleOne 同样的保护：挂死（尊重 ctx）或 panic 的回调不会冻结 reaper 扫描循环
func (m *Manager) invokeDeadLetterHandler(ctx context.Context, spec ConsumerSpec, msg Message, info DeadLetterInfo) (err error) {
	hctx := ctx
	if spec.HandlerTimeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(ctx, spec.HandlerTimeout)
		defer cancel()
	}

	defer func() {
		if r := recover(); r != nil {
			m.logger.Errorf(ctx,
				"[redisstream] dead-letter handler panic, stream=%s id=%s err=%v stack=%s",
				spec.Stream, msg.ID, r, debug.Stack(),
			)
			err = &handlerPanicError{value: r}
		}
	}()

	return spec.DeadLetterHandler(hctx, msg, info)
}

// marshalPayload 把 Values 序列化为 JSON 字符串，方便告警 / 死信流查看
// 失败时降级为 fmt.Sprint
func marshalPayload(values map[string]interface{}) string {
	b, err := json.Marshal(values)
	if err != nil {
		return fmt.Sprintf("%+v", values)
	}
	return string(b)
}
