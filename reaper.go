package redisstream

import (
	"context"
	"time"
)

// runReaper 一个 stream 的 reaper 协程
//
// 周期巡检 PEL，且是失败 / 卡住消息重投的唯一驱动者：
//   - 扫所有 pending（XPendingExt），筛 idle ≥ ClaimMinIdle 的"卡住"消息
//   - RetryCount 已达 MaxDeliver → 转死信流 + XACK 原消息 + 告警
//   - 否则 → XClaim 抢回（+1 deliver count、重置 idle）并用返回的消息体直接重投
//
// 重投只走 XClaim 这一条路，deliver count 才会单调增长并最终触达死信阈值；
// 重试节奏由 ClaimMinIdle 控制，不会出现 worker 自旋读 PEL 的热重试。
//
// 注意：handler 会被 worker（新消息）与 reaper（重投）两个 goroutine 并发调用，
// 调用方实现必须幂等且并发安全。
//
// 单实例假设：reaper 与 worker 同进程同 consumer 名
// 多实例可让多个 reaper 都跑：XClaim 传 minIdle=ClaimMinIdle，被别的 reaper
// 抢过、idle 已重置的消息会在本次 XClaim 静默落空，从而天然去重
func (m *Manager) runReaper(ctx context.Context, spec ConsumerSpec) {
	ticker := time.NewTicker(spec.ReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Infof(ctx, "[redisstream] reaper exit, stream=%s group=%s", spec.Stream, spec.Group)
			return
		case <-ticker.C:
			m.reapOnce(ctx, spec)
		}
	}
}

// reapOnce 单轮巡检
//
// 用 XPendingExt 扫整个 group 的 pending（不是仅当前 consumer 的）
// 这样即使 consumer 名变了也能兜底
func (m *Manager) reapOnce(ctx context.Context, spec ConsumerSpec) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Errorf(ctx, "[redisstream] reaper panic, stream=%s err=%v", spec.Stream, r)
		}
	}()

	streamKey := m.keyPrefix + spec.Stream

	pending, err := m.client.XPendingExt(ctx, streamKey, spec.Group, "-", "+", reaperBatchSize)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		m.logger.Errorf(ctx, "[redisstream] XPendingExt failed, stream=%s err=%v", spec.Stream, err)
		return
	}

	for _, p := range pending {
		// 只处理 idle 超时的；未超时的等下一轮
		if p.Idle < spec.ClaimMinIdle {
			continue
		}

		if p.RetryCount >= spec.MaxDeliver {
			m.moveToDeadLetter(ctx, spec, p)
			continue
		}

		// 没到死信阈值：XClaim 抢到当前 consumer 名下（+1 deliver count、重置 idle），
		// 并拿到返回的消息体直接重投。重投只走这一条路，deliver count 才会单调增长，
		// 最终触达 MaxDeliver 转死信；重试节奏天然由 ClaimMinIdle 控制（不再热重试）。
		//
		// minIdle 传 spec.ClaimMinIdle（而非 0）：worker 已不再自行抢 PEL，唯一竞争是
		// 多 reaper 实例。让 Redis 用 minIdle 做最终校验，刚被别的 reaper 抢过、idle 被
		// 重置的消息会在本次 XClaim 静默落空，从而天然去重，避免同一条被多实例重复重投。
		claimed, err := m.client.XClaim(ctx, streamKey, spec.Group, spec.Consumer, spec.ClaimMinIdle, []string{p.ID})
		if err != nil {
			// ctx 取消属正常关停路径，不报错刷日志
			if ctx.Err() != nil {
				return
			}
			m.logger.Errorf(ctx, "[redisstream] XClaim failed, stream=%s id=%s err=%v",
				spec.Stream, p.ID, err)
			continue
		}

		// claimed 可能为空：被别的 reaper 抢先（idle 已被重置，minIdle 校验未过）
		// 或消息已被 ACK。重投失败不 ack，留 PEL 等下一轮，保持幂等
		for _, msg := range claimed {
			m.handleOne(ctx, spec, streamKey, msg)
		}
	}
}
