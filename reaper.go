package redisstream

import (
	"context"
	"time"
)

// runReaper 一个 stream 的 reaper 协程
//
// 周期巡检 PEL：
//   - 扫所有 pending（XPendingExt），筛 idle ≥ ClaimMinIdle 的"卡住"消息
//   - RetryCount 已达 MaxDeliver → 转死信流 + XACK 原消息 + 告警
//   - 否则 → XClaim 给同一 consumer，重置 idle 并 +1 RetryCount
//     worker 主循环下一轮读 "0" 时即可重新拿到
//
// 单实例假设：reaper 与 worker 同进程同 consumer 名
// 多实例需在 Client 实现侧加 SET NX 锁选主，或让多个 reaper 都跑（XClaim 会去重）
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

		// 没到死信阈值：XClaim 给同一 consumer，重置 idle，+1 RetryCount
		// worker 主循环下一轮读 "0" 时即可拿到
		//
		// 这里把 XCLAIM 的 minIdle 传 0，而不是 spec.ClaimMinIdle：
		// reaper 上面已经过滤过 p.Idle < spec.ClaimMinIdle 的；如果再传 spec.ClaimMinIdle
		// 进 XCLAIM，Redis 端会二次校验，存在 "worker 刚 ACK / 其它 reaper 刚抢" 的窄边界
		// 让 XCLAIM 静默不生效。传 0 表示"我已经判过了，直接抢"
		_, err := m.client.XClaim(ctx, streamKey, spec.Group, spec.Consumer, 0, []string{p.ID})
		if err != nil {
			m.logger.Errorf(ctx, "[redisstream] XClaim failed, stream=%s id=%s err=%v",
				spec.Stream, p.ID, err)
		}
	}
}
