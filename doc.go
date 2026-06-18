// Package redisstream 提供基于 Redis Streams 的消息队列通用能力，包括生产、消费、
// 死信处理、本地消息表（Transactional Outbox）派发。
//
// # 设计目标
//
// 零三方依赖：库本身不引入 Redis 客户端、日志库、告警渠道、ORM 等任何具体实现，
// 全部通过接口（Client / Logger / Alerter / OutboxStore）由调用方注入。这样：
//   - 不同业务可使用不同的 Redis 客户端（go-redis v8/v9、redigo、自研封装）
//   - 不同业务可对接不同的告警通道（Lark、钉钉、Slack、Email）
//   - 单元测试可注入 mock 实现
//
// # 三种使用形态
//
//  1. 直接生产（Manager.Publish）
//     适合不需要事务一致性的事件：日志、通知、缓存失效。
//
//  2. 消费（Manager.Register + Manager.Run）
//     注册 ConsumerSpec，库为每个 stream 启动 worker + reaper 协程。
//     worker 主循环先非阻塞读 PEL（重投），再阻塞读新消息；
//     reaper 周期扫超时 pending，根据 deliver count 决定 XClaim 重试或转死信。
//
//  3. 本地消息表（Dispatcher + OutboxStore）
//     适合需要"业务写库 + 投递消息"原子性的场景，例如下单后发奖、订单回调外部系统。
//     调用方在自己的事务里 INSERT 业务行 + INSERT outbox 行；Dispatcher 周期扫
//     outbox 表把消息搬到 Redis Stream。
//
// # 事务边界（重要）
//
// 本库不持有数据库连接、不管理事务。OutboxStore 的实现方必须保证：
//   - 业务表 INSERT 与 outbox 表 INSERT 使用同一个 *sql.Tx / *gorm.DB tx
//   - Dispatcher.FetchPending 在多实例部署时具备并发安全（SELECT ... FOR UPDATE
//     SKIP LOCKED 或乐观锁 UPDATE WHERE state=pending RETURNING ...）
//
// 失去任一保证都会破坏"消息至少投递一次"语义。
//
// # 语义保证
//
//   - Stream 内消息：至少一次投递（at-least-once），handler 必须幂等
//   - 死信：MaxDeliver 次仍处理失败的消息搬到 <stream>:dead 流并告警
//   - Outbox：业务 + outbox 一旦 commit，dispatcher 持续重试 XAdd 直到成功；
//     调用方决定何时放弃（在 FetchPending 的 SQL 里过滤掉超限的行即可）
//     消费方按 OutboxRecord.LocalID 做幂等
package redisstream
