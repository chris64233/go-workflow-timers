package workflowtimers

import "context"

// CreateTimerParams 创建定时器的入参。
type CreateTimerParams struct {
	// TimerID 外部定时器号，由工作流分配。
	TimerID string
	// RequestID 请求号；同号重试在内容一致时返回首次结果。
	RequestID string
	// FireAtUnixMilli 计划触发时刻（毫秒时间戳）。
	FireAtUnixMilli int64
	Payload         string
}

// RescheduleParams 重排定时器的入参。重排会使版本号加 1，
// 并令该定时器上任何尚未确认的领取租约立即失效。
type RescheduleParams struct {
	TimerID         string
	RequestID       string
	FireAtUnixMilli int64
	Payload         string
}

// CancelParams 取消定时器的入参。取消对已触发的定时器无效。
type CancelParams struct {
	TimerID   string
	RequestID string
}

// ClaimParams 调度器批量领取到期定时器的入参。
type ClaimParams struct {
	// MaxBatch 本次最多领取多少条；<=0 时使用默认值。
	MaxBatch int
	// LeaseDurationMillis 租约期限；租约到期后原领取者的确认必然失败，
	// 该定时器可被其他调度器重新领取。
	LeaseDurationMillis int64
	// WorkerID 领取者标识，仅用于记录/排障，不参与 fencing 判定
	// （fencing 令牌是返回的 LeaseToken）。
	WorkerID string
}

// ConfirmTriggerParams 确认触发的入参。
type ConfirmTriggerParams struct {
	TimerID string
	// Version 领取时拿到的定时器版本；不是当前版本则确认失败。
	Version int64
	// LeaseToken 领取时拿到的租约令牌；与当前租约不一致则确认失败。
	LeaseToken string
	// RequestID 可选的请求号，供传输层/调度器重试时幂等返回原结果。
	RequestID string
	// ResultPayload 原子保存的逻辑结果。
	ResultPayload string
	// OutboxPayload 与结果同事务写入 outbox 的投递负载；
	// 为空时使用 ResultPayload。
	OutboxPayload string
}

// PendingQuery 待执行查询入参。
type PendingQuery struct {
	// AtUnixMilli 判定“到期”的时刻；为 0 时使用存储时钟的当前时刻。
	AtUnixMilli int64
	// Limit 最多返回条数；<=0 表示不限制。
	Limit int
}

// Store 是持久化定时器存储的抽象。每个方法对应一条数据库事务：
// 方法内的条件判定与多行写入原子完成。调用方可以在任意时刻并发调用。
type Store interface {
	// CreateTimer 按外部定时器号创建定时器（版本 1，scheduled）。
	// 定时器号已存在 -> KindAlreadyExists；请求号冲突 -> KindIdempotentConflict。
	CreateTimer(ctx context.Context, p CreateTimerParams) (*Timer, error)

	// RescheduleTimer 重排定时器：版本加 1、回到 scheduled、旧租约失效。
	// 已触发/已取消 -> KindStateConflict；版本被更新的重排抢先提交后，
	// 旧租约的确认会得到 KindVersionConflict。
	RescheduleTimer(ctx context.Context, p RescheduleParams) (*Timer, error)

	// CancelTimer 取消定时器。已触发 -> KindStateConflict（触发不可撤销）；
	// 已取消按幂等成功返回。
	CancelTimer(ctx context.Context, p CancelParams) (*Timer, error)

	// ClaimDue 批量领取到期定时器：
	//   - state=scheduled 且 fire_at <= now；
	//   - state=claimed 但租约已过期（允许接管）。
	// 每条被领取的记录获得新的 LeaseToken（fencing 令牌）与新租约期限。
	ClaimDue(ctx context.Context, p ClaimParams) ([]DueTimer, error)

	// ConfirmTrigger 确认触发。仅当 版本==当前版本 且 状态==claimed 且
	// 租约令牌一致 且 租约未过期 时成功；成功即在同一事务内把定时器置为
	// triggered、写入触发结果与 outbox（稳定幂等键）。
	// 同版本的重复确认返回首次的 TriggerResult，逻辑触发只发生一次。
	ConfirmTrigger(ctx context.Context, p ConfirmTriggerParams) (*TriggerResult, error)

	// DuePending 查询“待执行”集合：已到期但尚未成功触发的版本，
	// 包含等待领取的 scheduled 记录与租约已过期、等待接管的 claimed 记录。
	DuePending(ctx context.Context, q PendingQuery) ([]PendingResult, error)

	// GetTimer 读取定时器当前快照；不存在 -> KindNotFound。
	GetTimer(ctx context.Context, timerID string) (*Timer, error)

	// GetTriggerResult 读取某定时器版本的触发结果；不存在 -> KindNotFound。
	GetTriggerResult(ctx context.Context, timerID string, version int64) (*TriggerResult, error)

	// ListOutboxPending 列出尚未确认投递的 outbox 事件（按创建顺序）。
	ListOutboxPending(ctx context.Context, limit int) ([]OutboxEvent, error)

	// MarkOutboxDelivered 把一条 outbox 事件标记为已投递；重复标记幂等成功。
	// 键不存在 -> KindNotFound。
	MarkOutboxDelivered(ctx context.Context, key string) (*OutboxEvent, error)
}
