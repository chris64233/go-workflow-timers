package workflowtimers

import (
	"strconv"
	"time"
)

// TimerState 是定时器的持久化状态。
type TimerState string

const (
	// StateScheduled 已调度，等待到期领取（重排后的新版本也处于该状态）。
	StateScheduled TimerState = "scheduled"
	// StateClaimed 已被某个领取者持有有效租约，等待确认触发。
	StateClaimed TimerState = "claimed"
	// StateTriggered 触发已提交（结果与 outbox 已原子落盘），终态。
	StateTriggered TimerState = "triggered"
	// StateCancelled 定时器已取消，终态。
	StateCancelled TimerState = "cancelled"
)

// Timer 是一条定时器记录的当前快照。
type Timer struct {
	// TimerID 外部定时器号，由工作流侧分配，全生命周期稳定。
	TimerID string
	// Version 定时器版本，从 1 开始，每次重排加 1。
	Version int64
	State   TimerState

	// FireAtUnixMilli 当前版本的计划触发时刻（毫秒时间戳）。
	FireAtUnixMilli int64
	// Payload 工作流随定时器携带的不透明负载。
	Payload string

	// 领取租约（仅 State == StateClaimed 时有意义）。
	// LeaseToken 领取时生成的稳定幂等令牌；重排/取消/重新领取会更换它。
	LeaseToken string
	// LeaseExpiresUnixMilli 租约到期时刻；到期后原领取者的确认必然失败。
	LeaseExpiresUnixMilli int64

	CreatedUnixMilli   int64
	UpdatedUnixMilli   int64
	TriggeredUnixMilli int64
}

// DueTimer 是领取成功后返回给领取者的信息。
type DueTimer struct {
	TimerID string
	Version int64
	// FireAtUnixMilli 该版本最初排定的触发时刻。
	FireAtUnixMilli int64
	Payload         string
	// LeaseToken 确认触发时必须原样带回的租约令牌。
	LeaseToken string
	// LeaseExpiresUnixMilli 租约到期时刻。
	LeaseExpiresUnixMilli int64
}

// TriggerResult 是一次逻辑触发的持久化结果（每个定时器版本至多一条）。
type TriggerResult struct {
	TimerID string
	Version int64
	// RequestID 首次成功确认触发所用的请求号。
	RequestID string
	// ResultPayload 触发时保存的逻辑结果。
	ResultPayload string
	// OutboxKey 对应 outbox 记录的稳定幂等键。
	OutboxKey string
	// OutboxPayload 已写入 outbox 的投递负载。
	OutboxPayload     string
	OccurredUnixMilli int64
}

// PendingResult 是待执行（DuePending）查询返回的一条记录。
type PendingResult struct {
	TimerID         string
	Version         int64
	FireAtUnixMilli int64
	Payload         string
	// State 当前状态：scheduled（等待领取）或 claimed（已领取、租约仍有效）。
	State TimerState
}

// OutboxStatus 是 outbox 记录的投递状态。
type OutboxStatus string

const (
	// OutboxPending 待传输。
	OutboxPending OutboxStatus = "pending"
	// OutboxDelivered 传输层已确认投递。
	OutboxDelivered OutboxStatus = "delivered"
)

// OutboxEvent 是一条 outbox 记录。
type OutboxEvent struct {
	// Key 稳定幂等键，全局唯一："timer:<timerID>:v<version>"。
	Key string
	// TimerID / Version 对应被触发的定时器版本。
	TimerID            string
	Version            int64
	Payload            string
	Status             OutboxStatus
	CreatedUnixMilli   int64
	DeliveredUnixMilli int64
}

// nowMilli 返回当前时刻的毫秒时间戳。测试可通过 MemoryStore.now 替换时钟。
func nowMilli(now func() time.Time) int64 {
	if now == nil {
		now = time.Now
	}
	return now().UnixMilli()
}

// outboxKeyFor 生成某定时器版本的稳定 outbox 幂等键。
func outboxKeyFor(timerID string, version int64) string {
	return "timer:" + timerID + ":v" + strconv.FormatInt(version, 10)
}
