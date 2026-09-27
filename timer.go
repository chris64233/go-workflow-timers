package workflowtimers

import (
	"errors"
	"fmt"
	"time"
)

// State 是定时器的生命周期状态。
type State string

const (
	// StatePending 等待触发（可能已被领取）。
	StatePending State = "PENDING"
	// StateFired 已提交触发，逻辑结果与 outbox 已原子落盘，不可撤销。
	StateFired State = "FIRED"
	// StateCanceled 已取消，未提交的触发全部作废。
	StateCanceled State = "CANCELED"
)

// 错误分类：调用方可用 errors.Is 区分版本、租约、状态与幂等冲突。
var (
	// ErrTimerNotFound 定时器不存在。
	ErrTimerNotFound = errors.New("timer not found")
	// ErrTimerExists 同一工作流下外部定时器号已被占用（非同一请求号的重试）。
	ErrTimerExists = errors.New("timer already exists")
	// ErrRequestConflict 幂等冲突：请求号相同但内容不同。
	ErrRequestConflict = errors.New("request id reused with different payload")
	// ErrVersionConflict 版本冲突：定时器已被重排，旧版本触发失败。
	ErrVersionConflict = errors.New("timer version conflict")
	// ErrNotClaimed 定时器当前没有有效租约。
	ErrNotClaimed = errors.New("timer is not claimed")
	// ErrLeaseMismatch 租约不属于调用者（旧租约的迟到确认）。
	ErrLeaseMismatch = errors.New("lease mismatch")
	// ErrLeaseExpired 租约已过期限。
	ErrLeaseExpired = errors.New("lease expired")
	// ErrAlreadyFired 定时器已触发：已提交的触发不能撤销，也不能重排。
	ErrAlreadyFired = errors.New("timer already fired")
	// ErrAlreadyCanceled 定时器已取消。
	ErrAlreadyCanceled = errors.New("timer already canceled")
)

// Timer 是持久化的定时器记录。
type Timer struct {
	WorkflowID string    `json:"workflow_id"`
	TimerID    string    `json:"timer_id"` // 外部定时器号，工作流内唯一
	Version    int64     `json:"version"`  // 每次重排递增
	State      State     `json:"state"`
	FireAt     time.Time `json:"fire_at"`
	Payload    []byte    `json:"payload,omitempty"`
	Result     []byte    `json:"result,omitempty"` // 触发确认时保存的逻辑结果
	Lease      *Lease    `json:"lease,omitempty"`  // 当前租约，未领取为 nil
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Lease 是有期限的领取租约。LeaseID 每次领取重新生成，
// 旧租约的迟到确认因 LeaseID 不匹配而被拒绝，无法影响接管者。
type Lease struct {
	LeaseID   string    `json:"lease_id"`
	Owner     string    `json:"owner"`
	Token     int64     `json:"token"` // 围栏令牌，单调递增
	ExpiresAt time.Time `json:"expires_at"`
}

// OutboxEntry 是触发时原子写入的 outbox 记录。
// IdempotencyKey 稳定（workflow/timer/version），传输层重复投递不会产生第二次逻辑触发。
type OutboxEntry struct {
	IdempotencyKey string    `json:"idempotency_key"`
	WorkflowID     string    `json:"workflow_id"`
	TimerID        string    `json:"timer_id"`
	Version        int64     `json:"version"`
	Result         []byte    `json:"result,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// IdempotencyKey 返回某个定时器版本的稳定幂等键。
func IdempotencyKey(workflowID, timerID string, version int64) string {
	return fmt.Sprintf("%s/%s/%d", workflowID, timerID, version)
}

// requestRecord 记录已处理的请求号，用于幂等重放与冲突检测。
type requestRecord struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
	Hash      string `json:"hash"`    // 请求内容指纹
	Version   int64  `json:"version"` // 请求完成时的定时器版本，用于重放原结果
}

// snapshot 是持久化的全部状态。
type snapshot struct {
	Timers   map[string]*Timer         `json:"timers"`
	Requests map[string]*requestRecord `json:"requests"`
	Outbox   map[string]*OutboxEntry   `json:"outbox"`
	LeaseSeq int64                     `json:"lease_seq"`
}

func newSnapshot() *snapshot {
	return &snapshot{
		Timers:   make(map[string]*Timer),
		Requests: make(map[string]*requestRecord),
		Outbox:   make(map[string]*OutboxEntry),
	}
}

func timerKey(workflowID, timerID string) string     { return workflowID + "\x1f" + timerID }
func requestKey(workflowID, requestID string) string { return workflowID + "\x1f" + requestID }
