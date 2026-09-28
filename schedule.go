package workflowtimers

import (
	"errors"
	"fmt"
	"time"
)

// MissedPolicy 是计划错过执行时间点时的补触发策略，冻结在计划版本上，
// 更新计划不会改变旧版本已经使用的策略。
type MissedPolicy string

const (
	// MissedCatchUpAll 全部补齐：停机期间错过的计划时间点全部补生成实例。
	MissedCatchUpAll MissedPolicy = "CATCH_UP_ALL"
	// MissedLatestOnly 只补最近一次：只补生成错过区间内最后一个时间点，
	// 其余时间点落为 SKIPPED 记录。
	MissedLatestOnly MissedPolicy = "LATEST_ONLY"
	// MissedSkip 直接跳过：错过的时间点全部落为 SKIPPED 记录，不触发。
	MissedSkip MissedPolicy = "SKIP"
)

// ScheduleState 是周期计划的生命周期状态。
type ScheduleState string

const (
	// ScheduleActive 计划生效中，扫描会生成实例。
	ScheduleActive ScheduleState = "ACTIVE"
	// SchedulePaused 计划暂停：不再生成新实例，已生成的实例不受影响。
	SchedulePaused ScheduleState = "PAUSED"
	// ScheduleCompleted 计划已越过结束时间，扫描不再生成实例。
	ScheduleCompleted ScheduleState = "COMPLETED"
)

// InstanceStatus 是具体触发实例的状态。
type InstanceStatus string

const (
	// InstancePending 等待触发（可能已被领取）。
	InstancePending InstanceStatus = "PENDING"
	// InstanceFailed 上次触发失败，租约已释放，可被重新领取重试。
	InstanceFailed InstanceStatus = "FAILED"
	// InstanceFired 已提交触发，结果与 outbox 已原子落盘，不可撤销。
	InstanceFired InstanceStatus = "FIRED"
	// InstanceCanceled 被计划更新作废（旧版本的未来实例），不再触发，记录保留。
	InstanceCanceled InstanceStatus = "CANCELED"
	// InstanceSkipped 按补触发策略跳过（LATEST_ONLY/SKIP），不触发，记录保留。
	InstanceSkipped InstanceStatus = "SKIPPED"
)

// 计划与实例相关的错误分类。
var (
	// ErrScheduleNotFound 计划不存在。
	ErrScheduleNotFound = errors.New("schedule not found")
	// ErrScheduleExists 同一工作流下计划号已被占用。
	ErrScheduleExists = errors.New("schedule already exists")
	// ErrScheduleCompleted 计划已完成（越过结束时间），不能再变更。
	ErrScheduleCompleted = errors.New("schedule already completed")
	// ErrScheduleAlreadyPaused 计划已暂停，不能重复暂停。
	ErrScheduleAlreadyPaused = errors.New("schedule already paused")
	// ErrScheduleNotPaused 计划未暂停，不能恢复。
	ErrScheduleNotPaused = errors.New("schedule not paused")
	// ErrInvalidSchedule 计划定义非法：时区、RRULE、起止时间或补触发策略有误。
	ErrInvalidSchedule = errors.New("invalid schedule spec")
	// ErrInstanceNotFound 触发实例不存在。
	ErrInstanceNotFound = errors.New("schedule instance not found")
	// ErrInstanceCanceled 实例已被计划更新作废。
	ErrInstanceCanceled = errors.New("schedule instance canceled")
	// ErrInstanceSkipped 实例按补触发策略被跳过，永不触发。
	ErrInstanceSkipped = errors.New("schedule instance skipped")
)

// ScheduleSpec 是一个计划版本的不可变定义。
type ScheduleSpec struct {
	// Timezone IANA 时区名（如 Asia/Shanghai），空值按 UTC 处理。
	Timezone string `json:"timezone"`
	// StartAt 计划起始时间（即 RRULE 的 DTSTART，含该时间点）。
	StartAt time.Time `json:"start_at"`
	// EndAt 计划结束时间，零值表示不限。越过该时间后计划转为 COMPLETED。
	EndAt time.Time `json:"end_at,omitempty"`
	// RRULE RFC5545 递归规则子集，如 FREQ=DAILY;INTERVAL=1;BYDAY=MO,WE,FR。
	RRULE string `json:"rrule"`
	// Payload 触发载荷，冻结在版本上：更新后新生成的实例才使用新载荷。
	Payload []byte `json:"payload,omitempty"`
	// MissedPolicy 错过执行时的补触发策略。
	MissedPolicy MissedPolicy `json:"missed_policy"`
}

// ScheduleVersion 是计划的不可变版本快照。更新计划递增版本并追加新快照，
// 旧版本与它已经生成的触发实例历史始终保留。
type ScheduleVersion struct {
	Version       int64        `json:"version"`
	Spec          ScheduleSpec `json:"spec"`
	EffectiveFrom time.Time    `json:"effective_from"` // 该版本开始生成实例的生效下界
	CreatedAt     time.Time    `json:"created_at"`
}

// Schedule 是周期计划记录。
type Schedule struct {
	WorkflowID     string `json:"workflow_id"`
	ScheduleID     string `json:"schedule_id"`
	State          ScheduleState
	CurrentVersion int64 `json:"current_version"`
	// EffectiveFrom 是当前版本生成实例的时间下界：创建时为创建时刻，
	// 更新/恢复时推进到操作时刻。早于该下界的计划时间点不会生成实例。
	EffectiveFrom time.Time `json:"effective_from"`
	// LastScanAt 是最近一次扫描该计划的时刻（停机判定与观测用）。
	LastScanAt time.Time                  `json:"last_scan_at"`
	Versions   map[int64]*ScheduleVersion `json:"versions"`
	CreatedAt  time.Time                  `json:"created_at"`
	UpdatedAt  time.Time                  `json:"updated_at"`
}

// Instance 是计划在某个具体时间点上物化出的触发实例。
// 每个（计划版本, 版本内序号）对应唯一一个实例和一个稳定幂等键，
// 多次扫描不会重复生成。
type Instance struct {
	WorkflowID string `json:"workflow_id"`
	ScheduleID string `json:"schedule_id"`
	InstanceID string `json:"instance_id"` // 稳定派生：scheduleID-v{version}-{seq}
	Version    int64  `json:"version"`     // 由哪个计划版本生成
	Seq        int64  `json:"seq"`         // 版本内时间点序号，从 0 起

	Status InstanceStatus `json:"status"`

	// 三个时间点可区分：计划时间、实际生成（物化）时间、最终触发确认时间。
	ScheduledAt time.Time `json:"scheduled_at"`
	GeneratedAt time.Time `json:"generated_at"`
	FiredAt     time.Time `json:"fired_at,omitempty"`

	Payload   []byte `json:"payload,omitempty"`    // 生成时从版本快照拷贝
	Result    []byte `json:"result,omitempty"`     // 最终触发结果
	Attempts  int    `json:"attempts"`             // 失败重试次数
	LastError string `json:"last_error,omitempty"` // 最近一次失败原因

	Lease     *Lease    `json:"lease,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// InstanceIdempotencyKey 返回实例的稳定幂等键：
// workflowID/scheduleID/version/seq。传输层重复投递不会产生第二次逻辑触发。
func InstanceIdempotencyKey(workflowID, scheduleID string, version, seq int64) string {
	return fmt.Sprintf("%s/%s/%d/%d", workflowID, scheduleID, version, seq)
}

// scheduleInstanceID 由版本与序号派生稳定实例号。
func scheduleInstanceID(scheduleID string, version, seq int64) string {
	return fmt.Sprintf("%s-v%d-%d", scheduleID, version, seq)
}
