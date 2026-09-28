package workflowtimers

import (
	"errors"
	"fmt"
	"time"
)

// ScheduleState 是周期计划的生命周期状态。
type ScheduleState string

const (
	// ScheduleActive 计划生效中，扫描会为当前版本生成实例。
	ScheduleActive ScheduleState = "ACTIVE"
	// SchedulePaused 计划已暂停：不再生成新实例，已生成（含已领取）的实例不受影响。
	SchedulePaused ScheduleState = "PAUSED"
	// ScheduleFinished 计划已越过结束时间，不再有未来实例。
	ScheduleFinished ScheduleState = "FINISHED"
)

// CatchUpPolicy 是服务停机期间错过计划时间点的补触发策略。
// 策略冻结在生成该批实例的计划版本上，事后更新策略不影响该版本已确定的算法。
type CatchUpPolicy string

const (
	// CatchUpAll 全部补齐：停机期间错过的每一个计划时间点各生成一个实例。
	CatchUpAll CatchUpPolicy = "CATCH_UP_ALL"
	// CatchUpLatest 只补最近一次：错过的时间点合并，只为最后一个时间点生成一个实例。
	CatchUpLatest CatchUpPolicy = "CATCH_UP_LATEST"
	// CatchUpSkip 直接跳过：错过的时间点一律不补，只生成恢复之后的时间点。
	CatchUpSkip CatchUpPolicy = "SKIP"
)

// RecurrenceKind 是周期规则类型。
type RecurrenceKind string

const (
	// RecurEvery 按固定间隔重复（按挂钟时间，遵守夏令时）。
	RecurEvery RecurrenceKind = "EVERY"
	// RecurDaily 每天在 Location 的指定时分重复。
	RecurDaily RecurrenceKind = "DAILY"
)

// Recurrence 描述周期规则。所有时间计算都在 Location 时区下进行。
type Recurrence struct {
	Kind RecurrenceKind `json:"kind"`
	// Every 为 RecurEvery 的固定间隔（必须 > 0）。
	Every time.Duration `json:"every,omitempty"`
	// Hour/Minute 为 RecurDaily 在 Location 下的时刻（0-23 / 0-59）。
	Hour   int `json:"hour,omitempty"`
	Minute int `json:"minute,omitempty"`
}

// InstanceState 是计划实例的生命周期状态。
type InstanceState string

const (
	// InstancePending 已生成、等待领取。
	InstancePending InstanceState = "PENDING"
	// InstanceFired 已确认触发，结果与 outbox 已原子落盘，不可撤销。
	InstanceFired InstanceState = "FIRED"
	// InstanceSuperseded 计划被更新或暂停，旧版本未完成的实例作废；
	// 已提交（FIRED）的实例永远保持 FIRED，触发历史保留。
	InstanceSuperseded InstanceState = "SUPERSEDED"
)

// 周期计划相关错误。
var (
	// ErrScheduleNotFound 计划不存在。
	ErrScheduleNotFound = errors.New("schedule not found")
	// ErrScheduleExists 同一工作流下外部计划号已被占用。
	ErrScheduleExists = errors.New("schedule already exists")
	// ErrScheduleNotActive 计划状态不允许该操作（已暂停/已结束）。
	ErrScheduleNotActive = errors.New("schedule not active")
	// ErrScheduleAlreadyPaused 计划已暂停。
	ErrScheduleAlreadyPaused = errors.New("schedule already paused")
	// ErrInstanceNotFound 计划实例不存在。
	ErrInstanceNotFound = errors.New("schedule instance not found")
	// ErrInstanceSuperseded 实例所属版本已被更新或暂停取代，未提交的触发作废。
	ErrInstanceSuperseded = errors.New("schedule instance superseded")
	// ErrInvalidSchedule 计划参数非法。
	ErrInvalidSchedule = errors.New("invalid schedule")
)

// Schedule 是周期计划头，始终指向当前生效版本。
type Schedule struct {
	WorkflowID     string        `json:"workflow_id"`
	ScheduleID     string        `json:"schedule_id"` // 外部计划号，工作流内唯一
	State          ScheduleState `json:"state"`
	CurrentVersion int64         `json:"current_version"` // 每次更新/恢复递增
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

// ScheduleVersion 是计划的一个冻结版本。规则与补触发策略在版本生成时固定，
// 后续更新只追加新版本、不修改历史版本；旧版本已生成的实例（触发历史）继续保留。
type ScheduleVersion struct {
	WorkflowID string `json:"workflow_id"`
	ScheduleID string `json:"schedule_id"`
	Version    int64  `json:"version"`

	// 冻结的计划内容。
	Location   string        `json:"location"`
	StartAt    time.Time     `json:"start_at"`
	EndAt      time.Time     `json:"end_at"` // 零值表示无结束时间
	Recurrence Recurrence    `json:"recurrence"`
	Policy     CatchUpPolicy `json:"policy"`
	Payload    []byte        `json:"payload,omitempty"`

	// ResumeAt 是该版本的生效起点：创建时为 StartAt，恢复时为恢复时刻
	// （对齐到下一个未错过的计划点）。扫描只生成 [ResumeAt, EndAt] 内的实例。
	ResumeAt time.Time `json:"resume_at"`

	// NextSeq 是下一个待生成实例的序号（从 1 开始）。
	NextSeq int64 `json:"next_seq"`
	// Cursor 是扫描推进位置：下一个候选计划时间点。多次扫描从游标继续，
	// 已生成的时间点不会被重复计算。
	Cursor time.Time `json:"cursor"`
	// Done 表示该版本的计划时间点已全部枚举（越过 EndAt）。
	Done bool `json:"done,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// Instance 是某个计划版本在一个具体计划时间点上的触发实例。
// 每个 (版本, 序号) 至多一个实例；实例的幂等键在生成时确定且永不变。
type Instance struct {
	WorkflowID string        `json:"workflow_id"`
	ScheduleID string        `json:"schedule_id"`
	Version    int64         `json:"version"` // 生成该实例的计划版本
	Seq        int64         `json:"seq"`     // 版本内单调序号
	State      InstanceState `json:"state"`

	// ScheduledAt 计划时间点（版本规则算出的理论时间）。
	ScheduledAt time.Time `json:"scheduled_at"`
	// GeneratedAt 实例实际被扫描生成的时间（停机恢复时明显晚于 ScheduledAt）。
	GeneratedAt time.Time `json:"generated_at"`
	// FiredAt 最终确认触发的时间；未触发为零值。
	FiredAt time.Time `json:"fired_at,omitempty"`

	Payload []byte `json:"payload,omitempty"`
	Result  []byte `json:"result,omitempty"` // 触发确认时保存的逻辑结果
	Lease   *Lease `json:"lease,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// InstanceIdempotencyKey 返回计划实例的稳定幂等键：
// workflow/schedule/vN/seq。计划更新产生新版本（新键空间），
// 同一版本内同一时间点重复扫描命中同一键，绝不生成第二个实例。
func InstanceIdempotencyKey(workflowID, scheduleID string, version, seq int64) string {
	return fmt.Sprintf("%s/%s/v%d/%d", workflowID, scheduleID, version, seq)
}

// firstScheduled 返回不早于 from 的第一个计划时间点；ok=false 表示不存在
// （from 晚于 EndAt，或规则无法再产生时间点）。
func (v *ScheduleVersion) firstScheduled(from time.Time, loc *time.Location) (t time.Time, ok bool) {
	switch v.Recurrence.Kind {
	case RecurEvery:
		if v.Recurrence.Every <= 0 {
			return time.Time{}, false
		}
		// 从 ResumeAt 起按间隔对齐：t = ResumeAt + k*Every。
		if from.Before(v.ResumeAt) {
			from = v.ResumeAt
		}
		elapsed := from.Sub(v.ResumeAt)
		k := int64(elapsed / v.Recurrence.Every)
		t = v.ResumeAt.Add(time.Duration(k) * v.Recurrence.Every)
		if t.Before(from) {
			t = t.Add(v.Recurrence.Every)
		}
	case RecurDaily:
		if from.Before(v.ResumeAt) {
			from = v.ResumeAt
		}
		local := from.In(loc)
		t = time.Date(local.Year(), local.Month(), local.Day(), v.Recurrence.Hour, v.Recurrence.Minute, 0, 0, loc)
		if t.Before(from) {
			t = t.AddDate(0, 0, 1)
		}
	default:
		return time.Time{}, false
	}
	if !v.EndAt.IsZero() && t.After(v.EndAt) {
		return time.Time{}, false
	}
	return t, true
}

// nextScheduled 返回 after 之后的下一个计划时间点；ok=false 表示已到尽头。
func (v *ScheduleVersion) nextScheduled(after time.Time, loc *time.Location) (time.Time, bool) {
	switch v.Recurrence.Kind {
	case RecurEvery:
		t := after.Add(v.Recurrence.Every)
		if !v.EndAt.IsZero() && t.After(v.EndAt) {
			return time.Time{}, false
		}
		return t, true
	case RecurDaily:
		t := after.AddDate(0, 0, 1)
		if !v.EndAt.IsZero() && t.After(v.EndAt) {
			return time.Time{}, false
		}
		return t, true
	default:
		return time.Time{}, false
	}
}
