package workflowtimers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// CreateScheduleRequest 创建周期计划。
type CreateScheduleRequest struct {
	WorkflowID string
	ScheduleID string // 外部计划号，工作流内唯一
	RequestID  string
	Spec       ScheduleSpec
}

// UpdateScheduleRequest 更新计划定义：递增版本、追加不可变版本快照，
// 旧版本已经生成的实例历史全部保留；旧版本尚未到期的未来实例被作废。
type UpdateScheduleRequest struct {
	WorkflowID string
	ScheduleID string
	RequestID  string
	Spec       ScheduleSpec
}

// ScheduleStateRequest 暂停/恢复请求。
type ScheduleStateRequest struct {
	WorkflowID string
	ScheduleID string
	RequestID  string
}

// ScanReport 是一次扫描的物化结果。
type ScanReport struct {
	Checked   int // 参与扫描的生效计划数
	Generated int // 新生成的待触发实例数
	Skipped   int // 按补触发策略落为 SKIPPED 的实例数
	Completed int // 本次扫描中转 COMPLETED 的计划数
}

// InstanceClaim 是一次实例领取获得的实例与租约。
type InstanceClaim struct {
	Instance Instance
	LeaseID  string
}

// InstanceFireReceipt 是实例触发确认回执。Duplicate 表示同一实例的重复确认。
type InstanceFireReceipt struct {
	IdempotencyKey string
	InstanceID     string
	Result         []byte
	Duplicate      bool
}

// validateSpec 校验计划定义并返回其时区与可求值的递归规则。
// anchor 取版本自身的 StartAt（DTSTART），求值在该版本冻结的时区上进行。
func validateSpec(spec ScheduleSpec) (*time.Location, *recurrence, error) {
	switch spec.MissedPolicy {
	case MissedCatchUpAll, MissedLatestOnly, MissedSkip:
	default:
		return nil, nil, fmt.Errorf("%w: unknown missed policy %q", ErrInvalidSchedule, spec.MissedPolicy)
	}
	if spec.StartAt.IsZero() {
		return nil, nil, fmt.Errorf("%w: start_at is required", ErrInvalidSchedule)
	}
	if !spec.EndAt.IsZero() && !spec.EndAt.After(spec.StartAt) {
		return nil, nil, fmt.Errorf("%w: end_at must be after start_at", ErrInvalidSchedule)
	}
	loc := time.UTC
	if spec.Timezone != "" {
		l, err := time.LoadLocation(spec.Timezone)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: bad timezone %q: %v", ErrInvalidSchedule, spec.Timezone, err)
		}
		loc = l
	}
	anchor := spec.StartAt.In(loc)
	rec, err := parseRRULE(spec.RRULE, anchor)
	if err != nil {
		return nil, nil, err
	}
	return loc, rec, nil
}

func scheduleContentHash(op string, spec ScheduleSpec) string {
	b, _ := json.Marshal(spec)
	h := sha256.Sum256(append([]byte(op+"\x00"), b...))
	return hex.EncodeToString(h[:])
}

// CreateSchedule 创建周期计划，初始版本为 1。生效下界为创建时刻：
// 早于创建时刻的计划时间点不会补生成。
func (s *Service) CreateSchedule(req CreateScheduleRequest) (*WriteResult, error) {
	if req.WorkflowID == "" || req.ScheduleID == "" {
		return nil, errors.New("workflow id and schedule id are required")
	}
	if _, _, err := validateSpec(req.Spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := scheduleContentHash("schedule-create", req.Spec)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule-create", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &WriteResult{TimerID: req.ScheduleID, Version: rec.Version}, nil
	}
	key := scheduleKey(req.WorkflowID, req.ScheduleID)
	if _, ok := s.state.Schedules[key]; ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleExists, req.ScheduleID)
	}
	now := s.now()
	sch := &Schedule{
		WorkflowID:     req.WorkflowID,
		ScheduleID:     req.ScheduleID,
		State:          ScheduleActive,
		CurrentVersion: 1,
		EffectiveFrom:  now,
		Versions: map[int64]*ScheduleVersion{
			1: {Version: 1, Spec: cloneSpec(req.Spec), EffectiveFrom: now, CreatedAt: now},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.state.Schedules[key] = sch
	rec.Version = 1
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &WriteResult{TimerID: req.ScheduleID, Version: 1}, nil
}

// UpdateSchedule 提交新的计划版本：追加不可变版本快照、生效下界推进到更新时刻，
// 此后扫描只按新版本物化；旧版本尚未派发给执行者的待触发实例被作废，
// 已领取与已终结的实例历史保留；新版本生成的实例绝不被删除。
func (s *Service) UpdateSchedule(req UpdateScheduleRequest) (*WriteResult, error) {
	if _, _, err := validateSpec(req.Spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := scheduleContentHash("schedule-update", req.Spec)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule-update", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		// 原请求重试：返回当时创建的版本，即使后续又有更新。
		return &WriteResult{TimerID: req.ScheduleID, Version: rec.Version}, nil
	}
	sch, err := s.mutableSchedule(req.WorkflowID, req.ScheduleID)
	if err != nil {
		return nil, err
	}

	now := s.now()
	newVersion := sch.CurrentVersion + 1
	sch.Versions[newVersion] = &ScheduleVersion{
		Version:       newVersion,
		Spec:          cloneSpec(req.Spec),
		EffectiveFrom: now,
		CreatedAt:     now,
	}
	sch.CurrentVersion = newVersion
	sch.EffectiveFrom = now
	sch.UpdatedAt = now

	// 旧版本不得继续生效：作废其尚未派发给执行者的待触发实例
	// （PENDING/FAILED 且没有当前有效租约，含租约已过期者）。
	// 已经被领取（持有有效租约，视为已提交给执行者）与已终结
	// （FIRED/CANCELED/SKIPPED）的实例历史全部保留，可继续触发或查询；
	// 任何属于新版本的实例都不在作废范围内。
	for _, inst := range s.state.Instances {
		if inst.WorkflowID != req.WorkflowID || inst.ScheduleID != req.ScheduleID {
			continue
		}
		if inst.Version == newVersion || terminalInstance(inst.Status) {
			continue
		}
		if inst.Lease != nil && inst.Lease.ExpiresAt.After(now) {
			continue
		}
		inst.Status = InstanceCanceled
		inst.Lease = nil
		inst.UpdatedAt = now
	}

	rec.Version = newVersion
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &WriteResult{TimerID: req.ScheduleID, Version: newVersion}, nil
}

// PauseSchedule 暂停计划：扫描不再物化新实例；已经生成（含已领取）与已提交
// 的实例不受影响，仍可被领取与确认。
func (s *Service) PauseSchedule(req ScheduleStateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hash := scheduleContentHash("schedule-pause", ScheduleSpec{})
	if _, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule-pause", hash); err != nil {
		return err
	} else if replayed {
		return nil
	}
	sch, err := s.mutableSchedule(req.WorkflowID, req.ScheduleID)
	if err != nil {
		return err
	}
	if sch.State == SchedulePaused {
		return fmt.Errorf("%w: %q", ErrScheduleAlreadyPaused, req.ScheduleID)
	}
	sch.State = SchedulePaused
	sch.UpdatedAt = s.now()
	return s.store.Save(s.state)
}

// ResumeSchedule 恢复计划：版本号不变，生效下界推进到恢复时刻，
// 暂停期间错过的时间点不再补生成。
func (s *Service) ResumeSchedule(req ScheduleStateRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hash := scheduleContentHash("schedule-resume", ScheduleSpec{})
	if _, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule-resume", hash); err != nil {
		return err
	} else if replayed {
		return nil
	}
	sch, ok := s.state.Schedules[scheduleKey(req.WorkflowID, req.ScheduleID)]
	if !ok {
		return fmt.Errorf("%w: %q", ErrScheduleNotFound, req.ScheduleID)
	}
	if sch.State == ScheduleCompleted {
		return fmt.Errorf("%w: %q", ErrScheduleCompleted, req.ScheduleID)
	}
	if sch.State != SchedulePaused {
		return fmt.Errorf("%w: %q", ErrScheduleNotPaused, req.ScheduleID)
	}
	now := s.now()
	sch.State = ScheduleActive
	sch.EffectiveFrom = now
	sch.UpdatedAt = now
	return s.store.Save(s.state)
}

// mutableSchedule 取出可变更的计划。调用方必须持有 s.mu。
func (s *Service) mutableSchedule(workflowID, scheduleID string) (*Schedule, error) {
	sch, ok := s.state.Schedules[scheduleKey(workflowID, scheduleID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleNotFound, scheduleID)
	}
	if sch.State == ScheduleCompleted {
		return nil, fmt.Errorf("%w: %q", ErrScheduleCompleted, scheduleID)
	}
	return sch, nil
}

func terminalInstance(st InstanceStatus) bool {
	return st == InstanceFired || st == InstanceCanceled || st == InstanceSkipped
}

func cloneSpec(spec ScheduleSpec) ScheduleSpec {
	cp := spec
	if spec.Payload != nil {
		cp.Payload = append([]byte(nil), spec.Payload...)
	}
	return cp
}

// ScanSchedules 按各计划的当前版本物化到期实例。扫描是幂等的：
// 每个（版本, 序号）只生成一个实例和一个稳定幂等键，重复扫描不重复生成。
//
// lateness 定义“准点”容差：now-ScheduledAt 超过 lateness 的时间点视为
// 停机期间错过，按该版本冻结的 MissedPolicy 处理（全部补齐 / 只补最近一次 /
// 直接跳过）；容差内的时间点无论策略如何都正常生成。
func (s *Service) ScanSchedules(lateness time.Duration) (*ScanReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	report := &ScanReport{}
	dirty := false
	var scanErr error

	ids := make([]string, 0, len(s.state.Schedules))
	for k := range s.state.Schedules {
		ids = append(ids, k)
	}
	sort.Strings(ids)

	for _, key := range ids {
		sch := s.state.Schedules[key]
		if sch.State != ScheduleActive {
			continue
		}
		report.Checked++
		cur, ok := sch.Versions[sch.CurrentVersion]
		if !ok {
			return nil, fmt.Errorf("%w: schedule %q missing version %d", ErrInvalidSchedule, sch.ScheduleID, sch.CurrentVersion)
		}

		// 扫描窗口下界：版本生效时间与上次扫描水位取较晚者，
		// 因此旧版本/暂停前的时间点不会被重新物化。
		lo := sch.EffectiveFrom
		if sch.LastScanAt.After(lo) {
			lo = sch.LastScanAt
		}
		hi := now
		completed := false
		if !cur.Spec.EndAt.IsZero() {
			if !now.Before(cur.Spec.EndAt) {
				completed = true
			}
			if cur.Spec.EndAt.Before(hi) {
				hi = cur.Spec.EndAt
			}
		}

		advances := true
		if hi.After(lo) {
			loc, err := time.LoadLocation(orUTC(cur.Spec.Timezone))
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidSchedule, err)
			}
			rec, err := parseRRULE(cur.Spec.RRULE, cur.Spec.StartAt.In(loc))
			if err != nil {
				return nil, err
			}
			occs, err := rec.between(lo, hi)
			if err != nil {
				// 窗口内待处理时间点超过上限：本计划不推进水位、不落任何实例，
				// 其他计划照常物化，错误随报告返回，由调用方择期重试。
				advances = false
				if scanErr == nil {
					scanErr = err
				}
			} else {
				generated, skipped := s.materialize(sch, cur, occs, now, lateness)
				report.Generated += generated
				report.Skipped += skipped
				if generated+skipped > 0 {
					dirty = true
				}
			}
		}

		if advances {
			sch.LastScanAt = now
			dirty = true
			// 仅当本次窗口已完整处理（未超限）才允许转 COMPLETED，
			// 否则保留 ACTIVE，下次扫描按稳定键继续补齐。
			if completed {
				sch.State = ScheduleCompleted
				report.Completed++
			}
		}
	}

	if dirty {
		if err := s.store.Save(s.state); err != nil {
			return nil, err
		}
	}
	return report, scanErr
}

// materialize 按补触发策略把窗口内时间点落为实例。调用方持有 s.mu。
// occs 已按时间（序号）升序。
func (s *Service) materialize(sch *Schedule, ver *ScheduleVersion, occs []occurrence, now time.Time, lateness time.Duration) (generated, skipped int) {
	isMissed := func(o occurrence) bool { return now.Sub(o.at) > lateness }

	switch ver.Spec.MissedPolicy {
	case MissedCatchUpAll:
		// 全部补齐：准点与错过的时间点都生成。
		for _, o := range occs {
			if s.putInstance(sch, ver, o, InstancePending, now) {
				generated++
			}
		}
	case MissedLatestOnly:
		// 只补最近一次：整个窗口内只有最晚一个时间点生成，其余落为 SKIPPED。
		// 稳态扫描窗口内通常只有一个时间点，因此不会误跳准点执行。
		if n := len(occs); n > 0 {
			for _, o := range occs[:n-1] {
				if s.putInstance(sch, ver, o, InstanceSkipped, now) {
					skipped++
				}
			}
			if s.putInstance(sch, ver, occs[n-1], InstancePending, now) {
				generated++
			}
		}
	case MissedSkip:
		// 直接跳过：错过的时间点落为 SKIPPED，准点容差内的照常生成。
		for _, o := range occs {
			status := InstancePending
			if isMissed(o) {
				status = InstanceSkipped
			}
			if s.putInstance(sch, ver, o, status, now) {
				if status == InstanceSkipped {
					skipped++
				} else {
					generated++
				}
			}
		}
	}
	return generated, skipped
}

// putInstance 按稳定实例号幂等落库；已存在（同一版本+序号）则保持原记录不动。
// 返回是否为本次新建。调用方必须持有 s.mu。
func (s *Service) putInstance(sch *Schedule, ver *ScheduleVersion, o occurrence, status InstanceStatus, now time.Time) bool {
	id := scheduleInstanceID(sch.ScheduleID, ver.Version, o.seq)
	key := instanceKey(sch.WorkflowID, id)
	if _, exists := s.state.Instances[key]; exists {
		return false
	}
	inst := &Instance{
		WorkflowID:  sch.WorkflowID,
		ScheduleID:  sch.ScheduleID,
		InstanceID:  id,
		Version:     ver.Version,
		Seq:         o.seq,
		Status:      status,
		ScheduledAt: o.at,
		GeneratedAt: now,
		Payload:     append([]byte(nil), ver.Spec.Payload...),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.state.Instances[key] = inst
	return true
}

func orUTC(tz string) string {
	if tz == "" {
		return "UTC"
	}
	return tz
}

// ClaimDueInstances 批量领取到期实例：状态为 PENDING/FAILED、租约缺失或已过期。
// 规则与单次定时器一致：新 LeaseID + 递增围栏令牌，租约过期可被接管。
// 前一个实例触发失败不会阻塞后续时间点：每个实例独立领取。
func (s *Service) ClaimDueInstances(owner string, limit int, ttl time.Duration) ([]InstanceClaim, error) {
	if owner == "" {
		return nil, errors.New("owner is required")
	}
	if limit <= 0 {
		limit = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	var due []*Instance
	for _, inst := range s.state.Instances {
		if inst.Status != InstancePending && inst.Status != InstanceFailed {
			continue
		}
		if inst.Lease != nil && inst.Lease.ExpiresAt.After(now) {
			continue
		}
		due = append(due, inst)
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].ScheduledAt.Equal(due[j].ScheduledAt) {
			return due[i].ScheduledAt.Before(due[j].ScheduledAt)
		}
		return due[i].InstanceID < due[j].InstanceID
	})
	if len(due) > limit {
		due = due[:limit]
	}

	claims := make([]InstanceClaim, 0, len(due))
	for _, inst := range due {
		s.state.LeaseSeq++
		lease := &Lease{
			LeaseID:   newLeaseID(),
			Owner:     owner,
			Token:     s.state.LeaseSeq,
			ExpiresAt: now.Add(ttl),
		}
		inst.Lease = lease
		inst.UpdatedAt = now
		cp := *inst
		leaseCopy := *lease
		cp.Lease = &leaseCopy
		claims = append(claims, InstanceClaim{Instance: cp, LeaseID: lease.LeaseID})
	}
	if len(claims) > 0 {
		if err := s.store.Save(s.state); err != nil {
			return nil, err
		}
	}
	return claims, nil
}

// ConfirmInstanceFire 确认实例触发。只有持有当前有效租约的领取者能提交
// （实例由哪个计划版本生成不影响其有效性——旧版本已生成的实例仍可触发）。
// 提交时原子保存结果、FiredAt 与 outbox（稳定键 workflow/schedule/version/seq）；
// 对同一实例的重复确认返回首次结果（Duplicate=true）。
func (s *Service) ConfirmInstanceFire(workflowID, instanceID, leaseID string, result []byte) (*InstanceFireReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inst, ok := s.state.Instances[instanceKey(workflowID, instanceID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, instanceID)
	}
	key := InstanceIdempotencyKey(workflowID, inst.ScheduleID, inst.Version, inst.Seq)

	if inst.Status == InstanceFired {
		return &InstanceFireReceipt{IdempotencyKey: key, InstanceID: instanceID, Result: inst.Result, Duplicate: true}, nil
	}
	switch inst.Status {
	case InstanceCanceled:
		return nil, fmt.Errorf("%w: %q", ErrInstanceCanceled, instanceID)
	case InstanceSkipped:
		return nil, fmt.Errorf("%w: %q", ErrInstanceSkipped, instanceID)
	}
	if inst.Lease == nil {
		return nil, fmt.Errorf("%w: %q", ErrNotClaimed, instanceID)
	}
	if inst.Lease.LeaseID != leaseID {
		return nil, fmt.Errorf("%w: %q", ErrLeaseMismatch, instanceID)
	}
	if !inst.Lease.ExpiresAt.After(s.now()) {
		return nil, fmt.Errorf("%w: %q", ErrLeaseExpired, instanceID)
	}

	now := s.now()
	inst.Status = InstanceFired
	inst.Result = result
	inst.FiredAt = now
	inst.Lease = nil
	inst.UpdatedAt = now
	if _, exists := s.state.Outbox[key]; !exists {
		s.state.Outbox[key] = &OutboxEntry{
			IdempotencyKey: key,
			WorkflowID:     workflowID,
			ScheduleID:     inst.ScheduleID,
			InstanceID:     instanceID,
			Version:        inst.Version,
			Seq:            inst.Seq,
			ScheduledAt:    inst.ScheduledAt,
			Result:         result,
			CreatedAt:      now,
		}
	}
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &InstanceFireReceipt{IdempotencyKey: key, InstanceID: instanceID, Result: result}, nil
}

// ReportInstanceFailure 上报实例触发失败：记录尝试次数与原因并释放租约，
// 实例转为 FAILED，可被重新领取重试。失败不影响、也不阻塞任何其他实例，
// 周期的下一个时间点独立物化与领取。
func (s *Service) ReportInstanceFailure(workflowID, instanceID, leaseID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	inst, ok := s.state.Instances[instanceKey(workflowID, instanceID)]
	if !ok {
		return fmt.Errorf("%w: %q", ErrInstanceNotFound, instanceID)
	}
	if inst.Status == InstanceFired {
		return fmt.Errorf("%w: %q", ErrAlreadyFired, instanceID)
	}
	if inst.Status == InstanceCanceled {
		return fmt.Errorf("%w: %q", ErrInstanceCanceled, instanceID)
	}
	if inst.Lease == nil {
		return fmt.Errorf("%w: %q", ErrNotClaimed, instanceID)
	}
	if inst.Lease.LeaseID != leaseID {
		return fmt.Errorf("%w: %q", ErrLeaseMismatch, instanceID)
	}
	now := s.now()
	inst.Attempts++
	inst.LastError = reason
	inst.Status = InstanceFailed
	inst.Lease = nil
	inst.UpdatedAt = now
	return s.store.Save(s.state)
}

// GetSchedule 查询计划（含全部版本快照）。
func (s *Service) GetSchedule(workflowID, scheduleID string) (*Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sch, ok := s.state.Schedules[scheduleKey(workflowID, scheduleID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleNotFound, scheduleID)
	}
	cp := *sch
	versions := make(map[int64]*ScheduleVersion, len(sch.Versions))
	for v, sv := range sch.Versions {
		vv := *sv
		if sv.Spec.Payload != nil {
			vv.Spec.Payload = append([]byte(nil), sv.Spec.Payload...)
		}
		versions[v] = &vv
	}
	cp.Versions = versions
	return &cp, nil
}

// ListSchedules 列出工作流下的计划，按计划号排序。返回深拷贝，
// 调用方可安全读取全部版本快照。
func (s *Service) ListSchedules(workflowID string) []Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Schedule
	for _, sch := range s.state.Schedules {
		if sch.WorkflowID != workflowID {
			continue
		}
		cp := *sch
		versions := make(map[int64]*ScheduleVersion, len(sch.Versions))
		for v, sv := range sch.Versions {
			vv := *sv
			if sv.Spec.Payload != nil {
				vv.Spec.Payload = append([]byte(nil), sv.Spec.Payload...)
			}
			versions[v] = &vv
		}
		cp.Versions = versions
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ScheduleID < out[j].ScheduleID })
	return out
}

// GetInstance 查询单个触发实例。
func (s *Service) GetInstance(workflowID, instanceID string) (*Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, ok := s.state.Instances[instanceKey(workflowID, instanceID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, instanceID)
	}
	cp := *inst
	if inst.Lease != nil {
		l := *inst.Lease
		cp.Lease = &l
	}
	return &cp, nil
}

// ListInstances 列出计划的全部触发实例（含已触发/已作废/已跳过的历史），
// 按版本与版本内序号排序。
func (s *Service) ListInstances(workflowID, scheduleID string) []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Instance
	for _, inst := range s.state.Instances {
		if inst.WorkflowID == workflowID && inst.ScheduleID == scheduleID {
			out = append(out, *inst)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		return out[i].Seq < out[j].Seq
	})
	// 租约可能被后台领取/过期接管替换，拷贝指针避免外部读取与内部变更竞争。
	for i := range out {
		if out[i].Lease != nil {
			l := *out[i].Lease
			out[i].Lease = &l
		}
	}
	return out
}
