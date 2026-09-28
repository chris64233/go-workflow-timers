package workflowtimers

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"
)

// 默认单次扫描最多生成的实例数，防止长时间停机后一次扫描无限膨胀。
const defaultScanLimit = 100

// CreateScheduleRequest 创建周期计划。计划时间点全部在 Location 时区下计算；
// StartAt/EndAt 携带的时区仅作为绝对时刻使用，展示与枚举以 Location 为准。
// Policy 决定停机期间错过时间点的补触发方式，且冻结在计划版本上。
type CreateScheduleRequest struct {
	WorkflowID string
	ScheduleID string // 外部计划号，工作流内唯一
	RequestID  string

	Location   string        // IANA 时区名（如 "Asia/Shanghai"）；空串按 UTC
	StartAt    time.Time     // 计划起点（含）
	EndAt      time.Time     // 计划终点（含）；零值表示永不结束
	Recurrence Recurrence    // 周期规则
	Policy     CatchUpPolicy // 补触发策略
	Payload    []byte
}

// UpdateScheduleRequest 用一套全新的计划内容开一个递增版本。
// 新版本从更新时刻（now）起生效；旧版本未完成的实例作废，已提交的触发历史保留。
type UpdateScheduleRequest struct {
	WorkflowID string
	ScheduleID string
	RequestID  string

	Location   string
	StartAt    time.Time
	EndAt      time.Time
	Recurrence Recurrence
	Policy     CatchUpPolicy
	Payload    []byte
}

// PauseScheduleRequest 暂停计划：不影响已经提交的触发，未完成的实例作废。
type PauseScheduleRequest struct {
	WorkflowID string
	ScheduleID string
	RequestID  string
}

// ResumeScheduleRequest 恢复计划：复制最近版本的规则开一个递增版本，
// 新版本从恢复时刻起继续，暂停期间错过的时间点不补。
type ResumeScheduleRequest struct {
	WorkflowID string
	ScheduleID string
	RequestID  string
}

// ScheduleWriteResult 是计划写操作的返回结果，请求号重放时返回原值。
type ScheduleWriteResult struct {
	ScheduleID string
	Version    int64
}

func validateSpec(loc *time.Location, start, end time.Time, r Recurrence, policy CatchUpPolicy) error {
	if start.IsZero() {
		return fmt.Errorf("%w: start time is required", ErrInvalidSchedule)
	}
	if !end.IsZero() && end.Before(start) {
		return fmt.Errorf("%w: end before start", ErrInvalidSchedule)
	}
	switch policy {
	case CatchUpAll, CatchUpLatest, CatchUpSkip:
	default:
		return fmt.Errorf("%w: unknown catch-up policy %q", ErrInvalidSchedule, policy)
	}
	switch r.Kind {
	case RecurEvery:
		if r.Every <= 0 {
			return fmt.Errorf("%w: every interval must be positive", ErrInvalidSchedule)
		}
	case RecurDaily:
		if r.Hour < 0 || r.Hour > 23 || r.Minute < 0 || r.Minute > 59 {
			return fmt.Errorf("%w: daily time out of range", ErrInvalidSchedule)
		}
	default:
		return fmt.Errorf("%w: unknown recurrence kind %q", ErrInvalidSchedule, r.Kind)
	}
	// 起点之后必须至少还能枚举到一个时间点。
	probe := ScheduleVersion{ResumeAt: start, EndAt: end, Recurrence: r}
	if _, ok := probe.firstScheduled(start, loc); !ok {
		return fmt.Errorf("%w: no occurrence within [start, end]", ErrInvalidSchedule)
	}
	return nil
}

func scheduleContentHash(op, id, location string, start, end time.Time, r Recurrence, policy CatchUpPolicy, payload []byte) string {
	h := sha256.New()
	w := func(b []byte) { h.Write(b); h.Write([]byte{0}) }
	w([]byte(op))
	w([]byte(id))
	w([]byte(location))
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(start.UnixNano()))
	w(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(end.UnixNano()))
	w(b[:])
	w([]byte(r.Kind))
	binary.BigEndian.PutUint64(b[:], uint64(r.Every))
	w(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(r.Hour))
	w(b[:])
	binary.BigEndian.PutUint64(b[:], uint64(r.Minute))
	w(b[:])
	w([]byte(policy))
	w(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// resolveLocation 加载时区并按名称缓存（冻结版本上的时区名不可变）。
// 调用方必须持有 s.mu。
func (s *Service) resolveLocation(name string) (*time.Location, error) {
	if name == "" {
		name = "UTC"
	}
	if loc, ok := s.locs[name]; ok {
		return loc, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: location %q: %v", ErrInvalidSchedule, name, err)
	}
	s.locs[name] = loc
	return loc, nil
}

// CreateSchedule 创建周期计划，初始版本为 1，补触发策略冻结在 v1 上。
func (s *Service) CreateSchedule(req CreateScheduleRequest) (*ScheduleWriteResult, error) {
	if req.WorkflowID == "" || req.ScheduleID == "" {
		return nil, errors.New("workflow id and schedule id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	loc, err := s.resolveLocation(req.Location)
	if err != nil {
		return nil, err
	}
	if err := validateSpec(loc, req.StartAt, req.EndAt, req.Recurrence, req.Policy); err != nil {
		return nil, err
	}
	hash := scheduleContentHash("schedule.create", req.ScheduleID, req.Location, req.StartAt, req.EndAt, req.Recurrence, req.Policy, req.Payload)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule.create", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &ScheduleWriteResult{ScheduleID: req.ScheduleID, Version: rec.Version}, nil
	}
	key := scheduleKey(req.WorkflowID, req.ScheduleID)
	if _, ok := s.state.Schedules[key]; ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleExists, req.ScheduleID)
	}

	now := s.now()
	ver := s.newVersion(req.WorkflowID, req.ScheduleID, 1, req.Location, req.StartAt, req.StartAt,
		req.EndAt, req.Recurrence, req.Policy, req.Payload, loc, now, false)
	s.state.Schedules[key] = &Schedule{
		WorkflowID:     req.WorkflowID,
		ScheduleID:     req.ScheduleID,
		State:          ScheduleActive,
		CurrentVersion: 1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.state.ScheduleVersions[versionKey(req.WorkflowID, req.ScheduleID, 1)] = ver
	rec.Version = 1
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &ScheduleWriteResult{ScheduleID: req.ScheduleID, Version: 1}, nil
}

// newVersion 构造一个冻结版本并把游标定位到第一个时间点。
// strict=true 时（更新/恢复开新版本）游标严格晚于当前时刻：新版本从操作提交后的
// 下一个时间点起生效，不在提交瞬间生成一个“当下”实例。创建时 strict=false，
// StartAt 本身就是首个槽（早于 now 时按冻结的补触发策略处理）。
// 调用方必须持有 s.mu。
func (s *Service) newVersion(workflowID, scheduleID string, version int64,
	location string, startAt, resumeAt, endAt time.Time, r Recurrence,
	policy CatchUpPolicy, payload []byte, loc *time.Location, now time.Time, strict bool) *ScheduleVersion {

	v := &ScheduleVersion{
		WorkflowID: workflowID,
		ScheduleID: scheduleID,
		Version:    version,
		Location:   location,
		StartAt:    startAt,
		EndAt:      endAt,
		Recurrence: r,
		Policy:     policy,
		Payload:    payload,
		ResumeAt:   resumeAt,
		NextSeq:    1,
		CreatedAt:  now,
	}
	first, ok := v.firstScheduled(resumeAt, loc)
	if strict && ok && !first.After(now) {
		first, ok = v.nextScheduled(first, loc)
	}
	if ok {
		v.Cursor = first
	} else {
		v.Done = true
	}
	return v
}

// UpdateSchedule 更新计划：旧版本未完成实例作废（已提交的保留），
// 以新内容开递增版本并从当前时刻起生效。旧版本的规则与补触发策略原样冻结保留。
func (s *Service) UpdateSchedule(req UpdateScheduleRequest) (*ScheduleWriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	loc, err := s.resolveLocation(req.Location)
	if err != nil {
		return nil, err
	}
	if err := validateSpec(loc, req.StartAt, req.EndAt, req.Recurrence, req.Policy); err != nil {
		return nil, err
	}
	hash := scheduleContentHash("schedule.update", req.ScheduleID, req.Location, req.StartAt, req.EndAt, req.Recurrence, req.Policy, req.Payload)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule.update", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &ScheduleWriteResult{ScheduleID: req.ScheduleID, Version: rec.Version}, nil
	}

	sch, err := s.activeSchedule(req.WorkflowID, req.ScheduleID)
	if err != nil {
		return nil, err
	}
	now := s.now()
	newVerNum := sch.CurrentVersion + 1
	ver := s.newVersion(req.WorkflowID, req.ScheduleID, newVerNum, req.Location, req.StartAt, now,
		req.EndAt, req.Recurrence, req.Policy, req.Payload, loc, now, true)
	s.rotateVersion(sch, ver, now)
	rec.Version = newVerNum
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &ScheduleWriteResult{ScheduleID: req.ScheduleID, Version: newVerNum}, nil
}

// PauseSchedule 暂停计划：旧版本未完成实例全部作废，已提交触发不受影响。
// 不产生新版本；恢复时才开新版本。
func (s *Service) PauseSchedule(req PauseScheduleRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := scheduleContentHash("schedule.pause", req.ScheduleID, "", time.Time{}, time.Time{}, Recurrence{}, "", nil)
	_, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule.pause", hash)
	if err != nil {
		return err
	}
	if replayed {
		return nil
	}
	sch, err := s.activeSchedule(req.WorkflowID, req.ScheduleID)
	if err != nil {
		return err
	}
	now := s.now()
	sch.State = SchedulePaused
	sch.UpdatedAt = now
	if old, ok := s.state.ScheduleVersions[versionKey(req.WorkflowID, req.ScheduleID, sch.CurrentVersion)]; ok {
		old.Done = true
	}
	s.supersedePending(req.WorkflowID, req.ScheduleID, sch.CurrentVersion, now)
	return s.store.Save(s.state)
}

// ResumeSchedule 恢复计划：沿用最近版本冻结的规则与补触发策略，开递增版本，
// 从恢复时刻起枚举时间点；暂停期间错过的时间点不补。
func (s *Service) ResumeSchedule(req ResumeScheduleRequest) (*ScheduleWriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := scheduleContentHash("schedule.resume", req.ScheduleID, "", time.Time{}, time.Time{}, Recurrence{}, "", nil)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "schedule.resume", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &ScheduleWriteResult{ScheduleID: req.ScheduleID, Version: rec.Version}, nil
	}
	key := scheduleKey(req.WorkflowID, req.ScheduleID)
	sch, ok := s.state.Schedules[key]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleNotFound, req.ScheduleID)
	}
	if sch.State != SchedulePaused {
		return nil, fmt.Errorf("%w: %q is %s", ErrScheduleNotActive, req.ScheduleID, sch.State)
	}
	last, ok := s.state.ScheduleVersions[versionKey(req.WorkflowID, req.ScheduleID, sch.CurrentVersion)]
	if !ok {
		return nil, fmt.Errorf("%w: frozen version %d", ErrScheduleNotFound, sch.CurrentVersion)
	}
	loc, err := s.resolveLocation(last.Location)
	if err != nil {
		return nil, err
	}
	now := s.now()
	newVerNum := sch.CurrentVersion + 1
	ver := s.newVersion(req.WorkflowID, req.ScheduleID, newVerNum, last.Location, last.StartAt, now,
		last.EndAt, last.Recurrence, last.Policy, last.Payload, loc, now, true)
	s.rotateVersion(sch, ver, now)
	rec.Version = newVerNum
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &ScheduleWriteResult{ScheduleID: req.ScheduleID, Version: newVerNum}, nil
}

// activeSchedule 取出处于 ACTIVE 的计划。调用方必须持有 s.mu。
func (s *Service) activeSchedule(workflowID, scheduleID string) (*Schedule, error) {
	sch, ok := s.state.Schedules[scheduleKey(workflowID, scheduleID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleNotFound, scheduleID)
	}
	switch sch.State {
	case ScheduleActive:
		return sch, nil
	case SchedulePaused:
		return nil, fmt.Errorf("%w: %q is paused", ErrScheduleAlreadyPaused, scheduleID)
	default:
		return nil, fmt.Errorf("%w: %q is %s", ErrScheduleNotActive, scheduleID, sch.State)
	}
}

// rotateVersion 切换计划当前版本：旧版本标记枚举结束、未完成实例作废，
// 登记新冻结版本。绝不触碰其他版本（含新版本已生成）的实例。
// 新版本已无时间点时计划直接进入 FINISHED。调用方必须持有 s.mu。
func (s *Service) rotateVersion(sch *Schedule, ver *ScheduleVersion, now time.Time) {
	if old, ok := s.state.ScheduleVersions[versionKey(sch.WorkflowID, sch.ScheduleID, sch.CurrentVersion)]; ok {
		old.Done = true
	}
	s.supersedePending(sch.WorkflowID, sch.ScheduleID, sch.CurrentVersion, now)
	sch.CurrentVersion = ver.Version
	if ver.Done {
		sch.State = ScheduleFinished
	} else {
		sch.State = ScheduleActive
	}
	sch.UpdatedAt = now
	s.state.ScheduleVersions[versionKey(sch.WorkflowID, sch.ScheduleID, ver.Version)] = ver
}

// supersedePending 把指定版本所有未完成（含已领取未确认）的实例作废。
// 已 FIRED 的实例是不可撤销的触发历史，原样保留；其他版本的实例一概不动。
// 调用方必须持有 s.mu。
func (s *Service) supersedePending(workflowID, scheduleID string, version int64, now time.Time) {
	for _, in := range s.state.Instances {
		if in.WorkflowID == workflowID && in.ScheduleID == scheduleID &&
			in.Version == version && in.State == InstancePending {
			in.State = InstanceSuperseded
			in.Lease = nil
			in.UpdatedAt = now
		}
	}
}

// ScanInstances 扫描全部 ACTIVE 计划，按各当前版本冻结的规则与补触发策略生成实例。
// 多次扫描幂等：版本游标只前进，每个计划时间点至多生成一个实例与一个稳定幂等键。
// limit 限制本次扫描最多生成的实例总数（<=0 用默认值）；返回新生成的实例数。
func (s *Service) ScanInstances(limit int) (int, error) {
	if limit <= 0 {
		limit = defaultScanLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	keys := make([]string, 0, len(s.state.Schedules))
	for key, sch := range s.state.Schedules {
		if sch.State == ScheduleActive {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	generated := 0
	changed := false
	for _, key := range keys {
		if generated >= limit {
			break
		}
		sch := s.state.Schedules[key]
		ver := s.state.ScheduleVersions[versionKey(sch.WorkflowID, sch.ScheduleID, sch.CurrentVersion)]
		n, ch, err := s.scanOne(sch, ver, now, limit-generated)
		if err != nil {
			return 0, err
		}
		generated += n
		changed = changed || ch
	}
	if generated > 0 || changed {
		if err := s.store.Save(s.state); err != nil {
			return 0, err
		}
	}
	return generated, nil
}

// scanOne 推进单个版本的游标并生成实例。调用方必须持有 s.mu。
//
// 补触发语义（策略冻结在版本上）：扫描时枚举所有 ScheduledAt <= now 的到期时间点。
// 健康运行时每个周期至少扫描一次，恰好一个到期点，三种策略都会正常生成它；
// 停机后多个到期点堆积时，按策略裁决：全部生成 / 只生成最后一个 / 全部跳过，
// 游标一次性越过整个积压，因此重复扫描不会重复生成。
func (s *Service) scanOne(sch *Schedule, v *ScheduleVersion, now time.Time, budget int) (int, bool, error) {
	if v.Done || budget <= 0 {
		return 0, false, nil
	}
	loc, err := s.resolveLocation(v.Location)
	if err != nil {
		return 0, false, err
	}
	changed := false
	if v.Cursor.IsZero() {
		first, ok := v.firstScheduled(v.ResumeAt, loc)
		if !ok {
			v.Done = true
			s.finishIfDone(sch, v, now)
			return 0, true, nil
		}
		v.Cursor = first
		changed = true
	}

	generated := 0
	for generated < budget {
		if v.Done {
			s.finishIfDone(sch, v, now)
			break
		}
		if v.Cursor.After(now) {
			break // 未来时间点，等下次扫描
		}

		switch v.Policy {
		case CatchUpLatest:
			// 积压时只补最后一个时间点：游标直接跳到 <= now 的最后一个槽位。
			last, ok := v.lastScheduled(now, loc)
			if !ok {
				v.Cursor = time.Time{}
				v.Done = true
				changed = true
				break
			}
			if last.After(v.Cursor) {
				// 中间槽位不生成，仅对齐序号。
				v.NextSeq += v.slotsBetween(v.Cursor, last, loc)
				v.Cursor = last
			}
			s.emitInstance(sch, v, v.Cursor, v.NextSeq, now)
			generated++
			changed = true
			s.advanceCursor(v, loc)

		case CatchUpSkip:
			next, hasNext := v.nextScheduled(v.Cursor, loc)
			if hasNext && !next.After(now) {
				// 至少两个到期点 = 扫描曾长期缺位（停机恢复）：跳过整个积压，
				// 游标直接落到最后一个错过点之后，序号与槽位一一对应。
				last, ok := v.lastScheduled(now, loc)
				if !ok {
					v.Cursor = time.Time{}
					v.Done = true
					changed = true
					break
				}
				v.NextSeq += v.slotsBetween(v.Cursor, last, loc) + 1
				afterLast, ok := v.nextScheduled(last, loc)
				if !ok {
					v.Cursor = time.Time{}
					v.Done = true
					changed = true
					break
				}
				v.Cursor = afterLast
				changed = true
				continue
			}
			// 恰好一个到期点：当前周期，正常生成。
			s.emitInstance(sch, v, v.Cursor, v.NextSeq, now)
			generated++
			changed = true
			s.advanceCursor(v, loc)

		default: // CatchUpAll
			s.emitInstance(sch, v, v.Cursor, v.NextSeq, now)
			generated++
			changed = true
			s.advanceCursor(v, loc)
		}

		if v.Done {
			s.finishIfDone(sch, v, now)
			break
		}
	}
	return generated, changed, nil
}

// emitInstance 在一个计划时间点上生成实例。键由 (版本, 槽位序号) 决定，
// 游标不回退保证同一时间点不会第二次进入这里。
// 调用方必须持有 s.mu。
func (s *Service) emitInstance(sch *Schedule, v *ScheduleVersion, scheduledAt time.Time, seq int64, now time.Time) {
	in := &Instance{
		WorkflowID:  sch.WorkflowID,
		ScheduleID:  sch.ScheduleID,
		Version:     v.Version,
		Seq:         seq,
		State:       InstancePending,
		ScheduledAt: scheduledAt,
		GeneratedAt: now,
		Payload:     v.Payload,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.state.Instances[instanceKey(sch.WorkflowID, sch.ScheduleID, v.Version, seq)] = in
}

// advanceCursor 把游标推进到下一个时间点；越过 EndAt 则版本枚举结束。
// 调用方必须持有 s.mu。
func (s *Service) advanceCursor(v *ScheduleVersion, loc *time.Location) {
	next, ok := v.nextScheduled(v.Cursor, loc)
	v.NextSeq++
	if !ok {
		v.Cursor = time.Time{}
		v.Done = true
		return
	}
	v.Cursor = next
}

// finishIfDone 在版本枚举完毕后把计划置为 FINISHED（终态）。
// 调用方必须持有 s.mu。
func (s *Service) finishIfDone(sch *Schedule, v *ScheduleVersion, now time.Time) {
	if v.Done && sch.State == ScheduleActive {
		sch.State = ScheduleFinished
		sch.UpdatedAt = now
	}
}

// lastScheduled 返回 <= upper（并以 EndAt 为上界）且不早于游标的最后一个计划时间点。
func (v *ScheduleVersion) lastScheduled(upper time.Time, loc *time.Location) (time.Time, bool) {
	if !v.EndAt.IsZero() && upper.After(v.EndAt) {
		upper = v.EndAt
	}
	var t time.Time
	switch v.Recurrence.Kind {
	case RecurEvery:
		elapsed := upper.Sub(v.ResumeAt)
		if elapsed < 0 {
			return time.Time{}, false
		}
		k := int64(elapsed / v.Recurrence.Every)
		t = v.ResumeAt.Add(time.Duration(k) * v.Recurrence.Every)
	case RecurDaily:
		local := upper.In(loc)
		t = time.Date(local.Year(), local.Month(), local.Day(), v.Recurrence.Hour, v.Recurrence.Minute, 0, 0, loc)
		if t.After(upper) {
			t = t.AddDate(0, 0, -1)
		}
	}
	if t.Before(v.Cursor) {
		return time.Time{}, false
	}
	return t, true
}

// slotsBetween 返回同一规则下 [from, to] 间隔的槽位数（to 相对 from 的序号差）。
func (v *ScheduleVersion) slotsBetween(from, to time.Time, loc *time.Location) int64 {
	switch v.Recurrence.Kind {
	case RecurEvery:
		return int64(to.Sub(from) / v.Recurrence.Every)
	case RecurDaily:
		return civilDays(to, loc) - civilDays(from, loc)
	default:
		return 0
	}
}

// civilDays 返回当地日历日期相对 1970-01-01 的天数（Howard Hinnant civil days）。
func civilDays(t time.Time, loc *time.Location) int64 {
	y, m, d := t.In(loc).Date()
	mm := int(m)
	if mm <= 2 {
		y--
		mm += 9
	} else {
		mm -= 3
	}
	era := y / 400
	if y < 0 && y%400 != 0 {
		era-- // Go 整除向零取整，负数需向下取整
	}
	yoe := y - era*400
	doy := int64((153*mm+2)/5 + d - 1)
	doe := int64(yoe)*365 + int64(yoe)/4 - int64(yoe)/100 + doy
	return int64(era)*146097 + doe
}

// ConfirmInstanceFire 确认计划实例触发，规则与一次性定时器一致：
// 只有持有当前有效租约的领取者能提交；实例已被新版本/暂停取代则报 ErrInstanceSuperseded。
// 提交时原子保存结果并写入带稳定幂等键（workflow/schedule/vN/seq）的 outbox；
// 重复确认返回首次结果（Duplicate=true）。上一个实例失败或未确认不会阻塞后续时间点。
func (s *Service) ConfirmInstanceFire(workflowID, scheduleID string, version, seq int64, leaseID string, result []byte) (*FireReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	in, ok := s.state.Instances[instanceKey(workflowID, scheduleID, version, seq)]
	if !ok {
		return nil, fmt.Errorf("%w: %s v%d #%d", ErrInstanceNotFound, scheduleID, version, seq)
	}
	key := InstanceIdempotencyKey(workflowID, scheduleID, version, seq)

	if in.State == InstanceFired {
		return &FireReceipt{IdempotencyKey: key, Result: in.Result, Duplicate: true}, nil
	}
	if in.State == InstanceSuperseded {
		return nil, fmt.Errorf("%w: %s v%d #%d", ErrInstanceSuperseded, scheduleID, version, seq)
	}
	if in.Lease == nil {
		return nil, fmt.Errorf("%w: %s #%d", ErrNotClaimed, scheduleID, seq)
	}
	if in.Lease.LeaseID != leaseID {
		return nil, fmt.Errorf("%w: %s #%d", ErrLeaseMismatch, scheduleID, seq)
	}
	if !in.Lease.ExpiresAt.After(s.now()) {
		return nil, fmt.Errorf("%w: %s #%d", ErrLeaseExpired, scheduleID, seq)
	}

	now := s.now()
	in.State = InstanceFired
	in.Result = result
	in.FiredAt = now
	in.Lease = nil
	in.UpdatedAt = now
	if _, exists := s.state.Outbox[key]; !exists {
		s.state.Outbox[key] = &OutboxEntry{
			IdempotencyKey: key,
			WorkflowID:     workflowID,
			Kind:           OutboxKindScheduleInstance,
			ScheduleID:     scheduleID,
			InstanceSeq:    seq,
			Version:        version,
			ScheduledAt:    in.ScheduledAt,
			Result:         result,
			CreatedAt:      now,
		}
	}
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &FireReceipt{IdempotencyKey: key, Result: result}, nil
}

// GetSchedule 查询计划头。
func (s *Service) GetSchedule(workflowID, scheduleID string) (*Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sch, ok := s.state.Schedules[scheduleKey(workflowID, scheduleID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrScheduleNotFound, scheduleID)
	}
	cp := *sch
	return &cp, nil
}

// GetScheduleVersion 查询某个冻结版本（含游标等内部状态），用于核对版本历史。
func (s *Service) GetScheduleVersion(workflowID, scheduleID string, version int64) (*ScheduleVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state.ScheduleVersions[versionKey(workflowID, scheduleID, version)]
	if !ok {
		return nil, fmt.Errorf("%w: %s v%d", ErrScheduleNotFound, scheduleID, version)
	}
	cp := *v
	return &cp, nil
}

// ListScheduleVersions 列出计划的全部冻结版本，按版本号升序。
func (s *Service) ListScheduleVersions(workflowID, scheduleID string) []ScheduleVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ScheduleVersion
	for _, v := range s.state.ScheduleVersions {
		if v.WorkflowID == workflowID && v.ScheduleID == scheduleID {
			out = append(out, *v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// ListInstances 查询计划在所有版本下的实例（触发历史），按版本与序号升序。
// 每条记录区分 ScheduledAt（计划时间）、GeneratedAt（实际生成时间）、
// FiredAt（最终触发时间）与 State/Result。
func (s *Service) ListInstances(workflowID, scheduleID string) []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Instance
	for _, in := range s.state.Instances {
		if in.WorkflowID == workflowID && in.ScheduleID == scheduleID {
			cp := *in
			if in.Lease != nil {
				l := *in.Lease
				cp.Lease = &l
			}
			out = append(out, cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}
