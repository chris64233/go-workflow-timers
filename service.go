package workflowtimers

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// CreateRequest 创建定时器。RequestID 是请求号：同号同内容重试返回原结果，
// 同号不同内容报 ErrRequestConflict。
type CreateRequest struct {
	WorkflowID string
	TimerID    string // 外部定时器号
	RequestID  string
	FireAt     time.Time
	Payload    []byte
}

// RescheduleRequest 重排定时器到新触发时间，成功后版本递增、旧租约作废。
type RescheduleRequest struct {
	WorkflowID string
	TimerID    string
	RequestID  string
	FireAt     time.Time
}

// CancelRequest 取消定时器。已提交的触发（StateFired）不能撤销。
type CancelRequest struct {
	WorkflowID string
	TimerID    string
	RequestID  string
}

// WriteResult 是创建/重排的返回结果，重放时返回原值。
type WriteResult struct {
	TimerID string
	Version int64
}

// Claim 是一次领取获得的定时器（或周期计划实例）与租约。
// Timer 与 Instance 互斥：Timer != nil 为一次性定时器，Instance != nil 为计划实例。
type Claim struct {
	Timer   *Timer
	LeaseID string
	// 以下仅周期计划实例有值。
	Instance *Instance
}

// FireReceipt 是触发确认的回执。Duplicate 表示这是对同一版本的重复确认，
// 返回的是首次提交的逻辑结果，未产生新的 outbox 记录。
type FireReceipt struct {
	IdempotencyKey string
	Result         []byte
	Duplicate      bool
}

// Service 是持久化定时器服务。所有状态变更在单把互斥锁保护的事务内完成，
// 并写穿透到 Store，因此调度、重排与触发之间的竞态由版本与租约裁决。
type Service struct {
	mu    sync.Mutex
	store Store
	now   func() time.Time
	state *snapshot
	// locs 缓存已加载的 IANA 时区。
	locs map[string]*time.Location
}

// NewService 从 Store 恢复状态并构建服务。
func NewService(store Store) (*Service, error) {
	s, err := store.Load()
	if err != nil {
		return nil, err
	}
	if s.Timers == nil {
		s = newSnapshot()
	}
	// 兼容旧版本快照（尚无周期计划字段）。
	if s.Schedules == nil {
		s.Schedules = make(map[string]*Schedule)
	}
	if s.ScheduleVersions == nil {
		s.ScheduleVersions = make(map[string]*ScheduleVersion)
	}
	if s.Instances == nil {
		s.Instances = make(map[string]*Instance)
	}
	return &Service{store: store, now: time.Now, state: s, locs: make(map[string]*time.Location)}, nil
}

// WithClock 注入时钟（测试用）。
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

func contentHash(op, timerID string, fireAt time.Time, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write([]byte(timerID))
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(fireAt.UnixNano()))
	h.Write(b[:])
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// checkRequest 实现请求号幂等：同号同内容返回已记录的原版本号（replayed=true），
// 同号不同内容报 ErrRequestConflict，未见过的请求号登记后继续执行。
// 调用方必须持有 s.mu。
func (s *Service) checkRequest(workflowID, requestID, op, hash string) (rec *requestRecord, replayed bool, err error) {
	if requestID == "" {
		return nil, false, errors.New("request id is required")
	}
	key := requestKey(workflowID, requestID)
	if rec, ok := s.state.Requests[key]; ok {
		if rec.Op != op || rec.Hash != hash {
			return nil, false, fmt.Errorf("%w: request %q", ErrRequestConflict, requestID)
		}
		return rec, true, nil
	}
	rec = &requestRecord{RequestID: requestID, Op: op, Hash: hash}
	s.state.Requests[key] = rec
	return rec, false, nil
}

// CreateTimer 创建定时器，初始版本为 1。
func (s *Service) CreateTimer(req CreateRequest) (*WriteResult, error) {
	if req.WorkflowID == "" || req.TimerID == "" {
		return nil, errors.New("workflow id and timer id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := contentHash("create", req.TimerID, req.FireAt, req.Payload)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "create", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &WriteResult{TimerID: req.TimerID, Version: rec.Version}, nil
	}

	key := timerKey(req.WorkflowID, req.TimerID)
	if _, ok := s.state.Timers[key]; ok {
		return nil, fmt.Errorf("%w: %q", ErrTimerExists, req.TimerID)
	}
	now := s.now()
	s.state.Timers[key] = &Timer{
		WorkflowID: req.WorkflowID,
		TimerID:    req.TimerID,
		Version:    1,
		State:      StatePending,
		FireAt:     req.FireAt,
		Payload:    req.Payload,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	rec.Version = 1
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &WriteResult{TimerID: req.TimerID, Version: 1}, nil
}

// RescheduleTimer 重排触发时间：版本递增，旧租约作废（旧租约的迟到确认将失败）。
// 与已提交触发相撞时，先提交者胜：已触发则报 ErrAlreadyFired。
func (s *Service) RescheduleTimer(req RescheduleRequest) (*WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := contentHash("reschedule", req.TimerID, req.FireAt, nil)
	rec, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "reschedule", hash)
	if err != nil {
		return nil, err
	}
	if replayed {
		return &WriteResult{TimerID: req.TimerID, Version: rec.Version}, nil
	}

	t, err := s.mutableTimer(req.WorkflowID, req.TimerID)
	if err != nil {
		return nil, err
	}
	t.Version++
	t.FireAt = req.FireAt
	t.Lease = nil // 作废旧租约
	t.UpdatedAt = s.now()
	rec.Version = t.Version
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &WriteResult{TimerID: req.TimerID, Version: t.Version}, nil
}

// CancelTimer 取消定时器。已触发报 ErrAlreadyFired（已提交的触发不能撤销）；
// 成功后任何未提交的确认都会因状态为 CANCELED 而失败。
func (s *Service) CancelTimer(req CancelRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := contentHash("cancel", req.TimerID, time.Time{}, nil)
	_, replayed, err := s.checkRequest(req.WorkflowID, req.RequestID, "cancel", hash)
	if err != nil {
		return err
	}
	if replayed {
		return nil
	}

	t, err := s.mutableTimer(req.WorkflowID, req.TimerID)
	if err != nil {
		return err
	}
	t.State = StateCanceled
	t.Lease = nil
	t.UpdatedAt = s.now()
	return s.store.Save(s.state)
}

// mutableTimer 取出可变更的定时器并校验状态。调用方必须持有 s.mu。
func (s *Service) mutableTimer(workflowID, timerID string) (*Timer, error) {
	t, ok := s.state.Timers[timerKey(workflowID, timerID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTimerNotFound, timerID)
	}
	switch t.State {
	case StateFired:
		return nil, fmt.Errorf("%w: %q", ErrAlreadyFired, timerID)
	case StateCanceled:
		return nil, fmt.Errorf("%w: %q", ErrAlreadyCanceled, timerID)
	}
	return t, nil
}

// claimCandidate 是领取队列中的一个可领取对象（一次性定时器或计划实例）。
type claimCandidate struct {
	dueAt time.Time
	order string // 到期时间相同时的稳定次序
	timer *Timer
	inst  *Instance
}

// ClaimDue 批量领取到期对象（一次性定时器与周期计划实例统一排队）：
// Pending、到期时间 <= now 且租约缺失或已过期。
// 每次领取生成新 LeaseID 与全局递增的围栏令牌，租约有效期为 ttl。
// 返回的每个 Claim 中 Timer 与 Instance 互斥。
func (s *Service) ClaimDue(owner string, limit int, ttl time.Duration) ([]Claim, error) {
	if owner == "" {
		return nil, errors.New("owner is required")
	}
	if limit <= 0 {
		limit = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	var due []claimCandidate
	for _, t := range s.state.Timers {
		if t.State != StatePending || t.FireAt.After(now) {
			continue
		}
		if t.Lease != nil && t.Lease.ExpiresAt.After(now) {
			continue // 租约仍有效
		}
		due = append(due, claimCandidate{dueAt: t.FireAt, order: "t:" + t.WorkflowID + "/" + t.TimerID, timer: t})
	}
	for _, in := range s.state.Instances {
		if in.State != InstancePending || in.ScheduledAt.After(now) {
			continue
		}
		if in.Lease != nil && in.Lease.ExpiresAt.After(now) {
			continue
		}
		due = append(due, claimCandidate{
			dueAt: in.ScheduledAt,
			order: fmt.Sprintf("i:%s/%s/v%d#%d", in.WorkflowID, in.ScheduleID, in.Version, in.Seq),
			inst:  in,
		})
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].dueAt.Equal(due[j].dueAt) {
			return due[i].dueAt.Before(due[j].dueAt)
		}
		return due[i].order < due[j].order
	})
	if len(due) > limit {
		due = due[:limit]
	}

	claims := make([]Claim, 0, len(due))
	for _, c := range due {
		s.state.LeaseSeq++
		lease := &Lease{
			LeaseID:   newLeaseID(),
			Owner:     owner,
			Token:     s.state.LeaseSeq,
			ExpiresAt: now.Add(ttl),
		}
		if c.timer != nil {
			t := c.timer
			t.Lease = lease
			t.UpdatedAt = now
			snap := *t
			leaseCopy := *lease
			snap.Lease = &leaseCopy
			claims = append(claims, Claim{Timer: &snap, LeaseID: lease.LeaseID})
		} else {
			in := c.inst
			in.Lease = lease
			in.UpdatedAt = now
			snap := *in
			leaseCopy := *lease
			snap.Lease = &leaseCopy
			claims = append(claims, Claim{Instance: &snap, LeaseID: lease.LeaseID})
		}
	}
	if len(claims) > 0 {
		if err := s.store.Save(s.state); err != nil {
			return nil, err
		}
	}
	return claims, nil
}

// ConfirmFire 确认触发。只有持有当前版本与当前有效租约的领取者能提交：
// 版本不符报 ErrVersionConflict，租约不符/过期报 ErrLeaseMismatch/ErrLeaseExpired，
// 已取消报 ErrAlreadyCanceled。提交时原子保存逻辑结果并写入带稳定幂等键的
// outbox；对同一版本的重复确认返回首次结果（Duplicate=true），不产生第二次触发。
func (s *Service) ConfirmFire(workflowID, timerID string, version int64, leaseID string, result []byte) (*FireReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.state.Timers[timerKey(workflowID, timerID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTimerNotFound, timerID)
	}
	key := IdempotencyKey(workflowID, timerID, version)

	// 幂等重放：同一版本已提交过触发，返回首次的逻辑结果。
	if t.State == StateFired {
		if t.Version != version {
			return nil, fmt.Errorf("%w: want %d, got %d", ErrVersionConflict, t.Version, version)
		}
		return &FireReceipt{IdempotencyKey: key, Result: t.Result, Duplicate: true}, nil
	}
	if t.State == StateCanceled {
		return nil, fmt.Errorf("%w: %q", ErrAlreadyCanceled, timerID)
	}
	if t.Version != version {
		return nil, fmt.Errorf("%w: want %d, got %d", ErrVersionConflict, t.Version, version)
	}
	if t.Lease == nil {
		return nil, fmt.Errorf("%w: %q", ErrNotClaimed, timerID)
	}
	if t.Lease.LeaseID != leaseID {
		return nil, fmt.Errorf("%w: %q", ErrLeaseMismatch, timerID)
	}
	if !t.Lease.ExpiresAt.After(s.now()) {
		return nil, fmt.Errorf("%w: %q", ErrLeaseExpired, timerID)
	}

	// 原子提交：状态 + 逻辑结果 + outbox（同键插入即幂等）。
	now := s.now()
	t.State = StateFired
	t.Result = result
	t.Lease = nil
	t.UpdatedAt = now
	if _, exists := s.state.Outbox[key]; !exists {
		s.state.Outbox[key] = &OutboxEntry{
			IdempotencyKey: key,
			WorkflowID:     workflowID,
			TimerID:        timerID,
			Version:        version,
			Result:         result,
			CreatedAt:      now,
		}
	}
	if err := s.store.Save(s.state); err != nil {
		return nil, err
	}
	return &FireReceipt{IdempotencyKey: key, Result: result}, nil
}

// GetTimer 查询单个定时器。
func (s *Service) GetTimer(workflowID, timerID string) (*Timer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.state.Timers[timerKey(workflowID, timerID)]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTimerNotFound, timerID)
	}
	cp := *t
	if t.Lease != nil {
		lease := *t.Lease
		cp.Lease = &lease
	}
	return &cp, nil
}

// ListPending 查询工作流下待执行的定时器，按触发时间排序。
func (s *Service) ListPending(workflowID string) []Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Timer
	for _, t := range s.state.Timers {
		if t.WorkflowID == workflowID && t.State == StatePending {
			out = append(out, *t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FireAt.Before(out[j].FireAt) })
	return out
}

// ListOutbox 查询工作流的 outbox 记录（供传输层投递）。
func (s *Service) ListOutbox(workflowID string) []OutboxEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []OutboxEntry
	for _, e := range s.state.Outbox {
		if e.WorkflowID == workflowID {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IdempotencyKey < out[j].IdempotencyKey })
	return out
}

func newLeaseID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
