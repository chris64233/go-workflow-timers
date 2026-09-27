package workflowtimers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Compile-time assertion: MemoryStore 实现 Store。
var _ Store = (*MemoryStore)(nil)

// idemRecord 记录一个请求号首次成功处理时的内容指纹与结果，用于重试幂等。
type idemRecord struct {
	op      string
	timerID string
	fp      string
	// 以下二者择一：定时器类操作保存当时的定时器快照；触发确认保存触发结果。
	timer  *Timer
	result *TriggerResult
}

type timerRow struct {
	t Timer
}

type outboxRow struct {
	e OutboxEvent
}

// MemoryStore 是 Store 的内存实现，用于测试与本地开发。
//
// 并发模型：单一 sync.RWMutex 模拟可串行化事务。每个公开方法在一次
// Lock/Unlock 之间完成“条件检查 + 多行写入”，因此：
//   - 领取与取消/重排并发时，谁先进入临界区谁生效，后来者依据已提交的
//     状态得到 VersionConflict / StateConflict / LeaseExpired，不会双触发；
//   - 触发确认写 timer、trigger_result、outbox 三行在同一临界区，原子完成。
//
// 真实数据库可以直接用短事务 + 条件 UPDATE 复现这些判定，
// 例如确认触发等价于：
//
//	UPDATE timers SET state='triggered', ...
//	 WHERE timer_id=? AND version=? AND state='claimed'
//	   AND lease_token=? AND lease_expires_ms > ?;
//	-- 影响行数为 0 时在同一事务内复查原因（版本/状态/租约）。
type MemoryStore struct {
	mu sync.Mutex

	now func() time.Time
	seq int64

	timers   map[string]*timerRow
	requests map[string]*idemRecord
	results  map[string]*TriggerResult // key: timerID + "\x00" + version
	outbox   map[string]*outboxRow
	// outboxOrder 保存 outbox 键的插入顺序。
	outboxOrder []string
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		now:      time.Now,
		timers:   make(map[string]*timerRow),
		requests: make(map[string]*idemRecord),
		results:  make(map[string]*TriggerResult),
		outbox:   make(map[string]*outboxRow),
	}
}

// WithClock 替换存储时钟（主要用于测试），返回存储自身以便链式构造。
func (s *MemoryStore) WithClock(now func() time.Time) *MemoryStore {
	s.mu.Lock()
	s.now = now
	s.mu.Unlock()
	return s
}

func (s *MemoryStore) ms() int64 { return nowMilli(s.now) }

func (s *MemoryStore) nextLeaseToken() string {
	s.seq++
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("lease-%d-%s", s.seq, hex.EncodeToString(b[:]))
}

func resultKey(timerID string, version int64) string {
	return timerID + "\x00" + fmt.Sprintf("%d", version)
}

// ---- 请求号指纹 ----

func fpCreate(p CreateTimerParams) string {
	return strings.Join([]string{
		"create", p.TimerID, fmt.Sprintf("%d", p.FireAtUnixMilli), p.Payload,
	}, "|")
}

func fpReschedule(p RescheduleParams) string {
	return strings.Join([]string{
		"reschedule", p.TimerID, fmt.Sprintf("%d", p.FireAtUnixMilli), p.Payload,
	}, "|")
}

func fpCancel(p CancelParams) string {
	return "cancel|" + p.TimerID
}

func fpConfirm(p ConfirmTriggerParams) string {
	out := p.OutboxPayload
	return strings.Join([]string{
		"confirm", p.TimerID,
		fmt.Sprintf("%d", p.Version),
		p.ResultPayload, out,
	}, "|")
}

// replayIdem 在请求号已存在时返回首次结果；内容不一致则报幂等冲突。
func (s *MemoryStore) replayIdem(requestID, fp string, conflictVersion int64) (*Timer, *TriggerResult, error) {
	if requestID == "" {
		return nil, nil, nil
	}
	rec := s.requests[requestID]
	if rec == nil {
		return nil, nil, nil
	}
	if rec.fp != fp {
		return nil, nil, errf(KindIdempotentConflict, rec.timerID, conflictVersion,
			"request %s replayed with different content", requestID)
	}
	if rec.timer != nil {
		cp := *rec.timer
		return &cp, nil, nil
	}
	cp := *rec.result
	return nil, &cp, nil
}

// ---- 创建 ----

func (s *MemoryStore) CreateTimer(_ context.Context, p CreateTimerParams) (*Timer, error) {
	if p.TimerID == "" {
		return nil, errf(KindNotFound, "", 0, "timer id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	fp := fpCreate(p)
	if t, r, err := s.replayIdem(p.RequestID, fp, 0); err != nil {
		return nil, err
	} else if t != nil || r != nil {
		return t, nil
	}

	if _, exists := s.timers[p.TimerID]; exists {
		return nil, errf(KindAlreadyExists, p.TimerID, 0, "timer already exists")
	}

	now := s.ms()
	t := &Timer{
		TimerID:          p.TimerID,
		Version:          1,
		State:            StateScheduled,
		FireAtUnixMilli:  p.FireAtUnixMilli,
		Payload:          p.Payload,
		CreatedUnixMilli: now,
		UpdatedUnixMilli: now,
	}
	s.timers[p.TimerID] = &timerRow{t: *t}
	if p.RequestID != "" {
		s.requests[p.RequestID] = &idemRecord{
			op: "create", timerID: p.TimerID, fp: fp, timer: t,
		}
	}
	return t, nil
}

// ---- 重排 ----

func (s *MemoryStore) RescheduleTimer(_ context.Context, p RescheduleParams) (*Timer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fp := fpReschedule(p)
	if t, r, err := s.replayIdem(p.RequestID, fp, 0); err != nil {
		return nil, err
	} else if t != nil || r != nil {
		// 正常情况下重排只会重放定时器快照；r != nil 属于请求号跨操作复用，
		// 指纹不同已在 replayIdem 内拦截，这里防御性处理。
		if r != nil {
			return nil, errf(KindIdempotentConflict, p.TimerID, r.Version,
				"request id was used for a trigger confirmation")
		}
		return t, nil
	}

	row, ok := s.timers[p.TimerID]
	if !ok {
		return nil, errf(KindNotFound, p.TimerID, 0, "timer not found")
	}
	switch row.t.State {
	case StateTriggered, StateCancelled:
		return nil, errf(KindStateConflict, p.TimerID, row.t.Version,
			"cannot reschedule timer in terminal state %s", row.t.State)
	}

	// 重排提交即接管全部未完成的领取：版本 +1，回到 scheduled，旧租约作废。
	row.t.Version++
	row.t.State = StateScheduled
	row.t.FireAtUnixMilli = p.FireAtUnixMilli
	row.t.Payload = p.Payload
	row.t.LeaseToken = ""
	row.t.LeaseExpiresUnixMilli = 0
	row.t.UpdatedUnixMilli = s.ms()

	out := row.t
	if p.RequestID != "" {
		snap := out
		s.requests[p.RequestID] = &idemRecord{
			op: "reschedule", timerID: p.TimerID, fp: fp, timer: &snap,
		}
	}
	cp := out
	return &cp, nil
}

// ---- 取消 ----

func (s *MemoryStore) CancelTimer(_ context.Context, p CancelParams) (*Timer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fp := fpCancel(p)
	if t, _, err := s.replayIdem(p.RequestID, fp, 0); err != nil {
		return nil, err
	} else if t != nil {
		return t, nil
	}

	row, ok := s.timers[p.TimerID]
	if !ok {
		return nil, errf(KindNotFound, p.TimerID, 0, "timer not found")
	}
	if row.t.State == StateTriggered {
		// 触发先于取消提交：触发不可撤销。
		return nil, errf(KindStateConflict, p.TimerID, row.t.Version,
			"timer already triggered; trigger cannot be undone")
	}
	if row.t.State == StateCancelled {
		// 不同请求号的重复取消按幂等成功处理。
		cp := row.t
		return &cp, nil
	}

	row.t.State = StateCancelled
	row.t.LeaseToken = ""
	row.t.LeaseExpiresUnixMilli = 0
	row.t.UpdatedUnixMilli = s.ms()

	if p.RequestID != "" {
		snap := row.t
		s.requests[p.RequestID] = &idemRecord{
			op: "cancel", timerID: p.TimerID, fp: fp, timer: &snap,
		}
	}
	cp := row.t
	return &cp, nil
}

// ---- 领取 ----

func (s *MemoryStore) ClaimDue(_ context.Context, p ClaimParams) ([]DueTimer, error) {
	if p.LeaseDurationMillis <= 0 {
		return nil, errf(KindStateConflict, "", 0, "lease duration must be positive")
	}
	batch := p.MaxBatch
	if batch <= 0 {
		batch = 100
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.ms()

	// 收集候选：到期未领取，或领取租约已过期可被接管。
	type cand struct {
		id     string
		fireAt int64
	}
	var cands []cand
	for id, row := range s.timers {
		t := row.t
		switch t.State {
		case StateScheduled:
			if t.FireAtUnixMilli <= now {
				cands = append(cands, cand{id, t.FireAtUnixMilli})
			}
		case StateClaimed:
			if t.LeaseExpiresUnixMilli <= now {
				cands = append(cands, cand{id, t.FireAtUnixMilli})
			}
		}
	}
	// 按计划触发时刻先后领取，时刻相同按定时器号，保证跨进程行为可预期。
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].fireAt != cands[j].fireAt {
			return cands[i].fireAt < cands[j].fireAt
		}
		return cands[i].id < cands[j].id
	})
	if len(cands) > batch {
		cands = cands[:batch]
	}

	out := make([]DueTimer, 0, len(cands))
	for _, c := range cands {
		row := s.timers[c.id]
		token := s.nextLeaseToken()
		expires := now + p.LeaseDurationMillis
		row.t.State = StateClaimed
		row.t.LeaseToken = token
		row.t.LeaseExpiresUnixMilli = expires
		row.t.UpdatedUnixMilli = now
		out = append(out, DueTimer{
			TimerID:               row.t.TimerID,
			Version:               row.t.Version,
			FireAtUnixMilli:       row.t.FireAtUnixMilli,
			Payload:               row.t.Payload,
			LeaseToken:            token,
			LeaseExpiresUnixMilli: expires,
		})
	}
	return out, nil
}

// ---- 触发确认 ----

func (s *MemoryStore) ConfirmTrigger(_ context.Context, p ConfirmTriggerParams) (*TriggerResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fp := fpConfirm(p)
	if _, r, err := s.replayIdem(p.RequestID, fp, p.Version); err != nil {
		return nil, err
	} else if r != nil {
		// 相同请求号与内容的重试：无论当前租约状态如何，都返回首次结果。
		return r, nil
	}

	row, ok := s.timers[p.TimerID]
	if !ok {
		return nil, errf(KindNotFound, p.TimerID, p.Version, "timer not found")
	}

	// 已提交的触发不可撤销：同版本的重复确认（即使没带原请求号）直接返回
	// 既有结果，保证传输层 at-least-once 重试下“逻辑触发只发生一次”。
	if row.t.State == StateTriggered && row.t.Version == p.Version {
		if r := s.results[resultKey(p.TimerID, p.Version)]; r != nil {
			cp := *r
			return &cp, nil
		}
	}

	// 以下顺序区分三类竞态失败：
	// 1) 重排先提交 -> 版本不再是领取时的版本（fencing）。
	if row.t.Version != p.Version {
		return nil, errf(KindVersionConflict, p.TimerID, row.t.Version,
			"stale version %d: current version is %d (rescheduled)", p.Version, row.t.Version)
	}
	// 2) 取消先提交，或从未领取/已触发 -> 状态不允许确认。
	if row.t.State != StateClaimed {
		return nil, errf(KindStateConflict, p.TimerID, row.t.Version,
			"cannot confirm trigger in state %s", row.t.State)
	}
	// 3) 旧租约的迟到确认：令牌已被接管者替换，或租约已过期。
	if row.t.LeaseToken != p.LeaseToken || p.LeaseToken == "" {
		return nil, errf(KindLeaseExpired, p.TimerID, row.t.Version,
			"lease token does not match current lease")
	}
	now := s.ms()
	if row.t.LeaseExpiresUnixMilli <= now {
		return nil, errf(KindLeaseExpired, p.TimerID, row.t.Version,
			"lease expired at %d (now %d)", row.t.LeaseExpiresUnixMilli, now)
	}

	outboxPayload := p.OutboxPayload
	if outboxPayload == "" {
		outboxPayload = p.ResultPayload
	}
	key := outboxKeyFor(p.TimerID, p.Version)

	// 临界区内原子写入：定时器终态 + 触发结果 + outbox。
	row.t.State = StateTriggered
	row.t.LeaseToken = ""
	row.t.LeaseExpiresUnixMilli = 0
	row.t.TriggeredUnixMilli = now
	row.t.UpdatedUnixMilli = now

	r := &TriggerResult{
		TimerID:           p.TimerID,
		Version:           p.Version,
		RequestID:         p.RequestID,
		ResultPayload:     p.ResultPayload,
		OutboxKey:         key,
		OutboxPayload:     outboxPayload,
		OccurredUnixMilli: now,
	}
	s.results[resultKey(p.TimerID, p.Version)] = r

	if _, exists := s.outbox[key]; !exists {
		s.outbox[key] = &outboxRow{e: OutboxEvent{
			Key:              key,
			TimerID:          p.TimerID,
			Version:          p.Version,
			Payload:          outboxPayload,
			Status:           OutboxPending,
			CreatedUnixMilli: now,
		}}
		s.outboxOrder = append(s.outboxOrder, key)
	}

	if p.RequestID != "" {
		snap := *r
		s.requests[p.RequestID] = &idemRecord{
			op: "confirm", timerID: p.TimerID, fp: fp, result: &snap,
		}
	}
	cp := *r
	return &cp, nil
}

// ---- 待执行查询 ----

func (s *MemoryStore) DuePending(_ context.Context, q PendingQuery) ([]PendingResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	at := q.AtUnixMilli
	if at == 0 {
		at = s.ms()
	}

	var out []PendingResult
	for _, row := range s.timers {
		t := row.t
		switch t.State {
		case StateScheduled:
			if t.FireAtUnixMilli <= at {
				out = append(out, PendingResult{
					TimerID: t.TimerID, Version: t.Version,
					FireAtUnixMilli: t.FireAtUnixMilli, Payload: t.Payload,
					State: StateScheduled,
				})
			}
		case StateClaimed:
			// 租约仍有效的记录有人负责，不属于“待执行”；
			// 租约已过期的记录等待接管。
			if t.LeaseExpiresUnixMilli <= at {
				out = append(out, PendingResult{
					TimerID: t.TimerID, Version: t.Version,
					FireAtUnixMilli: t.FireAtUnixMilli, Payload: t.Payload,
					State: StateClaimed,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FireAtUnixMilli != out[j].FireAtUnixMilli {
			return out[i].FireAtUnixMilli < out[j].FireAtUnixMilli
		}
		return out[i].TimerID < out[j].TimerID
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// ---- 读取与 outbox 投递 ----

func (s *MemoryStore) GetTimer(_ context.Context, timerID string) (*Timer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.timers[timerID]
	if !ok {
		return nil, errf(KindNotFound, timerID, 0, "timer not found")
	}
	cp := row.t
	return &cp, nil
}

func (s *MemoryStore) GetTriggerResult(_ context.Context, timerID string, version int64) (*TriggerResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[resultKey(timerID, version)]
	if !ok {
		return nil, errf(KindNotFound, timerID, version, "trigger result not found")
	}
	cp := *r
	return &cp, nil
}

func (s *MemoryStore) ListOutboxPending(_ context.Context, limit int) ([]OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []OutboxEvent
	for _, key := range s.outboxOrder {
		row := s.outbox[key]
		if row.e.Status == OutboxPending {
			out = append(out, row.e)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *MemoryStore) MarkOutboxDelivered(_ context.Context, key string) (*OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.outbox[key]
	if !ok {
		return nil, errf(KindNotFound, "", 0, "outbox event not found: %s", key)
	}
	if row.e.Status != OutboxDelivered {
		row.e.Status = OutboxDelivered
		row.e.DeliveredUnixMilli = s.ms()
	}
	cp := row.e
	return &cp, nil
}
