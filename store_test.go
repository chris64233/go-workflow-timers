package workflowtimers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 测试用可控时钟 ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *fakeClock) millis() int64 { return c.now().UnixMilli() }

func newTestStore() (*MemoryStore, *fakeClock) {
	c := newFakeClock()
	return NewMemoryStore().WithClock(c.now), c
}

func mustKind(t *testing.T, err error, want ErrorKind) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error kind %s, got nil", want)
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *workflowtimers.Error, got %T: %v", err, err)
	}
	if e.Kind != want {
		t.Fatalf("want error kind %s, got %s (%v)", want, e.Kind, err)
	}
}

func mkTimer(t *testing.T, store Store, clock *fakeClock, id string, fireIn time.Duration) *Timer {
	t.Helper()
	got, err := store.CreateTimer(context.Background(), CreateTimerParams{
		TimerID:         id,
		RequestID:       "create-" + id,
		FireAtUnixMilli: clock.millis() + fireIn.Milliseconds(),
		Payload:         "payload-" + id,
	})
	if err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	return got
}

func claimOne(t *testing.T, store Store, lease time.Duration) DueTimer {
	t.Helper()
	got, err := store.ClaimDue(context.Background(), ClaimParams{
		MaxBatch: 10, LeaseDurationMillis: lease.Milliseconds(), WorkerID: "w1",
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 claimed timer, got %d", len(got))
	}
	return got[0]
}

// ---- 创建 ----

func TestCreateTimer(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()

	t1, err := store.CreateTimer(ctx, CreateTimerParams{
		TimerID: "T1", RequestID: "r1",
		FireAtUnixMilli: clock.millis() + 1000, Payload: "p",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if t1.Version != 1 || t1.State != StateScheduled {
		t.Fatalf("unexpected timer: %+v", t1)
	}

	// 同号再创建 -> AlreadyExists。
	_, err = store.CreateTimer(ctx, CreateTimerParams{
		TimerID: "T1", RequestID: "r2", FireAtUnixMilli: clock.millis() + 2000,
	})
	mustKind(t, err, KindAlreadyExists)
}

func TestCreateIdempotentRetry(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	params := CreateTimerParams{
		TimerID: "T1", RequestID: "r1",
		FireAtUnixMilli: clock.millis() + 1000, Payload: "p",
	}
	first, err := store.CreateTimer(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	// 相同请求号 + 相同内容的重试：返回原结果，不重复创建。
	second, err := store.CreateTimer(ctx, params)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first.Version != second.Version || first.State != second.State {
		t.Fatalf("retry changed result: %+v vs %+v", first, second)
	}

	// 同号不同内容 -> 幂等冲突。
	params.Payload = "different"
	_, err = store.CreateTimer(ctx, params)
	mustKind(t, err, KindIdempotentConflict)

	// 换个未使用过的定时器号、复用该请求号也算冲突（指纹不同）。
	_, err = store.CreateTimer(ctx, CreateTimerParams{
		TimerID: "T2", RequestID: "r1", FireAtUnixMilli: 1, Payload: "p2",
	})
	mustKind(t, err, KindIdempotentConflict)
}

// ---- 重排 ----

func TestRescheduleIncrementsVersion(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 10*time.Second)

	t2, err := store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "T1", RequestID: "rs-1",
		FireAtUnixMilli: clock.millis() + 20_000, Payload: "p2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if t2.Version != 2 || t2.State != StateScheduled {
		t.Fatalf("want v2 scheduled, got %+v", t2)
	}
	if t2.FireAtUnixMilli != clock.millis()+20_000 {
		t.Fatalf("fire_at not updated: %+v", t2)
	}

	// 相同请求号重试：返回重排后的原结果（不会再次递增版本）。
	retry, err := store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "T1", RequestID: "rs-1",
		FireAtUnixMilli: clock.millis() + 20_000, Payload: "p2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Version != 2 {
		t.Fatalf("idempotent retry must not bump version, got %d", retry.Version)
	}

	// 同号不同内容 -> 冲突。
	_, err = store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "T1", RequestID: "rs-1",
		FireAtUnixMilli: clock.millis() + 99_000, Payload: "p2",
	})
	mustKind(t, err, KindIdempotentConflict)
}

func TestRescheduleOnTerminalStates(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()

	// 已取消 -> 拒绝重排。
	mkTimer(t, store, clock, "TC", time.Second)
	if _, err := store.CancelTimer(ctx, CancelParams{TimerID: "TC", RequestID: "c1"}); err != nil {
		t.Fatal(err)
	}
	_, err := store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "TC", RequestID: "rs", FireAtUnixMilli: clock.millis() + 1000,
	})
	mustKind(t, err, KindStateConflict)

	// 已触发 -> 拒绝重排。
	mkTimer(t, store, clock, "TT", 0)
	d := claimOne(t, store, time.Minute)
	if _, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "TT", Version: d.Version, LeaseToken: d.LeaseToken,
		RequestID: "trig-1", ResultPayload: "r",
	}); err != nil {
		t.Fatal(err)
	}
	_, err = store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "TT", RequestID: "rs2", FireAtUnixMilli: clock.millis() + 1000,
	})
	mustKind(t, err, KindStateConflict)

	// 不存在。
	_, err = store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "nope", RequestID: "rs3", FireAtUnixMilli: 1,
	})
	mustKind(t, err, KindNotFound)
}

// ---- 取消 ----

func TestCancelStates(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()

	// 取消 scheduled。
	mkTimer(t, store, clock, "T1", 10*time.Second)
	got, err := store.CancelTimer(ctx, CancelParams{TimerID: "T1", RequestID: "cancel-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateCancelled {
		t.Fatalf("want cancelled, got %s", got.State)
	}
	// 重复取消幂等。
	got2, err := store.CancelTimer(ctx, CancelParams{TimerID: "T1", RequestID: "cancel-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got2.State != StateCancelled {
		t.Fatalf("want cancelled, got %s", got2.State)
	}
	// 已到期也不会被领取。
	clock.advance(time.Minute)
	due, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 10, LeaseDurationMillis: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("cancelled timer must not be claimable, got %+v", due)
	}

	// 取消不存在。
	_, err = store.CancelTimer(ctx, CancelParams{TimerID: "nope", RequestID: "c"})
	mustKind(t, err, KindNotFound)
}

// ---- 领取与租约 ----

func TestClaimDueBatch(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "future", time.Hour)
	mkTimer(t, store, clock, "due1", 0)
	mkTimer(t, store, clock, "due2", -time.Second)
	mkTimer(t, store, clock, "due3", -time.Minute)

	// 未到期不领取；批量上限生效。
	got, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 2, LeaseDurationMillis: 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want batch of 2, got %d", len(got))
	}
	// 按 fire_at 先后领取：due3(-60s), due2(-1s)。
	if got[0].TimerID != "due3" || got[1].TimerID != "due2" {
		t.Fatalf("unexpected claim order: %+v", got)
	}
	for _, d := range got {
		if d.Version != 1 || d.LeaseToken == "" || d.LeaseExpiresUnixMilli != clock.millis()+60_000 {
			t.Fatalf("bad lease: %+v", d)
		}
	}

	// 租约有效期间不会重复领取同一条。
	got, err = store.ClaimDue(ctx, ClaimParams{MaxBatch: 10, LeaseDurationMillis: 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TimerID != "due1" {
		t.Fatalf("only due1 should remain claimable, got %+v", got)
	}

	// 租约必须为正。
	_, err = store.ClaimDue(ctx, ClaimParams{LeaseDurationMillis: 0})
	mustKind(t, err, KindStateConflict)
}

func TestClaimLeaseTakeover(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)

	first := claimOne(t, store, 10*time.Second)

	// 租约未过期：不能被接管。
	if got, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 10, LeaseDurationMillis: 10_000}); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("active lease must not be claimed, got %+v", got)
	}

	// 租约过期：另一个领取者拿到新令牌接管。
	clock.advance(11 * time.Second)
	second := claimOne(t, store, 10*time.Second)
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("takeover must rotate lease token")
	}
	if second.Version != first.Version {
		t.Fatalf("takeover must not change version: %d vs %d", second.Version, first.Version)
	}

	// 旧领取者的迟到确认（版本仍对，但令牌旧）-> LeaseExpired。
	_, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: first.Version, LeaseToken: first.LeaseToken,
		RequestID: "late", ResultPayload: "old",
	})
	mustKind(t, err, KindLeaseExpired)

	// 接管者确认成功。
	r, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: second.Version, LeaseToken: second.LeaseToken,
		RequestID: "new", ResultPayload: "new",
	})
	if err != nil {
		t.Fatalf("takeover owner confirm: %v", err)
	}
	if r.ResultPayload != "new" {
		t.Fatalf("wrong winning result: %+v", r)
	}
}

// ---- 触发确认 / outbox ----

func TestConfirmTriggerAtomicallyWritesResultAndOutbox(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	d := claimOne(t, store, time.Minute)

	r, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: d.TimerID, Version: d.Version, LeaseToken: d.LeaseToken,
		RequestID: "trig-1", ResultPayload: `{"ok":true}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantKey := "timer:T1:v1"
	if r.OutboxKey != wantKey {
		t.Fatalf("outbox key = %q, want %q", r.OutboxKey, wantKey)
	}
	if r.OutboxPayload != `{"ok":true}` {
		t.Fatalf("outbox payload = %q", r.OutboxPayload)
	}

	got, err := store.GetTimer(ctx, "T1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateTriggered || got.TriggeredUnixMilli != clock.millis() {
		t.Fatalf("timer not triggered: %+v", got)
	}

	pending, err := store.ListOutboxPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Key != wantKey || pending[0].Status != OutboxPending {
		t.Fatalf("outbox not written atomically: %+v", pending)
	}

	// 标记投递，重复标记幂等。
	if _, err := store.MarkOutboxDelivered(ctx, wantKey); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkOutboxDelivered(ctx, wantKey); err != nil {
		t.Fatalf("double mark should be idempotent: %v", err)
	}
	if pending, err := store.ListOutboxPending(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("delivered event must leave pending set: %v %+v", err, pending)
	}
	_, err = store.MarkOutboxDelivered(ctx, "timer:nope:v1")
	mustKind(t, err, KindNotFound)
}

func TestConfirmTriggerIdempotent(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	d := claimOne(t, store, time.Minute)
	params := ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, LeaseToken: d.LeaseToken,
		RequestID: "trig-1", ResultPayload: "r",
	}
	first, err := store.ConfirmTrigger(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	// 传输层重复：相同请求号与内容 -> 返回首次结果。
	second, err := store.ConfirmTrigger(ctx, params)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if second.OutboxKey != first.OutboxKey || second.ResultPayload != first.ResultPayload {
		t.Fatalf("replay changed result: %+v vs %+v", first, second)
	}

	// 即使不带请求号重复确认同版本，也返回已持久化的结果（逻辑触发只有一次）。
	third, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, LeaseToken: d.LeaseToken, ResultPayload: "r",
	})
	if err != nil {
		t.Fatalf("duplicate confirm without request id: %v", err)
	}
	if third.OutboxKey != first.OutboxKey {
		t.Fatalf("duplicate confirm produced different outbox key")
	}

	// 同号不同内容 -> 幂等冲突。
	other := params
	other.ResultPayload = "different"
	_, err = store.ConfirmTrigger(ctx, other)
	mustKind(t, err, KindIdempotentConflict)

	// outbox 仍然只有一条。
	pending, _ := store.ListOutboxPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("exactly one outbox event expected, got %d", len(pending))
	}
	r, err := store.GetTriggerResult(ctx, "T1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.OccurredUnixMilli != first.OccurredUnixMilli {
		t.Fatal("trigger occurrence time must stay stable across replays")
	}
}

func TestConfirmExpiredLeaseFailsAndAllowsTakeover(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	first := claimOne(t, store, 10*time.Second)

	clock.advance(11 * time.Second)
	// 旧租约过期后的迟到确认（此时还没人接管，令牌仍写在记录上）。
	_, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: first.Version, LeaseToken: first.LeaseToken,
		ResultPayload: "late",
	})
	mustKind(t, err, KindLeaseExpired)

	// 过期租约可被重新领取。
	second := claimOne(t, store, 10*time.Second)
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("re-claim must rotate lease token")
	}
	if _, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: second.Version, LeaseToken: second.LeaseToken,
		ResultPayload: "win",
	}); err != nil {
		t.Fatalf("new owner should confirm: %v", err)
	}
}

func TestConfirmValidationErrors(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	d := claimOne(t, store, time.Minute)

	// 空令牌。
	_, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, ResultPayload: "r",
	})
	mustKind(t, err, KindLeaseExpired)

	// 错误令牌。
	_, err = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, LeaseToken: "bogus", ResultPayload: "r",
	})
	mustKind(t, err, KindLeaseExpired)

	// 不存在的定时器。
	_, err = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "nope", Version: 1, LeaseToken: d.LeaseToken,
	})
	mustKind(t, err, KindNotFound)

	// 未领取（scheduled）不能确认。
	mkTimer(t, store, clock, "T2", 0)
	_, err = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T2", Version: 1, LeaseToken: "x",
	})
	mustKind(t, err, KindStateConflict)
}

// ---- 调度 / 重排 / 取消 / 触发 之间的竞态顺序 ----

// 重排先于旧版本的确认提交：旧版本确认失败，接管的新版本可正常触发。
func TestRaceRescheduleBeatsStaleConfirm(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)

	v1 := claimOne(t, store, time.Minute)

	// 工作流重排：版本递增、旧租约失效。
	t2, err := store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "T1", RequestID: "rs-1",
		FireAtUnixMilli: clock.millis() + 60_000, Payload: "p2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if t2.Version != 2 {
		t.Fatalf("want v2, got %d", t2.Version)
	}

	// 旧领取者的迟到确认：版本已过期 -> VersionConflict，且不得写结果/outbox。
	_, err = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: v1.Version, LeaseToken: v1.LeaseToken,
		RequestID: "late-v1", ResultPayload: "old",
	})
	mustKind(t, err, KindVersionConflict)

	if pending, _ := store.ListOutboxPending(ctx, 10); len(pending) != 0 {
		t.Fatalf("failed stale confirm must not write outbox, got %+v", pending)
	}
	if _, err := store.GetTriggerResult(ctx, "T1", 1); err == nil {
		t.Fatal("failed stale confirm must not persist a trigger result")
	}

	// 新版本到期后领取并触发成功，outbox 键按新版本稳定生成。
	clock.advance(61 * time.Second)
	v2 := claimOne(t, store, time.Minute)
	if v2.Version != 2 {
		t.Fatalf("want claim v2, got %d", v2.Version)
	}
	r, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: 2, LeaseToken: v2.LeaseToken,
		RequestID: "trig-v2", ResultPayload: "new",
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.OutboxKey != "timer:T1:v2" {
		t.Fatalf("unexpected outbox key %q", r.OutboxKey)
	}

	// 旧版本的迟到确认再来一次，依旧不能影响已提交的新版本触发。
	_, err = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: v1.Version, LeaseToken: v1.LeaseToken,
		ResultPayload: "old",
	})
	mustKind(t, err, KindVersionConflict)
}

// 取消先于确认提交：确认失败；定时器保持取消。
func TestRaceCancelBeatsConfirm(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	d := claimOne(t, store, time.Minute)

	if _, err := store.CancelTimer(ctx, CancelParams{TimerID: "T1", RequestID: "cancel-1"}); err != nil {
		t.Fatal(err)
	}
	_, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, LeaseToken: d.LeaseToken,
		ResultPayload: "r",
	})
	mustKind(t, err, KindStateConflict)

	got, _ := store.GetTimer(ctx, "T1")
	if got.State != StateCancelled {
		t.Fatalf("want cancelled, got %s", got.State)
	}
	if pending, _ := store.ListOutboxPending(ctx, 10); len(pending) != 0 {
		t.Fatalf("cancelled timer must have no outbox, got %+v", pending)
	}
}

// 触发先于取消提交：取消失败，触发不可撤销。
func TestRaceTriggerBeatsCancel(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	d := claimOne(t, store, time.Minute)

	if _, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, LeaseToken: d.LeaseToken,
		RequestID: "trig-1", ResultPayload: "r",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := store.CancelTimer(ctx, CancelParams{TimerID: "T1", RequestID: "cancel-late"})
	mustKind(t, err, KindStateConflict)

	got, _ := store.GetTimer(ctx, "T1")
	if got.State != StateTriggered {
		t.Fatalf("committed trigger must survive cancel, got %s", got.State)
	}
}

// 触发先于重排提交：重排失败。
func TestRaceTriggerBeatsReschedule(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	mkTimer(t, store, clock, "T1", 0)
	d := claimOne(t, store, time.Minute)
	if _, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "T1", Version: d.Version, LeaseToken: d.LeaseToken, ResultPayload: "r",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "T1", RequestID: "rs-late", FireAtUnixMilli: clock.millis() + 1000,
	})
	mustKind(t, err, KindStateConflict)
}

// ---- 待执行查询 ----

// putRow 白盒构造一条定时器记录（测试同包），用于精确布置租约状态。
func putRow(s *MemoryStore, t Timer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timers[t.TimerID] = &timerRow{t: t}
}

func TestDuePending(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()
	now := clock.millis()

	putRow(store, Timer{TimerID: "future", Version: 1, State: StateScheduled,
		FireAtUnixMilli: now + 3_600_000})
	putRow(store, Timer{TimerID: "due", Version: 1, State: StateScheduled,
		FireAtUnixMilli: now})
	putRow(store, Timer{TimerID: "active", Version: 1, State: StateClaimed,
		FireAtUnixMilli: now, LeaseToken: "L-active", LeaseExpiresUnixMilli: now + 60_000})
	putRow(store, Timer{TimerID: "expired", Version: 1, State: StateClaimed,
		FireAtUnixMilli: now, LeaseToken: "L-expired", LeaseExpiresUnixMilli: now - 1})
	putRow(store, Timer{TimerID: "cancelled", Version: 1, State: StateCancelled,
		FireAtUnixMilli: now})
	putRow(store, Timer{TimerID: "triggered", Version: 1, State: StateTriggered,
		FireAtUnixMilli: now})

	got, err := store.DuePending(ctx, PendingQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 pending (due + expired-lease), got %+v", got)
	}
	byID := map[string]PendingResult{}
	for _, p := range got {
		byID[p.TimerID] = p
	}
	if p, ok := byID["due"]; !ok || p.State != StateScheduled {
		t.Fatalf("due scheduled timer missing: %+v", got)
	}
	if p, ok := byID["expired"]; !ok || p.State != StateClaimed {
		t.Fatalf("expired-lease timer should be pending for takeover: %+v", got)
	}
	if _, ok := byID["active"]; ok {
		t.Fatal("timer with active lease must not be pending")
	}

	// limit 生效。
	got, err = store.DuePending(ctx, PendingQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("limit not honored: %+v", got)
	}

	// 显式 AtUnixMilli：把查询时刻拨回租约过期之前，expired 不再待执行。
	got, err = store.DuePending(ctx, PendingQuery{AtUnixMilli: now - 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("nothing should be due in the past, got %+v", got)
	}

	// 待执行集合中的过期租约记录可被领取接管。
	taken, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 10, LeaseDurationMillis: 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if len(taken) != 2 {
		t.Fatalf("want due + expired claimed, got %+v", taken)
	}
}

// ---- 并发竞态压力测试 ----

// 触发与取消并发：无论锁顺序如何，结果必须是“触发赢”或“取消赢”之一，
// 且不变量成立：触发赢 => state=triggered 且恰有一条 outbox；
// 取消赢 => state=cancelled 且没有 outbox，确认返回失败。
func TestConcurrentConfirmVsCancel(t *testing.T) {
	const iter = 200
	var triggeredWins, cancelledWins int

	for i := 0; i < iter; i++ {
		store, clock := newTestStore()
		ctx := context.Background()
		id := fmt.Sprintf("T%d", i)
		mkTimer(t, store, clock, id, 0)

		var confirmErr, cancelErr error
		var claimed atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			d, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 1, LeaseDurationMillis: 600_000})
			if err != nil || len(d) != 1 {
				return // 取消先于领取提交：已无定时器可领。
			}
			claimed.Store(true)
			_, confirmErr = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
				TimerID: id, Version: d[0].Version, LeaseToken: d[0].LeaseToken,
				ResultPayload: "r",
			})
		}()
		go func() {
			defer wg.Done()
			_, cancelErr = store.CancelTimer(ctx, CancelParams{
				TimerID: id, RequestID: fmt.Sprintf("cancel-%d", i),
			})
		}()
		wg.Wait()

		got, err := store.GetTimer(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		pending, _ := store.ListOutboxPending(ctx, 10)

		switch got.State {
		case StateTriggered:
			triggeredWins++
			if cancelErr == nil {
				t.Fatalf("iter %d: timer triggered but cancel reported success", i)
			}
			mustKind(t, cancelErr, KindStateConflict)
			if confirmErr != nil {
				t.Fatalf("iter %d: timer triggered but confirm errored: %v", i, confirmErr)
			}
			if len(pending) != 1 || pending[0].Key != outboxKeyFor(id, 1) {
				t.Fatalf("iter %d: triggered timer must have exactly one outbox event, got %+v", i, pending)
			}
		case StateCancelled:
			cancelledWins++
			if cancelErr != nil {
				t.Fatalf("iter %d: cancel winner got error: %v", i, cancelErr)
			}
			// 只有领取确实发生了，确认才会被发起并失败；取消先于领取时
			// 领取为空（无可领），确认未发起，属于正常的取消赢顺序。
			if claimed.Load() {
				mustKind(t, confirmErr, KindStateConflict)
			}
			if len(pending) != 0 {
				t.Fatalf("iter %d: cancelled timer must have no outbox, got %+v", i, pending)
			}
		default:
			t.Fatalf("iter %d: unexpected terminal state %s", i, got.State)
		}
	}

	if triggeredWins == 0 || cancelledWins == 0 {
		t.Fatalf("stress test should observe both orderings: triggered=%d cancelled=%d",
			triggeredWins, cancelledWins)
	}
	t.Logf("confirm-vs-cancel orderings over %d iters: triggered=%d cancelled=%d",
		iter, triggeredWins, cancelledWins)
}

// 触发与重排并发：重排赢 => 旧版本确认 VersionConflict、无 outbox，新版本可触发；
// 触发赢 => 重排 StateConflict、恰一条 v1 outbox。
func TestConcurrentConfirmVsReschedule(t *testing.T) {
	const iter = 200
	var triggerWins, rescheduleWins int

	for i := 0; i < iter; i++ {
		store, clock := newTestStore()
		ctx := context.Background()
		id := fmt.Sprintf("T%d", i)
		mkTimer(t, store, clock, id, 0)

		var confirmErr, rescheduleErr error
		var claimed atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			d, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 1, LeaseDurationMillis: 600_000})
			if err != nil || len(d) != 1 {
				return // 重排先于领取提交：无可领取的旧版本。
			}
			claimed.Store(true)
			_, confirmErr = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
				TimerID: id, Version: d[0].Version, LeaseToken: d[0].LeaseToken,
				ResultPayload: "r",
			})
		}()
		go func() {
			defer wg.Done()
			_, rescheduleErr = store.RescheduleTimer(ctx, RescheduleParams{
				TimerID: id, RequestID: fmt.Sprintf("rs-%d", i),
				FireAtUnixMilli: clock.millis() + 60_000, Payload: "p2",
			})
		}()
		wg.Wait()

		got, _ := store.GetTimer(ctx, id)
		pending, _ := store.ListOutboxPending(ctx, 10)

		switch {
		case got.State == StateTriggered:
			triggerWins++
			mustKind(t, rescheduleErr, KindStateConflict)
			if confirmErr != nil || len(pending) != 1 || pending[0].Key != outboxKeyFor(id, 1) {
				t.Fatalf("iter %d: bad trigger-win outcome: confirmErr=%v outbox=%+v",
					i, confirmErr, pending)
			}
		case got.State == StateScheduled && got.Version == 2:
			rescheduleWins++
			if rescheduleErr != nil {
				t.Fatalf("iter %d: reschedule winner got error: %v", i, rescheduleErr)
			}
			// 重排先于领取时，领取为空、确认未发起；先领取后重排时，
			// 旧版本的确认必须收到 VersionConflict。
			if claimed.Load() {
				mustKind(t, confirmErr, KindVersionConflict)
			}
			if len(pending) != 0 {
				t.Fatalf("iter %d: rescheduled timer must have no v1 outbox, got %+v", i, pending)
			}
			if _, err := store.GetTriggerResult(ctx, id, 1); err == nil {
				t.Fatalf("iter %d: stale v1 confirm must not persist result", i)
			}
		default:
			t.Fatalf("iter %d: unexpected outcome state=%s version=%d", i, got.State, got.Version)
		}
	}

	if triggerWins == 0 || rescheduleWins == 0 {
		t.Fatalf("stress test should observe both orderings: trigger=%d reschedule=%d",
			triggerWins, rescheduleWins)
	}
	t.Logf("confirm-vs-reschedule orderings: trigger=%d reschedule=%d", triggerWins, rescheduleWins)
}

// 多个定时器（含“被接管的过期租约”）触发后，outbox 按提交顺序产生、
// 稳定键唯一；投递端逐条标记后待投递集合清空，模拟 at-least-once 传输。
func TestOutboxDeliveryLifecycle(t *testing.T) {
	store, clock := newTestStore()
	ctx := context.Background()

	mkTimer(t, store, clock, "A", 0)
	mkTimer(t, store, clock, "B", 0)
	// 第一次只领 1 条拿到 A（A、B 的 fire_at 相同，按定时器号 A 在前），
	// 第二次领取拿到 B。
	first, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 1, LeaseDurationMillis: 60_000})
	if err != nil || len(first) != 1 || first[0].TimerID != "A" {
		t.Fatalf("want A claimed first, got %v %+v", err, first)
	}
	d1 := first[0]
	d2, err := store.ClaimDue(ctx, ClaimParams{MaxBatch: 10, LeaseDurationMillis: 60_000})
	if err != nil || len(d2) != 1 || d2[0].TimerID != "B" {
		t.Fatalf("want B claimed second, got %v %+v", err, d2)
	}

	confirm := func(d DueTimer) {
		t.Helper()
		if _, err := store.ConfirmTrigger(ctx, ConfirmTriggerParams{
			TimerID: d.TimerID, Version: d.Version, LeaseToken: d.LeaseToken,
			RequestID: "trig-" + d.TimerID, ResultPayload: "r-" + d.TimerID,
		}); err != nil {
			t.Fatalf("confirm %s: %v", d.TimerID, err)
		}
	}
	// 逆序提交，验证 outbox 顺序按提交时刻而非定时器号。
	confirm(d2[0])
	confirm(d1)

	pending, err := store.ListOutboxPending(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].Key != "timer:B:v1" || pending[1].Key != "timer:A:v1" {
		t.Fatalf("outbox must preserve commit order, got %+v", pending)
	}

	// 模拟传输层重复确认：重复标记幂等；全部投递后 pending 为空。
	for _, e := range pending {
		if _, err := store.MarkOutboxDelivered(ctx, e.Key); err != nil {
			t.Fatal(err)
		}
	}
	if rest, _ := store.ListOutboxPending(ctx, 0); len(rest) != 0 {
		t.Fatalf("all delivered, pending should be empty, got %+v", rest)
	}

	// 重排后的新版本触发会生成新版本键，与旧版本 outbox 互不影响。
	mkTimer(t, store, clock, "C", 0)
	c1 := claimOne(t, store, time.Minute)
	if c1.TimerID != "C" {
		t.Fatalf("want C, got %s", c1.TimerID)
	}
	clock.advance(2 * time.Minute)
	if _, err := store.RescheduleTimer(ctx, RescheduleParams{
		TimerID: "C", RequestID: "rs-c",
		FireAtUnixMilli: clock.millis(), Payload: "p2",
	}); err != nil {
		t.Fatal(err)
	}
	// 旧租约的确认失败。
	_, err = store.ConfirmTrigger(ctx, ConfirmTriggerParams{
		TimerID: "C", Version: c1.Version, LeaseToken: c1.LeaseToken, ResultPayload: "old",
	})
	mustKind(t, err, KindVersionConflict)
	c2 := claimOne(t, store, time.Minute)
	if c2.Version != 2 {
		t.Fatalf("want v2 claim, got %d", c2.Version)
	}
	confirm(c2)
	rest, _ := store.ListOutboxPending(ctx, 0)
	if len(rest) != 1 || rest[0].Key != "timer:C:v2" {
		t.Fatalf("only the new version's outbox event should be pending, got %+v", rest)
	}
}
