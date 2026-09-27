package workflowtimers

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var base = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

// testClock 可手动推进的时钟。
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock { return &testClock{now: base} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestService(t *testing.T) (*Service, *testClock) {
	t.Helper()
	clk := newTestClock()
	svc, err := NewService(&MemoryStore{})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc.WithClock(clk.Now), clk
}

func mustCreate(t *testing.T, svc *Service, wf, timerID, reqID string, fireAt time.Time) *WriteResult {
	t.Helper()
	res, err := svc.CreateTimer(CreateRequest{
		WorkflowID: wf, TimerID: timerID, RequestID: reqID, FireAt: fireAt,
	})
	if err != nil {
		t.Fatalf("CreateTimer(%s): %v", timerID, err)
	}
	return res
}

func mustClaimOne(t *testing.T, svc *Service, owner string, ttl time.Duration) Claim {
	t.Helper()
	claims, err := svc.ClaimDue(owner, 10, ttl)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("ClaimDue: want 1 claim, got %d", len(claims))
	}
	return claims[0]
}

func TestCreateIdempotentReplay(t *testing.T) {
	svc, _ := newTestService(t)
	r1 := mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(time.Hour))

	// 同号同内容重试：返回原结果。
	r2, err := svc.CreateTimer(CreateRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-1", FireAt: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("replay create: %v", err)
	}
	if *r1 != *r2 {
		t.Fatalf("replay mismatch: %+v vs %+v", r1, r2)
	}

	// 同号不同内容：幂等冲突。
	_, err = svc.CreateTimer(CreateRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-1", FireAt: base.Add(2 * time.Hour),
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want ErrRequestConflict, got %v", err)
	}

	// 不同请求号但定时器号已存在。
	_, err = svc.CreateTimer(CreateRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2", FireAt: base.Add(time.Hour),
	})
	if !errors.Is(err, ErrTimerExists) {
		t.Fatalf("want ErrTimerExists, got %v", err)
	}
}

func TestRescheduleIncrementsVersionAndReplays(t *testing.T) {
	svc, _ := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(time.Hour))

	r1, err := svc.RescheduleTimer(RescheduleRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2", FireAt: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	if r1.Version != 2 {
		t.Fatalf("want version 2, got %d", r1.Version)
	}

	// 同号同内容重试返回原版本。
	r2, err := svc.RescheduleTimer(RescheduleRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2", FireAt: base.Add(2 * time.Hour),
	})
	if err != nil || r2.Version != 2 {
		t.Fatalf("replay reschedule: %+v, %v", r2, err)
	}

	// 同号不同内容报冲突。
	_, err = svc.RescheduleTimer(RescheduleRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2", FireAt: base.Add(3 * time.Hour),
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want ErrRequestConflict, got %v", err)
	}

	tm, _ := svc.GetTimer("wf1", "t1")
	if tm.Version != 2 || !tm.FireAt.Equal(base.Add(2*time.Hour)) {
		t.Fatalf("unexpected timer: %+v", tm)
	}
}

func TestClaimBatchAndLeaseExclusion(t *testing.T) {
	svc, clk := newTestService(t)
	for i := 0; i < 3; i++ {
		mustCreate(t, svc, "wf1", fmt.Sprintf("t%d", i), fmt.Sprintf("req-%d", i), base.Add(-time.Minute))
	}
	mustCreate(t, svc, "wf1", "future", "req-f", base.Add(time.Hour)) // 未到期

	// 批量领取，limit=2。
	claims, err := svc.ClaimDue("worker-a", 2, 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claims) != 2 {
		t.Fatalf("want 2 claims, got %d", len(claims))
	}
	if claims[0].LeaseID == "" || claims[0].Timer.Lease == nil {
		t.Fatal("claim missing lease")
	}

	// 租约有效期内，其他领取者只能拿到剩余 1 个。
	claims2, _ := svc.ClaimDue("worker-b", 10, 30*time.Second)
	if len(claims2) != 1 {
		t.Fatalf("want 1 claim for worker-b, got %d", len(claims2))
	}

	// 租约过期后可被接管，LeaseID 必须更新。
	clk.Advance(31 * time.Second)
	claims3, _ := svc.ClaimDue("worker-b", 10, 30*time.Second)
	if len(claims3) != 3 {
		t.Fatalf("want 3 reclaimed, got %d", len(claims3))
	}
	for _, c := range claims3 {
		if c.LeaseID == claims[0].LeaseID || c.LeaseID == claims[1].LeaseID {
			t.Fatal("reclaim must issue a new lease id")
		}
	}
}

func TestConfirmFireAtomicResultAndOutbox(t *testing.T) {
	svc, _ := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))
	c := mustClaimOne(t, svc, "worker-a", time.Minute)

	rcpt, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, []byte("done"))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if rcpt.Duplicate {
		t.Fatal("first confirm must not be duplicate")
	}
	wantKey := IdempotencyKey("wf1", "t1", 1)
	if rcpt.IdempotencyKey != wantKey {
		t.Fatalf("idempotency key: want %s, got %s", wantKey, rcpt.IdempotencyKey)
	}

	// 传输层重复确认：返回首次结果，不产生第二次逻辑触发。
	rcpt2, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, []byte("done-again"))
	if err != nil {
		t.Fatalf("duplicate confirm: %v", err)
	}
	if !rcpt2.Duplicate || string(rcpt2.Result) != "done" {
		t.Fatalf("duplicate confirm: %+v", rcpt2)
	}
	outbox := svc.ListOutbox("wf1")
	if len(outbox) != 1 || string(outbox[0].Result) != "done" {
		t.Fatalf("outbox must contain exactly one logical fire: %+v", outbox)
	}

	tm, _ := svc.GetTimer("wf1", "t1")
	if tm.State != StateFired || string(tm.Result) != "done" {
		t.Fatalf("timer state: %+v", tm)
	}
}

func TestStaleLeaseConfirmCannotAffectNewOwner(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))

	old := mustClaimOne(t, svc, "worker-a", 10*time.Second)
	clk.Advance(11 * time.Second) // 旧租约过期
	taken := mustClaimOne(t, svc, "worker-b", time.Minute)

	// 旧租约的迟到确认被拒绝。
	_, err := svc.ConfirmFire("wf1", "t1", old.Timer.Version, old.LeaseID, []byte("stale"))
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("want ErrLeaseMismatch, got %v", err)
	}

	// 接管者正常触发。
	if _, err := svc.ConfirmFire("wf1", "t1", taken.Timer.Version, taken.LeaseID, []byte("ok")); err != nil {
		t.Fatalf("new owner confirm: %v", err)
	}
	if got := len(svc.ListOutbox("wf1")); got != 1 {
		t.Fatalf("outbox entries: %d", got)
	}
}

func TestExpiredLeaseConfirmRejected(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))
	c := mustClaimOne(t, svc, "worker-a", 5*time.Second)
	clk.Advance(6 * time.Second)

	_, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, []byte("late"))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("want ErrLeaseExpired, got %v", err)
	}
}

func TestRescheduleRacesClaimAndConfirm(t *testing.T) {
	svc, _ := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))
	c := mustClaimOne(t, svc, "worker-a", time.Minute)

	// 领取后、确认前发生重排：新版本先提交，旧版本触发必须失败。
	if _, err := svc.RescheduleTimer(RescheduleRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2", FireAt: base.Add(-time.Second),
	}); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	_, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, nil)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}

	// 旧租约同时被作废：即使伪造版本 2，旧 LeaseID 也不被接受。
	_, err = svc.ConfirmFire("wf1", "t1", 2, c.LeaseID, nil)
	if !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("want ErrNotClaimed, got %v", err)
	}

	// 新版本可被重新领取并触发。
	c2 := mustClaimOne(t, svc, "worker-b", time.Minute)
	if c2.Timer.Version != 2 {
		t.Fatalf("want version 2, got %d", c2.Timer.Version)
	}
	if _, err := svc.ConfirmFire("wf1", "t1", 2, c2.LeaseID, []byte("v2")); err != nil {
		t.Fatalf("confirm v2: %v", err)
	}
	if got := len(svc.ListOutbox("wf1")); got != 1 {
		t.Fatalf("only one logical fire allowed, outbox=%d", got)
	}
}

func TestCancelRacesClaimAndConfirm(t *testing.T) {
	svc, _ := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))
	c := mustClaimOne(t, svc, "worker-a", time.Minute)

	// 取消先提交：旧版本确认因状态为 CANCELED 而失败。
	if err := svc.CancelTimer(CancelRequest{WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	_, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, nil)
	if !errors.Is(err, ErrAlreadyCanceled) {
		t.Fatalf("want ErrAlreadyCanceled, got %v", err)
	}

	// 取消幂等重放。
	if err := svc.CancelTimer(CancelRequest{WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2"}); err != nil {
		t.Fatalf("replay cancel: %v", err)
	}
	// 已取消的定时器不能再取消/重排。
	if err := svc.CancelTimer(CancelRequest{WorkflowID: "wf1", TimerID: "t1", RequestID: "req-3"}); !errors.Is(err, ErrAlreadyCanceled) {
		t.Fatalf("want ErrAlreadyCanceled, got %v", err)
	}
	if p := svc.ListPending("wf1"); len(p) != 0 {
		t.Fatalf("pending: %+v", p)
	}
}

func TestCommittedFireCannotBeRevoked(t *testing.T) {
	svc, _ := newTestService(t)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))
	c := mustClaimOne(t, svc, "worker-a", time.Minute)
	if _, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, []byte("done")); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// 已提交的触发不能撤销：取消与重排都失败。
	if err := svc.CancelTimer(CancelRequest{WorkflowID: "wf1", TimerID: "t1", RequestID: "req-2"}); !errors.Is(err, ErrAlreadyFired) {
		t.Fatalf("cancel after fire: want ErrAlreadyFired, got %v", err)
	}
	if _, err := svc.RescheduleTimer(RescheduleRequest{
		WorkflowID: "wf1", TimerID: "t1", RequestID: "req-3", FireAt: base.Add(time.Hour),
	}); !errors.Is(err, ErrAlreadyFired) {
		t.Fatalf("reschedule after fire: want ErrAlreadyFired, got %v", err)
	}
}

func TestListPendingAndOutboxQueries(t *testing.T) {
	svc, _ := newTestService(t)
	mustCreate(t, svc, "wf1", "a", "req-a", base.Add(2*time.Hour))
	mustCreate(t, svc, "wf1", "b", "req-b", base.Add(time.Hour))
	mustCreate(t, svc, "wf2", "c", "req-c", base.Add(time.Hour))

	p := svc.ListPending("wf1")
	if len(p) != 2 || p[0].TimerID != "b" || p[1].TimerID != "a" {
		t.Fatalf("pending order: %+v", p)
	}
	if got := len(svc.ListOutbox("wf1")); got != 0 {
		t.Fatalf("outbox before fire: %d", got)
	}
}

func TestFileStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timers.json")
	clk := newTestClock()

	svc, err := NewService(NewFileStore(path))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc = svc.WithClock(clk.Now)
	mustCreate(t, svc, "wf1", "t1", "req-1", base.Add(-time.Minute))
	c := mustClaimOne(t, svc, "worker-a", time.Minute)
	if _, err := svc.ConfirmFire("wf1", "t1", c.Timer.Version, c.LeaseID, []byte("done")); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	mustCreate(t, svc, "wf1", "t2", "req-2", base.Add(time.Hour))

	// 重新打开：定时器、outbox 与请求号记录全部恢复。
	svc2, err := NewService(NewFileStore(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	tm, err := svc2.GetTimer("wf1", "t1")
	if err != nil || tm.State != StateFired || string(tm.Result) != "done" {
		t.Fatalf("restored timer: %+v, %v", tm, err)
	}
	if got := len(svc2.ListOutbox("wf1")); got != 1 {
		t.Fatalf("restored outbox: %d", got)
	}
	// 请求号重放在重启后仍返回原结果。
	r, err := svc2.CreateTimer(CreateRequest{
		WorkflowID: "wf1", TimerID: "t2", RequestID: "req-2", FireAt: base.Add(time.Hour),
	})
	if err != nil || r.Version != 1 {
		t.Fatalf("replay after restart: %+v, %v", r, err)
	}
	if _, err := svc2.CreateTimer(CreateRequest{
		WorkflowID: "wf1", TimerID: "t2", RequestID: "req-2", FireAt: base.Add(2 * time.Hour),
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict after restart: %v", err)
	}
}

// TestConcurrentRace 并发压测领取/确认/重排/取消的竞态：
// 无论交织顺序如何，每个定时器版本至多产生一条 outbox 记录，
// 且定时器最终停在某个终态或待执行态。配合 go test -race 运行。
func TestConcurrentRace(t *testing.T) {
	svc, _ := newTestService(t)
	const timers = 20
	for i := 0; i < timers; i++ {
		id := fmt.Sprintf("t%d", i)
		mustCreate(t, svc, "wf1", id, "req-create-"+id, base.Add(-time.Minute))
	}

	var wg sync.WaitGroup
	for i := 0; i < timers; i++ {
		id := fmt.Sprintf("t%d", i)
		// 多个 worker 并发领取并确认。
		for w := 0; w < 3; w++ {
			wg.Add(1)
			go func(owner string) {
				defer wg.Done()
				for {
					claims, err := svc.ClaimDue(owner, 5, 50*time.Millisecond)
					if err != nil || len(claims) == 0 {
						return
					}
					for _, c := range claims {
						_, _ = svc.ConfirmFire("wf1", c.Timer.TimerID, c.Timer.Version, c.LeaseID, []byte("done"))
					}
				}
			}(fmt.Sprintf("worker-%d", w))
		}
		// 并发重排与取消。
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = svc.RescheduleTimer(RescheduleRequest{
				WorkflowID: "wf1", TimerID: id, RequestID: "req-resched-" + id, FireAt: base.Add(-time.Second),
			})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.CancelTimer(CancelRequest{WorkflowID: "wf1", TimerID: id, RequestID: "req-cancel-" + id})
		}()
	}
	wg.Wait()

	// 不变量：每个定时器版本至多一条 outbox 记录；已触发定时器的结果与 outbox 一致。
	outbox := svc.ListOutbox("wf1")
	seen := make(map[string]bool)
	for _, e := range outbox {
		if seen[e.IdempotencyKey] {
			t.Fatalf("duplicate logical fire for key %s", e.IdempotencyKey)
		}
		seen[e.IdempotencyKey] = true
		tm, err := svc.GetTimer(e.WorkflowID, e.TimerID)
		if err != nil {
			t.Fatalf("get timer: %v", err)
		}
		if tm.State != StateFired && tm.Version == e.Version {
			t.Fatalf("outbox entry without fired timer: %+v vs %+v", e, tm)
		}
	}
}
