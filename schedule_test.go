package workflowtimers

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func hourlySchedule(wf, id, reqID string, policy CatchUpPolicy, start time.Time) CreateScheduleRequest {
	return CreateScheduleRequest{
		WorkflowID: wf, ScheduleID: id, RequestID: reqID,
		Location:   "UTC",
		StartAt:    start,
		Recurrence: Recurrence{Kind: RecurEvery, Every: time.Hour},
		Policy:     policy,
		Payload:    []byte("p-" + id),
	}
}

func mustCreateSchedule(t *testing.T, svc *Service, req CreateScheduleRequest) *ScheduleWriteResult {
	t.Helper()
	r, err := svc.CreateSchedule(req)
	if err != nil {
		t.Fatalf("CreateSchedule(%s): %v", req.ScheduleID, err)
	}
	return r
}

// claimInstance 领取一个计划实例（要求队列中恰有一个可领取对象且为实例）。
func claimInstance(t *testing.T, svc *Service, owner string, ttl time.Duration) Claim {
	t.Helper()
	claims, err := svc.ClaimDue(owner, 10, ttl)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	for _, c := range claims {
		if c.Instance != nil {
			return c
		}
	}
	t.Fatalf("want a schedule instance claim, got %+v", claims)
	return Claim{}
}

func TestScheduleCreateAndVersionHistory(t *testing.T) {
	svc, _ := newTestService(t)
	start := base.Add(time.Hour)
	r := mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, start))
	if r.Version != 1 {
		t.Fatalf("want version 1, got %d", r.Version)
	}

	sch, err := svc.GetSchedule("wf1", "s1")
	if err != nil || sch.State != ScheduleActive || sch.CurrentVersion != 1 {
		t.Fatalf("schedule head: %+v, %v", sch, err)
	}

	// 请求号重放：同号同内容返回原版本。
	r2, err := svc.CreateSchedule(hourlySchedule("wf1", "s1", "req-1", CatchUpAll, start))
	if err != nil || r2.Version != 1 {
		t.Fatalf("replay: %+v, %v", r2, err)
	}
	// 同号不同内容冲突；计划号占用冲突。
	bad := hourlySchedule("wf1", "s1", "req-1", CatchUpAll, start.Add(time.Hour))
	if _, err := svc.CreateSchedule(bad); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want ErrRequestConflict, got %v", err)
	}
	dup := hourlySchedule("wf1", "s1", "req-2", CatchUpAll, start)
	if _, err := svc.CreateSchedule(dup); !errors.Is(err, ErrScheduleExists) {
		t.Fatalf("want ErrScheduleExists, got %v", err)
	}

	// 非法参数。
	cases := []CreateScheduleRequest{
		{WorkflowID: "wf1", ScheduleID: "bad1", RequestID: "x", Recurrence: Recurrence{Kind: RecurEvery, Every: 0}, Policy: CatchUpAll, StartAt: base},
		{WorkflowID: "wf1", ScheduleID: "bad2", RequestID: "x", Recurrence: Recurrence{Kind: RecurDaily, Hour: 24}, Policy: CatchUpAll, StartAt: base},
		{WorkflowID: "wf1", ScheduleID: "bad3", RequestID: "x", Recurrence: Recurrence{Kind: "EVERY2"}, Policy: CatchUpAll, StartAt: base},
		{WorkflowID: "wf1", ScheduleID: "bad4", RequestID: "x", Recurrence: Recurrence{Kind: RecurEvery, Every: time.Hour}, Policy: "WHAT", StartAt: base},
		{WorkflowID: "wf1", ScheduleID: "bad5", RequestID: "x", Location: "Mars/Olympus", Recurrence: Recurrence{Kind: RecurEvery, Every: time.Hour}, Policy: CatchUpAll, StartAt: base},
	}
	for i, c := range cases {
		if _, err := svc.CreateSchedule(c); !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("case %d: want ErrInvalidSchedule, got %v", i, err)
		}
	}
}

func TestScanGeneratesInstancesIdempotently(t *testing.T) {
	svc, clk := newTestService(t)
	// 第一个计划点在 base+1h，扫描时未到期。
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))

	n, err := svc.ScanInstances(0)
	if err != nil || n != 0 {
		t.Fatalf("scan before due: n=%d err=%v", n, err)
	}

	clk.Advance(time.Hour)
	n, _ = svc.ScanInstances(10)
	if n != 1 {
		t.Fatalf("want 1 generated, got %d", n)
	}
	// 多次扫描不重复生成。
	n, _ = svc.ScanInstances(10)
	if n != 0 {
		t.Fatalf("repeat scan must generate nothing, got %d", n)
	}
	insts := svc.ListInstances("wf1", "s1")
	if len(insts) != 1 {
		t.Fatalf("want 1 instance, got %d", len(insts))
	}
	in := insts[0]
	if in.Version != 1 || in.Seq != 1 || in.State != InstancePending {
		t.Fatalf("unexpected instance: %+v", in)
	}
	if !in.ScheduledAt.Equal(base.Add(time.Hour)) || !in.GeneratedAt.Equal(clk.Now()) {
		t.Fatalf("time fields: scheduled=%s generated=%s now=%s", in.ScheduledAt, in.GeneratedAt, clk.Now())
	}

	// 再推进两个周期：生成第 2、3 个实例，序号与幂等键稳定。
	clk.Advance(2 * time.Hour)
	n, _ = svc.ScanInstances(10)
	if n != 2 {
		t.Fatalf("want 2 more, got %d", n)
	}
	insts = svc.ListInstances("wf1", "s1")
	for i, in := range insts {
		wantKey := InstanceIdempotencyKey("wf1", "s1", 1, int64(i+1))
		if got := InstanceIdempotencyKey(in.WorkflowID, in.ScheduleID, in.Version, in.Seq); got != wantKey {
			t.Fatalf("instance key: want %s, got %s", wantKey, got)
		}
		if !in.ScheduledAt.Equal(base.Add(time.Duration(i+1) * time.Hour)) {
			t.Fatalf("instance %d scheduled at %s", i, in.ScheduledAt)
		}
	}
}

// TestCatchUpPoliciesAfterDowntime 服务长时间停机后恢复，按冻结策略计算缺失实例，
// 重复扫描不重复生成。
func TestCatchUpPoliciesAfterDowntime(t *testing.T) {
	// CATCH_UP_ALL：错过的 1h/2h/3h 三个点全部补齐。
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "all", "req-1", CatchUpAll, base.Add(time.Hour)))
	clk.Advance(3*time.Hour + time.Minute) // 停机到 base+3h01
	n, _ := svc.ScanInstances(100)
	if n != 3 {
		t.Fatalf("CATCH_UP_ALL: want 3, got %d", n)
	}
	got := svc.ListInstances("wf1", "all")
	for i, in := range got {
		if !in.ScheduledAt.Equal(base.Add(time.Duration(i+1) * time.Hour)) {
			t.Fatalf("all[%d] scheduled=%s", i, in.ScheduledAt)
		}
		if !in.GeneratedAt.Equal(clk.Now()) {
			t.Fatalf("all[%d] generated=%s want %s", i, in.GeneratedAt, clk.Now())
		}
	}
	n, _ = svc.ScanInstances(100)
	if n != 0 {
		t.Fatalf("CATCH_UP_ALL rescan dup: %d", n)
	}

	// CATCH_UP_LATEST：积压 3 个点只补最后一个（序号仍对齐第 3 槽）。
	svc2, clk2 := newTestService(t)
	mustCreateSchedule(t, svc2, hourlySchedule("wf2", "latest", "req-1", CatchUpLatest, base.Add(time.Hour)))
	clk2.Advance(3*time.Hour + time.Minute)
	n, _ = svc2.ScanInstances(100)
	if n != 1 {
		t.Fatalf("CATCH_UP_LATEST: want 1, got %d", n)
	}
	got = svc2.ListInstances("wf2", "latest")
	if got[0].Seq != 3 || !got[0].ScheduledAt.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("latest backlog: %+v", got[0])
	}
	// 恢复后正常运行：下一个周期照常生成，不被补触发影响。
	clk2.Advance(time.Hour - time.Minute) // base+4h00
	n, _ = svc2.ScanInstances(100)
	if n != 1 {
		t.Fatalf("CATCH_UP_LATEST next: want 1, got %d", n)
	}
	got = svc2.ListInstances("wf2", "latest")
	if got[1].Seq != 4 || !got[1].ScheduledAt.Equal(base.Add(4*time.Hour)) {
		t.Fatalf("latest next: %+v", got[1])
	}

	// SKIP：积压全部跳过，只生成恢复之后的点。
	svc3, clk3 := newTestService(t)
	mustCreateSchedule(t, svc3, hourlySchedule("wf3", "skip", "req-1", CatchUpSkip, base.Add(time.Hour)))
	clk3.Advance(3*time.Hour + time.Minute)
	n, _ = svc3.ScanInstances(100)
	if n != 0 {
		t.Fatalf("SKIP backlog: want 0, got %d", n)
	}
	if len(svc3.ListInstances("wf3", "skip")) != 0 {
		t.Fatalf("SKIP must not materialize missed slots")
	}
	// 游标已越过积压：base+4h 的点正常生成。
	clk3.Advance(time.Hour - time.Minute) // base+4h00
	n, _ = svc3.ScanInstances(100)
	if n != 1 {
		t.Fatalf("SKIP next: want 1, got %d", n)
	}
	got = svc3.ListInstances("wf3", "skip")
	if got[0].Seq != 4 || !got[0].ScheduledAt.Equal(base.Add(4*time.Hour)) {
		t.Fatalf("skip next: %+v", got[0])
	}
}

// TestPolicyFrozenOnVersion 补触发策略冻结在版本上：v1 积压未扫，
// 更新到 v2（改用 SKIP）后，v1 保留原策略；积压按各自冻结策略处理。
func TestPolicyFrozenOnVersion(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))

	// 停机两周期后更新计划：旧版本未完成实例作废，新版本用 SKIP、从更新时刻起算。
	clk.Advance(2*time.Hour + time.Minute)
	if _, err := svc.UpdateSchedule(UpdateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-2",
		Location: "UTC", StartAt: clk.Now(),
		Recurrence: Recurrence{Kind: RecurEvery, Every: time.Hour},
		Policy:     CatchUpSkip,
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	vers := svc.ListScheduleVersions("wf1", "s1")
	if len(vers) != 2 || vers[0].Policy != CatchUpAll || vers[1].Policy != CatchUpSkip {
		t.Fatalf("frozen versions: %+v", vers)
	}
	// 扫描只会处理当前版本 v2；v1 的错过点不会被补（也不会被翻出）。
	n, _ := svc.ScanInstances(100)
	if n != 0 {
		t.Fatalf("v2 cursor starts at update time (future slot): %d", n)
	}
	if len(svc.ListInstances("wf1", "s1")) != 0 {
		t.Fatalf("old version missed slots must not appear")
	}
}

// TestPausePreservesCommittedFires 暂停不影响已提交触发；恢复后从新的生效时间继续。
func TestPauseResume(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("generate first instance")
	}
	c := claimInstance(t, svc, "worker-a", time.Minute)
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, c.LeaseID, []byte("fired-1")); err != nil {
		t.Fatalf("confirm #1: %v", err)
	}

	// 再生成两个：一个领取未确认，一个未领取，然后暂停。
	clk.Advance(2 * time.Hour)
	if n, _ := svc.ScanInstances(10); n != 2 {
		t.Fatal("generate #2 #3")
	}
	c2 := claimInstance(t, svc, "worker-a", time.Minute) // 领取 #2
	if err := svc.PauseSchedule(PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-p"}); err != nil {
		t.Fatalf("pause: %v", err)
	}
	// 暂停幂等重放；重复暂停报已暂停。
	if err := svc.PauseSchedule(PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-p"}); err != nil {
		t.Fatalf("pause replay: %v", err)
	}
	if err := svc.PauseSchedule(PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-p2"}); !errors.Is(err, ErrScheduleAlreadyPaused) {
		t.Fatalf("want ErrScheduleAlreadyPaused, got %v", err)
	}
	// 扫描对暂停计划无效。
	if n, _ := svc.ScanInstances(10); n != 0 {
		t.Fatalf("paused scan: %d", n)
	}
	// 已提交的 #1 保留；#2/#3 未完成全部作废，旧租约的迟到确认被拒。
	insts := svc.ListInstances("wf1", "s1")
	states := map[int64]InstanceState{}
	for _, in := range insts {
		states[in.Seq] = in.State
	}
	if states[1] != InstanceFired || states[2] != InstanceSuperseded || states[3] != InstanceSuperseded {
		t.Fatalf("states after pause: %+v", states)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", c2.Instance.Version, c2.Instance.Seq, c2.LeaseID, nil); !errors.Is(err, ErrInstanceSuperseded) {
		t.Fatalf("stale confirm after pause: %v", err)
	}

	// 停机 3 个周期后恢复：开 v2，从恢复时刻继续，暂停期间不补。
	clk.Advance(3 * time.Hour)
	r, err := svc.ResumeSchedule(ResumeScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-r"})
	if err != nil || r.Version != 2 {
		t.Fatalf("resume: %+v, %v", r, err)
	}
	if n, _ := svc.ScanInstances(10); n != 0 {
		t.Fatalf("resume cursor aligns forward, no backlog: %d", n)
	}
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatalf("resumed schedule must generate again: %d", n)
	}
	insts = svc.ListInstances("wf1", "s1")
	last := insts[len(insts)-1]
	if last.Version != 2 || last.Seq != 1 || last.State != InstancePending {
		t.Fatalf("new version instance: %+v", last)
	}
	// v1 的触发历史仍可查。
	var firedHistory int
	for _, in := range insts {
		if in.Version == 1 && in.State == InstanceFired {
			firedHistory++
		}
	}
	if firedHistory != 1 {
		t.Fatalf("v1 fired history must be retained, got %d", firedHistory)
	}
}

// TestUpdateSupersedesOnlyOldVersion 更新只作废旧版本未完成实例，
// 新版本已生成的实例永不被删除；旧版本不再生成未来实例。
func TestUpdateVersionRaces(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #1")
	}
	c := claimInstance(t, svc, "worker-a", time.Minute)
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, c.LeaseID, []byte("ok")); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #2")
	}

	// 更新开 v2。
	r, err := svc.UpdateSchedule(UpdateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-2",
		Location: "UTC", StartAt: base.Add(3 * time.Hour),
		Recurrence: Recurrence{Kind: RecurEvery, Every: 2 * time.Hour},
		Policy:     CatchUpAll,
	})
	if err != nil || r.Version != 2 {
		t.Fatalf("update: %+v %v", r, err)
	}
	// 更新请求号重放返回新版本。
	if r2, err := svc.UpdateSchedule(UpdateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-2",
		Location: "UTC", StartAt: base.Add(3 * time.Hour),
		Recurrence: Recurrence{Kind: RecurEvery, Every: 2 * time.Hour},
		Policy:     CatchUpAll,
	}); err != nil || r2.Version != 2 {
		t.Fatalf("update replay: %+v %v", r2, err)
	}

	insts := svc.ListInstances("wf1", "s1")
	if insts[0].State != InstanceFired {
		t.Fatalf("committed fire preserved: %+v", insts[0])
	}
	if insts[1].State != InstanceSuperseded {
		t.Fatalf("old pending superseded: %+v", insts[1])
	}

	// 更新发生在 base+2h，v2 间隔 2h，strict 对齐后首槽为 base+4h。
	clk.Advance(2 * time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatalf("v2 gen: %d", n)
	}
	before := len(svc.ListInstances("wf1", "s1"))
	if n, _ := svc.ScanInstances(10); n != 0 {
		t.Fatalf("v2 rescan: %d", n)
	}
	if len(svc.ListInstances("wf1", "s1")) != before {
		t.Fatal("new version instances deleted by scan")
	}
}

// TestInstanceLeaseConfirmAndFailureDoesNotBlock 实例沿用租约与确认规则；
// 前一个实例触发失败/重试不阻塞下一个时间点。
func TestInstanceLeaseConfirmAndFailureDoesNotBlock(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))

	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #1")
	}
	c1 := claimInstance(t, svc, "worker-a", 10*time.Second)

	// 触发失败（worker 崩溃，未确认）。扫描独立推进：下一个时间点照常生成。
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #2 must not be blocked by #1 failure")
	}
	// 租约过期后 #1 可被接管重试，#2 也同时可领取。
	clk.Advance(time.Second)
	claims, err := svc.ClaimDue("worker-b", 10, time.Minute)
	if err != nil || len(claims) != 2 {
		t.Fatalf("reclaim both: %d claims, %v", len(claims), err)
	}
	// 旧租约迟到确认被拒。
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", c1.Instance.Version, c1.Instance.Seq, c1.LeaseID, nil); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale lease: %v", err)
	}

	// 先确认 #2，再重试 #1：后续时间点不被前序失败阻塞。
	var c1again, c2 Claim
	for _, c := range claims {
		if c.Instance.Seq == 2 {
			c2 = c
		} else {
			c1again = c
		}
	}
	rcpt, err := svc.ConfirmInstanceFire("wf1", "s1", c2.Instance.Version, 2, c2.LeaseID, []byte("two"))
	if err != nil {
		t.Fatalf("confirm #2: %v", err)
	}
	if rcpt.IdempotencyKey != InstanceIdempotencyKey("wf1", "s1", 1, 2) {
		t.Fatalf("key: %s", rcpt.IdempotencyKey)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, c1again.LeaseID, []byte("one-retried")); err != nil {
		t.Fatalf("retry #1: %v", err)
	}
	// 重复确认返回首次结果。
	dup, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 2, c2.LeaseID, []byte("ignored"))
	if err != nil || !dup.Duplicate || string(dup.Result) != "two" {
		t.Fatalf("dup confirm: %+v %v", dup, err)
	}

	// 查询能区分计划时间、实际生成时间与最终触发结果。
	insts := svc.ListInstances("wf1", "s1")
	for _, in := range insts {
		if in.State != InstanceFired || in.FiredAt.IsZero() {
			t.Fatalf("instance not fired: %+v", in)
		}
		if !in.FiredAt.After(in.GeneratedAt) && !in.FiredAt.Equal(in.GeneratedAt) {
			t.Fatalf("time order: %+v", in)
		}
	}
	// outbox 带实例元数据，每个实例恰好一条逻辑触发。
	var entries []OutboxEntry
	for _, e := range svc.ListOutbox("wf1") {
		if e.Kind == OutboxKindScheduleInstance {
			entries = append(entries, e)
			if e.ScheduleID != "s1" || e.ScheduledAt.IsZero() {
				t.Fatalf("outbox instance metadata: %+v", e)
			}
		}
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 instance outbox entries, got %d", len(entries))
	}
}

// TestDailyScheduleWithTimezone 跨时区（含夏令时切换）的每日计划。
func TestDailyScheduleWithTimezone(t *testing.T) {
	svc, clk := newTestService(t)
	loc, _ := time.LoadLocation("America/New_York")
	// 2026-11-01 当地 02:00 夏令时结束（EDT→EST）。构造跨过该切换的每日 09:00 计划。
	start := time.Date(2026, 10, 30, 9, 0, 0, 0, loc) // = 13:00 UTC (EDT)
	clk.now = start.Add(-24 * time.Hour)

	req := CreateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-1",
		Location:   "America/New_York",
		StartAt:    start,
		Recurrence: Recurrence{Kind: RecurDaily, Hour: 9, Minute: 0},
		Policy:     CatchUpAll,
	}
	mustCreateSchedule(t, svc, req)

	// 连续 4 天扫描：本地时刻恒为 09:00；UTC 偏移在 DST 当天变化（25h 间隔）。
	wantScheduled := []time.Time{
		time.Date(2026, 10, 30, 9, 0, 0, 0, loc),
		time.Date(2026, 10, 31, 9, 0, 0, 0, loc),
		time.Date(2026, 11, 1, 9, 0, 0, 0, loc), // EST，UTC 14:00
		time.Date(2026, 11, 2, 9, 0, 0, 0, loc),
	}
	for i := 0; i < 4; i++ {
		clk.now = wantScheduled[i]
		if n, err := svc.ScanInstances(10); err != nil || n != 1 {
			t.Fatalf("day %d: n=%d err=%v", i, n, err)
		}
	}
	insts := svc.ListInstances("wf1", "daily")
	if len(insts) != 4 {
		t.Fatalf("want 4 daily instances, got %d", len(insts))
	}
	for i, in := range insts {
		if !in.ScheduledAt.Equal(wantScheduled[i]) {
			t.Fatalf("day %d: want %s, got %s", i, wantScheduled[i], in.ScheduledAt)
		}
	}
	// DST 当天与前一天在 UTC 上相差 25 小时。
	if gap := insts[2].ScheduledAt.Sub(insts[1].ScheduledAt); gap != 25*time.Hour {
		t.Fatalf("DST gap: want 25h, got %s", gap)
	}
}

// TestScheduleFinishesAtEnd 越过 EndAt 后计划进入 FINISHED，不再生成。
func TestScheduleFinishesAtEnd(t *testing.T) {
	svc, clk := newTestService(t)
	end := base.Add(2 * time.Hour)
	req := hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base)
	req.EndAt = end
	mustCreateSchedule(t, svc, req)

	clk.Advance(3 * time.Hour)
	if n, _ := svc.ScanInstances(100); n != 3 {
		t.Fatalf("want 3 within end (base, +1h, +2h), got %d", n)
	}
	sch, _ := svc.GetSchedule("wf1", "s1")
	if sch.State != ScheduleFinished {
		t.Fatalf("want FINISHED, got %s", sch.State)
	}
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(100); n != 0 {
		t.Fatalf("finished scan: %d", n)
	}
	if _, err := svc.UpdateSchedule(UpdateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "s1", RequestID: "req-2",
		Location: "UTC", StartAt: clk.Now(),
		Recurrence: Recurrence{Kind: RecurEvery, Every: time.Hour}, Policy: CatchUpAll,
	}); !errors.Is(err, ErrScheduleNotActive) {
		t.Fatalf("update finished: want ErrScheduleNotActive, got %v", err)
	}
}

// TestScheduleFileStorePersistence 重启后计划头、冻结版本、游标、实例与
// outbox 全部恢复；扫描继续且不重复生成。
func TestScheduleFileStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timers.json")
	clk := newTestClock()
	svc, err := NewService(NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	svc = svc.WithClock(clk.Now)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #1")
	}
	c := claimInstance(t, svc, "worker-a", time.Minute)
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, c.LeaseID, []byte("done")); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	clk.Advance(time.Hour)
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #2")
	}

	svc2, err := NewService(NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	svc2 = svc2.WithClock(clk.Now)
	sch, err := svc2.GetSchedule("wf1", "s1")
	if err != nil || sch.CurrentVersion != 1 || sch.State != ScheduleActive {
		t.Fatalf("restored schedule: %+v %v", sch, err)
	}
	insts := svc2.ListInstances("wf1", "s1")
	if len(insts) != 2 || insts[0].State != InstanceFired || insts[1].State != InstancePending {
		t.Fatalf("restored instances: %+v", insts)
	}
	// 游标恢复：重复扫描不产生重复实例，继续推进。
	if n, _ := svc2.ScanInstances(10); n != 0 {
		t.Fatalf("rescan after restart dup: %d", n)
	}
	clk.Advance(time.Hour)
	if n, _ := svc2.ScanInstances(10); n != 1 {
		t.Fatalf("scan continues after restart: %d", n)
	}
	var instEntries int
	for _, e := range svc2.ListOutbox("wf1") {
		if e.Kind == OutboxKindScheduleInstance {
			instEntries++
		}
	}
	if instEntries != 1 {
		t.Fatalf("restored instance outbox: %d", instEntries)
	}
}

// TestCatchUpEndsDuringDowntime 停机期间计划越过 EndAt：恢复扫描以 EndAt 为上界，
// 三种策略都不会生成 EndAt 之后的实例，且计划最终 FINISHED。
func TestCatchUpEndsDuringDowntime(t *testing.T) {
	end := base.Add(2 * time.Hour)
	mk := func(policy CatchUpPolicy) (*Service, *testClock) {
		svc, clk := newTestService(t)
		req := hourlySchedule("wf1", "s1", "req-1", policy, base)
		req.EndAt = end
		mustCreateSchedule(t, svc, req)
		clk.Advance(5 * time.Hour) // 停机时计划早已结束
		return svc, clk
	}

	for _, tc := range []struct {
		policy CatchUpPolicy
		want   int
	}{
		{CatchUpAll, 3},    // base, +1h, +2h
		{CatchUpLatest, 1}, // 只补 EndAt 槽
		{CatchUpSkip, 0},   // 积压全部跳过
	} {
		svc, _ := mk(tc.policy)
		n, err := svc.ScanInstances(100)
		if err != nil {
			t.Fatalf("%s scan: %v", tc.policy, err)
		}
		if n != tc.want {
			t.Fatalf("%s: want %d instances, got %d", tc.policy, tc.want, n)
		}
		sch, _ := svc.GetSchedule("wf1", "s1")
		if sch.State != ScheduleFinished {
			t.Fatalf("%s: want FINISHED, got %s", tc.policy, sch.State)
		}
		for _, in := range svc.ListInstances("wf1", "s1") {
			if in.ScheduledAt.After(end) {
				t.Fatalf("%s: instance beyond end: %s", tc.policy, in.ScheduledAt)
			}
		}
	}
}

// TestScheduleErrorPaths 计划操作的错误分支与实例确认守卫。
func TestScheduleErrorPaths(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base))

	// 更新/暂停不存在的计划。
	_, err := svc.UpdateSchedule(UpdateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "nope", RequestID: "x-u",
		Location: "UTC", StartAt: clk.Now(),
		Recurrence: Recurrence{Kind: RecurEvery, Every: time.Hour}, Policy: CatchUpAll,
	})
	if !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("update missing: %v", err)
	}
	if err := svc.PauseSchedule(PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "nope", RequestID: "x-p"}); !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("pause missing: %v", err)
	}
	// 对 ACTIVE 计划恢复。
	if _, err := svc.ResumeSchedule(ResumeScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: "x-r"}); !errors.Is(err, ErrScheduleNotActive) {
		t.Fatalf("resume active: %v", err)
	}
	// end < start。
	bad := hourlySchedule("wf1", "s2", "req-2", CatchUpAll, base)
	bad.EndAt = base.Add(-time.Hour)
	if _, err := svc.CreateSchedule(bad); !errors.Is(err, ErrInvalidSchedule) {
		t.Fatalf("end<start: %v", err)
	}
	// 每日 09:00 但当天 10:00 才开始、当天结束：首个时间点落在次日，超出 EndAt。
	bad2 := CreateScheduleRequest{
		WorkflowID: "wf1", ScheduleID: "s3", RequestID: "req-3",
		Location:   "UTC",
		StartAt:    time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC),
		EndAt:      time.Date(2026, 9, 27, 23, 0, 0, 0, time.UTC),
		Recurrence: Recurrence{Kind: RecurDaily, Hour: 9},
		Policy:     CatchUpAll,
	}
	if _, err := svc.CreateSchedule(bad2); !errors.Is(err, ErrInvalidSchedule) {
		t.Fatalf("no occurrence: %v", err)
	}

	// 实例确认守卫：不存在、未领取、租约过期。
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen #1")
	}
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 99, "lease-x", nil); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("confirm missing instance: %v", err)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, "lease-x", nil); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("confirm unclaimed: %v", err)
	}
	c := claimInstance(t, svc, "worker-a", time.Second)
	clk.Advance(2 * time.Second)
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, c.LeaseID, nil); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("confirm expired: %v", err)
	}

	// FINISHED 计划不能更新或暂停。
	svc2, clk2 := newTestService(t)
	req := hourlySchedule("wf1", "f", "req-1", CatchUpAll, base)
	req.EndAt = base
	mustCreateSchedule(t, svc2, req)
	clk2.Advance(time.Minute)
	if n, _ := svc2.ScanInstances(10); n != 1 {
		t.Fatal("gen last")
	}
	if err := svc2.PauseSchedule(PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "f", RequestID: "p"}); !errors.Is(err, ErrScheduleNotActive) {
		t.Fatalf("pause finished: %v", err)
	}
}

// TestScanBudgetPartial 大积压受 limit 限制分批生成，游标精确续接、不漏不重。
func TestScanBudgetPartial(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-1", CatchUpAll, base.Add(time.Hour)))
	clk.Advance(5*time.Hour + time.Minute) // 5 个到期点

	n, _ := svc.ScanInstances(2)
	if n != 2 {
		t.Fatalf("batch1: want 2, got %d", n)
	}
	n, _ = svc.ScanInstances(2)
	if n != 2 {
		t.Fatalf("batch2: want 2, got %d", n)
	}
	n, _ = svc.ScanInstances(10)
	if n != 1 {
		t.Fatalf("batch3: want 1, got %d", n)
	}
	n, _ = svc.ScanInstances(10)
	if n != 0 {
		t.Fatalf("exhausted scan must be empty: %d", n)
	}
	insts := svc.ListInstances("wf1", "s1")
	if len(insts) != 5 {
		t.Fatalf("want 5 instances, got %d", len(insts))
	}
	for i, in := range insts {
		if in.Seq != int64(i+1) || !in.ScheduledAt.Equal(base.Add(time.Duration(i+1)*time.Hour)) {
			t.Fatalf("instance %d: %+v", i, in)
		}
	}
}

// TestMixedTimerAndInstanceClaim 一次性定时器与计划实例统一排队，
// 按到期时间排序，各自的确认路径互不干扰。
func TestMixedTimerAndInstanceClaim(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, hourlySchedule("wf1", "s1", "req-s", CatchUpAll, base))
	mustCreate(t, svc, "wf1", "tmr", "req-t", base.Add(30*time.Minute))
	if n, _ := svc.ScanInstances(10); n != 1 {
		t.Fatal("gen instance at base")
	}
	clk.Advance(30 * time.Minute) // 定时器到期；下一个计划点（base+1h）未到期

	claims, err := svc.ClaimDue("worker-a", 10, time.Minute)
	if err != nil || len(claims) != 2 {
		t.Fatalf("claims: %d, %v", len(claims), err)
	}
	// 到期时间排序：实例 ScheduledAt=base 在前，定时器 FireAt=base+30m 在后。
	if claims[0].Instance == nil || claims[0].Instance.ScheduleID != "s1" {
		t.Fatalf("first claim want schedule instance: %+v", claims[0])
	}
	if claims[1].Timer == nil || claims[1].Timer.TimerID != "tmr" {
		t.Fatalf("second claim want one-shot timer: %+v", claims[1])
	}
	// 各走各的确认。
	if _, err := svc.ConfirmInstanceFire("wf1", "s1", 1, 1, claims[0].LeaseID, []byte("inst")); err != nil {
		t.Fatalf("confirm instance: %v", err)
	}
	if _, err := svc.ConfirmFire("wf1", "tmr", 1, claims[1].LeaseID, []byte("tmr")); err != nil {
		t.Fatalf("confirm timer: %v", err)
	}
	if got := len(svc.ListOutbox("wf1")); got != 2 {
		t.Fatalf("want 2 outbox entries, got %d", got)
	}
}

// TestScheduleConcurrentRace 扫描/更新/暂停/恢复/领取/确认并发压测。
// 不变量：幂等键唯一；旧版本不再有 PENDING 实例；FIRED 实例与 outbox 永不变。
func TestScheduleConcurrentRace(t *testing.T) {
	svc, clk := newTestService(t)
	req := hourlySchedule("wf1", "s1", "req-create", CatchUpLatest, base)
	req.Recurrence.Every = 5 * time.Millisecond
	mustCreateSchedule(t, svc, req)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 时钟持续推进，模拟停机恢复与租约过期交织。
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				clk.Advance(5 * time.Millisecond)
			}
		}
	}()

	// 扫描器。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = svc.ScanInstances(20)
			}
		}()
	}
	// 领取 + 确认。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				claims, err := svc.ClaimDue(owner, 10, 50*time.Millisecond)
				if err != nil {
					return
				}
				for _, c := range claims {
					if c.Instance != nil {
						_, _ = svc.ConfirmInstanceFire(c.Instance.WorkflowID, c.Instance.ScheduleID,
							c.Instance.Version, c.Instance.Seq, c.LeaseID, []byte("done"))
					} else if c.Timer != nil {
						_, _ = svc.ConfirmFire(c.Timer.WorkflowID, c.Timer.TimerID, c.Timer.Version, c.LeaseID, nil)
					}
				}
			}
		}(fmt.Sprintf("worker-%d", w))
	}
	// 更新 / 暂停 / 恢复 交替。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if i%2 == 0 {
				_, _ = svc.UpdateSchedule(UpdateScheduleRequest{
					WorkflowID: "wf1", ScheduleID: "s1", RequestID: fmt.Sprintf("req-u-%d", i),
					Location: "UTC", StartAt: clk.Now(),
					Recurrence: Recurrence{Kind: RecurEvery, Every: 5 * time.Millisecond},
					Policy:     CatchUpLatest,
				})
			} else {
				_ = svc.PauseSchedule(PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: fmt.Sprintf("req-p-%d", i)})
				_, _ = svc.ResumeSchedule(ResumeScheduleRequest{WorkflowID: "wf1", ScheduleID: "s1", RequestID: fmt.Sprintf("req-r-%d", i)})
			}
		}
	}()

	// 跑一小段时间后收束。
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	sch, err := svc.GetSchedule("wf1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	insts := svc.ListInstances("wf1", "s1")
	keys := map[string]bool{}
	for _, in := range insts {
		key := InstanceIdempotencyKey(in.WorkflowID, in.ScheduleID, in.Version, in.Seq)
		if keys[key] {
			t.Fatalf("duplicate instance key %s", key)
		}
		keys[key] = true
		// 旧版本不得残留 PENDING（旋转版本时全部作废，之后也不再生成）。
		if in.Version < sch.CurrentVersion && in.State == InstancePending {
			t.Fatalf("old version %d still has PENDING instance #%d", in.Version, in.Seq)
		}
		// FIRED 是不可撤销历史：必然有匹配 outbox，结果一致。
		if in.State == InstanceFired {
			e, ok := svc.state.Outbox[key]
			if !ok {
				t.Fatalf("fired instance without outbox: %s", key)
			}
			if string(e.Result) != "done" {
				t.Fatalf("outbox result mismatch: %s", e.Result)
			}
		}
	}
}
