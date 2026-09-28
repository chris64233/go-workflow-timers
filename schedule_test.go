package workflowtimers

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func dailySpec(start time.Time, policy MissedPolicy) ScheduleSpec {
	return ScheduleSpec{
		Timezone:     "UTC",
		StartAt:      start,
		RRULE:        "FREQ=DAILY",
		MissedPolicy: policy,
		Payload:      []byte("p"),
	}
}

func mustCreateSchedule(t *testing.T, svc *Service, wf, id, reqID string, spec ScheduleSpec) *WriteResult {
	t.Helper()
	r, err := svc.CreateSchedule(CreateScheduleRequest{WorkflowID: wf, ScheduleID: id, RequestID: reqID, Spec: spec})
	if err != nil {
		t.Fatalf("CreateSchedule(%s): %v", id, err)
	}
	return r
}

func mustScan(t *testing.T, svc *Service, lateness time.Duration) *ScanReport {
	t.Helper()
	r, err := svc.ScanSchedules(lateness)
	if err != nil {
		t.Fatalf("ScanSchedules: %v", err)
	}
	return r
}

func TestRRULEEnumerationAndStableSeq(t *testing.T) {
	loc := time.UTC
	anchor := time.Date(2026, 9, 27, 12, 0, 0, 0, loc) // 周日

	cases := []struct {
		name    string
		rrule   string
		lo      time.Duration
		hi      time.Duration
		wantAt  []time.Duration // 相对 anchor 的偏移
		wantSeq []int64
	}{
		{
			name:    "hourly fast forward",
			rrule:   "FREQ=HOURLY;INTERVAL=6",
			lo:      100 * time.Hour,
			hi:      205 * time.Hour,
			wantAt:  []time.Duration{102 * time.Hour, 108 * time.Hour, 114 * time.Hour, 120 * time.Hour, 126 * time.Hour, 132 * time.Hour, 138 * time.Hour, 144 * time.Hour, 150 * time.Hour, 156 * time.Hour, 162 * time.Hour, 168 * time.Hour, 174 * time.Hour, 180 * time.Hour, 186 * time.Hour, 192 * time.Hour, 198 * time.Hour, 204 * time.Hour},
			wantSeq: []int64{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34},
		},
		{
			name:    "daily with byday filter",
			rrule:   "FREQ=DAILY;BYDAY=MO,WE",
			lo:      0,
			hi:      72 * time.Hour,
			wantAt:  []time.Duration{24 * time.Hour, 72 * time.Hour}, // 周一 9/28、周三 9/30
			wantSeq: []int64{1, 3},
		},
		{
			name:    "weekly byday",
			rrule:   "FREQ=WEEKLY;BYDAY=MO,WE",
			lo:      0,
			hi:      96 * time.Hour,
			wantAt:  []time.Duration{24 * time.Hour, 72 * time.Hour}, // 周一 9/28、周三 9/30
			wantSeq: []int64{2, 3},
		},
		{
			name:    "weekly interval 2 default byday anchor weekday",
			rrule:   "FREQ=WEEKLY;INTERVAL=2",
			lo:      0,
			hi:      24 * 20 * time.Hour,
			wantAt:  []time.Duration{14 * 24 * time.Hour}, // 下一个周日 10/11
			wantSeq: []int64{1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := parseRRULE(tc.rrule, anchor)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got, err := rec.between(anchor.Add(tc.lo), anchor.Add(tc.hi))
			if err != nil {
				t.Fatalf("between: %v", err)
			}
			if len(got) != len(tc.wantAt) {
				t.Fatalf("want %d occurrences, got %d: %+v", len(tc.wantAt), len(got), got)
			}
			for i, o := range got {
				if !o.at.Equal(anchor.Add(tc.wantAt[i])) || o.seq != tc.wantSeq[i] {
					t.Fatalf("[%d] want (+%v, seq=%d), got (%s, seq=%d)", i, tc.wantAt[i], tc.wantSeq[i], o.at.Sub(anchor), o.seq)
				}
			}
		})
	}
}

func TestRRULEMonthlySkipsShortMonth(t *testing.T) {
	loc := time.UTC
	anchor := time.Date(2026, 1, 31, 9, 0, 0, 0, loc)
	rec, err := parseRRULE("FREQ=MONTHLY;BYMONTHDAY=31", anchor)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rec.between(anchor.Add(-time.Second), time.Date(2026, 5, 1, 0, 0, 0, 0, loc))
	if err != nil {
		t.Fatal(err)
	}
	// 1/31、3/31，2 月与 4 月无 31 日。
	wantMonths := []time.Month{time.January, time.March}
	if len(got) != len(wantMonths) {
		t.Fatalf("want %d, got %d: %+v", len(wantMonths), len(got), got)
	}
	for i, o := range got {
		if o.at.Month() != wantMonths[i] || o.at.Day() != 31 {
			t.Fatalf("[%d] got %s", i, o.at.Format("2006-01-02"))
		}
	}
	if got[0].seq != 0 || got[1].seq != 2 { // 短月仍占用序号位
		t.Fatalf("seqs: %d,%d", got[0].seq, got[1].seq)
	}
}

// TestRRULEWindowedSeqMatchesFullEnumeration 任意扫描窗口（含停机很久后的
// 快进窗口）枚举到的时间点，其 (seq, at) 必须与从 DTSTART 起全量枚举完全一致。
func TestRRULEWindowedSeqMatchesFullEnumeration(t *testing.T) {
	loc := time.UTC
	anchor := time.Date(2026, 1, 1, 9, 30, 0, 0, loc)
	hi := anchor.AddDate(1, 0, 0)
	rules := []string{
		"FREQ=MINUTELY;INTERVAL=17",
		"FREQ=HOURLY;INTERVAL=3",
		"FREQ=DAILY",
		"FREQ=DAILY;INTERVAL=2;BYDAY=MO,WE,FR",
		"FREQ=WEEKLY;BYDAY=MO,WE,FR",
		"FREQ=WEEKLY;INTERVAL=2;BYDAY=TU,SA",
		"FREQ=MONTHLY;BYMONTHDAY=1,15",
		"FREQ=MONTHLY;INTERVAL=3",
		"FREQ=YEARLY",
	}
	windows := []time.Duration{
		0, time.Minute, time.Hour, 24 * time.Hour, 30 * 24 * time.Hour, 100 * 24 * time.Hour,
	}
	for _, rrule := range rules {
		rec, err := parseRRULE(rrule, anchor)
		if err != nil {
			t.Fatalf("parse %s: %v", rrule, err)
		}
		full, err := rec.between(anchor.Add(-time.Nanosecond), hi)
		if err != nil {
			t.Fatalf("full %s: %v", rrule, err)
		}
		fullByAt := map[int64]occurrence{}
		for _, o := range full {
			fullByAt[o.at.UnixNano()] = o
		}
		for _, w := range windows {
			lo := anchor.Add(w - time.Nanosecond)
			got, err := rec.between(lo, hi)
			if err != nil {
				t.Fatalf("%s window %s: %v", rrule, w, err)
			}
			for _, o := range got {
				want, ok := fullByAt[o.at.UnixNano()]
				if !ok {
					t.Fatalf("%s window %s: stray occurrence %s", rrule, w, o.at)
				}
				if o.seq != want.seq {
					t.Fatalf("%s window %s: seq drift at %s, want %d got %d", rrule, w, o.at, want.seq, o.seq)
				}
			}
		}
	}
}

func TestRRULETimezoneWallClock(t *testing.T) {
	sh, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, 9, 27, 9, 0, 0, 0, sh) // 本地每天 09:00
	rec, err := parseRRULE("FREQ=DAILY", anchor)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rec.between(anchor, anchor.Add(25*time.Hour))
	if err != nil || len(got) != 1 {
		t.Fatalf("between: %+v, %v", got, err)
	}
	next := got[0]
	if next.at.Hour() != 9 || next.at.Location().String() != "Asia/Shanghai" {
		t.Fatalf("wall clock drifted: %s", next.at)
	}
	if want := anchor.Add(24 * time.Hour); !next.at.Equal(want) {
		t.Fatalf("want %s, got %s", want, next.at)
	}
	// UTC 视角是 01:00。
	if next.at.UTC().Hour() != 1 {
		t.Fatalf("utc hour: %d", next.at.UTC().Hour())
	}
}

// TestRRULEDSTKeepsWallClock 跨 DST 切换（2026-03-08 美国春令时）时，
// 每天的本地挂钟时刻必须保持不变，UTC 偏移随之变化。
func TestRRULEDSTKeepsWallClock(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	// 3/6、3/7 是 EST（UTC-5），3/8 起 EDT（UTC-4）。
	anchor := time.Date(2026, 3, 6, 7, 0, 0, 0, ny)
	rec, err := parseRRULE("FREQ=DAILY", anchor)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rec.between(anchor.Add(-time.Second), anchor.AddDate(0, 0, 2))
	if err != nil || len(got) != 3 {
		t.Fatalf("between: %+v, %v", got, err)
	}
	for i, o := range got {
		if h, _, _ := o.at.Clock(); h != 7 {
			t.Fatalf("[%d] wall clock drifted: %s", i, o.at)
		}
	}
	if got[0].at.UTC().Hour() != 12 || got[2].at.UTC().Hour() != 11 {
		t.Fatalf("utc offsets did not shift: %s -> %s", got[0].at.UTC(), got[2].at.UTC())
	}
}

// TestRRULEDedupAndOrder 重复与乱序的 BYDAY/BYMONTHDAY 必须去重并按时间顺序枚举，
// 不能产生同一时间点的重复实例，也不能因乱序提前结束而漏掉较早日期。
func TestRRULEDedupAndOrder(t *testing.T) {
	loc := time.UTC
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, loc) // 周四
	rec, err := parseRRULE("FREQ=WEEKLY;BYDAY=MO,MO,FR,WE", anchor)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rec.between(anchor.Add(-time.Second), anchor.Add(7*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// 首周的 MO/WE 在 anchor(周四) 之前被排除；窗口内为 周五、下周一、下周三——去重且有序。
	wantWeekdays := []time.Weekday{time.Friday, time.Monday, time.Wednesday}
	if len(got) != len(wantWeekdays) {
		t.Fatalf("want %d, got %d: %+v", len(wantWeekdays), len(got), got)
	}
	for i, o := range got {
		if o.at.Weekday() != wantWeekdays[i] {
			t.Fatalf("[%d] want %s, got %s", i, wantWeekdays[i], o.at.Weekday())
		}
	}

	anchor2 := time.Date(2026, 1, 1, 9, 0, 0, 0, loc)
	rec2, err := parseRRULE("FREQ=MONTHLY;BYMONTHDAY=31,1,15,15", anchor2)
	if err != nil {
		t.Fatal(err)
	}
	got2, err := rec2.between(anchor2.Add(-time.Second), time.Date(2026, 2, 1, 0, 0, 0, 0, loc))
	if err != nil {
		t.Fatal(err)
	}
	// 1 月应按 1、15、31 升序枚举且去重。
	wantDays := []int{1, 15, 31}
	if len(got2) != len(wantDays) {
		t.Fatalf("want %d, got %d: %+v", len(wantDays), len(got2), got2)
	}
	for i, o := range got2 {
		if o.at.Day() != wantDays[i] {
			t.Fatalf("[%d] want day %d, got %s", i, wantDays[i], o.at.Format("2006-01-02"))
		}
	}
}

func TestInvalidSpecs(t *testing.T) {
	svc, _ := newTestService(t)
	base2 := base
	bad := []ScheduleSpec{
		{StartAt: base2, RRULE: "FREQ=DAILY", MissedPolicy: "UNKNOWN"},
		{StartAt: time.Time{}, RRULE: "FREQ=DAILY", MissedPolicy: MissedSkip},
		{StartAt: base2, EndAt: base2.Add(-time.Hour), RRULE: "FREQ=DAILY", MissedPolicy: MissedSkip},
		{StartAt: base2, RRULE: "FREQ=MINUTELY;INTERVAL=0", MissedPolicy: MissedSkip},
		{StartAt: base2, RRULE: "FREQ=YEARLY;BYDAY=MO", MissedPolicy: MissedSkip},
		{StartAt: base2, RRULE: "FREQ=DAILY;BYDAY=XX", MissedPolicy: MissedSkip},
		{StartAt: base2, RRULE: "DAILY", MissedPolicy: MissedSkip},
		{StartAt: base2, RRULE: "FREQ=DAILY", MissedPolicy: MissedSkip, Timezone: "Mars/Olympus"},
	}
	for i, spec := range bad {
		_, err := svc.CreateSchedule(CreateScheduleRequest{
			WorkflowID: "wf1", ScheduleID: fmt.Sprintf("bad%d", i), RequestID: fmt.Sprintf("req-%d", i), Spec: spec,
		})
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("[%d] want ErrInvalidSchedule, got %v", i, err)
		}
	}
}

func TestScanMaterializesOnTimeWithStableKeys(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))

	rep := mustScan(t, svc, time.Minute) // 创建时刻窗口为空
	if rep.Generated != 0 {
		t.Fatalf("first scan must be empty: %+v", rep)
	}

	clk.Advance(25 * time.Hour)
	rep = mustScan(t, svc, time.Minute) // base+24h，准点
	if rep.Generated != 1 || rep.Skipped != 0 {
		t.Fatalf("want 1 generated, got %+v", rep)
	}
	insts := svc.ListInstances("wf1", "daily")
	if len(insts) != 1 {
		t.Fatalf("instances: %+v", insts)
	}
	in := insts[0]
	// StartAt 时刻（seq0）与创建时刻重合，落在窗口开界之外，首个实例是 seq1。
	if in.Version != 1 || in.Seq != 1 || in.Status != InstancePending {
		t.Fatalf("unexpected instance: %+v", in)
	}
	if !in.ScheduledAt.Equal(base.Add(24*time.Hour)) || !in.GeneratedAt.Equal(clk.Now()) {
		t.Fatalf("timestamps: scheduled=%s generated=%s now=%s", in.ScheduledAt, in.GeneratedAt, clk.Now())
	}
	if in.InstanceID != "daily-v1-1" {
		t.Fatalf("instance id: got %s", in.InstanceID)
	}

	// 再扫不重复生成。
	rep = mustScan(t, svc, time.Minute)
	if rep.Generated != 0 {
		t.Fatalf("duplicate generation: %+v", rep)
	}
	if got := len(svc.ListInstances("wf1", "daily")); got != 1 {
		t.Fatalf("want still 1 instance, got %d", got)
	}

	// 推进一天，第二个时间点序号为 2，键稳定。
	clk.Advance(24 * time.Hour)
	rep = mustScan(t, svc, time.Minute)
	if rep.Generated != 1 {
		t.Fatalf("second generation: %+v", rep)
	}
	insts = svc.ListInstances("wf1", "daily")
	if insts[1].Seq != 2 || insts[1].InstanceID != "daily-v1-2" {
		t.Fatalf("second instance: %+v", insts[1])
	}
}

func TestMissedPoliciesAfterDowntime(t *testing.T) {
	// 停机约 3 天后恢复（越过 base+72h），窗口内有 base+24h/48h/72h 三个错过时间点。
	for _, tc := range []struct {
		name          MissedPolicy
		wantGenerated int
		wantSkipped   int
		latestSeq     int64 // 补齐实例中最晚的序号
	}{
		{MissedCatchUpAll, 3, 0, 3},
		{MissedLatestOnly, 1, 2, 3},
		{MissedSkip, 0, 3, -1},
	} {
		t.Run(string(tc.name), func(t *testing.T) {
			svc, clk := newTestService(t)
			mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, tc.name))
			clk.Advance(73 * time.Hour) // 长时间停机后恢复，越过最后一个错过时间点
			rep := mustScan(t, svc, time.Minute)
			if rep.Generated != tc.wantGenerated || rep.Skipped != tc.wantSkipped {
				t.Fatalf("report: %+v, want gen=%d skip=%d", rep, tc.wantGenerated, tc.wantSkipped)
			}
			insts := svc.ListInstances("wf1", "daily")
			if len(insts) != tc.wantGenerated+tc.wantSkipped {
				t.Fatalf("total instances: %d", len(insts))
			}
			var pending, skipped []Instance
			for _, in := range insts {
				switch in.Status {
				case InstancePending:
					pending = append(pending, in)
				case InstanceSkipped:
					skipped = append(skipped, in)
				default:
					t.Fatalf("unexpected status %s", in.Status)
				}
			}
			if len(pending) != tc.wantGenerated || len(skipped) != tc.wantSkipped {
				t.Fatalf("pending=%d skipped=%d", len(pending), len(skipped))
			}
			if tc.wantGenerated > 0 && pending[len(pending)-1].Seq != tc.latestSeq {
				t.Fatalf("latest pending seq: %d, want %d", pending[len(pending)-1].Seq, tc.latestSeq)
			}
			// 跳过的实例保留历史，且永不触发。
			for _, sk := range skipped {
				if !sk.GeneratedAt.Equal(clk.Now()) || sk.FiredAt != (time.Time{}) {
					t.Fatalf("skipped instance timestamps: %+v", sk)
				}
			}

			// 多次扫描不重复生成。
			rep = mustScan(t, svc, time.Minute)
			if rep.Generated != 0 || rep.Skipped != 0 {
				t.Fatalf("rescan must be no-op: %+v", rep)
			}
		})
	}
}

func TestOnTimeWithinLatenessIgnoresPolicy(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedSkip))

	// 只晚了 30 秒，在准点容差内，即使策略是 SKIP 也正常生成。
	clk.Advance(24*time.Hour + 30*time.Second)
	rep := mustScan(t, svc, time.Minute)
	if rep.Generated != 1 || rep.Skipped != 0 {
		t.Fatalf("on-time occurrence must generate: %+v", rep)
	}
}

func TestPauseDoesNotAffectSubmittedOrClaimed(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))

	clk.Advance(25 * time.Hour)
	mustScan(t, svc, time.Minute)
	claims, err := svc.ClaimDueInstances("worker-a", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim: %+v, %v", claims, err)
	}

	// 暂停：已领取的实例仍可确认。
	if err := svc.PauseSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2"}); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", claims[0].Instance.InstanceID, claims[0].LeaseID, []byte("ok")); err != nil {
		t.Fatalf("confirm claimed while paused: %v", err)
	}

	// 暂停期间扫描不产生任何实例。
	clk.Advance(48 * time.Hour)
	rep := mustScan(t, svc, time.Minute)
	if rep.Checked != 0 || rep.Generated != 0 {
		t.Fatalf("paused scan must skip schedule: %+v", rep)
	}

	// 重复暂停报错；暂停幂等重放。
	if err := svc.PauseSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2"}); err != nil {
		t.Fatalf("pause replay: %v", err)
	}
	if err := svc.PauseSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-3"}); !errors.Is(err, ErrScheduleAlreadyPaused) {
		t.Fatalf("want ErrScheduleAlreadyPaused, got %v", err)
	}

	// 恢复：从恢复时刻继续，暂停期间错过的不补。
	if err := svc.ResumeSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-4"}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := svc.ResumeSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-5"}); !errors.Is(err, ErrScheduleNotPaused) {
		t.Fatalf("want ErrScheduleNotPaused, got %v", err)
	}
	clk.Advance(25 * time.Hour)
	rep = mustScan(t, svc, time.Minute)
	if rep.Generated != 1 {
		t.Fatalf("after resume want exactly 1 new instance, got %+v", rep)
	}
	insts := svc.ListInstances("wf1", "daily")
	for _, in := range insts {
		if in.Status == InstanceFired && in.InstanceID != "daily-v1-1" {
			t.Fatalf("unexpected fired: %+v", in)
		}
	}
}

func TestUpdateVersionCancelsOldFutureKeepsHistory(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))

	// 停机恢复：一次补齐 3 个错过的实例。
	clk.Advance(72 * time.Hour)
	mustScan(t, svc, time.Minute)
	insts := svc.ListInstances("wf1", "daily")
	if len(insts) != 3 {
		t.Fatalf("want 3 catch-up instances, got %d", len(insts))
	}

	// 领取第一个（已到期），随后更新计划。
	claims, _ := svc.ClaimDueInstances("worker-a", 1, time.Minute)
	claimed := claims[0].Instance

	newSpec := dailySpec(base, MissedSkip)
	newSpec.RRULE = "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR"
	r, err := svc.UpdateSchedule(UpdateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2", Spec: newSpec})
	if err != nil || r.Version != 2 {
		t.Fatalf("update: %+v, %v", r, err)
	}

	// 旧版本未来的两个实例被作废，已领取的到期实例保留。
	insts = svc.ListInstances("wf1", "daily")
	var canceled, pending int
	for _, in := range insts {
		switch in.Status {
		case InstanceCanceled:
			canceled++
			if in.Version != 1 {
				t.Fatalf("canceled instance must belong to old version: %+v", in)
			}
		case InstancePending, InstanceFailed:
			pending++
		}
	}
	if canceled != 2 || pending != 1 {
		t.Fatalf("want 2 canceled, 1 pending, got %d/%d: %+v", canceled, pending, insts)
	}

	// 已领取的旧版本实例仍可正常触发（旧版本已生成的历史保留）。
	if _, err := svc.ConfirmInstanceFire("wf1", claimed.InstanceID, claims[0].LeaseID, []byte("v1")); err != nil {
		t.Fatalf("claimed old instance must still fire: %v", err)
	}

	// 被作废的实例既不会被领取，也无法直接确认触发。
	if more, _ := svc.ClaimDueInstances("worker-a", 10, time.Minute); len(more) != 0 {
		t.Fatalf("canceled instances must not be claimable: %+v", more)
	}
	canceledID := "daily-v1-2"
	if _, err := svc.ConfirmInstanceFire("wf1", canceledID, "whatever", nil); !errors.Is(err, ErrInstanceCanceled) {
		t.Fatalf("confirm canceled instance: %v", err)
	}

	// 更新幂等重放返回原版本。
	r2, err := svc.UpdateSchedule(UpdateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2", Spec: newSpec})
	if err != nil || r2.Version != 2 {
		t.Fatalf("update replay: %+v, %v", r2, err)
	}

	// 旧版本不得继续生成未来实例：再扫描只按版本 2 物化。
	clk.Advance(25 * time.Hour)
	mustScan(t, svc, time.Minute)
	for _, in := range svc.ListInstances("wf1", "daily") {
		if in.Status == InstancePending && in.ScheduledAt.After(clk.Now().Add(-26*time.Hour)) && in.Version != 2 {
			t.Fatalf("old version generated a future instance: %+v", in)
		}
	}

	// 历史仍可查询：版本 1 快照与已触发/已作废实例都在。
	sch, err := svc.GetSchedule("wf1", "daily")
	if err != nil {
		t.Fatal(err)
	}
	if len(sch.Versions) != 2 || sch.CurrentVersion != 2 {
		t.Fatalf("versions: %+v", sch.Versions)
	}
	if v1 := sch.Versions[1]; v1.Spec.MissedPolicy != MissedCatchUpAll {
		t.Fatalf("frozen v1 policy changed: %s", v1.Spec.MissedPolicy)
	}
}

func TestUpdateNewVersionInstancesNeverDeletedByOlderCancel(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))
	clk.Advance(25 * time.Hour)
	mustScan(t, svc, time.Minute) // v1 seq0

	// 更新到 v2 并物化 v2 的实例。
	r, err := svc.UpdateSchedule(UpdateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2", Spec: dailySpec(base, MissedCatchUpAll)})
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != 2 {
		t.Fatalf("version: %d", r.Version)
	}
	clk.Advance(25 * time.Hour)
	mustScan(t, svc, time.Minute)

	var v2pending []Instance
	for _, in := range svc.ListInstances("wf1", "daily") {
		if in.Version == 2 {
			v2pending = append(v2pending, in)
		}
	}
	if len(v2pending) != 1 || v2pending[0].Status != InstancePending {
		t.Fatalf("want one v2 pending instance: %+v", v2pending)
	}

	// 再更新到 v3：v1 的 seq0（已到期）保留；v2 的未来实例作废，
	// 任何已经确认的 v2 实例不被删除。
	claims, _ := svc.ClaimDueInstances("worker-a", 10, time.Minute)
	if len(claims) != 1 || claims[0].Instance.Version != 2 {
		t.Fatalf("claim v2: %+v", claims)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", claims[0].Instance.InstanceID, claims[0].LeaseID, []byte("v2")); err != nil {
		t.Fatalf("fire v2: %v", err)
	}
	if _, err := svc.UpdateSchedule(UpdateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-3", Spec: dailySpec(base, MissedSkip)}); err != nil {
		t.Fatalf("update v3: %v", err)
	}
	in, err := svc.GetInstance("wf1", v2pending[0].InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if in.Status != InstanceFired || string(in.Result) != "v2" {
		t.Fatalf("fired v2 instance must survive update: %+v", in)
	}
}

func TestInstanceFailureRetriesButDoesNotBlockNext(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))
	clk.Advance(49 * time.Hour) // base+24h、base+48h 两个错过时间点
	mustScan(t, svc, time.Minute)

	claims, err := svc.ClaimDueInstances("worker-a", 10, time.Minute)
	if err != nil || len(claims) != 2 {
		t.Fatalf("claims: %+v, %v", claims, err)
	}
	// 按计划时间排序：seq1（base+24h）在前。
	first := claims[0]
	if first.Instance.Seq != 1 {
		t.Fatalf("order: %+v", first.Instance)
	}

	// 第一个实例触发失败：释放租约、进入 FAILED，不影响第二个实例领取与触发。
	if err := svc.ReportInstanceFailure("wf1", first.Instance.InstanceID, first.LeaseID, "boom"); err != nil {
		t.Fatalf("report failure: %v", err)
	}
	in, _ := svc.GetInstance("wf1", first.Instance.InstanceID)
	if in.Status != InstanceFailed || in.Attempts != 1 || in.LastError != "boom" || in.Lease != nil {
		t.Fatalf("failed instance: %+v", in)
	}
	second := claims[1]
	if _, err := svc.ConfirmInstanceFire("wf1", second.Instance.InstanceID, second.LeaseID, []byte("second")); err != nil {
		t.Fatalf("second must fire despite first failure: %v", err)
	}

	// 失败实例可被重新领取并重试成功。
	retry, err := svc.ClaimDueInstances("worker-b", 10, time.Minute)
	if err != nil || len(retry) != 1 || retry[0].Instance.InstanceID != first.Instance.InstanceID {
		t.Fatalf("retry claim: %+v, %v", retry, err)
	}
	rcpt, err := svc.ConfirmInstanceFire("wf1", retry[0].Instance.InstanceID, retry[0].LeaseID, []byte("first"))
	if err != nil || rcpt.Duplicate {
		t.Fatalf("retry fire: %+v, %v", rcpt, err)
	}

	outbox := svc.ListOutbox("wf1")
	if len(outbox) != 2 {
		t.Fatalf("outbox: %+v", outbox)
	}
	for _, e := range outbox {
		if e.ScheduleID != "daily" || e.InstanceID == "" {
			t.Fatalf("outbox entry missing schedule fields: %+v", e)
		}
	}
}

func TestInstanceConfirmRulesAndThreeTimestamps(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))
	clk.Advance(25 * time.Hour)
	scheduledAt := clk.Now().Add(-time.Hour) // base+24h
	mustScan(t, svc, time.Minute)
	generatedAt := clk.Now()

	const instID = "daily-v1-1"
	// 未领取确认：ErrNotClaimed。
	if _, err := svc.ConfirmInstanceFire("wf1", instID, "nope", nil); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("want ErrNotClaimed, got %v", err)
	}

	// 物化后过了 2 分钟才领取并确认，三个时间点应各不相同。
	clk.Advance(2 * time.Minute)
	claims, _ := svc.ClaimDueInstances("worker-a", 10, time.Minute)
	if claims[0].Instance.InstanceID != instID {
		t.Fatalf("unexpected claim: %s", claims[0].Instance.InstanceID)
	}
	rcpt, err := svc.ConfirmInstanceFire("wf1", claims[0].Instance.InstanceID, claims[0].LeaseID, []byte("done"))
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if rcpt.IdempotencyKey != InstanceIdempotencyKey("wf1", "daily", 1, 1) {
		t.Fatalf("key: %s", rcpt.IdempotencyKey)
	}

	// 重复确认返回首次结果。
	rcpt2, err := svc.ConfirmInstanceFire("wf1", claims[0].Instance.InstanceID, claims[0].LeaseID, []byte("again"))
	if err != nil || !rcpt2.Duplicate || string(rcpt2.Result) != "done" {
		t.Fatalf("duplicate: %+v, %v", rcpt2, err)
	}

	in, _ := svc.GetInstance("wf1", instID)
	if in.Status != InstanceFired ||
		!in.ScheduledAt.Equal(scheduledAt) ||
		!in.GeneratedAt.Equal(generatedAt) ||
		!in.FiredAt.Equal(clk.Now()) {
		t.Fatalf("three timestamps not distinguishable: scheduled=%s generated=%s fired=%s now=%s",
			in.ScheduledAt, in.GeneratedAt, in.FiredAt, clk.Now())
	}
	if in.ScheduledAt.Equal(in.FiredAt) || in.GeneratedAt.Equal(in.FiredAt) {
		t.Fatal("scheduled/generated/fired must be distinct")
	}
}

func TestLeaseExpiryAndMismatchForInstances(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))
	clk.Advance(25 * time.Hour)
	mustScan(t, svc, time.Minute)

	old, _ := svc.ClaimDueInstances("worker-a", 10, 10*time.Second)
	clk.Advance(11 * time.Second)
	taken, _ := svc.ClaimDueInstances("worker-b", 10, time.Minute)
	if len(taken) != 1 || taken[0].LeaseID == old[0].LeaseID {
		t.Fatalf("takeover must issue new lease: %+v", taken)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", old[0].Instance.InstanceID, old[0].LeaseID, nil); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale lease: %v", err)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", taken[0].Instance.InstanceID, taken[0].LeaseID, nil); err != nil {
		t.Fatalf("new owner: %v", err)
	}

	// 跳过的实例永不触发。
	svc2, clk2 := newTestService(t)
	spec := dailySpec(base, MissedSkip)
	mustCreateSchedule(t, svc2, "wf1", "s", "req-1", spec)
	clk2.Advance(49 * time.Hour) // 越过 base+48h，两个时间点都算错过
	mustScan(t, svc2, time.Minute)
	for _, in := range svc2.ListInstances("wf1", "s") {
		if in.Status != InstanceSkipped {
			t.Fatalf("want skipped, got %+v", in)
		}
		if _, err := svc2.ConfirmInstanceFire("wf1", in.InstanceID, "x", nil); !errors.Is(err, ErrInstanceSkipped) {
			t.Fatalf("confirm skipped: %v", err)
		}
	}
}

func TestScheduleEndAtCompletes(t *testing.T) {
	svc, clk := newTestService(t)
	spec := dailySpec(base, MissedCatchUpAll)
	spec.EndAt = base.Add(36 * time.Hour) // 含 base+24h，不含 base+48h
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", spec)

	clk.Advance(48 * time.Hour)
	rep := mustScan(t, svc, time.Minute)
	if rep.Generated != 1 || rep.Completed != 1 {
		t.Fatalf("report: %+v", rep)
	}
	sch, _ := svc.GetSchedule("wf1", "daily")
	if sch.State != ScheduleCompleted {
		t.Fatalf("state: %s", sch.State)
	}
	if err := svc.PauseSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2"}); !errors.Is(err, ErrScheduleCompleted) {
		t.Fatalf("pause completed: %v", err)
	}
	if _, err := svc.UpdateSchedule(UpdateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-3", Spec: spec}); !errors.Is(err, ErrScheduleCompleted) {
		t.Fatalf("update completed: %v", err)
	}
}

func TestSchedulePersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timers.json")
	clk := newTestClock()

	svc, err := NewService(NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	svc = svc.WithClock(clk.Now)
	spec := dailySpec(base, MissedLatestOnly)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", spec)
	clk.Advance(72 * time.Hour)
	mustScan(t, svc, time.Minute)
	claims, _ := svc.ClaimDueInstances("worker-a", 10, time.Minute)
	if len(claims) != 1 {
		t.Fatalf("latest-only claim: %+v", claims)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", claims[0].Instance.InstanceID, claims[0].LeaseID, []byte("done")); err != nil {
		t.Fatalf("fire: %v", err)
	}

	// 重启：计划、版本快照、实例历史、outbox、扫描水位全部恢复。
	svc2, err := NewService(NewFileStore(path))
	if err != nil {
		t.Fatal(err)
	}
	svc2 = svc2.WithClock(clk.Now)
	sch, err := svc2.GetSchedule("wf1", "daily")
	if err != nil {
		t.Fatal(err)
	}
	if sch.CurrentVersion != 1 || sch.Versions[1].Spec.MissedPolicy != MissedLatestOnly {
		t.Fatalf("restored schedule: %+v", sch)
	}
	insts := svc2.ListInstances("wf1", "daily")
	if len(insts) != 3 { // 2 skipped + 1 fired
		t.Fatalf("restored instances: %+v", insts)
	}
	var fired, skipped int
	for _, in := range insts {
		switch in.Status {
		case InstanceFired:
			fired++
		case InstanceSkipped:
			skipped++
		}
	}
	if fired != 1 || skipped != 2 {
		t.Fatalf("restored statuses fired=%d skipped=%d", fired, skipped)
	}
	if got := len(svc2.ListOutbox("wf1")); got != 1 {
		t.Fatalf("restored outbox: %d", got)
	}

	// 重启后扫描水位仍在，不重复物化；推进一天只补一个新时间点。
	clk.Advance(25 * time.Hour)
	rep := mustScan(t, svc2, time.Minute)
	if rep.Generated != 1 {
		t.Fatalf("post-restart scan: %+v", rep)
	}
}

func TestScheduleAndInstanceErrorPaths(t *testing.T) {
	svc, clk := newTestService(t)
	mustCreateSchedule(t, svc, "wf1", "daily", "req-1", dailySpec(base, MissedCatchUpAll))

	// 不存在的计划。
	if _, err := svc.GetSchedule("wf1", "nope"); !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("get: %v", err)
	}
	if err := svc.ResumeSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "nope", RequestID: "r"}); !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("resume missing: %v", err)
	}
	// 恢复一个未暂停的计划。
	if err := svc.ResumeSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "r2"}); !errors.Is(err, ErrScheduleNotPaused) {
		t.Fatalf("resume active: %v", err)
	}
	// 已完成的计划不能暂停/恢复/更新。
	clk.Advance(73 * time.Hour)
	spec := dailySpec(base, MissedSkip)
	spec.EndAt = base.Add(25 * time.Hour)
	svc2, clk2 := newTestService(t)
	mustCreateSchedule(t, svc2, "wf1", "s2", "req-1", spec)
	clk2.Advance(49 * time.Hour)
	mustScan(t, svc2, time.Minute)
	sch, _ := svc2.GetSchedule("wf1", "s2")
	if sch.State != ScheduleCompleted {
		t.Fatalf("state: %s", sch.State)
	}
	if err := svc2.ResumeSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: "s2", RequestID: "r3"}); !errors.Is(err, ErrScheduleCompleted) {
		t.Fatalf("resume completed: %v", err)
	}

	// 不存在的实例。
	if _, err := svc.GetInstance("wf1", "ghost"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("get instance: %v", err)
	}
	if err := svc.ReportInstanceFailure("wf1", "ghost", "l", "x"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("fail missing: %v", err)
	}

	// 未领取实例上报失败：ErrNotClaimed；租约不符：ErrLeaseMismatch；已触发：ErrAlreadyFired。
	mustScan(t, svc, time.Minute) // 补齐 base+24/48/72h（CATCH_UP_ALL）
	insts := svc.ListInstances("wf1", "daily")
	id := insts[0].InstanceID
	if err := svc.ReportInstanceFailure("wf1", id, "l", "x"); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("fail unclaimed: %v", err)
	}
	claims, _ := svc.ClaimDueInstances("worker-a", 10, time.Minute)
	c := claims[0]
	if err := svc.ReportInstanceFailure("wf1", c.Instance.InstanceID, "wrong", "x"); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("fail lease mismatch: %v", err)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", c.Instance.InstanceID, c.LeaseID, []byte("ok")); err != nil {
		t.Fatalf("fire: %v", err)
	}
	if err := svc.ReportInstanceFailure("wf1", c.Instance.InstanceID, c.LeaseID, "x"); !errors.Is(err, ErrAlreadyFired) {
		t.Fatalf("fail fired: %v", err)
	}
	if _, err := svc.ConfirmInstanceFire("wf1", "missing", c.LeaseID, nil); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("confirm missing: %v", err)
	}
}

func TestCreateScheduleValidationAndReplay(t *testing.T) {
	svc, _ := newTestService(t)
	spec := dailySpec(base, MissedCatchUpAll)
	r1 := mustCreateSchedule(t, svc, "wf1", "daily", "req-1", spec)
	r2, err := svc.CreateSchedule(CreateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-1", Spec: spec})
	if err != nil || r2.Version != r1.Version {
		t.Fatalf("replay: %+v, %v", r2, err)
	}
	// 同号不同内容报冲突。
	other := dailySpec(base, MissedSkip)
	if _, err := svc.CreateSchedule(CreateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-1", Spec: other}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("want ErrRequestConflict, got %v", err)
	}
	if _, err := svc.CreateSchedule(CreateScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily", RequestID: "req-2", Spec: spec}); !errors.Is(err, ErrScheduleExists) {
		t.Fatalf("want ErrScheduleExists, got %v", err)
	}
}

// TestSchedulesConcurrentRace 并发压测扫描/更新/暂停/恢复/领取/确认。
// 不变量：实例号（版本+序号）唯一且只生成一次；outbox 键唯一；
// 旧版本不得残留可触发的未来实例；新版本的已终结实例不被删除或改判。
func TestSchedulesConcurrentRace(t *testing.T) {
	svc, clk := newTestService(t)
	const schedulesN = 8
	for i := 0; i < schedulesN; i++ {
		id := fmt.Sprintf("s%d", i)
		spec := ScheduleSpec{
			Timezone: "UTC", StartAt: base, RRULE: "FREQ=HOURLY",
			MissedPolicy: []MissedPolicy{MissedCatchUpAll, MissedLatestOnly, MissedSkip}[i%3],
			Payload:      []byte(id),
		}
		mustCreateSchedule(t, svc, "wf1", id, "create-"+id, spec)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 时钟推进者：模拟时间流逝。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(time.Hour)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 扫描者。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = svc.ScanSchedules(time.Minute)
				}
			}
		}()
	}

	// 领取 + 触发 / 上报失败。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					claims, err := svc.ClaimDueInstances(owner, 5, 50*time.Millisecond)
					if err != nil {
						continue
					}
					for _, c := range claims {
						if c.Instance.Seq%4 == 0 {
							_ = svc.ReportInstanceFailure("wf1", c.Instance.InstanceID, c.LeaseID, "transient")
						} else {
							_, _ = svc.ConfirmInstanceFire("wf1", c.Instance.InstanceID, c.LeaseID, []byte("done"))
						}
					}
				}
			}
		}(fmt.Sprintf("worker-%d", w))
	}

	// 更新 / 暂停 / 恢复。
	for i := 0; i < schedulesN; i++ {
		id := fmt.Sprintf("s%d", i)
		wg.Add(3)
		go func() {
			defer wg.Done()
			for v := int64(2); v <= 4; v++ {
				_, _ = svc.UpdateSchedule(UpdateScheduleRequest{
					WorkflowID: "wf1", ScheduleID: id,
					RequestID: fmt.Sprintf("upd-%s-%d", id, v),
					Spec: ScheduleSpec{
						Timezone: "UTC", StartAt: base, RRULE: "FREQ=HOURLY;INTERVAL=2",
						MissedPolicy: MissedCatchUpAll, Payload: []byte("new"),
					},
				})
			}
		}()
		go func() {
			defer wg.Done()
			_ = svc.PauseSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: id, RequestID: "pause-" + id})
		}()
		go func() {
			defer wg.Done()
			_ = svc.ResumeSchedule(ScheduleStateRequest{WorkflowID: "wf1", ScheduleID: id, RequestID: "resume-" + id})
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 最终再扫一次，让所有生效计划物化到当前时刻。
	clk.Advance(2 * time.Hour)
	_, _ = svc.ScanSchedules(time.Minute)

	type vk struct {
		id  string
		ver int64
		seq int64
	}
	seen := map[vk]bool{}
	outboxKeys := map[string]bool{}
	for _, e := range svc.ListOutbox("wf1") {
		if outboxKeys[e.IdempotencyKey] {
			t.Fatalf("duplicate outbox key %s", e.IdempotencyKey)
		}
		outboxKeys[e.IdempotencyKey] = true
	}
	for _, sch := range svc.ListSchedules("wf1") {
		for _, in := range svc.ListInstances("wf1", sch.ScheduleID) {
			k := vk{in.ScheduleID, in.Version, in.Seq}
			if seen[k] {
				t.Fatalf("duplicate instance %s", in.InstanceID)
			}
			seen[k] = true

			switch in.Status {
			case InstanceFired:
				if !outboxKeys[InstanceIdempotencyKey("wf1", in.ScheduleID, in.Version, in.Seq)] {
					t.Fatalf("fired instance without outbox: %+v", in)
				}
			case InstanceCanceled:
				// 只有旧版本实例可被作废；新版本实例永不被旧操作删除/改判。
				if in.Version >= sch.CurrentVersion {
					t.Fatalf("current-version instance was canceled: %+v", in)
				}
			case InstancePending, InstanceFailed:
				// 旧版本不得在新版本生效后继续被物化：旧版本实例的生成时间
				// 必须不晚于取代它的版本的生效时间（扫描只认当前版本）。
				if in.Version < sch.CurrentVersion {
					superseded := sch.Versions[in.Version+1].EffectiveFrom
					if in.GeneratedAt.After(superseded) {
						t.Fatalf("old version %d materialized after superseding version: %+v", in.Version, in)
					}
				}
			}
		}
	}
}
