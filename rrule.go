package workflowtimers

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 支持的 RRULE（RFC5545）子集：
//
//	FREQ=SECONDLY|MINUTELY|HOURLY|DAILY|WEEKLY|MONTHLY|YEARLY
//	INTERVAL=n（默认 1）
//	BYDAY=MO,TU,WE,TH,FR,SA,SU（DAILY 过滤、WEEKLY 展开）
//	BYMONTHDAY=n[,...]（仅 MONTHLY，1..31）
//	WKST=MO..SU（默认 MO，仅校验，不影响枚举）
//
// 时间一律在计划时区的 Location 上计算，序号从 DTSTART 起从 0 计数，
// 因此每个计划时间点都有确定的、与扫描次数无关的版本内序号。
type recurrence struct {
	freq       string
	interval   int
	byDay      []time.Weekday // 已按周一到周日排序
	byMonthDay []int
	anchor     time.Time // 即 DTSTART，携带时区与日内时刻
}

var weekdayIndex = map[string]time.Weekday{
	"MO": time.Monday, "TU": time.Tuesday, "WE": time.Wednesday, "TH": time.Thursday,
	"FR": time.Friday, "SA": time.Saturday, "SU": time.Sunday,
}

func parseRRULE(rrule string, anchor time.Time) (*recurrence, error) {
	r := &recurrence{freq: "", interval: 1, anchor: anchor}
	seen := map[string]bool{}
	for _, part := range strings.Split(rrule, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("%w: malformed RRULE part %q", ErrInvalidSchedule, part)
		}
		name, value := strings.ToUpper(kv[0]), strings.ToUpper(strings.TrimSpace(kv[1]))
		if seen[name] {
			return nil, fmt.Errorf("%w: duplicate RRULE part %q", ErrInvalidSchedule, name)
		}
		seen[name] = true
		switch name {
		case "FREQ":
			switch value {
			case "SECONDLY", "MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
				r.freq = value
			default:
				return nil, fmt.Errorf("%w: unsupported FREQ %q", ErrInvalidSchedule, value)
			}
		case "INTERVAL":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("%w: INTERVAL must be >= 1", ErrInvalidSchedule)
			}
			r.interval = n
		case "BYDAY":
			for _, tok := range strings.Split(value, ",") {
				wd, ok := weekdayIndex[tok]
				if !ok {
					return nil, fmt.Errorf("%w: bad BYDAY token %q", ErrInvalidSchedule, tok)
				}
				r.byDay = append(r.byDay, wd)
			}
		case "BYMONTHDAY":
			for _, tok := range strings.Split(value, ",") {
				d, err := strconv.Atoi(tok)
				if err != nil || d < 1 || d > 31 {
					return nil, fmt.Errorf("%w: BYMONTHDAY supports 1..31, got %q", ErrInvalidSchedule, tok)
				}
				r.byMonthDay = append(r.byMonthDay, d)
			}
		case "WKST":
			if _, ok := weekdayIndex[value]; !ok {
				return nil, fmt.Errorf("%w: bad WKST %q", ErrInvalidSchedule, value)
			}
		default:
			return nil, fmt.Errorf("%w: unsupported RRULE part %q", ErrInvalidSchedule, name)
		}
	}
	if r.freq == "" {
		return nil, fmt.Errorf("%w: RRULE requires FREQ", ErrInvalidSchedule)
	}
	if len(r.byDay) > 0 {
		switch r.freq {
		case "DAILY", "WEEKLY":
		default:
			return nil, fmt.Errorf("%w: BYDAY only supported with DAILY/WEEKLY", ErrInvalidSchedule)
		}
		r.byDay = dedupWeekdays(r.byDay)
		sortWeekdays(r.byDay)
	}
	if len(r.byMonthDay) > 0 {
		if r.freq != "MONTHLY" {
			return nil, fmt.Errorf("%w: BYMONTHDAY only supported with MONTHLY", ErrInvalidSchedule)
		}
		// 升序去重：枚举依赖月内顺序，乱序会让“越过 hi 提前返回”漏掉较早日期。
		r.byMonthDay = dedupInts(r.byMonthDay)
	}
	return r, nil
}

func dedupWeekdays(days []time.Weekday) []time.Weekday {
	seen := make(map[time.Weekday]bool, len(days))
	out := days[:0]
	for _, d := range days {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	return out
}

func dedupInts(xs []int) []int {
	seen := make(map[int]bool, len(xs))
	out := xs[:0]
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Ints(out)
	return out
}

func sortWeekdays(days []time.Weekday) {
	// 周一为序首。
	less := func(a time.Weekday) int { return (int(a) + 6) % 7 }
	for i := 1; i < len(days); i++ {
		for j := i; j > 0 && less(days[j-1]) > less(days[j]); j-- {
			days[j-1], days[j] = days[j], days[j-1]
		}
	}
}

// occurrence 是一个带稳定版本内序号的计划时间点。
type occurrence struct {
	seq int64
	at  time.Time
}

// maxScanInstances 限制单次扫描单个计划最多物化的实例数，
// 避免停机很久配合高频规则造成无界膨胀。
const maxScanInstances = 50000

// between 枚举满足 lo < t <= hi 的时间点。序号始终从 DTSTART 起计数，
// 因此同一时间点在任意扫描窗口中得到的序号都相同；为避免停机很久时
// 从 DTSTART 逐日空转，枚举起点按周期保守前移（再回退一个周期，
// 由窗口判断兜底），绝不跳过任何晚于 lo 的时间点。
func (r *recurrence) between(lo, hi time.Time) ([]occurrence, error) {
	if !hi.After(lo) {
		return nil, nil
	}
	loc := r.anchor.Location()
	h, m, sec := r.anchor.Clock()
	tod := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(sec)*time.Second
	y0, m0, d0 := r.anchor.Date()

	var out []occurrence
	add := func(seq int64, t time.Time) error {
		if t.After(lo) && !t.After(hi) {
			out = append(out, occurrence{seq: seq, at: t})
			if len(out) > maxScanInstances {
				return fmt.Errorf("%w: more than %d instances in scan window", ErrInvalidSchedule, maxScanInstances)
			}
		}
		return nil
	}

	switch r.freq {
	case "SECONDLY", "MINUTELY", "HOURLY":
		var unit time.Duration
		switch r.freq {
		case "SECONDLY":
			unit = time.Second
		case "MINUTELY":
			unit = time.Minute
		default:
			unit = time.Hour
		}
		step := time.Duration(r.interval) * unit
		n := 0 // 第 n 个时间点为 anchor + n*step
		if lo.After(r.anchor) {
			n = int(lo.Sub(r.anchor)/step) - 1 // 绝对时长整除定界，回退一步兜底
			if n < 0 {
				n = 0
			}
		}
		for {
			t := r.anchor.Add(time.Duration(n) * step)
			if t.After(hi) {
				return out, nil
			}
			if t.After(lo) {
				if err := add(int64(n), t); err != nil {
					return nil, err
				}
			}
			n++
		}
	case "DAILY":
		k := 0 // 第 k 个时间点为 anchor 加 k 个日历日
		if lo.After(r.anchor) {
			// 日历日在 DST 切换时可能不是 24h，整除后回退一日，
			// 保证不会跳过任何晚于 lo 的时间点。
			k = int(lo.Sub(r.anchor)/(time.Duration(r.interval)*24*time.Hour)) - 1
			if k < 0 {
				k = 0
			}
		}
		for {
			t := time.Date(y0, time.Month(m0), d0, h, m, sec, 0, loc).AddDate(0, 0, k*r.interval)
			if t.After(hi) {
				return out, nil
			}
			if t.After(lo) && (len(r.byDay) == 0 || containsWeekday(r.byDay, t.Weekday())) {
				if err := add(int64(k), t); err != nil {
					return nil, err
				}
			}
			k++
		}
	case "WEEKLY":
		// 以 DTSTART 所在周一为周块起点，每 INTERVAL 周一块；
		// 块内按 BYDAY（缺省为 DTSTART 的星期几）展开。
		days := r.byDay
		if len(days) == 0 {
			days = []time.Weekday{r.anchor.Weekday()}
		}
		mondayOffset := (int(r.anchor.Weekday()) + 6) % 7
		week0 := time.Date(y0, time.Month(m0), d0, 0, 0, 0, 0, loc).AddDate(0, 0, -mondayOffset)
		b := 0
		if lo.After(week0) {
			weeks := int(lo.Sub(week0) / (7 * 24 * time.Hour))
			b = weeks/r.interval - 1 // 按“块”前移并回退一块
			if b < 0 {
				b = 0
			}
		}
		for {
			block := week0.AddDate(0, 0, 7*r.interval*b)
			for i, wd := range days {
				off := (int(wd) + 6) % 7
				t := block.AddDate(0, 0, off).Add(tod)
				if t.Before(r.anchor) {
					continue // DTSTART 之前的星期点不属于本序列
				}
				if t.After(hi) {
					return out, nil
				}
				if t.After(lo) {
					if err := add(int64(b*len(days)+i), t); err != nil {
						return nil, err
					}
				}
			}
			b++
		}
	case "MONTHLY":
		days := r.byMonthDay
		if len(days) == 0 {
			days = []int{d0}
		}
		b := 0
		if lo.After(r.anchor) {
			ly, lm, _ := lo.In(loc).Date()
			b = (ly*12+int(lm)-(y0*12+int(m0)))/r.interval - 1
			if b < 0 {
				b = 0
			}
		}
		for {
			ym := y0*12 + int(m0-1) + b*r.interval
			y, mo := ym/12, time.Month(ym%12+1)
			for i, d := range days {
				t := time.Date(y, mo, d, h, m, sec, 0, loc)
				if t.Month() != mo || t.Year() != y {
					continue // 短月无此日（如 2/31），按 RRULE 语义跳过；序号位保留
				}
				if t.After(hi) {
					return out, nil
				}
				if t.After(lo) {
					if err := add(int64(b*len(days)+i), t); err != nil {
						return nil, err
					}
				}
			}
			b++
		}
	case "YEARLY":
		b := 0
		if lo.After(r.anchor) {
			ly, _, _ := lo.In(loc).Date()
			b = (ly-y0)/r.interval - 1
			if b < 0 {
				b = 0
			}
		}
		for {
			y := y0 + b*r.interval
			t := time.Date(y, time.Month(m0), d0, h, m, sec, 0, loc)
			if t.After(hi) {
				return out, nil
			}
			if t.After(lo) && t.Month() == time.Month(m0) {
				if err := add(int64(b), t); err != nil {
					return nil, err
				}
			}
			b++
		}
	}
	return out, nil
}

func containsWeekday(days []time.Weekday, wd time.Weekday) bool {
	for _, d := range days {
		if d == wd {
			return true
		}
	}
	return false
}
