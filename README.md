# go-workflow-timers

用于承载工作流定时器、周期计划、触发与取消状态管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

## 功能概览

持久化工作流定时器服务，重点解决**调度、重排与触发之间的竞态**：

- 工作流通过外部定时器号创建、重排、取消单次定时器；每次重排递增定时器版本。
- 工作流可以创建**带时区的周期计划**，声明开始时间、结束时间、RRULE 递归规则和**错过执行时的补触发策略**（全部补齐 / 只补最近一次 / 直接跳过）。
- 调度器按计划的**当前版本**扫描并物化具体触发实例；每个计划时间点只有一个实例和一个**稳定幂等键**，重复扫描不重复生成。
- 调度器批量领取到期对象（单次定时器或周期实例）并获得**有期限租约**；只有持有当前有效租约的领取者能确认触发。
- 触发确认原子保存逻辑结果，并写入带**稳定幂等键**的 outbox；传输层重复投递不会产生第二次逻辑触发。
- 取消/重排与领取/触发相撞时，先提交者胜：已提交的触发不能撤销；先提交的取消或新版本使旧版本触发失败。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 定时器版本 `Version` | 单次定时器初始为 1，每次重排 +1；触发确认必须匹配当前版本 |
| 计划版本 `ScheduleVersion` | 周期计划每次更新 +1，版本快照（时区、起止、RRULE、载荷、补触发策略）**不可变**；旧版本及其触发历史始终保留 |
| 生效下界 `EffectiveFrom` | 当前版本开始物化实例的时间下界；创建/更新/恢复时推进到操作时刻，早于该下界的计划时间点不会被物化 |
| 扫描水位 `LastScanAt` | 计划最近一次扫描时刻；扫描窗口为 `(max(生效下界, 上次水位), now]`，因此停机恢复与多次扫描都按稳定键幂等 |
| 触发实例 `Instance` | 一个计划时间点物化出一条实例，实例号 `scheduleID-v{version}-{seq}`；`seq` 是从 DTSTART 起计的稳定版本内序号 |
| 实例幂等键 | `workflowID/scheduleID/version/seq`，与单次定时器的 `workflowID/timerID/version` 同表共存、同键插入即幂等 |
| 补触发策略 `MissedPolicy` | `CATCH_UP_ALL` 全部补齐；`LATEST_ONLY` 只补窗口内最晚一次，其余记为 `SKIPPED`；`SKIP` 错过的全部记为 `SKIPPED` |
| 租约 `Lease` | 领取时签发，含 `LeaseID`、围栏令牌 `Token` 与过期时间；重排/取消/触发/失败都会释放或作废旧租约 |
| 状态 | 单次定时器：`PENDING` → `FIRED` / `CANCELED`；计划：`ACTIVE` / `PAUSED` / `COMPLETED`；实例：`PENDING` / `FAILED` → `FIRED`，另有终态 `CANCELED` / `SKIPPED` |
| 请求号 `RequestID` | 写操作幂等：同号同内容重试返回原结果，同号不同内容报 `ErrRequestConflict` |

## 计划版本规则

1. **创建**：计划初始版本为 1，生效下界为创建时刻；早于创建时刻的计划时间点不补生成。
2. **更新**：追加新的不可变版本快照并把当前版本 +1，生效下界推进到更新时刻：
   - 此后扫描只按新版本的定义物化实例，**旧版本不再生成任何实例**；
   - 旧版本**尚未派发给执行者**的待触发实例（未持有有效租约）作废为 `CANCELED`；
   - 旧版本**已经领取**（持有有效租约）的实例仍可正常确认触发；
   - 已终结（`FIRED` / `CANCELED` / `SKIPPED`）的实例与全部版本快照永久保留，供历史查询；
   - 新版本生成的实例绝不会被旧操作删除或改判。
3. **暂停**：`PAUSED` 计划不参与扫描，不再物化新实例；已生成（含已领取）与已提交的实例不受影响，仍可领取与确认。
4. **恢复**：版本号不变，生效下界推进到恢复时刻，**从新的生效时间继续**，暂停期间错过的时间点不补。
5. **结束**：越过 `EndAt` 后计划转为 `COMPLETED`，不再物化实例，也不能再更新或暂停。
6. 更新、暂停、恢复与扫描/领取/确认全部在同一把互斥锁的事务内完成并写穿透，因此上述裁决不依赖执行顺序。

## 补触发（停机恢复）规则

- 扫描窗口为左开右闭 `(max(EffectiveFrom, LastScanAt), now]`，只枚举窗口内的计划时间点。
- `ScanSchedules(lateness)` 的 `lateness` 是“准点”容差：`now - ScheduledAt <= lateness` 视为准点，无论策略如何都正常生成；超过容差才视为停机错过，按该版本**冻结**的策略处理：
  - `CATCH_UP_ALL`：错过的时间点全部补生成 `PENDING` 实例（按计划时间排队领取）；
  - `LATEST_ONLY`：窗口内只物化最晚一个时间点，其余落为 `SKIPPED` 实例（历史保留、永不触发）；
  - `SKIP`：错过的时间点全部落为 `SKIPPED`。
- 实例号与幂等键只由 `(版本, 序号)` 派生，序号从 DTSTART 起稳定计数，因此服务长时间停机后恢复、或同一窗口被多次扫描，都不会重复生成。
- 单窗口待物化实例数有上限（`maxScanInstances`）；超限时该计划不推进水位、不落任何实例，其他计划照常处理，下次扫描幂等重试。

## 实例的时间与失败语义

- 每个实例区分三个时间点：`ScheduledAt`（计划时间）、`GeneratedAt`（实际物化时间）、`FiredAt`（最终触发确认时间）。
- 实例沿用与单次定时器相同的租约领取、过期接管、围栏令牌与确认规则。
- 触发失败用 `ReportInstanceFailure` 上报：实例转为 `FAILED`、记录 `Attempts`/`LastError` 并释放租约，之后可被重新领取重试。
- **失败不阻塞后续时间点**：周期的下一个实例独立物化、独立领取；前一个实例是否成功与后续实例无关。

## RRULE 子集

时区感知的 RFC5545 递归规则子集，时间点始终在计划时区的本地挂钟时间上计算（DST 切换不漂移）：

```
FREQ=SECONDLY|MINUTELY|HOURLY|DAILY|WEEKLY|MONTHLY|YEARLY
INTERVAL=n（默认 1）
BYDAY=MO,TU,WE,TH,FR,SA,SU（DAILY 过滤、WEEKLY 展开）
BYMONTHDAY=n[,...]（仅 MONTHLY，短月无此日则跳过，如 2/31）
WKST=MO..SU（默认 MO）
```

`StartAt` 即 DTSTART；序号从它起从 0 计数。

## API

```go
svc, _ := workflowtimers.NewService(workflowtimers.NewFileStore("timers.json"))

// —— 单次定时器 ——
svc.CreateTimer(workflowtimers.CreateRequest{ /* 初始版本 1 */ })
svc.RescheduleTimer(workflowtimers.RescheduleRequest{ /* 版本 +1，旧租约作废 */ })
svc.CancelTimer(workflowtimers.CancelRequest{})

// —— 周期计划 ——
svc.CreateSchedule(workflowtimers.CreateScheduleRequest{
    WorkflowID: "wf1", ScheduleID: "daily", RequestID: "sreq-1",
    Spec: workflowtimers.ScheduleSpec{
        Timezone:     "Asia/Shanghai",
        StartAt:      start,
        EndAt:        end,                 // 零值表示不限
        RRULE:        "FREQ=DAILY",
        MissedPolicy: workflowtimers.MissedCatchUpAll,
        Payload:      payload,
    },
})
svc.UpdateSchedule(workflowtimers.UpdateScheduleRequest{ /* 版本 +1 */ })
svc.PauseSchedule(workflowtimers.ScheduleStateRequest{RequestID: "sreq-2"})
svc.ResumeSchedule(workflowtimers.ScheduleStateRequest{RequestID: "sreq-3"})

// —— 调度循环 ——
svc.ScanSchedules(time.Minute) // 按当前版本物化到期实例，幂等
claims, _ := svc.ClaimDue("worker-1", 100, 30*time.Second)          // 单次定时器
sclaims, _ := svc.ClaimDueInstances("worker-1", 100, 30*time.Second) // 周期实例

// 实例触发确认 / 失败上报
rcpt, _ := svc.ConfirmInstanceFire("wf1", sclaims[0].Instance.InstanceID, sclaims[0].LeaseID, result)
_ = svc.ReportInstanceFailure("wf1", instID, leaseID, "transient error")

// —— 查询（可区分计划时间 / 物化时间 / 触发结果）——
svc.GetSchedule("wf1", "daily")      // 含全部版本快照
svc.ListInstances("wf1", "daily")    // 含 FIRED/CANCELED/SKIPPED 历史
svc.GetInstance("wf1", "daily-v2-3") // ScheduledAt/GeneratedAt/FiredAt + Result
svc.ListPending("wf1")
svc.ListOutbox("wf1")
```

## 错误分类

调用方用 `errors.Is` 区分冲突类型：

| 错误 | 含义 |
| --- | --- |
| `ErrRequestConflict` | 幂等冲突：请求号相同但内容不同 |
| `ErrVersionConflict` | 版本冲突：单次定时器已被重排，旧版本触发失败 |
| `ErrLeaseMismatch` / `ErrLeaseExpired` / `ErrNotClaimed` | 租约不属于调用者 / 已过期 / 不存在 |
| `ErrAlreadyFired` / `ErrAlreadyCanceled` | 单次定时器状态冲突：已触发（不可撤销）/ 已取消 |
| `ErrTimerNotFound` / `ErrTimerExists` | 单次定时器不存在 / 定时器号已占用 |
| `ErrInvalidSchedule` | 计划定义非法：时区、RRULE、起止时间或补触发策略有误 |
| `ErrScheduleNotFound` / `ErrScheduleExists` | 计划不存在 / 计划号已占用 |
| `ErrScheduleCompleted` / `ErrScheduleAlreadyPaused` / `ErrScheduleNotPaused` | 计划已完成 / 已暂停 / 未暂停 |
| `ErrInstanceNotFound` / `ErrInstanceCanceled` / `ErrInstanceSkipped` | 实例不存在 / 已被新版本作废 / 按策略跳过 |

## 竞态裁决规则

所有状态变更在单锁事务内完成并写穿透持久化：

1. **领取 vs 领取**：租约有效期内对象对其他领取者不可见；租约过期后可被接管，接管签发新 `LeaseID`，旧租约的迟到确认以 `ErrLeaseMismatch` 拒绝，不影响接管者。
2. **重排 vs 触发（单次定时器）**：重排先提交则版本递增、租约作废，旧版本确认以 `ErrVersionConflict` 失败；触发先提交则重排以 `ErrAlreadyFired` 失败。
3. **更新计划 vs 扫描/实例**：更新先提交则扫描只认新版本，旧版本未派发实例作废；已领取或已终结的实例保留并仍可触发；新版本实例不被删除。
4. **暂停/恢复 vs 扫描/触发**：暂停后不再物化新实例，但已生成实例照常领取与确认；恢复只影响恢复时刻之后的时间点。
5. **重复扫描/触发**：同一 `(版本, 序号)` 的物化命中稳定实例键即幂等，重复确认命中 outbox 键返回首次结果，全系统对同一实例只发生一次逻辑触发。
6. **失败 vs 后续时间点**：失败实例进入 `FAILED` 可重试，不影响任何其他实例的物化与领取。

## 持久化

`Store` 接口抽象持久化后端：

- `FileStore`：JSON 快照写穿透（临时文件 + rename 原子替换），重启后单次定时器、计划版本快照、实例历史、租约、outbox 与请求号记录全部恢复。
- `MemoryStore`：纯内存，用于测试。

## 测试

```sh
go test ./...        # 单元测试
go test -race ./...  # 含并发竞态压测（领取/确认/重排/取消/扫描/更新/暂停/恢复交织）
```

覆盖场景：

- 原有：请求号幂等重放与冲突、批量领取与租约互斥、旧租约迟到确认、租约过期、重排/取消与触发的双向竞态、已触发不可撤销、outbox 幂等、文件持久化重启恢复、多 goroutine 竞态不变量。
- 新增：
  - RRULE 各频率/`INTERVAL`/`BYDAY`/`BYMONTHDAY` 枚举、短月跳号、窗口化枚举序号与全量枚举一致、时区挂钟时间与 DST 不漂移、非法规则校验；
  - 准点物化、稳定实例号与幂等键、多次扫描不重复生成；
  - 三种补触发策略在停机恢复后的行为、准点容差不受策略影响；
  - 暂停不影响已领取/已提交实例、恢复从新生效时间继续且不补暂停窗口；
  - 更新产生版本快照、作废旧版本未派发实例、保留已领取/已终结历史、新版本实例不被删除；
  - 实例失败转 `FAILED` 可重试且不阻塞后续实例、三个时间点可区分、跳过实例永不触发；
  - 结束时间转 `COMPLETED`；文件持久化重启后计划/实例/outbox/扫描水位恢复；
  - 扫描/更新/暂停/恢复/领取/确认/失败并发压测下：实例键与 outbox 键唯一、旧版本不在新版本生效后继续物化、终结实例不被删除或改判。
