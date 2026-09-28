# go-workflow-timers

用于承载工作流定时器、周期计划、触发与取消状态管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

## 功能概览

持久化工作流定时服务，重点解决**调度、重排、周期计划与触发之间的竞态**：

- 工作流通过外部定时器号创建、重排、取消一次性定时器；每次重排递增定时器版本。
- 工作流可以创建**带时区的周期计划**，声明开始/结束时间、周期规则与停机错过执行时的补触发策略（全部补齐 / 只补最近一次 / 直接跳过）。
- 计划更新产生**递增版本**，每个版本的规则与补触发策略**冻结保留**；旧版本已生成的触发历史继续保留。
- 调度器根据当前计划版本生成具体触发实例，每个计划时间点只有一个实例和一个**稳定幂等键**；服务长时间停机恢复时按冻结策略一次性结清积压，多次扫描不重复生成。
- 调度器批量领取到期定时器与实例并获得**有期限租约**；只有持有当前版本 + 当前有效租约的领取者能确认触发。
- 触发确认原子保存逻辑结果，并写入带稳定幂等键的 outbox；传输层重复投递不会产生第二次逻辑触发。
- 取消/重排/更新/暂停与领取/触发相撞时，先提交者胜：已提交的触发不能撤销；先提交的取消、新版本或暂停使旧版本触发失败。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 定时器版本 `Version` | 一次性定时器初始为 1，每次重排 +1；触发确认必须匹配当前版本 |
| 计划版本 `ScheduleVersion` | 每次创建/更新/恢复产生一个**冻结版本**：时区、起止时间、周期规则与补触发策略在版本上不可变；旧版本记录与触发历史永久保留 |
| 补触发策略 `CatchUpPolicy` | `CatchUpAll`（全部补齐）/ `CatchUpLatest`（只补最近一次）/ `CatchUpSkip`（直接跳过）；冻结在版本上，事后修改不影响已确定算法 |
| 计划实例 `Instance` | 某个计划版本在一个计划时间点上的具体触发；含 `ScheduledAt`（计划时间）、`GeneratedAt`（实际生成时间）、`FiredAt`（最终触发时间） |
| 实例幂等键 | `workflowID/scheduleID/vN/seq`：计划更新开新版本即新键空间；同版本同时间点重复扫描命中同键 |
| 计划状态 | `ACTIVE` → `PAUSED`（可恢复）/ `FINISHED`（越过结束时间，终态） |
| 租约 `Lease` | 领取时签发，含 `LeaseID`、围栏令牌 `Token` 与过期时间；重排/取消/触发/版本切换都会作废旧租约 |
| 请求号 `RequestID` | 写操作幂等：同号同内容重试返回原结果，同号不同内容报 `ErrRequestConflict` |

## API

```go
svc, _ := workflowtimers.NewService(workflowtimers.NewFileStore("timers.json"))

// —— 一次性定时器 ——
svc.CreateTimer(workflowtimers.CreateRequest{ /* ... */ })
svc.RescheduleTimer(workflowtimers.RescheduleRequest{ /* ... */ })
svc.CancelTimer(workflowtimers.CancelRequest{ /* ... */ })

// —— 周期计划 ——
// 创建带时区的周期计划（初始版本 1，补触发策略冻结）
svc.CreateSchedule(workflowtimers.CreateScheduleRequest{
    WorkflowID: "wf1", ScheduleID: "daily-report", RequestID: "req-1",
    Location: "Asia/Shanghai",                       // 计划时间点全部在该时区枚举（支持夏令时）
    StartAt:  start, EndAt: end,                    // EndAt 零值表示永不结束
    Recurrence: workflowtimers.Recurrence{Kind: workflowtimers.RecurDaily, Hour: 9},
    Policy: workflowtimers.CatchUpAll,              // 停机后错过的时间点全部补齐
})

// 更新计划：以新内容开递增版本，从更新时刻的下一个时间点起生效
svc.UpdateSchedule(workflowtimers.UpdateScheduleRequest{ /* ... */ })

// 暂停：不影响已提交触发，未完成实例作废；恢复：沿用旧规则开新版本，从恢复时刻继续
svc.PauseSchedule(workflowtimers.PauseScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily-report", RequestID: "req-2"})
svc.ResumeSchedule(workflowtimers.ResumeScheduleRequest{WorkflowID: "wf1", ScheduleID: "daily-report", RequestID: "req-3"})

// —— 调度循环 ——
svc.ScanInstances(100) // 按各计划当前版本生成到期实例（多次调用幂等）
claims, _ := svc.ClaimDue("worker-1", 100, 30*time.Second) // 定时器与实例统一排队
for _, c := range claims {
    if c.Timer != nil {
        svc.ConfirmFire(c.Timer.WorkflowID, c.Timer.TimerID, c.Timer.Version, c.LeaseID, result)
    } else {
        svc.ConfirmInstanceFire(c.Instance.WorkflowID, c.Instance.ScheduleID,
            c.Instance.Version, c.Instance.Seq, c.LeaseID, result)
    }
}

// —— 查询 ——
svc.ListScheduleVersions("wf1", "daily-report") // 全部冻结版本（版本历史）
svc.ListInstances("wf1", "daily-report")        // 所有版本的实例/触发历史
svc.ListPending("wf1")                          // 待执行一次性定时器
svc.ListOutbox("wf1")                           // outbox（含实例的计划时间元数据）
```

`Recurrence` 支持两种规则：

- `RecurEvery{Every: d}`：按固定间隔重复；
- `RecurDaily{Hour, Minute}`：每天在 `Location` 时区的指定时刻重复（日历日推进，正确处理夏令时跳变）。

## 计划版本与补触发规则

1. **版本冻结**：创建/更新/恢复各产生一个新版本，版本上的时区、起止时间、规则、补触发策略不可变；更新只追加新版本，不修改历史版本。
2. **触发历史保留**：版本切换（更新/暂停）只把旧版本**未完成**（含已领取未确认）的实例置为 `SUPERSEDED`；已 `FIRED` 的实例与 outbox 永不删除、不可撤销。
3. **旧版本停发生命**：版本切换的同一事务内旧版本标记 `Done`，扫描只读取计划头指向的当前版本——旧版本不会再生成任何未来实例。
4. **补触发（停机恢复）**：扫描按版本游标只前进地枚举 `ScheduledAt <= now` 的时间点。健康运行时每周期恰好一个到期点，三种策略都会正常生成它；停机导致多个点堆积时按**版本冻结的策略**裁决：
   - `CATCH_UP_ALL`：积压的每个时间点各生成一个实例；
   - `CATCH_UP_LATEST`：积压合并为最后一个时间点的一个实例（序号仍对齐真实槽位）；
   - `CATCH_UP_SKIP`：积压全部跳过，游标越过最后一个错过点，只生成恢复之后的实例。
   游标在一次扫描内越过整个积压，因此**多次扫描天然幂等**，配合稳定幂等键双重保证不重复。
5. **暂停/恢复**：暂停不产生新版本，未完成实例作废但已提交触发保留；恢复时沿用最近版本的规则开新版本，从恢复时刻的下一个时间点继续，暂停期间不补。
6. **失败不阻塞后续**：实例触发失败（未确认）不影响扫描推进与后续时间点生成；租约过期后该实例可被其他 worker 接管重试。

## 错误分类

调用方用 `errors.Is` 区分冲突类型：

| 错误 | 含义 |
| --- | --- |
| `ErrRequestConflict` | 幂等冲突：请求号相同但内容不同 |
| `ErrVersionConflict` | 版本冲突：一次性定时器已被重排，旧版本触发失败 |
| `ErrInstanceSuperseded` | 计划实例所属版本已被更新或暂停取代，未提交的触发作废 |
| `ErrLeaseMismatch` / `ErrLeaseExpired` / `ErrNotClaimed` | 租约不属于调用者 / 已过期 / 不存在 |
| `ErrAlreadyFired` / `ErrAlreadyCanceled` | 状态冲突：已触发（不可撤销）/ 已取消 |
| `ErrScheduleAlreadyPaused` / `ErrScheduleNotActive` | 计划已暂停 / 状态不允许（如已结束） |
| `ErrTimerNotFound` / `ErrTimerExists` | 定时器不存在 / 定时器号已占用 |
| `ErrScheduleNotFound` / `ErrScheduleExists` / `ErrInstanceNotFound` | 计划不存在 / 计划号已占用 / 实例不存在 |
| `ErrInvalidSchedule` | 时区、周期规则、起止时间或补触发策略非法 |

## 竞态裁决规则

所有状态变更在单锁事务内完成并写穿透持久化：

1. **领取 vs 领取**：租约有效期内对象对其他领取者不可见；租约过期后可被接管，接管签发新 `LeaseID`，旧租约的迟到确认以 `ErrLeaseMismatch` 拒绝，不影响接管者。
2. **重排 vs 触发**：重排先提交则版本递增、租约作废，旧版本确认以 `ErrVersionConflict` 失败；触发先提交则重排以 `ErrAlreadyFired` 失败。
3. **取消 vs 触发**：取消先提交则确认以 `ErrAlreadyCanceled` 失败；触发先提交则取消以 `ErrAlreadyFired` 失败——已提交的触发不能撤销。
4. **更新/暂停 vs 扫描/触发**：更新或暂停先提交则同事务内旧版本未完成实例全部 `SUPERSEDED`、旧版本 `Done`，旧版本的迟到确认以 `ErrInstanceSuperseded` 失败，旧版本不再生成未来实例；新版本已生成的实例属于新键空间（`vN+1/seq`），任何旧版本操作都无法删除或覆盖它。触发先提交则该实例作为历史保留，不受版本切换影响。
5. **扫描 vs 扫描**：版本游标单调前进、实例以 `(版本, 槽位序号)` 幂等落库，多个扫描器并发或重复扫描不会为同一计划时间点生成两个实例。
6. **重复触发**：同一版本/实例的重复确认命中 outbox 幂等键，返回首次的逻辑结果，全系统只发生一次逻辑触发。

## 持久化

`Store` 接口抽象持久化后端：

- `FileStore`：JSON 快照写穿透（临时文件 + rename 原子替换），重启后定时器、计划头、冻结版本、扫描游标、实例、租约、outbox 与请求号记录全部恢复。
- `MemoryStore`：纯内存，用于测试。

## 测试

```sh
go test ./...        # 单元测试
go test -race ./...  # 含并发竞态压测（领取/确认/重排/取消/扫描/更新/暂停/恢复交织）
```

覆盖场景：

- 一次性定时器：请求号幂等重放与冲突、批量领取与租约互斥、旧租约迟到确认、租约过期、重排/取消与触发的双向竞态、已触发不可撤销、outbox 幂等、文件持久化重启恢复。
- 周期计划：创建与版本历史/参数校验、扫描幂等与稳定幂等键、三种补触发策略在停机恢复下的行为、策略冻结在版本上、暂停保留已提交触发并作废旧实例、恢复从新生效时间继续、更新只作废自己旧版本且不删新版本实例、实例租约领取/接管/重复确认、触发失败不阻塞后续时间点、计划时间/生成时间/触发时间三时间字段查询、跨时区与夏令时每日计划、结束时间自动 `FINISHED`、扫描预算分批、定时器与实例混合领取排序、重启游标续接、多 goroutine 竞态不变量（实例键唯一、旧版本无残留 PENDING、FIRED 与 outbox 恒一致）。
