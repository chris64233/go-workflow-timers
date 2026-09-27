# go-workflow-timers

用于承载工作流定时器、触发与取消状态管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

## 功能概览

持久化工作流定时器服务，重点解决**调度、重排与触发之间的竞态**：

- 工作流通过外部定时器号创建、重排、取消定时器；每次重排递增定时器版本。
- 调度器批量领取到期定时器并获得**有期限租约**；只有持有当前版本 + 当前有效租约的领取者能确认触发。
- 触发确认原子保存逻辑结果，并写入带**稳定幂等键**（`workflow/timer/version`）的 outbox；传输层重复投递不会产生第二次逻辑触发。
- 取消/重排与领取/触发相撞时，先提交者胜：已提交的触发不能撤销；先提交的取消或新版本使旧版本触发失败。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 定时器版本 `Version` | 初始为 1，每次重排 +1；触发确认必须匹配当前版本 |
| 租约 `Lease` | 领取时签发，含 `LeaseID`、围栏令牌 `Token` 与过期时间；重排/取消/触发都会作废旧租约 |
| 状态 `State` | `PENDING` → `FIRED` / `CANCELED`，终态不可变更 |
| 幂等键 | outbox 记录键为 `workflowID/timerID/version`，同键插入即幂等 |
| 请求号 `RequestID` | 写操作幂等：同号同内容重试返回原结果，同号不同内容报 `ErrRequestConflict` |

## API

```go
svc, _ := workflowtimers.NewService(workflowtimers.NewFileStore("timers.json"))

// 创建（初始版本 1）
svc.CreateTimer(workflowtimers.CreateRequest{
    WorkflowID: "wf1", TimerID: "remind", RequestID: "req-1",
    FireAt: fireAt, Payload: payload,
})

// 重排（版本 +1，旧租约作废）
svc.RescheduleTimer(workflowtimers.RescheduleRequest{
    WorkflowID: "wf1", TimerID: "remind", RequestID: "req-2", FireAt: newFireAt,
})

// 取消（已触发则报 ErrAlreadyFired）
svc.CancelTimer(workflowtimers.CancelRequest{
    WorkflowID: "wf1", TimerID: "remind", RequestID: "req-3",
})

// 调度器批量领取到期定时器，获得有期限租约
claims, _ := svc.ClaimDue("worker-1", 100, 30*time.Second)

// 触发确认：原子保存逻辑结果 + 写入 outbox；重复确认返回首次结果（Duplicate=true）
rcpt, _ := svc.ConfirmFire("wf1", "remind", claims[0].Timer.Version, claims[0].LeaseID, result)

// 查询
svc.ListPending("wf1") // 待执行定时器
svc.ListOutbox("wf1")  // 待投递的 outbox 记录
svc.GetTimer("wf1", "remind")
```

## 错误分类

调用方用 `errors.Is` 区分冲突类型：

| 错误 | 含义 |
| --- | --- |
| `ErrRequestConflict` | 幂等冲突：请求号相同但内容不同 |
| `ErrVersionConflict` | 版本冲突：定时器已被重排，旧版本触发失败 |
| `ErrLeaseMismatch` / `ErrLeaseExpired` / `ErrNotClaimed` | 租约不属于调用者 / 已过期 / 不存在 |
| `ErrAlreadyFired` / `ErrAlreadyCanceled` | 状态冲突：已触发（不可撤销）/ 已取消 |
| `ErrTimerNotFound` / `ErrTimerExists` | 定时器不存在 / 定时器号已占用 |

## 竞态裁决规则

所有状态变更在单锁事务内完成并写穿透持久化：

1. **领取 vs 领取**：租约有效期内定时器对其他领取者不可见；租约过期后可被接管，接管签发新 `LeaseID`，旧租约的迟到确认以 `ErrLeaseMismatch` 拒绝，不影响接管者。
2. **重排 vs 触发**：重排先提交则版本递增、租约作废，旧版本确认以 `ErrVersionConflict` 失败；触发先提交则重排以 `ErrAlreadyFired` 失败。
3. **取消 vs 触发**：取消先提交则确认以 `ErrAlreadyCanceled` 失败；触发先提交则取消以 `ErrAlreadyFired` 失败——已提交的触发不能撤销。
4. **重复触发**：同一版本的重复确认命中 outbox 幂等键，返回首次的逻辑结果，全系统对同一版本只发生一次逻辑触发。

## 持久化

`Store` 接口抽象持久化后端：

- `FileStore`：JSON 快照写穿透（临时文件 + rename 原子替换），重启后定时器、租约、outbox 与请求号记录全部恢复。
- `MemoryStore`：纯内存，用于测试。

## 测试

```sh
go test ./...        # 单元测试
go test -race ./...  # 含并发竞态压测（领取/确认/重排/取消交织）
```

覆盖场景：请求号幂等重放与冲突、批量领取与租约互斥、旧租约迟到确认、租约过期、重排/取消与触发的双向竞态、已触发不可撤销、outbox 幂等、文件持久化重启恢复、多 goroutine 竞态不变量（每个版本至多一次逻辑触发）。
