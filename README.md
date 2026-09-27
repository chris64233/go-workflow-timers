# go-workflow-timers

持久化工作流定时器库：工作流按**外部定时器号**创建、重排、取消定时器；调度器批量
**领取**到期定时器并取得**有期限租约**；持有当前版本与当前租约的领取者才能**确认
触发**，触发时把逻辑结果与 [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
事件在同一事务落盘。

开发环境：Go 1.23.0，零第三方依赖。

运行测试：

    go test -race ./...

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 定时器号 `TimerID` | 工作流侧分配的外部 ID，全生命周期稳定。 |
| 版本 `Version` | 从 1 开始，每次重排加 1。版本就是 **fencing token**：旧版本的一切写入都会被拒绝。 |
| 请求号 `RequestID` | 工作流/调度器请求的幂等键。同号 + 同内容重试返回首次结果；同号不同内容报 `idempotent_conflict`。 |
| 租约 `LeaseToken` / `LeaseExpires` | 领取时生成的随机令牌与期限。重排、取消、接管都会更换/作废租约；过期后可被他人接管。 |
| outbox 键 `OutboxKey` | 稳定幂等键 `timer:<timerID>:v<version>`，每个定时器版本至多一条，传输层可安全重复投递。 |

### 状态机

```
                create                  claim (到期)
    (不存在) ──────────────▶ scheduled ──────────────▶ claimed
                               ▲  │                      │  │
                  reschedule ─┘  │ cancel               │  │ reschedule / cancel
                                 ▼                      │  ▼
                             cancelled ◀──────── cancel ─┤  (版本+1，旧租约作废，回到 scheduled)
                                                        │
                                       confirm trigger  ▼
                                                    triggered（终态，不可取消/重排）
```

- `claimed` 的租约过期后，定时器重新对领取开放（新租约、新令牌），即**接管**。
- `triggered` / `cancelled` 是终态；已提交的触发不可撤销。

## API（`Store` 接口）

| 方法 | 语义 |
| --- | --- |
| `CreateTimer` | 创建定时器（v1, scheduled）。号已存在 → `already_exists`。 |
| `RescheduleTimer` | 版本 +1、回到 scheduled、旧租约立即失效。终态 → `state_conflict`。 |
| `CancelTimer` | 取消；已触发 → `state_conflict`（触发不可撤销）；重复取消幂等。 |
| `ClaimDue` | 批量领取 `scheduled 且 fire_at<=now` 与 `claimed 且租约已过期` 的记录，返回新租约。 |
| `ConfirmTrigger` | 见下方判定；成功则同事务写定时器终态、触发结果、outbox 事件。 |
| `DuePending` | 待执行查询：到期待领取 + 租约已过期待接管（不含租约有效者与终态）。 |
| `GetTimer` / `GetTriggerResult` | 读取快照 / 某版本的触发结果。 |
| `ListOutboxPending` / `MarkOutboxDelivered` | 传输层拉取待投递事件、确认投递（重复标记幂等）。 |

### 确认触发的判定顺序（竞态分类）

`ConfirmTrigger` 在同一事务内按下列顺序判定，失败互不混淆：

1. **幂等重放**：请求号与内容都相同 → 直接返回首次结果（传输层可随意重试）。
2. 同版本已是 `triggered` → 返回已持久化的结果（即使没带请求号，逻辑触发也只一次）。
3. `version != 当前版本` → **`version_conflict`**：重排已先提交，旧版本不得触发。
4. `state != claimed` → **`state_conflict`**：取消先提交，或从未领取。
5. 令牌不一致 / 为空 → **`lease_expired`**：旧领取者的迟到确认，接管者已换令牌。
6. 租约已过期 → **`lease_expired`**。

错误类型为 `*workflowtimers.Error`，用 `errors.As` 取出 `Kind`：

| Kind | 含义 |
| --- | --- |
| `not_found` | 定时器 / 结果 / outbox 事件不存在。 |
| `already_exists` | 创建时定时器号已存在。 |
| `state_conflict` | 状态不允许该操作（终态重排/取消、未领取就确认等）。 |
| `version_conflict` | 操作针对的版本已过期（重排抢先提交）。 |
| `lease_expired` | 租约令牌不匹配或租约到期。 |
| `idempotent_conflict` | 请求号相同但内容不同。 |

### 竞态保证

- **触发原子性**：`timer → triggered`、`trigger_result`、`outbox` 三行在一个事务内提交，
  外部只能观察到“全有”或“全无”。
- **只触发一次**：同版本的重复确认（重传、超时重试）一律重放首次结果，不会产生第二条
  outbox；稳定 outbox 键在真实库上再加唯一索引兜底。
- **重排 vs 触发**：谁先提交谁赢。重排先提交 → 旧版本确认 `version_conflict` 且不留任何
  结果/outbox；触发先提交 → 重排 `state_conflict`。
- **取消 vs 触发**：同理。触发先提交则取消失败、触发保留；取消先提交则确认失败、
  无 outbox。
- **接管 vs 旧领取者**：租约过期后接管者拿到新令牌；旧领取者的迟到确认因令牌不符
  `lease_expired`，不可能覆盖接管者的结果。

## 持久化映射

当前提供 `MemoryStore`（生产语义的内存实现，单互斥锁模拟可串行化事务，支持并发，
可用 `WithClock` 注入时钟）。每个方法即一条数据库事务，迁移到 SQL 时的关键写法：

```sql
-- 领取：把到期/过期记录批量原子地转为自己的新租约（可用 SKIP LOCKED 并行化）
UPDATE timers
   SET state='claimed', lease_token=$newToken,
       lease_expires_ms=$now+$lease, updated_ms=$now
 WHERE id IN (
   SELECT id FROM timers
    WHERE (state='scheduled' AND fire_at_ms <= $now)
       OR (state='claimed'   AND lease_expires_ms <= $now)
    ORDER BY fire_at_ms, id LIMIT $batch
   FOR UPDATE SKIP LOCKED)
RETURNING id, version, fire_at_ms, payload, lease_token, lease_expires_ms;

-- 确认触发：条件更新，影响行数 0 时在同事务内复查版本/状态/租约以分类报错
UPDATE timers
   SET state='triggered', lease_token=NULL, lease_expires_ms=NULL,
       triggered_ms=$now, updated_ms=$now
 WHERE id=$id AND version=$version AND state='claimed'
   AND lease_token=$token AND lease_expires_ms > $now;

-- 结果 + outbox 同事务插入；outbox.key 上有 UNIQUE 索引
INSERT INTO trigger_results (...) VALUES (...);
INSERT INTO outbox (key, timer_id, version, payload, status, created_ms)
VALUES ($key, $id, $version, $payload, 'pending', $now)
ON CONFLICT (key) DO NOTHING;
```

- 请求号表 `requests(request_id PRIMARY KEY, op, timer_id, content_fingerprint, snapshot)`
  保存首次成功请求的内容指纹与结果，支撑幂等重放与冲突检测。
- 指纹建议包含“操作类型 + 定时器号 + 全部业务字段”，防止请求号跨操作/跨定时器复用。

## 典型调用时序

```
工作流:  CreateTimer(T, req=1, fireAt)
调度器:  ClaimDue(lease=30s) ──▶ (T, v1, token=L1)
工作流:  RescheduleTimer(T, req=2, fireAt')        # 版本 → v2，L1 作废
调度器:  ConfirmTrigger(T, v1, L1) ──▶ version_conflict   # 旧租约迟到确认
调度器:  ClaimDue(...) ──▶ (T, v2, token=L2)             # 新版本到期后
调度器:  ConfirmTrigger(T, v2, L2, result) ──▶ ok
         （同事务：timer=triggered、result 落盘、outbox[timer:T:v2] 待投递）
传输层:  ListOutboxPending → 投递 → MarkOutboxDelivered（重复投递/确认均安全）
```

## 代码结构

| 文件 | 内容 |
| --- | --- |
| `model.go` | 状态、定时器/结果/outbox 数据结构与稳定键生成。 |
| `errors.go` | 结构化错误与 `ErrorKind` 分类。 |
| `store.go` | `Store` 接口及各操作入参，文档化每个竞态判定。 |
| `memory.go` | `MemoryStore`：并发安全的事务语义参考实现。 |
| `store_test.go` | 单元测试与并发竞态压力测试（`-race`）。 |
