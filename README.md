# Distributed Lease Coordinator (local, reproducible)

一个可在本地完整复现、**不依赖外部服务或真实凭据**的分布式租约协调服务。
多个并发客户端可以对同一资源进行租约的申请（acquire）、续约（renew）、
释放（release）与过期回收（expire/reap），并获得一致且可解释的结果。
在租约尚未到期时，当前持有者还可以发起**持有凭证换发（credential
rotation）**：旧凭证立即失效，新凭证只通过本次换发结果交付，可继续续约或释放。

- 语言 / 运行时：Python 3.14，仅依赖 `fastapi[standard]`（测试用 `pytest`）
- 存储：本地 SQLite（WAL 模式，文件库或内存库），无需任何外部数据库
- 时间：可注入的时钟（`FakeClock`），测试结果与执行速度无关、可重复
- 凭据：服务端自动生成 `secrets.token_hex(16)` 凭证，也允许调用方显式传入；
  申请与换发均支持这两种凭证来源

## 1. 快速开始

```bash
uv sync
uv run uvicorn main:app                       # 启动 HTTP 服务（默认 ./lease.db）
LEASE_DB=/tmp/lease.db uv run uvicorn main:app --port 8000
uv run pytest -s                              # 运行全部测试并打印判定日志
```

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/leases` | 申请租约，body: `resource_id, holder, ttl_ms, credential?` |
| GET | `/leases?limit=&cursor=` | 游标分页列出全部租约（含历史终态） |
| GET | `/leases/{lease_id}` | 查询单个租约当前快照（不含凭证本体） |
| POST | `/leases/{lease_id}/renew` | 续约，body: `credential, ttl_ms` |
| POST | `/leases/{lease_id}/release` | 释放，body: `credential` |
| POST | `/leases/{lease_id}/rotate` | 凭证换发，body: `credential, new_credential?, request_id?` |
| POST | `/maintenance/expire-due` | 主动回收所有到期租约 |
| GET | `/leases/{lease_id}/events?limit=&cursor=` | 单个租约的状态迁移历史 |
| GET | `/resources/{resource_id}/events?limit=&cursor=` | 某资源的完整迁移链 |

游标（cursor）为不透明的 base64url 字符串，以上一页最后一条记录的位置编码，
重复请求稳定，不随过期回收而变化。

### 最小调用示例

```bash
curl -s -X POST localhost:8000/leases \
  -H 'content-type: application/json' \
  -d '{"resource_id":"orders/42","holder":"worker-1","ttl_ms":5000}'
# -> 201 返回 lease_id / fencing_token / expires_at_ms（凭证由服务端生成）
```

## 2. 租约模型

每条租约（`lease_coordinator.model.Lease`）包含：

- `lease_id`：租约唯一标识（生产用 uuid4，测试用确定性序列 `lease-1, lease-2, ...`）
- `resource_id` / `holder`：资源与持有者
- `credential`：持有凭证。续约、释放必须原样携带，服务端在单个数据库事务内比对；
  可通过凭证换发（见第 6 节）整体替换
- `credential_version`：凭证版本号，初始为 0，每成功换发一次 +1，
  用于查询中识别“发生过几次换发”（凭证本体不会出现在查询里）
- `fencing_token`：**每个资源单调递增的 fencing token**，每次成功申请 +1，
  用于下游识别“旧持有者”的过期写操作（换发不改变 fencing token）
- `status`：生命周期状态机，仅有三种状态
  - `HELD`：已授予且未到期
  - `RELEASED`：持有者显式释放（终态）
  - `EXPIRED`：TTL 到期后被回收（终态）
- `acquired_at_ms` / `expires_at_ms` / `released_at_ms`：毫秒时间戳
- `expire_reason`：回收原因（惰性回收 / 续约时发现到期 / 后台回收器等）
- `version`：每次状态迁移自增的乐观版本号

状态迁移：

```
                 acquire
(none) ───────────────────────► HELD
                                  │  renew（只延长 expires_at_ms，状态仍为 HELD）
                                  │  release（凭证正确且未到期）
                                  ├──────────────► RELEASED（终态）
                                  │  expires_at_ms <= now
                                  └──────────────► EXPIRED（终态）
                                   ▲ 续约/再申请时惰性触发，或后台回收器主动触发
```

被拒绝的操作**不会产生状态迁移**，但拒绝原因仍记录在服务日志中；
每次成功的状态变化都会向 `lease_events` 追加一条只增（append-only）事件。

## 3. 失败原因（可区分）

所有业务失败都抛出/返回带稳定 `code` 的类型化错误，HTTP 层映射如下：

| code | 异常 | HTTP | 触发条件 |
| --- | --- | --- | --- |
| `lease_conflict` | `LeaseConflictError` | 409 | 申请时资源仍被另一未到期租约持有 |
| `invalid_credential` | `InvalidCredentialError` | 403 | 续约/释放凭证与记录不符 |
| `lease_expired` | `LeaseExpiredError` | 409 | 租约已到期（含本次请求惰性判定到期） |
| `lease_released` | `LeaseReleasedError` | 409 | 租约此前已被释放 |
| `lease_not_found` | `LeaseNotFoundError` | 404 | 租约 id 不存在 |
| `invalid_cursor` | `InvalidCursorError` | 400 | 分页游标损坏 |
| `rotation_conflict` | `RotationConflictError` | 409 | 换发请求号已被用于另一组参数 |
| `invalid_request` | `ValueError` | 400 | 请求参数非法（如新旧凭证相同） |

判定顺序（在同一数据库事务内）：存在性 → 凭证 → 已释放 → 已到期 → 执行。
因此“凭证错误”和“已到期”即使同时成立也只会得到一个确定的、可复现的原因。
换发在进入上述判定前，还会先按请求号做幂等去重（见 6.3 节）。

## 4. 过期与回收规则

1. **判定基准是注入时钟的当前毫秒时间**，过期条件为 `expires_at_ms <= now_ms`。
   所有判定只依赖记录里的绝对时间戳，进程重启后用新时钟继续判定，结论不变。
2. **两种回收路径，结果幂等且一致**：
   - 主动回收：后台 daemon 线程（`start_reaper`，默认每 100ms 一批）或
     `POST /maintenance/expire-due`，扫描 `HELD 且到期` 的租约逐个置为 `EXPIRED`。
   - 惰性回收：对到期租约执行 renew/release 时在同一事务内将其置为 `EXPIRED` 并拒绝；
     对到期资源再次 acquire 时在同一事务内先回收旧租约再授予新租约。
3. **不会重复回收**：回收 SQL 带 `WHERE status='HELD' AND expires_at_ms <= now`
   条件并检查 `rowcount`；已处于终态的租约再次回收返回空，不追加重复事件。
4. 到期释放出的资源可被重新申请，新租约获得更大的 fencing token。
5. 租约行与事件行**只增不删**：过期/回收/清理都不会让查询结果静默缺失。

## 5. 并发保证范围

- **单资源互斥**：同一资源在同一时刻最多一个 `HELD` 租约。
  申请路径（“读最新租约 → 判定 → 回收到期者 → 插入新租约 → 递增 fencing token”）
  整体放在一个 `BEGIN IMMEDIATE` 事务中，由 SQLite 写锁串行化，
  因此同进程多线程（另有进程内 `RLock`）与**多进程共享同一 db 文件**
  都不会出现重复授予。锁冲突会做有界退避重试（默认 10 次）。
- **凭证校验原子性**：续约/释放的“读取 + 条件更新”在同一事务内完成，
  条件更新要求 `status='HELD' AND credential=? AND expires_at_ms > now`，
  并发的回收与续约只有一方能成功，另一方得到准确的终态原因。
- **时间一致性**：时钟可注入；所有时间判定使用绝对时间戳，事务冲突重试不会
  改变最终状态（事务是“全有或全无”的），进程重启后历史与判定结论保持一致。
- **不在保证范围内**：本服务是单机本地实现（SQLite），不提供跨地域网络复制、
  不做墙钟之间的时钟同步（多机部署需由部署方保证时钟，或统一使用本服务节点时间）；
  fencing token 的**下游强制执行**（例如存储层拒绝旧 token 写入）需要资源持有方配合，
  本服务负责单调发放与暴露。

## 6. 持有凭证换发（credential rotation）

### 6.1 使用场景

租约尚未到期、但持有者需要更换凭证时（例如凭证疑似泄露、安全策略要求
定期轮换、进程要把持有权交给一组新的运行时凭证），可以调用
`rotate_credential` / `POST /leases/{id}/rotate`：

- 旧凭证在换发成功的同一事务内**立即失效**，之后用旧凭证续约、释放或再次
  换发都会得到 `invalid_credential`，不会出现旧、新两套凭证同时有效。
- 租约本身不变：`lease_id`、`resource_id`、`fencing_token`、`expires_at_ms`
  都保持不变，只替换 `credential`，并把 `credential_version` 加 1。
  换发**不延长也不缩短** TTL，回收链路仍按原 `expires_at_ms` 判定。
- 业务方拿到新凭证后，照常走续约 / 释放主链路。

### 6.2 凭证交付规则（只此一处）

新凭证**只在换发结果中返回**（`CredentialRotation.credential` / HTTP
响应的 `credential` 字段）。普通租约快照、`GET /leases` 列表、
`GET /leases/{id}/events` 与资源历史都不会带出任何凭证本体；历史里只追加
一条 `CREDENTIAL_ROTATED` 事件，detail 仅记录凭证版本变化、凭证来源
（`server`/`caller`）和请求号，不包含旧凭证或新凭证。

两种凭证来源都支持：

- 调用方自带：请求带 `new_credential`，服务端原样采用（要求非空且与旧凭证不同）。
- 服务端生成：省略 `new_credential`，服务端生成 `secrets.token_hex(16)`，
  同样通过换发结果正式交付。

### 6.3 幂等请求号与明确结果

每次换发都有一个 `request_id`（可由调用方传入；省略时服务端分配一个
`srv-` 前缀的一次性请求号，这种情况下调用方无法透明重试，重试请自带请求号）。

`credential_rotations` 表以 `request_id` 为主键记录每一次**成功**换发：

- 重复提交 / 并发提交 / 超时重试使用**相同请求号与相同参数**时，只有第一次真正换发
  （结果 `ROTATED`），其余全部返回 `REPLAYED` 并交出同一个新凭证——
  同一租约的并发换发因此最多一次真正成功。
- 相同请求号被用于**不同的目标租约或不同的旧凭证**时，返回
  `rotation_conflict`（HTTP 409），不会有任何状态被改动。
- 新凭证与旧凭证相同属于调用方参数错误，返回 `invalid_request`（HTTP 400）。
- 换发在到期判定上与续约/释放完全一致：凭证错误 → `invalid_credential`；
  已释放 → `lease_released`；已到期（含本次请求惰性判定到期）→ `lease_expired`；
  不存在 → `lease_not_found`。

被拒绝的换发不留 `credential_rotations` 记录、不产生 `CREDENTIAL_ROTATED`
事件，同一个请求号在租约恢复有效后（例如失败仅仅因为凭证错误）可再次提交。

### 6.4 与过期回收、重启的一致性

- 换发与续约/释放使用同一套“`status='HELD' AND credential=? AND
  expires_at_ms > now`”条件更新，与后台回收器互斥：回收先到则换发被拒绝，
  租约置为 `EXPIRED`；换发先到则回收按原 TTL 到期后回收新凭证所在租约。
- 已经 `EXPIRED` / `RELEASED` 的租约不会因为换发重新有效，也不会被延长或提前结束。
- 换发记录与事件落盘，进程重启后用新服务打开同一文件：相同请求号重试仍是
  `REPLAYED` 并返回同一个新凭证，旧凭证依旧不可用；失败过的换发结论也不变。
- 旧版本数据库（无 `credential_version` 列）在启动时通过一次
  `ALTER TABLE` 迁移升级，历史租约的凭证版本从 0 开始。

### 6.5 最小调用示例

```bash
# 1) 申请（可自带凭证，也可省略由服务端生成）
curl -s -X POST localhost:8000/leases -H 'content-type: application/json' \
  -d '{"resource_id":"orders/42","holder":"worker-1","ttl_ms":30000,"credential":"c-old"}'

# 2) 换发：固定 request_id 以便安全重试；响应里的 credential 是唯一交付渠道
curl -s -X POST localhost:8000/leases/<lease_id>/rotate \
  -H 'content-type: application/json' \
  -d '{"credential":"c-old","new_credential":"c-new","request_id":"rot-2026-09-30-001"}'
# -> {"status":"ROTATED","credential":"c-new","credential_version":1,"request_id":"...", ...}

# 3) 之后只用新凭证续约/释放；旧凭证立即 403
```

## 7. 可追溯查询

- `GET /leases`：按插入顺序（SQLite rowid）游标分页，终态租约同样保留。
  换发只更新已有租约行（不新增租约行），因此换发不会造成分页缺口；
  `credential_version` 可看出该租约发生过几次换发。
- `GET /leases/{id}/events`：单个租约
  `ACQUIRED / RENEWED / RELEASED / EXPIRED / CREDENTIAL_ROTATED`
  的完整迁移链，含 `from_status`、`to_status`、时间戳与文字化判定依据（detail）。
- `GET /resources/{id}/events`：同一资源跨多代租约的完整历史
  （例如 `ACQUIRED → CREDENTIAL_ROTATED → RENEWED`），可解释“谁在什么时候因为什么失去了资源”。
- 事件表只增，事件 id 单调，分页按 id 游标进行，过期回收与换发都不会造成历史缺口。
- 所有查询与事件 detail 均不包含凭证本体；新凭证只出现在对应换发请求的响应中。

## 8. 代码结构

```
lease_coordinator/
  model.py     # 状态枚举、Lease/LeaseEvent/分页值对象、轮换结果 CredentialRotation、Clock/FakeClock
  errors.py    # 7 种可区分失败原因（含 rotation_conflict）
  ids.py       # uuid 与确定性序列 id 生成器
  storage.py   # SQLite schema、原子事务（acquire/renew/release/expire/rotate）、
               # credential_rotations 幂等表、旧库迁移、游标编解码
  service.py   # 领域服务：时钟、凭证生成、轮换幂等与原因映射、后台回收线程、判定日志
  schema.py    # HTTP 请求/响应模型
  api.py       # FastAPI 工厂 create_app(service) 与错误码映射
main.py        # uvicorn 入口：文件库 + 后台回收器
tests/         # 生命周期/失败原因、并发、过期回收、重启一致性、
               # 凭证换发（含 HTTP）、分页与历史（全部使用 FakeClock，结果确定）
```

## 9. 验证方法

```bash
uv run pytest -s          # -s 可看到每个请求的输入与判定依据日志
uv run pytest             # 连续多次执行结果稳定（测试不依赖真实时间与 sleep 竞态）
```

重点边界场景均有对应测试：

- 并发申请：20 线程 / 16 HTTP 并发争抢同一资源，断言**恰好 1 个成功、其余全部
  `lease_conflict`**，且没有产生多余的 HELD 行；另有多服务共享同一 db 文件的用例。
- 过期回收：主动回收两次只生效一次；后台回收线程最终把到期租约置为 EXPIRED；
  acquire 路径的惰性回收同样记录 `EXPIRED` 事件。
- 非法续约/释放：凭证错误、已释放、已到期、租约不存在分别得到不同 code。
- 凭证换发：正常换发后旧凭证对续约/释放/再换发立即失效；同请求号重放返回同一新凭证；
  12 线程同请求号并发仅 1 次 `ROTATED`、11 次 `REPLAYED`；不同请求号并发仅 1 个真正成功；
  请求号复用为不同参数得到 `rotation_conflict`；到期/已回收租约换发被拒且不会复活或延期；
  换发不延长 TTL，回收按原时间生效；重启后重放仍返回同一凭证；旧库可平滑迁移；
  历史只暴露换发事实而不含新旧凭证；换发与回收穿插时分页无重复无遗漏。
- 两种凭证来源（调用方自带 / 服务端生成）均有从申请 → 换发 → 续约 → 释放的完整链路用例。
- 重启一致性：关闭服务后以新服务、新时钟打开同一文件，到期判定与历史不变。
- 分页与历史：7 条租约分 3 页遍历无重复无遗漏；终态行不消失；资源级事件链完整。
