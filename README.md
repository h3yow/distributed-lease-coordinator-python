# Distributed Lease Coordinator (local, reproducible)

一个可在本地完整复现、**不依赖外部服务或真实凭据**的分布式租约协调服务。
多个并发客户端可以对同一资源进行租约的申请（acquire）、续约（renew）、
释放（release）与过期回收（expire/reap），并获得一致且可解释的结果。
在租约尚未到期时，当前持有者还可以发起**持有凭证换发（credential
rotation）**：旧凭证立即失效，新凭证只通过本次换发结果交付，可继续续约或释放。
换发必须携带调用方自己的去重编号（`request_id`），以便响应丢失/超时后安全重试，
重试与并发提交都收敛到同一次换发的完整结果。

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
| POST | `/leases` | 申请租约，body: `resource_id, holder, ttl_ms, credential?`；初始凭证只在此响应交付一次 |
| GET | `/leases?limit=&cursor=` | 游标分页列出全部租约（含历史终态；不含凭证本体） |
| GET | `/leases/{lease_id}` | 查询单个租约当前快照（不含凭证本体，只有凭证指纹/版本） |
| POST | `/leases/{lease_id}/renew` | 续约，body: `credential, ttl_ms` |
| POST | `/leases/{lease_id}/release` | 释放，body: `credential` |
| POST | `/leases/{lease_id}/rotate` | 凭证换发（**必须带 `request_id`**），body: `credential, new_credential?, request_id` |
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
- `credential`：持有凭证，**只在申请成功的响应里交付一次**。续约、释放必须原样携带，
  服务端把它换算成密钥相关的指纹后在单个数据库事务内比对；可通过凭证换发（见第 6 节）整体替换。
- `credential_version`：凭证版本号，初始为 0，每成功换发一次 +1，
  用于查询中识别“发生过几次换发”（凭证本体不会出现在任何查询里）
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
| `missing_request_id` | `MissingRequestIdError` | 400 | 换发未带调用方自己的去重编号 |
| `rotation_conflict` | `RotationConflictError` | 409 | 换发请求号已被用于另一组参数 |
| `invalid_request` | `ValueError` | 400 | 请求参数非法（如新旧凭证相同） |

判定顺序（在同一数据库事务内）：存在性 → 凭证 → 已释放 → 已到期 → 执行。
因此“凭证错误”和“已到期”即使同时成立也只会得到一个确定的、可复现的原因。
换发还会在进入状态判定前先校验去重编号、再按请求号做幂等去重（见 6.3 节）。

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
  条件更新要求 `status='HELD' AND credential_hash=? AND expires_at_ms > now`，
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

### 6.2 凭证交付与失效规则（只此一处，落盘无明文）

**交付**：新凭证**只在换发结果中返回**（`CredentialRotation.credential` / HTTP
响应的 `credential` 字段）；申请时的初始凭证同样**只在申请成功的响应里交付一次**。
普通租约快照（`Lease`）、`GET /leases/{id}`、`GET /leases` 列表、
`GET /leases/{id}/events` 与资源历史里都没有凭证本体，只能看到
`credential_version`（换发次数）这类事实。

**失效**：旧凭证在换发成功的同一事务内**立即失效**，之后用旧凭证续约、释放或再次
换发都会得到 `invalid_credential`，不会出现旧、新两套凭证同时有效。
换发只替换凭证与版本号，`lease_id`/`resource_id`/`fencing_token`/`expires_at_ms`
全部不变，也不延长或缩短 TTL。

**落盘保护（检查数据库文件也看不到明文）**：

- 租约行只保存当前凭证的**密钥相关指纹**（HMAC-SHA256，hex），不保存明文；
  续约/释放/换发都按指纹做恒定时间比对。
- 换发记录（`credential_rotations`）只保存“旧凭证指纹、新凭证指纹、
  **加密并带完整性校验后的新凭证**、换发时间、换发时刻冻结的租约事实快照”，
  不保存任何明文。加密用于让“同编号重试（含进程重启）”仍能取回同一个新凭证，
  但直接查看数据库文件只能看到指纹与密文。
- 密钥默认放在数据库文件旁的 `<db名>.key`（权限 `0600`，与 db 分离）；
  内存库使用一次性随机密钥（进程结束即失效）。
- 历史事件 detail 仅记录凭证版本变化、来源（`server`/`caller`）与请求号。
- 早期开发版本若库里还留着明文 `credential` 列，服务启动时会在一次事务内
  自动迁移为指纹并删除明文列。

两种凭证来源都支持：

- 调用方自带：请求带 `new_credential`，服务端原样采用（要求非空且与旧凭证不同）。
- 服务端生成：省略 `new_credential`，服务端生成 `secrets.token_hex(16)`，
  同样只通过换发结果正式交付。

### 6.3 去重编号（request_id）是强制的，重试/并发结果严格一致

**缺少去重编号的约定**：换发必须由调用方提供稳定的 `request_id`。
服务端**不再代分配一次性编号**——因为代分配的编号调用方无法在重试时重现，
一旦第一次响应在返回路上丢失，就会落入“旧凭证已失效、新凭证又取不回来”的死局。
缺少 `request_id`（含空串）时，服务端在**改动任何状态之前**直接返回
`missing_request_id`（HTTP 400）：不换发、不写记录、不产生事件，旧凭证照常可用，
调用方改用自带编号重新发起即可。

**同编号的重复 / 并发 / 超时重试**：`credential_rotations` 以 `request_id` 为主键，
所有写操作在一个 `BEGIN IMMEDIATE` 事务里串行化，因此：

- 相同请求号 + 相同参数（同一租约、同一旧凭证）无论重复多少次、由多少线程/进程并发，
  **只有第一次真正换发**（结果 `ROTATED`），其余全部返回 `REPLAYED`，
  并交出**同一个新凭证、同一凭证版本**。
- 重放结果与首次**逐字段一致**：新凭证、凭证版本、`rotated_at_ms`（见 6.4）以及
  附带的租约事实快照都是首次换发那一刻冻结的内容，不会随重试时刻的续约/到期而变化。
- 该一致性是**持久的**：进程重启后用新服务打开同一数据库文件、用更晚的时钟重试，
  仍然得到同样的 `REPLAYED` 结果与同样的结论；曾经失败（到期/凭证错误）的换发，
  其结论在重启后也不变。
- 相同请求号被用于**不同的目标租约或不同的旧凭证**时，返回
  `rotation_conflict`（HTTP 409），不改动任何状态。
- 新旧凭证相同属于调用方参数错误，返回 `invalid_request`（HTTP 400）。
- 换发在到期判定上与续约/释放完全一致：凭证错误 → `invalid_credential`；
  已释放 → `lease_released`；已到期（含本次请求惰性判定到期）→ `lease_expired`；
  不存在 → `lease_not_found`。

被拒绝的换发不留 `credential_rotations` 记录、不产生 `CREDENTIAL_ROTATED`
事件，同一个请求号在租约恢复有效后（例如失败仅仅因为凭证错误）可再次提交。

### 6.4 `rotated_at_ms` 的含义

换发结果中的 `rotated_at_ms` 是**第一次真正换发发生的时间**，不是本次请求的时间：

- 首次换发（`ROTATED`）：等于该次请求使用的时钟时间。
- 任意重放（`REPLAYED`，含时间推进后的重试、并发重复提交、进程重启后重试）：
  严格等于首次换发时间，恒定不变。
- 换发不改变租约的 `expires_at_ms`；到期回收始终按原始 TTL 判定。

### 6.5 与过期回收、重启的一致性

- 换发与续约/释放使用同一套“`status='HELD' AND credential_hash=? AND
  expires_at_ms > now`”条件更新，与后台回收器互斥：回收先到则换发被拒绝，
  租约置为 `EXPIRED`；换发先到则回收按原 TTL 到期后回收新凭证所在租约。
  两条事务交叉时，最终只会落到“恰好一次 `EXPIRED`”这一种一致状态。
- 已经 `EXPIRED` / `RELEASED` 的租约不会因为换发重新有效，也不会被延长或提前结束。
- 换发记录与事件落盘，进程重启后用新服务打开同一文件：相同请求号重试仍是
  `REPLAYED` 并返回同一个新凭证、同一换发时间，旧凭证依旧不可用；
  失败过的换发结论也不变。
- 旧版本数据库在启动时自动迁移：补 `credential_version` 列、把明文 `credential`
  列重写为 `credential_hash` 指纹并删除明文列。

### 6.6 最小调用示例

```bash
# 1) 申请（可自带凭证，也可省略由服务端生成）；初始凭证只在这一次响应里交付
curl -s -X POST localhost:8000/leases -H 'content-type: application/json' \
  -d '{"resource_id":"orders/42","holder":"worker-1","ttl_ms":30000,"credential":"c-old"}'
# -> 201 {"lease_id":...,"credential":"c-old",...}（之后 GET 不再返回 credential）

# 2a) 换发必须带调用方自己的 request_id；漏掉会在改动任何状态前被明确拒绝
curl -s -X POST localhost:8000/leases/<lease_id>/rotate \
  -H 'content-type: application/json' \
  -d '{"credential":"c-old","new_credential":"c-new"}'
# -> 400 {"code":"missing_request_id", ...}（旧凭证仍可正常使用）

# 2b) 固定 request_id 发起换发；响应里的 credential 是新凭证唯一交付渠道
curl -s -X POST localhost:8000/leases/<lease_id>/rotate \
  -H 'content-type: application/json' \
  -d '{"credential":"c-old","new_credential":"c-new","request_id":"rot-2026-09-30-001"}'
# -> {"status":"ROTATED","credential":"c-new","credential_version":1,
#     "rotated_at_ms":1759200000000,"request_id":"rot-2026-09-30-001", ...}

# 2c) 响应丢失就拿同一个 request_id 原样重试：结论/凭证/rotated_at_ms 全部一致
#     （status 变为 REPLAYED，rotated_at_ms 仍是第一次的时间）

# 3) 之后只用新凭证续约/释放；旧凭证立即 403
```

## 7. 可追溯查询

- `GET /leases`：按插入顺序（SQLite rowid）游标分页，终态租约同样保留。
  换发只更新已有租约行（不新增租约行），因此换发不会造成分页缺口；
  `credential_version` 可看出该租约发生过几次换发；任何列表/快照都不含凭证明文。
- `GET /leases/{id}/events`：单个租约
  `ACQUIRED / RENEWED / RELEASED / EXPIRED / CREDENTIAL_ROTATED`
  的完整迁移链，含 `from_status`、`to_status`、时间戳与文字化判定依据（detail）。
- `GET /resources/{id}/events`：同一资源跨多代租约的完整历史
  （例如 `ACQUIRED → CREDENTIAL_ROTATED → RENEWED`），可解释“谁在什么时候因为什么失去了资源”。
- 事件表只增，事件 id 单调，分页按 id 游标进行，过期回收与换发都不会造成历史缺口。
- 所有查询与事件 detail 均不包含凭证本体（数据库里也只有指纹与加密密文）；
  新凭证只出现在对应换发请求的响应中，初始凭证只出现在申请响应中。

## 8. 代码结构

```
lease_coordinator/
  model.py     # 状态枚举、Lease（只含 credential_hash）/AcquireResult（初始凭证交付）/
               # LeaseEvent/分页值对象、换发结果 CredentialRotation、Clock/FakeClock
  crypto.py    # 凭证静态保护：HMAC 指纹、认证加密（seal/open）、密钥文件加载
  errors.py    # 8 种可区分失败原因（含 missing_request_id、rotation_conflict）
  ids.py       # uuid 与确定性序列 id 生成器
  storage.py   # SQLite schema、原子事务（acquire/renew/release/expire/rotate）、
               # 指纹比对、换发幂等表（封存凭证+冻结快照）、旧库明文迁移、游标编解码
  service.py   # 领域服务：时钟、凭证生成与封存、换发去重/重放与原因映射、后台回收线程、判定日志
  schema.py    # HTTP 请求/响应模型（AcquireView 交付一次初始凭证，LeaseView 无凭证）
  api.py       # FastAPI 工厂 create_app(service) 与错误码映射
main.py        # uvicorn 入口：文件库 + 后台回收器
tests/         # 生命周期/失败原因、并发、过期回收、重启一致性、
               # 凭证换发（含 HTTP）、分页与历史（全部使用 FakeClock，结果确定）
```

## 9. 验证方法

```bash
uv sync
uv run pytest -s          # -s 可看到每个请求的输入与判定依据日志
uv run pytest             # 结尾“业务覆盖概览”按业务分组列出用例与通过/失败结果；
                          # 连续多次执行结果稳定（测试不依赖真实时间与 sleep 竞态）
uv run uvicorn main:app   # 手工验证 HTTP（默认 ./lease.db，密钥落盘在 ./lease.key）
```

落盘无明文可直接检查文件：换发后用 `sqlite3 lease.db` 查看 `leases` 与
`credential_rotations` 表，只能看到 64 位指纹与 hex 密文，看不到任何凭证明文
（测试 `test_persistence_file_contains_no_credential_plaintext` 自动断言这一点）。

重点边界场景均有对应测试：

- 并发申请：20 线程 / 16 HTTP 并发争抢同一资源，断言**恰好 1 个成功、其余全部
  `lease_conflict`**，且没有产生多余的 HELD 行；另有多服务共享同一 db 文件的用例。
- 过期回收：主动回收两次只生效一次；后台回收线程最终把到期租约置为 EXPIRED；
  acquire 路径的惰性回收同样记录 `EXPIRED` 事件。
- 非法续约/释放：凭证错误、已释放、已到期、租约不存在分别得到不同 code。
- 凭证换发（`test_credential_rotation.py`，按 A–F 业务分区）：
  - **缺少去重编号**：不带 `request_id` 在任何状态改动前返回 `missing_request_id`，
    旧凭证仍可续约；并覆盖“响应丢失后带自有编号安全重试、取回完整新凭证”的场景；
  - **重复与并发提交**：同编号 12 线程并发仅 1 次 `ROTATED`、11 次 `REPLAYED`，
    所有人拿到同一凭证、同一换发时间；不同编号并发仅 1 个真正成功；
    编号复用为不同参数得到 `rotation_conflict`；
  - **时间推进后重试**：重放的 `rotated_at_ms` 恒为首次换发时间，
    附带租约快照冻结在换发时刻（即便之后续约/到期）；
  - **进程重启后重试**：新服务、更晚时钟打开同一文件，仍返回同一凭证、同一时间、同一结论；
  - **旧凭证失效 / 新凭证可用**：旧凭证对续约/释放/再换发立即 `invalid_credential`，
    新凭证可正常续约和释放；
  - **查询/历史不泄密**：快照、列表、租约/资源历史都不含新旧凭证，
    数据库文件本身也不含明文；
  - **换发与到期回收交叉**：换发与回收线程并发时最终只有一种一致状态，
    `EXPIRED` 与 `CREDENTIAL_ROTATED` 各至多一次；到期/已回收租约换发被拒且不会复活或延期；
  - 两种凭证来源（调用方自带 / 服务端生成）均有从申请 → 换发 → 续约 → 释放的完整链路用例；
    旧库（明文列）可平滑迁移为指纹且明文列被删除。
- 重启一致性：关闭服务后以新服务、新时钟打开同一文件，到期判定与历史不变。
- 分页与历史：7 条租约分 3 页遍历无重复无遗漏；终态行不消失；资源级事件链完整。
