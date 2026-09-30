# Distributed Lease Coordinator (local, reproducible)

一个可在本地完整复现、**不依赖外部服务或真实凭据**的分布式租约协调服务。
多个并发客户端可以对同一资源进行租约的申请（acquire）、续约（renew）、
释放（release）与过期回收（expire/reap），并获得一致且可解释的结果。

- 语言 / 运行时：Python 3.14，仅依赖 `fastapi[standard]`（测试用 `pytest`）
- 存储：本地 SQLite（WAL 模式，文件库或内存库），无需任何外部数据库
- 时间：可注入的时钟（`FakeClock`），测试结果与执行速度无关、可重复
- 凭据：服务端自动生成 `secrets.token_hex(16)` 凭证，也允许调用方显式传入
- 持有凭证换发（rotation）：租约未到期时，持有者可显式换一套新凭证，
  旧凭证立即失效；换发以调用方提供的 `rotation_id` 做幂等，重试结果明确

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
| GET | `/leases/{lease_id}` | 查询单个租约当前快照 |
| POST | `/leases/{lease_id}/renew` | 续约，body: `credential, ttl_ms` |
| POST | `/leases/{lease_id}/release` | 释放，body: `credential` |
| POST | `/leases/{lease_id}/rotate` | **持有凭证换发**，body: `credential, rotation_id, new_credential?` |
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

> 注意：HTTP 的申请响应出于“查询不泄露凭证”的考虑**不回传凭证本体**，
> 因此经 HTTP 使用服务端生成的初始凭证时，应在申请时显式传入 `credential`。
> 无论初始凭证来自哪一方，**换发接口都会在本次响应里明确交付新凭证**，
> 这是拿到轮换后正确凭证的正式途径（见第 5 节）。

```bash
# 持有期间换发：服务端生成新凭证
curl -s -X POST localhost:8000/leases/<lease_id>/rotate \
  -H 'content-type: application/json' \
  -d '{"credential":"<当前凭证>","rotation_id":"rotate-0001"}'
# -> 200 {"rotation_id":..., "new_credential":"...", "replayed":false, "lease":{...}}
# 之后续约/释放一律使用 new_credential；旧凭证立即失效
```

## 2. 租约模型

每条租约（`lease_coordinator.model.Lease`）包含：

- `lease_id`：租约唯一标识（生产用 uuid4，测试用确定性序列 `lease-1, lease-2, ...`）
- `resource_id` / `holder`：资源与持有者
- `credential`：持有凭证。续约、释放必须原样携带，服务端在单个数据库事务内比对
- `fencing_token`：**每个资源单调递增的 fencing token**，每次成功申请 +1，
  用于下游识别“旧持有者”的过期写操作
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

**凭证换发不改变租约状态**：`HELD → HELD`，仅替换 `credential` 并使
`version + 1`，`expires_at_ms` / `holder` / `fencing_token` 均保持不变，
另追加一条 `CREDENTIAL_ROTATED` 事件（详见第 5 节）。

## 3. 失败原因（可区分）

所有业务失败都抛出/返回带稳定 `code` 的类型化错误，HTTP 层映射如下：

| code | 异常 | HTTP | 触发条件 |
| --- | --- | --- | --- |
| `lease_conflict` | `LeaseConflictError` | 409 | 申请时资源仍被另一未到期租约持有 |
| `invalid_credential` | `InvalidCredentialError` | 403 | 续约/释放/换发凭证与记录不符（含并发换发落败方） |
| `lease_expired` | `LeaseExpiredError` | 409 | 租约已到期（含本次请求惰性判定到期） |
| `lease_released` | `LeaseReleasedError` | 409 | 租约此前已被释放 |
| `lease_not_found` | `LeaseNotFoundError` | 404 | 租约 id 不存在 |
| `invalid_cursor` | `InvalidCursorError` | 400 | 分页游标损坏 |
| `rotation_conflict` | `RotationConflictError` | 409 | `rotation_id` 曾以不同参数提交（幂等号冲突） |
| `invalid_rotation` | `InvalidRotationError` | 422 | 换发请求非法（如新凭证与当前凭证相同） |

判定顺序（在同一数据库事务内）：存在性 → 凭证 → 已释放 → 已到期 → 执行。
因此“凭证错误”和“已到期”即使同时成立也只会得到一个确定的、可复现的原因
（换发在真正执行切换前再检查新旧凭证是否相同）。

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

## 5. 持有凭证换发（credential rotation）

### 使用场景

- 怀疑当前持有凭证泄露，需要在**不释放、不重新申请**（租约不中断、
  fencing token 不变）的前提下作废旧凭证、启用新凭证；
- 按安全策略周期性更换持有密钥；
- 持有者内部交接（例如进程内主控切换），由当前持有者主动换发后把
  新凭证交给后续业务方继续续约/释放。

### 调用与凭证交付规则

`POST /leases/{lease_id}/rotate`，请求体：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `credential` | 是 | 当前有效凭证（换发发起方身份证明） |
| `rotation_id` | 是 | 调用方生成的**一次性幂等号**（非密，建议 uuid），用于重试去重 |
| `new_credential` | 否 | 调用方自带的新凭证；不传则服务端生成 `secrets.token_hex(16)` |

成功响应 `RotationResultView`：

- `new_credential`：**新凭证只在这一次换发结果里交付**。
  调用方自带时即其提交值；服务端生成时为生成值。
- `replayed`：`false` 表示这是首次生效的换发；`true` 表示这是同一个
  `rotation_id` 的重复请求，服务端从持久化记录中原样回放，
  `new_credential` 与首次完全一致。**因此网络重试 / 事务重试 / 进程重启后
  再提交都能得到确定、可验收的同一结论，不会悄悄又换一套凭证。**
- `lease`：换发后的租约快照（与普通查询一样不含凭证字段）。

### 失效与一致性规则

1. **换发成功即原子切换**：在同一个 `BEGIN IMMEDIATE` 事务内完成
   “校验当前凭证 → 条件更新 credential、version+1 → 写轮换记录 →
   追加 `CREDENTIAL_ROTATED` 事件”。提交后旧凭证**立即**对续约、释放、
   再次换发全部失效；不存在新旧两套凭证同时有效的窗口。
2. **换发不延长、不缩短租约**：`expires_at_ms`、`holder`、`fencing_token`
   均不变；后台回收仍按原到期时间工作，轮换不会把到期时间往后推。
3. **并发轮换只有一次真正生效**：同一租约的多个并发换发由 SQLite 写锁
   串行化。不同 `rotation_id` 竞争时，只有第一个用当前凭证成功，其余因
   凭证已被更换而得到明确的 `invalid_credential`；同一个 `rotation_id`
   被并发/重复提交时，所有调用方拿到同一条结果（恰好 1 个 `replayed=false`，
   其余 `replayed=true`）。
4. **幂等号冲突明确报错**：同一个 `rotation_id` 已提交过，却带着**不同的
   lease / 当前凭证 / 自带新凭证**再次提交，返回 `rotation_conflict`
   （HTTP 409），且**不修改任何状态**——这是“无法判定是错误重试还是
   号被复用”的情形，拒绝得毫不含糊。
5. **与到期回收严格交错**：判定顺序与续约一致
   （存在性 → 凭证 → 已释放 → 已到期 → 执行）。
   - 换发时租约恰好到期：在同一事务内将其置为 `EXPIRED`（惰性回收），
     返回 `lease_expired`，记录 `EXPIRED` 事件，**不会**因换发复活或延长；
   - 已过期 / 已回收 / 已释放的租约，换发一律被拒，终态不变；
   - 换发成功后租约仍按原 TTL 到期，到期后新凭证同样不能再操作。
6. **幂等回放与终态无关**：换发已成功、此后租约到期，用原参数重放同一个
   `rotation_id` 仍返回当初那条 `replayed=true` 的结果（历史事实可复查），
   但这不会改变租约当前的 `EXPIRED` 终态。
7. **两种凭证来源都走同一主链路**：申请时无论自带凭证还是服务端生成，
   换发后都能用新凭证完成续约与释放（领域层 `LeaseService` 在申请结果里
   携带凭证；HTTP 层出于防泄露不在申请响应回传，故 HTTP 调用方申请时
   建议自带初始凭证，或直接以换发接口获取后续凭证）。
8. **新凭证不得与当前凭证相同**，否则返回 `invalid_rotation`（HTTP 422），
   不产生状态迁移。

### 历史与隐私

- 每次成功换发追加一条 `CREDENTIAL_ROTATED` 事件（`HELD → HELD`），
  detail 中记录非密的 `rotation_id`、fencing token、version，
  便于回答“何时、因哪次申请发生过换发、结果如何”。
- **任何查询与历史都不含凭证本体**：旧凭证、新凭证都不会出现在
  `/leases`、`/leases/{id}`、events 接口或事件 detail 中；轮换记录里
  对“发起方出示的凭证”只保存不可逆的 SHA-256 摘要，仅供幂等回放校验。
- 新凭证仅出现在换发接口那一次响应中。

## 6. 与现有行为的兼容范围

- 换发是**纯增量能力**：不调用 `/rotate` 时，申请 / 续约 / 释放 / 回收 /
  查询的请求与响应、状态机、事件类型、错误码、分页游标全部与之前一致。
- 租约行结构未变；新增的 `credential_rotations` 表由 `CREATE TABLE IF NOT
  EXISTS` 幂等创建，**打开旧 db 文件自动建表**，历史数据无需迁移。
- `CREDENTIAL_ROTATED` 是新增事件类型；既有按事件类型过滤/展示的客户端
  会把它视为一条普通的 `HELD → HELD` 迁移记录，事件不含凭证，兼容旧消费方。
- 换发不改变 fencing token：下游基于 fencing token 的旧持有者防护不受影响。
- 新增错误码 `rotation_conflict`（409）与 `invalid_rotation`（422）只在
  `/rotate` 上出现，不改变既有接口的错误码集合。

## 7. 并发保证范围

- **单资源互斥**：同一资源在同一时刻最多一个 `HELD` 租约。
  申请路径（“读最新租约 → 判定 → 回收到期者 → 插入新租约 → 递增 fencing token”）
  整体放在一个 `BEGIN IMMEDIATE` 事务中，由 SQLite 写锁串行化，
  因此同进程多线程（另有进程内 `RLock`）与**多进程共享同一 db 文件**
  都不会出现重复授予。锁冲突会做有界退避重试（默认 10 次）。
- **凭证校验原子性**：续约/释放的“读取 + 条件更新”在同一事务内完成，
  条件更新要求 `status='HELD' AND credential=? AND expires_at_ms > now`，
  并发的回收与续约只有一方能成功，另一方得到准确的终态原因。
- **凭证换发原子性**：换发的“查幂等记录 → 读租约 → 校验 → 条件更新凭证 →
  写轮换记录/事件”全部在同一个 `BEGIN IMMEDIATE` 事务中，条件更新要求
  `status='HELD' AND credential=? AND expires_at_ms > now`，因此并发换发、
  并发续约/释放与并发回收之间严格串行；同一 `rotation_id` 靠唯一主键去重，
  任何重试都只会回放已提交的那一条结果。
- **时间一致性**：时钟可注入；所有时间判定使用绝对时间戳，事务冲突重试不会
  改变最终状态（事务是“全有或全无”的），进程重启后历史与判定结论保持一致。
- **不在保证范围内**：本服务是单机本地实现（SQLite），不提供跨地域网络复制、
  不做墙钟之间的时钟同步（多机部署需由部署方保证时钟，或统一使用本服务节点时间）；
  fencing token 的**下游强制执行**（例如存储层拒绝旧 token 写入）需要资源持有方配合，
  本服务负责单调发放与暴露。

## 8. 可追溯查询

- `GET /leases`：按插入顺序（SQLite rowid）游标分页，终态租约同样保留。
- `GET /leases/{id}/events`：单个租约 `ACQUIRED / RENEWED / RELEASED /
  EXPIRED / CREDENTIAL_ROTATED` 的完整迁移链，含 `from_status`、`to_status`、
  时间戳与文字化判定依据（detail）。
- `GET /resources/{id}/events`：同一资源跨多代租约的完整历史
  （例如 `ACQUIRED → EXPIRED → ACQUIRED`），可解释“谁在什么时候因为什么失去了资源”。
- 事件表只增，事件 id 单调，分页按 id 游标进行，过期回收不会造成历史缺口。
- 轮换事实（`CREDENTIAL_ROTATED`）对查询可见，但其 detail 只携带非密的
  `rotation_id` / fencing token / version，**不含任何凭证本体**。

## 9. 代码结构

```
lease_coordinator/
  model.py     # 状态枚举、Lease/LeaseEvent、RotationRecord/CredentialRotation、
               #            分页值对象、Clock/FakeClock
  errors.py    # 8 种可区分失败原因（含 rotation_conflict / invalid_rotation）
  ids.py       # uuid 与确定性序列 id 生成器
  storage.py   # SQLite schema（含 credential_rotations 表）、原子事务
               #            （acquire/renew/release/expire/rotate）、游标编解码
  service.py   # 领域服务：时钟、凭证生成、SHA-256 摘要、原因映射、
               #            幂等回放、后台回收线程、判定日志
  schema.py    # HTTP 请求/响应模型（含 RotateRequest / RotationResultView）
  api.py       # FastAPI 工厂 create_app(service)、错误码映射与 /rotate 端点
main.py        # uvicorn 入口：文件库 + 后台回收器
tests/         # 自动化测试：生命周期/失败原因、并发（含并发换发）、过期回收、
               #            重启/重试一致性、分页与历史、凭证换发专项、HTTP 层
               #            （全部使用 FakeClock，结果确定）
```

## 10. 验证方法

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
- 重启一致性：关闭服务后以新服务、新时钟打开同一文件，到期判定与历史不变。
- 分页与历史：7 条租约分 3 页遍历无重复无遗漏；终态行不消失；资源级事件链完整。
- **凭证换发专项**（`tests/test_credential_rotation.py`）：
  - 正常换发（自带 / 服务端生成新凭证两种来源）后新凭证可续约、可释放，
    旧凭证立刻对续约/释放失效；
  - 同一 `rotation_id` 的重复/并发提交：恰好 1 次首次生效、其余回放，
    多次重试 version 只增 1；不同 `rotation_id` 的并发换发恰好 1 次成功；
  - `rotation_id` 复用但参数不同 → `rotation_conflict` 且状态不变；
    新旧凭证相同 → `invalid_rotation`；
  - 换发与到期回收交错：到期后换发被拒并惰性置 EXPIRED、租约不复活不延长；
    换发不改变到期时间，回收仍按原 TTL 生效；成功换发后到期则新凭证也失效；
  - 重启一致性：换发落盘后新服务实例认新凭证、旧凭证失效、回放稳定、
    历史为 `ACQUIRED → CREDENTIAL_ROTATED → RENEWED`；
  - 两个服务实例共享同一 db 文件时换发立即互见，回放结论一致；
  - 分页在换发与回收穿插时稳定；历史只显示轮换事实，不含新旧凭证本体。
- 全部测试使用 `FakeClock`，连续多次执行结果稳定，不依赖真实时间与竞态 sleep。
