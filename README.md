# Distributed Lease Coordinator (local, reproducible)

一个可在本地完整复现、**不依赖外部服务或真实凭据**的分布式租约协调服务。
多个并发客户端可以对同一资源进行租约的申请（acquire）、续约（renew）、
释放（release）与过期回收（expire/reap），并获得一致且可解释的结果。

- 语言 / 运行时：Python 3.14，仅依赖 `fastapi[standard]`（测试用 `pytest`）
- 存储：本地 SQLite（WAL 模式，文件库或内存库），无需任何外部数据库
- 时间：可注入的时钟（`FakeClock`），测试结果与执行速度无关、可重复
- 凭据：服务端自动生成 `secrets.token_hex(16)` 凭证，也允许调用方显式传入

## 1. 快速开始

```bash
uv sync
uv run uvicorn main:app                       # 启动 HTTP 服务（默认 ./lease.db）
LEASE_DB=/tmp/lease.db uv run uvicorn main:app --port 8000
uv run pytest -s                              # 运行全部 31 个测试并打印判定日志
```

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/leases` | 申请租约，body: `resource_id, holder, ttl_ms, credential?` |
| GET | `/leases?limit=&cursor=` | 游标分页列出全部租约（含历史终态） |
| GET | `/leases/{lease_id}` | 查询单个租约当前快照 |
| POST | `/leases/{lease_id}/renew` | 续约，body: `credential, ttl_ms` |
| POST | `/leases/{lease_id}/release` | 释放，body: `credential` |
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

判定顺序（在同一数据库事务内）：存在性 → 凭证 → 已释放 → 已到期 → 执行。
因此“凭证错误”和“已到期”即使同时成立也只会得到一个确定的、可复现的原因。

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

## 6. 可追溯查询

- `GET /leases`：按插入顺序（SQLite rowid）游标分页，终态租约同样保留。
- `GET /leases/{id}/events`：单个租约 `ACQUIRED / RENEWED / RELEASED / EXPIRED`
  的完整迁移链，含 `from_status`、`to_status`、时间戳与文字化判定依据（detail）。
- `GET /resources/{id}/events`：同一资源跨多代租约的完整历史
  （例如 `ACQUIRED → EXPIRED → ACQUIRED`），可解释“谁在什么时候因为什么失去了资源”。
- 事件表只增，事件 id 单调，分页按 id 游标进行，过期回收不会造成历史缺口。

## 7. 代码结构

```
lease_coordinator/
  model.py     # 状态枚举、Lease/LeaseEvent/分页值对象、Clock/FakeClock
  errors.py    # 6 种可区分失败原因
  ids.py       # uuid 与确定性序列 id 生成器
  storage.py   # SQLite schema、原子事务（acquire/renew/release/expire）、游标编解码
  service.py   # 领域服务：时钟、凭证生成、原因映射、后台回收线程、判定日志
  schema.py    # HTTP 请求/响应模型
  api.py       # FastAPI 工厂 create_app(service) 与错误码映射
main.py        # uvicorn 入口：文件库 + 后台回收器
tests/         # 31 个测试：生命周期/失败原因、并发、过期回收、重启一致性、
               #            分页与历史、HTTP 层（全部使用 FakeClock，结果确定）
```

## 8. 验证方法

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
