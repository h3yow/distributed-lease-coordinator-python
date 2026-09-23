# 分布式租约协调服务（本地可复现版）

一个**完全本地、不依赖任何外部服务或真实凭据**的分布式租约协调服务。多个并发客户端
可以对同一资源申请（acquire）、续约（renew）、释放（release）租约；服务保证同一资源
任一时刻最多一个有效持有者，并对未续约的租约做安全、幂等的过期回收（reap）。所有状态
迁移都落为只追加事件，支持按游标分页追溯完整历史。

实现基于 Python 3.14 + FastAPI，持久化只用本地文件（JSONL 追加日志 + `fcntl.flock`
文件锁），无数据库、无网络中间件、无账号密钥。

---

## 1. 目录结构

```
main.py                      # uvicorn 入口：装配 EventStore + Service + FastAPI
lease_coordinator/
  models.py                  # 领域模型：Lease / LeaseEvent / 状态枚举 / DTO / Clock
  errors.py                  # 可区分错误码（resource_held / invalid_token / ...）
  store.py                   # JSONL 事件日志：flock 互斥、OCC 提交、回放、游标分页
  service.py                 # 业务编排：申请/续约/释放/回收 + 冲突重试 + 判定日志
  api.py                     # HTTP 接口
tests/                       # 40 个确定性单测（注入 FakeClock，无需真实等待）
data/lease-events.log        # 默认事件日志路径（可用 LEASE_LOG_PATH 覆盖，已 gitignore）
```

---

## 2. 租约模型

每个**资源（resource_id）**至多对应一条当前租约投影；每次成功的状态迁移都追加一条
不可变事件。

### 2.1 租约字段

| 字段 | 含义 |
| --- | --- |
| `lease_id` | 一次授予的唯一标识（UUID），重新申请会得到新一代 lease_id |
| `holder` | 持有者标识（调用方自报，如 `worker-1`） |
| `token` | **持有凭证**，申请成功时一次性返回；续约/释放必须携带。对外列表/历史/查询均不回显 |
| `fence_token` | 单调递增的**代际栅栏号**：同一资源每重新授予一代 +1，可用于防止过期持有者的过期写 |
| `version` | 该资源的乐观并发版本号，每追加一条事件 +1 |
| `status` | `ACTIVE` / `RELEASED` / `EXPIRED` |
| `acquired_at_ms` / `expire_at_ms` | 授予时刻与**绝对到期时刻**（毫秒） |
| `ttl_ms` | 租约时长；续约不改变 ttl，只把到期点滑动为 `now + ttl` |
| `last_event_seq` | 产生当前投影的最后一条事件全局序号 |

### 2.2 状态机

```
                 acquire（资源空闲：不存在 / RELEASED / EXPIRED）
        ┌──────────────────────────────────────────────┐
        ▼                                                │
     ACTIVE ──release(凭证正确)──▶ RELEASED（终态）──────┘
        │
        │ now >= expire_at_ms，由以下任一途径迁移：
        │   · 主动回收 POST /v1/reap-expired
        │   · 针对该资源的 acquire / renew / release 惰性补记
        ▼
     EXPIRED（终态）────────────────────────────────────┘
```

* 终态不可逆；终态后资源视为**空闲**，可被申请为新一代租约（`fence_token + 1`、
  发放新 `token`、新 `lease_id`）。
* 事件类型只有四种：`ACQUIRED` / `RENEWED` / `RELEASED` / `EXPIRED`。
* **判定边界**：`now < expire_at_ms` 才算有效；即 `now == expire_at_ms` 那一刻已失效。

---

## 3. 过期与回收规则

1. 到期判定**只使用事件内保存的绝对时间戳** `expire_at_ms` 与当前时间比较，不依赖
   "授予后经过多久"这类相对计时。因此进程重启、墙上时钟基点变化都不影响判定
   （见 `tests/test_lease_recovery.py::test_new_process_against_same_log_keeps_state_and_wall_clock_independence`）。
2. 到期不会自动删除数据。回收只是**追加一条 `EXPIRED` 事件**，把投影状态置为 EXPIRED。
3. 两条回收路径，结果一致且都幂等：
   * **主动回收**：`POST /v1/reap-expired` 扫描所有"状态仍 ACTIVE 但 `now >= expire_at_ms`"
     的租约，可带 `resource_id` 过滤、`limit` 限量；
   * **惰性回收**：对某资源执行 acquire/renew/release 时若发现其已到期，先补记 EXPIRED
     再继续判定。
4. **不会被重复回收**：状态已是 EXPIRED/RELEASED 的租约不会再产生 EXPIRED 事件；
   回收提交若撞上并发续约/释放（OCC 版本冲突），本次扫描跳过该资源，不报错、不重复。
5. **回收不导致查询缺失**：事件日志与租约投影都只追加/原地改状态，从不删除。
   释放后、过期后，该资源依旧出现在列表与历史中，只是状态不同。

---

## 4. 并发保证范围

### 4.1 保证什么

* **单持有者**：同一资源在任一有效时刻最多一个 ACTIVE 持有者。并发申请时只有一个
  `ACQUIRED` 能落盘，其余收到 `409 resource_held`（见 20 线程压测
  `test_concurrent_acquires_grant_exactly_one_lease`）。
* **凭证校验**：续约/释放用 `hmac.compare_digest` 常量时间比较 token，凭证不符收到
  `403 invalid_token`。
* **乐观并发 + 有限重试**：每次写遵循"读最新投影 → 判定 → 带 `expected_version` 提交"；
  提交遇到版本冲突会指数退避重试（默认最多 8 次，可注入 `sleep_fn` 在测试中零等待），
  重试耗尽返回 `503 conflict_retry_limit`。
* **跨线程 / 跨进程互斥**：存储层每次操作持有 `fcntl.flock` 排他文件锁（进程间）
  + `threading.RLock`（线程间），并在锁内"追赶"其他进程追加的日志尾部。多个指向
  同一日志文件的进程不会双授，seq/version 全局连续。
* **重启一致**：重启即回放整条 JSONL 日志重建投影；全局 `seq` 与每资源 `version`
  在重启后继续写入时严格延续，不回退、不撞号。
* **崩溃安全**：事件用 `O_APPEND` 单行追加并 `fsync`，串行持锁下单条写入原子，
  崩溃不会产生半条记录；重启回放忽略任何尾部不完整行。

### 4.2 不在保证范围内（边界与取舍）

* **单节点本地存储**：协调状态保存在单个本地日志文件，靠本机文件锁互斥，**不提供**
  多机复制、故障切换或网络分区容错；多个进程需能访问同一文件系统。
* **不代持者自动续期**：调用方必须在到期前自行 renew；到期即失效，即使持有者仍然存活。
* **不做持有者对下游资源的写保护**：`fence_token` 仅随租约单调返回供下游自行校验，
  服务本身不拦截持有者对外部资源的访问。
* **时间源可信假设**：生产使用系统墙上时钟（毫秒）。多进程部署在同一机器上共享时钟；
  跨机器时钟漂移不在本实现的处理范围内。测试通过 `FakeClock` 彻底消除时间不确定性。
* **日志无压缩/保留策略**：日志只追加，长周期运行需运维侧自行归档（归档不得破坏
  从文件头的重放，除非配合快照）。

---

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/leases` | 申请租约 `{resource_id, holder, ttl_ms}`，成功 201 |
| POST | `/v1/leases/renew` | 续约 `{resource_id, token}` |
| POST | `/v1/leases/release` | 释放 `{resource_id, token}` |
| POST | `/v1/reap-expired?resource_id=&limit=` | 主动回收过期租约 |
| GET | `/v1/leases?cursor=&limit=` | 游标分页列出**全部**租约（含终态） |
| GET | `/v1/leases/{resource_id}` | 查询单资源当前投影（含 `effective_active`） |
| GET | `/v1/history?resource_id=&cursor=&limit=` | 游标分页查询状态迁移历史 |
| GET | `/healthz` | 健康检查与当前服务时间 |

* 租约分页游标按 `resource_id` 字典序；历史游标按全局 `seq` 升序；游标为不透明
  base64，翻页期间新写入不会改变既有游标边界。
* 每页上限 200 条。

### 可区分的失败响应

统一错误体：`{"error": <机器码>, "message": <人类可读原因>, "resource_id": ...}`

| error 码 | HTTP | 触发条件 |
| --- | --- | --- |
| `resource_held` | 409 | 申请时资源被其他有效持有者占用 |
| `invalid_token` | 403 | 续约/释放凭证与当前持有者不匹配 |
| `lease_not_found` | 404 | 资源从未有过租约 |
| `lease_expired` | 410 | 租约已过期（含到期点之后操作） |
| `lease_released` | 410 | 租约已被释放 |
| `conflict_retry_limit` | 503 | 乐观并发冲突重试次数耗尽 |
| —（请求校验） | 422 | ttl 非法、字段缺失等 |

> `lease_expired` 与 `lease_released` 同为 410，但 `error` 码不同，调用方可以精确区分。
> 终态判定先于凭证判定，因此对已释放/已过期租约用错误凭证操作时，得到的是终态错误码。

### 快速试用

```bash
# 启动（日志默认 ./data/lease-events.log）
uv run uvicorn main:app --port 8000

# 申请
curl -s -X POST localhost:8000/v1/leases -H 'Content-Type: application/json' \
  -d '{"resource_id":"orders/42","holder":"worker-1","ttl_ms":10000}'
# 续约 / 释放（带上返回的 token）
curl -s -X POST localhost:8000/v1/leases/renew  -H 'Content-Type: application/json' \
  -d '{"resource_id":"orders/42","token":"<TOKEN>"}'
# 主动回收过期租约
curl -s -X POST localhost:8000/v1/reap-expired
# 追溯历史
curl -s "localhost:8000/v1/history?resource_id=orders/42"
```

---

## 6. 验证方法

### 6.1 运行测试

```bash
uv run pytest                                  # 运行全部 40 个用例
uv run pytest --log-cli-level=INFO             # 实时打印每个用例的输入与判定依据
uv run pytest -k concurrency                   # 只看并发/冲突重试
uv run pytest tests/test_lease_recovery.py     # 只看过期回收与重启一致性
```

测试分布：

* `tests/test_lease_basic.py`：申请/续约/释放生命周期与全部失败码的可区分性；
* `tests/test_lease_concurrency.py`：20 线程并发申请不双授、续约/释放竞争、
  确定性注入 OCC 冲突验证重试与重试耗尽、双 store 句柄模拟跨进程可见性；
* `tests/test_lease_recovery.py`：到期边界、回收选择性与幂等、续约防误回收、
  旧凭证失效、显式 replay 与"新进程+无关时钟基点"重启一致性、重启后续号/版本延续；
* `tests/test_lease_queries.py`：租约/历史游标分页的完整性与稳定性、终态不消失、
  历史可解释性、非法游标、凭证不外泄；
* `tests/test_api.py`：HTTP 状态码/错误码映射、分页、回收幂等、参数校验。

### 6.2 可重复性保证

* 全部用例注入 `FakeClock`（线程安全、手动 `advance`），**没有任何 `sleep`/真实等待**，
  不依赖墙上时钟，因此重复执行、任意机器、任意顺序结果都稳定；
* 冲突重试的退避睡眠被注入为空操作（`sleep_fn=lambda _: None`）；
* 写冲突由 `EventStore(inject_conflict=...)` 钩子**确定性注入**，而非依赖调度概率；
* 每个用例使用 pytest 的 `tmp_path` 独立日志文件，互不污染。

### 6.3 判定可解释性（日志）

service 在每个判定点以 `lease_coordinator` logger 打印**输入与判定依据**，例如：

```
INFO lease_coordinator: acquire 拒绝 resource_id=r1 holder=h2；判定：租约 lease-xxx 仍有效
     status=ACTIVE holder=h1 expire_at_ms=1000000005000 > now_ms=1000000000000 version=1
INFO lease_coordinator: renew 拒绝 resource_id=r1 lease_id=lease-xxx holder=h1；判定：
     凭证不符 expire_at_ms=... now_ms=... version=1
INFO lease_coordinator: reap 回收 resource_id=a ...；判定：expire_at_ms=... <= now_ms=...
     且 status=ACTIVE，追加 EXPIRED（version=...）
```

每条事件自身也带 `detail`（如"后台扫描到期回收""申请时检测到过期，惰性回收"），
可通过历史接口回看每次状态变化的原因。

### 6.4 手工端到端复现

```bash
LEASE_LOG_PATH=/tmp/demo/events.log uv run uvicorn main:app --port 39177
# 申请 -> 重复申请得到 409 -> 错误凭证续约得到 403
# 等 ttl 过后 POST /v1/reap-expired -> 再续约得到 410 lease_expired
# 停服后用同一路径重启：GET /v1/leases/<rid> 状态仍在，GET /v1/history 完整可查
```

`/tmp/demo/events.log` 即人类可读的 JSONL 事实日志，每行一个事件，可直接核对
`seq / version / expire_at_ms / fence_token / detail`。
