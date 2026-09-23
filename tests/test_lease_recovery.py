"""过期回收与重启一致性测试。

覆盖：
- 到期边界（now == expire_at_ms 即失效）；
- reap 只回收真正过期的租约，且幂等、不重复回收；
- 回收不删除任何历史，查询不静默缺失；
- 进程重启（同路径新 store 实例 / 显式 replay）后投影、版本、
  基于绝对到期时间戳的判定全部保持一致；
- 重启后新服务用新时钟读到的 ACTIVE 租约仍按事件内 expire_at_ms 判定。
"""

from __future__ import annotations

import logging
import pathlib

import pytest

from lease_coordinator.errors import InvalidToken, LeaseExpired, ResourceHeld
from lease_coordinator.service import LeaseCoordinatorService
from lease_coordinator.store import EventStore
from tests.conftest import FakeClock


@pytest.fixture(autouse=True)
def _capture_logs(caplog):
    caplog.set_level(logging.INFO, logger="lease_coordinator")


def test_expiry_boundary_now_equals_expire_at_is_inactive(service, clock):
    """边界：t=0 授予 ttl=5000，推进到恰好 5000 时 is_active 为 False。"""
    lease = service.acquire("r1", "h1", 5000)
    clock.advance(4999)
    assert service.get_lease("r1").is_active(clock.now_ms())

    clock.advance(1)
    assert not service.get_lease("r1").is_active(clock.now_ms())
    with pytest.raises(LeaseExpired):
        service.renew("r1", lease.token)


def test_reap_collects_only_due_leases_and_is_idempotent(service, clock, caplog):
    """输入：3 个资源，ttl 各 1000/3000/5000，时钟推进到 4000；
    判定：前两个应回收，第三个不动；重复 reap 不再产生事件。"""
    service.acquire("a", "h", 1000)
    service.acquire("b", "h", 3000)
    service.acquire("c", "h", 5000)

    clock.advance(4000)
    first = service.reap_expired()
    assert sorted(e.resource_id for e in first) == ["a", "b"]
    assert all(e.event_type == "EXPIRED" and e.detail == "后台扫描到期回收" for e in first)

    # 未过期资源不受影响，凭证仍可续约
    c_lease = service.get_lease("c")
    assert c_lease.status.value == "ACTIVE"
    renewed = service.renew("c", c_lease.token)
    assert renewed.expire_at_ms == clock.now_ms() + 5000

    second = service.reap_expired()
    assert second == []  # 幂等：不会重复回收

    statuses = {rid: service.get_lease(rid).status.value for rid in "abc"}
    assert statuses == {"a": "EXPIRED", "b": "EXPIRED", "c": "ACTIVE"}

    # 每个过期资源在历史中恰好有一条 EXPIRED
    for rid in ("a", "b"):
        types = [e.event_type for e in service.list_history(rid).items]
        assert types == ["ACQUIRED", "EXPIRED"]
    assert "reap 完成" in caplog.text and "reaped=2" in caplog.text


def test_reap_respects_resource_filter_and_limit(service, clock):
    for rid, ttl in [("r1", 1000), ("r2", 1000), ("r3", 1000)]:
        service.acquire(rid, "h", ttl)
    clock.advance(2000)

    only_r2 = service.reap_expired(resource_id="r2")
    assert [e.resource_id for e in only_r2] == ["r2"]

    limited = service.reap_expired(limit=1)
    assert len(limited) == 1 and limited[0].resource_id == "r1"
    rest = service.reap_expired()
    assert [e.resource_id for e in rest] == ["r3"]


def test_renew_just_before_expiry_prevents_reap(service, clock):
    """输入：到期前 1ms 续约；判定：滑动到期点后 reap 不得回收该租约。"""
    lease = service.acquire("r1", "h1", 5000)
    clock.advance(4999)
    renewed = service.renew("r1", lease.token)
    assert renewed.expire_at_ms == clock.now_ms() + 5000

    clock.advance(4000)  # 距原始到期已过，但续约后仍有效
    assert service.reap_expired() == []
    assert service.get_lease("r1").status.value == "ACTIVE"


def test_expired_resource_can_be_reacquired_but_old_token_cannot_renew(service, clock):
    """输入：过期回收后，老持有者拿旧凭证续约/释放都必须被拒绝；新持有者正常工作。"""
    old = service.acquire("r1", "h-old", 1000)
    clock.advance(2000)
    service.reap_expired()

    with pytest.raises(LeaseExpired):
        service.renew("r1", old.token)
    with pytest.raises(LeaseExpired):
        service.release("r1", old.token)

    new = service.acquire("r1", "h-new", 5000)
    assert new.fence_token == old.fence_token + 1
    # 旧凭证对新一代租约无效
    with pytest.raises(InvalidToken):
        service.renew("r1", old.token)
    # 新凭证有效（投影对象会被原地更新，断言前重新读取）
    renewed = service.renew("r1", new.token)
    assert renewed.version == 4
    assert renewed.lease_id == new.lease_id
    assert renewed.expire_at_ms == clock.now_ms() + 5000


def test_replay_rebuilds_projection_and_absolute_time_judgement(service, store, clock):
    """显式 replay（模拟进程崩溃后重启）：投影、状态、基于绝对时间戳的判定一致。"""
    service.acquire("r1", "h1", 10_000)
    clock.advance(2000)
    lease = service.get_lease("r1")
    service.renew("r1", lease.token)  # expire -> t0+12000
    service.acquire("r2", "h2", 1000)  # expire -> t0+3000
    clock.advance(6000)  # now=t0+8000：r2 已过期，r1 仍有效
    service.reap_expired()

    store.replay()

    r1 = store.get_lease("r1")
    r2 = store.get_lease("r2")
    assert r1.status.value == "ACTIVE" and r1.expire_at_ms == clock.now_ms() + 4000
    assert r1.is_active(clock.now_ms())
    assert r2.status.value == "EXPIRED"
    assert not r2.is_active(clock.now_ms())

    all_events, _ = store.list_history(None, None, 100)
    assert [e.event_type for e in all_events] == [
        "ACQUIRED", "RENEWED", "ACQUIRED", "EXPIRED",
    ]


def test_new_process_against_same_log_keeps_state_and_wall_clock_independence(tmp_path):
    """模拟真正的进程重启：新 EventStore + 新 FakeClock（启动时刻完全不同），
    所有存活判定仍以事件内绝对 expire_at_ms 为准。"""
    log_path = tmp_path / "events.log"
    clock1 = FakeClock(start_ms=1_000_000)  # 老"进程"的时间
    store1 = EventStore(log_path)
    store1.start()
    svc1 = LeaseCoordinatorService(store1, clock1, sleep_fn=lambda _s: None)
    lease = svc1.acquire("res-X", "worker-1", ttl_ms := 10_000)
    store1.close()

    # 新进程：时钟从另一个完全无关的基点开始，但仍在老租约有效期内
    clock2 = FakeClock(start_ms=1_005_000)
    store2 = EventStore(log_path)
    store2.start()
    svc2 = LeaseCoordinatorService(store2, clock2, sleep_fn=lambda _s: None)

    loaded = svc2.get_lease("res-X")
    assert loaded is not None
    assert loaded.lease_id == lease.lease_id
    assert loaded.holder == "worker-1"
    assert loaded.status.value == "ACTIVE"
    assert loaded.is_active(clock2.now_ms())
    assert loaded.expire_at_ms == 1_010_000

    # 有效期内别的申请者仍被拒
    with pytest.raises(ResourceHeld):
        svc2.acquire("res-X", "worker-2", 1000)
    # 原持有者凭证在新进程中续约成功，到期点基于新提交时刻
    renewed = svc2.renew("res-X", lease.token)
    assert renewed.expire_at_ms == 1_015_000

    # 再"重启"到到期之后：判定为过期且可安全回收
    clock3 = FakeClock(start_ms=2_000_000)
    store3 = EventStore(log_path)
    store3.start()
    svc3 = LeaseCoordinatorService(store3, clock3, sleep_fn=lambda _s: None)
    assert svc3.get_lease("res-X").status.value == "ACTIVE"  # 尚未补记 EXPIRED
    reaped = svc3.reap_expired()
    assert len(reaped) == 1 and reaped[0].event_type == "EXPIRED"
    assert svc3.get_lease("res-X").status.value == "EXPIRED"
    # 旧时间戳证据仍在，可解释为何回收
    assert reaped[0].expire_at_ms == 1_015_000

    store2.close()
    store3.close()


def test_replay_then_new_writes_continue_seq_and_versions(tmp_path):
    """重启后继续写入：全局 seq 与每资源版本严格延续，不回退、不撞号。"""
    log_path = tmp_path / "events.log"
    clock = FakeClock()
    store1 = EventStore(log_path)
    store1.start()
    svc1 = LeaseCoordinatorService(store1, clock, sleep_fn=lambda _s: None)
    l = svc1.acquire("r1", "h", 1000)
    svc1.renew("r1", l.token)
    store1.close()

    store2 = EventStore(log_path)
    store2.start()
    clock.advance(2000)
    svc2 = LeaseCoordinatorService(store2, clock, sleep_fn=lambda _s: None)
    svc2.reap_expired()
    svc2.acquire("r1", "h2", 1000)

    events, _ = store2.list_history(None, None, 100)
    assert [e.seq for e in events] == [1, 2, 3, 4]
    assert [e.version for e in events] == [1, 2, 3, 4]
    assert [e.event_type for e in events] == ["ACQUIRED", "RENEWED", "EXPIRED", "ACQUIRED"]
    store2.close()
