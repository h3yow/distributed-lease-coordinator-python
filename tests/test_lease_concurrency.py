"""并发一致性测试：

- 多线程同时申请同一资源，恰好一个成功，其余得到可区分的 resource_held；
- store 的 inject_conflict 钩子确定性制造一次乐观冲突，验证 service 重试后成功；
- 两个 EventStore 实例模拟多进程：各自持锁、互见对方追加并按版本冲突失败。
"""

from __future__ import annotations

import logging
import threading

import pytest

from lease_coordinator.errors import ConflictRetryLimit, ResourceHeld
from lease_coordinator.service import LeaseCoordinatorService
from lease_coordinator.store import EventStore, WriteConflict


def test_concurrent_acquires_grant_exactly_one_lease(tmp_path, clock):
    """输入：20 个线程同时申请 r1；判定：版本串行化，只允许 1 个 ACQUIRED。"""
    store = EventStore(tmp_path / "events.log")
    store.start()
    results: list[str] = []  # "ok:<holder>" / "held:<holder>"
    barrier = threading.Barrier(20)

    def worker(idx: int) -> None:
        clock.advance(0)  # 触发时钟锁，无时间推进
        svc = LeaseCoordinatorService(store, clock, sleep_fn=lambda _s: None)
        barrier.wait()
        try:
            lease = svc.acquire("r1", f"h{idx}", 60_000)
            results.append(f"ok:{lease.holder}:{lease.lease_id}")
        except ResourceHeld as exc:
            assert exc.code == "resource_held"
            results.append(f"held:h{idx}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    oks = [r for r in results if r.startswith("ok:")]
    held = [r for r in results if r.startswith("held:")]
    assert len(oks) == 1, results
    assert len(held) == 19
    assert len({r.split(":")[1] for r in oks}) == 1

    events, _ = store.list_history("r1", None, 100)
    acquired = [e for e in events if e.event_type == "ACQUIRED"]
    assert len(acquired) == 1  # 无重复授予
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))  # seq 全局唯一递增
    store.close()


def test_concurrent_renew_and_release_only_one_terminal_wins(tmp_path, clock):
    """输入：10 线程续约 + 10 线程释放同一有效租约；判定：要么持续 ACTIVE 且版本连续，
    要么恰有一次 RELEASED；凭证错误的线程得到 invalid_token 不参与计数。"""
    from lease_coordinator.errors import InvalidToken, LeaseReleased

    store = EventStore(tmp_path / "events.log")
    store.start()
    svc = LeaseCoordinatorService(store, clock, sleep_fn=lambda _s: None)
    lease = svc.acquire("r1", "owner", 60_000)
    outcomes: dict[str, int] = {}
    barrier = threading.Barrier(20)

    def worker(kind: str) -> None:
        barrier.wait()
        try:
            if kind == "renew":
                svc.renew("r1", lease.token)
                outcomes["renewed"] = outcomes.get("renewed", 0) + 1
            else:
                svc.release("r1", lease.token)
                outcomes["released"] = outcomes.get("released", 0) + 1
        except (LeaseReleased, InvalidToken):
            outcomes["rejected_after_terminal"] = outcomes.get("rejected_after_terminal", 0) + 1

    threads = [threading.Thread(target=worker, args=("renew",)) for _ in range(10)]
    threads += [threading.Thread(target=worker, args=("release",)) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.get("released", 0) == 1
    renewed = outcomes.get("renewed", 0)
    rejected = outcomes.get("rejected_after_terminal", 0)
    assert renewed + rejected == 19

    final = store.get_lease("r1")
    assert final.status.value == "RELEASED"
    events, _ = store.list_history("r1", None, 100)
    assert sum(1 for e in events if e.event_type == "RELEASED") == 1  # 不重复释放
    versions = [e.version for e in events]
    assert versions == list(range(1, len(versions) + 1))  # 版本严格连续
    store.close()


def test_conflict_injection_is_retried_then_succeeds(tmp_path, clock, caplog):
    """输入：store 确定性注入一次写冲突；判定：service 重读重试，第 2 次提交成功。"""
    caplog.set_level(logging.INFO, logger="lease_coordinator")
    calls = {"n": 0}

    def inject() -> bool:
        calls["n"] += 1
        return calls["n"] == 1  # 仅第一次提交冲突

    store = EventStore(tmp_path / "events.log", inject_conflict=inject)
    store.start()
    svc = LeaseCoordinatorService(store, clock, max_retries=3, sleep_fn=lambda _s: None)

    lease = svc.acquire("r1", "h1", 5000)
    assert lease.version == 1
    assert calls["n"] == 2  # 冲突 1 次 + 成功 1 次

    events, _ = store.list_history("r1", None, 10)
    assert len(events) == 1 and events[0].event_type == "ACQUIRED"
    store.close()


def test_retry_exhaustion_raises_distinguishable_conflict_error(tmp_path, clock):
    """输入：每次提交都被注入冲突；判定：超过 max_retries 后报 conflict_retry_limit。"""
    store = EventStore(tmp_path / "events.log", inject_conflict=lambda: True)
    store.start()
    svc = LeaseCoordinatorService(store, clock, max_retries=2, sleep_fn=lambda _s: None)

    with pytest.raises(ConflictRetryLimit) as exc:
        svc.acquire("r1", "h1", 5000)
    assert exc.value.code == "conflict_retry_limit"

    assert store.get_lease("r1") is None  # 冲突提交没有任何副作用落盘
    store.close()


def test_two_store_handles_simulate_cross_process_visibility(tmp_path, clock):
    """输入：storeA/storeB 指向同一日志（模拟两个进程）；判定：B 先看到 A 的授予，
    再以旧版本提交时 OCC 失败，重读重试后基于最新状态给出 resource_held。"""
    store_a = EventStore(tmp_path / "events.log")
    store_b = EventStore(tmp_path / "events.log")
    store_a.start()
    store_b.start()
    svc_a = LeaseCoordinatorService(store_a, clock, sleep_fn=lambda _s: None)
    svc_b = LeaseCoordinatorService(store_b, clock, sleep_fn=lambda _s: None)

    lease_a = svc_a.acquire("r1", "proc-A", 60_000)
    assert lease_a.fence_token == 1

    # B 通过自己的句柄直接看到 A 的落盘事件（持锁 + catch-up）
    seen_by_b = store_b.get_lease("r1")
    assert seen_by_b is not None and seen_by_b.holder == "proc-A"

    # B 用过期版本号直接提交必被 OCC 拒绝
    with pytest.raises(WriteConflict):
        store_b.append_event(
            "ACQUIRED", "r1", "lease-from-B", holder="proc-B", token="tb",
            fence_token=99, ttl_ms=1, expire_at_ms=1, at_ms=clock.now_ms(),
            expected_version=None,
        )

    # 走业务层：B 重读后识别为被占用
    with pytest.raises(ResourceHeld) as exc:
        svc_b.acquire("r1", "proc-B", 60_000)
    assert exc.value.code == "resource_held"

    store_a.close()
    store_b.close()
