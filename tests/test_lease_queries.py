"""游标分页与状态迁移历史的可追溯查询测试。

关键不变量：
- 释放/过期回收只追加事件、不删除投影，列表与历史永不静默缺失；
- 租约分页按 resource_id 字典序稳定翻页；
- 历史分页按全局 seq 升序，可按资源过滤；
- 新写入落在 seq 末尾，不影响已有游标之前的结果。
"""

from __future__ import annotations

import pytest

from lease_coordinator.store import decode_cursor, encode_cursor


def test_list_leases_cursor_pagination_is_stable_and_complete(service):
    for rid in [f"res-{i:02d}" for i in range(5)]:
        service.acquire(rid, "h", 10_000)

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        page = service.list_leases(cursor=cursor, limit=2)
        pages += 1
        seen.extend(l.resource_id for l in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert pages == 3  # 2 + 2 + 1
    assert seen == [f"res-{i:02d}" for i in range(5)]  # 字典序、无重复无遗漏


def test_list_leases_includes_terminal_states_forever(service, clock):
    """释放与过期回收后，租约投影仍在列表中（只是状态变化），不静默消失。"""
    service.acquire("alive", "h", 10_000)
    dead = service.acquire("dead", "h", 1000)
    service.release("alive", service.get_lease("alive").token)
    clock.advance(2000)
    service.reap_expired()

    all_items = {l.resource_id: l.status.value for l in service.list_leases(limit=100).items}
    assert all_items == {"alive": "RELEASED", "dead": "EXPIRED"}
    assert dead.lease_id  # 旧 lease_id 仍可对照历史


def test_list_history_global_seq_order_with_cursor(service, clock):
    l1 = service.acquire("r1", "h1", 5000)
    service.acquire("r2", "h2", 9000)
    clock.advance(100)
    service.renew("r1", l1.token)

    page1 = service.list_history(cursor=None, limit=2)
    assert [e.seq for e in page1.items] == [1, 2]
    assert page1.next_cursor is not None

    page2 = service.list_history(cursor=page1.next_cursor, limit=2)
    assert [e.seq for e in page2.items] == [3]
    assert page2.next_cursor is None

    # 游标内容可解释
    assert decode_cursor(page1.next_cursor) == {"s": 2}


def test_list_history_filtered_by_resource_explains_full_lifecycle(service, clock):
    l1 = service.acquire("r1", "h1", 1000)
    service.acquire("other", "h", 1000)
    clock.advance(500)
    service.renew("r1", l1.token)
    clock.advance(2000)
    service.reap_expired()
    service.acquire("r1", "h-next", 1000)  # 第二代

    page = service.list_history(resource_id="r1", limit=50)
    events = page.items
    assert [e.event_type for e in events] == [
        "ACQUIRED", "RENEWED", "EXPIRED", "ACQUIRED",
    ]
    assert all(e.resource_id == "r1" for e in events)
    # 相邻版本严格 +1；每代 fence 可解释
    assert [e.version for e in events] == [1, 2, 3, 4]
    assert [e.fence_token for e in events] == [1, 1, 1, 2]
    # 过期事件记录的是原始绝对到期点，回收时刻在 at_ms，原因写入 detail
    expired = events[2]
    assert expired.expire_at_ms == events[1].expire_at_ms
    assert expired.at_ms > expired.expire_at_ms
    assert "回收" in expired.detail
    # 全局历史中过滤结果的 seq 子集顺序一致
    global_seqs = [e.seq for e in service.list_history(limit=100).items]
    assert [e.seq for e in events] == [s for s in global_seqs if s in {e.seq for e in events}]


def test_history_survives_reap_and_writes_after_cursor_remain_consistent(service, clock):
    """先取游标；之后发生的回收与新申请不应改变旧游标之前/之后的边界。"""
    service.acquire("a", "h", 1000)
    service.acquire("b", "h", 1000)
    page1 = service.list_history(limit=1)
    assert [e.event_type for e in page1.items] == ["ACQUIRED"]

    clock.advance(2000)
    service.reap_expired()
    service.acquire("c", "h", 1000)

    rest = service.list_history(cursor=page1.next_cursor, limit=100)
    seqs = [e.seq for e in rest.items]
    assert seqs == [2, 3, 4, 5]
    assert [e.event_type for e in rest.items] == [
        "ACQUIRED", "EXPIRED", "EXPIRED", "ACQUIRED",
    ]
    assert [e.resource_id for e in rest.items] == ["b", "a", "b", "c"]


def test_invalid_cursor_is_rejected_at_store_boundary(service):
    from lease_coordinator.store import decode_cursor
    with pytest.raises(ValueError):
        decode_cursor("not-base64!!")
    with pytest.raises(ValueError):
        decode_cursor(encode_cursor([1, 2, 3]))  # 类型不对


def test_event_payloads_never_leak_token(service):
    """凭证只在申请/续约/释放的响应中回显给持有者；事件对外序列化不含 token。"""
    l = service.acquire("secret-res", "h", 1000)
    service.renew("secret-res", l.token)
    events = service.list_history(limit=10).items
    assert events  # 有事件可查
    assert all("token" not in e.to_dict() for e in events)
