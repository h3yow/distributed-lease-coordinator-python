"""Tests for cursor pagination and append-only state transition history."""

from __future__ import annotations

import pytest

from lease_coordinator.errors import InvalidCursorError
from lease_coordinator.model import FakeClock, LeaseStatus
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import decode_cursor, encode_cursor


def test_lease_listing_never_drops_expired_or_released_rows(
    service: LeaseService, clock: FakeClock
) -> None:
    a = service.acquire("r-a", "A", ttl_ms=100).lease
    b_res = service.acquire("r-b", "B", ttl_ms=1000)
    b = b_res.lease
    c = service.acquire("r-c", "C", ttl_ms=1000).lease
    clock.advance(101)
    service.expire_due_leases()
    service.release(b.lease_id, b_res.credential)

    all_rows = service.list_leases(limit=100).items
    ids = {row.lease_id for row in all_rows}
    assert ids == {a.lease_id, b.lease_id, c.lease_id}
    statuses = {row.lease_id: row.status for row in all_rows}
    assert statuses[a.lease_id] is LeaseStatus.EXPIRED
    assert statuses[b.lease_id] is LeaseStatus.RELEASED
    assert statuses[c.lease_id] is LeaseStatus.HELD


def test_cursor_pagination_walks_every_row_exactly_once(service: LeaseService) -> None:
    for i in range(7):
        service.acquire(f"r-{i}", f"h-{i}", ttl_ms=10_000)

    seen: list[str] = []
    cursor: str | None = None
    for _ in range(10):
        page = service.list_leases(limit=3, cursor=cursor)
        seen.extend(item.lease_id for item in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert len(seen) == 7
    assert len(set(seen)) == 7  # no duplicates across pages


def test_cursor_is_opaque_and_validated(service: LeaseService) -> None:
    assert decode_cursor(encode_cursor(42)) == 42
    with pytest.raises(InvalidCursorError):
        service.list_leases(cursor="%%%not-base64%%%")
    with pytest.raises(InvalidCursorError):
        service.list_leases(cursor=encode_cursor(-1))


def test_full_transition_history_is_recorded_for_a_lease(
    service: LeaseService, clock: FakeClock
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=1000)
    lease = acquired.lease
    clock.advance(10)
    service.renew(lease.lease_id, acquired.credential, ttl_ms=1000)
    clock.advance(10)
    service.release(lease.lease_id, acquired.credential)

    events = service.list_events_for_lease(lease.lease_id, limit=100).items
    chain = [(e.event_type, e.from_status, e.to_status) for e in events]
    assert chain == [
        ("ACQUIRED", None, "HELD"),
        ("RENEWED", "HELD", "HELD"),
        ("RELEASED", "HELD", "RELEASED"),
    ]
    # Every event is timestamped and carries an explainable detail string.
    for event in events:
        assert event.occurred_at_ms > 0
        assert event.detail


def test_expiry_history_is_recorded_even_when_it_happens_lazily(
    service: LeaseService, clock: FakeClock
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1000).lease
    clock.advance(1001)
    service.acquire("r", "B", ttl_ms=1000)  # lazily expires the first one

    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "EXPIRED"]
    assert "lazily" in events[-1].detail

    resource_events = service.list_events_for_resource("r", limit=100).items
    assert [e.event_type for e in resource_events] == [
        "ACQUIRED", "EXPIRED", "ACQUIRED"
    ]


def test_event_pagination_for_resource(service: LeaseService, clock: FakeClock) -> None:
    acquired = service.acquire("r", "A", ttl_ms=1000)
    lease = acquired.lease
    for i in range(5):
        clock.advance(10)
        service.renew(lease.lease_id, acquired.credential, ttl_ms=1000)

    seen: list[int] = []
    cursor = None
    while True:
        page = service.list_events_for_resource("r", limit=2, cursor=cursor)
        seen.extend(e.event_id for e in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break
    assert len(seen) == 6  # 1 acquire + 5 renewals
    assert seen == sorted(seen)
    assert len(set(seen)) == 6
