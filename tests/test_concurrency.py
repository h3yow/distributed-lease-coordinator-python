"""Tests for concurrent acquisition, expiry reaping and restart consistency."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from lease_coordinator.errors import LeaseConflictError
from lease_coordinator.ids import UuidLeaseIdGenerator
from lease_coordinator.model import FakeClock, LeaseStatus
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage


def test_only_one_winner_under_concurrent_acquire(tmp_path: Path) -> None:
    """Many threads racing on one resource: exactly one grant, N-1 conflicts."""
    db = tmp_path / "race.db"
    svc = LeaseService(db_path=db, id_generator=UuidLeaseIdGenerator())
    winners: list[str] = []
    rejected: list[str] = []
    barrier = threading.Barrier(20)

    def attempt(idx: int) -> None:
        barrier.wait()
        try:
            result = svc.acquire("hot-resource", f"client-{idx}", ttl_ms=60_000)
            winners.append(result.lease.lease_id)
        except LeaseConflictError:
            rejected.append(f"client-{idx}")

    with ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(attempt, range(20)))

    assert len(winners) == 1, winners
    assert len(rejected) == 19
    rows = svc.list_leases(limit=100).items
    held = [r for r in rows if r.resource_id == "hot-resource" and r.status is LeaseStatus.HELD]
    assert len(held) == 1
    svc.close()


def test_in_memory_shared_storage_also_serializes_racers(service: LeaseService) -> None:
    winners: list[str] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(10)

    def attempt(idx: int) -> None:
        barrier.wait()
        try:
            winners.append(service.acquire("r", f"c{idx}", ttl_ms=1000).lease.lease_id)
        except Exception as exc:  # noqa: BLE001 - want to surface any non-conflict
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(attempt, range(10)))

    assert len(winners) == 1
    assert all(isinstance(e, LeaseConflictError) for e in errors)
    assert len(errors) == 9


def test_expired_lease_is_reaped_at_most_once(service: LeaseService, clock: FakeClock) -> None:
    lease = service.acquire("r", "c", ttl_ms=1_000).lease
    clock.advance(1_001)

    first_pass = service.expire_due_leases()
    second_pass = service.expire_due_leases()

    assert [l.lease_id for l in first_pass] == [lease.lease_id]
    assert second_pass == []
    stored = service.get_lease(lease.lease_id)
    assert stored.status is LeaseStatus.EXPIRED
    assert stored.expire_reason == "ttl_elapsed_reaper"
    assert stored.version == 1

    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "EXPIRED"]


def test_expiry_then_acquire_grants_new_holder_with_new_token(
    service: LeaseService, clock: FakeClock
) -> None:
    first = service.acquire("r", "A", ttl_ms=1_000).lease
    clock.advance(1_001)
    service.expire_due_leases()

    second = service.acquire("r", "B", ttl_ms=1_000).lease

    assert second.fencing_token == 2
    assert service.get_lease(first.lease_id).status is LeaseStatus.EXPIRED
    assert service.get_lease(second.lease_id).status is LeaseStatus.HELD


def test_lazy_expiry_during_acquire_frees_slot_without_explicit_reap(
    service: LeaseService, clock: FakeClock
) -> None:
    first = service.acquire("r", "A", ttl_ms=1_000).lease
    clock.advance(1_001)

    second = service.acquire("r", "B", ttl_ms=1_000).lease  # must lazily expire first

    assert second.fencing_token == 2
    assert service.get_lease(first.lease_id).status is LeaseStatus.EXPIRED
    assert service.get_lease(first.lease_id).expire_reason == "ttl_elapsed_before_acquire"


def test_background_reaper_eventually_expires_due_leases(
    service: LeaseService, clock: FakeClock
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000).lease
    service.start_reaper(interval_ms=20)
    clock.advance(1_001)

    deadline_loops = 0
    while service.get_lease(lease.lease_id).status is LeaseStatus.HELD and deadline_loops < 100:
        threading.Event().wait(0.02)
        deadline_loops += 1

    assert service.get_lease(lease.lease_id).status is LeaseStatus.EXPIRED
    service.stop_reaper()


def test_state_is_consistent_after_process_restart(tmp_path: Path) -> None:
    """A newly created service over the same file must see prior decisions."""
    db = tmp_path / "persist.db"
    clock = FakeClock(current_ms=5_000)
    svc1 = LeaseService(db_path=db, clock=clock, id_generator=_PrefixIds("lease"))
    acquired = svc1.acquire("r", "A", ttl_ms=1_000)
    lease = acquired.lease
    svc1.renew(lease.lease_id, acquired.credential, ttl_ms=2_000)  # expiry -> 7000
    svc1.close()

    # "Restart": a new service/clock continues from a later point in time.
    clock2 = FakeClock(current_ms=7_001)
    svc2 = LeaseService(db_path=db, clock=clock2, id_generator=_PrefixIds("lease"))
    reaped = svc2.expire_due_leases()
    assert [l.lease_id for l in reaped] == [lease.lease_id]

    # The expired lease must not be renewable after restart.
    from lease_coordinator.errors import LeaseExpiredError

    with pytest.raises(LeaseExpiredError):
        svc2.renew(lease.lease_id, acquired.credential, ttl_ms=10_000)

    # History survived the restart and is still queryable.
    events = svc2.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "RENEWED", "EXPIRED"]
    svc2.close()


def test_two_services_on_same_file_do_not_double_grant(tmp_path: Path) -> None:
    db = tmp_path / "multi.db"
    clock = FakeClock(current_ms=0)
    svc_a = LeaseService(db_path=db, clock=clock, id_generator=UuidLeaseIdGenerator())
    svc_b = LeaseService(db_path=db, clock=clock, id_generator=UuidLeaseIdGenerator())

    a_wins = svc_a.acquire("shared", "A", ttl_ms=1000).lease
    with pytest.raises(LeaseConflictError):
        svc_b.acquire("shared", "B", ttl_ms=1000)

    # Expiry performed by service A is visible to service B; B may then win.
    clock.advance(1001)
    svc_a.expire_due_leases()
    b_lease = svc_b.acquire("shared", "B", ttl_ms=1000).lease
    assert b_lease.fencing_token == a_wins.fencing_token + 1
    svc_a.close()
    svc_b.close()


class _PrefixIds:
    """Deterministic id generator shared across service restarts."""

    def __init__(self, prefix: str) -> None:
        self._n = 0
        self._prefix = prefix

    def new_lease_id(self) -> str:
        self._n += 1
        return f"{self._prefix}-{self._n}"


def test_direct_storage_restart_with_file(tmp_path: Path) -> None:
    db = tmp_path / "s.db"
    st = LeaseStorage(db)
    tok, conflict = st.atomic_acquire(
        "r", "l1", "h", st.codec.fingerprint("c"), 0, 100
    )
    assert tok == 1 and conflict is None
    st.close()

    st2 = LeaseStorage(db)
    lease = st2.get_lease("l1")
    assert lease is not None and lease.fencing_token == 1
    tok, conflict = st2.atomic_acquire(
        "r", "l2", "h", st2.codec.fingerprint("c"), 50, 200
    )
    assert conflict is not None and tok == 0
    st2.close()
