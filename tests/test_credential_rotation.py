"""Tests for holder credential rotation (credential re-issue).

Coverage:
* normal rotation with caller-supplied and server-generated credentials,
* idempotent retry via ``rotation_id`` and reuse conflicts,
* old credential invalidation + continuation with the new credential,
* concurrent rotations (exactly one fresh swap),
* rotation interleaved with expiry/reaping (never resurrected/extended),
* restart / retry consistency,
* credentials never leaking through queries or history.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from lease_coordinator.errors import (
    InvalidCredentialError,
    InvalidRotationError,
    LeaseExpiredError,
    LeaseNotFoundError,
    LeaseReleasedError,
    RotationConflictError,
)
from lease_coordinator.model import FakeClock, LeaseStatus
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage


# --------------------------------------------------------------------------
# Normal rotation: caller-supplied new credential
# --------------------------------------------------------------------------

def test_rotation_succeeds_and_swaps_credential(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")

    rotation = service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )

    assert rotation.rotation_id == "rot-1"
    assert rotation.new_credential == "new-secret"
    assert rotation.replayed is False
    # Rotation does not extend the lease or change holder/token.
    assert rotation.lease.status is LeaseStatus.HELD
    assert rotation.lease.expires_at_ms == lease.expires_at_ms
    assert rotation.lease.holder == "A"
    assert rotation.lease.fencing_token == lease.fencing_token
    assert rotation.lease.version == lease.version + 1
    stored = service.get_lease(lease.lease_id)
    assert stored.credential == "new-secret"
    assert stored.version == 1


def test_old_credential_is_immediately_rejected_for_renew_and_release(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )

    with pytest.raises(InvalidCredentialError):
        service.renew(lease.lease_id, "old-secret", ttl_ms=10_000)
    with pytest.raises(InvalidCredentialError):
        service.release(lease.lease_id, "old-secret")

    # The rejected renewal must not have changed expiry.
    assert service.get_lease(lease.lease_id).expires_at_ms == lease.expires_at_ms


def test_new_credential_continues_renew_then_release_chain(service: LeaseService, clock: FakeClock) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )

    clock.advance(1_000)
    renewed = service.renew(lease.lease_id, "new-secret", ttl_ms=10_000)
    assert renewed.credential == "new-secret"
    assert renewed.expires_at_ms == clock.now_ms() + 10_000

    released = service.release(lease.lease_id, "new-secret")
    assert released.status is LeaseStatus.RELEASED


# --------------------------------------------------------------------------
# Server-generated new credential
# --------------------------------------------------------------------------

def test_server_generated_new_credential_is_returned_and_usable(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000)  # credential server-made
    original = lease.credential
    assert len(original) == 32

    rotation = service.rotate_credential(lease.lease_id, original, "rot-1")

    assert rotation.new_credential != original
    assert len(rotation.new_credential) == 32
    assert service.get_lease(lease.lease_id).credential == rotation.new_credential

    renewed = service.renew(lease.lease_id, rotation.new_credential, ttl_ms=5_000)
    assert renewed.status is LeaseStatus.HELD
    released = service.release(lease.lease_id, rotation.new_credential)
    assert released.status is LeaseStatus.RELEASED


# --------------------------------------------------------------------------
# Idempotent retry / rotation_id handling
# --------------------------------------------------------------------------

def test_identical_retry_is_replayed_with_same_credential(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    first = service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )

    retry = service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )

    assert retry.replayed is True
    assert retry.new_credential == first.new_credential
    assert retry.rotated_at_ms == first.rotated_at_ms
    # A replay must not perform another swap or bump the version.
    assert service.get_lease(lease.lease_id).version == 1


def test_server_generated_rotation_retry_returns_same_generated_credential(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000)
    first = service.rotate_credential(lease.lease_id, lease.credential, "rot-1")

    retry = service.rotate_credential(lease.lease_id, lease.credential, "rot-1")

    assert retry.replayed is True
    assert retry.new_credential == first.new_credential
    assert service.get_lease(lease.lease_id).version == 1


def test_reused_rotation_id_with_different_params_is_conflict(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )
    # A subsequent, genuinely new rotation is fine...
    service.rotate_credential(
        lease.lease_id, "new-secret", "rot-2", new_credential="third-secret"
    )
    # ...but reusing rot-1 with a different presented credential is ambiguous.
    with pytest.raises(RotationConflictError) as exc_info:
        service.rotate_credential(
            lease.lease_id, "third-secret", "rot-1", new_credential="whatever"
        )
    assert exc_info.value.code == "rotation_conflict"
    # The conflicting request must not have touched the current credential.
    assert service.get_lease(lease.lease_id).credential == "third-secret"


def test_reused_rotation_id_with_different_new_credential_is_conflict(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )
    with pytest.raises(RotationConflictError):
        service.rotate_credential(
            lease.lease_id, "old-secret", "rot-1", new_credential="tampered"
        )
    assert service.get_lease(lease.lease_id).credential == "new-secret"


# --------------------------------------------------------------------------
# Failure reasons / decision order
# --------------------------------------------------------------------------

def test_rotation_requires_the_current_credential(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    with pytest.raises(InvalidCredentialError) as exc_info:
        service.rotate_credential(
            lease.lease_id, "forged", "rot-1", new_credential="new-secret"
        )
    assert exc_info.value.code == "invalid_credential"
    assert service.get_lease(lease.lease_id).credential == "old-secret"


def test_rotation_new_credential_must_differ(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="same")
    with pytest.raises(InvalidRotationError) as exc_info:
        service.rotate_credential(
            lease.lease_id, "same", "rot-1", new_credential="same"
        )
    assert exc_info.value.code == "invalid_rotation"
    assert service.get_lease(lease.lease_id).version == 0


def test_rotation_on_released_lease_is_rejected(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="c")
    service.release(lease.lease_id, "c")
    with pytest.raises(LeaseReleasedError):
        service.rotate_credential(lease.lease_id, "c", "rot-1", new_credential="c2")


def test_rotation_on_unknown_lease_is_not_found(service: LeaseService) -> None:
    with pytest.raises(LeaseNotFoundError):
        service.rotate_credential("lease-404", "c", "rot-1", new_credential="c2")


# --------------------------------------------------------------------------
# Rotation interleaved with expiry / reaping
# --------------------------------------------------------------------------

def test_rotation_after_expiry_is_rejected_and_lease_stays_expired(
    service: LeaseService, clock: FakeClock
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000, credential="old-secret")
    clock.advance(1_001)
    service.expire_due_leases()

    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
        )

    stored = service.get_lease(lease.lease_id)
    assert stored.status is LeaseStatus.EXPIRED
    assert stored.credential == "old-secret"
    assert stored.expires_at_ms == lease.expires_at_ms  # never extended


def test_rotation_lazily_expires_overdue_lease_without_reviving_it(
    service: LeaseService, clock: FakeClock
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000, credential="old-secret")
    clock.advance(1_001)

    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
        )

    stored = service.get_lease(lease.lease_id)
    assert stored.status is LeaseStatus.EXPIRED
    assert stored.expire_reason == "ttl_elapsed_before_rotate"
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "EXPIRED"]
    # No rotation record must exist for a rejected application.
    assert not _rotation_rows_exist(service, "rot-1")


def test_reaper_and_rotation_race_resolves_to_single_outcome(
    service: LeaseService, clock: FakeClock
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000, credential="old-secret")
    clock.advance(1_001)
    # Reaper wins the race: the lease is terminal before rotation is tried.
    service.expire_due_leases()
    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
        )
    # Expiring again changes nothing; the rejected rotation is still rejected.
    assert service.expire_due_leases() == []


def test_rotation_does_not_change_expiry_so_reaper_still_bounds_it(
    service: LeaseService, clock: FakeClock
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000, credential="old-secret")
    clock.advance(200)
    service.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )
    # expiry was NOT pushed forward by the rotation
    clock.advance(801)
    reaped = service.expire_due_leases()
    assert [l.lease_id for l in reaped] == [lease.lease_id]
    stored = service.get_lease(lease.lease_id)
    assert stored.status is LeaseStatus.EXPIRED
    assert stored.credential == "new-secret"  # last swap preserved, lease dead


def _rotation_rows_exist(service: LeaseService, rotation_id: str) -> bool:
    conn = service._storage._get_conn()  # type: ignore[attr-defined]
    row = conn.execute(
        "SELECT 1 FROM credential_rotations WHERE rotation_id = ?",
        (rotation_id,),
    ).fetchone()
    return row is not None


# --------------------------------------------------------------------------
# Concurrent rotations: exactly one fresh swap per lease
# --------------------------------------------------------------------------

def test_concurrent_rotations_have_exactly_one_fresh_swap(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=60_000, credential="cred-0")
    barrier = threading.Barrier(12)
    fresh: list[str] = []
    rejected: list[Exception] = []

    def attempt(idx: int) -> None:
        barrier.wait()
        try:
            rotation = service.rotate_credential(
                lease.lease_id, "cred-0", f"rot-{idx}",
                new_credential=f"cred-{idx + 1}",
            )
            fresh.append(rotation.new_credential)
        except Exception as exc:  # noqa: BLE001 - want to see non-credential errors
            rejected.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(attempt, range(12)))

    assert len(fresh) == 1, fresh
    assert len(rejected) == 11
    assert all(isinstance(e, InvalidCredentialError) for e in rejected), rejected
    final = service.get_lease(lease.lease_id)
    assert final.credential == fresh[0]
    assert final.version == 1  # only one swap committed


def test_concurrent_submissions_of_same_rotation_id_all_see_one_outcome(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=60_000, credential="old-secret")
    barrier = threading.Barrier(10)
    results: list[tuple[str, bool]] = []
    errors: list[Exception] = []

    def attempt() -> None:
        barrier.wait()
        try:
            rotation = service.rotate_credential(
                lease.lease_id, "old-secret", "rot-shared",
                new_credential="new-secret",
            )
            results.append((rotation.new_credential, rotation.replayed))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=10) as pool:
        for _ in range(10):
            pool.submit(attempt)

    assert errors == []
    assert len(results) == 10
    assert {cred for cred, _ in results} == {"new-secret"}
    assert sum(1 for _, replayed in results if not replayed) == 1
    assert sum(1 for _, replayed in results if replayed) == 9
    assert service.get_lease(lease.lease_id).version == 1


# --------------------------------------------------------------------------
# Restart / retry consistency
# --------------------------------------------------------------------------

def test_rotation_state_survives_restart_and_replay_is_stable(tmp_path: Path) -> None:
    db = tmp_path / "rot-restart.db"
    clock = FakeClock(current_ms=10_000)
    svc1 = LeaseService(db_path=db, clock=clock)
    lease = svc1.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    rotation = svc1.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )
    svc1.close()

    svc2 = LeaseService(db_path=db, clock=clock)
    # The new credential works after restart; the old one does not.
    renewed = svc2.renew(lease.lease_id, "new-secret", ttl_ms=10_000)
    assert renewed.status is LeaseStatus.HELD
    with pytest.raises(InvalidCredentialError):
        svc2.release(lease.lease_id, "old-secret")
    # Replaying the same rotation id after restart is deterministic.
    replay = svc2.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )
    assert replay.replayed is True
    assert replay.new_credential == rotation.new_credential
    events = svc2.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == [
        "ACQUIRED", "CREDENTIAL_ROTATED", "RENEWED"
    ]
    svc2.close()


def test_retry_after_transaction_failure_does_not_double_rotate(
    service: LeaseService,
) -> None:
    """An identical retry must be the canonical recovery path: one swap."""
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")

    def call() -> object:
        return service.rotate_credential(
            lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
        )

    first = call()
    second = call()  # simulated retry of the very same request
    third = call()
    assert first.replayed is False
    assert second.replayed is True
    assert third.replayed is True
    assert service.get_lease(lease.lease_id).version == 1


# --------------------------------------------------------------------------
# Credentials must not leak via queries / history
# --------------------------------------------------------------------------

def test_history_shows_rotation_fact_without_credential_values(
    service: LeaseService,
) -> None:
    """The append-only history records the rotation fact, never the secrets.

    (HTTP view-level leak checks live in test_api.py; the domain ``Lease``
    object intentionally keeps the credential for internal callers.)
    """
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    service.rotate_credential(
        lease.lease_id, "old-secret", "rot-correlation-1",
        new_credential="new-secret",
    )

    lease_events = service.list_events_for_lease(lease.lease_id, limit=100).items
    rotated = [e for e in lease_events if e.event_type == "CREDENTIAL_ROTATED"]
    assert len(rotated) == 1
    event_blob = repr(rotated[0])
    assert "new-secret" not in event_blob
    assert "old-secret" not in event_blob
    # The non-secret correlation id stays visible so the fact is explainable.
    assert "rot-correlation-1" in rotated[0].detail
    assert rotated[0].from_status == "HELD" and rotated[0].to_status == "HELD"
    assert rotated[0].occurred_at_ms > 0

    resource_events = service.list_events_for_resource("r", limit=100).items
    assert any(e.event_type == "CREDENTIAL_ROTATED" for e in resource_events)
    assert "new-secret" not in repr(resource_events)
    assert "old-secret" not in repr(resource_events)


def test_pagination_stays_stable_when_rotation_and_reaping_interleave(
    service: LeaseService, clock: FakeClock
) -> None:
    leases = [service.acquire(f"r-{i}", f"h-{i}", ttl_ms=10_000) for i in range(5)]
    # Rotate some, expire others while walking pages with a small page size.
    service.rotate_credential(
        leases[0].lease_id, leases[0].credential, "rot-a", new_credential="c2"
    )
    first_page = service.list_leases(limit=2)
    assert len(first_page.items) == 2
    cursor = first_page.next_cursor

    clock.advance(10_001)
    service.expire_due_leases()  # every lease now EXPIRED; rows are append-only

    seen = [l.lease_id for l in first_page.items]
    while cursor is not None:
        page = service.list_leases(limit=2, cursor=cursor)
        seen.extend(l.lease_id for l in page.items)
        cursor = page.next_cursor

    assert len(seen) == 5
    assert len(set(seen)) == 5
    assert set(seen) == {l.lease_id for l in leases}


# --------------------------------------------------------------------------
# Full main-link coverage for both credential sources
# --------------------------------------------------------------------------

@pytest.mark.parametrize("caller_supplied", [False, True])
def test_full_chain_acquire_rotate_renew_release_for_both_sources(
    service: LeaseService, clock: FakeClock, caller_supplied: bool
) -> None:
    acquire_credential = "boot-secret" if caller_supplied else None
    lease = service.acquire("r", "A", ttl_ms=10_000, credential=acquire_credential)
    current = lease.credential

    new_credential = "rotated-secret" if caller_supplied else None
    rotation = service.rotate_credential(lease.lease_id, current, "rot-1",
                                         new_credential=new_credential)
    current = rotation.new_credential

    clock.advance(10)
    renewed = service.renew(lease.lease_id, current, ttl_ms=10_000)
    assert renewed.status is LeaseStatus.HELD
    current_lease = renewed

    clock.advance(10)
    # A second rotation using the first rotated credential also succeeds.
    rotation2 = service.rotate_credential(
        current_lease.lease_id, current, "rot-2",
        new_credential="final-secret" if caller_supplied else None,
    )
    current = rotation2.new_credential

    released = service.release(current_lease.lease_id, current)
    assert released.status is LeaseStatus.RELEASED
    with pytest.raises(LeaseReleasedError):
        service.renew(current_lease.lease_id, current, ttl_ms=10_000)


# --------------------------------------------------------------------------
# Cross-instance (file-backed) rotation serialization
# --------------------------------------------------------------------------

def test_two_services_on_same_file_serialize_rotation(tmp_path: Path) -> None:
    db = tmp_path / "rot-multi.db"
    clock = FakeClock(current_ms=0)
    svc_a = LeaseService(db_path=db, clock=clock)
    svc_b = LeaseService(db_path=db, clock=clock)

    lease = svc_a.acquire("shared", "A", ttl_ms=10_000, credential="old-secret")
    rotation = svc_a.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )

    # Service B sees the swap immediately; old credential rejected, new works.
    with pytest.raises(InvalidCredentialError):
        svc_b.renew(lease.lease_id, "old-secret", ttl_ms=10_000)
    renewed = svc_b.renew(lease.lease_id, rotation.new_credential, ttl_ms=10_000)
    assert renewed.status is LeaseStatus.HELD

    # Replaying the idempotency key through the other instance is identical.
    replay = svc_b.rotate_credential(
        lease.lease_id, "old-secret", "rot-1", new_credential="new-secret"
    )
    assert replay.replayed is True
    assert replay.new_credential == "new-secret"

    # Expiry performed by A makes rotation through B fail, never resurrect.
    clock.advance(20_001)
    svc_a.expire_due_leases()
    with pytest.raises(LeaseExpiredError):
        svc_b.rotate_credential(
            lease.lease_id, "new-secret", "rot-2", new_credential="c3"
        )
    svc_a.close()
    svc_b.close()


def test_storage_initialize_is_idempotent_for_rotation_table(tmp_path: Path) -> None:
    db = tmp_path / "init.db"
    st1 = LeaseStorage(db)
    st1.close()
    # Reopening an existing database must add/survive the rotations table.
    st2 = LeaseStorage(db)
    conn = st2._get_conn()
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "credential_rotations" in tables
    st2.close()


def test_overdue_lease_with_identical_credentials_is_reported_as_expired(
    service: LeaseService, clock: FakeClock
) -> None:
    """Decision order: expiry (with lazy terminal transition) wins over the
    identical-new-credential input check."""
    lease = service.acquire("r", "A", ttl_ms=1_000, credential="same")
    clock.advance(1_001)
    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            lease.lease_id, "same", "rot-1", new_credential="same"
        )
    stored = service.get_lease(lease.lease_id)
    assert stored.status is LeaseStatus.EXPIRED
    assert stored.expire_reason == "ttl_elapsed_before_rotate"
