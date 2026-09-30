"""Tests for holder credential rotation (credential reissue).

Covers: normal rotation with both credential sources, old-credential
invalidation, idempotent replay, concurrent rotation, interleaving with
expiry/reaping, restart/retry consistency, non-leaking history and stable
pagination across rotation/reaping.
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from lease_coordinator.errors import (
    InvalidCredentialError,
    LeaseExpiredError,
    LeaseNotFoundError,
    LeaseReleasedError,
    RotationConflictError,
)
from lease_coordinator.model import FakeClock, RotationStatus, LeaseStatus
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage


# --------------------------------------------------------------------------
# Normal rotation: old credential dies, new credential continues the chain
# --------------------------------------------------------------------------

def test_rotate_swaps_credential_without_touching_lease_identity(
    service: LeaseService,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    old = lease.credential

    rotation = service.rotate_credential(
        lease.lease_id, old, request_id="req-1"
    )

    assert rotation.status is RotationStatus.ROTATED
    assert rotation.request_id == "req-1"
    assert rotation.credential != old
    assert len(rotation.credential) == 32  # secrets.token_hex(16)
    assert rotation.credential_version == 1
    assert rotation.rotated_at_ms == 1_000_000
    # The lease identity / expiry / fencing token are untouched.
    snap = service.get_lease(lease.lease_id)
    assert snap.credential == rotation.credential
    assert snap.credential_version == 1
    assert snap.version == 1
    assert snap.expires_at_ms == lease.expires_at_ms
    assert snap.fencing_token == lease.fencing_token
    assert snap.status is LeaseStatus.HELD


def test_old_credential_is_dead_for_renew_release_and_another_rotation(
    service: LeaseService,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    rotation = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    old = lease.credential

    with pytest.raises(InvalidCredentialError):
        service.renew(lease.lease_id, old, ttl_ms=10_000)
    with pytest.raises(InvalidCredentialError):
        service.release(lease.lease_id, old)
    with pytest.raises(InvalidCredentialError):
        service.rotate_credential(lease.lease_id, old, request_id="req-2")

    # State was not disturbed by the rejected calls.
    snap = service.get_lease(lease.lease_id)
    assert snap.status is LeaseStatus.HELD
    assert snap.credential == rotation.credential


def test_caller_supplied_new_credential_is_used_as_is(service: LeaseService) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000,
                            credential="first-secret")
    rotation = service.rotate_credential(
        lease.lease_id, "first-secret",
        new_credential="second-secret", request_id="req-1",
    )
    assert rotation.credential == "second-secret"
    assert service.get_lease(lease.lease_id).credential == "second-secret"


@pytest.mark.parametrize("caller_supplied", [False, True])
def test_both_credential_sources_walk_acquire_rotate_renew_release(
    service: LeaseService, clock: FakeClock, caller_supplied: bool
) -> None:
    lease = service.acquire(
        "doc-1", "writer-A", ttl_ms=10_000,
        credential="c-0" if caller_supplied else None,
    )
    current = lease.credential
    for i in range(1, 3):
        clock.advance(10)
        new = f"c-{i}" if caller_supplied else None
        rotation = service.rotate_credential(
            lease.lease_id, current, new_credential=new, request_id=f"req-{i}"
        )
        assert rotation.status is RotationStatus.ROTATED
        assert rotation.credential_version == i
        current = rotation.credential
        # Business continues with the new credential.
        renewed = service.renew(lease.lease_id, current, ttl_ms=10_000)
        assert renewed.status is LeaseStatus.HELD

    released = service.release(lease.lease_id, current)
    assert released.status is LeaseStatus.RELEASED


def test_rotation_rejects_identical_new_credential(service: LeaseService) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000,
                            credential="same")
    with pytest.raises(ValueError):
        service.rotate_credential(
            lease.lease_id, "same", new_credential="same", request_id="req-1"
        )
    # Nothing changed.
    snap = service.get_lease(lease.lease_id)
    assert snap.credential == "same"
    assert snap.credential_version == 0
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED"]


def test_rotation_on_unknown_lease_is_not_found(service: LeaseService) -> None:
    with pytest.raises(LeaseNotFoundError):
        service.rotate_credential("lease-404", "c", request_id="req-1")


def test_rotation_with_wrong_credential_is_rejected(service: LeaseService) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    with pytest.raises(InvalidCredentialError):
        service.rotate_credential(lease.lease_id, "forged", request_id="req-1")
    snap = service.get_lease(lease.lease_id)
    assert snap.credential_version == 0
    assert snap.credential == lease.credential


def test_server_assigns_request_id_when_caller_omits_it(
    service: LeaseService,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    rotation = service.rotate_credential(lease.lease_id, lease.credential)
    assert rotation.request_id.startswith("srv-")


# --------------------------------------------------------------------------
# Idempotency: replays and request-id misuse
# --------------------------------------------------------------------------

def test_repeated_request_is_replayed_with_the_same_credential(
    service: LeaseService,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    first = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    second = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    third = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )

    assert first.status is RotationStatus.ROTATED
    assert second.status is RotationStatus.REPLAYED
    assert third.status is RotationStatus.REPLAYED
    assert second.credential == first.credential
    assert third.credential == first.credential
    # Exactly one actual swap happened.
    snap = service.get_lease(lease.lease_id)
    assert snap.credential_version == 1
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "CREDENTIAL_ROTATED"]


def test_request_id_reused_with_different_target_is_conflict(
    service: LeaseService,
) -> None:
    lease_a = service.acquire("a", "A", ttl_ms=10_000, credential="ca")
    lease_b = service.acquire("b", "B", ttl_ms=10_000, credential="cb")
    service.rotate_credential(
        lease_a.lease_id, "ca", new_credential="ca2", request_id="shared"
    )
    with pytest.raises(RotationConflictError) as exc:
        service.rotate_credential(
            lease_b.lease_id, "cb", new_credential="cb2",
            request_id="shared",
        )
    assert exc.value.code == "rotation_conflict"
    # The second lease was untouched.
    assert service.get_lease(lease_b.lease_id).credential == "cb"


def test_request_id_reused_with_different_caller_credential_is_conflict(
    service: LeaseService,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000,
                            credential="first")
    service.rotate_credential(
        lease.lease_id, "first", new_credential="second", request_id="req-1"
    )
    # Same key but now presenting the *new* credential as the old one.
    with pytest.raises(RotationConflictError):
        service.rotate_credential(
            lease.lease_id, "second", new_credential="third",
            request_id="req-1",
        )


# --------------------------------------------------------------------------
# Concurrency: one real rotation wins
# --------------------------------------------------------------------------

def test_concurrent_rotations_with_same_request_id_have_single_winner(
    service: LeaseService,
) -> None:
    lease = service.acquire("hot", "A", ttl_ms=10_000)
    results: list[str] = []
    credentials: list[str] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(12)

    def attempt() -> None:
        barrier.wait()
        try:
            rotation = service.rotate_credential(
                lease.lease_id, lease.credential, request_id="req-1"
            )
            results.append(rotation.status.value)
            credentials.append(rotation.credential)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(lambda _: attempt(), range(12)))

    assert errors == []
    assert results.count("ROTATED") == 1
    assert results.count("REPLAYED") == 11
    # Every participant hands the business the same new credential.
    assert set(credentials) == {credentials[0]}
    snap = service.get_lease(lease.lease_id)
    assert snap.credential_version == 1
    assert snap.credential == credentials[0]


def test_concurrent_rotations_with_distinct_request_ids_have_single_winner(
    service: LeaseService,
) -> None:
    lease = service.acquire("hot", "A", ttl_ms=10_000)
    winners: list[str] = []
    rejected: list[str] = []
    barrier = threading.Barrier(12)

    def attempt(idx: int) -> None:
        barrier.wait()
        try:
            rotation = service.rotate_credential(
                lease.lease_id, lease.credential, request_id=f"req-{idx}"
            )
            winners.append(rotation.credential)
        except InvalidCredentialError:
            # The winner already invalidated the credential this thread knew.
            rejected.append(f"req-{idx}")
        except Exception as exc:  # noqa: BLE001
            rejected.append(f"unexpected:{exc!r}")

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(attempt, range(12)))

    assert len(winners) == 1
    assert len(rejected) == 11
    assert not any(r.startswith("unexpected") for r in rejected)
    assert service.get_lease(lease.lease_id).credential_version == 1


# --------------------------------------------------------------------------
# Interleaving with expiry / reaping
# --------------------------------------------------------------------------

def test_rotation_on_expired_lease_is_rejected_and_expires_it_lazily(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(1_001)

    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(lease.lease_id, lease.credential,
                                  request_id="req-1")

    snap = service.get_lease(lease.lease_id)
    assert snap.status is LeaseStatus.EXPIRED
    assert snap.expire_reason == "ttl_elapsed_before_rotate"
    assert snap.credential == lease.credential
    assert snap.credential_version == 0
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "EXPIRED"]


def test_reaper_expired_lease_cannot_be_rotated_back_to_life(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(1_001)
    service.expire_due_leases()

    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(lease.lease_id, lease.credential,
                                  request_id="req-1")

    snap = service.get_lease(lease.lease_id)
    assert snap.status is LeaseStatus.EXPIRED
    assert snap.expires_at_ms == lease.expires_at_ms  # not extended
    assert snap.credential_version == 0


def test_rotation_does_not_extend_lease_and_reaper_still_reaps_on_time(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(900)
    rotation = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    assert service.get_lease(lease.lease_id).expires_at_ms == lease.expires_at_ms

    clock.advance(101)  # original ttl now elapsed
    reaped = service.expire_due_leases()
    assert [l.lease_id for l in reaped] == [lease.lease_id]

    # Even the brand-new credential cannot operate after expiry.
    with pytest.raises(LeaseExpiredError):
        service.renew(lease.lease_id, rotation.credential, ttl_ms=10_000)


def test_rotation_on_released_lease_is_rejected(service: LeaseService) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000)
    service.release(lease.lease_id, lease.credential)
    with pytest.raises(LeaseReleasedError):
        service.rotate_credential(lease.lease_id, lease.credential,
                                  request_id="req-1")


def test_rejected_rotation_then_retry_with_correct_credential_succeeds(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000)
    with pytest.raises(InvalidCredentialError):
        service.rotate_credential(lease.lease_id, "wrong", request_id="req-1")
    rotation = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    assert rotation.status is RotationStatus.ROTATED
    assert service.get_lease(lease.lease_id).credential_version == 1


# --------------------------------------------------------------------------
# Restart / retry consistency on a file database
# --------------------------------------------------------------------------

def test_rotation_survives_restart_and_replay_hands_back_same_credential(
    tmp_path: Path,
) -> None:
    db = tmp_path / "rot.db"
    clock = FakeClock(current_ms=5_000)
    svc1 = LeaseService(db_path=db, clock=clock)
    lease = svc1.acquire("r", "A", ttl_ms=10_000, credential="c-old")
    rotation = svc1.rotate_credential(
        lease.lease_id, "c-old", new_credential="c-new", request_id="req-1"
    )
    svc1.close()

    # "Restart" with a new service instance over the same file.
    clock2 = FakeClock(current_ms=6_000)
    svc2 = LeaseService(db_path=db, clock=clock2)
    replay = svc2.rotate_credential(
        lease.lease_id, "c-old", new_credential="c-new", request_id="req-1"
    )
    assert replay.status is RotationStatus.REPLAYED
    assert replay.credential == rotation.credential == "c-new"

    # Old credential stays dead; the new credential continues normally.
    with pytest.raises(InvalidCredentialError):
        svc2.renew(lease.lease_id, "c-old", ttl_ms=10_000)
    renewed = svc2.renew(lease.lease_id, "c-new", ttl_ms=10_000)
    assert renewed.status is LeaseStatus.HELD

    events = svc2.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == [
        "ACQUIRED", "CREDENTIAL_ROTATED", "RENEWED"
    ]
    svc2.close()


def test_failed_rotation_is_stable_across_restart(tmp_path: Path) -> None:
    db = tmp_path / "rot-fail.db"
    clock = FakeClock(current_ms=0)
    svc1 = LeaseService(db_path=db, clock=clock)
    lease = svc1.acquire("r", "A", ttl_ms=1_000)
    clock.advance(1_001)
    with pytest.raises(LeaseExpiredError):
        svc1.rotate_credential(lease.lease_id, lease.credential,
                               request_id="req-1")
    svc1.close()

    svc2 = LeaseService(db_path=db, clock=FakeClock(current_ms=2_000))
    with pytest.raises(LeaseExpiredError):
        svc2.rotate_credential(lease.lease_id, lease.credential,
                               request_id="req-1")
    events = svc2.list_events_for_lease(lease.lease_id).items
    assert "CREDENTIAL_ROTATED" not in [e.event_type for e in events]
    assert svc2.get_lease(lease.lease_id).credential_version == 0
    svc2.close()


def test_database_created_before_rotation_is_migrated(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    # Simulate a database written by the pre-rotation release.
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE resources (resource_id TEXT PRIMARY KEY,
                                fencing_seq INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE leases (
            lease_id TEXT PRIMARY KEY, resource_id TEXT NOT NULL,
            holder TEXT NOT NULL, credential TEXT NOT NULL,
            fencing_token INTEGER NOT NULL, status TEXT NOT NULL,
            acquired_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL,
            released_at_ms INTEGER, expire_reason TEXT,
            version INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE lease_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, lease_id TEXT NOT NULL,
            resource_id TEXT NOT NULL, event_type TEXT NOT NULL,
            from_status TEXT, to_status TEXT, occurred_at_ms INTEGER NOT NULL,
            detail TEXT NOT NULL DEFAULT '');
        INSERT INTO resources(resource_id, fencing_seq) VALUES ('r', 1);
        INSERT INTO leases VALUES ('l1', 'r', 'A', 'legacy-cred', 1, 'HELD',
                                  0, 10000, NULL, NULL, 0);
        """
    )
    conn.commit()
    conn.close()

    storage = LeaseStorage(db)
    legacy = storage.get_lease("l1")
    assert legacy is not None and legacy.credential_version == 0

    clock = FakeClock(current_ms=0)
    svc = LeaseService(db_path=db, clock=clock, storage=storage)
    rotation = svc.rotate_credential(
        "l1", "legacy-cred", new_credential="fresh", request_id="req-1"
    )
    assert rotation.credential == "fresh"
    assert svc.get_lease("l1").credential_version == 1
    svc.close()


# --------------------------------------------------------------------------
# History / query visibility and pagination stability
# --------------------------------------------------------------------------

def test_rotation_event_is_recorded_but_never_contains_credentials(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000, credential="old-secret")
    clock.advance(5)
    rotation = service.rotate_credential(
        lease.lease_id, "old-secret",
        new_credential="new-secret", request_id="req-1",
    )
    events = service.list_events_for_lease(lease.lease_id, limit=100).items
    [rot_event] = [e for e in events if e.event_type == "CREDENTIAL_ROTATED"]
    assert rot_event.from_status == "HELD"
    assert rot_event.to_status == "HELD"
    assert rot_event.occurred_at_ms == clock.now_ms()
    assert "req-1" in rot_event.detail  # explainability: which request
    assert "source=caller" in rot_event.detail
    assert "old-secret" not in rot_event.detail
    assert "new-secret" not in rotation.lease.lease_id  # sanity on fixture
    # Neither credential may appear anywhere in history or its details.
    for event in events:
        assert "old-secret" not in event.detail
        assert "new-secret" not in event.detail

    resource_events = service.list_events_for_resource("r", limit=100).items
    assert any(e.event_type == "CREDENTIAL_ROTATED" for e in resource_events)
    for event in resource_events:
        assert "old-secret" not in event.detail
        assert "new-secret" not in event.detail


def test_rotation_credential_appears_only_in_its_own_result(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=10_000)
    rotation = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    # Lease listing exposes the rotation fact (version counter) but never
    # embeds the new credential in any history detail.
    listing = service.list_leases(limit=100).items
    [row] = [r for r in listing if r.lease_id == lease.lease_id]
    assert row.credential_version == 1
    events = service.list_events_for_lease(lease.lease_id).items
    assert all(rotation.credential not in e.detail for e in events)
    resource_events = service.list_events_for_resource("r").items
    assert all(rotation.credential not in e.detail for e in resource_events)


def test_pagination_cursor_stays_stable_while_rotation_and_reap_interleave(
    service: LeaseService, clock: FakeClock,
) -> None:
    ids: list[str] = []
    for i in range(5):
        ids.append(service.acquire(f"r-{i}", f"h-{i}", ttl_ms=1_000).lease_id)

    # Walk one page, then mutate the world before continuing.
    first_page = service.list_leases(limit=2)
    assert [i.lease_id for i in first_page.items] == ids[:2]
    cursor = first_page.next_cursor

    # Rotations update rows in place (no new lease rows); expiry of an old
    # lease + acquire of a new one append one row at the end.
    target = service.get_lease(ids[2])
    service.rotate_credential(ids[2], target.credential, request_id="req-1")
    clock.advance(1_001)
    service.expire_due_leases()
    new_id = service.acquire("r-5", "h-5", ttl_ms=1_000).lease_id

    seen = [i.lease_id for i in first_page.items]
    while cursor is not None:
        page = service.list_leases(limit=2, cursor=cursor)
        seen.extend(i.lease_id for i in page.items)
        cursor = page.next_cursor

    # Old cursor position remained valid: every row exactly once, in order.
    assert seen == ids + [new_id]
