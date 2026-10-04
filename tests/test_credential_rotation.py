"""Tests for holder credential rotation (credential reissue).

Business areas covered:
  A. basic rotation: identity unchanged, old credential dead, new usable
  B. missing request id: rejected before any state change, safely retryable
  C. idempotent replay: same key / duplicates / concurrent / time-advanced
  D. restart durability: replay outcome survives a process restart
  E. expiry interleave: rotation vs lazy/background expiry stays consistent
  F. at-rest secrecy: no credential plaintext on disk, in queries or history
  G. legacy database migration
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
    MissingRequestIdError,
    RotationConflictError,
)
from lease_coordinator.model import FakeClock, RotationStatus, LeaseStatus
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage

ACQUIRE_TTL = 10_000


# --------------------------------------------------------------------------
# A. Basic rotation
# --------------------------------------------------------------------------

def test_rotate_swaps_credential_without_touching_lease_identity(
    service: LeaseService,
) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=ACQUIRE_TTL)
    lease, old = acquired.lease, acquired.credential

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
    assert snap.credential_version == 1
    assert snap.version == 1
    assert snap.expires_at_ms == lease.expires_at_ms
    assert snap.fencing_token == lease.fencing_token
    assert snap.status is LeaseStatus.HELD


def test_old_credential_is_dead_for_renew_release_and_another_rotation(
    service: LeaseService,
) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=ACQUIRE_TTL)
    rotation = service.rotate_credential(
        acquired.lease.lease_id, acquired.credential, request_id="req-1"
    )
    old = acquired.credential

    with pytest.raises(InvalidCredentialError):
        service.renew(acquired.lease.lease_id, old, ttl_ms=ACQUIRE_TTL)
    with pytest.raises(InvalidCredentialError):
        service.release(acquired.lease.lease_id, old)
    with pytest.raises(InvalidCredentialError):
        service.rotate_credential(acquired.lease.lease_id, old, request_id="req-2")

    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.status is LeaseStatus.HELD
    assert snap.credential_version == 1
    assert rotation.credential  # delivered in the rotation result only


def test_caller_supplied_new_credential_is_used_as_is(service: LeaseService) -> None:
    acquired = service.acquire(
        "doc-1", "writer-A", ttl_ms=ACQUIRE_TTL, credential="first-secret"
    )
    rotation = service.rotate_credential(
        acquired.lease.lease_id, "first-secret",
        new_credential="second-secret", request_id="req-1",
    )
    assert rotation.credential == "second-secret"
    assert service.get_lease(acquired.lease.lease_id).credential_version == 1


@pytest.mark.parametrize("caller_supplied", [False, True])
def test_both_credential_sources_walk_acquire_rotate_renew_release(
    service: LeaseService, clock: FakeClock, caller_supplied: bool
) -> None:
    acquired = service.acquire(
        "doc-1", "writer-A", ttl_ms=ACQUIRE_TTL,
        credential="c-0" if caller_supplied else None,
    )
    lease, current = acquired.lease, acquired.credential
    for i in range(1, 3):
        clock.advance(10)
        new = f"c-{i}" if caller_supplied else None
        rotation = service.rotate_credential(
            lease.lease_id, current, new_credential=new, request_id=f"req-{i}"
        )
        assert rotation.status is RotationStatus.ROTATED
        assert rotation.credential_version == i
        current = rotation.credential
        renewed = service.renew(lease.lease_id, current, ttl_ms=ACQUIRE_TTL)
        assert renewed.status is LeaseStatus.HELD

    released = service.release(lease.lease_id, current)
    assert released.status is LeaseStatus.RELEASED


def test_rotation_rejects_identical_new_credential(service: LeaseService) -> None:
    acquired = service.acquire(
        "doc-1", "writer-A", ttl_ms=ACQUIRE_TTL, credential="same"
    )
    with pytest.raises(ValueError):
        service.rotate_credential(
            acquired.lease.lease_id, "same", new_credential="same",
            request_id="req-1",
        )
    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.credential_version == 0
    events = service.list_events_for_lease(acquired.lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED"]


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"new_credential": "n", "request_id": "r"}, LeaseNotFoundError),
        ({"request_id": "r"}, InvalidCredentialError),
    ],
)
def test_rotation_basic_rejections(
    service: LeaseService, kwargs: dict, expected: type[Exception],
) -> None:
    if expected is InvalidCredentialError:
        lease_id = service.acquire("d", "h", ttl_ms=ACQUIRE_TTL).lease.lease_id
    else:
        lease_id = "lease-404"
    with pytest.raises(expected):
        service.rotate_credential(lease_id, "c", **kwargs)


# --------------------------------------------------------------------------
# B. Missing request id: reject before any state change
# --------------------------------------------------------------------------

def test_rotation_without_request_id_is_rejected_and_changes_nothing(
    service: LeaseService,
) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=ACQUIRE_TTL)
    lease, credential = acquired.lease, acquired.credential

    with pytest.raises(MissingRequestIdError) as exc:
        service.rotate_credential(lease.lease_id, credential)
    assert exc.value.code == "missing_request_id"

    # No state change at all.
    snap = service.get_lease(lease.lease_id)
    assert snap.status is LeaseStatus.HELD
    assert snap.credential_version == 0
    assert snap.version == 0
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED"]
    # The old credential is still fully usable: the danger window cannot open.
    renewed = service.renew(lease.lease_id, credential, ttl_ms=ACQUIRE_TTL)
    assert renewed.status is LeaseStatus.HELD


def test_missing_request_id_after_a_lost_response_scenario_cannot_strand_a_credential(
    service: LeaseService, clock: FakeClock,
) -> None:
    """The original bug: response lost, old credential dead, new unreachable.

    With a missing request id the server now refuses up front, so the
    "old dead / new unreachable" window can never exist. The client can
    then issue the rotation with its own id and retry that key freely.
    """
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=ACQUIRE_TTL)
    lease, credential = acquired.lease, acquired.credential

    # First attempt without an id is rejected before touching state.
    with pytest.raises(MissingRequestIdError):
        service.rotate_credential(lease.lease_id, credential)

    # The client comes back (network blip, timeout) with its own stable id.
    first = service.rotate_credential(
        lease.lease_id, credential, new_credential="new-cred", request_id="dedup-7"
    )
    # Simulate the response being lost: the client retries with the SAME id.
    clock.advance(2_000)
    retry = service.rotate_credential(
        lease.lease_id, credential, new_credential="new-cred", request_id="dedup-7"
    )
    assert retry.status is RotationStatus.REPLAYED
    assert retry.credential == first.credential == "new-cred"
    # The retried result is complete: new credential works, old stays dead.
    renewed = service.renew(lease.lease_id, "new-cred", ttl_ms=ACQUIRE_TTL)
    assert renewed.status is LeaseStatus.HELD
    with pytest.raises(InvalidCredentialError):
        service.renew(lease.lease_id, credential, ttl_ms=ACQUIRE_TTL)


def test_empty_request_id_is_treated_as_missing(service: LeaseService) -> None:
    acquired = service.acquire("d", "h", ttl_ms=ACQUIRE_TTL)
    with pytest.raises(MissingRequestIdError):
        service.rotate_credential(acquired.lease.lease_id, acquired.credential,
                                  request_id="")


# --------------------------------------------------------------------------
# C. Idempotent replay, time advancement, concurrency
# --------------------------------------------------------------------------

def test_repeated_request_is_replayed_with_identical_outcome(
    service: LeaseService, clock: FakeClock,
) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=ACQUIRE_TTL)
    t0 = clock.now_ms()
    first = service.rotate_credential(
        acquired.lease.lease_id, acquired.credential, request_id="req-1"
    )

    # Advance time well past the rotation: replay time must not move.
    clock.advance(5_000)
    second = service.rotate_credential(
        acquired.lease.lease_id, acquired.credential, request_id="req-1"
    )
    clock.advance(5_000)
    third = service.rotate_credential(
        acquired.lease.lease_id, acquired.credential, request_id="req-1"
    )

    assert first.status is RotationStatus.ROTATED
    assert second.status is RotationStatus.REPLAYED
    assert third.status is RotationStatus.REPLAYED
    # Credential, version and rotation time are exactly the first swap's.
    for replay in (second, third):
        assert replay.credential == first.credential
        assert replay.credential_version == first.credential_version
        assert replay.rotated_at_ms == t0 == first.rotated_at_ms
        assert replay.rotated_at_ms != clock.now_ms()
        # Frozen lease facts as of the rotation, not the retry moment.
        assert replay.lease.version == 1
        assert replay.lease.credential_version == 1
        assert replay.lease.status is LeaseStatus.HELD
    # Exactly one actual swap happened.
    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.credential_version == 1
    events = service.list_events_for_lease(acquired.lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "CREDENTIAL_ROTATED"]


def test_replay_after_renewals_still_returns_original_facts(
    service: LeaseService, clock: FakeClock,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=2_000, credential="old")
    first = service.rotate_credential(
        acquired.lease.lease_id, "old", new_credential="new", request_id="req-1"
    )
    # Life goes on with the new credential; expiry moves forward.
    clock.advance(1_000)
    service.renew(acquired.lease.lease_id, "new", ttl_ms=10_000)
    clock.advance(1_000)
    replay = service.rotate_credential(
        acquired.lease.lease_id, "old", new_credential="new", request_id="req-1"
    )
    assert replay.credential == "new" == first.credential
    assert replay.rotated_at_ms == first.rotated_at_ms
    # Snapshot frozen at rotation time: original expiry, version 1.
    assert replay.lease.expires_at_ms == acquired.lease.expires_at_ms
    assert replay.lease.version == 1
    # The live lease, queried normally, shows the renewals.
    live = service.get_lease(acquired.lease.lease_id)
    assert live.version == 2
    assert live.expires_at_ms > acquired.lease.expires_at_ms


def test_request_id_reused_with_different_target_is_conflict(
    service: LeaseService,
) -> None:
    lease_a = service.acquire("a", "A", ttl_ms=ACQUIRE_TTL, credential="ca")
    lease_b = service.acquire("b", "B", ttl_ms=ACQUIRE_TTL, credential="cb")
    service.rotate_credential(
        lease_a.lease.lease_id, "ca", new_credential="ca2", request_id="shared"
    )
    with pytest.raises(RotationConflictError) as exc:
        service.rotate_credential(
            lease_b.lease.lease_id, "cb", new_credential="cb2",
            request_id="shared",
        )
    assert exc.value.code == "rotation_conflict"
    assert service.get_lease(lease_b.lease.lease_id).credential_version == 0


def test_request_id_reused_with_different_caller_credential_is_conflict(
    service: LeaseService,
) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=ACQUIRE_TTL,
                               credential="first")
    service.rotate_credential(
        acquired.lease.lease_id, "first", new_credential="second",
        request_id="req-1",
    )
    with pytest.raises(RotationConflictError):
        service.rotate_credential(
            acquired.lease.lease_id, "second", new_credential="third",
            request_id="req-1",
        )


def test_concurrent_rotations_with_same_request_id_have_single_winner(
    service: LeaseService,
) -> None:
    acquired = service.acquire("hot", "A", ttl_ms=ACQUIRE_TTL)
    results: list[str] = []
    credentials: list[str] = []
    times: list[int] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(12)

    def attempt() -> None:
        barrier.wait()
        try:
            rotation = service.rotate_credential(
                acquired.lease.lease_id, acquired.credential, request_id="req-1"
            )
            results.append(rotation.status.value)
            credentials.append(rotation.credential)
            times.append(rotation.rotated_at_ms)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(lambda _: attempt(), range(12)))

    assert errors == []
    assert results.count("ROTATED") == 1
    assert results.count("REPLAYED") == 11
    assert set(credentials) == {credentials[0]}
    assert set(times) == {times[0]}  # every participant sees the same time
    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.credential_version == 1


def test_concurrent_rotations_with_distinct_request_ids_have_single_winner(
    service: LeaseService,
) -> None:
    acquired = service.acquire("hot", "A", ttl_ms=ACQUIRE_TTL)
    winners: list[str] = []
    rejected: list[str] = []
    barrier = threading.Barrier(12)

    def attempt(idx: int) -> None:
        barrier.wait()
        try:
            rotation = service.rotate_credential(
                acquired.lease.lease_id, acquired.credential,
                request_id=f"req-{idx}",
            )
            winners.append(rotation.credential)
        except InvalidCredentialError:
            rejected.append(f"req-{idx}")
        except Exception as exc:  # noqa: BLE001
            rejected.append(f"unexpected:{exc!r}")

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(attempt, range(12)))

    assert len(winners) == 1
    assert len(rejected) == 11
    assert not any(r.startswith("unexpected") for r in rejected)
    assert service.get_lease(acquired.lease.lease_id).credential_version == 1


def test_rejected_rotation_then_retry_with_correct_credential_succeeds(
    service: LeaseService,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=ACQUIRE_TTL)
    with pytest.raises(InvalidCredentialError):
        service.rotate_credential(
            acquired.lease.lease_id, "wrong", request_id="req-1"
        )
    rotation = service.rotate_credential(
        acquired.lease.lease_id, acquired.credential, request_id="req-1"
    )
    assert rotation.status is RotationStatus.ROTATED
    assert service.get_lease(acquired.lease.lease_id).credential_version == 1


# --------------------------------------------------------------------------
# D. Restart durability
# --------------------------------------------------------------------------

def test_rotation_survives_restart_and_replay_hands_back_same_credential(
    tmp_path: Path,
) -> None:
    db = tmp_path / "rot.db"
    clock = FakeClock(current_ms=5_000)
    svc1 = LeaseService(db_path=db, clock=clock)
    acquired = svc1.acquire("r", "A", ttl_ms=ACQUIRE_TTL, credential="c-old")
    rotation = svc1.rotate_credential(
        acquired.lease.lease_id, "c-old", new_credential="c-new",
        request_id="req-1",
    )
    svc1.close()

    # "Restart" with a new service instance and a later clock.
    clock2 = FakeClock(current_ms=9_000)
    svc2 = LeaseService(db_path=db, clock=clock2)
    replay = svc2.rotate_credential(
        acquired.lease.lease_id, "c-old", new_credential="c-new",
        request_id="req-1",
    )
    assert replay.status is RotationStatus.REPLAYED
    assert replay.credential == rotation.credential == "c-new"
    # The rotation time is the original time, not the restart-time clock.
    assert replay.rotated_at_ms == rotation.rotated_at_ms == 5_000

    with pytest.raises(InvalidCredentialError):
        svc2.renew(acquired.lease.lease_id, "c-old", ttl_ms=ACQUIRE_TTL)
    renewed = svc2.renew(acquired.lease.lease_id, "c-new", ttl_ms=ACQUIRE_TTL)
    assert renewed.status is LeaseStatus.HELD

    events = svc2.list_events_for_lease(acquired.lease.lease_id).items
    assert [e.event_type for e in events] == [
        "ACQUIRED", "CREDENTIAL_ROTATED", "RENEWED"
    ]
    svc2.close()


def test_restart_replay_conclusion_holds_even_after_expiry(tmp_path: Path) -> None:
    """A restarted retry still reports the original successful rotation
    even when the lease has since expired; it does not flip to an error."""
    db = tmp_path / "rot-exp.db"
    svc1 = LeaseService(db_path=db, clock=FakeClock(current_ms=0))
    acquired = svc1.acquire("r", "A", ttl_ms=1_000, credential="c-old")
    first = svc1.rotate_credential(
        acquired.lease.lease_id, "c-old", new_credential="c-new",
        request_id="req-1",
    )
    svc1.close()

    svc2 = LeaseService(db_path=db, clock=FakeClock(current_ms=5_000))
    replay = svc2.rotate_credential(
        acquired.lease.lease_id, "c-old", new_credential="c-new",
        request_id="req-1",
    )
    assert replay.status is RotationStatus.REPLAYED
    assert replay.credential == first.credential
    assert replay.rotated_at_ms == 0
    assert replay.lease.status is LeaseStatus.HELD  # frozen at rotation time
    # After an explicit reaping pass the live lease is expired; neither the
    # new nor the old credential can renew it, yet the replay result itself
    # stayed exactly as first committed.
    svc2.expire_due_leases()
    assert svc2.get_lease(acquired.lease.lease_id).status is LeaseStatus.EXPIRED
    with pytest.raises(LeaseExpiredError):
        svc2.renew(acquired.lease.lease_id, "c-new", ttl_ms=10_000)
    svc2.close()


def test_cross_process_replay_shares_one_outcome(tmp_path: Path) -> None:
    """Two real OS processes sharing one db file: process A rotates, process
    B retries the same key and gets the identical result (shared key file)."""
    import os
    import subprocess
    import sys
    import textwrap

    db = tmp_path / "m.db"
    lid_file = tmp_path / "lid"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    proc_a = textwrap.dedent(
        f"""
        from lease_coordinator.model import FakeClock
        from lease_coordinator.service import LeaseService
        svc = LeaseService(db_path=r"{db}", clock=FakeClock(1000))
        a = svc.acquire("r", "A", ttl_ms=20000, credential="old")
        open(r"{lid_file}", "w").write(a.lease.lease_id)
        r = svc.rotate_credential(
            a.lease.lease_id, "old", new_credential="new", request_id="K"
        )
        print("A", r.status.value, r.credential, r.rotated_at_ms)
        svc.close()
        """
    )
    proc_b = textwrap.dedent(
        f"""
        from lease_coordinator.model import FakeClock
        from lease_coordinator.service import LeaseService
        lid = open(r"{lid_file}").read()
        svc = LeaseService(db_path=r"{db}", clock=FakeClock(7000))
        r = svc.rotate_credential(
            lid, "old", new_credential="new", request_id="K"
        )
        print("B", r.status.value, r.credential, r.rotated_at_ms)
        rn = svc.renew(lid, "new", ttl_ms=20000)
        print("B-renew", rn.status.value, rn.version)
        svc.close()
        """
    )
    pa = tmp_path / "a.py"; pa.write_text(proc_a)
    pb = tmp_path / "b.py"; pb.write_text(proc_b)
    out1 = subprocess.run([sys.executable, str(pa)], capture_output=True,
                          text=True, env=env, check=True)
    out2 = subprocess.run([sys.executable, str(pb)], capture_output=True,
                          text=True, env=env, check=True)
    assert "A ROTATED new 1000" in out1.stdout
    assert "B REPLAYED new 1000" in out2.stdout
    assert "B-renew HELD 2" in out2.stdout


def test_failed_rotation_is_stable_across_restart(tmp_path: Path) -> None:
    db = tmp_path / "rot-fail.db"
    clock = FakeClock(current_ms=0)
    svc1 = LeaseService(db_path=db, clock=clock)
    acquired = svc1.acquire("r", "A", ttl_ms=1_000)
    clock.advance(1_001)
    with pytest.raises(LeaseExpiredError):
        svc1.rotate_credential(
            acquired.lease.lease_id, acquired.credential, request_id="req-1"
        )
    svc1.close()

    svc2 = LeaseService(db_path=db, clock=FakeClock(current_ms=2_000))
    with pytest.raises(LeaseExpiredError):
        svc2.rotate_credential(
            acquired.lease.lease_id, acquired.credential, request_id="req-1"
        )
    events = svc2.list_events_for_lease(acquired.lease.lease_id).items
    assert "CREDENTIAL_ROTATED" not in [e.event_type for e in events]
    assert svc2.get_lease(acquired.lease.lease_id).credential_version == 0
    svc2.close()


def test_database_created_before_rotation_is_migrated(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
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

    # The migration must have erased the plaintext column entirely.
    conn = sqlite3.connect(db)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(leases)")}
    assert "credential" not in cols
    assert "credential_hash" in cols
    dump = "\n".join(
        str(r) for r in conn.execute("SELECT * FROM leases").fetchall()
    )
    assert "legacy-cred" not in dump and "fresh" not in dump
    conn.close()


# --------------------------------------------------------------------------
# E. Expiry / reaping interleave
# --------------------------------------------------------------------------

def test_rotation_on_expired_lease_is_rejected_and_expires_it_lazily(
    service: LeaseService, clock: FakeClock,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(1_001)

    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            acquired.lease.lease_id, acquired.credential, request_id="req-1"
        )

    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.status is LeaseStatus.EXPIRED
    assert snap.expire_reason == "ttl_elapsed_before_rotate"
    assert snap.credential_version == 0
    events = service.list_events_for_lease(acquired.lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "EXPIRED"]


def test_reaper_expired_lease_cannot_be_rotated_back_to_life(
    service: LeaseService, clock: FakeClock,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(1_001)
    service.expire_due_leases()

    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            acquired.lease.lease_id, acquired.credential, request_id="req-1"
        )

    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.status is LeaseStatus.EXPIRED
    assert snap.expires_at_ms == acquired.lease.expires_at_ms
    assert snap.credential_version == 0


def test_rotation_does_not_extend_lease_and_reaper_still_reaps_on_time(
    service: LeaseService, clock: FakeClock,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(900)
    rotation = service.rotate_credential(
        acquired.lease.lease_id, acquired.credential, request_id="req-1"
    )
    assert (
        service.get_lease(acquired.lease.lease_id).expires_at_ms
        == acquired.lease.expires_at_ms
    )

    clock.advance(101)  # original ttl now elapsed
    reaped = service.expire_due_leases()
    assert [l.lease_id for l in reaped] == [acquired.lease.lease_id]

    with pytest.raises(LeaseExpiredError):
        service.renew(acquired.lease.lease_id, rotation.credential, ttl_ms=10_000)


def test_rotation_and_reaper_race_leaves_a_single_consistent_state(
    service: LeaseService, clock: FakeClock,
) -> None:
    """Rotation crossing the expiry boundary under concurrency: either the
    rotation wins and the reaper later reaps the new credential on the
    original schedule, or the reaper wins and rotation is rejected. The
    lease is never left both rotated-and-not-reaped ambiguously."""
    acquired = service.acquire("r", "A", ttl_ms=1_000, credential="old")
    rotated_ok: list[bool] = []
    expired_rejections: list[bool] = []
    errors: list[Exception] = []

    def rotate() -> None:
        try:
            r = service.rotate_credential(
                acquired.lease.lease_id, "old", new_credential="new",
                request_id="req-1",
            )
            rotated_ok.append(r.status is RotationStatus.ROTATED)
        except LeaseExpiredError:
            # Legitimate cross outcome: the reaper committed first.
            expired_rejections.append(True)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def reap() -> None:
        service.expire_due_leases()

    clock.advance(1_001)
    t1 = threading.Thread(target=rotate)
    t2 = threading.Thread(target=reap)
    t1.start(); t2.start(); t1.join(); t2.join()

    assert errors == []
    # Exactly one of the two terminal outcomes, never both committed.
    assert len(rotated_ok) + len(expired_rejections) == 1
    snap = service.get_lease(acquired.lease.lease_id)
    assert snap.status is LeaseStatus.EXPIRED
    events = service.list_events_for_lease(acquired.lease.lease_id).items
    types = [e.event_type for e in events]
    assert types.count("EXPIRED") == 1
    if rotated_ok:
        assert snap.credential_version == 1
        assert types.count("CREDENTIAL_ROTATED") == 1
    else:
        assert snap.credential_version == 0
        assert "CREDENTIAL_ROTATED" not in types


def test_rotation_on_released_lease_is_rejected(service: LeaseService) -> None:
    acquired = service.acquire("r", "A", ttl_ms=ACQUIRE_TTL)
    service.release(acquired.lease.lease_id, acquired.credential)
    with pytest.raises(LeaseReleasedError):
        service.rotate_credential(
            acquired.lease.lease_id, acquired.credential, request_id="req-1"
        )


# --------------------------------------------------------------------------
# F. At-rest secrecy: nothing but facts
# --------------------------------------------------------------------------

def test_rotation_event_is_recorded_but_never_contains_credentials(
    service: LeaseService, clock: FakeClock,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=ACQUIRE_TTL,
                               credential="old-secret")
    clock.advance(5)
    service.rotate_credential(
        acquired.lease.lease_id, "old-secret",
        new_credential="new-secret", request_id="req-1",
    )
    events = service.list_events_for_lease(acquired.lease.lease_id, limit=100).items
    [rot_event] = [e for e in events if e.event_type == "CREDENTIAL_ROTATED"]
    assert rot_event.from_status == "HELD"
    assert rot_event.to_status == "HELD"
    assert rot_event.occurred_at_ms == clock.now_ms()
    assert "req-1" in rot_event.detail
    assert "source=caller" in rot_event.detail
    for event in events:
        assert "old-secret" not in event.detail
        assert "new-secret" not in event.detail

    resource_events = service.list_events_for_resource("r", limit=100).items
    assert any(e.event_type == "CREDENTIAL_ROTATED" for e in resource_events)
    for event in resource_events:
        assert "old-secret" not in event.detail
        assert "new-secret" not in event.detail


def test_queries_listings_and_snapshots_expose_facts_only(
    service: LeaseService,
) -> None:
    acquired = service.acquire("r", "A", ttl_ms=ACQUIRE_TTL,
                               credential="old-secret")
    rotation = service.rotate_credential(
        acquired.lease.lease_id, "old-secret",
        new_credential="new-secret", request_id="req-1",
    )
    snap = service.get_lease(acquired.lease.lease_id)
    assert not hasattr(snap, "credential")
    listing = service.list_leases(limit=100).items
    [row] = [r for r in listing if r.lease_id == acquired.lease.lease_id]
    assert row.credential_version == 1
    assert not hasattr(row, "credential")
    events = service.list_events_for_lease(acquired.lease.lease_id).items
    resource_events = service.list_events_for_resource("r").items
    for collection in (events, resource_events):
        for event in collection:
            assert rotation.credential not in event.detail
            assert "old-secret" not in event.detail
            assert "new-secret" not in event.detail


def test_persistence_file_contains_no_credential_plaintext(tmp_path: Path) -> None:
    db = tmp_path / "secret.db"
    svc = LeaseService(db_path=db, clock=FakeClock(current_ms=0))
    acquired = svc.acquire(
        "r", "A", ttl_ms=ACQUIRE_TTL,
        credential="PLAINTEXT-OLD-9f3a",
    )
    svc.rotate_credential(
        acquired.lease.lease_id, "PLAINTEXT-OLD-9f3a",
        new_credential="PLAINTEXT-NEW-2b7c", request_id="req-1",
    )
    svc.close()

    raw = db.read_bytes()
    assert b"PLAINTEXT-OLD-9f3a" not in raw
    assert b"PLAINTEXT-NEW-2b7c" not in raw
    conn = sqlite3.connect(db)
    for table in ("leases", "credential_rotations", "lease_events"):
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        blob = repr(rows)
        assert "PLAINTEXT-OLD-9f3a" not in blob
        assert "PLAINTEXT-NEW-2b7c" not in blob
    conn.close()


def test_pagination_cursor_stays_stable_while_rotation_and_reap_interleave(
    service: LeaseService, clock: FakeClock,
) -> None:
    ids: list[str] = []
    held: list[str] = []
    for i in range(5):
        acquired = service.acquire(f"r-{i}", f"h-{i}", ttl_ms=1_000)
        ids.append(acquired.lease.lease_id)
        held.append(acquired.credential)

    first_page = service.list_leases(limit=2)
    assert [i.lease_id for i in first_page.items] == ids[:2]
    cursor = first_page.next_cursor

    service.rotate_credential(ids[2], held[2], request_id="req-1")
    clock.advance(1_001)
    service.expire_due_leases()
    new_id = service.acquire("r-5", "h-5", ttl_ms=1_000).lease.lease_id

    seen = [i.lease_id for i in first_page.items]
    while cursor is not None:
        page = service.list_leases(limit=2, cursor=cursor)
        seen.extend(i.lease_id for i in page.items)
        cursor = page.next_cursor

    assert seen == ids + [new_id]
