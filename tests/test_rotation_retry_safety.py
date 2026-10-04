"""Retry-safety and at-rest hygiene tests for credential rotation.

Covers the failure modes found while hardening client timeout retries:

* a rotation without a caller idempotency key is rejected *before* any
  state changes, so a lost response can never strand the caller;
* replays (sequential, concurrent, after time passes, after a restart)
  return the identical result, including the original rotation time;
* rotation records on disk contain no plaintext credential;
* rotation interleaved with the expiry reaper never corrupts state.
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lease_coordinator.api import create_app
from lease_coordinator.errors import (
    InvalidCredentialError,
    LeaseExpiredError,
    RotationConflictError,
    RotationRequestIdRequiredError,
)
from lease_coordinator.ids import UuidLeaseIdGenerator
from lease_coordinator.model import FakeClock, LeaseStatus, RotationStatus
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage


# --------------------------------------------------------------------------
# Missing idempotency key: rejected before anything changes
# --------------------------------------------------------------------------

def test_lost_response_without_request_id_cannot_strand_caller(
    service: LeaseService,
) -> None:
    """Simulate a client whose rotation response was lost: because the
    request carried no idempotency key it was rejected up front, so the
    old credential is still fully valid and nothing was applied."""
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000)

    # First attempt: response "lost" -> in reality rejected pre-state.
    with pytest.raises(RotationRequestIdRequiredError):
        service.rotate_credential(lease.lease_id, lease.credential)
    # Client retries with the old credential: still rejected, still no
    # state change, and crucially the old credential was never killed.
    with pytest.raises(RotationRequestIdRequiredError):
        service.rotate_credential(lease.lease_id, lease.credential)

    renewed = service.renew(lease.lease_id, lease.credential, ttl_ms=10_000)
    assert renewed.status is LeaseStatus.HELD
    assert renewed.credential_version == 0

    # Once the client sends a proper key, rotation works normally.
    rotation = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    assert rotation.status is RotationStatus.ROTATED
    with pytest.raises(InvalidCredentialError):
        service.renew(lease.lease_id, lease.credential, ttl_ms=10_000)


def test_missing_request_id_does_not_even_trigger_lazy_expiry(
    service: LeaseService, clock: FakeClock,
) -> None:
    """The rejection happens before the lease is even inspected, so an
    overdue lease is *not* lazily expired by the rejected call."""
    lease = service.acquire("doc-1", "writer-A", ttl_ms=1_000)
    clock.advance(1_001)  # lease is now overdue

    with pytest.raises(RotationRequestIdRequiredError):
        service.rotate_credential(lease.lease_id, lease.credential)

    snap = service.get_lease(lease.lease_id)
    assert snap.status is LeaseStatus.HELD  # untouched, not lazily expired
    assert snap.version == 0
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED"]

    # A properly keyed request now performs the lazy expiry exactly once.
    with pytest.raises(LeaseExpiredError):
        service.rotate_credential(
            lease.lease_id, lease.credential, request_id="req-1"
        )
    assert service.get_lease(lease.lease_id).status is LeaseStatus.EXPIRED


def test_http_rotation_without_request_id_is_400_and_changes_nothing(
    clock: FakeClock,
) -> None:
    svc = LeaseService(":memory:", clock=clock, id_generator=UuidLeaseIdGenerator())
    client = TestClient(create_app(svc))
    created = client.post(
        "/leases",
        json={"resource_id": "r", "holder": "A", "ttl_ms": 10_000,
              "credential": "c-old"},
    ).json()

    resp = client.post(
        f"/leases/{created['lease_id']}/rotate", json={"credential": "c-old"}
    )
    assert resp.status_code == 400
    assert resp.json()["code"] == "rotation_request_id_required"

    # Old credential still works afterwards.
    ok = client.post(
        f"/leases/{created['lease_id']}/renew",
        json={"credential": "c-old", "ttl_ms": 10_000},
    )
    assert ok.status_code == 200
    svc.close()


# --------------------------------------------------------------------------
# Same-key retries: identical result, identical rotation time
# --------------------------------------------------------------------------

def test_replay_after_time_passes_keeps_original_rotation_time(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=60_000)
    first = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    rotated_at = first.rotated_at_ms

    clock.advance(30_000)  # half the ttl later the client retries
    replay = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    assert replay.status is RotationStatus.REPLAYED
    assert replay.rotated_at_ms == rotated_at == 1_000_000
    assert replay.credential == first.credential
    assert replay.credential_version == first.credential_version
    assert replay.request_id == first.request_id

    clock.advance(20_000)
    third = service.rotate_credential(
        lease.lease_id, lease.credential, request_id="req-1"
    )
    assert third.rotated_at_ms == rotated_at
    assert third.credential == first.credential


def test_concurrent_same_key_submissions_return_identical_results(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("hot", "A", ttl_ms=60_000)
    barrier = threading.Barrier(8)
    outcomes: list[tuple[str, str, int, int]] = []
    errors: list[Exception] = []

    def attempt() -> None:
        barrier.wait()
        try:
            r = service.rotate_credential(
                lease.lease_id, lease.credential, request_id="req-1"
            )
            outcomes.append(
                (r.status.value, r.credential, r.credential_version,
                 r.rotated_at_ms)
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: attempt(), range(8)))

    assert errors == []
    statuses = [o[0] for o in outcomes]
    assert statuses.count("ROTATED") == 1
    assert statuses.count("REPLAYED") == 7
    # Every submission observed the exact same result payload (credential,
    # version, rotation time); only the ROTATED/REPLAYED marker differs.
    payloads = {(o[1], o[2], o[3]) for o in outcomes}
    assert len(payloads) == 1
    credential, version, rotated_at = payloads.pop()
    assert version == 1
    assert rotated_at == 1_000_000
    assert service.get_lease(lease.lease_id).credential == credential
    events = service.list_events_for_lease(lease.lease_id).items
    assert [e.event_type for e in events] == ["ACQUIRED", "CREDENTIAL_ROTATED"]


def test_same_key_with_different_new_credential_is_conflict(
    service: LeaseService,
) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000,
                            credential="c-old")
    service.rotate_credential(
        lease.lease_id, "c-old", new_credential="c-new", request_id="req-1"
    )
    # Same key, same old credential, but a *different* requested new
    # credential is a different request and must not silently replay.
    with pytest.raises(RotationConflictError):
        service.rotate_credential(
            lease.lease_id, "c-old", new_credential="c-other",
            request_id="req-1",
        )
    # Omitting the caller credential on replay of a caller-sourced
    # rotation is likewise a parameter mismatch.
    with pytest.raises(RotationConflictError):
        service.rotate_credential(lease.lease_id, "c-old", request_id="req-1")
    # ... and adding one to a server-sourced rotation is too.
    service.rotate_credential(
        lease.lease_id, "c-new", request_id="req-2"
    )
    with pytest.raises(RotationConflictError):
        service.rotate_credential(
            lease.lease_id, "c-new", new_credential="c-3", request_id="req-2"
        )


# --------------------------------------------------------------------------
# Restart: replay after reopening the database file
# --------------------------------------------------------------------------

@pytest.mark.parametrize("caller_supplied", [False, True])
def test_replay_after_restart_is_identical_and_old_credential_stays_dead(
    tmp_path: Path, caller_supplied: bool,
) -> None:
    db = tmp_path / "rot.db"
    clock = FakeClock(current_ms=10_000)
    svc1 = LeaseService(db_path=db, clock=clock)
    lease = svc1.acquire("r", "A", ttl_ms=60_000, credential="c-old")
    first = svc1.rotate_credential(
        lease.lease_id, "c-old",
        new_credential="c-new" if caller_supplied else None,
        request_id="req-1",
    )
    assert first.status is RotationStatus.ROTATED
    svc1.close()

    # Restart with a new process/service and a later clock.
    svc2 = LeaseService(db_path=db, clock=FakeClock(current_ms=40_000))
    replay = svc2.rotate_credential(
        lease.lease_id, "c-old",
        new_credential="c-new" if caller_supplied else None,
        request_id="req-1",
    )
    assert replay.status is RotationStatus.REPLAYED
    assert replay.credential == first.credential
    assert replay.rotated_at_ms == first.rotated_at_ms == 10_000
    assert replay.credential_version == first.credential_version

    # Conclusions survive the restart: old credential dead, new one works.
    with pytest.raises(InvalidCredentialError):
        svc2.renew(lease.lease_id, "c-old", ttl_ms=10_000)
    renewed = svc2.renew(lease.lease_id, first.credential, ttl_ms=10_000)
    assert renewed.status is LeaseStatus.HELD
    released = svc2.release(lease.lease_id, first.credential)
    assert released.status is LeaseStatus.RELEASED
    svc2.close()


# --------------------------------------------------------------------------
# At-rest hygiene: no plaintext credential anywhere in the database
# --------------------------------------------------------------------------

def _dump_all_text(db: Path) -> str:
    conn = sqlite3.connect(db)
    try:
        chunks: list[str] = []
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
        for table in tables:
            for row in conn.execute(f"SELECT * FROM {table}"):  # noqa: S608
                chunks.append(repr(tuple(row)))
        return "\n".join(chunks)
    finally:
        conn.close()


def _dump_table(db: Path, table: str) -> str:
    conn = sqlite3.connect(db)
    try:
        return "\n".join(
            repr(tuple(row))
            for row in conn.execute(f"SELECT * FROM {table}")  # noqa: S608
        )
    finally:
        conn.close()


def test_rotation_records_and_history_contain_no_plaintext_credential(
    tmp_path: Path,
) -> None:
    db = tmp_path / "hygiene.db"
    clock = FakeClock(current_ms=0)
    svc = LeaseService(db_path=db, clock=clock)
    lease = svc.acquire("r", "A", ttl_ms=60_000, credential="old-secret-1")
    caller_rot = svc.rotate_credential(
        lease.lease_id, "old-secret-1",
        new_credential="new-secret-1", request_id="req-caller",
    )
    server_rot = svc.rotate_credential(
        lease.lease_id, "new-secret-1", request_id="req-server",
    )
    svc.renew(lease.lease_id, server_rot.credential, ttl_ms=60_000)
    svc.close()

    # Rotation records and the event history hold no credential material at
    # all: not the presented old ones, not the caller-supplied new one, and
    # not the server-generated one (which is only ever derived on demand).
    records = _dump_table(db, "credential_rotations")
    events = _dump_table(db, "lease_events")
    for secret in (
        "old-secret-1", "new-secret-1",
        caller_rot.credential, server_rot.credential,
    ):
        assert secret not in records
        assert secret not in events

    # Retired credentials must not linger *anywhere* in the database; the
    # only credential on disk is the currently live one, and it lives only
    # in the operational leases row — never in records or history.
    full_dump = "\n".join(
        _dump_table(db, t)
        for t in ("resources", "leases", "credential_rotations",
                  "lease_events", "meta")
    )
    assert "old-secret-1" not in full_dump
    assert "new-secret-1" not in full_dump
    assert server_rot.credential not in records

    # Rotation facts are still visible: request ids, source, version, time.
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT request_id, credential_source, sequence_no, rotated_at_ms "
        "FROM credential_rotations ORDER BY sequence_no"
    ).fetchall()
    conn.close()
    assert [r[0] for r in rows] == ["req-caller", "req-server"]
    assert [r[1] for r in rows] == ["caller", "server"]
    assert [r[2] for r in rows] == [1, 2]


def test_queries_and_history_expose_facts_but_never_credentials(
    service: LeaseService,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=60_000, credential="q-old")
    rotation = service.rotate_credential(
        lease.lease_id, "q-old", new_credential="q-new", request_id="req-1"
    )
    # Facts are visible: version counter bumped, rotation event present.
    snap = service.get_lease(lease.lease_id)
    assert snap.credential_version == 1
    events = service.list_events_for_lease(lease.lease_id).items
    assert any(e.event_type == "CREDENTIAL_ROTATED" for e in events)
    # But no query or history payload carries either credential.
    for event in events:
        assert "q-old" not in event.detail
        assert "q-new" not in event.detail
        assert rotation.credential not in event.detail
    for event in service.list_events_for_resource("r").items:
        assert "q-old" not in event.detail
        assert "q-new" not in event.detail


# --------------------------------------------------------------------------
# Legacy database migration: plaintext rows become digest-only rows
# --------------------------------------------------------------------------

def test_legacy_plaintext_rotation_rows_are_migrated(tmp_path: Path) -> None:
    db = tmp_path / "legacy-rot.db"
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
            version INTEGER NOT NULL DEFAULT 0,
            credential_version INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE credential_rotations (
            request_id TEXT PRIMARY KEY, lease_id TEXT NOT NULL,
            resource_id TEXT NOT NULL, sequence_no INTEGER NOT NULL,
            credential_source TEXT NOT NULL, requested_credential TEXT,
            resulting_credential TEXT NOT NULL, rotated_at_ms INTEGER NOT NULL);
        CREATE TABLE lease_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, lease_id TEXT NOT NULL,
            resource_id TEXT NOT NULL, event_type TEXT NOT NULL,
            from_status TEXT, to_status TEXT, occurred_at_ms INTEGER NOT NULL,
            detail TEXT NOT NULL DEFAULT '');
        INSERT INTO resources(resource_id, fencing_seq) VALUES ('r', 1);
        INSERT INTO leases VALUES ('l1', 'r', 'A', 'live-cred', 1, 'HELD',
                                  0, 60000, NULL, NULL, 1, 1);
        INSERT INTO credential_rotations VALUES
            ('req-caller', 'l1', 'r', 1, 'caller',
             'pre-old-cred', 'live-cred', 5000),
            ('req-server', 'l1', 'r', 1, 'server',
             'pre-old-cred-2', 'generated-cred', 6000);
        """
    )
    conn.commit()
    conn.close()

    svc = LeaseService(db_path=db, clock=FakeClock(current_ms=10_000))

    # Migration removed every plaintext credential from the rotation table.
    dump = _dump_all_text(db)
    assert "pre-old-cred" not in dump
    assert "generated-cred" not in dump

    # A caller-sourced legacy rotation still replays when the caller
    # re-presents the very same new credential.
    replay = svc.rotate_credential(
        "l1", "pre-old-cred", new_credential="live-cred",
        request_id="req-caller",
    )
    assert replay.status is RotationStatus.REPLAYED
    assert replay.credential == "live-cred"
    assert replay.rotated_at_ms == 5000

    # A server-generated legacy result cannot be reconstructed; the replay
    # fails explicitly instead of returning a wrong credential.
    with pytest.raises(RotationConflictError):
        svc.rotate_credential("l1", "pre-old-cred-2", request_id="req-server")

    # The live credential on the lease is unaffected throughout.
    renewed = svc.renew("l1", "live-cred", ttl_ms=60_000)
    assert renewed.status is LeaseStatus.HELD
    svc.close()


# --------------------------------------------------------------------------
# Rotation vs. expiry reaper: state stays consistent in either interleaving
# --------------------------------------------------------------------------

def test_rotation_and_reaper_race_leaves_consistent_state(
    service: LeaseService, clock: FakeClock,
) -> None:
    lease = service.acquire("r", "A", ttl_ms=1_000)
    clock.advance(999)  # 1ms before expiry: both outcomes are legal

    barrier = threading.Barrier(2)
    rotation_outcome: list[object] = []

    def rotate() -> None:
        barrier.wait()
        try:
            rotation_outcome.append(
                service.rotate_credential(
                    lease.lease_id, lease.credential, request_id="req-1"
                )
            )
        except LeaseExpiredError:
            rotation_outcome.append("expired")

    def reap() -> None:
        barrier.wait()
        clock.advance(1)  # now the ttl has fully elapsed
        service.expire_due_leases()

    t = threading.Thread(target=rotate)
    t.start()
    reap()
    t.join()

    snap = service.get_lease(lease.lease_id)
    events = service.list_events_for_lease(lease.lease_id).items
    event_types = [e.event_type for e in events]
    if rotation_outcome[0] == "expired":
        # Reaper won: no rotation happened, exactly one expiry.
        assert snap.status is LeaseStatus.EXPIRED
        assert snap.credential_version == 0
        assert event_types.count("EXPIRED") == 1
        assert "CREDENTIAL_ROTATED" not in event_types
    else:
        # Rotation won the race but did not extend the lease: the reaper
        # still expired it at the original deadline, exactly once.
        rotation = rotation_outcome[0]
        assert snap.status is LeaseStatus.EXPIRED
        assert snap.credential_version == 1
        assert snap.expires_at_ms == lease.expires_at_ms
        assert event_types.count("EXPIRED") == 1
        assert event_types.count("CREDENTIAL_ROTATED") == 1
        # The rotated credential cannot bring the lease back.
        with pytest.raises(LeaseExpiredError):
            service.renew(lease.lease_id, rotation.credential, ttl_ms=10_000)
