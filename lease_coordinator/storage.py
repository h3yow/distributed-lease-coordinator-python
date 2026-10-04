"""SQLite-backed storage for leases and append-only transition history.

The storage layer exposes a small number of *atomic* operations only.
All domain decisions (credential checks, expiry ordering, failure
reasons) live in :mod:`lease_coordinator.service`.

Concurrency notes
-----------------
* Every mutator runs inside one ``BEGIN IMMEDIATE`` transaction, so the
  read-modify-write sequence for a resource is atomic in SQLite even with
  multiple OS processes sharing the database file (WAL + busy_timeout).
* Lease rows and event rows are append-only: leases only transition
  between states and are never deleted, which keeps cursor pagination and
  history queries stable across expiry/reaping/cleanup.
"""

from __future__ import annotations

import base64
import hmac
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, TypeVar

from .crypto import CredentialCodec, generate_key, load_or_create_key
from .model import EventPage, Lease, LeaseEvent, LeasePage, LeaseStatus

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS resources (
    resource_id   TEXT PRIMARY KEY,
    fencing_seq   INTEGER NOT NULL DEFAULT 0
);

-- Lease rows carry only a keyed *fingerprint* of the current holder
-- credential, never the credential itself. An operator inspecting the file
-- can see holder/version/facts but cannot recover or verify a credential
-- without the separate server key.
CREATE TABLE IF NOT EXISTS leases (
    lease_id           TEXT PRIMARY KEY,
    resource_id        TEXT NOT NULL,
    holder             TEXT NOT NULL,
    credential_hash    TEXT NOT NULL,
    fencing_token      INTEGER NOT NULL,
    status             TEXT NOT NULL,
    acquired_at_ms     INTEGER NOT NULL,
    expires_at_ms      INTEGER NOT NULL,
    released_at_ms     INTEGER,
    expire_reason      TEXT,
    version            INTEGER NOT NULL DEFAULT 0,
    credential_version INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_leases_resource ON leases(resource_id, lease_id);

-- One row per *successful* rotation request. The primary key is the
-- caller-supplied idempotency key, so retries and duplicate/concurrent
-- submissions resolve to the exact same outcome. The presented old
-- credential is kept only as a fingerprint; the resulting credential is
-- kept authenticated-encrypted so replays (even after restart) can hand
-- back the identical secret while the database itself holds no plaintext.
-- lease_snapshot freezes the lease facts as of the rotation so a replay
-- returns exactly the first outcome regardless of later renewals/expiry.
CREATE TABLE IF NOT EXISTS credential_rotations (
    request_id                TEXT PRIMARY KEY,
    lease_id                  TEXT NOT NULL,
    resource_id               TEXT NOT NULL,
    sequence_no               INTEGER NOT NULL,
    credential_source         TEXT NOT NULL,
    requested_fingerprint     TEXT NOT NULL,
    new_fingerprint           TEXT NOT NULL,
    sealed_result_credential  TEXT NOT NULL,
    rotated_at_ms             INTEGER NOT NULL,
    lease_snapshot            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rotations_lease
    ON credential_rotations(lease_id, rotated_at_ms);

CREATE TABLE IF NOT EXISTS lease_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id       TEXT NOT NULL,
    resource_id    TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    from_status    TEXT,
    to_status      TEXT,
    occurred_at_ms INTEGER NOT NULL,
    detail         TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_lease ON lease_events(lease_id, id);
CREATE INDEX IF NOT EXISTS idx_events_resource ON lease_events(resource_id, id);
"""

T = TypeVar("T")


def encode_cursor(value: int) -> str:
    """Encode an integer position into an opaque base64url cursor."""
    return base64.urlsafe_b64encode(str(value).encode("ascii")).decode("ascii")


def decode_cursor(cursor: str) -> int:
    """Decode a cursor previously produced by :func:`encode_cursor`."""
    import binascii

    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("ascii")
        value = int(raw)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError(f"malformed cursor: {cursor!r}") from exc
    if value < 0:
        raise ValueError(f"malformed cursor: {cursor!r}")
    return value


class LeaseStorage:
    """Thin data-access layer over a single SQLite database file."""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        codec: CredentialCodec | None = None,
    ) -> None:
        self._db_path = str(db_path)
        # The codec keys all at-rest credential fingerprints and sealed
        # credentials. For a file database it defaults to a sibling key file
        # so data stays decryptable across process restarts; an in-memory
        # database uses an ephemeral random key (nothing outlives the process).
        if codec is not None:
            self._codec = codec
        elif self._db_path == ":memory:":
            self._codec = CredentialCodec(generate_key())
        else:
            self._codec = CredentialCodec(
                load_or_create_key(Path(self._db_path).with_suffix(".key"))
            )
        # In-process write serialization. SQLite itself additionally
        # serializes writers across processes via its file lock.
        self._write_lock = threading.RLock()
        self._local = threading.local()
        if self._db_path == ":memory:":
            # A private shared-cache in-memory DB per storage instance:
            # every thread/connection of THIS storage shares it, but distinct
            # storage instances are fully isolated from each other.
            unique = uuid.uuid4().hex
            self._uri = f"file:lease_mem_{unique}?mode=memory&cache=shared"
            self._conn = self._connect()
        else:
            self._uri = self._db_path
            self._conn = None
        self.initialize()

    # -- connection helpers -------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._uri if hasattr(self, "_uri") else self._db_path,
            timeout=5.0,
            check_same_thread=False,
            isolation_level=None,
            uri=self._db_path == ":memory:",
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        if self._db_path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def _with_transaction(self, fn: Callable[[sqlite3.Connection], T], retries: int = 10) -> T:
        """Run ``fn`` in an IMMEDIATE transaction, retrying lock conflicts."""
        conn = self._get_conn()
        with self._write_lock:
            attempt = 0
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        result = fn(conn)
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    else:
                        conn.execute("COMMIT")
                        return result
                except sqlite3.OperationalError as exc:
                    # database is locked / busy: safe to retry the whole txn
                    attempt += 1
                    if attempt > retries:
                        raise
                    time.sleep(min(0.01 * attempt, 0.1))

    def initialize(self) -> None:
        """Create tables and pragmas; safe to call repeatedly."""
        conn = self._get_conn()
        with self._write_lock:
            conn.executescript(SCHEMA_SQL)
            self._migrate(conn)
            # Rewrite any legacy plaintext credential column atomically now
            # that the new schema exists, so on-disk data never keeps it.
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(leases)")}
            if "credential" in cols:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    self._migrate_plaintext_credentials_txn(conn)
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                else:
                    conn.execute("COMMIT")

    @property
    def codec(self) -> CredentialCodec:
        """The credential codec used by this storage (fingerprint/seal)."""
        return self._codec

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Upgrade databases created before credentials were protected.

        Older databases kept a plaintext ``credential`` column on leases and
        may predate the rotations table. Rebuilding is done by callers that
        know the codec key; :meth:`migrate_plaintext_credentials` performs it.
        Here we only add the version column if a legacy leases table exists.
        """
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(leases)")}
        if "credential_version" not in cols and "credential" in cols:
            conn.execute(
                "ALTER TABLE leases ADD COLUMN credential_version "
                "INTEGER NOT NULL DEFAULT 0"
            )

    def migrate_plaintext_credentials(self) -> None:
        """Convert a legacy plaintext-credential database to fingerprints.

        Best-effort cleanup for files created by earlier development
        versions: the plaintext column is dropped so on-disk data never
        retains credential material. Idempotent on already-new databases.
        """
        conn = self._get_conn()
        with self._write_lock:
            self._with_transaction(
                lambda c: self._migrate_plaintext_credentials_txn(c)
            )

    def _migrate_plaintext_credentials_txn(
        self, conn: sqlite3.Connection,
    ) -> None:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(leases)")}
        if "credential" not in cols:
            return
        conn.execute(
            """
            CREATE TABLE leases_new (
                lease_id TEXT PRIMARY KEY, resource_id TEXT NOT NULL,
                holder TEXT NOT NULL, credential_hash TEXT NOT NULL,
                fencing_token INTEGER NOT NULL, status TEXT NOT NULL,
                acquired_at_ms INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL,
                released_at_ms INTEGER, expire_reason TEXT,
                version INTEGER NOT NULL DEFAULT 0,
                credential_version INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        rows = conn.execute(
            "SELECT * FROM leases"
        ).fetchall()
        for r in rows:
            conn.execute(
                "INSERT INTO leases_new VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    r["lease_id"], r["resource_id"], r["holder"],
                    self._codec.fingerprint(r["credential"]),
                    r["fencing_token"], r["status"], r["acquired_at_ms"],
                    r["expires_at_ms"], r["released_at_ms"], r["expire_reason"],
                    r["version"], r["credential_version"],
                ),
            )
        conn.execute("DROP TABLE leases")
        conn.execute("ALTER TABLE leases_new RENAME TO leases")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_leases_resource "
            "ON leases(resource_id, lease_id)"
        )

    def close(self) -> None:
        with self._write_lock:
            conn = self._get_conn()
            conn.close()

    # -- row mapping --------------------------------------------------------

    @staticmethod
    def _row_to_lease(row: sqlite3.Row | None) -> Lease | None:
        if row is None:
            return None
        return Lease(
            lease_id=row["lease_id"],
            resource_id=row["resource_id"],
            holder=row["holder"],
            credential_hash=row["credential_hash"],
            fencing_token=row["fencing_token"],
            status=LeaseStatus(row["status"]),
            acquired_at_ms=row["acquired_at_ms"],
            expires_at_ms=row["expires_at_ms"],
            released_at_ms=row["released_at_ms"],
            expire_reason=row["expire_reason"],
            version=row["version"],
            credential_version=row["credential_version"],
        )

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> LeaseEvent:
        return LeaseEvent(
            event_id=row["id"],
            lease_id=row["lease_id"],
            resource_id=row["resource_id"],
            event_type=row["event_type"],
            from_status=row["from_status"],
            to_status=row["to_status"],
            occurred_at_ms=row["occurred_at_ms"],
            detail=row["detail"],
        )

    # -- events -------------------------------------------------------------

    @staticmethod
    def _insert_event(
        conn: sqlite3.Connection,
        lease_id: str,
        resource_id: str,
        event_type: str,
        occurred_at_ms: int,
        from_status: str | None,
        to_status: str | None,
        detail: str = "",
    ) -> None:
        conn.execute(
            """
            INSERT INTO lease_events
                (lease_id, resource_id, event_type, from_status, to_status,
                 occurred_at_ms, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (lease_id, resource_id, event_type, from_status, to_status,
             occurred_at_ms, detail),
        )

    # -- atomic mutators ----------------------------------------------------

    def atomic_acquire(
        self,
        resource_id: str,
        lease_id: str,
        holder: str,
        credential_fingerprint: str,
        now_ms: int,
        expires_at_ms: int,
    ) -> tuple[int, Lease | None]:
        """Insert a new lease, expiring the current one first if needed.

        Returns ``(fencing_token, conflict_lease)``. On success the
        conflict lease is ``None``; otherwise no row is inserted and the
        conflicting valid lease is returned.
        """

        def txn(conn: sqlite3.Connection) -> tuple[int, Lease | None]:
            row = conn.execute(
                "SELECT * FROM leases WHERE resource_id = ? "
                "ORDER BY fencing_token DESC LIMIT 1",
                (resource_id,),
            ).fetchone()
            current = self._row_to_lease(row)
            if current is not None and current.status is LeaseStatus.HELD:
                if current.expires_at_ms > now_ms:
                    # Another client holds a valid lease: must not grant twice.
                    return 0, current
                # The previous holder is overdue: expire it inside the same
                # transaction so the slot is freed exactly once.
                conn.execute(
                    "UPDATE leases SET status = ?, expire_reason = ?, version = version + 1 "
                    "WHERE lease_id = ? AND status = ?",
                    (LeaseStatus.EXPIRED.value, "ttl_elapsed_before_acquire",
                     current.lease_id, LeaseStatus.HELD.value),
                )
                self._insert_event(
                    conn, current.lease_id, resource_id, "EXPIRED", now_ms,
                    LeaseStatus.HELD.value, LeaseStatus.EXPIRED.value,
                    "ttl elapsed; expired lazily during acquire",
                )
            conn.execute(
                "INSERT INTO resources(resource_id, fencing_seq) VALUES(?, 1) "
                "ON CONFLICT(resource_id) DO UPDATE SET fencing_seq = fencing_seq + 1",
                (resource_id,),
            )
            token_row = conn.execute(
                "SELECT fencing_seq FROM resources WHERE resource_id = ?",
                (resource_id,),
            ).fetchone()
            fencing_token = int(token_row["fencing_seq"])
            conn.execute(
                """
                INSERT INTO leases (lease_id, resource_id, holder, credential_hash,
                    fencing_token, status, acquired_at_ms, expires_at_ms,
                    released_at_ms, expire_reason, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)
                """,
                (lease_id, resource_id, holder, credential_fingerprint,
                 fencing_token,
                 LeaseStatus.HELD.value, now_ms, expires_at_ms),
            )
            self._insert_event(
                conn, lease_id, resource_id, "ACQUIRED", now_ms,
                None, LeaseStatus.HELD.value,
                f"holder={holder} ttl_to={expires_at_ms} fencing_token={fencing_token}",
            )
            return fencing_token, None

        return self._with_transaction(txn)

    def atomic_renew(
        self,
        lease_id: str,
        credential_fingerprint: str,
        now_ms: int,
        expires_at_ms: int,
    ) -> tuple[Lease | None, str | None]:
        """Renew a lease inside one database transaction.

        Returns ``(lease, reason)`` where reason is one of
        ``not_found`` / ``bad_credential`` / ``released`` / ``expired``.
        """

        def txn(conn: sqlite3.Connection) -> tuple[Lease | None, str | None]:
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            current = self._row_to_lease(row)
            if current is None:
                return None, "not_found"
            if not hmac.compare_digest(
                current.credential_hash, credential_fingerprint
            ):
                return current, "bad_credential"
            if current.status is LeaseStatus.RELEASED:
                return current, "released"
            if current.status is LeaseStatus.EXPIRED or current.expires_at_ms <= now_ms:
                if current.status is LeaseStatus.HELD:
                    conn.execute(
                        "UPDATE leases SET status = ?, expire_reason = ?, version = version + 1 "
                        "WHERE lease_id = ? AND status = ?",
                        (LeaseStatus.EXPIRED.value, "ttl_elapsed_before_renew",
                         lease_id, LeaseStatus.HELD.value),
                    )
                    self._insert_event(
                        conn, lease_id, current.resource_id, "EXPIRED", now_ms,
                        LeaseStatus.HELD.value, LeaseStatus.EXPIRED.value,
                        "renew rejected: ttl already elapsed",
                    )
                    current = self._row_to_lease(
                        conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
                    )
                return current, "expired"
            cur = conn.execute(
                "UPDATE leases SET expires_at_ms = ?, version = version + 1 "
                "WHERE lease_id = ? AND status = ? AND credential_hash = ? AND expires_at_ms > ?",
                (expires_at_ms, lease_id, LeaseStatus.HELD.value,
                 credential_fingerprint, now_ms),
            )
            if cur.rowcount != 1:
                # Lost a concurrent transition: re-read for an accurate reason.
                fresh = self._row_to_lease(
                    conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
                )
                reason = "expired" if fresh is not None and fresh.status is not LeaseStatus.HELD else "bad_credential"
                return fresh, reason
            self._insert_event(
                conn, lease_id, current.resource_id, "RENEWED", now_ms,
                LeaseStatus.HELD.value, LeaseStatus.HELD.value,
                f"renewed ttl_to={expires_at_ms}",
            )
            renewed = self._row_to_lease(
                conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            )
            return renewed, None

        return self._with_transaction(txn)

    def atomic_release(
        self,
        lease_id: str,
        credential_fingerprint: str,
        now_ms: int,
    ) -> tuple[Lease | None, str | None]:
        """Release a lease inside one database transaction.

        Returns ``(lease, reason)`` with the same reason vocabulary as
        :meth:`atomic_renew`.
        """

        def txn(conn: sqlite3.Connection) -> tuple[Lease | None, str | None]:
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            current = self._row_to_lease(row)
            if current is None:
                return None, "not_found"
            if not hmac.compare_digest(
                current.credential_hash, credential_fingerprint
            ):
                return current, "bad_credential"
            if current.status is LeaseStatus.RELEASED:
                return current, "released"
            if current.status is LeaseStatus.EXPIRED:
                return current, "expired"
            if current.expires_at_ms <= now_ms:
                conn.execute(
                    "UPDATE leases SET status = ?, expire_reason = ?, version = version + 1 "
                    "WHERE lease_id = ? AND status = ?",
                    (LeaseStatus.EXPIRED.value, "ttl_elapsed_before_release",
                     lease_id, LeaseStatus.HELD.value),
                )
                self._insert_event(
                    conn, lease_id, current.resource_id, "EXPIRED", now_ms,
                    LeaseStatus.HELD.value, LeaseStatus.EXPIRED.value,
                    "release rejected: ttl already elapsed",
                )
                expired = self._row_to_lease(
                    conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
                )
                return expired, "expired"
            cur = conn.execute(
                "UPDATE leases SET status = ?, released_at_ms = ?, version = version + 1 "
                "WHERE lease_id = ? AND status = ? AND credential_hash = ? AND expires_at_ms > ?",
                (LeaseStatus.RELEASED.value, now_ms, lease_id,
                 LeaseStatus.HELD.value, credential_fingerprint, now_ms),
            )
            if cur.rowcount != 1:
                fresh = self._row_to_lease(
                    conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
                )
                reason = "released" if fresh is not None and fresh.status is LeaseStatus.RELEASED else "expired"
                return fresh, reason
            self._insert_event(
                conn, lease_id, current.resource_id, "RELEASED", now_ms,
                LeaseStatus.HELD.value, LeaseStatus.RELEASED.value,
                "released by credential holder",
            )
            released = self._row_to_lease(
                conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            )
            return released, None

        return self._with_transaction(txn)

    def atomic_expire(
        self,
        lease_id: str,
        now_ms: int,
        reason: str,
    ) -> Lease | None:
        """Mark one HELD lease EXPIRED if it is still due.

        Returns the updated lease, or ``None`` if the lease was not
        HELD / not due / missing, so expiry is never applied twice.
        """

        def txn(conn: sqlite3.Connection) -> Lease | None:
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            current = self._row_to_lease(row)
            if current is None or current.status is not LeaseStatus.HELD:
                return None
            if current.expires_at_ms > now_ms:
                return None
            cur = conn.execute(
                "UPDATE leases SET status = ?, expire_reason = ?, version = version + 1 "
                "WHERE lease_id = ? AND status = ? AND expires_at_ms <= ?",
                (LeaseStatus.EXPIRED.value, reason, lease_id,
                 LeaseStatus.HELD.value, now_ms),
            )
            if cur.rowcount != 1:
                return None
            self._insert_event(
                conn, lease_id, current.resource_id, "EXPIRED", now_ms,
                LeaseStatus.HELD.value, LeaseStatus.EXPIRED.value,
                f"reaper: {reason}",
            )
            return self._row_to_lease(
                conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            )

        return self._with_transaction(txn)

    # -- reads --------------------------------------------------------------

    def _snapshot_json(self, lease: Lease) -> str:
        """Freeze the rotation-time lease facts as canonical JSON."""
        return json.dumps(
            {
                "lease_id": lease.lease_id,
                "resource_id": lease.resource_id,
                "holder": lease.holder,
                "credential_hash": lease.credential_hash,
                "fencing_token": lease.fencing_token,
                "status": lease.status.value,
                "acquired_at_ms": lease.acquired_at_ms,
                "expires_at_ms": lease.expires_at_ms,
                "released_at_ms": lease.released_at_ms,
                "expire_reason": lease.expire_reason,
                "version": lease.version,
                "credential_version": lease.credential_version,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _snapshot_to_lease(payload: str) -> Lease:
        data = json.loads(payload)
        return Lease(
            lease_id=data["lease_id"],
            resource_id=data["resource_id"],
            holder=data["holder"],
            credential_hash=data["credential_hash"],
            fencing_token=data["fencing_token"],
            status=LeaseStatus(data["status"]),
            acquired_at_ms=data["acquired_at_ms"],
            expires_at_ms=data["expires_at_ms"],
            released_at_ms=data["released_at_ms"],
            expire_reason=data["expire_reason"],
            version=data["version"],
            credential_version=data["credential_version"],
        )

    def atomic_rotate_credential(
        self,
        lease_id: str,
        presented_fingerprint: str,
        new_fingerprint: str,
        sealed_new_credential: str,
        credential_source: str,
        request_id: str,
        now_ms: int,
    ) -> tuple[dict[str, object] | None, Lease | None, str | None]:
        """Swap the holder credential of one lease in a single transaction.

        ``presented_fingerprint`` is the fingerprint of the credential the
        caller proved it holds; ``new_fingerprint`` /
        ``sealed_new_credential`` describe the replacement credential
        (already fingerprinted and sealed by the caller with this storage's
        codec).

        Returns ``(outcome, lease, reason)``.

        * ``outcome`` is ``{"status": "rotated"|"replayed", "credential":
          str (plaintext, decrypted for delivery only),
          "credential_version": int, "request_id": str,
          "rotated_at_ms": int}``.
        * ``reason`` is one of ``not_found`` / ``bad_credential`` /
          ``released`` / ``expired`` / ``request_conflict``; when it is set,
          nothing has changed.

        Idempotency: ``request_id`` is the primary key of
        ``credential_rotations``. A repeated identical request (including a
        retry after process restart or a concurrent duplicate) returns the
        stored result as ``replayed`` with the *original* rotation time and
        the *frozen* lease snapshot, byte-identical in its facts. The same
        key reused with a different lease or presented credential fails with
        ``request_conflict`` without touching anything.

        Expiry interaction: rotation never extends or shortens the lease;
        if ``expires_at_ms <= now_ms`` the lease is expired inside this
        transaction (lazily) and rotation is rejected.
        """

        def txn(
            conn: sqlite3.Connection,
        ) -> tuple[dict[str, object] | None, Lease | None, str | None]:
            # The idempotency record is inspected first. Every mutator holds
            # one BEGIN IMMEDIATE write transaction, so the first writer of a
            # given request_id wins; concurrent duplicates block on the write
            # lock and observe the completed row here.
            dup = conn.execute(
                "SELECT * FROM credential_rotations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if dup is not None:
                if (
                    dup["lease_id"] != lease_id
                    or dup["requested_fingerprint"] != presented_fingerprint
                ):
                    return None, None, "request_conflict"
                try:
                    delivered = self._codec.open(
                        dup["sealed_result_credential"]
                    )
                except ValueError:
                    # A key mismatch means this database/key pair cannot
                    # safely reconstruct the committed result: fail loudly
                    # rather than hand out a wrong credential.
                    raise
                frozen = self._snapshot_to_lease(dup["lease_snapshot"])
                return (
                    {
                        "status": "replayed",
                        "credential": delivered,
                        "credential_version": int(dup["sequence_no"]),
                        "request_id": request_id,
                        "rotated_at_ms": int(dup["rotated_at_ms"]),
                    },
                    frozen,
                    None,
                )

            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            current = self._row_to_lease(row)
            if current is None:
                return None, None, "not_found"
            if not hmac.compare_digest(
                current.credential_hash, presented_fingerprint
            ):
                return None, current, "bad_credential"
            if current.status is LeaseStatus.RELEASED:
                return None, current, "released"
            if current.status is LeaseStatus.EXPIRED or current.expires_at_ms <= now_ms:
                if current.status is LeaseStatus.HELD:
                    conn.execute(
                        "UPDATE leases SET status = ?, expire_reason = ?, version = version + 1 "
                        "WHERE lease_id = ? AND status = ?",
                        (LeaseStatus.EXPIRED.value,
                         "ttl_elapsed_before_rotate",
                         lease_id, LeaseStatus.HELD.value),
                    )
                    self._insert_event(
                        conn, lease_id, current.resource_id, "EXPIRED", now_ms,
                        LeaseStatus.HELD.value, LeaseStatus.EXPIRED.value,
                        "rotation rejected: ttl already elapsed",
                    )
                    current = self._row_to_lease(
                        conn.execute(
                            "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
                        ).fetchone()
                    )
                return None, current, "expired"

            new_version = current.credential_version + 1
            cur = conn.execute(
                "UPDATE leases SET credential_hash = ?, credential_version = ?, "
                "version = version + 1 "
                "WHERE lease_id = ? AND status = ? AND credential_hash = ? "
                "AND expires_at_ms > ?",
                (new_fingerprint, new_version, lease_id,
                 LeaseStatus.HELD.value, presented_fingerprint, now_ms),
            )
            if cur.rowcount != 1:
                # Lost a concurrent transition: re-read for an accurate reason.
                fresh = self._row_to_lease(
                    conn.execute(
                        "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
                    ).fetchone()
                )
                if fresh is None:
                    return None, None, "not_found"
                if not hmac.compare_digest(
                    fresh.credential_hash, presented_fingerprint
                ):
                    return None, fresh, "bad_credential"
                if fresh.status is LeaseStatus.RELEASED:
                    return None, fresh, "released"
                return None, fresh, "expired"
            rotated = self._row_to_lease(
                conn.execute(
                    "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
                ).fetchone()
            )
            # The resulting credential is stored sealed; the frozen snapshot
            # keeps the lease facts as of this exact rotation for replays.
            conn.execute(
                """
                INSERT INTO credential_rotations
                    (request_id, lease_id, resource_id, sequence_no,
                     credential_source, requested_fingerprint, new_fingerprint,
                     sealed_result_credential, rotated_at_ms, lease_snapshot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (request_id, lease_id, current.resource_id, new_version,
                 credential_source, presented_fingerprint, new_fingerprint,
                 sealed_new_credential, now_ms,
                 self._snapshot_json(rotated)),
            )
            self._insert_event(
                conn, lease_id, current.resource_id, "CREDENTIAL_ROTATED",
                now_ms, LeaseStatus.HELD.value, LeaseStatus.HELD.value,
                f"credential_version {current.credential_version} -> {new_version} "
                f"source={credential_source} request_id={request_id}",
            )
            return (
                {
                    "status": "rotated",
                    "credential": self._codec.open(sealed_new_credential),
                    "credential_version": new_version,
                    "request_id": request_id,
                    "rotated_at_ms": now_ms,
                },
                rotated,
                None,
            )

        return self._with_transaction(txn)

    # -- reads --------------------------------------------------------------

    def get_lease(self, lease_id: str) -> Lease | None:
        conn = self._get_conn()
        return self._row_to_lease(
            conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        )

    def list_leases(self, limit: int, cursor: str | None) -> LeasePage:
        """List leases in stable insertion (rowid) order with cursor paging."""
        after_rowid = 0 if cursor is None else decode_cursor(cursor)
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT rowid AS rid, * FROM leases WHERE rowid > ? ORDER BY rowid ASC LIMIT ?",
            (after_rowid, limit + 1),
        ).fetchall()
        items = [self._row_to_lease(r) for r in rows[:limit]]  # type: ignore[misc]
        next_cursor: str | None = None
        if len(rows) > limit and items:
            next_cursor = encode_cursor(int(rows[limit - 1]["rid"]))
        return LeasePage(items=items, next_cursor=next_cursor)

    def list_events_for_lease(self, lease_id: str, limit: int, cursor: str | None) -> EventPage:
        after_id = 0 if cursor is None else decode_cursor(cursor)
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM lease_events WHERE lease_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
            (lease_id, after_id, limit + 1),
        ).fetchall()
        items = [self._row_to_event(r) for r in rows[:limit]]
        next_cursor = encode_cursor(items[-1].event_id) if len(rows) > limit and items else None
        return EventPage(items=items, next_cursor=next_cursor)

    def list_events_for_resource(self, resource_id: str, limit: int, cursor: str | None) -> EventPage:
        after_id = 0 if cursor is None else decode_cursor(cursor)
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM lease_events WHERE resource_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
            (resource_id, after_id, limit + 1),
        ).fetchall()
        items = [self._row_to_event(r) for r in rows[:limit]]
        next_cursor = encode_cursor(items[-1].event_id) if len(rows) > limit and items else None
        return EventPage(items=items, next_cursor=next_cursor)

    def list_due_leases(self, now_ms: int, limit: int = 100) -> list[Lease]:
        """Return HELD leases whose expiry time has passed."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM leases WHERE status = ? AND expires_at_ms <= ? "
            "ORDER BY expires_at_ms ASC, fencing_token ASC LIMIT ?",
            (LeaseStatus.HELD.value, now_ms, limit),
        ).fetchall()
        return [self._row_to_lease(r) for r in rows]  # type: ignore[misc]
