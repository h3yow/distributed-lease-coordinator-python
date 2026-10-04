"""Domain service orchestrating storage, the clock and failure reasons.

Every public method logs the *input* it received and the *basis* of its
decision (current status, expiry time, credential match), so test logs
explain why a request was granted or rejected.
"""

from __future__ import annotations

import logging
import secrets
import threading
from pathlib import Path

from .errors import (
    InvalidCredentialError,
    InvalidCursorError,
    LeaseConflictError,
    LeaseError,
    LeaseExpiredError,
    LeaseNotFoundError,
    LeaseReleasedError,
    RotationConflictError,
    RotationRequestIdRequiredError,
)
from .ids import IdGenerator, UuidLeaseIdGenerator
from .model import (
    Clock,
    CredentialRotation,
    EventPage,
    Lease,
    LeasePage,
    RotationStatus,
    SystemClock,
)
from .storage import LeaseStorage

logger = logging.getLogger("lease_coordinator")

_REASON_TO_ERROR: dict[str, type[LeaseError]] = {
    "not_found": LeaseNotFoundError,
    "bad_credential": InvalidCredentialError,
    "released": LeaseReleasedError,
    "expired": LeaseExpiredError,
}


class LeaseService:
    """Application service for acquire / renew / release / expiry / query."""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        clock: Clock | None = None,
        id_generator: IdGenerator | None = None,
        storage: LeaseStorage | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._ids = id_generator or UuidLeaseIdGenerator()
        self._storage = storage or LeaseStorage(db_path)
        self._reaper_thread: threading.Thread | None = None
        self._reaper_stop = threading.Event()
        logger.info(
            "service initialized db=%s clock=%s",
            db_path, type(self._clock).__name__,
        )

    # -- lifecycle ----------------------------------------------------------

    def start_reaper(self, interval_ms: int = 100, batch_size: int = 100) -> None:
        """Start the background expiry-reaping thread (idempotent)."""
        if self._reaper_thread is not None and self._reaper_thread.is_alive():
            return
        self._reaper_stop.clear()

        def run() -> None:
            logger.info("reaper started interval_ms=%s batch_size=%s", interval_ms, batch_size)
            while not self._reaper_stop.wait(interval_ms / 1000.0):
                try:
                    expired = self.expire_due_leases(batch_size=batch_size)
                    if expired:
                        logger.info("reaper expired %s lease(s)", len(expired))
                except Exception:
                    logger.exception("reaper iteration failed")
            logger.info("reaper stopped")

        self._reaper_thread = threading.Thread(
            target=run, name="lease-reaper", daemon=True
        )
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        self._reaper_stop.set()
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=2.0)
            self._reaper_thread = None

    def close(self) -> None:
        self.stop_reaper()
        self._storage.close()

    def __enter__(self) -> LeaseService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- helpers ------------------------------------------------------------

    def _raise_for_reason(self, reason: str, lease: Lease | None, action: str) -> None:
        if reason is None:
            return
        if reason == "not_found":
            logger.warning("%s rejected: lease does not exist", action)
            raise LeaseNotFoundError("lease does not exist")
        assert lease is not None
        basis = (
            f"status={lease.status.value} expires_at_ms={lease.expires_at_ms} "
            f"credential_match={'yes' if reason != 'bad_credential' else 'no'}"
        )
        logger.warning("%s rejected for lease=%s reason=%s (%s)",
                       action, lease.lease_id, reason, basis)
        if reason == "bad_credential":
            raise InvalidCredentialError(
                f"credential does not match holder of lease {lease.lease_id}"
            )
        if reason == "released":
            raise LeaseReleasedError(f"lease {lease.lease_id} was already released")
        if reason == "expired":
            raise LeaseExpiredError(f"lease {lease.lease_id} has expired")
        raise LeaseError(f"unknown failure reason: {reason}")

    # -- core operations ----------------------------------------------------

    def acquire(
        self,
        resource_id: str,
        holder: str,
        ttl_ms: int,
        credential: str | None = None,
    ) -> Lease:
        """Acquire a lease; raises a typed LeaseError on rejection."""
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")
        now_ms = self._clock.now_ms()
        lease_id = self._ids.new_lease_id()
        credential = credential or secrets.token_hex(16)
        expires_at_ms = now_ms + ttl_ms
        logger.info(
            "acquire request resource=%s holder=%s lease_id=%s ttl_ms=%s now_ms=%s",
            resource_id, holder, lease_id, ttl_ms, now_ms,
        )
        fencing_token, conflict = self._storage.atomic_acquire(
            resource_id, lease_id, holder, credential, now_ms, expires_at_ms
        )
        if conflict is not None:
            logger.warning(
                "acquire rejected resource=%s conflict_lease=%s holder=%s "
                "status=%s expires_at_ms=%s (> now_ms=%s)",
                resource_id, conflict.lease_id, conflict.holder,
                conflict.status.value, conflict.expires_at_ms, now_ms,
            )
            raise LeaseConflictError(
                f"resource {resource_id} is held by lease {conflict.lease_id} "
                f"until {conflict.expires_at_ms}"
            )
        lease = self._storage.get_lease(lease_id)
        assert lease is not None
        logger.info(
            "acquire granted resource=%s lease_id=%s fencing_token=%s expires_at_ms=%s",
            resource_id, lease_id, fencing_token, expires_at_ms,
        )
        return lease

    def renew(self, lease_id: str, credential: str, ttl_ms: int) -> Lease:
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")
        now_ms = self._clock.now_ms()
        expires_at_ms = now_ms + ttl_ms
        logger.info(
            "renew request lease_id=%s ttl_ms=%s now_ms=%s credential_len=%s",
            lease_id, ttl_ms, now_ms, len(credential),
        )
        lease, reason = self._storage.atomic_renew(
            lease_id, credential, now_ms, expires_at_ms
        )
        self._raise_for_reason(reason, lease, "renew")
        assert lease is not None
        logger.info(
            "renew granted lease_id=%s version=%s new_expires_at_ms=%s",
            lease_id, lease.version, expires_at_ms,
        )
        return lease

    def release(self, lease_id: str, credential: str) -> Lease:
        now_ms = self._clock.now_ms()
        logger.info(
            "release request lease_id=%s now_ms=%s credential_len=%s",
            lease_id, now_ms, len(credential),
        )
        lease, reason = self._storage.atomic_release(lease_id, credential, now_ms)
        self._raise_for_reason(reason, lease, "release")
        assert lease is not None
        logger.info("release granted lease_id=%s", lease_id)
        return lease

    def rotate_credential(
        self,
        lease_id: str,
        credential: str,
        new_credential: str | None = None,
        request_id: str | None = None,
    ) -> CredentialRotation:
        """Rotate the holder credential of a not-yet-expired lease.

        On success the old credential is invalid immediately and the new
        credential is returned *only* in this result. The lease keeps the
        same lease id, fencing token and expiry time; only the credential
        (and its version) changes.

        ``request_id`` is the idempotency key and is **mandatory**. Retries
        and concurrent duplicates carrying the same key and parameters
        converge to one rotation: the first call is ``ROTATED``, later ones
        are ``REPLAYED`` and hand back the same credential and the original
        rotation timestamp. Reusing a key for a different request raises
        :class:`RotationConflictError`.

        A call without ``request_id`` is rejected with
        :class:`RotationRequestIdRequiredError` *before* any state is
        inspected or changed (not even a lazy expiry is triggered). This
        guarantees there is no window where the old credential is already
        dead but the caller — whose response was lost — cannot recover the
        new credential, because nothing was ever applied.
        """
        if not request_id or not request_id.strip():
            logger.warning(
                "rotate rejected lease_id=%s: missing request_id "
                "(no state inspected or changed)", lease_id,
            )
            raise RotationRequestIdRequiredError(
                "credential rotation requires a caller-supplied request_id "
                "so a lost response can be recovered by retrying with the "
                "same idempotency key"
            )
        if not credential:
            raise ValueError("credential must be a non-empty string")
        if new_credential is not None and not new_credential:
            raise ValueError("new_credential must be a non-empty string when given")
        now_ms = self._clock.now_ms()
        logger.info(
            "rotate request lease_id=%s request_id=%s now_ms=%s "
            "credential_len=%s new_credential_source=%s",
            lease_id, request_id, now_ms, len(credential),
            "caller" if new_credential is not None else "server",
        )
        outcome, lease, reason = self._storage.atomic_rotate_credential(
            lease_id, credential, new_credential, request_id, now_ms
        )
        if reason is not None:
            self._raise_for_rotation_reason(reason, lease, lease_id, request_id)
        assert outcome is not None and lease is not None
        status = RotationStatus(str(outcome["status"]).upper())
        result = CredentialRotation(
            lease_id=lease_id,
            resource_id=lease.resource_id,
            request_id=str(outcome["request_id"]),
            status=status,
            credential=str(outcome["credential"]),
            credential_version=int(outcome["credential_version"]),
            rotated_at_ms=int(outcome["rotated_at_ms"]),
            lease=lease,
        )
        logger.info(
            "rotate %s lease_id=%s request_id=%s credential_version=%s "
            "lease_expires_at_ms=%s lease_status=%s",
            status.value.lower(), lease_id, result.request_id,
            result.credential_version, lease.expires_at_ms,
            lease.status.value,
        )
        return result

    def _raise_for_rotation_reason(
        self,
        reason: str,
        lease: Lease | None,
        lease_id: str,
        request_id: str,
    ) -> None:
        if reason == "not_found":
            logger.warning(
                "rotate rejected lease_id=%s request_id=%s: lease does not exist",
                lease_id, request_id,
            )
            raise LeaseNotFoundError("lease does not exist")
        if reason == "request_conflict":
            logger.warning(
                "rotate rejected lease_id=%s request_id=%s: request id was "
                "already used with different parameters",
                lease_id, request_id,
            )
            raise RotationConflictError(
                f"request id {request_id} was already used for a different "
                f"rotation request"
            )
        if reason == "same_credential":
            raise ValueError(
                "new credential must differ from the current credential"
            )
        if reason == "legacy_replay":
            logger.warning(
                "rotate rejected lease_id=%s request_id=%s: rotation record "
                "predates deterministic credentials and cannot be replayed",
                lease_id, request_id,
            )
            raise RotationConflictError(
                f"rotation {request_id} was recorded by an older release "
                f"that stored credentials directly; its result cannot be "
                f"reconstructed, please rotate again with a new request id"
            )
        # Other reasons share the lease-scoped decision vocabulary.
        self._raise_for_reason(reason, lease, "rotate")

    def expire_due_leases(self, batch_size: int = 100) -> list[Lease]:
        """Actively expire every due lease; each lease expires at most once."""
        now_ms = self._clock.now_ms()
        due = self._storage.list_due_leases(now_ms, limit=batch_size)
        expired: list[Lease] = []
        for candidate in due:
            lease = self._storage.atomic_expire(
                candidate.lease_id, now_ms, "ttl_elapsed_reaper"
            )
            if lease is not None:
                expired.append(lease)
                logger.info(
                    "lease expired lease_id=%s resource=%s expires_at_ms=%s (<= now_ms=%s)",
                    lease.lease_id, lease.resource_id,
                    candidate.expires_at_ms, now_ms,
                )
        return expired

    # -- queries ------------------------------------------------------------

    def get_lease(self, lease_id: str) -> Lease:
        lease = self._storage.get_lease(lease_id)
        if lease is None:
            logger.warning("get_lease rejected: lease_id=%s not found", lease_id)
            raise LeaseNotFoundError(f"lease {lease_id} does not exist")
        logger.info(
            "get_lease lease_id=%s status=%s expires_at_ms=%s version=%s",
            lease_id, lease.status.value, lease.expires_at_ms, lease.version,
        )
        return lease

    def _safe_cursor(self, cursor: str | None) -> str | None:
        if cursor is None:
            return None
        from .storage import decode_cursor

        try:
            decode_cursor(cursor)
        except ValueError as exc:
            raise InvalidCursorError(str(exc)) from exc
        return cursor

    def list_leases(self, limit: int = 50, cursor: str | None = None) -> LeasePage:
        cursor = self._safe_cursor(cursor)
        page = self._storage.list_leases(limit, cursor)
        logger.info(
            "list_leases limit=%s returned=%s next_cursor=%s",
            limit, len(page.items), bool(page.next_cursor),
        )
        return page

    def list_events_for_lease(
        self, lease_id: str, limit: int = 50, cursor: str | None = None
    ) -> EventPage:
        cursor = self._safe_cursor(cursor)
        page = self._storage.list_events_for_lease(lease_id, limit, cursor)
        logger.info(
            "list_events_for_lease lease_id=%s returned=%s next_cursor=%s",
            lease_id, len(page.items), bool(page.next_cursor),
        )
        return page

    def list_events_for_resource(
        self, resource_id: str, limit: int = 50, cursor: str | None = None
    ) -> EventPage:
        cursor = self._safe_cursor(cursor)
        page = self._storage.list_events_for_resource(resource_id, limit, cursor)
        logger.info(
            "list_events_for_resource resource=%s returned=%s next_cursor=%s",
            resource_id, len(page.items), bool(page.next_cursor),
        )
        return page
