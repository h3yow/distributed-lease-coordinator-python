"""Core domain model: lease status, value objects and clock protocol."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol


class LeaseStatus(str, Enum):
    """Lifecycle states of a lease.

    HELD      : currently held by a client and not yet expired.
    RELEASED  : explicitly released by the credential holder.
    EXPIRED   : ttl elapsed without renewal; lazily or actively reaped.
    """

    HELD = "HELD"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class Lease:
    """An immutable snapshot of one lease record."""

    lease_id: str
    resource_id: str
    holder: str
    credential: str
    fencing_token: int
    status: LeaseStatus
    acquired_at_ms: int
    expires_at_ms: int
    released_at_ms: int | None = None
    expire_reason: str | None = None
    version: int = 0
    credential_version: int = 0


class RotationStatus(str, Enum):
    """Outcome of one credential rotation request.

    ROTATED  : this call actually performed the credential swap.
    REPLAYED : the same request id had already completed; the previously
               generated credential is returned deterministically.
    """

    ROTATED = "ROTATED"
    REPLAYED = "REPLAYED"


@dataclass(frozen=True, slots=True)
class CredentialRotation:
    """Explicit result of a credential rotation request.

    The new credential is delivered *only* through this result (and its
    idempotent replays); ordinary lease snapshots and history events never
    carry credential material.
    """

    lease_id: str
    resource_id: str
    request_id: str
    status: RotationStatus
    credential: str
    credential_version: int
    rotated_at_ms: int
    lease: Lease


@dataclass(frozen=True, slots=True)
class LeaseEvent:
    """One append-only entry in the lease state transition history."""

    event_id: int
    lease_id: str
    resource_id: str
    event_type: str
    from_status: str | None
    to_status: str | None
    occurred_at_ms: int
    detail: str = ""


@dataclass(frozen=True, slots=True)
class LeasePage:
    """One cursor-paginated page of lease snapshots."""

    items: list[Lease]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class EventPage:
    """One cursor-paginated page of lease history events."""

    items: list[LeaseEvent]
    next_cursor: str | None


class Clock(Protocol):
    """Injectable clock so that all time-sensitive decisions are testable."""

    def now_ms(self) -> int:
        """Return the current epoch time in milliseconds."""
        ...


class SystemClock:
    """Wall clock used outside of tests."""

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


@dataclass(slots=True)
class FakeClock:
    """Deterministic manually-advanced clock used by tests."""

    current_ms: int = 0

    def now_ms(self) -> int:
        return self.current_ms

    def advance(self, delta_ms: int) -> int:
        """Advance the clock by delta_ms and return the new time."""
        self.current_ms += delta_ms
        return self.current_ms
