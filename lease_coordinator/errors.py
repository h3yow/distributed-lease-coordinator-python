"""Distinguishable failure reasons for lease operations."""

from __future__ import annotations


class LeaseError(Exception):
    """Base class for all lease related failures."""

    code: str = "lease_error"


class LeaseNotFoundError(LeaseError):
    """The referenced lease id does not exist."""

    code = "lease_not_found"


class LeaseConflictError(LeaseError):
    """The resource is currently held by another valid lease."""

    code = "lease_conflict"


class InvalidCredentialError(LeaseError):
    """Holder credential does not match the lease record."""

    code = "invalid_credential"


class LeaseExpiredError(LeaseError):
    """The lease has passed its expiry time and is no longer valid."""

    code = "lease_expired"


class LeaseReleasedError(LeaseError):
    """The lease has already been released."""

    code = "lease_released"


class InvalidCursorError(LeaseError):
    """The pagination cursor is malformed or points to a deleted position."""

    code = "invalid_cursor"


class RotationRequestIdRequiredError(LeaseError):
    """A credential rotation was attempted without a caller idempotency key.

    Rotations without a caller-supplied ``request_id`` are rejected *before*
    any state is inspected or changed: if the response were lost after a
    server-side key assignment, the caller could never recover the result
    and the old credential would already be dead.
    """

    code = "rotation_request_id_required"


class RotationConflictError(LeaseError):
    """A rotation request id was reused with different parameters.

    The same idempotency key must always carry the same target lease and
    the same caller-supplied credential. Replaying the original request
    (identical parameters) is safe; reusing the key for a *different*
    request is rejected explicitly rather than silently applied.
    """

    code = "rotation_conflict"
