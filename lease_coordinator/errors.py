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


class RotationConflictError(LeaseError):
    """A rotation id is already committed with different request parameters.

    This covers the *ambiguous retry* case: the same client supplied
    ``rotation_id`` before but with a different lease / presented
    credential, so the server cannot tell whether the caller retried
    wrongly or reused an id by mistake. Nothing is mutated.
    """

    code = "rotation_conflict"


class InvalidRotationError(LeaseError):
    """The rotation request itself is malformed (e.g. identical credentials)."""

    code = "invalid_rotation"
