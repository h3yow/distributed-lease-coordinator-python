"""Tests for acquire / renew / release and distinguishable failure reasons."""

from __future__ import annotations

import pytest

from lease_coordinator.errors import (
    InvalidCredentialError,
    LeaseConflictError,
    LeaseExpiredError,
    LeaseNotFoundError,
    LeaseReleasedError,
)
from lease_coordinator.model import LeaseStatus
from lease_coordinator.service import LeaseService


def test_acquire_delivers_credential_once_and_snapshot_has_no_credential(
    service: LeaseService,
) -> None:
    result = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    lease = result.lease

    assert lease.status is LeaseStatus.HELD
    assert lease.holder == "writer-A"
    # The plaintext credential is delivered only through the acquire result.
    assert len(result.credential) == 32  # secrets.token_hex(16)
    assert not hasattr(lease, "credential")
    assert len(lease.credential_hash) == 64
    assert lease.fencing_token == 1
    assert lease.expires_at_ms == lease.acquired_at_ms + 10_000
    assert lease.version == 0
    # Later queries repeat the facts, never the credential.
    again = service.get_lease(lease.lease_id)
    assert not hasattr(again, "credential")
    assert again.credential_hash == lease.credential_hash


def test_second_concurrent_acquire_is_rejected_with_conflict(service: LeaseService) -> None:
    first_res = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    first = first_res.lease

    with pytest.raises(LeaseConflictError) as exc_info:
        service.acquire("doc-1", "writer-B", ttl_ms=10_000)

    assert exc_info.value.code == "lease_conflict"
    assert first.lease_id in str(exc_info.value)
    # The rejected request must not have created a lease row.
    assert service.list_leases().items[0].lease_id == first.lease_id


def test_renew_with_wrong_credential_is_rejected(service: LeaseService) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000).lease

    with pytest.raises(InvalidCredentialError) as exc_info:
        service.renew(lease.lease_id, "forged-credential", ttl_ms=10_000)

    assert exc_info.value.code == "invalid_credential"
    # Expiry must be unchanged after the rejected renewal.
    assert service.get_lease(lease.lease_id).expires_at_ms == lease.expires_at_ms


def test_renew_valid_lease_extends_expiry_and_bumps_version(service: LeaseService, clock) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    lease, credential = acquired.lease, acquired.credential
    original_expiry = lease.expires_at_ms

    clock.advance(3_000)
    renewed = service.renew(lease.lease_id, credential, ttl_ms=10_000)

    assert renewed.status is LeaseStatus.HELD
    assert renewed.expires_at_ms == clock.now_ms() + 10_000
    assert renewed.expires_at_ms > original_expiry
    assert renewed.version == 1


def test_renew_after_expiry_is_rejected_as_expired(service: LeaseService, clock) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=1_000)
    lease, credential = acquired.lease, acquired.credential
    clock.advance(1_001)

    with pytest.raises(LeaseExpiredError) as exc_info:
        service.renew(lease.lease_id, credential, ttl_ms=1_000)

    assert exc_info.value.code == "lease_expired"
    # Lazy expiry transition must be observable afterwards.
    assert service.get_lease(lease.lease_id).status is LeaseStatus.EXPIRED


def test_release_with_wrong_credential_is_rejected(service: LeaseService) -> None:
    lease = service.acquire("doc-1", "writer-A", ttl_ms=10_000).lease

    with pytest.raises(InvalidCredentialError):
        service.release(lease.lease_id, "attacker")

    assert service.get_lease(lease.lease_id).status is LeaseStatus.HELD


def test_release_then_second_release_is_rejected_as_released(service: LeaseService) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    lease, credential = acquired.lease, acquired.credential

    released = service.release(lease.lease_id, credential)
    assert released.status is LeaseStatus.RELEASED
    assert released.released_at_ms is not None

    with pytest.raises(LeaseReleasedError) as exc_info:
        service.release(lease.lease_id, credential)
    assert exc_info.value.code == "lease_released"


def test_release_after_expiry_is_rejected_as_expired(service: LeaseService, clock) -> None:
    acquired = service.acquire("doc-1", "writer-A", ttl_ms=1_000)
    lease, credential = acquired.lease, acquired.credential
    clock.advance(1_001)

    with pytest.raises(LeaseExpiredError):
        service.release(lease.lease_id, credential)


def test_operations_on_unknown_lease_return_not_found(service: LeaseService) -> None:
    with pytest.raises(LeaseNotFoundError):
        service.renew("lease-404", "c", ttl_ms=100)
    with pytest.raises(LeaseNotFoundError):
        service.release("lease-404", "c")
    with pytest.raises(LeaseNotFoundError):
        service.get_lease("lease-404")


def test_released_resource_can_be_reacquired_with_monotonic_fencing_token(
    service: LeaseService,
) -> None:
    first = service.acquire("doc-1", "writer-A", ttl_ms=10_000)
    service.release(first.lease.lease_id, first.credential)

    second = service.acquire("doc-1", "writer-B", ttl_ms=10_000).lease

    assert second.fencing_token == first.lease.fencing_token + 1
    assert second.lease_id != first.lease.lease_id


def test_invalid_ttl_is_rejected(service: LeaseService) -> None:
    with pytest.raises(ValueError):
        service.acquire("doc-1", "writer-A", ttl_ms=0)
