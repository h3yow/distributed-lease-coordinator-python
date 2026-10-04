"""FastAPI application exposing the lease coordinator over HTTP."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

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
from .model import EventPage, Lease, LeaseEvent, LeasePage
from .schema import (
    AcquireRequest,
    EventPageView,
    EventView,
    LeasePageView,
    LeaseView,
    ReleaseRequest,
    RenewRequest,
    RotateRequest,
    RotationView,
)
from .service import LeaseService

# Error codes are part of the public contract so clients can distinguish
# rejection reasons (409 conflict family, 404 missing, 403 wrong credential,
# 400 bad cursor).
_ERROR_STATUS: dict[type[LeaseError], int] = {
    LeaseNotFoundError: 404,
    LeaseConflictError: 409,
    InvalidCredentialError: 403,
    LeaseExpiredError: 409,
    LeaseReleasedError: 409,
    InvalidCursorError: 400,
    RotationConflictError: 409,
    RotationRequestIdRequiredError: 400,
}


def _lease_view(lease: Lease) -> LeaseView:
    return LeaseView(
        lease_id=lease.lease_id,
        resource_id=lease.resource_id,
        holder=lease.holder,
        fencing_token=lease.fencing_token,
        status=lease.status,
        acquired_at_ms=lease.acquired_at_ms,
        expires_at_ms=lease.expires_at_ms,
        released_at_ms=lease.released_at_ms,
        expire_reason=lease.expire_reason,
        version=lease.version,
        credential_version=lease.credential_version,
    )


def _rotation_view(rotation) -> RotationView:
    return RotationView(
        lease_id=rotation.lease_id,
        resource_id=rotation.resource_id,
        request_id=rotation.request_id,
        status=rotation.status.value,
        credential=rotation.credential,
        credential_version=rotation.credential_version,
        rotated_at_ms=rotation.rotated_at_ms,
    )


def _event_view(event: LeaseEvent) -> EventView:
    return EventView(
        event_id=event.event_id,
        lease_id=event.lease_id,
        resource_id=event.resource_id,
        event_type=event.event_type,
        from_status=event.from_status,
        to_status=event.to_status,
        occurred_at_ms=event.occurred_at_ms,
        detail=event.detail,
    )


def _lease_page_view(page: LeasePage) -> LeasePageView:
    return LeasePageView(
        items=[_lease_view(i) for i in page.items],
        next_cursor=page.next_cursor,
    )


def _event_page_view(page: EventPage) -> EventPageView:
    return EventPageView(
        items=[_event_view(i) for i in page.items],
        next_cursor=page.next_cursor,
    )


def create_app(service: LeaseService) -> FastAPI:
    """Build a FastAPI application bound to the given lease service."""
    application = FastAPI(title="distributed-lease-coordinator", version="0.1.0")
    application.state.service = service

    @application.exception_handler(LeaseError)
    async def handle_lease_error(_request: Request, exc: LeaseError) -> JSONResponse:
        status_code = _ERROR_STATUS.get(type(exc), 422)
        return JSONResponse(
            status_code=status_code,
            content={
                "error": type(exc).__name__,
                "code": exc.code,
                "detail": str(exc),
            },
        )

    @application.exception_handler(ValueError)
    async def handle_value_error(_request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "error": "ValueError",
                "code": "invalid_request",
                "detail": str(exc),
            },
        )

    @application.post("/leases", response_model=LeaseView, status_code=201)
    def acquire_lease(body: AcquireRequest) -> LeaseView:
        lease = service.acquire(
            resource_id=body.resource_id,
            holder=body.holder,
            ttl_ms=body.ttl_ms,
            credential=body.credential,
        )
        return _lease_view(lease)

    @application.get("/leases", response_model=LeasePageView)
    def list_leases(limit: int = 50, cursor: str | None = None) -> LeasePageView:
        return _lease_page_view(service.list_leases(limit=limit, cursor=cursor))

    @application.get("/leases/{lease_id}", response_model=LeaseView)
    def read_lease(lease_id: str) -> LeaseView:
        return _lease_view(service.get_lease(lease_id))

    @application.post("/leases/{lease_id}/renew", response_model=LeaseView)
    def renew_lease(lease_id: str, body: RenewRequest) -> LeaseView:
        return _lease_view(service.renew(lease_id, body.credential, body.ttl_ms))

    @application.post("/leases/{lease_id}/release", response_model=LeaseView)
    def release_lease(lease_id: str, body: ReleaseRequest) -> LeaseView:
        return _lease_view(service.release(lease_id, body.credential))

    @application.post("/leases/{lease_id}/rotate", response_model=RotationView)
    def rotate_credential(lease_id: str, body: RotateRequest) -> RotationView:
        rotation = service.rotate_credential(
            lease_id=lease_id,
            credential=body.credential,
            new_credential=body.new_credential,
            request_id=body.request_id,
        )
        return _rotation_view(rotation)

    @application.post("/maintenance/expire-due")
    def expire_due() -> dict[str, int]:
        expired = service.expire_due_leases()
        return {"expired_count": len(expired)}

    @application.get("/leases/{lease_id}/events", response_model=EventPageView)
    def lease_events(
        lease_id: str, limit: int = 50, cursor: str | None = None
    ) -> EventPageView:
        service.get_lease(lease_id)  # 404 early if the lease does not exist
        page = service.list_events_for_lease(lease_id, limit=limit, cursor=cursor)
        return _event_page_view(page)

    @application.get("/resources/{resource_id}/events", response_model=EventPageView)
    def resource_events(
        resource_id: str, limit: int = 50, cursor: str | None = None
    ) -> EventPageView:
        page = service.list_events_for_resource(resource_id, limit=limit, cursor=cursor)
        return _event_page_view(page)

    return application
