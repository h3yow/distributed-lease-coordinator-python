"""Pydantic request/response models for the HTTP layer."""

from __future__ import annotations

from pydantic import BaseModel, Field

from .model import LeaseStatus


class AcquireRequest(BaseModel):
    resource_id: str = Field(min_length=1)
    holder: str = Field(min_length=1)
    ttl_ms: int = Field(gt=0)
    credential: str | None = None


class RenewRequest(BaseModel):
    credential: str = Field(min_length=1)
    ttl_ms: int = Field(gt=0)


class ReleaseRequest(BaseModel):
    credential: str = Field(min_length=1)


class LeaseView(BaseModel):
    lease_id: str
    resource_id: str
    holder: str
    fencing_token: int
    status: LeaseStatus
    acquired_at_ms: int
    expires_at_ms: int
    released_at_ms: int | None = None
    expire_reason: str | None = None
    version: int


class LeasePageView(BaseModel):
    items: list[LeaseView]
    next_cursor: str | None


class EventView(BaseModel):
    event_id: int
    lease_id: str
    resource_id: str
    event_type: str
    from_status: str | None
    to_status: str | None
    occurred_at_ms: int
    detail: str


class EventPageView(BaseModel):
    items: list[EventView]
    next_cursor: str | None


class ErrorView(BaseModel):
    error: str
    code: str
    detail: str
