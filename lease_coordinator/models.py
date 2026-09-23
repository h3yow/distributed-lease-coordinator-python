"""领域模型：租约状态、事件、请求/响应 DTO。"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, Field


class LeaseStatus(str, Enum):
    """租约状态机：ACTIVE -> RELEASED / EXPIRED（终态不可逆）。"""

    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class LeaseEvent:
    """追加到日志的一次不可变状态迁移事件。"""

    seq: int
    event_id: str
    event_type: str  # ACQUIRED / RENEWED / RELEASED / EXPIRED
    resource_id: str
    lease_id: str
    at_ms: int  # 事件提交时刻（单调毫秒）
    version: int  # 该资源在事件后的版本号
    ttl_ms: int
    expire_at_ms: int
    holder: str
    token: str
    fence_token: int
    detail: str = ""

    def to_dict(self) -> dict:
        d = {
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "resource_id": self.resource_id,
            "lease_id": self.lease_id,
            "at_ms": self.at_ms,
            "version": self.version,
            "ttl_ms": self.ttl_ms,
            "expire_at_ms": self.expire_at_ms,
            "holder": self.holder,
            "fence_token": self.fence_token,
            "detail": self.detail,
        }
        return d


@dataclass(slots=True)
class Lease:
    """某个资源当前最新的租约投影（由事件回放得到）。"""

    resource_id: str
    lease_id: str
    holder: str
    token: str
    fence_token: int
    status: LeaseStatus
    version: int
    ttl_ms: int
    acquired_at_ms: int
    expire_at_ms: int
    last_event_seq: int

    def is_active(self, now_ms: int) -> bool:
        """有效 = 状态为 ACTIVE 且当前时刻未到 expire_at_ms（边界：到期时刻即失效）。"""
        return self.status is LeaseStatus.ACTIVE and now_ms < self.expire_at_ms

    def to_dict(self) -> dict:
        return {
            "resource_id": self.resource_id,
            "lease_id": self.lease_id,
            "holder": self.holder,
            "status": self.status.value,
            "version": self.version,
            "fence_token": self.fence_token,
            "ttl_ms": self.ttl_ms,
            "acquired_at_ms": self.acquired_at_ms,
            "expire_at_ms": self.expire_at_ms,
            "last_event_seq": self.last_event_seq,
        }


@dataclass(slots=True)
class HistoryPage:
    """游标分页结果。"""

    items: list[LeaseEvent]
    next_cursor: str | None


@dataclass(slots=True)
class LeasePage:
    """租约列表的游标分页结果。"""

    items: list[Lease]
    next_cursor: str | None


class Clock:
    """时间源：毫秒时间戳，生产用系统时钟，测试可注入。"""

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


# ---- HTTP DTO ----


class AcquireRequest(BaseModel):
    resource_id: str = Field(min_length=1, max_length=512)
    holder: str = Field(min_length=1, max_length=256)
    ttl_ms: int = Field(gt=0, le=24 * 3600 * 1000)


class RenewRequest(BaseModel):
    resource_id: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1, max_length=256)


class ReleaseRequest(BaseModel):
    resource_id: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1, max_length=256)
