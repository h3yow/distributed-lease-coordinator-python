"""FastAPI HTTP 接口层。

端点：
- POST   /v1/leases           申请租约
- POST   /v1/leases/renew     续约（凭 token）
- POST   /v1/leases/release   释放（凭 token）
- POST   /v1/reap-expired     主动扫描回收过期租约
- GET    /v1/leases           游标分页列出全部资源租约（含终态）
- GET    /v1/leases/{rid}     查询单个资源当前租约投影
- GET    /v1/history          游标分页查询状态迁移历史（可按资源过滤）
"""

from __future__ import annotations

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from .errors import LeaseError
from .models import AcquireRequest, ReleaseRequest, RenewRequest
from .service import LeaseCoordinatorService

MAX_LIMIT = 200


def create_app(service: LeaseCoordinatorService) -> FastAPI:
    app = FastAPI(title="distributed-lease-coordinator", version="0.1.0")

    @app.exception_handler(LeaseError)
    async def lease_error_handler(request: Request, exc: LeaseError) -> JSONResponse:
        # 统一可解释错误体：错误码 + 人类可读原因 + 涉及资源
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.post("/v1/leases", status_code=201)
    def acquire(req: AcquireRequest) -> dict:
        lease = service.acquire(req.resource_id, req.holder, req.ttl_ms)
        return {"lease": _lease_with_token(lease, service.clock.now_ms())}

    @app.post("/v1/leases/renew")
    def renew(req: RenewRequest) -> dict:
        lease = service.renew(req.resource_id, req.token)
        return {"lease": _lease_with_token(lease, service.clock.now_ms())}

    @app.post("/v1/leases/release")
    def release(req: ReleaseRequest) -> dict:
        lease = service.release(req.resource_id, req.token)
        return {"lease": _lease_with_token(lease, service.clock.now_ms())}

    @app.post("/v1/reap-expired")
    def reap_expired(resource_id: str | None = None, limit: int = Query(default=100, ge=1, le=MAX_LIMIT)) -> dict:
        events = service.reap_expired(resource_id=resource_id, limit=limit)
        return {
            "reaped": len(events),
            "events": [e.to_dict() for e in events],
        }

    @app.get("/v1/leases")
    def list_leases(cursor: str | None = None, limit: int = Query(default=50, ge=1, le=MAX_LIMIT)) -> dict:
        page = service.list_leases(cursor, limit)
        return {
            "items": [l.to_dict() for l in page.items],  # 列表不回显 token
            "next_cursor": page.next_cursor,
        }

    @app.get("/v1/leases/{resource_id}")
    def get_lease(resource_id: str) -> dict:
        lease = service.get_lease(resource_id)
        if lease is None:
            return {"lease": None}
        # 查询接口不回显凭证；是否仍有效由 expire_at_ms + 当前服务时间共同解释
        now_ms = service.clock.now_ms()
        body = lease.to_dict()
        body["effective_active"] = lease.is_active(now_ms)
        body["server_now_ms"] = now_ms
        return {"lease": body}

    @app.get("/v1/history")
    def list_history(
        resource_id: str | None = None,
        cursor: str | None = None,
        limit: int = Query(default=50, ge=1, le=MAX_LIMIT),
    ) -> dict:
        page = service.list_history(resource_id, cursor, limit)
        return {
            "items": [e.to_dict() for e in page.items],  # 历史事件不回显 token
            "next_cursor": page.next_cursor,
        }

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True, "server_now_ms": service.clock.now_ms()}

    return app


def _lease_with_token(lease, now_ms: int) -> dict:
    """申请/续约/释放的响应回显一次凭证；之后凭证只能由持有者自己保存。"""
    body = lease.to_dict()
    body["token"] = lease.token
    body["effective_active"] = lease.is_active(now_ms)
    body["server_now_ms"] = now_ms
    return body
