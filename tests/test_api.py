"""HTTP API 端到端测试：错误码/状态码映射、分页、回收与历史链路。

env 夹具同时暴露 TestClient、service 与 FakeClock，测试可推进时钟
以确定性地触发过期，再通过 HTTP 验证判定结果。
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from lease_coordinator.api import create_app
from lease_coordinator.service import LeaseCoordinatorService
from lease_coordinator.store import EventStore


class Env:
    def __init__(self, client: TestClient, service: LeaseCoordinatorService, clock) -> None:
        self.client = client
        self.service = service
        self.clock = clock


@pytest.fixture
def env(tmp_path, clock):
    store = EventStore(tmp_path / "api-events.log")
    store.start()
    svc = LeaseCoordinatorService(store, clock, sleep_fn=lambda _s: None)
    yield Env(TestClient(create_app(svc)), svc, clock)
    store.close()


def test_full_lifecycle_over_http(env):
    r = env.client.post("/v1/leases", json={"resource_id": "db-table-1", "holder": "job-A", "ttl_ms": 5000})
    assert r.status_code == 201
    body = r.json()["lease"]
    assert body["status"] == "ACTIVE"
    assert body["fence_token"] == 1
    assert body["effective_active"] is True
    token = body["token"]

    # 列表/查询不回显凭证
    listed = env.client.get("/v1/leases").json()["items"]
    assert listed[0]["resource_id"] == "db-table-1"
    assert "token" not in listed[0]
    got = env.client.get("/v1/leases/db-table-1").json()["lease"]
    assert "token" not in got and got["effective_active"] is True

    assert env.client.post(
        "/v1/leases/renew", json={"resource_id": "db-table-1", "token": token}
    ).status_code == 200
    released = env.client.post(
        "/v1/leases/release", json={"resource_id": "db-table-1", "token": token}
    ).json()["lease"]
    assert released["status"] == "RELEASED"


def test_conflict_status_code_and_body(env):
    env.client.post("/v1/leases", json={"resource_id": "r1", "holder": "a", "ttl_ms": 5000})
    r = env.client.post("/v1/leases", json={"resource_id": "r1", "holder": "b", "ttl_ms": 5000})
    assert r.status_code == 409
    assert r.json()["error"] == "resource_held"
    assert r.json()["resource_id"] == "r1"
    assert "a" in r.json()["message"]  # 可解释：被谁持有


def test_distinguishable_failure_reasons_over_http(env):
    # 不存在 -> 404 lease_not_found
    r = env.client.post("/v1/leases/renew", json={"resource_id": "ghost", "token": "t"})
    assert r.status_code == 404 and r.json()["error"] == "lease_not_found"

    tok = env.client.post(
        "/v1/leases", json={"resource_id": "r", "holder": "h", "ttl_ms": 1000}
    ).json()["lease"]["token"]

    # 凭证错误 -> 403 invalid_token
    r = env.client.post("/v1/leases/renew", json={"resource_id": "r", "token": tok + "x"})
    assert r.status_code == 403 and r.json()["error"] == "invalid_token"

    # 释放后续约 -> 410 lease_released
    env.client.post("/v1/leases/release", json={"resource_id": "r", "token": tok})
    r = env.client.post("/v1/leases/renew", json={"resource_id": "r", "token": tok})
    assert r.status_code == 410 and r.json()["error"] == "lease_released"


def test_expired_lease_http_code_distinguishable_from_released(env):
    """同样 410，error 字段必须区分 lease_expired 与 lease_released。"""
    tok = env.client.post(
        "/v1/leases", json={"resource_id": "e", "holder": "h", "ttl_ms": 1000}
    ).json()["lease"]["token"]
    env.clock.advance(2000)  # 推进确定性时钟，租约到期

    r = env.client.post("/v1/leases/renew", json={"resource_id": "e", "token": tok})
    assert r.status_code == 410 and r.json()["error"] == "lease_expired"

    # 过期后查询：状态已被惰性标记为 EXPIRED，且 effective_active 为 False
    got = env.client.get("/v1/leases/e").json()["lease"]
    assert got["status"] == "EXPIRED"
    assert got["effective_active"] is False


def test_reap_endpoint_collects_and_is_idempotent(env):
    for rid in ("a", "b"):
        env.client.post("/v1/leases", json={"resource_id": rid, "holder": "h", "ttl_ms": 1000})
    env.client.post("/v1/leases", json={"resource_id": "c", "holder": "h", "ttl_ms": 60_000})
    env.clock.advance(2000)

    r1 = env.client.post("/v1/reap-expired")
    assert r1.status_code == 200 and r1.json()["reaped"] == 2
    assert sorted(e["resource_id"] for e in r1.json()["events"]) == ["a", "b"]
    assert all(e["event_type"] == "EXPIRED" for e in r1.json()["events"])
    assert all("token" not in e for e in r1.json()["events"])

    r2 = env.client.post("/v1/reap-expired")
    assert r2.json()["reaped"] == 0  # 幂等

    # 回收后历史完整可查，c 仍有效
    hist = env.client.get("/v1/history").json()["items"]
    assert [e["event_type"] for e in hist] == [
        "ACQUIRED", "ACQUIRED", "ACQUIRED", "EXPIRED", "EXPIRED",
    ]
    assert env.client.get("/v1/leases/c").json()["lease"]["effective_active"] is True


def test_history_and_pagination_over_http(env):
    for i in range(3):
        env.client.post("/v1/leases", json={"resource_id": f"k{i}", "holder": "h", "ttl_ms": 10_000})

    page = env.client.get("/v1/leases", params={"limit": 2}).json()
    assert [x["resource_id"] for x in page["items"]] == ["k0", "k1"]
    assert page["next_cursor"]

    page2 = env.client.get("/v1/leases", params={"limit": 2, "cursor": page["next_cursor"]}).json()
    assert [x["resource_id"] for x in page2["items"]] == ["k2"]
    assert page2["next_cursor"] is None

    hist_page = env.client.get("/v1/history", params={"limit": 2}).json()
    assert [e["seq"] for e in hist_page["items"]] == [1, 2]
    hist_page2 = env.client.get(
        "/v1/history", params={"limit": 2, "cursor": hist_page["next_cursor"]}
    ).json()
    assert [e["seq"] for e in hist_page2["items"]] == [3]

    # 按资源过滤
    k1_hist = env.client.get("/v1/history", params={"resource_id": "k1"}).json()["items"]
    assert len(k1_hist) == 1 and k1_hist[0]["resource_id"] == "k1"


def test_request_validation_rejects_bad_inputs(env):
    assert env.client.post(
        "/v1/leases", json={"resource_id": "", "holder": "h", "ttl_ms": 1}
    ).status_code == 422
    assert env.client.post(
        "/v1/leases", json={"resource_id": "r", "holder": "h", "ttl_ms": 0}
    ).status_code == 422
    assert env.client.post("/v1/leases/renew", json={"resource_id": "r"}).status_code == 422


def test_healthz(env):
    assert env.client.get("/healthz").json()["ok"] is True
