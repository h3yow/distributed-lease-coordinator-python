"""HTTP-level tests via FastAPI TestClient with an injected FakeClock."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from lease_coordinator.api import create_app
from lease_coordinator.ids import UuidLeaseIdGenerator
from lease_coordinator.model import FakeClock
from lease_coordinator.service import LeaseService


@pytest.fixture
def client(clock: FakeClock) -> TestClient:
    svc = LeaseService(":memory:", clock=clock, id_generator=UuidLeaseIdGenerator())
    return TestClient(create_app(svc))


def test_acquire_returns_held_lease_with_fencing_token(client: TestClient) -> None:
    r = client.post("/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 1000})
    assert r.status_code == 201
    lease = r.json()
    assert lease["status"] == "HELD"
    assert lease["fencing_token"] == 1
    assert lease["expires_at_ms"] == lease["acquired_at_ms"] + 1000
    # The secret credential must never be exposed through read endpoints.
    assert "credential" not in lease


def test_credential_is_required_for_renew_and_release(client: TestClient) -> None:
    created = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 1000,
                         "credential": "secret"}
    ).json()

    ok = client.post(f"/leases/{created['lease_id']}/renew",
                     json={"credential": "secret", "ttl_ms": 2000})
    assert ok.status_code == 200

    bad = client.post(f"/leases/{created['lease_id']}/renew",
                      json={"credential": "wrong", "ttl_ms": 2000})
    assert bad.status_code == 403
    assert bad.json()["code"] == "invalid_credential"

    released = client.post(f"/leases/{created['lease_id']}/release",
                           json={"credential": "secret"})
    assert released.status_code == 200 and released.json()["status"] == "RELEASED"


def test_conflict_expired_released_404_and_bad_cursor_codes(client: TestClient, clock) -> None:
    first = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 100,
                         "credential": "c1"}
    ).json()

    r = client.post("/leases", json={"resource_id": "r", "holder": "B", "ttl_ms": 100})
    assert r.status_code == 409 and r.json()["code"] == "lease_conflict"

    clock.advance(101)
    r = client.post(f"/leases/{first['lease_id']}/renew",
                    json={"credential": "c1", "ttl_ms": 100})
    assert r.status_code == 409 and r.json()["code"] == "lease_expired"

    second = client.post(
        "/leases", json={"resource_id": "r2", "holder": "A", "ttl_ms": 1000,
                         "credential": "c2"}
    ).json()
    client.post(f"/leases/{second['lease_id']}/release", json={"credential": "c2"})
    r = client.post(f"/leases/{second['lease_id']}/release", json={"credential": "c2"})
    assert r.status_code == 409 and r.json()["code"] == "lease_released"

    assert client.get("/leases/nope").status_code == 404
    r = client.get("/leases?cursor=garbage")
    assert r.status_code == 400 and r.json()["code"] == "invalid_cursor"


def test_event_history_endpoint(client: TestClient, clock) -> None:
    lease = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 1000,
                         "credential": "c"}
    ).json()
    client.post(f"/leases/{lease['lease_id']}/renew",
                json={"credential": "c", "ttl_ms": 1000})
    client.post(f"/leases/{lease['lease_id']}/release", json={"credential": "c"})

    r = client.get(f"/leases/{lease['lease_id']}/events")
    assert [e["event_type"] for e in r.json()["items"]] == [
        "ACQUIRED", "RENEWED", "RELEASED"
    ]
    r = client.get("/resources/r/events")
    assert len(r.json()["items"]) == 3


def test_http_concurrent_acquires_have_single_winner(client: TestClient) -> None:
    def attempt(i: int) -> int:
        r = client.post("/leases", json={"resource_id": "hot", "holder": f"c{i}",
                                         "ttl_ms": 10_000})
        return r.status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        statuses = list(pool.map(attempt, range(16)))

    assert statuses.count(201) == 1
    assert statuses.count(409) == 15
