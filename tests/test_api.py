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


# --------------------------------------------------------------------------
# Credential rotation endpoint
# --------------------------------------------------------------------------

def test_rotate_returns_new_credential_once_and_old_stops_working(
    client: TestClient, clock
) -> None:
    created = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 10_000,
                         "credential": "old-secret"}
    ).json()
    lease_id = created["lease_id"]

    r = client.post(f"/leases/{lease_id}/rotate",
                    json={"credential": "old-secret",
                          "rotation_id": "rot-1",
                          "new_credential": "new-secret"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["new_credential"] == "new-secret"
    assert body["replayed"] is False
    assert body["rotation_id"] == "rot-1"
    assert body["lease"]["version"] == 1
    assert "credential" not in body["lease"]  # the lease view stays secret-free

    # The old credential is dead for renew and release.
    bad = client.post(f"/leases/{lease_id}/renew",
                      json={"credential": "old-secret", "ttl_ms": 1000})
    assert bad.status_code == 403 and bad.json()["code"] == "invalid_credential"

    # The new credential continues the main link: renew then release.
    ok = client.post(f"/leases/{lease_id}/renew",
                     json={"credential": "new-secret", "ttl_ms": 5000})
    assert ok.status_code == 200
    released = client.post(f"/leases/{lease_id}/release",
                           json={"credential": "new-secret"})
    assert released.status_code == 200
    assert released.json()["status"] == "RELEASED"


def test_rotate_server_generated_credential_is_delivered_and_replayable(
    client: TestClient,
) -> None:
    # Boot credential is caller-supplied (the HTTP acquire endpoint never
    # returns server-made credentials); the *rotation* new credential is
    # server-generated and handed back exactly in the rotation result.
    created = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 10_000,
                         "credential": "boot-secret"}
    ).json()
    lease_id = created["lease_id"]

    r = client.post(f"/leases/{lease_id}/rotate",
                    json={"credential": "boot-secret", "rotation_id": "rot-1"})
    assert r.status_code == 200, r.text
    body = r.json()
    generated = body["new_credential"]
    assert len(generated) == 32
    assert generated != "boot-secret"
    assert body["replayed"] is False

    # Replaying the same request returns the identical credential, flagged.
    replay = client.post(f"/leases/{lease_id}/rotate",
                         json={"credential": "boot-secret", "rotation_id": "rot-1"})
    assert replay.status_code == 200
    replay_body = replay.json()
    assert replay_body["replayed"] is True
    assert replay_body["new_credential"] == generated

    # The generated credential genuinely works for follow-up operations.
    ok = client.post(f"/leases/{lease_id}/renew",
                     json={"credential": generated, "ttl_ms": 3000})
    assert ok.status_code == 200


def test_rotate_id_reuse_conflict_and_other_error_codes(
    client: TestClient, clock
) -> None:
    created = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 10_000,
                         "credential": "c1"}
    ).json()
    lease_id = created["lease_id"]

    # wrong boot credential
    r = client.post(f"/leases/{lease_id}/rotate",
                    json={"credential": "forged", "rotation_id": "rot-1",
                          "new_credential": "c2"})
    assert r.status_code == 403 and r.json()["code"] == "invalid_credential"

    # first good rotation
    r = client.post(f"/leases/{lease_id}/rotate",
                    json={"credential": "c1", "rotation_id": "rot-1",
                          "new_credential": "c2"})
    assert r.status_code == 200

    # reusing rot-1 with a different new credential -> rotation_conflict
    r = client.post(f"/leases/{lease_id}/rotate",
                    json={"credential": "c1", "rotation_id": "rot-1",
                          "new_credential": "c9"})
    assert r.status_code == 409 and r.json()["code"] == "rotation_conflict"

    # identical new/old credential -> invalid_rotation
    r = client.post(f"/leases/{lease_id}/rotate",
                    json={"credential": "c2", "rotation_id": "rot-2",
                          "new_credential": "c2"})
    assert r.status_code == 422 and r.json()["code"] == "invalid_rotation"

    # unknown lease -> 404
    r = client.post("/leases/nope/rotate",
                    json={"credential": "c2", "rotation_id": "rot-x",
                          "new_credential": "c3"})
    assert r.status_code == 404 and r.json()["code"] == "lease_not_found"

    # expired lease -> 409 lease_expired
    expired_lease = client.post(
        "/leases", json={"resource_id": "r2", "holder": "A", "ttl_ms": 100,
                         "credential": "e1"}
    ).json()
    clock.advance(101)
    r = client.post(f"/leases/{expired_lease['lease_id']}/rotate",
                    json={"credential": "e1", "rotation_id": "rot-e",
                          "new_credential": "e2"})
    assert r.status_code == 409 and r.json()["code"] == "lease_expired"


def test_rotate_history_endpoint_records_fact_without_secrets(
    client: TestClient,
) -> None:
    created = client.post(
        "/leases", json={"resource_id": "r", "holder": "A", "ttl_ms": 10_000,
                         "credential": "old-secret"}
    ).json()
    lease_id = created["lease_id"]
    client.post(f"/leases/{lease_id}/rotate",
                json={"credential": "old-secret", "rotation_id": "rot-1",
                      "new_credential": "new-secret"})

    r = client.get(f"/leases/{lease_id}/events")
    assert r.status_code == 200
    events = r.json()["items"]
    assert [e["event_type"] for e in events] == ["ACQUIRED", "CREDENTIAL_ROTATED"]
    blob = r.text
    assert "new-secret" not in blob
    assert "old-secret" not in blob

    # Plain lease listing / snapshot also stay secret-free.
    assert "new-secret" not in client.get("/leases").text
    assert "new-secret" not in client.get(f"/leases/{lease_id}").text


def test_http_concurrent_rotations_have_single_fresh_swap(
    client: TestClient,
) -> None:
    created = client.post(
        "/leases", json={"resource_id": "hot", "holder": "A", "ttl_ms": 60_000,
                         "credential": "c0"}
    ).json()
    lease_id = created["lease_id"]

    def attempt(i: int) -> int:
        r = client.post(f"/leases/{lease_id}/rotate",
                        json={"credential": "c0", "rotation_id": f"rot-{i}",
                              "new_credential": f"c{i + 1}"})
        return r.status_code

    with ThreadPoolExecutor(max_workers=12) as pool:
        statuses = list(pool.map(attempt, range(12)))

    assert statuses.count(200) == 1
    assert statuses.count(403) == 11
