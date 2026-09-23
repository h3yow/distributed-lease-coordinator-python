"""租约基础生命周期：申请、续约、释放与各失败原因的可区分性。

所有用例注入 FakeClock，不依赖墙上时钟，重复执行结果稳定；
判定依据通过 lease_coordinator logger 打印（见 caplog 断言）。
"""

from __future__ import annotations

import logging

import pytest

from lease_coordinator.errors import (
    InvalidToken,
    LeaseExpired,
    LeaseNotFound,
    LeaseReleased,
    ResourceHeld,
)

pytestmark = pytest.mark.usefixtures("caplog")


@pytest.fixture(autouse=True)
def _capture_logs(caplog):
    caplog.set_level(logging.INFO, logger="lease_coordinator")


def test_acquire_grants_new_lease_with_explained_fields(service, clock, caplog):
    """输入：resource=r1, holder=h1, ttl=5000@t=0；判定：无历史租约，首次授予。"""
    lease = service.acquire("r1", "h1", 5000)

    assert lease.status.value == "ACTIVE"
    assert lease.holder == "h1"
    assert lease.fence_token == 1
    assert lease.version == 1
    assert lease.acquired_at_ms == clock.now_ms()
    assert lease.expire_at_ms == clock.now_ms() + 5000
    assert lease.token and len(lease.token) >= 20
    assert "无历史租约" in caplog.text
    assert "resource_id=r1" in caplog.text and "ttl_ms=5000" in caplog.text


def test_concurrent_logical_acquire_while_held_is_rejected_with_resource_held(service):
    """输入：h1 已持有 r1 时 h2 申请；判定：status=ACTIVE 且未到期 -> resource_held。"""
    service.acquire("r1", "h1", 5000)

    with pytest.raises(ResourceHeld) as exc:
        service.acquire("r1", "h2", 5000)
    assert exc.value.code == "resource_held"
    assert "h1" in str(exc.value)


def test_renew_with_wrong_token_is_rejected_with_invalid_token(service, clock, caplog):
    """输入：正确持有者租约 + 错误凭证；判定：存活但凭证不符 -> invalid_token。"""
    lease = service.acquire("r1", "h1", 5000)
    clock.advance(100)

    with pytest.raises(InvalidToken) as exc:
        service.renew("r1", lease.token + "-tampered")
    assert exc.value.code == "invalid_token"
    assert "凭证不符" in caplog.text


def test_renew_after_expiry_is_rejected_with_lease_expired_and_lazily_marked(service, clock, caplog):
    """输入：租约 ttl=5000，时钟推进 6000 后续约；判定：now>=expire_at -> lease_expired。

    同时惰性补记 EXPIRED，投影状态变为 EXPIRED。
    """
    lease = service.acquire("r1", "h1", 5000)
    clock.advance(6000)

    with pytest.raises(LeaseExpired) as exc:
        service.renew("r1", lease.token)
    assert exc.value.code == "lease_expired"
    assert "过期" in caplog.text

    current = service.get_lease("r1")
    assert current.status.value == "EXPIRED"
    history = service.list_history("r1")
    assert [e.event_type for e in history.items] == ["ACQUIRED", "EXPIRED"]


def test_renew_unknown_resource_is_not_found(service):
    """输入：不存在的资源；判定：无任何租约投影 -> lease_not_found。"""
    with pytest.raises(LeaseNotFound) as exc:
        service.renew("ghost", "whatever")
    assert exc.value.code == "lease_not_found"


def test_release_with_wrong_token_while_active_is_invalid_token(service, clock):
    """输入：ACTIVE 租约 + 错误凭证释放；判定：存活但凭证不符 -> invalid_token。"""
    service.acquire("r1", "h1", 5000)
    clock.advance(100)

    with pytest.raises(InvalidToken) as exc:
        service.release("r1", "not-the-token")
    assert exc.value.code == "invalid_token"


def test_release_then_renew_is_rejected_with_lease_released(service, clock, caplog):
    """输入：正确释放后续约；判定：终态 RELEASED 优先 -> lease_released。"""
    lease = service.acquire("r1", "h1", 5000)
    released = service.release("r1", lease.token)
    assert released.status.value == "RELEASED"

    with pytest.raises(LeaseReleased) as exc:
        service.renew("r1", lease.token)
    assert exc.value.code == "lease_released"
    assert "已被释放" in caplog.text


def test_double_release_is_rejected_with_lease_released(service):
    """输入：同一凭证连续释放两次；判定：第二次看到 RELEASED -> lease_released。"""
    lease = service.acquire("r1", "h1", 5000)
    service.release("r1", lease.token)

    with pytest.raises(LeaseReleased) as exc:
        service.release("r1", lease.token)
    assert exc.value.code == "lease_released"


def test_release_after_expiry_is_rejected_with_lease_expired(service, clock):
    """输入：到期后凭正确凭证释放；判定：已过期（并惰性回收）-> lease_expired。"""
    lease = service.acquire("r1", "h1", 5000)
    clock.advance(5000)  # 恰好到达 expire_at_ms，边界上即为失效

    with pytest.raises(LeaseExpired):
        service.release("r1", lease.token)
    assert service.get_lease("r1").status.value == "EXPIRED"


def test_renew_extends_expiry_using_current_ttl_and_tick(service, clock):
    """输入：t=0 授予 ttl=5000，t=2000 续约；判定：新到期点 = 2000 + 5000。"""
    lease = service.acquire("r1", "h1", 5000)
    clock.advance(2000)
    renewed = service.renew("r1", lease.token)

    assert renewed.version == 2
    assert renewed.expire_at_ms == clock.now_ms() + 5000
    assert renewed.lease_id == lease.lease_id
    assert renewed.fence_token == lease.fence_token


def test_reacquire_after_expiry_creates_new_generation_with_incremented_fence(service, clock):
    """输入：第一代过期回收后 h2 申请；判定：终态资源空闲，fence 单调 +1。"""
    lease1 = service.acquire("r1", "h1", 5000)
    clock.advance(6000)
    service.reap_expired(resource_id="r1")

    lease2 = service.acquire("r1", "h2", 3000)
    assert lease2.lease_id != lease1.lease_id
    assert lease2.fence_token == 2
    assert lease2.holder == "h2"
    assert lease2.ttl_ms == 3000
    assert lease2.expire_at_ms == clock.now_ms() + 3000


def test_reacquire_after_release_creates_new_generation(service):
    """输入：释放后重新申请；判定：RELEASED 终态允许新代，fence +1，凭证不同。"""
    lease1 = service.acquire("r1", "h1", 5000)
    service.release("r1", lease1.token)

    lease2 = service.acquire("r1", "h2", 5000)
    assert lease2.fence_token == 2
    assert lease2.token != lease1.token
