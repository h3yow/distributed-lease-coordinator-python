"""租约协调服务：申请/续约/释放/过期回收/查询的业务编排。

一致性模型：
- 所有状态迁移都是 store 中一条带全局 seq、资源版本号、绝对到期时间戳的事件；
- 写操作采用"读最新投影 -> 判定 -> 以 expected_version 乐观提交"循环，
  提交冲突则有限次退避重试，因此并发申请同一资源只有一个能成功授予；
- 续约/释放先做存活判定（RELEASED / 已到期），再用 hmac 常量时间比较
  校验持有凭证，失败原因各自有独立错误码；
- 到期但尚未写 EXPIRED 的租约会在写路径上被惰性补记一次 EXPIRED，
  reap_expired 提供批量主动回收；回收幂等，重复扫描不会重复产生事件。
"""

from __future__ import annotations

import hmac
import logging
import secrets
import time
import uuid
from typing import Callable

from .errors import (
    ConflictRetryLimit,
    InvalidToken,
    LeaseExpired,
    LeaseNotFound,
    LeaseReleased,
    ResourceHeld,
)
from .models import (
    Clock,
    HistoryPage,
    Lease,
    LeaseEvent,
    LeasePage,
    LeaseStatus,
)
from .store import EventStore, WriteConflict

logger = logging.getLogger("lease_coordinator")


class LeaseCoordinatorService:
    def __init__(
        self,
        store: EventStore,
        clock: Clock | None = None,
        *,
        max_retries: int = 8,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.store = store
        self.clock = clock or Clock()
        self.max_retries = max_retries
        self._sleep = sleep_fn or time.sleep

    # ---- 写操作 ----

    def acquire(self, resource_id: str, holder: str, ttl_ms: int) -> Lease:
        """申请资源租约；资源空闲（不存在/已释放/已过期）时授予一把全新租约。"""
        for attempt in range(self.max_retries + 1):
            lease = self.store.get_lease(resource_id)
            now = self.clock.now_ms()
            if lease is None:
                logger.info(
                    "acquire 输入 resource_id=%s holder=%s ttl_ms=%s；判定：无历史租约，"
                    "以 expected_version=None 申请（now_ms=%s, attempt=%s）",
                    resource_id, holder, ttl_ms, now, attempt,
                )
                try:
                    self.store.append_event(
                        "ACQUIRED", resource_id, f"lease-{uuid.uuid4().hex}",
                        holder=holder, token=self._new_token(), fence_token=1,
                        ttl_ms=ttl_ms, expire_at_ms=now + ttl_ms, at_ms=now,
                        detail="首次申请", expected_version=None,
                    )
                except WriteConflict:
                    self._backoff(attempt)
                    continue
                return self._must_get(resource_id)

            if lease.is_active(now):
                logger.info(
                    "acquire 拒绝 resource_id=%s holder=%s；判定：租约 %s 仍有效 "
                    "status=%s holder=%s expire_at_ms=%s > now_ms=%s version=%s",
                    resource_id, holder, lease.lease_id, lease.status.value,
                    lease.holder, lease.expire_at_ms, now, lease.version,
                )
                raise ResourceHeld(
                    f"资源 {resource_id!r} 正被 {lease.holder!r} 持有，"
                    f"租约 {lease.lease_id} 在 {lease.expire_at_ms} 前有效"
                    f"（当前 {now}）",
                    resource_id=resource_id,
                )

            # 状态仍为 ACTIVE 但墙上时间已过到期点：先惰性补记 EXPIRED
            if lease.status is LeaseStatus.ACTIVE:
                logger.info(
                    "acquire 发现惰性过期 resource_id=%s lease_id=%s expire_at_ms=%s <= "
                    "now_ms=%s version=%s，先补记 EXPIRED 后重试",
                    resource_id, lease.lease_id, lease.expire_at_ms, now, lease.version,
                )
                try:
                    self.store.append_event(
                        "EXPIRED", resource_id, lease.lease_id,
                        holder=lease.holder, token=lease.token,
                        fence_token=lease.fence_token, ttl_ms=lease.ttl_ms,
                        expire_at_ms=lease.expire_at_ms, at_ms=now,
                        detail="申请时检测到过期，惰性回收",
                        expected_version=lease.version,
                    )
                except WriteConflict:
                    self._backoff(attempt)
                    continue
                continue

            # RELEASED / EXPIRED：资源空闲，授予全新一代租约
            new_fence = lease.fence_token + 1
            logger.info(
                "acquire 输入 resource_id=%s holder=%s ttl_ms=%s；判定：上一代租约 %s "
                "已处于终态 %s，以 expected_version=%s 申请新代（fence=%s, now_ms=%s）",
                resource_id, holder, ttl_ms, lease.lease_id, lease.status.value,
                lease.version, new_fence, now,
            )
            try:
                self.store.append_event(
                    "ACQUIRED", resource_id, f"lease-{uuid.uuid4().hex}",
                    holder=holder, token=self._new_token(), fence_token=new_fence,
                    ttl_ms=ttl_ms, expire_at_ms=now + ttl_ms, at_ms=now,
                    detail=f"上一代租约 {lease.lease_id} 已{lease.status.value}后重新申请",
                    expected_version=lease.version,
                )
            except WriteConflict:
                self._backoff(attempt)
                continue
            return self._must_get(resource_id)

        raise ConflictRetryLimit(
            f"申请资源 {resource_id!r} 时并发冲突重试 {self.max_retries} 次仍未成功",
            resource_id=resource_id,
        )

    def renew(self, resource_id: str, token: str) -> Lease:
        """续约：必须由当前有效租约、且持有正确凭证的客户端发起。"""
        for attempt in range(self.max_retries + 1):
            lease = self.store.get_lease(resource_id)
            now = self.clock.now_ms()
            if lease is None:
                logger.info("renew 拒绝 resource_id=%s；判定：资源不存在任何租约（now_ms=%s）",
                            resource_id, now)
                raise LeaseNotFound(f"资源 {resource_id!r} 不存在租约", resource_id=resource_id)

            if lease.status is LeaseStatus.RELEASED:
                logger.info(
                    "renew 拒绝 resource_id=%s lease_id=%s；判定：租约已被释放（version=%s）",
                    resource_id, lease.lease_id, lease.version,
                )
                raise LeaseReleased(
                    f"租约 {lease.lease_id} 已释放，不能续约", resource_id=resource_id
                )

            if lease.status is LeaseStatus.EXPIRED or now >= lease.expire_at_ms:
                logger.info(
                    "renew 拒绝 resource_id=%s lease_id=%s；判定：租约已过期 "
                    "expire_at_ms=%s now_ms=%s status=%s version=%s",
                    resource_id, lease.lease_id, lease.expire_at_ms, now,
                    lease.status.value, lease.version,
                )
                if lease.status is LeaseStatus.ACTIVE:
                    try:
                        self.store.append_event(
                            "EXPIRED", resource_id, lease.lease_id,
                            holder=lease.holder, token=lease.token,
                            fence_token=lease.fence_token, ttl_ms=lease.ttl_ms,
                            expire_at_ms=lease.expire_at_ms, at_ms=now,
                            detail="续约时检测到过期，惰性回收",
                            expected_version=lease.version,
                        )
                    except WriteConflict:
                        self._backoff(attempt)
                        continue
                raise LeaseExpired(
                    f"租约 {lease.lease_id} 已于 {lease.expire_at_ms} 过期"
                    f"（当前 {now}），请重新申请",
                    resource_id=resource_id,
                )

            if not self._token_matches(lease.token, token):
                logger.info(
                    "renew 拒绝 resource_id=%s lease_id=%s holder=%s；判定：凭证不符 "
                    "expire_at_ms=%s now_ms=%s version=%s",
                    resource_id, lease.lease_id, lease.holder,
                    lease.expire_at_ms, now, lease.version,
                )
                raise InvalidToken("续约凭证与当前持有者不匹配", resource_id=resource_id)

            new_expire = now + lease.ttl_ms
            logger.info(
                "renew 输入 resource_id=%s lease_id=%s；判定：凭证正确且租约有效，"
                "expire_at_ms %s -> %s（now_ms=%s, ttl_ms=%s, version=%s）",
                resource_id, lease.lease_id, lease.expire_at_ms, new_expire,
                now, lease.ttl_ms, lease.version,
            )
            try:
                self.store.append_event(
                    "RENEWED", resource_id, lease.lease_id,
                    holder=lease.holder, token=lease.token,
                    fence_token=lease.fence_token, ttl_ms=lease.ttl_ms,
                    expire_at_ms=new_expire, at_ms=now,
                    detail=f"续约，到期时间延至 {new_expire}",
                    expected_version=lease.version,
                )
            except WriteConflict:
                self._backoff(attempt)
                continue
            return self._must_get(resource_id)

        raise ConflictRetryLimit(
            f"续约资源 {resource_id!r} 时并发冲突重试 {self.max_retries} 次仍未成功",
            resource_id=resource_id,
        )

    def release(self, resource_id: str, token: str) -> Lease:
        """释放：仅当前有效持有者凭正确凭证可释放；终态操作返回终态投影。"""
        for attempt in range(self.max_retries + 1):
            lease = self.store.get_lease(resource_id)
            now = self.clock.now_ms()
            if lease is None:
                logger.info("release 拒绝 resource_id=%s；判定：资源不存在任何租约（now_ms=%s）",
                            resource_id, now)
                raise LeaseNotFound(f"资源 {resource_id!r} 不存在租约", resource_id=resource_id)

            if lease.status is LeaseStatus.RELEASED:
                logger.info(
                    "release 拒绝 resource_id=%s lease_id=%s；判定：租约已释放（version=%s）",
                    resource_id, lease.lease_id, lease.version,
                )
                raise LeaseReleased(
                    f"租约 {lease.lease_id} 已释放", resource_id=resource_id
                )

            if lease.status is LeaseStatus.EXPIRED or now >= lease.expire_at_ms:
                logger.info(
                    "release 拒绝 resource_id=%s lease_id=%s；判定：租约已过期 "
                    "expire_at_ms=%s now_ms=%s status=%s",
                    resource_id, lease.lease_id, lease.expire_at_ms, now,
                    lease.status.value,
                )
                if lease.status is LeaseStatus.ACTIVE:
                    try:
                        self.store.append_event(
                            "EXPIRED", resource_id, lease.lease_id,
                            holder=lease.holder, token=lease.token,
                            fence_token=lease.fence_token, ttl_ms=lease.ttl_ms,
                            expire_at_ms=lease.expire_at_ms, at_ms=now,
                            detail="释放时检测到过期，惰性回收",
                            expected_version=lease.version,
                        )
                    except WriteConflict:
                        self._backoff(attempt)
                        continue
                raise LeaseExpired(
                    f"租约 {lease.lease_id} 已于 {lease.expire_at_ms} 过期"
                    f"（当前 {now}），无需释放",
                    resource_id=resource_id,
                )

            if not self._token_matches(lease.token, token):
                logger.info(
                    "release 拒绝 resource_id=%s lease_id=%s holder=%s；判定：凭证不符 "
                    "expire_at_ms=%s now_ms=%s version=%s",
                    resource_id, lease.lease_id, lease.holder,
                    lease.expire_at_ms, now, lease.version,
                )
                raise InvalidToken("释放凭证与当前持有者不匹配", resource_id=resource_id)

            logger.info(
                "release 输入 resource_id=%s lease_id=%s；判定：凭证正确且租约有效，"
                "执行释放（now_ms=%s, expire_at_ms=%s, version=%s）",
                resource_id, lease.lease_id, now, lease.expire_at_ms, lease.version,
            )
            try:
                self.store.append_event(
                    "RELEASED", resource_id, lease.lease_id,
                    holder=lease.holder, token=lease.token,
                    fence_token=lease.fence_token, ttl_ms=lease.ttl_ms,
                    expire_at_ms=lease.expire_at_ms, at_ms=now,
                    detail="持有者主动释放",
                    expected_version=lease.version,
                )
            except WriteConflict:
                self._backoff(attempt)
                continue
            return self._must_get(resource_id)

        raise ConflictRetryLimit(
            f"释放资源 {resource_id!r} 时并发冲突重试 {self.max_retries} 次仍未成功",
            resource_id=resource_id,
        )

    def reap_expired(self, *, resource_id: str | None = None, limit: int = 100) -> list[LeaseEvent]:
        """扫描并回收"已过到期时刻但状态仍 ACTIVE"的租约。

        幂等：状态已是 EXPIRED/RELEASED 的不会再产生事件；
        扫描到提交冲突（别人刚续约/回收/释放）时跳过该资源，不报错。
        """
        now = self.clock.now_ms()
        reaped: list[LeaseEvent] = []
        leases = self.store.all_leases()
        leases.sort(key=lambda x: x.resource_id)
        for lease in leases:
            if len(reaped) >= limit:
                break
            if resource_id is not None and lease.resource_id != resource_id:
                continue
            if lease.status is not LeaseStatus.ACTIVE or now < lease.expire_at_ms:
                continue
            logger.info(
                "reap 回收 resource_id=%s lease_id=%s holder=%s；判定：expire_at_ms=%s <= "
                "now_ms=%s 且 status=ACTIVE，追加 EXPIRED（version=%s）",
                lease.resource_id, lease.lease_id, lease.holder,
                lease.expire_at_ms, now, lease.version,
            )
            try:
                event = self.store.append_event(
                    "EXPIRED", lease.resource_id, lease.lease_id,
                    holder=lease.holder, token=lease.token,
                    fence_token=lease.fence_token, ttl_ms=lease.ttl_ms,
                    expire_at_ms=lease.expire_at_ms, at_ms=now,
                    detail="后台扫描到期回收",
                    expected_version=lease.version,
                )
            except WriteConflict:
                logger.info(
                    "reap 跳过 resource_id=%s；判定：提交冲突 version 已从 %s 变更"
                    "（其他并发操作已处理）",
                    lease.resource_id, lease.version,
                )
                continue
            reaped.append(event)
        logger.info(
            "reap 完成 now_ms=%s resource_id=%s scanned=%s reaped=%s",
            now, resource_id, len(leases), len(reaped),
        )
        return reaped

    # ---- 读操作 ----

    def get_lease(self, resource_id: str) -> Lease | None:
        lease = self.store.get_lease(resource_id)
        logger.info(
            "get_lease resource_id=%s -> %s",
            resource_id,
            None if lease is None else (
                f"{lease.lease_id} status={lease.status.value} holder={lease.holder} "
                f"version={lease.version} expire_at_ms={lease.expire_at_ms} "
                f"effective_active={lease.is_active(self.clock.now_ms())}"
            ),
        )
        return lease

    def list_leases(self, cursor: str | None = None, limit: int = 50) -> LeasePage:
        items, next_cursor = self.store.list_leases(cursor, limit)
        logger.info("list_leases cursor=%s limit=%s -> %s 条, next=%s",
                    cursor, limit, len(items), next_cursor)
        return LeasePage(items=items, next_cursor=next_cursor)

    def list_history(
        self, resource_id: str | None = None, cursor: str | None = None, limit: int = 50
    ) -> HistoryPage:
        items, next_cursor = self.store.list_history(resource_id, cursor, limit)
        logger.info("list_history resource_id=%s cursor=%s limit=%s -> %s 条, next=%s",
                    resource_id, cursor, limit, len(items), next_cursor)
        return HistoryPage(items=items, next_cursor=next_cursor)

    # ---- 内部工具 ----

    @staticmethod
    def _new_token() -> str:
        return secrets.token_urlsafe(18)

    @staticmethod
    def _token_matches(expected: str, provided: str) -> bool:
        return hmac.compare_digest(expected, provided)

    def _backoff(self, attempt: int) -> None:
        delay = min(0.05 * (2 ** attempt), 0.5)
        self._sleep(delay)

    def _must_get(self, resource_id: str) -> Lease:
        lease = self.store.get_lease(resource_id)
        assert lease is not None, f"提交成功后应能读到 {resource_id!r} 的投影"
        return lease
