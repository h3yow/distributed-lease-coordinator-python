"""事件存储：基于 JSONL 追加日志的本地持久化，支持回放与乐观并发提交。

设计要点：
- 仅依赖本地文件系统，无外部服务、无真实凭据；
- 每次读/写操作期间持有 fcntl.flock 排他文件锁（跨进程互斥），
  进程内再用 threading.RLock（跨线程互斥），因此多线程、多进程的
  提交都被串行化，且每次操作前都会"追赶"其他进程追加的日志；
- 事件以 O_APPEND 单行 JSON 追加并 fsync，进程崩溃不产生半条记录
  （flock 保证串行，单条写入远小于 PIPE_BUF/页大小，行原子）；
- 进程重启后回放整条日志重建内存投影，所有时间判定基于事件内的
  绝对时间戳，不依赖重启时刻的墙上时钟；
- 日志只追加、不删除：过期/释放也以 EXPIRED/RELEASED 事件体现，
  查询永远不会因为回收而静默缺失。
"""

from __future__ import annotations

import base64
import dataclasses
import fcntl
import json
import pathlib
import threading
from typing import Callable

from .models import Lease, LeaseEvent, LeaseStatus


class WriteConflict(Exception):
    """乐观并发校验失败：资源版本与调用方预期不一致，调用方应重试。"""

    def __init__(self, resource_id: str, expected: int | None, current: int) -> None:
        self.resource_id = resource_id
        self.expected = expected
        self.current = current
        super().__init__(
            f"write conflict on {resource_id!r}: expected version {expected}, "
            f"current version {current}"
        )


def encode_cursor(payload: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def decode_cursor(cursor: str) -> dict:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except Exception as exc:  # noqa: BLE001 - 任意解码失败统一归类
        raise ValueError(f"invalid cursor: {cursor!r}") from exc


class EventStore:
    """JSONL 追加日志 + 内存投影。"""

    def __init__(
        self,
        path: str | pathlib.Path,
        *,
        inject_conflict: Callable[[], bool] | None = None,
    ) -> None:
        self.path = pathlib.Path(path)
        self._lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._thread_lock = threading.RLock()
        self._leases: dict[str, Lease] = {}
        self._events: list[LeaseEvent] = []
        self._loaded_size = 0
        self._lock_fd: int | None = None
        # 测试钩子：返回 True 时下一次提交抛 WriteConflict（用于确定性地验证重试）
        self.inject_conflict = inject_conflict

    # ---- 生命周期 ----

    def start(self) -> None:
        """打开（必要时创建）日志、锁文件并回放已有事件。"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path.touch(exist_ok=True)
        self.path.touch(exist_ok=True)
        fd = os_open(self._lock_path)
        self._lock_fd = fd
        with self._locked():
            self._catch_up()

    def close(self) -> None:
        with self._thread_lock:
            if self._lock_fd is not None:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os_close(self._lock_fd)
                self._lock_fd = None

    def __enter__(self) -> "EventStore":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- 锁与追赶 ----

    def _locked(self):
        class _Guard:
            def __init__(inner, owner: "EventStore") -> None:
                inner.owner = owner

            def __enter__(inner) -> "EventStore":
                inner.owner._thread_lock.acquire()
                assert inner.owner._lock_fd is not None, "store not started"
                fcntl.flock(inner.owner._lock_fd, fcntl.LOCK_EX)
                return inner.owner

            def __exit__(inner, *exc: object) -> None:
                try:
                    fcntl.flock(inner.owner._lock_fd, fcntl.LOCK_UN)
                finally:
                    inner.owner._thread_lock.release()

        return _Guard(self)

    def _catch_up(self) -> None:
        """读取本进程尚未加载的日志尾部并重放到投影（持锁调用）。"""
        size = self.path.stat().st_size
        if size == self._loaded_size:
            return
        with open(self.path, "rb") as fh:
            fh.seek(self._loaded_size)
            chunk = fh.read(size - self._loaded_size)
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            self._apply(LeaseEvent(**json.loads(line)))
        self._loaded_size = size

    def _apply(self, event: LeaseEvent) -> None:
        """把单条事件应用到内存投影。"""
        self._events.append(event)
        current = self._leases.get(event.resource_id)
        if event.event_type == "ACQUIRED":
            self._leases[event.resource_id] = Lease(
                resource_id=event.resource_id,
                lease_id=event.lease_id,
                holder=event.holder,
                token=event.token,
                fence_token=event.fence_token,
                status=LeaseStatus.ACTIVE,
                version=event.version,
                ttl_ms=event.ttl_ms,
                acquired_at_ms=event.at_ms,
                expire_at_ms=event.expire_at_ms,
                last_event_seq=event.seq,
            )
            return
        assert current is not None, f"event {event.event_id} 缺少前置租约"
        current.version = event.version
        current.last_event_seq = event.seq
        if event.event_type == "RENEWED":
            current.status = LeaseStatus.ACTIVE
            current.ttl_ms = event.ttl_ms
            current.expire_at_ms = event.expire_at_ms
        elif event.event_type == "RELEASED":
            current.status = LeaseStatus.RELEASED
            current.expire_at_ms = event.expire_at_ms
        elif event.event_type == "EXPIRED":
            current.status = LeaseStatus.EXPIRED
        else:  # pragma: no cover - 防御
            raise ValueError(f"unknown event type: {event.event_type!r}")

    # ---- 读侧 ----

    def get_lease(self, resource_id: str) -> Lease | None:
        with self._locked():
            self._catch_up()
            return self._leases.get(resource_id)

    def list_leases(
        self, cursor: str | None, limit: int
    ) -> tuple[list[Lease], str | None]:
        """按 resource_id 字典序游标分页；租约投影永不删除。"""
        with self._locked():
            self._catch_up()
            after = ""
            if cursor is not None:
                after = decode_cursor(cursor)["r"]
            ids = sorted(rid for rid in self._leases if rid > after)
            page = ids[: max(1, limit)]
            items = [self._leases[rid] for rid in page]
            next_cursor = encode_cursor({"r": page[-1]}) if len(ids) > len(page) else None
            return items, next_cursor

    def list_history(
        self, resource_id: str | None, cursor: str | None, limit: int
    ) -> tuple[list[LeaseEvent], str | None]:
        """按全局 seq 升序游标分页，可按资源过滤；事件永不删除。"""
        with self._locked():
            self._catch_up()
            after_seq = 0
            if cursor is not None:
                after_seq = int(decode_cursor(cursor)["s"])
            matched = [
                e
                for e in self._events
                if e.seq > after_seq and (resource_id is None or e.resource_id == resource_id)
            ]
            page = matched[: max(1, limit)]
            next_cursor = (
                encode_cursor({"s": page[-1].seq}) if len(matched) > len(page) else None
            )
            return list(page), next_cursor

    def all_leases(self) -> list[Lease]:
        with self._locked():
            self._catch_up()
            return list(self._leases.values())

    # ---- 写侧 ----

    def append_event(
        self,
        event_type: str,
        resource_id: str,
        lease_id: str,
        *,
        holder: str,
        token: str,
        fence_token: int,
        ttl_ms: int,
        expire_at_ms: int,
        at_ms: int,
        detail: str = "",
        expected_version: int | None = None,
    ) -> LeaseEvent:
        """在文件锁内：追赶其他进程 -> OCC 校验 -> 分配全局 seq -> 追加落盘。"""
        with self._locked():
            self._catch_up()
            current = self._leases.get(resource_id)
            current_version = current.version if current is not None else None
            if self.inject_conflict is not None and self.inject_conflict():
                raise WriteConflict(resource_id, expected_version, current_version or 0)
            if current_version != expected_version:
                raise WriteConflict(resource_id, expected_version, current_version or 0)

            seq = (self._events[-1].seq + 1) if self._events else 1
            new_version = (current_version or 0) + 1
            event = LeaseEvent(
                seq=seq,
                event_id=f"evt-{_uuid()}",
                event_type=event_type,
                resource_id=resource_id,
                lease_id=lease_id,
                at_ms=at_ms,
                version=new_version,
                ttl_ms=ttl_ms,
                expire_at_ms=expire_at_ms,
                holder=holder,
                token=token,
                fence_token=fence_token,
                detail=detail,
            )
            line = json.dumps(dataclasses.asdict(event), separators=(",", ":"), ensure_ascii=False)
            fd = os_open_append(self.path)
            try:
                os_write_all(fd, (line + "\n").encode("utf-8"))
                os_fsync(fd)
            finally:
                os_close(fd)
            self._loaded_size = self.path.stat().st_size
            self._apply(event)
            return event

    def replay(self) -> None:
        """清空内存投影并从日志头回放（确定性地模拟进程重启）。"""
        with self._locked():
            self._leases.clear()
            self._events.clear()
            self._loaded_size = 0
            self._catch_up()


# ---- os 模块别名（集中在文件尾部，便于看清持久化原语）----
import os  # noqa: E402


def os_open(path: pathlib.Path) -> int:
    return os.open(path, os.O_RDWR | os.O_CREAT, 0o600)


def os_open_append(path: pathlib.Path) -> int:
    return os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)


def os_write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def os_fsync(fd: int) -> None:
    os.fsync(fd)


def os_close(fd: int) -> None:
    os.close(fd)


def _uuid() -> str:
    import uuid

    return uuid.uuid4().hex
