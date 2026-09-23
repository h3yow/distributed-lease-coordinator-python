"""pytest 公共夹具：确定性时钟、临时日志路径。"""

from __future__ import annotations

import threading

import pytest

from lease_coordinator.models import Clock
from lease_coordinator.service import LeaseCoordinatorService
from lease_coordinator.store import EventStore


class FakeClock(Clock):
    """可手动推进的线程安全时钟，保证测试结果稳定可重复。"""

    def __init__(self, start_ms: int = 1_000_000_000_000) -> None:
        self._now = start_ms
        self._lock = threading.Lock()

    def now_ms(self) -> int:
        with self._lock:
            return self._now

    def advance(self, ms: int) -> int:
        """向前推进 ms 毫秒，返回推进后的时间。"""
        with self._lock:
            self._now += ms
            return self._now

    def set(self, ms: int) -> None:
        with self._lock:
            self._now = ms


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path / "events.log")
    s.start()
    yield s
    s.close()


@pytest.fixture
def service(store, clock) -> LeaseCoordinatorService:
    # 不做真实退避睡眠，冲突重试立即发生 -> 测试快速且确定
    return LeaseCoordinatorService(store, clock, sleep_fn=lambda _s: None)
