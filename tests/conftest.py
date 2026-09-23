"""Shared pytest fixtures and logging setup.

Logging is enabled at INFO for the ``lease_coordinator`` logger so every
test run prints the inputs of each request and the decision basis
(status / expiry time / credential match) recorded by the service.
"""

from __future__ import annotations

import logging
import sys

import pytest

from lease_coordinator.ids import SeqLeaseIdGenerator
from lease_coordinator.model import FakeClock
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(current_ms=1_000_000)


@pytest.fixture
def service(clock: FakeClock) -> LeaseService:
    storage = LeaseStorage(":memory:")
    svc = LeaseService(
        ":memory:",
        clock=clock,
        id_generator=SeqLeaseIdGenerator(),
        storage=storage,
    )
    yield svc
    svc.close()


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    root = logging.getLogger("lease_coordinator")
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False


_configure_logging()
