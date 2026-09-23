"""本地启动入口：uvicorn main:app

事件日志默认写入 ./data/lease-events.log，可用环境变量 LEASE_LOG_PATH 覆盖。
"""

from __future__ import annotations

import os

from lease_coordinator.api import create_app
from lease_coordinator.models import Clock
from lease_coordinator.service import LeaseCoordinatorService
from lease_coordinator.store import EventStore

_log_path = os.environ.get("LEASE_LOG_PATH", "data/lease-events.log")
_store = EventStore(_log_path)
_store.start()

service = LeaseCoordinatorService(_store, Clock())
app = create_app(service)
