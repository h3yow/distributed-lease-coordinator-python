"""Entry point: build the FastAPI app backed by a local SQLite database.

Run with::

    uv run uvicorn main:app
"""

from __future__ import annotations

import os
from pathlib import Path

from lease_coordinator.api import create_app
from lease_coordinator.service import LeaseService

_DB_PATH = os.environ.get("LEASE_DB", str(Path(__file__).parent / "lease.db"))
_SERVICE = LeaseService(db_path=_DB_PATH)
_SERVICE.start_reaper()
app = create_app(_SERVICE)
