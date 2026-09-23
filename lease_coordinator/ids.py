"""Lease id generation with a deterministic, injectable sequence."""

from __future__ import annotations

import itertools
import uuid
from typing import Protocol


class IdGenerator(Protocol):
    def new_lease_id(self) -> str: ...


class UuidLeaseIdGenerator:
    """Production id generator based on uuid4 hex."""

    def new_lease_id(self) -> str:
        return uuid.uuid4().hex


class SeqLeaseIdGenerator:
    """Deterministic generator ``lease-1``, ``lease-2`` ... used in tests."""

    def __init__(self, prefix: str = "lease") -> None:
        self._counter = itertools.count(1)
        self._prefix = prefix

    def new_lease_id(self) -> str:
        return f"{self._prefix}-{next(self._counter)}"
