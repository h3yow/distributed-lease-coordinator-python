"""Shared pytest fixtures and logging setup.

Logging is enabled at INFO for the ``lease_coordinator`` logger so every
test run prints the inputs of each request and the decision basis
(status / expiry time / credential match) recorded by the service.

A terminal summary groups cases by *business area* (test module) and
reports per-area pass/fail counts, so a test run shows at a glance which
businesses are covered and how their cases turned out.
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict

import pytest

from lease_coordinator.ids import SeqLeaseIdGenerator
from lease_coordinator.model import FakeClock
from lease_coordinator.service import LeaseService
from lease_coordinator.storage import LeaseStorage

# Human-readable business name for each test module/file.
_BUSINESS_AREAS = {
    "test_lease_lifecycle.py": "申请/续约/释放/失败原因",
    "test_concurrency.py": "并发互斥/回收/多进程与重启",
    "test_history_and_paging.py": "游标分页与历史追溯",
    "test_credential_rotation.py": "凭证换发（去重/重放/并发/重启/加密落盘/回收交叉）",
    "test_api.py": "HTTP 接口契约",
}


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


# -- business-area coverage summary -----------------------------------------

def pytest_configure(config: pytest.Config) -> None:
    config._business_stats = defaultdict(lambda: {"passed": 0, "failed": 0, "skipped": 0})  # type: ignore[attr-defined]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        # Count setup failures as failures too; teardown noise is ignored.
        if report.when == "setup" and report.failed:
            stats = item.config._business_stats  # type: ignore[attr-defined]
            stats[item.path.name]["failed"] += 1
        return
    stats = item.config._business_stats  # type: ignore[attr-defined]
    area = item.path.name
    if report.passed:
        stats[area]["passed"] += 1
    elif report.skipped:
        stats[area]["skipped"] += 1
    else:
        stats[area]["failed"] += 1


def pytest_terminal_summary(terminalreporter, config, exitstatus):
    stats = config._business_stats  # type: ignore[attr-defined]
    if not stats:
        return
    writer = terminalreporter
    # Collect per-case results keyed by test module.
    by_module: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for phase, mark in (("passed", "PASS"), ("failed", "FAIL"), ("skipped", "SKIP")):
        for report in terminalreporter.stats.get(phase, []):
            if getattr(report, "when", "call") not in ("call", "setup"):
                continue
            path = getattr(report, "path", None)
            if path is None:
                module = str(report.fspath).rsplit("/", 1)[-1]
            else:
                module = path.name
            by_module[module].append((report.head_line.split("[")[0], mark))
    writer.write_sep("=", "业务覆盖概览（按业务分组的用例与结果）")
    total = {"passed": 0, "failed": 0, "skipped": 0}
    for module in sorted(stats):
        counts = stats[module]
        for key in total:
            total[key] += counts[key]
        writer.write_line(
            f"[{counts['passed']:>2} 通过 / {counts['failed']:>2} 失败 / "
            f"{counts['skipped']:>2} 跳过] {_BUSINESS_AREAS.get(module, module)}"
        )
        for name, mark in sorted(by_module.get(module, [])):
            writer.write_line(f"    {mark:>4}  {name}")
    writer.write_line(
        f"合计：{total['passed']} 通过 / {total['failed']} 失败 / "
        f"{total['skipped']} 跳过"
    )

