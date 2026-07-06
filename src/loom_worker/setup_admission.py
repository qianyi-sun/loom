"""Admission guard for worker-side Docker setup/build work.

Trial execution concurrency and setup/build concurrency are separate
resources. A warm trial mostly consumes the sandbox runtime budget; a cold
Dockerfile, sidecar image, or layered agent-cache build can consume host disk
I/O, swap, and containerd/dockerd capacity before the trial ever reaches
``started_at``. This module is the shared health gate used before a worker
launches setup/build work against its Docker daemon.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class NodeHealthSnapshot:
    io_full_avg10: float | None
    swap_total_mb: int | None
    swap_free_mb: int | None
    d_state_processes: int | None


@dataclass(frozen=True)
class NodeHealthDecision:
    ok: bool
    reason: str
    detail: str


@dataclass(frozen=True)
class NodeHealthPolicy:
    io_full_avg10_max: float = 50.0
    min_swap_free_mb: int = 1024
    d_state_process_max: int = 32
    wait_timeout_sec: float = 300.0
    poll_interval_sec: float = 5.0
    enabled: bool = True

    def evaluate(self, snapshot: NodeHealthSnapshot) -> NodeHealthDecision:
        if not self.enabled:
            return NodeHealthDecision(
                ok=True,
                reason="disabled",
                detail="setup health guard disabled",
            )

        if (
            snapshot.io_full_avg10 is not None
            and snapshot.io_full_avg10 > self.io_full_avg10_max
        ):
            return NodeHealthDecision(
                ok=False,
                reason="node_io_pressure",
                detail=(
                    f"io.full.avg10={snapshot.io_full_avg10:g} "
                    f"max={self.io_full_avg10_max:g}"
                ),
            )

        if (
            snapshot.swap_total_mb is not None
            and snapshot.swap_total_mb > 0
            and snapshot.swap_free_mb is not None
            and snapshot.swap_free_mb < self.min_swap_free_mb
        ):
            return NodeHealthDecision(
                ok=False,
                reason="node_swap_exhausted",
                detail=(
                    f"swap.free_mb={snapshot.swap_free_mb} "
                    f"min={self.min_swap_free_mb} "
                    f"swap.total_mb={snapshot.swap_total_mb}"
                ),
            )

        if (
            snapshot.d_state_processes is not None
            and snapshot.d_state_processes > self.d_state_process_max
        ):
            return NodeHealthDecision(
                ok=False,
                reason="node_dstate_pressure",
                detail=(
                    f"d_state_processes={snapshot.d_state_processes} "
                    f"max={self.d_state_process_max}"
                ),
            )

        return NodeHealthDecision(
            ok=True,
            reason="healthy",
            detail=(
                f"io.full.avg10={_fmt_unknown(snapshot.io_full_avg10)} "
                f"swap.free_mb={_fmt_unknown(snapshot.swap_free_mb)} "
                f"d_state_processes={_fmt_unknown(snapshot.d_state_processes)}"
            ),
        )


class SetupAdmissionError(RuntimeError):
    def __init__(self, *, reason: str, operation: str, detail: str) -> None:
        self.reason = reason
        self.operation = operation
        self.detail = detail
        super().__init__(
            "SETUP_ADMISSION_BLOCKED "
            f"reason={reason} operation={operation} {detail}",
        )


def policy_from_settings(settings: Any) -> NodeHealthPolicy:
    return NodeHealthPolicy(
        io_full_avg10_max=float(
            getattr(settings, "setup_health_io_full_avg10_max", 50.0),
        ),
        min_swap_free_mb=int(
            getattr(settings, "setup_health_min_swap_free_mb", 1024),
        ),
        d_state_process_max=int(getattr(settings, "setup_health_dstate_max", 32)),
        wait_timeout_sec=float(
            getattr(settings, "setup_health_wait_timeout_sec", 300.0),
        ),
        poll_interval_sec=float(
            getattr(settings, "setup_health_poll_interval_sec", 5.0),
        ),
        enabled=bool(getattr(settings, "setup_health_guard_enabled", True)),
    )


async def wait_for_setup_health(
    *,
    policy: NodeHealthPolicy,
    operation: str = "setup-build",
    read_snapshot: Callable[[], NodeHealthSnapshot] = lambda: read_node_health_snapshot(),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> NodeHealthDecision:
    deadline = monotonic() + max(0.0, policy.wait_timeout_sec)
    last_decision: NodeHealthDecision | None = None

    while True:
        decision = policy.evaluate(read_snapshot())
        if decision.ok:
            return decision
        last_decision = decision
        now = monotonic()
        if now >= deadline:
            raise SetupAdmissionError(
                reason=decision.reason,
                operation=operation,
                detail=decision.detail,
            )
        interval = max(0.001, min(policy.poll_interval_sec, deadline - now))
        await sleep(interval)

    # Keeps type-checkers happy if the loop shape changes later.
    assert last_decision is not None


def read_node_health_snapshot(
    *,
    proc_root: Path = Path("/proc"),
) -> NodeHealthSnapshot:
    return NodeHealthSnapshot(
        io_full_avg10=_read_io_full_avg10(proc_root / "pressure" / "io"),
        swap_total_mb=_read_meminfo_mb(proc_root / "meminfo", "SwapTotal"),
        swap_free_mb=_read_meminfo_mb(proc_root / "meminfo", "SwapFree"),
        d_state_processes=_count_d_state_processes(proc_root),
    )


def _read_io_full_avg10(path: Path) -> float | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        parts = line.split()
        if not parts or parts[0] != "full":
            continue
        for part in parts[1:]:
            key, sep, raw = part.partition("=")
            if sep and key == "avg10":
                try:
                    return float(raw)
                except ValueError:
                    return None
    return None


def _read_meminfo_mb(path: Path, key: str) -> int | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    prefix = f"{key}:"
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(int(parts[1]) / 1024)
        except ValueError:
            return None
    return None


def _count_d_state_processes(proc_root: Path) -> int | None:
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return None

    count = 0
    for entry in entries:
        if not entry.name.isdigit():
            continue
        stat_path = entry / "stat"
        try:
            stat = stat_path.read_text(encoding="utf-8")
        except OSError:
            continue
        state = _parse_proc_stat_state(stat)
        if state == "D":
            count += 1
    return count


def _parse_proc_stat_state(stat: str) -> str | None:
    close = stat.rfind(")")
    if close < 0 or close + 2 >= len(stat):
        return None
    tail = stat[close + 2 :].split(maxsplit=1)
    if not tail:
        return None
    return tail[0]


def _fmt_unknown(value: object) -> str:
    return "unknown" if value is None else str(value)
