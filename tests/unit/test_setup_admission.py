from __future__ import annotations

from dataclasses import dataclass

import pytest

from loom_worker.setup_admission import (
    NodeHealthPolicy,
    NodeHealthSnapshot,
    SetupAdmissionError,
    wait_for_setup_health,
)


def test_node_health_policy_blocks_high_io_pressure() -> None:
    policy = NodeHealthPolicy(
        io_full_avg10_max=50.0,
        min_swap_free_mb=1024,
        d_state_process_max=32,
    )
    snapshot = NodeHealthSnapshot(
        io_full_avg10=76.15,
        swap_total_mb=8192,
        swap_free_mb=4096,
        d_state_processes=3,
    )

    decision = policy.evaluate(snapshot)

    assert decision.ok is False
    assert decision.reason == "node_io_pressure"
    assert "io.full.avg10=76.15" in decision.detail


def test_node_health_policy_allows_hosts_without_swap() -> None:
    policy = NodeHealthPolicy(
        io_full_avg10_max=50.0,
        min_swap_free_mb=1024,
        d_state_process_max=32,
    )
    snapshot = NodeHealthSnapshot(
        io_full_avg10=0.0,
        swap_total_mb=0,
        swap_free_mb=0,
        d_state_processes=0,
    )

    decision = policy.evaluate(snapshot)

    assert decision.ok is True
    assert decision.reason == "healthy"


@pytest.mark.asyncio
async def test_wait_for_setup_health_rejects_after_timeout() -> None:
    policy = NodeHealthPolicy(
        io_full_avg10_max=50.0,
        min_swap_free_mb=1024,
        d_state_process_max=32,
        wait_timeout_sec=0.0,
        poll_interval_sec=0.0,
    )

    with pytest.raises(SetupAdmissionError) as exc:
        await wait_for_setup_health(
            policy=policy,
            read_snapshot=lambda: NodeHealthSnapshot(
                io_full_avg10=1.0,
                swap_total_mb=4096,
                swap_free_mb=0,
                d_state_processes=1,
            ),
            sleep=lambda _seconds: _noop_sleep(),
        )

    assert exc.value.reason == "node_swap_exhausted"
    assert "SETUP_ADMISSION_BLOCKED" in str(exc.value)


@dataclass
class _Clock:
    now: float = 0.0

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.mark.asyncio
async def test_wait_for_setup_health_delays_until_recovered() -> None:
    policy = NodeHealthPolicy(
        io_full_avg10_max=50.0,
        min_swap_free_mb=1024,
        d_state_process_max=32,
        wait_timeout_sec=10.0,
        poll_interval_sec=2.0,
    )
    clock = _Clock()
    snapshots = [
        NodeHealthSnapshot(
            io_full_avg10=80.0,
            swap_total_mb=4096,
            swap_free_mb=2048,
            d_state_processes=1,
        ),
        NodeHealthSnapshot(
            io_full_avg10=4.0,
            swap_total_mb=4096,
            swap_free_mb=2048,
            d_state_processes=1,
        ),
    ]

    async def _sleep(seconds: float) -> None:
        await clock.sleep(seconds)

    decision = await wait_for_setup_health(
        policy=policy,
        read_snapshot=lambda: snapshots.pop(0),
        sleep=_sleep,
        monotonic=lambda: clock.now,
    )

    assert decision.ok is True
    assert decision.reason == "healthy"
    assert clock.now == 2.0


async def _noop_sleep() -> None:
    return None
