from __future__ import annotations

import asyncio
from uuid import uuid4

from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool
from loom_worker.vllm_registry import WorkerVLLMRegistry


class _Settings:
    def __init__(
        self,
        *,
        max_concurrent: int,
        blocking_io_max_workers: int | None = None,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.blocking_io_max_workers = blocking_io_max_workers


def test_blocking_io_worker_count_defaults_from_trial_concurrency() -> None:
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=5),
        )
        == 32
    )
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=64),
        )
        == 256
    )
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=128),
        )
        == 256
    )


def test_blocking_io_worker_count_accepts_operator_override() -> None:
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=64, blocking_io_max_workers=96),
        )
        == 96
    )


def test_configure_blocking_io_executor_sets_loop_default(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    created: list[tuple[int, str]] = []
    installed: list[object] = []

    class _FakeExecutor:
        def __init__(self, *, max_workers: int, thread_name_prefix: str) -> None:
            created.append((max_workers, thread_name_prefix))

    class _FakeLoop:
        def set_default_executor(self, executor: object) -> None:
            installed.append(executor)

    monkeypatch.setattr(ml, "ThreadPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(ml.asyncio, "get_running_loop", lambda: _FakeLoop())

    ml._configure_blocking_io_executor(  # type: ignore[attr-defined]
        _Settings(max_concurrent=8),
    )

    assert created == [(32, "loom-worker-io")]
    assert len(installed) == 1


class _ClaimingCP:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self.payloads = list(payloads)
        self.claim_calls = 0

    async def claim(self, *, worker_id, caps):  # type: ignore[no-untyped-def]
        self.claim_calls += 1
        if not self.payloads:
            return None
        return self.payloads.pop(0)


async def test_claim_cycle_fills_available_pool_capacity(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    pool = RunnerPool(max_concurrent=3)
    release = asyncio.Event()
    cp = _ClaimingCP(
        [
            {
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": f"task-{idx}",
                "config": {"agent_name": "oracle", "agent_model": None},
            }
            for idx in range(3)
        ]
    )

    async def _fake_spawn_trial(**kwargs) -> None:  # type: ignore[no-untyped-def]
        async def _held() -> None:
            await release.wait()

        await kwargs["pool"].spawn(_held())

    monkeypatch.setattr(ml, "_spawn_trial", _fake_spawn_trial)

    claimed = await ml._claim_available_trials(  # type: ignore[attr-defined]
        pool=pool,
        settings=_Settings(max_concurrent=3),
        cp_client=cp,
        gateway_client=None,
        object_store=None,
        worker_id=uuid4(),
        vllm_registry=WorkerVLLMRegistry(enabled=False),
        sandbox_allocator=None,
        sandbox_singleton=None,
    )

    assert claimed == 3
    assert cp.claim_calls == 3
    assert pool.in_flight == 3
    release.set()
    await pool.wait_all(timeout=2.0)
