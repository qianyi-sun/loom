"""Regression: _spawn_trial's per-trial tempdir is cleaned up after the
trial body finishes — both on success and on exception.

Bug 4 from the post-Plan-7 review: long-running workers leak one mkdtemp
dir per claim until the host runs out of inodes / the trajectory PV
fills up.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest

from loom.models.result import FailureReason
from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool


class _FakeCPClient:
    def __init__(self) -> None:
        self.patch_calls: list[dict[str, object]] = []
        self.bundle = {
            "id": "fake",
            "checksum": "0" * 64,
            "config": {
                "schema_version": "1",
                "task": {"id": "fake", "name": "fake"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            "source": None,
        }

    async def get_task_bundle(self, _task_id: str) -> dict:
        return self.bundle

    async def get_trial_llm_calls(self, _trial_id) -> list:  # type: ignore[no-untyped-def]
        return []

    async def patch_state(
        self,
        *,
        trial_id,
        worker_id,
        state: str,
        failure_reason: str | None = None,
    ) -> bool:  # type: ignore[no-untyped-def]
        self.patch_calls.append(
            {
                "trial_id": trial_id,
                "worker_id": worker_id,
                "state": state,
                "failure_reason": failure_reason,
            }
        )
        return True


class _Bundle404CPClient(_FakeCPClient):
    async def get_task_bundle(self, task_id: str) -> dict:
        request = httpx.Request(
            "GET",
            f"http://cp/tasks/{task_id}/bundle",
        )
        response = httpx.Response(
            404,
            request=request,
            json={"detail": "task not found"},
        )
        raise httpx.HTTPStatusError(
            "404 task bundle not found",
            request=request,
            response=response,
        )


class _BlockingBundleCPClient(_FakeCPClient):
    def __init__(self) -> None:
        super().__init__()
        self.bundle_requested = asyncio.Event()
        self.release_bundle = asyncio.Event()

    async def get_task_bundle(self, _task_id: str) -> dict:
        self.bundle_requested.set()
        await self.release_bundle.wait()
        return self.bundle


class _BlockingFailingBundleCPClient(_FakeCPClient):
    def __init__(self) -> None:
        super().__init__()
        self.bundle_requested = asyncio.Event()
        self.release_bundle = asyncio.Event()

    async def get_task_bundle(self, task_id: str) -> dict:
        self.bundle_requested.set()
        await self.release_bundle.wait()
        request = httpx.Request(
            "GET",
            f"http://cp/tasks/{task_id}/bundle",
        )
        response = httpx.Response(
            500,
            request=request,
            json={"detail": "storage timeout"},
        )
        raise httpx.HTTPStatusError(
            "500 task bundle unavailable",
            request=request,
            response=response,
        )


class _FakeSettings:
    trajectory_cache_dir = Path("/tmp/loom-test-cleanup-cache")
    gateway_url = "http://gw:9100"
    fixtures_root = None  # disables the fixture:// resolver path
    benchmark_cache = None  # use HF's default cache
    # Phase D: required even when isolation is off because the worker
    # reads it to construct the LocalTrialRunner.
    sandbox_step_jwt_ttl_sec = 600


async def _drive_spawn(runner_target: object) -> Path:
    """Spawn one fake trial through _spawn_trial; capture the task_dir
    it created (via the patched mkdtemp), return its Path so the caller
    can assert it was cleaned up."""
    captured: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def capture_mkdtemp(**kwargs: str) -> str:
        d = real_mkdtemp(**kwargs)
        captured.append(Path(d))
        return d

    settings = _FakeSettings()
    cp = _FakeCPClient()
    payload = {
        "trial_id": str(uuid4()),
        "team_id": str(uuid4()),
        "task_id": "fake",
        # Plan 23: TrialConfig requires agent_name + agent_model.
        # `agent_model: None` is allowed for agents that don't call an LLM.
        "config": {"agent_name": "oracle", "agent_model": None},
    }

    pool = RunnerPool(max_concurrent=1)

    with (
        patch.object(ml, "tempfile") as fake_tempfile,
        patch.object(ml, "LocalTrialRunner") as fake_runner_cls,
    ):
        fake_tempfile.mkdtemp.side_effect = capture_mkdtemp
        fake_runner_cls.return_value = runner_target
        from loom_worker.vllm_registry import WorkerVLLMRegistry

        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload=payload,
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    assert captured, "_spawn_trial should have called tempfile.mkdtemp once"
    return captured[0]


class _SucceedingRunner:
    async def run(self) -> None:
        return None


class _FailingRunner:
    async def run(self) -> None:
        raise RuntimeError("simulated agent error")


async def test_tempdir_cleaned_on_success() -> None:
    task_dir = await _drive_spawn(_SucceedingRunner())
    assert not task_dir.exists(), f"task_dir {task_dir} leaked after a successful trial"


async def test_tempdir_cleaned_on_runner_exception() -> None:
    task_dir = await _drive_spawn(_FailingRunner())
    assert not task_dir.exists(), f"task_dir {task_dir} leaked after a failing trial"


async def test_tempdir_cleaned_on_cancellation() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    class _SlowRunner:
        async def run(self) -> None:
            await asyncio.sleep(10.0)

    settings = _FakeSettings()
    cp = _FakeCPClient()
    captured: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def capture_mkdtemp(**kwargs: str) -> str:
        d = real_mkdtemp(**kwargs)
        captured.append(Path(d))
        return d

    pool = RunnerPool(max_concurrent=1)
    with (
        patch.object(ml, "tempfile") as fake_tempfile,
        patch.object(ml, "LocalTrialRunner") as fake_runner_cls,
    ):
        fake_tempfile.mkdtemp.side_effect = capture_mkdtemp
        fake_runner_cls.return_value = _SlowRunner()
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload={
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await asyncio.sleep(0.05)
        pool.cancel_all()
        await pool.wait_all(timeout=2.0)

    assert captured
    assert not captured[0].exists(), f"task_dir {captured[0]} leaked after cancellation"


async def test_bundle_lookup_failure_marks_trial_failed_without_spawning() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _Bundle404CPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    await ml._spawn_trial(
        pool=pool,
        settings=settings,  # type: ignore[arg-type]
        cp_client=cp,  # type: ignore[arg-type]
        gateway_client=None,  # type: ignore[arg-type]
        object_store=None,  # type: ignore[arg-type]
        worker_id=worker_id,
        payload={
            "trial_id": str(trial_id),
            "team_id": str(uuid4()),
            "task_id": "humaneval/HumanEval/26",
            "config": {"agent_name": "oracle", "agent_model": None},
        },
        vllm_registry=WorkerVLLMRegistry(enabled=False),
    )

    await pool.wait_all(timeout=0.1)
    assert pool.in_flight == 0
    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.INTERNAL_ERROR.value,
        }
    ]


async def test_claimed_trial_counts_in_flight_before_setup_finishes() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _BlockingBundleCPClient()
    pool = RunnerPool(max_concurrent=1)

    with patch.object(ml, "LocalTrialRunner") as fake_runner_cls:
        fake_runner_cls.return_value = _SucceedingRunner()
        spawn_task = asyncio.create_task(
            ml._spawn_trial(
                pool=pool,
                settings=settings,  # type: ignore[arg-type]
                cp_client=cp,  # type: ignore[arg-type]
                gateway_client=None,  # type: ignore[arg-type]
                object_store=None,  # type: ignore[arg-type]
                worker_id=uuid4(),
                payload={
                    "trial_id": str(uuid4()),
                    "team_id": str(uuid4()),
                    "task_id": "fake",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
                vllm_registry=WorkerVLLMRegistry(enabled=False),
            )
        )
        await cp.bundle_requested.wait()
        assert pool.in_flight == 1
        cp.release_bundle.set()
        await spawn_task
        await pool.wait_all(timeout=2.0)


async def test_setup_failure_inside_pool_marks_claimed_trial_failed() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _BlockingFailingBundleCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    spawn_task = asyncio.create_task(
        ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "humaneval/HumanEval/26",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
    )

    await cp.bundle_requested.wait()
    assert pool.in_flight == 1
    cp.release_bundle.set()
    await spawn_task
    await pool.wait_all(timeout=2.0)

    assert pool.in_flight == 0
    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.INTERNAL_ERROR.value,
        }
    ]


async def test_runtime_bucket_bootstrap_creates_required_runtime_buckets() -> None:
    class _RuntimeBucketStore:
        def __init__(self) -> None:
            self.created: list[str] = []

        async def ensure_bucket(self, bucket: str) -> None:
            if bucket not in self.created:
                self.created.append(bucket)

    store = _RuntimeBucketStore()

    await ml._ensure_runtime_buckets(store)
    await ml._ensure_runtime_buckets(store)

    assert store.created == ["trajectories", "artifacts"]


# Suppress pytest's "unused" warning on the helper.
_ = pytest
