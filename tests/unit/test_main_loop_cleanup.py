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

import pytest

from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool


class _FakeCPClient:
    def __init__(self) -> None:
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


class _FakeSettings:
    trajectory_cache_dir = Path("/tmp/loom-test-cleanup-cache")
    gateway_url = "http://gw:9100"
    fixtures_root = None  # disables the fixture:// resolver path


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

    with patch.object(ml, "tempfile") as fake_tempfile, \
            patch.object(ml, "LocalTrialRunner") as fake_runner_cls:
        fake_tempfile.mkdtemp.side_effect = capture_mkdtemp
        fake_runner_cls.return_value = runner_target
        await ml._spawn_trial(
            pool=pool, settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload=payload,
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
    assert not task_dir.exists(), (
        f"task_dir {task_dir} leaked after a successful trial"
    )


async def test_tempdir_cleaned_on_runner_exception() -> None:
    task_dir = await _drive_spawn(_FailingRunner())
    assert not task_dir.exists(), (
        f"task_dir {task_dir} leaked after a failing trial"
    )


async def test_tempdir_cleaned_on_cancellation() -> None:
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
    with patch.object(ml, "tempfile") as fake_tempfile, \
            patch.object(ml, "LocalTrialRunner") as fake_runner_cls:
        fake_tempfile.mkdtemp.side_effect = capture_mkdtemp
        fake_runner_cls.return_value = _SlowRunner()
        await ml._spawn_trial(
            pool=pool, settings=settings,  # type: ignore[arg-type]
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
        )
        await asyncio.sleep(0.05)
        pool.cancel_all()
        await pool.wait_all(timeout=2.0)

    assert captured
    assert not captured[0].exists(), (
        f"task_dir {captured[0]} leaked after cancellation"
    )


# Suppress pytest's "unused" warning on the helper.
_ = pytest
