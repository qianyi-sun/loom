"""Runtime helper unit tests (#744, #782)."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

import pytest

from loom.agent.terminus2.runtime import (
    LoomTerminus2Runtime,
    _harbor_model_name,
    _openai_gateway_base,
)
from loom.driver.fake import FakeDriver
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter


def test_openai_gateway_base_appends_v1() -> None:
    assert _openai_gateway_base("http://gateway/openai") == "http://gateway/openai/v1"
    assert (
        _openai_gateway_base("http://gateway/openai/v1")
        == "http://gateway/openai/v1"
    )


def test_harbor_model_name_prefixes_openai() -> None:
    assert _harbor_model_name(ModelSpec(provider="openai", name="gpt-4")) == (
        "openai/gpt-4"
    )


class _TokenCP:
    def __init__(self) -> None:
        self._counter = 0

    async def mint_step_token(self, **kwargs: object) -> str:
        self._counter += 1
        return f"token-{self._counter}"

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, object]]:
        return []


def _patch_harbor(
    monkeypatch,
    *,
    tokens_seen: list[str],
    lifecycle: list[str] | None = None,
) -> None:
    class _FakeTerminus2:
        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir
            llm_kwargs = kwargs.get("llm_kwargs")
            if isinstance(llm_kwargs, dict) and "api_key" in llm_kwargs:
                tokens_seen.append(str(llm_kwargs["api_key"]))

        async def setup(self, env: object) -> None:
            if lifecycle is not None:
                lifecycle.append("setup")
            await asyncio.sleep(0.05)

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            await asyncio.sleep(0.05)
            traj = self._logs_dir / "trajectory.json"
            traj.write_text(
                json.dumps({"steps": []}),
                encoding="utf-8",
            )

    class _FakeContext:
        pass

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime._import_terminus2",
        lambda: (_FakeTerminus2, _FakeContext),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.harbor_environment._import_harbor",
        lambda: None,
    )

    class _TrialPaths:
        def __init__(self, trial_dir: Path) -> None:
            self.trial_dir = trial_dir

        def mkdir(self) -> None:
            self.trial_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.make_trial_paths",
        lambda logs_root: _TrialPaths(logs_root),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.LoomHarborEnvironment.create",
        staticmethod(lambda **kwargs: object()),
    )


@pytest.mark.asyncio
async def test_concurrent_runtimes_do_not_mutate_process_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    tokens_seen: list[str] = []
    _patch_harbor(monkeypatch, tokens_seen=tokens_seen)

    prior_api_key = os.environ.get("OPENAI_API_KEY")
    prior_base = os.environ.get("OPENAI_BASE_URL")
    cp = _TokenCP()

    async def _run_one(trial_id: UUID, path: Path) -> None:
        runtime = LoomTerminus2Runtime(
            model=ModelSpec(provider="openai", name="gpt-4"),
            team_id=str(uuid4()),
            trial_id=trial_id,
            cp_client=cp,
            gateway_url="http://127.0.0.1:19100",
        )
        driver = FakeDriver()
        await driver.start()
        writer = TrajectoryWriter(
            local_path=path,
            store=FakeObjectStore(),
            bucket="trajectories",
            key=f"{trial_id}/events.jsonl",
            flush_event_count=1000,
            flush_bytes=10_000_000,
            flush_sec=3600,
            min_part_bytes=0,
        )
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )
        await driver.stop()

    await asyncio.gather(
        _run_one(uuid4(), tmp_path / "a.jsonl"),
        _run_one(uuid4(), tmp_path / "b.jsonl"),
    )

    assert os.environ.get("OPENAI_API_KEY") == prior_api_key
    assert os.environ.get("OPENAI_BASE_URL") == prior_base
    assert set(tokens_seen) == {"token-1", "token-2"}


@pytest.mark.asyncio
async def test_retry_clears_harbor_fixed_tmux_session_before_each_setup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    lifecycle: list[str] = []
    tokens_seen: list[str] = []
    _patch_harbor(
        monkeypatch,
        tokens_seen=tokens_seen,
        lifecycle=lifecycle,
    )

    def _record_exec(
        cmd: str,
        user: str | int | None,
        cwd: PurePosixPath | None,
        env: object,
    ):
        del user, cwd, env
        lifecycle.append(cmd)
        from loom.models.exec import ExecResult

        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.0,
        )

    trial_id = uuid4()
    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="gpt-4"),
        team_id=str(uuid4()),
        trial_id=trial_id,
        cp_client=_TokenCP(),
        gateway_url="http://127.0.0.1:19100",
    )
    driver = FakeDriver(exec_handler=_record_exec)
    await driver.start()

    for attempt in range(2):
        writer = TrajectoryWriter(
            local_path=tmp_path / f"retry-{attempt}.jsonl",
            store=FakeObjectStore(),
            bucket="trajectories",
            key=f"{trial_id}/{attempt}/events.jsonl",
            flush_event_count=1000,
            flush_bytes=10_000_000,
            flush_sec=3600,
            min_part_bytes=0,
        )
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )

    cleanup = "tmux kill-session -t terminus-2 2>/dev/null || true"
    assert lifecycle == [cleanup, "setup", cleanup, "setup"]
    assert tokens_seen == ["token-1", "token-2"]
    await driver.stop()


@pytest.mark.asyncio
async def test_runtime_cleanup_leaves_process_env_unchanged_on_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _FailingTerminus2:
        def __init__(self, logs_dir, **kwargs: object) -> None:
            self._logs_dir = logs_dir

        async def setup(self, env: object) -> None:
            return None

        async def run(
            self,
            instruction: str,
            env: object,
            context: object,
        ) -> None:
            traj = self._logs_dir / "trajectory.json"
            traj.write_text(json.dumps({"steps": []}), encoding="utf-8")
            raise RuntimeError("harbor boom")

    class _FakeContext:
        pass

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime._import_terminus2",
        lambda: (_FailingTerminus2, _FakeContext),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.harbor_environment._import_harbor",
        lambda: None,
    )

    class _TrialPaths:
        def __init__(self, trial_dir: Path) -> None:
            self.trial_dir = trial_dir

        def mkdir(self) -> None:
            self.trial_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.make_trial_paths",
        lambda logs_root: _TrialPaths(logs_root),
    )
    monkeypatch.setattr(
        "loom.agent.terminus2.runtime.LoomHarborEnvironment.create",
        staticmethod(lambda **kwargs: object()),
    )

    prior_api_key = os.environ.get("OPENAI_API_KEY")
    prior_base = os.environ.get("OPENAI_BASE_URL")

    runtime = LoomTerminus2Runtime(
        model=ModelSpec(provider="openai", name="gpt-4"),
        team_id=str(uuid4()),
        trial_id=uuid4(),
        cp_client=_TokenCP(),
        gateway_url="http://127.0.0.1:19100",
    )
    driver = FakeDriver()
    await driver.start()
    writer = TrajectoryWriter(
        local_path=tmp_path / "fail.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )

    with pytest.raises(RuntimeError, match="harbor boom"):
        async with writer:
            await runtime.run(
                instruction="x",
                env=driver,
                trajectory=writer,
                mcp=[],
                skills_dir=PurePosixPath("/workspace"),
                step_id="main",
            )

    assert os.environ.get("OPENAI_API_KEY") == prior_api_key
    assert os.environ.get("OPENAI_BASE_URL") == prior_base
    await driver.stop()
