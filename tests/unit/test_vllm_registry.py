"""Worker-spawned vLLM registry — lifecycle + dedup.

Heavy: doesn't actually launch vLLM (that needs a GPU + the vllm wheel).
Patches `launch_vllm` with a stub that returns a fake VLLMServerInfo so
we exercise the per-model cache, the disabled-state error path, and the
re-entry-after-shutdown semantics."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from loom.errors import AgentError
from loom_worker.vllm_registry import WorkerVLLMRegistry


@dataclass
class _FakeInfo:
    base_url: str
    served_model_name: str
    pid: int


def _install_launch_stub(
    monkeypatch: pytest.MonkeyPatch, *, counter: list[int],
) -> None:
    """Patch loom_cli.vllm_runner.launch_vllm with a stub that records
    invocation count and returns a synthesized VLLMServerInfo."""
    from loom_cli import vllm_runner as vr

    def _stub(spec):  # type: ignore[no-untyped-def]
        counter.append(1)
        return _FakeInfo(
            base_url=f"http://127.0.0.1:{8234 + len(counter)}/v1",
            served_model_name=spec.model,
            pid=10_000 + len(counter),
        )

    monkeypatch.setattr(vr, "launch_vllm", _stub)


async def test_disabled_raises_agent_error() -> None:
    reg = WorkerVLLMRegistry(enabled=False)
    with pytest.raises(AgentError, match="not configured"):
        await reg.get_or_launch("meta-llama/Llama-3-8B-Instruct")


async def test_get_or_launch_spawns_on_first_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counter: list[int] = []
    _install_launch_stub(monkeypatch, counter=counter)
    reg = WorkerVLLMRegistry(enabled=True)
    handle = await reg.get_or_launch("foo/bar")
    assert handle.served_model_name == "foo/bar"
    assert len(counter) == 1


async def test_second_get_reuses_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counter: list[int] = []
    _install_launch_stub(monkeypatch, counter=counter)
    reg = WorkerVLLMRegistry(enabled=True)
    h1 = await reg.get_or_launch("foo/bar")
    h2 = await reg.get_or_launch("foo/bar")
    assert h1.pid == h2.pid
    assert len(counter) == 1, "second get_or_launch should not respawn"


async def test_different_models_get_separate_servers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counter: list[int] = []
    _install_launch_stub(monkeypatch, counter=counter)
    reg = WorkerVLLMRegistry(enabled=True)
    h_a = await reg.get_or_launch("foo/bar")
    h_b = await reg.get_or_launch("baz/qux")
    assert h_a.pid != h_b.pid
    assert len(counter) == 2


async def test_concurrent_calls_serialize_to_one_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two trials claiming the same model concurrently must dedupe to
    a single launch — otherwise they both spawn vLLM, race for the
    same port, and one OOMs."""
    counter: list[int] = []

    from loom_cli import vllm_runner as vr

    async def _slow_launch(spec):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.05)
        counter.append(1)
        return _FakeInfo(
            base_url=f"http://127.0.0.1:{8234 + len(counter)}/v1",
            served_model_name=spec.model,
            pid=10_000 + len(counter),
        )

    # to_thread will call the sync function, so wrap it.
    def _sync_launch(spec):  # type: ignore[no-untyped-def]
        return asyncio.run(_slow_launch(spec))

    monkeypatch.setattr(vr, "launch_vllm", _sync_launch)
    reg = WorkerVLLMRegistry(enabled=True)
    a, b, c = await asyncio.gather(
        reg.get_or_launch("foo/bar"),
        reg.get_or_launch("foo/bar"),
        reg.get_or_launch("foo/bar"),
    )
    assert a.pid == b.pid == c.pid
    assert len(counter) == 1


async def test_shutdown_clears_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counter: list[int] = []
    _install_launch_stub(monkeypatch, counter=counter)
    # Stub the killer so we don't actually try to SIGKILL fake pids.
    from loom_cli import vllm_runner as vr
    monkeypatch.setattr(vr, "_stop_process", lambda proc: None)
    monkeypatch.setattr(vr, "_LIVE_PROCESSES", [])
    reg = WorkerVLLMRegistry(enabled=True)
    await reg.get_or_launch("foo/bar")
    await reg.shutdown()
    # Re-fetch should respawn.
    await reg.get_or_launch("foo/bar")
    assert len(counter) == 2
