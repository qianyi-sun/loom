"""vllm_runner — subprocess lifecycle, dep-check, health-probe, cleanup.

vLLM itself is a heavy GPU dep; these tests use mocks throughout.
Coverage: the orchestration glue (find-free-port + cmd construction +
poll-until-healthy + graceful-stop), not the vLLM process itself.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import loom_cli.vllm_runner as vr


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner has module-level state (live-process list, signal-
    handler-installed flag). Reset between tests so order doesn't
    matter."""
    vr._LIVE_PROCESSES.clear()
    monkeypatch.setattr(vr, "_SIGNAL_HANDLERS_INSTALLED", False)


def test_missing_vllm_dep_raises_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vr.shutil, "which", lambda _: None)
    with pytest.raises(vr.MissingVLLMDependencyError) as exc:
        vr.launch_vllm(vr.VLLMLaunchSpec(model="meta-llama/Llama-3.1-8B"))
    assert "pip install loom[vllm]" in str(exc.value)


def test_resolve_model_path_passes_through_hf_id() -> None:
    # HuggingFace ids look like `org/name` — no slash-prefix to
    # canonicalize, just pass through.
    assert vr._resolve_model_path("meta-llama/Llama-3.1-8B") == "meta-llama/Llama-3.1-8B"


def test_resolve_model_path_rejects_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    with pytest.raises(RuntimeError, match="does not exist"):
        vr._resolve_model_path(str(missing))


def test_resolve_model_path_accepts_existing_dir(tmp_path: Path) -> None:
    d = tmp_path / "weights"
    d.mkdir()
    resolved = vr._resolve_model_path(str(d))
    assert Path(resolved) == d.resolve()


def test_build_cmd_includes_required_flags() -> None:
    spec = vr.VLLMLaunchSpec(
        model="meta-llama/Llama-3.1-8B",
        gpu_memory_utilization=0.85,
        tensor_parallel_size=2,
        max_model_len=8192,
        enforce_eager=True,
        extra_args=("--served-model-name", "mymodel"),
    )
    cmd = vr._build_cmd("meta-llama/Llama-3.1-8B", port=8234, spec=spec)
    assert cmd[0:3] == ["vllm", "serve", "meta-llama/Llama-3.1-8B"]
    assert "--port" in cmd
    assert "8234" in cmd
    assert "--gpu-memory-utilization" in cmd
    assert "0.85" in cmd
    assert "--tensor-parallel-size" in cmd
    assert "2" in cmd
    assert "--max-model-len" in cmd
    assert "8192" in cmd
    assert "--enforce-eager" in cmd
    assert "--served-model-name" in cmd
    assert "mymodel" in cmd


def test_find_free_port_skips_bound() -> None:
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", 0))
    bound_port = s.getsockname()[1]
    try:
        # _find_free_port starts at our bound port; should return the
        # next free one (not the bound one).
        found = vr._find_free_port(bound_port)
        assert found != bound_port
        assert found > bound_port
    finally:
        s.close()


def test_wait_for_ready_returns_on_200(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_resp = MagicMock(status_code=200)
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake_resp)

    proc = MagicMock()
    proc.poll.return_value = None  # still running
    # Should not raise
    vr._wait_for_ready("http://localhost:8000/v1", proc, timeout_sec=5.0)


def test_wait_for_ready_fails_fast_on_dead_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc = MagicMock()
    proc.poll.return_value = 1  # exited with code 1
    proc.returncode = 1
    with pytest.raises(RuntimeError, match="exited prematurely"):
        vr._wait_for_ready("http://localhost:8000/v1", proc, timeout_sec=5.0)


def test_wait_for_ready_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    import httpx
    proc = MagicMock()
    proc.poll.return_value = None  # never exits
    proc.pid = 12345

    def _refused(*_a: Any, **_kw: Any) -> Any:
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(vr.httpx, "get", _refused)
    # Tiny timeout so this test is fast
    monkeypatch.setattr(vr.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="did not become healthy"):
        vr._wait_for_ready("http://localhost:8000/v1", proc, timeout_sec=0.01)


def test_query_served_model_name_returns_first_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = MagicMock()
    fake.json.return_value = {"data": [{"id": "vllm-served-name"}]}
    fake.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake)
    assert vr._query_served_model_name("http://localhost:8000/v1") == "vllm-served-name"


def test_query_served_model_name_errors_on_empty_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = MagicMock()
    fake.json.return_value = {"data": []}
    fake.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake)
    with pytest.raises(RuntimeError, match="no entries"):
        vr._query_served_model_name("http://localhost:8000/v1")


def test_stop_process_graceful_then_kill(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = MagicMock()
    proc.poll.return_value = None  # alive
    # First wait() times out → escalate to kill
    proc.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="vllm", timeout=30),
        0,  # second wait after kill succeeds
    ]
    vr._stop_process(proc)
    proc.terminate.assert_called_once()
    proc.kill.assert_called_once()


def test_stop_process_noop_if_already_exited() -> None:
    proc = MagicMock()
    proc.poll.return_value = 0  # already done
    vr._stop_process(proc)
    proc.terminate.assert_not_called()
    proc.kill.assert_not_called()


def test_stop_all_drains_live_processes() -> None:
    p1 = MagicMock()
    p1.poll.return_value = None
    p1.wait.return_value = 0
    p2 = MagicMock()
    p2.poll.return_value = None
    p2.wait.return_value = 0
    vr._LIVE_PROCESSES.extend([p1, p2])

    vr.stop_all()

    assert vr._LIVE_PROCESSES == []
    p1.terminate.assert_called_once()
    p2.terminate.assert_called_once()


def test_launch_vllm_end_to_end_with_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The orchestration path: dep-check passes, subprocess starts,
    health check returns 200, served-model-name is queried, info is
    returned, atexit hook is installed."""
    monkeypatch.setattr(vr.shutil, "which", lambda _: "/usr/bin/vllm")

    fake_proc = MagicMock()
    fake_proc.poll.return_value = None
    fake_proc.pid = 99999
    monkeypatch.setattr(vr.subprocess, "Popen", lambda *a, **kw: fake_proc)

    fake_resp = MagicMock(status_code=200)
    fake_resp.json.return_value = {"data": [{"id": "meta-llama/Llama-3.1-8B"}]}
    fake_resp.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake_resp)

    info = vr.launch_vllm(vr.VLLMLaunchSpec(
        model="meta-llama/Llama-3.1-8B", port=18234,
    ))
    assert info.base_url == "http://localhost:18234/v1"
    assert info.served_model_name == "meta-llama/Llama-3.1-8B"
    assert info.pid == 99999
    assert fake_proc in vr._LIVE_PROCESSES


def test_launch_vllm_cleans_up_on_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If wait_for_ready raises (server didn't come up), `_stop_process`
    is invoked on the subprocess rather than being orphaned. (When
    the subprocess died on its own, _stop_process detects this and
    no-ops — but the launch function still called it.)"""
    monkeypatch.setattr(vr.shutil, "which", lambda _: "/usr/bin/vllm")

    fake_proc = MagicMock()
    fake_proc.poll.return_value = 1
    fake_proc.returncode = 1
    fake_proc.pid = 99998
    monkeypatch.setattr(vr.subprocess, "Popen", lambda *a, **kw: fake_proc)

    stop_calls: list[object] = []
    monkeypatch.setattr(vr, "_stop_process", lambda p: stop_calls.append(p))

    with pytest.raises(RuntimeError, match="exited prematurely"):
        vr.launch_vllm(vr.VLLMLaunchSpec(
            model="meta-llama/Llama-3.1-8B", port=18235,
        ))
    # _stop_process was called on the failed subprocess
    assert stop_calls == [fake_proc]
