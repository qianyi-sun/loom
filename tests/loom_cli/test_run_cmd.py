"""End-to-end through `main(["run", ...])` with a stub adapter +
FakeDriver. Verifies fan-out + result printing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_task_loader import _StubAdapter  # reuse stub


def test_run_text_output(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run", _StubAdapter(name="stub-run"),
    )
    rc = main([
        "run",
        "--dataset", "stub-run",
        "--agent", "oracle",
        "--backend", "fake",
        "--concurrency", "2",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc in {0, 1}
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 2
    assert all("stub-run/t" in line for line in lines)
    assert (tmp_path / "out").is_dir()


def test_run_json_output(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run-j", _StubAdapter(name="stub-run-j"),
    )
    rc = main([
        "run",
        "--dataset", "stub-run-j",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
        "--json",
    ])
    assert rc in {0, 1}
    out = capsys.readouterr().out
    parsed = [json.loads(line) for line in out.splitlines() if line.strip()]
    assert len(parsed) == 2
    assert all("trial_id" in obj and "state" in obj for obj in parsed)


def test_run_task_filter(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run-f", _StubAdapter(name="stub-run-f"),
    )
    rc = main([
        "run",
        "--task", "stub-run-f/t2",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc in {0, 1}
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 1
    assert "stub-run-f/t2" in lines[0]


def test_run_unknown_agent_errors_cleanly(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run-u", _StubAdapter(name="stub-run-u"),
    )
    rc = main([
        "run",
        "--dataset", "stub-run-u",
        "--agent", "no-such-agent-zzz",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc == 1
