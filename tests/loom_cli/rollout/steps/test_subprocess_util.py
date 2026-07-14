"""Tests for rollout subprocess capture helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.steps.subprocess_util import run_captured


def test_run_captured_without_stdin_preserves_existing_subprocess_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    def fake_run(argv, **kwargs):
        seen.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.subprocess_util.subprocess.run",
        fake_run,
    )

    result = run_captured(["example", "command"])

    assert result.stdout == "ok"
    assert "input" not in seen
    assert "encoding" not in seen


def test_run_captured_provides_utf8_text_stdin_and_writes_logs(tmp_path: Path) -> None:
    stdout_log = tmp_path / "stdout.log"
    stderr_log = tmp_path / "stderr.log"

    result = run_captured(
        [
            sys.executable,
            "-c",
            (
                "import sys; data = sys.stdin.read(); "
                "sys.stdout.write(data); sys.stderr.write('captured')"
            ),
        ],
        stdin_text="kind: ConfigMap\nname: 测试\n",
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )

    assert result.returncode == 0
    assert result.stdout == "kind: ConfigMap\nname: 测试\n"
    assert result.stderr == "captured"
    assert stdout_log.read_text(encoding="utf-8") == result.stdout
    assert stderr_log.read_text(encoding="utf-8") == result.stderr
