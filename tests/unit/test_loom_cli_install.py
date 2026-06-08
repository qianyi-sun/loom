"""install.py wraps `pip install <spec>` with sys.executable."""

from __future__ import annotations

import subprocess
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom_cli.install import InstallError, install_dataset


def test_install_invokes_pip_with_sys_executable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        captured["cmd"] = list(cmd)
        return MagicMock(
            returncode=0,
            stdout="Successfully installed loom-benchmarks-0.1.0\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.install.subprocess.run", fake_run)
    install_dataset(pip_spec="loom-benchmarks")
    assert captured["cmd"] == [
        sys.executable, "-m", "pip", "install", "loom-benchmarks",
    ]


def test_install_raises_on_pip_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        raise subprocess.CalledProcessError(
            returncode=1, cmd=cmd,
            stderr="ERROR: Could not find a version that satisfies the requirement nope\n",
        )

    monkeypatch.setattr("loom_cli.install.subprocess.run", fake_run)
    with pytest.raises(InstallError) as ei:
        install_dataset(pip_spec="nope")
    assert "Could not find" in str(ei.value)


def test_install_rejects_shell_metacharacters() -> None:
    with pytest.raises(InstallError):
        install_dataset(pip_spec="foo; rm -rf /")
