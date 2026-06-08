"""argparse wiring — no command should print help to stderr, --version
prints the version string."""

from __future__ import annotations

import subprocess
import sys


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "loom_cli", *args],
        check=False, capture_output=True, text=True,
    )


def test_no_args_prints_help_to_stderr_and_exits_2() -> None:
    res = _run()
    assert res.returncode == 2
    assert "usage:" in res.stderr.lower()


def test_help_lists_top_level_subcommands() -> None:
    res = _run("--help")
    assert res.returncode == 0
    for cmd in ("run", "config", "datasets"):
        assert cmd in res.stdout


def test_version_flag_prints_version() -> None:
    res = _run("--version")
    assert res.returncode == 0
    assert res.stdout.strip()
