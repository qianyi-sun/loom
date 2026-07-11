"""Import hygiene for `loom_cli.model_spec` (issue #94)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_model_spec_import_does_not_pull_benchmark_fetch_deps() -> None:
    """Parser helpers must not require optional benchmark/datasets deps."""
    repo_root = Path(__file__).resolve().parents[2]
    src = repo_root / "src"
    code = """
import importlib.abc
import sys

class _Blocker(importlib.abc.MetaPathFinder):
    _blocked = ("datasets", "loom_benchmarks", "loom_cli.task_loader")

    def find_module(self, fullname, path=None):
        for blocked in self._blocked:
            if fullname == blocked or fullname.startswith(f"{blocked}."):
                return self
        return None

    def load_module(self, fullname):
        raise ImportError(f"blocked import: {fullname}")

sys.meta_path.insert(0, _Blocker())

from loom_cli.model_spec import parse_model

spec = parse_model("anthropic/claude-opus-4-7")
assert spec.provider == "anthropic"
assert spec.name == "claude-opus-4-7"
"""
    env = {**os.environ, "PYTHONPATH": str(src)}
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        "model_spec import pulled benchmark/datasets deps\\n"
        f"stdout:\\n{result.stdout}\\n"
        f"stderr:\\n{result.stderr}"
    )
