"""Regression: every fixture under tests/fixtures/tasks/ must parse
against TaskConfig (extra='forbid' would otherwise silently mask schema
drift between docs/agent-authoring-guide.md and the actual Pydantic
models).

Bug 1 from the post-Plan-7 review: in-box-cli/task.toml had `mode =
"in-box"` under [agent], which AgentDefaults rejects. This regression
test catches that and any future fixture-schema drift.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from loom.models.task import TaskConfig

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURES_DIR = _REPO_ROOT / "tests" / "fixtures" / "tasks"


def _all_fixture_dirs() -> list[Path]:
    return sorted(p for p in _FIXTURES_DIR.iterdir() if p.is_dir())


@pytest.mark.parametrize(
    "fixture_dir",
    _all_fixture_dirs(),
    ids=lambda p: p.name,
)
def test_fixture_task_toml_parses(fixture_dir: Path) -> None:
    config = tomllib.loads((fixture_dir / "task.toml").read_text())
    TaskConfig.model_validate(config)
