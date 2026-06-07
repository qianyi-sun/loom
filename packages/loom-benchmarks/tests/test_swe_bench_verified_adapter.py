"""SWE-Bench Verified adapter contract (Plan 15 Phase 1)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = (
    Path(__file__).parent
    / "fixtures" / "swe_bench_verified" / "sample.json"
)


def test_convert_emits_valid_task(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    adapter = SWEBenchVerifiedAdapter()
    converted = adapter.convert_instance(inst, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id == "swe-bench-verified/django__django-12345"
    assert cfg.verifier.name == "pytest"
    assert "swebench/sweb.eval.x86_64" in (cfg.environment.docker_image or "")
    assert (tmp_path / "instruction.md").read_text().startswith(
        "Querysets with `.filter",
    )

    solve_sh = (tmp_path / "solution" / "solve.sh").read_text()
    assert "git apply" in solve_sh
    assert "django/db/models/query.py" in solve_sh

    test_py = (tmp_path / "tests" / "test_swebench.py").read_text()
    assert "test_empty_in_short_circuits" in test_py
    assert "test_basic_filter" in test_py
    assert converted.license_spdx == "MIT"


def test_solve_sh_is_executable(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchVerifiedAdapter().convert_instance(inst, out_dir=tmp_path)
    assert (tmp_path / "solution" / "solve.sh").stat().st_mode & 0o111


def test_image_slug_replaces_double_underscore() -> None:
    from loom_benchmarks.adapters.swe_bench_verified import _image_for
    assert "_1776_" in _image_for("django__django-12345")
    assert "django__" not in _image_for("django__django-12345")
