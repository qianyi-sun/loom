"""SkillFlow + SkillLearnBench adapter contract (Plan 15 Phase 11).

Both adapters share the same passthrough behavior; one parametrized
test exercises both."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from loom_benchmarks.adapters.skillflow import SkillFlowAdapter
from loom_benchmarks.adapters.skilllearnbench import SkillLearnBenchAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_name"),
    [
        (SkillFlowAdapter, "skillflow"),
        (SkillLearnBenchAdapter, "skilllearnbench"),
    ],
)
def test_skill_adapter_passthrough(
    adapter_cls: type, fixture_name: str, tmp_path: Path,
) -> None:
    rec = json.loads(
        (
            Path(__file__).parent / "fixtures" / fixture_name / "sample.json"
        ).read_text(),
    )[0]
    adapter = adapter_cls()
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    adapter.convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "pytest"
    assert cfg.task.id.startswith(f"{adapter.name}/")
    assert (tmp_path / "instruction.md").exists()
    assert (tmp_path / "tests" / "test_main.py").exists()


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_name"),
    [
        (SkillFlowAdapter, "skillflow"),
        (SkillLearnBenchAdapter, "skilllearnbench"),
    ],
)
def test_skill_solution_runs(
    adapter_cls: type, fixture_name: str, tmp_path: Path,
) -> None:
    """The pre-baked solution + tests in the upstream bundle are
    expected to pass under pytest end-to-end."""
    rec = json.loads(
        (
            Path(__file__).parent / "fixtures" / fixture_name / "sample.json"
        ).read_text(),
    )[0]
    adapter = adapter_cls()
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    adapter.convert_instance(inst, out_dir=tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


def _write_real_bundle(
    root: Path,
    *,
    family: str,
    task: str,
    base_image: str = "ubuntu:24.04",
    copy_skills: bool = True,
) -> Path:
    bundle = root / "repo" / "tasks" / family / task
    (bundle / "environment").mkdir(parents=True)
    (bundle / "tests").mkdir()
    dockerfile = (
        f"FROM {base_image}\n"
        "RUN apt-get update && apt-get install -y python3\n"
        "WORKDIR /root\n"
    )
    if copy_skills:
        dockerfile += "COPY skills /root/.codex/skills\n"
    else:
        dockerfile += "COPY task_input.txt /root/task_input.txt\n"
        (bundle / "environment" / "task_input.txt").write_text("input\n")
    (bundle / "environment" / "Dockerfile").write_text(dockerfile)
    (bundle / "task.toml").write_text(
        "[task]\n"
        f"id = \"{task}\"\n"
        "name = \"Real upstream task\"\n"
        "category = \"Utilities\"\n"
        "difficulty = \"Easy\"\n"
        "\n"
        "[evaluation]\n"
        "required_files = [\"/root/result.txt\"]\n",
    )
    (bundle / "instruction.md").write_text("Create /root/result.txt.\n")
    (bundle / "tests" / "test.sh").write_text(
        "#!/bin/bash\n"
        "mkdir -p /logs/verifier\n"
        "echo 1 > /logs/verifier/reward.txt\n",
    )
    return bundle


def test_skilllearnbench_lists_real_upstream_bundle_layout(tmp_path: Path) -> None:
    _write_real_bundle(
        tmp_path,
        family="stock-data-visualization",
        task="stock-data-visualization-1",
    )

    instances = list(
        SkillLearnBenchAdapter().list_instances(source_dir=tmp_path, split="test"),
    )

    assert [i.instance_id for i in instances] == [
        "stock-data-visualization/stock-data-visualization-1",
    ]
    assert instances[0].raw["__source_path"].endswith(
        "stock-data-visualization-1",
    )


def test_skilllearnbench_converts_real_bundle_to_loom_task_config(
    tmp_path: Path,
) -> None:
    bundle = _write_real_bundle(
        tmp_path,
        family="stock-data-visualization",
        task="stock-data-visualization-1",
    )
    adapter = SkillLearnBenchAdapter()
    inst = BenchmarkInstance(
        instance_id="stock-data-visualization/stock-data-visualization-1",
        split="test",
        raw={"__source_path": str(bundle)},
    )
    out_dir = tmp_path / "out"

    converted = adapter.convert_instance(inst, out_dir=out_dir)
    cfg = TaskConfig.model_validate(
        tomllib.loads((out_dir / "task.toml").read_text()),
    )

    assert converted.task_id == (
        "skilllearnbench/stock-data-visualization/stock-data-visualization-1"
    )
    assert cfg.task.id == converted.task_id
    assert cfg.environment.dockerfile.as_posix() == "environment/Dockerfile"
    assert cfg.environment.docker_build_context.as_posix() == "."
    assert cfg.environment.workdir.as_posix() == "/root"
    assert cfg.environment.user == "root"
    assert cfg.verifier.name == "script"
    assert cfg.verifier.args["script_path"] == "/root/verifier/run.sh"
    assert (out_dir / "skills" / ".keep").exists()
    run_sh = (out_dir / "verifier" / "run.sh").read_text()
    assert "LOOM_VERIFIER_OUTPUT" in run_sh
    assert "/logs/verifier/reward.txt" in run_sh


def test_skillflow_uses_environment_build_context_for_environment_assets(
    tmp_path: Path,
) -> None:
    bundle = _write_real_bundle(
        tmp_path,
        family="workflow",
        task="environment-context-task",
        copy_skills=False,
    )
    adapter = SkillFlowAdapter()
    inst = BenchmarkInstance(
        instance_id="workflow/environment-context-task",
        split="test",
        raw={"__source_path": str(bundle)},
    )
    out_dir = tmp_path / "out"

    adapter.convert_instance(inst, out_dir=out_dir)
    cfg = TaskConfig.model_validate(
        tomllib.loads((out_dir / "task.toml").read_text()),
    )

    assert cfg.environment.dockerfile.as_posix() == "environment/Dockerfile"
    assert cfg.environment.docker_build_context.as_posix() == "environment"
    assert not (out_dir / "skills" / ".keep").exists()


def test_skilllearnbench_uses_root_build_context_for_root_assets(
    tmp_path: Path,
) -> None:
    bundle = _write_real_bundle(
        tmp_path,
        family="enterprise-information-search",
        task="enterprise-information-search-1",
        copy_skills=False,
    )
    (bundle / "DATA").mkdir()
    (bundle / "DATA" / "records.json").write_text("[]\n")
    (bundle / "environment" / "Dockerfile").write_text(
        "FROM ubuntu:24.04\n"
        "WORKDIR /root\n"
        "COPY DATA /root/DATA\n",
    )
    adapter = SkillLearnBenchAdapter()
    inst = BenchmarkInstance(
        instance_id="enterprise-information-search/enterprise-information-search-1",
        split="test",
        raw={"__source_path": str(bundle)},
    )
    out_dir = tmp_path / "out"

    adapter.convert_instance(inst, out_dir=out_dir)
    cfg = TaskConfig.model_validate(
        tomllib.loads((out_dir / "task.toml").read_text()),
    )

    assert cfg.environment.dockerfile.as_posix() == "environment/Dockerfile"
    assert cfg.environment.docker_build_context.as_posix() == "."
    assert (out_dir / "DATA" / "records.json").read_text() == "[]\n"


def test_skillflow_rewrites_unpublished_harbor_base_image(
    tmp_path: Path,
) -> None:
    bundle = _write_real_bundle(
        tmp_path,
        family="workflow",
        task="base-image-task",
        base_image="skillevlove/harbor-cli-openhands:ubuntu24.04",
    )
    adapter = SkillFlowAdapter()
    inst = BenchmarkInstance(
        instance_id="workflow/base-image-task",
        split="test",
        raw={"__source_path": str(bundle)},
    )
    out_dir = tmp_path / "out"

    adapter.convert_instance(inst, out_dir=out_dir)

    dockerfile = (out_dir / "environment" / "Dockerfile").read_text()
    assert dockerfile.startswith("FROM skillflow/harbor-cli-base:ubuntu24.04\n")


def test_skillflow_normalizes_absolute_solution_paths_for_oracle_layout(
    tmp_path: Path,
) -> None:
    bundle = _write_real_bundle(
        tmp_path,
        family="workflow",
        task="absolute-solution-path-task",
    )
    (bundle / "solution").mkdir()
    (bundle / "solution" / "answer.xlsx").write_text("answer\n")
    (bundle / "solution" / "solve.sh").write_text(
        "#!/bin/bash\n"
        "set -euo pipefail\n"
        "cp /solution/answer.xlsx result.xlsx\n",
    )
    adapter = SkillFlowAdapter()
    inst = BenchmarkInstance(
        instance_id="workflow/absolute-solution-path-task",
        split="test",
        raw={"__source_path": str(bundle)},
    )
    out_dir = tmp_path / "out"

    adapter.convert_instance(inst, out_dir=out_dir)
    root_solve = out_dir / "solve.sh"
    root_solve.write_text((out_dir / "solution" / "solve.sh").read_text())
    result = subprocess.run(
        ["bash", "solve.sh"],
        cwd=out_dir,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    assert (out_dir / "result.xlsx").read_text() == "answer\n"
    assert "cp /solution/" not in root_solve.read_text()


def test_skillflow_points_at_published_task_dataset() -> None:
    adapter = SkillFlowAdapter()

    assert adapter.upstream_source.kind == "git"
    assert adapter.upstream_source.locator == (
        "https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task"
    )


def test_skillflow_lists_hf_task_dataset_layout(tmp_path: Path) -> None:
    bundle = tmp_path / "repo" / "test_tasks" / "Workflow" / "01_task"
    (bundle / "tests").mkdir(parents=True)
    (bundle / "task.toml").write_text(
        "[task]\n"
        "id = \"01_task\"\n"
        "name = \"HF task\"\n",
    )
    (bundle / "instruction.md").write_text("Do the task.\n")
    (bundle / "tests" / "test.sh").write_text(
        "#!/bin/bash\n"
        "mkdir -p /logs/verifier\n"
        "echo 0 > /logs/verifier/reward.txt\n",
    )
    unsafe_bundle = (
        tmp_path
        / "repo"
        / "test_tasks"
        / "Inventory-&-Finance-Integration"
        / "harbor gdpval 21"
    )
    (unsafe_bundle / "tests").mkdir(parents=True)
    (unsafe_bundle / "task.toml").write_text(
        "[task]\n"
        "id = \"harbor gdpval 21\"\n"
        "name = \"Unsafe path task\"\n",
    )
    (unsafe_bundle / "instruction.md").write_text("Do the unsafe path task.\n")
    (unsafe_bundle / "tests" / "test.sh").write_text(
        "#!/bin/bash\n"
        "mkdir -p /logs/verifier\n"
        "echo 0 > /logs/verifier/reward.txt\n",
    )

    instances = list(
        SkillFlowAdapter().list_instances(source_dir=tmp_path, split="test"),
    )

    assert [i.instance_id for i in instances] == [
        "Inventory-_-Finance-Integration/harbor_gdpval_21",
        "Workflow/01_task",
    ]
