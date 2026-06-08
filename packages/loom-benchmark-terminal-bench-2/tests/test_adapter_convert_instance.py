"""convert_instance writes Loom's canonical task layout for a TB-2 task."""

from __future__ import annotations

import shutil
import tomllib
from pathlib import Path

import pytest
from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig


@pytest.fixture
def hello_world_instance(
    fixtures_dir: Path, tmp_path: Path,
) -> BenchmarkInstance:
    """Stage the vendored fixture as a source-tree task so the adapter
    can read its on-disk auxiliaries through `__source_path`."""
    staged = tmp_path / "tasks" / "hello-world"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    adapter = TerminalBench2Adapter()
    (only,) = list(adapter.list_instances(source_dir=tmp_path, split="test"))
    return only


def test_convert_writes_instruction_md(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    text = (out / "instruction.md").read_text()
    assert text.startswith("Create a file called hello.txt")
    assert text.endswith("\n")


def test_convert_writes_task_toml_with_required_fields(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.task.id == "terminal-bench-2/hello-world"
    assert cfg.task.name.endswith("hello-world")
    assert cfg.environment.os == "linux"
    assert cfg.environment.docker_image is not None
    assert cfg.verifier.name == "script"
    assert cfg.agent.name == "oracle"


def test_convert_task_toml_id_escapes_special_chars(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """toml_string-escaped instance_ids cannot break the TOML document."""
    inst = BenchmarkInstance(
        instance_id='weird"name',
        split="test",
        raw={
            "instruction": "hi",
            "parser_name": "pytest",
            "max_agent_timeout_sec": 1.0,
            "max_test_timeout_sec": 1.0,
            "__source_path": str(fixtures_dir / "tb2-task-hello-world"),
        },
    )
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(inst, out_dir=out)
    parsed = tomllib.loads((out / "task.toml").read_text())
    assert parsed["task"]["id"] == 'terminal-bench-2/weird"name'


def test_convert_copies_tb2_test_tree(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    tb2 = out / "environment" / "tb2-tests"
    assert (tb2 / "test_outputs.py").read_text().startswith("from pathlib")
    assert (tb2 / "run-uv-pytest.sh").exists()
    assert (tb2 / "setup-uv-pytest.sh").exists()
    assert (tb2 / "run-tests.sh").read_text().startswith("#!/bin/bash")


def test_convert_writes_verifier_shim(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    shim = out / "verifier" / "run.sh"
    assert shim.exists()
    assert shim.stat().st_mode & 0o111  # executable
    body = shim.read_text()
    assert "$LOOM_VERIFIER_OUTPUT" in body
    assert "run-tests.sh" in body
    assert '"rewards":' in body


@pytest.fixture
def multiservice_instance(
    fixtures_dir: Path, tmp_path: Path,
) -> BenchmarkInstance:
    staged = tmp_path / "tasks" / "ssh-flag"
    shutil.copytree(fixtures_dir / "tb2-task-multiservice", staged)
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    return only


def test_multiservice_emits_warning(
    multiservice_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    result = TerminalBench2Adapter().convert_instance(
        multiservice_instance, out_dir=out,
    )
    assert any(
        "docker-compose" in w and "single-image" in w for w in result.warnings
    ), result.warnings


def test_multiservice_falls_back_to_client_dockerfile(
    multiservice_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        multiservice_instance, out_dir=out,
    )
    parsed = tomllib.loads((out / "task.toml").read_text())
    assert parsed["environment"]["docker_image"].startswith(
        "ghcr.io/laude-institute/t-bench/",
    )


def test_checksum_stable_across_runs(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """Two converts of the same source produce identical checksums."""
    staged = tmp_path / "tasks" / "chess-best-move"
    shutil.copytree(fixtures_dir / "tb2-task-chess-best-move", staged)
    adapter = TerminalBench2Adapter()
    (inst,) = list(adapter.list_instances(source_dir=tmp_path, split="test"))

    a = tmp_path / "out-a"
    b = tmp_path / "out-b"
    ca = adapter.convert_instance(inst, out_dir=a)
    cb = adapter.convert_instance(inst, out_dir=b)
    assert ca.checksum == cb.checksum
    assert ca.task_id == "terminal-bench-2/chess-best-move"
    assert ca.license_spdx == "Apache-2.0"


def test_convert_skips_symlinks_in_tests_dir(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """A malicious upstream task at some future SHA could ship a symlink
    under tests/ pointing at a host file. _copy_tests must skip symlinks
    so we don't copy host bytes into the converted task."""
    import os

    staged = tmp_path / "tasks" / "evil"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    sensitive = tmp_path / "host-secret.txt"
    sensitive.write_text("DO NOT LEAK")
    os.symlink(sensitive, staged / "tests" / "leaked.txt")
    (only,) = list(TerminalBench2Adapter().list_instances(
        source_dir=tmp_path, split="test",
    ))
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(only, out_dir=out)
    leaked = out / "environment" / "tb2-tests" / "leaked.txt"
    assert not leaked.exists(), "symlink under tests/ was copied through"
