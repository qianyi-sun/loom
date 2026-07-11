"""Terminal-Bench task.toml normalizer (#341).

Verifies the mapping rules that make Terminal-Bench-shaped bundles
loadable by ``loom datasets publish-local`` without operator-side
conversion.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from loom.models.task import TaskConfig
from loom_cli.terminal_bench_normalize import (
    DEFAULT_AGENT_TIMEOUT_SEC,
    DEFAULT_VERIFIER_SCRIPT_PATH,
    DEFAULT_VERIFIER_TIMEOUT_SEC,
    is_terminal_bench_shape,
    normalize_terminal_bench_task_toml,
)


def _tb_raw(**environment_extras: object) -> dict[str, Any]:
    """A minimal TB-shaped task.toml dict."""
    return {
        "version": "1",
        "metadata": {"id": "src-useful/task-1", "name": "Task One"},
        "environment": {
            "cpus": 2,
            "memory": "4G",
            "storage": "10G",
            "dockerfile": "Dockerfile",
            **environment_extras,
        },
    }


class TestIsTerminalBenchShape:
    def test_true_when_metadata_present_and_task_absent(self) -> None:
        assert is_terminal_bench_shape(_tb_raw()) is True

    def test_false_when_task_section_present(self) -> None:
        raw = _tb_raw()
        raw["task"] = {"id": "foo", "name": "bar"}
        assert is_terminal_bench_shape(raw) is False

    def test_false_when_no_metadata_section(self) -> None:
        raw = {"task": {"id": "foo", "name": "bar"}}
        assert is_terminal_bench_shape(raw) is False

    def test_false_when_metadata_is_not_a_dict(self) -> None:
        assert is_terminal_bench_shape({"metadata": "oops"}) is False


class TestNormalizeMapping:
    def test_normalizes_native_tb21_schema_1_1_task_toml(self) -> None:
        """Harbor-native TB2.1 bundles use ``schema_version = \"1.1\"``
        plus a ``[task]`` section whose name is the upstream identity, rather
        than Loom's schema-1 task id.  The converter must be able to re-stamp
        that identity without throwing away the supported execution contract.
        """
        raw = {
            "schema_version": "1.1",
            "artifacts": [],
            "task": {
                "name": "terminal-bench/adaptive-rejection-sampler",
                "description": "Native TB2.1 task.",
                "keywords": ["terminal", "statistics"],
            },
            "verifier": {
                "timeout_sec": 900.0,
                "env": {"KEEP_NATIVE_VERIFIER_ENV": "1"},
            },
            "agent": {"timeout_sec": 900.0},
            "environment": {
                "build_timeout_sec": 600.0,
                "docker_image": "example/tb21:rev6",
                "cpus": 1,
                "memory_mb": 2048,
                "storage_mb": 10240,
                "gpus": 0,
                "allow_internet": True,
                "architecture": "x86_64",
                "env": {"NATIVE_ENV": "preserve-supported-values"},
            },
            "solution": {"env": {"REFERENCE_ONLY": "true"}},
        }

        normalized = normalize_terminal_bench_task_toml(raw)
        cfg = TaskConfig.model_validate(normalized)

        assert cfg.task.id == "terminal-bench/adaptive-rejection-sampler"
        assert cfg.task.name == "terminal-bench/adaptive-rejection-sampler"
        assert cfg.task.description == "Native TB2.1 task."
        assert cfg.task.labels == ["terminal", "statistics"]
        assert cfg.environment.os == "linux"
        assert cfg.environment.docker_image == "example/tb21:rev6"
        assert cfg.environment.build_timeout_sec == 600.0
        assert cfg.environment.workdir.as_posix() == "/app"
        assert cfg.environment.environment == {
            "NATIVE_ENV": "preserve-supported-values",
        }
        assert cfg.agent.name == "oracle"
        assert cfg.agent.timeout_sec == 900.0
        assert cfg.verifier.name == "script"
        assert cfg.verifier.timeout_sec == 900.0
        assert cfg.verifier.args == {
            "script_path": DEFAULT_VERIFIER_SCRIPT_PATH,
        }
        assert cfg.steps[0].artifacts == ["logs/verifier/**"]
        assert raw["schema_version"] == "1.1"
        assert raw["environment"]["architecture"] == "x86_64"

    def test_native_tb21_appends_verifier_artifact_glob_without_replacing_source_patterns(
        self,
    ) -> None:
        raw = {
            "schema_version": "1.1",
            "artifacts": ["result.json"],
            "task": {"name": "terminal-bench/with-artifacts"},
            "environment": {"docker_image": "example/tb21:rev6"},
        }

        normalized = normalize_terminal_bench_task_toml(raw)
        cfg = TaskConfig.model_validate(normalized)

        assert cfg.steps[0].artifacts == ["result.json", "logs/verifier/**"]
        assert raw["artifacts"] == ["result.json"]

    def test_produces_valid_loom_taskconfig(self) -> None:
        normalized = normalize_terminal_bench_task_toml(_tb_raw())
        cfg = TaskConfig.model_validate(normalized)
        assert cfg.task.id == "src-useful/task-1"
        assert cfg.task.name == "Task One"
        assert cfg.environment.os == "linux"
        assert cfg.environment.dockerfile.as_posix() == "Dockerfile"
        assert cfg.agent.name == "oracle"
        assert cfg.agent.timeout_sec == DEFAULT_AGENT_TIMEOUT_SEC
        assert cfg.verifier.name == "script"
        assert cfg.verifier.timeout_sec == DEFAULT_VERIFIER_TIMEOUT_SEC
        assert cfg.verifier.args == {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH}

    def test_drops_top_level_version(self) -> None:
        normalized = normalize_terminal_bench_task_toml(_tb_raw())
        assert "version" not in normalized
        assert normalized["schema_version"] == "1"

    def test_drops_unsupported_environment_resource_fields(self) -> None:
        normalized = normalize_terminal_bench_task_toml(_tb_raw())
        env = normalized["environment"]
        assert "cpus" not in env
        assert "memory" not in env
        assert "storage" not in env
        assert env["dockerfile"] == "Dockerfile"

    def test_preserves_other_environment_fields(self) -> None:
        raw = _tb_raw(workdir="/task", docker_build_context=".")
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["environment"]["workdir"] == "/task"
        assert normalized["environment"]["docker_build_context"] == "."

    def test_metadata_name_falls_back_to_id(self) -> None:
        raw = _tb_raw()
        del raw["metadata"]["name"]
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["task"]["name"] == "src-useful/task-1"

    def test_metadata_description_promoted_to_task_description(self) -> None:
        raw = _tb_raw()
        raw["metadata"]["description"] = "Do the thing."
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["task"]["description"] == "Do the thing."

    def test_metadata_tags_become_task_labels(self) -> None:
        raw = _tb_raw()
        raw["metadata"]["tags"] = ["frontier", "coding"]
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["task"]["labels"] == ["frontier", "coding"]

    def test_explicit_agent_choices_are_preserved(self) -> None:
        raw = _tb_raw()
        raw["agent"] = {"name": "opencode", "timeout_sec": 1200.0}
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["agent"]["name"] == "opencode"
        assert normalized["agent"]["timeout_sec"] == 1200.0

    def test_explicit_verifier_script_path_preserved(self) -> None:
        raw = _tb_raw()
        raw["verifier"] = {"args": {"script_path": "/custom.sh"}}
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized["verifier"]["args"]["script_path"] == "/custom.sh"
        # name defaulted, timeout defaulted
        assert normalized["verifier"]["name"] == "script"

    def test_does_not_mutate_input(self) -> None:
        raw = _tb_raw()
        snapshot = repr(raw)
        _ = normalize_terminal_bench_task_toml(raw)
        assert repr(raw) == snapshot

    def test_idempotent_on_already_loom_shaped_input(self) -> None:
        raw = {
            "schema_version": "1",
            "task": {"id": "t/1", "name": "T"},
            "environment": {"os": "linux", "dockerfile": "Dockerfile"},
            "agent": {"name": "oracle", "timeout_sec": 360.0},
            "verifier": {
                "name": "script",
                "timeout_sec": 60.0,
                "args": {"script_path": "/x.sh"},
            },
        }
        normalized = normalize_terminal_bench_task_toml(raw)
        assert normalized == raw


class TestErrorPaths:
    def test_missing_metadata_id_produces_invalid_taskconfig(self) -> None:
        """The normalizer promotes what's there; a bundle missing
        metadata.id becomes a TaskConfig missing task.id, which is
        rejected at validation time — the operator sees the same field
        name from Loom's schema either way."""
        raw = _tb_raw()
        del raw["metadata"]["id"]
        del raw["metadata"]["name"]
        normalized = normalize_terminal_bench_task_toml(raw)
        with pytest.raises(ValidationError):
            TaskConfig.model_validate(normalized)
