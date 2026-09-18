"""Unit coverage for Nebius Terminus publish-local ingest profile (#1996)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.nebius_terminus_ingest import (
    DEFAULT_CPUS,
    DEFAULT_MEMORY_MB,
    DEFAULT_STORAGE_MB,
    NEBIUS_TERMINUS_PROFILE,
    VERIFIER_SCRIPT_PATH,
    adapt_bundle_for_nebius_terminus,
    offline_verifier_run_sh_bytes,
    preflight_nebius_terminus_admission,
    resolve_execution_profile,
)
from loom.terminal_bench_normalize import (
    DEFAULT_VERIFIER_SCRIPT_PATH,
    normalize_terminal_bench_task_toml,
)

_SEI = {
    "service_execution_input": {
        "schema_version": "loom.service-execution-input.v1",
        "manifest_uri": "s3://artifacts/task-inputs/task-1.json",
        "manifest_sha256": "sha256:" + "d" * 64,
        "file_count": 3,
        "total_bytes": 4096,
    }
}


def _harbor_shaped_config(**env_updates: object) -> dict:
    environment: dict = {
        "os": "linux",
        "dockerfile": "environment/Dockerfile",
        "docker_build_context": "environment",
        "user": "root",
        "workdir": "/home/root",
    }
    environment.update(env_updates)
    return {
        "schema_version": "1",
        "task": {"id": "sample", "name": "Sample"},
        "environment": environment,
        "agent": {"name": "oracle"},
        "verifier": {
            "name": "script",
            "user": "root",
            "env_mode": "separate",
            "args": {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH},
        },
        "steps": [{"name": "main", "instruction_file": "instruction.md"}],
    }


def test_resolve_execution_profile_accepts_known_and_rejects_unknown() -> None:
    assert resolve_execution_profile(None) is None
    assert resolve_execution_profile(NEBIUS_TERMINUS_PROFILE) == NEBIUS_TERMINUS_PROFILE
    with pytest.raises(ValueError, match="unknown execution profile"):
        resolve_execution_profile("gb10-magic")


def test_adapt_fills_resources_forces_gateway_and_verifier(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    (staged / "tests").mkdir()
    (staged / "tests" / "test_outputs.py").write_text("def test_ok():\n    assert True\n")

    adapted, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())

    env = adapted["environment"]
    assert env["cpus"] == DEFAULT_CPUS
    assert env["memory_mb"] == DEFAULT_MEMORY_MB
    assert env["storage_mb"] == DEFAULT_STORAGE_MB
    assert env["cpu_arch"] == "x86_64"
    assert env["user"] == "agent"
    assert env["workdir"] == "/app"
    assert env["network_policies_supported"] == ["gateway-only"]
    assert env["baseline_network_policy"] == {"kind": "gateway-only"}
    assert "user" not in adapted["verifier"]
    assert adapted["verifier"]["env_mode"] == "shared"
    assert adapted["verifier"]["args"]["script_path"] == VERIFIER_SCRIPT_PATH
    assert stats.resources_filled
    assert stats.network_forced_gateway_only
    assert stats.verifier_identity_stripped
    assert stats.verifier_path_forced
    assert stats.cpu_arch_forced
    assert stats.workspace_identity_forced
    assert stats.verifier_wrapper_installed
    wrapper = staged / "verifier" / "run.sh"
    assert wrapper.is_file()
    assert wrapper.stat().st_mode & 0o111
    body = wrapper.read_bytes()
    assert b"tests/test.sh" in body
    assert b"/opt/verifier/bin/pytest" not in body


def test_adapt_preserves_complete_resource_triple(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    adapted, stats = adapt_bundle_for_nebius_terminus(
        staged,
        _harbor_shaped_config(cpus=2, memory_mb=4096, storage_mb=8192),
    )
    env = adapted["environment"]
    assert env["cpus"] == 2
    assert env["memory_mb"] == 4096
    assert env["storage_mb"] == 8192
    assert not stats.resources_filled


def test_adapt_replaces_harbor_online_bridge_wrapper(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    verifier = staged / "verifier"
    verifier.mkdir()
    online = verifier / "run.sh"
    online.write_text(
        "#!/bin/sh\n"
        "echo 'harbor loom bridge: tests/test.sh not found' >&2\n"
        "python3 -m pip install -q pytest\n",
        encoding="utf-8",
    )
    online.chmod(0o755)

    _, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())
    assert stats.verifier_wrapper_installed
    body = online.read_bytes()
    assert body == offline_verifier_run_sh_bytes()
    assert b"pip install" not in body


def test_adapt_replaces_old_pytest_only_wrapper(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    verifier = staged / "verifier"
    verifier.mkdir()
    old = verifier / "run.sh"
    old.write_text(
        "#!/bin/sh\n/opt/verifier/bin/pytest /tests/test_outputs.py\n",
        encoding="utf-8",
    )
    old.chmod(0o755)

    _, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())
    assert stats.verifier_wrapper_installed
    body = old.read_bytes()
    assert body == offline_verifier_run_sh_bytes()
    assert b"tests/test.sh" in body
    assert b"/opt/verifier/bin/pytest" not in body


def test_adapt_keeps_script_that_already_runs_test_sh(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    verifier = staged / "verifier"
    verifier.mkdir()
    existing = offline_verifier_run_sh_bytes() + b"\n# operator note\n"
    target = verifier / "run.sh"
    target.write_bytes(existing)
    target.chmod(0o755)

    _, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())
    assert not stats.verifier_wrapper_installed
    assert target.read_bytes() == existing


def test_adapt_after_normalize_fixes_absolute_verifier_path(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    raw = {
        "schema_version": "1.1",
        "task": {
            "name": "terminal-bench/sample",
            "description": "Native TB2.1 task.",
        },
        "environment": {
            "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment",
            "cpus": 1,
            "memory_mb": 2048,
            "storage_mb": 4096,
            "user": "root",
            "architecture": "any",
            "allow_internet": True,
        },
        "verifier": {"timeout_sec": 100.0, "user": "root"},
        "agent": {"timeout_sec": 100.0},
    }
    normalized = normalize_terminal_bench_task_toml(raw)
    assert normalized["verifier"]["args"]["script_path"] == DEFAULT_VERIFIER_SCRIPT_PATH
    assert normalized["environment"]["cpu_arch"] == "any"

    adapted, stats = adapt_bundle_for_nebius_terminus(staged, normalized)
    assert adapted["verifier"]["args"]["script_path"] == VERIFIER_SCRIPT_PATH
    assert adapted["environment"]["cpu_arch"] == "x86_64"
    assert adapted["environment"]["user"] == "agent"
    assert "user" not in adapted["verifier"]
    assert stats.cpu_arch_forced
    assert stats.verifier_wrapper_installed
    assert stats.artifact_globs_stripped
    assert adapted.get("steps") == [{"name": "main"}]
    reasons = preflight_nebius_terminus_admission(adapted, _SEI)
    assert reasons == ()


def test_preflight_rejects_unadapted_harbor_config(tmp_path: Path) -> None:
    reasons = preflight_nebius_terminus_admission(_harbor_shaped_config(), _SEI)
    assert "gateway_only_network_required" in reasons
    assert "resource_limits_required" in reasons
    assert "standard_workspace_identity_required" in reasons
    assert "custom_verifier_identity_unsupported" in reasons
    assert "shared_script_verifier_required" in reasons
    assert "private_verifier_directory_required" in reasons
