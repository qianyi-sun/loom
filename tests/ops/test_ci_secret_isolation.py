from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
REUSABLE_WORKFLOWS = ("images", "cluster-smoke", "staging-smoke")


def _workflow(name: str) -> dict[str, Any]:
    return yaml.safe_load(
        (REPO_ROOT / f".github/workflows/{name}.yml").read_text(encoding="utf-8")
    )


@pytest.fixture(autouse=True)
def _ubuntu_amd64_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the workflow's Linux runner contract on any developer host."""
    command_dir = tmp_path / "runner-commands"
    command_dir.mkdir()
    uname = command_dir / "uname"
    uname.write_text("#!/bin/sh\nprintf 'x86_64\\n'\n", encoding="utf-8")
    uname.chmod(0o755)
    monkeypatch.setenv("PATH", f"{command_dir}{os.pathsep}{os.environ['PATH']}")


def _validation_env() -> dict[str, str]:
    return {
        "IMAGE_NAME": "service",
        "IMAGE_DIGEST_NAME": "loom-service",
        "DOCKERFILE": "deploy/Dockerfile.service",
        "BUILD_CONTEXT": ".",
        "EVENT_NAME": "pull_request",
        "REF_NAME": "42/merge",
        "PR_NUMBER": "42",
        "HEAD_SHA": "a" * 40,
        "BASE_SHA": "b" * 40,
        "ARCHITECTURE": "amd64",
        "PLATFORM": "linux/amd64",
    }


def _run_image_validation(**overrides: str) -> subprocess.CompletedProcess[str]:
    step = next(
        step for step in _workflow("images")["jobs"]["build"]["steps"]
        if step.get("name") == "Validate image build inputs"
    )
    return subprocess.run(
        ["bash"],
        input=step["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={**os.environ, **_validation_env(), **overrides},
        check=False,
    )


@pytest.mark.parametrize("name", REUSABLE_WORKFLOWS)
def test_reusable_validation_has_no_secrets_or_write_authority(name: str) -> None:
    workflow = _workflow(name)
    # PyYAML treats the unquoted GitHub Actions key `on` as YAML 1.1 bool.
    events = workflow.get("on", workflow.get(True))
    assert set(events) == {"workflow_call"}
    assert not (events["workflow_call"] or {}).get("secrets")
    assert workflow["permissions"] == {"contents": "read"}
    assert "secrets." not in str(workflow)
    for job in workflow["jobs"].values():
        assert job.get("permissions", workflow["permissions"]) == {"contents": "read"}
        assert "environment" not in job
        assert job.get("continue-on-error") is not True
        for step in job.get("steps", []):
            assert step.get("continue-on-error") is not True
            if str(step.get("uses", "")).startswith("actions/checkout@"):
                assert step.get("with", {}).get("persist-credentials") is False
            # Event and matrix values must enter shell scripts through env.
            assert "${{" not in step.get("run", "")


@pytest.mark.parametrize("name", REUSABLE_WORKFLOWS)
def test_reusable_validation_cannot_write_shared_pull_request_caches(name: str) -> None:
    for job in _workflow(name)["jobs"].values():
        for step in job.get("steps", []):
            action = str(step.get("uses", ""))
            assert not action.startswith("actions/cache@")
            if action.startswith("astral-sh/setup-uv@"):
                assert step["with"]["save-cache"] == (
                    "${{ github.event_name != 'pull_request' "
                    "&& github.event_name != 'merge_group' }}"
                )


def test_image_build_emits_only_local_archives_without_registry_authority() -> None:
    build = _workflow("images")["jobs"]["build"]
    script = "\n".join(step["run"] for step in build["steps"] if "run" in step)
    assert "type=docker,dest=${archive}" in script
    assert ".docker.tar" in script
    for forbidden in (
        "docker login", "--push", "type=oci", "type=registry", "ghcr.io",
        "--cache-from", "--cache-to",
    ):
        assert forbidden not in script


@pytest.mark.parametrize(
    ("field", "payload", "error_marker"),
    [
        ("IMAGE_NAME", "service$(id)", "component ownership validation failed:"),
        ("IMAGE_DIGEST_NAME", "loom-service; id", "component ownership validation failed:"),
        ("DOCKERFILE", "../deploy/Dockerfile.service", "component ownership validation failed:"),
        ("BUILD_CONTEXT", "..", "component ownership validation failed:"),
        ("EVENT_NAME", "pull_request\npush", "FAIL:"),
        ("EVENT_NAME", "pull_request_target", "FAIL:"),
        ("REF_NAME", "dev; id", "FAIL:"),
        ("PR_NUMBER", "--help", "FAIL:"),
        ("HEAD_SHA", "abc`id`", "FAIL:"),
        ("BASE_SHA", "abc$(id)", "FAIL:"),
    ],
)
def test_image_input_validation_rejects_ambiguous_values(
    field: str, payload: str, error_marker: str,
) -> None:
    result = _run_image_validation(**{field: payload})
    assert result.returncode != 0, (field, payload, result.stdout)
    assert error_marker in result.stderr


@pytest.mark.parametrize("field", ["IMAGE_NAME", "DOCKERFILE", "BUILD_CONTEXT"])
def test_image_input_validation_never_evaluates_command_substitution(
    tmp_path: Path, field: str,
) -> None:
    sentinel = tmp_path / "shell-injection-ran"
    result = _run_image_validation(**{field: f"service$(touch {sentinel})"})
    assert result.returncode != 0
    assert not sentinel.exists()


def test_image_input_validation_rejects_a_matrix_row_outside_active_manifest() -> None:
    result = _run_image_validation(
        IMAGE_NAME="unowned-image", IMAGE_DIGEST_NAME="loom-unowned-image",
    )
    assert result.returncode != 0
    assert "component ownership validation failed:" in result.stderr


def test_image_input_validation_requires_native_platform() -> None:
    result = _run_image_validation(ARCHITECTURE="arm64", PLATFORM="linux/arm64")
    assert result.returncode != 0
    assert "runner architecture does not match the build platform" in result.stderr


@pytest.mark.parametrize(
    ("event_name", "ref_name", "pr_number"),
    [
        ("pull_request", "42/merge", "42"),
        ("merge_group", "gh-readonly-queue/codex/nebius-main/pr-42-deadbeef", ""),
        ("workflow_dispatch", "codex/nebius-ci-isolation", ""),
    ],
)
def test_image_input_validation_accepts_actual_github_context_shapes(
    event_name: str, ref_name: str, pr_number: str,
) -> None:
    result = _run_image_validation(
        EVENT_NAME=event_name, REF_NAME=ref_name, PR_NUMBER=pr_number,
    )
    assert result.returncode == 0, result.stderr
