"""Executable contracts for the reusable native image build and scan lane."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def _workflow() -> dict[str, Any]:
    return yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())


def _step(name: str) -> dict[str, Any]:
    return next(step for step in _workflow()["jobs"]["build"]["steps"] if step.get("name") == name)


def _environment(tmp_path: Path) -> dict[str, str]:
    # Only native CPU detection is synthetic; execute the actual manifest validator.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    uname = bin_dir / "uname"
    uname.write_text("#!/bin/sh\nprintf 'x86_64\\n'\n")
    uname.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "IMAGE_NAME": "service",
        "IMAGE_DIGEST_NAME": "loom-service",
        "DOCKERFILE": "deploy/Dockerfile.service",
        "BUILD_CONTEXT": ".",
        "EVENT_NAME": "pull_request",
        "REF_NAME": "codex/nebius-ci",
        "PR_NUMBER": "123",
        "HEAD_SHA": "a" * 40,
        "BASE_SHA": "b" * 40,
        "ARCHITECTURE": "amd64",
        "PLATFORM": "linux/amd64",
    }


def test_reusable_images_only_build_and_scan_the_callers_native_matrix() -> None:
    workflow = _workflow()
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers) == {"workflow_call"}
    assert triggers["workflow_call"]["inputs"]["native_builds"]["required"] is True
    assert set(workflow["jobs"]) == {"trivy-binary", "build"}
    build = workflow["jobs"]["build"]
    assert build["needs"] == "trivy-binary"
    assert build["strategy"]["matrix"]["include"] == "${{ fromJSON(inputs.native_builds) }}"
    assert build["strategy"]["fail-fast"] is False
    assert build["runs-on"] == "ubuntu-24.04"
    for job in workflow["jobs"].values():
        assert job["if"] == "inputs.native_builds != '[]'"
        assert job["permissions"] == {"contents": "read"}
        assert "continue-on-error" not in job
    # No always()/cancelled() override: a failed binary dependency must skip the build.
    assert "always()" not in build["if"]


@pytest.mark.parametrize("event", ["pull_request", "merge_group", "workflow_dispatch"])
def test_valid_native_inputs_pass_the_actual_shell_validation(tmp_path: Path, event: str) -> None:
    env = _environment(tmp_path)
    env.update(EVENT_NAME=event, PR_NUMBER="123" if event == "pull_request" else "")
    result = subprocess.run(
        ["bash"], input=_step("Validate image build inputs")["run"],
        cwd=ROOT, text=True, capture_output=True, env=env, check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("IMAGE_NAME", "$(touch injected)"),
        ("DOCKERFILE", "deploy/Dockerfile.worker"),
        ("BUILD_CONTEXT", "../outside"),
        ("HEAD_SHA", "a" * 39),
        ("BASE_SHA", "not-a-sha"),
        ("REF_NAME", "branch;touch injected"),
        ("PR_NUMBER", "0"),
        ("EVENT_NAME", "push"),
        ("ARCHITECTURE", "arm64"),
        ("PLATFORM", "linux/arm64"),
    ],
)
def test_untrusted_inputs_fail_before_build_or_shell_expansion(
    tmp_path: Path, name: str, value: str,
) -> None:
    env = _environment(tmp_path)
    sentinel = tmp_path / "injected"
    env[name] = value.replace("touch injected", f"touch {sentinel}")
    result = subprocess.run(
        ["bash"], input=_step("Validate image build inputs")["run"],
        cwd=ROOT, text=True, capture_output=True, env=env, check=False,
    )
    assert result.returncode != 0
    assert not sentinel.exists()


def test_arm_matrix_is_rejected_even_on_matching_native_hardware(tmp_path: Path) -> None:
    env = _environment(tmp_path)
    (tmp_path / "bin" / "uname").write_text("#!/bin/sh\nprintf 'aarch64\\n'\n")
    env.update(ARCHITECTURE="arm64", PLATFORM="linux/arm64")
    result = subprocess.run(
        ["bash"], input=_step("Validate image build inputs")["run"],
        cwd=ROOT, text=True, capture_output=True, env=env, check=False,
    )
    assert result.returncode != 0


def test_build_uses_exact_head_and_local_archive_without_publication(tmp_path: Path) -> None:
    env = _environment(tmp_path)
    capture = tmp_path / "docker-argv.json"
    docker = tmp_path / "bin" / "docker"
    docker.write_text(
        "#!/usr/bin/env python3\nimport json, os, sys\n"
        "with open(os.environ['CAPTURE_ARGV'], 'w') as stream:\n"
        "    json.dump(sys.argv[1:], stream)\n"
    )
    docker.chmod(0o755)
    env["CAPTURE_ARGV"] = str(capture)
    result = subprocess.run(
        ["bash"], input=_step("Build without registry or cache write authority")["run"],
        cwd=ROOT, text=True, capture_output=True, env=env, check=False,
    )
    assert result.returncode == 0, result.stderr
    args = json.loads(capture.read_text())
    assert args[:2] == ["buildx", "build"]
    assert args[args.index("--platform") + 1] == "linux/amd64"
    assert args[args.index("--build-arg") + 1] == "LOOM_BUILD_SHA=" + "a" * 40
    assert args[args.index("--output") + 1] == "type=docker,dest=/tmp/service-amd64.docker.tar"
    assert args[-1] == "."
    assert set(args).isdisjoint({"--push", "--cache-to"})


def test_scan_is_job_local_and_uses_verified_binary_and_controlled_policy() -> None:
    build = _workflow()["jobs"]["build"]
    names = [step.get("name") for step in build["steps"]]
    assert names.index("Verify distributed Trivy binary") < names.index("Scan native image archive")
    assert names.index("Build without registry or cache write authority") < names.index("Scan native image archive")
    verify = _step("Verify distributed Trivy binary")["run"]
    assert "sha256sum --check trivy.sha256" in verify
    scan = _step("Scan native image archive")
    assert scan["env"]["ARCHIVE"].endswith(".docker.tar")
    assert '--input "$ARCHIVE"' in scan["run"]
    assert "scripts/validate_trivy_release_report.py" in scan["run"]
    assert "scripts/write_trivy_release_policy.py" in _step("Generate controlled Trivy policy")["run"]
    assert all("upload-artifact" not in step.get("uses", "") for step in build["steps"])
    scripts = "\n".join(step.get("run", "") for step in build["steps"])
    assert "docker login" not in scripts
    assert "--cache-to" not in scripts
    assert "--push" not in scripts
    assert "secrets." not in json.dumps(build)
