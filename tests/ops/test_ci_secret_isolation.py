from __future__ import annotations

import os
import platform
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML treats the unquoted GitHub Actions key `on` as YAML 1.1 bool.
    return workflow.get("on", workflow.get(True))


def _checkout_steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        step
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]


def _run_blocks(job: dict[str, Any]) -> list[str]:
    return [str(step["run"]) for step in job.get("steps", []) if "run" in step]


def _named_step(job: dict[str, Any], name: str) -> dict[str, Any]:
    return next(step for step in job["steps"] if step.get("name") == name)


def _normalized_expression(value: str) -> str:
    return " ".join(value.split())


def _native_arch_env() -> dict[str, str]:
    machine = platform.machine().lower()
    if machine in {"arm64", "aarch64"}:
        return {"ARCHITECTURE": "arm64", "PLATFORM": "linux/arm64"}
    return {"ARCHITECTURE": "amd64", "PLATFORM": "linux/amd64"}


def _run_validation_step(
    step: dict[str, Any],
    *,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash"],
        input=step["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={**os.environ, **env},
        check=False,
    )


def test_images_untrusted_build_is_read_only_and_cannot_publish_or_write_cache() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build = workflow["jobs"]["build"]

    assert workflow["permissions"] == {"contents": "read"}
    assert build["permissions"] == {"contents": "read"}
    assert _normalized_expression(build["if"]) == (
        "github.event_name != 'push' && "
        "needs.plan.outputs.trusted_publish != 'true' && "
        "needs.plan.outputs.gate_mode == 'full' && "
        "needs.plan.outputs.required == 'true' && "
        "needs.plan.outputs.images != '[]'"
    )
    assert _checkout_steps(build)
    assert all(
        step.get("with", {}).get("persist-credentials") is False for step in _checkout_steps(build)
    )

    script = "\n".join(_run_blocks(build))
    assert "docker login" not in script
    assert "--push" not in script
    assert "type=docker,dest=${archive}" in script
    assert ".docker.tar" in script
    assert "type=oci" not in script
    assert "type=registry" not in script
    assert "ghcr.io" not in script
    assert "--cache-from" not in script
    assert "--cache-to" not in script
    assert "secrets." not in str(build)
    assert "${{" not in script


def test_images_publish_authority_is_protected_push_or_reconciler_only() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    publish = workflow["jobs"]["publish"]

    write_capable_jobs = {
        job_name
        for job_name, job in workflow["jobs"].items()
        if job.get("permissions", {}).get("packages") == "write"
    }
    assert write_capable_jobs == {
        "publish",
        "publish-manifest",
    }
    assert publish["permissions"] == {
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    manifest = workflow["jobs"]["publish-manifest"]
    assert manifest["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    trusted_event = (
        "(github.event_name == 'push' || "
        "(github.event_name == 'workflow_dispatch' && "
        "needs.plan.outputs.trusted_publish == 'true')) && "
    )
    assert _normalized_expression(publish["if"]) == (
        trusted_event + "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main') && "
        "needs.plan.outputs.gate_mode == 'full' && "
        "needs.plan.outputs.required == 'true' && "
        "needs.plan.outputs.images != '[]'"
    )
    assert _checkout_steps(publish)
    assert all(
        step.get("with", {}).get("persist-credentials") is False
        for step in _checkout_steps(publish)
    )
    assert _normalized_expression(manifest["if"]) == (
        trusted_event + "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main') && "
        "needs.plan.outputs.gate_mode == 'full' && "
        "needs.plan.outputs.required == 'true' && "
        "needs.plan.outputs.images != '[]' && "
        "needs.publish.result == 'success'"
    )
    assert manifest["needs"] == ["plan", "publish"]
    assert _checkout_steps(manifest)
    assert all(
        step.get("with", {}).get("persist-credentials") is False
        for step in _checkout_steps(manifest)
    )
    login = _named_step(publish, "Log in to GHCR")
    assert login["env"]["GHCR_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert "${{" not in login["run"]

    script = "\n".join(_run_blocks(publish))
    assert "docker login" in script
    assert "docker push" in script
    assert "--cache-from" not in script
    assert "--cache-to" not in script
    assert "Scan trusted image archive" in str(publish)
    assert "Attest published architecture digest" in str(publish)
    assert "${{" not in script

    manifest_script = "\n".join(_run_blocks(manifest))
    assert "docker buildx imagetools create" in manifest_script
    assert '"${image}@${amd64_digest}"' in manifest_script
    assert '"${image}@${arm64_digest}"' in manifest_script
    assert '--architecture-digest "linux/amd64=${AMD64_DIGEST}"' in manifest_script
    assert '--architecture-digest "linux/arm64=${ARM64_DIGEST}"' in manifest_script
    assert "LOOM_CI_IMAGE_RUNS_ON" not in str(publish)
    assert "LOOM_CI_IMAGE_RUNS_ON" not in str(manifest)


def test_images_manual_dispatch_is_build_only() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    on_config = _workflow_on(workflow)
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]

    assert "workflow_dispatch" in on_config
    assert _normalized_expression(build["if"]).startswith(
        "github.event_name != 'push' && needs.plan.outputs.trusted_publish != 'true' &&"
    )
    assert "needs.plan.outputs.trusted_publish == 'true'" in _normalized_expression(publish["if"])


def test_images_trusted_dispatch_is_validated_before_any_publish_job() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    plan = workflow["jobs"]["plan"]
    trust = _named_step(plan, "Validate trusted release reconciliation")
    script = trust["run"]

    assert trust["env"]["ACTOR"] == "${{ github.actor }}"
    assert trust["env"]["BASE_SHA"] == "${{ inputs.trusted_base_sha || '' }}"
    assert '[[ "$ACTOR" == "github-actions[bot]" ]]' in script
    assert '[[ "$REF_NAME" == "dev" || "$REF_NAME" == "main" ]]' in script
    assert 'git merge-base --is-ancestor "$BASE_SHA" "$HEAD_SHA"' in script
    assert 'test "$(git rev-parse HEAD)" = "$HEAD_SHA"' in script
    for job_name in ("publish", "publish-manifest"):
        assert "needs.plan.outputs.trusted_publish == 'true'" in workflow["jobs"][job_name]["if"]


def test_images_trusted_dispatch_accepts_only_bot_exact_ancestor_range(tmp_path: Path) -> None:
    plan = _workflow(".github/workflows/images.yml")["jobs"]["plan"]
    trust = _named_step(plan, "Validate trusted release reconciliation")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    base = subprocess.run(
        ["git", "rev-parse", "HEAD^"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    output = tmp_path / "github-output.txt"
    common = {
        "EVENT_NAME": "workflow_dispatch",
        "REQUESTED": "true",
        "BASE_SHA": base,
        "HEAD_SHA": head,
        "ACTOR": "github-actions[bot]",
        "REF_NAME": "dev",
        "GITHUB_OUTPUT": str(output),
    }

    accepted = subprocess.run(
        ["bash"],
        cwd=REPO_ROOT,
        input=trust["run"],
        text=True,
        capture_output=True,
        env={**os.environ, **common},
        check=False,
    )

    assert accepted.returncode == 0, accepted.stderr
    assert output.read_text(encoding="utf-8") == "trusted_publish=true\n"
    for drift in (
        {"ACTOR": "qianyi-sun"},
        {"REF_NAME": "feature"},
        {"BASE_SHA": head},
        {"BASE_SHA": "0" * 40},
    ):
        rejected = subprocess.run(
            ["bash"],
            cwd=REPO_ROOT,
            input=trust["run"],
            text=True,
            capture_output=True,
            env={**os.environ, **common, **drift},
            check=False,
        )
        assert rejected.returncode != 0


def test_images_permissions_are_an_exact_job_allowlist() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert set(jobs) == {
        "plan",
        "image-route",
        "trivy-binary",
        "personal-dev-scanner-cache-assets",
        "build",
        "publish",
        "publish-manifest",
        "personal-dev-trusted-release",
        "images-gate",
    }
    for job_name in (
        "plan",
        "trivy-binary",
        "build",
        "images-gate",
    ):
        effective = jobs[job_name].get("permissions", workflow["permissions"])
        assert effective == {"contents": "read"}
        assert "environment" not in jobs[job_name]
        assert "id-token" not in effective
        assert all(value != "write" for value in effective.values())

    assert jobs["image-route"]["permissions"] == {
        "checks": "read",
        "contents": "read",
    }
    assert "environment" not in jobs["image-route"]

    scanner_cache_assets = jobs["personal-dev-scanner-cache-assets"]
    assert scanner_cache_assets["permissions"] == {
        "actions": "read",
        "contents": "read",
    }
    assert "environment" not in scanner_cache_assets
    assert "id-token" not in scanner_cache_assets["permissions"]
    assert all(value != "write" for value in scanner_cache_assets["permissions"].values())

    assert jobs["publish"]["permissions"] == {
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    assert jobs["publish-manifest"]["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    assert jobs["personal-dev-trusted-release"]["permissions"] == {
        "actions": "read",
        "attestations": "read",
        "contents": "read",
        "packages": "read",
    }
    assert "environment" not in jobs["publish"]
    assert "environment" not in jobs["publish-manifest"]
    assert "environment" not in jobs["personal-dev-trusted-release"]


def test_images_secret_and_cache_authority_is_exact() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    secret_references = [
        value
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        for value in step.get("env", {}).values()
        if isinstance(value, str) and "secrets." in value
    ]

    assert secret_references == [
        "${{ secrets.GITHUB_TOKEN }}",
        "${{ secrets.GITHUB_TOKEN }}",
        "${{ secrets.GITHUB_TOKEN }}",
    ]
    for job in workflow["jobs"].values():
        assert job.get("continue-on-error") is not True
        for step in job.get("steps", []):
            assert not str(step.get("uses", "")).startswith("actions/cache@")
            assert step.get("continue-on-error") is not True


@pytest.mark.parametrize(
    "workflow_path",
    [".github/workflows/images.yml", ".github/workflows/staging-smoke.yml"],
)
def test_untrusted_workflows_disable_setup_uv_cache_writes(workflow_path: str) -> None:
    workflow = _workflow(workflow_path)
    setup_steps = [
        step
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if str(step.get("uses", "")).startswith("astral-sh/setup-uv@")
    ]

    for step in setup_steps:
        inputs = step.get("with", {})
        assert inputs.get("enable-cache") is True
        assert inputs.get("save-cache") == (
            "${{ github.event_name != 'pull_request' && github.event_name != 'merge_group' }}"
        )
    for job in workflow["jobs"].values():
        assert job.get("continue-on-error") is not True
        for step in job.get("steps", []):
            assert not str(step.get("uses", "")).startswith("actions/cache@")
            assert step.get("continue-on-error") is not True


def test_staging_pr_gate_is_credential_free_and_does_not_depend_on_real_aws() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    jobs = workflow["jobs"]
    gate = jobs["staging-smoke-gate"]

    assert workflow["permissions"] == {"contents": "read"}
    assert "smoke-storage-aws-s3" not in jobs
    assert set(gate["needs"]) == {"plan", "system-smoke"}

    gate_step = _named_step(gate, "Enforce selected staging smoke results")
    assert "AWS_S3_RESULT" not in gate_step["env"]
    assert "AWS_S3_RESULT" not in gate_step["run"]
    assert "${{ secrets." not in str(workflow)
    assert "ci-aws" not in str(workflow)

    for job_name, job in jobs.items():
        effective = job.get("permissions", workflow["permissions"])
        if job_name == "staging-route":
            assert effective == {"checks": "read", "contents": "read"}
        else:
            assert effective == {"contents": "read"}
        assert "environment" not in job
        assert "id-token" not in effective
        assert all(value != "write" for value in effective.values())
        assert all(
            step.get("with", {}).get("persist-credentials") is False
            for step in _checkout_steps(job)
        )


@pytest.mark.parametrize(
    "workflow_path",
    [
        ".github/workflows/images.yml",
        ".github/workflows/staging-smoke.yml",
    ],
)
def test_untrusted_workflows_never_use_pull_request_target(workflow_path: str) -> None:
    workflow = _workflow(workflow_path)
    assert "pull_request_target" not in _workflow_on(workflow)


@pytest.mark.parametrize(
    "workflow_path",
    [
        ".github/workflows/images.yml",
        ".github/workflows/staging-smoke.yml",
    ],
)
def test_untrusted_workflow_shell_receives_context_only_through_env(
    workflow_path: str,
) -> None:
    workflow = _workflow(workflow_path)
    for job_name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            if "run" in step:
                assert "${{" not in step["run"], (workflow_path, job_name, step)


@pytest.mark.parametrize(
    ("job_name", "field", "payload", "error_marker"),
    [
        ("build", "IMAGE_NAME", "worker$(id)", "component ownership validation failed:"),
        (
            "build",
            "IMAGE_DIGEST_NAME",
            "loom-worker; id",
            "component ownership validation failed:",
        ),
        (
            "build",
            "DOCKERFILE",
            "../deploy/Dockerfile.worker",
            "component ownership validation failed:",
        ),
        ("build", "BUILD_CONTEXT", "..", "component ownership validation failed:"),
        ("build", "EVENT_NAME", "pull_request\npush", "FAIL:"),
        ("build", "REF_NAME", "dev; id", "FAIL:"),
        ("build", "PR_NUMBER", "--help", "FAIL:"),
        ("build", "HEAD_SHA", "abc`id`", "FAIL:"),
        ("publish", "IMAGE_NAME", "worker; id", "component ownership validation failed:"),
        (
            "publish",
            "IMAGE_DIGEST_NAME",
            "loom-worker$(id)",
            "component ownership validation failed:",
        ),
        (
            "publish",
            "DOCKERFILE",
            "deploy/Dockerfile.worker\n--push",
            "component ownership validation failed:",
        ),
        ("publish", "BUILD_CONTEXT", "../.", "component ownership validation failed:"),
        ("publish", "EVENT_NAME", "push$(id)", "FAIL:"),
        ("publish", "REF_NAME", "dev/../../main", "FAIL:"),
        ("publish", "REPOSITORY_OWNER", "owner`id`", "FAIL:"),
        ("publish", "GHCR_ACTOR", "--password-stdin", "FAIL:"),
        ("publish", "HEAD_SHA", "deadbeef$(id)", "FAIL:"),
    ],
)
def test_image_input_validation_rejects_shell_metacharacters_and_ambiguous_values(
    job_name: str,
    field: str,
    payload: str,
    error_marker: str,
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"][job_name], "Validate image build inputs")
    env = {
        "IMAGE_NAME": "worker",
        "IMAGE_DIGEST_NAME": "loom-worker",
        "DOCKERFILE": "deploy/Dockerfile.worker",
        "BUILD_CONTEXT": ".",
        "EVENT_NAME": "pull_request" if job_name == "build" else "push",
        "REF_NAME": "feature-safe" if job_name == "build" else "dev",
        "PR_NUMBER": "42" if job_name == "build" else "",
        "HEAD_SHA": "a" * 40,
        "BASE_SHA": "b" * 40,
        "REPOSITORY_OWNER": "qianyi-sun",
        "GHCR_ACTOR": "qianyi-sun",
        **_native_arch_env(),
    }
    env[field] = payload

    result = _run_validation_step(step, env=env)

    assert result.returncode != 0, (job_name, field, payload, result.stdout)
    assert error_marker in result.stderr


def test_image_input_validation_never_evaluates_command_substitution(
    tmp_path: Path,
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"]["build"], "Validate image build inputs")
    sentinel = tmp_path / "shell-injection-ran"
    result = _run_validation_step(
        step,
        env={
            "IMAGE_NAME": f"worker$(touch {sentinel})",
            "IMAGE_DIGEST_NAME": "loom-worker",
            "DOCKERFILE": "deploy/Dockerfile.worker",
            "BUILD_CONTEXT": ".",
            "EVENT_NAME": "pull_request",
            "REF_NAME": "42/merge",
            "PR_NUMBER": "42",
            "HEAD_SHA": "a" * 40,
            **_native_arch_env(),
        },
    )

    assert result.returncode != 0
    assert not sentinel.exists()


@pytest.mark.parametrize(
    ("job_name", "event_name", "ref_name", "pr_number"),
    [
        ("build", "pull_request", "42/merge", "42"),
        (
            "build",
            "merge_group",
            "gh-readonly-queue/dev/pr-42-deadbeef",
            "",
        ),
        ("build", "workflow_dispatch", "codex/ci-secret-isolation", ""),
        ("publish", "push", "dev", ""),
        ("publish", "push", "main", ""),
    ],
)
def test_image_input_validation_accepts_actual_github_context_shapes(
    job_name: str,
    event_name: str,
    ref_name: str,
    pr_number: str,
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"][job_name], "Validate image build inputs")
    result = _run_validation_step(
        step,
        env={
            "IMAGE_NAME": "worker",
            "IMAGE_DIGEST_NAME": "loom-worker",
            "DOCKERFILE": "deploy/Dockerfile.worker",
            "BUILD_CONTEXT": ".",
            "EVENT_NAME": event_name,
            "REF_NAME": ref_name,
            "PR_NUMBER": pr_number,
            "HEAD_SHA": "a" * 40,
            "BASE_SHA": "b" * 40,
            "REPOSITORY_OWNER": "qianyi-sun",
            "GHCR_ACTOR": "github-actions[bot]",
            **_native_arch_env(),
        },
    )

    assert result.returncode == 0, result.stderr


def test_secret_isolation_contract_does_not_depend_on_one_reviewer() -> None:
    assert not (REPO_ROOT / ".github/CODEOWNERS").exists()
