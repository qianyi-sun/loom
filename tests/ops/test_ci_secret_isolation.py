from __future__ import annotations

import os
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
        "needs.plan.outputs.required == 'true' && "
        "needs.plan.outputs.images != '[]'"
    )
    assert _checkout_steps(build)
    assert all(
        step.get("with", {}).get("persist-credentials") is False
        for step in _checkout_steps(build)
    )

    script = "\n".join(_run_blocks(build))
    assert "docker login" not in script
    assert "--push" not in script
    assert "--output" not in script
    assert "type=registry" not in script
    assert "ghcr.io" not in script
    assert "--cache-from" not in script
    assert "--cache-to" not in script
    assert "secrets." not in str(build)
    assert "${{" not in script


def test_images_publish_authority_is_push_only_on_trusted_branches() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    publish = workflow["jobs"]["publish"]

    write_capable_jobs = {
        job_name
        for job_name, job in workflow["jobs"].items()
        if job.get("permissions", {}).get("packages") == "write"
    }
    assert write_capable_jobs == {"publish"}
    assert publish["permissions"] == {"contents": "read", "packages": "write"}
    assert _normalized_expression(publish["if"]) == (
        "github.event_name == 'push' && "
        "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main') && "
        "needs.plan.outputs.required == 'true' && "
        "needs.plan.outputs.images != '[]'"
    )
    assert _checkout_steps(publish)
    assert all(
        step.get("with", {}).get("persist-credentials") is False
        for step in _checkout_steps(publish)
    )

    login = _named_step(publish, "Log in to GHCR")
    assert login["env"]["GHCR_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert "${{" not in login["run"]

    script = "\n".join(_run_blocks(publish))
    assert "docker login" in script
    assert "--push" in script
    assert "--cache-from" not in script
    assert "--cache-to" not in script
    assert "build_args=(" in script
    assert '"${build_args[@]}"' in script
    assert "${{" not in script


def test_images_manual_dispatch_is_build_only() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    on_config = _workflow_on(workflow)
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]

    assert "workflow_dispatch" in on_config
    assert _normalized_expression(build["if"]).startswith(
        "github.event_name != 'push' &&"
    )
    assert _normalized_expression(publish["if"]).startswith(
        "github.event_name == 'push' &&"
    )


def test_images_permissions_are_an_exact_job_allowlist() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert set(jobs) == {"plan", "build", "publish", "images-gate"}
    for job_name in ("plan", "build", "images-gate"):
        effective = jobs[job_name].get("permissions", workflow["permissions"])
        assert effective == {"contents": "read"}
        assert "environment" not in jobs[job_name]
        assert "id-token" not in effective
        assert all(value != "write" for value in effective.values())

    assert jobs["publish"]["permissions"] == {
        "contents": "read",
        "packages": "write",
    }
    assert "environment" not in jobs["publish"]


def test_images_secret_and_cache_authority_is_exact() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    secret_references = [
        value
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        for value in step.get("env", {}).values()
        if isinstance(value, str) and "secrets." in value
    ]

    assert secret_references == ["${{ secrets.GITHUB_TOKEN }}"]
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

    assert setup_steps
    assert all(step.get("with", {}).get("enable-cache") is False for step in setup_steps)
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
    assert set(gate["needs"]) == {"plan", "smoke"}

    gate_step = _named_step(gate, "Enforce selected staging smoke results")
    assert "AWS_S3_RESULT" not in gate_step["env"]
    assert "AWS_S3_RESULT" not in gate_step["run"]
    assert "${{ secrets." not in str(workflow)
    assert "ci-aws" not in str(workflow)

    for job in jobs.values():
        effective = job.get("permissions", workflow["permissions"])
        assert effective == {"contents": "read"}
        assert "environment" not in job
        assert "id-token" not in effective
        assert all(value != "write" for value in effective.values())
        assert all(
            step.get("with", {}).get("persist-credentials") is False
            for step in _checkout_steps(job)
        )


@pytest.mark.parametrize(
    ("event_name", "image_tag"),
    [
        ("pull_request\npush", "smoke-123"),
        ("pull_request", "smoke-123; id"),
        ("pull_request", "../smoke-123"),
        ("workflow_dispatch", "--push"),
    ],
)
def test_staging_smoke_rejects_malformed_event_and_image_tag(
    event_name: str,
    image_tag: str,
) -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    step = _named_step(workflow["jobs"]["smoke"], "Validate staging smoke inputs")

    result = _run_validation_step(
        step,
        env={"EVENT_NAME": event_name, "IMAGE_TAG": image_tag},
    )

    assert result.returncode != 0
    assert "FAIL:" in result.stderr


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
    ("job_name", "field", "payload"),
    [
        ("build", "IMAGE_NAME", "worker$(id)"),
        ("build", "DOCKERFILE", "../deploy/Dockerfile.worker"),
        ("build", "EVENT_NAME", "pull_request\npush"),
        ("build", "REF_NAME", "dev; id"),
        ("build", "PR_NUMBER", "--help"),
        ("build", "HEAD_SHA", "abc`id`"),
        ("publish", "IMAGE_NAME", "worker; id"),
        ("publish", "DOCKERFILE", "deploy/Dockerfile.worker\n--push"),
        ("publish", "EVENT_NAME", "push$(id)"),
        ("publish", "REF_NAME", "dev/../../main"),
        ("publish", "REPOSITORY_OWNER", "owner`id`"),
        ("publish", "GHCR_ACTOR", "--password-stdin"),
        ("publish", "HEAD_SHA", "deadbeef$(id)"),
    ],
)
def test_image_input_validation_rejects_shell_metacharacters_and_ambiguous_values(
    job_name: str,
    field: str,
    payload: str,
) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    step = _named_step(workflow["jobs"][job_name], "Validate image build inputs")
    env = {
        "IMAGE_NAME": "worker",
        "DOCKERFILE": "deploy/Dockerfile.worker",
        "EVENT_NAME": "pull_request" if job_name == "build" else "push",
        "REF_NAME": "feature-safe" if job_name == "build" else "dev",
        "PR_NUMBER": "42" if job_name == "build" else "",
        "HEAD_SHA": "a" * 40,
        "REPOSITORY_OWNER": "qianyi-sun",
        "GHCR_ACTOR": "qianyi-sun",
    }
    env[field] = payload

    result = _run_validation_step(step, env=env)

    assert result.returncode != 0, (job_name, field, payload, result.stdout)
    assert "FAIL:" in result.stderr


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
            "DOCKERFILE": "deploy/Dockerfile.worker",
            "EVENT_NAME": "pull_request",
            "REF_NAME": "42/merge",
            "PR_NUMBER": "42",
            "HEAD_SHA": "a" * 40,
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
            "DOCKERFILE": "deploy/Dockerfile.worker",
            "EVENT_NAME": event_name,
            "REF_NAME": ref_name,
            "PR_NUMBER": pr_number,
            "HEAD_SHA": "a" * 40,
            "REPOSITORY_OWNER": "qianyi-sun",
            "GHCR_ACTOR": "github-actions[bot]",
        },
    )

    assert result.returncode == 0, result.stderr


def test_secret_isolation_contract_is_covered_by_advisory_catch_all() -> None:
    codeowners = (REPO_ROOT / ".github/CODEOWNERS").read_text(encoding="utf-8")
    entries = {
        line.strip()
        for line in codeowners.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert entries == {"* @qianyi-sun"}
    assert "not a `dev` merge gate" in codeowners
