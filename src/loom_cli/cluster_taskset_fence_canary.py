"""Staging-rollout launcher for the internal TaskSet fencing canary."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

from loom_cli.secret_source import SecretSourceError, resolve_secret_source
from loom_cli.taskset_fence_canary import (
    TaskSetFenceCanaryContract,
    TaskSetFenceCanaryContractError,
)

_SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA64_RE = re.compile(r"^[0-9a-f]{64}$")
_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{12}$")
_FORBIDDEN_EVIDENCE_TEXT = (
    "s3://",
    "http://",
    "https://",
    "source",
    "manifest",
    "token",
    "secret",
    "password",
    "authorization",
)
_STAGING_NAMESPACE = "loom-staging"
_STAGING_ROLLOUT_ROOT = Path("/data/loom-staging")
_EVIDENCE_RELATIVE_PATH = Path("canaries/taskset-lease-fencing/evidence.json")
# This operator-local reference must equal the optional loom-secrets
# taskset-fence-canary-token key mounted into loom-service.  It is deliberately
# not a user-provided flag and no raw token enters the rollout evidence tree.
_STAGING_CANARY_TOKEN_SOURCE = (
    "file:/shared_work/qianyi/loom-worker-capacity/"
    "staging-taskset-fence-canary-token"
)


class TaskSetFenceCanaryDeploymentError(RuntimeError):
    """The deployment-only launcher rejected an unsafe canary invocation."""


def _eligible_candidate(rollout_dir: Path, *, rollout_root: Path) -> tuple[str, str]:
    try:
        resolved_dir = rollout_dir.resolve(strict=True)
        resolved_root = rollout_root.resolve(strict=True)
    except OSError as exc:
        raise TaskSetFenceCanaryDeploymentError(
            "rollout is not an eligible staging candidate",
        ) from exc
    if resolved_dir.parent != resolved_root / "rollouts":
        raise TaskSetFenceCanaryDeploymentError("rollout is not an eligible staging candidate")

    try:
        inputs = json.loads((resolved_dir / "inputs.json").read_text(encoding="utf-8"))
        state = json.loads((resolved_dir / "state.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskSetFenceCanaryDeploymentError(
            "rollout is not an eligible staging candidate",
        ) from exc
    if not isinstance(inputs, Mapping) or not isinstance(state, Mapping):
        raise TaskSetFenceCanaryDeploymentError("rollout is not an eligible staging candidate")
    candidate_sha = inputs.get("resolved_sha")
    image_tag = inputs.get("image_tag")
    if (
        state.get("status") != "done"
        or inputs.get("environment") != "staging"
        or inputs.get("namespace") != _STAGING_NAMESPACE
        or inputs.get("rollout_root") != str(_STAGING_ROLLOUT_ROOT)
        or not isinstance(candidate_sha, str)
        or not _SHA40_RE.fullmatch(candidate_sha)
        or not isinstance(image_tag, str)
        or image_tag != f"staging-{candidate_sha[:7]}"
    ):
        raise TaskSetFenceCanaryDeploymentError("rollout is not an eligible staging candidate")
    return candidate_sha, image_tag


def _invalid_evidence() -> TaskSetFenceCanaryDeploymentError:
    return TaskSetFenceCanaryDeploymentError("internal canary evidence was rejected")


def _validate_evidence(
    evidence: Any,
    *,
    contract: TaskSetFenceCanaryContract,
) -> dict[str, Any]:
    if not isinstance(evidence, dict) or set(evidence) != {
        "schema_version",
        "candidate_sha",
        "image_tag",
        "task_set_id",
        "winner",
        "loser",
        "published_task",
        "stale_cas_outcome",
        "timestamps",
    }:
        raise _invalid_evidence()
    if (
        evidence.get("schema_version") != 1
        or evidence.get("candidate_sha") != contract.candidate_sha
        or evidence.get("image_tag") != contract.image_tag
        or evidence.get("task_set_id") != contract.task_set_id
        or evidence.get("stale_cas_outcome") != "LeaseLost"
    ):
        raise _invalid_evidence()

    winner = evidence.get("winner")
    loser = evidence.get("loser")
    task = evidence.get("published_task")
    timestamps = evidence.get("timestamps")
    if not isinstance(winner, dict):
        raise _invalid_evidence()
    if not isinstance(loser, dict):
        raise _invalid_evidence()
    if not isinstance(task, dict):
        raise _invalid_evidence()
    if not isinstance(timestamps, dict):
        raise _invalid_evidence()
    if set(winner) != {
        "job_id", "lease_epoch", "owner_fingerprint", "published_generation", "outcome",
    } or set(loser) != {
        "job_id", "lease_epoch", "owner_fingerprint", "outcome", "gc_eligible",
    } or set(task) != {"task_count", "checksum"} or set(timestamps) != {
        "a_staged_at", "b_published_at", "a_lease_lost_at",
    }:
        raise _invalid_evidence()
    try:
        UUID(str(winner["job_id"]))
        UUID(str(loser["job_id"]))
    except (KeyError, ValueError, AttributeError) as exc:
        raise _invalid_evidence() from exc
    if (
        not isinstance(winner["lease_epoch"], int)
        or isinstance(winner["lease_epoch"], bool)
        or not isinstance(loser["lease_epoch"], int)
        or isinstance(loser["lease_epoch"], bool)
        or winner["job_id"] != loser["job_id"]
        or winner["lease_epoch"] <= loser["lease_epoch"]
        or winner["published_generation"] != winner["lease_epoch"]
        or winner["outcome"] != "published"
        or loser["outcome"] != "fenced_before_publish"
        or loser["gc_eligible"] is not True
        or not isinstance(winner["owner_fingerprint"], str)
        or not _FINGERPRINT_RE.fullmatch(winner["owner_fingerprint"])
        or not isinstance(loser["owner_fingerprint"], str)
        or not _FINGERPRINT_RE.fullmatch(loser["owner_fingerprint"])
        or task["task_count"] != 1
        or task["checksum"] != contract.expected_task_checksum
        or not isinstance(task["checksum"], str)
        or not _SHA64_RE.fullmatch(task["checksum"])
        or not all(
            isinstance(value, str) and value.endswith("Z")
            for value in timestamps.values()
        )
    ):
        raise _invalid_evidence()
    encoded = json.dumps(evidence, sort_keys=True).lower()
    if any(forbidden in encoded for forbidden in _FORBIDDEN_EVIDENCE_TEXT):
        raise _invalid_evidence()
    return evidence


def _kubectl_runner(command: list[str], payload: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=payload,
        text=True,
        capture_output=True,
        check=False,
        timeout=900,
    )


def run_staging_fence_canary(
    *,
    rollout_dir: Path,
    task_set_id: str,
    expected_task_checksum: str,
    authorization_token: str,
    rollout_root: Path = _STAGING_ROLLOUT_ROOT,
    runner: Callable[[list[str], str], subprocess.CompletedProcess[str]] = _kubectl_runner,
) -> Path:
    """Run the one-shot canary from an already-complete staging rollout only."""
    candidate_sha, image_tag = _eligible_candidate(
        rollout_dir,
        rollout_root=rollout_root,
    )
    try:
        contract = TaskSetFenceCanaryContract.from_mapping({
            "candidate_sha": candidate_sha,
            "image_tag": image_tag,
            "task_set_id": task_set_id,
            "expected_task_checksum": expected_task_checksum,
            "authorization_token": authorization_token,
        })
    except TaskSetFenceCanaryContractError as exc:
        raise TaskSetFenceCanaryDeploymentError("invalid canary contract") from exc

    command = [
        "kubectl",
        "-n",
        _STAGING_NAMESPACE,
        "exec",
        "deploy/loom-service",
        "-c",
        "loom-service",
        "-i",
        "--",
        "python3",
        "-m",
        "loom_cli.taskset_fence_canary",
        "--internal",
    ]
    result = runner(command, json.dumps({
        "candidate_sha": contract.candidate_sha,
        "image_tag": contract.image_tag,
        "task_set_id": contract.task_set_id,
        "expected_task_checksum": contract.expected_task_checksum,
        "authorization_token": contract.authorization_token,
    }, sort_keys=True))
    if result.returncode != 0:
        raise TaskSetFenceCanaryDeploymentError("internal canary runner failed")
    try:
        evidence = _validate_evidence(json.loads(result.stdout), contract=contract)
    except json.JSONDecodeError as exc:
        raise TaskSetFenceCanaryDeploymentError("internal canary evidence was rejected") from exc

    evidence_path = rollout_dir / _EVIDENCE_RELATIVE_PATH
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with evidence_path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    except FileExistsError as exc:
        raise TaskSetFenceCanaryDeploymentError("canary evidence already exists") from exc
    return evidence_path


def add_subparser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the deployment-only command below ``loom cluster``."""
    parser = sub.add_parser(
        "taskset-fence-canary",
        help="Run the authorized disposable TaskSet fencing canary for staging.",
    )
    parser.add_argument(
        "--rollout-dir",
        type=Path,
        required=True,
        help="Completed staging rollout evidence directory under /data/loom-staging/rollouts.",
    )
    parser.add_argument("--task-set-id", required=True)
    parser.add_argument("--expected-task-checksum", required=True)
    parser.set_defaults(handler=handle)


def handle(args: argparse.Namespace) -> int:
    """Resolve the fixed rollout capability and write only sanitised evidence."""
    try:
        authorization_token = resolve_secret_source(
            _STAGING_CANARY_TOKEN_SOURCE,
            flag_name="staging taskset-fence-canary capability",
        )
        evidence_path = run_staging_fence_canary(
            rollout_dir=args.rollout_dir,
            task_set_id=args.task_set_id,
            expected_task_checksum=args.expected_task_checksum,
            authorization_token=authorization_token,
        )
    except (
        SecretSourceError,
        TaskSetFenceCanaryDeploymentError,
    ):
        sys.stderr.write("error: staging TaskSet fence canary was not run\n")
        return 2
    sys.stdout.write(json.dumps({"evidence_path": str(evidence_path)}) + "\n")
    return 0
