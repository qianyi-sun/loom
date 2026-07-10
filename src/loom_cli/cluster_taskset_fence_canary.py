"""Staging-rollout launcher for the internal TaskSet fencing canary."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from errno import ENOENT
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
_SHA256_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{12}$")
_RFC3339_UTC_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$",
)
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
_STAGING_KUBE_CONTEXT = "kind-loom-staging"
_SERVICE_DEPLOYMENT = "loom-service"
_SERVICE_CONTAINER = "loom-service"
_STAGING_ROLLOUT_ROOT = Path("/data/loom-staging")
_EVIDENCE_RELATIVE_PATH = Path("canaries/taskset-lease-fencing/evidence.json")
# This operator-local reference must equal the optional loom-secrets
# taskset-fence-canary-token key mounted into loom-service.  It is deliberately
# not a user-provided flag and no raw token enters the rollout evidence tree.
_STAGING_CANARY_TOKEN_SOURCE = (
    "file:/shared_work/qianyi/loom-worker-capacity/staging-taskset-fence-canary-token"
)


class TaskSetFenceCanaryDeploymentError(RuntimeError):
    """The deployment-only launcher rejected an unsafe canary invocation."""


@dataclass(frozen=True, slots=True)
class LiveTarget:
    """The exact candidate pod selected before, then revalidated after, exec."""

    pod_name: str
    pod_uid: str
    deployment_generation: int
    service_image_digest: str


def _eligible_candidate(rollout_dir: Path, *, rollout_root: Path) -> Path:
    try:
        resolved_dir = rollout_dir.resolve(strict=True)
        resolved_root = rollout_root.resolve(strict=True)
    except OSError as exc:
        raise TaskSetFenceCanaryDeploymentError(
            "rollout is not an eligible staging candidate",
        ) from exc
    if resolved_dir.parent != resolved_root / "rollouts":
        raise TaskSetFenceCanaryDeploymentError("rollout is not an eligible staging candidate")

    return resolved_dir


def _file_open_flags() -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _read_json_from_directory(directory_fd: int, name: str) -> Mapping[str, Any]:
    try:
        file_fd = os.open(name, _file_open_flags(), dir_fd=directory_fd)
        with os.fdopen(file_fd, "r", encoding="utf-8") as handle:
            parsed = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise TaskSetFenceCanaryDeploymentError(
            "rollout is not an eligible staging candidate",
        ) from exc
    if not isinstance(parsed, Mapping):
        raise TaskSetFenceCanaryDeploymentError("rollout is not an eligible staging candidate")
    return parsed


def _candidate_from_rollout(rollout_fd: int) -> tuple[str, str]:
    inputs = _read_json_from_directory(rollout_fd, "inputs.json")
    state = _read_json_from_directory(rollout_fd, "state.json")
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


def _directory_open_flags() -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _open_directory(path: Path, *, dir_fd: int | None = None) -> int:
    try:
        if dir_fd is None:
            return os.open(path, _directory_open_flags())
        return os.open(path, _directory_open_flags(), dir_fd=dir_fd)
    except OSError as exc:
        raise TaskSetFenceCanaryDeploymentError("rollout evidence path changed") from exc


def _assert_directory_identity(directory: Path, directory_fd: int) -> None:
    try:
        path_stat = os.stat(directory, follow_symlinks=False)
        fd_stat = os.fstat(directory_fd)
    except OSError as exc:
        raise TaskSetFenceCanaryDeploymentError("rollout evidence path changed") from exc
    if (
        not stat.S_ISDIR(path_stat.st_mode)
        or path_stat.st_dev != fd_stat.st_dev
        or path_stat.st_ino != fd_stat.st_ino
    ):
        raise TaskSetFenceCanaryDeploymentError("rollout evidence path changed")


def _open_or_create_directory(parent_fd: int, name: str) -> int:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        raise TaskSetFenceCanaryDeploymentError("rollout evidence path changed") from exc
    return _open_directory(Path(name), dir_fd=parent_fd)


def _evidence_directory(rollout_fd: int) -> int:
    canaries_fd = _open_or_create_directory(rollout_fd, "canaries")
    try:
        return _open_or_create_directory(canaries_fd, "taskset-lease-fencing")
    finally:
        os.close(canaries_fd)


def _evidence_exists(evidence_dir_fd: int) -> bool:
    try:
        os.stat("evidence.json", dir_fd=evidence_dir_fd, follow_symlinks=False)
    except OSError as exc:
        if exc.errno == ENOENT:
            return False
        raise TaskSetFenceCanaryDeploymentError("rollout evidence path changed") from exc
    return True


def _discard_interrupted_evidence_temps(evidence_dir_fd: int) -> None:
    for name in os.listdir(evidence_dir_fd):
        if not name.startswith(".evidence.") or not name.endswith(".tmp"):
            continue
        try:
            file_stat = os.stat(name, dir_fd=evidence_dir_fd, follow_symlinks=False)
        except OSError:
            continue
        if stat.S_ISREG(file_stat.st_mode):
            try:
                os.unlink(name, dir_fd=evidence_dir_fd)
            except OSError:
                continue


def _publish_evidence(
    evidence_dir_fd: int,
    evidence: Mapping[str, Any],
) -> None:
    _discard_interrupted_evidence_temps(evidence_dir_fd)
    if _evidence_exists(evidence_dir_fd):
        raise TaskSetFenceCanaryDeploymentError("canary evidence already exists")
    from uuid import uuid4

    temp_name = f".evidence.{uuid4().hex}.tmp"
    payload = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temp_fd: int | None = None
    published = False
    try:
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=evidence_dir_fd,
        )
        with os.fdopen(temp_fd, "wb", closefd=True) as handle:
            temp_fd = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(
                temp_name,
                "evidence.json",
                src_dir_fd=evidence_dir_fd,
                dst_dir_fd=evidence_dir_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise TaskSetFenceCanaryDeploymentError("canary evidence already exists") from exc
        published = True
        os.fsync(evidence_dir_fd)
    except OSError as exc:
        raise TaskSetFenceCanaryDeploymentError("rollout evidence path changed") from exc
    finally:
        if temp_fd is not None:
            os.close(temp_fd)
        if published:
            with contextlib.suppress(OSError):
                os.unlink(temp_name, dir_fd=evidence_dir_fd)
                os.fsync(evidence_dir_fd)


def _invalid_evidence() -> TaskSetFenceCanaryDeploymentError:
    return TaskSetFenceCanaryDeploymentError("internal canary evidence was rejected")


def _parse_rfc3339_utc(value: object) -> datetime:
    if not isinstance(value, str) or not _RFC3339_UTC_RE.fullmatch(value):
        raise _invalid_evidence()
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise _invalid_evidence() from exc
    if parsed.tzinfo != UTC:
        raise _invalid_evidence()
    return parsed


def _invalid_live_target() -> TaskSetFenceCanaryDeploymentError:
    return TaskSetFenceCanaryDeploymentError("live staging target was rejected")


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _strict_positive_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _digest_from_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    matches: list[str] = _SHA256_DIGEST_RE.findall(value)
    if len(matches) != 1:
        return None
    return matches[0]


def _container_by_name(spec: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    containers = spec.get("containers")
    if not isinstance(containers, list):
        return None
    matches = [
        container
        for container in containers
        if isinstance(container, Mapping) and container.get("name") == name
    ]
    return matches[0] if len(matches) == 1 else None


def _release_service_identity(
    manifest: Mapping[str, Any],
    *,
    candidate_sha: str,
    image_tag: str,
) -> tuple[str, str | None, str | None]:
    release = _as_mapping(manifest.get("release"))
    cluster_config = _as_mapping(manifest.get("cluster_config"))
    rendered = _as_mapping(manifest.get("rendered_manifest"))
    identities = (
        _as_mapping(rendered.get("deployment_image_identities")) if rendered is not None else None
    )
    service_identities = (
        _as_mapping(identities.get(_SERVICE_DEPLOYMENT)) if identities is not None else None
    )
    identity = (
        _as_mapping(service_identities.get(_SERVICE_CONTAINER))
        if service_identities is not None
        else None
    )
    if (
        release is None
        or cluster_config is None
        or identity is None
        or release.get("environment") != "staging"
        or release.get("git_sha") != candidate_sha
        or release.get("image_tag") != image_tag
        or cluster_config.get("namespace") != _STAGING_NAMESPACE
    ):
        raise _invalid_live_target()
    expected_image = identity.get("image")
    if not isinstance(expected_image, str) or not expected_image.endswith(f":{image_tag}"):
        raise _invalid_live_target()
    expected_repo_digest = _digest_from_value(identity.get("repo_digest"))
    expected_image_id = identity.get("image_id")
    if expected_repo_digest is not None:
        return expected_image, expected_repo_digest, None
    if not isinstance(expected_image_id, str) or not expected_image_id:
        raise _invalid_live_target()
    return expected_image, None, expected_image_id


def _pod_is_ready(pod: Mapping[str, Any]) -> bool:
    metadata = _as_mapping(pod.get("metadata"))
    status = _as_mapping(pod.get("status"))
    if metadata is None or status is None or metadata.get("deletionTimestamp") is not None:
        return False
    if status.get("phase") != "Running":
        return False
    conditions = status.get("conditions")
    return isinstance(conditions, list) and any(
        isinstance(condition, Mapping)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    )


def _pod_matches_labels(pod: Mapping[str, Any], labels: Mapping[str, str]) -> bool:
    metadata = _as_mapping(pod.get("metadata"))
    pod_labels = _as_mapping(metadata.get("labels")) if metadata is not None else None
    return pod_labels is not None and all(
        pod_labels.get(key) == value for key, value in labels.items()
    )


def _select_live_target(
    manifest: Mapping[str, Any],
    *,
    candidate_sha: str,
    image_tag: str,
    deployment: Mapping[str, Any],
    pods: Mapping[str, Any],
) -> LiveTarget:
    """Accept only an observed-ready candidate Deployment and homogeneous Pods."""
    expected_image, expected_repo_digest, expected_image_id = _release_service_identity(
        manifest,
        candidate_sha=candidate_sha,
        image_tag=image_tag,
    )
    metadata = _as_mapping(deployment.get("metadata"))
    spec = _as_mapping(deployment.get("spec"))
    status = _as_mapping(deployment.get("status"))
    if (
        metadata is None
        or spec is None
        or status is None
        or metadata.get("name") != _SERVICE_DEPLOYMENT
    ):
        raise _invalid_live_target()
    generation = _strict_positive_int(metadata.get("generation"))
    desired_replicas = _strict_positive_int(spec.get("replicas"))
    observed_generation = _strict_positive_int(status.get("observedGeneration"))
    ready_replicas = _strict_positive_int(status.get("readyReplicas"))
    updated_replicas = _strict_positive_int(status.get("updatedReplicas"))
    total_replicas = _strict_positive_int(status.get("replicas"))
    selector = _as_mapping(spec.get("selector"))
    match_labels = _as_mapping(selector.get("matchLabels")) if selector is not None else None
    template = _as_mapping(spec.get("template"))
    template_spec = _as_mapping(template.get("spec")) if template is not None else None
    container = _container_by_name(template_spec, _SERVICE_CONTAINER) if template_spec else None
    if (
        generation is None
        or desired_replicas is None
        or observed_generation != generation
        or ready_replicas != desired_replicas
        or updated_replicas != desired_replicas
        or total_replicas != desired_replicas
        or match_labels is None
        or not match_labels
        or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in match_labels.items()
        )
        or container is None
        or container.get("image") != expected_image
    ):
        raise _invalid_live_target()
    items = pods.get("items")
    if not isinstance(items, list):
        raise _invalid_live_target()
    selected = [
        pod for pod in items if isinstance(pod, Mapping) and _pod_matches_labels(pod, match_labels)
    ]
    if len(selected) != desired_replicas:
        raise _invalid_live_target()
    accepted: list[LiveTarget] = []
    for pod in selected:
        pod_metadata = _as_mapping(pod.get("metadata"))
        pod_spec = _as_mapping(pod.get("spec"))
        pod_status = _as_mapping(pod.get("status"))
        pod_container = _container_by_name(pod_spec, _SERVICE_CONTAINER) if pod_spec else None
        statuses = pod_status.get("containerStatuses") if pod_status is not None else None
        status_matches = (
            [
                item
                for item in statuses
                if isinstance(item, Mapping) and item.get("name") == _SERVICE_CONTAINER
            ]
            if isinstance(statuses, list)
            else []
        )
        if (
            not _pod_is_ready(pod)
            or pod_metadata is None
            or not isinstance(pod_metadata.get("name"), str)
            or not isinstance(pod_metadata.get("uid"), str)
            or pod_container is None
            or pod_container.get("image") != expected_image
            or len(status_matches) != 1
            or (
                _digest_from_value(status_matches[0].get("imageID")) != expected_repo_digest
                if expected_repo_digest is not None
                else status_matches[0].get("imageID") != expected_image_id
            )
        ):
            raise _invalid_live_target()
        expected_runtime_identity = expected_repo_digest or expected_image_id
        assert expected_runtime_identity is not None
        accepted.append(
            LiveTarget(
                pod_name=pod_metadata["name"],
                pod_uid=pod_metadata["uid"],
                deployment_generation=generation,
                service_image_digest=expected_runtime_identity,
            )
        )
    return sorted(accepted, key=lambda target: target.pod_name)[0]


def _candidate_release_manifest(
    rollout_fd: int,
    *,
    image_tag: str,
) -> Mapping[str, Any]:
    release_gate_fd = _open_directory(Path("14-release-gate"), dir_fd=rollout_fd)
    try:
        return _read_json_from_directory(
            release_gate_fd,
            f"release-manifest-{image_tag}.json",
        )
    finally:
        os.close(release_gate_fd)


def _kubectl_json(
    runner: Callable[[list[str], str], subprocess.CompletedProcess[str]],
    command: list[str],
) -> Mapping[str, Any]:
    try:
        result = runner(command, "")
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise _invalid_live_target() from exc
    if result.returncode != 0:
        raise _invalid_live_target()
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise _invalid_live_target() from exc
    if not isinstance(parsed, Mapping):
        raise _invalid_live_target()
    return parsed


def _deployment_selector_text(deployment: Mapping[str, Any]) -> str:
    spec = _as_mapping(deployment.get("spec"))
    selector = _as_mapping(spec.get("selector")) if spec is not None else None
    match_labels = _as_mapping(selector.get("matchLabels")) if selector is not None else None
    if (
        match_labels is None
        or not match_labels
        or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in match_labels.items()
        )
    ):
        raise _invalid_live_target()
    return ",".join(f"{key}={value}" for key, value in sorted(match_labels.items()))


def _live_target_for_candidate(
    *,
    runner: Callable[[list[str], str], subprocess.CompletedProcess[str]],
    manifest: Mapping[str, Any],
    candidate_sha: str,
    image_tag: str,
) -> LiveTarget:
    base_command = [
        "kubectl",
        "--context",
        _STAGING_KUBE_CONTEXT,
        "-n",
        _STAGING_NAMESPACE,
    ]
    deployment = _kubectl_json(
        runner,
        [*base_command, "get", "deployment", _SERVICE_DEPLOYMENT, "-o", "json"],
    )
    selector_text = _deployment_selector_text(deployment)
    pods = _kubectl_json(
        runner,
        [*base_command, "get", "pods", "-l", selector_text, "-o", "json"],
    )
    return _select_live_target(
        manifest,
        candidate_sha=candidate_sha,
        image_tag=image_tag,
        deployment=deployment,
        pods=pods,
    )


def _evidence_with_live_target(
    evidence: Mapping[str, Any],
    target: LiveTarget,
) -> dict[str, Any]:
    return {
        **evidence,
        "schema_version": 2,
        "live_target": {
            "deployment_generation": target.deployment_generation,
            "service_image_digest": target.service_image_digest,
        },
    }


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
    if (
        set(winner)
        != {
            "job_id",
            "lease_epoch",
            "owner_fingerprint",
            "published_generation",
            "outcome",
        }
        or set(loser)
        != {
            "job_id",
            "lease_epoch",
            "owner_fingerprint",
            "outcome",
            "gc_eligible",
        }
        or set(task) != {"task_count", "checksum"}
        or set(timestamps)
        != {
            "a_staged_at",
            "b_published_at",
            "a_lease_lost_at",
        }
    ):
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
    ):
        raise _invalid_evidence()
    a_staged_at = _parse_rfc3339_utc(timestamps.get("a_staged_at"))
    b_published_at = _parse_rfc3339_utc(timestamps.get("b_published_at"))
    a_lease_lost_at = _parse_rfc3339_utc(timestamps.get("a_lease_lost_at"))
    if not a_staged_at <= b_published_at <= a_lease_lost_at:
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


def _internal_command(
    target: LiveTarget,
    *,
    prepare: bool = False,
) -> list[str]:
    command = [
        "kubectl",
        "--context",
        _STAGING_KUBE_CONTEXT,
        "-n",
        _STAGING_NAMESPACE,
        "exec",
        f"pod/{target.pod_name}",
        "-c",
        _SERVICE_CONTAINER,
        "-i",
        "--",
        "python3",
        "-m",
        "loom_cli.taskset_fence_canary",
        "--internal",
    ]
    if prepare:
        command.append("--prepare")
    return command


def _internal_contract_payload(contract: TaskSetFenceCanaryContract) -> str:
    return json.dumps(
        {
            "candidate_sha": contract.candidate_sha,
            "image_tag": contract.image_tag,
            "task_set_id": contract.task_set_id,
            "expected_task_checksum": contract.expected_task_checksum,
            "authorization_token": contract.authorization_token,
            "nonce": contract.nonce,
        },
        sort_keys=True,
    )


def _preparation_payload(
    *,
    candidate_sha: str,
    image_tag: str,
    authorization_token: str,
) -> str:
    return json.dumps(
        {
            "candidate_sha": candidate_sha,
            "image_tag": image_tag,
            "authorization_token": authorization_token,
        },
        sort_keys=True,
    )


def _contract_from_preparation(
    payload: object,
    *,
    authorization_token: str,
) -> TaskSetFenceCanaryContract:
    if not isinstance(payload, Mapping):
        raise TaskSetFenceCanaryDeploymentError("invalid canary contract")
    try:
        return TaskSetFenceCanaryContract.from_mapping(
            {
                **payload,
                "authorization_token": authorization_token,
            }
        )
    except TaskSetFenceCanaryContractError as exc:
        raise TaskSetFenceCanaryDeploymentError("invalid canary contract") from exc


def _run_internal_command(
    runner: Callable[[list[str], str], subprocess.CompletedProcess[str]],
    *,
    command: list[str],
    payload: str,
) -> subprocess.CompletedProcess[str]:
    try:
        result = runner(command, payload)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TaskSetFenceCanaryDeploymentError("internal canary runner failed") from exc
    if result.returncode != 0:
        raise TaskSetFenceCanaryDeploymentError("internal canary runner failed")
    return result


def run_staging_fence_canary(
    *,
    rollout_dir: Path,
    authorization_token: str,
    rollout_root: Path = _STAGING_ROLLOUT_ROOT,
    runner: Callable[[list[str], str], subprocess.CompletedProcess[str]] = _kubectl_runner,
) -> Path:
    """Run the one-shot canary from an already-complete staging rollout only."""
    canonical_rollout_dir = _eligible_candidate(
        rollout_dir,
        rollout_root=rollout_root,
    )
    rollout_fd = _open_directory(canonical_rollout_dir)
    try:
        _assert_directory_identity(canonical_rollout_dir, rollout_fd)
        candidate_sha, image_tag = _candidate_from_rollout(rollout_fd)
        manifest = _candidate_release_manifest(rollout_fd, image_tag=image_tag)
        evidence_dir_fd = _evidence_directory(rollout_fd)
        try:
            _discard_interrupted_evidence_temps(evidence_dir_fd)
            if _evidence_exists(evidence_dir_fd):
                raise TaskSetFenceCanaryDeploymentError("canary evidence already exists")
            live_target = _live_target_for_candidate(
                runner=runner,
                manifest=manifest,
                candidate_sha=candidate_sha,
                image_tag=image_tag,
            )
            prepared = _run_internal_command(
                runner,
                command=_internal_command(live_target, prepare=True),
                payload=_preparation_payload(
                    candidate_sha=candidate_sha,
                    image_tag=image_tag,
                    authorization_token=authorization_token,
                ),
            )
            try:
                contract = _contract_from_preparation(
                    json.loads(prepared.stdout),
                    authorization_token=authorization_token,
                )
            except json.JSONDecodeError as exc:
                raise TaskSetFenceCanaryDeploymentError("invalid canary contract") from exc
            authorized_target = _live_target_for_candidate(
                runner=runner,
                manifest=manifest,
                candidate_sha=contract.candidate_sha,
                image_tag=contract.image_tag,
            )
            if authorized_target != live_target:
                raise _invalid_live_target()
            payload = _internal_contract_payload(contract)
            result = _run_internal_command(
                runner,
                command=_internal_command(live_target),
                payload=payload,
            )
            try:
                evidence = _validate_evidence(json.loads(result.stdout), contract=contract)
            except json.JSONDecodeError as exc:
                raise TaskSetFenceCanaryDeploymentError(
                    "internal canary evidence was rejected"
                ) from exc
            post_exec_target = _live_target_for_candidate(
                runner=runner,
                manifest=manifest,
                candidate_sha=contract.candidate_sha,
                image_tag=contract.image_tag,
            )
            if post_exec_target != live_target:
                raise _invalid_live_target()
            _assert_directory_identity(canonical_rollout_dir, rollout_fd)
            _publish_evidence(
                evidence_dir_fd,
                _evidence_with_live_target(evidence, live_target),
            )
        finally:
            os.close(evidence_dir_fd)
    finally:
        os.close(rollout_fd)
    return canonical_rollout_dir / _EVIDENCE_RELATIVE_PATH


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
