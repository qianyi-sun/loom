"""Release-manifest convergence checks for protected cluster rollouts."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Literal

from loom_cli.canary_task_filter import task_filter_targets_only_benchmark
from loom_cli.cluster_sandbox_deadline import (
    FAILURE_CLASS as SANDBOX_DEADLINE_FAILURE_CLASS,
)
from loom_cli.cluster_sandbox_deadline import (
    RECOVERY_KIND as SANDBOX_DEADLINE_RECOVERY_KIND,
)
from loom_cli.cluster_sandbox_deadline import (
    diagnostic_summaries,
    sandbox_deadline_diagnostics_for_deployment,
)
from loom_cli.cluster_workload_trust import (
    workload_contract_environment,
    workload_contract_from_mapping,
)
from loom_cli.gb10_release_gate import gb10_release_target_mismatches

_Outcome = Literal["pass", "fail"]
_HF_MIRROR_BOUNDARY_ENVIRONMENTS = frozenset({"staging", "production"})
_HF_MIRROR_BENCHMARK_ID = "skilllearnbench"
_GB10_ALLOWED_HOST_INTENTS = frozenset({"active", "draining", "stopped"})
_GB10_DESIRED_CONTRACT_FIELDS = (
    "environment",
    "pool_name",
    "image_tag",
    "env_config_version",
    "source_git_commit",
    "max_concurrent",
    "target_slots",
    "host_intents",
)
_STAGING_GB10_HOST_INTENTS = {
    **{f"trt-gb10-{index}": "active" for index in range(1, 16) if index != 7},
    "trt-gb10-7": "stopped",
}
_STAGING_GB10_POOL_NAME = "gb10-arm64"
_STAGING_GB10_MAX_CONCURRENT = 10
_STAGING_GB10_TARGET_SLOTS = 140
_SAFE_WORKLOAD_CONTRACT_ENV_VALUES = frozenset({"internal_trusted", "True", "False"})
_RAW_SECRET_RE = re.compile(
    r"(?i)\b(?:HF_TOKEN|TOKEN|SECRET|API_KEY|ACCESS_KEY|SECRET_KEY)\s*[:=]\s*"
    r"(?!<redacted>|redacted|false|none|null|absent|isolated)[^\s,\"']{8,}"
    r"|\bhf_[A-Za-z0-9_]{20,}\b"
    r"|\b(?:sk|ghp|github_pat)_[A-Za-z0-9_]{20,}\b",
)


@dataclass(frozen=True)
class ReleaseGateCheck:
    name: str
    outcome: _Outcome
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)
    remediation: str | None = None


@dataclass(frozen=True)
class LiveAlembicHeads:
    heads: list[str]
    database_target: str = "env:LOOM_CP_DB_URL"
    error: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReleaseGateReport:
    environment: str
    namespace: str
    checks: list[ReleaseGateCheck]

    @property
    def all_pass(self) -> bool:
        return all(check.outcome == "pass" for check in self.checks)


def _get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _labels(obj: Any) -> dict[str, str]:
    raw = _get_field(obj, "labels", {}) or {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _deployment_selector_labels(deployment: Any, *, fallback_name: str) -> dict[str, str]:
    selector = _get_field(_get_field(deployment, "spec"), "selector")
    match_labels = _get_field(selector, "match_labels")
    if isinstance(match_labels, dict) and match_labels:
        return {str(key): str(value) for key, value in match_labels.items()}
    return {"app": fallback_name}


def _pod_matches_selector(pod: Any, selector: dict[str, str]) -> bool:
    pod_labels = _labels(_get_field(pod, "metadata"))
    return all(pod_labels.get(key) == value for key, value in selector.items())


def _pod_ready(pod: Any) -> bool:
    for condition in _get_field(_get_field(pod, "status"), "conditions", []) or []:
        if _get_field(condition, "type") == "Ready":
            return str(_get_field(condition, "status", "")).lower() == "true"
    return False


def _container_image_by_name(pod_spec: Any) -> dict[str, str]:
    images: dict[str, str] = {}
    for container in _get_field(pod_spec, "containers", []) or []:
        name = _get_field(container, "name")
        image = _get_field(container, "image")
        if name and image:
            images[str(name)] = str(image)
    return images


def _container_env_by_name(container: Any) -> dict[str, str]:
    values: dict[str, str] = {}
    for entry in _get_field(container, "env", []) or []:
        name = _get_field(entry, "name")
        value = _get_field(entry, "value")
        if name and isinstance(value, str):
            values[str(name)] = value
    return values


def _safe_workload_contract_actual(value: str | None) -> str | None:
    if value is None or value in _SAFE_WORKLOAD_CONTRACT_ENV_VALUES:
        return value
    return "[REDACTED]"


def _workload_contract_check(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    namespace: str,
) -> ReleaseGateCheck:
    """Require manifest and live loom-service trust contracts to agree exactly."""
    try:
        contract = workload_contract_from_mapping(manifest.get("workload_contract"))
    except ValueError as exc:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail=f"release manifest has invalid workload contract: {exc}",
            remediation=(
                "Rebuild the release manifest from a protected profile with the exact "
                "v1 workload contract."
            ),
        )

    violations = contract.v1_violations()
    if violations:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail="release manifest violates the v1 workload trust contract",
            evidence={"candidate_valid": False, "violations": violations},
            remediation=(
                "Use internal_trusted with all workload capability flags false, then "
                "rebuild and apply the candidate."
            ),
        )

    expected = workload_contract_environment(contract)
    actual: dict[str, str | None] = {name: None for name in expected}
    try:
        deployment = apps_v1.read_namespaced_deployment(
            name="loom-service",
            namespace=namespace,
        )
    except Exception as exc:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail="could not inspect the live loom-service Deployment workload contract",
            evidence={"expected": expected, "actual": actual, "error": _exception_note(exc)},
            remediation="restore Deployment read access, then rerun the release gate",
        )

    containers = _get_field(
        _get_field(_get_field(deployment, "spec"), "template"),
        "spec",
    )
    loom_service = next(
        (
            container
            for container in _get_field(containers, "containers", []) or []
            if _get_field(container, "name") == "loom-service"
        ),
        None,
    )
    if loom_service is not None:
        live_env = _container_env_by_name(loom_service)
        actual.update(
            {name: _safe_workload_contract_actual(live_env.get(name)) for name in expected}
        )

    if actual == expected:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="pass",
            detail="live loom-service workload contract matches release manifest",
            evidence={"expected": expected, "actual": actual},
        )
    return ReleaseGateCheck(
        name="workload-trust-contract",
        outcome="fail",
        detail="live loom-service workload contract does not match release manifest",
        evidence={"expected": expected, "actual": actual},
        remediation=(
            "Apply the candidate loom-service Deployment and wait for the exact v1 "
            "workload contract environment values before accepting release."
        ),
    )


def _container_status_by_name(pod: Any) -> dict[str, Any]:
    statuses: dict[str, Any] = {}
    for status in _get_field(_get_field(pod, "status"), "container_statuses", []) or []:
        name = _get_field(status, "name")
        if name:
            statuses[str(name)] = status
    return statuses


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _exception_note(exc: Exception) -> str:
    cls = type(exc).__name__
    status = getattr(exc, "status", None)
    if cls == "ApiException" and status:
        return f"k8s {status}: {str(exc)[:80]}"
    return f"{cls}: {str(exc)[:80]}"


def _is_not_found(exc: Exception) -> bool:
    if type(exc).__name__ == "ApiException" and getattr(exc, "status", None) == 404:
        return True
    return isinstance(exc, KeyError)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _sha_from_ref(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"sha256:[0-9a-fA-F]{64}", value)
    return match.group(0).lower() if match else None


def _image_identity_matches(
    *,
    expected_repo_digest: str | None,
    expected_image_id: str | None,
    live_image_id: str | None,
) -> bool:
    live_digest = _sha_from_ref(live_image_id)
    expected_digest = _sha_from_ref(expected_repo_digest)
    if expected_digest and live_digest == expected_digest:
        return True
    if expected_image_id and live_image_id:
        return live_image_id == expected_image_id
    return False


def _runtime_identity_kind(live_image_id: str | None) -> str:
    if not live_image_id:
        return "missing"
    if re.search(r"(?:^|/)import-\d{4}-\d{2}-\d{2}@", live_image_id):
        return "kind-import"
    return "runtime"


def _local_image_ref_key(image: str | None) -> str | None:
    if not image:
        return None
    for prefix in ("docker.io/library/", "library/"):
        if image.startswith(prefix):
            return image.removeprefix(prefix)
    return image


def _status_image_matches_template(
    *,
    live_image: str | None,
    pod_template_image: str | None,
) -> bool:
    if not live_image or not pod_template_image:
        return False
    return _local_image_ref_key(live_image) == _local_image_ref_key(pod_template_image)


def _hash_check(
    *,
    name: str,
    expected: str | None,
    live: str | None,
    drift_detail: str,
    remediation: str,
) -> ReleaseGateCheck:
    evidence = {
        "expected_sha256": expected,
        "live_sha256": live,
    }
    if expected and live and expected == live:
        return ReleaseGateCheck(
            name=name,
            outcome="pass",
            detail=f"{name} matches release manifest",
            evidence=evidence,
        )
    return ReleaseGateCheck(
        name=name,
        outcome="fail",
        detail=drift_detail,
        evidence=evidence,
        remediation=remediation,
    )


def _deployment_rollout_issue(deployment: Any) -> tuple[str, dict[str, Any]] | None:
    spec = _get_field(deployment, "spec")
    status = _get_field(deployment, "status")
    metadata = _get_field(deployment, "metadata")
    desired = _int_or_none(_get_field(spec, "replicas")) or 0
    generation = _int_or_none(_get_field(metadata, "generation"))
    observed_generation = _int_or_none(_get_field(status, "observed_generation"))
    updated_replicas = _int_or_none(_get_field(status, "updated_replicas"))
    ready_replicas = _int_or_none(_get_field(status, "ready_replicas"))
    total_replicas = _int_or_none(_get_field(status, "replicas"))
    evidence = {
        "generation": generation,
        "observed_generation": observed_generation,
        "desired_replicas": desired,
        "updated_replicas": updated_replicas,
        "ready_replicas": ready_replicas,
        "total_replicas": total_replicas,
    }
    if generation is not None and observed_generation is not None:
        if observed_generation < generation:
            return "Deployment rollout is not target-generation converged", evidence
    if desired > 0:
        if updated_replicas is None or updated_replicas < desired:
            return "Deployment rollout is not target-generation converged", evidence
        if ready_replicas is None or ready_replicas < desired:
            return "Deployment rollout is not target-generation converged", evidence
    if (
        updated_replicas is not None
        and total_replicas is not None
        and total_replicas > updated_replicas
    ):
        return "Deployment rollout is not target-generation converged", evidence
    return None


def _list_namespace_events(core_v1: Any, namespace: str) -> list[Any]:
    try:
        return list(core_v1.list_namespaced_event(namespace=namespace).items)
    except AttributeError:
        return []
    except Exception:
        return []


def _image_identity_checks(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    core_v1: Any,
    namespace: str,
) -> list[ReleaseGateCheck]:
    rendered = manifest.get("rendered_manifest", {})
    identities = rendered.get("deployment_image_identities", {})
    if not isinstance(identities, dict) or not identities:
        return [
            ReleaseGateCheck(
                name="image-identities-recorded",
                outcome="fail",
                detail="release manifest does not record expected image digests or IDs",
                evidence={
                    "deployment_image_identities": identities
                    if isinstance(identities, dict)
                    else None
                },
                remediation="regenerate the release manifest with expected immutable image identities",
            )
        ]

    pod_list_error: str | None
    try:
        pods = list(core_v1.list_namespaced_pod(namespace=namespace).items)
    except Exception as exc:
        pods = []
        pod_list_error = _exception_note(exc)
    else:
        pod_list_error = None
    events = _list_namespace_events(core_v1, namespace)

    checks: list[ReleaseGateCheck] = []
    for deployment_name, by_container in identities.items():
        if not isinstance(by_container, dict):
            continue
        try:
            deployment = apps_v1.read_namespaced_deployment(
                name=str(deployment_name),
                namespace=namespace,
            )
        except Exception as exc:
            for container_name in by_container:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="cannot inspect live Deployment image identity",
                        evidence={"error": _exception_note(exc)},
                    )
                )
            continue

        generation = _get_field(_get_field(deployment, "metadata"), "generation")
        observed_generation = _get_field(_get_field(deployment, "status"), "observed_generation")
        spec = _get_field(deployment, "spec")
        status = _get_field(deployment, "status")
        desired_replicas = _int_or_none(_get_field(spec, "replicas")) or 0
        updated_replicas = _int_or_none(_get_field(status, "updated_replicas"))
        ready_replicas = _int_or_none(_get_field(status, "ready_replicas"))
        rollout_issue = _deployment_rollout_issue(deployment)
        selector = _deployment_selector_labels(deployment, fallback_name=str(deployment_name))
        pod_template_spec = _get_field(
            _get_field(spec, "template"),
            "spec",
        )
        template_images = _container_image_by_name(pod_template_spec)
        matching_ready_pods = [
            pod for pod in pods if _pod_matches_selector(pod, selector) and _pod_ready(pod)
        ]
        sandbox_diagnostics = sandbox_deadline_diagnostics_for_deployment(
            deployment=deployment,
            fallback_name=str(deployment_name),
            pods=pods,
            events=events,
        )
        sandbox_diagnostic_evidence = diagnostic_summaries(sandbox_diagnostics)

        for container_name, expected_identity in by_container.items():
            if not isinstance(expected_identity, dict):
                expected_identity = {}
            expected_image = str(expected_identity.get("image") or "")
            expected_repo_digest = expected_identity.get("repo_digest")
            expected_image_id = expected_identity.get("image_id")
            base_evidence = {
                "deployment": str(deployment_name),
                "container": str(container_name),
                "expected_image": expected_image,
                "expected_repo_digest": expected_repo_digest,
                "expected_image_id": expected_image_id,
                "generation": generation,
                "observed_generation": observed_generation,
                "desired_replicas": desired_replicas,
                "updated_replicas": updated_replicas,
                "ready_replicas": ready_replicas,
                "pod_template_image": template_images.get(str(container_name)),
                "selector": selector,
            }
            if rollout_issue is not None:
                detail, rollout_evidence = rollout_issue
                if sandbox_diagnostics:
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="fail",
                            detail=("node runtime sandbox deadline blocked Deployment rollout"),
                            evidence={
                                **base_evidence,
                                **rollout_evidence,
                                "failure_class": SANDBOX_DEADLINE_FAILURE_CLASS,
                                "runtime_recovery": SANDBOX_DEADLINE_RECOVERY_KIND,
                                "sandbox_deadline_diagnostics": (sandbox_diagnostic_evidence),
                            },
                            remediation=(
                                "rerun `loom cluster up "
                                "--recover-sandbox-deadlines` so the "
                                "preflighted rollout path deletes only "
                                "classified sandbox-deadline pods and "
                                "retries readiness once; if it still fails, "
                                "inspect kind/containerd/kubelet on the node"
                            ),
                        )
                    )
                    continue
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail=detail,
                        evidence={**base_evidence, **rollout_evidence},
                        remediation="wait for the Deployment rollout to converge before accepting release",
                    )
                )
                continue
            pod_template_image = template_images.get(str(container_name))
            if expected_image and pod_template_image != expected_image:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="Deployment template image does not match release manifest",
                        evidence={
                            **base_evidence,
                            "identity_strategy": "deployment-template-image",
                        },
                        remediation="apply the rendered manifest for this release before accepting rollout",
                    )
                )
                continue
            if desired_replicas == 0:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="pass",
                        detail="zero-replica Deployment template image matches release manifest",
                        evidence={
                            **base_evidence,
                            "identity_strategy": "zero-replica-template-image",
                            "zero_replica": True,
                        },
                    )
                )
                continue
            if pod_list_error is not None:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="cannot inspect target-generation Ready pods",
                        evidence={**base_evidence, "error": pod_list_error},
                    )
                )
                continue
            if not matching_ready_pods:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="no Ready pods found for managed Deployment",
                        evidence=base_evidence,
                    )
                )
                continue

            candidate_evidence: dict[str, Any] | None = None
            saw_target_generation_pod = False
            for pod in matching_ready_pods:
                pod_spec_images = _container_image_by_name(_get_field(pod, "spec"))
                if (
                    pod_template_image
                    and pod_spec_images.get(str(container_name)) != pod_template_image
                ):
                    continue
                saw_target_generation_pod = True
                statuses = _container_status_by_name(pod)
                container_status = statuses.get(str(container_name))
                pod_name = _get_field(_get_field(pod, "metadata"), "name")
                if container_status is None:
                    candidate_evidence = {
                        **base_evidence,
                        "pod": pod_name,
                        "runtime_identity_kind": "missing",
                    }
                    continue
                live_image_id = _get_field(container_status, "image_id")
                live_image = _get_field(container_status, "image")
                runtime_identity_kind = _runtime_identity_kind(
                    str(live_image_id) if live_image_id else None
                )
                status_image_matches_template = _status_image_matches_template(
                    live_image=str(live_image) if live_image else None,
                    pod_template_image=pod_template_image,
                )
                evidence = {
                    **base_evidence,
                    "pod": pod_name,
                    "live_image": live_image,
                    "live_image_id": live_image_id,
                    "runtime_identity_kind": runtime_identity_kind,
                    "status_image_matches_template": status_image_matches_template,
                    "status_image_stale": (
                        live_image is not None
                        and pod_template_image is not None
                        and not status_image_matches_template
                    ),
                }
                candidate_evidence = evidence
                if _image_identity_matches(
                    expected_repo_digest=(
                        str(expected_repo_digest) if expected_repo_digest else None
                    ),
                    expected_image_id=(str(expected_image_id) if expected_image_id else None),
                    live_image_id=(str(live_image_id) if live_image_id else None),
                ):
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="pass",
                            detail="Ready pod image identity matches release manifest",
                            evidence={
                                **evidence,
                                "identity_strategy": "runtime-image-id-or-repo-digest",
                                "expected_digest": _sha_from_ref(
                                    str(expected_repo_digest) if expected_repo_digest else None
                                ),
                                "runtime_identity_mismatch": False,
                            },
                        )
                    )
                    break
                if runtime_identity_kind == "kind-import":
                    # The pod reached this branch only after its Pod spec image
                    # matched the Deployment template. In kind/containerd,
                    # status.containerStatuses[].image may report another tag
                    # attached to the imported image, so keep that drift in
                    # evidence without treating it as an old ReplicaSet.
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="pass",
                            detail="Ready pod uses kind-imported runtime identity for release template image",
                            evidence={
                                **evidence,
                                "identity_strategy": "kind-import-template-image",
                                "expected_digest": _sha_from_ref(
                                    str(expected_repo_digest) if expected_repo_digest else None
                                ),
                                "runtime_identity_mismatch": True,
                            },
                        )
                    )
                    break
            else:
                if not saw_target_generation_pod:
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="fail",
                            detail="no target-generation Ready pods found for managed Deployment",
                            evidence=base_evidence,
                            remediation="wait for the Deployment rollout to converge before accepting release",
                        )
                    )
                    continue
                if (
                    candidate_evidence
                    and candidate_evidence.get("runtime_identity_kind") == "missing"
                ):
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="fail",
                            detail="Ready pod is missing runtime image identity",
                            evidence=candidate_evidence,
                            remediation="wait for kubelet container status imageID before accepting release",
                        )
                    )
                    continue
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="Ready pod image identity does not match release manifest",
                        evidence={
                            **(candidate_evidence or base_evidence),
                            "identity_strategy": "runtime-image-id-or-repo-digest",
                            "expected_digest": _sha_from_ref(
                                str(expected_repo_digest) if expected_repo_digest else None
                            ),
                            "runtime_identity_mismatch": True,
                        },
                        remediation="roll the Deployment to pods built from the release manifest image digest",
                    )
                )
    return checks


def _alembic_check(
    *,
    manifest: dict[str, Any],
    live_alembic_heads: list[str],
    database_target: str,
    live_alembic_error: str | None,
    live_alembic_evidence: dict[str, Any] | None,
) -> ReleaseGateCheck:
    alembic = manifest.get("alembic", {})
    expected_heads = sorted(str(head) for head in alembic.get("expected_heads", []) or [])
    compatible_heads = sorted(
        str(head) for head in alembic.get("compatible_heads", expected_heads) or []
    )
    live_heads = sorted(str(head) for head in live_alembic_heads)
    evidence = {
        "expected_heads": expected_heads,
        "compatible_heads": compatible_heads,
        "live_heads": live_heads,
        "database_target": database_target,
    }
    if live_alembic_evidence:
        evidence.update(live_alembic_evidence)
    if live_alembic_error is not None:
        return ReleaseGateCheck(
            name="alembic-heads",
            outcome="fail",
            detail=f"could not query live DB revision state through {database_target}",
            evidence={**evidence, "error": live_alembic_error},
            remediation="restore DB connectivity, then rerun the release gate",
        )
    live_set = set(live_heads)
    expected_set = set(expected_heads)
    compatible_set = set(compatible_heads)
    if live_set == expected_set or (live_set and live_set.issubset(compatible_set)):
        return ReleaseGateCheck(
            name="alembic-heads",
            outcome="pass",
            detail=f"live DB revision matches {database_target}",
            evidence=evidence,
        )
    return ReleaseGateCheck(
        name="alembic-heads",
        outcome="fail",
        detail=f"live DB revision does not match {database_target}",
        evidence=evidence,
        remediation="run alembic upgrade head before accepting release",
    )


def _environment_state_required(manifest: dict[str, Any]) -> bool:
    external_workers = manifest.get("external_workers")
    if not isinstance(external_workers, dict):
        return False
    state_file = external_workers.get("environment_state_file")
    if isinstance(state_file, dict) and (state_file.get("path") or state_file.get("sha256")):
        return True
    for external_worker_field in ("slurm_pools", "gb10_desired_states"):
        values = external_workers.get(external_worker_field)
        if isinstance(values, list) and values:
            return True
    return False


def _environment_state_manifest_evidence(manifest: dict[str, Any]) -> dict[str, Any]:
    external_workers = manifest.get("external_workers")
    if not isinstance(external_workers, dict):
        return {}
    state_file = external_workers.get("environment_state_file")
    evidence: dict[str, Any] = {}
    if isinstance(state_file, dict):
        evidence["expected_profile"] = state_file.get("path")
        evidence["expected_profile_sha256"] = state_file.get("sha256")
    slurm_pools = external_workers.get("slurm_pools")
    gb10_desired_states = external_workers.get("gb10_desired_states")
    if isinstance(slurm_pools, list):
        evidence["slurm_pool_count"] = len(slurm_pools)
        evidence["slurm_pools"] = [
            pool.get("pool_name") if isinstance(pool, dict) else None for pool in slurm_pools
        ]
    if isinstance(gb10_desired_states, list):
        evidence["gb10_desired_state_count"] = len(gb10_desired_states)
        evidence["gb10_pools"] = [
            desired.get("pool_name") if isinstance(desired, dict) else None
            for desired in gb10_desired_states
        ]
    return evidence


def _environment_state_check(
    *,
    manifest: dict[str, Any],
    artifact: dict[str, Any] | None,
    artifact_path: str | None,
    artifact_error: str | None,
) -> ReleaseGateCheck | None:
    required = _environment_state_required(manifest)
    if not required and artifact is None and artifact_error is None:
        return None

    evidence = {
        **_environment_state_manifest_evidence(manifest),
        "artifact": artifact_path,
    }
    remediation = (
        "run environment-state apply/check: "
        "`loom admin environment-state apply` and "
        "`loom admin environment-state check --format json` for this release, "
        "then pass the check JSON artifact to `loom cluster release-gate`"
    )
    if artifact_error is not None:
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="fail",
            detail="environment-state check artifact is unreadable",
            evidence={**evidence, "error": artifact_error},
            remediation=remediation,
        )
    if artifact is None:
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="fail",
            detail="environment-state check artifact is required",
            evidence=evidence,
            remediation=remediation,
        )
    if not isinstance(artifact, dict):
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="fail",
            detail="environment-state check artifact is invalid",
            evidence={**evidence, "artifact_type": type(artifact).__name__},
            remediation=remediation,
        )

    artifact_environment = artifact.get("environment")
    manifest_environment = manifest.get("release", {}).get("environment")
    drift = artifact.get("drift")
    ok = artifact.get("ok")
    autoscaler_blockers = artifact.get("autoscaler_blockers")
    artifact_evidence = {
        **evidence,
        "environment": artifact_environment,
        "control_plane_environment": artifact.get("control_plane_environment"),
        "profile": artifact.get("profile"),
        "ok": ok,
        "drift_count": len(drift) if isinstance(drift, list) else None,
        "autoscaler_blocker_count": (
            len(autoscaler_blockers) if isinstance(autoscaler_blockers, list) else None
        ),
    }
    if manifest_environment and artifact_environment != manifest_environment:
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="fail",
            detail="environment-state check environment does not match release manifest",
            evidence={
                **artifact_evidence,
                "expected_environment": manifest_environment,
            },
            remediation="rerun environment-state check for the release manifest environment",
        )
    if not isinstance(ok, bool) or not isinstance(drift, list):
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="fail",
            detail="environment-state check artifact is invalid",
            evidence=artifact_evidence,
            remediation=remediation,
        )
    if not isinstance(autoscaler_blockers, list):
        autoscaler_blockers = []
    if ok and not drift and not autoscaler_blockers:
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="pass",
            detail="live environment-state check passed",
            evidence=artifact_evidence,
        )
    if autoscaler_blockers:
        return ReleaseGateCheck(
            name="environment-state-convergence",
            outcome="fail",
            detail="live environment-state check reports autoscaler blockers",
            evidence={
                **artifact_evidence,
                "autoscaler_blockers": autoscaler_blockers,
            },
            remediation="resolve autoscaler blockers before accepting release-gate capacity evidence",
        )
    return ReleaseGateCheck(
        name="environment-state-convergence",
        outcome="fail",
        detail="live environment-state check reports drift",
        evidence={
            **artifact_evidence,
            "drift": drift,
        },
        remediation=remediation,
    )


def _disabled_k8s_worker_check(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    core_v1: Any,
    namespace: str,
) -> ReleaseGateCheck | None:
    cluster_config = manifest.get("cluster_config")
    if not isinstance(cluster_config, dict):
        return None
    if cluster_config.get("k8s_worker_enabled") is not False:
        return None

    # Dynamic-storage profiles render loom-worker as a StatefulSet
    # (#673); static-host-path profiles keep the Deployment shape.
    # Either kind lingering after k8s_worker.enabled flips false is
    # a fail. Look at both.
    deployment_found = False
    deployment_error: str | None = None
    desired_replicas: int | None = None
    ready_replicas: int | None = None
    updated_replicas: int | None = None
    workload_kind: str | None = None
    try:
        deployment = apps_v1.read_namespaced_deployment(
            name="loom-worker",
            namespace=namespace,
        )
    except Exception as exc:
        if not _is_not_found(exc):
            deployment_error = _exception_note(exc)
    else:
        deployment_found = True
        workload_kind = "Deployment"
        desired_replicas = _int_or_none(_get_field(_get_field(deployment, "spec"), "replicas")) or 0
        status = _get_field(deployment, "status")
        ready_replicas = _int_or_none(_get_field(status, "ready_replicas")) or 0
        updated_replicas = _int_or_none(_get_field(status, "updated_replicas")) or 0

    if not deployment_found and deployment_error is None:
        read_sts = getattr(apps_v1, "read_namespaced_stateful_set", None)
        if callable(read_sts):
            try:
                sts = read_sts(name="loom-worker", namespace=namespace)
            except Exception as exc:
                if not _is_not_found(exc):
                    deployment_error = _exception_note(exc)
            else:
                deployment_found = True
                workload_kind = "StatefulSet"
                desired_replicas = (
                    _int_or_none(_get_field(_get_field(sts, "spec"), "replicas")) or 0
                )
                status = _get_field(sts, "status")
                ready_replicas = _int_or_none(_get_field(status, "ready_replicas")) or 0
                updated_replicas = _int_or_none(_get_field(status, "updated_replicas")) or 0

    ready_pods: list[str] = []
    pod_list_error: str | None = None
    try:
        pods = list(core_v1.list_namespaced_pod(namespace=namespace).items)
    except Exception as exc:
        pod_list_error = _exception_note(exc)
    else:
        for pod in pods:
            labels = _labels(_get_field(pod, "metadata"))
            if labels.get("app") != "loom-worker":
                continue
            if not _pod_ready(pod):
                continue
            name = _get_field(_get_field(pod, "metadata"), "name")
            if name:
                ready_pods.append(str(name))

    evidence: dict[str, Any] = {
        "deployment": "loom-worker",
        "namespace": namespace,
        "deployment_found": deployment_found,
        "workload_kind": workload_kind,
        "desired_replicas": desired_replicas,
        "ready_replicas": ready_replicas,
        "updated_replicas": updated_replicas,
        "ready_pods": ready_pods,
    }
    if deployment_error is not None:
        return ReleaseGateCheck(
            name="disabled-k8s-worker-pruned",
            outcome="fail",
            detail="disabled k8s worker prune state is unverifiable",
            evidence={**evidence, "deployment_error": deployment_error},
            remediation=(
                "restore Kubernetes Deployment/StatefulSet read access and rerun release-gate"
            ),
        )
    if pod_list_error is not None:
        return ReleaseGateCheck(
            name="disabled-k8s-worker-pruned",
            outcome="fail",
            detail="disabled k8s worker pod state is unverifiable",
            evidence={**evidence, "pod_list_error": pod_list_error},
            remediation="restore Kubernetes Pod list access and rerun release-gate",
        )
    if deployment_found or ready_pods:
        return ReleaseGateCheck(
            name="disabled-k8s-worker-pruned",
            outcome="fail",
            detail="disabled k8s worker remains live",
            evidence=evidence,
            remediation=(
                "rerun `loom cluster up` with the disabled-worker profile or "
                "delete stale deploy/loom-worker (or statefulset/loom-worker) "
                "and networkpolicy/loom-worker; preserve "
                "persistentvolumeclaim/loom-worker-trajectories unless an "
                "operator explicitly approves artifact deletion"
            ),
        )
    return ReleaseGateCheck(
        name="disabled-k8s-worker-pruned",
        outcome="pass",
        detail="disabled k8s worker resources are absent",
        evidence=evidence,
    )


def _gb10_worker_check(
    *,
    manifest: dict[str, Any],
    artifact: dict[str, Any] | None,
    artifact_path: str | None,
    artifact_error: str | None,
) -> ReleaseGateCheck | None:
    external_workers = manifest.get("external_workers")
    if not isinstance(external_workers, dict):
        external_workers = {}
    desired_states = external_workers.get("gb10_desired_states")
    if not isinstance(desired_states, list):
        desired_states = []
    if (
        not desired_states
        and artifact is None
        and artifact_error is None
        and not _gb10_manifest_contract_required(manifest)
    ):
        return None

    release = manifest.get("release", {})
    image_tag = release.get("image_tag")
    release_image_tag = str(image_tag) if image_tag else None
    manifest_env_versions = [
        str(row.get("env_config_version"))
        for row in desired_states
        if isinstance(row, dict) and row.get("env_config_version")
    ]
    release_env_config_version = (
        manifest_env_versions[0] if len(set(manifest_env_versions)) == 1 else release_image_tag
    )
    evidence = {
        "artifact": artifact_path,
        "manifest_desired_state_count": len(desired_states),
        "release_environment": release.get("environment"),
        "release_image_tag": release_image_tag,
        "release_env_config_version": release_env_config_version,
    }
    remediation = (
        "run `loom admin gb10-workers status --format json "
        "--release-image-tag <image-tag> --release-env-config-version "
        "<env-config-version>` for the release and pass the JSON artifact to "
        "`loom cluster release-gate --gb10-workers-status`"
    )
    manifest_contract_mismatches = _gb10_desired_contract_mismatches(
        manifest_states=desired_states,
        artifact_states=desired_states,
    )
    manifest_policy_mismatches = _gb10_manifest_policy_mismatches(
        manifest=manifest,
        desired_states=desired_states,
    )
    if manifest_contract_mismatches or manifest_policy_mismatches:
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="release manifest declares an invalid GB10 desired-state policy",
            evidence={
                **evidence,
                "manifest_contract_mismatches": manifest_contract_mismatches,
                "manifest_policy_mismatches": manifest_policy_mismatches,
            },
            remediation=(
                "rebuild the release manifest from the merged environment-state "
                "profile and exact release identity"
            ),
        )
    if artifact_error is not None:
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="GB10 worker status artifact is unreadable",
            evidence={**evidence, "error": artifact_error},
            remediation=remediation,
        )
    if artifact is None:
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="GB10 worker status artifact is required",
            evidence=evidence,
            remediation=remediation,
        )
    if not desired_states:
        artifact_desired_states = artifact.get("desired_states")
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="release manifest declares no GB10 desired state",
            evidence={
                **evidence,
                "artifact_desired_state_count": (
                    len(artifact_desired_states)
                    if isinstance(artifact_desired_states, list)
                    else None
                ),
            },
            remediation=(
                "declare gb10_worker_pool_desired_states through the release "
                "environment-state profile before accepting GB10 worker evidence"
            ),
        )
    if (
        not isinstance(artifact.get("desired_states"), list)
        or not isinstance(
            artifact.get("nodes"),
            list,
        )
        or not isinstance(artifact.get("unlinked_workers"), list)
    ):
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="GB10 worker status artifact is invalid",
            evidence={
                **evidence,
                "has_desired_states": isinstance(artifact.get("desired_states"), list),
                "has_nodes": isinstance(artifact.get("nodes"), list),
                "has_unlinked_workers": isinstance(
                    artifact.get("unlinked_workers"),
                    list,
                ),
            },
            remediation=remediation,
        )

    artifact_desired_states = artifact["desired_states"]
    contract_mismatches = _gb10_desired_contract_mismatches(
        manifest_states=desired_states,
        artifact_states=artifact_desired_states,
    )
    if contract_mismatches:
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="GB10 worker status desired state does not match release manifest",
            evidence={
                **evidence,
                "artifact_desired_state_count": len(artifact_desired_states),
                "contract_mismatches": contract_mismatches,
            },
            remediation=(
                "reapply the candidate environment-state profile and collect a fresh "
                "GB10 status artifact whose complete desired-state contract exactly "
                "matches the release manifest"
            ),
        )

    mismatches = gb10_release_target_mismatches(
        artifact,
        release_image_tag=release_image_tag,
        release_env_config_version=release_env_config_version,
    )
    if mismatches:
        return ReleaseGateCheck(
            name="gb10-worker-convergence",
            outcome="fail",
            detail="GB10 worker status reports release-target drift",
            evidence={**evidence, "mismatches": mismatches},
            remediation="drain or repair stale/unreachable GB10 hosts before release",
        )
    return ReleaseGateCheck(
        name="gb10-worker-convergence",
        outcome="pass",
        detail="GB10 worker status matches release target",
        evidence={
            **evidence,
            "desired_state_count": len(artifact.get("desired_states", [])),
            "node_count": len(artifact.get("nodes", [])),
        },
    )


def _gb10_desired_contract_mismatches(
    *,
    manifest_states: list[Any],
    artifact_states: list[Any],
) -> list[str]:
    """Compare the complete release-relevant GB10 desired-state contract."""

    def _index(
        states: list[Any],
        *,
        label: str,
    ) -> tuple[dict[tuple[str, str], dict[str, Any]], list[str]]:
        indexed: dict[tuple[str, str], dict[str, Any]] = {}
        issues: list[str] = []
        for index, row in enumerate(states):
            if not isinstance(row, dict):
                issues.append(f"{label}[{index}] must be an object")
                continue
            missing = [
                contract_field
                for contract_field in _GB10_DESIRED_CONTRACT_FIELDS
                if contract_field not in row
            ]
            if missing:
                issues.append(f"{label}[{index}] missing required fields: {','.join(missing)}")
            environment = row.get("environment")
            pool_name = row.get("pool_name")
            if not isinstance(environment, str) or not environment.strip():
                issues.append(f"{label}[{index}].environment must be non-empty")
                continue
            if not isinstance(pool_name, str) or not pool_name.strip():
                issues.append(f"{label}[{index}].pool_name must be non-empty")
                continue
            key = (environment, pool_name)
            if key in indexed:
                issues.append(f"{label} contains duplicate {environment}/{pool_name}")
                continue
            indexed[key] = row

            for contract_field in (
                "image_tag",
                "env_config_version",
                "source_git_commit",
            ):
                value = row.get(contract_field)
                if not isinstance(value, str) or not value.strip():
                    issues.append(
                        f"{label} {environment}/{pool_name} {contract_field} must be non-empty"
                    )
            for contract_field in ("max_concurrent", "target_slots"):
                value = row.get(contract_field)
                if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    issues.append(
                        f"{label} {environment}/{pool_name} {contract_field} "
                        "must be a positive integer"
                    )
            host_intents = row.get("host_intents")
            if not isinstance(host_intents, dict) or not host_intents:
                issues.append(f"{label} {environment}/{pool_name} host_intents must be non-empty")
            elif any(
                not isinstance(host, str)
                or not host.strip()
                or not isinstance(intent, str)
                or not intent.strip()
                for host, intent in host_intents.items()
            ):
                issues.append(
                    f"{label} {environment}/{pool_name} host_intents must map "
                    "non-empty host names to non-empty intents"
                )
            elif any(intent not in _GB10_ALLOWED_HOST_INTENTS for intent in host_intents.values()):
                issues.append(
                    f"{label} {environment}/{pool_name} host_intents values must be "
                    "active, draining, or stopped"
                )
            else:
                max_concurrent = row.get("max_concurrent")
                target_slots = row.get("target_slots")
                if (
                    isinstance(max_concurrent, int)
                    and not isinstance(max_concurrent, bool)
                    and isinstance(target_slots, int)
                    and not isinstance(target_slots, bool)
                ):
                    active_slots = (
                        sum(intent == "active" for intent in host_intents.values()) * max_concurrent
                    )
                    if target_slots != active_slots:
                        issues.append(
                            f"{label} {environment}/{pool_name} target_slots must equal "
                            "active host count times max_concurrent"
                        )
        return indexed, issues

    manifest_by_key, issues = _index(manifest_states, label="manifest desired_states")
    artifact_by_key, artifact_issues = _index(
        artifact_states,
        label="artifact desired_states",
    )
    issues.extend(artifact_issues)

    manifest_keys = set(manifest_by_key)
    artifact_keys = set(artifact_by_key)
    for environment, pool_name in sorted(manifest_keys - artifact_keys):
        issues.append(f"artifact desired_states missing {environment}/{pool_name}")
    for environment, pool_name in sorted(artifact_keys - manifest_keys):
        issues.append(f"artifact desired_states has unexpected {environment}/{pool_name}")
    for key in sorted(manifest_keys & artifact_keys):
        expected = manifest_by_key[key]
        actual = artifact_by_key[key]
        for contract_field in _GB10_DESIRED_CONTRACT_FIELDS:
            if actual.get(contract_field) != expected.get(contract_field):
                issues.append(
                    f"artifact desired_states {key[0]}/{key[1]} {contract_field} "
                    f"does not match release manifest"
                )
    return issues


def _gb10_manifest_policy_mismatches(
    *,
    manifest: dict[str, Any],
    desired_states: list[Any],
) -> list[str]:
    if not desired_states:
        return (
            ["staging release manifest must declare the GB10 desired-state contract"]
            if _gb10_manifest_contract_required(manifest)
            else []
        )
    release = manifest.get("release")
    if not isinstance(release, dict):
        return ["release manifest release section must be an object"]
    release_environment = release.get("environment")
    release_image_tag = release.get("image_tag")
    release_git_sha = release.get("git_sha")
    issues: list[str] = []
    if (
        not isinstance(release_git_sha, str)
        or re.fullmatch(r"[0-9a-f]{40}", release_git_sha) is None
    ):
        issues.append("release.git_sha must be a full lowercase 40-hex commit")
    external_workers = manifest.get("external_workers")
    if isinstance(external_workers, dict):
        control_plane_environment = external_workers.get("control_plane_environment")
        if (
            control_plane_environment is not None
            and control_plane_environment != release_environment
        ):
            issues.append(
                "external_workers.control_plane_environment must match release.environment"
            )
    for index, desired in enumerate(desired_states):
        if not isinstance(desired, dict):
            continue
        if desired.get("environment") != release_environment:
            issues.append(
                f"gb10_desired_states[{index}].environment must match release.environment"
            )
        if desired.get("image_tag") != release_image_tag:
            issues.append(f"gb10_desired_states[{index}].image_tag must match release.image_tag")
        if desired.get("env_config_version") != release_image_tag:
            issues.append(
                f"gb10_desired_states[{index}].env_config_version must match release.image_tag"
            )
        if desired.get("source_git_commit") != release_git_sha:
            issues.append(
                f"gb10_desired_states[{index}].source_git_commit must match release.git_sha"
            )

    if release_environment != "staging":
        return issues
    if len(desired_states) != 1 or not isinstance(desired_states[0], dict):
        issues.append("staging release must declare exactly one GB10 desired state")
        return issues
    desired = desired_states[0]
    if desired.get("pool_name") != _STAGING_GB10_POOL_NAME:
        issues.append(f"staging GB10 pool_name must be {_STAGING_GB10_POOL_NAME}")
    if desired.get("max_concurrent") != _STAGING_GB10_MAX_CONCURRENT:
        issues.append(f"staging GB10 max_concurrent must be {_STAGING_GB10_MAX_CONCURRENT}")
    if desired.get("target_slots") != _STAGING_GB10_TARGET_SLOTS:
        issues.append(f"staging GB10 target_slots must be {_STAGING_GB10_TARGET_SLOTS}")
    if desired.get("host_intents") != _STAGING_GB10_HOST_INTENTS:
        issues.append("staging GB10 host_intents must be exact active14 with trt-gb10-7 stopped")
    return issues


def _gb10_manifest_contract_required(manifest: dict[str, Any]) -> bool:
    release = manifest.get("release")
    release_environment = release.get("environment") if isinstance(release, dict) else None
    return release_environment == "staging"


def _minio_storage_preflight_check(
    *,
    artifact: dict[str, Any] | None,
    artifact_path: str | None,
    artifact_error: str | None,
) -> ReleaseGateCheck | None:
    if artifact is None and artifact_error is None:
        return None
    evidence: dict[str, Any] = {"artifact": artifact_path}
    if artifact_error is not None:
        return ReleaseGateCheck(
            name="minio-storage-pressure",
            outcome="fail",
            detail="MinIO storage preflight artifact is unreadable",
            evidence={**evidence, "error": artifact_error},
            remediation=(
                "rerun `loom cluster minio-storage-preflight --output ...` "
                "and pass the JSON artifact to release-gate"
            ),
        )
    assert artifact is not None
    filesystem = artifact.get("filesystem") if isinstance(artifact, dict) else {}
    thresholds = artifact.get("thresholds") if isinstance(artifact, dict) else {}
    checks = artifact.get("checks") if isinstance(artifact, dict) else []
    if not isinstance(filesystem, dict):
        filesystem = {}
    if not isinstance(thresholds, dict):
        thresholds = {}
    outcome = str(artifact.get("outcome") or "unknown")
    evidence.update(
        {
            "outcome": outcome,
            "free_bytes": filesystem.get("free_bytes"),
            "free_percent": filesystem.get("free_percent"),
            "used_percent": filesystem.get("used_percent"),
            "warn_free_percent": thresholds.get("warn_free_percent"),
            "stop_free_percent": thresholds.get("stop_free_percent"),
            "checks": checks if isinstance(checks, list) else [],
        }
    )
    if outcome == "stop":
        return ReleaseGateCheck(
            name="minio-storage-pressure",
            outcome="fail",
            detail="MinIO storage preflight reports stop",
            evidence=evidence,
            remediation=(
                "reclaim MinIO artifacts/trajectories, provision storage, "
                "or record an explicit operator override before large runs"
            ),
        )
    detail = (
        "MinIO storage preflight warns" if outcome == "warn" else "MinIO storage preflight passed"
    )
    return ReleaseGateCheck(
        name="minio-storage-pressure",
        outcome="pass",
        detail=detail,
        evidence=evidence,
    )


def _hf_mirror_boundary_required(manifest: dict[str, Any]) -> bool:
    release_environment = str(manifest.get("release", {}).get("environment") or "")
    if release_environment not in _HF_MIRROR_BOUNDARY_ENVIRONMENTS:
        return False
    catalog = manifest.get("catalog_provisioning")
    if not isinstance(catalog, dict):
        return release_environment == "production"
    command = str(catalog.get("command") or "").lower()
    if _HF_MIRROR_BENCHMARK_ID in command:
        return catalog.get("required") is True
    return release_environment == "production" and catalog.get("required") is True


def _int_field(mapping: dict[str, Any], key: str) -> int | None:
    value = mapping.get(key)
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _list_field(mapping: dict[str, Any], key: str) -> list[Any]:
    value = mapping.get(key)
    return list(value) if isinstance(value, list) else []


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _manifest_active_gb10_hosts(manifest: dict[str, Any]) -> list[str]:
    external_workers = manifest.get("external_workers")
    if not isinstance(external_workers, dict):
        return []
    desired_states = external_workers.get("gb10_desired_states")
    if not isinstance(desired_states, list):
        return []
    active_hosts: set[str] = set()
    for desired in desired_states:
        if not isinstance(desired, dict):
            continue
        host_intents = desired.get("host_intents")
        if not isinstance(host_intents, dict):
            continue
        active_hosts.update(
            str(host)
            for host, intent in host_intents.items()
            if isinstance(host, str) and host and intent == "active"
        )
    return sorted(active_hosts)


def _secret_leak_paths(value: Any, *, path: str = "") -> list[str]:
    if isinstance(value, str):
        return [path or "$"] if _RAW_SECRET_RE.search(value) else []
    if isinstance(value, dict):
        paths: list[str] = []
        for key, child in value.items():
            child_path = str(key) if not path else f"{path}.{key}"
            paths.extend(_secret_leak_paths(child, path=child_path))
        return paths
    if isinstance(value, list):
        paths = []
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            paths.extend(_secret_leak_paths(child, path=child_path))
        return paths
    return []


def _hf_mirror_boundary_evidence(
    *,
    manifest: dict[str, Any],
    artifact: dict[str, Any] | None,
    artifact_path: str | None,
) -> dict[str, Any]:
    catalog = manifest.get("catalog_provisioning")
    catalog_required = catalog.get("required") is True if isinstance(catalog, dict) else False
    release_environment = str(manifest.get("release", {}).get("environment") or "")
    evidence: dict[str, Any] = {
        "artifact": artifact_path,
        "benchmark_id": _HF_MIRROR_BENCHMARK_ID,
        "catalog_provisioning_required": catalog_required,
        "release_environment": release_environment,
    }
    if artifact is None:
        return evidence

    runtime_sources = artifact.get("runtime_sources")
    if not isinstance(runtime_sources, dict):
        runtime_sources = {}
    hf_provenance = artifact.get("hf_provenance")
    if not isinstance(hf_provenance, dict):
        hf_provenance = {}
    worker_boundary = artifact.get("worker_boundary")
    if not isinstance(worker_boundary, dict):
        worker_boundary = {}
    gb10_summary = worker_boundary.get("gb10_hf_token_check_summary")
    if not isinstance(gb10_summary, dict):
        gb10_summary = {}
    required_gb10_list_fields = (
        "checked_host_names",
        "ssh_failed_hosts",
        "docker_ps_failed_hosts",
        "hosts_without_containers",
        "env_file_missing_hosts",
        "env_file_hf_token_present_hosts",
        "hosts_with_container_hf_token_present",
        "inspect_failed",
    )
    gb10_summary_schema_errors = [
        f"{summary_field} must be a list"
        for summary_field in required_gb10_list_fields
        if not isinstance(gb10_summary.get(summary_field), list)
    ]
    for summary_field in ("checked_hosts", "containers_checked"):
        value = gb10_summary.get(summary_field)
        if isinstance(value, bool) or not isinstance(value, int):
            gb10_summary_schema_errors.append(f"{summary_field} must be an integer")
    secret_scan = artifact.get("secret_scan")
    if not isinstance(secret_scan, dict):
        secret_scan = {}
    catalog_evidence = artifact.get("catalog")
    if not isinstance(catalog_evidence, dict):
        catalog_evidence = {}
    requires_caps = catalog_evidence.get("requires_caps")
    if not isinstance(requires_caps, dict):
        requires_caps = {}

    total_sources = runtime_sources.get("total_task_sources")
    internal_sources = runtime_sources.get("internal_s3_sources")
    non_internal_sources = runtime_sources.get("non_internal_sources")
    runtime_sources_schema_valid = isinstance(non_internal_sources, list)
    if not isinstance(non_internal_sources, list):
        non_internal_sources = []
    sample_source = runtime_sources.get("sample_s3_source")
    upstream_kind = hf_provenance.get("upstream_kind")
    upstream_locator = hf_provenance.get("upstream_locator")
    upstream_revision = hf_provenance.get("upstream_revision")
    worker_hf_token_present = worker_boundary.get("hf_token_present")
    direct_hf_egress_required = worker_boundary.get("direct_hf_egress_required")
    secret_leaks = _secret_leak_paths(artifact)

    evidence.update(
        {
            "environment": artifact.get("environment"),
            "runnable_tasks": catalog_evidence.get("runnable_tasks"),
            "requires_cpu_arch": requires_caps.get("cpu_arch"),
            "total_task_sources": total_sources,
            "internal_s3_sources": internal_sources,
            "runtime_sources_schema_valid": runtime_sources_schema_valid,
            "non_internal_source_count": len(non_internal_sources),
            "sample_s3_source": sample_source,
            "hf_upstream_kind": upstream_kind,
            "hf_upstream_locator": upstream_locator,
            "hf_upstream_revision": upstream_revision,
            "hf_provenance_retained": (
                upstream_kind == "huggingface"
                and isinstance(upstream_locator, str)
                and bool(upstream_locator.strip())
                and isinstance(upstream_revision, str)
                and bool(upstream_revision.strip())
            ),
            "canary_started": worker_boundary.get("canary_started"),
            "terminal_state": worker_boundary.get("terminal_state"),
            "canary_task_filter": worker_boundary.get("canary_task_filter"),
            "canary_worker_pools": worker_boundary.get("canary_worker_pools"),
            "canary_expected_trial_count": worker_boundary.get("expected_trial_count"),
            "canary_succeeded_trials": worker_boundary.get("succeeded_trials"),
            "canary_task_provenance": worker_boundary.get("canary_task_provenance"),
            "worker_hf_token_present": worker_hf_token_present,
            "direct_hf_egress_required": direct_hf_egress_required,
            "materialized_from_internal_source": (
                worker_boundary.get("materialized_from_internal_source")
            ),
            "gb10_summary_schema_valid": not gb10_summary_schema_errors,
            "gb10_summary_schema_errors": gb10_summary_schema_errors,
            "gb10_expected_host_names": _manifest_active_gb10_hosts(manifest),
            "gb10_checked_hosts": _int_field(gb10_summary, "checked_hosts"),
            "gb10_checked_host_names": _list_field(gb10_summary, "checked_host_names"),
            "gb10_ssh_failed_hosts": _list_field(gb10_summary, "ssh_failed_hosts"),
            "gb10_docker_ps_failed_hosts": _list_field(
                gb10_summary,
                "docker_ps_failed_hosts",
            ),
            "gb10_hosts_without_containers": _list_field(
                gb10_summary,
                "hosts_without_containers",
            ),
            "gb10_containers_checked": _int_field(gb10_summary, "containers_checked"),
            "gb10_inspect_failed": _list_field(gb10_summary, "inspect_failed"),
            "gb10_env_file_missing_hosts": _list_field(gb10_summary, "env_file_missing_hosts"),
            "gb10_env_file_hf_token_present_hosts": _list_field(
                gb10_summary,
                "env_file_hf_token_present_hosts",
            ),
            "gb10_hosts_with_container_hf_token_present": _list_field(
                gb10_summary,
                "hosts_with_container_hf_token_present",
            ),
            "secret_safe": (
                not secret_leaks and secret_scan.get("raw_secret_values_present") is False
            ),
            "secret_leak_paths": secret_leaks,
        }
    )
    return evidence


def _hf_mirror_boundary_check(
    *,
    manifest: dict[str, Any],
    artifact: dict[str, Any] | None,
    artifact_path: str | None,
    artifact_error: str | None,
    gb10_status_artifact: dict[str, Any] | None = None,
) -> ReleaseGateCheck | None:
    required = _hf_mirror_boundary_required(manifest)
    if not required and artifact is None and artifact_error is None:
        return None

    remediation = (
        "collect secret-safe staging HF mirror boundary evidence after catalog "
        "provisioning/audit and a SkillLearnBench canary, then pass it to "
        "`loom cluster release-gate --hf-mirror-boundary-evidence`"
    )
    if artifact_error is not None:
        return ReleaseGateCheck(
            name="hf-mirror-token-boundary",
            outcome="fail",
            detail="HF mirror/token boundary evidence artifact is unreadable",
            evidence={
                **_hf_mirror_boundary_evidence(
                    manifest=manifest,
                    artifact=None,
                    artifact_path=artifact_path,
                ),
                "error": artifact_error,
            },
            remediation=remediation,
        )
    if artifact is None:
        return ReleaseGateCheck(
            name="hf-mirror-token-boundary",
            outcome="fail",
            detail="HF mirror/token boundary evidence artifact is required",
            evidence=_hf_mirror_boundary_evidence(
                manifest=manifest,
                artifact=None,
                artifact_path=artifact_path,
            ),
            remediation=remediation,
        )
    if not isinstance(artifact, dict):
        return ReleaseGateCheck(
            name="hf-mirror-token-boundary",
            outcome="fail",
            detail="HF mirror/token boundary evidence artifact is invalid",
            evidence={
                **_hf_mirror_boundary_evidence(
                    manifest=manifest,
                    artifact=None,
                    artifact_path=artifact_path,
                ),
                "artifact_type": type(artifact).__name__,
            },
            remediation=remediation,
        )

    evidence = _hf_mirror_boundary_evidence(
        manifest=manifest,
        artifact=artifact,
        artifact_path=artifact_path,
    )
    release_environment = evidence.get("release_environment")
    release = manifest.get("release")
    if not isinstance(release, dict):
        release = {}
    candidate_binding = artifact.get("candidate_binding")
    if not isinstance(candidate_binding, dict):
        candidate_binding = {}
    expected_status_sha256 = (
        _canonical_json_sha256(gb10_status_artifact)
        if isinstance(gb10_status_artifact, dict)
        else None
    )
    evidence.update(
        {
            "candidate_binding_environment": candidate_binding.get("environment"),
            "candidate_binding_image_tag": candidate_binding.get("release_image_tag"),
            "candidate_binding_git_sha": candidate_binding.get("release_git_sha"),
            "candidate_binding_status_sha256": candidate_binding.get("gb10_workers_status_sha256"),
            "expected_candidate_image_tag": release.get("image_tag"),
            "expected_candidate_git_sha": release.get("git_sha"),
            "expected_gb10_status_sha256": expected_status_sha256,
        }
    )
    issues: list[str] = []
    if artifact.get("environment") != release_environment:
        issues.append("evidence environment must match the release manifest")
    if artifact.get("benchmark_id") != _HF_MIRROR_BENCHMARK_ID:
        issues.append("evidence benchmark_id must be skilllearnbench")
    if (
        candidate_binding.get("environment") != release_environment
        or candidate_binding.get("release_image_tag") != release.get("image_tag")
        or candidate_binding.get("release_git_sha") != release.get("git_sha")
        or expected_status_sha256 is None
        or candidate_binding.get("gb10_workers_status_sha256") != expected_status_sha256
    ):
        issues.append("HF boundary evidence must bind the exact candidate and GB10 status artifact")
    runnable_tasks = evidence.get("runnable_tasks")
    if (
        isinstance(runnable_tasks, bool)
        or not isinstance(runnable_tasks, int)
        or runnable_tasks <= 0
    ):
        issues.append("SkillLearnBench catalog must report runnable tasks")
    if evidence.get("requires_cpu_arch") != "any":
        issues.append("SkillLearnBench requires_caps.cpu_arch must remain any")
    total_sources = evidence.get("total_task_sources")
    internal_sources = evidence.get("internal_s3_sources")
    if evidence.get("runtime_sources_schema_valid") is not True:
        issues.append("runtime_sources.non_internal_sources must be a list")
    if (
        not isinstance(total_sources, int)
        or isinstance(total_sources, bool)
        or total_sources <= 0
        or not isinstance(internal_sources, int)
        or isinstance(internal_sources, bool)
        or internal_sources != total_sources
        or evidence.get("non_internal_source_count") != 0
        or not str(evidence.get("sample_s3_source") or "").startswith("s3://")
    ):
        issues.append("SkillLearnBench must use internal s3:// runtime sources")
    if not evidence.get("hf_provenance_retained"):
        issues.append("HF provenance must retain upstream kind, locator, and revision")
    if evidence.get("worker_hf_token_present") is not False:
        issues.append("worker HF_TOKEN must be absent in canary evidence")
    checked_hosts = evidence.get("gb10_checked_hosts")
    checked_host_names = evidence.get("gb10_checked_host_names")
    expected_host_names = evidence.get("gb10_expected_host_names")
    containers_checked = evidence.get("gb10_containers_checked")
    checked_host_name_list = checked_host_names if isinstance(checked_host_names, list) else []
    checked_host_names_valid = isinstance(checked_host_names, list) and all(
        isinstance(host, str) and bool(host) for host in checked_host_name_list
    )
    if evidence.get("gb10_summary_schema_valid") is not True:
        issues.append("GB10 HF token check summary is incomplete or invalid")
    if (
        not isinstance(expected_host_names, list)
        or not expected_host_names
        or not isinstance(checked_hosts, int)
        or checked_hosts != len(expected_host_names)
        or not checked_host_names_valid
        or len(checked_host_name_list) != len(expected_host_names)
        or sorted(checked_host_name_list) != expected_host_names
        or not isinstance(containers_checked, int)
        or containers_checked < len(expected_host_names)
    ):
        issues.append("GB10 HF token checks must inspect the exact manifest-active host set")
    if evidence.get("gb10_ssh_failed_hosts"):
        issues.append("GB10 HF token checks must reach every required host")
    if evidence.get("gb10_docker_ps_failed_hosts"):
        issues.append("GB10 HF token checks must list containers on every active host")
    if evidence.get("gb10_hosts_without_containers"):
        issues.append("GB10 HF token checks must find a worker container on every active host")
    if evidence.get("gb10_inspect_failed"):
        issues.append("GB10 HF token checks must inspect every worker container")
    if evidence.get("gb10_env_file_missing_hosts"):
        issues.append("GB10 HF token checks must inspect the worker env file on every active host")
    if evidence.get("gb10_env_file_hf_token_present_hosts"):
        issues.append("GB10 worker env files must not contain HF_TOKEN")
    if evidence.get("gb10_hosts_with_container_hf_token_present"):
        issues.append("GB10 worker containers must not contain HF_TOKEN")
    if evidence.get("direct_hf_egress_required") is not False:
        issues.append("canary must not require direct worker HF egress")
    if evidence.get("materialized_from_internal_source") is not True:
        issues.append("canary must materialize from the internal mirror source")
    expected_trials = evidence.get("canary_expected_trial_count")
    succeeded_trials = evidence.get("canary_succeeded_trials")
    canary_worker_pools = evidence.get("canary_worker_pools")
    task_provenance = evidence.get("canary_task_provenance")
    terminal_pools = (
        canary_worker_pools.get("terminal") if isinstance(canary_worker_pools, dict) else None
    )
    active_pools = (
        canary_worker_pools.get("active") if isinstance(canary_worker_pools, dict) else None
    )
    if (
        evidence.get("canary_started") is not True
        or evidence.get("terminal_state") != "succeeded"
        or isinstance(expected_trials, bool)
        or not isinstance(expected_trials, int)
        or expected_trials <= 0
        or isinstance(succeeded_trials, bool)
        or not isinstance(succeeded_trials, int)
        or succeeded_trials != expected_trials
        or not task_filter_targets_only_benchmark(
            evidence.get("canary_task_filter"),
            _HF_MIRROR_BENCHMARK_ID,
        )
        or not isinstance(task_provenance, dict)
        or isinstance(task_provenance.get("trial_count"), bool)
        or task_provenance.get("trial_count") != expected_trials
        or isinstance(task_provenance.get("target_benchmark_trial_count"), bool)
        or task_provenance.get("target_benchmark_trial_count") != expected_trials
        or isinstance(task_provenance.get("non_target_trial_count"), bool)
        or task_provenance.get("non_target_trial_count") != 0
        or isinstance(task_provenance.get("task_set_trial_count"), bool)
        or task_provenance.get("task_set_trial_count") != 0
        or task_provenance.get("benchmark_ids") != [_HF_MIRROR_BENCHMARK_ID]
        or not isinstance(terminal_pools, dict)
        or set(terminal_pools) != {_STAGING_GB10_POOL_NAME}
        or isinstance(terminal_pools.get(_STAGING_GB10_POOL_NAME), bool)
        or not isinstance(terminal_pools.get(_STAGING_GB10_POOL_NAME), int)
        or terminal_pools.get(_STAGING_GB10_POOL_NAME) != expected_trials
        or active_pools != {}
    ):
        issues.append("canary must be a fully succeeded SkillLearnBench GB10 batch")
    if not evidence.get("secret_safe"):
        issues.append("evidence must not contain raw HF/API/object-store secrets")

    if issues:
        return ReleaseGateCheck(
            name="hf-mirror-token-boundary",
            outcome="fail",
            detail=issues[0],
            evidence={**evidence, "issues": issues},
            remediation=remediation,
        )
    return ReleaseGateCheck(
        name="hf-mirror-token-boundary",
        outcome="pass",
        detail="SkillLearnBench HF mirror/token boundary evidence passed",
        evidence=evidence,
    )


def collect_release_gate_report(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    core_v1: Any,
    namespace: str,
    rendered_manifest_sha256: str | None,
    cluster_config_sha256: str | None,
    live_alembic_heads: list[str],
    database_target: str = "env:LOOM_CP_DB_URL",
    live_alembic_error: str | None = None,
    live_alembic_evidence: dict[str, Any] | None = None,
    environment_state_check_artifact: dict[str, Any] | None = None,
    environment_state_check_path: str | None = None,
    environment_state_check_error: str | None = None,
    gb10_workers_status_artifact: dict[str, Any] | None = None,
    gb10_workers_status_path: str | None = None,
    gb10_workers_status_error: str | None = None,
    minio_storage_preflight_artifact: dict[str, Any] | None = None,
    minio_storage_preflight_path: str | None = None,
    minio_storage_preflight_error: str | None = None,
    hf_mirror_boundary_artifact: dict[str, Any] | None = None,
    hf_mirror_boundary_path: str | None = None,
    hf_mirror_boundary_error: str | None = None,
) -> ReleaseGateReport:
    environment = str(manifest.get("release", {}).get("environment") or "")
    expected_rendered = manifest.get("rendered_manifest", {}).get("sha256")
    expected_config = manifest.get("cluster_config", {}).get("sha256")
    checks = [
        _hash_check(
            name="rendered-manifest-sha256",
            expected=str(expected_rendered) if expected_rendered else None,
            live=rendered_manifest_sha256,
            drift_detail="rendered manifest hash drift",
            remediation="rerender from the release manifest inputs before accepting rollout",
        ),
        _hash_check(
            name="cluster-config-sha256",
            expected=str(expected_config) if expected_config else None,
            live=cluster_config_sha256,
            drift_detail="cluster config hash drift",
            remediation="use the cluster config that produced the release manifest",
        ),
    ]
    checks.extend(
        _image_identity_checks(
            manifest=manifest,
            apps_v1=apps_v1,
            core_v1=core_v1,
            namespace=namespace,
        )
    )
    checks.append(
        _workload_contract_check(
            manifest=manifest,
            apps_v1=apps_v1,
            namespace=namespace,
        )
    )
    checks.append(
        _alembic_check(
            manifest=manifest,
            live_alembic_heads=live_alembic_heads,
            database_target=database_target,
            live_alembic_error=live_alembic_error,
            live_alembic_evidence=live_alembic_evidence,
        )
    )
    disabled_k8s_worker_check = _disabled_k8s_worker_check(
        manifest=manifest,
        apps_v1=apps_v1,
        core_v1=core_v1,
        namespace=namespace,
    )
    if disabled_k8s_worker_check is not None:
        checks.append(disabled_k8s_worker_check)
    environment_state_check = _environment_state_check(
        manifest=manifest,
        artifact=environment_state_check_artifact,
        artifact_path=environment_state_check_path,
        artifact_error=environment_state_check_error,
    )
    if environment_state_check is not None:
        checks.append(environment_state_check)
    gb10_worker_check = _gb10_worker_check(
        manifest=manifest,
        artifact=gb10_workers_status_artifact,
        artifact_path=gb10_workers_status_path,
        artifact_error=gb10_workers_status_error,
    )
    if gb10_worker_check is not None:
        checks.append(gb10_worker_check)
    minio_storage_check = _minio_storage_preflight_check(
        artifact=minio_storage_preflight_artifact,
        artifact_path=minio_storage_preflight_path,
        artifact_error=minio_storage_preflight_error,
    )
    if minio_storage_check is not None:
        checks.append(minio_storage_check)
    hf_mirror_boundary_check = _hf_mirror_boundary_check(
        manifest=manifest,
        artifact=hf_mirror_boundary_artifact,
        artifact_path=hf_mirror_boundary_path,
        artifact_error=hf_mirror_boundary_error,
        gb10_status_artifact=gb10_workers_status_artifact,
    )
    if hf_mirror_boundary_check is not None:
        checks.append(hf_mirror_boundary_check)
    return ReleaseGateReport(
        environment=environment,
        namespace=namespace,
        checks=checks,
    )


def _redact(text: str) -> str:
    text = re.sub(
        r"postgres(?:ql)?(?:\+[^:]+)?://[^\s\"']+",
        "postgresql://<redacted>",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?i)(password|token|secret)=\S+", r"\1=<redacted>", text)
    return text


def query_live_alembic_heads(
    *,
    namespace: str,
    context: str | None = None,
    runner: Any | None = None,
    timeout_sec: int = 60,
) -> LiveAlembicHeads:
    script = (
        "import json, os\n"
        "from alembic.runtime.migration import MigrationContext\n"
        "from sqlalchemy import create_engine\n"
        "db_url = os.environ['LOOM_CP_DB_URL']\n"
        "engine = create_engine(db_url, connect_args={'connect_timeout': 10})\n"
        "with engine.connect() as conn:\n"
        "    heads = sorted(MigrationContext.configure(conn).get_current_heads())\n"
        "print(json.dumps({'database_target':'env:LOOM_CP_DB_URL','heads':heads}))\n"
    )
    cmd = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "deploy/loom-control-plane",
    ]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(["--", "python", "-c", script])
    if runner is None:
        try:
            proc = subprocess.run(
                cmd,
                text=True,
                capture_output=True,
                check=False,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            return LiveAlembicHeads(
                heads=[],
                error=f"kubectl exec timed out after {timeout_sec}s",
                evidence={
                    "command": [
                        "kubectl",
                        "exec",
                        "-n",
                        namespace,
                        "deploy/loom-control-plane",
                        "--",
                        "python",
                        "-c",
                        "<script>",
                    ],
                    "stderr": _redact(_safe_text(exc.stderr).strip())[:500],
                    "stdout": _redact(_safe_text(exc.output).strip())[:500],
                },
            )
        returncode, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
    else:
        try:
            returncode, stdout, stderr = runner(cmd)
        except subprocess.TimeoutExpired as exc:
            return LiveAlembicHeads(
                heads=[],
                error=f"kubectl exec timed out after {timeout_sec}s",
                evidence={
                    "command": [
                        "kubectl",
                        "exec",
                        "-n",
                        namespace,
                        "deploy/loom-control-plane",
                        "--",
                        "python",
                        "-c",
                        "<script>",
                    ],
                    "stderr": _redact(_safe_text(exc.stderr).strip())[:500],
                    "stdout": _redact(_safe_text(exc.output).strip())[:500],
                },
            )
    if returncode != 0:
        return LiveAlembicHeads(
            heads=[],
            error=f"kubectl exec exited {returncode}",
            evidence={
                "command": [
                    "kubectl",
                    "exec",
                    "-n",
                    namespace,
                    "deploy/loom-control-plane",
                    "--",
                    "python",
                    "-c",
                    "<script>",
                ],
                "stderr": _redact(stderr.strip())[:500],
                "stdout": _redact(stdout.strip())[:500],
            },
        )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return LiveAlembicHeads(
            heads=[],
            error=f"invalid JSON from Alembic probe: {exc}",
            evidence={"stdout": _redact(stdout.strip())[:500]},
        )
    heads = data.get("heads", [])
    if not isinstance(heads, list):
        return LiveAlembicHeads(
            heads=[],
            error="Alembic probe JSON did not contain a heads list",
            evidence={"stdout": _redact(stdout.strip())[:500]},
        )
    database_target = data.get("database_target") or "env:LOOM_CP_DB_URL"
    return LiveAlembicHeads(
        heads=sorted(str(head) for head in heads),
        database_target=str(database_target),
        evidence={
            "database_target": str(database_target),
            "heads": sorted(str(head) for head in heads),
        },
    )


def release_gate_report_to_dict(report: ReleaseGateReport) -> dict[str, Any]:
    return {
        "environment": report.environment,
        "namespace": report.namespace,
        "all_pass": report.all_pass,
        "component_evidence": build_component_evidence_rows(report),
        "checks": [
            {
                "name": check.name,
                "outcome": check.outcome,
                "detail": check.detail,
                "evidence": check.evidence,
                "remediation": check.remediation,
            }
            for check in report.checks
        ],
    }


def format_release_gate_json(report: ReleaseGateReport) -> str:
    return json.dumps(release_gate_report_to_dict(report), indent=2, sort_keys=True) + "\n"


def _first_text(*values: Any) -> str:
    for value in values:
        if value is not None and str(value) != "":
            return str(value)
    return ""


def _replica_readiness(evidence: dict[str, Any]) -> str:
    desired = evidence.get("desired_replicas")
    ready = evidence.get("ready_replicas")
    if desired is None:
        return ""
    if ready is None:
        return f"?/{desired} ready"
    return f"{ready}/{desired} ready"


def _kubernetes_component_row(check: ReleaseGateCheck) -> dict[str, Any] | None:
    if not check.name.startswith("image-identity:"):
        return None
    evidence = check.evidence
    deployment = evidence.get("deployment")
    container = evidence.get("container")
    if deployment is None or container is None:
        component = check.name.removeprefix("image-identity:")
    else:
        component = f"{deployment}/{container}"
    row_evidence = []
    if evidence.get("pod"):
        row_evidence.append(f"pod={evidence['pod']}")
    if evidence.get("identity_strategy"):
        row_evidence.append(f"strategy={evidence['identity_strategy']}")
    if evidence.get("zero_replica"):
        row_evidence.append("zero-replica")
    return {
        "surface": "kubernetes",
        "component": component,
        "expected_release": _first_text(evidence.get("expected_image")),
        "expected_digest": _first_text(
            evidence.get("expected_repo_digest"),
            evidence.get("expected_image_id"),
            evidence.get("expected_digest"),
        ),
        "live_release": _first_text(
            evidence.get("live_image"),
            evidence.get("pod_template_image"),
        ),
        "live_digest": _first_text(evidence.get("live_image_id")),
        "generation": evidence.get("generation"),
        "readiness": _replica_readiness(evidence),
        "restart_crash_reason": _first_text(
            evidence.get("restart_crash_reason"),
            evidence.get("waiting_reason"),
            evidence.get("terminated_reason"),
        ),
        "evidence": row_evidence,
        "outcome": check.outcome,
        "detail": check.detail,
    }


def _drift_component_and_job(path: str) -> tuple[str | None, str | None]:
    slurm_match = re.search(r"slurm_worker_jobs\[[^/\]]+/([^/\]]+)/([^\]]+)\]", path)
    if slurm_match:
        return slurm_match.group(1), slurm_match.group(2)
    gb10_match = re.search(r"gb10[^.\[]*\[[^/\]]+/([^/\]]+)", path)
    if gb10_match:
        return gb10_match.group(1), None
    return None, None


def _environment_state_component_rows(check: ReleaseGateCheck) -> list[dict[str, Any]]:
    if check.name != "environment-state-convergence":
        return []
    evidence = check.evidence
    components = [str(pool) for pool in evidence.get("slurm_pools", []) or [] if pool is not None]
    components.extend(
        str(pool) for pool in evidence.get("gb10_pools", []) or [] if pool is not None
    )
    if not components:
        return []

    drifts_by_component: dict[str, list[dict[str, Any]]] = {
        component: [] for component in components
    }
    jobs_by_component: dict[str, set[str]] = {component: set() for component in components}
    for drift in evidence.get("drift", []) or []:
        if not isinstance(drift, dict):
            continue
        component, job = _drift_component_and_job(str(drift.get("path") or ""))
        if component in drifts_by_component:
            drifts_by_component[component].append(drift)
            if job:
                jobs_by_component[component].add(job)

    rows: list[dict[str, Any]] = []
    for component in components:
        component_drifts = drifts_by_component.get(component, [])
        if check.outcome == "pass":
            outcome = "pass"
        elif component_drifts or not evidence.get("drift"):
            outcome = "fail"
        else:
            outcome = "pass"
        row_evidence = []
        if evidence.get("artifact"):
            row_evidence.append(str(evidence["artifact"]))
        if evidence.get("expected_profile"):
            row_evidence.append(str(evidence["expected_profile"]))
        rows.append(
            {
                "surface": "external-worker",
                "component": component,
                "expected_release": _first_text(
                    evidence.get("expected_profile"),
                    evidence.get("expected_profile_sha256"),
                ),
                "expected_digest": _first_text(evidence.get("expected_profile_sha256")),
                "live_release": _first_text(evidence.get("artifact"), evidence.get("profile")),
                "live_digest": "",
                "generation": ",".join(sorted(jobs_by_component.get(component, set()))),
                "readiness": (
                    "environment-state converged"
                    if outcome == "pass"
                    else "environment-state drift"
                ),
                "restart_crash_reason": "" if outcome == "pass" else check.detail,
                "evidence": row_evidence,
                "outcome": outcome,
                "detail": check.detail
                if outcome == "fail"
                else "live environment-state check passed",
            }
        )
    return rows


def _minio_storage_component_row(check: ReleaseGateCheck) -> dict[str, Any] | None:
    if check.name != "minio-storage-pressure":
        return None
    evidence = check.evidence
    row_evidence = []
    if evidence.get("artifact"):
        row_evidence.append(str(evidence["artifact"]))
    free_percent = evidence.get("free_percent")
    stop_free_percent = evidence.get("stop_free_percent")
    readiness = (
        f"free={free_percent}% stop={stop_free_percent}%"
        if free_percent is not None and stop_free_percent is not None
        else str(evidence.get("outcome") or "")
    )
    return {
        "surface": "object-store",
        "component": "minio-storage",
        "expected_release": f"stop_free_percent={stop_free_percent}",
        "expected_digest": "",
        "live_release": _first_text(evidence.get("outcome")),
        "live_digest": "",
        "generation": "",
        "readiness": readiness,
        "restart_crash_reason": "" if check.outcome == "pass" else check.detail,
        "evidence": row_evidence,
        "outcome": check.outcome,
        "detail": check.detail,
    }


def _hf_mirror_boundary_component_row(check: ReleaseGateCheck) -> dict[str, Any] | None:
    if check.name != "hf-mirror-token-boundary":
        return None
    evidence = check.evidence
    row_evidence = []
    if evidence.get("artifact"):
        row_evidence.append(str(evidence["artifact"]))
    if evidence.get("sample_s3_source"):
        row_evidence.append(str(evidence["sample_s3_source"]))
    readiness = (
        "s3 mirror, HF provenance, no worker HF_TOKEN" if check.outcome == "pass" else check.detail
    )
    return {
        "surface": "catalog",
        "component": "skilllearnbench-hf-mirror-boundary",
        "expected_release": "s3:// runtime sources + HF provenance",
        "expected_digest": "",
        "live_release": _first_text(evidence.get("terminal_state")),
        "live_digest": _first_text(evidence.get("hf_upstream_revision")),
        "generation": "",
        "readiness": readiness,
        "restart_crash_reason": "" if check.outcome == "pass" else check.detail,
        "evidence": row_evidence,
        "outcome": check.outcome,
        "detail": check.detail,
    }


def build_component_evidence_rows(report: ReleaseGateReport) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for check in report.checks:
        kubernetes_row = _kubernetes_component_row(check)
        if kubernetes_row is not None:
            rows.append(kubernetes_row)
        rows.extend(_environment_state_component_rows(check))
        minio_storage_row = _minio_storage_component_row(check)
        if minio_storage_row is not None:
            rows.append(minio_storage_row)
        hf_mirror_boundary_row = _hf_mirror_boundary_component_row(check)
        if hf_mirror_boundary_row is not None:
            rows.append(hf_mirror_boundary_row)
    return rows


def _markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        text = ", ".join(str(item) for item in value if item is not None and str(item) != "")
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _code_cell(value: Any) -> str:
    text = _markdown_cell(value)
    if not text:
        return ""
    return f"`{text.replace('`', '')}`"


def _release_identity_cell(row: dict[str, Any], *, release_key: str, digest_key: str) -> str:
    release = _first_text(row.get(release_key))
    digest = _first_text(row.get(digest_key))
    if release and digest and digest != release:
        return f"{release} / {digest}"
    return _first_text(release, digest)


def format_release_gate_markdown(report: ReleaseGateReport) -> str:
    lines = [
        f"Release gate: `{report.environment}` / namespace `{report.namespace}`",
        "",
        "| Surface | Component | Expected | Live | Generation/job | Readiness | Restart/crash | Evidence | Result |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in build_component_evidence_rows(report):
        expected = _release_identity_cell(
            row,
            release_key="expected_release",
            digest_key="expected_digest",
        )
        live = _release_identity_cell(
            row,
            release_key="live_release",
            digest_key="live_digest",
        )
        result = "PASS" if row.get("outcome") == "pass" else "FAIL"
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(row.get("surface")),
                    _markdown_cell(row.get("component")),
                    _code_cell(expected),
                    _code_cell(live),
                    _code_cell(row.get("generation")),
                    _markdown_cell(row.get("readiness")),
                    _markdown_cell(row.get("restart_crash_reason")),
                    _code_cell(row.get("evidence")),
                    result,
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def format_release_gate_table(report: ReleaseGateReport) -> str:
    lines = [
        f"environment: {report.environment}",
        f"namespace: {report.namespace}",
        "",
        f"{'CHECK':<42} {'OUTCOME':<8} DETAIL",
    ]
    for check in report.checks:
        lines.append(f"{check.name:<42} {check.outcome:<8} {check.detail}")
        if check.remediation and check.outcome == "fail":
            lines.append(f"  remediation: {check.remediation}")
    return "\n".join(lines) + "\n"
