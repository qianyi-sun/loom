"""Release-manifest convergence checks for protected cluster rollouts."""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Literal

_Outcome = Literal["pass", "fail"]


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
    evidence = {
        "generation": generation,
        "observed_generation": observed_generation,
        "desired_replicas": desired,
        "updated_replicas": updated_replicas,
        "ready_replicas": ready_replicas,
    }
    if generation is not None and observed_generation is not None:
        if observed_generation < generation:
            return "Deployment rollout is not target-generation converged", evidence
    if desired > 0:
        if updated_replicas is None or updated_replicas < desired:
            return "Deployment rollout is not target-generation converged", evidence
        if ready_replicas is None or ready_replicas < desired:
            return "Deployment rollout is not target-generation converged", evidence
    return None


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
                evidence={"deployment_image_identities": identities if isinstance(identities, dict) else None},
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
        rollout_issue = _deployment_rollout_issue(deployment)
        selector = _deployment_selector_labels(deployment, fallback_name=str(deployment_name))
        pod_template_spec = _get_field(
            _get_field(_get_field(deployment, "spec"), "template"),
            "spec",
        )
        template_images = _container_image_by_name(pod_template_spec)
        matching_ready_pods = [
            pod
            for pod in pods
            if _pod_matches_selector(pod, selector) and _pod_ready(pod)
        ]

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
                "pod_template_image": template_images.get(str(container_name)),
                "selector": selector,
            }
            if rollout_issue is not None:
                detail, rollout_evidence = rollout_issue
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
                pod_template_image = template_images.get(str(container_name))
                if pod_template_image and pod_spec_images.get(str(container_name)) != pod_template_image:
                    continue
                saw_target_generation_pod = True
                statuses = _container_status_by_name(pod)
                container_status = statuses.get(str(container_name))
                if container_status is None:
                    continue
                live_image_id = _get_field(container_status, "image_id")
                live_image = _get_field(container_status, "image")
                pod_name = _get_field(_get_field(pod, "metadata"), "name")
                evidence = {
                    **base_evidence,
                    "pod": pod_name,
                    "live_image": live_image,
                    "live_image_id": live_image_id,
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
                                "expected_digest": _sha_from_ref(
                                    str(expected_repo_digest) if expected_repo_digest else None
                                ),
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
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="Ready pod image identity does not match release manifest",
                        evidence={
                            **(candidate_evidence or base_evidence),
                            "expected_digest": _sha_from_ref(
                                str(expected_repo_digest) if expected_repo_digest else None
                            ),
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
        _alembic_check(
            manifest=manifest,
            live_alembic_heads=live_alembic_heads,
            database_target=database_target,
            live_alembic_error=live_alembic_error,
            live_alembic_evidence=live_alembic_evidence,
        )
    )
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
        evidence={"database_target": str(database_target), "heads": sorted(str(head) for head in heads)},
    )


def release_gate_report_to_dict(report: ReleaseGateReport) -> dict[str, Any]:
    return {
        "environment": report.environment,
        "namespace": report.namespace,
        "all_pass": report.all_pass,
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
