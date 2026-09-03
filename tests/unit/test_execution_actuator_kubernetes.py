from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom_execution_actuator.__main__ import ActuatorRuntimeHealth, _health_app
from loom_execution_actuator.contracts import ExecutionTerminationSummaryV1, NormalizedJobState
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi, _normalize

_ROOT = Path(__file__).resolve().parents[2]


def _ns(**values: Any) -> SimpleNamespace:
    return SimpleNamespace(**values)


def _job(*, conditions: list[Any] | None = None, deleting: bool = False) -> Any:
    return _ns(
        metadata=_ns(
            labels={
                "loom.openai.com/lease-id": "00000000-0000-0000-0000-000000000001",
                "loom.openai.com/generation": "1",
            },
            annotations={
                "loom.openai.com/target-id": "nebius-eu-north1-staging",
                "loom.openai.com/execution-unit-key": ("00000000-0000-0000-0000-000000000002"),
                "loom.openai.com/runtime-contract-sha256": "sha256:" + "1" * 64,
                "loom.openai.com/command-identity-sha256": "sha256:" + "2" * 64,
                "loom.openai.com/execution-role": "attempt",
            },
            namespace="loom-nebius-staging",
            name="loom-unit-a1-g1",
            uid="job-uid",
            resource_version="42",
            deletion_timestamp=datetime.now(UTC) if deleting else None,
        ),
        status=_ns(conditions=conditions, start_time=None, completion_time=None),
    )


def _pod(
    *,
    phase: str = "Pending",
    reason: str | None = None,
    scheduled: Any | None = None,
    waiting_reason: str | None = None,
    terminated_reason: str | None = None,
    deleting: bool = False,
) -> Any:
    succeeded_summary = json.dumps(
        {
            "schema_version": "loom.execution-termination-summary.v1",
            "runtime_contract_sha256": "sha256:" + "1" * 64,
            "command_identity_sha256": "sha256:" + "2" * 64,
            "execution_role": "attempt",
            "status": "succeeded",
            "partial_evidence": False,
            "phase_count": 2,
            "finished_at": datetime.now(UTC).isoformat(),
            "result_path": "result.json",
            "output_committed": True,
            "output_upload_session_id": "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
            "output_manifest_sha256": "sha256:" + "3" * 64,
            "output_marker_sha256": "sha256:" + "4" * 64,
        }
    )
    effective_terminated_reason = terminated_reason or (
        "Completed" if phase == "Succeeded" else None
    )
    state = _ns(
        waiting=(
            _ns(reason=waiting_reason, message=f"{waiting_reason} message")
            if waiting_reason
            else None
        ),
        terminated=(
            _ns(
                reason=effective_terminated_reason,
                message=(
                    succeeded_summary
                    if phase == "Succeeded" and terminated_reason is None
                    else f"{effective_terminated_reason} message"
                ),
                finished_at=None,
                exit_code=0 if phase == "Succeeded" else 1,
            )
            if effective_terminated_reason
            else None
        ),
    )
    return _ns(
        metadata=_ns(
            creation_timestamp=datetime.now(UTC),
            deletion_timestamp=datetime.now(UTC) if deleting else None,
            uid="pod-uid",
        ),
        spec=_ns(node_name="node-a"),
        status=_ns(
            phase=phase,
            pod_ip="10.24.7.19",
            reason=reason,
            message=f"{reason} message" if reason else None,
            conditions=[scheduled] if scheduled else [],
            start_time=None,
            container_statuses=(
                [_ns(name="execution", state=state)]
                if waiting_reason or effective_terminated_reason
                else []
            ),
        ),
    )


@pytest.mark.parametrize(
    ("job", "pods", "expected"),
    [
        (_job(), [], NormalizedJobState.PENDING),
        (
            _job(),
            [
                _pod(
                    scheduled=_ns(
                        type="PodScheduled",
                        status="False",
                        reason="Unschedulable",
                        message="insufficient cpu",
                        last_transition_time=None,
                    )
                )
            ],
            NormalizedJobState.UNSCHEDULABLE,
        ),
        (_job(), [_pod(waiting_reason="ImagePullBackOff")], NormalizedJobState.IMAGE_PULL_BACKOFF),
        (_job(), [_pod(phase="Running")], NormalizedJobState.RUNNING),
        (_job(), [_pod(phase="Succeeded")], NormalizedJobState.SUCCEEDED),
        (_job(), [_pod(phase="Failed")], NormalizedJobState.FAILED),
        (
            _job(),
            [_pod(phase="Failed", terminated_reason="OOMKilled")],
            NormalizedJobState.OOM_KILLED,
        ),
        (_job(), [_pod(phase="Failed", reason="Evicted")], NormalizedJobState.EVICTED),
        (_job(), [_pod(phase="Failed", reason="NodeLost")], NormalizedJobState.NODE_LOST),
        (
            _job(conditions=[_ns(type="Failed", reason="DeadlineExceeded", message="expired")]),
            [],
            NormalizedJobState.DEADLINE_EXCEEDED,
        ),
        (_job(deleting=True), [], NormalizedJobState.TERMINATING),
        (
            _job(conditions=[_ns(type="Complete", reason=None, message=None)]),
            [],
            NormalizedJobState.FAILED,
        ),
    ],
)
def test_kubernetes_status_normalization_is_exhaustive(
    job: Any,
    pods: list[Any],
    expected: NormalizedJobState,
) -> None:
    observation = _normalize(job, pods)
    assert observation.normalized_state is expected
    assert observation.job_uid == "job-uid"
    assert observation.resource_version == "42"


def test_unschedulable_transition_is_not_reported_as_scheduled() -> None:
    job_started = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    job = _job()
    job.status.start_time = job_started
    observation = _normalize(
        job,
        [
            _pod(
                scheduled=_ns(
                    type="PodScheduled",
                    status="False",
                    reason="Unschedulable",
                    message="insufficient cpu",
                    last_transition_time=datetime(2026, 9, 3, 5, 19, tzinfo=UTC),
                )
            )
        ],
    )

    assert observation.normalized_state is NormalizedJobState.UNSCHEDULABLE
    assert observation.scheduled_at is None
    assert observation.started_at == job_started


def test_scheduled_transition_is_clamped_to_pod_start_time() -> None:
    pod_started = datetime(2026, 9, 3, 5, 16, tzinfo=UTC)
    pod = _pod(
        phase="Running",
        scheduled=_ns(
            type="PodScheduled",
            status="True",
            reason=None,
            message=None,
            last_transition_time=pod_started + timedelta(seconds=1),
        ),
    )
    pod.status.start_time = pod_started

    observation = _normalize(_job(), [pod])

    assert observation.scheduled_at == pod_started
    assert observation.started_at == pod_started


def test_termination_summary_is_identity_bound_and_retained() -> None:
    job = _job(conditions=[_ns(type="Complete", reason=None, message=None)])
    observation = _normalize(job, [_pod(phase="Succeeded")])
    assert observation.normalized_state is NormalizedJobState.SUCCEEDED
    assert observation.termination_summary is not None
    assert observation.termination_summary.phase_count == 2

    job.metadata.annotations["loom.openai.com/command-identity-sha256"] = "sha256:" + "9" * 64
    rejected = _normalize(job, [_pod(phase="Succeeded")])
    assert rejected.normalized_state is NormalizedJobState.FAILED
    assert rejected.reason == "TerminationSummaryIdentityMismatch"


@pytest.mark.parametrize(
    "status",
    [
        "artifact_upload_failed",
        "missing_required_artifacts",
        "trajectory_flush_failed",
    ],
)
def test_termination_summary_accepts_complete_bundle_failures(status: str) -> None:
    payload = {
        "schema_version": "loom.execution-termination-summary.v1",
        "runtime_contract_sha256": "sha256:" + "1" * 64,
        "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt",
        "status": status,
        "partial_evidence": True,
        "phase_count": 2,
        "finished_at": datetime.now(UTC).isoformat(),
        "result_path": "result.json",
        "output_committed": True,
        "output_upload_session_id": "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
        "output_manifest_sha256": "sha256:" + "3" * 64,
        "output_marker_sha256": "sha256:" + "4" * 64,
    }

    summary = ExecutionTerminationSummaryV1.model_validate(payload)

    assert summary.status == status


def test_kubernetes_error_translation_handles_non_integer_status() -> None:
    api = InClusterKubernetesJobApi.__new__(InClusterKubernetesJobApi)
    translated = api._translate(_ns(status="transport", headers={}), "get")
    assert translated.status_code is None
    assert translated.ambiguous is True


def test_list_quarantines_malformed_managed_job_without_poisoning_valid_inventory() -> None:
    valid = _job()
    malformed = _job()
    malformed.metadata.labels = {"app.kubernetes.io/managed-by": "loom-execution-actuator"}
    batch = _ns(
        list_namespaced_job=lambda **_: _ns(items=[valid, malformed]),
    )
    core = _ns(list_namespaced_pod=lambda **_: _ns(items=[]))
    api = InClusterKubernetesJobApi(client_module=_ns(), batch_api=batch, core_api=core)

    inventory = api._list_sync(
        "loom-nebius-staging",
        "app.kubernetes.io/managed-by=loom-execution-actuator",
    )

    assert len(inventory.observations) == 1
    assert inventory.rejected_count == 1


def test_health_readiness_requires_fresh_database_and_reconcile_success() -> None:
    runtime = ActuatorRuntimeHealth(stale_after_seconds=60)
    client = TestClient(_health_app(runtime))
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 503
    runtime.mark_success("command")
    assert client.get("/readyz").status_code == 503
    runtime.mark_success("reconcile")
    assert client.get("/readyz").json() == {"status": "ready"}


def test_actuator_manifest_is_namespace_scoped_and_active_for_development() -> None:
    documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-execution-actuator.yaml").read_text(encoding="utf-8")
        )
    )
    kinds = [document["kind"] for document in documents]
    assert "ClusterRole" not in kinds
    assert "ClusterRoleBinding" not in kinds
    role = next(document for document in documents if document["kind"] == "Role")
    assert role["metadata"]["namespace"] == "loom-nebius-development"
    assert role["rules"] == [
        {
            "apiGroups": ["batch"],
            "resources": ["jobs"],
            "verbs": ["create", "get", "list", "watch", "delete"],
        },
        {
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get", "list", "watch"],
        },
    ]
    assert not (
        {"secrets", "configmaps", "serviceaccounts"}
        & {resource for rule in role["rules"] for resource in rule["resources"]}
    )
    assert not (
        {"patch", "update", "exec", "impersonate"}
        & {verb for rule in role["rules"] for verb in rule["verbs"]}
    )
    attempt = next(
        document
        for document in documents
        if document["kind"] == "ServiceAccount"
        and document["metadata"]["name"] == "loom-execution-attempt"
    )
    assert attempt["automountServiceAccountToken"] is False
    deployment = next(document for document in documents if document["kind"] == "Deployment")
    assert deployment["spec"]["replicas"] == 1
    pod = deployment["spec"]["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/readyz"


def test_development_patch_persists_service_execution_scheduler_identity() -> None:
    patch = yaml.safe_load(
        (_ROOT / "deploy/k8s/nebius-control-plane-development-patch.yaml").read_text(
            encoding="utf-8"
        )
    )
    container = patch["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "control-plane"
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["LOOM_ENV"]["value"] == "development"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED"]["value"] == "True"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"]["value"] == "development"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID"]["value"] == "nebius-cpu"
    assert env["LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON"]["valueFrom"] == {
        "secretKeyRef": {"name": "loom-image-admission", "key": "keyring-json"}
    }


def test_development_service_patch_persists_backend_environment_identity() -> None:
    patch = yaml.safe_load(
        (_ROOT / "deploy/k8s/nebius-service-development-patch.yaml").read_text(encoding="utf-8")
    )
    container = patch["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "service"
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["LOOM_ENV"]["value"] == "development"
    assert env["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["valueFrom"] == {
        "secretKeyRef": {
            "name": "loom-service-execution-runtime-profile",
            "key": "profile-json",
        }
    }


def test_development_gateway_patch_persists_model_provider_identity() -> None:
    patch = yaml.safe_load(
        (_ROOT / "deploy/k8s/nebius-gateway-development-patch.yaml").read_text(encoding="utf-8")
    )
    container = patch["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "gateway"
    env = {entry["name"]: entry for entry in container["env"]}
    assert env["LOOM_ENV"]["value"] == "development"
    assert env["LOOM_GW_LOCAL_YIBU_BASE_URL"]["value"] == "https://yibuapi.com/v1"
    assert env["LOOM_GW_LOCAL_YIBU_API_KEY"]["valueFrom"] == {
        "secretKeyRef": {"name": "loom-nebius-model-provider", "key": "api-key"}
    }


def test_attempt_network_policy_is_default_deny_with_exact_egress_peers() -> None:
    actuator_documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-execution-actuator.yaml").read_text(encoding="utf-8")
        )
    )
    policies = {
        document["metadata"]["name"]: document
        for document in actuator_documents
        if document["kind"] == "NetworkPolicy"
    }
    selector = {"app.kubernetes.io/component": "execution-unit"}
    deny = policies["loom-execution-attempt-default-deny"]
    assert deny["metadata"]["namespace"] == "loom-nebius-development"
    assert deny["spec"] == {
        "podSelector": {"matchLabels": selector},
        "policyTypes": ["Ingress", "Egress"],
        "ingress": [],
        "egress": [],
    }

    allow = policies["loom-execution-attempt-egress"]
    assert allow["spec"]["podSelector"] == {"matchLabels": selector}
    assert allow["spec"]["policyTypes"] == ["Egress"]
    assert "ingress" not in allow["spec"]
    expected = {
        ("kube-system", "k8s-app", ("coredns", "kube-dns"), 53, "UDP"),
        ("kube-system", "k8s-app", ("coredns", "kube-dns"), 53, "TCP"),
        ("loom", "app", ("loom-llm-gateway",), 9100, "TCP"),
    }
    actual: set[tuple[str, str, tuple[str, ...], int, str]] = set()
    for rule in allow["spec"]["egress"]:
        assert len(rule["to"]) == 1
        peer = rule["to"][0]
        assert "ipBlock" not in peer
        namespace = peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        pod_selector = peer["podSelector"]
        if "matchLabels" in pod_selector:
            pod_labels = pod_selector["matchLabels"]
            assert len(pod_labels) == 1
            label_name, label_value = next(iter(pod_labels.items()))
            label_values = (label_value,)
        else:
            expressions = pod_selector["matchExpressions"]
            assert len(expressions) == 1
            expression = expressions[0]
            assert expression["operator"] == "In"
            label_name = expression["key"]
            label_values = tuple(sorted(expression["values"]))
        for port in rule["ports"]:
            actual.add((namespace, label_name, label_values, port["port"], port["protocol"]))
    assert actual == expected


def test_platform_network_policies_admit_only_execution_units_from_nebius_namespace() -> None:
    documents = list(
        yaml.safe_load_all((_ROOT / "deploy/k8s/network-policies.yaml").read_text(encoding="utf-8"))
    )
    policies = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "NetworkPolicy"
    }
    for name, port in (("loom-llm-gateway", 9100),):
        policy = policies[name]
        ingress = next(
            rule for rule in policy["spec"]["ingress"] if rule["ports"][0]["port"] == port
        )
        nebius_peers = [
            peer
            for peer in ingress["from"]
            if peer.get("namespaceSelector", {})
            .get("matchLabels", {})
            .get("kubernetes.io/metadata.name")
            == "loom-nebius-development"
        ]
        assert nebius_peers == [
            {
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "loom-nebius-development"}
                },
                "podSelector": {"matchLabels": {"app.kubernetes.io/component": "execution-unit"}},
            }
        ]
    minio = policies["loom-minio"]
    assert not any(
        peer.get("namespaceSelector", {}).get("matchLabels", {}).get("kubernetes.io/metadata.name")
        == "loom-nebius-development"
        for rule in minio["spec"]["ingress"]
        for peer in rule["from"]
    )


def test_platform_network_policies_support_kube_dns_and_coredns_labels() -> None:
    documents = list(
        yaml.safe_load_all((_ROOT / "deploy/k8s/network-policies.yaml").read_text(encoding="utf-8"))
    )
    dns_rules = []
    for document in documents:
        if document.get("kind") != "NetworkPolicy":
            continue
        for rule in document["spec"].get("egress", []):
            if {port.get("port") for port in rule.get("ports", [])} == {53}:
                dns_rules.append(rule)

    assert dns_rules
    for rule in dns_rules:
        assert rule["to"] == [
            {
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                },
                "podSelector": {
                    "matchExpressions": [
                        {
                            "key": "k8s-app",
                            "operator": "In",
                            "values": ["kube-dns", "coredns"],
                        }
                    ]
                },
            }
        ]
