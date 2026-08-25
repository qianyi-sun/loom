from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom_execution_actuator.__main__ import ActuatorRuntimeHealth, _health_app
from loom_execution_actuator.contracts import NormalizedJobState
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
    state = _ns(
        waiting=(
            _ns(reason=waiting_reason, message=f"{waiting_reason} message")
            if waiting_reason
            else None
        ),
        terminated=(
            _ns(reason=terminated_reason, message=f"{terminated_reason} message", finished_at=None)
            if terminated_reason
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
            reason=reason,
            message=f"{reason} message" if reason else None,
            conditions=[scheduled] if scheduled else [],
            start_time=None,
            container_statuses=[_ns(state=state)] if waiting_reason or terminated_reason else [],
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
            NormalizedJobState.SUCCEEDED,
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


def test_actuator_manifest_is_namespace_scoped_and_inert() -> None:
    documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-execution-actuator.yaml").read_text(encoding="utf-8")
        )
    )
    kinds = [document["kind"] for document in documents]
    assert "ClusterRole" not in kinds
    assert "ClusterRoleBinding" not in kinds
    role = next(document for document in documents if document["kind"] == "Role")
    assert role["metadata"]["namespace"] == "loom-nebius-staging"
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
    attempt = next(
        document
        for document in documents
        if document["kind"] == "ServiceAccount"
        and document["metadata"]["name"] == "loom-execution-attempt"
    )
    assert attempt["automountServiceAccountToken"] is False
    deployment = next(document for document in documents if document["kind"] == "Deployment")
    assert deployment["spec"]["replicas"] == 0
    pod = deployment["spec"]["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/readyz"
