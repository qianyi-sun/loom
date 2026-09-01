from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml

from loom_execution_capacity_collector.collector import collect_capacity_observation
from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings
from loom_execution_capacity_collector.contracts import (
    CapacityObservationReceipt,
    CapacityPolicyBinding,
    KubernetesCapacitySnapshot,
    ProviderCapacitySnapshot,
    ResourceTotals,
)
from loom_execution_capacity_collector.control_plane import CapacityControlPlaneClient
from loom_execution_capacity_collector.kubernetes import (
    InClusterKubernetesCapacityReader,
    KubernetesObservationError,
    _pod_request,
)
from loom_execution_capacity_collector.nebius import (
    NebiusCapacityReader,
    NebiusObservationError,
)
from loom_execution_capacity_collector.secret_init import copy_projected_credentials

_ROOT = Path(__file__).resolve().parents[2]


def _settings(tmp_path: Path) -> ExecutionCapacityCollectorSettings:
    return ExecutionCapacityCollectorSettings(
        target_id="nebius-eu-north1-staging",
        pool_id="nebius-cpu",
        namespace="loom-nebius-staging",
        node_label_selector="loom.openai.com/execution-target=nebius-eu-north1-staging",
        nebius_project_id="project-test",
        nebius_quota_parent_id="tenant-test",
        nebius_node_group_id="nodegroup-test",
        nebius_region="eu-north1",
        nebius_credentials_file=tmp_path / "nebius.json",
        control_plane_url="https://loom.test",
        control_plane_bearer_token_file=tmp_path / "token",
        quota_nodes_name="non-gpu-vms",
        quota_vcpu_name="non-gpu-vcpu",
        quota_memory_name="non-gpu-memory",
        quota_storage_name="ssd-storage",
        quota_nodes_unit="count",
        quota_vcpu_unit="vcpu",
        quota_memory_unit="byte",
        quota_storage_unit="byte",
    )


class _ControlPlane:
    def __init__(self) -> None:
        self.observations: list[Any] = []

    async def fetch_policy(self, *, target_id: str, pool_id: str) -> CapacityPolicyBinding:
        return CapacityPolicyBinding(
            target_id=target_id,
            pool_id=pool_id,
            enabled=True,
            max_nodes=10,
            node_cpu_millis=4000,
            node_memory_mib=8192,
            node_storage_mib=102400,
            version=3,
        )

    async def publish(self, observation: Any) -> CapacityObservationReceipt:
        self.observations.append(observation)
        return CapacityObservationReceipt(
            id="observation-1",
            created=True,
            target_id=observation.target_id,
            source=observation.source,
            source_version=observation.source_version,
            observed_at=observation.observed_at,
            provider_capacity_state=observation.provider_capacity_state,
            autoscaler_state=observation.autoscaler_state,
            observation_sha256="sha256:" + "a" * 64,
        )


class _Provider:
    async def capture(self, _policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
        return ProviderCapacitySnapshot(
            source_versions={"node_group": "7", "quota_nodes": "11"},
            provider_capacity_state="available",
            provider_capacity_reason="node_group_running_without_error_events",
            autoscaler_state="scaling",
            autoscaler_reason="node_group_reconciling",
            quota_nodes=20,
            quota_vcpu_millis=80_000,
            quota_memory_mib=160_000,
            quota_storage_mib=2_000_000,
            used_nodes=3,
            used_vcpu_millis=12_000,
            used_memory_mib=25_000,
            used_storage_mib=310_000,
            node_count=3,
            target_node_count=4,
            ready_node_count=3,
        )


class _Kubernetes:
    async def capture(self, **_kwargs: Any) -> KubernetesCapacitySnapshot:
        return KubernetesCapacitySnapshot(
            source_versions={"nodes": "21", "pods": "34"},
            active_nodes=3,
            ready_nodes=3,
            provisioned=ResourceTotals(
                cpu_millis=12_000,
                memory_mib=24_576,
                storage_mib=307_200,
            ),
            allocatable=ResourceTotals(
                cpu_millis=10_500,
                memory_mib=22_000,
                storage_mib=280_000,
            ),
            requested=ResourceTotals(
                cpu_millis=6_000,
                memory_mib=12_000,
                storage_mib=100_000,
            ),
            pending_jobs=1,
            unschedulable_jobs=0,
            image_pull_backoff_jobs=0,
            pending_reasons={"Pending": 1},
        )


@pytest.mark.asyncio
async def test_collector_publishes_only_after_complete_provider_and_cluster_capture(
    tmp_path: Path,
) -> None:
    control_plane = _ControlPlane()
    observed_at = datetime(2026, 8, 26, 22, 0, tzinfo=UTC)

    receipt = await collect_capacity_observation(
        _settings(tmp_path),
        control_plane=control_plane,
        provider=_Provider(),
        kubernetes=_Kubernetes(),
        now=observed_at,
    )

    assert receipt.created is True
    observation = control_plane.observations[0]
    assert observation.active_nodes == 4
    assert observation.provisioned_vcpu_millis == 16_000
    assert observation.provisioned_memory_mib == 32_768
    assert observation.provisioned_storage_mib == 409_600
    assert observation.source_version.startswith("sha256:")
    assert observation.observed_at == observed_at


@pytest.mark.asyncio
async def test_collector_never_publishes_a_partial_snapshot(tmp_path: Path) -> None:
    class FailedProvider:
        async def capture(self, _policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
            raise NebiusObservationError("quota usage is unknown")

    control_plane = _ControlPlane()
    with pytest.raises(NebiusObservationError, match="quota usage is unknown"):
        await collect_capacity_observation(
            _settings(tmp_path),
            control_plane=control_plane,
            provider=FailedProvider(),
            kubernetes=_Kubernetes(),
        )
    assert control_plane.observations == []


def _resource_container(
    *, cpu: str, memory: str = "0", storage: str = "0", restart_policy: str | None = None
) -> Any:
    return SimpleNamespace(
        resources=SimpleNamespace(
            requests={"cpu": cpu, "memory": memory, "ephemeral-storage": storage}
        ),
        restart_policy=restart_policy,
    )


def test_kubernetes_scheduler_request_includes_restartable_init_sidecars() -> None:
    pod = SimpleNamespace(
        spec=SimpleNamespace(
            containers=[_resource_container(cpu="1")],
            init_containers=[
                _resource_container(cpu="100m", restart_policy="Always"),
                _resource_container(cpu="2"),
            ],
            overhead={"cpu": "50m"},
        )
    )
    assert _pod_request(pod).cpu_millis == 2150


def _node(name: str) -> Any:
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, deletion_timestamp=None),
        spec=SimpleNamespace(unschedulable=False),
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True")],
            capacity={"cpu": "4", "memory": "8Gi", "ephemeral-storage": "100Gi"},
            allocatable={"cpu": "3500m", "memory": "7Gi", "ephemeral-storage": "90Gi"},
        ),
    )


def _pod(
    *,
    name: str,
    namespace: str,
    node_name: str | None,
    target: bool,
    pending: bool = False,
) -> Any:
    conditions = (
        [SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable")]
        if pending
        else []
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=namespace,
            deletion_timestamp=None,
            labels=({"app.kubernetes.io/managed-by": "loom-execution-actuator"} if target else {}),
            annotations=(
                {"loom.openai.com/target-id": "nebius-eu-north1-staging"} if target else {}
            ),
        ),
        spec=SimpleNamespace(
            node_name=node_name,
            containers=[
                _resource_container(cpu="500m" if not pending else "2", memory="1Gi", storage="1Gi")
            ],
            init_containers=[],
            overhead={},
        ),
        status=SimpleNamespace(
            phase="Pending" if pending else "Running",
            reason=None,
            conditions=conditions,
            init_container_statuses=[],
            container_statuses=[],
        ),
    )


@pytest.mark.asyncio
async def test_kubernetes_capture_counts_selected_node_load_and_target_pending_demand() -> None:
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[_node("node-1")], metadata=SimpleNamespace(resource_version="nodes-7")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[
                _pod(
                    name="system",
                    namespace="kube-system",
                    node_name="node-1",
                    target=False,
                ),
                _pod(
                    name="pending",
                    namespace="loom-nebius-staging",
                    node_name=None,
                    target=True,
                    pending=True,
                ),
            ],
            metadata=SimpleNamespace(resource_version="pods-9"),
        ),
    )
    snapshot = await InClusterKubernetesCapacityReader(core_api=core).capture(
        namespace="loom-nebius-staging",
        target_id="nebius-eu-north1-staging",
        node_label_selector="target=staging",
    )
    assert snapshot.allocatable.cpu_millis == 3500
    assert snapshot.requested.cpu_millis == 2500
    assert snapshot.requested.memory_mib == 2048
    assert snapshot.pending_jobs == 1
    assert snapshot.unschedulable_jobs == 1
    assert snapshot.pending_reasons == {"Unschedulable": 1}


@pytest.mark.asyncio
async def test_kubernetes_capture_rejects_target_pod_outside_bound_node_group() -> None:
    core = SimpleNamespace(
        list_node=lambda **_: SimpleNamespace(
            items=[_node("node-1")], metadata=SimpleNamespace(resource_version="nodes-7")
        ),
        list_pod_for_all_namespaces=lambda **_: SimpleNamespace(
            items=[
                _pod(
                    name="drift",
                    namespace="loom-nebius-staging",
                    node_name="node-other",
                    target=True,
                )
            ],
            metadata=SimpleNamespace(resource_version="pods-9"),
        ),
    )
    with pytest.raises(KubernetesObservationError, match="outside the selected node group"):
        await InClusterKubernetesCapacityReader(core_api=core).capture(
            namespace="loom-nebius-staging",
            target_id="nebius-eu-north1-staging",
            node_label_selector="target=staging",
        )


def _enum(name: str) -> Any:
    return SimpleNamespace(name=name)


def _quota(name: str, unit: str, limit: int, usage: int, version: int) -> Any:
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, resource_version=version),
        spec=SimpleNamespace(region="eu-north1", limit=limit),
        status=SimpleNamespace(
            state=_enum("STATE_ACTIVE"),
            usage_state=_enum("USAGE_STATE_USED"),
            service="compute",
            unit=unit,
            usage=usage,
        ),
    )


@pytest.mark.asyncio
async def test_nebius_reader_validates_quota_units_region_and_node_group_state(
    tmp_path: Path,
) -> None:
    gib = 1024**3
    quota_client = SimpleNamespace(
        list=lambda *_args, **_kwargs: _awaitable(
            SimpleNamespace(
                items=[
                    _quota("non-gpu-vms", "count", 20, 3, 1),
                    _quota("non-gpu-vcpu", "vcpu", 80, 12, 2),
                    _quota("non-gpu-memory", "byte", 160 * gib, 24 * gib, 3),
                    _quota("ssd-storage", "byte", 2000 * gib, 300 * gib, 4),
                ],
                next_page_token="",
            )
        )
    )
    node_group_client = SimpleNamespace(
        get=lambda *_args, **_kwargs: _awaitable(
            SimpleNamespace(
                metadata=SimpleNamespace(
                    id="nodegroup-test", parent_id="cluster-test", resource_version=9
                ),
                spec=SimpleNamespace(autoscaling=SimpleNamespace(max_node_count=10)),
                status=SimpleNamespace(
                    state=_enum("RUNNING"),
                    node_count=3,
                    target_node_count=4,
                    ready_node_count=3,
                    reconciling=True,
                    events=[],
                ),
            )
        )
    )
    reader = NebiusCapacityReader(
        _settings(tmp_path),
        sdk=object(),
        quota_client=quota_client,
        node_group_client=node_group_client,
    )
    snapshot = await reader.capture(await _ControlPlane().fetch_policy(target_id="x", pool_id="y"))
    assert snapshot.quota_vcpu_millis == 80_000
    assert snapshot.used_memory_mib == 24 * 1024
    assert snapshot.provider_capacity_state == "available"
    assert snapshot.autoscaler_state == "scaling"


@pytest.mark.asyncio
async def test_nebius_reader_uses_tenant_quotas_and_derives_unexposed_memory(
    tmp_path: Path,
) -> None:
    gib = 1024**3
    observed_parents: list[str] = []

    async def list_quotas(request: Any, **_kwargs: Any) -> Any:
        observed_parents.append(request.parent_id)
        return SimpleNamespace(
            items=[
                _quota("non-gpu-vms", "count", 20, 3, 1),
                _quota("non-gpu-vcpu", "vcpu", 80, 12, 2),
                _quota("ssd-storage", "byte", 2000 * gib, 300 * gib, 4),
            ],
            next_page_token="",
        )

    settings = _settings(tmp_path).model_copy(
        update={"quota_memory_name": None, "quota_memory_unit": None}
    )
    reader = NebiusCapacityReader(
        settings,
        sdk=object(),
        quota_client=SimpleNamespace(list=list_quotas),
        node_group_client=SimpleNamespace(
            get=lambda *_args, **_kwargs: _awaitable(
                SimpleNamespace(
                    metadata=SimpleNamespace(
                        id="nodegroup-test",
                        parent_id="cluster-test",
                        resource_version=9,
                    ),
                    spec=SimpleNamespace(autoscaling=SimpleNamespace(max_node_count=10)),
                    status=SimpleNamespace(
                        state=_enum("RUNNING"),
                        node_count=3,
                        target_node_count=3,
                        ready_node_count=3,
                        reconciling=False,
                        events=[],
                    ),
                )
            )
        ),
    )
    policy = await _ControlPlane().fetch_policy(target_id="x", pool_id="y")
    snapshot = await reader.capture(policy)
    assert observed_parents == ["tenant-test"]
    assert snapshot.quota_memory_mib == policy.max_nodes * policy.node_memory_mib
    assert snapshot.used_memory_mib == 3 * policy.node_memory_mib
    assert snapshot.source_versions["quota_memory"] == "derived:policy-3:node-group-9"


async def _return(value: Any) -> Any:
    return value


def _awaitable(value: Any) -> Any:
    return _return(value)


@pytest.mark.asyncio
async def test_control_plane_client_binds_policy_and_receipt(tmp_path: Path) -> None:
    token = tmp_path / "token"
    token.write_text("secret-token", encoding="utf-8")
    token.chmod(0o600)
    observed_at = datetime(2026, 8, 26, 22, 0, tzinfo=UTC)

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer secret-token"
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "target_id": "target",
                    "pool_id": "pool",
                    "enabled": True,
                    "max_nodes": 2,
                    "node_cpu_millis": 4000,
                    "node_memory_mib": 8192,
                    "node_storage_mib": 102400,
                    "version": 1,
                },
            )
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "id": "id-1",
                "created": True,
                "target_id": body["target_id"],
                "source": body["source"],
                "source_version": body["source_version"],
                "observed_at": body["observed_at"],
                "provider_capacity_state": body["provider_capacity_state"],
                "autoscaler_state": body["autoscaler_state"],
                "observation_sha256": "sha256:" + "a" * 64,
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityControlPlaneClient(
        origin="https://loom.test",
        bearer_token_file=token,
        timeout_seconds=5.0,
        attempts=1,
        client=http_client,
    )
    policy = await client.fetch_policy(target_id="target", pool_id="pool")
    assert policy.node_cpu_millis == 4000
    observation = _observation(observed_at)
    receipt = await client.publish(observation)
    assert receipt.source_version == observation.source_version
    await http_client.aclose()


def _observation(observed_at: datetime) -> Any:
    from loom_execution_capacity_collector.contracts import CapacityObservationV1

    return CapacityObservationV1(
        target_id="target",
        source="collector",
        source_version="sha256:" + "b" * 64,
        observed_at=observed_at,
        provider_capacity_state="available",
        provider_capacity_reason=None,
        autoscaler_state="ready",
        autoscaler_reason=None,
        provider_quota_nodes=2,
        provider_quota_vcpu_millis=8000,
        provider_quota_memory_mib=16384,
        provider_quota_storage_mib=204800,
        provider_used_nodes=0,
        provider_used_vcpu_millis=0,
        provider_used_memory_mib=0,
        provider_used_storage_mib=0,
        active_nodes=1,
        provisioned_vcpu_millis=4000,
        provisioned_memory_mib=8192,
        provisioned_storage_mib=102400,
        allocatable_cpu_millis=3500,
        allocatable_memory_mib=7000,
        allocatable_storage_mib=90000,
        requested_cpu_millis=1000,
        requested_memory_mib=1000,
        requested_storage_mib=1000,
        pending_jobs=0,
        unschedulable_jobs=0,
        image_pull_backoff_jobs=0,
        pending_reasons={},
    )


def test_collector_secret_init_creates_only_owner_files(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "control-plane-token").write_text("token", encoding="utf-8")
    (source / "nebius-credentials.json").write_text("{}", encoding="utf-8")
    destination = tmp_path / "destination"
    copy_projected_credentials(source, destination)
    assert oct(destination.stat().st_mode & 0o777) == "0o700"
    assert {path.name for path in destination.iterdir()} == {
        "control-plane-token",
        "nebius-credentials.json",
    }
    assert all((path.stat().st_mode & 0o777) == 0o600 for path in destination.iterdir())


def test_collector_secret_init_normalizes_kubernetes_fsgroup_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "control-plane-token").write_text("token", encoding="utf-8")
    (source / "nebius-credentials.json").write_text("{}", encoding="utf-8")
    destination = tmp_path / "destination"
    destination.mkdir(mode=0o770)

    copy_projected_credentials(source, destination)

    assert oct(destination.stat().st_mode & 0o777) == "0o700"


def test_collector_manifest_is_active_configured_and_strictly_read_only() -> None:
    documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy/k8s/nebius-capacity-collector.yaml").read_text(encoding="utf-8")
        )
    )
    role = next(row for row in documents if row["kind"] == "ClusterRole")
    assert role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["nodes", "pods"],
            "verbs": ["get", "list"],
        }
    ]
    config = next(row for row in documents if row["kind"] == "ConfigMap")
    assert config["metadata"]["namespace"] == "loom-nebius-development"
    assert config["data"] == {
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_TARGET_ID": "nebius-eu-north1-development",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_POOL_ID": "nebius-cpu",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NAMESPACE": "loom-nebius-development",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NODE_LABEL_SELECTOR": (
            "loom.nebius/node-role=execution"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_PROJECT_ID": (
            "project-e00ksehzpr00ftw5pe61gt"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_QUOTA_PARENT_ID": (
            "tenant-e00zcze7mmwb61vk7e"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID": (
            "mk8snodegroup-e00n6mbxcz8jgp8bat"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_REGION": "eu-north1",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL": (
            "http://loom-control-plane.loom.svc.cluster.local:8080"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_NODES_NAME": "compute.instance.count",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_VCPU_NAME": (
            "compute.instance.non-gpu.vcpu"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_NAME": (
            "compute.disk.size.network-ssd"
        ),
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_NODES_UNIT": "count",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_VCPU_UNIT": "count",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_UNIT": "byte",
        "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_SERVICE": "compute",
    }
    cron = next(row for row in documents if row["kind"] == "CronJob")
    assert cron["metadata"]["namespace"] == "loom-nebius-development"
    assert cron["spec"]["suspend"] is False
    assert cron["spec"]["concurrencyPolicy"] == "Forbid"
    pod = cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"loom.nebius/node-role": "system"}
    assert pod["securityContext"]["runAsUser"] == 65532
    assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
    assert not {
        "create",
        "delete",
        "patch",
        "update",
        "watch",
        "exec",
        "impersonate",
    } & {verb for rule in role["rules"] for verb in rule["verbs"]}
