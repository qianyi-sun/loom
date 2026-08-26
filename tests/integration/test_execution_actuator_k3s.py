from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from loom.db.schema import ServiceExecutionLease
from loom.execution_contract import (
    ImageMaterialization,
    IsolationLevel,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
)
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    ProcessPhaseV1,
    SidecarContainerV1,
)
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.support.execution_image_admission import signed_image_admission_bundle

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 to run the disposable Kubernetes API conformance test",
)


def _lease(namespace: str) -> ServiceExecutionLease:
    now = datetime.now(UTC)
    image_ref = "invalid.local/loom-conformance@sha256:" + "a" * 64
    requirements = WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=100,
        memory_mib=128,
        ephemeral_storage_mib=128,
        isolation_level=IsolationLevel.SANDBOXED_RUNTIME,
        network_access=NetworkAccess.GATEWAY_ONLY,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref=image_ref,
        sidecar_count=0,
        verifier_topology=VerifierTopology.IN_ATTEMPT,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )
    runtime_image_ref = "invalid.local/runtime@sha256:" + "b" * 64
    runtime = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256="sha256:" + "c" * 64,
        image_admission=signed_image_admission_bundle((image_ref, runtime_image_ref), now=now),
        task_resources=ContainerResourcesV1(
            cpu_millis=100,
            memory_mib=128,
            ephemeral_storage_mib=128,
        ),
        workspace_mib=128,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=30,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=30,
        ),
    )
    requirements_json = requirements.model_dump(mode="json")
    runtime_json = runtime.canonical_payload()
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        execution_role="attempt",
        parent_lease_id=None,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="disposable-k3s",
        # The Job is suspended before submission, so the conformance test never pulls it.
        workload_requirements_json=requirements_json,
        workload_requirements_sha256=canonical_digest(requirements_json),
        runtime_contract_json=runtime_json,
        runtime_contract_sha256=canonical_digest(runtime_json),
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name=namespace,
        job_name=f"loom-{uuid4().hex[:12]}-a1-g1-a",
        execution_unit_key=uuid4(),
        deadline_at=now + timedelta(minutes=5),
    )


def _executable_lease(
    namespace: str,
    *,
    task_image_ref: str,
    runtime_image_ref: str,
    runtime_binary_sha256: str,
) -> ServiceExecutionLease:
    now = datetime.now(UTC)
    requirements = WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=100,
        memory_mib=128,
        ephemeral_storage_mib=256,
        isolation_level=IsolationLevel.SANDBOXED_RUNTIME,
        network_access=NetworkAccess.NONE,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref=task_image_ref,
        sidecar_count=1,
        verifier_topology=VerifierTopology.IN_ATTEMPT,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )
    resources = ContainerResourcesV1(
        cpu_millis=100,
        memory_mib=128,
        ephemeral_storage_mib=256,
    )
    phase = {
        "working_directory": "/workspace",
        "timeout_seconds": 30,
        "environment": {},
    }
    runtime = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=task_image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256=runtime_binary_sha256,
        image_admission=signed_image_admission_bundle((task_image_ref, runtime_image_ref), now=now),
        task_resources=resources,
        workspace_mib=128,
        runtime_volume_mib=32,
        setup=(
            ProcessPhaseV1(
                role="setup",
                argv=("/fixture", "phase", "setup"),
                **phase,
            ),
        ),
        main=ProcessPhaseV1(
            role="agent",
            argv=("/fixture", "phase", "agent"),
            **phase,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/fixture", "phase", "verifier"),
            **phase,
        ),
        sidecars=(
            SidecarContainerV1(
                role_name="fixture-sidecar",
                image_ref=task_image_ref,
                argv=("/fixture", "sidecar"),
                resources=ContainerResourcesV1(
                    cpu_millis=50,
                    memory_mib=64,
                    ephemeral_storage_mib=32,
                ),
                startup_probe=ProbeV1(kind="http", port=8080, path="/healthz"),
                readiness_probe=ProbeV1(kind="http", port=8080, path="/readyz"),
            ),
        ),
        max_log_bytes_per_stream=1024 * 1024,
        max_artifact_bytes=16 * 1024 * 1024,
    )
    requirements_json = requirements.model_dump(mode="json")
    runtime_json = runtime.canonical_payload()
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        execution_role="attempt",
        parent_lease_id=None,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="disposable-k3s",
        workload_requirements_json=requirements_json,
        workload_requirements_sha256=canonical_digest(requirements_json),
        runtime_contract_json=runtime_json,
        runtime_contract_sha256=canonical_digest(runtime_json),
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name=namespace,
        job_name=f"loom-{uuid4().hex[:12]}-a1-g1-a",
        execution_unit_key=uuid4(),
        deadline_at=now + timedelta(minutes=5),
    )


def _start_k3s() -> object:
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(
            "rancher/k3s@sha256:08fdebd14db9ab7d5ea821d5bfa95d02341a6ef886842fcc8d9dfd0e9fa9e0cd"
        )
        .with_exposed_ports(6443)
        .with_command(
            [
                "server",
                "--disable=traefik",
                "--disable=servicelb",
                "--tls-san=127.0.0.1",
                "--write-kubeconfig-mode=644",
            ]
        )
        .with_kwargs(privileged=True)
    )
    container.start()
    return container


def _load_client(container: object) -> tuple[object, object, object]:
    from kubernetes import client, config

    deadline = time.monotonic() + 90
    last_error = "kubeconfig unavailable"
    while time.monotonic() < deadline:
        result = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        if result.exit_code == 0:
            payload = result.output.decode("utf-8")
            mapped_port = container.get_exposed_port(6443)
            payload = payload.replace("https://127.0.0.1:6443", f"https://127.0.0.1:{mapped_port}")
            config.load_kube_config_from_dict(yaml.safe_load(payload))
            core = client.CoreV1Api()
            batch = client.BatchV1Api()
            try:
                core.get_api_resources()
                return client, core, batch
            except Exception as exc:  # API server is not ready yet.
                last_error = str(exc)
        time.sleep(1)
    raise AssertionError(f"disposable k3s did not become ready: {last_error}")


def _docker(*arguments: str) -> str:
    result = subprocess.run(
        ["docker", *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _docker_platform() -> str:
    architecture = _docker("info", "--format", "{{.Architecture}}")
    normalized = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64"}.get(
        architecture,
        architecture,
    )
    return f"linux/{normalized}"


def _build_image(*, tag: str, dockerfile: str, platform: str) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    _docker(
        "build",
        "--platform",
        platform,
        "--file",
        str(repo_root / dockerfile),
        "--tag",
        tag,
        str(repo_root),
    )


def _runtime_binary_digest(tag: str, root: Path, platform: str) -> str:
    container_id = _docker("create", "--platform", platform, tag)
    destination = root / "loom-execution-runtime"
    try:
        _docker("cp", f"{container_id}:/loom-execution-runtime", str(destination))
    finally:
        _docker("rm", "--force", container_id)
    return "sha256:" + hashlib.sha256(destination.read_bytes()).hexdigest()


def _import_image(container: object, *, tag: str, root: Path, ordinal: int) -> str:
    archive = root / f"image-{ordinal}.tar"
    _docker("save", "--output", str(archive), tag)
    container_id = container.get_wrapped_container().id
    remote = f"/tmp/loom-image-{ordinal}.tar"
    _docker("cp", str(archive), f"{container_id}:{remote}")
    result = container.exec(["ctr", "images", "import", remote])
    if result.exit_code != 0:
        raise AssertionError(result.output.decode("utf-8", errors="replace"))
    listing = container.exec(["ctr", "images", "ls"])
    if listing.exit_code != 0:
        raise AssertionError(listing.output.decode("utf-8", errors="replace"))
    for line in listing.output.decode("utf-8").splitlines():
        fields = line.split()
        if fields and fields[0] == tag and len(fields) >= 3:
            pinned = tag.rsplit(":", 1)[0] + "@" + fields[2]
            tagged = container.exec(["ctr", "images", "tag", tag, pinned])
            if tagged.exit_code != 0:
                raise AssertionError(tagged.output.decode("utf-8", errors="replace"))
            return pinned
    raise AssertionError(f"imported image {tag} is absent from k3s inventory")


def _wait_for_pod(core: object, namespace: str, name: str) -> object:
    deadline = time.monotonic() + 60
    last_phase = "missing"
    while time.monotonic() < deadline:
        pod = core.read_namespaced_pod(name, namespace)
        last_phase = pod.status.phase
        if last_phase == "Running" and any(
            condition.type == "Ready" and condition.status == "True"
            for condition in (pod.status.conditions or [])
        ):
            return pod
        time.sleep(0.25)
    raise AssertionError(f"Pod {namespace}/{name} did not become ready: {last_phase}")


def _pod_probe(core: object, namespace: str, name: str, url: str) -> str:
    from kubernetes.stream import stream

    return str(
        stream(
            core.connect_get_namespaced_pod_exec,
            name,
            namespace,
            command=["/fixture", "probe-report", url],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
    )


async def test_actuator_api_converges_against_disposable_k3s() -> None:
    from kubernetes import client

    container = _start_k3s()
    try:
        client_module, core, batch = await asyncio.to_thread(_load_client, container)
        namespace = f"loom-actuator-{uuid4().hex[:8]}"
        await asyncio.to_thread(
            core.create_namespace,
            client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
        )
        await asyncio.to_thread(
            core.create_namespaced_service_account,
            namespace,
            client.V1ServiceAccount(
                metadata=client.V1ObjectMeta(name="loom-execution-attempt"),
                automount_service_account_token=False,
            ),
        )
        api = InClusterKubernetesJobApi(
            client_module=client_module,
            batch_api=batch,
            core_api=core,
        )
        lease = _lease(namespace)
        target = ExecutionTargetRuntime(
            target_id="disposable-k3s",
            namespace=namespace,
            runtime_class_name="loom-sandbox",
        )
        manifest = render_execution_job(lease, target=target)
        manifest["spec"]["suspend"] = True

        created = await api.create_job(namespace=namespace, manifest=manifest)
        assert created.job_uid
        assert created.normalized_state == "pending"
        assert await api.get_job(namespace=namespace, job_name=lease.job_name) == created
        listed = await api.list_jobs(
            namespace=namespace,
            label_selector="app.kubernetes.io/managed-by=loom-execution-actuator",
        )
        assert [item.job_uid for item in listed.observations] == [created.job_uid]
        assert listed.rejected_count == 0

        watch_task = asyncio.create_task(
            api.watch_jobs(
                namespace=namespace,
                label_selector="app.kubernetes.io/managed-by=loom-execution-actuator",
                resource_version=created.resource_version,
                timeout_seconds=3,
            )
        )
        await asyncio.sleep(0.25)
        await asyncio.to_thread(
            batch.patch_namespaced_job,
            lease.job_name,
            namespace,
            {"metadata": {"annotations": {"loom.openai.com/conformance": "observed"}}},
        )
        watched = await watch_task
        assert watched
        assert watched[-1].job_uid == created.job_uid

        await api.delete_job(
            namespace=namespace,
            job_name=lease.job_name,
            expected_uid=created.job_uid,
            grace_period_seconds=0,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if await api.get_job(namespace=namespace, job_name=lease.job_name) is None:
                break
            await asyncio.sleep(0.25)
        else:
            raise AssertionError("Kubernetes Job did not disappear after exact-UID deletion")
    finally:
        await asyncio.to_thread(container.stop)


@pytest.mark.timeout(180)
async def test_attempt_network_policy_allows_only_dns_and_gateway() -> None:
    from kubernetes import client, utils

    suffix = uuid4().hex[:10]
    fixture_tag = f"docker.io/library/loom-network-fixture:{suffix}"
    container = None
    try:
        platform = await asyncio.to_thread(_docker_platform)
        await asyncio.to_thread(
            _build_image,
            tag=fixture_tag,
            dockerfile="tests/fixtures/execution_runtime_fixture/Dockerfile",
            platform=platform,
        )
        with tempfile.TemporaryDirectory(prefix="loom-network-k3s-") as temporary:
            root = Path(temporary)
            container = await asyncio.to_thread(_start_k3s)
            _, core, _ = await asyncio.to_thread(_load_client, container)
            dns_deadline = time.monotonic() + 60
            dns_pods = []
            while time.monotonic() < dns_deadline:
                dns_pods = (
                    await asyncio.to_thread(
                        core.list_namespaced_pod,
                        "kube-system",
                        label_selector="k8s-app=kube-dns",
                    )
                ).items
                if dns_pods:
                    break
                await asyncio.sleep(0.25)
            if not dns_pods:
                raise AssertionError("disposable k3s did not create a CoreDNS Pod")
            await asyncio.to_thread(
                _wait_for_pod,
                core,
                "kube-system",
                dns_pods[0].metadata.name,
            )
            image_ref = await asyncio.to_thread(
                _import_image,
                container,
                tag=fixture_tag,
                root=root,
                ordinal=1,
            )
            attempt_namespace = "loom-nebius-staging"
            platform_namespace = "loom"
            for namespace in (attempt_namespace, platform_namespace):
                await asyncio.to_thread(
                    core.create_namespace,
                    client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
                )
                await asyncio.to_thread(
                    core.create_namespaced_service_account,
                    namespace,
                    client.V1ServiceAccount(
                        metadata=client.V1ObjectMeta(name="network-fixture"),
                        automount_service_account_token=False,
                    ),
                )

            repo_root = Path(__file__).resolve().parents[2]
            api_client = client.ApiClient()
            attempt_documents = yaml.safe_load_all(
                (repo_root / "deploy/k8s/nebius-execution-actuator.yaml").read_text()
            )
            for document in attempt_documents:
                if document and document.get("kind") == "NetworkPolicy":
                    await asyncio.to_thread(utils.create_from_dict, api_client, document)
            platform_documents = yaml.safe_load_all(
                (repo_root / "deploy/k8s/network-policies.yaml").read_text()
            )
            for document in platform_documents:
                if (
                    document
                    and document.get("kind") == "NetworkPolicy"
                    and document["metadata"]["name"] in {"loom-llm-gateway", "loom-minio"}
                ):
                    await asyncio.to_thread(utils.create_from_dict, api_client, document)

            def pod(name: str, namespace: str, labels: dict[str, str], command: list[str]):
                return client.V1Pod(
                    metadata=client.V1ObjectMeta(
                        name=name,
                        namespace=namespace,
                        labels=labels,
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="network-fixture",
                        automount_service_account_token=False,
                        containers=[
                            client.V1Container(
                                name="main",
                                image=image_ref,
                                image_pull_policy="IfNotPresent",
                                command=command,
                            )
                        ],
                    ),
                )

            pods = (
                pod(
                    "gateway",
                    platform_namespace,
                    {"app": "loom-llm-gateway"},
                    ["/fixture", "server", "9100"],
                ),
                pod(
                    "object-store",
                    platform_namespace,
                    {"app": "loom-minio"},
                    ["/fixture", "server", "9000"],
                ),
                pod(
                    "blocked-service",
                    platform_namespace,
                    {"app": "blocked-service"},
                    ["/fixture", "server", "8080"],
                ),
                pod(
                    "execution-client",
                    attempt_namespace,
                    {"app.kubernetes.io/component": "execution-unit"},
                    ["/fixture", "idle"],
                ),
                pod(
                    "execution-server",
                    attempt_namespace,
                    {"app.kubernetes.io/component": "execution-unit"},
                    ["/fixture", "server", "8080"],
                ),
                pod(
                    "probe",
                    attempt_namespace,
                    {"app": "probe"},
                    ["/fixture", "idle"],
                ),
            )
            for item in pods:
                await asyncio.to_thread(
                    core.create_namespaced_pod,
                    item.metadata.namespace,
                    item,
                )
            for name, selector, port in (
                ("gateway", {"app": "loom-llm-gateway"}, 9100),
                ("object-store", {"app": "loom-minio"}, 9000),
                ("blocked-service", {"app": "blocked-service"}, 8080),
            ):
                await asyncio.to_thread(
                    core.create_namespaced_service,
                    platform_namespace,
                    client.V1Service(
                        metadata=client.V1ObjectMeta(name=name),
                        spec=client.V1ServiceSpec(
                            selector=selector,
                            ports=[client.V1ServicePort(port=port, target_port=port)],
                        ),
                    ),
                )
            ready = {
                item.metadata.name: await asyncio.to_thread(
                    _wait_for_pod,
                    core,
                    item.metadata.namespace,
                    item.metadata.name,
                )
                for item in pods
            }
            await asyncio.sleep(3)

            direct_gateway = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                f"http://{ready['gateway'].status.pod_ip}:9100",
            )
            allowed = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://gateway.loom.svc.cluster.local:9100",
            )
            assert "exit:0" in direct_gateway, f"direct Gateway peer was denied: {direct_gateway}"
            assert "exit:0" in allowed, f"DNS Gateway peer was denied: {allowed}"
            object_store = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://object-store.loom.svc.cluster.local:9000",
            )
            assert "exit:0" not in object_store
            blocked = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://blocked-service.loom.svc.cluster.local:8080",
            )
            assert "exit:0" not in blocked
            public = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://1.1.1.1:80",
            )
            assert "exit:0" not in public
            execution_ip = ready["execution-server"].status.pod_ip
            ingress = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "probe",
                f"http://{execution_ip}:8080",
            )
            assert "exit:0" not in ingress
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "image", "rm", "--force", fixture_tag],
            capture_output=True,
            check=False,
        )


@pytest.mark.timeout(300)
async def test_runtime_executes_task_native_sidecar_and_verifier_without_docker_socket() -> None:
    from kubernetes import client

    suffix = uuid4().hex[:10]
    runtime_tag = f"docker.io/library/loom-runtime-e2e:{suffix}"
    fixture_tag = f"docker.io/library/loom-runtime-fixture:{suffix}"
    container = None
    try:
        platform = await asyncio.to_thread(_docker_platform)
        await asyncio.to_thread(
            _build_image,
            tag=runtime_tag,
            dockerfile="deploy/Dockerfile.execution-runtime",
            platform=platform,
        )
        await asyncio.to_thread(
            _build_image,
            tag=fixture_tag,
            dockerfile="tests/fixtures/execution_runtime_fixture/Dockerfile",
            platform=platform,
        )
        with tempfile.TemporaryDirectory(prefix="loom-runtime-k3s-") as temporary:
            root = Path(temporary)
            runtime_binary_sha256 = await asyncio.to_thread(
                _runtime_binary_digest, runtime_tag, root, platform
            )
            container = await asyncio.to_thread(_start_k3s)
            client_module, core, batch = await asyncio.to_thread(_load_client, container)
            runtime_image_ref = await asyncio.to_thread(
                _import_image,
                container,
                tag=runtime_tag,
                root=root,
                ordinal=1,
            )
            task_image_ref = await asyncio.to_thread(
                _import_image,
                container,
                tag=fixture_tag,
                root=root,
                ordinal=2,
            )
            namespace = f"loom-runtime-{suffix}"
            await asyncio.to_thread(
                core.create_namespace,
                client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
            )
            await asyncio.to_thread(
                core.create_namespaced_service_account,
                namespace,
                client.V1ServiceAccount(
                    metadata=client.V1ObjectMeta(name="loom-execution-attempt"),
                    automount_service_account_token=False,
                ),
            )
            await asyncio.to_thread(
                core.create_namespaced_pod,
                namespace,
                client.V1Pod(
                    metadata=client.V1ObjectMeta(
                        name="execution-broker",
                        labels={"app": "execution-broker"},
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="loom-execution-attempt",
                        automount_service_account_token=False,
                        containers=[
                            client.V1Container(
                                name="broker",
                                image=task_image_ref,
                                image_pull_policy="IfNotPresent",
                                command=["/fixture", "broker"],
                            )
                        ],
                    ),
                ),
            )
            await asyncio.to_thread(
                core.create_namespaced_service,
                namespace,
                client.V1Service(
                    metadata=client.V1ObjectMeta(name="execution-broker"),
                    spec=client.V1ServiceSpec(
                        selector={"app": "execution-broker"},
                        ports=[client.V1ServicePort(port=9100, target_port=9100)],
                    ),
                ),
            )
            await asyncio.to_thread(_wait_for_pod, core, namespace, "execution-broker")
            node_api = client.NodeV1Api()
            await asyncio.to_thread(
                node_api.create_runtime_class,
                client.V1RuntimeClass(
                    metadata=client.V1ObjectMeta(name="loom-sandbox"),
                    handler="runc",
                ),
            )
            api = InClusterKubernetesJobApi(
                client_module=client_module,
                batch_api=batch,
                core_api=core,
            )
            lease = _executable_lease(
                namespace,
                task_image_ref=task_image_ref,
                runtime_image_ref=runtime_image_ref,
                runtime_binary_sha256=runtime_binary_sha256,
            )
            manifest = render_execution_job(
                lease,
                target=ExecutionTargetRuntime(
                    target_id="disposable-k3s",
                    namespace=namespace,
                    runtime_class_name="loom-sandbox",
                    credential_broker_url=(
                        f"http://execution-broker.{namespace}.svc.cluster.local:9100"
                        "/internal/service-execution"
                    ),
                ),
            )
            await api.create_job(namespace=namespace, manifest=manifest)

            deadline = time.monotonic() + 120
            inspect_after = time.monotonic() + 15
            observation = None
            while time.monotonic() < deadline:
                observation = await api.get_job(
                    namespace=namespace,
                    job_name=lease.job_name,
                )
                if observation is not None and observation.normalized_state == "succeeded":
                    break
                if observation is not None and observation.normalized_state in {
                    "failed",
                    "oom_killed",
                    "evicted",
                    "node_lost",
                    "deadline_exceeded",
                    "image_pull_backoff",
                }:
                    failed_pods = await asyncio.to_thread(
                        core.list_namespaced_pod,
                        namespace,
                        label_selector=f"loom.openai.com/lease-id={lease.id}",
                    )
                    details = None
                    logs: dict[str, str] = {}
                    if failed_pods.items:
                        failed_pod = failed_pods.items[0]
                        details = client.ApiClient().sanitize_for_serialization(failed_pod.status)
                        for container_name in (
                            "runtime-materializer",
                            "fixture-sidecar",
                            "execution",
                        ):
                            try:
                                logs[container_name] = await asyncio.to_thread(
                                    core.read_namespaced_pod_log,
                                    failed_pod.metadata.name,
                                    namespace,
                                    container=container_name,
                                )
                            except Exception as exc:
                                logs[container_name] = f"unavailable: {exc}"
                    raise AssertionError(
                        f"runtime Job failed: {observation} pod={details} logs={logs}"
                    )
                if time.monotonic() >= inspect_after:
                    current_pods = await asyncio.to_thread(
                        core.list_namespaced_pod,
                        namespace,
                        label_selector=f"loom.openai.com/lease-id={lease.id}",
                    )
                    if current_pods.items:
                        current = current_pods.items[0]
                        statuses = [
                            *(current.status.init_container_statuses or []),
                            *(current.status.container_statuses or []),
                        ]
                        failed = [
                            status
                            for status in statuses
                            if status.name in {"runtime-materializer", "execution"}
                            if status.state.terminated is not None
                            and status.state.terminated.exit_code != 0
                        ]
                        blocked = [
                            status
                            for status in statuses
                            if status.state.waiting is not None
                            and status.state.waiting.reason
                            not in {"ContainerCreating", "PodInitializing"}
                        ]
                        if failed or blocked:
                            details = client.ApiClient().sanitize_for_serialization(current.status)
                            raise AssertionError(f"runtime Pod failed closed: {details}")
                    inspect_after = time.monotonic() + 5
                await asyncio.sleep(0.5)
            else:
                current_pods = await asyncio.to_thread(
                    core.list_namespaced_pod,
                    namespace,
                    label_selector=f"loom.openai.com/lease-id={lease.id}",
                )
                details = (
                    client.ApiClient().sanitize_for_serialization(current_pods.items[0].status)
                    if current_pods.items
                    else None
                )
                raise AssertionError(
                    f"runtime Job did not succeed: observation={observation} pod={details}"
                )

            pods = await asyncio.to_thread(
                core.list_namespaced_pod,
                namespace,
                label_selector=f"loom.openai.com/lease-id={lease.id}",
            )
            assert len(pods.items) == 1
            assert observation is not None
            assert observation.termination_summary is not None
            assert observation.termination_summary.output_committed is True
            pod = pods.items[0]
            pod_dict = client.ApiClient().sanitize_for_serialization(pod)
            assert "/var/run/docker.sock" not in str(pod_dict)
            assert "hostPath" not in str(pod_dict)
            assert [item.name for item in pod.spec.init_containers] == [
                "runtime-materializer",
                "fixture-sidecar",
            ]
            logs = await asyncio.to_thread(
                core.read_namespaced_pod_log,
                pod.metadata.name,
                namespace,
                container="execution",
            )
            assert "fixture-phase=setup" in logs
            assert "fixture-phase=agent" in logs
            assert "fixture-phase=verifier" in logs
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)
        for tag in (runtime_tag, fixture_tag):
            subprocess.run(
                ["docker", "image", "rm", "--force", tag],
                check=False,
                capture_output=True,
            )
