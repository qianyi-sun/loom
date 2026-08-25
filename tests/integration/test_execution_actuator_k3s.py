from __future__ import annotations

import asyncio
import os
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import yaml

from loom.db.schema import ServiceExecutionLease
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 to run the disposable Kubernetes API conformance test",
)


def _lease(namespace: str) -> ServiceExecutionLease:
    now = datetime.now(UTC)
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="disposable-k3s",
        workload_requirements_json={
            # The Job is suspended before submission, so the conformance test never pulls it.
            "image_ref": "invalid.local/loom-conformance@sha256:" + "a" * 64,
            "cpu_millis": 100,
            "memory_mib": 128,
            "ephemeral_storage_mib": 128,
        },
        workload_requirements_sha256="sha256:" + "b" * 64,
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name=namespace,
        job_name=f"loom-{uuid4().hex[:12]}-a1-g1",
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
