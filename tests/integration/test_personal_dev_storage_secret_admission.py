"""Actual admission preserves Secret purpose isolation with incarnation names."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError
from loom.dev_instance_manifest import dev_instance_manifest_documents
from loom.personal_dev_control_plane_render import (
    _RenderContext,
    _management_namespace_admission,
    _management_resource_admission,
)
from loom.personal_dev_incarnation_storage import personal_dev_secret_name, personal_dev_storage_annotations
from tests.integration.test_personal_dev_storage_namespace import disposable_storage_kubectl  # noqa: F401
from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def test_management_admission_accepts_only_current_incarnation_secret_names(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    principal = "system:serviceaccount:loom-dev:loom-personal-dev-management"
    for document in (
        {
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {
                "name": identity.namespace, "annotations": personal_dev_storage_annotations(identity),
                "labels": {"pod-security.kubernetes.io/enforce": "restricted", "app.kubernetes.io/managed-by": "loom-dev-instance-controller"},
            },
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
            "metadata": {"name": "disposable-admission-probe"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
            "subjects": [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": principal}],
        },
    ):
        await kubectl.apply(json.dumps(document))

    async def probe(name, *, capacity=False):
        document = {
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {
                "name": name, "namespace": identity.namespace,
                "labels": {"app.kubernetes.io/managed-by": "loom-personal-dev-lifecycle" if capacity else "loom-dev-instance-controller"},
            },
        }
        await kubectl.runner.run(
            kubectl._argv(f"--as={principal}", "create", "--dry-run=server", "-f", "-"),
            stdin=json.dumps(document),
        )

    # Establish test-only impersonation authorization before installing policy,
    # so a later negative probe cannot mistake missing RBAC for admission.
    async with asyncio.timeout(15):
        while True:
            try:
                await probe("forbidden-storage-probe")
                break
            except DevInstanceRuntimeError:
                await asyncio.sleep(0.1)
    documents = _management_resource_admission(
        _RenderContext("a" * 64, "b" * 64),
        builder_image="registry.example/builder@sha256:" + "c" * 64,
        runtime_class_name="runsc",
    )
    for document in documents:
        await kubectl.apply(json.dumps(document))
    # Wait for observed enforcement, not a guessed admission-cache delay.
    async with asyncio.timeout(15):
        while True:
            try:
                await probe("forbidden-storage-probe")
            except DevInstanceRuntimeError:
                break
            await asyncio.sleep(0.1)
    for purpose in (
        "loom-secrets", "loom-admin-secret", "loom-protected-worker-runtime",
        "loom-capacity-agent", "loom-capacity-agent-credentials",
    ):
        capacity = purpose.startswith("loom-capacity-")
        await probe(personal_dev_secret_name(identity, purpose), capacity=capacity)
        with pytest.raises(DevInstanceRuntimeError):
            await probe(purpose, capacity=capacity)
        with pytest.raises(DevInstanceRuntimeError):
            await probe(f"{purpose}-{uuid4().hex}", capacity=capacity)

    config = _immutable_config()
    config = replace(config, lifecycle_binding=replace(
        config.lifecycle_binding, subject_id=identity.storage_binding.subject_id,
        subject_incarnation=identity.storage_incarnation,
    ))
    for workload in dev_instance_manifest_documents(identity, config):
        if workload["kind"] != "Deployment" or "loom-web" in workload["metadata"]["name"]:
            continue
        await kubectl.runner.run(
            kubectl._argv(f"--as={principal}", "create", "--dry-run=server", "-f", "-"),
            stdin=json.dumps(workload),
        )
        forbidden = deepcopy(workload)
        forbidden["spec"]["template"]["spec"]["containers"][0]["env"].append({
            "name": "FORBIDDEN", "valueFrom": {"secretKeyRef": {
                "name": personal_dev_secret_name(identity, "loom-capacity-agent"), "key": "reporter-token",
            }},
        })
        with pytest.raises(DevInstanceRuntimeError):
            await kubectl.runner.run(
                kubectl._argv(f"--as={principal}", "create", "--dry-run=server", "-f", "-"),
                stdin=json.dumps(forbidden),
            )


async def test_namespace_admission_rejects_storage_binding_reassignment(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    binding = _bound_claim().operation.storage_binding
    identity = binding.identity
    principal = "system:serviceaccount:loom-dev:loom-personal-dev-management"
    await kubectl.apply(json.dumps({
        "apiVersion": "v1", "kind": "Namespace",
        "metadata": {
            "name": identity.namespace, "annotations": personal_dev_storage_annotations(identity),
            "labels": {"pod-security.kubernetes.io/enforce": "restricted", "app.kubernetes.io/managed-by": "loom-dev-instance-controller"},
        },
    }))
    await kubectl.apply(json.dumps({
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
        "metadata": {"name": "disposable-namespace-probe"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
        "subjects": [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": principal}],
    }))
    for document in _management_namespace_admission(_RenderContext("a" * 64, "b" * 64)):
        await kubectl.apply(json.dumps(document))
    original = await kubectl.read_storage_namespace(identity)

    async def update(document):
        await kubectl.runner.run(
            kubectl._argv(f"--as={principal}", "replace", "--dry-run=server", "-f", "-"),
            stdin=json.dumps(document),
        )

    # Admission activation is observed with an already-forbidden PodSecurity
    # downgrade before testing the new storage-identity invariant.
    forbidden = deepcopy(original)
    forbidden["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"
    async with asyncio.timeout(15):
        while True:
            try:
                await update(forbidden)
            except DevInstanceRuntimeError:
                break
            await asyncio.sleep(0.1)
    await update(original)
    for annotations in (
        {},
        personal_dev_storage_annotations(binding.model_copy(update={"subject_incarnation": uuid4()}).identity),
    ):
        changed = deepcopy(original)
        changed["metadata"]["annotations"] = annotations
        with pytest.raises(DevInstanceRuntimeError):
            await update(changed)
