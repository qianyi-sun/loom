"""Actual admission preserves Secret purpose isolation with incarnation names."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import dev_instance_manifest_documents
from loom.dev_instance_runtime import (
    DevInstanceRuntimeError,
    KubectlCandidateGenerationProvisioner,
    KubectlClient,
    KubectlSecretVault,
)
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from loom.personal_dev_control_plane_render import (
    _management_namespace_admission,
    _management_resource_admission,
    _RenderContext,
)
from loom.personal_dev_incarnation_storage import (
    personal_dev_secret_name,
    personal_dev_storage_annotations,
)
from loom.personal_dev_storage_secret_write import write_storage_secret
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_control_plane_render import _render
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


@pytest.mark.parametrize("bound_storage", (False, True))
async def test_management_admission_accepts_only_current_incarnation_secret_names(
    disposable_storage_kubectl,  # noqa: F811
    bound_storage,
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    if not bound_storage:
        identity = derive_identity(identity.name)
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
        if bound_storage:
            with pytest.raises(DevInstanceRuntimeError):
                await probe(purpose, capacity=capacity)
        with pytest.raises(DevInstanceRuntimeError):
            await probe(f"{purpose}-{uuid4().hex}", capacity=capacity)

    config = _immutable_config()
    if bound_storage:
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


async def test_bound_bootstrap_with_real_management_rbac_grants_only_exact_secret_reads(
    disposable_storage_kubectl,  # noqa: F811
    tmp_path,
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    principal = "system:serviceaccount:loom-dev:loom-personal-dev-management"
    _, _, _, rendered = _render(tmp_path)
    allowed = {
        "loom-personal-dev-management-mutation", "loom-personal-dev-managed-namespace",
        "loom-personal-dev-managed-namespace-bound", "loom-personal-dev-management-namespaces",
        "loom-personal-dev-management-resources",
    }
    for document in rendered:
        if document["kind"] in {"ClusterRole", "ClusterRoleBinding", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"} and document["metadata"]["name"] in allowed:
            await kubectl.apply(json.dumps(document))
    config = _immutable_config()
    config = replace(config, lifecycle_binding=replace(
        config.lifecycle_binding, subject_id=identity.storage_binding.subject_id,
        subject_incarnation=identity.storage_incarnation,
    ))
    namespace = dev_instance_manifest_documents(identity, config)[0]
    await kubectl.apply(json.dumps(namespace))

    class ManagementRunner:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            return await kubectl.runner.run(
                [argv[0], f"--as={principal}", *argv[1:]],
                stdin=stdin, timeout_seconds=timeout_seconds,
            )

    managed = KubectlClient("kubectl", runner=ManagementRunner())
    async with asyncio.timeout(15):
        while True:
            try:
                await managed.read_namespace_optional(identity.namespace)
                break
            except DevInstanceRuntimeError:
                await asyncio.sleep(0.1)
    await KubectlCandidateGenerationProvisioner(managed).bootstrap(identity, config)
    await KubectlSecretVault(managed, "postgresql://admin:fixture@database.example/postgres", protected_worker_runtime=True).store(identity, "b" * 32)
    claim = _bound_claim()
    installer = KubectlPersonalDevCapacityInstaller(kubectl=managed, database=None, config=None)
    credentials = await installer._credentials(claim, identity)
    await installer._persist_credentials(claim, identity, credentials)
    await write_storage_secret(managed, identity, {
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {
            "name": personal_dev_secret_name(identity, "loom-capacity-agent"),
            "namespace": identity.namespace,
            "labels": {"app.kubernetes.io/managed-by": "loom-personal-dev-lifecycle"},
        },
        "stringData": {"probe": "not-a-credential"},
    }, operation_epoch=claim.operation.operation_epoch)
    for purpose in ("loom-secrets", "loom-admin-secret", "loom-protected-worker-runtime", "loom-capacity-agent", "loom-capacity-agent-credentials"):
        assert await managed.read_secret(identity.namespace, personal_dev_secret_name(identity, purpose))
        with pytest.raises(DevInstanceRuntimeError):
            await managed.read_secret_optional(identity.namespace, purpose)
        with pytest.raises(DevInstanceRuntimeError):
            await managed.read_secret_optional(identity.namespace, f"{purpose}-{uuid4().hex}")

    # Actual GET/CREATE/dry-run PUT/activation PUT under the rendered management
    # role, not an impersonated cluster-admin grant.
    workloads = tuple(document for document in dev_instance_manifest_documents(identity, config)
                      if document["kind"] == "Job" or (document["kind"] == "Deployment" and "loom-web" in document["metadata"]["name"]))
    await KubectlCandidateGenerationProvisioner(managed)._apply_generation_workloads(identity, config, workloads)
    await KubectlCandidateGenerationProvisioner(managed)._apply_generation_workloads(identity, config, workloads)

    retry = replace(config, lifecycle_binding=replace(config.lifecycle_binding, attempt_id=uuid4(), attempt_sequence=1))
    retry_jobs = tuple(document for document in dev_instance_manifest_documents(identity, retry) if document["kind"] == "Job")
    await KubectlCandidateGenerationProvisioner(managed)._apply_generation_workloads(identity, retry, retry_jobs)
    reservation_name = "loom-workload-fence-" + retry_jobs[0]["metadata"]["name"]
    reservation = await managed.read_resource_json(namespace=identity.namespace, kind="configmap", name=reservation_name)
    assert reservation["data"]["sequence"] == "1"
    for field, value in (("sequence", "0"), ("attempt", str(uuid4())), ("intent", "{}"), ("extra", "not-allowed")):
        changed = deepcopy(reservation)
        changed["data"][field] = value
        with pytest.raises(DevInstanceRuntimeError):
            await managed.runner.run(managed._argv("replace", "--dry-run=server", "-f", "-"), stdin=json.dumps(changed))
    changed = deepcopy(reservation)
    changed["metadata"]["ownerReferences"][0]["controller"] = True
    with pytest.raises(DevInstanceRuntimeError):
        await managed.runner.run(managed._argv("replace", "--dry-run=server", "-f", "-"), stdin=json.dumps(changed))
    delete = kubectl._argv("delete", "configmap", reservation_name, "-n", identity.namespace, "--dry-run=server")
    # Establish that the test's exact DELETE request is valid, then demonstrate
    # denial for the management principal with its real DELETE RBAC permission.
    await kubectl.runner.run(delete)
    with pytest.raises(DevInstanceRuntimeError):
        await managed.runner.run(delete)
    assert (await managed.read_resource_json(namespace=identity.namespace, kind="configmap", name=reservation_name))["metadata"]["uid"] == reservation["metadata"]["uid"]

    role = next(document for document in dev_instance_manifest_documents(identity, config) if document["kind"] == "Role")
    for widened in (
        {**role["rules"][0], "resourceNames": ["unrelated-secret"]},
        {**role["rules"][0], "verbs": ["get", "list"]},
        {**role["rules"][0], "resources": ["configmaps"]},
    ):
        changed = {**role, "rules": [widened]}
        with pytest.raises(DevInstanceRuntimeError):
            await managed.apply(json.dumps(changed))
    binding = next(document for document in dev_instance_manifest_documents(identity, config) if document["kind"] == "RoleBinding" and document["roleRef"]["kind"] == "Role")
    with pytest.raises(DevInstanceRuntimeError):
        await managed.apply(json.dumps({**binding, "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": identity.namespace}]}))
    with pytest.raises(DevInstanceRuntimeError):
        await managed.apply(json.dumps({**role, "metadata": {**role["metadata"], "name": "unrelated-role"}}))
    other_namespace = "loom-dev-another-owner"
    await kubectl.apply(json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": other_namespace}}))
    with pytest.raises(DevInstanceRuntimeError):
        await managed.read_secret_optional(other_namespace, personal_dev_secret_name(identity, "loom-secrets"))
