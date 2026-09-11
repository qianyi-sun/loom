"""Real Kubernetes enforces the UID DELETE precondition used by storage cleanup."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from testcontainers.core.container import DockerContainer

from loom.dev_instance_runtime import (
    AsyncCommandRunner,
    DevInstanceRuntimeError,
    KubectlCandidateGenerationProvisioner,
    KubectlClient,
    KubectlSecretVault,
)
from loom.personal_dev_incarnation_storage import personal_dev_secret_name
from tests.unit.test_dev_instance_runtime import _personal_manifest_config
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim

_K3S = "rancher/k3s@sha256:08fdebd14db9ab7d5ea821d5bfa95d02341a6ef886842fcc8d9dfd0e9fa9e0cd"


def _failure_category(stderr):
    # Finite labels only: kubectl can quote Secret input, names or server URLs.
    lowered = stderr.lower()
    if 'namespaces "' in lowered and "not found" in lowered:
        return "namespace-not-found"
    for label, fragment in (
        ("namespace-terminating", "namespace is being terminated"),
        ("resource-conflict", "the object has been modified"),
        ("already-exists", "already exists"),
        ("not-found", "notfound"),
        ("forbidden", "forbidden"),
        ("service-unavailable", "serviceunavailable"),
        ("internal-error", "internalerror"),
        ("connection-refused", "connection refused"),
        ("connection-reset", "connection reset"),
        ("timeout", "timed out"),
        ("timeout", "deadline exceeded"),
        ("tls-error", "tls:"),
        ("certificate-error", "x509:"),
        ("container-stopped", "is not running"),
        ("unexpected-eof", "unexpected eof"),
    ):
        if fragment in lowered:
            return label
    return "unclassified"


class _ContainerKubectl:
    def __init__(self, container_id):
        self.container_id = container_id

    async def run(self, argv, *, stdin=None, timeout_seconds=120):
        assert argv[0] == "kubectl"
        diagnostic = "/tmp/loom-test-kubectl-" + uuid4().hex
        # Preserve the production runner's exit handling and Conflict subtype.
        # This wrapper executes kubectl exactly once. Its stderr copy exists only
        # in this test-owned container and disappears when that container stops.
        command = ["docker", "exec", "-i", self.container_id, "sh", "-c",
            'loom_fixture_diag="$1"; shift; "$@" 2>"$loom_fixture_diag"; '
            'loom_fixture_exit=$?; cat "$loom_fixture_diag" >&2; exit "$loom_fixture_exit"',
            "loom-test-kubectl", diagnostic, "kubectl",
            "--kubeconfig=/etc/rancher/k3s/k3s.yaml", *argv[1:]]
        try:
            return await AsyncCommandRunner().run(command, stdin=stdin, timeout_seconds=timeout_seconds)
        except DevInstanceRuntimeError as error:
            try:
                captured = await AsyncCommandRunner().run(
                    ["docker", "exec", self.container_id, "head", "-c", "8192", diagnostic], timeout_seconds=5)
                error.add_note("disposable kubectl failure category: " + _failure_category(captured.stdout))
            except DevInstanceRuntimeError:
                error.add_note("disposable kubectl diagnostic unavailable")
            try:
                state = await AsyncCommandRunner().run(["docker", "inspect", "--format",
                    '{"Running":{{.State.Running}},"OOMKilled":{{.State.OOMKilled}},"ExitCode":{{.State.ExitCode}}}',
                    self.container_id], timeout_seconds=5)
                value = json.loads(state.stdout)
                if (isinstance(value, dict) and set(value) == {"Running", "OOMKilled", "ExitCode"}
                    and type(value["Running"]) is bool and type(value["OOMKilled"]) is bool
                    and type(value["ExitCode"]) is int):
                    error.add_note("disposable container state: " + json.dumps(value, sort_keys=True))
            except (DevInstanceRuntimeError, ValueError):
                error.add_note("disposable container state unavailable")
            raise


@pytest.fixture
async def disposable_storage_kubectl():
    container = DockerContainer(_K3S).with_command([
        "server", "--disable-agent", "--disable=traefik", "--disable=servicelb",
        "--disable=metrics-server", "--disable=local-storage", "--disable=coredns",
    ]).with_kwargs(privileged=True)
    try:
        await asyncio.to_thread(container.start)
        runner = _ContainerKubectl(container.get_wrapped_container().id)
        async with asyncio.timeout(90):
            while True:
                try:
                    await runner.run(["kubectl", "get", "--raw=/readyz"], timeout_seconds=10)
                    break
                except DevInstanceRuntimeError:
                    await asyncio.sleep(1)
        yield KubectlClient("kubectl", runner=runner)
    finally:
        await asyncio.to_thread(container.stop)


async def test_storage_cleanup_real_uid_precondition_and_secret_recovery(disposable_storage_kubectl):
    kubectl = disposable_storage_kubectl
    binding = _bound_claim().operation.storage_binding
    identity = binding.identity
    vault = KubectlSecretVault(kubectl, "postgresql://admin:fixture@database.example/postgres",
                              protected_worker_runtime=True)
    await vault.store(identity, "b" * 32)
    config = _personal_manifest_config()
    config = replace(config, lifecycle_binding=replace(config.lifecycle_binding,
                     subject_id=binding.subject_id, subject_incarnation=binding.subject_incarnation))
    provisioner = KubectlCandidateGenerationProvisioner(kubectl)
    await provisioner.bootstrap(identity, config)
    old = await kubectl.read_storage_namespace(identity)
    old_uid = old["metadata"]["uid"]
    # A lost write reply is handled by a fresh reader. Removing only the generated
    # admin fixture also exercises the missing-Secret kubectl response/recovery.
    await kubectl.runner.run(["kubectl", "delete", "secret", personal_dev_secret_name(identity, "loom-admin-secret"), "-n", identity.namespace])
    assert await vault.database_password(identity) == "b" * 32
    await vault.store(identity, "b" * 32)
    assert await vault.admin_token(identity)
    await kubectl.delete_storage_namespace(identity)
    assert await kubectl.read_namespace_optional(identity.namespace) is None

    successor = binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    await vault.store(successor, "c" * 32)
    current_uid = (await kubectl.read_storage_namespace(successor))["metadata"]["uid"]
    assert current_uid != old_uid
    # Actual kubectl --raw -f - must send the DeleteOptions body, and the API
    # server must reject a formerly valid UID for this stable namespace name.
    with pytest.raises(DevInstanceRuntimeError):
        await kubectl.runner.run([
            "kubectl", "delete", f"--raw=/api/v1/namespaces/{identity.namespace}", "-f", "-",
        ], stdin=json.dumps({"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": old_uid}}))
    assert (await kubectl.read_storage_namespace(successor))["metadata"]["uid"] == current_uid
    with pytest.raises(DevInstanceRuntimeError):
        await kubectl.delete_storage_namespace(identity)
    with pytest.raises(DevInstanceRuntimeError):
        await provisioner.bootstrap(identity, config)
    assert await vault.database_password(successor) == "c" * 32
    await kubectl.delete_storage_namespace(successor)


async def test_bootstrap_rejects_unbound_namespace_owned_by_same_field_manager(disposable_storage_kubectl):
    kubectl = disposable_storage_kubectl
    binding = _bound_claim().operation.storage_binding
    config = _personal_manifest_config()
    config = replace(config, lifecycle_binding=replace(config.lifecycle_binding,
                     subject_id=binding.subject_id, subject_incarnation=binding.subject_incarnation))
    await kubectl.apply(json.dumps({"apiVersion": "v1", "kind": "Namespace",
                                   "metadata": {"name": binding.identity.namespace}}))
    with pytest.raises(DevInstanceRuntimeError):
        await KubectlCandidateGenerationProvisioner(kubectl).bootstrap(binding.identity, config)
    namespace = await kubectl.read_namespace_optional(binding.identity.namespace)
    assert "loom.dev/storage-binding" not in namespace["metadata"].get("annotations", {})
