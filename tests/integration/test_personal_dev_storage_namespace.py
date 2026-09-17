"""Real Kubernetes enforces the UID DELETE precondition used by storage cleanup."""

import asyncio
import json
import time
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


def _is_disposable_read_refusal(stderr):
    lowered = stderr.lower()
    return (len(lowered.splitlines()) == 1
            and lowered.startswith("the connection to the server ")
            and lowered.rstrip().endswith(" was refused - did you specify the right host or port?"))


def _server_failure_categories(logs):
    lowered = logs.lower()
    categories = {label for label, fragments in (
        ("api-server-exit", ("kube-apiserver exited",)),
        ("controller-manager-exit", ("kube-controller-manager exited",)),
        ("scheduler-exit", ("kube-scheduler exited",)),
        ("containerd-exit", ("containerd exited",)),
        ("leader-election-lost", ("leaderelection lost", "leader election lost")),
        ("disk-full", ("no space left on device",)),
        ("file-descriptor-limit", ("too many open files",)),
        ("memory-exhausted", ("out of memory", "cannot allocate memory")),
        ("permission-denied", ("permission denied",)),
        ("address-in-use", ("address already in use",)),
        ("fatal", ("level=fatal", '"level":"fatal"')),
        ("panic", ("panic:",)),
        ("termination-signal", ("received signal", "received sigterm", "received sigint")),
        ("context-cancelled", ("context canceled",)),
    ) if any(fragment in lowered for fragment in fragments)}
    return ",".join(sorted(categories)) or "unclassified"


def _container_lifecycle_categories(events):
    categories = set()
    for line in events.splitlines():
        fields = line.split()
        if fields and fields[0] in {"oom", "die", "stop", "destroy"}:
            categories.add(fields[0])
        elif len(fields) == 2 and fields[0] == "kill" and fields[1] in {str(value) for value in range(1, 65)}:
            categories.add("kill-" + fields[1])
    return ",".join(sorted(categories)) or "none-observed"


def _failure_category(stderr):
    # Finite labels only: kubectl can quote Secret input, names or server URLs.
    lowered = stderr.lower()
    if not lowered.strip():
        return "empty-stderr"
    if lowered.strip() == "eof" or lowered.rstrip().endswith(": eof"):
        return "unexpected-eof"
    if _is_disposable_read_refusal(stderr):
        return "connection-refused"
    if 'namespaces "' in lowered and "not found" in lowered:
        return "namespace-not-found"
    for label, fragment in (
        ("namespace-terminating", "namespace is being terminated"),
        ("resource-conflict", "the object has been modified"),
        ("request-rejected", "the server rejected our request"),
        ("already-exists", "already exists"),
        ("not-found", "notfound"),
        ("forbidden", "forbidden"),
        ("service-unavailable", "serviceunavailable"),
        ("internal-error", "internalerror"),
        ("connection-refused", "connection refused"),
        ("connection-reset", "connection reset"),
        ("timeout", "timed out"),
        ("timeout", "deadline exceeded"),
        ("timeout", "i/o timeout"),
        ("timeout", "handshake timeout"),
        ("http2-error", "http2:"),
        ("request-cancelled", "context canceled"),
        ("discovery-error", "the server doesn't have a resource type"),
        ("resource-unavailable", "resource temporarily unavailable"),
        ("file-descriptor-limit", "too many open files"),
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
        self.last_failure_notes = []

    async def run(self, argv, *, stdin=None, timeout_seconds=120):
        deadline = time.monotonic() + timeout_seconds
        retry_deadline = None
        remaining = timeout_seconds
        while True:
            try:
                return await self._run_once(argv, stdin=stdin, timeout_seconds=remaining)
            except DevInstanceRuntimeError as error:
                # A just-started disposable API can briefly refuse a connection
                # after /readyz passed. Only repeat the read, never a write or an
                # ambiguous/authority failure. Keep the caller's original budget.
                if argv[1:2] != ["get"] or not getattr(error, "_disposable_connection_refused", False):
                    raise
                now = time.monotonic()
                if retry_deadline is None:
                    retry_deadline = min(deadline, now + 5)
                remaining = retry_deadline - now
                if remaining <= 0:
                    raise
                await asyncio.sleep(min(0.1, remaining))
                remaining = retry_deadline - time.monotonic()
                if remaining <= 0:
                    raise

    async def _run_once(self, argv, *, stdin=None, timeout_seconds=120):
        assert argv[0] == "kubectl"
        diagnostic = "/tmp/loom-test-kubectl-" + uuid4().hex
        # Preserve the production runner's exit handling and Conflict subtype.
        # This wrapper executes kubectl exactly once. Its stderr copy exists only
        # in this test-owned container and disappears when that container stops.
        command = ["docker", "exec", "-i", self.container_id, "sh", "-c",
            'loom_fixture_diag="$1"; shift; "$@" 2>"$loom_fixture_diag"; '
            'loom_fixture_exit=$?; printf "%s" "$loom_fixture_exit" >"$loom_fixture_diag.status"; '
            'cat "$loom_fixture_diag" >&2; exit "$loom_fixture_exit"',
            "loom-test-kubectl", diagnostic, "kubectl",
            "--kubeconfig=/etc/rancher/k3s/k3s.yaml", *argv[1:]]
        try:
            result = await AsyncCommandRunner().run(command, stdin=stdin, timeout_seconds=timeout_seconds)
            self.last_failure_notes = []
            return result
        except DevInstanceRuntimeError as error:
            error._disposable_connection_refused = False
            current_failure_notes = []

            def note(message, failure=error):
                # Retain only diagnostics constructed here, never arbitrary
                # exception text, for a readiness loop's outer deadline.
                current_failure_notes.append(message)
                self.last_failure_notes = current_failure_notes
                failure.add_note(message)

            try:
                captured = await AsyncCommandRunner().run(
                    ["docker", "exec", self.container_id, "head", "-c", "8193", diagnostic], timeout_seconds=5)
                stderr = captured.stdout[:8192]
                category = _failure_category(stderr)
                error._disposable_connection_refused = _is_disposable_read_refusal(stderr)
                note("disposable kubectl failure category: " + category)
                # Decoded characters, not exact bytes for non-UTF8 diagnostics.
                note(f"disposable kubectl stderr: chars={len(stderr)}; at-read-limit={len(captured.stdout) >= 8192}")
            except DevInstanceRuntimeError:
                note("disposable kubectl diagnostic unavailable")
            inner_status = "unavailable"
            try:
                captured = await AsyncCommandRunner().run(
                    ["docker", "exec", self.container_id, "head", "-c", "12", diagnostic + ".status"], timeout_seconds=5)
                if captured.stdout in {str(value) for value in range(256)}:
                    inner_status = captured.stdout
            except DevInstanceRuntimeError:
                pass
            # A missing status differs from a nonzero kubectl exit: Docker exec
            # or the shell may have failed before the command completed.
            note("disposable kubectl exit status: " + inner_status)
            try:
                state = await AsyncCommandRunner().run(["docker", "inspect", "--format",
                    '{"Running":{{.State.Running}},"OOMKilled":{{.State.OOMKilled}},"ExitCode":{{.State.ExitCode}}}',
                    self.container_id], timeout_seconds=5)
                value = json.loads(state.stdout)
                if (isinstance(value, dict) and set(value) == {"Running", "OOMKilled", "ExitCode"}
                    and type(value["Running"]) is bool and type(value["OOMKilled"]) is bool
                    and type(value["ExitCode"]) is int):
                    note("disposable container state: " + json.dumps(value, sort_keys=True))
                    if not value["Running"]:
                        await self._stopped_diagnostics(note)
            except (DevInstanceRuntimeError, ValueError):
                note("disposable container state unavailable")
            raise

    async def _stopped_diagnostics(self, note):
        # docker exec cannot recover stderr once PID 1 exits. Inspect only this
        # disposable container; retain finite labels, never raw server output.
        try:
            captured = await AsyncCommandRunner().run([
                "sh", "-c", 'docker logs --tail 100 "$1" 2>&1 | head -c 32769',
                "loom-k3s-diagnostics", self.container_id,
            ], timeout_seconds=5)
            note("disposable k3s log categories: " + _server_failure_categories(captured.stdout[:32768]))
        except DevInstanceRuntimeError:
            note("disposable k3s log categories unavailable")
        try:
            captured = await AsyncCommandRunner().run([
                "docker", "events", "--filter", "container=" + self.container_id,
                "--since", "10m", "--until", str(int(time.time()) + 1),
                "--format", '{{.Action}} {{index .Actor.Attributes "signal"}}',
            ], timeout_seconds=5)
            note("disposable container lifecycle: " + _container_lifecycle_categories(captured.stdout[:8192]))
        except DevInstanceRuntimeError:
            note("disposable container lifecycle unavailable")


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


async def test_stopped_disposable_server_retains_actual_shutdown_events(disposable_storage_kubectl):
    runner = disposable_storage_kubectl.runner
    await AsyncCommandRunner().run(["docker", "stop", "--time", "1", runner.container_id])
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await runner.run(["kubectl", "get", "namespace", "default"])
    notes = raised.value.__notes__
    assert any(note.startswith("disposable k3s log categories: ") for note in notes)
    lifecycle = next(note for note in notes if note.startswith("disposable container lifecycle: "))
    assert "kill-15" in lifecycle and "stop" in lifecycle and "die" in lifecycle
