"""Status-only Kubernetes conflicts leave authority-owned attempts retryable."""

import asyncio

import pytest

from loom.dev_instance_runtime import (
    AsyncCommandRunner, CommandResult, DevInstanceRuntimeError, KubectlClient,
    KubernetesResourceVersionConflict, WorkloadStatusConflict,
)
from loom.personal_dev_reconciler import PersonalDevEnvironmentReconciler
from tests.unit.test_personal_dev_reconciler import (
    _NOW, _Authority, _Executor, _Installer, _Projector, _async_value, _claim,
)


@pytest.mark.parametrize("failure", (WorkloadStatusConflict, DevInstanceRuntimeError, asyncio.CancelledError))
async def test_status_conflict_does_not_terminalize_or_activate_attempt(failure):
    authority = _Authority(_claim())

    class Executor(_Executor):
        async def prepare(self, claim, *, access):
            self.prepared += 1
            raise failure("fixture")

    executor = Executor()
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority, executor=executor, capacity_installer=_Installer(),
        capacity_projector=_Projector(), access_loader=lambda _claim: _async_value("owner-access-before"),
        reconciler_id="reconciler-a", lease_seconds=60,
    )
    if failure is asyncio.CancelledError:
        with pytest.raises(asyncio.CancelledError):
            await reconciler.reconcile_once(now=_NOW)
    else:
        assert await reconciler.reconcile_once(now=_NOW)
    assert executor.prepared == 1  # No internal fresh-authority mutation retry.
    assert executor.bootstrapped == 0
    assert authority.begun == []
    assert authority.failed == (["provisioning_failed"] if failure is DevInstanceRuntimeError else [])


_CONFLICT = ('Error from server (Conflict): error when replacing "STDIN": '
             'Operation cannot be fulfilled on deployments.apps "probe": '
             'the object has been modified; please apply your changes to the latest version and try again')


@pytest.mark.parametrize("diagnostic,conflict", (
    (_CONFLICT + "\n", True),
    ("Error from server (Forbidden): " + _CONFLICT, False),
    ("Error from server (Forbidden): denied\n" + _CONFLICT, False),
    ("Warning: unknown\n" + _CONFLICT, False),
    ("Unable to connect to the server", False),
    ("Error from server (Conflict): unrelated conflict", False),
))
async def test_runner_exposes_only_canonical_nonsecret_resource_version_conflicts(monkeypatch, diagnostic, conflict):
    class Process:
        returncode = 1

        async def communicate(self, _stdin):
            return b"", diagnostic.encode()

    async def start(*_args, **_kwargs):
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", start)
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await AsyncCommandRunner().run(["kubectl", "replace", "-f", "-"])
    assert isinstance(raised.value, KubernetesResourceVersionConflict) is conflict
    assert diagnostic.strip() not in str(raised.value)


@pytest.mark.parametrize("mutation", ("status", "denial_status", "spec", "uid", "namespace", "unchanged", "malformed"))
@pytest.mark.parametrize("phase", ("dry_run", "activation"))
async def test_failed_cas_is_retryable_only_with_conflict_and_status_only_readback(mutation, phase):
    from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
    from loom.personal_dev_storage_workload_write import write_storage_workload
    from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
    from tests.unit.test_personal_dev_storage_vault import _Cluster

    identity = _bound_claim().operation.storage_binding.identity

    class Cluster(_Cluster):
        failed = False
        replacements = 0

        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "replace" in argv:
                self.replacements += 1
                if ("--dry-run=server" in argv) == (phase == "dry_run"):
                    assert not self.failed, "the writer must never retry a failed CAS"
                    self.failed = True
                    current = self.workloads[("Deployment", "probe")]
                    if mutation != "unchanged":
                        current["metadata"]["resourceVersion"] = "new-version"
                        current["status"] = {"observedGeneration": 1}
                    if mutation == "spec":
                        current["spec"]["replicas"] = 2
                    elif mutation == "uid":
                        current["metadata"]["uid"] = "different-workload"
                    elif mutation == "namespace":
                        self.namespace["metadata"]["uid"] = "different-namespace"
                    error = DevInstanceRuntimeError if mutation == "denial_status" else KubernetesResourceVersionConflict
                    raise error("bounded failure")
            if self.failed and mutation == "malformed" and "deployment" in argv:
                return CommandResult("{}", "")
            return await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    cluster = Cluster()
    cluster.namespace = {"metadata": {"name": identity.namespace, "uid": "namespace-uid",
                                     "annotations": personal_dev_storage_annotations(identity)}}
    document = {"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "probe", "namespace": identity.namespace},
        "spec": {"replicas": 1, "template": {}}}
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await write_storage_workload(KubectlClient("kubectl", runner=cluster), identity, document, operation_epoch=1)
    assert isinstance(raised.value, WorkloadStatusConflict) is (mutation == "status")
    assert cluster.replacements == (1 if phase == "dry_run" else 2)
