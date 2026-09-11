"""Malformed cluster observations never become permission to create workloads."""

import pytest

from loom.dev_instance_runtime import CommandResult, DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_storage_workload_write import write_storage_workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


@pytest.mark.parametrize("owner_change", ("absent", "wrong_uid", "extra", "controller", "block_deletion"))
async def test_workload_rejects_noncanonical_namespace_owner_without_writes(owner_change):
    from copy import deepcopy

    from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
    from tests.unit.test_personal_dev_storage_vault import _Cluster

    identity = _bound_claim().operation.storage_binding.identity
    cluster = _Cluster()
    cluster.namespace = {"metadata": {"name": identity.namespace, "uid": "namespace-uid",
                                     "annotations": personal_dev_storage_annotations(identity)}}
    kubectl = KubectlClient("kubectl", runner=cluster)
    document = {"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "probe", "namespace": identity.namespace},
        "spec": {"replicas": 1, "template": {}}}
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    stored = cluster.workloads[("Deployment", "probe")]
    references = stored["metadata"]["ownerReferences"]
    if owner_change == "absent":
        stored["metadata"].pop("ownerReferences")
    elif owner_change == "wrong_uid":
        references[0]["uid"] = "foreign-namespace-uid"
    elif owner_change == "extra":
        references.append(deepcopy(references[0]))
    else:
        references[0]["controller" if owner_change == "controller" else "blockOwnerDeletion"] = True
    before = deepcopy(cluster.writes)
    with pytest.raises(DevInstanceRuntimeError, match="incarnation"):
        await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    assert cluster.writes == before


@pytest.mark.parametrize("payload", ('{}', '[]', 'null', 'false', '0', '"bad"', '{', '{"metadata":{}}'))
async def test_malformed_workload_readback_rejects_before_mutation(payload):
    identity = _bound_claim().operation.storage_binding.identity

    class Cluster:
        runner = None

        @staticmethod
        def _argv(*args):
            return ["kubectl", *args]

        _namespace_uid = staticmethod(KubectlClient._namespace_uid)

        async def read_storage_namespace(self, supplied):
            assert supplied == identity
            return {"metadata": {"uid": "namespace-uid"}}

        async def run(self, argv, **_kwargs):
            assert "get" in argv, "malformed readback must not trigger any mutation"
            return CommandResult(payload, "")

    cluster = Cluster()
    cluster.runner = cluster
    document = {"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "probe", "namespace": identity.namespace},
        "spec": {"replicas": 1, "template": {}}}
    with pytest.raises(DevInstanceRuntimeError):
        await write_storage_workload(cluster, identity, document, operation_epoch=1)


@pytest.mark.parametrize("recovery", ("missing", "malformed", "foreign", "drift", "namespace_replaced"))
async def test_ambiguous_create_never_activates_unauthenticated_readback(recovery):
    from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
    from tests.unit.test_personal_dev_storage_vault import _Cluster

    identity = _bound_claim().operation.storage_binding.identity

    class Cluster(_Cluster):
        creates = 0
        recovering = False

        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            if "create" in argv:
                self.creates += 1
                if recovery != "missing":
                    await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
                    stored = self.workloads[("Deployment", "probe")]
                    if recovery == "foreign":
                        stored["metadata"]["ownerReferences"][0]["uid"] = "foreign-uid"
                    elif recovery == "drift":
                        stored["spec"]["replicas"] = 99
                    elif recovery == "namespace_replaced":
                        self.namespace["metadata"]["uid"] = "successor-uid"
                self.recovering = True
                raise DevInstanceRuntimeError("ambiguous create")
            if self.recovering and "get" in argv and "deployment" in argv and recovery == "malformed":
                return CommandResult("{}", "")
            if "replace" in argv:
                assert "--dry-run=server" in argv, "ambiguous objects must not be activated"
            return await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    cluster = Cluster()
    cluster.namespace = {"metadata": {"name": identity.namespace, "uid": "namespace-uid",
                                     "annotations": personal_dev_storage_annotations(identity)}}
    document = {"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "probe", "namespace": identity.namespace},
        "spec": {"replicas": 1, "template": {}}}
    with pytest.raises(DevInstanceRuntimeError):
        await write_storage_workload(KubectlClient("kubectl", runner=cluster), identity, document, operation_epoch=1)
    assert cluster.creates == 1
    assert all("replace" not in argv or "--dry-run=server" in argv for argv, _ in cluster.writes)
    assert document["spec"]["replicas"] == 1
