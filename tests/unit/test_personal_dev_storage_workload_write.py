"""Malformed cluster observations never become permission to create workloads."""

import pytest

from loom.dev_instance_runtime import CommandResult, DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_storage_workload_write import write_storage_workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


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
