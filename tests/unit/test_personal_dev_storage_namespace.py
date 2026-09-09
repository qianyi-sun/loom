"""A retired incarnation must not delete a replacement namespace with its name."""

import json

import pytest

from loom.dev_instance_manifest import dev_instance_manifest_documents
from loom.dev_instance_runtime import CommandResult, DevInstanceRuntimeError, KubectlClient
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_dev_instance_runtime import _manifest_config
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import (
    _ANN_JSON,
    _ANN_SHA,
    _PASSWORD,
    _Cluster,
    _vault,
)


def test_every_bound_namespace_manifest_retains_storage_annotations():
    identity = _bound_claim().operation.storage_binding.identity
    document = dev_instance_manifest_documents(identity, _manifest_config())[0]
    assert document["metadata"]["annotations"][_ANN_JSON] == canonical_bytes(identity.storage_binding).decode()
    assert document["metadata"]["annotations"][_ANN_SHA] == canonical_digest(identity.storage_binding)
    assert _ANN_SHA not in document["metadata"]["labels"]


class _DeletingCluster(_Cluster):
    replacement = False
    conflict = False

    def __init__(self):
        super().__init__()
        self.deletes = []

    async def run(self, argv, *, stdin=None, timeout_seconds=120):
        if "delete" in argv:
            self.deletes.append((argv, json.loads(stdin)))
            self.namespace = {"metadata": {"name": "loom-dev-alice", "uid": "new-uid"}} if self.replacement else None
            if self.conflict:
                raise DevInstanceRuntimeError("UID precondition conflict")
            return CommandResult("{}", "")
        return await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)


@pytest.mark.parametrize("replacement,conflict", ((False, False), (True, False), (True, True)))
async def test_namespace_cleanup_uses_captured_uid_and_never_redeletes_replacement(replacement, conflict):
    cluster = _DeletingCluster()
    cluster.replacement, cluster.conflict = replacement, conflict
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    await KubectlClient("kubectl", runner=cluster).delete_storage_namespace(identity)
    assert len(cluster.deletes) == 1
    argv, body = cluster.deletes[0]
    assert "--raw=/api/v1/namespaces/loom-dev-alice" in argv
    assert argv[-2:] == ["-f", "-"]
    assert body == {"apiVersion": "v1", "kind": "DeleteOptions",
                    "preconditions": {"uid": "fixture-namespace-uid"}}
    if replacement:
        assert cluster.namespace["metadata"]["uid"] == "new-uid"


async def test_namespace_cleanup_rejects_different_binding_before_delete():
    cluster = _DeletingCluster()
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    cluster.namespace["metadata"]["annotations"][_ANN_SHA] = "0" * 64
    with pytest.raises(DevInstanceRuntimeError):
        await KubectlClient("kubectl", runner=cluster).delete_storage_namespace(identity)
    assert cluster.deletes == []


async def test_absent_bound_namespace_cleanup_is_idempotent():
    cluster = _DeletingCluster()
    identity = _bound_claim().operation.storage_binding.identity
    await KubectlClient("kubectl", runner=cluster).delete_storage_namespace(identity)
    assert cluster.deletes == []


async def test_terminating_current_namespace_cleanup_can_resume():
    cluster = _DeletingCluster()
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    cluster.namespace["metadata"]["deletionTimestamp"] = "2026-09-09T00:00:00Z"
    await KubectlClient("kubectl", runner=cluster).delete_storage_namespace(identity)
    assert len(cluster.deletes) == 1


@pytest.mark.parametrize("delete_failed", (False, True))
@pytest.mark.parametrize("malformed_uid", (None, "", 1, False))
async def test_cleanup_never_treats_malformed_readback_as_proof_of_replacement(delete_failed, malformed_uid):
    class MalformedReadback(_DeletingCluster):
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            result = await super().run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if "delete" in argv:
                self.namespace = {"metadata": {"name": "loom-dev-alice", "uid": malformed_uid}}
                if delete_failed:
                    raise DevInstanceRuntimeError("delete failed")
            return result

    cluster = MalformedReadback()
    identity = _bound_claim().operation.storage_binding.identity
    await _vault(cluster).store(identity, _PASSWORD)
    with pytest.raises(DevInstanceRuntimeError):
        await KubectlClient("kubectl", runner=cluster).delete_storage_namespace(identity)
