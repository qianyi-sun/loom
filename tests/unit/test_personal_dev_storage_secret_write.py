"""Only demonstrably inert, owner-matching stale placeholders are disposable."""

from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
from loom.personal_dev_storage_secret_write import _stale_placeholder
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import _Cluster, _vault


@pytest.mark.parametrize(
    "defect",
    (
        None,
        "data",
        "data_shape",
        "immutable",
        "phase",
        "owner",
        "subject",
        "name",
        "namespace",
        "uid",
        "version",
        "current_namespace_uid",
        "epoch",
        "future_epoch",
        "annotations",
        "finalizer",
        "owner_reference",
    ),
)
def test_stale_placeholder_cleanup_requires_exact_inert_owner_provenance(defect):
    binding = _bound_claim().operation.storage_binding
    identity = binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    old = binding
    if defect == "owner":
        old = old.model_copy(update={"owner_user_id": uuid4()})
    if defect == "subject":
        old = old.model_copy(update={"subject_id": uuid4()})
    document = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "data": {},
        "metadata": {
            "name": "seed",
            "namespace": identity.namespace,
            "uid": str(uuid4()),
            "resourceVersion": "3",
            "annotations": {
                **personal_dev_storage_annotations(old.identity),
                "loom.dev/storage-namespace-uid": "old-namespace",
                "loom.dev/storage-write-phase": "empty",
                "loom.dev/storage-operation-epoch": "1",
            },
        },
    }
    metadata = document["metadata"]
    annotations = metadata["annotations"]
    if defect == "data":
        document["data"] = {"password": "c2VjcmV0"}
    elif defect == "data_shape":
        document["data"] = []
    elif defect == "immutable":
        document["immutable"] = True
    elif defect == "phase":
        annotations["loom.dev/storage-write-phase"] = "ready"
    elif defect in ("name", "namespace"):
        metadata[defect] = "other"
    elif defect in ("uid", "version"):
        metadata.pop("uid" if defect == "uid" else "resourceVersion")
    elif defect == "current_namespace_uid":
        annotations["loom.dev/storage-namespace-uid"] = "current-namespace"
    elif defect in ("epoch", "future_epoch"):
        annotations["loom.dev/storage-operation-epoch"] = "0" if defect == "epoch" else "3"
    elif defect == "annotations":
        annotations["unrecognized"] = "value"
    elif defect == "finalizer":
        metadata["finalizers"] = ["someone-else"]
    elif defect == "owner_reference":
        metadata["ownerReferences"] = [{"uid": str(uuid4())}]
    assert _stale_placeholder(document, identity, "current-namespace", "seed", 2) is (
        defect is None
    )


async def test_prepared_same_operation_writer_cannot_replace_winner_credentials():
    cluster = _Cluster()
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    await _vault(cluster).store(identity, "b" * 32)
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=cluster), database=None, config=None,
    )
    first = await installer._credentials(claim, identity)
    delayed = await installer._credentials(claim, identity)
    assert first.reporter_token != delayed.reporter_token
    await installer._persist_credentials(claim, identity, first)
    before = list(cluster.writes)
    with pytest.raises(DevInstanceRuntimeError):
        await installer._persist_credentials(claim, identity, delayed)
    assert cluster.writes == before
    retained = await installer._credentials(claim, identity)
    assert retained.reporter_token == first.reporter_token
    assert retained.agent_password == first.agent_password
    assert retained.observer_password == first.observer_password
    await installer._persist_credentials(claim, identity, retained)
