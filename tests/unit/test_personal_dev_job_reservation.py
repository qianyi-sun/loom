"""Persistent Job attempt authority rejects stale and ambiguous observations."""

import json
from copy import deepcopy
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import CommandResult, DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_job_reservation import reserve_job_attempt
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


class _Cluster:
    _namespace_uid = staticmethod(KubectlClient._namespace_uid)

    def __init__(self):
        self.runner = self
        self.identity = _bound_claim().operation.storage_binding.identity
        self.namespace_uid = str(uuid4())
        self.record = None
        self.writes = []
        self.lost_at = None
        self.corrupt_ack_uid = False
        self.conflict = False

    @staticmethod
    def _argv(*args):
        return ["kubectl", *args]

    async def read_storage_namespace(self, identity):
        assert identity == self.identity
        return {"metadata": {"uid": self.namespace_uid}}

    async def run(self, argv, *, stdin=None):
        verb = argv[1]
        if verb == "get":
            return CommandResult(json.dumps(self.record) if self.record is not None else "", "")
        self.writes.append(verb)
        document = json.loads(stdin)
        if verb == "create":
            assert self.record is None
            document["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        else:
            assert verb == "replace"
            if self.conflict:
                raise DevInstanceRuntimeError("reservation CAS conflict")
            assert document["metadata"]["uid"] == self.record["metadata"]["uid"]
            assert document["metadata"]["resourceVersion"] == self.record["metadata"]["resourceVersion"]
            document["metadata"]["resourceVersion"] = str(int(document["metadata"]["resourceVersion"]) + 1)
            if self.corrupt_ack_uid:
                document["metadata"]["uid"] = str(uuid4())
        self.record = document
        if self.lost_at == verb:
            raise DevInstanceRuntimeError("lost reply")
        return CommandResult(json.dumps(document), "")


async def _reserve(cluster, *, epoch=1, sequence=0, attempt=None, intent='{"template":{}}'):
    return await reserve_job_attempt(
        cluster, cluster.identity, namespace_uid=cluster.namespace_uid,
        job_name="loom-migrate-abcdef0-g1", intent=intent,
        operation_epoch=epoch, attempt=(sequence, attempt or uuid4()),
    )


@pytest.mark.parametrize("lost_at", ("create", "replace"))
async def test_lost_reservation_reply_reuses_same_record_without_repeated_mutation(lost_at):
    cluster, attempt = _Cluster(), uuid4()
    if lost_at == "replace":
        await _reserve(cluster)
    cluster.lost_at = lost_at
    if lost_at == "create":
        await _reserve(cluster, attempt=attempt)
    else:
        with pytest.raises(DevInstanceRuntimeError, match="lost reply"):
            await _reserve(cluster, sequence=1, attempt=attempt)
    before = deepcopy(cluster.record)
    writes = list(cluster.writes)
    await _reserve(cluster, sequence=int(lost_at == "replace"), attempt=attempt)
    assert cluster.record == before
    assert cluster.writes == writes


@pytest.mark.parametrize("epoch,sequence", ((1, 99), (2, 0), (2, 1)))
async def test_reservation_rejects_lower_order_or_equal_order_foreign_attempt(epoch, sequence):
    cluster = _Cluster()
    await _reserve(cluster, epoch=2, sequence=1)
    before = deepcopy(cluster.record)
    with pytest.raises(DevInstanceRuntimeError, match="superseded"):
        await _reserve(cluster, epoch=epoch, sequence=sequence)
    assert cluster.record == before
    assert cluster.writes == ["create"]


async def test_higher_epoch_can_restart_sequence_but_invalidates_old_receipt():
    cluster = _Cluster()
    old = await _reserve(cluster, sequence=5)
    current = await _reserve(cluster, epoch=2)
    await current.verify(cluster)
    with pytest.raises(DevInstanceRuntimeError, match="superseded"):
        await old.verify(cluster)
    assert old.document["metadata"]["uid"] == current.document["metadata"]["uid"]


@pytest.mark.parametrize("corruption", ("owner", "namespace", "uid", "extra_data", "binary", "immutable", "deleting", "finalizer", "intent", "epoch", "sequence", "attempt"))
async def test_foreign_or_malformed_record_is_not_mutation_authority(corruption):
    cluster, attempt = _Cluster(), uuid4()
    receipt = await _reserve(cluster, attempt=attempt)
    if corruption == "owner":
        cluster.record["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif corruption == "namespace":
        cluster.namespace_uid = str(uuid4())
    elif corruption == "uid":
        cluster.record["metadata"]["uid"] = str(uuid4())
    elif corruption == "extra_data":
        cluster.record["data"]["extra"] = "value"
    elif corruption == "binary":
        cluster.record["binaryData"] = {"extra": "dmFsdWU="}
    elif corruption == "immutable":
        cluster.record["immutable"] = True
    elif corruption == "deleting":
        cluster.record["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    elif corruption == "finalizer":
        cluster.record["metadata"]["finalizers"] = ["example.org/hold"]
    else:
        cluster.record["data"][corruption] = "invalid"
    with pytest.raises(DevInstanceRuntimeError):
        await receipt.verify(cluster)
    if corruption != "uid":
        with pytest.raises(DevInstanceRuntimeError):
            await _reserve(cluster, attempt=attempt)
    assert cluster.writes == ["create"]


async def test_reservation_cas_conflict_does_not_retry_mutation():
    cluster = _Cluster()
    await _reserve(cluster)
    before = deepcopy(cluster.record)
    cluster.conflict = True
    with pytest.raises(DevInstanceRuntimeError, match="CAS conflict"):
        await _reserve(cluster, sequence=1)
    assert cluster.record == before
    assert cluster.writes == ["create", "replace"]


async def test_reservation_put_acknowledgement_must_retain_supplied_uid():
    cluster = _Cluster()
    await _reserve(cluster)
    cluster.corrupt_ack_uid = True
    with pytest.raises(DevInstanceRuntimeError, match="acknowledged"):
        await _reserve(cluster, sequence=1)
