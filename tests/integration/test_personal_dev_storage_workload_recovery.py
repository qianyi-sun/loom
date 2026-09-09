"""Workload recovery must reject drift and replace only terminal failed jobs."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError
from loom.personal_dev_storage_workload_write import write_storage_workload
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.integration.test_personal_dev_storage_workload_write import _namespace, _workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def test_job_rejects_tampering_with_api_defaulted_replacement_policy(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, "Job")
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    observed = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    assert observed["spec"]["podReplacementPolicy"] == "TerminatingOrFailed"
    observed["spec"]["podReplacementPolicy"] = "Failed"
    await kubectl.runner.run(kubectl._argv("replace", "-f", "-"), stdin=json.dumps(observed))
    with pytest.raises(DevInstanceRuntimeError, match="persisted template"):
        await write_storage_workload(kubectl, identity, document, operation_epoch=1)


async def test_new_attempt_replaces_failed_job_but_replays_success_without_duplication(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, "Job")
    document["metadata"]["labels"] = {"loom.dev/attempt": str(uuid4())}
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    old = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    await kubectl.runner.run(kubectl._argv("patch", "job", document["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", json.dumps({"status": {
            "startTime": datetime.now(UTC).isoformat(), "failed": 1,
            "conditions": [
                {"type": "FailureTarget", "status": "True", "reason": "BackoffLimitExceeded"},
                {"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"},
            ],
        }})))
    retry = deepcopy(document)
    retry["metadata"]["labels"]["loom.dev/attempt"] = str(uuid4())
    await write_storage_workload(kubectl, identity, retry, operation_epoch=1)
    current = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    assert current["metadata"]["uid"] != old["metadata"]["uid"]
    assert not current.get("status", {}).get("failed")
    await kubectl.runner.run(kubectl._argv("patch", "job", document["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", json.dumps({"status": {
            "startTime": datetime.now(UTC).isoformat(), "completionTime": datetime.now(UTC).isoformat(),
            "succeeded": 1, "conditions": [
                {"type": "SuccessCriteriaMet", "status": "True"},
                {"type": "Complete", "status": "True"},
            ],
        }})))
    retry["metadata"]["labels"]["loom.dev/attempt"] = str(uuid4())
    await write_storage_workload(kubectl, identity, retry, operation_epoch=1)
    complete = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    assert complete["metadata"]["uid"] == current["metadata"]["uid"]
    assert complete["status"]["succeeded"] == 1
