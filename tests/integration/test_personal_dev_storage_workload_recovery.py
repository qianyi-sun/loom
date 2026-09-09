"""Workload recovery must reject drift and replace only terminal failed jobs."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_storage_workload_write import write_storage_workload
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.integration.test_personal_dev_storage_workload_write import _namespace, _workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


async def _reconcile_workload(kubectl, identity, document, *, operation_epoch):
    # A fresh caller may reconcile again after a rejected CAS; Kubernetes
    # status controllers legitimately change RV while a workload is staged.
    # Keep retries here, outside the one-shot writer. Negative tests call the
    # writer directly so conflicts/tampering cannot be swallowed as success.
    for attempt in range(5):
        try:
            await write_storage_workload(kubectl, identity, document, operation_epoch=operation_epoch)
            return
        except DevInstanceRuntimeError:
            if attempt == 4:
                raise
            await asyncio.sleep(0)


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
    document["metadata"]["labels"] = {"loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": "0"}
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    old = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    await kubectl.runner.run(kubectl._argv("patch", "job", document["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", json.dumps({"status": {
            "startTime": datetime.now(UTC).isoformat(), "failed": 1,
            "active": 0, "ready": 0, "terminating": 0,
            "conditions": [
                {"type": "FailureTarget", "status": "True", "reason": "BackoffLimitExceeded"},
                {"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"},
            ],
        }})))
    retry = deepcopy(document)
    retry["metadata"]["labels"]["loom.dev/attempt"] = str(uuid4())
    retry["metadata"]["labels"]["loom.dev/attempt-sequence"] = "1"
    await _reconcile_workload(kubectl, identity, retry, operation_epoch=1)
    current = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    assert current["metadata"]["uid"] != old["metadata"]["uid"]
    assert not current.get("status", {}).get("failed")
    # The real controller can already have counted an unscheduled Pod active.
    # A synthetic terminal fixture must explicitly clear all live-Pod counters.
    await kubectl.runner.run(kubectl._argv("patch", "job", document["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", '{"status":{"active":1}}'))
    await kubectl.runner.run(kubectl._argv("patch", "job", document["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", json.dumps({"status": {
            "startTime": datetime.now(UTC).isoformat(), "completionTime": datetime.now(UTC).isoformat(),
            "active": 0, "ready": 0, "terminating": 0,
            "succeeded": 1, "conditions": [
                {"type": "SuccessCriteriaMet", "status": "True"},
                {"type": "Complete", "status": "True"},
            ],
        }})))
    retry["metadata"]["labels"]["loom.dev/attempt"] = str(uuid4())
    retry["metadata"]["labels"]["loom.dev/attempt-sequence"] = "2"
    await _reconcile_workload(kubectl, identity, retry, operation_epoch=1)
    complete = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    assert complete["metadata"]["uid"] == current["metadata"]["uid"]
    assert complete["status"]["succeeded"] == 1


@pytest.mark.parametrize("meaningful_change", (False, True))
async def test_acknowledged_create_allows_status_only_observation_not_new_spec_authority(
    disposable_storage_kubectl,  # noqa: F811
    meaningful_change,
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity)

    class UpdatedAfterCreate:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if "create" in argv:
                mutation = {"spec": {"replicas": 2}} if meaningful_change else {"status": {"observedGeneration": 98}}
                await kubectl.runner.run(kubectl._argv("patch", "deployment", document["metadata"]["name"],
                    "-n", identity.namespace, "--type=merge", *([] if meaningful_change else ["--subresource=status"]),
                    "-p", json.dumps(mutation)))
            return result

    writer = KubectlClient("kubectl", runner=UpdatedAfterCreate())
    if meaningful_change:
        with pytest.raises(DevInstanceRuntimeError):
            await write_storage_workload(writer, identity, document, operation_epoch=1)
        observed = await kubectl.read_resource_json(namespace=identity.namespace, kind="deployment", name=document["metadata"]["name"])
        assert observed["spec"]["replicas"] == 2
    else:
        await write_storage_workload(writer, identity, document, operation_epoch=1)
        observed = await kubectl.read_resource_json(namespace=identity.namespace, kind="deployment", name=document["metadata"]["name"])
        assert observed["spec"]["replicas"] == 1


@pytest.mark.parametrize("stale_sequence", ("0", "1"))
async def test_stale_attempt_cannot_replace_newer_terminal_failed_job(
    disposable_storage_kubectl, stale_sequence,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, "Job")
    document["metadata"]["labels"] = {
        "loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": "1",
    }
    await _reconcile_workload(kubectl, identity, document, operation_epoch=1)
    current = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    await kubectl.runner.run(kubectl._argv("patch", "job", document["metadata"]["name"], "-n", identity.namespace,
        "--subresource=status", "--type=merge", "-p", json.dumps({"status": {
            "startTime": datetime.now(UTC).isoformat(), "failed": 1,
            "active": 0, "ready": 0, "terminating": 0,
            "conditions": [{"type": "FailureTarget", "status": "True"}, {"type": "Failed", "status": "True"}],
        }})))
    stale = deepcopy(document)
    stale["metadata"]["labels"].update({"loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": stale_sequence})
    with pytest.raises(DevInstanceRuntimeError, match="attempt"):
        await write_storage_workload(kubectl, identity, stale, operation_epoch=1)
    remaining = await kubectl.read_resource_json(namespace=identity.namespace, kind="job", name=document["metadata"]["name"])
    assert remaining["metadata"]["uid"] == current["metadata"]["uid"]
