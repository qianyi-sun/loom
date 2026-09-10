"""Delayed personal controllers cannot CREATE runnable successor workloads."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from uuid import uuid4

import pytest
import yaml

from loom.dev_instance_runtime import (
    DevInstanceRuntimeError,
    KubectlClient,
    WorkloadStatusConflictError,
)
from loom.personal_dev_capacity_runtime import KubectlPersonalDevCapacityInstaller
from loom.personal_dev_incarnation_storage import personal_dev_storage_annotations
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


def _workload(identity, kind="Deployment"):
    template = {
        "metadata": {"labels": {"app": "storage-workload-probe"}},
        "spec": {
            "automountServiceAccountToken": False,
            "containers": [{"name": "probe", "image": "registry.example/probe@sha256:" + "a" * 64}],
        },
    }
    spec = {"template": template}
    if kind == "Job":
        template["spec"]["restartPolicy"] = "Never"
        spec["backoffLimit"] = 0
    else:
        spec.update(replicas=1, selector={"matchLabels": template["metadata"]["labels"]})
    return {
        "apiVersion": "batch/v1" if kind == "Job" else "apps/v1",
        "kind": kind,
        "metadata": {"name": "storage-workload-probe", "namespace": identity.namespace,
                     **({"labels": {"loom.dev/attempt": str(uuid4()), "loom.dev/attempt-sequence": "0"}} if kind == "Job" else {})},
        "spec": spec,
    }


async def _namespace(kubectl, identity):
    await kubectl.apply(json.dumps({
        "apiVersion": "v1", "kind": "Namespace",
        "metadata": {"name": identity.namespace, "annotations": personal_dev_storage_annotations(identity)},
    }))


async def test_delayed_capacity_workload_creation_is_inert_after_namespace_replacement(
    disposable_storage_kubectl,  # noqa: F811
):
    kubectl = disposable_storage_kubectl
    claim = _bound_claim()
    identity = claim.operation.storage_binding.identity
    await _namespace(kubectl, identity)
    paused, resume = asyncio.Event(), asyncio.Event()
    acknowledged = []

    class PausedWriter:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            documents = tuple(yaml.safe_load_all(stdin)) if stdin is not None else ()
            if any(isinstance(item, dict) and item.get("kind") == "Deployment" for item in documents):
                paused.set()
                await resume.wait()
            reply = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if "create" in argv:
                acknowledged.append(json.loads(reply.stdout))
            return reply

    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=KubectlClient("kubectl", runner=PausedWriter()), database=None, config=None,
    )
    task = asyncio.create_task(installer._apply_manifests(claim, identity, (_workload(identity),)))
    try:
        await asyncio.wait_for(paused.wait(), timeout=30)
        await kubectl.delete_storage_namespace(identity)
        successor = identity.storage_binding.model_copy(update={"subject_incarnation": uuid4()}).identity
        await _namespace(kubectl, successor)
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, timeout=30)
        assert len(acknowledged) == 1
        assert acknowledged[0]["spec"]["replicas"] == 0
        # Namespace-owner GC may already have collected the inert object.
        reply = await kubectl.runner.run(kubectl._argv(
            "get", "deployment", "storage-workload-probe", "-n", identity.namespace,
            "--ignore-not-found", "-o", "json",
        ))
        if reply.stdout.strip():
            assert json.loads(reply.stdout)["spec"]["replicas"] == 0
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("kind", ("Job", "Deployment"))
async def test_workload_staging_preserves_api_defaults_and_exact_replay(
    disposable_storage_kubectl,  # noqa: F811
    kind,
):
    from loom.personal_dev_storage_workload_write import write_storage_workload

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, kind)
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    before = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"])
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    after = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"])
    assert before["metadata"]["uid"] == after["metadata"]["uid"]
    assert before["spec"] == after["spec"]
    assert after["spec"].get("suspend", False) is False
    if kind == "Deployment":
        assert after["spec"]["replicas"] == 1


async def test_real_candidate_new_attempt_reuses_job_and_updates_deployment(
    disposable_storage_kubectl,  # noqa: F811
):
    from loom.dev_instance_manifest import dev_instance_manifest_documents
    from loom.dev_instance_runtime import KubectlCandidateGenerationProvisioner
    from tests.unit.test_dev_instance_manifest import _immutable_config

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    config = _immutable_config()
    config = replace(config, lifecycle_binding=replace(
        config.lifecycle_binding, subject_id=identity.storage_binding.subject_id,
        subject_incarnation=identity.storage_incarnation,
    ))
    retry = replace(config, lifecycle_binding=replace(config.lifecycle_binding, attempt_id=uuid4(), attempt_sequence=1))
    provisioner = KubectlCandidateGenerationProvisioner(kubectl)
    originals = {}
    for selected in (config, retry):
        documents = tuple(document for document in dev_instance_manifest_documents(identity, selected)
                          if document["kind"] == "Job" or (document["kind"] == "Deployment" and "loom-web" in document["metadata"]["name"]))
        # This low-level probe has no durable reconciler. Model its fresh
        # reconciliation after an explicitly classified status-only conflict;
        # all authority/spec conflicts still fail immediately.
        for attempt in range(5):
            try:
                await provisioner._apply_generation_workloads(identity, selected, documents)
                break
            except WorkloadStatusConflictError:
                if attempt == 4:
                    raise
        for document in documents:
            resource = await kubectl.read_resource_json(namespace=identity.namespace, kind=document["kind"].lower(), name=document["metadata"]["name"])
            assert resource["metadata"]["labels"]["loom.dev/attempt"] == str(selected.lifecycle_binding.attempt_id)
            if selected is config:
                originals[document["kind"]] = resource["metadata"]["uid"]
            else:
                assert resource["metadata"]["uid"] == originals[document["kind"]]


async def test_deployment_update_removes_fields_and_rejects_observed_template_tampering(
    disposable_storage_kubectl,  # noqa: F811
):
    from loom.personal_dev_storage_workload_write import write_storage_workload

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity)
    document["spec"]["template"]["spec"]["containers"][0]["env"] = [{"name": "REMOVED", "value": "old"}]
    await write_storage_workload(kubectl, identity, document, operation_epoch=1)
    changed = deepcopy(document)
    changed["spec"]["template"]["spec"]["containers"][0].pop("env")
    changed["spec"]["template"]["spec"]["containers"][0]["command"] = ["probe", "--max-attempts", "9999"]
    await write_storage_workload(kubectl, identity, changed, operation_epoch=1)
    observed = await kubectl.read_resource_json(namespace=identity.namespace, kind="deployment", name=document["metadata"]["name"])
    container = observed["spec"]["template"]["spec"]["containers"][0]
    assert not container.get("env")
    assert container["command"] == ["probe", "--max-attempts", "9999"]
    container["command"] = ["tampered"]
    await kubectl.runner.run(kubectl._argv("replace", "-f", "-"), stdin=json.dumps(observed))
    with pytest.raises(DevInstanceRuntimeError, match="persisted template"):
        await write_storage_workload(kubectl, identity, changed, operation_epoch=1)


@pytest.mark.parametrize("kind", ("Job", "Deployment"))
@pytest.mark.parametrize("barrier", ("after_create", "before_put_absent", "before_put_replaced"))
async def test_workload_put_cannot_create_or_replace_successor_object(
    disposable_storage_kubectl,  # noqa: F811
    kind, barrier,
):
    from loom.personal_dev_storage_workload_write import write_storage_workload

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, kind)
    paused, resume = asyncio.Event(), asyncio.Event()

    class PausedWriter:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            payload = json.loads(stdin) if stdin else None
            target = isinstance(payload, dict) and payload.get("kind") == kind
            if target and "replace" in argv and "--dry-run=server" not in argv and barrier.startswith("before_put"):
                paused.set()
                await resume.wait()
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if target and "create" in argv and barrier == "after_create":
                paused.set()
                await resume.wait()
            return result

    task = asyncio.create_task(write_storage_workload(KubectlClient("kubectl", runner=PausedWriter()), identity, document, operation_epoch=1))
    try:
        await asyncio.wait_for(paused.wait(), timeout=30)
        await kubectl.delete_storage_namespace(identity)
        successor = identity.storage_binding.model_copy(update={"subject_incarnation": uuid4()}).identity
        await _namespace(kubectl, successor)
        expected_uid = None
        if barrier == "before_put_replaced":
            # Independent successor fixture, not a second installer race: built-in
            # controllers may update a staged object's RV before its dry-run PUT.
            await kubectl.runner.run(kubectl._argv("create", "-f", "-"), stdin=json.dumps(document))
            expected_uid = (await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"]))["metadata"]["uid"]
        resume.set()
        with pytest.raises(DevInstanceRuntimeError):
            await asyncio.wait_for(task, timeout=30)
        result = await kubectl.runner.run(kubectl._argv("get", kind.lower(), document["metadata"]["name"], "-n", identity.namespace, "--ignore-not-found", "-o", "json"))
        if expected_uid is None:
            assert not result.stdout.strip()
        else:
            assert json.loads(result.stdout)["metadata"]["uid"] == expected_uid
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("kind", ("Job", "Deployment"))
@pytest.mark.parametrize("lost_at", ("create", "replace"))
async def test_workload_lost_reply_retries_same_uid_and_rejects_old_epoch(
    disposable_storage_kubectl,  # noqa: F811
    kind, lost_at,
):
    from loom.personal_dev_storage_workload_write import write_storage_workload

    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity, kind)

    class LostReply:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            result = await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            payload = json.loads(stdin) if stdin else None
            if lost_at in argv and "--dry-run=server" not in argv and isinstance(payload, dict) and payload.get("kind") == kind:
                raise DevInstanceRuntimeError("lost reply")
            return result

    if lost_at == "create":
        await write_storage_workload(KubectlClient("kubectl", runner=LostReply()), identity, document, operation_epoch=2)
    else:
        with pytest.raises(DevInstanceRuntimeError, match="lost reply"):
            await write_storage_workload(KubectlClient("kubectl", runner=LostReply()), identity, document, operation_epoch=2)
    before = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"])
    await write_storage_workload(kubectl, identity, document, operation_epoch=2)
    after = await kubectl.read_resource_json(namespace=identity.namespace, kind=kind.lower(), name=document["metadata"]["name"])
    assert after["metadata"]["uid"] == before["metadata"]["uid"]
    with pytest.raises(DevInstanceRuntimeError, match=r"epoch|reservation attempt was superseded"):
        await write_storage_workload(kubectl, identity, document, operation_epoch=1)
