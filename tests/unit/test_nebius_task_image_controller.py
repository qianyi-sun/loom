from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from kubernetes.client import ApiException

from loom_execution_actuator.task_image_controller import (
    NativeBuildKubernetesApi,
    _matches,
    build_observation,
    publication_receipt,
)


def receipt_job(*, message=None):
    image_id = uuid4()
    message = message if message is not None else json.dumps({
        "materialization_id": str(image_id), "lease_epoch": 2,
        "registry_images": {"task": "registry.example/tasks@sha256:" + "a" * 64},
    })
    return image_id, {
        "metadata": {"uid": "job-uid"}, "status": {"succeeded": 1},
        "pods": [{"metadata": {"name": "pod", "uid": "pod-uid", "ownerReferences": [{"kind": "Job", "uid": "job-uid"}]},
                  "status": {"containerStatuses": [{"name": "publish", "state": {
                      "terminated": {"exitCode": 0, "message": message, "reason": "Completed"},
                  }}]}}],
    }


def test_receipt_requires_owned_pod_successful_publisher_and_exact_identity():
    image_id, job = receipt_job()
    assert "task" in publication_receipt(job, materialization_id=image_id, lease_epoch=2)
    with pytest.raises(ValueError, match="belong"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=3)
    job["pods"][0]["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    with pytest.raises(ValueError, match="exit zero"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)
    job["pods"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign"
    with pytest.raises(ValueError, match="owned Pod"):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)


@pytest.mark.parametrize("message", ["[]", "{}", "x" * 4097, "invalid json"])
def test_bad_receipt_is_bounded_validation_error(message):
    image_id, job = receipt_job(message=message)
    with pytest.raises(ValueError):
        publication_receipt(job, materialization_id=image_id, lease_epoch=2)


def test_observation_excludes_raw_messages_and_redacts_bounded_builder_log():
    _, job = receipt_job(message="private receipt token")
    job["pods"][0]["status"]["initContainerStatuses"] = [{"name": "prepare", "state": {
        "terminated": {"exitCode": 1, "reason": "Error", "message": "password=raw-secret", "containerID": "private"},
    }}]
    job["builder_log"] = "compile error password=secretvalue token=othervalue " + "x" * 20000
    result = build_observation(job)
    encoded = json.dumps(result)
    assert "private receipt" not in encoded and "raw-secret" not in encoded
    assert "secretvalue" not in encoded and "othervalue" not in encoded
    assert len(result["builder_log"]) <= 16384
    assert result["phases"][0]["state"]["terminated"] == {"exitCode": 1, "reason": "Error"}


def test_expected_job_accepts_api_defaults_and_equivalent_quantities_but_not_security_mutation():
    desired = {"spec": {"resources": {"requests": {"cpu": "1000m", "memory": "2048Mi"}}, "securityContext": {"privileged": False}}}
    observed = {"spec": {"resources": {"requests": {"cpu": "1", "memory": "2Gi"}}, "securityContext": {"privileged": False}, "default": True}}
    assert _matches(observed, desired)
    observed["spec"]["securityContext"]["privileged"] = True
    assert not _matches(observed, desired)


class FakeClients:
    def __init__(self):
        self.job = {"metadata": {"name": "build", "namespace": "builds", "uid": "job-uid", "labels": {"owner": "ours"}}}
        self.cm = {"metadata": {"name": "build", "namespace": "builds", "uid": "cm-uid", "labels": {"owner": "ours"}}, "immutable": True, "data": {"claim.json": "{}"}}
        self.pods = [{"metadata": {"name": "pod", "uid": "pod-uid", "ownerReferences": [{"kind": "Job", "uid": "job-uid"}]}}]
        self.deleted = []

    def read_namespaced_job(self, *args, **kwargs):
        if self.job is None:
            raise ApiException(status=404)
        return copy.deepcopy(self.job)

    def read_namespaced_config_map(self, *args, **kwargs):
        if self.cm is None:
            raise ApiException(status=404)
        return copy.deepcopy(self.cm)

    def list_namespaced_pod(self, *args, **kwargs):
        return SimpleNamespace(items=copy.deepcopy(self.pods))

    def create_namespaced_job(self, *args, **kwargs):
        raise ApiException(status=409)

    def create_namespaced_config_map(self, *args, **kwargs):
        raise ApiException(status=409)

    def delete_namespaced_job(self, *args, **kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == self.job["metadata"]["uid"]
        self.deleted.append("job")
        self.job = None

    def delete_namespaced_pod(self, *args, **kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == "pod-uid"
        self.deleted.append("pod")
        self.pods = []

    def delete_namespaced_config_map(self, *args, **kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == "cm-uid"
        self.deleted.append("cm")
        self.cm = None


def fake_api():
    clients = FakeClients()
    api = object.__new__(NativeBuildKubernetesApi)
    api._api = SimpleNamespace(sanitize_for_serialization=lambda item: item)
    api._core = api._batch = clients
    return api, clients


async def test_cleanup_waits_for_exact_job_and_orphan_pods_and_configmap_absence():
    api, clients = fake_api()
    cm = copy.deepcopy(clients.cm)
    assert not await api.delete("builds", "build", "job-uid", configmap=cm)
    observed = await api.observe("builds", "build")
    assert observed["job_missing"] and len(observed["pods"]) == 1
    assert not await api.delete("builds", "build", "job-uid", configmap=cm)
    assert not await api.delete("builds", "build", "job-uid", configmap=cm)
    assert await api.delete("builds", "build", "job-uid", configmap=cm)
    assert clients.deleted == ["job", "pod", "cm"]
    assert await api.observe("builds", "build") is None


async def test_cleanup_refuses_replaced_job_foreign_pod_and_foreign_configmap():
    api, clients = fake_api()
    cm = copy.deepcopy(clients.cm)
    with pytest.raises(ValueError, match="UID"):
        await api.delete("builds", "build", "old-uid", configmap=cm)
    clients.job = None
    with pytest.raises(ValueError, match="ownership"):
        await api.delete("builds", "build", None, configmap=cm)
    clients.pods = []
    clients.cm["data"] = {"claim.json": "foreign"}
    with pytest.raises(ValueError, match="ownership"):
        await api.delete("builds", "build", None, configmap=cm)
    assert clients.deleted == []


async def test_create_conflict_recovers_same_objects_but_rejects_changed_configuration():
    api, clients = fake_api()
    cm, job = copy.deepcopy(clients.cm), copy.deepcopy(clients.job)
    assert (await api.ensure(cm, job))["metadata"]["uid"] == "job-uid"
    clients.cm["data"]["claim.json"] = "changed"
    with pytest.raises(ValueError, match="configuration"):
        await api.ensure(cm, job)
