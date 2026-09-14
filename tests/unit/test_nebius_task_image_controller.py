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
from loom_execution_actuator.task_image_renderer import render_task_image_job
from tests.unit.test_nebius_task_image_renderer import inputs  # noqa: F401


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


def canonical_api_job(job, *, null_defaults=False):
    """Captured API transformations from the first real native build."""
    observed = copy.deepcopy(job)
    pod = observed["spec"]["template"]["spec"]
    for key in ("hostIPC", "hostPID", "hostNetwork"):
        if null_defaults:
            pod[key] = None
        else:
            pod.pop(key)
    for container in pod["initContainers"] + pod["containers"]:
        for mount in container["volumeMounts"]:
            if mount.get("readOnly") is False:
                if null_defaults:
                    mount["readOnly"] = None
                else:
                    mount.pop("readOnly")
    for volume in pod["volumes"]:
        if "emptyDir" in volume:
            size = volume["emptyDir"]["sizeLimit"]
            volume["emptyDir"]["sizeLimit"] = {"7168Mi": "7Gi", "4096Mi": "4Gi"}.get(size, size)
    return observed


@pytest.mark.parametrize("null_defaults", [False, True])
def test_full_native_job_accepts_kubernetes_omitted_defaults_and_emptydir_units(request, null_defaults):
    _, job = render_task_image_job(**request.getfixturevalue("inputs"))
    observed = canonical_api_job(job, null_defaults=null_defaults)
    assert observed != job
    assert _matches(observed, job)


@pytest.mark.parametrize("field", ["hostIPC", "hostPID", "hostNetwork", "volume_readonly", "storage", "publisher_readonly", "automount"])
def test_normalized_native_job_still_rejects_non_equivalent_security_and_resources(request, field):
    _, job = render_task_image_job(**request.getfixturevalue("inputs"))
    observed = canonical_api_job(job)
    pod = observed["spec"]["template"]["spec"]
    if field in {"hostIPC", "hostPID", "hostNetwork"}:
        pod[field] = True
    elif field == "volume_readonly":
        pod["initContainers"][0]["volumeMounts"][1]["readOnly"] = True
    elif field == "publisher_readonly":
        pod["containers"][0]["volumeMounts"][1].pop("readOnly")
    elif field == "automount":
        pod.pop("automountServiceAccountToken")  # Unlike host*, this defaults true.
    else:
        pod["volumes"][1]["emptyDir"]["sizeLimit"] = "8Gi"
    assert not _matches(observed, job)


def test_boolean_and_quantity_defaults_are_scoped_to_kubernetes_fields():
    assert not _matches({}, {"readOnly": False})
    assert not _matches({"other": {}}, {"other": {"hostIPC": False}})
    assert not _matches({"data": {"sizeLimit": "7Gi"}}, {"data": {"sizeLimit": "7168Mi"}})


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


async def test_create_conflict_recovers_api_normalized_native_job(request):
    api, clients = fake_api()
    cm, job = render_task_image_job(**request.getfixturevalue("inputs"))
    clients.cm = {**copy.deepcopy(cm), "metadata": {**cm["metadata"], "uid": "cm-uid"}}
    clients.job = canonical_api_job(job)
    clients.job["metadata"]["uid"] = "job-uid"
    assert (await api.ensure(cm, job))["metadata"]["uid"] == "job-uid"
    clients.job["spec"]["template"]["spec"]["volumes"][1]["emptyDir"]["sizeLimit"] = "8Gi"
    with pytest.raises(ValueError, match="differs"):
        await api.ensure(cm, job)
