"""Protected evidence must refer to the installed subject and exact backup Job."""
from __future__ import annotations

import copy
import json
import ssl
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def evidence(installation):
    from scripts.ops.nebius_management_evidence import HTTPSManagementEvidenceAPI
    from scripts.ops.nebius_management_install import render_installation
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    rendered = render_installation(request)
    job = copy.deepcopy(rendered.files["85-backup-verify.yaml"][0])
    job["metadata"]["uid"] = str(uuid4())
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {
        "name": job["metadata"]["name"] + "-abcde", "namespace": binding.namespace, "uid": str(uuid4()),
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": job["metadata"]["name"],
                             "uid": job["metadata"]["uid"], "controller": True}],
        "labels": {"batch.kubernetes.io/controller-uid": job["metadata"]["uid"]},
    }, "spec": copy.deepcopy(job["spec"]["template"]["spec"]), "status": {"phase": "Succeeded"}}
    pod["status"]["containerStatuses"] = [{"name": "loom-platform-backup", "restartCount": 0,
                                             "state": {"terminated": {"exitCode": 0}}}]
    pod["status"]["initContainerStatuses"] = [{"name": "pg-dump", "restartCount": 0,
                                                 "state": {"terminated": {"exitCode": 0}}}]
    account = {"apiVersion": "v1", "kind": "ServiceAccount", "automountServiceAccountToken": False,
               "metadata": {"name": "loom-management-provisioner", "namespace": binding.namespace,
                            "uid": str(uuid4()), "labels": {"loom.nebius/management-installation": binding.installation_id}}}
    values = {"pod": pod, "job": job, "account": account, "calls": [], "fault": None}
    report = {"backup_key": binding.namespace + "/2026/09/24/180000-" + "a" * 12 + ".dump",
              "sha256": "a" * 64, "bytes": 12}

    def handler(req):
        values["calls"].append(req)
        path = req.url.path
        if path.endswith("/namespaces/kube-system"):
            return httpx.Response(200, json={"kind": "Namespace", "metadata": {
                "name": "kube-system", "uid": binding.kube_system_uid}})
        if path.endswith("/namespaces/" + binding.namespace):
            return httpx.Response(200, json={"kind": "Namespace", "metadata": {
                "name": binding.namespace, "uid": binding.namespace_uid,
                "labels": {"loom.nebius/management-installation": binding.installation_id,
                           "pod-security.kubernetes.io/enforce": "restricted"}}})
        if path.endswith("/serviceaccounts/loom-management-provisioner/token"):
            assert req.method == "POST"
            if values["fault"] == "lost_token":
                raise httpx.ReadTimeout("private token response")
            if values["fault"] == "replaced_account":
                values["account"]["metadata"]["uid"] = str(uuid4())
            return httpx.Response(201, json={"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                "status": {"token": "private-runtime-token", "expirationTimestamp":
                           (datetime.now(UTC) + timedelta(minutes=10)).isoformat()}})
        if path.endswith("/serviceaccounts/loom-management-provisioner"):
            return httpx.Response(200, json=values["account"])
        if path.endswith("/jobs/" + job["metadata"]["name"]):
            return httpx.Response(200, json=values["job"])
        if path.endswith("/pods"):
            assert req.url.params["labelSelector"] == "batch.kubernetes.io/controller-uid=" + job["metadata"]["uid"]
            return httpx.Response(200, json={"kind": "PodList", "metadata": {}, "items": [values["pod"]]})
        if path.endswith("/log"):
            assert req.url.params["container"] == "loom-platform-backup"
            if values["fault"] == "replaced_pod":
                values["pod"]["metadata"]["uid"] = str(uuid4())
            return httpx.Response(200, text=json.dumps(report) + "\n")
        if path.endswith("/pods/" + pod["metadata"]["name"]):
            return httpx.Response(200, json=values["pod"])
        raise AssertionError((req.method, str(req.url)))

    with HTTPSManagementEvidenceAPI(binding=binding, rendered=rendered, api_server="https://cluster.example.com",
                                    ssl_context=ssl.create_default_context(), token="operator-token") as api:
        api.client.close()
        api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(handler))
        yield api, values, report


def test_short_lived_token_is_for_exact_recorded_runtime_account(evidence):
    api, values, _ = evidence
    token = api.runtime_token(service_account_uid=values["account"]["metadata"]["uid"])
    assert token == "private-runtime-token"
    writes = [req for req in values["calls"] if req.method != "GET"]
    assert len(writes) == 1
    assert json.loads(writes[0].content) == {"apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest",
                                          "spec": {"audiences": [], "expirationSeconds": 600}}


@pytest.mark.parametrize("fault", ["lost_token", "replaced_account", "foreign_account"])
def test_runtime_token_does_not_retry_or_accept_account_replacement(evidence, fault):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, _ = evidence
    uid = values["account"]["metadata"]["uid"]
    values["fault"] = fault
    if fault == "foreign_account":
        values["account"]["metadata"]["labels"] = {}
    with pytest.raises(ManagementInstallError) as error:
        api.runtime_token(service_account_uid=uid)
    assert "private" not in str(error.value)
    writes = [req for req in values["calls"] if req.method != "GET"]
    assert len(writes) == (0 if fault == "foreign_account" else 1)


def test_backup_report_is_from_exact_completed_job_pod_and_uploader(evidence):
    api, values, report = evidence
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report
    assert all(req.method == "GET" for req in values["calls"])


def test_backup_pod_accepts_kubernetes_equivalent_resource_quantities(evidence):
    api, values, report = evidence
    values["pod"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "0.1"
    assert api.backup_report(job_uid=values["job"]["metadata"]["uid"]) == report


@pytest.mark.parametrize("fault", ["owner", "image", "failed", "replaced_pod", "restarted"])
def test_wrong_or_changed_backup_execution_never_supplies_object_proof(evidence, fault):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, values, _ = evidence
    values["fault"] = fault
    if fault == "owner":
        values["pod"]["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif fault == "image":
        values["pod"]["spec"]["containers"][0]["image"] = "foreign:latest"
    elif fault == "failed":
        values["job"]["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
    elif fault == "restarted":
        values["pod"]["status"]["containerStatuses"][0]["restartCount"] = 1
    with pytest.raises(ManagementInstallError):
        api.backup_report(job_uid=values["job"]["metadata"]["uid"])
    if fault != "replaced_pod":
        assert not any(req.url.path.endswith("/log") for req in values["calls"])
