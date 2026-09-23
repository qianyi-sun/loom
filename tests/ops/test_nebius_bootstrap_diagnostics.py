"""Protected failed-bootstrap readback exports only fixed diagnostic fields."""

from __future__ import annotations

import json
from copy import deepcopy

from scripts.ops import nebius_management_preflight as preflight
from tests.ops.test_nebius_management_preflight import Cluster


class FailedBootstrap(Cluster):
    def __init__(self):
        super().__init__()
        self.job = "loom-platform-configure-123456789abc"
        self.pod = self.job + "-abcde"
        self.lists["pods"].append({
            "metadata": {"name": self.pod, "namespace": "loom-nebius-platform", "uid": "failed-pod",
                         "ownerReferences": [{"kind": "Job", "name": self.job, "uid": "job-uid", "controller": True}]},
            "spec": {"containers": [{"name": self.job, "command": [
                "python", "-m", "loom.nebius_platform_bootstrap", "configure",
            ]}]}, "status": {"phase": "Failed"},
        })
        self.job_uid = "job-uid"
        self.failed = True
        self.raw = 'private arbitrary output\n' + json.dumps({
            "phase": "configure", "error_type": "ConfigurationRequestError", "method": "POST",
            "route": "/admin/service-execution/catalog", "http_status": 409,
            "credential": "private-token", "reason": "private-error-message",
        })

    def get(self, kind, name, namespace):
        if kind == "job":
            self.calls.append(("get", kind, name, namespace))
            assert name == self.job
            return {"metadata": {"name": name, "namespace": namespace, "uid": self.job_uid},
                    "status": {"conditions": [{"type": "Failed" if self.failed else "Complete", "status": "True"}]}}
        return super().get(kind, name, namespace)

    def run(self, *args, **kwargs):
        if args[0] == "logs":
            self.calls.append(args)
            assert args[1].startswith(self.job + "-")
            assert args == ("logs", args[1], "-n", "loom-nebius-platform", "-c", self.job,
                            "--tail=50", "--limit-bytes=16384")
            return self.raw
        return super().run(*args, **kwargs)


def test_inspect_reads_exact_failed_bootstrap_and_projects_error_without_payload():
    cluster = FailedBootstrap()
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    failure = result["failed_bootstrap_jobs"][0]
    assert failure["pod_uid"] == "failed-pod"
    assert failure["job_uid"] == "job-uid"
    assert failure["diagnostic"] == {"phase": "configure", "error_type": "ConfigurationRequestError",
                                     "operation": "catalog", "method": "POST", "http_status": 409}
    assert "private-" not in json.dumps(result)
    assert all(command[0] in {"get", "config", "logs"} for command in cluster.calls)


def test_recreated_job_is_not_treated_as_owner_of_old_failed_pod():
    cluster = FailedBootstrap()
    cluster.job_uid = "replacement-job"
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["failed_bootstrap_jobs"] == []
    assert not any(command[0] == "logs" for command in cluster.calls)


def test_foreign_command_and_namespace_are_never_read():
    for mutation in ("namespace", "command"):
        cluster = FailedBootstrap()
        pod = cluster.lists["pods"][-1]
        if mutation == "namespace":
            pod["metadata"]["namespace"] = "other-owner"
        else:
            pod["spec"]["containers"][0]["command"] = ["private-task"]
        result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
        assert result["failed_bootstrap_jobs"] == []
        assert not any(command[0] == "logs" for command in cluster.calls)


def test_malformed_or_unrecognized_log_never_escapes_as_diagnostic():
    for raw in ("private-secret", "[private-json", json.dumps({"phase": "private", "error_type": "private"}),
                '[' * 4000 + ']' * 4000):
        cluster = FailedBootstrap()
        cluster.raw = raw
        result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
        assert result["failed_bootstrap_jobs"][0]["diagnostic"] == {"status": "unavailable"}
        assert "private" not in json.dumps(result)


def test_unknown_fields_are_not_a_covert_payload_export():
    cluster = FailedBootstrap()
    cluster.raw = json.dumps({"phase": "configure", "error_type": "private-type",
                              "method": "private-method", "http_status": "private-status",
                              "route": "/admin/private-token", "sqlstate": "private-state",
                              "reason": "private-reason"})
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["failed_bootstrap_jobs"][0]["diagnostic"] == {"phase": "configure", "error_type": "OtherError"}


def test_completed_job_and_missing_owner_uid_are_not_inspected():
    for mutation in ("completed", "missing_uid"):
        cluster = FailedBootstrap()
        if mutation == "completed":
            cluster.failed = False
        else:
            cluster.lists["pods"][-1]["metadata"]["ownerReferences"][0].pop("uid")
            cluster.job_uid = None
        result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
        assert result["failed_bootstrap_jobs"] == []
        assert not any(command[0] == "logs" for command in cluster.calls)


def test_retained_failures_have_bounded_log_calls():
    cluster = FailedBootstrap()
    for index in range(8):
        pod = deepcopy(cluster.lists["pods"][-1])
        pod["metadata"].update(name=cluster.job + f"-pod{index}", uid=f"pod-uid-{index}")
        cluster.lists["pods"].append(pod)
    result = preflight.inspect(cluster, namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert len(result["failed_bootstrap_jobs"]) == 3
    assert len([c for c in cluster.calls if c[0] == "logs"]) == 3


def test_log_read_failure_does_not_export_error_or_hide_inventory():
    class Unreadable(FailedBootstrap):
        def run(self, *args, **kwargs):
            if args[0] == "logs":
                raise RuntimeError("private-remote-diagnostic")
            return super().run(*args, **kwargs)

    result = preflight.inspect(Unreadable(), namespace="loom-nebius-platform", expected_cluster_id="mk8scluster-test")
    assert result["status"] == "observed"
    assert result["failed_bootstrap_jobs"][0]["diagnostic"] == {"status": "unavailable"}
    assert "private-" not in json.dumps(result)
