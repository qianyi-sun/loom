"""Migration cleanup must stay within the exact recorded Kubernetes resources."""

import copy
import json

import pytest
import yaml

from loom_cli.cluster_migration import render_migration_manifest
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


class Runner:
    def __init__(self):
        self.environment = {"KUBECONFIG": "/var/lib/loom-staging-rollout/kubeconfig"}
        self.objects = {}
        self.pods = []
        self.events = []
        self.lose_create = False

    def capture_stdout(self, argv, **kwargs):
        if "pods" in argv:
            return json.dumps({"items": self.pods}).encode()
        kind = "Job" if "job" in argv else "Secret"
        return json.dumps(self.objects[kind]).encode() if kind in self.objects else b""

    def run_checked(self, argv, *, input_payload, **kwargs):
        if "create" in argv:
            document = json.loads(input_payload)
            kind = document["kind"]
            assert kind not in self.objects
            document["metadata"].update(uid=("11111111-1111-4111-8111-111111111111" if kind == "Job" else "22222222-2222-4222-8222-222222222222"), resourceVersion="10")
            self.objects[kind] = document
            self.events.append(("create", kind))
            if self.lose_create:
                self.lose_create = False
                raise RuntimeError("creation reply lost")
        elif "delete" in argv:
            kind = "Job" if "/jobs/" in argv[-1] else "Secret"
            options = json.loads(input_payload)
            assert options["preconditions"] == {"uid": self.objects[kind]["metadata"]["uid"], "resourceVersion": "10"}
            assert options["propagationPolicy"] == "Foreground"
            del self.objects[kind]
            self.events.append(("delete", kind))
        else:
            pytest.fail("unexpected resource mutation")


def _resources(tmp_path):
    from loom_cli.rollout.operator.protected_application_migration_resources import (
        ProtectedApplicationMigrationResources,
    )

    guard = _guard(_plan(tmp_path))
    job = yaml.safe_load(render_migration_manifest(image_tag="staging-aaaaaaa", namespace="loom-staging",
        job_suffix="test", application_owner_role="loom_app_staging_owner", container_registry="registry.example",
        registry_digest="sha256:" + "a" * 64))
    annotation = {"loom.carin.dev/migration-generation": "f" * 64,
        "loom.carin.dev/request-id": guard.request_id, "loom.carin.dev/candidate-sha": guard.candidate_sha,
        "loom.carin.dev/candidate-tree": guard.candidate_tree}
    job["metadata"]["annotations"] = annotation
    name = job["spec"]["template"]["spec"]["containers"][0]["env"][0]["valueFrom"]["secretKeyRef"]["name"]
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"namespace": "loom-staging", "name": name,
        "annotations": annotation}, "type": "Opaque", "immutable": True, "data": {"db-url": "dGVzdA==", "ca.crt": "Y2VydA=="}}
    runner = Runner()
    return ProtectedApplicationMigrationResources(runner=runner, job=job, secret=secret,
        guard=guard, assert_guard=lambda: guard), runner


@pytest.mark.parametrize("lost_reply", [False, True])
def test_lost_create_reply_reuses_only_bound_resources_and_deletes_with_preconditions(tmp_path, lost_reply):
    resources, runner = _resources(tmp_path)
    runner.lose_create = lost_reply
    if lost_reply:
        with pytest.raises(RuntimeError, match="reply lost"):
            resources.ensure_secret(creation_dispatched=True)
    secret = resources.ensure_secret(creation_dispatched=True)
    assert resources.ensure_secret(creation_dispatched=True, expected_uid=secret.uid) == secret
    job = resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
    runner.objects["Job"]["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    assert resources.job_complete(expected_uid=job.uid)
    resources.delete_job(expected_uid=job.uid)
    resources.delete_secret(expected_uid=secret.uid)
    resources.delete_job(expected_uid=job.uid)
    resources.delete_secret(expected_uid=secret.uid)
    assert runner.events == [("create", "Secret"), ("create", "Job"), ("delete", "Job"), ("delete", "Secret")]


@pytest.mark.parametrize("drift", ["unrecorded", "uid", "body", "foreign-consumer", "guard"])
def test_migration_resource_drift_never_deletes_or_overwrites_unrelated_objects(tmp_path, drift):
    resources, runner = _resources(tmp_path)
    secret = resources.ensure_secret(creation_dispatched=True)
    before = copy.deepcopy(runner.events)
    if drift == "uid":
        runner.objects["Secret"]["metadata"]["uid"] = "33333333-3333-4333-8333-333333333333"
    elif drift == "body":
        runner.objects["Secret"]["data"]["db-url"] = "Y2hhbmdlZA=="
    elif drift == "foreign-consumer":
        runner.pods = [{"metadata": {"name": "foreign"}, "spec": {"containers": [{"envFrom": [{"secretRef": {"name": resources.secret["metadata"]["name"]}}]}]}}]
    elif drift == "guard":
        object.__setattr__(resources, "assert_guard", lambda: None)
    with pytest.raises((RuntimeError, ValueError)):
        if drift == "unrecorded":
            resources.ensure_secret(creation_dispatched=False)
        else:
            resources.delete_secret(expected_uid=secret.uid)
    assert runner.events == before and "Secret" in runner.objects


def test_job_creation_requires_the_recorded_secret_uid(tmp_path):
    resources, runner = _resources(tmp_path)
    secret = resources.ensure_secret(creation_dispatched=True)
    runner.objects["Secret"]["metadata"]["uid"] = "33333333-3333-4333-8333-333333333333"
    with pytest.raises(RuntimeError, match="drifted"):
        resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
    assert runner.events == [("create", "Secret")]


def test_waiting_foreign_pod_cannot_receive_a_new_migration_credential(tmp_path):
    resources, runner = _resources(tmp_path)
    runner.pods = [{"metadata": {"name": "foreign"}, "spec": {"containers": [{"envFrom": [{"secretRef": {
        "name": resources.secret["metadata"]["name"]}}]}]}}]
    with pytest.raises(RuntimeError, match="consumer"):
        resources.ensure_secret(creation_dispatched=True)
    assert runner.events == []


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_cleanup_observes_lost_creation_acknowledgement_without_creating(tmp_path, kind):
    resources, runner = _resources(tmp_path)
    assert resources.observe_secret() is None
    assert resources.observe_job() is None
    assert runner.events == []
    runner.lose_create = kind == "Secret"
    if kind == "Secret":
        with pytest.raises(RuntimeError, match="reply lost"):
            resources.ensure_secret(creation_dispatched=True)
    else:
        secret = resources.ensure_secret(creation_dispatched=True)
        runner.lose_create = True
        with pytest.raises(RuntimeError, match="reply lost"):
            resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
    observed = resources.observe_secret() if kind == "Secret" else resources.observe_job()
    before = list(runner.events)
    assert observed is not None
    assert runner.events == before
    if kind == "Job":
        resources.delete_job(expected_uid=observed.uid)
    else:
        resources.delete_secret(expected_uid=observed.uid)
    assert (resources.observe_secret() if kind == "Secret" else resources.observe_job()) is None
    assert len(runner.events) == len(before) + 1
