"""Saved workload identities must survive partial shutdown and recovery."""

import copy
from uuid import uuid4

import pytest


def _workload(kind="Deployment", name="loom-control-plane", value=2):
    spec = {"replicas": value, "selector": {"matchLabels": {"app": name}},
            "template": {"metadata": {"labels": {"app": name}}, "spec": {
                "containers": [{"name": "app", "image": "test@sha256:" + "a" * 64}],
            }}}
    if kind == "CronJob":
        spec = {"schedule": "*/5 * * * *", "suspend": value,
                "jobTemplate": {"spec": {"template": spec["template"]}}}
    if kind == "Job":
        spec = {"suspend": value, "template": spec["template"]}
    return {"apiVersion": "apps/v1" if kind == "Deployment" else "batch/v1", "kind": kind,
            "metadata": {"name": name, "namespace": "loom-staging", "uid": str(uuid4()),
                         "resourceVersion": "10", "generation": 1}, "spec": spec}


def _patched(value, patch):
    result = copy.deepcopy(value)
    for operation in patch:
        parts = operation["path"].strip("/").split("/")
        parent = result
        for part in parts[:-1]:
            parent = parent[part]
        key = parts[-1]
        if operation["op"] == "test":
            assert parent[key] == operation["value"], "compare-and-swap failed"
        elif operation["op"] == "remove":
            del parent[key]
        else:
            parent[key] = operation["value"]
    result["metadata"]["resourceVersion"] = str(int(result["metadata"]["resourceVersion"]) + 1)
    result["metadata"]["generation"] += 1
    return result


@pytest.mark.parametrize("kind,name,value", [
    ("Deployment", "loom-control-plane", 2),
    ("Deployment", "loom-pipeline-orchestrator", 0),
    ("CronJob", "loom-staging-data-lifecycle", False),
    ("Job", "loom-staging-data-lifecycle-123", False),
])
def test_saved_workload_round_trip_and_partial_patch_replay(kind, name, value):
    from loom_cli.rollout.operator.protected_application_workloads import ApplicationWorkload

    original = _workload(kind, name, value)
    saved = ApplicationWorkload.capture(original)
    assert ApplicationWorkload.from_dict(saved.to_dict()) == saved
    patch = saved.patch(original, recovering=False)
    paused = _patched(original, patch) if patch else original
    assert saved.patch(paused, recovering=False) is None
    restored_patch = saved.patch(paused, recovering=True)
    restored = _patched(paused, restored_patch) if restored_patch else paused
    assert restored["spec"] == original["spec"]
    assert restored["metadata"]["uid"] == original["metadata"]["uid"]
    assert saved.patch(restored, recovering=True) is None
    assert original["spec"] == _workload(kind, name, value)["spec"]


def test_workload_restore_preserves_an_omitted_suspend_field():
    from loom_cli.rollout.operator.protected_application_workloads import ApplicationWorkload

    original = _workload("CronJob", "loom-staging-data-lifecycle", False)
    del original["spec"]["suspend"]
    saved = ApplicationWorkload.capture(original)
    paused = _patched(original, saved.patch(original, recovering=False))
    restored = _patched(paused, saved.patch(paused, recovering=True))
    assert "suspend" not in restored["spec"]


@pytest.mark.parametrize("drift", ["uid", "template", "owner", "replicas", "namespace", "deleting"])
def test_workload_patch_never_adopts_a_replacement_or_changed_spec(drift):
    from loom_cli.rollout.operator.protected_application_workloads import ApplicationWorkload

    original = _workload()
    saved = ApplicationWorkload.capture(original)
    current = copy.deepcopy(original)
    if drift == "uid":
        current["metadata"]["uid"] = str(uuid4())
    elif drift == "template":
        current["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign"
    elif drift == "owner":
        current["metadata"]["ownerReferences"] = [{"kind": "Example", "uid": str(uuid4())}]
    elif drift == "replicas":
        current["spec"]["replicas"] = 3
    elif drift == "namespace":
        current["metadata"]["namespace"] = "other"
    else:
        current["metadata"]["deletionTimestamp"] = "2026-09-11T00:00:00Z"
    for recovering in (False, True):
        with pytest.raises(ValueError):
            saved.patch(current, recovering=recovering)


@pytest.mark.parametrize("kind,name", [("Deployment", "foreign"), ("StatefulSet", "loom-postgres"),
                                         ("CronJob", "foreign"), ("Job", "foreign")])
def test_only_the_fixed_application_writer_set_can_be_saved(kind, name):
    from loom_cli.rollout.operator.protected_application_workloads import ApplicationWorkload

    with pytest.raises(ValueError):
        ApplicationWorkload.capture(_workload(kind, name, False if kind != "Deployment" else 1))


def test_workload_patch_contains_uid_version_and_full_spec_preconditions():
    from loom_cli.rollout.operator.protected_application_workloads import ApplicationWorkload

    original = _workload()
    saved = ApplicationWorkload.capture(original)
    patch = saved.patch(original, recovering=False)
    for target in ("uid", "resourceVersion"):
        assert {"op": "test", "path": f"/metadata/{target}", "value": original["metadata"][target]} in patch
    assert {"op": "test", "path": "/spec", "value": original["spec"]} in patch
    raced = copy.deepcopy(original)
    raced["metadata"]["resourceVersion"] = "11"
    with pytest.raises(AssertionError, match="compare-and-swap"):
        _patched(raced, patch)
