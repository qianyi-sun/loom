from __future__ import annotations

import json

from loom_cli.rollout.shadow_reconcile import (
    DriftStatus,
    ResourceDrift,
    ShadowDriftReport,
    compute_drift,
)


def _deploy(name: str, *, image: str, replicas: int, namespace: str = "loom-staging") -> dict:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "replicas": replicas,
            "template": {"spec": {"containers": [{"name": name, "image": image}]}},
        },
    }


def _drift(report: ShadowDriftReport, name: str) -> ResourceDrift:
    return next(r for r in report.resources if r.name == name)


def test_desired_subset_of_live_is_in_sync() -> None:
    # Live may carry extra controller-set fields; desired ⊆ live means no change.
    desired = _deploy("loom-control-plane", image="loom:abc", replicas=2)
    live = _deploy("loom-control-plane", image="loom:abc", replicas=2)
    live["metadata"]["uid"] = "1234"
    live["metadata"]["resourceVersion"] = "9999"
    live["status"] = {"readyReplicas": 2}
    live["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"

    report = compute_drift([desired], [live], environment="staging", target="abc")

    assert report.in_sync is True
    assert _drift(report, "loom-control-plane").status is DriftStatus.IN_SYNC
    assert report.summary()["in-sync"] == 1


def test_modified_reports_the_changed_paths_only() -> None:
    desired = _deploy("loom-control-plane", image="loom:NEW", replicas=3)
    live = _deploy("loom-control-plane", image="loom:OLD", replicas=2)

    report = compute_drift([desired], [live], environment="staging", target="new")

    drift = _drift(report, "loom-control-plane")
    assert drift.status is DriftStatus.MODIFIED
    assert drift.changed_paths == (
        "spec.replicas",
        "spec.template.spec.containers[0].image",
    )
    assert report.in_sync is False


def test_absent_from_live_when_desired_object_missing() -> None:
    desired = _deploy("loom-minio", image="minio:1", replicas=1)
    report = compute_drift([desired], [], environment="staging", target="t")
    assert _drift(report, "loom-minio").status is DriftStatus.ABSENT_FROM_LIVE


def test_absent_from_desired_only_with_prune() -> None:
    live = _deploy("orphan", image="x:1", replicas=1)

    without = compute_drift([], [live], environment="staging", target="t")
    assert without.resources == ()

    withp = compute_drift([], [live], environment="staging", target="t", prune=True)
    assert _drift(withp, "orphan").status is DriftStatus.ABSENT_FROM_DESIRED


def test_volatile_and_status_paths_are_ignored() -> None:
    desired = _deploy("loom-service", image="loom:abc", replicas=1)
    live = _deploy("loom-service", image="loom:abc", replicas=1)
    live["metadata"]["generation"] = 7
    live["metadata"]["managedFields"] = [{"manager": "kubectl-set"}]
    live["status"] = {"observedGeneration": 7}
    # desired never declares these, so they must not surface as drift
    report = compute_drift([desired], [live], environment="staging", target="abc")
    assert _drift(report, "loom-service").status is DriftStatus.IN_SYNC


def test_report_never_contains_field_values_only_paths() -> None:
    # Secret-safety: a differing secret-bearing env value must not appear in the report.
    desired = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "loom-secrets", "namespace": "loom-staging"},
        "stringData": {"DB_PASSWORD": "desired-super-secret-value"},
    }
    live = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "loom-secrets", "namespace": "loom-staging"},
        "stringData": {"DB_PASSWORD": "live-other-secret-value"},
    }

    report = compute_drift([desired], [live], environment="staging", target="t")
    blob = json.dumps(report.to_dict())

    drift = _drift(report, "loom-secrets")
    assert drift.status is DriftStatus.MODIFIED
    assert drift.changed_paths == ("stringData.DB_PASSWORD",)
    assert "desired-super-secret-value" not in blob
    assert "live-other-secret-value" not in blob


def test_list_length_change_is_drift_at_the_list_path() -> None:
    desired = _deploy("d", image="i:1", replicas=1)
    desired["spec"]["template"]["spec"]["containers"].append({"name": "sidecar", "image": "s:1"})
    live = _deploy("d", image="i:1", replicas=1)

    drift = _drift(compute_drift([desired], [live], environment="s", target="t"), "d")
    assert drift.status is DriftStatus.MODIFIED
    assert drift.changed_paths == ("spec.template.spec.containers",)


def test_report_to_dict_shape_and_summary() -> None:
    desired = [
        _deploy("a", image="i:1", replicas=1),
        _deploy("b", image="i:NEW", replicas=1),
        _deploy("c", image="i:1", replicas=1),
    ]
    live = [
        _deploy("a", image="i:1", replicas=1),  # in-sync
        _deploy("b", image="i:OLD", replicas=1),  # modified
        # c absent from live
    ]
    report = compute_drift(desired, live, environment="staging", target="rev1")
    doc = report.to_dict()

    assert doc["schema_version"] == 1
    assert doc["environment"] == "staging"
    assert doc["target"] == "rev1"
    assert doc["in_sync"] is False
    assert doc["summary"] == {
        "in-sync": 1,
        "absent-from-live": 1,
        "modified": 1,
        "absent-from-desired": 0,
    }
    assert len(doc["resources"]) == 3
    # resources are ordered by (kind, namespace, name) for stable output
    assert [r["name"] for r in doc["resources"]] == ["a", "b", "c"]
