from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from scripts.ops import nebius_image_retention as retention

NOW = datetime(2026, 9, 21, tzinfo=UTC)
PREFIX = "cr.eu-north1.nebius.cloud/testregistry"
TASK_REPO = PREFIX + "/tasks"


def image(number, *, task=False, age=40, tags=None):
    return {"id": f"artifact-{number}", "name": "testregistry/" + ("tasks" if task else "loom-service"),
            "digest": f"sha256:{number:064x}", "size": "100", "status": "ACTIVE", "type": "MANIFEST",
            "tags": tags if tags is not None else [f"{'b' * 64}-1-0" if task else f"candidate-{number:040x}"],
            "created_at": (NOW - timedelta(days=age)).isoformat(),
            "updated_at": (NOW - timedelta(days=age)).isoformat()}


def image_ref(row):
    return retention.ref(row, PREFIX.split("/")[0])


def materialization(row, *, identity="one", referenced=False, age=40):
    return {"id": identity, "key": "b" * 64, "references": [image_ref(row)], "state": "ready",
            "referenced": referenced, "legacy_publication": False, "lease_expires_at": None,
            "unreferenced_at": (NOW - timedelta(days=age)).isoformat() if age is not None else None}


def test_plan_protects_runtime_catalog_rollback_tags_and_recent_publications():
    images = [image(n, age=40 + n) for n in range(1, 8)]
    images += [image(8, age=2), image(9, tags=["candidate-" + "c" * 40, "keep-me"])]
    database = {"protected": [image_ref(images[3])], "materializations": []}
    live = {PREFIX + "/loom-service:" + images[4]["tags"][0]}
    result = retention.plan(images, database, live, prefix=PREFIX, task_repository=TASK_REPO, now=NOW, keep=3)
    reasons = {row["id"]: row["reason"] for row in result["images"]}
    assert reasons["artifact-4"] == reasons["artifact-5"] == "referenced"
    assert reasons["artifact-1"] == "rollback_window"
    assert reasons["artifact-8"] == "rollback_window"
    assert reasons["artifact-9"] == "unmanaged"
    assert reasons["artifact-6"] == "release_expired"


def test_task_age_is_not_a_substitute_for_observed_absence_of_references():
    images = [image(n, task=True, age=90) for n in range(1, 5)]
    rows = [materialization(images[0], identity="unobserved", age=None),
            materialization(images[1], identity="recent", age=1),
            materialization(images[2], identity="used", referenced=True),
            materialization(images[3], identity="eligible")]
    result = retention.plan(images, {"protected": [], "materializations": rows}, set(),
                            prefix=PREFIX, task_repository=TASK_REPO, now=NOW)
    assert result["task_retirement_candidates"] == ["eligible"]
    assert result["counts"] == {"task_tracked": 3, "task_retirement_claim_required": 1}


def setup_maintenance(monkeypatch, tmp_path, *, apply=True, shared=False, fail_delete=False, readded=False):
    img = image(1, task=True, age=1000)
    rows = [materialization(img)]
    if shared:
        rows.append(materialization(img, identity="other", referenced=True))
    database = {"protected": [], "materializations": rows}
    events = []
    claimed = False
    class Registry:
        deleted = False
        def call(self, action, image_id=None):
            events.append(action)
            if action == "list":
                return {"items": [] if self.deleted else [img]}
            if action == "get":
                return img
            if fail_delete:
                raise RuntimeError("provider failure")
            self.deleted = True
            return {}
    config = {"task_image_builder": {"registry_repository": TASK_REPO}}
    monkeypatch.setattr(retention, "read_live", lambda *a: (config, set()))
    monkeypatch.setattr(retention, "verify_cluster_identity", lambda *a: None)
    def guard(kube, namespace, action, *args):
        events.append(action)
        return {"status": "acquired" if action == "acquire" else "released"}
    monkeypatch.setattr(retention, "rollout_guard", guard)
    def database_call(kube, namespace, payload):
        nonlocal claimed
        events.append(payload["action"])
        if payload["action"] == "snapshot":
            if claimed and readded:
                rows[0]["referenced"] = True
            return database
        if payload["action"] == "claim":
            if claimed:
                return {"claim": None}
            claimed = True
            return {"claim": {"id": "one", "lease_epoch": 1, "references": [image_ref(img)]}}
        return {"state": "queued" if readded else "retired"}
    monkeypatch.setattr(retention, "db", database_call)
    kube = SimpleNamespace(get=lambda *a: {"data": {"profile.json": '{"candidate_sha":"abc"}'}})
    args = SimpleNamespace(namespace="loom", expected_cluster_id="cluster", registry_prefix=PREFIX,
                           days=30, keep=3, max_delete=20, apply=apply, output=tmp_path / "report.json")
    return args, kube, Registry(), events


def test_preview_has_no_claim_guard_or_delete(monkeypatch, tmp_path):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path, apply=False)
    assert retention.maintain(args, kube, registry)["status"] == "preview"
    assert events == ["list", "snapshot"]


@pytest.mark.parametrize("shared,readded", [(False, False), (True, False), (False, True)])
def test_task_retirement_protects_shared_digest_and_readmission(monkeypatch, tmp_path, shared, readded):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path, shared=shared, readded=readded)
    result = retention.maintain(args, kube, registry)
    assert result["status"] == "complete"
    assert ("delete" in events) == (not shared and not readded)
    assert events.index("acquire") < events.index("claim") < events.index("complete") < events.index("release")
    if "delete" in events:
        assert events.index("delete") < events.index("complete")


def test_provider_failure_preserves_retirement_claim_and_releases_own_guard(monkeypatch, tmp_path):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path, fail_delete=True)
    with pytest.raises(RuntimeError, match="provider failure"):
        retention.maintain(args, kube, registry)
    assert "complete" not in events
    assert events[-1] == "release"


def test_busy_platform_is_not_mutated(monkeypatch, tmp_path):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path)
    monkeypatch.setattr(retention, "rollout_guard", lambda *a: {"status": "skipped_busy"})
    assert retention.maintain(args, kube, registry)["status"] == "skipped_busy"
    assert events == ["list", "snapshot"]


def test_added_foreign_tag_stops_deletion_using_fresh_list_not_tagless_get(monkeypatch, tmp_path):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path)
    original = registry.call
    lists = 0
    def retag(action, image_id=None):
        nonlocal lists
        result = original(action, image_id)
        if action == "list":
            lists += 1
            if lists == 3:
                result = {"items": [{**result["items"][0], "tags": ["keep-me"]}]}
        return result
    registry.call = retag
    with pytest.raises(ValueError, match="changed during maintenance"):
        retention.maintain(args, kube, registry)
    assert "delete" not in events and "complete" not in events
    assert events[-1] == "release"


def test_deletion_readback_failure_does_not_acknowledge_retirement(monkeypatch, tmp_path):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path)
    original = registry.call
    def incomplete_delete(action, image_id=None):
        result = original(action, image_id)
        if action == "delete":
            registry.deleted = False
        return result
    registry.call = incomplete_delete
    with pytest.raises(ValueError, match="remains visible"):
        retention.maintain(args, kube, registry)
    assert "complete" not in events and events[-1] == "release"


def test_unknown_registry_operation_cannot_fall_through_to_delete(monkeypatch):
    monkeypatch.setattr(retention.subprocess, "run", lambda *a, **kw: pytest.fail("unexpected registry call"))
    with pytest.raises(ValueError, match="unsupported"):
        retention.Registry("registry-test").call("get", "artifact-test")


def test_deletion_limit_preserves_unfinished_claim_without_reporting_provider_failure(monkeypatch, tmp_path):
    args, kube, registry, events = setup_maintenance(monkeypatch, tmp_path)
    args.max_delete = 1
    second = image(2, task=True, tags=["b" * 64 + "-1-1"])
    original_call, original_db = registry.call, retention.db
    def call(action, image_id=None):
        result = original_call(action, image_id)
        if action == "list":
            result["items"].append(second)
        return result
    def database(*arguments):
        result = original_db(*arguments)
        if arguments[-1]["action"] == "snapshot":
            refs = result["materializations"][0]["references"]
            if image_ref(second) not in refs:
                refs.append(image_ref(second))
        elif arguments[-1]["action"] == "claim" and result["claim"]:
            result["claim"]["references"].append(image_ref(second))
        return result
    registry.call = call
    monkeypatch.setattr(retention, "db", database)
    result = retention.maintain(args, kube, registry)
    assert result["status"] == "bounded" and result["pending_retirement"] == "one"
    assert result["deleted"] == ["artifact-1"]
    assert "complete" not in events and events[-1] == "release"
