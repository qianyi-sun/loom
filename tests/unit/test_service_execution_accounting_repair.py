from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.db.schema import ServiceExecutionLease, Task, Trial
from loom.models.trajectory import TrialStartEvent
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane import service_execution_accounting_repair as repair
from tests.unit.test_service_execution_materialization import _task, _trial


@pytest.fixture
def case(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    team_id, trial_id, lease_id, artifact_id, authority = (uuid4() for _ in range(5))
    now = datetime.now(UTC)
    task_config = _task(agent={"name": "terminus-2", "version": "2.0.0"})
    trial_config = _trial().model_copy(update={"agent_name": "terminus-2"})
    trial = SimpleNamespace(id=trial_id, team_id=team_id, task_id="task-1", state="succeeded",
                            attempt_count=1, config=trial_config.model_dump(mode="json"),
                            trajectory_index={"trajectory_uri": "s3://trajectories/old/events.jsonl",
                                              "atif_uri": "s3://trajectories/old/atif.json"},
                            finished_at=now, result={"reward": 1.0})
    lease = SimpleNamespace(id=lease_id, trial_id=trial_id, team_id=team_id, attempt=1,
                            runtime_contract_json=None,
                            generation=1, output_generation=1, materialization_state="committed", output_commit_state="committed",
                            materialization_committed_at=now, source_retain_until=now,
                            canonical_trajectory_sha256="old", canonical_atif_sha256="old")
    store = FakeObjectStore()
    records = []
    for path in (*repair._PATHS, "result.json", "verifier/output.json"):
        body = json.dumps({"source": path}).encode()
        store.objects[("artifacts", "original/" + path)] = body
        records.append({"relative_path": path, "bucket": "artifacts", "key": "original/" + path,
                        "size_bytes": len(body), "sha256": "sha256:" + repair._digest(body),
                        "media_type": "application/json"})
    artifact = SimpleNamespace(id=artifact_id, trial_id=trial_id, team_id=team_id,
                               control_producer_kind="service_execution", control_producer_id=lease_id,
                               lifecycle_authority_id=authority, created_at=now, artifact_metadata={},
                               storage={"schema_version": "loom.canonical-trial-bundle-storage.v1",
                                        "attempt": 1, "files": records, "source_evidence": []})
    original_event = SimpleNamespace(seq=0, kind="trial_start", payload={"old": True},
                                     source="service-execution-materializer", lifecycle_authority_id=authority)
    data = SimpleNamespace(lease=lease, trial=trial, artifact=artifact, team_id=team_id, store=store,
                           events=[original_event], commits=0, enters=0, change_on_commit=None)
    event = TrialStartEvent(trial_id=trial_id, step_id="trial", seq=0, emitted_at=now,
                           task_id="task-1", agent_name="terminus-2", agent_mode="out-of-box")
    projector = AsyncMock()  # Projection itself has separate full ledger regression tests.
    monkeypatch.setattr(repair.ExecutionRuntimeResultV1, "model_validate_json", lambda body: object())
    monkeypatch.setattr(repair, "build_canonical_events", lambda **kwargs: (event,))
    monkeypatch.setattr(repair, "build_canonical_atif", lambda *args, **kwargs: b'{"schema_version":"harbor-tb2-v2-projection"}')
    monkeypatch.setattr(repair, "terminus_usage", lambda *args: {"call_count": 6, "totals": {"input_tokens": 13493, "output_tokens": 11888}})
    monkeypatch.setattr(repair, "read_service_execution_llm_calls", AsyncMock(return_value=[{"id": "length-call"}]))
    monkeypatch.setattr(repair, "register_lifecycle_object", projector)

    class Session:
        async def __aenter__(self):
            data.enters += 1
            if data.enters == 2 and data.change_on_commit:
                data.change_on_commit()
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, model, identity, **kwargs):
            return {ServiceExecutionLease: lease, Trial: trial,
                    Task: SimpleNamespace(config=task_config.model_dump(mode="json"), source=None,
                                          source_provenance={})}[model]

        async def scalar(self, query):
            return artifact

        async def scalars(self, query):
            return SimpleNamespace(all=lambda: list(data.events))

        async def execute(self, query):
            data.events.clear()

        def add(self, row):
            data.events.append(row)

        async def flush(self):
            pass

        async def commit(self):
            data.commits += 1

    # The commit lookup also requests Artifact, while scalar selects it initially.
    from loom.db.schema import Artifact
    original_get = Session.get

    async def get(self, model, identity, **kwargs):
        return artifact if model is Artifact else await original_get(self, model, identity, **kwargs)

    Session.get = get
    data.kwargs = {"session_factory": Session, "store": store, "artifacts_bucket": "artifacts",
                   "trajectories_bucket": "trajectories", "lease_id": lease_id, "team_id": team_id}
    data.register = projector
    return data


async def test_repair_publishes_separate_revision_and_is_idempotent(case: SimpleNamespace) -> None:
    original_objects = copy.deepcopy(case.store.objects)
    preserved = (case.trial.finished_at, case.trial.result, case.lease.materialization_committed_at,
                 case.lease.source_retain_until, case.lease.output_commit_state)
    assert (await repair.repair_accounting(**case.kwargs))["status"] == "prepared"
    assert case.store.objects == original_objects and case.commits == 0
    result = await repair.repair_accounting(**case.kwargs, apply=True)
    assert result["status"] == "corrected" and result["usage"]["call_count"] == 6
    assert case.commits == 1 and case.register.await_count == 5
    assert all(case.store.objects[key] == body for key, body in original_objects.items())
    assert len(case.store.objects) == len(original_objects) + 5
    assert "/accounting-v2/" in case.trial.trajectory_index["atif_uri"]
    assert {item["relative_path"] for item in case.artifact.storage["source_evidence"]} == {
        "source/trajectory/events.jsonl", "source/accounting/usage.json",
    }
    assert preserved == (case.trial.finished_at, case.trial.result, case.lease.materialization_committed_at,
                         case.lease.source_retain_until, case.lease.output_commit_state)
    assert case.trial.state == "succeeded" and case.trial.attempt_count == 1
    assert case.events[0].payload != {"old": True}
    count = len(case.store.objects)
    assert (await repair.repair_accounting(**case.kwargs, apply=True))["status"] == "already_corrected"
    assert len(case.store.objects) == count and case.commits == 1


async def test_repair_fences_concurrent_pointer_change(case: SimpleNamespace) -> None:
    old_storage = copy.deepcopy(case.artifact.storage)
    case.change_on_commit = lambda: case.trial.trajectory_index.update({"concurrent": True})
    with pytest.raises(ValueError, match="repair input changed"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.commits == 0 and case.artifact.storage == old_storage
    assert case.events[0].payload == {"old": True}


@pytest.mark.parametrize("violation", ["team", "foreign_events", "running", "retry"])
async def test_repair_rejects_unowned_or_ineligible_trial(case: SimpleNamespace, violation: str) -> None:
    if violation == "team":
        case.kwargs["team_id"] = uuid4()
    elif violation == "foreign_events":
        case.events[0].source = "other-producer"
    elif violation == "running":
        case.trial.state = "running"
    else:
        case.trial.attempt_count = 2
    original_objects = copy.deepcopy(case.store.objects)
    with pytest.raises(ValueError):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.store.objects == original_objects and case.commits == 0


async def test_repair_source_corruption_never_updates_pointers(case: SimpleNamespace) -> None:
    case.store.objects[("artifacts", "original/result.json")] = b"corrupt"
    old_index = copy.deepcopy(case.trial.trajectory_index)
    with pytest.raises(ValueError, match="canonical file"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.trial.trajectory_index == old_index and case.commits == 0


async def test_corrected_download_assembles_with_original_source(case: SimpleNamespace) -> None:
    import io
    import tarfile

    from loom_service.delivery_export import (
        build_canonical_trial_bundle_archive,
        canonical_bundle_from_artifact,
    )

    case.artifact.manifest_sha256 = "sha256:" + "a" * 64
    case.artifact.content_hash = "sha256:" + "b" * 64
    await repair.repair_accounting(**case.kwargs, apply=True)
    bundle = canonical_bundle_from_artifact(case.artifact, trial=case.trial)
    assert bundle is not None

    def get_object(**kwargs):
        body = case.store.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}

    archive = build_canonical_trial_bundle_archive(client=SimpleNamespace(get_object=get_object), bundle=bundle)
    try:
        with tarfile.open(fileobj=archive.body, mode="r:gz") as tar:
            usage = json.load(tar.extractfile("files/accounting/usage.json"))
            assert usage["call_count"] == 6
            assert usage["totals"] == {"input_tokens": 13493, "output_tokens": 11888}
            assert json.load(tar.extractfile("source/accounting/usage.json")) == {"source": "accounting/usage.json"}
            assert json.load(tar.extractfile("files/accounting/gateway-calls.json"))["calls"] == [{"id": "length-call"}]
            assert json.load(tar.extractfile("files/trajectory/events.jsonl"))["trial_id"] == str(case.trial.id)
    finally:
        archive.body.close()


async def test_partial_object_write_failure_cleans_only_new_revision(case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    original_objects = copy.deepcopy(case.store.objects)
    write = case.store.put_object_with_metadata
    calls = 0

    async def fail_third(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("injected object outage")
        return await write(**kwargs)

    monkeypatch.setattr(case.store, "put_object_with_metadata", fail_third)
    with pytest.raises(RuntimeError, match="injected object outage"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.commits == 0 and case.store.objects == original_objects
