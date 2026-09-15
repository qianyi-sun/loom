"""Protected output publication preserves artifacts and lifecycle evidence after freeze."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_worker_session import (
    _seed_bearer,
    _seed_claimed_protected_trial,
)
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.parametrize("frozen", [False, True])
def test_protected_output_publishes_artifact_and_lifecycle_atomically(
    capacity_guard_database, monkeypatch, tmp_path, frozen,
):
    database = capacity_guard_database
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-staging")
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    bearer = _seed_bearer(database, raw="output-worker-bearer", token_type="worker",
                          scopes=["worker:index"])
    headers = {**seeded.claim_headers, "Authorization": f"Bearer {bearer}"}
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    if frozen:
        asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    payload = {
        "worker_id": str(seeded.worker.worker.worker_id),
        "result": {"state": "succeeded", "reward": 0},
        "artifacts": [{"key": f"outputs/{seeded.trial_id}/answer.json",
                       "content_hash": "sha256:" + "a" * 64,
                       "size_bytes": 42, "version_id": "version-1"}],
    }
    errors = []
    with TestClient(seeded.app, raise_server_exceptions=False) as client:
        def observe_error(context):
            original = context.original_exception
            errors.append((getattr(original, "sqlstate", None),
                           getattr(getattr(original, "diag", None), "message_primary", None)))

        event.listen(seeded.app.state.session_factory.kw["bind"].sync_engine,
                     "handle_error", observe_error)
        for _ in range(2):
            response = client.patch(f"/trials/{seeded.trial_id}/trajectory_index",
                                    headers=headers, json=payload)
            assert response.status_code == 200, (response.text, errors)
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            trial = connection.execute(text(
                "SELECT result, trajectory_index FROM public.trials WHERE id = :id"
            ), {"id": seeded.trial_id}).mappings().one()
            assert trial["result"] == payload["result"]
            assert trial["trajectory_index"] == {"artifacts": payload["artifacts"]}
            artifact = connection.execute(text(
                "SELECT a.id, a.content_hash, a.storage, authority.owner_id, "
                "object.content_sha256, object.size_bytes, object.version_id "
                "FROM public.artifacts a JOIN public.data_lifecycle_authorities authority "
                "ON authority.id = a.lifecycle_authority_id "
                "JOIN public.data_lifecycle_objects object ON object.authority_id = authority.id "
                "WHERE a.trial_id = :id"
            ), {"id": seeded.trial_id}).mappings().one()
            assert artifact["owner_id"] == str(artifact["id"])
            assert artifact["content_sha256"] == "a" * 64
            assert artifact["size_bytes"] == 42
            assert artifact["version_id"] == "version-1"
    finally:
        engine.dispose()


@pytest.mark.parametrize("interference", [
    "credential", "extra_field", "wrong_owner", "stale_snapshot", "bad_sha", "scope",
    "suppressed_trial", "extra_trial_column", "suppressed_artifact", "suppressed_object",
    "suppressed_authority", "corrupt_artifact", "extra_artifact_column",
])
def test_frozen_output_rejection_rolls_back_every_projection(
    capacity_guard_database, monkeypatch, tmp_path, interference,
):
    from loom_control_plane.protected_worker_session import ProtectedWorkerSessionStore

    database = capacity_guard_database
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-staging")
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    bearer = _seed_bearer(database, raw="output-worker-bearer", token_type="worker", scopes=["worker:index"])
    headers = {**seeded.claim_headers, "Authorization": f"Bearer {bearer}"}
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            before = connection.execute(text("SELECT to_jsonb(trial) FROM public.trials trial WHERE id = :id"),
                                        {"id": seeded.trial_id}).scalar_one()
            authority_count = connection.execute(text("SELECT count(*) FROM public.data_lifecycle_authorities")).scalar_one()
            trigger = {
                "suppressed_trial": ("trials", "UPDATE", "RETURN NULL;"),
                "extra_trial_column": ("trials", "UPDATE", "NEW.config := NEW.config || '{\"unauthorized\":true}'::jsonb; RETURN NEW;"),
                "suppressed_artifact": ("artifacts", "INSERT", "RETURN NULL;"),
                "corrupt_artifact": ("artifacts", "INSERT", "NEW.name := 'corrupt'; RETURN NEW;"),
                "extra_artifact_column": ("artifacts", "INSERT", "NEW.access_class := 'authoring_restricted'; RETURN NEW;"),
                "suppressed_object": ("data_lifecycle_objects", "INSERT", "RETURN NULL;"),
                "suppressed_authority": ("data_lifecycle_authorities", "INSERT", "RETURN NULL;"),
            }.get(interference)
            if trigger is not None:
                table, operation, body = trigger
                connection.exec_driver_sql(
                    "CREATE FUNCTION public.output_interference() RETURNS trigger LANGUAGE plpgsql "
                    "AS $test$ BEGIN " + body + " END $test$; "
                    f"CREATE TRIGGER output_interference BEFORE {operation} ON public.{table} "
                    "FOR EACH ROW EXECUTE FUNCTION public.output_interference();"
                )
        original = ProtectedWorkerSessionStore.publish_trial_output

        async def intercepted(self, **kwargs):
            report = kwargs["report"]
            if interference == "credential":
                kwargs["worker_credential"] = "revoked-or-wrong-credential"
            elif interference == "extra_field":
                report["unapproved"] = True
            elif interference == "wrong_owner":
                report["artifacts"][0]["team_id"] = str(uuid4())
            elif interference == "stale_snapshot":
                report["expected"]["trial"]["visibility"] = "stale"
            elif interference == "bad_sha":
                report["artifacts"][0]["content_hash"] = "sha256:invalid"
            elif interference == "scope":
                report["scope"]["namespace"] = "another-namespace"
            return await original(self, **kwargs)

        monkeypatch.setattr(ProtectedWorkerSessionStore, "publish_trial_output", intercepted)
        initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
        asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
        with TestClient(seeded.app, raise_server_exceptions=False) as client:
            response = client.patch(f"/trials/{seeded.trial_id}/trajectory_index", headers=headers, json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "result": {"state": "succeeded", "reward": 0},
                "artifacts": [{"key": f"outputs/{seeded.trial_id}/answer.json", "version_id": "v1",
                               "content_hash": "sha256:" + "a" * 64, "size_bytes": 42}],
            })
        assert response.status_code == (401 if interference == "credential" else 409), response.text
        with engine.connect() as connection:
            assert connection.execute(text("SELECT to_jsonb(trial) FROM public.trials trial WHERE id = :id"),
                                      {"id": seeded.trial_id}).scalar_one() == before
            for table in ("artifacts", "data_lifecycle_objects", "artifact_lineage_edges"):
                assert connection.exec_driver_sql(f"SELECT count(*) FROM public.{table}").scalar_one() == 0
            assert connection.execute(text("SELECT count(*) FROM public.data_lifecycle_authorities")).scalar_one() == authority_count
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")).scalar_one() == 0
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")).scalar_one() == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("case", ["version_conflict", "lineage", "suppress_lineage"])
def test_frozen_output_preserves_prior_version_and_synchronizes_lineage(
    capacity_guard_database, monkeypatch, tmp_path, case,
):
    import json

    database = capacity_guard_database
    monkeypatch.setenv("LOOM_ENV", "staging")
    monkeypatch.setenv("LOOM_NAMESPACE", "loom-staging")
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    bearer = _seed_bearer(database, raw="output-worker-bearer", token_type="worker", scopes=["worker:index"])
    headers = {**seeded.claim_headers, "Authorization": f"Bearer {bearer}"}
    payload = {"worker_id": str(seeded.worker.worker.worker_id),
               "artifacts": [{"key": f"outputs/{seeded.trial_id}/parent.json", "version_id": "v1",
                              "content_hash": "sha256:" + "a" * 64, "size_bytes": 42}]}
    with TestClient(seeded.app) as client:
        response = client.patch(f"/trials/{seeded.trial_id}/trajectory_index", headers=headers, json=payload)
        assert response.status_code == 200, response.text
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            parent_id = connection.execute(text("SELECT id FROM public.artifacts WHERE trial_id = :id"),
                                           {"id": seeded.trial_id}).scalar_one()
            connection.execute(text("UPDATE public.trials SET source_provenance = CAST(:source AS jsonb) WHERE id = :id"),
                               {"id": seeded.trial_id, "source": json.dumps([{"source_artifact_id": str(parent_id),
                                    "kind": "reused_artifact", "source_artifact_key": "input.json"}])})
            if case == "suppress_lineage":
                connection.exec_driver_sql(
                    "CREATE FUNCTION public.output_lineage_interference() RETURNS trigger LANGUAGE plpgsql "
                    "AS $test$ BEGIN RETURN NULL; END $test$; "
                    "CREATE TRIGGER output_lineage_interference BEFORE INSERT ON public.artifact_lineage_edges "
                    "FOR EACH ROW EXECUTE FUNCTION public.output_lineage_interference();"
                )
            before = connection.execute(text("SELECT to_jsonb(t) FROM public.trials t WHERE id = :id"),
                                        {"id": seeded.trial_id}).scalar_one()
        initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
        asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
        if case == "version_conflict":
            payload["artifacts"][0]["content_hash"] = "sha256:" + "b" * 64
        else:
            payload["artifacts"][0]["key"] = f"outputs/{seeded.trial_id}/child.json"
        with TestClient(seeded.app, raise_server_exceptions=False) as client:
            response = client.patch(f"/trials/{seeded.trial_id}/trajectory_index", headers=headers, json=payload)
            assert response.status_code == (200 if case == "lineage" else 409), response.text
        with engine.connect() as connection:
            if case == "lineage":
                edge = connection.execute(text("SELECT child_artifact_id, parent_artifact_id, relation, metadata FROM public.artifact_lineage_edges")).mappings().one()
                assert edge["parent_artifact_id"] == parent_id
                assert edge["child_artifact_id"] != parent_id
                assert edge["relation"] == "reused_as_input"
                assert edge["metadata"] == {"kind": "reused_artifact", "source_artifact_key": "input.json"}
            else:
                assert connection.execute(text("SELECT to_jsonb(t) FROM public.trials t WHERE id = :id"),
                                          {"id": seeded.trial_id}).scalar_one() == before
                assert connection.execute(text("SELECT count(*) FROM public.artifacts")).scalar_one() == 1
                assert connection.execute(text("SELECT count(*) FROM public.data_lifecycle_objects")).scalar_one() == 1
                assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")).scalar_one() == 0
    finally:
        engine.dispose()
