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
