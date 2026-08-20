from __future__ import annotations

import json
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, text

from loom.pipeline.keys import digest_bytes
from loom_service.routes.terminalgen_corpora import canonical_manifest_bytes


@pytest.mark.asyncio
async def test_published_corpus_is_team_scoped_and_downloads_verified_smoke(
    tasksets_setup,
) -> None:
    app, tokens, teams = tasksets_setup
    team_id = teams["team_a"]
    run_id, audit_id, authoring_id, runtime_id, version_id = (uuid4() for _ in range(5))
    digest = "sha256:" + "1" * 64
    archive = b"deterministic terminalgen taskset smoke\n"
    manifest_json = {
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {
            "display_name": "TerminalGen Smoke",
            "name": "terminalgen-authorized-v1-smoke",
        },
        "source": {
            "locator": "terminalgen-smoke.tar",
            "subset": "tasks",
            "type": "bundle-upload",
        },
    }
    manifest = canonical_manifest_bytes(manifest_json)
    archive_key = f"pipeline-corpora/{team_id}/fixture/smoke.tar"
    manifest_key = f"pipeline-corpora/{team_id}/fixture/manifest.yaml"
    sync_engine = create_engine(str(app.state.settings.db_url))
    try:
        with sync_engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id, team_id, submission_policy, recipe_name, recipe_version,
                        recipe_digest, graph_spec_json, graph_spec_digest,
                        parameters_json, parameters_digest, resolved_inputs_json,
                        budget_json, request_digest, idempotency_key, state
                    ) VALUES (
                        :id, :team, 'ordinary', 'terminalgen-authoring', 1, :digest,
                        '{}'::jsonb, :digest, '{}'::jsonb, :digest,
                        '[]'::jsonb, '{}'::jsonb, :digest, :key, 'running'
                    )
                """),
                {
                    "id": run_id,
                    "team": team_id,
                    "digest": digest,
                    "key": f"terminalgen-corpus-route-{run_id}",
                },
            )
            connection.execute(
                    text("""
                        UPDATE pipeline_runs
                           SET state='finished', result='succeeded',
                               finished_at=clock_timestamp()
                         WHERE id=:id
                    """),
                {"id": run_id},
            )
            for artifact_id, artifact_type in (
                (audit_id, "terminalgen_final_audit.v1"),
                (authoring_id, "terminalgen_corpus.v1"),
                (runtime_id, "terminalgen_corpus.v1"),
            ):
                connection.execute(
                    text("""
                        INSERT INTO artifacts (
                            id, artifact_type, name, team_id, content_hash
                        ) VALUES (:id, :artifact_type, 'fixture', :team, :digest)
                    """),
                    {
                        "id": artifact_id,
                        "artifact_type": artifact_type,
                        "team": team_id,
                        "digest": digest,
                    },
                )
            connection.execute(
                text("""
                    INSERT INTO terminalgen_corpus_versions (
                        id, team_id, pipeline_run_id, corpus_id, corpus_version,
                        version_sha256, recipe_digest, plan_identity_sha256,
                        final_audit_artifact_id, authoring_corpus_artifact_id,
                        runtime_corpus_artifact_id, authoring_tree_sha256,
                        runtime_tree_sha256, task_count, taskset_smoke_task_count,
                        taskset_smoke_object_key, taskset_smoke_sha256,
                        taskset_smoke_size_bytes, taskset_manifest_object_key,
                        taskset_manifest_json, taskset_manifest_sha256
                    ) VALUES (
                        :id, :team, :run, 'terminalgen-authorized', 1,
                        :digest, :digest, :digest, :audit, :authoring, :runtime,
                        :digest, :digest, 20, 20, :archive_key, :archive_digest,
                        :archive_size, :manifest_key, CAST(:manifest AS jsonb),
                        :manifest_digest
                    )
                """),
                {
                    "id": version_id,
                    "team": team_id,
                    "run": run_id,
                    "digest": digest,
                    "audit": audit_id,
                    "authoring": authoring_id,
                    "runtime": runtime_id,
                    "archive_key": archive_key,
                    "archive_digest": digest_bytes(archive),
                    "archive_size": len(archive),
                    "manifest_key": manifest_key,
                    "manifest": json.dumps(manifest_json),
                    "manifest_digest": digest_bytes(manifest),
                },
            )
            connection.execute(
                text("""
                    INSERT INTO terminalgen_corpus_aliases (
                        team_id, alias, corpus_version_id, generation
                    ) VALUES (:team, 'terminalgen-current', :version, 1)
                """),
                {"team": team_id, "version": version_id},
            )
        client = app.state.minio_client
        bucket = app.state.settings.artifacts_bucket
        client.put_object(Bucket=bucket, Key=archive_key, Body=archive)
        client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=manifest,
        )
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as http:
            own = await http.get(
                "/api/v1/terminalgen-corpora/terminalgen-current",
                headers={"Authorization": f"Bearer {tokens['team_a']}"},
            )
            cross_team = await http.get(
                "/api/v1/terminalgen-corpora/terminalgen-current",
                headers={"Authorization": f"Bearer {tokens['team_b']}"},
            )
            archive_response = await http.get(
                "/api/v1/terminalgen-corpora/terminalgen-current/taskset-smoke/archive",
                headers={"Authorization": f"Bearer {tokens['team_a']}"},
            )
            manifest_response = await http.get(
                "/api/v1/terminalgen-corpora/terminalgen-current/taskset-smoke/manifest",
                headers={"Authorization": f"Bearer {tokens['team_a']}"},
            )
        assert own.status_code == 200, own.text
        assert own.json()["task_count"] == 20
        assert "taskset_smoke_object_key" not in own.json()
        assert cross_team.status_code == 404
        assert archive_response.status_code == 200
        assert archive_response.content == archive
        assert manifest_response.status_code == 200
        assert manifest_response.content == manifest
    finally:
        with sync_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM terminalgen_corpus_aliases WHERE corpus_version_id=:id"),
                {"id": version_id},
            )
            connection.execute(
                text("DELETE FROM terminalgen_corpus_versions WHERE id=:id"),
                {"id": version_id},
            )
            connection.execute(
                text("DELETE FROM artifacts WHERE id IN (:a, :b, :c)"),
                {"a": audit_id, "b": authoring_id, "c": runtime_id},
            )
            connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
        sync_engine.dispose()
