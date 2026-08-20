from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.integrations.terminalgen.artifacts import (
    ArtifactRefV1,
    CorpusTaskEntryV1,
    FinalAuditCountsV1,
    PipelineArtifactProvenanceV1,
    TaskBundleFileV1,
    TerminalGenCorpusArtifactV1,
    TerminalGenFinalAuditArtifactV1,
    TerminalGenPublicationRequestV1,
    TerminalGenTaskSetSmokeV1,
)
from loom.integrations.terminalgen.publication import TerminalGenPublicationMaterial
from loom.pipeline.keys import canonical_document, digest_bytes
from loom_pipeline_orchestrator.repository import (
    PipelineRepository,
    PublicationStoredFile,
    RunLease,
    TerminalGenPublicationArtifactSource,
    TerminalGenPublicationCandidate,
)

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.invalid/loom/terminalgen@sha256:" + "b" * 64


def _ref(artifact_id: UUID, artifact_type: str) -> ArtifactRefV1:
    return ArtifactRefV1(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        manifest_sha256=DIGEST,
    )


def _source(artifact_id: UUID, artifact_type: str) -> TerminalGenPublicationArtifactSource:
    body = canonical_document({"artifact_id": str(artifact_id)})
    return TerminalGenPublicationArtifactSource(
        artifact_id=artifact_id,
        artifact_name="publication_request",
        artifact_type=artifact_type,
        access_class="authoring_restricted",
        manifest_sha256=DIGEST,
        content_sha256=digest_bytes(body),
        node_key="publish_boundary",
        root_manifest_bytes=body,
        root_manifest_sha256=digest_bytes(body),
        root_manifest_key="fixture/_manifest.json",
        committed_marker_bytes=body,
        committed_marker_sha256=digest_bytes(body),
        committed_marker_key="fixture/_COMMITTED",
        files=(
            PublicationStoredFile(
                relative_path="artifact.json",
                role="semantic_document",
                archive_format="none",
                media_type="application/json",
                size_bytes=len(body),
                sha256=digest_bytes(body),
                storage_key="fixture/artifact.json",
            ),
        ),
    )


def _material(ids: dict[str, UUID], run_id: UUID) -> TerminalGenPublicationMaterial:
    source_ref = _ref(ids["source"], "terminalgen_task_bundle.v1")
    validation_ref = _ref(ids["validation"], "terminalgen_task_validation.v1")
    audit_ref = _ref(ids["audit"], "terminalgen_final_audit.v1")
    provenance = PipelineArtifactProvenanceV1(
        producer_kind="pipeline",
        loom_commit_sha="c" * 40,
        pipeline_run_id=run_id,
        stage_run_id=UUID(int=21),
        execution_attempt_id=UUID(int=22),
        recipe_digest=DIGEST,
        execution_spec_digest=DIGEST,
        image_digest=IMAGE,
        compatibility_manifest_sha256=DIGEST,
        source_artifacts=[],
    )
    slot_id = "capability-00__same-domain-parametric__0001"
    task_id = f"terminalgen-{slot_id}"
    roles = (
        ("dependency_lock", "payload/dependencies.lock"),
        ("environment", "payload/environment/Dockerfile"),
        ("instruction", "payload/instruction.md"),
        ("task_config", "payload/task.toml"),
        ("verifier", "payload/tests/test_task.py"),
    )

    def entry(*, authoring: bool) -> CorpusTaskEntryV1:
        files = [
            TaskBundleFileV1(
                role=role,  # type: ignore[arg-type]
                relative_path=path,
                sha256=DIGEST,
                size_bytes=1,
                media_type="text/plain",
            )
            for role, path in roles
        ]
        if authoring:
            files.append(
                TaskBundleFileV1(
                    role="reference_solution",
                    relative_path="payload/solution/solve.sh",
                    sha256=DIGEST,
                    size_bytes=1,
                    media_type="text/x-shellscript",
                )
            )
            files.sort(key=lambda item: item.relative_path.encode())
        return CorpusTaskEntryV1(
            slot_id=slot_id,
            task_id=task_id,
            task_name="Durable task",
            source_task_tree_sha256=DIGEST,
            projected_task_tree_sha256=DIGEST,
            source_task_artifact=source_ref,
            validation_artifact=validation_ref,
            bundle_relative_path=f"payload/tasks/{task_id}.tar",
            bundle_sha256=DIGEST,
            bundle_size_bytes=1,
            verifier_bridge_sha256=DIGEST,
            files=files,
        )

    common = {
        "schema_version": "terminalgen_corpus.v1",
        "corpus_id": "terminalgen-authorized",
        "corpus_version": 1,
        "final_audit_artifact": audit_ref,
        "plan_identity_sha256": DIGEST,
        "task_count": 1,
        "corpus_tree_sha256": DIGEST,
        "task_archive_format": "tar",
        "provenance": provenance,
    }
    authoring = TerminalGenCorpusArtifactV1(
        corpus_kind="authoring",
        access_class="authoring_restricted",
        contains_reference_solutions=True,
        tasks=[entry(authoring=True)],
        **common,  # type: ignore[arg-type]
    )
    runtime = TerminalGenCorpusArtifactV1(
        corpus_kind="runtime",
        access_class="team_runtime",
        contains_reference_solutions=False,
        tasks=[entry(authoring=False)],
        **common,  # type: ignore[arg-type]
    )
    return TerminalGenPublicationMaterial(
        request=TerminalGenPublicationRequestV1(
            schema_version="terminalgen.publication-request.v1",
            pipeline_run_id=run_id,
            recipe_digest=DIGEST,
            corpus_id="terminalgen-authorized",
            corpus_version=1,
            alias="terminalgen-current",
            expected_previous_version_sha256=None,
            final_audit_artifact=audit_ref,
            authoring_corpus_artifact=_ref(ids["authoring"], "terminalgen_corpus.v1"),
            runtime_corpus_artifact=_ref(ids["runtime"], "terminalgen_corpus.v1"),
            taskset_smoke_count=1,
        ),
        final_audit=TerminalGenFinalAuditArtifactV1(
            schema_version="terminalgen_final_audit.v1",
            access_class="sanitized_audit",
            terminal_outcome="complete",
            reason_code="quota_complete",
            plan_identity_sha256=DIGEST,
            slot_terminal_set_sha256=DIGEST,
            task_artifact_set_sha256=DIGEST,
            validation_artifact_set_sha256=DIGEST,
            counts=FinalAuditCountsV1(
                requested=1,
                accepted=1,
                rejected=0,
                exhausted=0,
                cancelled=0,
                cleanup_failed=0,
                dynamically_validated=1,
            ),
            all_slot_ids_unique=True,
            all_template_family_ids_unique=True,
            quota_complete=True,
            validation_complete=True,
            provenance=provenance,
        ),
        authoring_corpus=authoring,
        runtime_corpus=runtime,
    )


@pytest.mark.asyncio
async def test_repository_atomically_publishes_alias_tasks_and_idempotent_receipt(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, run_id, stage_id, attempt_id, upload_id = (uuid4() for _ in range(5))
    ids = {key: uuid4() for key in ("source", "validation", "audit", "authoring", "runtime", "request")}
    material = _material(ids, run_id)
    now = datetime.now(UTC)
    lease = RunLease(
        pipeline_run_id=run_id,
        claimed_by="terminalgen-publisher-test",
        lease_epoch=1,
        lease_expires_at=now + timedelta(minutes=5),
    )
    candidate = TerminalGenPublicationCandidate(
        pipeline_run_id=run_id,
        team_id=team_id,
        recipe_digest=DIGEST,
        request=_source(ids["request"], "terminalgen.publication-request.v1"),
        final_audit=_source(ids["audit"], "terminalgen_final_audit.v1"),
        authoring_corpus=_source(ids["authoring"], "terminalgen_corpus.v1"),
        runtime_corpus=_source(ids["runtime"], "terminalgen_corpus.v1"),
    )
    smoke = TerminalGenTaskSetSmokeV1(
        schema_version="terminalgen.taskset-smoke.v1",
        corpus_version_sha256=material.corpus_version_sha256,
        task_count=1,
        task_ids=[material.runtime_corpus.tasks[0].task_id],
        manifest_sha256=DIGEST,
        archive_sha256=DIGEST,
        archive_size_bytes=1,
    )
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"terminalgen-publisher-{team_id}"},
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id, team_id, submission_policy, recipe_name, recipe_version,
                        recipe_digest, graph_spec_json, graph_spec_digest,
                        parameters_json, parameters_digest, resolved_inputs_json,
                        budget_json, request_digest, idempotency_key, state,
                        claimed_by, lease_epoch, lease_expires_at
                    ) VALUES (
                        :id, :team, 'ordinary', 'terminalgen-authoring', 1, :digest,
                        '{}'::jsonb, :digest, '{}'::jsonb, :digest, '[]'::jsonb,
                        '{}'::jsonb, :digest, :key, 'running', :owner, 1, :expires
                    )
                """),
                {
                    "id": run_id,
                    "team": team_id,
                    "digest": DIGEST,
                    "key": f"terminalgen-publisher-{run_id}",
                    "owner": lease.claimed_by,
                    "expires": lease.lease_expires_at,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id, pipeline_run_id, node_key, shard_key, node_kind, state,
                        resource_profile_json, resource_profile_digest, failure_policy
                    ) VALUES (
                        :id, :run, 'fixture', 'singleton', 'container', 'blocked',
                        '{}'::jsonb, :digest, 'fail_run'
                    )
                """),
                {"id": stage_id, "run": run_id, "digest": DIGEST},
            )
            await session.execute(
                text("""
                    INSERT INTO execution_attempts (
                        id, stage_run_id, attempt_number, state
                    ) VALUES (:id, :stage, 1, 'fault_pending')
                """),
                {"id": attempt_id, "stage": stage_id},
            )
            await session.execute(
                text("""
                    INSERT INTO artifact_upload_sessions (
                        id, team_id, commit_kind, pipeline_run_id,
                        pipeline_stage_run_id, execution_attempt_id, attempt_number,
                        idempotency_key, request_digest, stage_result_json,
                        stage_result_digest, inventory_digest, prefix, state,
                        expected_total_max_bytes, actual_total_bytes,
                        canonical_manifest_json, manifest_sha256,
                        committed_marker_sha256, expires_at, committed_at
                    ) VALUES (
                        :id, :team, 'final_output', :run, :stage, :attempt, 1,
                        :key, :digest, '{}'::jsonb, :digest, :digest, :prefix,
                        'committed', 100, 2, '{}'::jsonb, :digest, :digest,
                        :expires, :now
                    )
                """),
                {
                    "id": upload_id,
                    "team": team_id,
                    "run": run_id,
                    "stage": stage_id,
                    "attempt": attempt_id,
                    "key": f"terminalgen-upload-{upload_id}",
                    "digest": DIGEST,
                    "prefix": f"fixture/{upload_id}/",
                    "expires": now + timedelta(hours=1),
                    "now": now,
                },
            )
            for key, artifact_type, access_class, with_manifest in (
                ("source", "terminalgen_task_bundle.v1", "authoring_restricted", True),
                ("validation", "terminalgen_task_validation.v1", "authoring_restricted", True),
                ("audit", "terminalgen_final_audit.v1", "sanitized_audit", False),
                ("authoring", "terminalgen_corpus.v1", "authoring_restricted", False),
                ("runtime", "terminalgen_corpus.v1", "team_runtime", False),
                ("request", "terminalgen.publication-request.v1", "authoring_restricted", False),
            ):
                await session.execute(
                    text("""
                        INSERT INTO artifacts (
                            id, artifact_type, name, team_id, pipeline_run_id,
                            artifact_upload_session_id, manifest_sha256,
                            stored_size_bytes, unpacked_size_bytes, file_count,
                            content_hash, access_class
                        ) VALUES (
                            :id, :type, :name, :team, :run,
                            :upload, :manifest, :stored, :unpacked, :count,
                            :digest, :access_class
                        )
                    """),
                    {
                        "id": ids[key],
                        "type": artifact_type,
                        "name": key,
                        "team": team_id,
                        "run": run_id,
                        "upload": upload_id if with_manifest else None,
                        "manifest": DIGEST if with_manifest else None,
                        "stored": 1 if with_manifest else None,
                        "unpacked": 1 if with_manifest else None,
                        "count": 1 if with_manifest else None,
                        "digest": DIGEST,
                        "access_class": access_class,
                    },
                )

        repository = PipelineRepository(sessions)
        values = {
            "candidate": candidate,
            "request_sha256": digest_bytes(canonical_document(material.request)),
            "material": material,
            "smoke": smoke,
            "smoke_object_key": "fixture/taskset-smoke.tar",
            "taskset_manifest_object_key": "fixture/taskset-smoke.yaml",
            "taskset_manifest_json": {},
        }
        receipt = await repository.publish_terminalgen_corpus(lease, **values)
        replay = await repository.publish_terminalgen_corpus(lease, **values)
        assert replay == receipt
        async with sessions() as session:
            row = (
                await session.execute(
                    text("""
                        SELECT a.generation, v.task_count, p.state,
                               (SELECT count(*) FROM terminalgen_corpus_tasks t
                                 WHERE t.corpus_version_id=v.id) AS projected_tasks
                          FROM terminalgen_corpus_aliases a
                          JOIN terminalgen_corpus_versions v ON v.id=a.corpus_version_id
                          JOIN terminalgen_corpus_publications p ON p.corpus_version_id=v.id
                         WHERE a.team_id=:team AND a.alias='terminalgen-current'
                    """),
                    {"team": team_id},
                )
            ).mappings().one()
            assert dict(row) == {
                "generation": 1,
                "task_count": 1,
                "state": "published",
                "projected_tasks": 1,
            }
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                text("DELETE FROM terminalgen_corpus_aliases WHERE team_id=:team"),
                {"team": team_id},
            )
            await session.execute(
                text("DELETE FROM terminalgen_corpus_publications WHERE team_id=:team"),
                {"team": team_id},
            )
            await session.execute(
                text("DELETE FROM terminalgen_corpus_versions WHERE team_id=:team"),
                {"team": team_id},
            )
            await session.execute(text("DELETE FROM artifacts WHERE team_id=:team"), {"team": team_id})
            await session.execute(
                text("DELETE FROM artifact_upload_sessions WHERE team_id=:team"),
                {"team": team_id},
            )
            await session.execute(
                text("DELETE FROM pipeline_runs WHERE id=:run"),
                {"run": run_id},
            )
            await session.execute(text("DELETE FROM teams WHERE id=:team"), {"team": team_id})
        await engine.dispose()
