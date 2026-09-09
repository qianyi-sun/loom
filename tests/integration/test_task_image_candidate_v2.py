from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import rfc8785
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import TaskImageMaterialization, TaskImagePublicationCandidate
from loom_task_image_authority import registry_credentials
from loom_task_image_authority.contracts import (
    TaskImageBaseResolutionEvidenceV1,
    TaskImagePublicationCandidateRequestV1,
    TaskImagePublicationCandidateRequestV2,
)
from loom_task_image_authority.http_contracts import TaskImagePublicationCandidateResponseV2
from loom_task_image_authority.materializations import (
    TaskImageBuildSessionAuthorization,
    TaskImageSessionMaterializationAuthorizationError,
    TaskImageSessionMaterializationConflictError,
)
from loom_task_image_authority.registry_credentials import (
    record_session_publication_candidate,
    record_session_publication_candidate_v2,
)
from loom_task_image_authority.registry_token import DistributionRegistryTokenIssuer
from tests.integration.test_task_image_registry_credentials import (
    CANDIDATE_ID,
    NOW,
    _candidate_request,
    _claimed_attempt,
    _issue_first,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def _prepared(
    session: AsyncSession,
    issuer: DistributionRegistryTokenIssuer,
    bases: tuple[str, ...] = (),
) -> tuple[
    TaskImageBuildSessionAuthorization,
    TaskImageMaterialization,
    TaskImagePublicationCandidateRequestV2,
    TaskImagePublicationCandidateRequestV1,
]:
    authorization, _, _, build_session, secrets, row, attempt = await _claimed_attempt(session)
    _, credential = await _issue_first(
        session,
        authorization=authorization,
        build_session=build_session,
        secrets=secrets,
        row=row,
        attempt=attempt,
        issuer=issuer,
    )
    legacy = _candidate_request(
        authorization,
        build_session,
        row,
        attempt,
        credential_id=credential.credential_id,
        credential_generation=credential.generation,
    )
    evidence = {
        "schema": "loom.task-image-base-resolution/v1",
        "solve_ref": "same-solve_1",
        "platform": "linux/arm64",
        "output_digest": "sha256:" + "a" * 64,
        "observed_base_digests": list(bases),
    }
    request = TaskImagePublicationCandidateRequestV2.model_validate(
        dict(legacy.model_dump(), schema_version=2, base_resolution=evidence)
    )
    return authorization, row, request, legacy


async def _record(
    session: AsyncSession,
    authorization: TaskImageBuildSessionAuthorization,
    request: TaskImagePublicationCandidateRequestV2,
) -> TaskImagePublicationCandidateResponseV2:
    return await record_session_publication_candidate_v2(
        session,
        authorization=authorization,
        request=request,
        now=NOW + timedelta(seconds=12),
        candidate_id_factory=lambda: CANDIDATE_ID,
    )


async def test_parse_stored_v2_candidate_returns_strict_typed_response(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _, request, _ = await _prepared(session, registry_issuer)
        response = await _record(session, authorization, request)
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()

        parsed = registry_credentials.parse_stored_publication_candidate_v2(
            stored,
            credential_generation=request.credential_generation,
        )

        assert type(parsed) is TaskImagePublicationCandidateResponseV2
        assert parsed == response


@pytest.mark.parametrize("corruption", ["hash", "row_binding"])
async def test_parse_stored_v2_candidate_rejects_corrupt_row(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
    corruption: str,
) -> None:
    async with registry_authority_session() as session:
        authorization, _, request, _ = await _prepared(session, registry_issuer)
        await _record(session, authorization, request)
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()
        if corruption == "hash":
            stored.response_sha256 = "0" * 64
        else:
            stored.manifest_size += 1

        with pytest.raises(TaskImageSessionMaterializationConflictError):
            registry_credentials.parse_stored_publication_candidate_v2(
                stored,
                credential_generation=request.credential_generation,
            )


async def test_parse_stored_v2_candidate_rejects_v1_row(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _, _, legacy = await _prepared(session, registry_issuer)
        await record_session_publication_candidate(
            session,
            authorization=authorization,
            request=legacy,
            now=NOW + timedelta(seconds=12),
            candidate_id_factory=lambda: CANDIDATE_ID,
        )
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()

        with pytest.raises(TaskImageSessionMaterializationConflictError):
            registry_credentials.parse_stored_publication_candidate_v2(
                stored,
                credential_generation=legacy.credential_generation,
            )


@pytest.mark.parametrize("bases", [(), ("sha256:" + "b" * 64, "sha256:" + "c" * 64)])
async def test_v2_candidate_persists_full_evidence_replays_and_remains_inert(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
    bases: tuple[str, ...],
) -> None:
    async with registry_authority_session() as session:
        authorization, row, request, _ = await _prepared(session, registry_issuer, bases)
        before = (row.state, row.registry_images, row.ready_at, row.finished_at)
        response = await _record(session, authorization, request)
        assert (row.state, row.registry_images, row.ready_at, row.finished_at) == before
        await session.commit()
    async with registry_authority_session() as session:
        replay = await _record(session, authorization, request)
        assert replay == response
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()
        assert stored.response_json["schema_version"] == "loom.task-image-publication-candidate.v2"
        assert stored.response_json["base_resolution"] == {
            "schema": "loom.task-image-base-resolution/v1",
            "solve_ref": "same-solve_1",
            "platform": "linux/arm64",
            "output_digest": "sha256:" + "a" * 64,
            "observed_base_digests": list(bases),
        }
        assert (
            stored.response_sha256
            == hashlib.sha256(rfc8785.dumps(stored.response_json)).hexdigest()
        )
        assert (
            await session.scalar(select(func.count(TaskImagePublicationCandidate.candidate_id)))
            == 1
        )


@pytest.mark.parametrize("changed", ["observations", "solve", "operation", "credential", "attempt"])
async def test_v2_candidate_rejects_changed_identity_or_evidence_without_rewrite(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer,
    changed: str,
) -> None:
    async with registry_authority_session() as session:
        authorization, row, request, _ = await _prepared(session, registry_issuer)
        await _record(session, authorization, request)
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()
        original = json.dumps(stored.response_json, sort_keys=True)
        values = request.model_dump()
        if changed == "observations":
            values["base_resolution"]["observed_base_digests"] = ("sha256:" + "b" * 64,)
        elif changed == "solve":
            values["base_resolution"]["solve_ref"] = "other-solve"
        else:
            values[
                {
                    "operation": "operation_id",
                    "credential": "credential_id",
                    "attempt": "attempt_id",
                }[changed]
            ] = uuid4()
        with pytest.raises(
            (
                TaskImageSessionMaterializationAuthorizationError,
                TaskImageSessionMaterializationConflictError,
            )
        ):
            await _record(
                session,
                authorization,
                TaskImagePublicationCandidateRequestV2.model_validate(values),
            )
        assert json.dumps(stored.response_json, sort_keys=True) == original
        assert row.ready_at is None and not row.registry_images


@pytest.mark.parametrize("first_version", [1, 2])
async def test_candidate_versions_cannot_upgrade_or_downgrade_by_replay(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer,
    first_version: int,
) -> None:
    async with registry_authority_session() as session:
        authorization, _, request, legacy = await _prepared(session, registry_issuer)

        async def record_v1():
            return await record_session_publication_candidate(
                session,
                authorization=authorization,
                request=legacy,
                now=NOW + timedelta(seconds=12),
                candidate_id_factory=lambda: CANDIDATE_ID,
            )

        if first_version == 1:
            await record_v1()
            with pytest.raises(TaskImageSessionMaterializationConflictError):
                await _record(session, authorization, request)
        else:
            await _record(session, authorization, request)
            with pytest.raises(TaskImageSessionMaterializationConflictError):
                await record_v1()


@pytest.mark.parametrize(
    "corruption", ["missing", "hash", "binding", "null", "version", "timestamp", "scalar"]
)
async def test_v2_candidate_replay_rejects_corrupted_stored_metadata(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer,
    corruption: str,
) -> None:
    async with registry_authority_session() as session:
        authorization, row, request, _ = await _prepared(session, registry_issuer)
        await _record(session, authorization, request)
        # Inject DB-owner corruption only into this isolated database, then
        # restore the audit guard before checking application-level rejection.
        await session.execute(
            text(
                "ALTER TABLE task_image_publication_candidates "
                "DISABLE TRIGGER task_image_publication_candidates_preserve"
            )
        )
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()
        payload = json.loads(json.dumps(stored.response_json))
        if corruption == "missing":
            del payload["base_resolution"]
        elif corruption == "hash":
            payload["base_resolution"]["solve_ref"] = "replaced"
        elif corruption == "binding":
            payload["base_resolution"]["output_digest"] = "sha256:" + "b" * 64
            stored.response_sha256 = hashlib.sha256(rfc8785.dumps(payload)).hexdigest()
        elif corruption == "version":
            del payload["schema_version"]
        elif corruption == "timestamp":
            payload["recorded_at"] = payload["recorded_at"].replace("Z", "+00:00")
        elif corruption == "scalar":
            stored.manifest_size += 1
        else:
            payload["base_resolution"]["observed_base_digests"] = None
        stored.response_json = payload
        await session.flush()
        await session.execute(
            text(
                "ALTER TABLE task_image_publication_candidates "
                "ENABLE TRIGGER task_image_publication_candidates_preserve"
            )
        )
        with pytest.raises(TaskImageSessionMaterializationConflictError):
            await _record(session, authorization, request)
        assert row.ready_at is None and not row.registry_images


@pytest.mark.parametrize("change", ["forged", "missing", "platform", "component", "generation"])
async def test_v2_candidate_store_rejects_invalid_or_unauthorized_request_before_insert(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
    change: str,
) -> None:
    async with registry_authority_session() as session:
        authorization, row, request, _ = await _prepared(session, registry_issuer)
        if change == "forged":
            request = request.model_copy(
                update={
                    "base_resolution": request.base_resolution.model_copy(
                        update={"observed_base_digests": ("invalid",)}
                    )
                }
            )
        elif change == "missing":
            values = request.model_dump()
            del values["base_resolution"]["observed_base_digests"]
            request = request.model_copy(
                update={
                    "base_resolution": TaskImageBaseResolutionEvidenceV1.model_construct(
                        **values["base_resolution"]
                    )
                }
            )
        else:
            values = request.model_dump()
            if change == "platform":
                values["platform"] = "linux/amd64"
                values["base_resolution"]["platform"] = "linux/amd64"
            elif change == "component":
                values["component"] = "sidecar:absent"
            else:
                values["credential_generation"] = 2
            request = TaskImagePublicationCandidateRequestV2.model_validate(values)
        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await _record(session, authorization, request)
        assert row.ready_at is None and not row.registry_images
        assert (
            await session.scalar(select(func.count(TaskImagePublicationCandidate.candidate_id)))
            == 0
        )


async def test_v2_candidate_transaction_rollback_discards_evidence(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _, request, _ = await _prepared(session, registry_issuer)
        await session.commit()
        await _record(session, authorization, request)
        await session.rollback()
    async with registry_authority_session() as session:
        assert (
            await session.scalar(select(func.count(TaskImagePublicationCandidate.candidate_id)))
            == 0
        )
