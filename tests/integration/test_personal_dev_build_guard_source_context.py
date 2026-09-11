"""Compact native metadata uses the same current-claim fence as source IO."""

import json
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.build_admission import BuildOutcomeRequestV1
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "unicode", "large-manifest", "missing", "uncommitted",
    "credential", "foreign", "cancelled", "expired", "outcome"])
async def test_native_context_is_compact_and_requires_current_claim(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    if boundary not in {"missing", "uncommitted"}:
        async with factory.begin() as session:
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    manifest = dict(source.candidate.manifest_json)
    if boundary in {"unicode", "large-manifest"}:
        manifest = {"path": "\u96ea/\U0001f680", "content": "x" * (9 * 1024 * 1024 if boundary == "large-manifest" else 1)}
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidates SET manifest_json=CAST(:manifest AS jsonb) WHERE id=:id"),
                {"manifest": json.dumps(manifest), "id": source.candidate.id})
    if boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
    if boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
    if boundary == "outcome":
        async with factory.begin() as session:
            await store(session, installation).record_outcome(BuildOutcomeRequestV1(claim=claim,
                operation_id=uuid4(), result="failed"), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        execution = store(session, installation)
        if boundary == "uncommitted":
            await execution.claim_platform(claim, worker_credential=CREDENTIAL)
        if boundary == "foreign":
            claim = claim.model_copy(update={"request_id": uuid4()})
        if boundary not in {"exact", "unicode", "large-manifest"}:
            with pytest.raises((ValueError, DBAPIError)):
                await execution.read_source_context(claim,
                    worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
            return
        context = await execution.read_source_context(claim, worker_credential=CREDENTIAL)
        assert context.claim_digest == canonical_digest(claim)
        assert context.request_id == claim.request_id
        assert context.platform == platform.platform
        assert context.candidate_id == source.candidate.id
        assert context.archive_sha256 == source.candidate.archive_sha256
        assert context.source_commit == source.candidate.source_commit
        assert context.dirty is source.candidate.dirty
        assert context.attempt_id == source.build_attempt.id
        assert context.attempt_sequence == source.build_attempt.attempt_sequence
        # The sealed manifest is already identified by the source digest. The
        # redundant application manifest is not a second worker authority.
        assert context.source_sha256 == source.candidate.source_sha256
        payload = context.model_dump_json()
        assert len(payload) < 8192
        for forbidden in ("object_bucket", "object_key", "manifest_json", "worker_credential", "claimed_by"):
            assert forbidden not in payload
