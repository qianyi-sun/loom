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
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
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


@pytest.mark.parametrize("boundary", ["exact", "disabled", "credential", "cancelled", "controller", "http"])
async def test_native_context_crosses_protected_http_only_for_live_claim(prepared_input, tmp_path, monkeypatch, boundary):
    import httpx

    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "native-claims" if boundary == "disabled" else "native-source"
    if boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
    responses = []
    async def capture(response):
        responses.append(response)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), event_hooks={"response": [capture]}) as http:
        client = client_for(http, claim)
        client._token = "wrong" if boundary == "controller" else "executor-secret"
        if boundary == "http":
            # Exercise server TLS enforcement; production client rejects this origin.
            client._origin = "http://management.test"
        if boundary == "exact":
            context = await client.read_source_context(claim, worker_credential=CREDENTIAL)
            assert context.candidate_id == source.candidate.id
            assert responses[0].headers["cache-control"] == "no-store"
        else:
            with pytest.raises(RuntimeError):
                await client.read_source_context(claim, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
            assert responses[0].status_code == {"disabled": 503, "controller": 401, "http": 403}.get(boundary, 409)


@pytest.mark.parametrize("boundary", ["execute", "public", "search-path"])
def test_native_context_private_acl_is_verified(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.read_source_context(uuid,jsonb,bytea,text,text)"
    statements = {"execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


def test_context_downgrade_preserves_prior_source_access(build_guard_database):
    from alembic import command

    config, engine, _owner, _agent, _url = build_guard_database
    command.upgrade(config, "head")
    command.downgrade(config, "build_guard_0027")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT to_regprocedure('loom_capacity_build_guard.read_source_context(uuid,jsonb,bytea,text,text)')")) is None
        assert connection.scalar(text("SELECT to_regprocedure('loom_capacity_build_guard.authorize_source(uuid,jsonb,bytea,text,text)')")) is not None
    command.upgrade(config, "head")
