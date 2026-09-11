"""Historical native claims cannot authorize fresh source access."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

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


@pytest.mark.parametrize("boundary", ["exact", "mode", "credential", "pool", "corrupt", "cancel"])
async def test_native_artifact_http_client_streams_through_committed_live_guard(prepared_input, tmp_path, monkeypatch, boundary):
    import asyncio
    import hashlib

    import httpx

    from loom_capacity_agent.build_admission import BuildArtifactV1
    from loom_capacity_build_guard.artifact_writer import BuildArtifactWriter
    from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError
    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for
    from tests.unit.test_native_build_artifact_writer import Objects

    factory, _engine, installation, _plan, _source, _platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "native-source" if boundary == "mode" else "native-artifacts"
    objects = Objects()
    writer = BuildArtifactWriter(session_factory=factory, object_store=objects, max_artifact_bytes=1024)
    app.state.personal_dev_build_artifact_writer = writer
    artifact = BuildArtifactV1(archive_size_bytes=8, archive_sha256=hashlib.sha256(b"artifact").hexdigest())
    async def chunks():
        if boundary == "cancel":
            raise asyncio.CancelledError
        yield b"wrong!!!" if boundary == "corrupt" else b"artifact"
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
            client = client_for(http, claim)
            client._token = "wrong" if boundary == "pool" else "executor-secret"
            async def send():
                return await client.upload_artifact(claim, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL,
                    artifact=artifact, chunks=chunks())
            if boundary == "exact":
                assert (await send()).artifact == artifact
                assert objects.object["Body"] == b"artifact"
            elif boundary == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await send()
            else:
                with pytest.raises(BuildAdmissionTransportError):
                    await send()
        if boundary in {"mode", "credential", "pool"}:
            assert objects.calls == []
        if boundary in {"corrupt", "cancel"}:
            assert objects.object is None and objects.calls[-1][0] == "abort"
    finally:
        await writer.aclose()


@pytest.mark.parametrize("boundary", ["exact", "credential", "before", "part", "complete"])
async def test_native_artifact_writer_uses_live_guard_without_holding_io_locks(prepared_input, monkeypatch, boundary):
    import hashlib

    from loom_capacity_agent.build_admission import BuildArtifactV1
    from loom_capacity_build_guard.artifact_writer import BuildArtifactWriter
    from tests.unit.test_native_build_artifact_writer import Objects

    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    def cancel():
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL lock_timeout='1000ms'"))
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
    objects = Objects()
    if boundary == "before":
        cancel()
    if boundary in {"part", "complete"}:
        name = "upload_part" if boundary == "part" else "complete_multipart_upload"
        original = getattr(objects, name)
        def revoke(**kwargs):
            result = original(**kwargs)
            cancel()
            return result
        monkeypatch.setattr(objects, name, revoke)
    artifact = BuildArtifactV1(archive_size_bytes=8, archive_sha256=hashlib.sha256(b"artifact").hexdigest())
    writer = BuildArtifactWriter(session_factory=factory, object_store=objects, max_artifact_bytes=1024)
    async def chunks():
        yield b"artifact"
    try:
        if boundary == "exact":
            assert await writer.write(claim, worker_credential=CREDENTIAL, artifact=artifact, chunks=chunks()) == artifact
            metadata = objects.object["Metadata"]
            assert metadata["candidate-sha256"] == source.candidate.candidate_sha
            assert metadata["build-attempt-id"] == str(source.build_attempt.id)
            assert metadata["build-lease-epoch"] == str(source.build_attempt.lease_epoch)
            assert metadata["platform"] == platform.platform
        else:
            with pytest.raises((ValueError, DBAPIError)):
                await writer.write(claim, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL,
                    artifact=artifact, chunks=chunks())
        if boundary in {"before", "credential"}:
            assert objects.calls == []
        if boundary == "part":
            assert objects.object is None
        if boundary == "complete":
            assert objects.object is not None  # unaccepted orphan, never a successful receipt
    finally:
        await writer.aclose()


@pytest.mark.parametrize("boundary", ["exact", "missing", "uncommitted", "credential", "claim", "binding",
    "cancelled", "expired", "source", "drain", "outcome"])
async def test_source_access_requires_exact_committed_live_claim(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    receipt = None
    if boundary not in {"missing", "uncommitted"}:
        async with factory.begin() as session:
            receipt = await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    if boundary in {"cancelled", "expired", "source"}:
        with engine.begin() as connection:
            if boundary == "cancelled":
                connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
            elif boundary == "expired":
                connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
            else:
                connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": source.candidate.id})
    if boundary == "drain":
        from loom_capacity_agent.admission import ExecutableDrainRequestV2

        async with factory.begin() as session:
            await store(session, installation).begin_drain(ExecutableDrainRequestV2(
                operation_id=uuid4(), binding=claim.binding, worker_id=claim.worker_id,
                worker_incarnation=claim.worker_incarnation, expected_claim_high_water=1, drain_epoch=3))
    if boundary == "outcome":
        from loom_capacity_agent.build_admission import BuildOutcomeRequestV1

        async with factory.begin() as session:
            await store(session, installation).record_outcome(BuildOutcomeRequestV1(
                claim=claim, operation_id=uuid4(), result="failed"), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        if boundary == "uncommitted":
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
        original = claim
        if boundary == "claim":
            claim = claim.model_copy(update={"operation_id": uuid4()})
        elif boundary == "binding":
            claim = claim.model_copy(update={"binding": claim.binding.model_copy(update={"account_id": "foreign"})})
        execution = store(session, installation)
        if boundary == "exact":
            access = await execution.authorize_source(claim, worker_credential=CREDENTIAL)
            assert access.claim == claim
            assert access.object_bucket == source.candidate.object_bucket
            assert access.object_key == source.candidate.object_key
            assert access.archive_sha256 == source.candidate.archive_sha256
            assert access.archive_size_bytes == source.candidate.archive_size_bytes
            assert access.lease_not_after == source.build_attempt.lease_expires_at
        else:
            with pytest.raises((ValueError, DBAPIError)):
                await execution.authorize_source(claim, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
        if receipt is not None:
            # Fresh access rejection must not break historical recovery.
            assert await execution.claim_platform(original, worker_credential=CREDENTIAL) == receipt


@pytest.mark.parametrize("boundary", ["execute", "public", "search-path"])
def test_source_access_private_surface_is_verified(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.authorize_source(uuid,jsonb,bytea,text,text)"
    statements = {"execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


@pytest.mark.parametrize("boundary", ["exact", "disabled", "credential", "controller", "cancel-during-io"])
async def test_protected_source_client_reads_only_after_post_io_fence(prepared_input, tmp_path, monkeypatch, boundary):
    from io import BytesIO
    from types import SimpleNamespace

    import httpx

    from loom_capacity_build_guard.source_reader import BuildSourceReader
    from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError
    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "native-claims" if boundary == "disabled" else "native-source"
    calls, bodies = [], []

    def get_object(**kwargs):
        calls.append(kwargs)
        if boundary == "cancel-during-io":
            # This would block if management held source locks across S3 IO.
            with engine.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout='1000ms'"))
                connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
        body = BytesIO(b"source")
        bodies.append(body)
        return {"Body": body, "ContentLength": 6, "ContentRange": f"bytes 0-5/{source.candidate.archive_size_bytes}"}

    app.state.personal_dev_build_source_reader = BuildSourceReader(session_factory=factory,
        object_store=SimpleNamespace(get_object=get_object))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        client = client_for(http, claim)
        client._token = "wrong" if boundary == "controller" else "executor-secret"
        if boundary == "exact":
            chunk = await client.read_source(claim, worker_credential=CREDENTIAL, offset=0, length=6)
            assert chunk.data == b"source"
            assert chunk.archive_sha256 == source.candidate.archive_sha256
            assert chunk.archive_size_bytes == source.candidate.archive_size_bytes
        else:
            with pytest.raises(BuildAdmissionTransportError):
                await client.read_source(claim, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL,
                    offset=0, length=6)
    assert len(calls) == int(boundary in {"exact", "cancel-during-io"})
    assert all(body.closed for body in bodies)


def test_source_access_empty_downgrade_preserves_prior_head(build_guard_database):
    from alembic import command

    config, engine, *_ = build_guard_database
    command.upgrade(config, "head")
    command.downgrade(config, "build_guard_0026")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT to_regprocedure('loom_capacity_build_guard.authorize_source(uuid,jsonb,bytea,text,text)')")) is None
    command.upgrade(config, "head")


async def test_source_access_rejects_terminal_job_before_outcome(prepared_input, monkeypatch):
    from tests.integration.test_personal_dev_build_guard_registered_release import (
        registered_release_input,
    )
    from tests.integration.test_personal_dev_build_guard_terminal import terminal_store

    factory, _engine, installation, *_ = prepared_input
    _release, claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch,
        result="live", drain=False)
    async with factory.begin() as session:
        await store(session, installation).authorize_source(claim, worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(terminal)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="execution is closed"):
            await store(session, installation).authorize_source(claim, worker_credential=CREDENTIAL)


async def test_source_access_tracks_current_whole_attempt_heartbeat(prepared_input, monkeypatch):
    from datetime import UTC, datetime, timedelta

    factory, engine, installation, _plan, source, _platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    renewed = (datetime.now(UTC) + timedelta(minutes=5)).replace(microsecond=0)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=:expiry WHERE id=:id"),
            {"id": source.build_attempt.id, "expiry": renewed})
    async with factory.begin() as session:
        access = await store(session, installation).authorize_source(claim, worker_credential=CREDENTIAL)
        assert access.lease_not_after == renewed
