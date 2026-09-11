"""The whole-attempt adapter stages native demand and publishes exact results."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom.db.schema import PersonalDevBuildPlatformRequest, PersonalDevCandidate
from loom.personal_dev_build_demand import personal_build_work_identity
from loom_capacity_agent.build_admission import BuildArtifactV1, BuildClaimRequestV1, BuildOutcomeReceiptV1, BuildOutcomeRequestV1
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_platform_requests import build_service
from tests.integration.test_personal_dev_native_builder_store import _seed_running_attempt
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_build_admission_client import native_registration
from tests.unit.test_native_build_source import sealed_source as sealed_source


@pytest.fixture
async def attempt_input(sessions, sealed_source, tmp_path):
    original, _archive, _workspace = sealed_source
    registration = await _seed_running_attempt(sessions, now=datetime.now(UTC))
    values = {name: getattr(original.candidate, name) for name in (
        "source_sha256", "archive_sha256", "build_contract_sha256", "source_commit", "dirty", "manifest_json", "archive_size_bytes")}
    candidate = replace(registration.candidate, **values)
    values["object_key"] = f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/{candidate.candidate_sha}/{candidate.source_generation_id}/{candidate.archive_sha256}.tar"
    registration = replace(registration, candidate=replace(candidate, object_key=values["object_key"]))
    async with sessions.begin() as session:
        await session.execute(update(PersonalDevCandidate).where(PersonalDevCandidate.id == candidate.id).values(**values))
    member, runtime = build_service(tmp_path, registration)
    return registration, tmp_path / "sealed.tar", member, runtime


def receipt_for(registration, member, platform, *, result="artifact-ready"):
    worker = native_registration("gb10" if platform == "linux/arm64" else "oldlab")
    config = member.configuration
    binding = worker.binding.model_copy(update={"subject_id": config.subject_id, "subject_incarnation": config.subject_incarnation,
        "deployment_generation": config.deployment_generation, "candidate_generation": config.candidate_generation,
        "candidate": member.acknowledgement.candidate})
    claim = BuildClaimRequestV1(binding=binding, operation_id=uuid4(), request_id=personal_build_work_identity(registration, platform)[1],
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
    outcome = BuildOutcomeRequestV1(claim=claim, operation_id=uuid4(), result=result,
        artifact=BuildArtifactV1(archive_sha256="a" * 64, archive_size_bytes=1024) if result == "artifact-ready" else None)
    return BuildOutcomeReceiptV1(request=outcome, request_digest=canonical_digest(outcome))


@pytest.mark.parametrize("boundary", ["exact", "failed", "cancelled-result", "wrong-request", "bad-digest", "db-error", "timeout", "cancel", "export-failure", "source", "pre-cancel"])
async def test_native_attempt_stages_both_platforms_and_never_publishes_partial_work(attempt_input, sessions, boundary):
    module = import_module("loom.personal_dev_native_attempt_executor")
    registration, archive, member, runtime = attempt_input
    observed, published = [], []
    entered = asyncio.Event()

    class Outcomes:
        async def observe(self, incoming, *, platform):
            assert incoming == registration
            # Staging must have committed both requests before any observation.
            async with sessions() as session:
                rows = (await session.scalars(select(PersonalDevBuildPlatformRequest))).all()
                assert {row.platform for row in rows} == {"linux/amd64", "linux/arm64"}
                assert all(row.cancelled_at is None for row in rows)
            observed.append(platform)
            entered.set()
            if boundary == "db-error":
                raise RuntimeError("database unavailable")
            if boundary in {"timeout", "cancel"}:
                await asyncio.Event().wait()
            if observed.count(platform) == 1:
                return None
            result = "failed" if boundary == "failed" else "cancelled" if boundary == "cancelled-result" else "artifact-ready"
            receipt = receipt_for(incoming, member, platform, result=result)
            if boundary == "wrong-request":
                wrong = receipt.request.model_copy(update={"claim": receipt.request.claim.model_copy(update={"request_id": uuid4()})})
                receipt = receipt.model_copy(update={"request": wrong, "request_digest": canonical_digest(wrong)})
            elif boundary == "bad-digest":
                receipt = receipt.model_copy(update={"request_digest": "f" * 64})
            return receipt

    class Exporter:
        async def publish(self, incoming):
            assert incoming == registration
            assert set(observed) == {"linux/amd64", "linux/arm64"}
            assert all(observed.count(platform) >= 2 for platform in set(observed))
            published.append(incoming)
            if boundary == "export-failure":
                raise RuntimeError("publication rejected")
            return {"verified-publication": True}

    executor = module.NativePersonalDevBuildExecutor(session_factory=sessions, member=member, runtime=runtime,
        outcomes=Outcomes(), exporter=Exporter(), poll_interval_seconds=0.01,
        wait_timeout_seconds=0.2 if boundary == "timeout" else 10)
    if boundary == "source":
        archive.write_bytes(b"changed-source")
    if boundary == "pre-cancel":
        await executor.cleanup(registration)

    async def run():
        try:
            return await executor.build(registration, source_archive=archive)
        finally:
            # This is the existing whole-attempt coordinator's cleanup contract.
            await executor.cleanup(registration)

    if boundary == "exact":
        assert await run() == {"verified-publication": True}
    elif boundary == "cancel":
        task = asyncio.create_task(run())
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises((ValueError, RuntimeError, TimeoutError)):
            await run()
    assert bool(published) == (boundary in {"exact", "export-failure"})
    if boundary in {"source", "pre-cancel"}:
        assert observed == []
    async with sessions() as session:
        rows = (await session.scalars(select(PersonalDevBuildPlatformRequest))).all()
        assert len(rows) == 2 and all(row.cancelled_at is not None for row in rows)
    await executor.cleanup(registration)  # Exact replay cannot requeue demand.
