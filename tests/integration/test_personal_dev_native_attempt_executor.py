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
from loom_capacity_agent.build_admission import (
    BuildArtifactV1,
    BuildClaimRequestV1,
    BuildOutcomeReceiptV1,
    BuildOutcomeRequestV1,
)
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_platform_requests import build_service
from tests.integration.test_personal_dev_native_builder_store import _seed_running_attempt
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_build_admission_client import native_registration
from tests.unit.test_native_build_source import sealed_source as sealed_source


@pytest.fixture
async def attempt_input(sessions, sealed_source, tmp_path):
    return await make_attempt_input(sessions, sealed_source, tmp_path)


async def make_attempt_input(sessions, sealed_source, tmp_path):
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
    identity = {"subject_id": uuid4(), "subject_incarnation": uuid4()}
    member = member.model_copy(update={"configuration": member.configuration.model_copy(update=identity),
        "acknowledgement": member.acknowledgement.model_copy(update=identity)})
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


@pytest.mark.parametrize("boundary", ["exact", "failed", "cancelled-result", "wrong-request", "bad-digest", "db-error", "timeout", "cancel", "export-failure", "resolver-drift", "source", "pre-cancel"])
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
            if boundary == "resolver-drift":
                exporter.accepted_artifact_resolver = None
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

    outcomes, exporter = Outcomes(), Exporter()
    exporter.accepted_artifact_resolver = outcomes
    executor = module.NativePersonalDevBuildExecutor(session_factory=sessions, member=member, runtime=runtime,
        outcomes=outcomes, exporter=exporter, poll_interval_seconds=0.01,
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


@pytest.mark.parametrize("boundary", ["legacy-exporter", "foreign-resolver", "infinite-poll", "nan-deadline", "boolean-timeout", "long-poll"])
async def test_native_attempt_rejects_unbounded_timing_and_legacy_publication(attempt_input, sessions, boundary):
    from types import SimpleNamespace

    module = import_module("loom.personal_dev_native_attempt_executor")
    _registration, _archive, member, runtime = attempt_input
    outcomes = object()
    exporter = SimpleNamespace(accepted_artifact_resolver=outcomes)
    timing = {}
    if boundary == "legacy-exporter":
        exporter.accepted_artifact_resolver = None
    elif boundary == "foreign-resolver":
        exporter.accepted_artifact_resolver = object()
    elif boundary == "infinite-poll":
        timing["poll_interval_seconds"] = float("inf")
    elif boundary == "nan-deadline":
        timing["wait_timeout_seconds"] = float("nan")
    elif boundary == "boolean-timeout":
        timing["wait_timeout_seconds"] = True
    else:
        timing.update(wait_timeout_seconds=1, poll_interval_seconds=2)
    with pytest.raises(ValueError):
        module.NativePersonalDevBuildExecutor(session_factory=sessions, member=member, runtime=runtime,
            outcomes=outcomes, exporter=exporter, **timing)


@pytest.mark.parametrize("one_owner_fails", [False, True])
async def test_two_owner_whole_attempt_coordinators_route_heartbeat_finish_and_cancel_demand(sessions, sealed_source, tmp_path, one_owner_fails):
    from contextlib import asynccontextmanager

    from loom.db.schema import PersonalDevCandidateBuildAttempt
    from loom.personal_dev_builder import PersonalDevBuildCoordinator
    from loom.personal_dev_candidate import PersonalDevCandidateLimits
    from loom_service.personal_dev_builder import SessionPersonalDevBuildAuthority
    from tests.unit.test_personal_dev_builder import _publication

    module = import_module("loom.personal_dev_native_attempt_executor")
    router_type = module.NativePersonalDevBuildExecutorRouter
    inputs = [await make_attempt_input(sessions, sealed_source, tmp_path) for _ in range(2)]
    failed_owner = inputs[0][0].candidate.owner_user_id if one_owner_fails else None
    async with sessions.begin() as session:
        await session.execute(update(PersonalDevCandidateBuildAttempt).values(state="queued", claimed_by=None,
            lease_expires_at=None, started_at=None))
    entered, release = set(), asyncio.Event()
    executors, publications = {}, []

    def executor_for(value):
        registration, _archive, member, runtime = value
        owner = registration.candidate.owner_user_id

        class Outcomes:
            async def observe(self, incoming, *, platform):
                assert incoming.candidate.owner_user_id == owner
                entered.add(owner)
                if len(entered) == 2:
                    release.set()
                await asyncio.wait_for(release.wait(), timeout=5)
                # Exercise coordinator heartbeats while both owners are waiting.
                await asyncio.sleep(0.12)
                return receipt_for(incoming, member, platform, result="failed" if owner == failed_owner else "artifact-ready")

        outcomes = Outcomes()

        class Exporter:
            accepted_artifact_resolver = outcomes

            async def publish(self, incoming):
                assert incoming.candidate.owner_user_id == owner
                publications.append(owner)
                return _publication(incoming.candidate)

        return module.NativePersonalDevBuildExecutor(session_factory=sessions, member=member, runtime=runtime,
            outcomes=outcomes, exporter=Exporter(), poll_interval_seconds=0.01, wait_timeout_seconds=10)

    for value in inputs:
        executors[value[0].candidate.owner_user_id] = executor_for(value)
    router = router_type(executors=executors)
    executors.clear()  # Caller mutation cannot remove/rebind in-flight ownership.

    @asynccontextmanager
    async def source(candidate):
        yield tmp_path / "sealed.tar"

    class Authority(SessionPersonalDevBuildAuthority):
        heartbeats = 0

        async def heartbeat_build(self, **kwargs):
            self.heartbeats += 1
            return await super().heartbeat_build(**kwargs)

    authority = Authority(sessions, limits=PersonalDevCandidateLimits())
    coordinators = [PersonalDevBuildCoordinator(authority=authority, source=source, executor=router,
        builder_id=f"native-coordinator-{index}", lease_seconds=10, heartbeat_interval_seconds=0.05) for index in range(2)]
    assert await asyncio.gather(*(item.build_once(now=datetime.now(UTC)) for item in coordinators)) == [True, True]
    assert authority.heartbeats >= 2 and len(entered) == 2
    assert set(publications) == entered - {failed_owner} and len(publications) == 2 - int(one_owner_fails)
    async with sessions() as session:
        attempts = (await session.scalars(select(PersonalDevCandidateBuildAttempt))).all()
        candidates = (await session.scalars(select(PersonalDevCandidate))).all()
        requests = (await session.scalars(select(PersonalDevBuildPlatformRequest))).all()
        assert len(attempts) == len(candidates) == 2 and len(requests) == 4
        candidate_owners = {row.id: row.owner_user_id for row in candidates}
        assert all(row.state == ("failed" if candidate_owners[row.candidate_id] == failed_owner else "succeeded")
            and row.lease_expires_at is None for row in attempts)
        assert all(row.status == ("failed" if row.owner_user_id == failed_owner else "ready") for row in candidates)
        assert all(row.cancelled_at is not None for row in requests)


@pytest.mark.parametrize("boundary", ["empty", "wrong-owner", "duplicate-subject", "unknown-owner"])
async def test_native_attempt_router_never_rebinds_or_falls_back(attempt_input, sessions, boundary):
    from types import SimpleNamespace

    module = import_module("loom.personal_dev_native_attempt_executor")
    registration, archive, member, runtime = attempt_input
    outcomes = object()
    executor = module.NativePersonalDevBuildExecutor(session_factory=sessions, member=member, runtime=runtime,
        outcomes=outcomes, exporter=SimpleNamespace(accepted_artifact_resolver=outcomes))
    mapping = {member.owner_id: executor}
    if boundary == "empty":
        mapping.clear()
    elif boundary == "wrong-owner":
        mapping = {uuid4(): executor}
    elif boundary == "duplicate-subject":
        owner = uuid4()
        other = member.model_copy(update={"owner_id": owner, "configuration": member.configuration.model_copy(
            update={"account_id": f"dev-owner-{owner.hex}"})})
        mapping[owner] = replace(executor, member=other)
    if boundary != "unknown-owner":
        with pytest.raises(ValueError, match="routing"):
            module.NativePersonalDevBuildExecutorRouter(executors=mapping)
        return
    router = module.NativePersonalDevBuildExecutorRouter(executors=mapping)
    foreign = replace(registration, candidate=replace(registration.candidate, owner_user_id=uuid4()))
    with pytest.raises(ValueError, match="installation is unavailable"):
        await router.build(foreign, source_archive=archive)
    with pytest.raises(ValueError, match="installation is unavailable"):
        await router.cleanup(foreign)
    async with sessions() as session:
        assert (await session.scalars(select(PersonalDevBuildPlatformRequest))).all() == []
