from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom.db.schema import (
    PersonalDevCandidate,
    PersonalDevCandidateBuildAttempt,
    PersonalDevNativeBuilderAgent,
    PersonalDevNativeBuildGrant,
    Team,
    User,
)
from loom.personal_dev_candidate import (
    CandidateRegistration,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateRecord,
)
from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_PLATFORM,
    NATIVE_BUILDER_PROVIDER,
    NativeBuilderAgentStatus,
    NativeBuilderCompletion,
    NativeBuilderHeartbeatRequest,
    NativeBuilderPollRequest,
    NativeBuilderRuntimeEvidence,
)
from loom.personal_dev_native_builder_store import (
    NativeBuilderArtifactHead,
    NativeBuilderGrantFencedError,
    NativeBuilderGrantPolicy,
    NativeBuilderPollResult,
    cancel_native_build_grant,
    complete_native_build_grant,
    get_native_build_grant,
    heartbeat_native_build_grant,
    issue_native_build_grant,
    poll_native_build_grant,
)

_NOW = datetime(2026, 8, 30, 18, 0, tzinfo=UTC)
_AGENT_ID = UUID("10000000-0000-0000-0000-000000000001")
_FOREIGN_AGENT_ID = UUID("10000000-0000-0000-0000-000000000002")
_AGENT_KEY_ID = "gb10-native-builder-v1"
_AGENT_IMAGE = "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64
_BUILDER_IMAGE = "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
_CONTRACT = '{"platform":"linux/arm64","schema_version":1}'
_CONTRACT_SHA256 = "6a70281ff4f91c00db4f7c1d0c0dadaead31b7a425fc1fb40dfb2c8a3b4bb714"


@pytest.fixture
async def sessions(
    isolated_migration_postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def _status(
    *,
    instance_id: UUID = _AGENT_ID,
    key_id: str = _AGENT_KEY_ID,
    managed: tuple[UUID, ...] = (),
    active: tuple[UUID, ...] = (),
    available: bool = True,
    agent_image: str = _AGENT_IMAGE,
) -> NativeBuilderAgentStatus:
    return NativeBuilderAgentStatus(
        agent_instance_id=instance_id,
        agent_key_id=key_id,
        provider=NATIVE_BUILDER_PROVIDER,
        platform=NATIVE_BUILDER_PLATFORM,
        protocol_version=1,
        host_name="gx10-01c7",
        host_architecture="aarch64",
        host_boot_id=UUID("10000000-0000-0000-0000-000000000003"),
        agent_image=agent_image,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        max_concurrency=2,
        managed_grant_ids=tuple(sorted(managed, key=str)),
        active_grant_ids=tuple(sorted(active, key=str)),
        available=available,
        unavailable_reason=None if available else "host_runtime_drift",
        readiness_evidence_sha256="d" * 64,
    )


def _poll(
    requested_at: datetime,
    *,
    instance_id: UUID = _AGENT_ID,
    key_id: str = _AGENT_KEY_ID,
    managed: tuple[UUID, ...] = (),
    active: tuple[UUID, ...] = (),
    available: bool = True,
    agent_image: str = _AGENT_IMAGE,
    nonce: UUID | None = None,
) -> NativeBuilderPollRequest:
    return NativeBuilderPollRequest(
        status=_status(
            instance_id=instance_id,
            key_id=key_id,
            managed=managed,
            active=active,
            available=available,
            agent_image=agent_image,
        ),
        requested_at=requested_at,
        request_nonce=nonce or uuid4(),
    )


def _policy(
    *,
    agent_id: UUID = _AGENT_ID,
    agent_image: str = _AGENT_IMAGE,
) -> NativeBuilderGrantPolicy:
    return NativeBuilderGrantPolicy(
        agent_instance_id=agent_id,
        agent_key_id=_AGENT_KEY_ID,
        agent_image=agent_image,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        contract_json=_CONTRACT,
        contract_sha256=_CONTRACT_SHA256,
        artifact_max_bytes=8 * 1024 * 1024 * 1024,
        active_deadline_seconds=3600,
    )


async def _register_agent(
    sessions: async_sessionmaker[AsyncSession],
    *,
    instance_id: UUID = _AGENT_ID,
    key_id: str = _AGENT_KEY_ID,
    now: datetime = _NOW,
) -> None:
    async with sessions() as session:
        result = await poll_native_build_grant(
            session,
            _poll(now, instance_id=instance_id, key_id=key_id),
            now,
        )
    assert result.grant is None
    assert result.cancel_grant_ids == ()


async def _seed_running_attempt(
    sessions: async_sessionmaker[AsyncSession],
    *,
    now: datetime,
) -> CandidateRegistration:
    owner_id = uuid4()
    team_id = uuid4()
    candidate_id = uuid4()
    attempt_id = uuid4()
    operation_id = uuid4()
    subject_id = uuid4()
    incarnation = uuid4()
    candidate_sha = uuid4().hex * 2
    archive_sha = uuid4().hex * 2
    source_sha = uuid4().hex * 2
    object_key = (
        f"personal-dev/sources/{team_id}/{owner_id}/{candidate_sha}/"
        f"{candidate_id}/{archive_sha}.tar"
    )
    candidate = PersonalDevCandidateRecord(
        id=candidate_id,
        owner_user_id=owner_id,
        owner_team_id=team_id,
        candidate_sha=candidate_sha,
        source_sha256=source_sha,
        archive_sha256=archive_sha,
        build_contract_sha256="4" * 64,
        source_commit="5" * 40,
        dirty=True,
        manifest_json={"attestation_scope": "personal-dev-only", "schema_version": 1},
        object_bucket="artifacts",
        object_key=object_key,
        source_generation_id=candidate_id,
        archive_size_bytes=4096,
        status="building",
        created_at=now,
        updated_at=now,
    )
    attempt = PersonalDevCandidateBuildAttemptRecord(
        id=attempt_id,
        candidate_id=candidate_id,
        subject_id=subject_id,
        subject_incarnation=incarnation,
        operation_id=operation_id,
        operation_epoch=3,
        attempt_sequence=2,
        state="running",
        lease_epoch=7,
        claimed_by="personal-builder-test",
        lease_expires_at=now + timedelta(minutes=30),
        created_at=now,
        updated_at=now,
        started_at=now,
    )
    async with sessions() as session:
        session.add(Team(id=team_id, name=f"native-store-{team_id}"))
        session.add(
            User(
                id=owner_id,
                email=f"{owner_id}@example.test",
                username=f"native-store-{owner_id}",
                username_normalized=f"native-store-{owner_id}",
                status="active",
            )
        )
        await session.flush()
        session.add(
            PersonalDevCandidate(
                id=candidate.id,
                owner_user_id=candidate.owner_user_id,
                owner_team_id=candidate.owner_team_id,
                candidate_sha=candidate.candidate_sha,
                source_sha256=candidate.source_sha256,
                archive_sha256=candidate.archive_sha256,
                build_contract_sha256=candidate.build_contract_sha256,
                source_commit=candidate.source_commit,
                dirty=candidate.dirty,
                manifest_json=dict(candidate.manifest_json),
                object_bucket=candidate.object_bucket,
                object_key=candidate.object_key,
                source_generation_id=candidate.source_generation_id,
                archive_size_bytes=candidate.archive_size_bytes,
                status="building",
                created_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        session.add(
            PersonalDevCandidateBuildAttempt(
                id=attempt.id,
                candidate_id=attempt.candidate_id,
                subject_id=attempt.subject_id,
                subject_incarnation=attempt.subject_incarnation,
                operation_id=attempt.operation_id,
                operation_epoch=attempt.operation_epoch,
                attempt_sequence=attempt.attempt_sequence,
                state="running",
                lease_epoch=attempt.lease_epoch,
                claimed_by=attempt.claimed_by,
                lease_expires_at=attempt.lease_expires_at,
                created_at=now,
                updated_at=now,
                started_at=now,
            )
        )
        await session.commit()
    return CandidateRegistration(candidate=candidate, build_attempt=attempt, created=False)


def _heartbeat(
    registration: CandidateRegistration,
    grant_id: UUID,
    requested_at: datetime,
    *,
    nonce: UUID | None = None,
) -> NativeBuilderHeartbeatRequest:
    attempt = registration.build_attempt
    assert attempt is not None
    return NativeBuilderHeartbeatRequest(
        agent_instance_id=_AGENT_ID,
        agent_key_id=_AGENT_KEY_ID,
        grant_id=grant_id,
        attempt_id=attempt.id,
        attempt_lease_epoch=attempt.lease_epoch,
        requested_at=requested_at,
        request_nonce=nonce or uuid4(),
    )


def _evidence(
    registration: CandidateRegistration,
    grant_id: UUID,
    *,
    network_digest: str = "6" * 64,
) -> NativeBuilderRuntimeEvidence:
    attempt = registration.build_attempt
    assert attempt is not None
    return NativeBuilderRuntimeEvidence(
        agent_instance_id=_AGENT_ID,
        grant_id=grant_id,
        attempt_id=attempt.id,
        attempt_lease_epoch=attempt.lease_epoch,
        provider=NATIVE_BUILDER_PROVIDER,
        platform=NATIVE_BUILDER_PLATFORM,
        host_name="gx10-01c7",
        host_architecture="aarch64",
        host_boot_id=UUID("10000000-0000-0000-0000-000000000003"),
        agent_image=_AGENT_IMAGE,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        contract_sha256=_CONTRACT_SHA256,
        runtime_name="runsc-personal-dev-native",
        client_container_id="1" * 64,
        buildkit_container_id="2" * 64,
        network_id="3" * 64,
        client_inspect_sha256="4" * 64,
        buildkit_inspect_sha256="5" * 64,
        network_inspect_sha256=network_digest,
        client_exit_code=0,
        client_oom_killed=False,
        client_restart_count=0,
        buildkit_restart_count=0,
        buildkit_running=True,
        observed_at=_NOW,
    )


def _completion(
    registration: CandidateRegistration,
    grant_id: UUID,
    requested_at: datetime,
    *,
    outcome: str = "succeeded",
    evidence: NativeBuilderRuntimeEvidence | None = None,
    nonce: UUID | None = None,
) -> NativeBuilderCompletion:
    attempt = registration.build_attempt
    assert attempt is not None
    success = outcome == "succeeded"
    return NativeBuilderCompletion(
        agent_instance_id=_AGENT_ID,
        agent_key_id=_AGENT_KEY_ID,
        grant_id=grant_id,
        attempt_id=attempt.id,
        attempt_lease_epoch=attempt.lease_epoch,
        outcome="succeeded" if success else "failed",
        failure_reason=None if success else "client_exit_nonzero",
        evidence=(evidence or _evidence(registration, grant_id)) if success else None,
        requested_at=requested_at,
        request_nonce=nonce or uuid4(),
    )


def _head(registration: CandidateRegistration, grant: PersonalDevNativeBuildGrant) -> NativeBuilderArtifactHead:
    attempt = registration.build_attempt
    assert attempt is not None
    return NativeBuilderArtifactHead(
        bucket=grant.artifact_bucket,
        object_key=grant.artifact_object_key,
        content_type="application/vnd.loom.personal-dev-build.v1+tar",
        size_bytes=1024,
        metadata={
            "attestation-scope": "personal-dev-only",
            "build-attempt-id": str(attempt.id),
            "build-lease-epoch": str(attempt.lease_epoch),
            "candidate-sha256": registration.candidate.candidate_sha,
            "platform": NATIVE_BUILDER_PLATFORM,
        },
    )


async def _issue_and_claim(
    sessions: async_sessionmaker[AsyncSession],
    registration: CandidateRegistration,
    *,
    now: datetime,
) -> PersonalDevNativeBuildGrant:
    async with sessions() as session:
        issued = await issue_native_build_grant(session, registration, _policy(), now)
    async with sessions() as session:
        result = await poll_native_build_grant(session, _poll(now + timedelta(seconds=1)), now)
    assert result.grant is not None
    assert result.grant.id == issued.id
    return result.grant


@pytest.mark.asyncio
async def test_poll_registers_exact_agent_and_rejects_replay_or_key_reassignment(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """A replay or same-key foreign instance must not overwrite signed readiness."""
    request = _poll(_NOW)
    async with sessions() as session:
        first = await poll_native_build_grant(session, request, _NOW)
    assert first.grant is None

    async with sessions() as session:
        row = await session.get(PersonalDevNativeBuilderAgent, _AGENT_ID)
        assert row is not None
        assert row.status_sha256 == hashlib.sha256(request.status.canonical_bytes()).hexdigest()
        assert row.active_grant_ids_json == []

    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="replay"):
            await poll_native_build_grant(session, request, _NOW)
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="monotonic"):
            await poll_native_build_grant(
                session,
                _poll(_NOW - timedelta(seconds=1)),
                _NOW,
            )
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="identity"):
            await poll_native_build_grant(
                session,
                _poll(
                    _NOW + timedelta(seconds=1),
                    instance_id=_FOREIGN_AGENT_ID,
                ),
                _NOW,
            )


@pytest.mark.asyncio
async def test_issue_is_one_writer_idempotent_and_rejects_policy_or_parent_drift(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """One attempt/lease/platform must produce one immutable grant policy."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)

    async def issue_once() -> UUID:
        async with sessions() as session:
            return (await issue_native_build_grant(session, registration, _policy(), _NOW)).id

    first_id, second_id = await asyncio.gather(issue_once(), issue_once())
    assert first_id == second_id
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="policy"):
            await issue_native_build_grant(
                session,
                registration,
                replace(_policy(), runtime_profile_sha256="9" * 64),
                _NOW,
            )

    later = await _seed_running_attempt(sessions, now=_NOW + timedelta(minutes=1))
    later_attempt = later.build_attempt
    assert later_attempt is not None
    async with sessions() as session:
        await session.execute(
            update(PersonalDevCandidateBuildAttempt)
            .where(PersonalDevCandidateBuildAttempt.id == later_attempt.id)
            .values(lease_expires_at=_NOW)
        )
        await session.commit()
    expired = replace(
        later,
        build_attempt=replace(later_attempt, lease_expires_at=_NOW),
    )
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="parent"):
            await issue_native_build_grant(session, expired, _policy(), _NOW)


@pytest.mark.asyncio
async def test_issue_rejects_forged_candidate_owner_binding(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Caller-supplied owner fields must not redirect the deterministic artifact key."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)
    forged = replace(
        registration,
        candidate=replace(registration.candidate, owner_user_id=uuid4()),
    )
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="candidate"):
            await issue_native_build_grant(session, forged, _policy(), _NOW)


@pytest.mark.asyncio
async def test_database_rejects_running_grant_reassignment(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """A direct writer cannot move a queued grant to a foreign agent."""
    await _register_agent(sessions)
    await _register_agent(
        sessions,
        instance_id=_FOREIGN_AGENT_ID,
        key_id="gb10-native-builder-foreign-v1",
    )
    registration = await _seed_running_attempt(sessions, now=_NOW)
    async with sessions() as session:
        grant = await issue_native_build_grant(session, registration, _policy(), _NOW)
    async with sessions() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                update(PersonalDevNativeBuildGrant)
                .where(PersonalDevNativeBuildGrant.id == grant.id)
                .values(
                    state="running",
                    running_agent_instance_id=_FOREIGN_AGENT_ID,
                    started_at=_NOW,
                    heartbeat_at=_NOW,
                    updated_at=_NOW,
                )
            )
            await session.commit()


@pytest.mark.asyncio
async def test_issue_does_not_deadlock_with_poll_waiting_on_the_same_parent(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Issuance must not invert poll's agent-to-parent row-lock order."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)
    attempt = registration.build_attempt
    assert attempt is not None
    async with sessions() as session:
        issued = await issue_native_build_grant(session, registration, _policy(), _NOW)

    async with (
        sessions() as issue_session,
        sessions() as poll_session,
        sessions() as observer_session,
    ):
        await issue_session.execute(
            select(PersonalDevCandidateBuildAttempt)
            .where(PersonalDevCandidateBuildAttempt.id == attempt.id)
            .with_for_update()
        )
        await poll_session.execute(
            select(PersonalDevNativeBuilderAgent)
            .where(PersonalDevNativeBuilderAgent.instance_id == _AGENT_ID)
            .with_for_update()
        )
        poll_pid = await poll_session.scalar(select(func.pg_backend_pid()))
        assert isinstance(poll_pid, int)
        poll_task = asyncio.create_task(
            poll_native_build_grant(
                poll_session,
                _poll(_NOW + timedelta(seconds=1)),
                _NOW + timedelta(seconds=1),
            )
        )
        for _ in range(500):
            wait_event_type = await observer_session.scalar(
                text(
                    "SELECT wait_event_type FROM pg_stat_activity "
                    "WHERE pid = :poll_pid"
                ),
                {"poll_pid": poll_pid},
            )
            if wait_event_type == "Lock":
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("native builder poll did not block on the locked parent")

        issue_task = asyncio.create_task(
            issue_native_build_grant(issue_session, registration, _policy(), _NOW)
        )
        issue_result, poll_result = await asyncio.wait_for(
            asyncio.gather(issue_task, poll_task, return_exceptions=True),
            timeout=5,
        )

    assert isinstance(issue_result, PersonalDevNativeBuildGrant), repr(issue_result)
    assert issue_result.id == issued.id
    assert isinstance(poll_result, NativeBuilderPollResult), repr(poll_result)
    assert poll_result.grant is not None
    assert poll_result.grant.id == issued.id


@pytest.mark.asyncio
async def test_poll_claims_fifo_at_two_slots_resumes_same_agent_and_denies_foreign_agent(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """The agent may hold two FIFO grants; no third or foreign claim is possible."""
    await _register_agent(sessions)
    registrations = [
        await _seed_running_attempt(sessions, now=_NOW + timedelta(seconds=index))
        for index in range(3)
    ]
    issued: list[PersonalDevNativeBuildGrant] = []
    for index, registration in enumerate(registrations):
        async with sessions() as session:
            issued.append(
                await issue_native_build_grant(
                    session,
                    registration,
                    _policy(),
                    _NOW + timedelta(seconds=index),
                )
            )

    async with sessions() as session:
        first = await poll_native_build_grant(
            session,
            _poll(_NOW + timedelta(seconds=10)),
            _NOW + timedelta(seconds=10),
        )
    assert first.grant is not None and first.grant.id == issued[0].id
    async with sessions() as session:
        second = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=11),
                managed=(issued[0].id,),
                active=(issued[0].id,),
            ),
            _NOW + timedelta(seconds=11),
        )
    assert second.grant is not None and second.grant.id == issued[1].id
    async with sessions() as session:
        full = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=12),
                managed=(issued[0].id, issued[1].id),
                active=(issued[0].id, issued[1].id),
            ),
            _NOW + timedelta(seconds=12),
        )
    assert full.grant is not None and full.grant.id == issued[0].id

    async with sessions() as session:
        resumed = await poll_native_build_grant(
            session,
            _poll(_NOW + timedelta(seconds=13)),
            _NOW + timedelta(seconds=13),
        )
    assert resumed.grant is not None and resumed.grant.id == issued[0].id

    await _register_agent(
        sessions,
        instance_id=_FOREIGN_AGENT_ID,
        key_id="gb10-native-builder-foreign-v1",
        now=_NOW + timedelta(seconds=20),
    )
    async with sessions() as session:
        foreign = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=21),
                instance_id=_FOREIGN_AGENT_ID,
                key_id="gb10-native-builder-foreign-v1",
            ),
            _NOW + timedelta(seconds=21),
        )
    assert foreign.grant is None
    async with sessions() as session:
        third = await session.get(PersonalDevNativeBuildGrant, issued[2].id)
        assert third is not None and third.state == "queued"


@pytest.mark.asyncio
async def test_full_agent_restart_redelivers_both_running_grants_by_oldest_heartbeat(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """A restarted same-instance agent can recover both occupied grant payloads."""
    await _register_agent(sessions)
    registrations = [
        await _seed_running_attempt(sessions, now=_NOW + timedelta(seconds=index))
        for index in range(2)
    ]
    issued: list[PersonalDevNativeBuildGrant] = []
    for index, registration in enumerate(registrations):
        async with sessions() as session:
            issued.append(
                await issue_native_build_grant(
                    session,
                    registration,
                    _policy(),
                    _NOW + timedelta(seconds=index),
                )
            )

    async with sessions() as session:
        first = await poll_native_build_grant(
            session,
            _poll(_NOW + timedelta(seconds=10)),
            _NOW + timedelta(seconds=10),
        )
    assert first.grant is not None and first.grant.id == issued[0].id
    async with sessions() as session:
        second = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=11),
                managed=(issued[0].id,),
                active=(issued[0].id,),
            ),
            _NOW + timedelta(seconds=11),
        )
    assert second.grant is not None and second.grant.id == issued[1].id

    full_inventory = (issued[0].id, issued[1].id)
    async with sessions() as session:
        recovered_first = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=12),
                managed=full_inventory,
                active=full_inventory,
            ),
            _NOW + timedelta(seconds=12),
        )
    assert recovered_first.grant is not None
    assert recovered_first.grant.id == issued[0].id

    async with sessions() as session:
        assert await heartbeat_native_build_grant(
            session,
            _heartbeat(
                registrations[0],
                issued[0].id,
                _NOW + timedelta(seconds=13),
            ),
            _NOW + timedelta(seconds=13),
        )
    async with sessions() as session:
        recovered_second = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=14),
                managed=full_inventory,
                active=full_inventory,
            ),
            _NOW + timedelta(seconds=14),
        )
    assert recovered_second.grant is not None
    assert recovered_second.grant.id == issued[1].id


@pytest.mark.asyncio
async def test_poll_counts_drifted_running_grants_before_claiming_new_policy_work(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Release drift must not let central running authority exceed two grants."""
    await _register_agent(sessions)
    registrations = [
        await _seed_running_attempt(sessions, now=_NOW + timedelta(seconds=index))
        for index in range(4)
    ]
    issued: list[PersonalDevNativeBuildGrant] = []
    for index, registration in enumerate(registrations[:3]):
        async with sessions() as session:
            issued.append(
                await issue_native_build_grant(
                    session,
                    registration,
                    _policy(),
                    _NOW + timedelta(seconds=index),
                )
            )
    async with sessions() as session:
        first = await poll_native_build_grant(
            session,
            _poll(_NOW + timedelta(seconds=10)),
            _NOW + timedelta(seconds=10),
        )
    assert first.grant is not None
    async with sessions() as session:
        second = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=11),
                managed=(first.grant.id,),
                active=(first.grant.id,),
            ),
            _NOW + timedelta(seconds=11),
        )
    assert second.grant is not None

    next_agent_image = (
        "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "e" * 64
    )
    async with sessions() as session:
        drift = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=12),
                agent_image=next_agent_image,
            ),
            _NOW + timedelta(seconds=12),
        )
    assert drift.grant is None
    async with sessions() as session:
        current = await issue_native_build_grant(
            session,
            registrations[3],
            _policy(agent_image=next_agent_image),
            _NOW + timedelta(seconds=12),
        )
    async with sessions() as session:
        result = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=13),
                agent_image=next_agent_image,
            ),
            _NOW + timedelta(seconds=13),
        )
    assert result.grant is None
    async with sessions() as session:
        current_row = await session.get(PersonalDevNativeBuildGrant, current.id)
        running_count = len(
            (
                await session.execute(
                    select(PersonalDevNativeBuildGrant).where(
                        PersonalDevNativeBuildGrant.running_agent_instance_id == _AGENT_ID,
                        PersonalDevNativeBuildGrant.state == "running",
                    )
                )
            ).scalars().all()
        )
    assert current_row is not None and current_row.state == "queued"
    assert running_count == 2


@pytest.mark.asyncio
async def test_heartbeat_is_monotonic_and_cancels_when_parent_lease_is_lost(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """A replay cannot extend liveness and a stale parent cannot continue."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)
    grant = await _issue_and_claim(sessions, registration, now=_NOW)
    heartbeat = _heartbeat(registration, grant.id, _NOW + timedelta(seconds=2))

    async with sessions() as session:
        assert await heartbeat_native_build_grant(
            session,
            heartbeat,
            _NOW + timedelta(seconds=2),
        )
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="replay"):
            await heartbeat_native_build_grant(
                session,
                heartbeat,
                _NOW + timedelta(seconds=3),
            )

    attempt = registration.build_attempt
    assert attempt is not None
    async with sessions() as session:
        await session.execute(
            update(PersonalDevCandidateBuildAttempt)
            .where(PersonalDevCandidateBuildAttempt.id == attempt.id)
            .values(lease_expires_at=_NOW + timedelta(seconds=3))
        )
        await session.commit()
    async with sessions() as session:
        assert not await heartbeat_native_build_grant(
            session,
            _heartbeat(registration, grant.id, _NOW + timedelta(seconds=4)),
            _NOW + timedelta(seconds=4),
        )
    async with sessions() as session:
        cancelled = await session.get(PersonalDevNativeBuildGrant, grant.id)
        assert cancelled is not None
        assert cancelled.state == "cancelled"
        assert cancelled.failure_reason == "coordinator_lease_lost"


@pytest.mark.asyncio
async def test_success_completion_requires_exact_head_and_is_semantically_idempotent(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Only the bound object may succeed; a fresh exact retry preserves first evidence."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)
    grant = await _issue_and_claim(sessions, registration, now=_NOW)
    completion = _completion(
        registration,
        grant.id,
        _NOW + timedelta(seconds=2),
    )
    wrong_head = replace(
        _head(registration, grant),
        metadata={**dict(_head(registration, grant).metadata), "platform": "linux/amd64"},
    )
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="artifact"):
            await complete_native_build_grant(
                session,
                completion,
                _NOW + timedelta(seconds=2),
                wrong_head,
            )

    async with sessions() as session:
        succeeded = await complete_native_build_grant(
            session,
            completion,
            _NOW + timedelta(seconds=2),
            _head(registration, grant),
        )
    assert succeeded.state == "succeeded"
    assert succeeded.runtime_evidence_sha256 == hashlib.sha256(
        completion.evidence.canonical_bytes()  # type: ignore[union-attr]
    ).hexdigest()
    original_completion = dict(succeeded.completion_json or {})

    retry = replace(
        completion,
        requested_at=_NOW + timedelta(seconds=3),
        request_nonce=uuid4(),
    )
    async with sessions() as session:
        repeated = await complete_native_build_grant(
            session,
            retry,
            _NOW + timedelta(seconds=3),
            _head(registration, grant),
        )
    assert repeated.id == succeeded.id
    assert repeated.completion_json == original_completion

    changed = replace(
        retry,
        requested_at=_NOW + timedelta(seconds=4),
        request_nonce=uuid4(),
        evidence=_evidence(registration, grant.id, network_digest="7" * 64),
    )
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="completion"):
            await complete_native_build_grant(
                session,
                changed,
                _NOW + timedelta(seconds=4),
                _head(registration, grant),
            )


@pytest.mark.asyncio
async def test_success_completion_rejects_runtime_evidence_policy_drift(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Signed runtime evidence must describe the exact immutable grant policy."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)
    grant = await _issue_and_claim(sessions, registration, now=_NOW)
    evidence = _evidence(registration, grant.id)
    drifted_evidence = (
        replace(
            evidence,
            agent_image="ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
            + "e" * 64,
        ),
        replace(
            evidence,
            builder_image="ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:"
            + "e" * 64,
        ),
        replace(evidence, runtime_profile_sha256="e" * 64),
        replace(evidence, contract_sha256="e" * 64),
    )
    for index, drifted in enumerate(drifted_evidence, start=2):
        completion = _completion(
            registration,
            grant.id,
            _NOW + timedelta(seconds=index),
            evidence=drifted,
        )
        async with sessions() as session:
            with pytest.raises(NativeBuilderGrantFencedError, match="evidence"):
                await complete_native_build_grant(
                    session,
                    completion,
                    _NOW + timedelta(seconds=index),
                    _head(registration, grant),
                )


@pytest.mark.asyncio
async def test_failure_cancel_and_stale_completion_preserve_terminal_evidence(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Signed failure is immutable; cancellation and stale success cannot revive work."""
    await _register_agent(sessions)
    failed_registration = await _seed_running_attempt(sessions, now=_NOW)
    failed_grant = await _issue_and_claim(sessions, failed_registration, now=_NOW)
    failure = _completion(
        failed_registration,
        failed_grant.id,
        _NOW + timedelta(seconds=2),
        outcome="failed",
    )
    async with sessions() as session:
        failed = await complete_native_build_grant(
            session,
            failure,
            _NOW + timedelta(seconds=2),
            None,
        )
    assert failed.state == "failed"
    assert failed.failure_reason == "client_exit_nonzero"
    assert failed.completion_json == json.loads(failure.canonical_bytes())

    queued_registration = await _seed_running_attempt(
        sessions,
        now=_NOW + timedelta(minutes=1),
    )
    async with sessions() as session:
        queued = await issue_native_build_grant(
            session,
            queued_registration,
            _policy(),
            _NOW + timedelta(minutes=1),
        )
    queued_attempt = queued_registration.build_attempt
    assert queued_attempt is not None
    async with sessions() as session:
        assert await cancel_native_build_grant(
            session,
            queued_attempt.id,
            queued_attempt.lease_epoch,
            NATIVE_BUILDER_PLATFORM,
            _NOW + timedelta(minutes=1, seconds=1),
        )
    async with sessions() as session:
        assert not await cancel_native_build_grant(
            session,
            queued_attempt.id,
            queued_attempt.lease_epoch,
            NATIVE_BUILDER_PLATFORM,
            _NOW + timedelta(minutes=1, seconds=2),
        )
        cancelled = await get_native_build_grant(
            session,
            queued_attempt.id,
            queued_attempt.lease_epoch,
            NATIVE_BUILDER_PLATFORM,
        )
    assert cancelled is not None and cancelled.id == queued.id
    assert cancelled.state == "cancelled"

    stale_registration = await _seed_running_attempt(
        sessions,
        now=_NOW + timedelta(minutes=2),
    )
    stale_grant = await _issue_and_claim(
        sessions,
        stale_registration,
        now=_NOW + timedelta(minutes=2),
    )
    stale_attempt = stale_registration.build_attempt
    assert stale_attempt is not None
    async with sessions() as session:
        await session.execute(
            update(PersonalDevCandidateBuildAttempt)
            .where(PersonalDevCandidateBuildAttempt.id == stale_attempt.id)
            .values(lease_epoch=stale_attempt.lease_epoch + 1)
        )
        await session.commit()
    async with sessions() as session:
        with pytest.raises(NativeBuilderGrantFencedError, match="parent"):
            await complete_native_build_grant(
                session,
                _completion(
                    stale_registration,
                    stale_grant.id,
                    _NOW + timedelta(minutes=2, seconds=2),
                ),
                _NOW + timedelta(minutes=2, seconds=2),
                _head(stale_registration, stale_grant),
            )
    async with sessions() as session:
        stale = await session.get(PersonalDevNativeBuildGrant, stale_grant.id)
        assert stale is not None and stale.state == "cancelled"


@pytest.mark.asyncio
async def test_poll_returns_exact_cancellations_for_unknown_and_terminal_inventory(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """The agent receives cancellation for every reported object not centrally current."""
    await _register_agent(sessions)
    registration = await _seed_running_attempt(sessions, now=_NOW)
    grant = await _issue_and_claim(sessions, registration, now=_NOW)
    attempt = registration.build_attempt
    assert attempt is not None
    async with sessions() as session:
        await cancel_native_build_grant(
            session,
            attempt.id,
            attempt.lease_epoch,
            NATIVE_BUILDER_PLATFORM,
            _NOW + timedelta(seconds=2),
        )
    unknown = uuid4()
    async with sessions() as session:
        result = await poll_native_build_grant(
            session,
            _poll(
                _NOW + timedelta(seconds=3),
                managed=(grant.id, unknown),
                active=(),
            ),
            _NOW + timedelta(seconds=3),
        )
    assert result.cancel_grant_ids == tuple(sorted((grant.id, unknown), key=str))
    assert result.grant is None
