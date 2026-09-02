from __future__ import annotations

import hashlib
import json
import traceback
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildGrantEvent,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
)
from loom.security.secret_store import SecretStore
from loom_control_plane.task_image_build_environment import (
    RootlessBuildResourceRequestV1,
    SlurmBuildEnvironmentPolicyV1,
    SlurmBuildInventoryV1,
    SlurmBuildJobObservationV1,
    canonical_request_sha256,
    issue_slurm_build_grant,
)
from loom_control_plane.task_image_build_grants import (
    begin_task_image_build_submission,
    issue_task_image_build_grant,
    reconcile_task_image_build_submission,
    record_task_image_build_release,
)
from loom_task_image_authority.contracts import (
    TaskImageAttachmentProofV1,
    TaskImageBootstrapExchangeV1,
    TaskImageBuildGrantAuthorityV1,
    TaskImageBuildSessionV1,
    TaskImageContainmentAttachmentV1,
    TaskImageContainmentAttestationV1,
    TaskImageGuardPrincipalV1,
    TaskImageProjectionChallengeV1,
    TaskImageProjectionRequestV1,
    canonical_authority_sha256,
    canonical_public_binding_sha256,
)
from loom_task_image_authority.store import (
    TaskImageProjectionAuthorizationError,
    TaskImageProjectionConflictError,
    TaskImageProjectionExpiredError,
    authorize_task_image_build_session,
    complete_task_image_projection,
    exchange_task_image_bootstrap,
    expire_task_image_projection,
    record_task_image_containment_attestation,
    request_task_image_projection,
    revoke_task_image_projection,
)

NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")
REQUEST_ID = UUID("22222222-2222-2222-2222-222222222222")
CHALLENGE_NONCE = UUID("33333333-3333-3333-3333-333333333333")
PROOF_ID = UUID("44444444-4444-4444-4444-444444444444")
EXCHANGE_ID = UUID("55555555-5555-5555-5555-555555555555")
NODE_BOOT_ID = UUID("77777777-7777-7777-7777-777777777777")
ATTESTATION_ID = UUID("88888888-8888-8888-8888-888888888888")
SUPERVISOR_SHA256 = "6" * 64
CGROUP_PATH = "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_12345/step_batch"


def _policy() -> SlurmBuildEnvironmentPolicyV1:
    return SlurmBuildEnvironmentPolicyV1(
        schema="loom.task-image-build-environment-policy/v1",
        enabled=False,
        activation_blockers=("guard_missing",),
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        submitting_identity="loom-builder",
        partition="loom-task-builder",
        account="loom-task-builder",
        qos="loom-task-image-builder-rootless-gb10",
        feature_constraint="loom_rootless_buildkit",
        supervisor_path="/usr/local/libexec/loom-task-builder-supervisor",
        sbatch_path="/usr/bin/sbatch",
        resources=RootlessBuildResourceRequestV1(
            cpus=8,
            memory_mib=32768,
            pids=4096,
            scratch_bytes=107374182400,
            scratch_inodes=1000000,
            wall_time="02:00:00",
            swap_bytes=0,
        ),
    )


def _grant(*, expires_at: datetime = NOW + timedelta(hours=2)):
    policy = _policy()
    authority = TaskImageBuildGrantAuthorityV1(
        purpose="production",
        shadow_campaign_id=None,
        environment="staging",
        pool_id="staging-gb10-task-image",
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        slurm_request_sha256=canonical_request_sha256(policy.request_identity()),
        builder_release_sha256=SUPERVISOR_SHA256,
        build_policy_sha256="3" * 64,
        containment_policy_sha256="4" * 64,
        resource_profile_sha256="5" * 64,
        issued_at=NOW - timedelta(minutes=1),
        expires_at=expires_at,
    )
    return issue_slurm_build_grant(
        policy,
        grant_id=GRANT_ID,
        authority=authority,
    )


def _principal(**changes: object) -> TaskImageGuardPrincipalV1:
    values: dict[str, object] = {
        "principal_id": "gb10-trt-gb10-1",
        "slurm_cluster_id": "gb10",
        "node_name": "trt-gb10-1",
        "scopes": ("task-image:project", "task-image:attest"),
    }
    values.update(changes)
    return TaskImageGuardPrincipalV1.model_validate(values)


def _request(**changes: object) -> TaskImageProjectionRequestV1:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "grant_id": GRANT_ID,
        "observed_at": NOW + timedelta(seconds=3),
        "node_name": "trt-gb10-1",
        "node_boot_id": NODE_BOOT_ID,
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "supervisor_pid": 42100,
        "supervisor_uid": 993,
        "supervisor_gid": 980,
        "supervisor_executable_sha256": SUPERVISOR_SHA256,
        "cgroup_path": CGROUP_PATH,
        "cgroup_inode": 987654,
        "submitting_identity": "loom-builder",
        "slurm_account": "loom-task-builder",
        "slurm_partition": "loom-task-builder",
        "slurm_qos": "loom-task-image-builder-rootless-gb10",
        "cpu_arch": "arm64",
        "slurm_request_sha256": canonical_request_sha256(_policy().request_identity()),
    }
    values.update(changes)
    return TaskImageProjectionRequestV1.model_validate(values)


def _attachment(**changes: object) -> TaskImageContainmentAttachmentV1:
    root = f"{CGROUP_PATH}/loom-builder"
    values: dict[str, object] = {
        "cgroup_inode": 987654,
        "containment_root": root,
        "trusted_service_cgroup": f"{root}/trusted-service",
        "build_egress_cgroup": f"{root}/build-egress",
        "bpf_program_sha256": "7" * 64,
        "bpf_map_schema_sha256": "8" * 64,
        "containment_policy_sha256": "4" * 64,
        "resource_limits_sha256": "5" * 64,
        "probe_sha256": "9" * 64,
        "link_ids": (101, 102, 103),
        "program_ids": (201, 202, 203),
        "map_ids": (301, 302),
    }
    values.update(changes)
    return TaskImageContainmentAttachmentV1.model_validate(values)


def _proof(*, challenge_nonce: UUID = CHALLENGE_NONCE, **changes: object):
    values: dict[str, object] = {
        "proof_id": PROOF_ID,
        "grant_id": GRANT_ID,
        "request_id": REQUEST_ID,
        "request_sha256": canonical_authority_sha256(_request()),
        "challenge_nonce": challenge_nonce,
        "observed_at": NOW + timedelta(seconds=5),
        "node_name": "trt-gb10-1",
        "node_boot_id": NODE_BOOT_ID,
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "cgroup_path": CGROUP_PATH,
        "cgroup_inode": 987654,
        "attachment": _attachment(),
        "attestation_generation": 1,
        "attestation_expires_at": NOW + timedelta(seconds=40),
    }
    values.update(changes)
    return TaskImageAttachmentProofV1.model_validate(values)


async def _release_grant(
    session: AsyncSession,
    *,
    expires_at: datetime = NOW + timedelta(hours=2),
):
    grant = _grant(expires_at=expires_at)
    await issue_task_image_build_grant(
        session,
        environment="staging",
        grant=grant,
        ambiguity_settle_seconds=30,
        now=NOW,
    )
    await begin_task_image_build_submission(session, grant_id=GRANT_ID, now=NOW)
    inventory = SlurmBuildInventoryV1(
        controller_authoritative=True,
        accounting_authoritative=True,
        observed_at=NOW + timedelta(seconds=1),
        jobs=(
            SlurmBuildJobObservationV1(
                job_id="12345",
                state="pending",
                held=True,
                comment=grant.comment,
                submitting_identity="loom-builder",
                request=grant.request,
            ),
        ),
    )
    await reconcile_task_image_build_submission(
        session,
        grant_id=GRANT_ID,
        inventory=inventory,
        now=NOW + timedelta(seconds=1),
    )
    await record_task_image_build_release(
        session,
        grant_id=GRANT_ID,
        job_id="12345",
        now=NOW + timedelta(seconds=2),
    )
    return grant


class _MemorySecretStore:
    def __init__(self, *, fail_put: bool = False) -> None:
        self.fail_put = fail_put
        self.values: dict[str, str] = {}
        self.put_count = 0

    async def put(self, *, namespace: str, value: str) -> str:
        if self.fail_put:
            raise RuntimeError("synthetic secret-store failure")
        self.put_count += 1
        ref = f"loom://{namespace}/{uuid4()}"
        self.values[ref] = value
        return ref

    async def get(self, ref: str) -> str:
        return self.values[ref]

    async def delete(self, ref: str) -> None:
        self.values.pop(ref, None)

    async def list_refs(self, *, namespace: str | None = None) -> AsyncIterator[str]:
        prefix = f"loom://{namespace}/" if namespace is not None else None
        for ref in self.values:
            if prefix is None or ref.startswith(prefix):
                yield ref

    async def rewrap(self, ref: str, *, new_master_key: bytes) -> str:
        del new_master_key
        return ref


@pytest.fixture
async def projection_session(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        async with factory() as session:
            await session.execute(delete(TaskImageBuildContainmentAttestation))
            await session.execute(delete(TaskImageBuildProjectionEvent))
            await session.execute(delete(TaskImageBuildProjection))
            await session.execute(delete(TaskImageBuildGrantEvent))
            await session.execute(delete(TaskImageBuildGrant))
            await session.commit()
        await engine.dispose()


async def test_challenge_is_durable_exactly_replayable_and_conflict_bound(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    request = _request()
    async with projection_session() as session:
        await _release_grant(session)
        challenge = await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        replay = await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=5),
            challenge_nonce_factory=uuid4,
        )
        second_replay = await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=5),
            challenge_nonce_factory=uuid4,
        )

        assert challenge == replay == second_replay
        assert challenge.challenge_nonce == CHALLENGE_NONCE
        assert challenge.request_sha256 == canonical_authority_sha256(request)
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.state == "challenged"
        assert row.principal_sha256 == canonical_authority_sha256(principal)
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent)
                    .where(TaskImageBuildProjectionEvent.grant_id == GRANT_ID)
                    .order_by(TaskImageBuildProjectionEvent.event_sequence)
                )
            ).all()
        )
        assert [event.event_type for event in events] == [
            "challenged",
            "challenge_replayed",
        ]

        with pytest.raises(TaskImageProjectionConflictError):
            await request_task_image_projection(
                session,
                principal=principal,
                request=_request(cgroup_inode=987655),
                now=NOW + timedelta(seconds=6),
                challenge_nonce_factory=uuid4,
            )
        assert row.state == "challenged"
        assert row.event_sequence == 2


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("principal_id", "gb10-attacker"),
        ("supervisor_pid", 42101),
        ("supervisor_uid", 994),
        ("supervisor_gid", 981),
        ("supervisor_executable_sha256", "a" * 64),
    ],
)
async def test_projection_rejects_stored_identity_scalar_drift(
    projection_session: async_sessionmaker[AsyncSession],
    field: str,
    changed_value: object,
) -> None:
    principal = _principal()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        setattr(row, field, changed_value)

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match=(
                r"task-image (guard principal is not authorized"
                "|projection request changed)"
            ),
        ):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        assert secrets.put_count == 0


async def test_projection_rejects_stored_request_document_drift(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        row.request_json = {**row.request_json, "supervisor_pid": 42101}

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image projection request changed",
        ):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        assert secrets.put_count == 0


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("challenge_issued_at", NOW + timedelta(seconds=3)),
        ("challenge_expires_at", NOW + timedelta(seconds=63)),
    ],
)
async def test_projection_rejects_stored_challenge_scalar_drift(
    projection_session: async_sessionmaker[AsyncSession],
    field: str,
    changed_value: object,
) -> None:
    principal = _principal()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        setattr(row, field, changed_value)

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image projection challenge changed",
        ):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        assert secrets.put_count == 0


async def test_projection_rejects_rehashed_challenge_authority_drift(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        changed = TaskImageProjectionChallengeV1.model_validate_json(
            json.dumps(
                {**row.challenge_json, "containment_policy_sha256": "a" * 64}
            )
        )
        row.challenge_json = changed.model_dump(mode="json", exclude_none=False)
        row.challenge_sha256 = canonical_authority_sha256(changed)

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image projection challenge changed",
        ):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        assert secrets.put_count == 0


async def test_projection_rejects_rehashed_challenge_before_request_observation(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        changed = TaskImageProjectionChallengeV1.model_validate_json(
            json.dumps(
                {
                    **row.challenge_json,
                    "issued_at": (NOW + timedelta(seconds=2)).isoformat(),
                    "expires_at": (NOW + timedelta(seconds=62)).isoformat(),
                }
            )
        )
        row.challenge_json = changed.model_dump(mode="json", exclude_none=False)
        row.challenge_sha256 = canonical_authority_sha256(changed)
        row.challenge_issued_at = changed.issued_at
        row.challenge_expires_at = changed.expires_at

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image projection challenge changed",
        ):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        assert secrets.put_count == 0


@pytest.mark.parametrize(
    "drift",
    [
        "principal_scope",
        "principal_node",
        "principal_cluster",
        "job_id",
        "account",
        "qos",
        "partition",
        "architecture",
        "request_digest",
        "supervisor_uid",
        "supervisor_gid",
        "supervisor_executable",
        "grant_state",
        "grant_expiry",
        "future_observation",
        "stale_observation",
    ],
)
async def test_challenge_rejects_untrusted_or_stale_job_facts_before_state(
    projection_session: async_sessionmaker[AsyncSession],
    drift: str,
) -> None:
    expires_at = NOW + (
        timedelta(seconds=4) if drift == "grant_expiry" else timedelta(hours=2)
    )
    principal = _principal()
    request = _request()
    now = NOW + timedelta(seconds=4)
    if drift == "principal_scope":
        principal = _principal(scopes=("task-image:attest",))
    elif drift == "principal_node":
        principal = _principal(node_name="trt-gb10-2")
    elif drift == "principal_cluster":
        principal = _principal(slurm_cluster_id="oldlab")
    elif drift == "job_id":
        request = _request(slurm_job_id="54321")
    elif drift == "account":
        request = request.model_copy(update={"slurm_account": "loom-other"})
    elif drift == "qos":
        request = request.model_copy(update={"slurm_qos": "loom-other"})
    elif drift == "partition":
        request = request.model_copy(update={"slurm_partition": "loom-other"})
    elif drift == "architecture":
        request = request.model_copy(update={"cpu_arch": "x86_64"})
    elif drift == "request_digest":
        request = _request(slurm_request_sha256="a" * 64)
    elif drift == "supervisor_uid":
        request = _request(supervisor_uid=994)
    elif drift == "supervisor_gid":
        request = _request(supervisor_gid=981)
    elif drift == "supervisor_executable":
        request = _request(supervisor_executable_sha256="a" * 64)
    elif drift == "future_observation":
        request = _request(observed_at=now + timedelta(seconds=1))
    elif drift == "stale_observation":
        request = _request(observed_at=now - timedelta(seconds=60))

    async with projection_session() as session:
        await _release_grant(session, expires_at=expires_at)
        if drift == "grant_state":
            row = await session.get(TaskImageBuildGrant, GRANT_ID)
            assert row is not None
            row.state = "bound"
            row.released_at = None
            await session.flush()

        expected = (
            TaskImageProjectionExpiredError
            if drift in {"grant_expiry", "stale_observation"}
            else TaskImageProjectionAuthorizationError
        )
        with pytest.raises(expected):
            await request_task_image_projection(
                session,
                principal=principal,
                request=request,
                now=now,
                challenge_nonce_factory=lambda: CHALLENGE_NONCE,
            )
        assert (
            await session.scalar(
                select(TaskImageBuildProjection).where(
                    TaskImageBuildProjection.grant_id == GRANT_ID
                )
            )
            is None
        )


async def test_projection_persists_one_hashed_secret_and_initial_attestation(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    request = _request()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        proof = _proof()
        receipt = await complete_task_image_projection(
            session,
            principal=principal,
            proof=proof,
            now=NOW + timedelta(seconds=6),
            secret_store=secrets,
            bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
        )

        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        attestations = list(
            (
                await session.scalars(
                    select(TaskImageBuildContainmentAttestation).where(
                        TaskImageBuildContainmentAttestation.grant_id == GRANT_ID
                    )
                )
            ).all()
        )
        assert row is not None
        assert receipt.bootstrap_token.startswith("loom_tibp_")
        assert row.state == "projected"
        assert row.bootstrap_token_hash == hashlib.sha256(
            receipt.bootstrap_token.encode("utf-8")
        ).digest()
        assert row.bootstrap_secret_ref is not None
        assert row.bootstrap_secret_ref.startswith("loom://task-image-bootstrap/")
        assert receipt.bootstrap_token not in json.dumps(row.proof_json)
        assert row.attestation_generation == 1
        assert len(attestations) == 1
        assert attestations[0].generation == 1
        assert attestations[0].attestation_sha256 == row.attestation_sha256
        assert secrets.put_count == 1


async def test_projection_exact_replay_is_bounded_and_returns_same_secret(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    request = _request()
    proof = _proof()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        first = await complete_task_image_projection(
            session,
            principal=principal,
            proof=proof,
            now=NOW + timedelta(seconds=6),
            secret_store=secrets,
            bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
        )
        replay = await complete_task_image_projection(
            session,
            principal=principal,
            proof=proof,
            now=NOW + timedelta(seconds=7),
            secret_store=secrets,
            bootstrap_token_factory=lambda: "loom_tibp_" + "B" * 64,
        )
        second_replay = await complete_task_image_projection(
            session,
            principal=principal,
            proof=proof,
            now=NOW + timedelta(seconds=7),
            secret_store=secrets,
            bootstrap_token_factory=lambda: "loom_tibp_" + "C" * 64,
        )

        assert first == replay == second_replay
        assert secrets.put_count == 1
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent).where(
                        TaskImageBuildProjectionEvent.grant_id == GRANT_ID
                    )
                )
            ).all()
        )
        assert [event.event_type for event in events].count("projection_replayed") == 1
        assert (
            await session.scalar(
                select(TaskImageBuildContainmentAttestation).where(
                    TaskImageBuildContainmentAttestation.grant_id == GRANT_ID
                )
            )
        ) is not None

        for changed_proof in (
            _proof(request_sha256="c" * 64),
            _proof(attachment=_attachment(link_ids=(101, 102, 104))),
        ):
            with pytest.raises(TaskImageProjectionConflictError):
                await complete_task_image_projection(
                    session,
                    principal=principal,
                    proof=changed_proof,
                    now=NOW + timedelta(seconds=8),
                    secret_store=secrets,
                    bootstrap_token_factory=lambda: "loom_tibp_" + "D" * 64,
                )
        assert secrets.put_count == 1


@pytest.mark.parametrize(
    "drift",
    [
        "wrong_challenge",
        "expired_challenge",
        "changed_request_id",
        "changed_request_digest",
        "changed_node",
        "changed_node_boot",
        "changed_cluster",
        "changed_job",
        "changed_cgroup",
        "changed_cgroup_path",
        "wrong_policy",
        "wrong_resource",
    ],
)
async def test_projection_rejects_invalid_proof_before_creating_a_secret(
    projection_session: async_sessionmaker[AsyncSession],
    drift: str,
) -> None:
    principal = _principal()
    request = _request()
    proof = _proof()
    now = NOW + timedelta(seconds=6)
    if drift == "wrong_challenge":
        proof = _proof(challenge_nonce=uuid4())
    elif drift == "expired_challenge":
        now = NOW + timedelta(seconds=64)
    elif drift == "changed_request_id":
        proof = _proof(request_id=uuid4())
    elif drift == "changed_request_digest":
        proof = _proof(request_sha256="a" * 64)
    elif drift == "changed_node":
        proof = _proof(node_name="trt-gb10-2")
    elif drift == "changed_node_boot":
        proof = _proof(node_boot_id=uuid4())
    elif drift == "changed_cluster":
        proof = _proof(slurm_cluster_id="oldlab")
    elif drift == "changed_job":
        proof = _proof(slurm_job_id="54321")
    elif drift == "changed_cgroup":
        proof = _proof(
            cgroup_inode=987655,
            attachment=_attachment(cgroup_inode=987655),
        )
    elif drift == "changed_cgroup_path":
        changed_cgroup = (
            "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_54321/step_batch"
        )
        changed_root = f"{changed_cgroup}/loom-builder"
        proof = _proof(
            cgroup_path=changed_cgroup,
            attachment=_attachment(
                containment_root=changed_root,
                trusted_service_cgroup=f"{changed_root}/trusted-service",
                build_egress_cgroup=f"{changed_root}/build-egress",
            ),
        )
    elif drift == "wrong_policy":
        proof = _proof(attachment=_attachment(containment_policy_sha256="a" * 64))
    elif drift == "wrong_resource":
        proof = _proof(attachment=_attachment(resource_limits_sha256="a" * 64))

    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        expected = (
            TaskImageProjectionExpiredError
            if drift == "expired_challenge"
            else TaskImageProjectionAuthorizationError
        )
        with pytest.raises(expected):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=proof,
                now=now,
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.state == "challenged"
        assert row.proof_id is None
        assert secrets.put_count == 0


async def test_secret_store_failure_rolls_projection_back_to_challenged(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    request = _request()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=request,
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        await session.commit()

        with pytest.raises(RuntimeError, match="synthetic secret-store failure"):
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=_MemorySecretStore(fail_put=True),
                bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
            )
        await session.rollback()

        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.state == "challenged"
        assert row.proof_id is None


async def test_secret_bearing_projection_validation_redacts_raw_tokens(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    principal = _principal()
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        invalid_bootstrap = "raw-projection-secret-that-must-not-reach-an-error"
        with pytest.raises(TaskImageProjectionAuthorizationError) as projection_error:
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=6),
                secret_store=secrets,
                bootstrap_token_factory=lambda: invalid_bootstrap,
            )
        projection_traceback = "".join(
            traceback.format_exception(
                projection_error.type,
                projection_error.value,
                projection_error.tb,
            )
        )
        assert invalid_bootstrap not in projection_traceback
        assert secrets.put_count == 0

        receipt = await complete_task_image_projection(
            session,
            principal=principal,
            proof=_proof(),
            now=NOW + timedelta(seconds=6),
            secret_store=secrets,
            bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        row.bootstrap_issued_at = NOW - timedelta(seconds=30)
        with pytest.raises(TaskImageProjectionAuthorizationError) as replay_error:
            await complete_task_image_projection(
                session,
                principal=principal,
                proof=_proof(),
                now=NOW + timedelta(seconds=7),
                secret_store=secrets,
                bootstrap_token_factory=lambda: "loom_tibp_" + "B" * 64,
            )
        replay_traceback = "".join(
            traceback.format_exception(
                replay_error.type,
                replay_error.value,
                replay_error.tb,
            )
        )
        assert receipt.bootstrap_token not in replay_traceback
        assert secrets.put_count == 1


async def _project_grant(
    session: AsyncSession,
    *,
    secret_store: _MemorySecretStore,
):
    grant = await _release_grant(session)
    principal = _principal()
    request = _request()
    await request_task_image_projection(
        session,
        principal=principal,
        request=request,
        now=NOW + timedelta(seconds=4),
        challenge_nonce_factory=lambda: CHALLENGE_NONCE,
    )
    proof = _proof()
    receipt = await complete_task_image_projection(
        session,
        principal=principal,
        proof=proof,
        now=NOW + timedelta(seconds=6),
        secret_store=secret_store,
        bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
    )
    return grant, principal, proof, receipt


def _exchange(receipt, **changes: object) -> TaskImageBootstrapExchangeV1:
    values: dict[str, object] = {
        "exchange_id": EXCHANGE_ID,
        "grant_id": GRANT_ID,
        "proof_sha256": receipt.proof_sha256,
        "bootstrap_token": receipt.bootstrap_token,
        "observed_at": NOW + timedelta(seconds=7),
    }
    values.update(changes)
    return TaskImageBootstrapExchangeV1.model_validate(values)


def _attestation(
    proof: TaskImageAttachmentProofV1,
    *,
    generation: int,
    **changes: object,
) -> TaskImageContainmentAttestationV1:
    values: dict[str, object] = {
        "attestation_id": PROOF_ID if generation == 1 else ATTESTATION_ID,
        "grant_id": GRANT_ID,
        "generation": generation,
        "node_name": proof.node_name,
        "node_boot_id": proof.node_boot_id,
        "slurm_cluster_id": proof.slurm_cluster_id,
        "slurm_job_id": proof.slurm_job_id,
        "cgroup_path": proof.cgroup_path,
        "cgroup_inode": proof.cgroup_inode,
        "attachment": proof.attachment,
        "issued_at": (
            proof.observed_at
            if generation == 1
            else NOW + timedelta(seconds=10 + generation)
        ),
        "expires_at": (
            proof.attestation_expires_at
            if generation == 1
            else NOW + timedelta(seconds=50 + generation)
        ),
    }
    values.update(changes)
    return TaskImageContainmentAttestationV1.model_validate(values)


async def test_exchange_consumes_one_bootstrap_and_exact_replay_is_bounded(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        grant, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        exchange = _exchange(receipt)
        build_session = await exchange_task_image_bootstrap(
            session,
            request=exchange,
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        replay = await exchange_task_image_bootstrap(
            session,
            request=exchange,
            now=NOW + timedelta(seconds=9),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
        )
        second_replay = await exchange_task_image_bootstrap(
            session,
            request=exchange,
            now=NOW + timedelta(seconds=9),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "D" * 64,
        )

        assert build_session == replay == second_replay
        assert build_session.session_id not in {exchange.exchange_id, exchange.grant_id}
        assert build_session.session_token.startswith("loom_tibs_")
        assert build_session.expires_at <= min(
            grant.authority.expires_at,
            receipt.expires_at,
            NOW + timedelta(seconds=40),
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.state == "exchanged"
        assert row.session_token_hash == hashlib.sha256(
            build_session.session_token.encode("utf-8")
        ).digest()
        assert row.session_secret_ref is not None
        assert row.session_secret_ref.startswith("loom://task-image-session/")
        assert row.session_json == build_session.public_binding()
        assert row.session_sha256 == canonical_public_binding_sha256(build_session)
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent).where(
                        TaskImageBuildProjectionEvent.grant_id == GRANT_ID
                    )
                )
            ).all()
        )
        persisted_json = json.dumps(
            [
                row.exchange_json,
                row.session_json,
                *(event.payload_json for event in events),
            ]
        )
        assert receipt.bootstrap_token not in persisted_json
        assert build_session.session_token not in persisted_json
        assert [event.event_type for event in events].count("exchange_replayed") == 1
        assert secrets.put_count == 2

        row.session_sha256 = "c" * 64
        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image build session changed",
        ):
            await exchange_task_image_bootstrap(
                session,
                request=exchange,
                now=NOW + timedelta(seconds=10),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "E" * 64,
            )


async def test_exchange_rejects_wrong_token_changed_body_second_exchange_and_expiry(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        exchange = _exchange(receipt)
        with pytest.raises(TaskImageProjectionAuthorizationError):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt, bootstrap_token="loom_tibp_" + "Z" * 64),
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "B" * 64,
            )
        assert secrets.put_count == 1

        await exchange_task_image_bootstrap(
            session,
            request=exchange,
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        with pytest.raises(TaskImageProjectionConflictError):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt, proof_sha256="c" * 64),
                now=NOW + timedelta(seconds=9),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            )
        with pytest.raises(TaskImageProjectionConflictError):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt, exchange_id=uuid4()),
                now=NOW + timedelta(seconds=9),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            )
        with pytest.raises(TaskImageProjectionExpiredError):
            await exchange_task_image_bootstrap(
                session,
                request=exchange,
                now=receipt.expires_at,
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            )
        assert secrets.put_count == 2


async def test_exchange_rejects_a_grant_revoked_after_projection(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        grant_row = await session.get(TaskImageBuildGrant, GRANT_ID)
        assert grant_row is not None
        grant_row.state = "revoked"
        grant_row.released_at = None
        grant_row.revoked_at = NOW + timedelta(seconds=7)
        grant_row.revoke_reason = "operator_revoked"
        await session.flush()

        with pytest.raises(TaskImageProjectionAuthorizationError):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt),
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "B" * 64,
            )
        assert secrets.put_count == 1


async def test_exchange_rejects_changed_attestation_high_water(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        row.attestation_sha256 = "a" * 64

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="containment attestation high-water changed",
        ):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt),
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "B" * 64,
            )
        assert secrets.put_count == 1


async def test_exchange_rejects_rehashed_stored_proof_drift(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.proof_json is not None
        changed = TaskImageAttachmentProofV1.model_validate_json(
            json.dumps(
                {
                    **row.proof_json,
                    "observed_at": (NOW + timedelta(seconds=6)).isoformat(),
                }
            )
        )
        changed_sha256 = canonical_authority_sha256(changed)
        row.proof_json = changed.model_dump(mode="json", exclude_none=False)
        row.proof_sha256 = changed_sha256

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image attachment proof changed",
        ):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt, proof_sha256=changed_sha256),
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "B" * 64,
            )
        assert secrets.put_count == 1


async def test_exchange_rejects_bootstrap_expiry_beyond_initial_attestation(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, proof, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        row.bootstrap_expires_at = proof.attestation_expires_at + timedelta(seconds=1)

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image bootstrap receipt changed",
        ):
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt),
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "B" * 64,
            )
        assert secrets.put_count == 1


async def test_exchange_replay_rejects_stored_exchange_document_drift(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        exchange = _exchange(receipt)
        await exchange_task_image_bootstrap(
            session,
            request=exchange,
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.exchange_json is not None
        row.exchange_json = {
            **row.exchange_json,
            "observed_at": "2026-09-02T14:00:08Z",
        }

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image bootstrap exchange changed",
        ):
            await exchange_task_image_bootstrap(
                session,
                request=exchange,
                now=NOW + timedelta(seconds=9),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            )
        assert secrets.put_count == 2


async def test_secret_bearing_exchange_validation_redacts_raw_tokens(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        invalid_bootstrap = "raw-bootstrap-that-must-not-reach-an-error"
        unchecked_exchange = _exchange(receipt).model_copy(
            update={"bootstrap_token": invalid_bootstrap}
        )
        with pytest.raises(TaskImageProjectionAuthorizationError) as exchange_error:
            await exchange_task_image_bootstrap(
                session,
                request=unchecked_exchange,
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: "loom_tibs_" + "B" * 64,
            )
        exchange_traceback = "".join(
            traceback.format_exception(
                exchange_error.type,
                exchange_error.value,
                exchange_error.tb,
            )
        )
        assert invalid_bootstrap not in exchange_traceback

        invalid_session = "raw-session-that-must-not-reach-an-error"
        with pytest.raises(TaskImageProjectionAuthorizationError) as session_error:
            await exchange_task_image_bootstrap(
                session,
                request=_exchange(receipt),
                now=NOW + timedelta(seconds=8),
                secret_store=secrets,
                session_token_factory=lambda: invalid_session,
            )
        session_traceback = "".join(
            traceback.format_exception(
                session_error.type,
                session_error.value,
                session_error.tb,
            )
        )
        assert invalid_session not in session_traceback
        assert secrets.put_count == 1


async def test_monotonic_attestation_authorizes_only_a_fresh_exact_session(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    principal = _principal()
    async with projection_session() as session:
        grant, _principal_value, proof, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        build_session = await exchange_task_image_bootstrap(
            session,
            request=_exchange(receipt),
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        generation_one = _attestation(proof, generation=1)
        replay = await record_task_image_containment_attestation(
            session,
            principal=principal,
            attestation=generation_one,
            now=NOW + timedelta(seconds=9),
        )
        second_replay = await record_task_image_containment_attestation(
            session,
            principal=principal,
            attestation=generation_one,
            now=NOW + timedelta(seconds=9),
        )
        assert replay == second_replay == generation_one

        generation_two = _attestation(proof, generation=2)
        recorded = await record_task_image_containment_attestation(
            session,
            principal=principal,
            attestation=generation_two,
            now=NOW + timedelta(seconds=13),
        )
        assert recorded == generation_two

        replayed_session = await exchange_task_image_bootstrap(
            session,
            request=_exchange(receipt),
            now=NOW + timedelta(seconds=14),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
        )
        assert replayed_session == build_session

        authorization = await authorize_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            raw_session_token=build_session.session_token,
            now=NOW + timedelta(seconds=14),
        )
        assert authorization.grant_id == GRANT_ID
        assert authorization.session_id == build_session.session_id
        assert authorization.purpose == "production"
        assert authorization.pool_id == "staging-gb10-task-image"
        assert authorization.cpu_arch == "arm64"
        assert authorization.attestation_generation == 2
        assert authorization.attestation_sha256 == canonical_authority_sha256(
            generation_two
        )
        assert authorization.grant_expires_at == grant.authority.expires_at

        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        original_session_json = row.session_json
        original_session_sha256 = row.session_sha256
        changed_session = build_session.model_copy(
            update={"pool_id": "attacker-pool"}
        )
        row.session_json = changed_session.public_binding()
        row.session_sha256 = canonical_public_binding_sha256(changed_session)
        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image build session changed",
        ):
            await authorize_task_image_build_session(
                session,
                grant_id=GRANT_ID,
                raw_session_token=build_session.session_token,
                now=NOW + timedelta(seconds=14),
            )
        row.session_json = original_session_json
        row.session_sha256 = original_session_sha256

        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent).where(
                        TaskImageBuildProjectionEvent.grant_id == GRANT_ID
                    )
                )
            ).all()
        )
        assert [event.event_type for event in events].count("attestation_replayed") == 1
        assert [event.event_type for event in events].count("attested") == 1

        with pytest.raises(TaskImageProjectionAuthorizationError):
            await authorize_task_image_build_session(
                session,
                grant_id=GRANT_ID,
                raw_session_token="loom_tibs_" + "Z" * 64,
                now=NOW + timedelta(seconds=14),
            )

        row.attestation_expires_at = NOW + timedelta(seconds=14)
        await session.flush()
        assert build_session.expires_at > NOW + timedelta(seconds=14)
        with pytest.raises(TaskImageProjectionExpiredError):
            await authorize_task_image_build_session(
                session,
                grant_id=GRANT_ID,
                raw_session_token=build_session.session_token,
                now=NOW + timedelta(seconds=14),
            )


async def test_session_rejects_rehashed_expiry_beyond_its_bootstrap_and_attestation(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, _principal_value, _proof_value, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        build_session = await exchange_task_image_bootstrap(
            session,
            request=_exchange(receipt),
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        changed = TaskImageBuildSessionV1.model_validate(
            build_session.model_dump(mode="python")
            | {"expires_at": NOW + timedelta(minutes=5)}
        )
        row.session_json = changed.public_binding()
        row.session_sha256 = canonical_public_binding_sha256(changed)
        row.session_expires_at = changed.expires_at

        with pytest.raises(
            TaskImageProjectionAuthorizationError,
            match="stored task-image build session changed",
        ):
            await authorize_task_image_build_session(
                session,
                grant_id=GRANT_ID,
                raw_session_token=build_session.session_token,
                now=NOW + timedelta(seconds=14),
            )


@pytest.mark.parametrize(
    "drift",
    [
        "skipped_generation",
        "principal_scope",
        "principal_node",
        "node_name",
        "node_boot_id",
        "cluster",
        "job_id",
        "cgroup_inode",
        "cgroup_path",
        "link_ids",
        "program_ids",
        "map_ids",
        "policy_digest",
        "resource_digest",
    ],
)
async def test_attestation_rejects_equivocation_skips_or_attachment_drift(
    projection_session: async_sessionmaker[AsyncSession],
    drift: str,
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, principal, proof, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        await exchange_task_image_bootstrap(
            session,
            request=_exchange(receipt),
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        candidate = _attestation(proof, generation=2)
        expected: type[Exception] = TaskImageProjectionAuthorizationError
        if drift == "skipped_generation":
            candidate = _attestation(proof, generation=3)
            expected = TaskImageProjectionConflictError
        elif drift == "principal_scope":
            principal = _principal(scopes=("task-image:project",))
        elif drift == "principal_node":
            principal = _principal(node_name="trt-gb10-2")
        elif drift == "node_name":
            candidate = _attestation(
                proof,
                generation=2,
                node_name="trt-gb10-2",
            )
        elif drift == "node_boot_id":
            candidate = _attestation(proof, generation=2, node_boot_id=uuid4())
        elif drift == "cluster":
            candidate = _attestation(
                proof,
                generation=2,
                slurm_cluster_id="oldlab",
            )
        elif drift == "job_id":
            candidate = _attestation(
                proof,
                generation=2,
                slurm_job_id="54321",
            )
        elif drift == "cgroup_inode":
            candidate = _attestation(
                proof,
                generation=2,
                cgroup_inode=987655,
                attachment=_attachment(cgroup_inode=987655),
            )
        elif drift == "cgroup_path":
            changed_cgroup = (
                "/sys/fs/cgroup/system.slice/slurmstepd.scope/job_54321/step_batch"
            )
            changed_root = f"{changed_cgroup}/loom-builder"
            candidate = _attestation(
                proof,
                generation=2,
                cgroup_path=changed_cgroup,
                attachment=_attachment(
                    containment_root=changed_root,
                    trusted_service_cgroup=f"{changed_root}/trusted-service",
                    build_egress_cgroup=f"{changed_root}/build-egress",
                ),
            )
        elif drift == "link_ids":
            candidate = _attestation(
                proof,
                generation=2,
                attachment=_attachment(link_ids=(101, 102, 104)),
            )
        elif drift == "program_ids":
            candidate = _attestation(
                proof,
                generation=2,
                attachment=_attachment(program_ids=(201, 202, 204)),
            )
        elif drift == "map_ids":
            candidate = _attestation(
                proof,
                generation=2,
                attachment=_attachment(map_ids=(301, 303)),
            )
        elif drift == "policy_digest":
            candidate = _attestation(
                proof,
                generation=2,
                attachment=_attachment(containment_policy_sha256="a" * 64),
            )
        elif drift == "resource_digest":
            candidate = _attestation(
                proof,
                generation=2,
                attachment=_attachment(resource_limits_sha256="a" * 64),
            )

        with pytest.raises(expected):
            await record_task_image_containment_attestation(
                session,
                principal=principal,
                attestation=candidate,
                now=NOW + timedelta(seconds=13),
            )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.attestation_generation == 1


async def test_attestation_equivocation_durably_revokes_projection(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    rejected_at = NOW + timedelta(seconds=13)
    async with projection_session() as session:
        _grant_value, principal, proof, _receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        changed_generation = _attestation(
            proof,
            generation=1,
            expires_at=proof.attestation_expires_at - timedelta(seconds=1),
        )

        with pytest.raises(
            TaskImageProjectionConflictError,
            match="containment attestation equivocated",
        ):
            await record_task_image_containment_attestation(
                session,
                principal=principal,
                attestation=changed_generation,
                now=rejected_at,
            )
        await session.commit()

    async with projection_session() as session:
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.state == "revoked"
        assert row.revoked_at == rejected_at
        assert row.revoke_reason == "attestation_equivocation"
        attestations = list(
            (
                await session.scalars(
                    select(TaskImageBuildContainmentAttestation).where(
                        TaskImageBuildContainmentAttestation.grant_id == GRANT_ID
                    )
                )
            ).all()
        )
        assert [item.generation for item in attestations] == [1]
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent).where(
                        TaskImageBuildProjectionEvent.grant_id == GRANT_ID,
                        TaskImageBuildProjectionEvent.event_type == "revoked",
                    )
                )
            ).all()
        )
        assert len(events) == 1
        assert events[0].event_key == "revocation"
        assert events[0].payload_json == {"reason": "attestation_equivocation"}


async def test_revocation_is_exact_irreversible_and_blocks_session_and_attestation(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        _grant_value, principal, proof, receipt = await _project_grant(
            session,
            secret_store=secrets,
        )
        build_session = await exchange_task_image_bootstrap(
            session,
            request=_exchange(receipt),
            now=NOW + timedelta(seconds=8),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )
        await revoke_task_image_projection(
            session,
            grant_id=GRANT_ID,
            reason="guard_attestation_lost",
            now=NOW + timedelta(seconds=9),
        )
        await revoke_task_image_projection(
            session,
            grant_id=GRANT_ID,
            reason="guard_attestation_lost",
            now=NOW + timedelta(seconds=10),
        )
        with pytest.raises(TaskImageProjectionConflictError):
            await revoke_task_image_projection(
                session,
                grant_id=GRANT_ID,
                reason="operator_revoked",
                now=NOW + timedelta(seconds=10),
            )
        with pytest.raises(TaskImageProjectionAuthorizationError):
            await record_task_image_containment_attestation(
                session,
                principal=principal,
                attestation=_attestation(proof, generation=2),
                now=NOW + timedelta(seconds=13),
            )
        with pytest.raises(TaskImageProjectionAuthorizationError):
            await authorize_task_image_build_session(
                session,
                grant_id=GRANT_ID,
                raw_session_token=build_session.session_token,
                now=NOW + timedelta(seconds=13),
            )
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent).where(
                        TaskImageBuildProjectionEvent.grant_id == GRANT_ID,
                        TaskImageBuildProjectionEvent.event_type == "revoked",
                    )
                )
            ).all()
        )
        assert len(events) == 1


async def test_expiration_requires_the_earliest_deadline_and_is_idempotent(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    async with projection_session() as session:
        await _project_grant(session, secret_store=secrets)
        with pytest.raises(TaskImageProjectionConflictError):
            await expire_task_image_projection(
                session,
                grant_id=GRANT_ID,
                now=NOW + timedelta(seconds=39),
            )
        await expire_task_image_projection(
            session,
            grant_id=GRANT_ID,
            now=NOW + timedelta(seconds=40),
        )
        await expire_task_image_projection(
            session,
            grant_id=GRANT_ID,
            now=NOW + timedelta(seconds=41),
        )
        row = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == GRANT_ID
            )
        )
        assert row is not None
        assert row.state == "expired"
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildProjectionEvent).where(
                        TaskImageBuildProjectionEvent.grant_id == GRANT_ID,
                        TaskImageBuildProjectionEvent.event_type == "expired",
                    )
                )
            ).all()
        )
        assert len(events) == 1


async def test_expiration_ignores_consumed_challenge_deadline(
    projection_session: async_sessionmaker[AsyncSession],
) -> None:
    secrets = _MemorySecretStore()
    principal = _principal()
    async with projection_session() as session:
        await _release_grant(session)
        await request_task_image_projection(
            session,
            principal=principal,
            request=_request(),
            now=NOW + timedelta(seconds=4),
            challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        )
        proof = _proof(
            observed_at=NOW + timedelta(seconds=60),
            attestation_expires_at=NOW + timedelta(seconds=110),
        )
        receipt = await complete_task_image_projection(
            session,
            principal=principal,
            proof=proof,
            now=NOW + timedelta(seconds=61),
            secret_store=secrets,
            bootstrap_token_factory=lambda: "loom_tibp_" + "A" * 64,
        )
        await exchange_task_image_bootstrap(
            session,
            request=_exchange(
                receipt,
                observed_at=NOW + timedelta(seconds=62),
            ),
            now=NOW + timedelta(seconds=63),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "B" * 64,
        )

        with pytest.raises(
            TaskImageProjectionConflictError,
            match="has not reached an expiry deadline",
        ):
            await expire_task_image_projection(
                session,
                grant_id=GRANT_ID,
                now=NOW + timedelta(seconds=65),
            )


def test_memory_secret_store_satisfies_protocol() -> None:
    assert isinstance(_MemorySecretStore(), SecretStore)
