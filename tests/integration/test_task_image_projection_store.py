from __future__ import annotations

import hashlib
import json
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
    TaskImageBuildGrantAuthorityV1,
    TaskImageContainmentAttachmentV1,
    TaskImageGuardPrincipalV1,
    TaskImageProjectionRequestV1,
    canonical_authority_sha256,
)
from loom_task_image_authority.store import (
    TaskImageProjectionAuthorizationError,
    TaskImageProjectionConflictError,
    TaskImageProjectionExpiredError,
    complete_task_image_projection,
    request_task_image_projection,
)

NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")
REQUEST_ID = UUID("22222222-2222-2222-2222-222222222222")
CHALLENGE_NONCE = UUID("33333333-3333-3333-3333-333333333333")
PROOF_ID = UUID("44444444-4444-4444-4444-444444444444")
NODE_BOOT_ID = UUID("77777777-7777-7777-7777-777777777777")
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
        "changed_cgroup",
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
    elif drift == "changed_cgroup":
        proof = proof.model_copy(update={"cgroup_inode": 987655})
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


def test_memory_secret_store_satisfies_protocol() -> None:
    assert isinstance(_MemorySecretStore(), SecretStore)
