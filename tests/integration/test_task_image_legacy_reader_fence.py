"""Native readiness must not issue unsigned authority to legacy claim readers."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    Team,
    TeamQuota,
    Trial,
    TrialTaskImageMaterialization,
    Worker,
)
from loom.task_image_materialization import get_trial_task_image_execution_grant
from loom_control_plane.scheduler.claim import claim_one, claim_work
from tests.integration.test_task_image_authority_materializations import _queued_materialization
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def _trial(session, image, team_id, *, priority):
    session.add(Task(
        id=image.task_id, checksum=image.task_checksum, config=image.task_config,
        source=image.task_source, source_provenance=image.task_source_provenance,
    ))
    await session.flush()
    trial = Trial(
        id=uuid4(), team_id=team_id, task_id=image.task_id, config={},
        requires_caps={"os": "linux", "cpu_arch": "arm64", "gpu_vendor": "none",
                       "network_policies": ["public"]},
        state="queued", submit_priority=priority,
        submitted_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    session.add(trial)
    await session.flush()
    session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=image.id))
    await session.flush()
    return trial


async def _seed(session, issuer, *, include_legacy):
    values = await _signed_job(session, issuer)
    await _complete(session, values)
    native_image = (await session.scalars(select(TaskImageMaterialization))).one()
    assert native_image.ready_publication_operation_id is not None
    team = Team(id=uuid4(), name="legacy-reader-" + uuid4().hex)
    worker = Worker(
        id=uuid4(), hostname="legacy-reader", version="legacy",
        capabilities=[{"os": "linux", "cpu_arch": "arm64", "gpu_vendor": "none",
                       "network_policies": ["public"]}],
        registered_at=datetime.now(UTC), last_seen_at=datetime.now(UTC), status="active",
        capability_snapshot_digest="a" * 64, auth_token_hash=b"b" * 32,
        supported_work_kinds=["trial"],
    )
    session.add_all([team, worker])
    await session.flush()
    session.add(TeamQuota(team_id=team.id))
    native_trial = await _trial(session, native_image, team.id, priority=10)
    legacy_trial = None
    if include_legacy:
        legacy_image = await _queued_materialization(session, task_id="legacy/eligible")
        legacy_image.state = "ready"
        legacy_image.registry_images = {"task": "registry.example/task@sha256:" + "d" * 64}
        assert legacy_image.ready_publication_operation_id is None
        legacy_trial = await _trial(session, legacy_image, team.id, priority=0)
    await session.commit()
    return worker, native_trial, legacy_trial


async def _claim(session, reader, worker):
    arguments = dict(
        worker_id=worker.id, worker_os=["linux"], worker_cpu_arches=["arm64"],
        worker_gpu_vendors=["none"], worker_network_policies=["public"],
    )
    if reader == "trial":
        return await claim_one(session, **arguments)
    result = await claim_work(
        session, **arguments, capability_snapshot_digest="a" * 64,
        worker_token_hash=b"b" * 32, supported_work_kinds=["trial"], free_slots=1,
    )
    return result[0] if result else None


async def test_unsigned_snapshot_refuses_native_ready_image(
    registry_authority_session, registry_issuer,
):
    async with registry_authority_session() as session:
        _, native_trial, _ = await _seed(session, registry_issuer, include_legacy=False)
        with pytest.raises(RuntimeError, match="ready task-image materialization"):
            await get_trial_task_image_execution_grant(
                session, trial_id=native_trial.id, cpu_arches=["arm64"],
            )


@pytest.mark.parametrize("reader", ["trial", "work"])
@pytest.mark.parametrize("include_legacy", [False, True])
async def test_legacy_selector_skips_native_without_starving_phase1(
    registry_authority_session, registry_issuer, reader, include_legacy,
):
    async with registry_authority_session() as session:
        worker, native_trial, legacy_trial = await _seed(
            session, registry_issuer, include_legacy=include_legacy,
        )
        claimed = await _claim(session, reader, worker)
        if legacy_trial is None:
            assert claimed is None, "legacy reader selected a native-only trial"
        else:
            assert claimed is not None and claimed["id"] == legacy_trial.id
        await session.commit()
        await session.refresh(native_trial)
        assert native_trial.state == "queued" and native_trial.attempt_count == 0
        assert native_trial.worker_id is None
