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
from loom.task_image_materialization import (
    ensure_task_image_materializations,
    get_trial_task_image_execution_grant,
    task_image_materialization_key,
)
from loom_control_plane.scheduler.claim import claim_one, claim_work
from tests.integration import test_task_image_publication_completion as completion_fixtures
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_journal import _publish, _receipts, _spec, _upload
from tests.integration.test_task_image_authority_materializations import _queued_materialization
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def _trial(session, image, team_id, *, priority):
    session.add(
        Task(
            id=image.task_id,
            checksum=image.task_checksum,
            config=image.task_config,
            source=image.task_source,
            source_provenance=image.task_source_provenance,
        )
    )
    await session.flush()
    trial = Trial(
        id=uuid4(),
        team_id=team_id,
        task_id=image.task_id,
        config={},
        requires_caps={
            "os": "linux",
            "cpu_arch": image.task_config["environment"]["cpu_arch"],
            "gpu_vendor": "none",
            "network_policies": ["public"],
        },
        state="queued",
        submit_priority=priority,
        submitted_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    session.add(trial)
    await session.flush()
    session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=image.id))
    await session.flush()
    return trial


async def _identities(session, *, cpu_arch="arm64"):
    team = Team(id=uuid4(), name="legacy-reader-" + uuid4().hex)
    worker = Worker(
        id=uuid4(),
        hostname="legacy-reader",
        version="legacy",
        capabilities=[
            {
                "os": "linux",
                "cpu_arch": cpu_arch,
                "gpu_vendor": "none",
                "network_policies": ["public"],
            }
        ],
        registered_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
        status="active",
        capability_snapshot_digest="a" * 64,
        auth_token_hash=b"b" * 32,
        supported_work_kinds=["trial"],
    )
    session.add_all([team, worker])
    await session.flush()
    session.add(TeamQuota(team_id=team.id))
    return team, worker


async def _seed(session, issuer, *, include_legacy):
    values = await _signed_job(session, issuer)
    await _complete(session, values)
    native_image = (await session.scalars(select(TaskImageMaterialization))).one()
    assert native_image.ready_publication_operation_id is not None
    team, worker = await _identities(session)
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
        worker_id=worker.id,
        worker_os=["linux"],
        worker_cpu_arches=[worker.capabilities[0]["cpu_arch"]],
        worker_gpu_vendors=["none"],
        worker_network_policies=["public"],
    )
    if reader == "trial":
        return await claim_one(session, **arguments)
    result = await claim_work(
        session,
        **arguments,
        capability_snapshot_digest="a" * 64,
        worker_token_hash=b"b" * 32,
        supported_work_kinds=["trial"],
        free_slots=1,
    )
    return result[0] if result else None


async def test_unsigned_snapshot_refuses_native_ready_image(
    registry_authority_session,
    registry_issuer,
):
    async with registry_authority_session() as session:
        _, native_trial, _ = await _seed(session, registry_issuer, include_legacy=False)
        with pytest.raises(RuntimeError, match="ready task-image materialization"):
            await get_trial_task_image_execution_grant(
                session,
                trial_id=native_trial.id,
                cpu_arches=["arm64"],
            )


@pytest.mark.parametrize("reader", ["trial", "work"])
@pytest.mark.parametrize("include_legacy", [False, True])
async def test_legacy_selector_skips_native_without_starving_phase1(
    registry_authority_session,
    registry_issuer,
    reader,
    include_legacy,
):
    async with registry_authority_session() as session:
        worker, native_trial, legacy_trial = await _seed(
            session,
            registry_issuer,
            include_legacy=include_legacy,
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


@pytest.mark.parametrize("reader", ["snapshot", "trial", "work"])
async def test_strong_source_is_not_mistaken_for_native_readiness(
    registry_authority_session,
    tmp_path,
    reader,
):
    spec = _spec(tmp_path)
    ticket = await _upload(registry_authority_session, spec)
    await _receipts(registry_authority_session, ticket)
    await _publish(registry_authority_session, ticket)
    async with registry_authority_session() as session:
        image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        assert image.bundle_content_manifest_sha256
        image.state = "ready"
        image.registry_images = {"task": "registry.example/task@sha256:" + "d" * 64}
        assert image.ready_publication_operation_id is None
        team, worker = await _identities(session, cpu_arch=image.cpu_arch)
        trial = await _trial(session, image, team.id, priority=0)
        await session.commit()
        if reader == "snapshot":
            grant = await get_trial_task_image_execution_grant(
                session,
                trial_id=trial.id,
                cpu_arches=[image.cpu_arch],
            )
            assert grant is not None and grant.materialization_id == image.id
        else:
            claimed = await _claim(session, reader, worker)
            assert claimed is not None and claimed["id"] == trial.id


@pytest.mark.parametrize("architectures", [["x86_64"], ["arm64", "x86_64"]])
async def test_native_architecture_does_not_veto_same_task_legacy_snapshot(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
    architectures,
):
    async def architecture_neutral_task(session):
        image = await _queued_materialization(session)
        config = dict(image.task_config)
        config["environment"] = dict(config["environment"], cpu_arch="any")
        image.task_config = config
        await session.flush()
        return image

    monkeypatch.setattr(completion_fixtures, "_queued_materialization", architecture_neutral_task)
    async with registry_authority_session() as session:
        _, trial, _ = await _seed(session, registry_issuer, include_legacy=False)
        native = (await session.scalars(select(TaskImageMaterialization))).one()
        legacy = TaskImageMaterialization(
            materialization_key=task_image_materialization_key(
                task_id=native.task_id,
                task_checksum=native.task_checksum,
                cpu_arch="x86_64",
            ),
            task_id=native.task_id,
            task_checksum=native.task_checksum,
            cpu_arch="x86_64",
            task_config=native.task_config,
            task_source=native.task_source,
            task_source_provenance=native.task_source_provenance,
            state="ready",
            registry_images={"task": "registry.example/task@sha256:" + "e" * 64},
        )
        session.add(legacy)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=legacy.id))
        await session.commit()
        grant = await get_trial_task_image_execution_grant(
            session,
            trial_id=trial.id,
            cpu_arches=architectures,
        )
        assert grant is not None and grant.materialization_id == legacy.id
        assert grant.cpu_arch == "x86_64"
        assert native.ready_publication_operation_id is not None
