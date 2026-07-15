"""TB2.1 register and atomic public-alias activation coverage."""

from __future__ import annotations

import copy
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from fastapi import HTTPException
from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock
from sqlalchemy import create_engine, delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, BenchmarkAlias, Team, Trial
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.audit_cmd import AuditResult, activate_tb21_alias
from loom_benchmark_tool.manifest import TB21_AGENT_WORKSPACE_POLICY
from loom_benchmark_tool.register_cmd import run_register
from loom_service.task_filter import resolve_task_filter_with_diagnostics

PROFILE = "terminal-bench-2@tb2.1-r6"
TASK_ID = f"{PROFILE}/chess-best-move"


@pytest.fixture
async def db(postgres_url: str) -> AsyncIterator[AsyncSession]:
    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as session:
            session.execute(
                delete(BenchmarkAlias).where(BenchmarkAlias.alias == "terminal-bench-2")
            )
            session.execute(delete(Trial).where(Trial.task_id == TASK_ID))
            session.execute(delete(TaskRow).where(TaskRow.benchmark_id == PROFILE))
            session.execute(delete(Benchmark).where(Benchmark.id == PROFILE))
            session.commit()
    finally:
        sync_engine.dispose()

    engine = create_async_engine(postgres_url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            yield session
    finally:
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        try:
            with sessionmaker(sync_engine)() as session:
                session.execute(
                    delete(BenchmarkAlias).where(BenchmarkAlias.alias == "terminal-bench-2")
                )
                session.execute(delete(Trial).where(Trial.task_id == TASK_ID))
                session.execute(delete(TaskRow).where(TaskRow.benchmark_id == PROFILE))
                session.execute(delete(Benchmark).where(Benchmark.id == PROFILE))
                session.commit()
        finally:
            sync_engine.dispose()


def _task_config() -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": TASK_ID, "name": "Chess best move"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "script", "args": {"script_path": "/workspace/verifier/run.sh"}},
        "steps": [{"name": "main"}],
    }


def _manifest() -> dict[str, object]:
    lock = load_tb21_lock()
    task_config = _task_config()
    source_provenance = {
        "harbor_package_digest": lock.digest_for("terminal-bench/chess-best-move"),
        "harbor_metadata_version": lock.hub_metadata_version,
        "source_reference": {"snapshot": lock.source_revision, "divergence": None},
        "verifier_identity": "tb21-native-reward-file-v1",
        "verifier_asset": {
            "script_path": "/workspace/verifier/run.sh",
            "sha256": "sha256:" + "b" * 64,
            "mode": "0755",
        },
        "bundle_file_metadata_sha256": "sha256:" + "e" * 64,
        "image_provenance": {"docker_image": "python:3.12-slim", "cpu_arch": "x86_64"},
        "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
    }
    return {
        "schema_version": 4,
        "benchmark_id": PROFILE,
        "display_name": "Terminal-Bench 2.1 (Harbor rev 6)",
        "upstream_kind": "harbor-package",
        "upstream_locator": lock.dataset,
        "upstream_revision": lock.revision,
        "license_spdx": "Apache-2.0",
        "license_url": "https://example.test/license",
        "splits": ["test"],
        "benchmark_profile_provenance": {
            "physical_profile": PROFILE,
            "hub_metadata_version": lock.hub_metadata_version,
            "source_reference_snapshot": lock.source_revision,
            "source_reference_divergences": lock.source_manifest_divergences,
            "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
        },
        "tasks": [
            {
                "task_id": TASK_ID,
                "instance_id": "chess-best-move",
                "hf_path": "chess-best-move/",
                "checksum": "sha256:" + "a" * 64,
                "license_spdx": "Apache-2.0",
                "split": "test",
                "tags": {},
                "task_config": task_config,
                "source_provenance": source_provenance,
            }
        ],
    }


async def test_tb21_hf_registration_requires_internal_mirror() -> None:
    with pytest.raises(ValueError, match="requires mirror_to_object_store"):
        await run_register(
            benchmark="terminal-bench-2",
            source="hf",
            hf_org="test-org",
            db_url="postgresql://unused/test",
            manifest=_manifest(),
        )


async def test_register_keeps_tb21_profile_pending_and_rejects_direct_physical_selection(
    db: AsyncSession,
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest()
    monkeypatch.setattr(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
        lambda **_kwargs: manifest,
    )

    result = await run_register(
        benchmark="terminal-bench-2",
        source="object-store",
        revision="test-revision",
        object_store=FakeObjectStore(),
        db_url=postgres_url,
        registered_by="test",
        manifest=manifest,
        activate_alias=False,
    )

    benchmark = await db.get(Benchmark, PROFILE)
    task = await db.get(TaskRow, TASK_ID)
    assert result["registered"] == 1
    assert benchmark is not None
    assert benchmark.execution_state == "pending"
    assert benchmark.profile_provenance["workspace_staging_policy"] == TB21_AGENT_WORKSPACE_POLICY
    assert task is not None
    assert task.source_provenance["harbor_package_digest"].startswith("sha256:")
    assert task.source_provenance["workspace_staging_policy"] == TB21_AGENT_WORKSPACE_POLICY
    TaskConfig.model_validate(task.config)
    assert await db.get(BenchmarkAlias, "terminal-bench-2") is None
    with pytest.raises(HTTPException) as exc_info:
        await resolve_task_filter_with_diagnostics(
            db,
            {"benchmark_id": PROFILE},
        )
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["reason"] == "benchmark_not_runnable"


async def test_exact_reregister_preserves_runnable_profile_and_alias(
    db: AsyncSession,
    postgres_url: str,
) -> None:
    manifest = _manifest()
    await run_register(
        benchmark="terminal-bench-2",
        source="object-store",
        revision="test-revision",
        object_store=FakeObjectStore(),
        db_url=postgres_url,
        registered_by="test",
        manifest=manifest,
    )
    benchmark = await db.get(Benchmark, PROFILE)
    assert benchmark is not None
    benchmark.execution_state = "runnable"
    activated_provenance = dict(benchmark.profile_provenance)
    activated_provenance["activation_audit"] = {
        "schema_version": 1,
        "snapshot_id": "sha256:" + "c" * 64,
        "verified_bundles": 89,
    }
    benchmark.profile_provenance = activated_provenance
    db.add(BenchmarkAlias(alias="terminal-bench-2", benchmark_id=PROFILE))
    await db.commit()

    result = await run_register(
        benchmark="terminal-bench-2",
        source="object-store",
        revision="test-revision",
        object_store=FakeObjectStore(),
        db_url=postgres_url,
        registered_by="second-operator",
        manifest=manifest,
    )

    db.expire_all()
    benchmark = await db.get(Benchmark, PROFILE)
    alias = await db.get(BenchmarkAlias, "terminal-bench-2")
    assert result["registered"] == 0
    assert result["skipped"] == 1
    assert benchmark is not None
    assert benchmark.execution_state == "runnable"
    assert benchmark.profile_provenance["activation_audit"]["snapshot_id"] == ("sha256:" + "c" * 64)
    assert alias is not None and alias.benchmark_id == PROFILE


async def test_reregister_drift_cannot_change_task_referenced_by_queued_trial(
    db: AsyncSession,
    postgres_url: str,
) -> None:
    manifest = _manifest()
    await run_register(
        benchmark="terminal-bench-2",
        source="object-store",
        revision="test-revision",
        object_store=FakeObjectStore(),
        db_url=postgres_url,
        registered_by="test",
        manifest=manifest,
    )
    benchmark = await db.get(Benchmark, PROFILE)
    assert benchmark is not None
    benchmark.execution_state = "runnable"
    team = Team(name=f"tb21-immutability-{uuid4()}")
    db.add(team)
    await db.flush()
    queued = Trial(
        team_id=team.id,
        task_id=TASK_ID,
        config={"agent_name": "oracle", "agent_model": None},
        requires_caps={},
        state="queued",
    )
    db.add(queued)
    await db.commit()
    queued_id = queued.id

    drifted = copy.deepcopy(manifest)
    drifted["tasks"][0]["checksum"] = "sha256:" + "d" * 64  # type: ignore[index]
    with pytest.raises(ValueError, match="new physical profile ID"):
        await run_register(
            benchmark="terminal-bench-2",
            source="object-store",
            revision="test-revision",
            object_store=FakeObjectStore(),
            db_url=postgres_url,
            registered_by="drifted-operator",
            manifest=drifted,
        )

    db.expire_all()
    task = await db.get(TaskRow, TASK_ID)
    benchmark = await db.get(Benchmark, PROFILE)
    queued_after = await db.get(Trial, queued_id)
    assert task is not None and task.checksum == "sha256:" + "a" * 64
    assert benchmark is not None and benchmark.execution_state == "runnable"
    assert queued_after is not None
    assert queued_after.state == "queued"
    assert queued_after.task_id == TASK_ID


async def test_activation_writes_alias_only_after_exact_isolated_audit(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await db.execute(
        Benchmark.__table__.insert().values(
            id=PROFILE,
            display_name="Terminal-Bench 2.1 (Harbor rev 6)",
            upstream_kind="harbor-package",
            upstream_locator="terminal-bench/terminal-bench-2-1",
            upstream_revision="6",
            license_spdx="Apache-2.0",
            license_url="https://example.test/license",
            splits=["test"],
            execution_state="pending",
        ),
    )
    await db.commit()

    object_store = object()

    async def fresh_audit(
        _session: object, *, object_store: object, **_kwargs: object
    ) -> AuditResult:
        assert object_store is object_store_ref
        return AuditResult(
            profile=PROFILE,
            verified_bundles=89,
            private_workspace_isolation=True,
            snapshot_id="sha256:" + "c" * 64,
        )

    object_store_ref = object_store
    monkeypatch.setattr("loom_benchmark_tool.audit_cmd.audit_tb21_profile", fresh_audit)
    await activate_tb21_alias(
        db,
        object_store=object_store,  # type: ignore[arg-type]
        audit_evidence=AuditResult(
            profile=PROFILE,
            verified_bundles=89,
            private_workspace_isolation=True,
            snapshot_id="sha256:" + "c" * 64,
        ),
    )

    alias = (
        await db.execute(
            select(BenchmarkAlias).where(BenchmarkAlias.alias == "terminal-bench-2"),
        )
    ).scalar_one()
    assert alias.benchmark_id == PROFILE
    benchmark = await db.get(Benchmark, PROFILE)
    assert benchmark is not None
    assert benchmark.execution_state == "runnable"
    assert benchmark.profile_provenance["activation_audit"]["snapshot_id"] == ("sha256:" + "c" * 64)
