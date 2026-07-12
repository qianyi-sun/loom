"""TB2.1 register and atomic public-alias activation coverage."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock
from sqlalchemy import create_engine, delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, BenchmarkAlias
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom_benchmark_tool.audit_cmd import AuditResult, activate_tb21_alias
from loom_benchmark_tool.manifest import TB21_AGENT_WORKSPACE_POLICY
from loom_benchmark_tool.register_cmd import run_register

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


async def test_register_persists_profile_and_task_provenance(
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
        hf_org="test-org",
        hf_token=None,
        db_url=postgres_url,
        registered_by="test",
        manifest=manifest,
        activate_alias=False,
    )

    benchmark = await db.get(Benchmark, PROFILE)
    task = await db.get(TaskRow, TASK_ID)
    assert result["registered"] == 1
    assert benchmark is not None
    assert benchmark.execution_state == "runnable"
    assert benchmark.profile_provenance["workspace_staging_policy"] == TB21_AGENT_WORKSPACE_POLICY
    assert task is not None
    assert task.source_provenance["harbor_package_digest"].startswith("sha256:")
    assert task.source_provenance["workspace_staging_policy"] == TB21_AGENT_WORKSPACE_POLICY
    TaskConfig.model_validate(task.config)
    assert await db.get(BenchmarkAlias, "terminal-bench-2") is None


async def test_activation_writes_alias_only_after_exact_isolated_audit(
    db: AsyncSession,
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
        ),
    )
    await db.commit()

    await activate_tb21_alias(
        db,
        AuditResult(
            profile=PROFILE,
            verified_bundles=89,
            private_workspace_isolation=True,
        ),
    )

    alias = (
        await db.execute(
            select(BenchmarkAlias).where(BenchmarkAlias.alias == "terminal-bench-2"),
        )
    ).scalar_one()
    assert alias.benchmark_id == PROFILE
