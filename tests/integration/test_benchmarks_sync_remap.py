"""Integration tests for `[[remap]]` sync (issue #234, PR-2).

Verifies sync writes a `benchmarks` row keyed on the remap's id (not
the inherit's), with the remap's upstream + license overriding the
base adapter's defaults. Task import lives in `import_cmd.py` and is
covered by `tests/loom_benchmark_tool/test_remap_adapter_resolve.py`.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom_cli.benchmarks_config import load_benchmarks_config
from loom_cli.benchmarks_sync import SyncError, sync


class _FakeAdapter:
    """Stand-in for a REGISTRY adapter: just exposes `series` + `splits`."""

    def __init__(self, *, series: str | None, splits: tuple[str, ...]) -> None:
        self.series = series
        self.splits = splits


_FAKE_BASES = {
    "humaneval": _FakeAdapter(series="code", splits=("test",)),
}


def _lookup(inherit: str) -> _FakeAdapter | None:
    return _FAKE_BASES.get(inherit)


_REMAP_TOML = """\
schema_version = 1

[[remap]]
id = "humaneval-fork"
inherit = "humaneval"
display_name = "HumanEval (internal fork)"
upstream_kind = "huggingface"
upstream_locator = "myorg/humaneval-fork"
license_spdx = "Apache-2.0"
license_url = "https://example.org/LICENSE"
"""


@pytest.fixture
def remap_toml(tmp_path: Path) -> Path:
    p = tmp_path / "benchmarks.toml"
    p.write_text(_REMAP_TOML)
    return p


async def _cleanup(session) -> None:  # type: ignore[no-untyped-def]
    await session.execute(
        delete(Benchmark).where(Benchmark.id.in_([
            "humaneval-fork", "humaneval-fork-with-overrides",
        ])),
    )
    await session.commit()


@pytest.mark.asyncio
async def test_remap_sync_creates_row(
    postgres_url: str, remap_toml: Path,
) -> None:
    cfg = load_benchmarks_config(remap_toml)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            plan = await sync(
                cfg,
                fixtures_root=Path("/"),
                session=session,
                registry_names={"humaneval"},
                base_adapter_lookup=_lookup,
            )

        assert [(r.kind, r.id, r.action) for r in plan.rows] == [
            ("remap", "humaneval-fork", "INSERT"),
        ]

        async with factory() as session:
            row = (await session.execute(
                select(Benchmark).where(Benchmark.id == "humaneval-fork"),
            )).scalar_one()
            # Key invariant: row is keyed on remap.id, NOT inherit
            assert row.id == "humaneval-fork"
            assert row.display_name == "HumanEval (internal fork)"
            assert row.upstream_kind == "huggingface"
            assert row.upstream_locator == "myorg/humaneval-fork"
            assert row.license_spdx == "Apache-2.0"
            assert row.license_url == "https://example.org/LICENSE"
            # Series + splits inherited from base adapter when not set on remap
            assert row.series == "code"
            assert row.splits == ["test"]
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_remap_sync_idempotent(
    postgres_url: str, remap_toml: Path,
) -> None:
    cfg = load_benchmarks_config(remap_toml)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await sync(
                cfg, fixtures_root=Path("/"), session=session,
                registry_names={"humaneval"}, base_adapter_lookup=_lookup,
            )
        async with factory() as session:
            plan_two = await sync(
                cfg, fixtures_root=Path("/"), session=session,
                registry_names={"humaneval"}, base_adapter_lookup=_lookup,
            )
        assert [r.action for r in plan_two.rows] == ["SKIP"]
        assert plan_two.rows[0].reason == "unchanged"
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_remap_sync_uses_explicit_overrides(
    postgres_url: str, tmp_path: Path,
) -> None:
    body = """\
schema_version = 1

[[remap]]
id = "humaneval-fork-with-overrides"
inherit = "humaneval"
display_name = "Fork with overrides"
upstream_kind = "git"
upstream_locator = "https://git.example.org/x.git"
license_spdx = "MIT"
license_url = "https://example.org/LICENSE"
series = "internal-code"
splits = ["test", "validation"]
"""
    toml = tmp_path / "benchmarks.toml"
    toml.write_text(body)
    cfg = load_benchmarks_config(toml)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await sync(
                cfg, fixtures_root=Path("/"), session=session,
                registry_names={"humaneval"}, base_adapter_lookup=_lookup,
            )
        async with factory() as session:
            row = (await session.execute(
                select(Benchmark).where(
                    Benchmark.id == "humaneval-fork-with-overrides",
                ),
            )).scalar_one()
            # Explicit override beats base
            assert row.series == "internal-code"
            assert row.splits == ["test", "validation"]
            assert row.upstream_kind == "git"
            assert row.upstream_locator == "https://git.example.org/x.git"
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_remap_preflight_rejects_unknown_inherit(
    postgres_url: str, remap_toml: Path,
) -> None:
    cfg = load_benchmarks_config(remap_toml)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            with pytest.raises(SyncError) as exc:
                await sync(
                    cfg, fixtures_root=Path("/"), session=session,
                    # Inherit name absent from registry → preflight fails
                    registry_names=set(),
                    base_adapter_lookup=_lookup,
                )
            assert "not in REGISTRY" in str(exc.value)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_remap_dry_run_writes_no_row(
    postgres_url: str, remap_toml: Path,
) -> None:
    cfg = load_benchmarks_config(remap_toml)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await sync(
                cfg, fixtures_root=Path("/"), session=session,
                registry_names={"humaneval"}, base_adapter_lookup=_lookup,
                dry_run=True,
            )
        async with factory() as session:
            assert (await session.execute(
                select(Benchmark).where(Benchmark.id == "humaneval-fork"),
            )).scalar_one_or_none() is None
    finally:
        await engine.dispose()
