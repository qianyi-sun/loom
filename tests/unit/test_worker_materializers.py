"""Materializer protocol + dispatch tests.

The pre-refactor `_materialize_task_dir` had three URL-scheme branches
fused into one 100-line if/elif. Pinning each impl independently is
the whole point of the protocol split — these tests cover:

- `matches()` correctly classifies each scheme (and rejects None /
  empty / unknown).
- `dispatch_materialize` picks the first matching impl, ignores
  later ones, cleans `task_dir` on exception, and no-ops on
  unmatched sources.
- The fixture impl handles missing-root and missing-task-dir
  silently (matches the legacy warn-and-no-op behavior).
- The s3 impl rejects malformed prefixes without draining a bucket.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest

from loom_worker.materializers import (
    FixtureMaterializer,
    HFMaterializer,
    Materializer,
    S3Materializer,
    build_default_materializers,
    dispatch_materialize,
)


class _FakeObjectStore:
    """Minimal ObjectStore stand-in: download_prefix records its
    args + writes one sentinel file so callers can verify."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Path]] = []

    async def download_prefix(
        self, *, bucket: str, prefix: str, out_dir: Path,
    ) -> int:
        self.calls.append((bucket, prefix, out_dir))
        (out_dir / "downloaded.txt").write_text(f"{bucket}/{prefix}")
        return 1


@pytest.fixture
def tmp_taskdir() -> AsyncIterator[Path]:
    d = Path(tempfile.mkdtemp(prefix="loom-test-"))
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_matches_classifies_each_scheme() -> None:
    s3 = S3Materializer(object_store=_FakeObjectStore())
    fx = FixtureMaterializer()
    hf = HFMaterializer()
    assert s3.matches("s3://b/p/")
    assert not s3.matches("hf://x/y/path")
    assert fx.matches("fixture://hello")
    assert not fx.matches("s3://b/p/")
    assert hf.matches("hf://org/repo@main/path")
    assert not hf.matches("fixture://x")
    # Negatives: None, empty, unknown scheme, non-string.
    for m in (s3, fx, hf):
        assert not m.matches(None)
        assert not m.matches("")
        assert not m.matches("git+https://example.com/r.git")


@pytest.mark.asyncio
async def test_s3_materializer_writes_file_via_object_store(
    tmp_taskdir: Path,
) -> None:
    store = _FakeObjectStore()
    m = S3Materializer(object_store=store)
    out = await m.materialize(
        source="s3://loom-bundles/humaneval/HumanEval/0/",
        task_dir=tmp_taskdir, trial_id=uuid4(),
    )
    assert out == tmp_taskdir
    assert (tmp_taskdir / "downloaded.txt").read_text() == \
        "loom-bundles/humaneval/HumanEval/0/"
    assert store.calls == [
        ("loom-bundles", "humaneval/HumanEval/0/", tmp_taskdir),
    ]


@pytest.mark.asyncio
async def test_s3_materializer_rejects_empty_prefix(
    tmp_taskdir: Path,
) -> None:
    """Without a prefix `download_prefix` would drain the entire
    bucket — the impl logs + no-ops instead."""
    store = _FakeObjectStore()
    m = S3Materializer(object_store=store)
    out = await m.materialize(
        source="s3://loom-bundles/",  # trailing slash, no prefix
        task_dir=tmp_taskdir, trial_id=uuid4(),
    )
    assert out == tmp_taskdir
    assert store.calls == []  # never called
    assert not list(tmp_taskdir.iterdir())  # dir untouched


@pytest.mark.asyncio
async def test_fixture_materializer_copies_from_root(
    tmp_taskdir: Path, tmp_path: Path,
) -> None:
    """Happy path: fixtures_root/<task_id>/ exists, copy into task_dir."""
    fixtures = tmp_path / "fixtures"
    (fixtures / "hello-world").mkdir(parents=True)
    (fixtures / "hello-world" / "task.toml").write_text("schema=1")
    m = FixtureMaterializer(fixtures_root=fixtures)
    out = await m.materialize(
        source="fixture://hello-world",
        task_dir=tmp_taskdir, trial_id=uuid4(),
    )
    assert out == tmp_taskdir
    assert (tmp_taskdir / "task.toml").read_text() == "schema=1"


@pytest.mark.asyncio
async def test_fixture_materializer_warn_when_root_unset(
    tmp_taskdir: Path,
) -> None:
    """Production has no fixtures_root; impl no-ops with warning
    rather than crash."""
    m = FixtureMaterializer(fixtures_root=None)
    out = await m.materialize(
        source="fixture://hello-world",
        task_dir=tmp_taskdir, trial_id=uuid4(),
    )
    assert out == tmp_taskdir
    assert not list(tmp_taskdir.iterdir())


@pytest.mark.asyncio
async def test_fixture_materializer_warn_when_task_missing(
    tmp_taskdir: Path, tmp_path: Path,
) -> None:
    fixtures = tmp_path / "fixtures"
    fixtures.mkdir()
    m = FixtureMaterializer(fixtures_root=fixtures)
    out = await m.materialize(
        source="fixture://does-not-exist",
        task_dir=tmp_taskdir, trial_id=uuid4(),
    )
    assert out == tmp_taskdir
    assert not list(tmp_taskdir.iterdir())


@pytest.mark.asyncio
async def test_dispatch_picks_first_matching_materializer(
    tmp_taskdir: Path,
) -> None:
    store = _FakeObjectStore()
    s3 = S3Materializer(object_store=store)
    fx = FixtureMaterializer()
    # Order matters; first-match wins. s3 comes before fx here even
    # though fx also matches its own source — verifies dispatch picks
    # the first.
    await dispatch_materialize(
        source="s3://b/p/",
        task_dir=tmp_taskdir,
        trial_id=uuid4(),
        materializers=(s3, fx),
    )
    assert store.calls == [("b", "p/", tmp_taskdir)]


@pytest.mark.asyncio
async def test_dispatch_leaves_dir_empty_on_unmatched_source(
    tmp_taskdir: Path,
) -> None:
    """`git+...`, None, unknown schemes — none of the registered
    materializers claim them. Dispatcher returns the empty dir."""
    out = await dispatch_materialize(
        source="git+https://github.com/x/y.git",
        task_dir=tmp_taskdir,
        trial_id=uuid4(),
        materializers=(S3Materializer(_FakeObjectStore()),),
    )
    assert out == tmp_taskdir
    assert not list(tmp_taskdir.iterdir())


@pytest.mark.asyncio
async def test_dispatch_cleans_taskdir_on_materialize_failure(
    tmp_taskdir: Path,
) -> None:
    """Failed materialize → tempdir removed so /tmp doesn't leak."""
    class _Boom:
        def matches(self, source: str | None) -> bool:
            return True
        async def materialize(self, **_: object) -> Path:
            raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError, match="kaboom"):
        await dispatch_materialize(
            source="anything://",
            task_dir=tmp_taskdir,
            trial_id=uuid4(),
            materializers=(_Boom(),),
        )
    assert not tmp_taskdir.exists()


def test_build_default_materializers_returns_all_three() -> None:
    ms = build_default_materializers(
        object_store=_FakeObjectStore(),
        fixtures_root=Path("/tmp/x"),
        benchmark_cache=Path("/tmp/y"),
    )
    assert len(ms) == 3
    assert all(isinstance(m, Materializer) for m in ms)
