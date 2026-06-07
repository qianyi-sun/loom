"""fetch_upstream dispatcher contract — fully offline (Plan 14 Task 4)."""

from __future__ import annotations

import hashlib
import tarfile
from io import BytesIO
from pathlib import Path

import httpx
import pytest
from loom_benchmarks.base import UpstreamSource
from loom_benchmarks.fetch import cache_key, fetch_upstream


def test_cache_key_stable() -> None:
    src = UpstreamSource(
        kind="huggingface", locator="openai_humaneval", revision="abc",
    )
    k1 = cache_key(src)
    k2 = cache_key(src)
    assert k1 == k2
    expected = hashlib.sha256(b"openai_humaneval\x00abc").hexdigest()
    assert k1 == expected


def test_cache_key_includes_subset() -> None:
    src1 = UpstreamSource(kind="huggingface", locator="mmlu", revision="r1")
    src2 = UpstreamSource(
        kind="huggingface", locator="mmlu", revision="r1", subset="abstract_algebra",
    )
    assert cache_key(src1) != cache_key(src2)


def test_fetch_huggingface_reuses_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class _FakeDS:
        @staticmethod
        def load_dataset(
            name: str, subset: str | None = None, revision: str | None = None,
            cache_dir: str | None = None,
        ) -> dict[str, str]:
            calls.append(name)
            assert cache_dir is not None
            (Path(cache_dir) / "DONE").write_text("ok")
            return {"_": "ok"}

    monkeypatch.setattr("loom_benchmarks.fetch.datasets", _FakeDS)
    src = UpstreamSource(
        kind="huggingface", locator="openai_humaneval", revision="r1",
    )
    p1 = fetch_upstream(src, cache_root=tmp_path)
    p2 = fetch_upstream(src, cache_root=tmp_path)
    assert p1 == p2
    assert len(calls) == 1  # second call reused cache
    assert (p1 / "DONE").exists()


def test_fetch_tarball_extracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("hello.txt")
        payload = b"world\n"
        info.size = len(payload)
        tar.addfile(info, BytesIO(payload))
    tar_bytes = buf.getvalue()

    class _R:
        content = tar_bytes
        def raise_for_status(self) -> None:
            return None

    def _fake_get(url: str, **kw: object) -> _R:
        return _R()

    monkeypatch.setattr(httpx, "get", _fake_get)

    src = UpstreamSource(
        kind="https-tarball",
        locator="https://example.com/data.tar.gz",
        revision="v1",
    )
    out = fetch_upstream(src, cache_root=tmp_path)
    assert (out / "hello.txt").read_text() == "world\n"


def test_fetch_refresh_redownloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class _FakeDS:
        @staticmethod
        def load_dataset(
            name: str, subset: str | None = None, revision: str | None = None,
            cache_dir: str | None = None,
        ) -> dict[str, str]:
            calls.append(name)
            assert cache_dir is not None
            (Path(cache_dir) / "OK").write_text("ok")
            return {"_": "ok"}

    monkeypatch.setattr("loom_benchmarks.fetch.datasets", _FakeDS)
    src = UpstreamSource(kind="huggingface", locator="x", revision="r")
    fetch_upstream(src, cache_root=tmp_path)
    fetch_upstream(src, cache_root=tmp_path, refresh=True)
    assert len(calls) == 2


def test_unknown_kind_rejected(tmp_path: Path) -> None:
    src = UpstreamSource(
        kind="ftp",  # type: ignore[arg-type]
        locator="ftp://nope",
    )
    with pytest.raises(ValueError, match=r"unknown UpstreamSource\.kind"):
        fetch_upstream(src, cache_root=tmp_path)
