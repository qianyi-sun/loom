"""fetch_upstream dispatcher contract — fully offline (Plan 14 Task 4)."""

from __future__ import annotations

import hashlib
import tarfile
from io import BytesIO
from pathlib import Path

import httpx
import pytest
from loom_benchmarks.base import UpstreamSource
from loom_benchmarks.fetch import _looks_like_sha, cache_key, fetch_upstream


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


def test_looks_like_sha_recognizes_short_and_full() -> None:
    assert _looks_like_sha("abc1234")
    assert _looks_like_sha("d0d4f3c2c8a5a2e6e3e2f6b3f7e2c5c9c8b9d4e7")
    assert not _looks_like_sha("main")
    assert not _looks_like_sha("v1.0")
    assert not _looks_like_sha("HEAD")
    assert not _looks_like_sha("ABCDEF1")  # uppercase: not a SHA
    assert not _looks_like_sha("xyz1234")


def test_git_fetch_uses_init_fetch_checkout_for_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SHA revision must go through git init + fetch + checkout,
    not `git clone --branch <sha>` which silently fails."""
    import subprocess as _sp
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], **kw: object) -> object:
        calls.append(cmd)

        class _R:
            returncode = 0
        return _R()

    monkeypatch.setattr(_sp, "run", _fake_run)
    monkeypatch.setattr(
        "loom_benchmarks.fetch.subprocess.run", _fake_run,
    )

    src = UpstreamSource(
        kind="git",
        locator="https://example.com/repo.git",
        revision="d0d4f3c2c8a5a2e6e3e2f6b3f7e2c5c9c8b9d4e7",
    )
    fetch_upstream(src, cache_root=tmp_path)
    # 4 calls: init, remote add, fetch, checkout — NOT a single `clone`.
    cmds = [c[0:2] for c in calls]
    assert ["git", "init"] in cmds
    assert ["git", "remote"] in cmds
    assert ["git", "fetch"] in cmds
    assert ["git", "checkout"] in cmds
    assert not any(c[:2] == ["git", "clone"] for c in calls)


def test_git_fetch_uses_clone_for_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], **kw: object) -> object:
        calls.append(cmd)

        class _R:
            returncode = 0
        return _R()

    monkeypatch.setattr(
        "loom_benchmarks.fetch.subprocess.run", _fake_run,
    )

    src = UpstreamSource(
        kind="git",
        locator="https://example.com/repo.git",
        revision="main",
    )
    fetch_upstream(src, cache_root=tmp_path)
    assert len(calls) == 1
    assert calls[0][:4] == ["git", "clone", "--depth", "1"]
    assert "--branch" in calls[0]
    assert "main" in calls[0]


def test_unknown_kind_rejected(tmp_path: Path) -> None:
    src = UpstreamSource(
        kind="ftp",  # type: ignore[arg-type]
        locator="ftp://nope",
    )
    with pytest.raises(ValueError, match=r"unknown UpstreamSource\.kind"):
        fetch_upstream(src, cache_root=tmp_path)
