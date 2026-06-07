"""Shared conversion helpers contract (Plan 14 Task 3)."""

from __future__ import annotations

import base64
from pathlib import Path

import httpx
import pytest

from loom_benchmarks.util import (
    download_files_from_record,
    embed_base64_image,
    pytest_from_test_strings,
    pytest_from_unittest,
    sha256_of_dir,
    structured_verifier_script,
)


def test_pytest_from_test_strings_writes_per_case_files(tmp_path: Path) -> None:
    written = pytest_from_test_strings(
        ["assert add(1, 2) == 3", "assert add(0, 0) == 0"],
        out_dir=tmp_path, prefix="add",
    )
    assert len(written) == 2
    txt = (tmp_path / "test_add_0.py").read_text()
    assert "assert add(1, 2) == 3" in txt
    assert "def test_add_0()" in txt


def test_pytest_from_unittest_wraps_testcase(tmp_path: Path) -> None:
    src = (
        "import unittest\n"
        "class T(unittest.TestCase):\n"
        "    def test_x(self):\n"
        "        self.assertEqual(1, 1)\n"
    )
    out = pytest_from_unittest(src, out_dir=tmp_path)
    assert out.exists()
    assert "import unittest" in out.read_text()
    assert "class T(unittest.TestCase)" in out.read_text()


def test_structured_verifier_script_writes_run_sh(tmp_path: Path) -> None:
    structured_verifier_script(
        'echo {"pass": true} > $LOOM_VERIFIER_OUTPUT', out_dir=tmp_path,
    )
    run_sh = tmp_path / "verifier" / "run.sh"
    assert run_sh.exists()
    assert run_sh.stat().st_mode & 0o111  # executable


def test_embed_base64_image_returns_markdown() -> None:
    md = embed_base64_image(b"\x89PNG\r\n\x1a\n", alt_text="screenshot")
    assert md.startswith("![screenshot](data:image/png;base64,")
    decoded = base64.b64decode(md.split("base64,", 1)[1].rstrip(")"))
    assert decoded.startswith(b"\x89PNG")


def test_embed_base64_image_detects_jpeg() -> None:
    md = embed_base64_image(b"\xff\xd8\xff\xe0junk", alt_text="x")
    assert "data:image/jpeg;base64," in md


def test_embed_base64_image_unknown_format_octet_stream() -> None:
    md = embed_base64_image(b"random bytes", alt_text="x")
    assert "data:application/octet-stream;base64," in md


def test_sha256_of_dir_deterministic(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("world")
    first = sha256_of_dir(tmp_path)
    second = sha256_of_dir(tmp_path)
    assert first == second
    assert len(first) == 64


def test_sha256_of_dir_differs_on_content_change(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("hello")
    h1 = sha256_of_dir(tmp_path)
    (tmp_path / "a.txt").write_text("hello!")
    h2 = sha256_of_dir(tmp_path)
    assert h1 != h2


def test_download_files_from_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _R:
        content = b"binary-blob"
        def raise_for_status(self) -> None:
            return None

    def _fake_get(url: str, **kw: object) -> _R:
        return _R()

    monkeypatch.setattr(httpx, "get", _fake_get)
    paths = download_files_from_record(
        {"file_url": "https://example.com/data.bin"},
        out_dir=tmp_path, fields=("file_url",),
    )
    assert len(paths) == 1
    assert paths[0].read_bytes() == b"binary-blob"


def test_download_files_from_record_skips_missing_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail_get(url: str, **kw: object) -> object:
        raise AssertionError("should not call httpx.get when field missing")

    monkeypatch.setattr(httpx, "get", _fail_get)
    paths = download_files_from_record(
        {}, out_dir=tmp_path, fields=("absent_field",),
    )
    assert paths == []
