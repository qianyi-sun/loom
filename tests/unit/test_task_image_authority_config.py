from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_task_image_authority import config
from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    read_owner_only_bytes,
    read_owner_only_secret,
)


def _owner_only(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def test_owner_only_reader_returns_exact_bounded_bytes(tmp_path: Path) -> None:
    path = _owner_only(tmp_path / "secret", b"exact\x00bytes")

    assert read_owner_only_bytes(path, max_bytes=11) == b"exact\x00bytes"


@pytest.mark.parametrize("max_bytes", [0, -1, True, 1.5])
def test_owner_only_reader_rejects_invalid_bounds(tmp_path: Path, max_bytes: Any) -> None:
    path = _owner_only(tmp_path / "secret", b"value")

    with pytest.raises(ValueError, match="positive integer"):
        read_owner_only_bytes(path, max_bytes=max_bytes)


def test_owner_only_reader_rejects_unsafe_file_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _owner_only(tmp_path / "secret", b"value")
    path.chmod(0o640)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="0600"):
        read_owner_only_bytes(path)

    path.chmod(0o600)
    link = tmp_path / "secret-link"
    link.symlink_to(path)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="nonsymlink"):
        read_owner_only_bytes(link)

    fifo = tmp_path / "secret-fifo"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="nonsymlink"):
        read_owner_only_bytes(fifo)

    current_uid = os.getuid()
    monkeypatch.setattr(config.os, "getuid", lambda: current_uid + 1)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="current-uid"):
        read_owner_only_bytes(path)


def test_owner_only_reader_rejects_oversize_and_metadata_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized = _owner_only(tmp_path / "oversized", b"12345")
    with pytest.raises(TaskImageAuthorityConfigurationError, match="maximum byte size"):
        read_owner_only_bytes(oversized, max_bytes=4)

    path = _owner_only(tmp_path / "secret", b"value")
    real_fstat = os.fstat
    calls = 0

    def changed_second_fstat(descriptor: int) -> os.stat_result | SimpleNamespace:
        nonlocal calls
        calls += 1
        metadata = real_fstat(descriptor)
        if calls == 1:
            return metadata
        return SimpleNamespace(
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino,
            st_mode=metadata.st_mode,
            st_uid=metadata.st_uid,
            st_size=metadata.st_size,
            st_mtime_ns=metadata.st_mtime_ns,
            st_ctime_ns=metadata.st_ctime_ns + 1,
        )

    monkeypatch.setattr(config.os, "fstat", changed_second_fstat)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="changed while reading"):
        read_owner_only_bytes(path)


def test_secret_reader_accepts_one_line_and_rejects_ambiguous_text(tmp_path: Path) -> None:
    assert read_owner_only_secret(_owner_only(tmp_path / "valid", b"value\n")) == "value"

    private = b"TOP_PRIVATE_VALUE"
    invalid_payloads = [b"", private + b"\nsecond", private + b"\r\n", private + b"\x00", b"\xff"]
    for index, payload in enumerate(invalid_payloads):
        path = _owner_only(tmp_path / f"invalid-{index}", payload)
        with pytest.raises(TaskImageAuthorityConfigurationError) as caught:
            read_owner_only_secret(path)
        assert private.decode("ascii") not in str(caught.value)
