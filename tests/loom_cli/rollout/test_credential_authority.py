from __future__ import annotations

import os
import stat
import struct
from pathlib import Path

import pytest

from loom_cli.rollout.credential_authority import converge_new_private_file, read_trusted_file


def _private(path: Path, payload: bytes = b"credential\n") -> None:
    path.write_bytes(payload)
    path.chmod(0o640)


def test_private_authority_returns_stable_metadata_fingerprint(tmp_path: Path) -> None:
    path = tmp_path / "admin-token"
    _private(path)
    first = read_trusted_file(
        path,
        service_uid=os.getuid(),
        private=True,
        require_nonempty=True,
    )
    second = read_trusted_file(
        path,
        service_uid=os.getuid(),
        private=True,
        require_nonempty=True,
    )
    assert first.payload == b"credential\n"
    assert first.metadata_fingerprint == second.metadata_fingerprint
    assert len(first.metadata_fingerprint) == 64
    assert first.acl_fingerprint == second.acl_fingerprint
    assert len(first.acl_fingerprint) == 64


@pytest.mark.parametrize("mode", [0o641, 0o660, 0o740, 0o4640])
def test_private_authority_rejects_unsafe_modes(tmp_path: Path, mode: int) -> None:
    path = tmp_path / "admin-token"
    _private(path)
    path.chmod(mode)
    with pytest.raises(ValueError, match="metadata is unsafe"):
        read_trusted_file(path, service_uid=os.getuid(), private=True)


def test_private_authority_rejects_symlink_parent_and_hardlink(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    path = real / "admin-token"
    _private(path)
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    with pytest.raises(ValueError, match="traversal is unsafe"):
        read_trusted_file(linked / path.name, service_uid=os.getuid(), private=True)

    hardlink = tmp_path / "hardlink"
    os.link(path, hardlink)
    with pytest.raises(ValueError, match="metadata is unsafe"):
        read_trusted_file(path, service_uid=os.getuid(), private=True)


def test_private_authority_requires_nonempty_when_requested(tmp_path: Path) -> None:
    path = tmp_path / "admin-token"
    _private(path, b"")
    with pytest.raises(ValueError, match="metadata is unsafe"):
        read_trusted_file(
            path,
            service_uid=os.getuid(),
            private=True,
            require_nonempty=True,
        )


def _acl_payload(*entries: tuple[int, int, int]) -> bytes:
    return struct.pack("<I", 2) + b"".join(
        struct.pack("<HHI", tag, permissions, identifier)
        for tag, permissions, identifier in entries
    )


def test_private_authority_rejects_undeclared_named_acl_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admin-token"
    _private(path)
    undefined = 0xFFFFFFFF
    payload = _acl_payload(
        (0x01, 0x6, undefined),
        (0x02, 0x4, os.getuid()),
        (0x02, 0x4, os.getuid() + 1),
        (0x04, 0x0, undefined),
        (0x10, 0x4, undefined),
        (0x20, 0x0, undefined),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.credential_authority._get_acl_xattr",
        lambda _fd, _name: payload,
    )

    with pytest.raises(ValueError, match="undeclared reader"):
        read_trusted_file(path, service_uid=os.getuid(), private=True)


def test_private_authority_accepts_sanitized_service_acl_and_group_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admin-token"
    _private(path)
    undefined = 0xFFFFFFFF
    payload = _acl_payload(
        (0x01, 0x6, undefined),
        (0x02, 0x4, os.getuid()),
        (0x04, 0x4, undefined),
        (0x10, 0x4, undefined),
        (0x20, 0x0, undefined),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.credential_authority._get_acl_xattr",
        lambda _fd, _name: payload,
    )

    trusted = read_trusted_file(path, service_uid=os.getuid(), private=True)

    assert trusted.payload == b"credential\n"


def test_private_authority_rejects_acl_drift_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "admin-token"
    _private(path)
    payloads = iter((b"", b"changed"))
    monkeypatch.setattr(
        "loom_cli.rollout.credential_authority._get_acl_xattr",
        lambda _fd, _name: next(payloads),
    )

    with pytest.raises(ValueError, match="changed while it was read"):
        read_trusted_file(path, service_uid=os.getuid(), private=True)


def test_new_private_file_removes_inherited_acl_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "backup-secret"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    payloads = [b"inherited-acl", b""]
    removals: list[tuple[int, str]] = []
    monkeypatch.setattr(
        "loom_cli.rollout.credential_authority._get_acl_xattr",
        lambda _fd, _name: payloads.pop(0),
    )
    monkeypatch.setattr(
        os,
        "removexattr",
        lambda actual_fd, name: removals.append((actual_fd, name)),
        raising=False,
    )
    try:
        converge_new_private_file(fd, service_uid=os.getuid())
        os.write(fd, b"secret")
    finally:
        os.close(fd)

    assert removals == [(fd, "system.posix_acl_access")]
    assert path.read_bytes() == b"secret"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_new_private_file_fails_closed_when_acl_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "backup-secret"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    monkeypatch.setattr(
        "loom_cli.rollout.credential_authority._get_acl_xattr",
        lambda _fd, _name: b"inherited-acl",
    )
    monkeypatch.setattr(os, "removexattr", lambda _fd, _name: None, raising=False)
    try:
        with pytest.raises(ValueError, match="convergence is unsafe"):
            converge_new_private_file(fd, service_uid=os.getuid())
        assert os.fstat(fd).st_size == 0
    finally:
        os.close(fd)
