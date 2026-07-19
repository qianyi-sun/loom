from __future__ import annotations

import os
from pathlib import Path

import pytest

from loom_cli.rollout.credential_authority import read_trusted_file


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
