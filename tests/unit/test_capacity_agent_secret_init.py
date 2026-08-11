from __future__ import annotations

from pathlib import Path
from stat import S_IMODE

import pytest

from loom_capacity_agent.secret_init import copy_projected_credentials

_FILES = (
    "ca.pem",
    "certificate.pem",
    "database-url",
    "private-key.pem",
    "reporter-configuration.json",
    "reporter-token",
)


def test_projected_credentials_become_exact_owner_only_regular_files(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    version = source / "..2026_08_11"
    version.mkdir()
    for name in _FILES:
        (version / name).write_text(name)
        (source / name).symlink_to(Path("..2026_08_11") / name)
    destination = tmp_path / "destination"
    copy_projected_credentials(source, destination)
    assert {path.name for path in destination.iterdir()} == set(_FILES)
    assert all(not path.is_symlink() for path in destination.iterdir())
    assert all(S_IMODE(path.stat().st_mode) == 0o600 for path in destination.iterdir())

    copy_projected_credentials(source, destination)
    assert {path.name: path.read_text() for path in destination.iterdir()} == {
        name: name for name in _FILES
    }


def test_reexecuted_projected_copy_rejects_credential_drift(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for name in _FILES:
        (source / name).write_text(name)
    destination = tmp_path / "destination"
    copy_projected_credentials(source, destination)
    (source / "reporter-token").write_text("changed")

    with pytest.raises(ValueError, match="differs"):
        copy_projected_credentials(source, destination)

    assert (destination / "reporter-token").read_text() == "reporter-token"


def test_projected_credential_cannot_escape_source_volume(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("secret")
    for name in _FILES:
        (source / name).write_text(name)
    (source / "private-key.pem").unlink()
    (source / "private-key.pem").symlink_to(outside)
    with pytest.raises(ValueError, match="outside"):
        copy_projected_credentials(source, tmp_path / "destination")


def test_failed_projected_copy_leaves_no_partial_destination(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for name in _FILES[:-1]:
        (source / name).write_text(name)
    destination = tmp_path / "destination"

    with pytest.raises(OSError):
        copy_projected_credentials(source, destination)

    assert not destination.exists()
    assert not tuple(tmp_path.glob(".destination-*"))
