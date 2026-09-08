from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from stat import S_IMODE

import pytest

from loom_capacity_agent.secret_init import copy_projected_credentials, main

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


def test_configuration_pin_rejects_stale_projection_before_publishing_files(tmp_path: Path) -> None:
    """Break caught: a pod copies stale cached ConfigMap data under the new template digest."""
    source = tmp_path / "source"
    source.mkdir()
    for name in _FILES:
        (source / name).write_text(name)
    destination = tmp_path / "destination"
    expected = hashlib.sha256(b"current configuration").hexdigest()
    with pytest.raises(ValueError, match="configuration digest"):
        copy_projected_credentials(source, destination, configuration_sha256=expected)
    assert not destination.exists()
    assert not tuple(tmp_path.glob(".destination-*"))
    (source / "reporter-configuration.json").write_bytes(b"current configuration")
    copy_projected_credentials(source, destination, configuration_sha256=expected)
    assert (destination / "reporter-configuration.json").read_bytes() == b"current configuration"
    copy_projected_credentials(source, destination, configuration_sha256=expected)


@pytest.mark.parametrize("digest", ["", "0" * 64, "g" * 64, "a" * 63])
def test_configuration_pin_rejects_invalid_digest(tmp_path: Path, digest: str) -> None:
    """Break caught: malformed pins are treated as absent and disable verification."""
    with pytest.raises(ValueError, match="configuration digest"):
        copy_projected_credentials(
            tmp_path / "source", tmp_path / "out", configuration_sha256=digest
        )


def test_cli_enforces_configuration_pin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Break caught: the pod's CLI pin is accepted but not forwarded to verification."""
    source = tmp_path / "source"
    source.mkdir()
    for name in _FILES:
        (source / name).write_text(name)
    destination = tmp_path / "out"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "secret-init",
            "--source",
            str(source),
            "--destination",
            str(destination),
            "--configuration-sha256",
            "a" * 64,
        ],
    )
    with pytest.raises(ValueError, match="configuration digest"):
        main()
    assert not destination.exists()
