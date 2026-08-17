from __future__ import annotations

from pathlib import Path
from stat import S_IMODE

import pytest

import loom_capacity_manager.secret_init as secret_init
from loom_capacity_manager.secret_init import copy_projected_credentials

_MANAGER_FILES = {
    "client-ca.pem",
    "database-url",
    "health-certificate.pem",
    "health-private-key.pem",
    "ownership-public-keys.json",
    "principals.json",
    "server-ca.pem",
    "server-certificate.pem",
    "server-private-key.pem",
}


def _projected_source(tmp_path: Path, filenames: set[str]) -> Path:
    source = tmp_path / "projected"
    source.mkdir()
    version = source / "..2026_08_11_120000"
    version.mkdir()
    (source / "..data").symlink_to(version.name)
    for filename in filenames:
        (version / filename).write_text(f"payload:{filename}", encoding="utf-8")
        (source / filename).symlink_to(Path("..data") / filename)
    return source


def test_manager_credentials_become_exact_owner_only_regular_files(
    tmp_path: Path,
) -> None:
    source = _projected_source(tmp_path, _MANAGER_FILES)
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    copy_projected_credentials(source, destination, profile="manager")

    assert S_IMODE(destination.stat().st_mode) == 0o700
    assert {path.name for path in destination.iterdir()} == _MANAGER_FILES
    assert all(path.is_file() and not path.is_symlink() for path in destination.iterdir())
    assert all(S_IMODE(path.stat().st_mode) == 0o600 for path in destination.iterdir())
    assert {path.name: path.read_text(encoding="utf-8") for path in destination.iterdir()} == {
        filename: f"payload:{filename}" for filename in _MANAGER_FILES
    }


def test_migration_profile_contains_only_the_database_url(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, {"database-url"})
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    copy_projected_credentials(source, destination, profile="migration")

    assert [path.name for path in destination.iterdir()] == ["database-url"]
    assert S_IMODE((destination / "database-url").stat().st_mode) == 0o600


def test_execution_policy_profile_becomes_one_owner_only_regular_file(
    tmp_path: Path,
) -> None:
    source = _projected_source(tmp_path, {"execution-policy.json"})
    destination = tmp_path / "runtime-policy" / "execution-policy"
    destination.parent.mkdir()

    copy_projected_credentials(source, destination, profile="execution-policy")

    assert S_IMODE(destination.stat().st_mode) == 0o700
    assert [path.name for path in destination.iterdir()] == ["execution-policy.json"]
    policy = destination / "execution-policy.json"
    assert policy.is_file() and not policy.is_symlink()
    assert S_IMODE(policy.stat().st_mode) == 0o600
    assert policy.stat().st_uid == destination.stat().st_uid


def test_identical_rerun_is_idempotent_but_credential_drift_is_rejected(
    tmp_path: Path,
) -> None:
    source = _projected_source(tmp_path, _MANAGER_FILES)
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()
    copy_projected_credentials(source, destination, profile="manager")

    copy_projected_credentials(source, destination, profile="manager")
    (source / "..2026_08_11_120000" / "database-url").write_text(
        "changed",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="differs"):
        copy_projected_credentials(source, destination, profile="manager")
    assert (destination / "database-url").read_text(encoding="utf-8") == ("payload:database-url")


def test_projected_credential_cannot_escape_its_volume(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, _MANAGER_FILES)
    outside = tmp_path / "outside-secret"
    outside.write_text("outside", encoding="utf-8")
    (source / "database-url").unlink()
    (source / "database-url").symlink_to(outside)
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    with pytest.raises(ValueError, match="outside"):
        copy_projected_credentials(source, destination, profile="manager")

    assert not destination.exists()


def test_projected_credentials_are_nonempty_and_bounded(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, {"database-url"})
    projected = source / "..2026_08_11_120000" / "database-url"
    projected.write_bytes(b"x" * (1024 * 1024 + 1))
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    with pytest.raises(ValueError, match="bounded"):
        copy_projected_credentials(source, destination, profile="migration")
    assert not destination.exists()

    projected.write_bytes(b"")
    with pytest.raises(ValueError, match="bounded"):
        copy_projected_credentials(source, destination, profile="migration")
    assert not destination.exists()


def test_missing_or_extra_projected_key_is_rejected(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, _MANAGER_FILES - {"database-url"})
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    with pytest.raises(ValueError, match="file set"):
        copy_projected_credentials(source, destination, profile="manager")

    extra = source / "unexpected-token"
    extra.write_text("unexpected", encoding="utf-8")
    with pytest.raises(ValueError, match="file set"):
        copy_projected_credentials(source, destination, profile="manager")


def test_failed_copy_leaves_no_partial_or_staging_directory(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, _MANAGER_FILES)
    (source / "database-url").unlink()
    (source / "database-url").symlink_to("missing-target")
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    with pytest.raises(OSError):
        copy_projected_credentials(source, destination, profile="manager")

    assert not destination.exists()
    assert not tuple(destination.parent.glob(".credentials-*"))


def test_projection_rotation_during_copy_fails_without_mixing_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _projected_source(tmp_path, _MANAGER_FILES)
    replacement = source / "..2026_08_11_120100"
    replacement.mkdir()
    for filename in _MANAGER_FILES:
        (replacement / filename).write_text(
            f"replacement:{filename}",
            encoding="utf-8",
        )
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()
    original_read = secret_init._read_bounded_descriptor
    reads = 0

    def rotate_after_first_read(descriptor: int, *, expected_size: int) -> bytes:
        nonlocal reads
        payload = original_read(descriptor, expected_size=expected_size)
        reads += 1
        if reads == 1:
            next_link = source / "..data-next"
            next_link.symlink_to(replacement.name)
            next_link.replace(source / "..data")
        return payload

    monkeypatch.setattr(
        secret_init,
        "_read_bounded_descriptor",
        rotate_after_first_read,
    )

    with pytest.raises(ValueError, match="generation changed"):
        copy_projected_credentials(source, destination, profile="manager")

    assert not destination.exists()
    assert not tuple(destination.parent.glob(".credentials-*"))


def test_projection_requires_standard_data_key_symlinks(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, {"database-url"})
    (source / "database-url").unlink()
    (source / "database-url").symlink_to(Path("..2026_08_11_120000") / "database-url")
    destination = tmp_path / "runtime" / "credentials"
    destination.parent.mkdir()

    with pytest.raises(ValueError, match="symlink layout"):
        copy_projected_credentials(source, destination, profile="migration")

    assert not destination.exists()
