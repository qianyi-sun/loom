from __future__ import annotations

import os
from pathlib import Path
from stat import S_IMODE

import pytest

import loom.personal_dev_secret_init as secret_init
from loom.personal_dev_secret_init import (
    PersonalDevCredentialError,
    copy_projected_credentials,
)

_MANAGEMENT_FILES = {
    "admin-secrets.toml",
    "capacity-lifecycle-ca.pem",
    "capacity-lifecycle-certificate.pem",
    "capacity-lifecycle-private-key.pem",
    "capacity-lifecycle-token",
    "capacity-reporter-ca.pem",
    "capacity-reporter-certificate.pem",
    "capacity-reporter-private-key.pem",
    "config.json",
}


def _projected_source(
    tmp_path: Path,
    filenames: set[str],
    *,
    generation_name: str = "..2026_08_17_120000.000000000",
) -> Path:
    source = tmp_path / "projected"
    source.mkdir()
    generation = source / generation_name
    generation.mkdir()
    (source / "..data").symlink_to(generation.name)
    for filename in filenames:
        (generation / filename).write_bytes(f"payload:{filename}".encode())
        (source / filename).symlink_to(Path("..data") / filename)
    return source


def _destination(tmp_path: Path) -> Path:
    parent = tmp_path / "runtime"
    parent.mkdir(mode=0o700)
    return parent / "credentials"


def test_management_projection_becomes_exact_owner_only_regular_files(
    tmp_path: Path,
) -> None:
    source = _projected_source(tmp_path, _MANAGEMENT_FILES)
    destination = _destination(tmp_path)

    copy_projected_credentials(source, destination, profile="management-files")

    assert S_IMODE(destination.stat().st_mode) == 0o700
    assert destination.stat().st_uid == os.geteuid()
    assert {item.name for item in destination.iterdir()} == _MANAGEMENT_FILES
    for target in destination.iterdir():
        metadata = target.lstat()
        assert target.is_file() and not target.is_symlink()
        assert S_IMODE(metadata.st_mode) == 0o600
        assert metadata.st_uid == os.geteuid()
        assert metadata.st_nlink == 1
        assert target.read_bytes() == f"payload:{target.name}".encode()


@pytest.mark.parametrize(
    ("profile", "filename"),
    [("activation-public", "public-key"), ("activation-private", "private-key")],
)
def test_activation_profiles_copy_only_their_one_key(
    tmp_path: Path,
    profile: str,
    filename: str,
) -> None:
    source = _projected_source(tmp_path, {filename})
    destination = _destination(tmp_path)

    copy_projected_credentials(source, destination, profile=profile)  # type: ignore[arg-type]

    assert [item.name for item in destination.iterdir()] == [filename]
    assert (destination / filename).read_bytes() == f"payload:{filename}".encode()


def test_identical_replay_is_idempotent_but_changed_replay_is_rejected(
    tmp_path: Path,
) -> None:
    source = _projected_source(tmp_path, {"public-key"})
    destination = _destination(tmp_path)
    copy_projected_credentials(source, destination, profile="activation-public")
    identity = (destination / "public-key").stat().st_ino

    copy_projected_credentials(source, destination, profile="activation-public")
    assert (destination / "public-key").stat().st_ino == identity

    (source / "..2026_08_17_120000.000000000" / "public-key").write_bytes(b"changed")
    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-public")
    assert (destination / "public-key").read_bytes() == b"payload:public-key"


@pytest.mark.parametrize("problem", ["missing", "extra", "empty", "oversize", "directory"])
def test_projection_rejects_wrong_key_set_or_unbounded_nonregular_payload(
    tmp_path: Path,
    problem: str,
) -> None:
    expected = set(_MANAGEMENT_FILES)
    if problem == "missing":
        expected.remove("capacity-lifecycle-token")
    source = _projected_source(tmp_path, expected)
    generation = source / "..2026_08_17_120000.000000000"
    if problem == "extra":
        (generation / "unexpected").write_bytes(b"unexpected")
        (source / "unexpected").symlink_to(Path("..data") / "unexpected")
    elif problem == "empty":
        (generation / "capacity-lifecycle-token").write_bytes(b"")
    elif problem == "oversize":
        (generation / "capacity-lifecycle-token").write_bytes(b"x" * (1024 * 1024 + 1))
    elif problem == "directory":
        target = generation / "capacity-lifecycle-token"
        target.unlink()
        target.mkdir()
    destination = _destination(tmp_path)

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="management-files")

    assert not destination.exists()


def test_source_and_destination_symlinks_are_rejected_without_following(
    tmp_path: Path,
) -> None:
    source = _projected_source(tmp_path, {"private-key"})
    source_link = tmp_path / "source-link"
    source_link.symlink_to(source, target_is_directory=True)
    destination = _destination(tmp_path)

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source_link, destination, profile="activation-private")

    outside = tmp_path / "outside"
    outside.mkdir()
    destination.symlink_to(outside, target_is_directory=True)
    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-private")
    assert not tuple(outside.iterdir())


def test_projection_rejects_nonstandard_data_and_key_links(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, {"public-key"})
    destination = _destination(tmp_path)
    (source / "public-key").unlink()
    (source / "public-key").symlink_to(Path("..2026_08_17_120000.000000000") / "public-key")

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-public")
    assert not destination.exists()

    (source / "public-key").unlink()
    (source / "public-key").symlink_to(Path("..data") / "public-key")
    (source / "..data").unlink()
    (source / "..data").symlink_to("../outside")
    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-public")


def test_generation_rotation_before_commit_fails_without_mixing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _projected_source(tmp_path, _MANAGEMENT_FILES)
    replacement = source / "..2026_08_17_120100.000000000"
    replacement.mkdir()
    for filename in _MANAGEMENT_FILES:
        (replacement / filename).write_bytes(f"replacement:{filename}".encode())
    destination = _destination(tmp_path)
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

    monkeypatch.setattr(secret_init, "_read_bounded_descriptor", rotate_after_first_read)

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="management-files")
    assert not destination.exists()
    assert not tuple(destination.parent.glob(".credentials-*"))


def test_visible_key_link_replacement_before_commit_is_detected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _projected_source(tmp_path, {"public-key"})
    destination = _destination(tmp_path)
    original_validate = secret_init._ProjectedSnapshot.validate
    calls = 0

    def replace_link_then_validate(snapshot: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            link = source / "public-key"
            replacement = source / "public-key-next"
            replacement.symlink_to(Path("..data") / "public-key")
            replacement.replace(link)
        original_validate(snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(secret_init._ProjectedSnapshot, "validate", replace_link_then_validate)

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-public")
    assert not destination.exists()


def test_generation_file_change_before_commit_is_detected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _projected_source(tmp_path, {"public-key"})
    destination = _destination(tmp_path)
    original_validate = secret_init._ProjectedSnapshot.validate
    calls = 0

    def change_file_then_validate(snapshot: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            (source / "..2026_08_17_120000.000000000" / "public-key").write_bytes(
                b"changed-public-key"
            )
        original_validate(snapshot)  # type: ignore[arg-type]

    monkeypatch.setattr(secret_init._ProjectedSnapshot, "validate", change_file_then_validate)

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-public")
    assert not destination.exists()


@pytest.mark.parametrize("problem", ["mode", "file-mode", "hardlink", "partial", "changed"])
def test_existing_destination_must_be_one_exact_owner_only_replay(
    tmp_path: Path,
    problem: str,
) -> None:
    source = _projected_source(tmp_path, {"private-key"})
    destination = _destination(tmp_path)
    copy_projected_credentials(source, destination, profile="activation-private")
    target = destination / "private-key"
    if problem == "mode":
        destination.chmod(0o750)
    elif problem == "file-mode":
        target.chmod(0o640)
    elif problem == "hardlink":
        os.link(target, tmp_path / "credential-hardlink")
    elif problem == "partial":
        target.unlink()
    elif problem == "changed":
        target.write_bytes(b"changed")

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-private")


def test_destination_parent_must_be_private_owned_and_nonsymlink(tmp_path: Path) -> None:
    source = _projected_source(tmp_path, {"public-key"})
    destination = _destination(tmp_path)
    destination.parent.chmod(0o755)

    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(source, destination, profile="activation-public")

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir(mode=0o700)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(
            source,
            linked_parent / "credentials",
            profile="activation-public",
        )


def test_interrupted_staging_is_removed_and_error_is_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _projected_source(tmp_path, {"private-key"})
    destination = _destination(tmp_path)

    def fail_rename(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated rename failure with an internal path")

    monkeypatch.setattr(secret_init.os, "rename", fail_rename)
    with pytest.raises(PersonalDevCredentialError) as captured:
        copy_projected_credentials(source, destination, profile="activation-private")

    assert str(tmp_path) not in str(captured.value)
    assert "private-key" not in str(captured.value)
    assert not destination.exists()
    assert not tuple(destination.parent.glob(".credentials-*"))


def test_unknown_profile_is_rejected_without_reading_paths(tmp_path: Path) -> None:
    with pytest.raises(PersonalDevCredentialError):
        copy_projected_credentials(
            tmp_path / "secret-source-do-not-report",
            tmp_path / "destination-do-not-report",
            profile="unknown",  # type: ignore[arg-type]
        )
