"""Security and reproducibility contracts for the local Aider wheel."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import os
import stat
import tempfile
import warnings
import zipfile
from collections.abc import Callable
from email.parser import BytesParser
from pathlib import Path

import pytest

from loom_launcher.aider_distribution import (
    AiderDistributionError,
    _rebuild_aider_wheel,
    main,
    patch_aider_wheel,
)

SOURCE_NAME = "aider_chat-0.86.2-py3-none-any.whl"
OUTPUT_NAME = "aider_chat-0.86.2+loom.1-py3-none-any.whl"
DIST_INFO = "aider_chat-0.86.2.dist-info"
LOCAL_DIST_INFO = "aider_chat-0.86.2+loom.1.dist-info"
APPLICATION = b'__version__ = "0.86.2"\n'
METADATA = (
    b"Metadata-Version: 2.1\n"
    b"Name: aider-chat\n"
    b"Version: 0.86.2\n"
    b"Summary: fixture\n"
    b"Requires-Dist: litellm==1.81.10\n"
    b"Requires-Dist: importlib-metadata==7.2.1\n"
    b"Requires-Dist: requests==2.32.3\n\n"
)
WHEEL = (
    b"Wheel-Version: 1.0\n"
    b"Generator: fixture\n"
    b"Root-Is-Purelib: true\n"
    b"Tag: py3-none-any\n\n"
)
RECORD = (
    b"aider/__init__.py,sha256=p2w1lOZCsNixTcC4CwTkeqA-kqhIGKq5OE-mIQKipaY,23\n"
    b"aider_chat-0.86.2.dist-info/METADATA,"
    b"sha256=Awsb6NcI7dEfuRfMKA1eSVidpmuT1kpkp_d9XVXVhZo,178\n"
    b"aider_chat-0.86.2.dist-info/RECORD,,\n"
    b"aider_chat-0.86.2.dist-info/WHEEL,"
    b"sha256=2_IrOF1jR2xNXEM3zpoG00Cl9D_z2vSqNSI6jX8JKVo,79\n"
)


def _default_members() -> list[tuple[str, bytes, int]]:
    return [
        ("aider/__init__.py", APPLICATION, 0o755),
        (f"{DIST_INFO}/METADATA", METADATA, 0o644),
        (f"{DIST_INFO}/WHEEL", WHEEL, 0o644),
        (f"{DIST_INFO}/RECORD", RECORD, 0o644),
    ]


def _write_wheel(
    path: Path,
    members: list[tuple[str, bytes, int]] | None = None,
) -> Path:
    wheel_members = members or _default_members()
    if members is not None:
        record_stream = io.StringIO(newline="")
        writer = csv.writer(record_stream, lineterminator="\n")
        for name, payload, _mode in sorted(wheel_members):
            if name == f"{DIST_INFO}/RECORD":
                writer.writerow((name, "", ""))
            else:
                digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=")
                writer.writerow((name, f"sha256={digest.decode('ascii')}", str(len(payload))))
        generated_record = record_stream.getvalue().encode()
        wheel_members = [
            (name, generated_record if name == f"{DIST_INFO}/RECORD" else payload, mode)
            for name, payload, mode in wheel_members
        ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            for name, payload, mode in wheel_members:
                info = zipfile.ZipInfo(name, (2024, 1, 2, 3, 4, 6))
                info.create_system = 3
                info.external_attr = mode << 16
                archive.writestr(info, payload)
    return path


def _rebuild_fixture(source: Path, output_directory: Path) -> Path:
    return _rebuild_aider_wheel(
        source,
        output_directory,
        expected_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
    )


def _make_owner_controlled_directory(path: Path) -> Path:
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


def _verify_record(wheel: Path) -> list[str]:
    errors: list[str] = []
    with zipfile.ZipFile(wheel) as archive:
        record_name = f"{LOCAL_DIST_INFO}/RECORD"
        rows = list(csv.reader(io.StringIO(archive.read(record_name).decode("utf-8"))))
        expected_names = sorted(archive.namelist())
        if [row[0] for row in rows] != expected_names:
            errors.append("RECORD names are not the sorted archive names")
        for name, digest, size in rows:
            if name == record_name:
                if digest or size:
                    errors.append("RECORD must not hash itself")
                continue
            payload = archive.read(name)
            wanted = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=")
            if digest != f"sha256={wanted.decode('ascii')}":
                errors.append(f"wrong digest for {name}")
            if size != str(len(payload)):
                errors.append(f"wrong size for {name}")
    return errors


def _mutated_members(
    *,
    replace_name: tuple[str, str] | None = None,
    replace_payload: tuple[str, bytes] | None = None,
    append: tuple[str, bytes, int] | None = None,
    remove: str | None = None,
) -> list[tuple[str, bytes, int]]:
    members = _default_members()
    if replace_name is not None:
        old, new = replace_name
        members = [(new if name == old else name, data, mode) for name, data, mode in members]
    if replace_payload is not None:
        target, payload = replace_payload
        members = [(name, payload if name == target else data, mode) for name, data, mode in members]
    if remove is not None:
        members = [(name, data, mode) for name, data, mode in members if name != remove]
    if append is not None:
        members.append(append)
    return members


def test_rebuilds_only_distribution_metadata_and_generates_a_valid_record(
    tmp_path: Path,
) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    output = _rebuild_fixture(source, output_directory)

    assert output.name == OUTPUT_NAME
    assert output.is_absolute()
    assert stat.S_IMODE(output.stat().st_mode) == 0o444
    with zipfile.ZipFile(output) as archive:
        metadata = BytesParser().parsebytes(archive.read(f"{LOCAL_DIST_INFO}/METADATA"))
        assert metadata["Version"] == "0.86.2+loom.1"
        assert "litellm==1.84.1" in metadata.get_all("Requires-Dist")
        assert "importlib-metadata==8.9.0" in metadata.get_all("Requires-Dist")
        assert "requests==2.32.3" in metadata.get_all("Requires-Dist")
        assert archive.read("aider/__init__.py") == APPLICATION
        assert f"{DIST_INFO}/METADATA" not in archive.namelist()
        assert all(info.date_time == (1980, 1, 1, 0, 0, 0) for info in archive.infolist())
        assert all(info.compress_type == zipfile.ZIP_DEFLATED for info in archive.infolist())
        assert [info.filename for info in archive.infolist()] == sorted(archive.namelist())
        modes = {info.filename: stat.S_IMODE(info.external_attr >> 16) for info in archive.infolist()}
        assert modes["aider/__init__.py"] == 0o755
        assert modes[f"{LOCAL_DIST_INFO}/METADATA"] == 0o644
    assert _verify_record(output) == []


def test_rebuild_is_byte_deterministic(tmp_path: Path) -> None:
    first_source = _write_wheel(tmp_path / SOURCE_NAME)
    first_directory = tmp_path / "first"
    second_directory = tmp_path / "second"
    _make_owner_controlled_directory(first_directory)
    _make_owner_controlled_directory(second_directory)

    first = _rebuild_fixture(first_source, first_directory)
    second = _rebuild_fixture(first_source, second_directory)

    assert first.read_bytes() == second.read_bytes()


def test_public_wrapper_rejects_an_unreviewed_wheel(tmp_path: Path) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="source SHA-256"):
        patch_aider_wheel(source, output_directory)

    assert list(output_directory.iterdir()) == []


@pytest.mark.parametrize(
    ("member_name", "message"),
    [
        ("/absolute.py", "unsafe archive member"),
        ("../escape.py", "unsafe archive member"),
        ("aider/../escape.py", "unsafe archive member"),
        (r"aider\\escape.py", "backslash"),
    ],
)
def test_rejects_unsafe_member_names(
    tmp_path: Path,
    member_name: str,
    message: str,
) -> None:
    members = _mutated_members(append=(member_name, b"secret archive payload", 0o644))
    source = _write_wheel(tmp_path / SOURCE_NAME, members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match=message):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


def test_rejects_duplicate_members(tmp_path: Path) -> None:
    members = _mutated_members(append=("aider/__init__.py", b"duplicate", 0o644))
    source = _write_wheel(tmp_path / SOURCE_NAME, members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="duplicate archive member"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


def test_rejects_symlink_members(tmp_path: Path) -> None:
    members = _mutated_members(append=("aider/link", b"/etc/passwd", stat.S_IFLNK | 0o777))
    source = _write_wheel(tmp_path / SOURCE_NAME, members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="regular file"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


def test_rejects_more_than_512_members(tmp_path: Path) -> None:
    members = _default_members() + [
        (f"aider/data-{index:03d}.txt", b"", 0o644) for index in range(509)
    ]
    source = _write_wheel(tmp_path / SOURCE_NAME, members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="too many archive members"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


@pytest.mark.parametrize(
    "extra_members",
    [
        [("aider/oversized.bin", b"x" * (16 * 1024 * 1024 + 1), 0o644)],
        [
            (f"aider/aggregate-{index}.bin", b"x" * (16 * 1024 * 1024), 0o644)
            for index in range(4)
        ],
    ],
    ids=["single-member", "aggregate"],
)
def test_rejects_oversized_archives(
    tmp_path: Path,
    extra_members: list[tuple[str, bytes, int]],
) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME, _default_members() + extra_members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="size limit"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


@pytest.mark.parametrize(
    ("source_name", "members_factory", "message"),
    [
        ("other.whl", _default_members, "source filename"),
        (
            SOURCE_NAME,
            lambda: _mutated_members(
                replace_payload=(f"{DIST_INFO}/WHEEL", WHEEL.replace(b"py3-none-any", b"py2-none-any"))
            ),
            "wheel tag",
        ),
        (
            SOURCE_NAME,
            lambda: _mutated_members(
                replace_payload=(f"{DIST_INFO}/METADATA", METADATA.replace(b"0.86.2", b"0.86.3", 1))
            ),
            "source version",
        ),
        (
            SOURCE_NAME,
            lambda: _mutated_members(
                replace_name=(
                    f"{DIST_INFO}/WHEEL",
                    "unexpected-0.86.2.dist-info/WHEEL",
                )
            ),
            "dist-info layout",
        ),
    ],
)
def test_rejects_wrong_wheel_identity_or_layout(
    tmp_path: Path,
    source_name: str,
    members_factory: Callable[[], list[tuple[str, bytes, int]]],
    message: str,
) -> None:
    source = _write_wheel(tmp_path / source_name, members_factory())
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match=message):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


@pytest.mark.parametrize(
    "metadata",
    [
        METADATA.replace(b"Requires-Dist: litellm==1.81.10\n", b""),
        METADATA + b"Requires-Dist: litellm==1.81.10\n",
        METADATA.replace(b"Requires-Dist: importlib-metadata==7.2.1\n", b""),
        METADATA + b"Requires-Dist: importlib-metadata==7.2.1\n",
    ],
    ids=["missing-litellm", "duplicate-litellm", "missing-importlib", "duplicate-importlib"],
)
def test_requires_exactly_one_copy_of_each_target_requirement(
    tmp_path: Path,
    metadata: bytes,
) -> None:
    members = _mutated_members(replace_payload=(f"{DIST_INFO}/METADATA", metadata))
    source = _write_wheel(tmp_path / SOURCE_NAME, members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="target requirement"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


def test_rejects_preexisting_local_dist_info(tmp_path: Path) -> None:
    members = _mutated_members(append=(f"{LOCAL_DIST_INFO}/METADATA", b"collision", 0o644))
    source = _write_wheel(tmp_path / SOURCE_NAME, members)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="local dist-info"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


def test_rejects_non_regular_source(tmp_path: Path) -> None:
    source = tmp_path / SOURCE_NAME
    source.mkdir()
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    with pytest.raises(AiderDistributionError, match="regular file"):
        _rebuild_aider_wheel(source, output_directory, expected_source_sha256="0" * 64)

    assert list(output_directory.iterdir()) == []


@pytest.mark.parametrize("kind", ["missing", "file", "relative", "symlink"])
def test_rejects_invalid_output_directory(tmp_path: Path, kind: str) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    if kind == "file":
        output_directory.write_text("not a directory")
    elif kind == "relative":
        output_directory = Path("relative-output")
    elif kind == "symlink":
        target = tmp_path / "target"
        target.mkdir()
        output_directory.symlink_to(target, target_is_directory=True)

    with pytest.raises(AiderDistributionError, match="output directory"):
        _rebuild_fixture(source, output_directory)

    if output_directory.is_dir():
        assert list(output_directory.iterdir()) == []


def test_rejects_world_writable_output_directory_before_creating_a_temporary_file(
    tmp_path: Path,
) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    output_directory.mkdir(mode=0o700)
    output_directory.chmod(0o777)

    with pytest.raises(AiderDistributionError, match="owner-controlled"):
        _rebuild_fixture(source, output_directory)

    assert list(output_directory.iterdir()) == []


def test_temporary_path_replacement_never_mutates_or_removes_the_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    output_directory.mkdir(mode=0o700)
    sentinel = tmp_path / "sentinel"
    sentinel_bytes = b"attacker-controlled sentinel bytes"
    sentinel.write_bytes(sentinel_bytes)
    surrounding = tmp_path / "surrounding"
    surrounding.write_bytes(b"surrounding entry")
    real_mkstemp = tempfile.mkstemp
    replacement_path: Path | None = None

    def replace_temporary_path(*args: object, **kwargs: object) -> tuple[int, str]:
        nonlocal replacement_path
        descriptor, temporary_name = real_mkstemp(*args, **kwargs)
        replacement_path = Path(temporary_name)
        replacement_path.unlink()
        replacement_path.symlink_to(sentinel)
        return descriptor, temporary_name

    monkeypatch.setattr(tempfile, "mkstemp", replace_temporary_path)

    with pytest.raises(AiderDistributionError, match="temporary wheel pathname identity"):
        _rebuild_fixture(source, output_directory)

    assert replacement_path is not None
    assert replacement_path.is_symlink()
    assert replacement_path.readlink() == sentinel
    assert sentinel.read_bytes() == sentinel_bytes
    assert surrounding.read_bytes() == b"surrounding entry"
    assert not (output_directory / OUTPUT_NAME).exists()


def test_rejects_output_collision_without_overwriting(tmp_path: Path) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)
    collision = output_directory / OUTPUT_NAME
    collision.write_bytes(b"existing")

    with pytest.raises(AiderDistributionError, match="already exists"):
        _rebuild_fixture(source, output_directory)

    assert collision.read_bytes() == b"existing"
    assert [path.name for path in output_directory.iterdir()] == [OUTPUT_NAME]


def test_cli_prints_only_absolute_output_path_on_success(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = _write_wheel(tmp_path / SOURCE_NAME)
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    # The private digest seam is already covered above; this CLI test supplies the reviewed
    # digest by temporarily replacing the synthetic wheel bytes with no archive disclosure.
    from loom_launcher import aider_distribution

    original = aider_distribution.SOURCE_SHA256
    aider_distribution.SOURCE_SHA256 = digest
    try:
        result = main(["--source-wheel", os.fspath(source), "--output-directory", os.fspath(output_directory)])
    finally:
        aider_distribution.SOURCE_SHA256 = original

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == f"{output_directory / OUTPUT_NAME}\n"
    assert captured.err == ""


def test_cli_reports_one_bounded_error_without_archive_content(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "DO-NOT-PRINT-ARCHIVE-CONTENT"
    source = _write_wheel(
        tmp_path / SOURCE_NAME,
        _mutated_members(append=("../escape.py", secret.encode(), 0o644)),
    )
    output_directory = tmp_path / "out"
    _make_owner_controlled_directory(output_directory)

    result = main(["--source-wheel", os.fspath(source), "--output-directory", os.fspath(output_directory)])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert len(captured.err.encode()) <= 512
    assert captured.err.count("\n") == 1
    assert secret not in captured.err


@pytest.mark.parametrize(
    "argv",
    [
        ["--source-wheel"],
        ["--output-directory"],
        ["--source-wheel", SOURCE_NAME, "--output-directory"],
    ],
    ids=["missing-source-value", "missing-output-value", "missing-final-value"],
)
def test_cli_normalizes_missing_option_values_to_one_bounded_error(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(argv)

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert len(captured.err.encode()) <= 512
    assert captured.err.count("\n") == 1
    assert captured.err.startswith("aider wheel error:")
