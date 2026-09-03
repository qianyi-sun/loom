"""Behavioral tests for the deterministic node-guard release assembler."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import stat
import struct
import zipfile
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_guard_release as release_module
from scripts.ops.task_image_builder_guard_release import (
    GuardReleaseError,
    build_release,
)

ROOT = Path(__file__).resolve().parents[2]
SPEC = Path("deploy/task-image-builder/guard-release-v1.json")
PACKAGE = Path("src/loom_task_image_builder_guard")
ARTIFACTS = (
    Path("deploy/task-image-builder/guard-network-v1.bpf.c"),
    Path("deploy/task-image-builder/guard-network-v1.bpf.o"),
    Path("deploy/task-image-builder/guard-network-v1.bpf.build.json"),
    Path("deploy/task-image-builder/guard-network-map-schema-v1.json"),
    Path("deploy/task-image-builder/loom-task-image-builder-node-guard.service"),
)


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "ascii"
    )


def _source_tree(destination: Path) -> Path:
    root = destination / "source"
    package = root / PACKAGE
    package.mkdir(parents=True)
    for source in sorted((ROOT / PACKAGE).glob("*.py"), reverse=True):
        shutil.copyfile(source, package / source.name)
    for relative in (*ARTIFACTS, SPEC):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    return root


def _elf_bpftool(path: Path, *, machine: int = 62) -> Path:
    ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + bytes(8)
    payload = struct.pack(
        "<16sHHIQQQIHHHHHH",
        ident,
        3,
        machine,
        1,
        0,
        0,
        0,
        0,
        64,
        0,
        0,
        64,
        0,
        0,
    )
    path.write_bytes(payload + b"test-bpftool")
    path.chmod(0o755)
    return path


def _manifest(release: Path) -> dict[str, object]:
    value = json.loads((release / "release-manifest.json").read_bytes())
    assert isinstance(value, dict)
    return value


def test_release_is_content_addressed_and_byte_reproducible_across_metadata(
    tmp_path: Path,
) -> None:
    first_source = _source_tree(tmp_path / "first")
    second_source = _source_tree(tmp_path / "second")
    for index, source in enumerate(sorted(second_source.rglob("*"), reverse=True)):
        if source.is_file():
            timestamp = 1_700_000_000 + index
            os.utime(source, (timestamp, timestamp))
    bpftool = _elf_bpftool(tmp_path / "bpftool")

    first = build_release(first_source, bpftool, tmp_path / "out-one", "x86_64")
    second = build_release(second_source, bpftool, tmp_path / "out-two", "x86_64")

    assert first.release_sha256 == second.release_sha256
    assert first.directory.name == first.release_sha256
    assert _manifest(first.directory) == _manifest(second.directory)
    first_files = {
        path.relative_to(first.directory).as_posix(): path.read_bytes()
        for path in first.directory.iterdir()
    }
    second_files = {
        path.relative_to(second.directory).as_posix(): path.read_bytes()
        for path in second.directory.iterdir()
    }
    assert first_files == second_files

    manifest = _manifest(first.directory)
    identity = dict(manifest)
    assert identity.pop("release_sha256") == first.release_sha256
    assert hashlib.sha256(_canonical(identity)).hexdigest() == first.release_sha256
    assert first.manifest_path.read_bytes() == first.sidecar_path.read_bytes()
    assert first.sidecar_path.name == f"{first.release_sha256}.manifest.json"


def test_zipapp_has_a_sorted_stored_canonical_archive_and_exact_payload(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    result = build_release(
        source,
        _elf_bpftool(tmp_path / "bpftool"),
        tmp_path / "out",
        "x86_64",
    )

    archive = result.directory / "loom-task-image-builder-guard.pyz"
    with zipfile.ZipFile(archive) as bundle:
        members = bundle.infolist()
        names = [item.filename for item in members]
        expected_sources = [
            f"loom_task_image_builder_guard/{path.name}"
            for path in sorted((source / PACKAGE).glob("*.py"))
        ]
        assert names == ["__main__.py", *expected_sources]
        assert all(item.compress_type == zipfile.ZIP_STORED for item in members)
        assert all(item.date_time == (1980, 1, 1, 0, 0, 0) for item in members)
        assert all(stat.S_IMODE(item.external_attr >> 16) == 0o444 for item in members)
        assert bundle.read("__main__.py") == (
            b"from loom_task_image_builder_guard.__main__ import main\n"
            b"raise SystemExit(main())\n"
        )

    manifest = _manifest(result.directory)
    assert manifest["schema"] == "loom.task-image-builder-guard-bundle/v1"
    assert manifest["architecture"] == "x86_64"
    records = manifest["files"]
    assert isinstance(records, list)
    assert [record["path"] for record in records] == [
        "bpftool",
        "guard-network-map-schema-v1.json",
        "guard-network-v1.bpf.build.json",
        "guard-network-v1.bpf.o",
        "loom-task-image-builder-node-guard.service",
        "loom-task-image-builder-guard.pyz",
    ]
    assert [record["mode"] for record in records] == [
        "0555",
        "0444",
        "0444",
        "0444",
        "0444",
        "0555",
    ]
    assert (
        result.directory / "loom-task-image-builder-node-guard.service"
    ).read_bytes() == (
        source
        / "deploy/task-image-builder/loom-task-image-builder-node-guard.service"
    ).read_bytes()


def test_release_rejects_a_weakened_unit_even_if_its_digest_is_rebound(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    unit = source / "deploy/task-image-builder/loom-task-image-builder-node-guard.service"
    unit.write_text(
        unit.read_text(encoding="utf-8").replace(
            "NoNewPrivileges=yes",
            "NoNewPrivileges=no",
        ),
        encoding="utf-8",
    )
    spec_path = source / SPEC
    spec = json.loads(spec_path.read_bytes())
    unit_record = next(
        record
        for record in spec["artifacts"]
        if record["path"].endswith("node-guard.service")
    )
    unit_record["sha256"] = hashlib.sha256(unit.read_bytes()).hexdigest()
    spec_path.write_bytes(_canonical(spec))

    with pytest.raises(GuardReleaseError, match="systemd unit"):
        build_release(
            source,
            _elf_bpftool(tmp_path / "bpftool"),
            tmp_path / "out",
            "x86_64",
        )


@pytest.mark.parametrize(
    ("architecture", "machine"),
    (("x86_64", 183), ("aarch64", 62)),
)
def test_release_rejects_a_bpftool_for_the_other_native_architecture(
    tmp_path: Path,
    architecture: str,
    machine: int,
) -> None:
    source = _source_tree(tmp_path)

    with pytest.raises(GuardReleaseError, match="bpftool architecture"):
        build_release(
            source,
            _elf_bpftool(tmp_path / "bpftool", machine=machine),
            tmp_path / "out",
            architecture,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "mutation",
    ("changed-source", "changed-object", "unsafe-spec-path", "source-symlink"),
)
def test_release_rejects_changed_or_unsafe_manifest_inputs(
    tmp_path: Path,
    mutation: str,
) -> None:
    source = _source_tree(tmp_path)
    if mutation == "changed-source":
        target = source / PACKAGE / "errors.py"
        target.write_bytes(target.read_bytes() + b"\n")
    elif mutation == "changed-object":
        target = source / ARTIFACTS[1]
        target.write_bytes(target.read_bytes() + b"changed")
    elif mutation == "unsafe-spec-path":
        spec = json.loads((source / SPEC).read_bytes())
        spec["sources"][0]["path"] = "../outside.py"
        (source / SPEC).write_bytes(_canonical(spec))
    else:
        target = source / PACKAGE / "errors.py"
        outside = tmp_path / "outside.py"
        outside.write_bytes(target.read_bytes())
        target.unlink()
        target.symlink_to(outside)

    with pytest.raises(GuardReleaseError):
        build_release(
            source,
            _elf_bpftool(tmp_path / "bpftool"),
            tmp_path / "out",
            "x86_64",
        )


def test_release_rejects_a_hardlinked_or_unbounded_bpftool(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    bpftool = _elf_bpftool(tmp_path / "bpftool")
    os.link(bpftool, tmp_path / "second-link")

    with pytest.raises(GuardReleaseError, match="single-link"):
        build_release(source, bpftool, tmp_path / "hardlink-out", "x86_64")

    bpftool.unlink()
    bpftool.write_bytes(b"\x7fELF" + b"x" * (64 * 1024 * 1024))
    bpftool.chmod(0o755)
    with pytest.raises(GuardReleaseError, match="too large"):
        build_release(source, bpftool, tmp_path / "large-out", "x86_64")


def test_release_publish_never_replaces_an_existing_digest_directory(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    bpftool = _elf_bpftool(tmp_path / "bpftool")
    first = build_release(source, bpftool, tmp_path / "out", "x86_64")
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in first.directory.iterdir()
    }

    with pytest.raises(GuardReleaseError, match="already exists"):
        build_release(source, bpftool, tmp_path / "out", "x86_64")

    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in first.directory.iterdir()
    }
    assert after == before


def test_release_retry_repairs_a_missing_sidecar_after_directory_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_tree(tmp_path)
    bpftool = _elf_bpftool(tmp_path / "bpftool")
    output = tmp_path / "out"
    original_publish_sidecar = release_module._publish_sidecar

    def fail_sidecar(_path: Path, _payload: bytes) -> None:
        raise GuardReleaseError("injected sidecar failure")

    monkeypatch.setattr(release_module, "_publish_sidecar", fail_sidecar)
    with pytest.raises(GuardReleaseError, match="injected sidecar failure"):
        build_release(source, bpftool, output, "x86_64")

    published = [path for path in output.iterdir() if path.is_dir()]
    assert len(published) == 1
    assert not (output / f"{published[0].name}.manifest.json").exists()

    monkeypatch.setattr(release_module, "_publish_sidecar", original_publish_sidecar)
    repaired = build_release(source, bpftool, output, "x86_64")

    assert repaired.directory == published[0]
    assert repaired.sidecar_path.read_bytes() == repaired.manifest_path.read_bytes()


def test_release_never_exposes_a_partial_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_tree(tmp_path)
    bpftool = _elf_bpftool(tmp_path / "bpftool")
    output = tmp_path / "out"
    original_write = release_module.os.write
    sidecar_started = False

    def fail_during_sidecar(descriptor: int, payload: bytes) -> int:
        nonlocal sidecar_started
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.parent == output:
            if not sidecar_started:
                sidecar_started = True
                return original_write(descriptor, payload[:8])
            raise OSError(errno.EIO, "injected sidecar write failure")
        return original_write(descriptor, payload)

    monkeypatch.setattr(release_module.os, "write", fail_during_sidecar)

    with pytest.raises(GuardReleaseError, match="could not be written"):
        build_release(source, bpftool, output, "x86_64")

    assert not [path for path in output.iterdir() if path.name.endswith(".manifest.json")]


def test_release_rejects_a_zero_length_file_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_tree(tmp_path)
    bpftool = _elf_bpftool(tmp_path / "bpftool")
    original_write = release_module.os.write
    first_write = True

    def zero_once(descriptor: int, payload: bytes) -> int:
        nonlocal first_write
        if first_write:
            first_write = False
            return 0
        return original_write(descriptor, payload)

    monkeypatch.setattr(release_module.os, "write", zero_once)

    with pytest.raises(GuardReleaseError, match="could not be written"):
        build_release(source, bpftool, tmp_path / "out", "x86_64")
