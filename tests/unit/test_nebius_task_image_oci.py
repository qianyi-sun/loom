from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import pytest

from loom_execution_actuator import task_image_oci as oci

MANIFEST = "blobs/sha256/" + "a" * 64
CONFIG = "blobs/sha256/" + "b" * 64
LAYER = "blobs/sha256/" + "c" * 64


def _descriptor(path: str, body: bytes, media_type: str) -> dict:
    return {"digest": "sha256:" + path.rsplit("/", 1)[1], "size": len(body), "mediaType": media_type}


def _json(value: object) -> bytes:
    return json.dumps(value).encode()


@pytest.fixture
def layout() -> dict[str, bytes]:
    # Deliberately synthetic digest names: this boundary checks local references,
    # while publication's Skopeo operation owns content digest verification.
    files = {
        LAYER: b"opaque layer bytes; never unpacked by the publisher validator",
        CONFIG: _json({"os": "linux", "architecture": "amd64",
                       "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "d" * 64]},
                       "config": {"Labels": {"url": "https://example.org/documentation"}}}),
        "oci-layout": _json({"imageLayoutVersion": "1.0.0"}),
    }
    files[MANIFEST] = _json({
        "schemaVersion": 2, "mediaType": oci._MANIFEST,
        "config": _descriptor(CONFIG, files[CONFIG], oci._CONFIG),
        "layers": [_descriptor(LAYER, files[LAYER], "application/vnd.oci.image.layer.v1.tar+gzip")],
    })
    files["index.json"] = _json({
        "schemaVersion": 2, "mediaType": oci._INDEX,
        "manifests": [{**_descriptor(MANIFEST, files[MANIFEST], oci._MANIFEST),
                       "platform": {"os": "linux", "architecture": "amd64"}}],
    })
    return files


def _update(files: dict[str, bytes], name: str, value: object) -> None:
    files[name] = _json(value)
    if name == CONFIG:
        manifest = json.loads(files[MANIFEST])
        manifest["config"]["size"] = len(files[name])
        _update(files, MANIFEST, manifest)
    elif name == MANIFEST:
        index = json.loads(files["index.json"])
        index["manifests"][0]["size"] = len(files[name])
        files["index.json"] = _json(index)


def _write(path: Path, files: dict[str, bytes], extras: list[tuple[tarfile.TarInfo, bytes]] | None = None) -> Path:
    with tarfile.open(path, "w:") as archive:
        for name in ("blobs", "blobs/sha256"):
            directory = tarfile.TarInfo(name)
            directory.type = tarfile.DIRTYPE
            archive.addfile(directory)
        for name, body in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(body)
            archive.addfile(entry, io.BytesIO(body))
        for entry, body in extras or []:
            archive.addfile(entry, io.BytesIO(body))
    return path


def test_accepts_local_image_without_rehashing_or_extracting_layers(layout, tmp_path) -> None:
    archive = _write(tmp_path / "image.tar", layout)
    oci.validate_native_oci_archive(archive)
    assert list(tmp_path.iterdir()) == [archive]


def test_config_platform_is_authoritative_when_index_omits_platform(layout, tmp_path) -> None:
    index = json.loads(layout["index.json"])
    del index["manifests"][0]["platform"]
    _update(layout, "index.json", index)
    oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout))


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.CHRTYPE, tarfile.FIFOTYPE])
def test_rejects_link_and_special_members(layout, tmp_path, kind) -> None:
    entry = tarfile.TarInfo("blobs/sha256/" + "e" * 64)
    entry.type = kind
    entry.linkname = "/var/run/loom-task-build/registry/config.json"
    with pytest.raises(oci.NativeOCIArchiveError, match="link or special"):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout, [(entry, b"")]))


@pytest.mark.parametrize("name", ["../outside", "/absolute", "blobs/../outside", "./index.json", "blobs//entry", "blobs\\entry"])
def test_rejects_unsafe_archive_paths(layout, tmp_path, name) -> None:
    entry = tarfile.TarInfo(name)
    with pytest.raises(oci.NativeOCIArchiveError, match="unsafe"):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout, [(entry, b"")]))


@pytest.mark.parametrize("name", ["index.json", "blobs", "blobs/sha256/"])
def test_rejects_duplicate_file_or_directory_paths(layout, tmp_path, name) -> None:
    entry = tarfile.TarInfo(name)
    if name.startswith("blobs"):
        entry.type = tarfile.DIRTYPE
    with pytest.raises(oci.NativeOCIArchiveError, match="duplicate"):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout, [(entry, b"")]))


def test_rejects_unexpected_archive_files_and_xattrs(layout, tmp_path) -> None:
    with pytest.raises(oci.NativeOCIArchiveError, match="unexpected file"):
        oci.validate_native_oci_archive(_write(tmp_path / "extra.tar", {**layout, "script.sh": b"exit 0"}))
    entry = tarfile.TarInfo("blobs/sha256/" + "e" * 64)
    entry.pax_headers = {"SCHILY.xattr.security.capability": "untrusted"}
    with pytest.raises(oci.NativeOCIArchiveError, match="tar metadata"):
        oci.validate_native_oci_archive(_write(tmp_path / "xattr.tar", layout, [(entry, b"")]))


@pytest.mark.parametrize("owner", ["index", "config", "layer"])
@pytest.mark.parametrize("field", ["urls", "data"])
def test_rejects_nonlocal_descriptor_sources(layout, tmp_path, owner, field) -> None:
    name = "index.json" if owner == "index" else MANIFEST
    document = json.loads(layout[name])
    descriptor = (document["manifests"][0] if owner == "index" else
                  document["config"] if owner == "config" else document["layers"][0])
    descriptor[field] = ["http://169.254.169.254/"] if field == "urls" else "aW5saW5l"
    _update(layout, name, document)
    with pytest.raises(oci.NativeOCIArchiveError, match="local blobs"):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout))


@pytest.mark.parametrize("missing", ["oci-layout", "index.json", MANIFEST, CONFIG, LAYER])
def test_rejects_incomplete_local_layout(layout, tmp_path, missing) -> None:
    del layout[missing]
    with pytest.raises(oci.NativeOCIArchiveError, match="missing"):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout))


@pytest.mark.parametrize("owner", ["index", "config"])
def test_rejects_wrong_platform_in_index_or_config(layout, tmp_path, owner) -> None:
    name = "index.json" if owner == "index" else CONFIG
    document = json.loads(layout[name])
    platform = document["manifests"][0]["platform"] if owner == "index" else document
    platform["architecture"] = "arm64"
    _update(layout, name, document)
    with pytest.raises(oci.NativeOCIArchiveError, match="linux/amd64"):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout))


@pytest.mark.parametrize("change", ["size", "traversal_digest", "bool_size", "foreign_layer", "bad_media_type"])
def test_rejects_invalid_layer_descriptors(layout, tmp_path, change) -> None:
    manifest = json.loads(layout[MANIFEST])
    layer = manifest["layers"][0]
    if change == "size":
        layer["size"] += 1
    elif change == "traversal_digest":
        layer["digest"] = "sha256:../../credential"
    elif change == "bool_size":
        layer["size"] = True
    elif change == "foreign_layer":
        layer["mediaType"] = "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip"
    else:
        layer["mediaType"] = []
    _update(layout, MANIFEST, manifest)
    with pytest.raises(oci.NativeOCIArchiveError):
        oci.validate_native_oci_archive(_write(tmp_path / "image.tar", layout))


def test_rejects_multiimage_index_and_incomplete_rootfs(layout, tmp_path) -> None:
    index = json.loads(layout["index.json"])
    index["manifests"].append(index["manifests"][0])
    _update(layout, "index.json", index)
    with pytest.raises(oci.NativeOCIArchiveError, match="one local platform"):
        oci.validate_native_oci_archive(_write(tmp_path / "multi.tar", layout))
    index["manifests"].pop()
    _update(layout, "index.json", index)
    config = json.loads(layout[CONFIG])
    config["rootfs"]["diff_ids"] = []
    _update(layout, CONFIG, config)
    with pytest.raises(oci.NativeOCIArchiveError, match="root filesystem"):
        oci.validate_native_oci_archive(_write(tmp_path / "rootfs.tar", layout))


@pytest.mark.parametrize("budget", ["archive_bytes", "members", "json_bytes"])
def test_enforces_archive_member_and_metadata_budgets(layout, tmp_path, monkeypatch, budget) -> None:
    path = _write(tmp_path / "image.tar", layout)
    if budget == "archive_bytes":
        monkeypatch.setattr(oci, "_MAX_BYTES", path.stat().st_size - 1)
    elif budget == "members":
        monkeypatch.setattr(oci, "_MAX_MEMBERS", 3)
    else:
        monkeypatch.setattr(oci, "_MAX_JSON_BYTES", 8)
    with pytest.raises(oci.NativeOCIArchiveError):
        oci.validate_native_oci_archive(path)


def test_rejects_malformed_json_and_duplicate_keys(layout, tmp_path) -> None:
    for index, payload in enumerate((b"not JSON", b'{"imageLayoutVersion":"1.0.0","imageLayoutVersion":"1.0.0"}')):
        layout["oci-layout"] = payload
        with pytest.raises(oci.NativeOCIArchiveError):
            oci.validate_native_oci_archive(_write(tmp_path / f"bad-{index}.tar", layout))


def test_rejects_compressed_or_truncated_tar_and_archive_symlink(layout, tmp_path) -> None:
    compressed = tmp_path / "compressed.tar.gz"
    with tarfile.open(compressed, "w:gz"):
        pass
    with pytest.raises(oci.NativeOCIArchiveError):
        oci.validate_native_oci_archive(compressed)
    header = tarfile.TarInfo("oci-layout")
    header.size = 2048
    truncated = tmp_path / "truncated.tar"
    truncated.write_bytes(header.tobuf() + b"short")
    with pytest.raises(oci.NativeOCIArchiveError, match="truncated"):
        oci.validate_native_oci_archive(truncated)
    image = _write(tmp_path / "image.tar", layout)
    linked = tmp_path / "linked.tar"
    linked.symlink_to(image)
    with pytest.raises(oci.NativeOCIArchiveError, match="regular file"):
        oci.validate_native_oci_archive(linked)
