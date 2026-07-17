from __future__ import annotations

import os
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_shared_work2 as helper


def _mountinfo(path: Path, *, source: str = helper.MOUNT_SOURCE, options: str | None = None) -> str:
    metadata = path.stat()
    super_options = options or ",".join(sorted(helper.REQUIRED_SUPER_OPTIONS))
    mount_options = ",".join(sorted(helper.REQUIRED_MOUNT_OPTIONS))
    return (
        f"42 1 {os.major(metadata.st_dev)}:{os.minor(metadata.st_dev)} / {path} "
        f"{mount_options} - nfs4 {source} {super_options}\n"
    )


def test_mount_identity_binds_exact_nfs_source_type_options_and_device(tmp_path: Path) -> None:
    mount_point = tmp_path / "shared_work2"
    mount_point.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(_mountinfo(mount_point), encoding="utf-8")

    report = helper.mount_identity(mountinfo=mountinfo, mount_point=mount_point)

    assert report["source"] == "192.168.20.12:/shared_work2"
    assert report["filesystem_type"] == "nfs4"
    assert report["device_major"] == os.major(mount_point.stat().st_dev)
    assert report["device_minor"] == os.minor(mount_point.stat().st_dev)
    assert report["mount_options"] == sorted(helper.REQUIRED_MOUNT_OPTIONS)
    assert report["super_options"] == sorted(helper.REQUIRED_SUPER_OPTIONS)


def test_mount_identity_rejects_local_directory_without_exact_mount(tmp_path: Path) -> None:
    mount_point = tmp_path / "shared_work2"
    mount_point.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text("", encoding="utf-8")

    with pytest.raises(helper.MountError, match="one exact mount"):
        helper.mount_identity(mountinfo=mountinfo, mount_point=mount_point)


@pytest.mark.parametrize(
    ("source", "options"),
    (
        ("192.168.20.99:/shared_work2", None),
        (helper.MOUNT_SOURCE, "rw,hard,vers=4.1,proto=tcp,sec=sys,timeo=600,retrans=2"),
    ),
)
def test_mount_identity_rejects_source_or_nfs_contract_drift(
    tmp_path: Path,
    source: str,
    options: str | None,
) -> None:
    mount_point = tmp_path / "shared_work2"
    mount_point.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(_mountinfo(mount_point, source=source, options=options), encoding="utf-8")

    with pytest.raises(helper.MountError, match="identity"):
        helper.mount_identity(mountinfo=mountinfo, mount_point=mount_point)


def test_mountinfo_parser_decodes_kernel_path_escapes() -> None:
    records = helper._parse_mountinfo(
        "42 1 0:99 / /shared\\137work2 rw,nosuid,nodev,noexec "
        "- nfs4 192.168.20.12:/shared\\137work2 "
        "rw,hard,vers=4.2,proto=tcp,sec=sys,timeo=600,retrans=2\n"
    )

    assert records[0].mount_point == "/shared_work2"
    assert records[0].source == "192.168.20.12:/shared_work2"
