#!/usr/bin/python3
"""Verify the fixed platform-dev GB10 NFS mount without trusting a directory name."""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

MOUNT_POINT = Path("/shared_work2")
MOUNT_SOURCE = "192.168.20.12:/shared_work2"
FILESYSTEM_TYPE = "nfs4"
REQUIRED_MOUNT_OPTIONS = frozenset({"rw", "nosuid", "nodev", "noexec"})
REQUIRED_SUPER_OPTIONS = frozenset(
    {"rw", "hard", "vers=4.2", "proto=tcp", "sec=sys", "timeo=600", "retrans=2"}
)
MOUNTINFO = Path("/proc/self/mountinfo")


class MountError(RuntimeError):
    """A bounded mount-contract failure safe for installer output."""


@dataclass(frozen=True, slots=True)
class MountRecord:
    mount_id: int
    parent_id: int
    major: int
    minor: int
    root: str
    mount_point: str
    mount_options: frozenset[str]
    filesystem_type: str
    source: str
    super_options: frozenset[str]


def _unescape_mount_field(value: str) -> str:
    result = bytearray()
    index = 0
    encoded = value.encode("ascii")
    while index < len(encoded):
        if encoded[index : index + 1] == b"\\" and index + 3 < len(encoded):
            digits = encoded[index + 1 : index + 4]
            if all(48 <= item <= 55 for item in digits):
                result.append(int(digits, 8))
                index += 4
                continue
        result.append(encoded[index])
        index += 1
    try:
        return result.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MountError("shared_work2 mountinfo contains an invalid field") from exc


def _parse_mountinfo(payload: str) -> tuple[MountRecord, ...]:
    records: list[MountRecord] = []
    for raw_line in payload.splitlines():
        left, separator, right = raw_line.partition(" - ")
        if not separator:
            raise MountError("shared_work2 mountinfo is malformed")
        left_fields = left.split()
        right_fields = right.split()
        if len(left_fields) < 6 or len(right_fields) != 3:
            raise MountError("shared_work2 mountinfo is malformed")
        device = left_fields[2].split(":", 1)
        if len(device) != 2:
            raise MountError("shared_work2 mountinfo is malformed")
        try:
            mount_id = int(left_fields[0])
            parent_id = int(left_fields[1])
            major = int(device[0])
            minor = int(device[1])
        except ValueError as exc:
            raise MountError("shared_work2 mountinfo is malformed") from exc
        records.append(
            MountRecord(
                mount_id=mount_id,
                parent_id=parent_id,
                major=major,
                minor=minor,
                root=_unescape_mount_field(left_fields[3]),
                mount_point=_unescape_mount_field(left_fields[4]),
                mount_options=frozenset(left_fields[5].split(",")),
                filesystem_type=right_fields[0],
                source=_unescape_mount_field(right_fields[1]),
                super_options=frozenset(right_fields[2].split(",")),
            )
        )
    return tuple(records)


def mount_identity(
    *,
    mountinfo: Path = MOUNTINFO,
    mount_point: Path = MOUNT_POINT,
) -> dict[str, object]:
    try:
        payload = mountinfo.read_text(encoding="utf-8")
        metadata = os.lstat(mount_point)
    except OSError as exc:
        raise MountError("shared_work2 mount is unavailable") from exc
    matches = [
        record for record in _parse_mountinfo(payload) if record.mount_point == str(mount_point)
    ]
    if len(matches) != 1:
        raise MountError("shared_work2 must be one exact mount, not a local directory")
    record = matches[0]
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or record.filesystem_type != FILESYSTEM_TYPE
        or record.source != MOUNT_SOURCE
        or not REQUIRED_MOUNT_OPTIONS.issubset(record.mount_options)
        or not REQUIRED_SUPER_OPTIONS.issubset(record.super_options)
        or (os.major(metadata.st_dev), os.minor(metadata.st_dev)) != (record.major, record.minor)
    ):
        raise MountError("shared_work2 mount identity is invalid")
    return {
        "schema_version": 1,
        "mount_point": str(MOUNT_POINT),
        "source": MOUNT_SOURCE,
        "filesystem_type": FILESYSTEM_TYPE,
        "mount_id": record.mount_id,
        "parent_id": record.parent_id,
        "device_major": record.major,
        "device_minor": record.minor,
        "mount_options": sorted(REQUIRED_MOUNT_OPTIONS),
        "super_options": sorted(REQUIRED_SUPER_OPTIONS),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check",))
    parser.parse_args(argv)
    try:
        report = mount_identity()
    except (MountError, OSError):
        print("error: shared_work2 mount check failed safely", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
