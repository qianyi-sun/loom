"""Complete current-namespace mapping observations, never recovery authority.

Outside IDs are relative to the reader's parent user namespace, not authenticated
host identities. Protected bootstrap must bind that namespace, the actual ranges
and their retention policy before these facts can authorize later recovery.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path

_MAX_MAP_BYTES = 16384
_MAX_RANGES = 340  # Linux >= 4.15 uid_map/gid_map extent bound.
_ID_LIMIT = 2**32 - 1  # (uid_t)-1 is deliberately unmappable.


@dataclass(frozen=True, slots=True)
class NativeIdentityRange:
    inside: int
    outside: int
    count: int


@dataclass(frozen=True, slots=True)
class NativeMappedIdentity:
    uid_ranges: tuple[NativeIdentityRange, ...]
    gid_ranges: tuple[NativeIdentityRange, ...]


def _read_map(name: str) -> tuple[NativeIdentityRange, ...]:
    with Path("/proc/self", name).open("rb") as stream:
        wire = stream.read(_MAX_MAP_BYTES + 1)
    rows = wire.splitlines()
    if not wire or len(wire) > _MAX_MAP_BYTES or not 1 <= len(rows) <= _MAX_RANGES:
        raise RuntimeError("native root mapping exceeds its observation bounds")
    ranges: list[NativeIdentityRange] = []
    for row in rows:
        fields = row.split()
        if len(fields) != 3 or not all(value.isdigit() and len(value) <= 10 for value in fields):
            raise RuntimeError("native root mapping contains an invalid range")
        inside, outside, count = map(int, fields)
        if count == 0 or outside == 0 or inside + count > _ID_LIMIT or outside + count > _ID_LIMIT:
            raise RuntimeError("native root mapping contains an unsafe range")
        ranges.append(NativeIdentityRange(inside, outside, count))
    root = ranges[0]
    if root.inside != 0 or root.count != 1:
        raise RuntimeError("native root mapping requires a single noninitial root")
    # Validate both namespace and parent coordinates, not only the root row.
    for coordinate in ("inside", "outside"):
        ordered = sorted((getattr(item, coordinate), item.count) for item in ranges)
        if any(start + count > following for (start, count), (following, _) in pairwise(ordered)):
            raise RuntimeError("native root mapping contains overlapping ranges")
    return tuple(ranges)


def observe_native_mapped_identity() -> NativeMappedIdentity:
    """Read all actual UID/GID extents before unpacking or mapped mutation.

    User-namespace maps are immutable after installation. This function creates
    no namespace, persists no record and grants no destructive cleanup permit.
    """
    if os.geteuid() != 0 or os.getegid() != 0:
        raise RuntimeError("native identity observation requires mapped root")
    return NativeMappedIdentity(uid_ranges=_read_map("uid_map"), gid_ranges=_read_map("gid_map"))
