#!/usr/bin/env python3
"""Reproducibly build and attest the task-image guard's eBPF object."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

CLANG_IMAGE = (
    "docker.io/silkeh/clang:18-bookworm@"
    "sha256:3914c93a02e866795aafc80737488e515b96390eff3d2787cf8c5095997baea9"
)
CLANG_VERSION = (
    "Debian clang version 18.1.8 "
    "(++20240731024826+3b5b5c1ec4a3-1~exp1~20240731144843.145)"
)
EM_BPF = 247
REQUIRED_PROGRAM_SECTIONS = frozenset(
    {
        "cgroup/connect4",
        "cgroup/connect6",
        "cgroup/sendmsg4",
        "cgroup/sendmsg6",
        "cgroup/sock_create",
        "cgroup/sock_release",
        "cgroup_skb/ingress",
        "cgroup_skb/egress",
    }
)
REQUIRED_PROGRAM_SYMBOLS = frozenset(
    {
        "guard_connect4",
        "guard_connect6",
        "guard_sendmsg4",
        "guard_sendmsg6",
        "guard_sock_create",
        "guard_sock_release",
        "guard_ingress",
        "guard_egress",
    }
)
REQUIRED_MAP_SYMBOLS = frozenset(
    {
        "scope_subject",
        "allow_v4",
        "allow_v6",
        "subject_limits",
        "flow_sockets",
        "drop_counters",
    }
)

_SOURCE = Path("deploy/task-image-builder/guard-network-v1.bpf.c")
_OBJECT = Path("deploy/task-image-builder/guard-network-v1.bpf.o")
_PROVENANCE = Path("deploy/task-image-builder/guard-network-v1.bpf.build.json")
_MAP_SCHEMA = Path("deploy/task-image-builder/guard-network-map-schema-v1.json")
_ELF_HEADER = struct.Struct("<16sHHIQQQIHHHHHH")
_SECTION_HEADER = struct.Struct("<IIQQQQIIQQ")
_SYMBOL = struct.Struct("<IBBHQQ")


class BpfBuildError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class InspectedElf:
    elf_class: int
    byte_order: str
    machine: int
    sections: tuple[str, ...]
    symbols: frozenset[str]


def render_commands(*, root: Path, uid: int, gid: int) -> tuple[tuple[str, ...], tuple[str, ...]]:
    prefix = (
        "/usr/bin/docker",
        "run",
        "--rm",
        "--network=none",
        "--platform=linux/amd64",
        f"--user={uid}:{gid}",
        f"--volume={root}:/src:rw",
        "--workdir=/src",
        CLANG_IMAGE,
    )
    return (
        (*prefix, "clang", "--version"),
        (
            *prefix,
            "clang",
            "-target",
            "bpfel",
            "-isystem",
            "/usr/include/x86_64-linux-gnu",
            "-O2",
            "-g",
            "-Wall",
            "-Werror",
            "-c",
            _SOURCE.as_posix(),
            "-o",
            _OBJECT.as_posix(),
        ),
    )


def _c_string(payload: bytes, offset: int) -> str:
    if offset < 0 or offset >= len(payload):
        raise BpfBuildError("ELF string offset is invalid")
    end = payload.find(b"\0", offset)
    if end < 0:
        raise BpfBuildError("ELF string is unterminated")
    try:
        return payload[offset:end].decode("ascii")
    except UnicodeDecodeError:
        raise BpfBuildError("ELF string is not ASCII") from None


def inspect_bpf_elf(payload: bytes) -> InspectedElf:
    if len(payload) < _ELF_HEADER.size:
        raise BpfBuildError("ELF object is truncated")
    header = _ELF_HEADER.unpack_from(payload)
    ident = header[0]
    if ident[:4] != b"\x7fELF" or ident[4] != 2 or ident[5] != 1 or header[1] != 1:
        raise BpfBuildError("ELF object is not 64-bit little-endian relocatable data")
    machine = header[2]
    section_offset = header[6]
    section_size = header[11]
    section_count = header[12]
    string_index = header[13]
    if (
        machine != EM_BPF
        or section_size != _SECTION_HEADER.size
        or not 1 <= section_count <= 4096
        or string_index >= section_count
        or section_offset + section_size * section_count > len(payload)
    ):
        raise BpfBuildError("ELF section table is invalid")
    headers = [
        _SECTION_HEADER.unpack_from(payload, section_offset + index * section_size)
        for index in range(section_count)
    ]
    string_header = headers[string_index]
    strings = payload[string_header[4] : string_header[4] + string_header[5]]
    if len(strings) != string_header[5]:
        raise BpfBuildError("ELF section strings are truncated")
    names = tuple(_c_string(strings, item[0]) for item in headers)
    required = REQUIRED_PROGRAM_SECTIONS | {".maps"}
    selected = tuple(sorted(name for name in names if name in required))
    if set(selected) != required or len(selected) != len(required):
        raise BpfBuildError("ELF program sections are incomplete or duplicate")
    symbols: set[str] = set()
    for index, header_item in enumerate(headers):
        if names[index] != ".symtab":
            continue
        offset, size, link, entry_size = (
            header_item[4],
            header_item[5],
            header_item[6],
            header_item[9],
        )
        if link >= section_count or entry_size != _SYMBOL.size or size % entry_size:
            raise BpfBuildError("ELF symbol table is invalid")
        linked = headers[link]
        symbol_strings = payload[linked[4] : linked[4] + linked[5]]
        for position in range(offset, offset + size, entry_size):
            if position + entry_size > len(payload):
                raise BpfBuildError("ELF symbols are truncated")
            name_offset = _SYMBOL.unpack_from(payload, position)[0]
            if name_offset:
                symbols.add(_c_string(symbol_strings, name_offset))
    if not (REQUIRED_PROGRAM_SYMBOLS | REQUIRED_MAP_SYMBOLS).issubset(symbols):
        raise BpfBuildError("ELF symbols are incomplete")
    return InspectedElf(64, "little", machine, selected, frozenset(symbols))


def provenance_document(
    *,
    source: bytes,
    object_payload: bytes,
    map_schema: bytes,
    clang_version: str,
) -> dict[str, object]:
    inspected = inspect_bpf_elf(object_payload)
    return {
        "schema": "loom.task-image-builder-guard-bpf-build/v1",
        "builder_image": CLANG_IMAGE,
        "builder_platform": "linux/amd64",
        "clang_version": clang_version,
        "target": "bpfel",
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "object_sha256": hashlib.sha256(object_payload).hexdigest(),
        "object_size": len(object_payload),
        "map_schema_sha256": hashlib.sha256(map_schema).hexdigest(),
        "program_sections": list(inspected.sections),
        "program_symbols": sorted(REQUIRED_PROGRAM_SYMBOLS),
        "map_symbols": sorted(REQUIRED_MAP_SYMBOLS),
    }


def _run(command: Sequence[str], *, maximum: int = 64 * 1024) -> str:
    completed = subprocess.run(
        tuple(command),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=120,
        env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
    )
    if (
        completed.returncode != 0
        or len(completed.stdout) > maximum
        or len(completed.stderr) > maximum
    ):
        raise BpfBuildError("pinned BPF build command failed")
    return completed.stdout.decode("utf-8")


def validate_clang_version(output: str) -> str:
    lines = output.splitlines()
    if not lines or lines[0] != CLANG_VERSION:
        raise BpfBuildError("pinned Clang version changed")
    return lines[0]


def build(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    version_command, compile_command = render_commands(
        root=root, uid=os.geteuid(), gid=os.getegid()
    )
    version = validate_clang_version(_run(version_command))
    _run(compile_command)
    document = provenance_document(
        source=(root / _SOURCE).read_bytes(),
        object_payload=(root / _OBJECT).read_bytes(),
        map_schema=(root / _MAP_SCHEMA).read_bytes(),
        clang_version=version,
    )
    destination = root / _PROVENANCE
    destination.write_text(
        json.dumps(document, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="ascii",
    )
    return document


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    build(arguments.root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
