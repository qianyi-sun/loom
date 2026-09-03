from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_guard_bpf_build as build

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "deploy/task-image-builder/guard-network-v1.bpf.c"
OBJECT = ROOT / "deploy/task-image-builder/guard-network-v1.bpf.o"
PROVENANCE = ROOT / "deploy/task-image-builder/guard-network-v1.bpf.build.json"
MAP_SCHEMA = ROOT / "deploy/task-image-builder/guard-network-map-schema-v1.json"
EXPECTED_MAP_SYMBOLS = frozenset(
    {
        "scope_subject",
        "allow_v4",
        "allow_v6",
        "subject_limits",
        "flow_sockets",
        "drop_counters",
    }
)


_PINNED_CLANG_OUTPUT = """\
Debian clang version 18.1.8 (++20240731024826+3b5b5c1ec4a3-1~exp1~20240731144843.145)
Target: x86_64-pc-linux-gnu
Thread model: posix
InstalledDir: /usr/bin
"""


def test_pinned_image_actual_clang_identity_is_accepted() -> None:
    assert build.validate_clang_version(_PINNED_CLANG_OUTPUT) == (
        "Debian clang version 18.1.8 "
        "(++20240731024826+3b5b5c1ec4a3-1~exp1~20240731144843.145)"
    )


def test_changed_clang_identity_is_rejected() -> None:
    with pytest.raises(build.BpfBuildError, match="pinned Clang version changed"):
        build.validate_clang_version("clang version 18.1.8\n")


def test_rendered_build_is_offline_digest_pinned_and_architecture_independent() -> None:
    version, compile_command = build.render_commands(
        root=ROOT,
        uid=os.geteuid(),
        gid=os.getegid(),
    )

    prefix = (
        "/usr/bin/docker",
        "run",
        "--rm",
        "--network=none",
        "--platform=linux/amd64",
        f"--user={os.geteuid()}:{os.getegid()}",
        f"--volume={ROOT}:/src:rw",
        "--workdir=/src",
        build.CLANG_IMAGE,
    )
    assert version == (*prefix, "clang", "--version")
    assert compile_command == (
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
        "deploy/task-image-builder/guard-network-v1.bpf.c",
        "-o",
        "deploy/task-image-builder/guard-network-v1.bpf.o",
    )


def test_checked_bpf_object_exports_every_default_deny_hook_and_map() -> None:
    inspected = build.inspect_bpf_elf(OBJECT.read_bytes())

    assert inspected.elf_class == 64
    assert inspected.byte_order == "little"
    assert inspected.machine == build.EM_BPF
    assert inspected.sections == tuple(sorted(build.REQUIRED_PROGRAM_SECTIONS | {".maps"}))
    assert build.REQUIRED_PROGRAM_SYMBOLS.issubset(inspected.symbols)
    assert EXPECTED_MAP_SYMBOLS.issubset(inspected.symbols)


def test_checked_bpf_provenance_binds_source_object_schema_and_compiler() -> None:
    document = json.loads(PROVENANCE.read_text(encoding="ascii"))
    schema = MAP_SCHEMA.read_bytes()

    assert document == build.provenance_document(
        source=SOURCE.read_bytes(),
        object_payload=OBJECT.read_bytes(),
        map_schema=schema,
        clang_version=build.CLANG_VERSION,
    )
    assert document["builder_image"] == build.CLANG_IMAGE
    assert document["target"] == "bpfel"
    assert document["source_sha256"] == hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    assert document["object_sha256"] == hashlib.sha256(OBJECT.read_bytes()).hexdigest()
    assert document["map_schema_sha256"] == hashlib.sha256(schema).hexdigest()


def test_map_schema_has_exact_binary_layout_and_positive_caps() -> None:
    document = json.loads(MAP_SCHEMA.read_text(encoding="ascii"))

    assert document["schema"] == "loom.task-image-builder-guard-bpf-maps/v1"
    assert document["maps"] == {
        "allow_v4": {
            "type": "hash",
            "key_size": 12,
            "max_entries": 4096,
            "value_size": 1,
        },
        "allow_v6": {
            "type": "hash",
            "key_size": 24,
            "max_entries": 4096,
            "value_size": 1,
        },
        "drop_counters": {
            "type": "percpu_array",
            "key_size": 4,
            "max_entries": 16,
            "value_size": 8,
        },
        "flow_sockets": {
            "type": "hash",
            "key_size": 8,
            "max_entries": 4096,
            "value_size": 8,
        },
        "scope_subject": {
            "type": "array",
            "key_size": 4,
            "max_entries": 1,
            "value_size": 4,
        },
        "subject_limits": {
            "type": "hash",
            "key_size": 4,
            "max_entries": 16,
            "value_size": 120,
        },
    }
