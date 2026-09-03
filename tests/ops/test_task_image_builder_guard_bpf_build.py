from __future__ import annotations

import hashlib
import json
import os
import subprocess
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


def test_egress_dns_limiter_covers_connected_udp_for_both_address_families(
    tmp_path: Path,
) -> None:
    harness = tmp_path / "dns-accounting.c"
    executable = tmp_path / "dns-accounting"
    harness.write_text(
        r'''#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/in6.h>
#include <linux/ip.h>
#include <linux/ipv6.h>
#include <linux/tcp.h>
#include <linux/udp.h>
#include <string.h>

static void *mock_lookup(void *map, const void *key);
static long mock_update(void *map, const void *key, const void *value,
                        unsigned long long flags);
static long mock_delete(void *map, const void *key);
static unsigned long long mock_time(void);
static unsigned long long mock_cookie(void *ctx);
static void mock_lock(struct bpf_spin_lock *lock);
static void mock_unlock(struct bpf_spin_lock *lock);

#define BPF_FUNC_map_lookup_elem ((long)mock_lookup)
#define BPF_FUNC_map_update_elem ((long)mock_update)
#define BPF_FUNC_map_delete_elem ((long)mock_delete)
#define BPF_FUNC_ktime_get_ns ((long)mock_time)
#define BPF_FUNC_get_socket_cookie ((long)mock_cookie)
#define BPF_FUNC_spin_lock ((long)mock_lock)
#define BPF_FUNC_spin_unlock ((long)mock_unlock)
#include "deploy/task-image-builder/guard-network-v1.bpf.c"

static struct subject_limit test_limit;
static unsigned char allow_value = 1;
static unsigned long long drop_values[16];

static void *mock_lookup(void *map, const void *key)
{
    if (map == &subject_limits)
        return &test_limit;
    if (map == &allow_v4 || map == &allow_v6)
        return &allow_value;
    if (map == &drop_counters)
        return &drop_values[*(const unsigned int *)key];
    return 0;
}

static long mock_update(void *map, const void *key, const void *value,
                        unsigned long long flags)
{
    (void)map; (void)key; (void)value; (void)flags;
    return 0;
}

static long mock_delete(void *map, const void *key)
{
    (void)map; (void)key;
    return 0;
}

static unsigned long long mock_time(void) { return 1; }
static unsigned long long mock_cookie(void *ctx) { (void)ctx; return 1; }
static void mock_lock(struct bpf_spin_lock *lock) { (void)lock; }
static void mock_unlock(struct bpf_spin_lock *lock) { (void)lock; }

static void reset_limits(void)
{
    memset(&test_limit, 0, sizeof(test_limit));
    test_limit.window_start_ns = 1;
    test_limit.ingress_bytes_per_second = 1000000;
    test_limit.egress_bytes_per_second = 1000000;
    test_limit.ingress_packets_per_second = 1000;
    test_limit.egress_packets_per_second = 1000;
    test_limit.new_flows_per_second = 1000;
    test_limit.dns_queries_per_second = 1;
    test_limit.max_concurrent_flows = 1000;
}

int main(void)
{
    struct __sk_buff ctx = {0};
    unsigned char frame4[sizeof(struct iphdr) + sizeof(struct udphdr)] = {0};
    struct iphdr *ip4 = (struct iphdr *)frame4;
    struct udphdr *udp4 = (struct udphdr *)(ip4 + 1);
    unsigned char frame6[sizeof(struct ipv6hdr) + sizeof(struct udphdr)] = {0};
    struct ipv6hdr *ip6 = (struct ipv6hdr *)frame6;
    struct udphdr *udp6 = (struct udphdr *)(ip6 + 1);

    ip4->version = 4;
    ip4->ihl = 5;
    ip4->protocol = IPPROTO_UDP;
    udp4->dest = __builtin_bswap16(53);
    ctx.len = sizeof(frame4);
    reset_limits();
    if (packet_v4(&ctx, frame4, frame4 + sizeof(frame4), 1, 0) != 1)
        return 10;
    if (packet_v4(&ctx, frame4, frame4 + sizeof(frame4), 1, 0) != 0)
        return 11;

    ip6->version = 6;
    ip6->nexthdr = IPPROTO_UDP;
    udp6->dest = __builtin_bswap16(53);
    ctx.len = sizeof(frame6);
    reset_limits();
    if (packet_v6(&ctx, frame6, frame6 + sizeof(frame6), 1, 0) != 1)
        return 20;
    if (packet_v6(&ctx, frame6, frame6 + sizeof(frame6), 1, 0) != 0)
        return 21;
    return 0;
}
''',
        encoding="ascii",
    )
    compiled = subprocess.run(
        (
            "/usr/bin/cc",
            "-std=gnu11",
            "-O0",
            "-Wall",
            "-Werror",
            f"-I{ROOT}",
            str(harness),
            "-o",
            str(executable),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert compiled.returncode == 0, compiled.stderr

    exercised = subprocess.run(
        (str(executable),),
        check=False,
        capture_output=True,
        text=True,
    )

    assert exercised.returncode == 0, exercised.stderr
