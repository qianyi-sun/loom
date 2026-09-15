"""Fixed three-container native build layout, not launch or installation authority.

The installed runtime must verify immutable rootfs/runtime and seccomp material,
private workspace ownership, fresh execution authority and actual Slurm cgroup
containment before consuming these bundles. Nothing here starts a process or
grants capacity. Feature source cannot select commands, mounts or runtime flags.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.personal_dev_candidate import PERSONAL_DEV_BUILD_CONTRACT_SHA256
from loom_capacity_agent.build_admission import BuildSourceContextV1

_MIB = 1024**2
_FORBIDDEN_CLIENT_CALLS = frozenset({
    "mount", "umount", "umount2", "pivot_root", "setns", "unshare", "bpf",
    "ptrace", "process_vm_readv", "process_vm_writev", "keyctl", "add_key",
    "request_key", "reboot", "kexec_load", "kexec_file_load", "init_module",
    "finit_module", "delete_module", "open_by_handle_at", "fsopen", "fsconfig",
    "fsmount", "move_mount", "open_tree", "mount_setattr", "clone3",
})


def _seccomp(wire: bytes, digest: str) -> dict[str, Any]:
    if (type(wire) is not bytes or not 2 <= len(wire) <= 256 * 1024
        or hashlib.sha256(wire).hexdigest() != digest):
        raise ValueError("native client seccomp material changed")
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("native client seccomp has duplicate keys")
            result[key] = value
        return result
    profile = json.loads(wire, object_pairs_hook=unique)
    if (not isinstance(profile, dict) or profile.get("defaultAction") != "SCMP_ACT_ERRNO"
        or set(profile) - {"defaultAction", "defaultErrnoRet", "architectures", "syscalls"}
        or type(profile.get("defaultErrnoRet", 1)) is not int
        or not 1 <= profile.get("defaultErrnoRet", 1) <= 4095):
        raise ValueError("native client requires a deny-default seccomp profile")
    entries = profile.get("syscalls")
    if not isinstance(entries, list) or not 1 <= len(entries) <= 1024:
        raise ValueError("native client seccomp syscall list is invalid")
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) - {"names", "action", "args", "errnoRet"}
            or not isinstance(entry.get("action"), str)
            or entry["action"] not in {"SCMP_ACT_ALLOW", "SCMP_ACT_ERRNO"}
            or not isinstance(entry.get("names"), list) or not 1 <= len(entry["names"]) <= 1024
            or any(not isinstance(name, str) or not name.isidentifier() for name in entry["names"])
            or (entry["action"] == "SCMP_ACT_ALLOW" and _FORBIDDEN_CLIENT_CALLS.intersection(entry["names"]))):
            raise ValueError("native client seccomp permits an invalid syscall boundary")
        if "errnoRet" in entry and (type(entry["errnoRet"]) is not int or not 1 <= entry["errnoRet"] <= 4095):
            raise ValueError("native client seccomp errno is invalid")
        arguments = entry.get("args", [])
        if not isinstance(arguments, list) or len(arguments) > 6:
            raise ValueError("native client seccomp arguments are invalid")
        for argument in arguments:
            if (not isinstance(argument, dict) or set(argument) - {"index", "value", "valueTwo", "op"}
                or type(argument.get("index")) is not int or not 0 <= argument["index"] <= 5
                or not isinstance(argument.get("op"), str)
                or argument["op"] not in {"SCMP_CMP_NE", "SCMP_CMP_LT", "SCMP_CMP_LE", "SCMP_CMP_EQ", "SCMP_CMP_GE", "SCMP_CMP_GT", "SCMP_CMP_MASKED_EQ"}
                or any(type(argument.get(key, 0)) is not int or not 0 <= argument.get(key, 0) < 2**64 for key in ("value", "valueTwo"))):
                raise ValueError("native client seccomp argument comparison is invalid")
        if entry["action"] == "SCMP_ACT_ALLOW" and "clone" in entry["names"]:
            # Threads may clone, but the client must not create namespaces.
            namespace_flags = 0x7E020000
            if not any(argument["index"] == 0 and argument["op"] == "SCMP_CMP_MASKED_EQ"
                and argument.get("value", 0) & namespace_flags == namespace_flags
                and argument.get("valueTwo", 0) == 0 for argument in arguments):
                raise ValueError("native client clone requires namespace exclusion")
    architectures = profile.get("architectures", [])
    if (not isinstance(architectures, list)
        or any(not isinstance(arch, str) or arch not in {"SCMP_ARCH_X86_64", "SCMP_ARCH_X86", "SCMP_ARCH_X32", "SCMP_ARCH_AARCH64", "SCMP_ARCH_ARM"} for arch in architectures)):
        raise ValueError("native client seccomp architectures are invalid")
    return profile


@dataclass(frozen=True, slots=True)
class NativeOciBundlePolicy:
    """Already protected material; path validation here does not prove ownership."""

    rootfs: Path
    workspace: Path
    client_seccomp: bytes
    client_seccomp_sha256: str
    tmp_bytes: int
    buildkit_state_bytes: int

    def __post_init__(self) -> None:
        for path in (self.rootfs, self.workspace):
            if (not isinstance(path, Path) or not path.is_absolute() or path == Path("/")
                or ".." in path.parts or any(character in str(path) for character in ("\x00", "\n", "\r"))):
                raise ValueError("native OCI paths must be absolute and non-root")
        if (self.rootfs == self.workspace or self.rootfs in self.workspace.parents
            or self.workspace in self.rootfs.parents):
            raise ValueError("native rootfs and private workspace must be disjoint")
        for value in (self.tmp_bytes, self.buildkit_state_bytes):
            if type(value) is not int or not _MIB <= value <= 64 * 1024**3:
                raise ValueError("native scratch bounds are invalid")
        _seccomp(self.client_seccomp, self.client_seccomp_sha256)


@dataclass(frozen=True, slots=True)
class NativeOciBundles:
    sandbox_id: str
    buildkit_id: str
    client_id: str
    pause: bytes
    buildkit: bytes
    client: bytes


def render_native_oci_bundles(context: BuildSourceContextV1, policy: NativeOciBundlePolicy) -> NativeOciBundles:
    """Produce immutable bytes; the caller still owns all launch/cleanup fences."""
    context = BuildSourceContextV1.model_validate_json(context.model_dump_json())
    policy.__post_init__()
    if context.build_contract_sha256 != PERSONAL_DEV_BUILD_CONTRACT_SHA256:
        raise ValueError("native OCI build contract changed")
    # runsc lifecycle commands resolve IDs by prefix, including exact-looking
    # IDs. Never make a child ID start with its root ID: root kill/state would
    # become ambiguous precisely while its children are alive.
    sandbox_id = "loom-native-pause-" + context.claim_digest
    buildkit_id = "loom-native-buildkit-" + context.claim_digest
    client_id = "loom-native-client-" + context.claim_digest
    shared = str(policy.workspace / "buildkit-run")

    def spec() -> dict[str, Any]:
        return {"ociVersion": "1.2.0", "root": {"path": str(policy.rootfs), "readonly": True},
            "process": {"terminal": False, "user": {"uid": 1000, "gid": 1000}, "args": [],
                "env": ["PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8", "HOME=/tmp", "TMPDIR=/tmp", "PYTHONPATH=/opt/loom-personal-dev-builder"],
                "cwd": "/", "capabilities": {name: [] for name in ("bounding", "effective", "inheritable", "permitted", "ambient")},
                "noNewPrivileges": True},
            "mounts": [
                {"destination": "/proc", "type": "proc", "source": "proc", "options": ["nosuid", "noexec", "nodev"]},
                {"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "mode=755", "size=67108864"]},
                {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "nodev", "mode=1777", f"size={policy.tmp_bytes}"]}],
            "linux": {"namespaces": [{"type": kind} for kind in ("pid", "network", "ipc", "uts", "mount")]}}

    pause, sidecar, client = spec(), spec(), spec()
    pause["process"]["args"] = ["/bin/sleep", "infinity"]
    pause["annotations"] = {"io.kubernetes.cri.container-type": "sandbox",
        "dev.gvisor.spec.mount.buildkit-run.source": shared,
        "dev.gvisor.spec.mount.buildkit-run.type": "tmpfs",
        "dev.gvisor.spec.mount.buildkit-run.share": "pod",
        "dev.gvisor.spec.mount.buildkit-run.options": "rw,rprivate,mode=1777,size=67108864"}
    for target, mode in ((sidecar, "rw"), (client, "ro")):
        target["annotations"] = {"io.kubernetes.cri.container-type": "container", "io.kubernetes.cri.sandbox-id": sandbox_id}
        target["mounts"].append({"destination": "/var/run/loom-buildkit", "type": "bind", "source": shared, "options": ["bind", mode]})
    sidecar["process"]["args"] = ["/usr/local/bin/loom-personal-dev-buildkitd"]
    sidecar["process"]["noNewPrivileges"] = False
    sidecar["process"]["capabilities"]["bounding"] = ["CAP_SETUID", "CAP_SETGID"]
    sidecar["mounts"].extend([
        {"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": ["nosuid", "noexec", "nodev", "ro"]},
        {"destination": "/sys/fs/cgroup", "type": "cgroup", "source": "cgroup", "options": ["nosuid", "noexec", "nodev", "relatime", "ro"]},
        {"destination": "/var/lib/loom-buildkit", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "nodev", "uid=1000", "gid=1000", "mode=700", f"size={policy.buildkit_state_bytes}"]}])
    client["process"]["args"] = ["/usr/bin/python3", "-m", "loom.personal_dev_sandbox_builder", "build-allocated",
        "--contract-file", "/input/contract.json", "--source-archive", "/input/source.tar", "--workspace", "/output/build"]
    client["linux"]["seccomp"] = _seccomp(policy.client_seccomp, policy.client_seccomp_sha256)
    architectures = client["linux"]["seccomp"].get("architectures", [])
    native_arch = "SCMP_ARCH_AARCH64" if context.platform == "linux/arm64" else "SCMP_ARCH_X86_64"
    if architectures and native_arch not in architectures:
        raise ValueError("native client seccomp architecture differs from source")
    for destination, source, mode in (("/input", "input", "ro"), ("/output", "output", "rw")):
        client["mounts"].append({"destination": destination, "type": "bind", "source": str(policy.workspace / source), "options": ["bind", mode]})

    def wire(value: dict[str, Any]) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    return NativeOciBundles(sandbox_id, buildkit_id, client_id, wire(pause), wire(sidecar), wire(client))
