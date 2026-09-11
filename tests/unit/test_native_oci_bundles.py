"""Direct native OCI composition preserves the proven client/sidecar boundary."""

import hashlib
import json
from importlib import import_module
from pathlib import Path

import pytest

from loom.personal_dev_candidate import PERSONAL_DEV_BUILD_CONTRACT_SHA256
from tests.unit.test_capacity_build_admission_client import native_registration
from tests.unit.test_native_build_context import claim_for, context_for


def inputs(pool="oldlab"):
    module = import_module("loom_capacity_executor.native_oci_bundles")
    context = context_for(claim_for(native_registration(pool).binding)).model_copy(
        update={"build_contract_sha256": PERSONAL_DEV_BUILD_CONTRACT_SHA256})
    seccomp = json.dumps({"defaultAction": "SCMP_ACT_ERRNO", "syscalls": [
        {"names": ["read", "write", "exit_group"], "action": "SCMP_ACT_ALLOW"}]}, sort_keys=True).encode()
    policy = module.NativeOciBundlePolicy(rootfs=Path("/opt/loom/native-builder/rootfs"),
        workspace=Path("/var/lib/loom/native-jobs/private-attempt"), client_seccomp=seccomp,
        client_seccomp_sha256=hashlib.sha256(seccomp).hexdigest(), tmp_bytes=64 * 1024**2,
        buildkit_state_bytes=1024**3)
    return module, context, policy


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
def test_shipped_native_client_profile_accepts_threads_but_not_namespace_creation(pool):
    from dataclasses import replace

    module, context, policy = inputs(pool)
    path = Path(__file__).resolve().parents[2] / "deploy/personal-dev-builder/client-seccomp-v1.json"
    wire = path.read_bytes()
    policy = replace(policy, client_seccomp=wire, client_seccomp_sha256=hashlib.sha256(wire).hexdigest())
    profile = json.loads(module.render_native_oci_bundles(context, policy).client)["linux"]["seccomp"]
    assert profile["defaultAction"] == "SCMP_ACT_ERRNO"
    assert profile["defaultErrnoRet"] == 1
    entries = profile["syscalls"]
    allowed = {name for entry in entries if entry["action"] == "SCMP_ACT_ALLOW" for name in entry["names"]}
    assert {"read", "write", "execve", "prctl", "clone", "futex"} <= allowed
    assert not allowed & {"unshare", "setns", "mount", "ptrace", "bpf", "clone3", "io_uring_setup"}
    clone = next(entry for entry in entries if "clone" in entry["names"])
    assert clone["args"] == [{"index": 0, "op": "SCMP_CMP_MASKED_EQ", "value": 0x7E020000, "valueTwo": 0}]
    # libc must get ENOSYS to fall back to the filtered clone syscall.
    assert next(entry for entry in entries if "clone3" in entry["names"]) == {
        "names": ["clone3"], "action": "SCMP_ACT_ERRNO", "errnoRet": 38}


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
def test_native_oci_bundles_separate_authority_and_preserve_rootless_buildkit(pool):
    module, context, policy = inputs(pool)
    result = module.render_native_oci_bundles(context, policy)
    pause, sidecar, client = [json.loads(wire) for wire in (result.pause, result.buildkit, result.client)]
    assert result == module.render_native_oci_bundles(context, policy)
    assert result.sandbox_id.endswith(context.claim_digest)
    ids = (result.sandbox_id, result.buildkit_id, result.client_id)
    assert len(set(ids)) == 3
    assert all(not right.startswith(left) for left in ids for right in ids if right != left)
    for spec in (pause, sidecar, client):
        assert spec["root"] == {"path": str(policy.rootfs), "readonly": True}
        assert spec["process"]["user"] == {"uid": 1000, "gid": 1000}
        assert not spec.get("hooks") and not spec["linux"].get("devices")
        assert "cgroupsPath" not in spec["linux"]
        assert all("path" not in item for item in spec["linux"]["namespaces"])
        assert spec["process"]["capabilities"]["effective"] == []
    assert pause["process"]["args"] == ["/bin/sleep", "infinity"]
    assert pause["process"]["noNewPrivileges"] is True
    for spec in (sidecar, client):
        assert spec["annotations"]["io.kubernetes.cri.sandbox-id"] == result.sandbox_id
    assert sidecar["process"]["args"] == ["/usr/local/bin/loom-personal-dev-buildkitd"]
    assert sidecar["process"]["noNewPrivileges"] is False
    assert sidecar["process"]["capabilities"]["bounding"] == ["CAP_SETUID", "CAP_SETGID"]
    assert "seccomp" not in sidecar["linux"]
    assert client["process"]["noNewPrivileges"] is True
    assert client["process"]["capabilities"]["bounding"] == []
    assert client["linux"]["seccomp"] == json.loads(policy.client_seccomp)
    assert client["process"]["args"] == ["/usr/bin/python3", "-m", "loom.personal_dev_sandbox_builder",
        "build-allocated", "--contract-file", "/input/contract.json", "--source-archive", "/input/source.tar",
        "--workspace", "/output/build"]
    side_mounts = {mount["destination"]: mount for mount in sidecar["mounts"]}
    client_mounts = {mount["destination"]: mount for mount in client["mounts"]}
    assert "ro" in client_mounts["/input"]["options"]
    assert "ro" in client_mounts["/var/run/loom-buildkit"]["options"]
    assert "rw" in side_mounts["/var/run/loom-buildkit"]["options"]
    assert side_mounts["/sys/fs/cgroup"]["type"] == "cgroup"
    assert "ro" in side_mounts["/sys/fs/cgroup"]["options"]
    assert side_mounts["/var/lib/loom-buildkit"]["type"] == "tmpfs"
    assert all(mount["type"] != "bind" or mount["destination"] == "/var/run/loom-buildkit"
        for mount in sidecar["mounts"])
    assert {mount["destination"] for mount in client["mounts"] if mount["type"] == "bind"} == {
        "/input", "/output", "/var/run/loom-buildkit"}
    assert pause["annotations"]["dev.gvisor.spec.mount.buildkit-run.source"] == side_mounts["/var/run/loom-buildkit"]["source"]
    assert str(policy.workspace) not in json.dumps(pause["process"])
    assert all("TOKEN" not in value and "CREDENTIAL" not in value for value in client["process"]["env"])


@pytest.mark.parametrize("field,value", [
    ("rootfs", Path("/")), ("workspace", Path("relative")),
    ("workspace", Path("/opt/loom/native-builder/rootfs/output")),
    ("client_seccomp_sha256", "f" * 64), ("tmp_bytes", True),
    ("tmp_bytes", 0), ("buildkit_state_bytes", 1 << 50),
])
def test_native_oci_policy_rejects_unbound_or_unbounded_material(field, value):
    from dataclasses import replace
    _module, _context, policy = inputs()
    with pytest.raises(ValueError):
        replace(policy, **{field: value})


@pytest.mark.parametrize("profile", [
    {"defaultAction": "SCMP_ACT_ALLOW"},
    {"defaultAction": "SCMP_ACT_ERRNO", "syscalls": []},
    {"defaultAction": "SCMP_ACT_ERRNO", "syscalls": [{"action": "SCMP_ACT_ALLOW", "names": ["mount"]}]},
    {"defaultAction": "SCMP_ACT_ERRNO", "syscalls": [{"action": "SCMP_ACT_ALLOW", "names": ["clone"]}]},
    {"defaultAction": "SCMP_ACT_ERRNO", "syscalls": [{"action": "SCMP_ACT_ALLOW", "names": ["clone3"]}]},
    {"defaultAction": "SCMP_ACT_ERRNO", "syscalls": [{"action": [], "names": ["read"]}]},
    {"defaultAction": "SCMP_ACT_ERRNO", "architectures": [{}], "syscalls": [{"action": "SCMP_ACT_ALLOW", "names": ["read"]}]},
])
def test_native_oci_policy_rejects_unconfined_client_even_with_matching_digest(profile):
    from dataclasses import replace
    _module, _context, policy = inputs()
    wire = json.dumps(profile).encode()
    with pytest.raises(ValueError):
        replace(policy, client_seccomp=wire, client_seccomp_sha256=hashlib.sha256(wire).hexdigest())


@pytest.mark.parametrize("boundary", ["safe-thread", "namespace", "wrong-argument", "wrong-arch", "duplicate-key", "wrong-contract"])
def test_native_oci_policy_binds_architecture_and_excludes_namespace_clone(boundary):
    from dataclasses import replace
    module, context, policy = inputs()
    argument = {"index": 0, "op": "SCMP_CMP_MASKED_EQ", "value": 0x7E020000, "valueTwo": 0}
    if boundary == "namespace":
        argument["value"] &= ~0x10000000
    elif boundary == "wrong-argument":
        argument["index"] = 1
    profile = {"defaultAction": "SCMP_ACT_ERRNO", "architectures": [
        "SCMP_ARCH_AARCH64" if boundary == "wrong-arch" else "SCMP_ARCH_X86_64"],
        "syscalls": [{"names": ["clone"], "action": "SCMP_ACT_ALLOW", "args": [argument]}]}
    wire = json.dumps(profile).encode()
    if boundary == "duplicate-key":
        wire = wire.replace(b'{"defaultAction":', b'{"defaultAction":"SCMP_ACT_ALLOW","defaultAction":', 1)
    if boundary == "wrong-contract":
        context = context.model_copy(update={"build_contract_sha256": "f" * 64})

    def render():
        bound = replace(policy, client_seccomp=wire, client_seccomp_sha256=hashlib.sha256(wire).hexdigest())
        return module.render_native_oci_bundles(context, bound)

    if boundary == "safe-thread":
        assert json.loads(render().client)["linux"]["seccomp"] == profile
    else:
        with pytest.raises(ValueError):
            render()
