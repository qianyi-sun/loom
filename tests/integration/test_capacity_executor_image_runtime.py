"""Run with stdlib unittest inside the built executor image, on both architectures."""

from __future__ import annotations

import base64
import json
import os
import socket
import stat
import subprocess
import sys
import unittest
from pathlib import Path
from uuid import uuid4

if __name__ != "__main__":
    import pytest

    pytestmark = [pytest.mark.docker, pytest.mark.timeout(1000)]

_INSTALLER = Path(
    "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"
)


class CapacityExecutorImageRuntimeTest(unittest.TestCase):
    # Pytest builds the real image below; unittest runs these cases inside it.
    __test__ = False

    def test_installed_native_worker_imports_without_source_mount(self) -> None:
        result = subprocess.run([sys.executable, "-I", "-B", "-c",
            "import loom_capacity_executor.native_installed_worker"],
            capture_output=True, text=True, check=False, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_bootstrap_contract_is_installed(self) -> None:
        from loom_capacity_executor.launch_renderer import NativeTaskImageExecutionV2
        from loom_capacity_executor.native_worker_bootstrap import (
            NativeWorkerBootstrap,
            native_bootstrap_pipe,
            read_native_bootstrap,
        )

        native = NativeTaskImageExecutionV2(
            protocol="loom.task-image-native-execution/v2",
            launch_protocol="immutable-container-stdin/v1",
            platform="linux/amd64",
            root_key_id="test-root",
            environment="test",
            root_public_key=base64.urlsafe_b64encode(b"t" * 32).rstrip(b"=").decode(),
            root_activated_at="2026-09-01T00:00:00Z",
            root_expires_at="2026-10-01T00:00:00Z",
        )
        original = NativeWorkerBootstrap(native_execution=native, worker_credential="t" * 43)
        descriptor = native_bootstrap_pipe(original)
        try:
            self.assertEqual(read_native_bootstrap(descriptor), original)
        finally:
            os.close(descriptor)

    def test_installer_can_load_its_command_contract(self) -> None:
        result = subprocess.run(
            [sys.executable, "-I", "-B", str(_INSTALLER), "--help"],
            capture_output=True, text=True, check=False, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("discover-controller", result.stdout)
        self.assertIn("converge-prerequisite", result.stdout)

    def test_invalid_discovery_reaches_validation_without_host_access(self) -> None:
        result = subprocess.run(
            [sys.executable, "-I", "-B", str(_INSTALLER), "--operation", "discover-controller"],
            input="{}\n", capture_output=True, text=True, check=False, timeout=30,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("controller discovery request is invalid", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(result.stdout, "")


def _host_namespace_probe() -> None:
    """Treat this disposable container as the host, never mount the real host.

    A child gets separate UTS and network namespaces just like the OLDLAB
    installer container. Its /host is a bind of this fixture's root; PID 1 and
    every namespace entered belong only to this disposable fixture.
    """
    Path("/host").mkdir()
    subprocess.run(["mount", "--bind", "/", "/host"], check=True, timeout=10)
    subprocess.run(["mount", "--bind", "/proc", "/host/proc"], check=True, timeout=10)
    expected_hostname = socket.gethostname()
    expected_network = os.readlink("/proc/self/ns/net")
    child = r'''
import importlib.util
import json
import os
import socket
import sys

socket.sethostname(b"isolated-installer")
path = "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"
spec = importlib.util.spec_from_file_location("namespace_test_installer", path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

def observe(installer, payload):
    assert payload == b"{}\n"
    return (json.dumps({
        "controller_hostname": installer.hostname,
        "command_hostname": installer._run("/usr/bin/hostname").stdout.strip(),
        "command_network": installer._run(
            "/usr/bin/readlink", "/proc/self/ns/net"
        ).stdout.strip(),
        "isolated_hostname": socket.gethostname(),
        "isolated_network": os.readlink("/proc/self/ns/net"),
    }) + "\n").encode()

# Observe the real main/constructor/subprocess boundary, without invoking Slurm
# or touching any host authority file. This is not discovery acceptance evidence.
module._controller_discovery_operation = observe
raise SystemExit(module.main(["--host-root", "/host", "--operation", "discover-controller"]))
'''
    result = subprocess.run(
        ["unshare", "--uts", "--net", sys.executable, "-I", "-B", "-c", child],
        input="{}\n", capture_output=True, text=True, check=False, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["isolated_hostname"] == "isolated-installer"
    assert value["isolated_network"] != expected_network
    assert value["controller_hostname"] == expected_hostname, value
    assert value["command_hostname"] == expected_hostname, value
    assert value["command_network"] == expected_network, value


def _slurm_authority_probe() -> None:
    """Real owners and stale descriptors, entirely inside a disposable image."""
    import importlib.util

    subprocess.run(["groupadd", "--gid", "2007", "sharedwork"], check=True, timeout=10)
    subprocess.run(
        ["useradd", "--uid", "1000", "--gid", "2007", "--no-create-home", "trt"],
        check=True, timeout=10,
    )
    directory = Path("/tmp/oldlab-authority-fixture")
    directory.mkdir(mode=0o755)
    config = directory / "slurm.conf"
    original = (
        b"ClusterName=trt-oldlab\n"
        b"PartitionName=all Nodes=ALL Default=YES MaxTime=INFINITE State=UP OverSubscribe=NO\n"
        b"PartitionName=loom-staging Nodes=trt-eai-oldlab-[3-5] Default=NO "
        b"MaxTime=2-00:00:00 State=UP PriorityTier=100 AllowGroups=loom-rollout OverSubscribe=NO\n"
        b"# foreign scheduler configuration must remain byte-for-byte intact\n"
    )
    config.write_bytes(original)
    os.chown(config, 1000, 2007)
    config.chmod(0o664)
    spec = importlib.util.spec_from_file_location("authority_test_installer", _INSTALLER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    installer = module.ControllerInstaller(
        context=module.InstallContext(), runner=module.SubprocessRunner(),
    )
    try:
        installer._authority_file_sha256(config, executable=False)
    except module.CapacityExecutorInstallError:
        pass
    else:
        raise AssertionError("legacy writable authority was accepted")

    probe = r'''
source /converger.sh
CONFIG=/tmp/oldlab-authority-fixture/slurm.conf
STATE_ROOT=/tmp/oldlab-authority-fixture/state
BACKUP="$STATE_ROOT/slurm.conf.before-loom-staging-partition"
scontrol() {
  case "$*" in
    'show config') printf 'ClusterName = trt-oldlab\n' ;;
    'show partition loom-staging -o') printf '%s\n' "$PARTITION_LINE" ;;
    'show hostnames trt-eai-oldlab-[3-5]') printf '%s\n' "$EXPECTED_NODES" ;;
    show\ node\ *\ -o) printf 'NodeName=%s Partitions=all,loom-staging State=IDLE\n' "$3" ;;
    *) printf 'unexpected scheduler mutation or query: %s\n' "$*" >&2; exit 90 ;;
  esac
}
loom_oldlab_converge_partition
'''
    descriptor = os.open(config, os.O_WRONLY | os.O_APPEND)
    try:
        subprocess.run(["bash", "-c", probe], check=True, timeout=30)
        metadata = config.stat()
        assert (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode)) == (0, 0, 0o644)
        assert metadata.st_ino != os.fstat(descriptor).st_ino
        child = os.fork()
        if child == 0:
            os.setgroups([])
            os.setgid(2007)
            os.setuid(1000)
            os.write(descriptor, b"# stale shared writer\n")
            try:
                reopened = os.open(config, os.O_WRONLY)
            except PermissionError:
                os._exit(0)
            else:
                os.close(reopened)
                os._exit(1)
        assert os.waitpid(child, 0)[1] == 0
        assert config.read_bytes() == original
        assert (directory / "state/slurm.conf.before-root-authority").read_bytes() == original
        installer._authority_file_sha256(config, executable=False)
        subprocess.run(["bash", "-c", probe], check=True, timeout=30)
        assert config.stat().st_ino == metadata.st_ino
        assert config.read_bytes() == original
    finally:
        os.close(descriptor)


def test_capacity_executor_image_build() -> None:
    """Exercise the release build, whose final layer runs the installer cases."""
    image = f"loom-capacity-executor-runtime-test:{uuid4().hex}"
    container = f"loom-executor-namespace-test-{uuid4().hex}"
    try:
        result = subprocess.run(
            ["docker", "build", "--build-arg", f"LOOM_BUILD_SHA={'1' * 40}",
             "--file", "deploy/Dockerfile.capacity-executor", "--tag", image, "."],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True, text=True, check=False, timeout=900,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        result = subprocess.run(
            [
                "docker", "run", "--rm", "--name", container,
                "--network=none", "--user", "0:0",
                "--mount", f"type=bind,src={Path(__file__).resolve()},dst=/test.py,readonly",
                "--mount", (
                    f"type=bind,src={Path(__file__).resolve().parents[2]}/deploy/slurm/"
                    "converge-loom-oldlab-slurm-partition.sh,dst=/converger.sh,readonly"
                ),
                "--entrypoint", "/usr/local/bin/python", image,
                "-I", "-B", "/test.py", "--slurm-authority-probe",
            ],
            capture_output=True, text=True, check=False, timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        result = subprocess.run(
            [
                "docker", "run", "--rm", "--name", container,
                "--network=none", "--user", "0:0", "--cap-add=SYS_ADMIN",
                "--security-opt=seccomp=unconfined", "--security-opt=apparmor=unconfined",
                "--hostname", "disposable-controller",
                "--mount", f"type=bind,src={Path(__file__).resolve()},dst=/test.py,readonly",
                "--entrypoint", "/usr/local/bin/python", image,
                "-I", "-B", "/test.py", "--host-namespace-probe",
            ],
            capture_output=True, text=True, check=False, timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container],
            capture_output=True, check=False, timeout=30,
        )
        subprocess.run(
            ["docker", "image", "rm", image],
            capture_output=True, check=False, timeout=30,
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["--host-namespace-probe"]:
        _host_namespace_probe()
    elif sys.argv[1:] == ["--slurm-authority-probe"]:
        _slurm_authority_probe()
    else:
        unittest.main()
