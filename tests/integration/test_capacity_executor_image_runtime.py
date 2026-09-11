"""Run with stdlib unittest inside the built executor image, on both architectures."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
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
        from loom_capacity_executor.native_worker_launch import run_native_worker_on_host
        from loom_control_plane.slurm_job_cgroup import discover_docker_cgroup_parent

        self.assertTrue(callable(run_native_worker_on_host))
        self.assertTrue(callable(discover_docker_cgroup_parent))
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

    def test_native_receiver_installed_process_hardens_and_sanitizes_exit(self) -> None:
        probe = """
import ctypes, os, resource, stat
from loom_capacity_executor.native_bootstrap_receiver import run_native_bootstrap_receiver_process
from loom_capacity_executor.native_bootstrap_transport import _assert_private_process
def factory():
    assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
    assert ctypes.CDLL(None).prctl(3, 0, 0, 0, 0) == 0
    _assert_private_process()
    raise SystemExit('disposable-private-admission-detail')
result = run_native_bootstrap_receiver_process(factory)
assert stat.S_ISCHR(os.fstat(0).st_mode) and os.read(0, 1) == b''
raise SystemExit(result)
"""
        result = subprocess.run([sys.executable, "-I", "-B", "-c", probe],
            input=b"disposable-capability-bytes", capture_output=True, check=False, timeout=30)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"native bootstrap receiver refused\n")

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
        _assert_fixed_native_receiver_process_adapter(image)
        _assert_native_docker_stdin_is_not_retained(image)
        _assert_native_image_environment_is_removed(image)
        asyncio.run(_assert_native_attachment_loss_needs_container_cleanup(image))
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container],
            capture_output=True, check=False, timeout=30,
        )
        subprocess.run(
            ["docker", "image", "rm", image],
            capture_output=True, check=False, timeout=30,
        )


def _assert_fixed_native_receiver_process_adapter(image: str) -> None:
    """Actual pinned interpreter, installed module and fixed config; no DB authority.

    A historical status lookup needs no live admission credential. Its unknown
    response proves this process composition, not delivery or native activation.
    """
    from uuid import UUID, uuid5

    from loom_capacity_executor.native_bootstrap_delivery import (
        NativeBootstrapDeliveryReceiptV1,
        encode_native_delivery_query,
    )
    from loom_capacity_manager.executable_contracts import canonical_executable_digest
    from tests.unit.test_capacity_executor_bootstrap_handoff import _physical
    from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture

    binding = launch_context_fixture().binding
    physical = _physical(binding).model_copy(update={"operation_id": uuid5(
        UUID("cb359b0c-a844-4bc5-9592-a4c35e344f3d"), f"physical-bind:{binding.intent_id}")})
    digest = canonical_executable_digest(binding)
    expected = NativeBootstrapDeliveryReceiptV1(target_node=binding.node_ids[0],
        reference=digest + ".json", binding_sha256=digest,
        physical_binding_sha256=canonical_executable_digest(physical),
        bootstrap_sha256="a" * 64, source_payload_sha256="b" * 64,
        expires_at=datetime(2026, 8, 13, tzinfo=UTC))
    query = encode_native_delivery_query(physical, expected)
    script = """
import asyncio, hashlib, os, stat, sys
from pathlib import Path
from loom_capacity_executor.native_bootstrap_delivery import parse_native_delivery_query, _canonical
from loom_capacity_executor.native_bootstrap_process_adapter import NativeBootstrapProcessAdapter, NativeBootstrapReceiverProcessPolicy
from loom_capacity_executor.native_bootstrap_receiver import NativeBootstrapReceiverConfigV1
from loom_capacity_executor.native_worker_bootstrap import _disable_bootstrap_dumps
from loom_capacity_executor.runtime import canonical_admission_directory_digest
from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2
from loom_capacity_executor.trusted_launcher import TrustedCandidateExecutableV2
_disable_bootstrap_dumps()
raw = sys.stdin.buffer.read(65537)
query = parse_native_delivery_query(raw)
binding = query.physical.binding
base = Path('/run/loom-receiver')
admission = base / 'admission'
admission.mkdir(mode=0o700)
config = NativeBootstrapReceiverConfigV1(directory=str(base), target_node=binding.node_ids[0],
    pool_id=binding.pool_id, trusted_release_sha256=binding.execution.trusted_fleet_release_sha256,
    admission_directory=str(admission), admission_directory_sha256=canonical_admission_directory_digest(admission))
wire = _canonical(config)
path = base / 'receiver.json'
path.write_bytes(wire)
path.chmod(0o600)
interpreter = Path(sys.executable).resolve(strict=True)
info = interpreter.stat()
policy = NativeBootstrapReceiverProcessPolicy(
    interpreter=TrustedCandidateExecutableV2(path=str(interpreter), sha256=hashlib.sha256(interpreter.read_bytes()).hexdigest(),
        owner_uid=info.st_uid, mode=stat.S_IMODE(info.st_mode)),
    configuration=SlurmFileIdentityV2(path=str(path), sha256=hashlib.sha256(wire).hexdigest(), owner_uid=os.geteuid()))
async def exercise():
    adapter = NativeBootstrapProcessAdapter(policy)
    try:
        assert await adapter.observe_receipt(raw) is None
        assert adapter.active_operations == 0
    finally:
        await adapter.aclose()
    assert adapter.active_operations == 0
asyncio.run(exercise())
print('fixed-receiver-status-unknown')
"""
    result = subprocess.run(["docker", "run", "--rm", "--interactive", "--read-only", "--network=none",
        "--cpus=1", "--memory=512m", "--pids-limit=32", "--user=65532:65532",
        "--tmpfs=/run/loom-receiver:rw,nosuid,nodev,noexec,mode=0700,uid=65532,gid=65532",
        "--entrypoint=/usr/local/bin/python", image, "-I", "-B", "-c", script],
        input=query, capture_output=True, check=False, timeout=45)
    assert result.returncode == 0, result.stderr.decode()
    assert result.stdout == b"fixed-receiver-status-unknown\n" and result.stderr == b""


def _assert_native_image_environment_is_removed(image: str) -> None:
    """Exercise production environment flags against inherited Docker image ENV.

    Uses a disposable diagnostic command, not the actual worker, so this is
    loader/environment-mechanism evidence rather than protected registration.
    """
    from loom_capacity_executor.native_worker_container import (
        PreparedNativeImage,
        native_create_argv,
    )
    from tests.unit.test_native_worker_container import _allocation

    cli = shutil.which("docker")
    assert cli is not None
    derived = f"loom-native-env-test:{uuid4().hex}"
    container = None
    try:
        build = subprocess.run([cli, "build", "--quiet", "--tag", derived, "-"],
            input=(f"FROM {image}\nENV LD_PRELOAD=/invalid.so PYTHONHOME=/invalid "
                   "PYTHONPATH=/invalid DOCKER_HOST=tcp://foreign:2375\n").encode(),
            capture_output=True, check=False, timeout=60)
        assert build.returncode == 0, build.stderr.decode()
        actual = json.loads(subprocess.check_output([cli, "image", "inspect", derived], timeout=15))[0]
        prepared = PreparedNativeImage(image_id=actual["Id"],
            platform=f"{actual['Os']}/{actual['Architecture']}",
            inherited_environment=tuple(item.split("=", 1)[0] for item in actual["Config"]["Env"]))
        environment_flags = [arg for arg in native_create_argv(prepared, _allocation(),
            name="loom-native-env-test", ownership="a" * 64) if arg.startswith("--env=")]
        diagnostic = (
            "import os; assert not ({'LD_PRELOAD','PYTHONHOME','PYTHONPATH'} & os.environ.keys()); "
            "assert os.environ['DOCKER_HOST']=='unix:///var/run/docker.sock'; print('isolated')"
        )
        container = subprocess.check_output([cli, "container", "create", "--read-only",
            "--network=none", "--entrypoint=/usr/local/bin/python", *environment_flags,
            actual["Id"], "-I", "-c", diagnostic], env={}, timeout=15).decode().strip()
        assert re.fullmatch(r"[0-9a-f]{64}", container)
        inspected = json.loads(subprocess.check_output([cli, "container", "inspect", container], env={}, timeout=15))[0]
        # Docker retains bare names in Config.Env as explicit unset markers;
        # only NAME=value becomes a process environment variable. The actual
        # Python startup below proves loader/config variables are unavailable.
        installed_environment = dict(item.split("=", 1) for item in inspected["Config"]["Env"] if "=" in item)
        assert not ({"LD_PRELOAD", "PYTHONHOME", "PYTHONPATH"} & installed_environment.keys())
        started = subprocess.run([cli, "container", "start", "--attach", container], env={},
            capture_output=True, check=False, timeout=30)
        assert started.returncode == 0, started.stderr.decode()
        assert started.stdout.strip() == b"isolated"
    finally:
        if container is not None:
            subprocess.run([cli, "container", "rm", "--force", container], capture_output=True, check=True, timeout=30)
        subprocess.run([cli, "image", "rm", derived], capture_output=True, check=False, timeout=30)


def _assert_native_docker_stdin_is_not_retained(image: str) -> None:
    """Exercise the installed production decoder through actual Docker restart.

    This certifies transport semantics only: it is not worker registration,
    Slurm containment, root attestation or execution-start acceptance.
    """
    from loom_capacity_executor.native_worker_bootstrap import (
        NativeWorkerBootstrap,
        encode_native_bootstrap,
    )
    from tests.unit.test_capacity_executor_native_launch_profile import native_profile_fixture

    native = native_profile_fixture().native_execution
    assert native is not None
    now = datetime.now(UTC).replace(microsecond=0)
    native = native.model_copy(update={
        "root_activated_at": (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "root_expires_at": (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    credential = "test-native-bootstrap-" + uuid4().hex
    configuration_secret = "test-native-config-" + uuid4().hex
    wire = encode_native_bootstrap(NativeWorkerBootstrap(
        native_execution=native, worker_credential=credential,
        canonical_worker_settings=json.dumps(
            {"minio_secret_key": configuration_secret}, sort_keys=True, separators=(",", ":"),
        ),
    ))
    image_id = subprocess.check_output(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        text=True, timeout=15,
    ).strip()
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", image_id)
    script = """import ctypes, os, resource
from loom_capacity_executor.native_worker_bootstrap import consume_native_worker_bootstrap, NativeBootstrapError
try:
    bootstrap = consume_native_worker_bootstrap()
except NativeBootstrapError:
    print('refused')
    raise SystemExit(65)
assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
assert ctypes.CDLL(None).prctl(3, 0, 0, 0, 0) == 0
assert os.fstat(0).st_rdev == os.makedev(1, 3)
assert os.read(0, 1) == b''
assert set(bootstrap.worker_settings()) == {'minio_secret_key'}
print('accepted')
"""
    container = subprocess.check_output(
        ["docker", "create", "--interactive", "--restart=no", "--read-only",
         "--network=none", "--cpus=0.25", "--memory=256m", "--pids-limit=32",
         "--entrypoint=/usr/local/bin/python", image_id, "-I", "-c", script],
        text=True, timeout=15,
    ).strip()
    assert re.fullmatch(r"[0-9a-f]{64}", container)
    try:
        first = subprocess.run(
            ["docker", "start", "--attach", "--interactive", container],
            input=wire, capture_output=True, check=False, timeout=30,
        )
        assert first.returncode == 0, first.stderr.decode()
        assert first.stdout.strip() == b"accepted"
        restarted = subprocess.run(
            ["docker", "start", "--attach", "--interactive", container],
            input=b"", capture_output=True, check=False, timeout=30,
        )
        assert restarted.returncode == 65, restarted.stderr.decode()
        assert restarted.stdout.strip() == b"refused"
        for command in (["docker", "inspect", container], ["docker", "logs", container]):
            retained = subprocess.check_output(command, stderr=subprocess.STDOUT, timeout=15)
            assert credential.encode() not in retained
            assert configuration_secret.encode() not in retained
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container], capture_output=True, check=True, timeout=30,
        )


async def _assert_native_attachment_loss_needs_container_cleanup(image: str) -> None:
    """Use a diagnostic container to exercise real attachment and exact cleanup."""
    from loom_capacity_executor.native_worker_container import (
        FixedDockerCLI,
        remove_native_container,
    )
    from loom_capacity_executor.native_worker_launch import (
        native_daemon_cgroup_driver,
        run_attached_native_worker,
    )
    from tests.unit.test_worker_native_entrypoint import _configured_bootstrap

    executable = shutil.which("docker")
    assert executable is not None
    descriptor = os.open(executable, os.O_RDONLY | os.O_CLOEXEC)
    with tempfile.TemporaryDirectory(prefix="loom-native-cli-test-") as configuration:
        cli = FixedDockerCLI(executable=f"/proc/self/fd/{descriptor}", descriptor=descriptor,
            config_directory=configuration)
        container = None
        attached = None
        try:
            daemon = cli.json("info", "--format={{json .}}")
            assert isinstance(daemon, dict)
            if daemon.get("CgroupVersion") == "2" and daemon.get("CgroupDriver") == "cgroupfs":
                assert native_daemon_cgroup_driver(cli) == "cgroupfs"
            else:
                # A diagnostic container can exercise Docker's attachment mechanics
                # on this host, but the actual native preflight must refuse its
                # incompatible topology. Do not turn transport proof into Slurm
                # containment acceptance or weaken the native check for this test.
                from loom_capacity_executor.native_worker_container import NativeContainerError

                with pytest.raises(NativeContainerError, match="cgroupfs"):
                    native_daemon_cgroup_driver(cli)
            diagnostic = (
                "from loom_capacity_executor.native_worker_bootstrap import consume_native_worker_bootstrap; "
                "consume_native_worker_bootstrap(); import time; time.sleep(120)"
            )
            container = cli.call("container", "create", "--interactive", "--restart=no", "--read-only",
                "--network=none", "--cpus=0.25", "--memory=256m", "--pids-limit=32",
                "--entrypoint=/usr/local/bin/python", image, "-I", "-c", diagnostic).decode().strip()
            assert re.fullmatch(r"[0-9a-f]{64}", container)
            attached = asyncio.create_task(run_attached_native_worker(cli, container, _configured_bootstrap()))
            for _attempt in range(100):
                await asyncio.sleep(0.1)
                if cli.json("container", "inspect", container)[0]["State"]["Running"]:
                    break
            else:
                raise AssertionError("diagnostic container never started")
            attached.cancel()
            try:
                await attached
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("attachment ended before cancellation")
            # Killing/reaping Docker's attached CLI is explicitly NOT cleanup.
            assert cli.json("container", "inspect", container)[0]["State"]["Running"]
            remove_native_container(cli, container)
            assert not cli.call("container", "ls", "--all", "--quiet", f"--filter=id={container}").strip()
            container = None
        finally:
            if attached is not None and not attached.done():
                attached.cancel()
                try:
                    await attached
                except asyncio.CancelledError:
                    pass
            if container is not None:
                remove_native_container(cli, container)
            os.close(descriptor)


if __name__ == "__main__":
    if sys.argv[1:] == ["--host-namespace-probe"]:
        _host_namespace_probe()
    elif sys.argv[1:] == ["--slurm-authority-probe"]:
        _slurm_authority_probe()
    else:
        unittest.main()
