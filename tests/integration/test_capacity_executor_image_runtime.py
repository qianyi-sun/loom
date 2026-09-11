"""Run with stdlib unittest inside the built executor image, on both architectures."""

from __future__ import annotations

import json
import os
import socket
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
    else:
        unittest.main()
