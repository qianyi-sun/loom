"""Run with stdlib unittest inside the built executor image, on both architectures."""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
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


def test_capacity_executor_image_build() -> None:
    """Exercise the release build, whose final layer runs the installer cases."""
    image = f"loom-capacity-executor-runtime-test:{uuid4().hex}"
    try:
        result = subprocess.run(
            ["docker", "build", "--build-arg", f"LOOM_BUILD_SHA={'1' * 40}",
             "--file", "deploy/Dockerfile.capacity-executor", "--tag", image, "."],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True, text=True, check=False, timeout=900,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        _assert_native_docker_stdin_is_not_retained(image)
    finally:
        subprocess.run(
            ["docker", "image", "rm", image],
            capture_output=True, check=False, timeout=30,
        )


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


if __name__ == "__main__":
    unittest.main()
