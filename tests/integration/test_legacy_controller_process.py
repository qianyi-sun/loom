"""Read a real systemd-owned cgroup without touching protected services."""

import os
import subprocess
import time
from uuid import uuid4

import pytest


def test_cgroup_retirement_distinguishes_live_and_exited_systemd_processes():
    from loom_cli.rollout.operator.protected_legacy_controller_process import (
        controller_cgroup_empty,
    )

    if not os.path.exists("/sys/fs/cgroup"):
        pytest.skip("Linux cgroup filesystem is unavailable")
    manager = subprocess.run(["systemctl", "--user", "show", "--property=Version", "--value"],
        capture_output=True, timeout=10)
    if manager.returncode != 0:
        pytest.skip("disposable user systemd manager is unavailable")
    unit = "loom-test-retirement-" + uuid4().hex + ".service"
    try:
        subprocess.run(["systemd-run", "--user", "--unit=" + unit,
            "--service-type=exec", "--property=KillMode=control-group", "/bin/sleep", "60"],
            check=True, capture_output=True, timeout=15)
        deadline = time.monotonic() + 5
        while True:
            result = subprocess.run(["systemctl", "--user", "show", unit,
                "--property=ControlGroup", "--value"], check=True, capture_output=True,
                text=True, timeout=10)
            group = result.stdout.strip()
            if group:
                break
            assert time.monotonic() < deadline, "disposable cgroup never appeared"
            time.sleep(0.02)
        assert controller_cgroup_empty(group) is False
        subprocess.run(["systemctl", "--user", "stop", unit], check=True, capture_output=True, timeout=15)
        assert controller_cgroup_empty(group) is True
    finally:
        subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True, timeout=15)
        subprocess.run(["systemctl", "--user", "reset-failed", unit], capture_output=True, timeout=10)
