"""Real native sandbox installation and fresh-verifier handoff; no network/model."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.sandbox_identity import ROOT_INSTALL_CAPABILITIES
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


@pytest.fixture(scope="module")
def native_binary(tmp_path_factory):
    directory = tmp_path_factory.mktemp("identity-runtime")
    repository = Path(__file__).resolve().parents[2]
    subprocess.run([
        "docker", "run", "--rm", "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{repository}:/src:ro", "-v", f"{directory}:/output", "-w", "/src",
        "-e", "GOCACHE=/tmp/go-cache", "-e", "CGO_ENABLED=0", "golang:1.26-alpine3.23",
        "go", "build", "-o", "/output/loom-sandbox-runtime", "./cmd/loom-sandbox-runtime",
    ], check=True, timeout=120, capture_output=True)
    return directory / "loom-sandbox-runtime"


@pytest.fixture
async def sandboxes(native_binary, tmp_path):
    import docker

    client = docker.from_env()
    containers = []
    drivers = []
    try:
        for name, uid, gid, home in (("agent", 0, 0, "/root"), ("verifier", 0, 0, "/root"),
                                     ("default", 65532, 65532, "/home/agent")):
            directory = tmp_path / name
            directory.mkdir()
            # Model the setgid socket emptyDir with the controller's fsGroup.
            directory.chmod(0o2777)
            container = client.containers.run(
                "python:3.11-slim", ["--socket", "/socket/sandbox.sock"], detach=True,
                entrypoint="/loom/bin/loom-sandbox-runtime", user=f"{uid}:{gid}",
                network_mode="none", cap_drop=["ALL"],
                cap_add=list(ROOT_INSTALL_CAPABILITIES) if uid == 0 else [],
                security_opt=["no-new-privileges"], environment={"HOME": home},
                volumes={str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
                         str(directory): {"bind": "/socket", "mode": "rw"}},
            )
            containers.append(container)
            driver = ServiceSandboxDriver(
                directory / "sandbox.sock",
                capabilities=Capabilities(os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
                                          dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"})),
                network_policy=NoNetwork(),
            )
            for attempt in range(100):
                try:
                    await driver.start()
                    break
                except (OSError, RuntimeError):
                    if attempt == 99:
                        raise
                    await asyncio.sleep(0.05)
            drivers.append(driver)
        yield drivers
    finally:
        for driver in drivers:
            await driver.stop()
        for container in containers:
            container.remove(force=True)
        client.close()


async def test_root_installs_real_deb_and_fresh_verifier_observes_owned_system_state(sandboxes, tmp_path):
    agent, verifier, default = sandboxes
    for driver in (agent, verifier):
        result = await driver.exec("mkdir -p /app && test ! -e /usr/local/share/loom-identity-proof && ! dpkg-query -W loom-identity-proof")
        assert result.return_code == 0, result.stderr
    default_result = await default.exec("id -u; id -g; printf '%s\\n' \"$HOME\"; touch /usr/local/forbidden")
    assert default_result.return_code != 0
    assert default_result.stdout.splitlines() == [b"65532", b"65532", b"/home/agent"]
    setup = r"""set -eu
test "$(id -u)" = 0
test "$HOME" = /root
mkdir -p /tmp/package/DEBIAN /tmp/package/usr/local/share/loom-identity-proof
printf '%s\n' installed-by-agent > /tmp/package/usr/local/share/loom-identity-proof/installed
cat > /tmp/package/DEBIAN/control <<'CONTROL'
Package: loom-identity-proof
Version: 1.0
Architecture: all
Maintainer: Loom test fixture
Description: Local task installation proof
CONTROL
cat > /tmp/package/DEBIAN/postinst <<'SCRIPT'
#!/bin/sh
set -eu
chown -R 1001:1002 /usr/local/share/loom-identity-proof
setpriv --reuid 1001 --regid 1002 --clear-groups /bin/sh -c '
id -u > /usr/local/share/loom-identity-proof/dropped-uid
sleep 3600 >/dev/null 2>&1 &
echo $! > /usr/local/share/loom-identity-proof/child-pid
'
SCRIPT
chmod 0755 /tmp/package/DEBIAN/postinst
dpkg-deb --root-owner-group --build /tmp/package /tmp/loom-identity-proof.deb
dpkg --install /tmp/loom-identity-proof.deb
"""
    installed = await agent.exec(setup)
    assert installed.return_code == 0, installed.stderr
    await agent.stop_processes()
    checked = await agent.exec("test ! -e /proc/$(cat /usr/local/share/loom-identity-proof/child-pid)")
    assert checked.return_code == 0, checked.stderr
    paths = (PurePosixPath("/usr/local/share/loom-identity-proof"), PurePosixPath("/var/lib/dpkg"))
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    checked = await verifier.exec(
        "set -eu; test \"$(dpkg-query -W -f='${Status}' loom-identity-proof)\" = 'install ok installed'; "
        "test \"$(cat /usr/local/share/loom-identity-proof/dropped-uid)\" = 1001; "
        "test \"$(stat -c '%u:%g' /usr/local/share/loom-identity-proof/installed)\" = 1001:1002; "
        "test \"$(cat /usr/local/share/loom-identity-proof/installed)\" = installed-by-agent",
    )
    assert checked.return_code == 0, checked.stderr
    unrelated = await default.exec("test ! -e /usr/local/share/loom-identity-proof && ! dpkg-query -W loom-identity-proof")
    assert unrelated.return_code == 0, unrelated.stderr
    await verifier.stop_processes()


async def test_handoff_preserves_numeric_ownership_when_account_names_differ(sandboxes, tmp_path):
    agent, verifier, _ = sandboxes
    for driver, uid in ((agent, 1201), (verifier, 1301)):
        result = await driver.exec(
            f"groupadd -g {uid} ownercheck && useradd -u {uid} -g {uid} ownercheck && "
            "mkdir -p /app /data"
        )
        assert result.return_code == 0, result.stderr
    result = await agent.exec("echo owned > /data/marker && chown 1201:1201 /data/marker")
    assert result.return_code == 0, result.stderr
    paths = (PurePosixPath("/data"),)
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    result = await verifier.exec("stat -c '%u:%g' /data/marker")
    assert result.return_code == 0, result.stderr
    assert result.stdout.strip() == b"1201:1201"


async def test_private_grading_inputs_do_not_change_the_tasks_file_manifest(sandboxes, tmp_path, monkeypatch):
    from loom.nebius_terminus_ingest import offline_verifier_run_sh_bytes
    from loom.service_execution_sandbox_task import run_verifier
    from loom.trial.workspace_snapshot import _export_workspace_archive
    from tests.unit.test_service_execution_terminus_plan import _inputs

    agent, verifier, _ = sandboxes
    task, trial, _ = _inputs()
    task.verifier.args["script_path"] = "verifier/run.sh"
    controller = tmp_path / "controller"
    (controller / ".loom").mkdir(parents=True)
    (controller / "tests").mkdir()
    (controller / "tests/private-marker").write_text("private grading input")
    (controller / "verifier").mkdir()
    (controller / "verifier/run.sh").write_bytes(offline_verifier_run_sh_bytes())
    (controller / "verifier/harbor-offline.sh").write_text("""set -eu
test "$(cat /tests/private-marker)" = 'private grading input'
test "$PWD" = /app
python - <<'PY'
from pathlib import Path
root = Path('/app')
expected = set((root / 'manifest').read_text().splitlines())
actual = {str(p) for p in root.rglob('*') if p.is_file()}
assert actual == expected, (actual, expected)
assert (root / 'answer').read_text() == 'answer'
PY
echo 1 > /logs/verifier/reward.txt
echo '{}' > /logs/verifier/ctrf.json
""")
    created = await agent.exec(
        "mkdir /app; printf answer > /app/answer; "
        "printf '/app/answer\\n/app/manifest\\n' > /app/manifest"
    )
    assert created.return_code == 0, created.stderr
    await agent.stop_processes()
    await _export_workspace_archive(agent, PurePosixPath("/app"), controller / ".loom/workspace.tar")
    # The fixture checks readiness; the production entrypoint owns its connection.
    await verifier.stop()
    connection = ServiceSandboxDriver(tmp_path / "verifier/sandbox.sock",
                                      capabilities=verifier.capabilities, network_policy=NoNetwork())
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver", lambda *_: connection)
    await run_verifier(controller, task, trial)
    result = json.loads((controller / ".loom/verifier/output.json").read_bytes())
    assert result["rewards"] == {"resolved": 1, "passed": 1}
