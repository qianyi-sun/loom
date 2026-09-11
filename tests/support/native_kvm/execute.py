"""Disposable Docker fixture only. Not an installer or an execution authority."""

import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path


def native_runtime_command():
    return ["/runtime/runsc", "--root=/tmp/runsc-state", "--platform=kvm",
        "--network=none", "--ignore-cgroups=true", "--gvisor-marker-file=true",
        "--host-settings=check", "--sidecar-release-enforcement-policy=ALWAYS",
        "--host-uds=none", "--host-fifo=none", "--directfs=false",
        "--allow-suid=false", "--oci-seccomp=true"]


def main():
    fixtures = Path("/fixtures")
    identity = json.loads((fixtures / "identity.json").read_text())
    prepare_only = sys.argv[1:] == ["prepare"]
    rootfs = Path("/tmp/native-rootfs")
    rootfs.mkdir()
    # The outer test exported this immutable, digest-selected trusted image.
    # Extraction and capability restoration occur only in this disposable,
    # network-disabled, bounded fixture -- never on a worker's filesystem.
    with tarfile.open(fixtures / "rootfs.tar") as archive:
        archive.extractall(rootfs, filter="fully_trusted")
    for name in ("input", "output", "var/run/loom-buildkit", "var/lib/loom-buildkit"):
        (rootfs / name).mkdir(parents=True, exist_ok=True)
    for name, value in {
        "newuidmap": "0100000280000000000000000000000000000000",
        "newgidmap": "0100000240000000000000000000000000000000",
    }.items():
        target = rootfs / "usr/bin" / name
        os.setxattr(target, "security.capability", bytes.fromhex(value))
        assert os.getxattr(target, "security.capability").hex() == value
    # Exercise current production Python, not the older published wrapper.
    for path in (fixtures / "client-modules").iterdir():
        shutil.copyfile(path, rootfs / "opt/loom-personal-dev-builder/loom" / path.name)
    shutil.copyfile("/test-support/client_probe.py", rootfs / "opt/client_probe.py")
    shutil.copyfile("/test-support/lifecycle_probe.py", rootfs / "opt/lifecycle_probe.py")
    workspace = Path("/tmp/native-work")
    if prepare_only:
        assert workspace.stat().st_mode & 0o777 == 0o700
        assert (workspace / "input").stat().st_mode & 0o777 == 0o555
    else:
        workspace.mkdir(mode=0o755)
        shutil.copytree(fixtures / "input", workspace / "input")
    output = workspace / "output"
    output.mkdir(mode=0o700)
    os.chown(output, 1000, 1000)
    (workspace / "buildkit-run").mkdir(mode=0o1777)
    if prepare_only:
        workspace.chmod(0o700)
        return
    sandbox_id = identity["sandbox_id"]
    buildkit_id = identity["buildkit_id"]
    client_id = identity["client_id"]
    probe_id, lifecycle_id, late_id = (role + "-" + sandbox_id for role in ("probe", "lifecycle", "late"))
    runtime = native_runtime_command()
    started = []
    logs = []
    root_launcher = None
    try:
        if identity["root_stop"].startswith("monitored"):
            from supervised import supervised_build

            # Production reconciliation owns these IDs even if it returns
            # uncertain after a partial delete. Do not put them in the fixture
            # retry list: failure cleanup is outer-container disposal only.
            expiry = identity["root_stop"].endswith("expiry")
            native_session = identity["root_stop"].startswith("monitored-rootless")
            if native_session:
                workspace.chmod(0o700)
            supervised_build(expiry=expiry, native_session=native_session)
            # Production reconciliation already deleted these exact IDs once.
            # Keep independent final empty-list verification, not duplicate writes.
            if expiry:
                pulse = output / "lifecycle-pulse"
                stopped = pulse.read_bytes()
                assert int(stopped) > 0, "deadline test never reached a live sandbox client"
                late = subprocess.run([*runtime, "run", "--bundle=/fixtures/client", late_id],
                    capture_output=True, timeout=5)
                started.append(late_id)
                assert late.returncode != 0 and pulse.read_bytes() == stopped
                print("native-supervised-expiry-stopped-live-client", flush=True)
                return
            shutil.copyfile(output / "build/artifacts.tar", "/result/artifacts.tar")
            os.chmod("/result/artifacts.tar", 0o644)
            assert list((output / "build/images").iterdir()) == []
            print("native-allocated-client-artifact-ok", flush=True)
            return  # The same exact cleanup below is still mandatory.
        for component, name in (("pause", sandbox_id), ("buildkit", buildkit_id)):
            started.append(name)
            log = open("/tmp/" + component + ".log", "w+")
            logs.append(log)
            if component == "pause":
                # Pinned runsc's attached root gives sentry/helper processes a
                # kernel parent-death signal; detached mode has no such bound.
                command = ([sys.executable, "/test-support/death_chain.py", "supervisor", str(os.getpid()), name]
                    if identity["root_stop"] == "supervisor-death"
                    else [*runtime, "run", "--bundle=/fixtures/pause", name])
                root_launcher = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                    env={"PATH": "/usr/local/bin:/usr/bin:/bin", "PYTHONPATH": "/fixtures/helper-modules"})
                deadline = time.monotonic() + 20
                while True:
                    state = subprocess.run([*runtime, "state", name], capture_output=True,
                        text=True, timeout=5)
                    if (state.returncode == 0 and json.loads(state.stdout)["status"] == "running"
                        and root_launcher.poll() is None):
                        break
                    if root_launcher.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("attached sandbox root did not become ready")
                    time.sleep(0.05)
            else:
                subprocess.run([*runtime, "run", "--detach", "--bundle=/fixtures/" + component, name],
                    stdout=log, stderr=subprocess.STDOUT, check=True, timeout=20)
        deadline = time.monotonic() + 20
        while True:
            ready = subprocess.run([*runtime, "exec", buildkit_id, "/usr/bin/test", "-S",
                "/var/run/loom-buildkit/buildkitd.sock"], capture_output=True, timeout=5)
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"BuildKit socket did not become ready: {ready.stdout!r} {ready.stderr!r}")
            time.sleep(0.1)
        subprocess.run([*runtime, "exec", buildkit_id, "/bin/touch",
            "/tmp/sidecar-private"], check=True, timeout=5)
        probe = json.loads((fixtures / "client/config.json").read_bytes())
        probe["process"]["args"] = ["/usr/bin/python3", "/opt/client_probe.py"]
        Path("/tmp/probe-bundle").mkdir()
        Path("/tmp/probe-bundle/config.json").write_text(json.dumps(probe))
        started.append(probe_id)
        subprocess.run([*runtime, "run", "--bundle=/tmp/probe-bundle", probe_id],
            check=True, timeout=15)
        started.append(client_id)
        subprocess.run([*runtime, "run", "--bundle=/fixtures/client", client_id],
            check=True, timeout=120)
        shutil.copyfile(output / "build/artifacts.tar", "/result/artifacts.tar")
        os.chmod("/result/artifacts.tar", 0o644)
        assert list((output / "build/images").iterdir()) == []
        print("native-allocated-client-artifact-ok", flush=True)
        # Establish the lifetime primitive for the future watchdog: stopping
        # the trusted pause root must stop all children, including live clients,
        # and must not admit a late in-flight child into the same sandbox.
        lifecycle = json.loads((fixtures / "client/config.json").read_bytes())
        lifecycle["process"]["args"] = ["/usr/bin/python3", "/opt/lifecycle_probe.py"]
        Path("/tmp/lifecycle-bundle").mkdir()
        Path("/tmp/lifecycle-bundle/config.json").write_text(json.dumps(lifecycle))
        started.append(lifecycle_id)
        subprocess.run([*runtime, "run", "--detach", "--bundle=/tmp/lifecycle-bundle", lifecycle_id],
            check=True, timeout=15)
        pulse = output / "lifecycle-pulse"
        deadline = time.monotonic() + 10
        while not pulse.exists() or pulse.stat().st_size == 0:
            if time.monotonic() >= deadline:
                raise RuntimeError("lifecycle child did not start")
            time.sleep(0.05)
        if identity["root_stop"] in {"launcher-death", "supervisor-death"}:
            assert root_launcher is not None
            root_launcher.kill()
            assert root_launcher.wait(timeout=5) == -9
        else:
            subprocess.run([*runtime, "kill", "--all", sandbox_id, "SIGKILL"], check=True, timeout=5)
        deadline = time.monotonic() + 10
        while True:
            # Foreground runsc may collect/remove its own root state on exit.
            # A successful full list distinguishes absence from a failed read.
            observed = json.loads(subprocess.run([*runtime, "list", "--format=json"],
                check=True, capture_output=True, text=True, timeout=5).stdout) or []
            by_id = {item["id"]: item["status"] for item in observed}
            states = [by_id.get(name, "absent") for name in (sandbox_id, buildkit_id, lifecycle_id)]
            if all(state in {"stopped", "absent"} for state in states):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"sandbox root stop left live children: {states}")
            time.sleep(0.05)
        stopped_pulse = pulse.read_bytes()
        started.append(late_id)
        late = subprocess.run([*runtime, "run", "--detach", "--bundle=/tmp/lifecycle-bundle", late_id],
            capture_output=True, timeout=5)
        assert late.returncode != 0, "stopped sandbox accepted a late child"
        assert pulse.read_bytes() == stopped_pulse
        print("native-root-stop-children-and-late-start-ok", flush=True)
    finally:
        cleanup_failures = []
        for name in reversed(started):
            result = subprocess.run([*runtime, "delete", "--force", name], timeout=15)
            if result.returncode:
                cleanup_failures.append(name)
        if root_launcher is not None:
            if root_launcher.poll() is None:
                root_launcher.kill()
            root_launcher.wait(timeout=5)
        for log in logs:
            log.seek(0)
            print(log.read()[-16000:], flush=True)
            log.close()
        assert not cleanup_failures, cleanup_failures
        remaining = subprocess.run([*runtime, "list", "--format=json"], check=True,
            capture_output=True, text=True, timeout=10)
        assert json.loads(remaining.stdout) in (None, []), remaining.stdout
        print("native-allocated-runtime-cleanup-ok", flush=True)


if __name__ == "__main__":
    main()
