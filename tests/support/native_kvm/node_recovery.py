"""Exercise actual helper IO against container-private kernel/filesystem scope.

The SSH case uses the actual forced command and sudo provenance. Other cases
simulate sudo identity to isolate node guards. No host cgroup or writable host
mount is exposed; this is not installed Slurm acceptance.
"""

import os
import pwd
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from loom_capacity_agent.native_recovery_publication import NativeRecoveryReceiptV1
from loom_capacity_build_guard.native_recovery_sender import (
    NativeNodeRecoveryRequestV1,
    NativeNodeRecoveryResultV1,
)
from loom_capacity_executor.native_mapped_scratch import _mount_id
from loom_capacity_executor.native_node_recovery import run_native_recovery_helper
from loom_capacity_executor.native_node_recovery_policy import (
    NativeNodeRecoveryPolicyV1,
    NativeNodeRecoveryScopeV1,
)
from loom_capacity_executor.native_recovery_observation import read_native_recovery_boot_id
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest


def ssh_recover(request, policy_path, policy_digest):
    from loom_capacity_executor.native_recovery_installation import (
        NativeRecoveryEndpointV1,
        render_native_recovery_endpoint,
    )

    root = Path("/opt/native-recovery-fixture")
    if root.exists():
        raise AssertionError("SSH fixture reused")
    root.mkdir(mode=0o755)
    host_key, client_key = root / "host_key", root / "client_key"
    for key in (host_key, client_key):
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True, timeout=5)
    keys = root / "authorized_keys"
    keys.write_bytes(client_key.with_suffix(".pub").read_bytes())
    keys.chmod(0o444)
    assets = render_native_recovery_endpoint(NativeRecoveryEndpointV1(address="127.0.0.1", port=22222,
        release_root=str(root), python="/usr/local/bin/python3", policy=str(policy_path), policy_sha256=policy_digest,
        host_key=str(host_key), authorized_keys=str(keys)))
    for name, data, mode in (("helper", assets.helper, 0o555), ("ssh-entry", assets.ssh_entry, 0o555),
        ("sshd_config", assets.sshd_config, 0o444)):
        (root / name).write_bytes(data)
        (root / name).chmod(mode)
    sudoers = Path("/etc/sudoers.d/native-recovery-fixture")
    sudoers.write_bytes(assets.sudoers)
    sudoers.chmod(0o440)
    subprocess.run(["visudo", "-cf", str(sudoers)], check=True, timeout=5, capture_output=True)
    # Fail closed host-key checking even for this private loopback-only daemon.
    known_hosts = root / "known_hosts"
    known_hosts.write_bytes(b"[127.0.0.1]:22222 " + host_key.with_suffix(".pub").read_bytes())
    # /run is a new container-private tmpfs, hiding image-build directories.
    Path("/run/sshd").mkdir(mode=0o755)
    Path("/run/loom-native-recovery").mkdir(mode=0o755)
    argv = ["ssh", "-F", "/dev/null", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "UserKnownHostsFile=" + str(known_hosts), "-o", "GlobalKnownHostsFile=/dev/null", "-o", "IdentitiesOnly=yes",
        "-i", str(client_key), "-p", "22222", "loom-native-recovery@127.0.0.1"]
    error_log = (root / "sshd-error").open("wb")
    daemon = subprocess.Popen(["/usr/sbin/sshd", "-D", "-e", "-f", str(root / "sshd_config")], stderr=error_log)
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                with socket.create_connection(("127.0.0.1", 22222), timeout=0.2):
                    break
            except OSError:
                if daemon.poll() is not None or time.monotonic() >= deadline:
                    raise AssertionError("disposable sshd startup failed: " +
                        (root / "sshd-error").read_text()[-2000:]) from None
                time.sleep(0.05)
        rejected = subprocess.run([*argv, "id"], input=b"", capture_output=True, timeout=5)
        assert rejected.returncode == 64 and rejected.stdout == b"", rejected.stderr.decode()
        rejected = subprocess.run(["runuser", "-u", "loom-native-recovery", "--", "sudo", "-n", "--",
            str(root / "helper"), "extra"], capture_output=True, timeout=5)
        assert rejected.returncode == 1 and rejected.stdout == b"", rejected.stderr.decode()
        rejected = subprocess.run(["runuser", "-u", "loom-native-recovery", "--", "sudo", "-n", "--",
            "/usr/bin/id"], capture_output=True, timeout=5)
        assert rejected.returncode == 1 and rejected.stdout == b"", rejected.stderr.decode()
        rejected = subprocess.run([*argv[:-1], "-N", "-o", "ExitOnForwardFailure=yes", "-R",
            "127.0.0.1:0:127.0.0.1:22222", argv[-1]], capture_output=True, timeout=5)
        assert rejected.returncode == 255 and b"remote port forwarding failed" in rejected.stderr
        result = subprocess.run(argv, input=canonical_bytes(request), capture_output=True, timeout=30)
        assert result.returncode == 0, (result.stderr.decode(), (root / "sshd-error").read_text())
        parsed = NativeNodeRecoveryResultV1.model_validate_json(result.stdout)
        assert canonical_bytes(parsed) + b"\n" == result.stdout
        return parsed
    finally:
        daemon.terminate()
        daemon.wait(timeout=5)
        error_log.close()


def main():
    incoming = NativeNodeRecoveryRequestV1.model_validate_json(sys.stdin.buffer.read(128 * 1024 + 1))
    mode = os.environ["FIXTURE_MODE"]
    history = incoming.history
    prepared = history.preparation.request.record
    final = history.finalization.request.record
    root = Path("/sys/fs/cgroup")
    subprocess.run(["mount", "-t", "cgroup2", "none", str(root)], check=True, timeout=5)
    job = root / prepared.cgroup_path.lstrip("/")
    job.mkdir(parents=True)
    child = None
    step = None
    try:
        scratch, quarantine = Path("/run/scratch"), Path("/run/quarantine")
        scratch.mkdir(mode=0o700)
        os.chown(scratch, prepared.original_uid, prepared.original_gid)
        quarantine.mkdir(mode=0o700)
        attempt = scratch / ("attempt-" + str(history.preparation.request.claim.operation_id))
        attempt.mkdir(mode=0o700)
        os.chown(attempt, prepared.original_uid, prepared.original_gid)
        locator = prepared.locator.model_copy(update={"directory": str(attempt),
            "device": attempt.stat().st_dev, "inode": attempt.stat().st_ino})
        namespace = os.stat("/proc/self/ns/cgroup")
        host = history.host.model_copy(update={"boot_id": read_native_recovery_boot_id(),
            "cgroup_namespace_device": namespace.st_dev, "cgroup_namespace_inode": namespace.st_ino})
        if mode == "boot":
            host = host.model_copy(update={"boot_id": uuid4()})
        job_fd = os.open(job, os.O_RDONLY | os.O_DIRECTORY)
        scratch_fd = os.open(scratch, os.O_RDONLY | os.O_DIRECTORY)
        try:
            prepared = prepared.model_copy(update={"locator": locator, "boot_id": host.boot_id,
                "node_configuration_sha256": canonical_digest(host), "cgroup_device": job.stat().st_dev,
                "cgroup_inode": job.stat().st_ino + int(mode == "inode"), "cgroup_mount_id": _mount_id(job_fd)})
            scope = NativeNodeRecoveryScopeV1(installation_id=history.profile.installation_id,
                pool_id=history.profile.pool_id, profile_sha256=canonical_digest(history.profile), host=host,
                scratch_root=str(scratch), quarantine_root=str(quarantine), scratch_device=locator.device,
                scratch_mount_id=_mount_id(scratch_fd), filesystem="tmpfs", uid_map=final.uid_map, gid_map=final.gid_map)
        finally:
            os.close(job_fd)
            os.close(scratch_fd)
        prepared_request = history.preparation.request.model_copy(update={"record": prepared})
        finalized_request = history.finalization.request.model_copy(update={"record": final.model_copy(update={"preparation": prepared})})
        history = history.model_copy(update={"host": host,
            "preparation": NativeRecoveryReceiptV1(request=prepared_request, request_digest=canonical_digest(prepared_request)),
            "finalization": NativeRecoveryReceiptV1(request=finalized_request, request_digest=canonical_digest(finalized_request))})
        request = NativeNodeRecoveryRequestV1(invocation_id=uuid4(), history=history)
        locator_path = attempt / "recovery.json"
        locator_path.write_bytes(canonical_bytes(locator))
        locator_path.chmod(0o400)
        os.chown(locator_path, prepared.original_uid, prepared.original_gid)
        payload = attempt / "payload"
        payload.write_bytes(b"subordinate-retained-unless-authorized")
        os.chown(payload, final.uid_map[1].outside, final.gid_map[1].outside)
        payload.chmod(0o000)
        policy = NativeNodeRecoveryPolicyV1(management_uid=25000, scopes=(scope,))
        policy_path = Path("/run/policy.json")
        policy_path.write_bytes(canonical_bytes(policy))
        policy_path.chmod(0o444)
        try:
            assert pwd.getpwnam("loom-native-recovery").pw_uid == 25000
        except KeyError:
            with Path("/etc/passwd").open("a") as accounts:
                accounts.write("loom-native-recovery:x:25000:25000::/nonexistent:/bin/sh\n")
        os.environ["SUDO_USER"], os.environ["SUDO_UID"] = "loom-native-recovery", "25000"
        sys.argv = ["/fixed-node-helper"]
        if mode == "locator":
            locator_path.chmod(0o600)
        elif mode == "delegated":
            os.chown(job / "cgroup.procs", prepared.original_uid, prepared.original_gid)
        elif mode == "populated":
            step = job / "step_batch"
            step.mkdir()
            child = subprocess.Popen(["/bin/sleep", "60"])
            (step / "cgroup.procs").write_text(str(child.pid))
            assert (job / "cgroup.procs").read_text() == ""
            assert "populated 1" in (job / "cgroup.events").read_text()
        elif mode == "missing":
            job.rmdir()
        for _iteration in range(1 if mode == "ssh" else 2):
            request_path = Path("/run/request.json")
            request_path.write_bytes(canonical_bytes(request))
            if mode == "ssh":
                result = ssh_recover(request, policy_path, canonical_digest(policy))
            else:
                with request_path.open("rb") as stream:
                    sys.stdin = stream
                    result = run_native_recovery_helper(policy_path=str(policy_path), policy_sha256=canonical_digest(policy))
            assert result.request_sha256 == canonical_digest(request)
            assert (result.state == "completed") == (mode in {"exact", "ssh"}), result
            if mode in {"exact", "ssh"}:
                assert not attempt.exists()
                if scratch.exists():
                    scratch.rmdir()  # Retained journal replay survives owner teardown.
            else:
                assert payload.read_bytes() == b"subordinate-retained-unless-authorized"
            request = request.model_copy(update={"invocation_id": uuid4()})
        print("native-node-recovery-" + mode + "-verified", flush=True)
    finally:
        if child is not None:
            child.terminate()
            child.wait(timeout=5)
        if step is not None:
            step.rmdir()
        if job.exists():
            job.rmdir()
        for ancestor in job.parents:
            if ancestor == root:
                break
            ancestor.rmdir()
        subprocess.run(["umount", str(root)], check=True, timeout=5)


if __name__ == "__main__":
    main()
