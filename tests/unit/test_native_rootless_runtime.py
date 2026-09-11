"""The fixed rootless boundary accepts identity data, never commands or secrets."""

import hashlib
import json
import os
import socket
import subprocess
import sys
from importlib import import_module
from pathlib import Path

import pytest

from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_native_execution_permit import execution_request
from tests.unit.test_native_sandbox_consumer import bound_context
from tests.unit.test_personal_dev_builder import _registration


def spec_file(tmp_path):
    module = import_module("loom_capacity_executor.native_rootless_runtime")
    workspace = tmp_path / "work"
    workspace.mkdir(mode=0o700)
    claim = execution_request().claim
    context = bound_context(_registration(), "oldlab").model_copy(update={
        "claim_digest": canonical_digest(claim), "request_id": claim.request_id})
    spec = module.NativeRootlessSpecV1(claim=claim, context=context,
        runsc="/runtime/runsc", state_root=str(workspace / "runsc"),
        bundle_root=str(workspace / "bundles"), workspace=str(workspace),
        max_artifact_bytes=1024**2, max_image_archive_bytes=256 * 1024)
    wire = canonical_bytes(spec)
    path = workspace / "runtime-spec.json"
    path.write_bytes(wire)
    path.chmod(0o400)
    return module, spec, path, hashlib.sha256(wire).hexdigest()


@pytest.mark.parametrize("boundary", ["exact", "digest", "writable", "symlink", "hardlink", "oversize",
    "noncanonical", "claim", "public-workspace", "state-overlap", "command", "limit"])
def test_spec_requires_canonical_private_bound_identity(tmp_path, boundary):
    module, spec, path, digest = spec_file(tmp_path)
    if boundary == "digest":
        digest = "f" * 64
    elif boundary == "writable":
        path.chmod(0o600)
    elif boundary == "symlink":
        target = path.with_name("target")
        path.rename(target)
        path.symlink_to(target)
    elif boundary == "hardlink":
        os.link(path, path.with_name("alias"))
    elif boundary == "public-workspace":
        path.parent.chmod(0o755)
    elif boundary in {"oversize", "noncanonical", "claim", "state-overlap", "command", "limit"}:
        document = json.loads(path.read_bytes())
        if boundary == "claim":
            document["context"]["claim_digest"] = "f" * 64
        elif boundary == "state-overlap":
            document["state_root"] = str(path.parent / "rootlesskit/runsc")
        elif boundary == "command":
            document["command"] = ["/bin/sh"]
        elif boundary == "limit":
            document["max_artifact_bytes"] = True
        wire = b"x" * 65537 if boundary == "oversize" else json.dumps(document,
            sort_keys=True, separators=(",", ":") if boundary != "noncanonical" else None).encode()
        path.chmod(0o600)
        path.write_bytes(wire)
        path.chmod(0o400)
        digest = hashlib.sha256(wire).hexdigest()
    if boundary == "exact":
        assert module.read_native_rootless_spec(path, expected_sha256=digest) == spec
    else:
        with pytest.raises((ValueError, OSError)):
            module.read_native_rootless_spec(path, expected_sha256=digest)


@pytest.mark.parametrize("boundary", ["exact", "reused-state", "wrong-socket", "same-fd"])
def test_launcher_fixed_exec_and_collision_safe_socket_passage(tmp_path, boundary):
    _module, _spec, path, digest = spec_file(tmp_path)
    if boundary == "reused-state":
        (path.parent / "rootlesskit").mkdir(mode=0o700)
        (path.parent / "rootlesskit/keep").write_text("not ours")
    authority, authority_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, artifact_peer = socket.socketpair(socket.AF_UNIX,
        socket.SOCK_SEQPACKET if boundary == "wrong-socket" else socket.SOCK_STREAM)
    try:
        artifact_fd = authority.fileno() if boundary == "same-fd" else artifact.fileno()
        # Descriptor closing/remapping is tested in a real dedicated child, never
        # against the pytest runner's descriptors. exec alone is intercepted.
        script = """
import json, os, socket, sys
from pathlib import Path
from loom_capacity_executor import native_rootless_runtime as m
def observed(executable, argv, environment):
    a, b = socket.socket(fileno=3), socket.socket(fileno=4)
    a.send(b'authority'); b.send(b'artifact')
    print(json.dumps({'executable': executable, 'argv': argv, 'env': environment,
        'pid': os.getpid(), 'fds': [fd for fd in range(3, 128) if valid(fd)]}), flush=True)
    raise SystemExit(0)
def valid(fd):
    try: os.fstat(fd); return True
    except OSError: return False
m.os.execve = observed
m.exec_native_rootless_runtime(Path(sys.argv[1]), expected_sha256=sys.argv[2],
    expected_parent_pid=int(sys.argv[3]), authority_fd=int(sys.argv[4]), artifact_fd=int(sys.argv[5]))
"""
        result = subprocess.run([sys.executable, "-c", script, str(path), digest, str(os.getpid()),
            str(authority.fileno()), str(artifact_fd)], pass_fds=(authority.fileno(), artifact.fileno()),
            capture_output=True, text=True, timeout=10, check=False,
            env={**os.environ, "UNTRUSTED_SECRET": "must-not-inherit", "LISTEN_FDS": "99"})
        if boundary == "exact":
            assert result.returncode == 0, result.stderr
            observed = json.loads(result.stdout)
            assert observed["executable"] == "/usr/bin/rootlesskit"
            args = observed["argv"]
            assert args[:4] == ["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
                f"--state-dir={path.parent / 'rootlesskit'}"]
            assert args[5:9] == ["-I", "-m", "loom_capacity_executor.native_rootless_runtime", "mapped"]
            assert observed["fds"] == [3, 4]
            assert observed["env"] == {"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C.UTF-8", "LISTEN_PID": str(observed["pid"]), "LISTEN_FDS": "2"}
            assert authority_peer.recv(32) == b"authority"
            assert artifact_peer.recv(32) == b"artifact"
            assert (path.parent / "rootlesskit").stat().st_mode & 0o777 == 0o700
        else:
            assert result.returncode != 0
            assert not result.stdout
            if boundary == "reused-state":
                assert (path.parent / "rootlesskit/keep").read_text() == "not ours"
            else:
                assert not (path.parent / "rootlesskit").exists()
    finally:
        for channel in (authority, authority_peer, artifact, artifact_peer):
            channel.close()
