"""The fixed rootless boundary accepts identity data, never commands or secrets."""

import hashlib
import json
import os
import socket
import subprocess
import sys
from importlib import import_module

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


@pytest.mark.parametrize("boundary", ["exact", "swapped-low-fds", "reused-state", "wrong-socket", "same-fd"])
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
authority, artifact = int(sys.argv[4]), int(sys.argv[5])
if sys.argv[6] == 'swapped-low-fds':
    import fcntl
    a = fcntl.fcntl(authority, fcntl.F_DUPFD_CLOEXEC, 10)
    b = fcntl.fcntl(artifact, fcntl.F_DUPFD_CLOEXEC, 10)
    os.dup2(a, 4); os.dup2(b, 3)
    authority, artifact = 4, 3
extra = os.open(sys.argv[1], os.O_RDONLY)
os.dup2(extra, 97, inheritable=True)
m.exec_native_rootless_runtime(Path(sys.argv[1]), expected_sha256=sys.argv[2],
    expected_parent_pid=int(sys.argv[3]), authority_fd=authority, artifact_fd=artifact)
"""
        result = subprocess.run([sys.executable, "-c", script, str(path), digest, str(os.getpid()),
            str(authority.fileno()), str(artifact_fd), boundary], pass_fds=(authority.fileno(), artifact.fileno()),
            capture_output=True, text=True, timeout=10, check=False,
            env={**os.environ, "UNTRUSTED_SECRET": "must-not-inherit", "LISTEN_FDS": "99"})
        if boundary in {"exact", "swapped-low-fds"}:
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


@pytest.mark.parametrize("boundary", ["exact", "wrong-type", "unconnected"])
def test_mapped_activation_validates_real_descriptors_and_disables_inheritance(tmp_path, boundary):
    spec_file(tmp_path)
    authority, authority_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, artifact_peer = socket.socketpair(socket.AF_UNIX,
        socket.SOCK_SEQPACKET if boundary == "wrong-type" else socket.SOCK_STREAM)
    if boundary == "unconnected":
        artifact.close()
        artifact = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    script = """
import fcntl, os, socket, sys
from loom_capacity_executor.native_rootless_runtime import _activation_channels
copies = [fcntl.fcntl(int(fd), fcntl.F_DUPFD_CLOEXEC, 10) for fd in sys.argv[1:]]
for source, destination in zip(copies, (3, 4)): os.dup2(source, destination, inheritable=True)
with_channels = _activation_channels()
for channel, message in zip(with_channels, (b'authority', b'artifact')):
    assert not channel.get_inheritable()
    channel.send(message)
    channel.close()
for fd in (3, 4):
    try: os.fstat(fd)
    except OSError: pass
    else: raise AssertionError('activation original leaked')
"""
    try:
        result = subprocess.run([sys.executable, "-c", script, str(authority.fileno()), str(artifact.fileno())],
            pass_fds=(authority.fileno(), artifact.fileno()), capture_output=True, timeout=10, check=False)
        if boundary == "exact":
            assert result.returncode == 0, result.stderr
            assert authority_peer.recv(32) == b"authority"
            assert artifact_peer.recv(32) == b"artifact"
        else:
            assert result.returncode != 0
    finally:
        for channel in (authority, authority_peer, artifact, artifact_peer):
            channel.close()


@pytest.mark.parametrize("boundary", ["success", "expired", "uncertain", "activation", "parent",
    "session-error", "send-error", "unreaped-artifact"])
def test_mapped_entry_validates_chain_before_session_and_exports_only_verified_artifact(tmp_path, monkeypatch, boundary):
    from loom.personal_dev_builder_artifact import VerifiedPersonalDevBuildArtifact
    from loom_capacity_agent.build_admission import BuildArtifactV1
    from loom_capacity_executor.native_build_session import NativeBuildSessionResult
    from loom_capacity_executor.native_runtime_cleanup import NativeRuntimeCleanupResult
    from loom_capacity_executor.native_supervisor import NativeSupervisionResult

    module, spec, path, digest = spec_file(tmp_path)
    artifact_value = BuildArtifactV1(archive_sha256="e" * 64, archive_size_bytes=123)
    verified = VerifiedPersonalDevBuildArtifact(platform=spec.context.platform, manifest_sha256="e" * 64, images={})
    events = []
    authority, authority_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, artifact_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)

    def bind(state, *, expected_rootless_pid):
        assert state == path.parent / "rootlesskit" and expected_rootless_pid == 123
        events.append("parent")
        if boundary == "parent":
            raise RuntimeError("adopted")
        return 321

    def session(**kwargs):
        assert events == ["parent"]
        assert kwargs["claim"] == spec.claim and kwargs["context"] == spec.context
        assert kwargs["authority"] is authority and kwargs["expected_parent_pid"] == 321
        events.append("session")
        if boundary == "session-error":
            raise RuntimeError("session failed")
        return NativeBuildSessionResult(NativeSupervisionResult(boundary != "expired", "test", boundary != "unreaped-artifact"),
            NativeRuntimeCleanupResult(boundary != "uncertain", "test"),
            verified if boundary in {"success", "send-error", "unreaped-artifact"} else None)

    async def send(channel, **kwargs):
        assert events == ["parent", "session"]
        assert channel is artifact and kwargs["archive"] == path.parent / "output/build/artifacts.tar"
        assert kwargs["claim_digest"] == spec.context.claim_digest
        assert kwargs["source_binding_sha256"] == spec.context.source_binding_sha256
        events.append("send")
        if boundary == "send-error":
            raise RuntimeError("send failed")
        return artifact_value

    monkeypatch.setenv("LISTEN_PID", str(os.getpid() + (boundary == "activation")))
    monkeypatch.setenv("LISTEN_FDS", "2")
    monkeypatch.setattr(module, "bind_native_rootless_parent", bind)
    monkeypatch.setattr(module, "_activation_channels", lambda: (authority, artifact))
    monkeypatch.setattr(module, "execute_native_build_session", session)
    monkeypatch.setattr(module, "send_native_artifact", send)
    try:
        if boundary in {"activation", "parent"}:
            with pytest.raises((ValueError, RuntimeError)):
                module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert "session" not in events
        elif boundary in {"session-error", "send-error", "unreaped-artifact"}:
            with pytest.raises((ValueError, RuntimeError)):
                module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert events == ["parent", "session"] + (["send"] if boundary == "send-error" else [])
            assert authority.fileno() == artifact.fileno() == -1
        else:
            result = module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert result.claim_digest == spec.context.claim_digest
            assert result.source_binding_sha256 == spec.context.source_binding_sha256
            assert result.artifact == (artifact_value if boundary == "success" else None)
            assert events == ["parent", "session"] + (["send"] if boundary == "success" else [])
            assert authority.fileno() == artifact.fileno() == -1
    finally:
        for channel in (authority, authority_peer, artifact, artifact_peer):
            channel.close()
