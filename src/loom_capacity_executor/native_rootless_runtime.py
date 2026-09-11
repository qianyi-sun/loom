"""Fixed credential-free RootlessKit exec boundary for preverified native material.

The outer allocated IO process owns the unreaped launcher and existing one-shot
fence, sealed inputs, authenticated channels, upload and outcome. This executable
does not install material or prove Slurm containment/physical capacity release.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import os
import socket
import stat
import sys
from pathlib import Path
from typing import NoReturn, Self

from pydantic import Field, model_validator

from loom_capacity_agent.build_admission import (
    BuildArtifactV1,
    BuildClaimRequestV1,
    BuildSourceContextV1,
)
from loom_capacity_executor.native_artifact_transfer import send_native_artifact
from loom_capacity_executor.native_build_session import execute_native_build_session
from loom_capacity_executor.native_parent_death import bind_native_parent_death
from loom_capacity_executor.native_rootless_parent import bind_native_rootless_parent
from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_sandbox_contract import render_native_sandbox_contract
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes, canonical_digest

_MAX_SPEC = 64 * 1024


def _path(value: str) -> Path:
    path = Path(value)
    if (not path.is_absolute() or path == Path("/") or str(path) != value
        or ".." in path.parts or any(c in value for c in ("\x00", "\n", "\r"))):
        raise ValueError("native rootless path is invalid")
    return path


class NativeRootlessSpecV1(StrictV1Model):
    claim: BuildClaimRequestV1
    context: BuildSourceContextV1
    runsc: str
    state_root: str
    bundle_root: str
    workspace: str
    max_artifact_bytes: int = Field(gt=0)
    max_image_archive_bytes: int = Field(gt=0)

    @model_validator(mode="after")
    def _identity(self) -> Self:
        paths = [_path(value) for value in (self.runsc, self.state_root, self.bundle_root, self.workspace)]
        rootless_state = paths[-1] / "rootlesskit"
        # RootlessKit recursively removes its state. It must not contain or be
        # contained by any runtime/material path; only workspace may contain it.
        for path in paths[:-1]:
            if path == rootless_state or path in rootless_state.parents or rootless_state in path.parents:
                raise ValueError("native rootless state overlaps runtime material")
        self.layout()
        if (self.context.claim_digest != canonical_digest(self.claim)
            or self.context.request_id != self.claim.request_id
            or self.claim.binding.pool_id != ("gb10" if self.context.platform == "linux/arm64" else "oldlab")):
            raise ValueError("native rootless specification identity changed")
        render_native_sandbox_contract(self.context, max_artifact_bytes=self.max_artifact_bytes,
            max_image_archive_bytes=self.max_image_archive_bytes)
        return self

    def layout(self) -> NativeRunscLayout:
        return NativeRunscLayout(Path(self.runsc), Path(self.state_root), Path(self.bundle_root), self.context.claim_digest)


class NativeRootlessResultV1(StrictV1Model):
    """Bounded local observations, not artifact publication or release authority."""

    claim_digest: Digest
    source_binding_sha256: Digest
    client_succeeded: bool
    broker_reaped: bool
    cleanup_confirmed: bool
    artifact: BuildArtifactV1 | None


def read_native_rootless_spec(path: Path, *, expected_sha256: str) -> NativeRootlessSpecV1:
    _path(str(path))
    if len(expected_sha256) != 64 or any(c not in "0123456789abcdef" for c in expected_sha256):
        raise ValueError("native rootless specification digest is invalid")
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(directory)
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("native rootless workspace must be private and owner-controlled")
        descriptor = os.open(path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            metadata = os.fstat(descriptor)
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != 0o400 or metadata.st_nlink != 1
                or not 1 <= metadata.st_size <= _MAX_SPEC):
                raise ValueError("native rootless specification must be a bounded private owner file")
            wire = os.pread(descriptor, _MAX_SPEC + 1, 0)
            if len(wire) != metadata.st_size or hashlib.sha256(wire).hexdigest() != expected_sha256:
                raise ValueError("native rootless specification bytes changed")
        finally:
            os.close(descriptor)
    finally:
        os.close(directory)
    spec = NativeRootlessSpecV1.model_validate_json(wire)
    if canonical_bytes(spec) != wire or path != Path(spec.workspace) / "runtime-spec.json":
        raise ValueError("native rootless specification is not canonical or workspace-bound")
    return spec


def _channel(descriptor: int, kind: int) -> socket.socket:
    if type(descriptor) is not int or descriptor < 3:
        raise ValueError("native rootless channel descriptor is invalid")
    duplicate = os.dup(descriptor)
    try:
        channel = socket.socket(fileno=duplicate)
    except BaseException:
        os.close(duplicate)
        raise
    try:
        if channel.family != socket.AF_UNIX or channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != kind:
            raise ValueError("native rootless channel type changed")
        channel.getpeername()  # Require an already-connected private channel.
        channel.set_inheritable(False)
        return channel
    except BaseException:
        channel.close()
        raise


def exec_native_rootless_runtime(spec_path: Path, *, expected_sha256: str,
    expected_parent_pid: int, authority_fd: int, artifact_fd: int,
) -> NoReturn:
    """Dedicated launcher only: closes every inherited FD except stdio and 3/4."""
    bind_native_parent_death(expected_parent_pid)
    spec = read_native_rootless_spec(spec_path, expected_sha256=expected_sha256)
    if authority_fd == artifact_fd:
        raise ValueError("native rootless channels must be distinct")
    with _channel(authority_fd, socket.SOCK_SEQPACKET), _channel(artifact_fd, socket.SOCK_STREAM):
        pass
    state = Path(spec.workspace) / "rootlesskit"
    state.mkdir(mode=0o700)  # No exist_ok: RootlessKit removes only this fresh directory.
    if stat.S_IMODE(state.stat().st_mode) != 0o700:
        raise ValueError("native rootless fresh state mode changed")
    originals = (authority_fd, artifact_fd)
    copied = [fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 10) for fd in originals]
    for source, target in zip(copied, (3, 4), strict=True):
        os.dup2(source, target, inheritable=True)
    # This is a dedicated exec process, not the authenticated IO process. Do not
    # let any unexpected credential/source descriptor cross the namespace edge.
    for name in os.listdir("/proc/self/fd"):
        descriptor = int(name)
        if descriptor > 4:
            try:
                os.close(descriptor)
            except OSError:
                pass  # listdir's directory handle is already closed.
    bind_native_parent_death(expected_parent_pid)
    os.chdir("/")  # Descendant fixed Python modules must not import from caller CWD.
    args = ["/usr/bin/rootlesskit", "--net=none", "--subid-source=static", f"--state-dir={state}",
        sys.executable, "-I", "-m", "loom_capacity_executor.native_rootless_runtime", "mapped",
        "--spec", str(spec_path), "--spec-sha256", expected_sha256, "--expected-parent", str(os.getpid())]
    os.execve(args[0], args, {"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8",
        "LISTEN_FDS": "2", "LISTEN_PID": str(os.getpid())})


def _activation_channels() -> tuple[socket.socket, socket.socket]:
    authority = _channel(3, socket.SOCK_SEQPACKET)
    try:
        artifact = _channel(4, socket.SOCK_STREAM)
    except BaseException:
        authority.close()
        raise
    os.close(3)
    os.close(4)
    return authority, artifact


def run_native_mapped_runtime(spec_path: Path, *, expected_sha256: str,
    expected_rootless_pid: int,
) -> NativeRootlessResultV1:
    if os.environ.get("LISTEN_PID") != str(os.getpid()) or os.environ.get("LISTEN_FDS") != "2":
        raise ValueError("native rootless activation identity changed")
    # Parent binding precedes potentially expensive spec parsing or any broker.
    parent = bind_native_rootless_parent(spec_path.parent / "rootlesskit", expected_rootless_pid=expected_rootless_pid)
    spec = read_native_rootless_spec(spec_path, expected_sha256=expected_sha256)
    authority, artifact_channel = _activation_channels()
    with authority, artifact_channel:
        result = execute_native_build_session(claim=spec.claim, context=spec.context, layout=spec.layout(),
            workspace=Path(spec.workspace), authority=authority, expected_parent_pid=parent,
            max_artifact_bytes=spec.max_artifact_bytes, max_image_archive_bytes=spec.max_image_archive_bytes)
        artifact = None
        if result.artifact is not None:
            if not (result.supervision.client_succeeded and result.supervision.broker_reaped and result.cleanup.confirmed):
                raise ValueError("native rootless artifact lacks confirmed cleanup")
            artifact = asyncio.run(send_native_artifact(artifact_channel,
                archive=Path(spec.workspace) / "output/build/artifacts.tar",
                claim_digest=spec.context.claim_digest, source_binding_sha256=spec.context.source_binding_sha256,
                max_artifact_bytes=spec.max_artifact_bytes))
        return NativeRootlessResultV1(claim_digest=spec.context.claim_digest,
            source_binding_sha256=spec.context.source_binding_sha256,
            client_succeeded=result.supervision.client_succeeded, broker_reaped=result.supervision.broker_reaped,
            cleanup_confirmed=result.cleanup.confirmed, artifact=artifact)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("launch", "mapped"))
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--spec-sha256", required=True)
    parser.add_argument("--expected-parent", type=int, required=True)
    parser.add_argument("--authority-fd", type=int)
    parser.add_argument("--artifact-fd", type=int)
    args = parser.parse_args()
    if args.mode == "launch":
        if args.authority_fd is None or args.artifact_fd is None:
            parser.error("launch requires two private channels")
        exec_native_rootless_runtime(args.spec, expected_sha256=args.spec_sha256,
            expected_parent_pid=args.expected_parent, authority_fd=args.authority_fd, artifact_fd=args.artifact_fd)
    if args.authority_fd is not None or args.artifact_fd is not None:
        parser.error("mapped mode requires fixed activation channels")
    result = run_native_mapped_runtime(args.spec, expected_sha256=args.spec_sha256, expected_rootless_pid=args.expected_parent)
    wire = canonical_bytes(result) + b"\n"
    if len(wire) > 4096:
        raise ValueError("native rootless result exceeds bound")
    sys.stdout.buffer.write(wire)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
