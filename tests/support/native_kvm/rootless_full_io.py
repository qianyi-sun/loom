"""Disposable full-build IO split; the synthetic authority stays outside mapping."""

import asyncio
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

from loom_capacity_agent.build_admission import BuildClaimRequestV1, BuildSourceContextV1
from loom_capacity_executor.native_artifact_transfer import receive_native_artifact
from loom_capacity_executor.native_build_source import NativeStagedBuildSource
from loom_capacity_executor.native_rootless_runtime import (
    NativeRootlessResultV1,
    NativeRootlessSpecV1,
    exec_native_rootless_runtime,
)
from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_runtime_input import prepare_native_runtime_input
from loom_capacity_manager.contracts import canonical_bytes


def launch():
    exec_native_rootless_runtime(Path("/tmp/native-work/runtime-spec.json"),
        expected_sha256=sys.argv[5], expected_parent_pid=int(sys.argv[2]),
        authority_fd=int(sys.argv[3]), artifact_fd=int(sys.argv[4]))


def verify_runtime():
    """Independent mapped readback; never repeat production runtime deletions."""
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
    layout = NativeRunscLayout(Path("/runtime/runsc"), Path("/tmp/runsc-state"), Path("/fixtures"), context.claim_digest)
    output = Path("/tmp/native-work/output")
    if expiry:
        pulse = output / "lifecycle-pulse"
        stopped = pulse.read_bytes()
        assert int(stopped) > 0, "deadline test never reached a live sandbox client"
        late = "late-" + layout.identity("pause")
        command = [*layout.command("start", "client")[:-1], late]
        try:
            assert subprocess.run(command, capture_output=True, timeout=5).returncode != 0
            assert pulse.read_bytes() == stopped
        finally:
            subprocess.run([*layout.command("delete", "client")[:-1], late], check=True, timeout=10)
        print("native-supervised-expiry-stopped-live-client", flush=True)
    else:
        assert list((output / "build/images").iterdir()) == []
        print("native-allocated-client-artifact-ok", flush=True)
    state = subprocess.run(layout.command("list", "pause"), check=True, capture_output=True, timeout=10)
    assert not json.loads(state.stdout)
    print("native-allocated-runtime-cleanup-ok", flush=True)


async def main():
    assert os.getuid() == 1000
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
    # Trusted disposable material preparation needs the same static UID mapping
    # to restore rootfs capabilities. It starts no feature/runtime process.
    runtime_workspace = Path("/tmp/native-work")
    runtime_workspace.mkdir(mode=0o700)
    await prepare_native_runtime_input(NativeStagedBuildSource(context, Path("/fixtures/input/source.tar")),
        workspace=runtime_workspace, max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2)
    subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
        "--state-dir=/tmp/rootless-preparation", sys.executable, "/test-support/execute.py", "prepare"],
        check=True, timeout=60)
    claim = BuildClaimRequestV1.model_validate_json(Path("/fixtures/claim.json").read_bytes())
    spec = NativeRootlessSpecV1(claim=claim, context=context, runsc="/runtime/runsc",
        state_root="/tmp/runsc-state", bundle_root="/fixtures", workspace="/tmp/native-work",
        max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2)
    wire = canonical_bytes(spec)
    spec_path = Path("/tmp/native-work/runtime-spec.json")
    spec_path.write_bytes(wire)
    spec_path.chmod(0o400)
    authority, mapped_authority = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, mapped_artifact = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    workspace = Path("/tmp/native-outer-io")
    workspace.mkdir(mode=0o700)
    children = []
    try:
        helper = subprocess.Popen([sys.executable, "/test-support/supervised.py", str(authority.fileno()),
            str(os.getpid()), str(int(expiry))], pass_fds=(authority.fileno(),))
        children.append(helper)
        authority.close()
        child = subprocess.Popen([sys.executable, __file__, "launch", str(os.getpid()),
            str(mapped_authority.fileno()), str(mapped_artifact.fileno()), hashlib.sha256(wire).hexdigest()],
            pass_fds=(mapped_authority.fileno(), mapped_artifact.fileno()), stdout=subprocess.PIPE)
        children.append(child)
        mapped_authority.close()
        mapped_artifact.close()
        if expiry:
            artifact.setblocking(False)
            async with asyncio.timeout(120):
                # A partial header is not evidence of zero export. Even one
                # byte is a failure; the expired session must close untouched.
                assert await asyncio.get_running_loop().sock_recv(artifact, 1) == b"", "expired runtime exported bytes"
        else:
            async with receive_native_artifact(artifact, workspace=workspace,
                claim_digest=context.claim_digest, source_binding_sha256=context.source_binding_sha256,
                max_artifact_bytes=32 * 1024**2, timeout_seconds=120) as received:
                # Receiver path stays in this process and context; no FD path RPC.
                shutil.copyfile(received.archive, "/result/artifacts.tar")
                try:
                    Path("/tmp/native-work/output/build/artifacts.tar").read_bytes()
                except PermissionError:
                    pass
                else:
                    raise AssertionError("outer IO directly read private mapped output")
                print("native-outer-io-received-private-artifact", flush=True)
        stdout, _stderr = child.communicate(timeout=10)
        assert child.returncode == 0 and 1 <= len(stdout) <= 4096
        result = NativeRootlessResultV1.model_validate_json(stdout)
        assert canonical_bytes(result) + b"\n" == stdout
        assert result.claim_digest == context.claim_digest and result.source_binding_sha256 == context.source_binding_sha256
        assert result.broker_reaped and result.cleanup_confirmed
        assert result.client_succeeded is not expiry
        assert (result.artifact is None) is expiry
        if not expiry:
            assert result.artifact == received.artifact
            print("native-supervised-build-completed", flush=True)
        print("native-supervised-cleanup-confirmed", flush=True)
        subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
            "--state-dir=/tmp/rootless-verification", sys.executable, __file__, "verify"], check=True, timeout=30)
        assert list(workspace.iterdir()) == []
        print("native-outer-io-session-settled", flush=True)
    finally:
        for process in reversed(children):
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        for channel in (authority, mapped_authority, artifact, mapped_artifact):
            channel.close()


if __name__ == "__main__":
    if sys.argv[1:] == ["verify"]:
        verify_runtime()
    elif len(sys.argv) > 1:
        launch()
    else:
        asyncio.run(main())
