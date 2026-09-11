"""Disposable full-build IO split; the synthetic authority stays outside mapping."""

import asyncio
import fcntl
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

from loom_capacity_agent.build_admission import BuildSourceContextV1
from loom_capacity_executor.native_artifact_transfer import receive_native_artifact
from loom_capacity_executor.native_parent_death import bind_native_parent_death


def launch():
    bind_native_parent_death(int(sys.argv[2]))
    originals = [int(value) for value in sys.argv[3:5]]
    copied = [fcntl.fcntl(value, fcntl.F_DUPFD_CLOEXEC, 10) for value in originals]
    for source, target in zip(copied, (3, 4), strict=True):
        os.dup2(source, target, inheritable=True)
    for descriptor in set(copied + originals) - {3, 4}:
        os.close(descriptor)
    args = ["/usr/bin/rootlesskit", "--net=none", "--subid-source=static", "--state-dir=/tmp/rootless-probe",
        sys.executable, "/test-support/execute.py"]
    os.execve(args[0], args, {"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "PYTHONPATH": "/trusted-src", "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8",
        "LISTEN_FDS": "2", "LISTEN_PID": str(os.getpid())})


async def main():
    assert os.getuid() == 1000
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
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
            str(mapped_authority.fileno()), str(mapped_artifact.fileno())],
            pass_fds=(mapped_authority.fileno(), mapped_artifact.fileno()))
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
        assert child.wait(timeout=10) == 0
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
    if len(sys.argv) > 1:
        launch()
    else:
        asyncio.run(main())
