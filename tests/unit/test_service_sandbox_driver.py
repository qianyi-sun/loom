"""Unix socket driver contract, without Kubernetes, cloud or model calls."""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverAlreadyStartedError, DriverError, DriverNotStartedError
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork, Public


def driver_for(path: Path, *, limit: int = 1024) -> ServiceSandboxDriver:
    return ServiceSandboxDriver(
        path,
        capabilities=Capabilities(
            os="linux",
            gpu_vendor="none",
            network_policies=frozenset({"public"}),
            dynamic_network_policy=False,
            mounted_fs=False,
            resource_modes=frozenset({"limit"}),
        ),
        network_policy=Public(),
        max_transfer_bytes=limit,
    )


@pytest.mark.asyncio
async def test_socket_rpc_exec_files_and_lifecycle(tmp_path: Path) -> None:
    requests: list[tuple[str, str, bytes]] = []
    data = b""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal data
        headers = (await reader.readuntil(b"\r\n\r\n")).decode().split("\r\n")
        method, target, _ = headers[0].split()
        length = next(
            (int(h.split(":", 1)[1]) for h in headers if h.lower().startswith("content-length:")), 0
        )
        body = await reader.readexactly(length)
        requests.append((method, target, body))
        if target == "/health":
            output = b'{"ready":true}'
        elif target == "/exec":
            output = json.dumps(
                {
                    "return_code": 3,
                    "stdout": base64.b64encode(b"out").decode(),
                    "stderr": base64.b64encode(b"err").decode(),
                    "truncated": False,
                    "duration_sec": 0.1,
                }
            ).encode()
        elif method == "PUT":
            data, output = body, b""
        else:
            output = data
        writer.write(
            f"HTTP/1.1 200 OK\r\nContent-Length: {len(output)}\r\nConnection: close\r\n\r\n".encode()
            + output
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    # pytest's macOS tmp path can exceed the Unix socket path length limit.
    import tempfile

    with tempfile.TemporaryDirectory(dir="/tmp", prefix="loom-driver-") as directory:
        socket = Path(directory) / "rpc.sock"
        server = await asyncio.start_unix_server(handle, path=socket)
        driver = driver_for(socket)
        async with server:
            with pytest.raises(DriverNotStartedError):
                await driver.exec("true")
            await driver.stop()
            await driver.start()
            result = await driver.exec(
                "printf x", user="root", cwd=PurePosixPath("/app"), env={"A": "b"}, timeout_sec=2
            )
            assert (result.return_code, result.stdout, result.stderr) == (3, b"out", b"err")
            body = json.loads(requests[-1][2])
            assert body == {
                "argv": ["/bin/sh", "-c", "printf x"],
                "user": "root",
                "cwd": "/app",
                "env": {"A": "b"},
                "timeout_sec": 2,
            }
            source = tmp_path / "source"
            source.write_bytes(b"transfer")
            await driver.upload(source, PurePosixPath("/app/source"))
            target = tmp_path / "download"
            # Resolve macOS /var -> /private/var in trusted local fixture.
            await driver.download(PurePosixPath("/app/source"), target.resolve())
            assert target.read_bytes() == b"transfer"
            source.write_bytes(b"x" * 1025)
            with pytest.raises(DriverError, match="transfer limit"):
                await driver.upload(source, PurePosixPath("/app/source"))
            data = b"x" * 1025
            with pytest.raises(DriverError, match="transfer limit"):
                await driver.download(PurePosixPath("/app/source"), target.resolve())
            assert target.read_bytes() == b"transfer"
            await driver.set_network_policy(Public())
            with pytest.raises(DriverError, match="fixed"):
                await driver.set_network_policy(NoNetwork())
            await driver.stop()
            await driver.stop()
            with pytest.raises(DriverAlreadyStartedError):
                await driver.start()
            with pytest.raises(DriverNotStartedError):
                await driver.download(PurePosixPath("/app/source"), target)


@pytest.mark.asyncio
async def test_nondefault_start_options_fail_closed(tmp_path: Path) -> None:
    driver = driver_for(tmp_path / "absent.sock")
    with pytest.raises(DriverError, match="Pod"):
        await driver.start(options=StartOptions(cpus=4))


@pytest.mark.asyncio
async def test_snapshot_hooks_preserve_public_files_without_root(tmp_path: Path) -> None:
    import shutil

    from loom.models.exec import ExecResult
    from loom.trial.workspace import WorkspaceStagingPolicy
    from loom.trial.workspace_snapshot import _strip_private_entries, _validate_workspace_archive

    class LocalTransport(ServiceSandboxDriver):
        # Exercise the production archive hooks against real POSIX processes;
        # only the RPC transport is replaced with this process's filesystem.
        async def exec(self, cmd, *, user=None, cwd=None, env=None, timeout_sec=None):
            assert user is None
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            return ExecResult(
                return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0
            )

        async def upload(self, src, dst):
            shutil.copyfile(src, str(dst))

        async def download(self, src, dst):
            shutil.copyfile(str(src), dst)

    base = driver_for(tmp_path / "unused.sock")
    driver = LocalTransport(
        tmp_path / "unused.sock", capabilities=base.capabilities, network_policy=Public()
    )
    source = tmp_path / "source"
    source.mkdir()
    (source / "answer.txt").write_text("result")
    (source / "tests").mkdir()
    (source / "tests" / "forged.py").write_text("untrusted verifier")
    archive = tmp_path / "workspace.tar"
    await driver.export_workspace_archive(PurePosixPath(str(source)), archive)
    policy = WorkspaceStagingPolicy(
        agent_excluded_paths=("tests/**",),
        verifier_only_paths=("tests/**",),
        trusted_oracle_paths=(),
    )
    _strip_private_entries(archive, policy)
    _validate_workspace_archive(archive, policy)
    destination = tmp_path / "verifier"
    await driver.import_workspace_archive(archive, PurePosixPath(str(destination)))
    assert (destination / "answer.txt").read_text() == "result"
    assert not (destination / "tests" / "forged.py").exists()
