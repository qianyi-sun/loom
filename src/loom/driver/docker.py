"""DockerDriver — production Driver using docker-py.

The container is created from `image` with no command (Loom holds it alive via
a sleep-infinity entrypoint override). All workloads run via `docker exec`.
Network policy switches happen via iptables rules applied inside the container
(see `loom.driver.network_policy`).

Spec §2.2 (Driver contract), §2.3 (Capabilities), §5.1 (timeouts).
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import tarfile
import threading
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import docker
from docker.errors import APIError, ImageNotFound, NotFound

from loom.driver.base import MAX_EXEC_STREAM_BYTES, ExecHandle, StartOptions
from loom.driver.network_policy import compute_iptables_rules, render_iptables_commands
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy, Public
from loom.models.types import OS

logger = logging.getLogger(__name__)

_KEEPALIVE_CMD = ["sh", "-c", "exec sleep infinity"]


def _default_caps() -> Capabilities:
    return Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public", "no-network", "allowlist"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto", "limit", "guarantee"]),
    )


@dataclass
class DockerDriver:
    """Real-Docker Driver. Uses docker-py SDK throughout."""

    image: str
    workspace: PurePosixPath = field(default_factory=lambda: PurePosixPath("/workspace"))
    container_name: str | None = None
    capabilities: Capabilities = field(default_factory=_default_caps)
    os: OS = "linux"
    network_policy_baseline: NetworkPolicy = field(default_factory=Public)
    _client: Any | None = field(default=None, init=False, repr=False)
    _container: Any | None = field(default=None, init=False, repr=False)
    _state: str = field(default="constructed", init=False)

    async def start(self, *, options: StartOptions | None = None) -> None:
        # Spec §2.2: start() at most once. Reject when running OR stopped.
        if self._state != "constructed":
            raise DriverAlreadyStartedError(
                f"DockerDriver.start() rejected in state {self._state!r}",
            )

        opts = options or StartOptions()
        try:
            self._client = docker.from_env()
            await asyncio.to_thread(self._ensure_image, opts)

            self._container = await asyncio.to_thread(
                self._client.containers.run,
                self.image,
                command=_KEEPALIVE_CMD,
                name=self.container_name,
                detach=True,
                tty=False,
                stdin_open=False,
                working_dir=str(self.workspace),
                remove=False,
                # NET_ADMIN lets the workload modify iptables inside its own
                # netns. Bounded to the container's network namespace — host
                # iptables is unaffected. Required for Allowlist + NoNetwork.
                cap_add=["NET_ADMIN"],
            )
            await self._wait_until_running()
            # Mark running BEFORE applying baseline policy —
            # set_network_policy() calls _require_running() which checks state.
            self._state = "running"
            await self.set_network_policy(self.network_policy_baseline)
        except BaseException:
            # Any failure during start() must NOT leak the partially-created
            # container/client. Self-clean before re-raising so the caller
            # isn't obligated to also call stop() on a failed start.
            with contextlib.suppress(Exception):
                await self._teardown(delete=True)
            self._state = "stopped"
            raise

    def _ensure_image(self, opts: StartOptions) -> None:
        assert self._client is not None
        try:
            self._client.images.get(self.image)
            if not opts.force_build:
                return
        except ImageNotFound:
            pass
        if opts.pull:
            self._client.images.pull(self.image)

    async def _wait_until_running(self, timeout_sec: float = 10.0) -> None:
        assert self._container is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_sec
        while loop.time() < deadline:
            await asyncio.to_thread(self._container.reload)
            if self._container.status == "running":
                return
            await asyncio.sleep(0.1)
        raise DriverError(
            f"Container failed to reach 'running' within {timeout_sec}s; "
            f"final status={self._container.status!r}",
        )

    async def stop(self, *, delete: bool = True) -> None:
        # Idempotent. Only running→stopped is a real transition; calling stop()
        # from 'constructed' leaves state intact so start() can still fire.
        await self._teardown(delete=delete)
        if self._state == "running":
            self._state = "stopped"

    async def _teardown(self, *, delete: bool) -> None:
        """Shared docker-resource cleanup used by stop() and start()'s failure
        path. Each step has its own suppress so a stop() error doesn't skip
        the remove() — the plan-shown stop() bundled them and leaked
        stopped-but-not-removed containers when stop() raised."""
        if self._container is not None:
            with contextlib.suppress(APIError, NotFound):
                await asyncio.to_thread(self._container.stop, timeout=10)
            if delete:
                with contextlib.suppress(APIError, NotFound):
                    await asyncio.to_thread(self._container.remove, force=True)
            self._container = None
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None

    def _require_running(self) -> None:
        if self._state != "running" or self._container is None:
            raise DriverNotStartedError(
                f"DockerDriver in state {self._state!r}",
            )

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        self._require_running()
        # Capture the container handle now. If stop() races in concurrently
        # while we're suspended in to_thread, self._container becomes None
        # but our local `container` is still valid for the in-flight call.
        container = self._container
        assert container is not None

        exec_kwargs: dict[str, Any] = {
            "cmd": ["/bin/sh", "-c", cmd],
            "stdout": True,
            "stderr": True,
            "tty": False,
            "detach": False,
            "stream": False,
            "demux": True,
        }
        if user is not None:
            exec_kwargs["user"] = str(user)
        if cwd is not None:
            exec_kwargs["workdir"] = str(cwd)
        if env is not None:
            exec_kwargs["environment"] = dict(env)

        loop = asyncio.get_running_loop()
        started = loop.time()

        def _sync() -> tuple[int, bytes, bytes]:
            result = container.exec_run(**exec_kwargs)
            output = result.output
            if isinstance(output, tuple):
                stdout, stderr = output
            else:
                stdout, stderr = output, b""
            return int(result.exit_code), stdout or b"", stderr or b""

        if timeout_sec is not None:
            exit_code, stdout, stderr = await asyncio.wait_for(
                asyncio.to_thread(_sync), timeout=timeout_sec,
            )
        else:
            exit_code, stdout, stderr = await asyncio.to_thread(_sync)

        duration = loop.time() - started

        truncated = False
        if len(stdout) > MAX_EXEC_STREAM_BYTES:
            stdout = stdout[:MAX_EXEC_STREAM_BYTES]
            truncated = True
        if len(stderr) > MAX_EXEC_STREAM_BYTES:
            stderr = stderr[:MAX_EXEC_STREAM_BYTES]
            truncated = True

        return ExecResult(
            return_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            truncated=truncated,
            duration_sec=duration,
        )

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        self._require_running()
        assert self._client is not None
        assert self._container is not None
        api = self._client.api  # docker.APIClient — low-level
        # Capture the container id once so the _kill closure doesn't
        # re-read self._container (which may be None after stop()).
        container_id = self._container.id

        exec_create_kwargs: dict[str, Any] = {
            "container": container_id,
            "cmd": argv,
            "environment": [f"{k}={v}" for k, v in env_vars.items()],
            "workdir": str(cwd),
            "tty": False,
            "stdout": True,
            "stderr": True,
        }
        if user is not None:
            exec_create_kwargs["user"] = str(user)

        exec_info = await asyncio.to_thread(api.exec_create, **exec_create_kwargs)
        exec_id = exec_info["Id"]

        # `exec_start(stream=True, demux=True)` returns a SYNC generator of
        # (stdout_chunk, stderr_chunk) tuples (either side may be None for
        # a chunk that only carries the other stream).
        raw_stream = await asyncio.to_thread(
            api.exec_start, exec_id, stream=True, demux=True,
        )

        stdout_q: asyncio.Queue[bytes | None] = asyncio.Queue()
        stderr_q: asyncio.Queue[bytes | None] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        # Signaled by the asyncio side (_wait race winner = poll path) to
        # tell the blocking drainer it can quit early; we still process
        # any chunks currently in flight before exiting.
        stop_reader = threading.Event()

        def _drain_blocking() -> None:
            try:
                for chunk in raw_stream:
                    if stop_reader.is_set():
                        break
                    out_chunk, err_chunk = chunk if isinstance(chunk, tuple) else (chunk, None)
                    if out_chunk:
                        loop.call_soon_threadsafe(stdout_q.put_nowait, out_chunk)
                    if err_chunk:
                        loop.call_soon_threadsafe(stderr_q.put_nowait, err_chunk)
            finally:
                loop.call_soon_threadsafe(stdout_q.put_nowait, None)
                loop.call_soon_threadsafe(stderr_q.put_nowait, None)

        reader_task = asyncio.create_task(asyncio.to_thread(_drain_blocking))

        async def _drain(q: asyncio.Queue[bytes | None]) -> AsyncIterator[bytes]:
            while True:
                chunk = await q.get()
                if chunk is None:
                    return
                yield chunk

        async def _poll_exit() -> int:
            # Poll exec_inspect until Running becomes false. Used as a
            # fallback when the stream iterator doesn't close cleanly
            # (e.g., after SIGKILL).
            backoff = 0.05
            while True:
                info = await asyncio.to_thread(api.exec_inspect, exec_id)
                if not info.get("Running"):
                    return int(info.get("ExitCode") or 0)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 0.5)

        async def _wait() -> int:
            # Race the stream-drain against a polled exec_inspect.
            # Stream-drain wins on normal exits; the poller wins after
            # SIGKILL when the iterator blocks indefinitely.
            poll_task = asyncio.create_task(_poll_exit())
            done, pending = await asyncio.wait(
                {reader_task, poll_task}, return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            # If the poll path won, signal the blocking drainer so it
            # doesn't outlive its useful life as an orphan thread holding
            # docker's iterator.
            stop_reader.set()
            if poll_task in done:
                return poll_task.result()
            # Normal path: reader finished; fetch the exit code.
            info = await asyncio.to_thread(api.exec_inspect, exec_id)
            return int(info.get("ExitCode") or 0)

        async def _kill() -> None:
            # docker exec has no direct kill API; exec_inspect's Pid field
            # is the HOST's view and is useless from inside the container's
            # PID namespace. Best-effort: pkill matching the joined argv.
            # May or may not succeed depending on whether `pkill -f` is
            # available (busybox without procps doesn't ship it). The
            # ExecHandle.kill() contract documents this as best-effort.
            try:
                target = " ".join(argv)
                killer = await asyncio.to_thread(
                    api.exec_create,
                    container=container_id,
                    cmd=["sh", "-c", f"pkill -9 -f {target!r} 2>/dev/null; true"],
                )
                await asyncio.to_thread(api.exec_start, killer["Id"], detach=True)
            except (APIError, NotFound):
                pass

        return ExecHandle(
            pid=0,  # docker exec doesn't surface a host-side PID we trust
            stdout=_drain(stdout_q),
            stderr=_drain(stderr_q),
            _wait=_wait,
            _kill=_kill,
        )

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self._require_running()
        container = self._container
        assert container is not None
        if not src.is_file():
            raise FileNotFoundError(f"upload source {src} is not a regular file")

        def _build_tar() -> bytes:
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tf:
                info = tarfile.TarInfo(name=dst.name)
                data = src.read_bytes()
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            return buf.getvalue()

        tar_bytes = await asyncio.to_thread(_build_tar)

        # Ensure parent dir exists in the container.
        await self.exec(f"mkdir -p {dst.parent.as_posix()}", user="root")

        def _put() -> None:
            ok = container.put_archive(path=str(dst.parent), data=tar_bytes)
            if not ok:
                raise DriverError(f"put_archive returned False for {dst}")

        await asyncio.to_thread(_put)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        self._require_running()
        container = self._container
        assert container is not None

        def _get() -> bytes:
            try:
                stream, _stat = container.get_archive(str(src))
            except NotFound as exc:
                raise FileNotFoundError(f"{src} not found in container") from exc
            return b"".join(stream)

        data = await asyncio.to_thread(_get)

        def _extract() -> None:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
                # File members only — get_archive on a directory returns the
                # whole subtree as a tar with many entries. The Driver spec
                # is single-file; refuse silently-lossy multi-member extracts.
                file_members = [m for m in tf.getmembers() if m.isfile()]
                if not file_members:
                    raise DriverError(f"no regular files in tarball for {src}")
                if len(file_members) > 1:
                    raise DriverError(
                        f"{src} is a directory (tarball had {len(file_members)} "
                        f"files); Driver.download is single-file only",
                    )
                fobj = tf.extractfile(file_members[0])
                if fobj is None:
                    raise DriverError(f"could not extract {src} from tarball")
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(fobj.read())

        await asyncio.to_thread(_extract)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        self._require_running()
        plan = compute_iptables_rules(policy)
        cmds = render_iptables_commands(plan)
        if not cmds:
            # Public-equivalent: no enforcement needed. Skip the exec so
            # containers without iptables installed still work.
            self.network_policy_baseline = policy
            return
        script = " && ".join(cmds)
        r = await self.exec(script, user="root", timeout_sec=30)
        if r.return_code != 0:
            raise DriverError(
                f"network policy apply failed: rc={r.return_code} "
                f"stderr={r.stderr[:512]!r}",
            )
        self.network_policy_baseline = policy

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        self._require_running()
        if hc is None:
            return

        loop = asyncio.get_running_loop()
        deadline_start_period = loop.time() + hc.start_period_sec
        consecutive_failures = 0
        while True:
            in_grace = loop.time() < deadline_start_period
            try:
                r = await self.exec(hc.command, timeout_sec=hc.timeout_sec)
                if r.return_code == 0:
                    return
                logger.debug("healthcheck non-zero exit", extra={"rc": r.return_code})
            except TimeoutError:
                logger.debug("healthcheck timed out")
            if not in_grace:
                consecutive_failures += 1
                if consecutive_failures > hc.retries:
                    raise DriverError(
                        f"Healthcheck failed after {hc.retries} consecutive "
                        f"retries: {hc.command!r}",
                    )
            await asyncio.sleep(hc.interval_sec)
