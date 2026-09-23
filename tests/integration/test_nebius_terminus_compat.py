"""Opt-in installed Harbor regression on original task images; no model/network.

LOOM_TERMINUS_COMPAT_IMAGES is a comma-separated list of local image references.
LOOM_TERMINUS_CONTROLLER_IMAGE selects the built native controller image.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath
from uuid import uuid4


def test_installed_harbor_legacy_tools(tmp_path: Path, task_image: str) -> None:
    repository = Path(__file__).resolve().parents[2]
    prefix = "loom-compat-" + uuid4().hex[:12]
    volume = prefix + "-socket"
    binary = tmp_path / "loom-sandbox-runtime"
    subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/loom-sandbox-runtime"],
        cwd=repository,
        env={**os.environ, "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"},
        check=True,
    )
    binary.chmod(0o755)
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    evidence.chmod(0o777)

    def docker(*args: str) -> str:
        result = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout

    try:
        docker("volume", "create", volume)
        docker("run", "--rm", "--platform", "linux/amd64", "--network", "none",
               "--user", "0", "-v", volume + ":/socket", "--entrypoint", "/bin/sh",
               task_image, "-c", "chown 65532:65532 /socket")
        docker("run", "-d", "--name", prefix, "--platform", "linux/amd64",
               "--network", "none", "--user", "65532:65532", "--cap-drop", "ALL",
               "--security-opt", "no-new-privileges",
               "-v", f"{binary}:/loom/bin/runtime:ro", "-v", volume + ":/socket",
               "--entrypoint", "/loom/bin/runtime", task_image,
               "--socket", "/socket/sandbox.sock")
        output = docker(
            "run", "--rm", "--name", prefix + "-controller",
            "--platform", "linux/amd64", "--network", "none",
            "--user", "65532:65532", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "-v", volume + ":/socket",
            "-v", f"{Path(__file__).resolve()}:/fixture/test.py:ro",
            "-v", f"{evidence}:/evidence", "-e", "HOME=/tmp/loom-home",
            "--entrypoint", "python", os.environ["LOOM_TERMINUS_CONTROLLER_IMAGE"],
            "-I", "-B", "/fixture/test.py", "--inside",
        )
        report = json.loads(output)
        assert report["exactly_once"] and report["failure_not_replayed"]
        assert report["recording_retained"] and report["nonroot_only"]
        assert report["staged_files_cleaned"] and report["existing_buffer_retained"]
        print(json.dumps({"task_image": task_image, **report}))
    finally:
        subprocess.run(["docker", "rm", "-f", prefix + "-controller", prefix], capture_output=True)
        subprocess.run(["docker", "volume", "rm", volume], capture_output=True)


async def _inside() -> None:
    from harbor.agents.terminus_2.tmux_session import TmuxSession
    from harbor.environments.base import ExecResult

    from loom.driver.service_sandbox import ServiceSandboxDriver
    from loom.models.capabilities import Capabilities
    from loom.models.networking import NoNetwork

    driver = ServiceSandboxDriver(
        Path("/socket/sandbox.sock"),
        capabilities=Capabilities(
            os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
            dynamic_network_policy=False, mounted_fs=False,
            resource_modes=frozenset({"guarantee"}),
        ),
        network_policy=NoNetwork(),
    )
    await driver.start()

    class Environment:
        session_id = "compat"

        def __init__(self) -> None:
            self.users: list[object] = []
            self.pastes = 0
            self.fail_after_paste = False

        async def exec(self, command: str, *, user: object = None, **kwargs: object) -> ExecResult:
            self.users.append(user)
            result = await driver.exec(command, user=user, **kwargs)
            if command.startswith("tmux paste-buffer"):
                self.pastes += 1
                if self.fail_after_paste:
                    # Delivery happened, but the caller sees failure. Retrying
                    # would execute user input twice; preserve the error instead.
                    return ExecResult(return_code=1, stdout="", stderr="injected response failure")
            return ExecResult(return_code=result.return_code,
                              stdout=result.stdout.decode(), stderr=result.stderr.decode())

        async def upload_file(self, source_path: Path, target_path: str) -> None:
            await driver.upload(Path(source_path), PurePosixPath(target_path))

        async def download_file(self, source_path: str, target_path: Path) -> None:
            await driver.download(PurePosixPath(source_path), Path(target_path))

    async def command(value: str) -> str:
        result = await driver.exec(value)
        assert result.return_code == 0, result.stderr.decode()
        return result.stdout.decode()

    async def wait_file(path: str, expected: str) -> None:
        for _ in range(100):
            result = await driver.exec("cat " + path)
            if result.return_code == 0 and result.stdout.decode() == expected:
                return
            await asyncio.sleep(0.1)
        pane = await driver.exec("tmux capture-pane -p -S -100 -t compat")
        raise AssertionError(f"payload mismatch at {path}: {result.stdout[:200]!r} {result.stderr!r}; "
                             f"pane={pane.stdout[-5000:]!r}")

    environment = Environment()
    recording = Path("/evidence/recording.cast")
    session = TmuxSession(
        session_name="compat", environment=environment,
        logging_path=Path("/tmp/compat.pane"),
        local_asciinema_recording_path=recording,
        remote_asciinema_recording_path=Path("/tmp/compat.cast"),
    )
    try:
        tmux_version = (await command("tmux -V")).strip()
        await session.start()
        await command("printf retained >/tmp/buffer-seed; tmux load-buffer /tmp/buffer-seed")
        # Exercise real recording with the historical dispatch size first.
        recorded = "cat >/tmp/recorded <<'LOOM_EOF'\nquotes ' \" $ and unicode 中文\nLOOM_EOF\n"
        await session._paste_key(recorded, "compat")
        await wait_file("/tmp/recorded", "quotes ' \" $ and unicode 中文\n")
        await session.stop()
        assert recording.is_file() and recording.stat().st_size > 0
        # Isolate buffer transport from asciinema 2.0's separate large-input
        # forwarding limitation. The outer tmux shell remains alive after stop.
        # Multiple lines avoid the shell's canonical single-line input limit.
        payload = ("quotes ' \" $ and unicode 中文\n" * 800)
        script = "cat >/tmp/payload <<'LOOM_EOF'\n" + payload + "LOOM_EOF\nprintf 'once\\n' >>/tmp/count\n"
        await session._send_keys_to_session([script], action="compat")
        await wait_file("/tmp/payload", payload)
        await wait_file("/tmp/count", "once\n")

        # Cover tmux 1.8's smaller limit and the send-keys -> paste fallback.
        shorter = "printf '%s' '" + ("x" * 4000) + "' >/tmp/shorter\n"
        before = environment.pastes
        await session._send_keys_to_session([shorter], action="compat")
        await wait_file("/tmp/shorter", "x" * 4000)
        if tmux_version.startswith("tmux 1."):
            assert environment.pastes == before + 1, "legacy send-keys fallback was not exercised"
        # Indexed buffers require serial push/paste/pop when callers overlap.
        await asyncio.gather(
            session._paste_key("printf A >/tmp/parallel-a\n", "compat"),
            session._paste_key("printf B >/tmp/parallel-b\n", "compat"),
        )
        await wait_file("/tmp/parallel-a", "A")
        await wait_file("/tmp/parallel-b", "B")

        before = environment.pastes
        environment.fail_after_paste = True
        try:
            await session._paste_key("printf 'once\\n' >>/tmp/failed-count\n", "compat")
        except RuntimeError as exc:
            assert "injected response failure" in str(exc)
        else:
            raise AssertionError("dispatch failure was hidden")
        environment.fail_after_paste = False
        assert environment.pastes == before + 1
        await wait_file("/tmp/failed-count", "once\n")
        assert await command("tmux show-buffer") == "retained"
        assert await command("find /tmp -maxdepth 1 -name '.harbor-tmux-paste-*'") == ""
        assert all(user is None for user in environment.users)
        await command("tmux kill-server")
        before = environment.pastes
        try:
            await session._paste_key("touch /tmp/unexpected-replay\n", "compat")
        except RuntimeError:
            pass
        else:
            raise AssertionError("missing tmux server was hidden")
        assert environment.pastes == before
        assert (await driver.exec("tmux has-session -t compat")).return_code != 0
        await command("test ! -e /tmp/unexpected-replay")
        assert await command("find /tmp -maxdepth 1 -name '.harbor-tmux-paste-*'") == ""
        report = {"tmux_version": tmux_version,
                  "exactly_once": True, "failure_not_replayed": True,
                  "recording_retained": True, "nonroot_only": True,
                  "staged_files_cleaned": True, "existing_buffer_retained": True,
                  "missing_server_not_hidden": True}
        Path("/evidence/report.json").write_text(json.dumps(report, indent=2))
        print(json.dumps(report))
    finally:
        await driver.exec("tmux kill-server")
        await driver.stop()


if __name__ == "__main__" and "--inside" in sys.argv:
    asyncio.run(_inside())
else:
    import pytest

    test_installed_harbor_legacy_tools = pytest.mark.parametrize(
        "task_image", [image for image in os.environ.get("LOOM_TERMINUS_COMPAT_IMAGES", "").split(",") if image],
    )(test_installed_harbor_legacy_tools)
    test_installed_harbor_legacy_tools = pytest.mark.timeout(240)(test_installed_harbor_legacy_tools)
    test_installed_harbor_legacy_tools = pytest.mark.docker(test_installed_harbor_legacy_tools)
