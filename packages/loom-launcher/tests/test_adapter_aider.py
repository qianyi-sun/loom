"""AiderAdapter contract: build_invocation + tail_log_file on chat history."""

from __future__ import annotations

import asyncio
import base64
import os
import stat
import subprocess
import sys
import tempfile
from collections.abc import AsyncIterator
from importlib import resources
from pathlib import Path, PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ExecHandle, ModelSpec, SandboxAccess
from loom_launcher.adapters._aider_install import AIDER_INSTALL_SCRIPT, AIDER_PATCHER_BASE64


class _ScriptedSandbox:
    """Yields a growing file across reads — each call returns the next snapshot."""

    def __init__(self, snapshots: list[str]) -> None:
        self.snapshots = list(snapshots)
        self.idx = 0

    async def read_text(self, path: PurePosixPath) -> str:
        if self.idx < len(self.snapshots) - 1:
            self.idx += 1
        return self.snapshots[self.idx]

    async def exec_oneshot(
        self, argv: list[str], *, timeout_sec: float = 10.0,
    ) -> tuple[int, bytes]:
        return (1, b"")


def _handle_with_sandbox(
    sandbox: SandboxAccess, *, runtime_sec: float = 0.3,
) -> ExecHandle:
    async def _empty() -> AsyncIterator[bytes]:
        if False:
            yield b""

    async def _wait() -> int:
        await asyncio.sleep(runtime_sec)
        return 0

    async def _kill() -> None:
        pass

    return ExecHandle(
        pid=0, stdout=_empty(), stderr=_empty(),
        _wait=_wait, _kill=_kill, sandbox=sandbox,
    )


def test_build_invocation_argv_and_telemetry_env() -> None:
    adapter = get_adapter("aider")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="fix the bug",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == [
        "aider",
        "--yes-always",
        "--no-auto-commits",
        "--model", "openai/gpt-5",
        "--message", "fix the bug",
    ]
    assert env["AIDER_NO_TELEMETRY"] == "1"


def test_installer_embeds_the_exact_reviewed_patcher_bytes() -> None:
    patcher = resources.files("loom_launcher").joinpath("aider_distribution.py").read_bytes()

    assert base64.b64decode(AIDER_PATCHER_BASE64, validate=True) == patcher
    assert AIDER_PATCHER_BASE64 in AIDER_INSTALL_SCRIPT


def test_adapter_uses_the_one_shared_aider_installer() -> None:
    adapter = get_adapter("aider")

    assert adapter is not None
    assert adapter.install_script is AIDER_INSTALL_SCRIPT


def test_installer_has_the_complete_pinned_security_and_smoke_contract() -> None:
    required_fragments = (
        "set -euo pipefail",
        "umask 077",
        "mktemp -d",
        "trap '",
        "EXIT",
        "command -v apk",
        "apk add --no-cache",
        "command -v apt-get",
        "apt-get install -y --no-install-recommends",
        "rm -rf /var/lib/apt/lists/*",
        "aider_chat-0.86.2-py3-none-any.whl",
        "64f6a0c66c9f4633ad9f479bca3e64ebcba02b9da03c6b604b74a44736b2416e",
        "aider-chat==0.86.2",
        "--only-binary=:all:",
        "--no-deps",
        "aider-chat==0.86.2+loom.1",
        "--no-index",
        "--find-links",
        "litellm==1.84.1",
        "importlib-metadata==8.9.0",
        "pip check",
        "importlib.metadata.version",
        "aider --version",
        "aider --help",
        "--message",
        "aider.models.Model",
        "aider.coders.Coder",
        "aider.io.InputOutput",
    )
    for fragment in required_fragments:
        assert fragment in AIDER_INSTALL_SCRIPT
    assert "python3 -m venv --clear" in AIDER_INSTALL_SCRIPT
    assert "rm -rf \"$AIDER_VENV\"" not in AIDER_INSTALL_SCRIPT


def test_installer_rejects_unapproved_venv_before_filesystem_mutation(tmp_path: Path) -> None:
    forbidden = tmp_path / "caller-selected"
    environment = os.environ.copy()
    environment["LOOM_AIDER_VENV"] = os.fspath(forbidden)

    completed = subprocess.run(
        ["bash", "-c", AIDER_INSTALL_SCRIPT],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode != 0
    assert "unsupported LOOM_AIDER_VENV" in completed.stderr
    assert not forbidden.exists()


def _remapped_installer(approved_root: Path) -> tuple[str, Path, Path]:
    """Exercise the literal installer against a private stand-in for /opt."""
    loom_agents = approved_root / "opt" / "loom-agents"
    python_cli = approved_root / "opt" / "agent-runtimes" / "python-cli"
    script = AIDER_INSTALL_SCRIPT.replace("/opt/loom-agents", os.fspath(loom_agents))
    script = script.replace("/opt/agent-runtimes/python-cli", os.fspath(python_cli))
    return script, loom_agents, python_cli


def _mkdir_trusted_tree(approved_root: Path, directory: Path) -> None:
    """Create every private stand-in component with installer-safe permissions."""
    approved_root.chmod(0o700)
    current = approved_root
    for component in directory.relative_to(approved_root).parts:
        current /= component
        current.mkdir(mode=0o700)


def _run_remapped_installer(
    tmp_path: Path, script: str, *, venv: Path,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    """Run only through the filesystem guard; fake apk records premature access."""
    command_dir = tmp_path / "commands"
    command_dir.mkdir()
    package_manager_marker = tmp_path / "package-manager-invoked"
    python_marker = tmp_path / "python-invoked"
    apk = command_dir / "apk"
    apk.write_text(
        "#!/bin/sh\nprintf invoked > \"$LOOM_AIDER_TEST_PACKAGE_MARKER\"\nexit 91\n",
        encoding="utf-8",
    )
    apk.chmod(0o700)
    python = command_dir / "python3"
    python.write_text(
        "#!/bin/sh\nprintf invoked > \"$LOOM_AIDER_TEST_PYTHON_MARKER\"\nexit 92\n",
        encoding="utf-8",
    )
    python.chmod(0o700)
    environment = os.environ.copy()
    environment.update(
        {
            "LOOM_AIDER_VENV": os.fspath(venv),
            "LOOM_AIDER_TEST_PACKAGE_MARKER": os.fspath(package_manager_marker),
            "LOOM_AIDER_TEST_PYTHON_MARKER": os.fspath(python_marker),
            "PATH": f"{command_dir}:{environment['PATH']}",
        },
    )
    return (
        subprocess.run(
            ["bash", "-c", script],
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        ),
        package_manager_marker,
        python_marker,
    )


def test_installer_rejects_approved_venv_symlink_before_package_work(
    tmp_path: Path,
) -> None:
    with tempfile.TemporaryDirectory(prefix="aider-installer-", dir=Path.home()) as root:
        script, loom_agents, _python_cli = _remapped_installer(Path(root))
        _mkdir_trusted_tree(Path(root), loom_agents)
        target = Path(root) / "target"
        target.mkdir(mode=0o700)
        sentinel = target / "sentinel"
        sentinel.write_bytes(b"do not delete these bytes")
        sibling = target / "sibling"
        sibling.write_bytes(b"also untouched")
        venv = loom_agents / "aider"
        venv.symlink_to(target, target_is_directory=True)

        completed, package_manager_marker, python_marker = _run_remapped_installer(
            tmp_path, script, venv=venv,
        )

        assert completed.returncode != 0
        assert "aider venv" in completed.stderr
        assert venv.is_symlink()
        assert sentinel.read_bytes() == b"do not delete these bytes"
        assert sibling.read_bytes() == b"also untouched"
        assert {entry.name for entry in target.iterdir()} == {"sentinel", "sibling"}
        assert not package_manager_marker.exists()
        assert not python_marker.exists()


def test_installer_rejects_unsafe_approved_venv_parent_before_package_work(
    tmp_path: Path,
) -> None:
    with tempfile.TemporaryDirectory(prefix="aider-installer-", dir=Path.home()) as root:
        script, loom_agents, _python_cli = _remapped_installer(Path(root))
        _mkdir_trusted_tree(Path(root), loom_agents)
        loom_agents.chmod(0o777)

        completed, package_manager_marker, python_marker = _run_remapped_installer(
            tmp_path, script, venv=loom_agents / "aider",
        )

        assert completed.returncode != 0
        assert "aider venv parent" in completed.stderr
        assert not package_manager_marker.exists()
        assert not python_marker.exists()


def test_installer_allows_valid_trusted_approved_venv_parent(tmp_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="aider-installer-", dir=Path.home()) as root:
        script, loom_agents, _python_cli = _remapped_installer(Path(root))
        _mkdir_trusted_tree(Path(root), loom_agents)

        completed, package_manager_marker, python_marker = _run_remapped_installer(
            tmp_path, script, venv=loom_agents / "aider",
        )

        assert completed.returncode == 91
        assert package_manager_marker.read_text(encoding="utf-8") == "invoked"
        assert not python_marker.exists()


def test_installer_rejects_intermediate_parent_made_unsafe_during_package_bootstrap(
    tmp_path: Path,
) -> None:
    with tempfile.TemporaryDirectory(prefix="aider-installer-", dir=Path.home()) as root:
        script, _loom_agents, python_cli = _remapped_installer(Path(root))
        _mkdir_trusted_tree(Path(root), python_cli)
        intermediate = python_cli.parent
        venv = python_cli / "aider"
        venv.mkdir(mode=0o700)
        sentinel = venv / "sentinel"
        sentinel.write_bytes(b"leave the existing venv untouched")

        command_dir = tmp_path / "commands-bootstrap-race"
        command_dir.mkdir()
        venv_marker = tmp_path / "venv-invoked"
        apk = command_dir / "apk"
        apk.write_text(
            "#!/bin/sh\nchmod 0777 -- \"$LOOM_AIDER_TEST_INTERMEDIATE\"\n",
            encoding="utf-8",
        )
        apk.chmod(0o700)
        python = command_dir / "python3"
        python.write_text(
            "#!/bin/sh\n"
            "if [ \"${1:-}\" = -m ] && [ \"${2:-}\" = venv ]; then\n"
            "  printf invoked > \"$LOOM_AIDER_TEST_VENV_MARKER\"\n"
            "  exit 93\n"
            "fi\n"
            f"exec {sys.executable} \"$@\"\n",
            encoding="utf-8",
        )
        python.chmod(0o700)
        environment = os.environ.copy()
        environment.update(
            {
                "LOOM_AIDER_VENV": os.fspath(venv),
                "LOOM_AIDER_TEST_INTERMEDIATE": os.fspath(intermediate),
                "LOOM_AIDER_TEST_VENV_MARKER": os.fspath(venv_marker),
                "PATH": f"{command_dir}:{environment['PATH']}",
            },
        )

        completed = subprocess.run(
            ["bash", "-c", script],
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert completed.returncode != 0
        assert "aider venv parent is unsafe" in completed.stderr
        assert stat.S_IMODE(intermediate.stat().st_mode) == 0o777
        assert not venv_marker.exists()
        assert sentinel.read_bytes() == b"leave the existing venv untouched"
        assert {entry.name for entry in venv.iterdir()} == {"sentinel"}


async def test_capture_via_log_file_tail() -> None:
    adapter = get_adapter("aider")
    assert adapter is not None
    # Real aider chat-history.md format: markdown sections per turn.
    snapshots = [
        "",
        "# user\n",
        "# user\nfix the bug\n",
        "# user\nfix the bug\n# assistant\n",
        "# user\nfix the bug\n# assistant\nlooking at it...\n",
    ]
    sandbox = _ScriptedSandbox(snapshots)
    handle = _handle_with_sandbox(sandbox, runtime_sec=1.2)
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    seen = [e["line"] for e in events]
    assert "# user" in seen
    assert "fix the bug" in seen
    assert "# assistant" in seen
    assert "looking at it..." in seen
