from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT = REPO_ROOT / "scripts/ops/bootstrap_staging_k3s_entry_tls.sh"


def test_bootstrap_restarts_an_already_active_cutover_unit(tmp_path: Path) -> None:
    command_log = tmp_path / "commands.log"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for command in ("install", "kubectl", "systemctl"):
        executable = fake_bin / command
        validate_install_source = (
            'test -f "$3" || { printf "missing source: %s\\n" "$3" >&2; exit 44; }\n'
            if command == "install"
            else ""
        )
        executable.write_text(
            "#!/bin/sh\n"
            f'printf "{command} %s\\n" "$*" >>"$COMMAND_LOG"\n'
            + validate_install_source
            + ('case "$1" in is-active) printf "active\\n" ;; esac\n' if command == "systemctl" else ""),
            encoding="utf-8",
        )
        executable.chmod(0o755)

    completed = subprocess.run(
        ["/bin/bash", str(BOOTSTRAP_SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "COMMAND_LOG": str(command_log),
            "KUBECONFIG": str(tmp_path / "kubeconfig"),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    assert completed.returncode == 0, completed.stderr
    systemctl_commands = [
        line
        for line in command_log.read_text(encoding="utf-8").splitlines()
        if line.startswith("systemctl ")
    ]
    assert systemctl_commands[:4] == [
        "systemctl daemon-reload",
        "systemctl enable loom-staging-k3s-cutover.service",
        "systemctl restart loom-staging-k3s-cutover.service",
        "systemctl is-active loom-staging-k3s-cutover.service",
    ]
