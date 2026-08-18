from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT = REPO_ROOT / "scripts/ops/bootstrap_staging_k3s_entry_tls.sh"


def test_bootstrap_installs_kubernetes_route_before_cleaning_host_rules(
    tmp_path: Path,
) -> None:
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
    commands = command_log.read_text(encoding="utf-8").splitlines()
    service_apply = next(
        index
        for index, command in enumerate(commands)
        if command.startswith("kubectl apply -f ")
        and command.endswith("/deploy/staging-k3s/loom-staging-public-entry.yaml")
    )
    proxy_script_install = next(
        index
        for index, command in enumerate(commands)
        if command.startswith("install -m 0755 ")
        and command.endswith(
            "/deploy/staging-k3s/loom-staging-public-proxy.sh "
            "/usr/local/sbin/loom-staging-public-proxy.sh"
        )
    )
    proxy_unit_install = next(
        index
        for index, command in enumerate(commands)
        if command.startswith("install -m 0644 ")
        and command.endswith(
            "/deploy/staging-k3s/loom-staging-public-proxy@.service "
            "/etc/systemd/system/loom-staging-public-proxy@.service"
        )
    )
    proxy_restart = commands.index(
        "systemctl restart loom-staging-public-proxy@18080-32080.service "
        "loom-staging-public-proxy@18443-32443.service"
    )
    cutover_restart = commands.index(
        "systemctl restart loom-staging-k3s-cutover.service"
    )
    assert service_apply < proxy_script_install < proxy_restart < cutover_restart
    assert service_apply < proxy_unit_install < proxy_restart

    systemctl_commands = [line for line in commands if line.startswith("systemctl ")]
    assert systemctl_commands[:6] == [
        "systemctl daemon-reload",
        "systemctl reenable loom-staging-public-proxy@18080-32080.service "
        "loom-staging-public-proxy@18443-32443.service "
        "loom-staging-k3s-cutover.service",
        "systemctl restart loom-staging-public-proxy@18080-32080.service "
        "loom-staging-public-proxy@18443-32443.service",
        "systemctl restart loom-staging-k3s-cutover.service",
        "systemctl is-active --quiet loom-staging-k3s-cutover.service",
        "systemctl is-active loom-staging-public-proxy@18080-32080.service "
        "loom-staging-public-proxy@18443-32443.service "
        "loom-staging-k3s-cutover.service",
    ]


def test_bootstrap_accepts_cutover_becoming_ready_at_timeout_boundary(
    tmp_path: Path,
) -> None:
    command_log = tmp_path / "commands.log"
    retry_marker = tmp_path / "retry-started"
    ready_marker = tmp_path / "ready"
    sleep_count = tmp_path / "sleep-count"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()

    fake_install = fake_bin / "install"
    fake_install.write_text(
        "#!/bin/sh\n"
        'printf "install %s\\n" "$*" >>"$COMMAND_LOG"\n'
        'test -f "$3" || exit 44\n',
        encoding="utf-8",
    )
    fake_install.chmod(0o755)

    fake_kubectl = fake_bin / "kubectl"
    fake_kubectl.write_text(
        "#!/bin/sh\n"
        'printf "kubectl %s\\n" "$*" >>"$COMMAND_LOG"\n',
        encoding="utf-8",
    )
    fake_kubectl.chmod(0o755)

    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        """#!/bin/sh
set -eu
printf "systemctl %s\\n" "$*" >>"$COMMAND_LOG"
case "$*" in
  "restart loom-staging-k3s-cutover.service")
    if [ ! -e "$RETRY_MARKER" ]; then
      touch "$RETRY_MARKER"
      exit 1
    fi
    ;;
  "is-active --quiet loom-staging-k3s-cutover.service")
    test -e "$READY_MARKER"
    ;;
  is-active*) printf "active\\n" ;;
esac
""",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)

    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text(
        "#!/bin/sh\n"
        'printf "sleep %s\\n" "$*" >>"$COMMAND_LOG"\n'
        'count="$(cat "$SLEEP_COUNT" 2>/dev/null || printf 0)"\n'
        'count=$((count + 1))\n'
        'printf "%s\\n" "$count" >"$SLEEP_COUNT"\n'
        'test "$count" -lt 12 || touch "$READY_MARKER"\n',
        encoding="utf-8",
    )
    fake_sleep.chmod(0o755)

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
            "READY_MARKER": str(ready_marker),
            "RETRY_MARKER": str(retry_marker),
            "SLEEP_COUNT": str(sleep_count),
        },
    )

    assert completed.returncode == 0, completed.stderr
    commands = command_log.read_text(encoding="utf-8").splitlines()
    assert commands.count("sleep 5") == 12
    ready_index = commands.index(
        "systemctl is-active --quiet loom-staging-k3s-cutover.service",
        len(commands) - 1 - commands[::-1].index("sleep 5"),
    )
    cert_manager_index = next(
        index
        for index, command in enumerate(commands)
        if command.startswith("kubectl -n cert-manager patch deploy cert-manager ")
    )
    assert ready_index < cert_manager_index
