from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CUTOVER_SCRIPT = REPO_ROOT / "deploy/staging-k3s/loom-staging-k3s-cutover.sh"


def test_cutover_dnat_only_matches_the_k3s_entry_address(tmp_path: Path) -> None:
    iptables_log = tmp_path / "iptables.log"
    iptables_state = tmp_path / "iptables-state"
    iptables_state.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_iptables = fake_bin / "iptables"
    fake_iptables.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >>"$IPTABLES_LOG"\n'
        'case " $* " in\n'
        '  *" -C PREROUTING -p tcp --dport 443 "*) marker="$IPTABLES_STATE/443" ;;\n'
        '  *" -C PREROUTING -p tcp --dport 80 "*) marker="$IPTABLES_STATE/80" ;;\n'
        '  *" -C "*) exit 1 ;;\n'
        "esac\n"
        'if [ -n "${marker:-}" ]; then\n'
        '  if [ ! -e "$marker" ]; then touch "$marker"; exit 0; fi\n'
        "  exit 1\n"
        "fi\n",
        encoding="utf-8",
    )
    fake_iptables.chmod(0o755)

    completed = subprocess.run(
        ["/bin/bash", str(CUTOVER_SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "IPTABLES_LOG": str(iptables_log),
            "IPTABLES_STATE": str(iptables_state),
            "K3S_INGRESS_IP": "192.168.50.103",
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    assert completed.returncode == 0, completed.stderr
    assert iptables_log.read_text(encoding="utf-8").splitlines() == [
        "-t nat -C PREROUTING -d 192.168.50.103/32 -p tcp --dport 443 "
        "-j DNAT --to-destination 192.168.50.103:8443",
        "-t nat -I PREROUTING -d 192.168.50.103/32 -p tcp --dport 443 "
        "-j DNAT --to-destination 192.168.50.103:8443",
        "-t nat -C PREROUTING -d 192.168.50.103/32 -p tcp --dport 80 "
        "-j DNAT --to-destination 192.168.50.103:8080",
        "-t nat -I PREROUTING -d 192.168.50.103/32 -p tcp --dport 80 "
        "-j DNAT --to-destination 192.168.50.103:8080",
        "-t nat -C PREROUTING -p tcp --dport 443 "
        "-j DNAT --to-destination 192.168.50.103:8443",
        "-t nat -D PREROUTING -p tcp --dport 443 "
        "-j DNAT --to-destination 192.168.50.103:8443",
        "-t nat -C PREROUTING -p tcp --dport 443 "
        "-j DNAT --to-destination 192.168.50.103:8443",
        "-t nat -C PREROUTING -p tcp --dport 80 "
        "-j DNAT --to-destination 192.168.50.103:8080",
        "-t nat -D PREROUTING -p tcp --dport 80 "
        "-j DNAT --to-destination 192.168.50.103:8080",
        "-t nat -C PREROUTING -p tcp --dport 80 "
        "-j DNAT --to-destination 192.168.50.103:8080",
    ]


def test_cutover_preserves_legacy_routes_when_scoped_insertion_fails(
    tmp_path: Path,
) -> None:
    iptables_log = tmp_path / "iptables.log"
    iptables_state = tmp_path / "iptables-state"
    iptables_state.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_iptables = fake_bin / "iptables"
    fake_iptables.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >>"$IPTABLES_LOG"\n'
        'case " $* " in\n'
        '  *" -C PREROUTING -d "*) exit 1 ;;\n'
        '  *" -I PREROUTING -d "*" --dport 80 "*) exit 42 ;;\n'
        '  *" -C PREROUTING -p tcp --dport 443 "*) marker="$IPTABLES_STATE/443" ;;\n'
        '  *" -C PREROUTING -p tcp --dport 80 "*) marker="$IPTABLES_STATE/80" ;;\n'
        "esac\n"
        'if [ -n "${marker:-}" ]; then\n'
        '  if [ ! -e "$marker" ]; then touch "$marker"; exit 0; fi\n'
        "  exit 1\n"
        "fi\n",
        encoding="utf-8",
    )
    fake_iptables.chmod(0o755)

    completed = subprocess.run(
        ["/bin/bash", str(CUTOVER_SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "IPTABLES_LOG": str(iptables_log),
            "IPTABLES_STATE": str(iptables_state),
            "K3S_INGRESS_IP": "192.168.50.103",
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    commands = iptables_log.read_text(encoding="utf-8").splitlines()
    assert completed.returncode == 42
    assert not any("-D PREROUTING" in command for command in commands)
