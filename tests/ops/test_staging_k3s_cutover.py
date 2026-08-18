from __future__ import annotations

import configparser
import os
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CUTOVER_SCRIPT = REPO_ROOT / "deploy/staging-k3s/loom-staging-k3s-cutover.sh"
CUTOVER_UNIT = REPO_ROOT / "deploy/staging-k3s/loom-staging-k3s-cutover.service"
PROXY_SCRIPT = REPO_ROOT / "deploy/staging-k3s/loom-staging-public-proxy.sh"
PROXY_UNIT = REPO_ROOT / "deploy/staging-k3s/loom-staging-public-proxy@.service"
PUBLIC_ENTRY_SERVICE = REPO_ROOT / "deploy/staging-k3s/loom-staging-public-entry.yaml"


def _write_fake_iptables(fake_bin: Path) -> None:
    fake_iptables = fake_bin / "iptables"
    fake_iptables.write_text(
        """#!/bin/sh
set -eu
printf "%s\\n" "$*" >>"$IPTABLES_LOG"

test "${1:-}" = "--wait" || exit 98
shift
test "${1:-}" = "-t" && test "${2:-}" = "nat" || exit 97
shift 2

if [ "$*" = "-S KUBE-NODEPORTS" ]; then
  test "${IPTABLES_RULESET_MODE:-ready}" = "operational-failure" && exit 4
  test "${IPTABLES_RULESET_MODE:-ready}" != "missing-http" && cat <<'EOF'
-A KUBE-NODEPORTS -p tcp -m comment --comment "ingress-nginx/loom-staging-public-entry:http" -m tcp --dport 32080 -j KUBE-EXT-HTTP
EOF
  test "${IPTABLES_RULESET_MODE:-ready}" != "missing-https" && cat <<'EOF'
-A KUBE-NODEPORTS -p tcp -m comment --comment "ingress-nginx/loom-staging-public-entry:https" -m tcp --dport 32443 -j KUBE-EXT-HTTPS
EOF
  exit 0
fi

if [ "$*" = "-S PREROUTING" ]; then
  test -e "$IPTABLES_STATE/kube-443" && printf '%s\\n' '-A PREROUTING -d 192.168.50.103/32 -p tcp -m tcp --dport 443 -j KUBE-EXT-HTTPS'
  test -e "$IPTABLES_STATE/kube-80" && printf '%s\\n' '-A PREROUTING -d 192.168.50.103/32 -p tcp -m tcp --dport 80 -j KUBE-EXT-HTTP'
  exit 0
fi

marker=""
case " $* " in
  *" PREROUTING -d 192.168.50.103/32 -p tcp --dport 443 -j DNAT --to-destination 192.168.50.103:18443 "*) marker="$IPTABLES_STATE/proxy-dnat-443" ;;
  *" PREROUTING -d 192.168.50.103/32 -p tcp --dport 80 -j DNAT --to-destination 192.168.50.103:18080 "*) marker="$IPTABLES_STATE/proxy-dnat-80" ;;
  *" PREROUTING -d 192.168.50.103/32 -p tcp --dport 443 -j KUBE-EXT-HTTPS "*) marker="$IPTABLES_STATE/kube-443" ;;
  *" PREROUTING -d 192.168.50.103/32 -p tcp --dport 80 -j KUBE-EXT-HTTP "*) marker="$IPTABLES_STATE/kube-80" ;;
  *" PREROUTING -d 192.168.50.103/32 -p tcp --dport 443 -j DNAT --to-destination 192.168.50.103:8443 "*) marker="$IPTABLES_STATE/scoped-dnat-443" ;;
  *" PREROUTING -d 192.168.50.103/32 -p tcp --dport 80 -j DNAT --to-destination 192.168.50.103:8080 "*) marker="$IPTABLES_STATE/scoped-dnat-80" ;;
  *" PREROUTING -p tcp --dport 443 -j DNAT --to-destination 192.168.50.103:8443 "*) marker="$IPTABLES_STATE/unscoped-dnat-443" ;;
  *" PREROUTING -p tcp --dport 80 -j DNAT --to-destination 192.168.50.103:8080 "*) marker="$IPTABLES_STATE/unscoped-dnat-80" ;;
esac

test -n "$marker" || exit 96
case " $* " in
  *" -C "*)
    test "${IPTABLES_CHECK_FAILURE:-0}" = "0" || exit 4
    test -e "$marker"
    ;;
  *" -I "*)
    test ! -e "$marker"
    touch "$marker"
    ;;
  *" -D "*)
    test -e "$marker"
    rm "$marker"
    ;;
  *) exit 95 ;;
esac
""",
        encoding="utf-8",
    )
    fake_iptables.chmod(0o755)


def _write_fake_curl(fake_bin: Path) -> None:
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        """#!/bin/sh
set -eu
printf "%s\\n" "$*" >>"$CURL_LOG"
test "${PROXY_PROBE_FAILURE:-0}" = "0"
""",
        encoding="utf-8",
    )
    fake_curl.chmod(0o755)


def _run_cutover(
    tmp_path: Path,
    *,
    ruleset_mode: str = "ready",
    check_failure: bool = False,
    proxy_probe_failure: bool = False,
) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path]:
    curl_log = tmp_path / "curl.log"
    iptables_log = tmp_path / "iptables.log"
    iptables_state = tmp_path / "iptables-state"
    initialize_state = not iptables_state.exists()
    iptables_state.mkdir(exist_ok=True)
    if initialize_state:
        for marker in (
            "kube-443",
            "kube-80",
            "scoped-dnat-443",
            "scoped-dnat-80",
            "unscoped-dnat-443",
            "unscoped-dnat-80",
        ):
            (iptables_state / marker).touch()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(exist_ok=True)
    _write_fake_curl(fake_bin)
    _write_fake_iptables(fake_bin)

    completed = subprocess.run(
        ["/bin/bash", str(CUTOVER_SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "CURL_BIN": str(fake_bin / "curl"),
            "CURL_LOG": str(curl_log),
            "IPTABLES_CHECK_FAILURE": "1" if check_failure else "0",
            "IPTABLES_LOG": str(iptables_log),
            "IPTABLES_RULESET_MODE": ruleset_mode,
            "IPTABLES_STATE": str(iptables_state),
            "K3S_INGRESS_IP": "192.168.50.103",
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "PROXY_PROBE_FAILURE": "1" if proxy_probe_failure else "0",
        },
    )
    return completed, iptables_log, iptables_state, curl_log


def test_public_entry_service_has_pinned_nodeports_for_ingress() -> None:
    service = yaml.safe_load(PUBLIC_ENTRY_SERVICE.read_text(encoding="utf-8"))

    assert service["apiVersion"] == "v1"
    assert service["kind"] == "Service"
    assert service["metadata"] == {
        "name": "loom-staging-public-entry",
        "namespace": "ingress-nginx",
        "labels": {
            "app.kubernetes.io/name": "loom-staging-public-entry",
            "app.kubernetes.io/part-of": "loom",
        },
    }
    assert service["spec"].get("externalIPs") is None
    assert service["spec"]["type"] == "NodePort"
    assert service["spec"]["externalTrafficPolicy"] == "Cluster"
    assert service["spec"]["selector"] == {
        "app.kubernetes.io/component": "controller",
        "app.kubernetes.io/instance": "ingress-nginx",
        "app.kubernetes.io/name": "ingress-nginx",
    }
    assert service["spec"]["ports"] == [
        {
            "appProtocol": "http",
            "name": "http",
            "nodePort": 32080,
            "port": 80,
            "protocol": "TCP",
            "targetPort": "http",
        },
        {
            "appProtocol": "https",
            "name": "https",
            "nodePort": 32443,
            "port": 443,
            "protocol": "TCP",
            "targetPort": "https",
        },
    ]


@pytest.mark.parametrize(
    ("mapping", "expected"),
    [
        (
            "18080-32080",
            "-d TCP-LISTEN:18080,bind=192.168.50.103,reuseaddr,fork "
            "TCP:192.168.50.103:32080,connect-timeout=5",
        ),
        (
            "18443-32443",
            "-d TCP-LISTEN:18443,bind=192.168.50.103,reuseaddr,fork "
            "TCP:192.168.50.103:32443,connect-timeout=5",
        ),
    ],
)
def test_public_proxy_maps_owned_listener_to_nodeport(
    tmp_path: Path,
    mapping: str,
    expected: str,
) -> None:
    socat_log = tmp_path / "socat.log"
    fake_socat = tmp_path / "socat"
    fake_socat.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >"$SOCAT_LOG"\n',
        encoding="utf-8",
    )
    fake_socat.chmod(0o755)

    completed = subprocess.run(
        ["/bin/bash", str(PROXY_SCRIPT), mapping],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "K3S_INGRESS_IP": "192.168.50.103",
            "SOCAT_BIN": str(fake_socat),
            "SOCAT_LOG": str(socat_log),
        },
    )

    assert completed.returncode == 0, completed.stderr
    assert socat_log.read_text(encoding="utf-8").strip() == expected


def test_cutover_installs_proxy_routes_before_removing_legacy_routes(
    tmp_path: Path,
) -> None:
    first, iptables_log, iptables_state, curl_log = _run_cutover(tmp_path)
    second, _, _, _ = _run_cutover(tmp_path)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert sorted(path.name for path in iptables_state.iterdir()) == [
        "proxy-dnat-443",
        "proxy-dnat-80",
    ]
    commands = iptables_log.read_text(encoding="utf-8").splitlines()
    assert commands
    assert all(command.startswith("--wait -t nat ") for command in commands)
    insertions = [command for command in commands if " -I PREROUTING " in command]
    assert insertions == [
        "--wait -t nat -I PREROUTING -d 192.168.50.103/32 -p tcp "
        "--dport 443 -j DNAT --to-destination 192.168.50.103:18443",
        "--wait -t nat -I PREROUTING -d 192.168.50.103/32 -p tcp "
        "--dport 80 -j DNAT --to-destination 192.168.50.103:18080",
    ]
    mutations = [
        command
        for command in commands
        if " -I PREROUTING " in command or " -D PREROUTING " in command
    ]
    assert mutations[:2] == insertions
    assert len([command for command in mutations if " -D PREROUTING " in command]) == 6
    assert not any(" -I PREROUTING " in command and " -j KUBE-" in command for command in commands)
    probes = curl_log.read_text(encoding="utf-8").splitlines()
    https_probe = next(
        probe
        for probe in probes
        if "https://yylx.world:18443/staging/api/v1/health" in probe
    )
    assert "--insecure" in https_probe.split()
    assert any("http://yylx.world:18080/staging/" in probe for probe in probes)


def test_cutover_preserves_host_routes_until_both_nodeports_exist(
    tmp_path: Path,
) -> None:
    completed, iptables_log, iptables_state, _ = _run_cutover(
        tmp_path,
        ruleset_mode="missing-https",
    )

    assert completed.returncode != 0
    assert sorted(path.name for path in iptables_state.iterdir()) == [
        "kube-443",
        "kube-80",
        "scoped-dnat-443",
        "scoped-dnat-80",
        "unscoped-dnat-443",
        "unscoped-dnat-80",
    ]
    assert iptables_log.read_text(encoding="utf-8").splitlines() == [
        "--wait -t nat -S KUBE-NODEPORTS"
    ]


def test_cutover_preserves_host_routes_when_proxy_readiness_fails(
    tmp_path: Path,
) -> None:
    completed, iptables_log, iptables_state, curl_log = _run_cutover(
        tmp_path,
        proxy_probe_failure=True,
    )

    assert completed.returncode != 0
    assert sorted(path.name for path in iptables_state.iterdir()) == [
        "kube-443",
        "kube-80",
        "scoped-dnat-443",
        "scoped-dnat-80",
        "unscoped-dnat-443",
        "unscoped-dnat-80",
    ]
    assert iptables_log.read_text(encoding="utf-8").splitlines() == [
        "--wait -t nat -S KUBE-NODEPORTS"
    ]
    assert curl_log.read_text(encoding="utf-8").splitlines()


def test_cutover_propagates_iptables_check_resource_failure(
    tmp_path: Path,
) -> None:
    completed, iptables_log, iptables_state, _ = _run_cutover(
        tmp_path,
        check_failure=True,
    )

    assert completed.returncode == 4
    assert not (iptables_state / "proxy-dnat-443").exists()
    commands = iptables_log.read_text(encoding="utf-8").splitlines()
    assert any(" -C PREROUTING " in command for command in commands)
    assert not any(" -I PREROUTING " in command or " -D PREROUTING " in command for command in commands)


def test_public_proxy_and_cutover_units_reconcile_with_k3s() -> None:
    proxy = configparser.ConfigParser(interpolation=None)
    proxy.optionxform = str
    proxy.read(PROXY_UNIT, encoding="utf-8")
    assert "k3s.service" in proxy["Unit"]["After"].split()
    assert "k3s.service" in proxy["Unit"]["PartOf"].split()
    assert proxy["Service"]["ExecStart"] == (
        "/usr/local/sbin/loom-staging-public-proxy.sh %i"
    )
    assert proxy["Service"]["Restart"] == "always"
    assert "k3s.service" in proxy["Install"]["WantedBy"].split()

    cutover = configparser.ConfigParser(interpolation=None)
    cutover.optionxform = str
    cutover.read(CUTOVER_UNIT, encoding="utf-8")
    required = set(cutover["Unit"]["Requires"].split())
    assert required == {
        "loom-staging-public-proxy@18080-32080.service",
        "loom-staging-public-proxy@18443-32443.service",
    }
    assert required.issubset(set(cutover["Unit"]["After"].split()))
    assert "k3s.service" in cutover["Unit"]["PartOf"].split()
    assert "docker.service" in cutover["Unit"]["After"].split()
    assert "docker.service" in cutover["Unit"]["PartOf"].split()
    assert cutover["Service"]["Restart"] == "on-failure"
    assert "k3s.service" in cutover["Install"]["WantedBy"].split()
    assert "docker.service" in cutover["Install"]["WantedBy"].split()
