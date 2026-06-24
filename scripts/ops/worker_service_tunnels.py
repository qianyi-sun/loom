#!/usr/bin/env python3
"""Manage private service tunnels used by remote Loom workers.

The public HTTPS entrypoint intentionally does not expose Control Plane,
Gateway, or MinIO internals. Remote workers still need private access to those
services, so operators can run these tunnels under systemd instead of ad-hoc
terminal-owned port-forward processes.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import urlopen


@dataclass(frozen=True)
class TunnelSpec:
    name: str
    service_name: str
    local_port: int
    service_port: int
    health_env: str
    health_path: str


@dataclass(frozen=True)
class ProbeResult:
    name: str
    url: str
    ok: bool
    detail: str


DEFAULT_TUNNELS: tuple[TunnelSpec, ...] = (
    TunnelSpec(
        name="control-plane",
        service_name="loom-control-plane",
        local_port=18081,
        service_port=8080,
        health_env="LOOM_WORKER_CONTROL_PLANE_URL",
        health_path="/healthz",
    ),
    TunnelSpec(
        name="gateway",
        service_name="loom-llm-gateway",
        local_port=19100,
        service_port=9100,
        health_env="LOOM_WORKER_GATEWAY_URL",
        health_path="/healthz",
    ),
    TunnelSpec(
        name="minio",
        service_name="loom-minio",
        local_port=19000,
        service_port=9000,
        health_env="LOOM_WORKER_MINIO_ENDPOINT",
        health_path="/minio/health/live",
    ),
)


def unit_name(spec: TunnelSpec) -> str:
    return f"loom-remote-worker-tunnel-{spec.name}.service"


def render_systemd_unit(
    spec: TunnelSpec,
    *,
    namespace: str,
    kubectl: str,
    kubeconfig: str,
    address: str,
) -> str:
    command = [
        kubectl,
        "--kubeconfig",
        kubeconfig,
        "-n",
        namespace,
        "port-forward",
        "--address",
        address,
        f"svc/{spec.service_name}",
        f"{spec.local_port}:{spec.service_port}",
    ]
    exec_start = " ".join(shlex.quote(part) for part in command)
    return "\n".join(
        [
            "[Unit]",
            f"Description=Loom remote-worker tunnel: {spec.name}",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            "Restart=always",
            "RestartSec=5",
            f"ExecStart={exec_start}",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def write_systemd_units(
    output_dir: Path,
    *,
    namespace: str,
    kubectl: str,
    kubeconfig: str,
    address: str,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for spec in DEFAULT_TUNNELS:
        path = output_dir / unit_name(spec)
        path.write_text(
            render_systemd_unit(
                spec,
                namespace=namespace,
                kubectl=kubectl,
                kubeconfig=kubeconfig,
                address=address,
            ),
            encoding="utf-8",
        )
        written.append(path)
    return written


def _is_volatile_path(path: str) -> bool:
    expanded = Path(path).expanduser()
    if not expanded.is_absolute():
        return False
    parts = expanded.parts
    return parts[:2] == ("/", "tmp") or parts[:3] in {
        ("/", "var", "tmp"),
        ("/", "var", "run"),
    }


def validate_install_paths(
    *,
    kubectl: str,
    kubeconfig: str,
    allow_volatile_paths: bool,
) -> None:
    if allow_volatile_paths:
        return
    volatile = [
        name
        for name, value in (("kubectl", kubectl), ("kubeconfig", kubeconfig))
        if _is_volatile_path(value)
    ]
    if volatile:
        joined = ", ".join(volatile)
        raise ValueError(
            f"{joined} must use a durable path for install-systemd; "
            "copy runtime files out of /tmp or pass --allow-volatile-paths "
            "for a disposable test only.",
        )


def load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in {"'", '"'}
        ):
            value = value[1:-1]
        env[key] = value
    return env


def _root_url(raw_url: str) -> str:
    parsed = urlsplit(raw_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"expected absolute URL, got {raw_url!r}")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _join_url(root: str, path: str) -> str:
    return root.rstrip("/") + "/" + path.lstrip("/")


def worker_health_urls(env: dict[str, str]) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    for spec in DEFAULT_TUNNELS:
        value = env.get(spec.health_env)
        if not value:
            raise KeyError(f"missing {spec.health_env}")
        urls.append((spec.name, _join_url(_root_url(value), spec.health_path)))
    return urls


def render_remote_healthcheck_script(env: dict[str, str]) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "probe() {",
        "  local name=$1",
        "  local url=$2",
        '  if curl -fsS --max-time "${LOOM_TUNNEL_PROBE_TIMEOUT:-5}" "$url" >/dev/null; then',
        '    echo "$name=ok"',
        "  else",
        '    echo "$name=failed"',
        "    return 1",
        "  fi",
        "}",
        "",
    ]
    for name, url in worker_health_urls(env):
        lines.append(f"probe {shlex.quote(name)} {shlex.quote(url)}")
    lines.append("")
    return "\n".join(lines)


def probe_health_urls(
    urls: list[tuple[str, str]],
    *,
    timeout_sec: float,
) -> list[ProbeResult]:
    results: list[ProbeResult] = []
    for name, url in urls:
        try:
            with urlopen(url, timeout=timeout_sec) as response:
                status = getattr(response, "status", 200)
                ok = 200 <= int(status) < 300
                detail = f"http_status={status}"
        except (OSError, URLError, TimeoutError) as exc:
            ok = False
            detail = exc.__class__.__name__
        results.append(ProbeResult(name=name, url=url, ok=ok, detail=detail))
    return results


def _iter_hosts(hostfile: Path) -> list[str]:
    hosts: list[str] = []
    for raw_line in hostfile.read_text(encoding="utf-8").splitlines():
        host = raw_line.split("#", 1)[0].strip()
        if host:
            hosts.append(host)
    return hosts


def _run_check(args: argparse.Namespace) -> int:
    env = load_env_file(Path(args.env_file))
    results = probe_health_urls(
        worker_health_urls(env),
        timeout_sec=args.timeout_sec,
    )
    failed = False
    for result in results:
        status = "ok" if result.ok else "failed"
        print(f"{result.name}={status} url={result.url} {result.detail}")
        failed = failed or not result.ok
    return 1 if failed else 0


def _run_check_remote(args: argparse.Namespace) -> int:
    env = load_env_file(Path(args.env_file))
    snippet = render_remote_healthcheck_script(env)
    hosts = _iter_hosts(Path(args.hostfile))
    failed = False
    ssh_timeout = str(args.ssh_timeout_sec)
    ssh_opts = shlex.split(os.environ.get("SSH_OPTS", ""))
    for host in hosts:
        print(f"## {host}")
        command = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={ssh_timeout}",
            *ssh_opts,
            host,
            "bash -s",
        ]
        completed = subprocess.run(
            command,
            input=snippet,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            failed = True
    return 1 if failed else 0


def _run_print_check_script(args: argparse.Namespace) -> int:
    env = load_env_file(Path(args.env_file))
    print(render_remote_healthcheck_script(env), end="")
    return 0


def _run_render_systemd(args: argparse.Namespace) -> int:
    written = write_systemd_units(
        Path(args.output_dir),
        namespace=args.namespace,
        kubectl=args.kubectl,
        kubeconfig=args.kubeconfig,
        address=args.address,
    )
    for path in written:
        print(path)
    return 0


def _run_install_systemd(args: argparse.Namespace) -> int:
    validate_install_paths(
        kubectl=args.kubectl,
        kubeconfig=args.kubeconfig,
        allow_volatile_paths=args.allow_volatile_paths,
    )
    unit_dir = Path(args.output_dir).expanduser()
    written = write_systemd_units(
        unit_dir,
        namespace=args.namespace,
        kubectl=args.kubectl,
        kubeconfig=args.kubeconfig,
        address=args.address,
    )
    unit_names = [path.name for path in written]
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", *unit_names], check=True)
    subprocess.run(["systemctl", "--user", "restart", *unit_names], check=True)
    for name in unit_names:
        print(f"enabled {name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage private service tunnels for remote Loom workers.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    render = subparsers.add_parser(
        "render-systemd",
        help="Render systemd user units without installing them.",
    )
    render.add_argument("--output-dir", required=True)
    render.add_argument("--namespace", default="loom-public-beta")
    render.add_argument("--kubectl", default="kubectl")
    render.add_argument("--kubeconfig", required=True)
    render.add_argument("--address", default="0.0.0.0")
    render.set_defaults(func=_run_render_systemd)

    install = subparsers.add_parser(
        "install-systemd",
        help="Install, enable, and start systemd user units.",
    )
    install.add_argument(
        "--output-dir",
        default="~/.config/systemd/user",
    )
    install.add_argument("--namespace", default="loom-public-beta")
    install.add_argument("--kubectl", default="kubectl")
    install.add_argument("--kubeconfig", required=True)
    install.add_argument("--address", default="0.0.0.0")
    install.add_argument(
        "--allow-volatile-paths",
        action="store_true",
        help="Allow /tmp-style kubectl or kubeconfig paths for disposable tests.",
    )
    install.set_defaults(func=_run_install_systemd)

    check = subparsers.add_parser(
        "check",
        help="Probe worker-facing tunnel URLs from the current host.",
    )
    check.add_argument("--env-file", required=True)
    check.add_argument("--timeout-sec", type=float, default=5)
    check.set_defaults(func=_run_check)

    remote = subparsers.add_parser(
        "check-remote",
        help="Probe worker-facing tunnel URLs from SSH worker hosts.",
    )
    remote.add_argument("hostfile")
    remote.add_argument("--env-file", required=True)
    remote.add_argument("--ssh-timeout-sec", type=int, default=5)
    remote.set_defaults(func=_run_check_remote)

    print_script = subparsers.add_parser(
        "print-check-script",
        help="Print a secret-free worker healthcheck script for schedulers such as Slurm.",
    )
    print_script.add_argument("--env-file", required=True)
    print_script.set_defaults(func=_run_print_check_script)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
