#!/usr/bin/env python3
"""Manage private service tunnels used by remote Loom workers.

The public HTTPS entrypoint intentionally does not expose Control Plane,
Gateway, or MinIO internals. Remote workers still need private access to those
services, so operators can run these tunnels under systemd instead of ad-hoc
terminal-owned port-forward processes.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
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

_SUBPROCESS_GATEWAY_ENV = "LOOM_WORKER_SUBPROCESS_GATEWAY_URL"
_SUBPROCESS_GATEWAY_TUNNEL_NAME = "subprocess-gateway"
_HOST_GATEWAY_NAMES = {"host.docker.internal"}
_RESTART_TUNNEL_BY_PROBE_NAME = {
    _SUBPROCESS_GATEWAY_TUNNEL_NAME: "gateway",
}


def unit_name(spec: TunnelSpec) -> str:
    return f"loom-remote-worker-tunnel-{spec.name}.service"


def unit_name_for_tunnel_name(name: str) -> str:
    known = {spec.name for spec in DEFAULT_TUNNELS} | {
        _SUBPROCESS_GATEWAY_TUNNEL_NAME,
    }
    if name not in known:
        raise KeyError(f"unknown tunnel {name!r}; expected one of {sorted(known)}")
    return f"loom-remote-worker-tunnel-{name}.service"


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


def watchdog_service_name() -> str:
    return "loom-remote-worker-tunnel-watchdog.service"


def watchdog_timer_name() -> str:
    return "loom-remote-worker-tunnel-watchdog.timer"


def render_watchdog_systemd_service(
    *,
    script_path: str,
    env_file: str,
    state_file: str,
    timeout_sec: float,
    failure_threshold: int,
) -> str:
    command = [
        script_path,
        "watchdog",
        "--env-file",
        env_file,
        "--state-file",
        state_file,
        "--timeout-sec",
        str(timeout_sec).removesuffix(".0"),
        "--failure-threshold",
        str(failure_threshold),
    ]
    exec_start = " ".join(shlex.quote(part) for part in command)
    return "\n".join(
        [
            "[Unit]",
            "Description=Loom remote-worker tunnel watchdog",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=oneshot",
            f"ExecStart={exec_start}",
            "",
        ]
    )


def render_watchdog_systemd_timer(*, interval_sec: int) -> str:
    if interval_sec < 1:
        raise ValueError("interval_sec must be >= 1")
    interval = str(interval_sec)
    return "\n".join(
        [
            "[Unit]",
            "Description=Run Loom remote-worker tunnel watchdog",
            "",
            "[Timer]",
            f"OnBootSec={interval}",
            f"OnUnitActiveSec={interval}",
            "AccuracySec=5",
            f"Unit={watchdog_service_name()}",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def write_watchdog_systemd_units(
    output_dir: Path,
    *,
    script_path: str,
    env_file: str,
    state_file: str,
    timeout_sec: float,
    failure_threshold: int,
    interval_sec: int,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    service = output_dir / watchdog_service_name()
    timer = output_dir / watchdog_timer_name()
    service.write_text(
        render_watchdog_systemd_service(
            script_path=script_path,
            env_file=env_file,
            state_file=state_file,
            timeout_sec=timeout_sec,
            failure_threshold=failure_threshold,
        ),
        encoding="utf-8",
    )
    timer.write_text(
        render_watchdog_systemd_timer(interval_sec=interval_sec),
        encoding="utf-8",
    )
    return [service, timer]


def write_systemd_units(
    output_dir: Path,
    *,
    namespace: str,
    kubectl: str,
    kubeconfig: str,
    address: str,
    local_port_overrides: dict[str, int] | None = None,
    subprocess_gateway_local_port: int | None = None,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    specs = _systemd_tunnels(
        local_port_overrides=local_port_overrides or {},
        subprocess_gateway_local_port=subprocess_gateway_local_port,
    )
    for spec in specs:
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


def _systemd_tunnels(
    *,
    local_port_overrides: dict[str, int],
    subprocess_gateway_local_port: int | None,
) -> list[TunnelSpec]:
    specs = _apply_local_port_overrides(DEFAULT_TUNNELS, local_port_overrides)
    if subprocess_gateway_local_port is None:
        return specs
    if subprocess_gateway_local_port < 1 or subprocess_gateway_local_port > 65535:
        raise ValueError("subprocess-gateway local port must be in 1..65535")
    used_ports = {spec.local_port for spec in specs}
    if subprocess_gateway_local_port in used_ports:
        raise ValueError(
            "subprocess-gateway local port must be distinct from existing tunnels",
        )
    specs.append(
        TunnelSpec(
            name=_SUBPROCESS_GATEWAY_TUNNEL_NAME,
            service_name="loom-llm-gateway",
            local_port=subprocess_gateway_local_port,
            service_port=9100,
            health_env=_SUBPROCESS_GATEWAY_ENV,
            health_path="/healthz",
        )
    )
    return specs


def _apply_local_port_overrides(
    specs: tuple[TunnelSpec, ...],
    overrides: dict[str, int],
) -> list[TunnelSpec]:
    known = {spec.name for spec in specs}
    unknown = sorted(set(overrides) - known)
    if unknown:
        raise KeyError(f"unknown tunnel local-port override(s): {unknown}")
    rendered: list[TunnelSpec] = []
    for spec in specs:
        local_port = overrides.get(spec.name)
        if local_port is None:
            rendered.append(spec)
            continue
        if local_port < 1 or local_port > 65535:
            raise ValueError(f"{spec.name} local port must be in 1..65535")
        rendered.append(replace(spec, local_port=local_port))
    return rendered


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


def _host_side_root_url(raw_url: str) -> str:
    parsed = urlsplit(raw_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"expected absolute URL, got {raw_url!r}")
    if parsed.hostname in _HOST_GATEWAY_NAMES:
        port = f":{parsed.port}" if parsed.port else ""
        return urlunsplit((parsed.scheme, f"127.0.0.1{port}", "", "", ""))
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _join_url(root: str, path: str) -> str:
    return root.rstrip("/") + "/" + path.lstrip("/")


def _url_port(raw_url: str) -> int | None:
    parsed = urlsplit(raw_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"expected absolute URL, got {raw_url!r}")
    if parsed.port is not None:
        return parsed.port
    if parsed.scheme == "http":
        return 80
    if parsed.scheme == "https":
        return 443
    return None


def _restart_tunnel_by_probe_name_for_env(env: dict[str, str]) -> dict[str, str]:
    mapping = dict(_RESTART_TUNNEL_BY_PROBE_NAME)
    subprocess_gateway = env.get(_SUBPROCESS_GATEWAY_ENV)
    gateway = env.get("LOOM_WORKER_GATEWAY_URL")
    if not subprocess_gateway or not gateway:
        return mapping
    if _url_port(subprocess_gateway) != _url_port(gateway):
        mapping[_SUBPROCESS_GATEWAY_TUNNEL_NAME] = _SUBPROCESS_GATEWAY_TUNNEL_NAME
    return mapping


def worker_health_urls(env: dict[str, str]) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    for spec in DEFAULT_TUNNELS:
        value = env.get(spec.health_env)
        if not value:
            raise KeyError(f"missing {spec.health_env}")
        urls.append((spec.name, _join_url(_root_url(value), spec.health_path)))
        if spec.name == "gateway":
            subprocess_gateway = env.get(_SUBPROCESS_GATEWAY_ENV)
            if subprocess_gateway:
                urls.append((
                    "subprocess-gateway",
                    _join_url(_host_side_root_url(subprocess_gateway), "/healthz"),
                ))
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
                # #18 fix: a stale `kubectl port-forward` can still
                # accept the TCP connection and return HTTP 200 with a
                # zero-byte body (or immediately FIN before writing a
                # body). Reading the response and requiring non-empty
                # content catches the stale-tunnel case that the pure
                # status-code check would silently accept as healthy.
                try:
                    body = response.read(1024)
                except (OSError, TimeoutError) as read_exc:
                    ok = False
                    detail = (
                        f"http_status={status} read_error="
                        f"{read_exc.__class__.__name__}"
                    )
                else:
                    status_ok = 200 <= int(status) < 300
                    if not status_ok:
                        ok = False
                        detail = f"http_status={status}"
                    elif not body:
                        ok = False
                        detail = f"http_status={status} empty_response"
                    else:
                        ok = True
                        detail = f"http_status={status}"
        except (OSError, URLError, TimeoutError) as exc:
            ok = False
            detail = exc.__class__.__name__
        results.append(ProbeResult(name=name, url=url, ok=ok, detail=detail))
    return results


def _load_watchdog_state(state_file: Path) -> dict[str, int]:
    if not state_file.exists():
        return {}
    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    state: dict[str, int] = {}
    for key, value in raw.items():
        try:
            state[str(key)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return state


def _write_watchdog_state(state_file: Path, state: dict[str, int]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(state_file)


def _evidence_diagnostic(
    code: str,
    message: str,
    **details: str,
) -> dict[str, str]:
    return {"code": code, "message": message, **details}


def _read_first_exec_start(unit_text: str) -> str | None:
    for raw_line in unit_text.splitlines():
        line = raw_line.strip()
        if line.startswith("ExecStart="):
            return line.removeprefix("ExecStart=").strip()
    return None


def _parse_watchdog_exec_start(exec_start: str | None) -> tuple[str | None, str | None]:
    if not exec_start:
        return None, None
    try:
        parts = shlex.split(exec_start)
    except ValueError:
        return None, None
    if not parts:
        return None, None
    script_path = parts[0]
    env_file: str | None = None
    for index, part in enumerate(parts):
        if part == "--env-file" and index + 1 < len(parts):
            env_file = parts[index + 1]
            break
        if part.startswith("--env-file="):
            env_file = part.split("=", 1)[1]
            break
    return script_path, env_file


def collect_watchdog_evidence(
    *,
    unit_dir: Path,
    expected_script_path: str | None = None,
    timer_active: str | None = None,
) -> dict[str, Any]:
    service_path = unit_dir.expanduser() / watchdog_service_name()
    timer_path = unit_dir.expanduser() / watchdog_timer_name()
    diagnostics: list[dict[str, str]] = []

    service_text = ""
    if service_path.exists():
        service_text = service_path.read_text(encoding="utf-8")
    else:
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_service_missing",
                "Watchdog service unit was not found.",
                path=str(service_path),
            )
        )

    if not timer_path.exists():
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_timer_missing",
                "Watchdog timer unit was not found.",
                path=str(timer_path),
            )
        )

    exec_start = _read_first_exec_start(service_text)
    script_path, env_file = _parse_watchdog_exec_start(exec_start)
    if exec_start is None and service_path.exists():
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_exec_start_missing",
                "Watchdog service unit does not contain ExecStart.",
                path=str(service_path),
            )
        )
    if env_file is None and service_path.exists():
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_env_file_unresolved",
                "Watchdog unit ExecStart does not include --env-file.",
                path=str(service_path),
            )
        )
    if (
        expected_script_path
        and script_path is not None
        and script_path != expected_script_path
    ):
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_script_path_drift",
                "Watchdog unit ExecStart points at a different script path.",
                expected_script_path=expected_script_path,
                actual_script_path=script_path,
            )
        )
    if timer_active is not None and timer_active != "active":
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_timer_inactive",
                "Watchdog timer is not active.",
                active_state=timer_active,
            )
        )

    env_path = Path(env_file).expanduser() if env_file else None
    if env_path is not None and not env_path.exists():
        diagnostics.append(
            _evidence_diagnostic(
                "watchdog_env_file_missing",
                "Watchdog unit --env-file path does not exist.",
                path=str(env_path),
            )
        )
    return {
        "schema_version": 1,
        "ok": not diagnostics,
        "service": {
            "name": watchdog_service_name(),
            "path": str(service_path),
            "exists": service_path.exists(),
            "script_path": script_path,
        },
        "timer": {
            "name": watchdog_timer_name(),
            "path": str(timer_path),
            "exists": timer_path.exists(),
            "active_state": timer_active,
        },
        "env_file": {
            "path": str(env_path) if env_path is not None else None,
            "exists": env_path.exists() if env_path is not None else False,
        },
        "diagnostics": diagnostics,
    }


def apply_watchdog_results(
    results: list[ProbeResult],
    *,
    state_file: Path,
    failure_threshold: int,
    restart_tunnel_by_probe_name: dict[str, str] | None = None,
) -> list[str]:
    if failure_threshold < 1:
        raise ValueError("failure_threshold must be >= 1")

    state = _load_watchdog_state(state_file)
    restart_map = restart_tunnel_by_probe_name or _RESTART_TUNNEL_BY_PROBE_NAME
    restarted: list[str] = []
    restarted_units: set[str] = set()
    for result in results:
        if result.ok:
            state[result.name] = 0
            continue
        failures = state.get(result.name, 0) + 1
        state[result.name] = failures
        if failures < failure_threshold:
            continue
        tunnel_name = restart_map.get(result.name, result.name)
        unit = unit_name_for_tunnel_name(tunnel_name)
        if unit not in restarted_units:
            subprocess.run(["systemctl", "--user", "restart", unit], check=True)
            restarted.append(unit)
            restarted_units.add(unit)
        state[result.name] = 0

    _write_watchdog_state(state_file, state)
    return restarted


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


def _run_watchdog(args: argparse.Namespace) -> int:
    env = load_env_file(Path(args.env_file))
    results = probe_health_urls(
        worker_health_urls(env),
        timeout_sec=args.timeout_sec,
    )
    restarted = apply_watchdog_results(
        results,
        state_file=Path(args.state_file).expanduser(),
        failure_threshold=args.failure_threshold,
        restart_tunnel_by_probe_name=_restart_tunnel_by_probe_name_for_env(env),
    )
    for result in results:
        status = "ok" if result.ok else "failed"
        print(f"{result.name}={status} url={result.url} {result.detail}")
    for unit in restarted:
        print(f"restarted={unit}")
    # `check` is the rollout gate that exits non-zero on failed probes. The
    # watchdog is a periodic self-healer; after it records state or restarts a
    # unit successfully, the systemd oneshot should not remain failed merely
    # because the pre-restart probe saw the stale tunnel.
    return 0


def _systemctl_user_is_active(unit: str) -> str:
    completed = subprocess.run(
        ["systemctl", "--user", "is-active", unit],
        check=False,
        text=True,
        capture_output=True,
    )
    state = completed.stdout.strip()
    if state:
        return state
    return "unknown"


def _run_watchdog_evidence(args: argparse.Namespace) -> int:
    timer_active = args.timer_active
    if timer_active is None:
        timer_active = _systemctl_user_is_active(watchdog_timer_name())
    evidence = collect_watchdog_evidence(
        unit_dir=Path(args.unit_dir),
        expected_script_path=args.expected_script_path,
        timer_active=timer_active,
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))
    return 0 if evidence["ok"] else 1


def _run_render_systemd(args: argparse.Namespace) -> int:
    written = write_systemd_units(
        Path(args.output_dir),
        namespace=args.namespace,
        kubectl=args.kubectl,
        kubeconfig=args.kubeconfig,
        address=args.address,
        local_port_overrides=_local_port_overrides_from_args(args),
        subprocess_gateway_local_port=getattr(args, "subprocess_gateway_local_port", None),
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
        local_port_overrides=_local_port_overrides_from_args(args),
        subprocess_gateway_local_port=getattr(args, "subprocess_gateway_local_port", None),
    )
    unit_names = [path.name for path in written]
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", *unit_names], check=True)
    subprocess.run(["systemctl", "--user", "restart", *unit_names], check=True)
    for name in unit_names:
        print(f"enabled {name}")
    return 0


def _local_port_overrides_from_args(args: argparse.Namespace) -> dict[str, int]:
    gateway_local_port = getattr(args, "gateway_local_port", None)
    if gateway_local_port is None:
        return {}
    return {"gateway": gateway_local_port}


def _run_render_watchdog_systemd(args: argparse.Namespace) -> int:
    written = write_watchdog_systemd_units(
        Path(args.output_dir),
        script_path=args.script_path,
        env_file=args.env_file,
        state_file=args.state_file,
        timeout_sec=args.timeout_sec,
        failure_threshold=args.failure_threshold,
        interval_sec=args.interval_sec,
    )
    for path in written:
        print(path)
    return 0


def _run_install_watchdog_systemd(args: argparse.Namespace) -> int:
    unit_dir = Path(args.output_dir).expanduser()
    written = write_watchdog_systemd_units(
        unit_dir,
        script_path=args.script_path,
        env_file=args.env_file,
        state_file=args.state_file,
        timeout_sec=args.timeout_sec,
        failure_threshold=args.failure_threshold,
        interval_sec=args.interval_sec,
    )
    names = [path.name for path in written]
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "--user", "enable", "--now", watchdog_timer_name()], check=True)
    for name in names:
        print(f"installed {name}")
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
    render.add_argument("--namespace", default="loom-staging")
    render.add_argument("--kubectl", default="kubectl")
    render.add_argument("--kubeconfig", required=True)
    render.add_argument("--address", default="0.0.0.0")
    render.add_argument(
        "--gateway-local-port",
        type=int,
        help="Override the local port for the Gateway tunnel, for example 30444.",
    )
    render.add_argument(
        "--subprocess-gateway-local-port",
        type=int,
        help=(
            "Add a distinct Gateway tunnel for sandbox-facing subprocess agent "
            "traffic, for example 30444 while the worker Gateway remains 19100."
        ),
    )
    render.set_defaults(func=_run_render_systemd)

    install = subparsers.add_parser(
        "install-systemd",
        help="Install, enable, and start systemd user units.",
    )
    install.add_argument(
        "--output-dir",
        default="~/.config/systemd/user",
    )
    install.add_argument("--namespace", default="loom-staging")
    install.add_argument("--kubectl", default="kubectl")
    install.add_argument("--kubeconfig", required=True)
    install.add_argument("--address", default="0.0.0.0")
    install.add_argument(
        "--gateway-local-port",
        type=int,
        help="Override the local port for the Gateway tunnel, for example 30444.",
    )
    install.add_argument(
        "--subprocess-gateway-local-port",
        type=int,
        help=(
            "Add a distinct Gateway tunnel for sandbox-facing subprocess agent "
            "traffic, for example 30444 while the worker Gateway remains 19100."
        ),
    )
    install.add_argument(
        "--allow-volatile-paths",
        action="store_true",
        help="Allow /tmp-style kubectl or kubeconfig paths for disposable tests.",
    )
    install.set_defaults(func=_run_install_systemd)

    render_watchdog = subparsers.add_parser(
        "render-watchdog-systemd",
        help="Render a systemd user service/timer for tunnel health self-healing.",
    )
    render_watchdog.add_argument("--output-dir", required=True)
    render_watchdog.add_argument("--env-file", required=True)
    render_watchdog.add_argument(
        "--script-path",
        default=str(Path(__file__).resolve()),
    )
    render_watchdog.add_argument(
        "--state-file",
        default="~/.local/state/loom/remote-worker-tunnel-watchdog.json",
    )
    render_watchdog.add_argument("--timeout-sec", type=float, default=5)
    render_watchdog.add_argument("--failure-threshold", type=int, default=3)
    render_watchdog.add_argument("--interval-sec", type=int, default=30)
    render_watchdog.set_defaults(func=_run_render_watchdog_systemd)

    install_watchdog = subparsers.add_parser(
        "install-watchdog-systemd",
        help="Install and enable the systemd user tunnel watchdog timer.",
    )
    install_watchdog.add_argument(
        "--output-dir",
        default="~/.config/systemd/user",
    )
    install_watchdog.add_argument("--env-file", required=True)
    install_watchdog.add_argument(
        "--script-path",
        default=str(Path(__file__).resolve()),
    )
    install_watchdog.add_argument(
        "--state-file",
        default="~/.local/state/loom/remote-worker-tunnel-watchdog.json",
    )
    install_watchdog.add_argument("--timeout-sec", type=float, default=5)
    install_watchdog.add_argument("--failure-threshold", type=int, default=3)
    install_watchdog.add_argument("--interval-sec", type=int, default=30)
    install_watchdog.set_defaults(func=_run_install_watchdog_systemd)

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

    watchdog = subparsers.add_parser(
        "watchdog",
        help=(
            "Probe worker-facing tunnel URLs and restart stale systemd "
            "tunnel units after repeated failures."
        ),
    )
    watchdog.add_argument("--env-file", required=True)
    watchdog.add_argument(
        "--state-file",
        default="~/.local/state/loom/remote-worker-tunnel-watchdog.json",
    )
    watchdog.add_argument("--timeout-sec", type=float, default=5)
    watchdog.add_argument("--failure-threshold", type=int, default=3)
    watchdog.set_defaults(func=_run_watchdog)

    watchdog_evidence = subparsers.add_parser(
        "watchdog-evidence",
        help=(
            "Print secret-free JSON evidence for the watchdog unit path, "
            "resolved env-file path, and timer state."
        ),
    )
    watchdog_evidence.add_argument(
        "--unit-dir",
        default="~/.config/systemd/user",
        help="systemd user unit directory to inspect.",
    )
    watchdog_evidence.add_argument(
        "--expected-script-path",
        default=None,
        help="Expected durable worker_service_tunnels.py path for drift checks.",
    )
    watchdog_evidence.add_argument(
        "--timer-active",
        default=None,
        help=(
            "Override timer active state for dry-run tests. If omitted, "
            "`systemctl --user is-active` is used."
        ),
    )
    watchdog_evidence.set_defaults(func=_run_watchdog_evidence)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
