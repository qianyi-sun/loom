#!/usr/bin/env python3
"""Run a staging validation command inside a bounded GB10 worker lease.

The worker-capacity manifest is the release contract. This runner is the live
operator wrapper for long staging validations: it activates the declared GB10
hosts, starts the host-local node-agent, waits for fresh workers, runs an
optional validation command, then drains or stops staging capacity on exit.

It intentionally does not submit Loom batches or inspect business artifacts.
Those remain normal user/API/CLI validation steps.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from loom_cli.secret_source import (
    SecretSourceError,
    resolve_secret_source,
    secret_source_argparse_type,
)

FULL_GB10_HOSTS = tuple(f"trt-gb10-{i}" for i in range(1, 16))
TEMPORARILY_EXCLUDED_HOSTS = frozenset({"trt-gb10-7"})
DEFAULT_HOSTS = tuple(host for host in FULL_GB10_HOSTS if host not in TEMPORARILY_EXCLUDED_HOSTS)
DEFAULT_NODE_AGENT_SERVICE = "loom-gb10-node-agent.service"
SECRET_PATTERNS = (
    re.compile(r"\b[Bb]earer\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"\bloom_(?:api|w|admin)_[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9._~+/=-]+"),
    re.compile(r"\bhf_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)(token|secret|password|api[_-]?key|credential)=\S+"),
)


@dataclass(frozen=True)
class PhaseResult:
    phase: str
    ok: bool
    artifact: str | None = None
    detail: str | None = None


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def redact_text(value: str) -> str:
    redacted = value
    for pattern in SECRET_PATTERNS:
        redacted = pattern.sub("<redacted>", redacted)
    return redacted


def _redact_json(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            key_str = str(key)
            if re.search(r"token|secret|password|credential|api[_-]?key", key_str, re.I):
                out[key_str] = "<redacted>"
            else:
                out[key_str] = _redact_json(child)
        return out
    if isinstance(value, list):
        return [_redact_json(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def _write_json(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_redact_json(dict(data)), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _parse_ttl(value: str) -> int:
    raw = value.strip().lower()
    match = re.fullmatch(r"(\d+)([smhd]?)", raw)
    if not match:
        raise argparse.ArgumentTypeError("TTL must be an integer with optional s/m/h/d suffix")
    amount = int(match.group(1))
    unit = match.group(2) or "s"
    multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    seconds = amount * multiplier
    if seconds <= 0:
        raise argparse.ArgumentTypeError("TTL must be positive")
    return seconds


def _host_list(value: str | None) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_HOSTS
    hosts = tuple(host.strip() for host in value.split(",") if host.strip())
    if not hosts:
        raise argparse.ArgumentTypeError("--hosts must contain at least one host")
    if len(hosts) != len(set(hosts)):
        raise argparse.ArgumentTypeError("--hosts must not contain duplicate hosts")
    unknown = sorted(set(hosts) - set(FULL_GB10_HOSTS))
    if unknown:
        raise argparse.ArgumentTypeError(
            f"--hosts contains hosts outside the fixed GB10 inventory: {', '.join(unknown)}"
        )
    excluded = sorted(set(hosts) & TEMPORARILY_EXCLUDED_HOSTS)
    if excluded:
        raise argparse.ArgumentTypeError(
            "--hosts contains temporarily excluded hosts that require merged re-admission: "
            + ", ".join(excluded)
        )
    if hosts != DEFAULT_HOSTS:
        raise argparse.ArgumentTypeError(
            "--hosts must match the exact merged active GB10 host set; runtime host skips "
            "or reordering are not allowed"
        )
    return hosts


def replayable_secret_source_arg(flag_name: str):
    base = secret_source_argparse_type(flag_name)

    def _validate(value: str) -> str:
        parsed = base(value)
        if parsed == "-":
            raise argparse.ArgumentTypeError(
                f"{flag_name} must be replayable for detached validation runners; "
                "use env:VAR or file:PATH",
            )
        return parsed

    return _validate


def _target_slots(hosts: Sequence[str], max_concurrent: int, intent: str) -> int:
    if intent == "active":
        return len(hosts) * max_concurrent
    return 0


def desired_state_payload(
    current: Mapping[str, Any],
    *,
    hosts: Sequence[str],
    intent: str,
    ttl_seconds: int,
    adjust_idle_exit: bool,
) -> dict[str, Any]:
    max_concurrent = int(current.get("max_concurrent") or 0)
    if max_concurrent <= 0:
        raise ValueError("current desired state lacks positive max_concurrent")
    env = dict(current.get("env") or {})
    if adjust_idle_exit and intent == "active":
        env["LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS"] = str(ttl_seconds)
    raw_host_intents = current.get("host_intents") or {}
    if not isinstance(raw_host_intents, Mapping):
        raise ValueError("current desired state host_intents must be an object")
    host_intents = {str(host): str(value) for host, value in raw_host_intents.items()}
    unknown_intents = sorted(set(host_intents) - set(FULL_GB10_HOSTS))
    if unknown_intents:
        raise ValueError(
            "current desired state contains hosts outside the fixed GB10 inventory: "
            + ", ".join(unknown_intents)
        )
    selected = set(hosts)
    excluded_selected = sorted(selected & TEMPORARILY_EXCLUDED_HOSTS)
    if excluded_selected:
        raise ValueError(
            "temporarily excluded hosts require merged re-admission: "
            + ", ".join(excluded_selected)
        )
    for host in TEMPORARILY_EXCLUDED_HOSTS:
        host_intents[host] = "stopped"
    host_intents.update({host: intent for host in hosts})
    return {
        "image_tag": current["image_tag"],
        "max_concurrent": max_concurrent,
        "env_config_version": current["env_config_version"],
        "source_git_commit": current.get("source_git_commit"),
        "target_slots": _target_slots(hosts, max_concurrent, intent),
        "host_intents": host_intents,
        "rollout_policy": current.get("rollout_policy") or {},
        "env": env,
        "force": bool(current.get("force") or False),
    }


def release_intent_for_result(exit_code: int, configured: str) -> str:
    if configured != "auto":
        return configured
    return "stopped" if exit_code == 0 else "draining"


def _http_json(
    *,
    method: str,
    cp_url: str,
    admin_token: str,
    path: str,
    body: Mapping[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    url = cp_url.rstrip("/") + path
    data = None
    headers = {"Authorization": f"Bearer {admin_token}"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {path} failed HTTP {exc.code}: {redact_text(detail)}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {redact_text(str(exc.reason))}") from exc
    if not payload:
        return {}
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{method} {path} returned non-object JSON")
    return parsed


def _desired_path(environment: str, pool_name: str) -> str:
    return f"/admin/gb10-worker-pools/{environment}/{pool_name}/desired-state"


def _status_path(environment: str, pool_name: str) -> str:
    return f"/admin/gb10-worker-pools/status?environment={environment}&pool_name={pool_name}"


def fetch_desired_state(args: argparse.Namespace, admin_token: str) -> dict[str, Any]:
    return _http_json(
        method="GET",
        cp_url=args.cp_url,
        admin_token=admin_token,
        path=_desired_path(args.environment, args.pool_name),
        timeout=args.http_timeout,
    )


def put_desired_state(
    args: argparse.Namespace,
    admin_token: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return _http_json(
        method="PUT",
        cp_url=args.cp_url,
        admin_token=admin_token,
        path=_desired_path(args.environment, args.pool_name),
        body=payload,
        timeout=args.http_timeout,
    )


def fetch_status(args: argparse.Namespace, admin_token: str) -> dict[str, Any]:
    return _http_json(
        method="GET",
        cp_url=args.cp_url,
        admin_token=admin_token,
        path=_status_path(args.environment, args.pool_name),
        timeout=args.http_timeout,
    )


def _node_by_host(status: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    nodes = status.get("nodes")
    if not isinstance(nodes, list):
        return {}
    out: dict[str, Mapping[str, Any]] = {}
    for node in nodes:
        if isinstance(node, Mapping) and isinstance(node.get("hostname"), str):
            out[str(node["hostname"])] = node
    return out


def status_mismatches(
    status: Mapping[str, Any],
    *,
    hosts: Sequence[str],
    intent: str,
    image_tag: str,
    env_config_version: str,
) -> list[str]:
    nodes = _node_by_host(status)
    errors: list[str] = []
    for host in hosts:
        node = nodes.get(host)
        if node is None:
            errors.append(f"{host}: missing node report")
            continue
        if node.get("desired_intent") != intent:
            errors.append(f"{host}: desired_intent={node.get('desired_intent')!r}")
        if node.get("current_intent") != intent:
            errors.append(f"{host}: current_intent={node.get('current_intent')!r}")
        expected_apply_state = {
            "active": "applied",
            "draining": "draining",
            "stopped": "stopped",
        }[intent]
        if node.get("apply_state") != expected_apply_state:
            errors.append(f"{host}: apply_state={node.get('apply_state')!r}")
        if node.get("current_image_tag") != image_tag:
            errors.append(f"{host}: current_image_tag={node.get('current_image_tag')!r}")
        if node.get("current_env_config_version") != env_config_version:
            errors.append(
                f"{host}: current_env_config_version={node.get('current_env_config_version')!r}",
            )
        if intent == "active":
            if node.get("worker_fresh") is not True:
                errors.append(f"{host}: worker_fresh={node.get('worker_fresh')!r}")
            backend_names = node.get("worker_backend_names") or []
            if "docker" not in backend_names:
                errors.append(f"{host}: docker backend missing")
        elif intent == "draining" and node.get("worker_fresh") is True:
            errors.append(f"{host}: worker still fresh after draining intent")
        elif intent == "stopped" and node.get("worker_fresh") is True:
            errors.append(f"{host}: worker still fresh after stopped intent")
    return errors


def _ssh_base(args: argparse.Namespace) -> list[str]:
    cmd = ["ssh", "-F", str(args.ssh_config)]
    if args.ssh_identity is not None:
        cmd.extend(["-i", str(args.ssh_identity)])
    cmd.extend(["-o", "BatchMode=yes", "-o", f"ConnectTimeout={args.ssh_connect_timeout}"])
    return cmd


def _node_agent_start_command(service: str) -> str:
    quoted_service = shlex.quote(service)
    return (
        f"systemctl --user start --no-block {quoted_service} && "
        f"systemctl --user show {quoted_service} "
        "-p Type -p Result -p ExecMainStatus -p ActiveState -p SubState --no-pager"
    )


def start_node_agents(
    args: argparse.Namespace,
    *,
    hosts: Sequence[str],
    phase: str,
    evidence_dir: Path,
) -> PhaseResult:
    run_dir = evidence_dir / f"gb10-node-agent-{phase}-{_now().strftime('%Y%m%dT%H%M%SZ')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    if args.dry_run:
        for host in hosts:
            (run_dir / f"{host}.log").write_text(
                "dry_run=true\n"
                + "command="
                + shlex.join(
                    [*_ssh_base(args), host, _node_agent_start_command(args.node_agent_service)]
                )
                + "\n",
                encoding="utf-8",
            )
        return PhaseResult(phase=f"node-agent-{phase}", ok=True, artifact=str(run_dir))

    processes: list[tuple[str, Path, subprocess.Popen[str]]] = []
    remote = _node_agent_start_command(args.node_agent_service)
    for host in hosts:
        log_path = run_dir / f"{host}.log"
        handle = log_path.open("w", encoding="utf-8")
        handle.write(f"host={host}\nstarted_at={_iso(_now())}\n")
        handle.flush()
        proc = subprocess.Popen(
            [*_ssh_base(args), host, remote],
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        handle.close()
        processes.append((host, log_path, proc))

    failed: list[str] = []
    for host, log_path, proc in processes:
        timed_out = False
        try:
            rc: int | str = proc.wait(timeout=args.node_agent_command_timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            rc = "timeout"
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        with log_path.open("a", encoding="utf-8") as handle:
            if timed_out:
                handle.write(
                    f"\ntimed_out_after_seconds={args.node_agent_command_timeout}\n",
                )
            handle.write(f"\nexit_code={rc}\nfinished_at={_iso(_now())}\n")
        if rc != 0:
            failed.append(host)
    if failed:
        return PhaseResult(
            phase=f"node-agent-{phase}",
            ok=False,
            artifact=str(run_dir),
            detail=f"node-agent failed on {', '.join(failed)}",
        )
    return PhaseResult(phase=f"node-agent-{phase}", ok=True, artifact=str(run_dir))


def wait_for_status(
    args: argparse.Namespace,
    admin_token: str,
    *,
    hosts: Sequence[str],
    intent: str,
    image_tag: str,
    env_config_version: str,
    evidence_dir: Path,
    phase: str,
) -> PhaseResult:
    deadline = time.monotonic() + args.status_timeout
    last_status: dict[str, Any] = {}
    last_mismatches: list[str] = []
    while True:
        last_status = fetch_status(args, admin_token)
        last_mismatches = status_mismatches(
            last_status,
            hosts=hosts,
            intent=intent,
            image_tag=image_tag,
            env_config_version=env_config_version,
        )
        if not last_mismatches:
            path = evidence_dir / f"gb10-status-{phase}.json"
            _write_json(path, {"ok": True, "status": last_status, "checked_at": _iso(_now())})
            return PhaseResult(phase=f"status-{phase}", ok=True, artifact=str(path))
        if time.monotonic() >= deadline:
            path = evidence_dir / f"gb10-status-{phase}-failed.json"
            _write_json(
                path,
                {
                    "ok": False,
                    "mismatches": last_mismatches,
                    "status": last_status,
                    "checked_at": _iso(_now()),
                },
            )
            return PhaseResult(
                phase=f"status-{phase}",
                ok=False,
                artifact=str(path),
                detail="; ".join(last_mismatches[:8]),
            )
        time.sleep(args.status_poll_interval)


def run_validation_command(args: argparse.Namespace, evidence_dir: Path) -> PhaseResult:
    if not args.validation_command:
        return PhaseResult(
            phase="validation-command", ok=True, detail="no validation command supplied"
        )
    log_path = evidence_dir / "validation-command.log"
    if args.dry_run:
        log_path.write_text(
            "dry_run=true\ncommand=" + shlex.join(args.validation_command) + "\n",
            encoding="utf-8",
        )
        return PhaseResult(phase="validation-command", ok=True, artifact=str(log_path))
    with log_path.open("w", encoding="utf-8") as handle:
        handle.write("started_at=" + _iso(_now()) + "\n")
        handle.write("command=" + shlex.join(args.validation_command) + "\n")
        handle.flush()
        proc = subprocess.Popen(
            args.validation_command,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        rc = proc.wait()
        handle.write("finished_at=" + _iso(_now()) + "\n")
        handle.write(f"exit_code={rc}\n")
    return PhaseResult(
        phase="validation-command",
        ok=rc == 0,
        artifact=str(log_path),
        detail=f"exit_code={rc}",
    )


def _phase_dict(result: PhaseResult) -> dict[str, Any]:
    return {
        "phase": result.phase,
        "ok": result.ok,
        "artifact": result.artifact,
        "detail": result.detail,
    }


def run(args: argparse.Namespace) -> int:
    evidence_dir = args.evidence_dir
    evidence_dir.mkdir(parents=True, exist_ok=True)
    started = _now()
    phases: list[PhaseResult] = []
    validation_rc = 0
    release_intent = "stopped"
    try:
        admin_token = resolve_secret_source(args.admin_token, flag_name="--admin-token")
    except SecretSourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    hosts = _host_list(args.hosts)
    lease_expires_at = started + timedelta(seconds=args.lease_ttl_seconds)
    summary_path = evidence_dir / "staging-validation-capacity-runner-summary.json"

    try:
        current = fetch_desired_state(args, admin_token)
        active_payload = desired_state_payload(
            current,
            hosts=hosts,
            intent="active",
            ttl_seconds=args.lease_ttl_seconds,
            adjust_idle_exit=not args.no_adjust_idle_exit,
        )
        active_desired = put_desired_state(args, admin_token, active_payload)
        active_desired_path = evidence_dir / "gb10-desired-state-active.json"
        _write_json(active_desired_path, active_desired)
        phases.append(PhaseResult("desired-state-active", True, str(active_desired_path)))

        phase = start_node_agents(args, hosts=hosts, phase="activate", evidence_dir=evidence_dir)
        phases.append(phase)
        if not phase.ok:
            validation_rc = 1
            return 1

        phase = wait_for_status(
            args,
            admin_token,
            hosts=hosts,
            intent="active",
            image_tag=str(active_payload["image_tag"]),
            env_config_version=str(active_payload["env_config_version"]),
            evidence_dir=evidence_dir,
            phase="active",
        )
        phases.append(phase)
        if not phase.ok:
            validation_rc = 1
            return 1

        validation = run_validation_command(args, evidence_dir)
        phases.append(validation)
        validation_rc = 0 if validation.ok else 1
        return validation_rc
    except Exception as exc:
        validation_rc = 1
        phases.append(PhaseResult("runner-error", False, detail=redact_text(str(exc))))
        print(f"error: {redact_text(str(exc))}", file=sys.stderr)
        return 1
    finally:
        try:
            release_intent = release_intent_for_result(validation_rc, args.release_intent)
            current_for_release = fetch_desired_state(args, admin_token)
            release_payload = desired_state_payload(
                current_for_release,
                hosts=hosts,
                intent=release_intent,
                ttl_seconds=args.lease_ttl_seconds,
                adjust_idle_exit=False,
            )
            released = put_desired_state(args, admin_token, release_payload)
            release_path = evidence_dir / f"gb10-desired-state-{release_intent}.json"
            _write_json(release_path, released)
            phases.append(PhaseResult(f"desired-state-{release_intent}", True, str(release_path)))
            phase = start_node_agents(
                args,
                hosts=hosts,
                phase=release_intent,
                evidence_dir=evidence_dir,
            )
            phases.append(phase)
            if phase.ok and args.wait_for_release:
                phases.append(
                    wait_for_status(
                        args,
                        admin_token,
                        hosts=hosts,
                        intent=release_intent,
                        image_tag=str(release_payload["image_tag"]),
                        env_config_version=str(release_payload["env_config_version"]),
                        evidence_dir=evidence_dir,
                        phase=release_intent,
                    ),
                )
        except Exception as exc:
            phases.append(PhaseResult("release-error", False, detail=redact_text(str(exc))))
        finally:
            _write_json(
                summary_path,
                {
                    "schema_version": 1,
                    "ok": validation_rc == 0 and all(phase.ok for phase in phases),
                    "environment": args.environment,
                    "pool_name": args.pool_name,
                    "hosts": list(hosts),
                    "lease": {
                        "ttl_seconds": args.lease_ttl_seconds,
                        "started_at": _iso(started),
                        "expires_at": _iso(lease_expires_at),
                        "idle_exit_adjusted_to_ttl": not args.no_adjust_idle_exit,
                    },
                    "release_intent": release_intent,
                    "validation_exit_code": validation_rc,
                    "phases": [_phase_dict(phase) for phase in phases],
                    "admin_token_source": args.admin_token,
                },
            )
            print(f"summary={summary_path}")


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cp-url", required=True, help="Control Plane base URL.")
    parser.add_argument(
        "--admin-token",
        required=True,
        type=replayable_secret_source_arg("--admin-token"),
        help="Replayable admin token source: env:VAR or file:PATH.",
    )
    parser.add_argument("--environment", default="staging")
    parser.add_argument("--pool-name", default="gb10-arm64")
    parser.add_argument(
        "--hosts",
        help="Exact comma-separated merged active GB10 set; runtime host skips are rejected.",
    )
    parser.add_argument("--ssh-config", required=True, type=Path)
    parser.add_argument("--ssh-identity", type=Path)
    parser.add_argument("--ssh-connect-timeout", type=int, default=10)
    parser.add_argument(
        "--node-agent-command-timeout",
        type=float,
        default=60.0,
        help=(
            "Per-host timeout for queueing node-agent systemd starts over SSH. "
            "Control-plane status polling proves convergence after the start is queued."
        ),
    )
    parser.add_argument("--node-agent-service", default=DEFAULT_NODE_AGENT_SERVICE)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--lease-ttl", dest="lease_ttl_seconds", type=_parse_ttl, default=7200)
    parser.add_argument("--status-timeout", type=float, default=900.0)
    parser.add_argument("--status-poll-interval", type=float, default=10.0)
    parser.add_argument("--http-timeout", type=float, default=30.0)
    parser.add_argument(
        "--release-intent",
        choices=("auto", "draining", "stopped"),
        default="auto",
        help="auto stops workers after success and drains after failure.",
    )
    parser.add_argument("--wait-for-release", action="store_true")
    parser.add_argument("--no-adjust-idle-exit", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--validation-command",
        nargs=argparse.REMAINDER,
        help="Optional command to run after workers are active. Place this flag last.",
    )
    return parser.parse_args(list(argv))


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
