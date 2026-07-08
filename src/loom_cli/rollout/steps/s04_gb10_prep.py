"""Step 11 — GB10 SSH prep (#340, #593).

For each GB10 host in cluster-config, SSH in and prepare:

* ``git fetch && git checkout <resolved_sha>`` in the repo checkout.
* Write the release ``IMAGE_TAG=<image_tag>`` and
  ``ENV_CONFIG_VERSION=<image_tag>`` env file used by systemd unit
  templates.
* Verify the checkout, env file, and any release-critical binaries.

Retries each host up to 3 times with backoff before failing the step.
Aggregates per-host outputs into the step's evidence dir.

Reads GB10 host list from the cluster-config via
``worker_pool_inventory.gb10_hosts`` (a helper we don't want to inline
here — kept in ``scripts/ops/worker_pool_inventory.sh`` for historical
reasons; step imports the Python wrapper).
"""

from __future__ import annotations

import shlex
import time
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import (
    SubprocessResult,
    run_captured,
)

_LEGACY_GB10_WORKER_SERVICE = "loom-gb10-worker.service"


@dataclass(frozen=True, slots=True)
class GB10Host:
    """One release-managed GB10 host."""

    ssh_target: str  # user@hostname or SSH alias
    repo_path: str  # e.g. /srv/loom/staging
    env_file_path: str  # e.g. /srv/loom/staging/.env
    repo_url: str = "https://github.com/qianyi-sun/loom.git"
    node_agent_service: str | None = None
    ssh_config_path: str | None = None
    ssh_identity_file: str | None = None
    ssh_certificate_file: str | None = None


def gb10_hosts_for(ctx: RolloutContext) -> list[GB10Host]:
    """Look up the GB10 hosts for the target scope.

    Reads from cluster-config's ``[gb10_pool]`` section. Returns an
    empty list when scope=current-gb10 has no hosts declared (i.e. a
    cluster without a GB10 pool). Callers should treat empty as a
    no-op step rather than a failure.
    """
    # Cluster-config loading is delegated to loom_cli.cluster_config to
    # avoid duplicating TOML plumbing here.
    from loom_cli.cluster_config import load_cluster_config

    try:
        cfg = load_cluster_config(ctx.cluster_config_path)
    except Exception:
        return []
    pool = getattr(cfg, "gb10_pool", None)
    if pool is None:
        return []
    ssh_config = getattr(pool, "ssh_config", "") or ""
    ssh_config_path: str | None = None
    if ssh_config:
        path = Path(str(ssh_config)).expanduser()
        if not path.is_absolute():
            path = ctx.cluster_config_path.parent / path
        ssh_config_path = str(path.resolve(strict=False))
    ssh_identity_file = _resolve_optional_pool_path(
        ctx,
        getattr(pool, "ssh_identity_file", "") or "",
    )
    ssh_certificate_file = _resolve_optional_pool_path(
        ctx,
        getattr(pool, "ssh_certificate_file", "") or "",
    )
    hosts_raw = getattr(pool, "hosts", None) or ()
    result: list[GB10Host] = []
    for h in hosts_raw:
        result.append(
            GB10Host(
                ssh_target=h.get("ssh_target"),
                repo_path=h.get("repo_path"),
                env_file_path=h.get("env_file_path"),
                repo_url=h.get("repo_url") or "https://github.com/qianyi-sun/loom.git",
                node_agent_service=h.get("node_agent_service"),
                ssh_config_path=ssh_config_path,
                ssh_identity_file=ssh_identity_file,
                ssh_certificate_file=ssh_certificate_file,
            )
        )
    return [h for h in result if h.ssh_target and h.repo_path and h.env_file_path]


def _resolve_optional_pool_path(ctx: RolloutContext, value: str) -> str | None:
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = ctx.cluster_config_path.parent / path
    return str(path.resolve(strict=False))


def _gb10_ssh_auth_preflight(hosts: list[GB10Host]) -> str | None:
    """Return a non-secret auth-material error, or None when ready."""
    if not hosts:
        return None
    identity = hosts[0].ssh_identity_file
    if not identity:
        return (
            "gb10-prep requires [gb10_pool].ssh_identity_file so the "
            "platform-dev rollout runner does not depend on Mac ssh-agent "
            "forwarding"
        )
    identity_path = Path(identity)
    if not identity_path.is_file():
        return f"gb10-prep ssh_identity_file does not exist: {identity_path}"
    mode = identity_path.stat().st_mode & 0o777
    if mode & 0o077:
        return (
            f"gb10-prep ssh_identity_file must not be group/world accessible: "
            f"{identity_path} mode={mode:03o}"
        )
    cert = hosts[0].ssh_certificate_file
    if cert and not Path(cert).is_file():
        return f"gb10-prep ssh_certificate_file does not exist: {cert}"
    return None


def _env_state_profile_path_for(ctx: RolloutContext) -> Path | None:
    from loom_cli.cluster_config import load_cluster_config

    try:
        cfg = load_cluster_config(ctx.cluster_config_path)
    except Exception:
        return None
    profile = getattr(cfg, "env_state_profile", None)
    if not profile:
        return None
    path = Path(str(profile))
    if path.is_absolute():
        return path
    return ctx.cluster_config_path.parent / path


def _gb10_desired_state_declared(ctx: RolloutContext) -> bool:
    from loom_cli.environment_state import load_environment_state_profile

    profile_path = _env_state_profile_path_for(ctx)
    if profile_path is None:
        return False
    try:
        profile = load_environment_state_profile(
            profile_path,
            variables={
                "IMAGE_TAG": ctx.image_tag,
                "ENV_CONFIG_VERSION": ctx.image_tag,
                "GIT_SHA": ctx.resolved_sha,
            },
            expected_environment=ctx.environment,
        )
    except Exception:
        return False
    return bool(profile.gb10_desired_states)


def _no_gb10_hosts_error(ctx: RolloutContext) -> str | None:
    if ctx.scope != "current-gb10":
        return None
    if not _gb10_desired_state_declared(ctx):
        return None
    return (
        "current-gb10 rollout declares GB10 desired state, but the cluster "
        "config has no [gb10_pool] hosts; add the actual release-managed "
        "GB10 hosts so rollout step 11 can deliver runner state before "
        "release-gate"
    )


def _ssh(host: GB10Host, remote_cmd: str) -> SubprocessResult:
    """Run a remote command over SSH with reasonable options.

    ``BatchMode=yes`` disables password prompts (fails fast if key
    auth isn't set up). ``ConnectTimeout=10`` bounds hang-on-connect.
    """
    argv = ["ssh"]
    if host.ssh_config_path:
        argv.extend(["-F", host.ssh_config_path])
    if host.ssh_identity_file:
        argv.extend(["-i", host.ssh_identity_file, "-o", "IdentitiesOnly=yes"])
    if host.ssh_certificate_file:
        argv.extend(["-o", f"CertificateFile={host.ssh_certificate_file}"])
    argv.extend(
        [
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=accept-new",
            host.ssh_target,
            remote_cmd,
        ]
    )
    return run_captured(argv)


def _parse_systemctl_properties(stdout: str) -> dict[str, str]:
    props: dict[str, str] = {}
    for line in stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            props[key] = value
    return props


def _node_agent_service_is_prepared(props: dict[str, str]) -> bool:
    if props.get("ActiveState") == "active":
        return True
    return (
        props.get("Type") == "oneshot"
        and props.get("Result") == "success"
        and props.get("ExecMainStatus") == "0"
    )


def _node_agent_status_summary(props: dict[str, str]) -> str:
    keys = ("Type", "Result", "ExecMainStatus", "ActiveState", "SubState")
    return " ".join(f"{key}={props.get(key, '<missing>')}" for key in keys)


def _legacy_worker_unit_retire_command() -> str:
    service = shlex.quote(_LEGACY_GB10_WORKER_SERVICE)
    return (
        f"if systemctl --user list-unit-files {service} >/dev/null 2>&1 "
        f"|| systemctl --user status {service} >/dev/null 2>&1; then "
        f"systemctl --user disable --now {service}; "
        f"systemctl --user reset-failed {service} >/dev/null 2>&1 || true; "
        "fi"
    )


def _legacy_worker_unit_mismatch(
    host: GB10Host,
    step_dir: StepDir,
) -> VerifyOutcome | None:
    service = shlex.quote(_LEGACY_GB10_WORKER_SERVICE)
    enabled = _ssh(host, f"systemctl --user is-enabled {service}")
    if enabled.returncode == 255:
        return VerifyOutcome.UNKNOWN
    if enabled.returncode == 0 and enabled.stdout.strip() == "enabled":
        step_dir.stderr_path().write_text(
            f"legacy {_LEGACY_GB10_WORKER_SERVICE} is still enabled on "
            f"{host.ssh_target}; rerun gb10-prep to retire it before "
            "node-agent release-gate validation\n",
        )
        return VerifyOutcome.MISMATCH

    status = _ssh(
        host,
        f"systemctl --user show {service} -p ActiveState -p SubState",
    )
    if status.returncode == 255:
        return VerifyOutcome.UNKNOWN
    if status.returncode == 0:
        props = _parse_systemctl_properties(status.stdout)
        if props.get("ActiveState") == "activating":
            step_dir.stderr_path().write_text(
                f"legacy {_LEGACY_GB10_WORKER_SERVICE} is still activating on "
                f"{host.ssh_target}; rerun gb10-prep to avoid a parallel "
                "compose worker start path\n",
            )
            return VerifyOutcome.MISMATCH
    return None


def _env_file_update_command(ctx: RolloutContext, host: GB10Host) -> str:
    updates = {
        "IMAGE_TAG": ctx.image_tag,
        "ENV_CONFIG_VERSION": ctx.image_tag,
        "LOOM_IMAGE_TAG": ctx.image_tag,
        "LOOM_WORKER_ENV_CONFIG_VERSION": ctx.image_tag,
    }
    return f"""python3 - <<'PY'
from pathlib import Path

path = Path({host.env_file_path!r})
updates = {updates!r}
path.parent.mkdir(parents=True, exist_ok=True)
existing = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
out = []
seen = set()
for line in existing:
    if "=" not in line or line.lstrip().startswith("#"):
        out.append(line)
        continue
    key = line.split("=", 1)[0].strip()
    if key in updates:
        if key not in seen:
            out.append(f"{{key}}={{updates[key]}}")
            seen.add(key)
        continue
    out.append(line)
for key, value in updates.items():
    if key not in seen:
        out.append(f"{{key}}={{value}}")
path.write_text("\\n".join(out) + "\\n", encoding="utf-8")
PY"""


def _prep_one_host(
    ctx: RolloutContext,
    host: GB10Host,
    host_dir: Path,
) -> tuple[bool, str]:
    """Run the prep sequence on one host. Returns (ok, summary)."""
    repo_path = shlex.quote(host.repo_path)
    env_file_path = shlex.quote(host.env_file_path)
    repo_url = shlex.quote(host.repo_url)
    image_tag = shlex.quote(ctx.image_tag)
    resolved_sha = shlex.quote(ctx.resolved_sha)
    steps: list[tuple[str, str]] = [
        (
            "checkout-present",
            (
                f"if [ -d {repo_path}/.git ]; then :; "
                f"elif [ -e {repo_path} ]; then "
                f"echo 'repo_path exists but is not a git checkout' >&2; exit 1; "
                f"else git clone --quiet {repo_url} {repo_path}; fi"
            ),
        ),
        (
            "fetch",
            f"cd {repo_path} && git fetch --quiet origin",
        ),
        (
            "checkout",
            f"cd {repo_path} && git checkout --detach {resolved_sha}",
        ),
        (
            "env-file",
            _env_file_update_command(ctx, host),
        ),
        (
            "verify-head",
            (f'test "$(cd {repo_path} && git rev-parse HEAD)" = {resolved_sha}'),
        ),
        (
            "verify-env-file",
            (f"grep -q '^LOOM_IMAGE_TAG={image_tag}$' {env_file_path}"),
        ),
    ]
    if host.node_agent_service:
        steps.append(("legacy-worker-unit", _legacy_worker_unit_retire_command()))
        service = shlex.quote(host.node_agent_service)
        steps.append(("node-agent", f"systemctl --user start {service}"))
    log_lines: list[str] = []
    for label, cmd in steps:
        result = _ssh(host, cmd)
        log_lines.append(f"# {label} ({host.ssh_target})")
        log_lines.append(f"$ {cmd}")
        log_lines.append(result.stdout.rstrip())
        if result.returncode != 0:
            log_lines.append(f"# {label} exited {result.returncode}")
            log_lines.append(result.stderr.rstrip())
            (host_dir / "prep.log").write_text("\n".join(log_lines))
            return False, f"{label} failed on {host.ssh_target}: rc={result.returncode}"
    (host_dir / "prep.log").write_text("\n".join(log_lines))
    return True, f"prepped {host.ssh_target}"


class GB10PrepStep(BaseStep):
    number = 11
    name = "gb10-prep"

    #: Number of retries per host on transient SSH failure.
    max_retries: int = 3
    #: Backoff seconds between retries (multiplied by attempt#).
    backoff_sec: float = 5.0

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "resolved_sha": ctx.resolved_sha,
            "image_tag": ctx.image_tag,
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "scope": ctx.scope,
        }

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        hosts = gb10_hosts_for(ctx)
        if not hosts:
            if _no_gb10_hosts_error(ctx):
                return VerifyOutcome.MISMATCH
            return VerifyOutcome.MATCH
        auth_error = _gb10_ssh_auth_preflight(hosts)
        if auth_error:
            step_dir.stderr_path().write_text(auth_error + "\n")
            return VerifyOutcome.UNKNOWN
        for host in hosts:
            repo_path = shlex.quote(host.repo_path)
            env_file_path = shlex.quote(host.env_file_path)
            image_tag = shlex.quote(ctx.image_tag)
            resolved_sha = shlex.quote(ctx.resolved_sha)
            checks = [
                (
                    "checkout-head",
                    f'test "$(cd {repo_path} && git rev-parse HEAD)" = {resolved_sha}',
                ),
                (
                    "image-tag",
                    f"grep -q '^LOOM_IMAGE_TAG={image_tag}$' {env_file_path}",
                ),
                (
                    "env-config-version",
                    f"grep -q '^LOOM_WORKER_ENV_CONFIG_VERSION={image_tag}$' {env_file_path}",
                ),
            ]
            for _label, cmd in checks:
                r = _ssh(host, cmd)
                if r.returncode != 0:
                    if r.returncode == 255:
                        # SSH/auth/connect failures are ambiguous; do not
                        # classify them as target drift.
                        return VerifyOutcome.UNKNOWN
                    # SSH succeeded but the release predicate did not match:
                    # the previous prep did not finish and resume should rerun.
                    return VerifyOutcome.MISMATCH
            if host.node_agent_service:
                legacy_outcome = _legacy_worker_unit_mismatch(host, step_dir)
                if legacy_outcome is not None:
                    return legacy_outcome
                service = shlex.quote(host.node_agent_service)
                r = _ssh(
                    host,
                    "systemctl --user show "
                    f"{service} "
                    "-p Type "
                    "-p Result "
                    "-p ExecMainStatus "
                    "-p ActiveState "
                    "-p SubState",
                )
                if r.returncode != 0:
                    if r.returncode == 255:
                        return VerifyOutcome.UNKNOWN
                    step_dir.stderr_path().write_text(
                        f"node-agent-status failed on {host.ssh_target}: rc={r.returncode}\n",
                    )
                    return VerifyOutcome.MISMATCH
                props = _parse_systemctl_properties(r.stdout)
                if not _node_agent_service_is_prepared(props):
                    step_dir.stderr_path().write_text(
                        "node-agent-status mismatch on "
                        f"{host.ssh_target}: {_node_agent_status_summary(props)}\n",
                    )
                    return VerifyOutcome.MISMATCH
        return VerifyOutcome.MATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        hosts = gb10_hosts_for(ctx)
        if not hosts:
            error = _no_gb10_hosts_error(ctx)
            if error:
                step_dir.stderr_path().write_text(error + "\n")
                return RunResult(exit_code=1, error=error)
            step_dir.stdout_path().write_text(
                "no GB10 hosts declared in cluster-config; skipping.\n",
            )
            return RunResult(
                exit_code=0,
                summary="no GB10 hosts declared; step is a no-op",
            )
        auth_error = _gb10_ssh_auth_preflight(hosts)
        if auth_error:
            step_dir.stderr_path().write_text(auth_error + "\n")
            return RunResult(exit_code=1, error=auth_error)
        # Per-host subdir for logs.
        summaries: list[str] = []
        failures: list[str] = []
        for host in hosts:
            host_dir = step_dir.path / f"host-{host.ssh_target.replace('@', '_at_')}"
            host_dir.mkdir(exist_ok=True)
            ok = False
            last_summary = ""
            for attempt in range(1, self.max_retries + 1):
                ok, last_summary = _prep_one_host(ctx, host, host_dir)
                if ok:
                    break
                if attempt < self.max_retries:
                    time.sleep(self.backoff_sec * attempt)
            summaries.append(last_summary)
            if not ok:
                failures.append(host.ssh_target)
        step_dir.stdout_path().write_text("\n".join(summaries) + "\n")
        if failures:
            return RunResult(
                exit_code=1,
                error=(
                    f"gb10-prep failed on {len(failures)}/{len(hosts)} "
                    f"host(s) after {self.max_retries} attempts: "
                    f"{', '.join(failures)}"
                ),
            )
        return RunResult(
            exit_code=0,
            summary=f"prepped {len(hosts)} GB10 host(s)",
        )
