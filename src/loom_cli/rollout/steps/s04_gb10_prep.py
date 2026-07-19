"""Step 12 — GB10 SSH prep (#340, #593).

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

import os
import re
import shlex
import stat
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.operator.redaction import (
    known_secrets_from_sources,
    redact_rollout_text,
    rollout_redaction_scope,
)
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_relative_path,
    rollout_cluster_config,
    rollout_cluster_config_path,
    validate_candidate_loom_source,
)
from loom_cli.rollout.steps.subprocess_util import (
    SubprocessResult,
    run_captured,
)

_LEGACY_GB10_WORKER_SERVICE = "loom-gb10-worker.service"
_SIMPLE_SYSTEMD_SERVICE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]*\.service\Z")
_GB10_NODE_AGENT_UNIT_DIR = PurePosixPath("deploy/worker-pools/gb10")
_NODE_AGENT_TIMER_SETTLE_ATTEMPTS = 16
_NODE_AGENT_TIMER_SETTLE_INTERVAL_SECONDS = 2.0


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


@dataclass(frozen=True, slots=True)
class HostPrepResult:
    """Aggregated prep result for one GB10 host."""

    host: str
    ok: bool
    summary: str
    attempts: int


def gb10_hosts_for(
    ctx: RolloutContext,
    *,
    config_path: Path | None = None,
) -> list[GB10Host]:
    """Look up the GB10 hosts for the target scope.

    Reads from cluster-config's ``[gb10_pool]`` section. Returns an
    empty list when scope=current-gb10 has no hosts declared (i.e. a
    cluster without a GB10 pool). Callers should treat empty as a
    no-op step rather than a failure.
    """
    # Cluster-config loading is delegated to loom_cli.cluster_config to
    # avoid duplicating TOML plumbing here.
    from loom_cli.cluster_config import load_cluster_config

    source_config = config_path or ctx.cluster_config_path
    try:
        cfg = load_cluster_config(source_config)
    except Exception as exc:
        if config_path is not None:
            raise CandidateToolingError(
                f"failed to load rollout-local GB10 cluster config: {exc}"
            ) from exc
        return []
    pool = getattr(cfg, "gb10_pool", None)
    if pool is None:
        return []
    ssh_config = getattr(pool, "ssh_config", "") or ""
    ssh_config_path: str | None = None
    if ssh_config:
        path = Path(str(ssh_config)).expanduser()
        if not path.is_absolute():
            path = source_config.parent / path
        ssh_config_path = str(path.resolve(strict=False))
    ssh_identity_file = _resolve_optional_pool_path(
        source_config,
        getattr(pool, "ssh_identity_file", "") or "",
    )
    ssh_certificate_file = _resolve_optional_pool_path(
        source_config,
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


def _resolve_optional_pool_path(config_path: Path, value: str) -> str | None:
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_path.parent / path
    return str(path.resolve(strict=False))


def _require_candidate_regular_file(
    path: Path,
    *,
    candidate_root: Path,
    label: str,
) -> Path:
    lexical = Path(os.path.normpath(path))
    try:
        lexical.relative_to(candidate_root)
    except ValueError as exc:
        raise CandidateToolingError(f"{label} is outside the candidate worktree") from exc
    try:
        metadata = lexical.lstat()
    except OSError as exc:
        raise CandidateToolingError(f"{label} is unavailable in the candidate worktree") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise CandidateToolingError(f"{label} must be a regular file, not a symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise CandidateToolingError(f"{label} must be a regular file")
    resolved = lexical.resolve(strict=True)
    try:
        resolved.relative_to(candidate_root)
    except ValueError as exc:
        raise CandidateToolingError(f"{label} resolves outside the candidate worktree") from exc
    return resolved


def _gb10_prep_config_paths(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> tuple[Path, Path]:
    """Bind GB10 host/SSH/profile inputs to the pinned candidate checkout."""
    from loom_cli.cluster_config import load_cluster_config

    candidate_root = validate_candidate_loom_source(step_dir).resolve(strict=True)
    mapped = candidate_relative_path(ctx.cluster_config_path, step_dir)
    if not mapped.is_absolute():
        mapped = candidate_root / mapped
    candidate_config = _require_candidate_regular_file(
        mapped,
        candidate_root=candidate_root,
        label="candidate cluster config",
    )

    try:
        candidate_cfg = load_cluster_config(candidate_config)
    except Exception as exc:
        raise CandidateToolingError(f"candidate cluster config is invalid: {exc}") from exc

    profile_value = getattr(candidate_cfg, "env_state_profile", None)
    if profile_value:
        profile = Path(str(profile_value)).expanduser()
        if not profile.is_absolute():
            profile = candidate_config.parent / profile
        _require_candidate_regular_file(
            profile,
            candidate_root=candidate_root,
            label="candidate environment-state profile",
        )

    pool = getattr(candidate_cfg, "gb10_pool", None)
    ssh_config_value = getattr(pool, "ssh_config", None) if pool is not None else None
    if ssh_config_value:
        ssh_config = Path(str(ssh_config_value)).expanduser()
        if not ssh_config.is_absolute():
            ssh_config = candidate_config.parent / ssh_config
            _require_candidate_regular_file(
                ssh_config,
                candidate_root=candidate_root,
                label="candidate GB10 SSH config",
            )

    materialized_target = rollout_cluster_config_path(step_dir)
    if os.path.lexists(materialized_target):
        metadata = materialized_target.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise CandidateToolingError(
                "rollout-local cluster config must be a regular file, not a symlink"
            )
    materialized = rollout_cluster_config(ctx, step_dir)
    metadata = materialized.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise CandidateToolingError("rollout-local cluster config is not a regular file")
    return candidate_config, materialized


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


def _env_state_profile_path_for(
    ctx: RolloutContext,
    *,
    config_path: Path | None = None,
) -> Path | None:
    from loom_cli.cluster_config import load_cluster_config

    source_config = config_path or ctx.cluster_config_path
    try:
        cfg = load_cluster_config(source_config)
    except Exception as exc:
        if config_path is not None:
            raise CandidateToolingError(
                f"failed to load candidate cluster config for GB10 desired state: {exc}"
            ) from exc
        return None
    profile = getattr(cfg, "env_state_profile", None)
    if not profile:
        return None
    path = Path(str(profile))
    if path.is_absolute():
        return path
    return source_config.parent / path


def _gb10_desired_state_declared(
    ctx: RolloutContext,
    *,
    config_path: Path | None = None,
) -> bool:
    from loom_cli.environment_state import load_environment_state_profile

    profile_path = _env_state_profile_path_for(ctx, config_path=config_path)
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
    except Exception as exc:
        if config_path is not None:
            raise CandidateToolingError(
                f"candidate environment-state profile is invalid: {exc}"
            ) from exc
        return False
    return bool(profile.gb10_desired_states)


def _no_gb10_hosts_error(
    ctx: RolloutContext,
    *,
    config_path: Path | None = None,
) -> str | None:
    if ctx.scope != "current-gb10":
        return None
    if not _gb10_desired_state_declared(ctx, config_path=config_path):
        return None
    return (
        "current-gb10 rollout declares GB10 desired state, but the cluster "
        "config has no [gb10_pool] hosts; add the actual release-managed "
        "GB10 hosts so rollout step 12 can deliver runner state before "
        "release-gate"
    )


_GB10_KNOWN_HOSTS = "/etc/loom/staging-rollout-gb10-known-hosts"
_SHARED_WORKER_REPO_ROOT = Path("/shared_work2/qianyi/.loom-staging-rollout/worker-repos")


def _ssh(
    host: GB10Host,
    remote_cmd: str,
    *,
    stdin_text: str | None = None,
) -> SubprocessResult:
    """Run a remote command over SSH with reasonable options.

    ``BatchMode=yes`` disables password prompts (fails fast if key
    auth isn't set up). ``ConnectTimeout=10`` bounds hang-on-connect.
    """
    argv = ["/usr/bin/ssh"]
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
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={_GB10_KNOWN_HOSTS}",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UpdateHostKeys=no",
            host.ssh_target,
            remote_cmd,
        ]
    )
    return run_captured(argv, stdin_text=stdin_text)


def _parse_systemctl_properties(stdout: str) -> dict[str, str]:
    props: dict[str, str] = {}
    for line in stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            props[key] = value
    return props


def _node_agent_service_is_prepared(props: dict[str, str]) -> bool:
    return (
        props.get("LoadState") == "loaded"
        and props.get("Type") == "oneshot"
        and props.get("Result") == "success"
        and props.get("ExecMainStatus") == "0"
        and props.get("NeedDaemonReload") == "no"
    )


def _node_agent_status_summary(props: dict[str, str]) -> str:
    keys = (
        "LoadState",
        "Type",
        "Result",
        "ExecMainStatus",
        "ActiveState",
        "SubState",
        "NeedDaemonReload",
    )
    return " ".join(f"{key}={props.get(key, '<missing>')}" for key in keys)


def _node_agent_timer_name(service: str) -> str:
    """Derive the paired timer from one simple, non-path service basename."""
    if not _SIMPLE_SYSTEMD_SERVICE_RE.fullmatch(service):
        raise CandidateToolingError(
            f"GB10 node_agent_service must be a simple .service basename: {service!r}"
        )
    return f"{service.removesuffix('.service')}.timer"


def _validate_node_agent_services(hosts: list[GB10Host]) -> None:
    for host in hosts:
        if host.node_agent_service:
            _node_agent_timer_name(host.node_agent_service)


def _node_agent_unit_relative_path(unit: str) -> str:
    return str(_GB10_NODE_AGENT_UNIT_DIR / unit)


def _node_agent_unit_source(host: GB10Host, unit: str) -> str:
    repo = PurePosixPath(host.repo_path)
    return shlex.quote(str(repo / _node_agent_unit_relative_path(unit)))


def _node_agent_unit_destination(unit: str) -> str:
    # ``unit`` is validated by ``_node_agent_timer_name`` before this helper
    # is used, so it cannot escape the per-user systemd directory.
    return f'"$HOME/.config/systemd/user/{unit}"'


def _node_agent_linger_command() -> str:
    return 'test "$(loginctl show-user "$(id -u)" --property=Linger --value)" = yes'


def _node_agent_units_clean_command(
    ctx: RolloutContext,
    host: GB10Host,
    *,
    service: str,
    timer: str,
) -> str:
    """Prove both unit sources are tracked and clean at the rollout SHA."""
    paths = (
        _node_agent_unit_relative_path(service),
        _node_agent_unit_relative_path(timer),
    )
    expected_objects = " && ".join(
        f"git cat-file -e {shlex.quote(f'{ctx.resolved_sha}:{path}')}" for path in paths
    )
    quoted_paths = " ".join(shlex.quote(path) for path in paths)
    return (
        f"cd {shlex.quote(host.repo_path)} && {expected_objects} "
        f"&& git diff --quiet {shlex.quote(ctx.resolved_sha)} -- {quoted_paths}"
    )


def _node_agent_unit_install_command(host: GB10Host, unit: str) -> str:
    return (
        "install -D -m 0644 "
        f"{_node_agent_unit_source(host, unit)} "
        f"{_node_agent_unit_destination(unit)}"
    )


def _node_agent_unit_matches_candidate_command(host: GB10Host, unit: str) -> str:
    source = _node_agent_unit_source(host, unit)
    destination = _node_agent_unit_destination(unit)
    return f'cmp -s {source} {destination} && test "$(stat -c %a {destination})" = 644'


def _node_agent_timer_is_prepared(
    props: dict[str, str],
    *,
    service: str,
) -> bool:
    return (
        props.get("LoadState") == "loaded"
        and props.get("ActiveState") == "active"
        and props.get("SubState") == "waiting"
        and props.get("Unit") == service
        and props.get("NeedDaemonReload") == "no"
    )


def _node_agent_timer_is_transiently_running(
    props: dict[str, str],
    *,
    service: str,
) -> bool:
    """Recognize only the timer's documented in-flight service state."""
    return (
        props.get("LoadState") == "loaded"
        and props.get("ActiveState") == "active"
        and props.get("SubState") == "running"
        and props.get("Unit") == service
        and props.get("NeedDaemonReload") == "no"
    )


def _node_agent_timer_status_summary(props: dict[str, str]) -> str:
    keys = (
        "LoadState",
        "ActiveState",
        "SubState",
        "Unit",
        "NeedDaemonReload",
    )
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


def _host_evidence_dir(step_dir: StepDir, host: GB10Host) -> Path:
    safe = host.ssh_target.replace("@", "_at_").replace("/", "_").replace(":", "_")
    return step_dir.path / f"host-{safe}"


def _write_prep_log(host_dir: Path, text: str) -> None:
    """Persist a host diagnostic under the worker's explicit redaction scope."""
    (host_dir / "prep.log").write_text(
        redact_rollout_text(text),
        encoding="utf-8",
    )


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
    if ctx.source_mode == "sealed-cumulative":
        shared_repo = _SHARED_WORKER_REPO_ROOT / f"loom-remote-worker-{ctx.image_tag}"
        if shared_repo.parent != _SHARED_WORKER_REPO_ROOT:
            return False, f"sealed source path is invalid on {host.ssh_target}"
        upload_pack = f"/usr/bin/git -c safe.directory={shared_repo}/.git upload-pack"
        fetch_command = (
            f"cd {repo_path} && "
            "git -c protocol.file.allow=always -c fetch.fsckObjects=true "
            "fetch --quiet --no-tags --no-recurse-submodules --no-write-fetch-head "
            f"--upload-pack={shlex.quote(upload_pack)} "
            f"{shlex.quote(str(shared_repo))} {resolved_sha}"
        )
    else:
        fetch_command = f"cd {repo_path} && git fetch --quiet origin"
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
            fetch_command,
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
        timer = _node_agent_timer_name(host.node_agent_service)
        steps.extend(
            [
                (
                    "verify-node-agent-unit-source",
                    _node_agent_units_clean_command(
                        ctx,
                        host,
                        service=host.node_agent_service,
                        timer=timer,
                    ),
                ),
                ("node-agent-linger", _node_agent_linger_command()),
                (
                    "install-node-agent-service",
                    _node_agent_unit_install_command(host, host.node_agent_service),
                ),
                (
                    "install-node-agent-timer",
                    _node_agent_unit_install_command(host, timer),
                ),
            ]
        )
        steps.append(("legacy-worker-unit", _legacy_worker_unit_retire_command()))
        service = shlex.quote(host.node_agent_service)
        timer_quoted = shlex.quote(timer)
        steps.extend(
            [
                ("node-agent-daemon-reload", "systemctl --user daemon-reload"),
                ("node-agent", f"systemctl --user start {service}"),
                (
                    "node-agent-timer-enable",
                    f"systemctl --user enable --now {timer_quoted}",
                ),
                (
                    "node-agent-timer-restart",
                    f"systemctl --user restart {timer_quoted}",
                ),
            ]
        )
    log_lines: list[str] = []
    for label, cmd in steps:
        result = _ssh(host, cmd)
        log_lines.append(f"# {label} ({host.ssh_target})")
        log_lines.append(f"$ {cmd}")
        log_lines.append(result.stdout.rstrip())
        if result.returncode != 0:
            log_lines.append(f"# {label} exited {result.returncode}")
            log_lines.append(result.stderr.rstrip())
            _write_prep_log(host_dir, "\n".join(log_lines))
            return False, f"{label} failed on {host.ssh_target}: rc={result.returncode}"
    _write_prep_log(host_dir, "\n".join(log_lines))
    return True, f"prepped {host.ssh_target}"


def _prep_host_with_retries(
    *,
    ctx: RolloutContext,
    host: GB10Host,
    host_dir: Path,
    max_retries: int,
    backoff_sec: float,
    known_secrets: tuple[str, ...] = (),
) -> HostPrepResult:
    # ContextVars are not copied into ThreadPoolExecutor workers. Install the
    # values derived by the parent thread explicitly before any worker sink.
    with rollout_redaction_scope(known_secrets):
        ok = False
        last_summary = ""
        attempts = 0
        host_dir.mkdir(exist_ok=True)
        for attempt in range(1, max_retries + 1):
            attempts = attempt
            try:
                ok, last_summary = _prep_one_host(ctx, host, host_dir)
            except Exception as exc:  # pragma: no cover - defensive evidence path
                ok = False
                last_summary = redact_rollout_text(
                    f"gb10-prep crashed on {host.ssh_target}: {exc!r}"
                )
                _write_prep_log(host_dir, last_summary + "\n")
            if ok:
                break
            if attempt < max_retries:
                time.sleep(backoff_sec * attempt)
    return HostPrepResult(
        host=host.ssh_target,
        ok=ok,
        summary=last_summary,
        attempts=attempts,
    )


def _gb10_prep_concurrency(
    ctx: RolloutContext,
    *,
    host_count: int,
    default: int,
) -> int:
    value = ctx.gb10_prep_concurrency or default
    value = max(1, value)
    return min(host_count, value)


class GB10PrepStep(BaseStep):
    number = 12
    name = "gb10-prep"

    #: Number of retries per host on transient SSH failure.
    max_retries: int = 3
    #: Backoff seconds between retries (multiplied by attempt#).
    backoff_sec: float = 5.0
    #: Conservative host-level parallelism; individual host commands stay ordered.
    host_concurrency: int = 4

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "resolved_sha": ctx.resolved_sha,
            "image_tag": ctx.image_tag,
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "scope": ctx.scope,
        }

    def verify_done(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        """Freshly revalidate host reconciliation before skipping DONE."""
        return self._verify_impl(ctx, step_dir)

    def requires_strict_live_verification(self) -> bool:
        """Do not finalize prep when the post-mutation host state is unknown."""
        return True

    def _verify_impl(
        self,
        ctx: RolloutContext,
        step_dir: StepDir,
    ) -> VerifyOutcome:
        try:
            candidate_config, materialized_config = _gb10_prep_config_paths(ctx, step_dir)
            hosts = gb10_hosts_for(ctx, config_path=materialized_config)
            _validate_node_agent_services(hosts)
            no_hosts_error = (
                _no_gb10_hosts_error(ctx, config_path=candidate_config) if not hosts else None
            )
        except CandidateToolingError as exc:
            message = redact_rollout_text(str(exc))
            step_dir.stderr_path().write_text(message + "\n", encoding="utf-8")
            return VerifyOutcome.UNKNOWN
        if not hosts:
            if no_hosts_error:
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
                timer = _node_agent_timer_name(host.node_agent_service)
                unit_checks = [
                    (
                        "node-agent-unit-source",
                        _node_agent_units_clean_command(
                            ctx,
                            host,
                            service=host.node_agent_service,
                            timer=timer,
                        ),
                    ),
                    ("node-agent-linger", _node_agent_linger_command()),
                    (
                        "node-agent-service-unit",
                        _node_agent_unit_matches_candidate_command(host, host.node_agent_service),
                    ),
                    (
                        "node-agent-timer-unit",
                        _node_agent_unit_matches_candidate_command(host, timer),
                    ),
                ]
                for label, cmd in unit_checks:
                    r = _ssh(host, cmd)
                    if r.returncode != 0:
                        if r.returncode == 255:
                            return VerifyOutcome.UNKNOWN
                        step_dir.stderr_path().write_text(
                            f"{label} mismatch on {host.ssh_target}: rc={r.returncode}\n",
                        )
                        return VerifyOutcome.MISMATCH
                legacy_outcome = _legacy_worker_unit_mismatch(host, step_dir)
                if legacy_outcome is not None:
                    return legacy_outcome
                service = shlex.quote(host.node_agent_service)
                r = _ssh(
                    host,
                    "systemctl --user show "
                    f"{service} "
                    "-p LoadState "
                    "-p Type "
                    "-p Result "
                    "-p ExecMainStatus "
                    "-p ActiveState "
                    "-p SubState "
                    "-p NeedDaemonReload",
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
                timer_quoted = shlex.quote(timer)
                enabled = _ssh(host, f"systemctl --user is-enabled {timer_quoted}")
                if enabled.returncode == 255:
                    return VerifyOutcome.UNKNOWN
                if enabled.returncode != 0 or enabled.stdout.strip() != "enabled":
                    step_dir.stderr_path().write_text(
                        "node-agent-timer-enable mismatch on "
                        f"{host.ssh_target}: rc={enabled.returncode} "
                        f"state={enabled.stdout.strip() or '<missing>'}\n",
                    )
                    return VerifyOutcome.MISMATCH
                timer_status_command = (
                    "systemctl --user show "
                    f"{timer_quoted} "
                    "-p LoadState "
                    "-p ActiveState "
                    "-p SubState "
                    "-p Unit "
                    "-p NeedDaemonReload"
                )
                timer_props: dict[str, str] = {}
                observed_transient_run = False
                for attempt in range(_NODE_AGENT_TIMER_SETTLE_ATTEMPTS):
                    timer_status = _ssh(host, timer_status_command)
                    if timer_status.returncode != 0:
                        if timer_status.returncode == 255:
                            return VerifyOutcome.UNKNOWN
                        step_dir.stderr_path().write_text(
                            "node-agent-timer-status failed on "
                            f"{host.ssh_target}: rc={timer_status.returncode}\n",
                        )
                        return VerifyOutcome.MISMATCH
                    timer_props = _parse_systemctl_properties(timer_status.stdout)
                    if _node_agent_timer_is_prepared(
                        timer_props,
                        service=host.node_agent_service,
                    ):
                        break
                    if not _node_agent_timer_is_transiently_running(
                        timer_props,
                        service=host.node_agent_service,
                    ):
                        break
                    observed_transient_run = True
                    if attempt + 1 < _NODE_AGENT_TIMER_SETTLE_ATTEMPTS:
                        time.sleep(_NODE_AGENT_TIMER_SETTLE_INTERVAL_SECONDS)
                if not _node_agent_timer_is_prepared(
                    timer_props,
                    service=host.node_agent_service,
                ):
                    step_dir.stderr_path().write_text(
                        "node-agent-timer-status mismatch on "
                        f"{host.ssh_target}: "
                        f"{_node_agent_timer_status_summary(timer_props)}\n",
                    )
                    return VerifyOutcome.MISMATCH
                if observed_transient_run:
                    # The timer can legitimately report active/running while
                    # its oneshot is executing.  Once it returns to waiting,
                    # re-read the service so acceptance is bound to the just-
                    # completed invocation rather than a previous success.
                    settled_service = _ssh(
                        host,
                        "systemctl --user show "
                        f"{service} "
                        "-p LoadState "
                        "-p Type "
                        "-p Result "
                        "-p ExecMainStatus "
                        "-p ActiveState "
                        "-p SubState "
                        "-p NeedDaemonReload",
                    )
                    if settled_service.returncode != 0:
                        if settled_service.returncode == 255:
                            return VerifyOutcome.UNKNOWN
                        step_dir.stderr_path().write_text(
                            "node-agent-settled-status failed on "
                            f"{host.ssh_target}: rc={settled_service.returncode}\n",
                        )
                        return VerifyOutcome.MISMATCH
                    settled_props = _parse_systemctl_properties(settled_service.stdout)
                    if not _node_agent_service_is_prepared(settled_props):
                        step_dir.stderr_path().write_text(
                            "node-agent-settled-status mismatch on "
                            f"{host.ssh_target}: {_node_agent_status_summary(settled_props)}\n",
                        )
                        return VerifyOutcome.MISMATCH
        return VerifyOutcome.MATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            candidate_config, materialized_config = _gb10_prep_config_paths(ctx, step_dir)
            hosts = gb10_hosts_for(ctx, config_path=materialized_config)
            _validate_node_agent_services(hosts)
            no_hosts_error = (
                _no_gb10_hosts_error(ctx, config_path=candidate_config) if not hosts else None
            )
        except CandidateToolingError as exc:
            message = redact_rollout_text(str(exc))
            step_dir.stderr_path().write_text(message + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=message)
        if not hosts:
            error = no_hosts_error
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
        concurrency = _gb10_prep_concurrency(
            ctx,
            host_count=len(hosts),
            default=self.host_concurrency,
        )
        known_secrets = known_secrets_from_sources(
            (
                ctx.admin_token_source,
                ctx.worker_token_source,
                ctx.service_token_source,
                ctx.smoke_api_token_source,
            )
        )
        known_secrets = (
            *known_secrets,
            *(
                path
                for host in hosts
                for path in (host.ssh_identity_file, host.ssh_certificate_file)
                if path
            ),
        )
        ordered_results: list[HostPrepResult | None] = [None] * len(hosts)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_index = {
                executor.submit(
                    _prep_host_with_retries,
                    ctx=ctx,
                    host=host,
                    host_dir=_host_evidence_dir(step_dir, host),
                    max_retries=self.max_retries,
                    backoff_sec=self.backoff_sec,
                    known_secrets=known_secrets,
                ): index
                for index, host in enumerate(hosts)
            }
            for future in as_completed(future_to_index):
                ordered_results[future_to_index[future]] = future.result()

        results = [result for result in ordered_results if result is not None]
        failures = [result.host for result in results if not result.ok]
        retried_hosts = sum(1 for result in results if result.attempts > 1)
        summary = (
            f"started={len(hosts)} "
            f"succeeded={len(hosts) - len(failures)} "
            f"failed={len(failures)} "
            f"retried={retried_hosts} "
            f"concurrency={concurrency}"
        )
        summaries = [summary, *(result.summary for result in results)]
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
