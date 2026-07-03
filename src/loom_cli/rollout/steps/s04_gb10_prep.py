"""Step 04 — GB10 SSH prep (#340).

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


@dataclass(frozen=True, slots=True)
class GB10Host:
    """One release-managed GB10 host."""

    ssh_target: str        # user@hostname or SSH alias
    repo_path: str         # e.g. /srv/loom/staging
    env_file_path: str     # e.g. /srv/loom/staging/.env


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
    hosts_raw = getattr(pool, "hosts", None) or ()
    result: list[GB10Host] = []
    for h in hosts_raw:
        result.append(GB10Host(
            ssh_target=h.get("ssh_target"),
            repo_path=h.get("repo_path"),
            env_file_path=h.get("env_file_path"),
        ))
    return [h for h in result if h.ssh_target and h.repo_path]


def _ssh(host: GB10Host, remote_cmd: str) -> SubprocessResult:
    """Run a remote command over SSH with reasonable options.

    ``BatchMode=yes`` disables password prompts (fails fast if key
    auth isn't set up). ``ConnectTimeout=10`` bounds hang-on-connect.
    """
    return run_captured([
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=accept-new",
        host.ssh_target,
        remote_cmd,
    ])


def _prep_one_host(
    ctx: RolloutContext, host: GB10Host, host_dir: Path,
) -> tuple[bool, str]:
    """Run the prep sequence on one host. Returns (ok, summary)."""
    steps: list[tuple[str, str]] = [
        (
            "fetch",
            f"cd {host.repo_path} && git fetch --quiet origin",
        ),
        (
            "checkout",
            f"cd {host.repo_path} && git checkout --detach {ctx.resolved_sha}",
        ),
        (
            "env-file",
            (
                f"printf 'IMAGE_TAG=%s\\nENV_CONFIG_VERSION=%s\\n' "
                f"{ctx.image_tag} {ctx.image_tag} > {host.env_file_path}"
            ),
        ),
        (
            "verify-head",
            (
                f"test \"$(cd {host.repo_path} && git rev-parse HEAD)\" = "
                f"{ctx.resolved_sha}"
            ),
        ),
        (
            "verify-env-file",
            (
                f"grep -q 'IMAGE_TAG={ctx.image_tag}' {host.env_file_path}"
            ),
        ),
    ]
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
    number = 4
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
        self, ctx: RolloutContext, step_dir: StepDir,
    ) -> VerifyOutcome:
        hosts = gb10_hosts_for(ctx)
        if not hosts:
            return VerifyOutcome.MATCH
        # For each host, cheap check: env file has correct IMAGE_TAG.
        for host in hosts:
            r = _ssh(host, f"grep -q 'IMAGE_TAG={ctx.image_tag}' {host.env_file_path}")
            if r.returncode != 0:
                # Could be transient; treat as UNKNOWN so we retry rather
                # than falsely declaring MATCH or MISMATCH.
                return VerifyOutcome.UNKNOWN
        return VerifyOutcome.MATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        hosts = gb10_hosts_for(ctx)
        if not hosts:
            step_dir.stdout_path().write_text(
                "no GB10 hosts declared in cluster-config; skipping.\n",
            )
            return RunResult(
                exit_code=0,
                summary="no GB10 hosts declared; step is a no-op",
            )
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
