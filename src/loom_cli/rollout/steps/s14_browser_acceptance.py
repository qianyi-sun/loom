"""Step 16 — candidate-bound authenticated staging browser acceptance.

The singleton admin bearer is consumed only inside a broker-owned rollout
attempt.  A candidate-built, revision-labelled Playwright image writes one
sanitized report into the attempt evidence directory.  No cookie, trace,
screenshot, storage state, or raw bearer is retained.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import cast

from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.browser_report_contract import (
    BROWSER_ACCEPTANCE_USERNAME,
    RolloutBrowserReportAuthority,
    browser_report_ready,
)
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.image_readiness import BROWSER_IMAGE
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.s02_build_images import image_tag
from loom_cli.rollout.steps.subprocess_util import run_captured

BROWSER_ACCEPTANCE_IMAGE = BROWSER_IMAGE
_OUTPUT_DIRECTORY_NAME = "browser-output"
_REPORT_NAME = "staging-admin-browser-acceptance.json"
_MAX_ENVELOPE_BYTES = 128 * 1024
_MAX_REPORT_BYTES = 1024 * 1024
_MAX_ADMIN_TOKEN_BYTES = 64 * 1024


def _read_private_file(path: Path, *, max_bytes: int) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size < 1
            or before.st_size > max_bytes
        ):
            raise ValueError("private rollout file metadata is unsafe")
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(fd, min(65536, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(fd)

        def identity(value: os.stat_result) -> tuple[int, ...]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_mode,
                value.st_uid,
                value.st_gid,
                value.st_nlink,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if len(payload) > max_bytes or identity(before) != identity(after):
            raise ValueError("private rollout file changed while it was read")
        return bytes(payload), before
    finally:
        os.close(fd)


def _browser_route(ctx: RolloutContext) -> str:
    config = load_cluster_config(ctx.cluster_config_path)
    route = (config.frontend_route_path or "").strip()
    if not route.startswith("/"):
        route = "/" + route
    route = route.rstrip("/")
    rendered = f"https://{config.ingress_host}{route}"
    # The route is derived from the cluster config's frontend_route_path (e.g.
    # /staging after #897); require a configured host and non-root path rather
    # than a hardcoded /dev, so it tracks the env-identity/#894 route.
    if not config.ingress_host or route in ("", "/"):
        raise ValueError("browser acceptance requires a configured staging route")
    return rendered


def _token_file(ctx: RolloutContext) -> Path:
    source = ctx.admin_token_source
    if not source.startswith("file:"):
        raise ValueError("browser acceptance requires a file-backed admin token")
    path = Path(source.removeprefix("file:"))
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("browser acceptance admin token path is unsafe")
    return path


def _validate_admin_token_file(path: Path) -> os.stat_result:
    """Validate the installed ACL-readable admin-token authority.

    The rollout installer deliberately preserves Qianyi-owned ``0640`` token
    files and grants ``loom-rollout`` a read-only ACL.  Requiring service
    ownership here would contradict the already validated operator contract.
    Open every parent without following symlinks, accept only the same trusted
    owner set as operator preflight, and reject any effective write/execute
    authority outside the owner before the path is bind-mounted read-only.
    """

    try:
        return read_trusted_file(
            path,
            service_uid=os.geteuid(),
            private=True,
            allow_qianyi_owner=True,
            max_bytes=_MAX_ADMIN_TOKEN_BYTES,
            require_nonempty=True,
        ).metadata
    except ValueError as exc:
        detail = str(exc)
        if "traversal" in detail or "path or authority" in detail:
            message = "browser acceptance admin token path is unsafe"
        elif "changed while" in detail:
            message = "browser acceptance admin token changed while it was read"
        else:
            message = "browser acceptance admin token metadata is unsafe"
        raise ValueError(message) from exc


def _report_is_valid(
    payload: object,
    *,
    ctx: RolloutContext,
    envelope_sha256: str,
) -> bool:
    assert ctx.request_id is not None
    assert ctx.attempt_number is not None
    return browser_report_ready(
        payload,
        authority=RolloutBrowserReportAuthority(
            request_id=ctx.request_id,
            attempt_number=ctx.attempt_number,
            request_envelope_sha256=envelope_sha256,
            candidate_sha=ctx.resolved_sha,
            route=_browser_route(ctx),
        ),
    )


class BrowserAcceptanceStep(BaseStep):
    number = 16
    name = "staging-admin-browser-acceptance"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        if (
            ctx.environment != "staging"
            or ctx.request_id is None
            or ctx.attempt_number is None
            or ctx.request_envelope_path is None
        ):
            return RunResult(
                exit_code=1,
                error="browser acceptance requires a broker-owned staging attempt envelope",
            )
        if ctx.request_id.startswith("req-") is False:
            return RunResult(exit_code=1, error="browser acceptance request identity is invalid")

        try:
            envelope, _metadata = _read_private_file(
                ctx.request_envelope_path,
                max_bytes=_MAX_ENVELOPE_BYTES,
            )
            envelope_payload = json.loads(envelope)
            if not isinstance(envelope_payload, dict) or any(
                envelope_payload.get(key) != value
                for key, value in (
                    ("request_id", ctx.request_id),
                    ("attempt_number", ctx.attempt_number),
                    ("resolved_sha", ctx.resolved_sha),
                )
            ):
                raise ValueError("browser acceptance envelope binding is invalid")
            envelope_sha256 = hashlib.sha256(envelope).hexdigest()
            route = _browser_route(ctx)
            token_file = _token_file(ctx)
            _validate_admin_token_file(token_file)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            return RunResult(exit_code=1, error=str(exc))

        output_directory = step_dir.path / _OUTPUT_DIRECTORY_NAME
        try:
            output_directory.mkdir(mode=0o700)
        except OSError as exc:
            return RunResult(exit_code=1, error=f"browser evidence directory failed: {exc}")
        report_path = output_directory / _REPORT_NAME
        if report_path.exists():
            return RunResult(exit_code=1, error="browser acceptance report already exists")

        container_name = f"loom-browser-{ctx.request_id}-{ctx.attempt_number}"
        command = [
            "docker",
            "run",
            "--rm",
            "--pull=never",
            "--name",
            container_name,
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--network=bridge",
            "--pids-limit=512",
            "--memory=2g",
            "--cpus=2",
            "--shm-size=512m",
            "--user",
            f"{os.geteuid()}:{os.getegid()}",
            "--env",
            "HOME=/tmp",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=512m,mode=1777",
            "--mount",
            f"type=bind,src={token_file},dst=/run/secrets/admin-token,readonly,bind-propagation=rprivate",
            "--mount",
            f"type=bind,src={output_directory},dst=/evidence,bind-propagation=rprivate",
            image_tag(BROWSER_ACCEPTANCE_IMAGE, ctx),
            "--route",
            route,
            "--expected-deployed-sha",
            ctx.resolved_sha,
            "--admin-token-source",
            "file:/run/secrets/admin-token",
            "--username",
            BROWSER_ACCEPTANCE_USERNAME,
            "--report",
            f"/evidence/{_REPORT_NAME}",
            "--rollout-request-id",
            ctx.request_id,
            "--rollout-attempt-number",
            str(ctx.attempt_number),
            "--request-envelope-sha256",
            envelope_sha256,
            "--timeout-ms",
            "120000",
        ]
        result = run_captured(command)
        self.write_stdout(step_dir, result.stdout)
        self.write_stderr(step_dir, result.stderr)

        try:
            report_bytes, _report_metadata = _read_private_file(
                report_path,
                max_bytes=_MAX_REPORT_BYTES,
            )
            report = cast(object, json.loads(report_bytes))
        except (OSError, ValueError, json.JSONDecodeError):
            return RunResult(
                exit_code=result.returncode or 1,
                error="browser acceptance did not produce a valid private report",
            )
        if result.returncode != 0 or not _report_is_valid(
            report,
            ctx=ctx,
            envelope_sha256=envelope_sha256,
        ):
            return RunResult(
                exit_code=result.returncode or 1,
                error="candidate-bound staging admin browser acceptance failed",
                artifacts={"browser_report": str(report_path)},
            )
        return RunResult(
            exit_code=0,
            summary="candidate-bound staging admin browser acceptance passed",
            artifacts={
                "browser_report": str(report_path),
                "browser_report_sha256": hashlib.sha256(report_bytes).hexdigest(),
                "request_envelope_sha256": envelope_sha256,
            },
        )


__all__ = ["BROWSER_ACCEPTANCE_IMAGE", "BrowserAcceptanceStep"]
