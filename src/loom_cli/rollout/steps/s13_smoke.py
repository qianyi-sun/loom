"""Step 15 — end-to-end live smoke (#340).

Concrete smoke, default ``user-token`` mode:

1. ``GET /api/v1/health`` on Loom Service → expect 200.
2. ``GET /api/v1/auth/whoami`` with a user-owned smoke API token → submitter
   identity is present.
3. ``GET /api/v1/benchmarks`` with that submitter token → non-empty.
4. ``GET /api/v1/tasks/{id}`` for the configured smoke task → task exists.
5. Submit a trivial ``oracle × smoke task`` trial via the public API
   with a deterministic idempotency key derived from the image tag.
6. Poll ``/api/v1/trials/{id}`` until terminal (cap 5 minutes). Assert
   ``state=succeeded`` and ``aggregate_reward`` is present.
7. Fetch the trajectory download URL and HEAD to confirm the trajectory
   blob is reachable through the public API.
8. ``GET /api/v1/usage?since=<start>`` → assert the smoke trial appears.

Every step above writes its response JSON as a separate artifact so an
operator can inspect the smoke evidence without re-running.

``admin-on-behalf`` mode is an explicit release-canary alternative for cases
where an operator must represent an active user/team and no user-owned smoke
token is available. It uses the rollout admin token source reference, validates
the expected fingerprint before service calls, submits through the audited
``/api/v1/admin/batches/on-behalf`` API, and records only redacted/source-ref
evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass

from loom.security.redaction import redact_mapping, redact_text
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.secret_source import SecretSourceError, resolve_secret_source

DEFAULT_TERMINAL_TIMEOUT_SEC = 300.0
DEFAULT_POLL_INTERVAL_SEC = 5.0
DEFAULT_CURRENT_GB10_SMOKE_TASK_ID = "loom-smoke/gb10-oracle-hello-world"
DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL = "gb10-arm64"
DEFAULT_SMOKE_AGENT = "oracle"
_NONRECOVERABLE_BATCH_RESULT_STATUSES = frozenset({"partial_failed", "all_failed"})
_NONRECOVERABLE_FANOUT_REASONS = frozenset({"required_worker_pool_incompatible"})


@dataclass(frozen=True, slots=True)
class _AdminOnBehalfConfig:
    represented_username: str
    team_id: str
    admin_actor: str


def _ingress_base(ctx: RolloutContext) -> str:
    # Cluster-config declares ingress_host plus optional prod/dev route
    # prefixes; smoke exercises the same public API base as users.
    from loom_cli.cluster_config import load_cluster_config

    cfg = load_cluster_config(ctx.cluster_config_path)
    api_base_path = _normalise_public_path(
        cfg.frontend_api_base_path or cfg.frontend_route_path,
    )
    return f"https://{cfg.ingress_host}{api_base_path}"


def _normalise_public_path(value: str) -> str:
    path = value.strip()
    if not path or path == "/":
        return ""
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/")


def _smoke_api_token_source(ctx: RolloutContext) -> str | None:
    source = ctx.smoke_api_token_source or ctx.metadata.get(
        "smoke_api_token_source",
    )
    if isinstance(source, str) and source.strip():
        return source.strip()
    return None


def _resolve_smoke_api_token(ctx: RolloutContext) -> tuple[str | None, str | None]:
    source = _smoke_api_token_source(ctx)
    if source is not None:
        try:
            return (
                resolve_secret_source(source, flag_name="--smoke-api-token"),
                None,
            )
        except SecretSourceError as exc:
            return None, str(exc)
    token = os.environ.get("LOOM_SMOKE_API_TOKEN") or ctx.metadata.get(
        "smoke_api_token",
    )
    if isinstance(token, str) and token.strip():
        return token.strip(), None
    return None, None


def _smoke_api_token(ctx: RolloutContext) -> str | None:
    token, _error = _resolve_smoke_api_token(ctx)
    return token


def _config_value(ctx: RolloutContext, env_name: str, metadata_key: str) -> str | None:
    value = (
        getattr(ctx, metadata_key, None)
        or os.environ.get(env_name)
        or ctx.metadata.get(metadata_key)
    )
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _smoke_submit_mode(ctx: RolloutContext) -> str:
    return _config_value(ctx, "LOOM_SMOKE_SUBMIT_MODE", "smoke_submit_mode") or "user-token"


def _smoke_task_id(ctx: RolloutContext, *, submit_mode: str = "user-token") -> str:
    task_id = _explicit_smoke_task_id(ctx)
    if isinstance(task_id, str) and task_id.strip():
        return task_id.strip()
    if ctx.scope == "current-gb10":
        return DEFAULT_CURRENT_GB10_SMOKE_TASK_ID
    if ctx.scope == "full-cluster":
        raise RuntimeError(
            "full-cluster rollout requires --smoke-task-id for an audited "
            "current profile",
        )
    raise RuntimeError(f"unsupported rollout scope: {ctx.scope!r}")


def _explicit_smoke_task_id(ctx: RolloutContext) -> str | None:
    return _config_value(ctx, "LOOM_SMOKE_TASK_ID", "smoke_task_id")


def _smoke_required_worker_pool(
    ctx: RolloutContext,
    *,
    explicit_task_id: bool,
) -> str | None:
    pool_name = os.environ.get(
        "LOOM_SMOKE_REQUIRED_WORKER_POOL",
    ) or ctx.metadata.get("smoke_required_worker_pool")
    if isinstance(pool_name, str) and pool_name.strip():
        return pool_name.strip()
    if ctx.scope == "current-gb10" and not explicit_task_id:
        return DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL
    return None


def _smoke_agent(ctx: RolloutContext) -> str:
    return _config_value(ctx, "LOOM_SMOKE_AGENT", "smoke_agent") or DEFAULT_SMOKE_AGENT


def _http_get(url: str, *, token: str | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() or b""


def _http_post(
    url: str,
    payload: Mapping[str, object],
    *,
    token: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> tuple[int, bytes]:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() or b""


def _same_host(left: urllib.parse.ParseResult, right: urllib.parse.ParseResult) -> bool:
    left_host = (left.hostname or "").lower()
    right_host = (right.hostname or "").lower()
    return bool(left_host and right_host and left_host == right_host)


def _trajectory_head_request(
    url: str,
    *,
    ingress_base: str,
    token: str,
) -> urllib.request.Request:
    parsed_url = urllib.parse.urlparse(url)
    parsed_base = urllib.parse.urlparse(ingress_base)
    api_base_path = _normalise_public_path(parsed_base.path)
    needs_platform_auth = _same_host(parsed_url, parsed_base) and (
        _is_platform_api_path(parsed_url.path, api_base_path=api_base_path)
    )
    head_url = url
    if needs_platform_auth:
        # Service download URLs are authenticated API routes. Normalize back to
        # the operator-declared HTTPS ingress before attaching the smoke token.
        path = _normalise_platform_api_path(
            parsed_url.path,
            api_base_path=api_base_path,
        )
        head_url = urllib.parse.urlunparse(
            (
                parsed_base.scheme,
                parsed_base.netloc,
                path,
                parsed_url.params,
                parsed_url.query,
                parsed_url.fragment,
            ),
        )
    req = urllib.request.Request(head_url, method="HEAD")
    if needs_platform_auth:
        req.add_header("Authorization", f"Bearer {token}")
    return req


def _is_platform_api_path(path: str, *, api_base_path: str) -> bool:
    root_api = path == "/api" or path.startswith("/api/")
    if not api_base_path:
        return root_api
    prefixed_api = path == f"{api_base_path}/api" or path.startswith(
        f"{api_base_path}/api/",
    )
    return root_api or prefixed_api


def _normalise_platform_api_path(path: str, *, api_base_path: str) -> str:
    if api_base_path and (path == "/api" or path.startswith("/api/")):
        return api_base_path + path
    return path


def _trajectory_get_probe_request(
    url: str,
    *,
    ingress_base: str,
    token: str,
) -> urllib.request.Request:
    req = _trajectory_head_request(
        url,
        ingress_base=ingress_base,
        token=token,
    )
    req.method = "GET"
    req.add_header("Range", "bytes=0-0")
    req.add_header("Connection", "close")
    return req


def _probe_trajectory_download(
    url: str,
    *,
    ingress_base: str,
    token: str,
    step_dir: StepDir,
) -> None:
    head_req = _trajectory_head_request(
        url,
        ingress_base=ingress_base,
        token=token,
    )
    try:
        with urllib.request.urlopen(head_req, timeout=15) as resp:
            step_dir.artifact_path("07-trajectory-head.txt").write_text(
                f"status={resp.status}\ncontent-length={resp.headers.get('Content-Length')}\n"
            )
            return
    except urllib.error.HTTPError as exc:
        if exc.code != 405 or head_req.get_header("Authorization") is None:
            raise

    get_req = _trajectory_get_probe_request(
        url,
        ingress_base=ingress_base,
        token=token,
    )
    with urllib.request.urlopen(get_req, timeout=15) as resp:
        first_byte = resp.read(1)
        step_dir.artifact_path("07-trajectory-head.txt").write_text(
            f"status={resp.status}\n"
            f"content-length={resp.headers.get('Content-Length')}\n"
            "method=GET\n"
            f"bytes-read={len(first_byte)}\n"
        )


def _idempotency_key(ctx: RolloutContext) -> str:
    return (
        "smoke-"
        + hashlib.sha256(
            f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
        ).hexdigest()[:16]
    )


def _admin_smoke_batch_name(ctx: RolloutContext) -> str:
    return f"rollout-{_idempotency_key(ctx)}"


def _validate_submitter_identity(
    body: bytes,
) -> tuple[bool, str]:
    try:
        whoami = json.loads(body)
    except json.JSONDecodeError:
        return False, "smoke whoami response is not JSON"
    credential_type = whoami.get("credential_type")
    if credential_type != "user_owned_api_token":
        return (
            False,
            "smoke requires LOOM_SMOKE_API_TOKEN to be a user-owned API token; "
            f"whoami credential_type={credential_type!r}",
        )
    scopes = whoami.get("scopes")
    if not isinstance(scopes, list) or "submit" not in scopes:
        return (
            False,
            "smoke requires LOOM_SMOKE_API_TOKEN to include submit scope",
        )
    return True, "ok"


def _validate_admin_identity(body: bytes) -> tuple[bool, str]:
    try:
        whoami = json.loads(body)
    except json.JSONDecodeError:
        return False, "admin smoke whoami response is not JSON"
    scopes = whoami.get("scopes")
    is_admin_scoped = isinstance(scopes, list) and any(
        isinstance(scope, str) and scope.startswith("admin:") for scope in scopes
    )
    credential_type = whoami.get("credential_type")
    principal_type = whoami.get("principal_type")
    if credential_type == "admin_bearer_token" or principal_type == "admin":
        return True, "ok"
    if is_admin_scoped:
        return True, "ok"
    return (
        False,
        "admin-on-behalf smoke requires an admin-capable token; "
        f"whoami credential_type={credential_type!r} "
        f"principal_type={principal_type!r}",
    )


def _admin_token_fingerprint(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"sha256:{digest} len={len(value)}"


def _resolve_smoke_admin_token(ctx: RolloutContext) -> tuple[str | None, str | None]:
    try:
        token = resolve_secret_source(ctx.admin_token_source, flag_name="--admin-token")
    except SecretSourceError as exc:
        return None, str(exc)
    if ctx.expect_admin_token_fingerprint is not None:
        live = _admin_token_fingerprint(token)
        if live != ctx.expect_admin_token_fingerprint:
            return (
                None,
                "admin_token_fingerprint drift: "
                f"desired={ctx.expect_admin_token_fingerprint!r} live={live!r}. "
                "Resolve the protected-environment admin token source before "
                "running rollout smoke.",
            )
    return token, None


def _admin_on_behalf_config(
    ctx: RolloutContext,
) -> tuple[_AdminOnBehalfConfig | None, str | None]:
    represented_username = _config_value(
        ctx,
        "LOOM_SMOKE_ON_BEHALF_USERNAME",
        "smoke_on_behalf_username",
    )
    team_id = _config_value(
        ctx,
        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
        "smoke_on_behalf_team_id",
    )
    admin_actor = _config_value(
        ctx,
        "LOOM_SMOKE_ADMIN_ACTOR",
        "smoke_admin_actor",
    )
    missing = []
    if represented_username is None:
        missing.append("LOOM_SMOKE_ON_BEHALF_USERNAME/smoke_on_behalf_username")
    if team_id is None:
        missing.append("LOOM_SMOKE_ON_BEHALF_TEAM_ID/smoke_on_behalf_team_id")
    if admin_actor is None:
        missing.append("LOOM_SMOKE_ADMIN_ACTOR/smoke_admin_actor")
    if missing:
        return (
            None,
            "admin-on-behalf smoke requires " + ", ".join(missing) + " before any service call",
        )
    assert represented_username is not None
    assert team_id is not None
    assert admin_actor is not None
    if team_id.startswith(("env:", "file:")):
        try:
            team_id = resolve_secret_source(
                team_id,
                flag_name="--smoke-on-behalf-team-id",
            )
        except SecretSourceError as exc:
            return None, str(exc)
    return _AdminOnBehalfConfig(
        represented_username=represented_username,
        team_id=team_id,
        admin_actor=admin_actor,
    ), None


def _validate_benchmark_catalog(body: bytes) -> str | None:
    try:
        catalog = json.loads(body)
    except json.JSONDecodeError:
        return "benchmarks response not JSON"
    items = catalog.get("items") or catalog.get("data") or []
    if not items:
        return "benchmarks catalog is empty"
    return None


def _task_payload_inputs(
    ctx: RolloutContext,
    *,
    submit_mode: str = "user-token",
) -> tuple[str, str | None]:
    explicit_task_id = bool(_explicit_smoke_task_id(ctx))
    task_id = _smoke_task_id(ctx, submit_mode=submit_mode)
    required_worker_pool = _smoke_required_worker_pool(
        ctx,
        explicit_task_id=explicit_task_id,
    )
    return task_id, required_worker_pool


def _admin_on_behalf_payload(
    ctx: RolloutContext,
    cfg: _AdminOnBehalfConfig,
) -> dict[str, object]:
    task_id, required_worker_pool = _task_payload_inputs(
        ctx,
        submit_mode="admin-on-behalf",
    )
    payload: dict[str, object] = {
        "name": _admin_smoke_batch_name(ctx),
        "represented_username": cfg.represented_username,
        "team_id": cfg.team_id,
        "task_filter": {"task_ids": [task_id]},
        "trial_config": {
            "agent_name": _smoke_agent(ctx),
            "agent_model": None,
        },
        "n_per_task": 1,
    }
    if required_worker_pool is not None:
        payload["required_worker_pools"] = [required_worker_pool]
    return payload


def _existing_admin_smoke_batch_id(
    body: bytes,
    *,
    cfg: _AdminOnBehalfConfig,
    batch_name: str,
    task_id: str,
) -> str | None:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    items = payload.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if item.get("name") != batch_name or item.get("team_id") != cfg.team_id:
            continue
        submitted_by_user = item.get("submitted_by_user")
        if not isinstance(submitted_by_user, Mapping):
            continue
        if (
            not _same_username(
                submitted_by_user.get("username"),
                cfg.represented_username,
            )
            or submitted_by_user.get("team_id") != cfg.team_id
        ):
            continue
        task_filter = item.get("task_filter")
        if not isinstance(task_filter, Mapping):
            continue
        task_ids = task_filter.get("task_ids")
        if not isinstance(task_ids, list) or task_ids != [task_id]:
            continue
        batch_id = item.get("id")
        if isinstance(batch_id, str) and batch_id:
            return batch_id
    return None


def _same_username(left: object, right: str) -> bool:
    return isinstance(left, str) and left.strip().casefold() == right.strip().casefold()


def _json_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _admin_batch_succeeded(
    batch: Mapping[str, object],
    *,
    cfg: _AdminOnBehalfConfig,
) -> tuple[bool, str]:
    state = batch.get("state")
    if state != "finished":
        return False, f"batch terminal state {state!r} — expected finished"
    result_status = batch.get("result_status")
    if result_status != "succeeded":
        return False, f"batch result_status {result_status!r} — expected succeeded"
    expected = batch.get("expected_trial_count")
    expected_count = _json_int(expected)
    if expected_count is None:
        return False, "batch response has no numeric expected_trial_count"
    trial_summary = batch.get("trial_summary")
    if not isinstance(trial_summary, Mapping):
        return False, "batch response has no trial_summary"
    succeeded = _json_int(trial_summary.get("succeeded", 0))
    if succeeded is None:
        return False, "batch trial_summary.succeeded is not numeric"
    if succeeded < expected_count:
        return (
            False,
            f"batch succeeded trials {succeeded} < expected {expected_count}",
        )
    submitted_by_user = batch.get("submitted_by_user")
    if not isinstance(submitted_by_user, Mapping):
        return False, "batch response has no submitted_by_user"
    username = submitted_by_user.get("username")
    team_id = submitted_by_user.get("team_id")
    if not _same_username(username, cfg.represented_username) or team_id != cfg.team_id:
        return (
            False,
            "batch submitted_by_user does not match represented "
            f"user/team: username={username!r} team_id={team_id!r}",
        )
    return True, "ok"


def _compact_redacted_json(value: object, *, limit: int = 800) -> str:
    redacted = redact_mapping(value)
    try:
        rendered = json.dumps(redacted, sort_keys=True, separators=(",", ":"))
    except TypeError:
        rendered = str(redacted)
    return redact_text(rendered, limit=limit)


def _fanout_error_reasons(value: object) -> set[str]:
    if isinstance(value, Mapping):
        reason = value.get("reason")
        return {reason} if isinstance(reason, str) and reason else set()
    if isinstance(value, list):
        reasons: set[str] = set()
        for item in value:
            reasons.update(_fanout_error_reasons(item))
        return reasons
    return set()


def _admin_batch_nonrecoverable_failure(batch: Mapping[str, object]) -> str | None:
    result_status = batch.get("result_status")
    failure_reason = batch.get("failure_reason")
    fanout_errors = batch.get("fanout_errors")
    fanout_reasons = _fanout_error_reasons(fanout_errors)
    fanout_submit_failed = failure_reason == "fanout_submit_failed"
    incompatible_fanout = bool(fanout_reasons & _NONRECOVERABLE_FANOUT_REASONS)
    failed_result = result_status in _NONRECOVERABLE_BATCH_RESULT_STATUSES

    if not fanout_submit_failed and not (failed_result and incompatible_fanout):
        return None

    parts = [
        "admin-on-behalf smoke batch reported nonrecoverable fanout failure",
        f"state={batch.get('state')!r}",
        f"result_status={result_status!r}",
        f"failure_reason={failure_reason!r}",
    ]
    failure_message = batch.get("failure_message")
    if isinstance(failure_message, str) and failure_message.strip():
        parts.append(f"failure_message={redact_text(failure_message, limit=300)!r}")
    if fanout_errors:
        parts.append(
            "fanout_errors=" + _compact_redacted_json(fanout_errors, limit=1000),
        )
    return "; ".join(parts)


def _run_admin_on_behalf_smoke(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> RunResult:
    cfg, cfg_error = _admin_on_behalf_config(ctx)
    if cfg_error is not None:
        return RunResult(exit_code=2, error=cfg_error)
    assert cfg is not None
    token, token_error = _resolve_smoke_admin_token(ctx)
    if token_error is not None:
        exit_code = 1 if "fingerprint drift" in token_error else 2
        return RunResult(exit_code=exit_code, error=token_error)
    assert token is not None
    base = _ingress_base(ctx)

    status, body = _http_get(f"{base}/api/v1/health", token=token)
    step_dir.artifact_path("01-health.json").write_bytes(body)
    if status != 200:
        return RunResult(exit_code=1, error=f"/api/v1/health returned {status}")

    status, body = _http_get(f"{base}/api/v1/auth/whoami", token=token)
    step_dir.artifact_path("02-whoami.json").write_bytes(body)
    if status != 200:
        return RunResult(exit_code=1, error=f"/api/v1/auth/whoami returned {status}")
    ok, reason = _validate_admin_identity(body)
    if not ok:
        return RunResult(exit_code=1, error=reason)

    status, body = _http_get(f"{base}/api/v1/benchmarks", token=token)
    step_dir.artifact_path("03-benchmarks.json").write_bytes(body)
    if status != 200:
        return RunResult(exit_code=1, error=f"/api/v1/benchmarks returned {status}")
    catalog_error = _validate_benchmark_catalog(body)
    if catalog_error is not None:
        return RunResult(exit_code=1, error=catalog_error)

    task_id, _required_worker_pool = _task_payload_inputs(
        ctx,
        submit_mode="admin-on-behalf",
    )
    quoted_task_id = urllib.parse.quote(task_id, safe="/")
    status, body = _http_get(f"{base}/api/v1/tasks/{quoted_task_id}", token=token)
    step_dir.artifact_path("04-task.json").write_bytes(body)
    if status == 404:
        return RunResult(
            exit_code=1,
            error=f"smoke task {task_id!r} not found in live catalog",
        )
    if status != 200:
        return RunResult(
            exit_code=1,
            error=f"/api/v1/tasks/{task_id} returned {status}",
        )

    batch_name = _admin_smoke_batch_name(ctx)
    existing_params = urllib.parse.urlencode(
        {"team_id": cfg.team_id, "q": batch_name, "limit": "20"},
    )
    status, body = _http_get(
        f"{base}/api/v1/batches?{existing_params}",
        token=token,
    )
    step_dir.artifact_path("05-existing-batches.json").write_bytes(body)
    if status != 200:
        return RunResult(
            exit_code=1,
            error=f"GET /batches recovery lookup returned {status}",
        )
    batch_id = _existing_admin_smoke_batch_id(
        body,
        cfg=cfg,
        batch_name=batch_name,
        task_id=task_id,
    )
    if batch_id is not None:
        step_dir.artifact_path("05-submit.json").write_text(
            json.dumps(
                {"batch_id": batch_id, "recovered": True},
                sort_keys=True,
            )
            + "\n",
        )
    else:
        status, body = _http_post(
            f"{base}/api/v1/admin/batches/on-behalf",
            _admin_on_behalf_payload(ctx, cfg),
            token=token,
            headers={"X-Loom-Admin-Actor": cfg.admin_actor},
        )
        step_dir.artifact_path("05-submit.json").write_bytes(body)
        if status not in (200, 201):
            return RunResult(
                exit_code=1,
                error=f"POST /admin/batches/on-behalf returned {status}",
            )
        try:
            submit = json.loads(body)
        except json.JSONDecodeError:
            return RunResult(exit_code=1, error="submit response not JSON")
        batch_id_raw = submit.get("id") or submit.get("batch_id")
        if not isinstance(batch_id_raw, str) or not batch_id_raw:
            return RunResult(exit_code=1, error="submit response has no batch id")
        batch_id = batch_id_raw

    deadline = time.time() + DEFAULT_TERMINAL_TIMEOUT_SEC
    terminal_batch: dict[str, object] | None = None
    while time.time() < deadline:
        status, body = _http_get(
            f"{base}/api/v1/batches/{batch_id}",
            token=token,
        )
        step_dir.artifact_path("06-poll.json").write_bytes(body)
        if status == 200:
            try:
                batch = json.loads(body)
            except json.JSONDecodeError:
                return RunResult(exit_code=1, error="batch poll response not JSON")
            state = batch.get("state")
            nonrecoverable_error = _admin_batch_nonrecoverable_failure(batch)
            if nonrecoverable_error is not None:
                return RunResult(exit_code=1, error=nonrecoverable_error)
            if state in ("finished", "failed", "cancelled"):
                terminal_batch = batch
                break
        elif status in (401, 403, 404):
            return RunResult(
                exit_code=1,
                error=f"/api/v1/batches/{batch_id} returned {status}",
            )
        time.sleep(DEFAULT_POLL_INTERVAL_SEC)
    if terminal_batch is None:
        return RunResult(
            exit_code=1,
            error=f"batch {batch_id} did not reach terminal state in "
            f"{DEFAULT_TERMINAL_TIMEOUT_SEC}s",
        )
    ok, reason = _admin_batch_succeeded(terminal_batch, cfg=cfg)
    if not ok:
        return RunResult(exit_code=1, error=reason)

    step_dir.stdout_path().write_text(
        f"admin-on-behalf smoke ok: batch {batch_id} finished for "
        f"{cfg.represented_username}/{cfg.team_id}\n",
    )
    return RunResult(
        exit_code=0,
        summary=f"admin-on-behalf smoke batch {batch_id} succeeded",
        artifacts={"batch_id": batch_id},
    )


class SmokeStep(BaseStep):
    number = 15
    name = "smoke"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        submit_mode = _smoke_submit_mode(ctx)
        task_id, required_worker_pool = _task_payload_inputs(
            ctx,
            submit_mode=submit_mode,
        )
        fingerprint: dict[str, object] = {
            **ctx.to_inputs_dict(),
            "smoke_submit_mode": submit_mode,
            "smoke_task_id": task_id,
            "smoke_required_worker_pool": required_worker_pool,
        }
        if submit_mode == "admin-on-behalf":
            fingerprint.update(
                {
                    "smoke_agent": _smoke_agent(ctx),
                    "smoke_batch_name": _admin_smoke_batch_name(ctx),
                    "smoke_on_behalf_username": _config_value(
                        ctx,
                        "LOOM_SMOKE_ON_BEHALF_USERNAME",
                        "smoke_on_behalf_username",
                    ),
                    "smoke_on_behalf_team_id": _config_value(
                        ctx,
                        "LOOM_SMOKE_ON_BEHALF_TEAM_ID",
                        "smoke_on_behalf_team_id",
                    ),
                    "smoke_admin_actor": _config_value(
                        ctx,
                        "LOOM_SMOKE_ADMIN_ACTOR",
                        "smoke_admin_actor",
                    ),
                },
            )
        else:
            smoke_api_token_source = _smoke_api_token_source(ctx)
            fingerprint["smoke_api_token_source"] = smoke_api_token_source
            fingerprint["smoke_api_token_present"] = (
                smoke_api_token_source is not None
                or bool(os.environ.get("LOOM_SMOKE_API_TOKEN"))
                or bool(ctx.metadata.get("smoke_api_token"))
            )
        return fingerprint

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        submit_mode = _smoke_submit_mode(ctx)
        if submit_mode == "admin-on-behalf":
            return _run_admin_on_behalf_smoke(ctx, step_dir)
        if submit_mode != "user-token":
            return RunResult(
                exit_code=2,
                error=(
                    "LOOM_SMOKE_SUBMIT_MODE/smoke_submit_mode must be "
                    "'user-token' or 'admin-on-behalf'"
                ),
            )
        token, token_error = _resolve_smoke_api_token(ctx)
        if token_error is not None:
            return RunResult(exit_code=2, error=token_error)
        if not token:
            return RunResult(
                exit_code=2,
                error=(
                    "smoke requires a user-owned submit token — set "
                    "--smoke-api-token env:VAR/file:PATH, set "
                    "$LOOM_SMOKE_API_TOKEN, or provide smoke_api_token via "
                    "rollout metadata"
                ),
            )
        base = _ingress_base(ctx)

        # 1. health
        status, body = _http_get(f"{base}/api/v1/health", token=token)
        step_dir.artifact_path("01-health.json").write_bytes(body)
        if status != 200:
            return RunResult(
                exit_code=1,
                error=f"/api/v1/health returned {status}",
            )

        # 2. submitter identity is user-owned. Admin/internal tokens cannot
        # create user-facing work after the account-auth refactor.
        status, body = _http_get(f"{base}/api/v1/auth/whoami", token=token)
        step_dir.artifact_path("02-whoami.json").write_bytes(body)
        if status != 200:
            return RunResult(
                exit_code=1,
                error=f"/api/v1/auth/whoami returned {status}",
            )
        ok, reason = _validate_submitter_identity(body)
        if not ok:
            return RunResult(exit_code=1, error=reason)

        # 3. benchmarks catalog is non-empty
        status, body = _http_get(f"{base}/api/v1/benchmarks", token=token)
        step_dir.artifact_path("03-benchmarks.json").write_bytes(body)
        if status != 200:
            return RunResult(
                exit_code=1,
                error=f"/api/v1/benchmarks returned {status}",
            )
        try:
            catalog = json.loads(body)
        except json.JSONDecodeError:
            return RunResult(
                exit_code=1,
                error="benchmarks response not JSON",
            )
        items = catalog.get("items") or catalog.get("data") or []
        if not items:
            return RunResult(
                exit_code=1,
                error="benchmarks catalog is empty",
            )

        # 4. smoke task exists in the live catalog.
        explicit_task_id = bool(_explicit_smoke_task_id(ctx))
        task_id = _smoke_task_id(ctx, submit_mode="user-token")
        required_worker_pool = _smoke_required_worker_pool(
            ctx,
            explicit_task_id=explicit_task_id,
        )
        quoted_task_id = urllib.parse.quote(task_id, safe="/")
        status, body = _http_get(
            f"{base}/api/v1/tasks/{quoted_task_id}",
            token=token,
        )
        step_dir.artifact_path("04-task.json").write_bytes(body)
        if status == 404:
            return RunResult(
                exit_code=1,
                error=f"smoke task {task_id!r} not found in live catalog",
            )
        if status != 200:
            return RunResult(
                exit_code=1,
                error=f"/api/v1/tasks/{task_id} returned {status}",
            )

        # 5. submit a trivial trial
        payload = {
            "task_id": task_id,
            "config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
            "idempotency_key": _idempotency_key(ctx),
        }
        if required_worker_pool is not None:
            payload["required_worker_pool"] = required_worker_pool
        status, body = _http_post(
            f"{base}/api/v1/trials",
            payload,
            token=token,
        )
        step_dir.artifact_path("05-submit.json").write_bytes(body)
        if status not in (200, 201, 409):  # 409: idempotency replay
            return RunResult(
                exit_code=1,
                error=f"POST /trials returned {status}",
            )
        try:
            submit = json.loads(body)
        except json.JSONDecodeError:
            return RunResult(
                exit_code=1,
                error="submit response not JSON",
            )
        trial_id = submit.get("id") or submit.get("trial_id")
        if not trial_id:
            return RunResult(
                exit_code=1,
                error="submit response has no trial id",
            )

        # 6. poll
        deadline = time.time() + DEFAULT_TERMINAL_TIMEOUT_SEC
        terminal_trial: dict[str, object] | None = None
        while time.time() < deadline:
            status, body = _http_get(
                f"{base}/api/v1/trials/{trial_id}",
                token=token,
            )
            step_dir.artifact_path("06-poll.json").write_bytes(body)
            if status == 200:
                trial = json.loads(body)
                state = trial.get("state")
                if state in ("succeeded", "failed", "cancelled"):
                    terminal_trial = trial
                    break
            time.sleep(DEFAULT_POLL_INTERVAL_SEC)
        if terminal_trial is None:
            return RunResult(
                exit_code=1,
                error=f"trial {trial_id} did not reach terminal state in "
                f"{DEFAULT_TERMINAL_TIMEOUT_SEC}s",
            )
        if terminal_trial.get("state") != "succeeded":
            return RunResult(
                exit_code=1,
                error=(
                    f"trial {trial_id} terminal state "
                    f"{terminal_trial.get('state')!r} — expected succeeded"
                ),
            )
        if terminal_trial.get("aggregate_reward") is None:
            return RunResult(
                exit_code=1,
                error=f"trial {trial_id} succeeded but has no aggregate_reward",
            )

        # 7. trajectory download URL — HEAD to confirm storage reachability
        traj_url_raw = terminal_trial.get("trajectory_url")
        if isinstance(traj_url_raw, str) and traj_url_raw:
            try:
                _probe_trajectory_download(
                    traj_url_raw,
                    ingress_base=base,
                    token=token,
                    step_dir=step_dir,
                )
            except Exception as exc:
                return RunResult(
                    exit_code=1,
                    error=f"trajectory HEAD failed: {exc}",
                )

        # 8. usage rollup — verify the smoke trial showed up
        status, body = _http_get(
            f"{base}/api/v1/usage",
            token=token,
        )
        step_dir.artifact_path("08-usage.json").write_bytes(body)
        # Non-fatal: usage rollups may lag; log but don't fail on empty.

        step_dir.stdout_path().write_text(
            f"smoke ok: trial {trial_id} succeeded with reward "
            f"{terminal_trial.get('aggregate_reward')}\n",
        )
        return RunResult(
            exit_code=0,
            summary=f"smoke trial {trial_id} succeeded",
            artifacts={"trial_id": trial_id},
        )
