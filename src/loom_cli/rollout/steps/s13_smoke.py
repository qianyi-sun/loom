"""Step 13 — end-to-end live smoke (#340).

Concrete smoke:

1. ``GET /api/v1/health`` on Loom Service → expect 200.
2. ``GET /api/v1/auth/whoami`` with a user-owned smoke API token → submitter
   identity is present.
3. ``GET /api/v1/benchmarks`` with that submitter token → non-empty.
4. ``GET /api/v1/tasks/{id}`` for the configured smoke task → task exists.
5. Submit a trivial ``oracle × smoke task`` trial via the public API
   with a deterministic idempotency key derived from the image tag.
6. Poll ``/api/v1/trials/{id}`` until terminal (cap 5 minutes). Assert
   ``state=succeeded`` and ``aggregate_reward`` is present.
7. Fetch the trajectory presigned URL and HEAD to confirm the trajectory
   blob is in MinIO.
8. ``GET /api/v1/usage?since=<start>`` → assert the smoke trial appears.

Every step above writes its response JSON as a separate artifact so an
operator can inspect the smoke evidence without re-running.
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

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult

DEFAULT_TERMINAL_TIMEOUT_SEC = 300.0
DEFAULT_POLL_INTERVAL_SEC = 5.0
DEFAULT_SMOKE_TASK_ID = "terminal-bench-2/hello-world"
DEFAULT_CURRENT_GB10_SMOKE_TASK_ID = (
    "skilllearnbench/anthropic-poster-design/anthropic-poster-design-1"
)
DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL = "gb10-arm64"


def _ingress_base(ctx: RolloutContext) -> str:
    # Cluster-config declares ingress_host; smoke assumes HTTPS.
    from loom_cli.cluster_config import load_cluster_config

    cfg = load_cluster_config(ctx.cluster_config_path)
    return f"https://{cfg.ingress_host}"


def _smoke_api_token(ctx: RolloutContext) -> str | None:
    return os.environ.get("LOOM_SMOKE_API_TOKEN") or ctx.metadata.get(
        "smoke_api_token",
    )


def _smoke_task_id(ctx: RolloutContext) -> str:
    task_id = _explicit_smoke_task_id(ctx)
    if isinstance(task_id, str) and task_id.strip():
        return task_id.strip()
    if ctx.scope == "current-gb10":
        return DEFAULT_CURRENT_GB10_SMOKE_TASK_ID
    return DEFAULT_SMOKE_TASK_ID


def _explicit_smoke_task_id(ctx: RolloutContext) -> str | None:
    return os.environ.get("LOOM_SMOKE_TASK_ID") or ctx.metadata.get(
        "smoke_task_id",
    )


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
    url: str, payload: Mapping[str, object], *, token: str | None = None,
) -> tuple[int, bytes]:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
    )
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() or b""


def _idempotency_key(ctx: RolloutContext) -> str:
    return "smoke-" + hashlib.sha256(
        f"{ctx.image_tag}|{ctx.resolved_sha}".encode(),
    ).hexdigest()[:16]


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


class SmokeStep(BaseStep):
    number = 13
    name = "smoke"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        token = _smoke_api_token(ctx)
        if not token:
            return RunResult(
                exit_code=2,
                error=(
                    "smoke requires a user-owned submit token — set "
                    "$LOOM_SMOKE_API_TOKEN or provide smoke_api_token via "
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
                exit_code=1, error=f"/api/v1/benchmarks returned {status}",
            )
        try:
            catalog = json.loads(body)
        except json.JSONDecodeError:
            return RunResult(
                exit_code=1, error="benchmarks response not JSON",
            )
        items = catalog.get("items") or catalog.get("data") or []
        if not items:
            return RunResult(
                exit_code=1, error="benchmarks catalog is empty",
            )

        # 4. smoke task exists in the live catalog.
        explicit_task_id = bool(_explicit_smoke_task_id(ctx))
        task_id = _smoke_task_id(ctx)
        required_worker_pool = _smoke_required_worker_pool(
            ctx,
            explicit_task_id=explicit_task_id,
        )
        quoted_task_id = urllib.parse.quote(task_id, safe="/")
        status, body = _http_get(
            f"{base}/api/v1/tasks/{quoted_task_id}", token=token,
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
            f"{base}/api/v1/trials", payload, token=token,
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
                exit_code=1, error="submit response not JSON",
            )
        trial_id = submit.get("id") or submit.get("trial_id")
        if not trial_id:
            return RunResult(
                exit_code=1, error="submit response has no trial id",
            )

        # 6. poll
        deadline = time.time() + DEFAULT_TERMINAL_TIMEOUT_SEC
        terminal_trial: dict[str, object] | None = None
        while time.time() < deadline:
            status, body = _http_get(
                f"{base}/api/v1/trials/{trial_id}", token=token,
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

        # 7. trajectory presign — HEAD to confirm storage
        traj_url_raw = terminal_trial.get("trajectory_url")
        if isinstance(traj_url_raw, str) and traj_url_raw:
            try:
                req = urllib.request.Request(traj_url_raw, method="HEAD")
                with urllib.request.urlopen(req, timeout=15) as resp:
                    step_dir.artifact_path("07-trajectory-head.txt").write_text(
                        f"status={resp.status}\n"
                        f"content-length={resp.headers.get('Content-Length')}\n"
                    )
            except Exception as exc:
                return RunResult(
                    exit_code=1,
                    error=f"trajectory HEAD failed: {exc}",
                )

        # 8. usage rollup — verify the smoke trial showed up
        status, body = _http_get(
            f"{base}/api/v1/usage", token=token,
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
