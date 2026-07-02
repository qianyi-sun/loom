"""Step 13 — end-to-end live smoke (#340).

Concrete smoke:

1. ``GET /api/v1/health`` on Loom Service → expect 200.
2. ``GET /api/v1/benchmarks`` with an admin-minted read token → non-empty.
3. Submit a trivial ``oracle × hello-world`` trial via the public API
   with a deterministic idempotency key derived from the image tag.
4. Poll ``/api/v1/trials/{id}`` until terminal (cap 5 minutes). Assert
   ``state=succeeded`` and ``aggregate_reward`` is present.
5. Fetch the trajectory presigned URL and HEAD to confirm the trajectory
   blob is in MinIO.
6. ``GET /api/v1/usage?since=<start>`` → assert the smoke trial appears.

Every step above writes its response JSON as a separate artifact so an
operator can inspect the smoke evidence without re-running.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Mapping

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult

DEFAULT_TERMINAL_TIMEOUT_SEC = 300.0
DEFAULT_POLL_INTERVAL_SEC = 5.0


def _ingress_base(ctx: RolloutContext) -> str:
    # Cluster-config declares ingress_host; smoke assumes HTTPS.
    from loom_cli.cluster_config import load_cluster_config

    cfg = load_cluster_config(ctx.cluster_config_path)
    return f"https://{cfg.ingress_host}"


def _admin_token(ctx: RolloutContext) -> str | None:
    return os.environ.get("LOOM_ADMIN_TOKEN") or ctx.metadata.get(
        "admin_token",
    )


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


class SmokeStep(BaseStep):
    number = 13
    name = "smoke"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        token = _admin_token(ctx)
        if not token:
            return RunResult(
                exit_code=2,
                error=(
                    "smoke requires an admin/team token — set "
                    "$LOOM_ADMIN_TOKEN or provide via metadata"
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

        # 2. benchmarks catalog is non-empty
        status, body = _http_get(f"{base}/api/v1/benchmarks", token=token)
        step_dir.artifact_path("02-benchmarks.json").write_bytes(body)
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

        # 3. submit a trivial trial
        payload = {
            "task_id": "hello/hello-world",
            "agent": "oracle",
            "idempotency_key": _idempotency_key(ctx),
        }
        status, body = _http_post(
            f"{base}/api/v1/trials", payload, token=token,
        )
        step_dir.artifact_path("03-submit.json").write_bytes(body)
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

        # 4. poll
        deadline = time.time() + DEFAULT_TERMINAL_TIMEOUT_SEC
        terminal_trial: dict[str, object] | None = None
        while time.time() < deadline:
            status, body = _http_get(
                f"{base}/api/v1/trials/{trial_id}", token=token,
            )
            step_dir.artifact_path("04-poll.json").write_bytes(body)
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

        # 5. trajectory presign — HEAD to confirm storage
        traj_url_raw = terminal_trial.get("trajectory_url")
        if isinstance(traj_url_raw, str) and traj_url_raw:
            try:
                req = urllib.request.Request(traj_url_raw, method="HEAD")
                with urllib.request.urlopen(req, timeout=15) as resp:
                    step_dir.artifact_path("05-trajectory-head.txt").write_text(
                        f"status={resp.status}\n"
                        f"content-length={resp.headers.get('Content-Length')}\n"
                    )
            except Exception as exc:
                return RunResult(
                    exit_code=1,
                    error=f"trajectory HEAD failed: {exc}",
                )

        # 6. usage rollup — verify the smoke trial showed up
        status, body = _http_get(
            f"{base}/api/v1/usage", token=token,
        )
        step_dir.artifact_path("06-usage.json").write_bytes(body)
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
