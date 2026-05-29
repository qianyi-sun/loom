from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol
from uuid import uuid4

import httpx


class ApiSmokeError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApiSmokeConfig:
    base_url: str
    auth_token: str
    run_id: str = field(default_factory=lambda: f"api_smoke_{uuid4().hex}")
    timeout_seconds: float = 120.0
    poll_interval_seconds: float = 5.0
    project_id: str = "pilot-project"
    owner_team: str = "pilot group"
    created_by_user_id: str = "[REDACTED_OWNER]"


@dataclass(frozen=True)
class ApiSmokeResult:
    run_id: str
    status: str
    artifact_count: int
    turn_count: int
    evaluator_status: str
    dashboard_total_runs: int
    dashboard_succeeded_runs: int

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "artifact_count": self.artifact_count,
            "turn_count": self.turn_count,
            "evaluator_status": self.evaluator_status,
            "dashboard_total_runs": self.dashboard_total_runs,
            "dashboard_succeeded_runs": self.dashboard_succeeded_runs,
        }


class ApiSmokeClient(Protocol):
    def post(self, path: str, *, json: dict[str, Any], headers: dict[str, str]) -> Any:
        ...

    def get(self, path: str, *, params: dict[str, Any] | None = None, headers: dict[str, str]) -> Any:
        ...


def run_api_smoke(
    config: ApiSmokeConfig,
    *,
    client: ApiSmokeClient | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ApiSmokeResult:
    _validate_config(config)
    headers = {"Authorization": f"Bearer {config.auth_token}", "X-Request-ID": f"{config.run_id}-smoke"}

    owns_client = client is None
    active_client: ApiSmokeClient = client or httpx.Client(
        base_url=config.base_url.rstrip("/"),
        timeout=httpx.Timeout(10.0),
    )

    try:
        _response_json(
            active_client.post(
                "/runs",
                json=_run_create_payload(config),
                headers=headers,
            ),
            expected_status=201,
            context="create smoke run",
        )

        detail = _wait_for_terminal_run(config, active_client, headers=headers, sleep=sleep)
        _validate_detail_contract(config, detail)

        dashboard = _response_json(
            active_client.get(
                "/dashboard/progress",
                params={"project_id": config.project_id},
                headers=headers,
            ),
            expected_status=200,
            context="read dashboard progress",
        )
        _validate_no_internal_leaks(dashboard)

        dashboard_total_runs = _int_at(dashboard, "summary", "total_runs")
        dashboard_succeeded_runs = _int_at(dashboard, "summary", "runs_by_status", "succeeded")
        if dashboard_total_runs < 1 or dashboard_succeeded_runs < 1:
            raise ApiSmokeError(
                "dashboard progress did not include the completed smoke run: "
                f"total_runs={dashboard_total_runs}, succeeded={dashboard_succeeded_runs}"
            )

        run = _dict_at(detail, "run")
        progress = _dict_at(run, "progress")
        evaluator = _dict_at(run, "evaluator")
        return ApiSmokeResult(
            run_id=str(run.get("run_id") or config.run_id),
            status=str(run.get("status")),
            artifact_count=_int_value(progress.get("artifact_count"), "run.progress.artifact_count"),
            turn_count=_int_value(progress.get("turn_count"), "run.progress.turn_count"),
            evaluator_status=str(evaluator.get("status")),
            dashboard_total_runs=dashboard_total_runs,
            dashboard_succeeded_runs=dashboard_succeeded_runs,
        )
    finally:
        if owns_client and hasattr(active_client, "close"):
            active_client.close()  # type: ignore[attr-defined]


def main() -> int:
    result = run_api_smoke(_config_from_env())
    print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
    return 0


def _config_from_env(environ: Mapping[str, str] | None = None) -> ApiSmokeConfig:
    values = os.environ if environ is None else environ
    return ApiSmokeConfig(
        base_url=_env(values, "API_SMOKE_BASE_URL", "http://127.0.0.1:8000"),
        auth_token=_env(values, "API_SMOKE_AUTH_TOKEN", "[REDACTED_TOKEN]"),
        run_id=_env(values, "API_SMOKE_RUN_ID", f"api_smoke_{uuid4().hex}"),
        timeout_seconds=float(_env(values, "API_SMOKE_TIMEOUT_SECONDS", "120")),
        poll_interval_seconds=float(_env(values, "API_SMOKE_POLL_INTERVAL_SECONDS", "5")),
        project_id=_env(values, "API_SMOKE_PROJECT_ID", "pilot-project"),
        owner_team=_env(values, "API_SMOKE_OWNER_TEAM", "pilot group"),
        created_by_user_id=_env(values, "API_SMOKE_CREATED_BY_USER_ID", "[REDACTED_OWNER]"),
    )


def _wait_for_terminal_run(
    config: ApiSmokeConfig,
    client: ApiSmokeClient,
    *,
    headers: dict[str, str],
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    deadline = time.monotonic() + config.timeout_seconds
    detail: dict[str, Any] | None = None
    last_not_found: str | None = None

    while time.monotonic() <= deadline:
        response = client.get(f"/runs/{config.run_id}", headers=headers)
        if int(getattr(response, "status_code", 0)) == 404:
            last_not_found = getattr(response, "text", "")
            sleep(config.poll_interval_seconds)
            continue
        detail = _response_json(response, expected_status=200, context="read smoke run detail")
        run = _dict_at(detail, "run")
        status = str(run.get("status"))
        if status in _TERMINAL_STATUSES:
            return detail
        sleep(config.poll_interval_seconds)

    last_status = _dict_at(detail or {}, "run").get("status") if detail else "unknown"
    if detail is None and last_not_found is not None:
        last_status = f"not_found: {last_not_found}"
    raise ApiSmokeError(
        f"smoke run {config.run_id} did not reach a terminal state within "
        f"{config.timeout_seconds:g}s; last_status={last_status}"
    )


def _validate_detail_contract(config: ApiSmokeConfig, detail: dict[str, Any]) -> None:
    _validate_no_internal_leaks(detail)
    run = _dict_at(detail, "run")
    if run.get("run_id") != config.run_id:
        raise ApiSmokeError(f"run detail returned unexpected run_id: {run.get('run_id')!r}")

    status = str(run.get("status"))
    if status != "succeeded":
        raise ApiSmokeError(f"smoke run finished with non-success status: {status}")

    progress = _dict_at(run, "progress")
    artifact_count = _int_value(progress.get("artifact_count"), "run.progress.artifact_count")
    turn_count = _int_value(progress.get("turn_count"), "run.progress.turn_count")
    if artifact_count < 3:
        raise ApiSmokeError(f"smoke run expected at least 3 artifacts, got {artifact_count}")
    if turn_count < 1:
        raise ApiSmokeError("smoke run expected at least one terminal trajectory turn")

    artifact_kinds = {str(artifact.get("kind")) for artifact in _list_at(run, "artifacts")}
    expected_artifacts = {"trajectory", "workspace_snapshot", "evaluator_report"}
    missing_artifacts = sorted(expected_artifacts - artifact_kinds)
    if missing_artifacts:
        raise ApiSmokeError(f"smoke run missing artifacts: {', '.join(missing_artifacts)}")

    trajectory = _list_at(detail, "trajectory")
    if not trajectory:
        raise ApiSmokeError("smoke run detail did not include full trajectory")
    first_turn = _dict_value(trajectory[0], "trajectory[0]")
    if _int_value(first_turn.get("exit_code"), "trajectory[0].exit_code") != 0:
        raise ApiSmokeError(f"smoke run command failed: {first_turn.get('stderr')}")
    changed_paths = first_turn.get("changed_paths") or []
    if "smoke-output.txt" not in changed_paths:
        raise ApiSmokeError(f"smoke run did not report final workspace output: {changed_paths}")

    evaluator = _dict_at(run, "evaluator")
    if evaluator.get("status") != "completed":
        raise ApiSmokeError(f"smoke evaluator did not complete: {evaluator.get('status')!r}")
    if not evaluator.get("verbal_feedback_summary"):
        raise ApiSmokeError("smoke evaluator feedback summary is empty")


def _validate_no_internal_leaks(payload: dict[str, Any]) -> None:
    rendered = json.dumps(payload, sort_keys=True)
    leaked_markers = ["file://", "X-Amz-Signature", ".runtime/sandbox-workspaces"]
    for marker in leaked_markers:
        if marker in rendered:
            raise ApiSmokeError(f"API smoke payload leaked internal marker: {marker}")


def _response_json(response: Any, *, expected_status: int, context: str) -> dict[str, Any]:
    status_code = int(getattr(response, "status_code", 0))
    if status_code != expected_status:
        text = getattr(response, "text", "")
        raise ApiSmokeError(f"API smoke failed to {context}: HTTP {status_code}: {text}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise ApiSmokeError(f"API smoke received non-object response while trying to {context}")
    return payload


def _run_create_payload(config: ApiSmokeConfig) -> dict[str, Any]:
    return {
        "run_id": config.run_id,
        "project_id": config.project_id,
        "owner_team": config.owner_team,
        "created_by_user_id": config.created_by_user_id,
        "task": {
            "benchmark_suite": "SkillLearnBench",
            "benchmark_version": "deployment-smoke",
            "task_family": "deployment-readiness",
            "instance_id": "api-created-docker-terminal-smoke",
            "source_uri": "local://deployment-smoke",
            "input_artifact_refs": [],
            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
            "metadata": {
                "instruction": "Create smoke-output.txt in the terminal sandbox workspace.",
            },
        },
        "model": {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
            "metadata": {"smoke": True},
        },
        "runner": {
            "kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "image": "python:3.12-slim",
            "entrypoint": ["python", "-c"],
            "internet_access": True,
            "resource_limits": {
                "cpu": 1,
                "memory_mb": 512,
                "pids_limit": 128,
                "timeout_seconds": 60,
            },
            "metadata": {"runner_contract": "api-created-docker-terminal-smoke-v0"},
        },
        "evaluators": [
            {
                "evaluator_id": "mock-judge-v0",
                "mode": "llm_judge",
                "judge": {
                    "provider": "mock",
                    "model_name": "deterministic-judge",
                    "rubric_version": "deployment-smoke-v0",
                },
            }
        ],
        "metadata": {
            "worker_commands": [
                {
                    "command": (
                        "python - <<'PY'\n"
                        "from pathlib import Path\n"
                        "Path('smoke-output.txt').write_text('api smoke ok\\n')\n"
                        "print('api smoke ok')\n"
                        "PY"
                    ),
                    "cwd": "/workspace",
                    "model_call_id": "call-api-smoke-1",
                }
            ],
        },
    }


def _validate_config(config: ApiSmokeConfig) -> None:
    if not config.base_url.strip():
        raise ApiSmokeError("API_SMOKE_BASE_URL must be non-empty")
    if not config.auth_token.strip():
        raise ApiSmokeError("API_SMOKE_AUTH_TOKEN must be non-empty")
    if config.timeout_seconds <= 0:
        raise ApiSmokeError("API_SMOKE_TIMEOUT_SECONDS must be positive")
    if config.poll_interval_seconds < 0:
        raise ApiSmokeError("API_SMOKE_POLL_INTERVAL_SECONDS cannot be negative")


def _env(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default


def _dict_at(payload: dict[str, Any], key: str) -> dict[str, Any]:
    return _dict_value(payload.get(key), key)


def _dict_value(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ApiSmokeError(f"API smoke expected object at {name}")
    return value


def _list_at(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ApiSmokeError(f"API smoke expected list at {key}")
    return value


def _int_at(payload: dict[str, Any], *path: str) -> int:
    current: Any = payload
    for part in path:
        if not isinstance(current, dict):
            raise ApiSmokeError(f"API smoke expected object at {'.'.join(path)}")
        current = current.get(part)
    return _int_value(current, ".".join(path))


def _int_value(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiSmokeError(f"API smoke expected integer at {name}")
    return value


_TERMINAL_STATUSES = {"succeeded", "failed", "canceled"}


if __name__ == "__main__":
    raise SystemExit(main())
