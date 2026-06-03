from __future__ import annotations

import io
import json
import os
import time
import zipfile
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol
from uuid import uuid4

import httpx


class FrontendSmokeError(RuntimeError):
    pass


@dataclass(frozen=True)
class FrontendSmokeConfig:
    base_url: str
    username: str
    password: str
    run_id: str = field(default_factory=lambda: f"frontend_smoke_{uuid4().hex}")
    timeout_seconds: float = 120.0
    poll_interval_seconds: float = 5.0
    project_id: str = ""
    preferred_harness_id: str = "harbor-local-docker"


@dataclass(frozen=True)
class FrontendSmokeResult:
    run_id: str
    project_id: str
    model_id: str
    harness_id: str
    status: str
    telemetry_status: str
    artifact_count: int
    bundle_file_count: int
    lifecycle_event_count: int
    sse_event_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "project_id": self.project_id,
            "model_id": self.model_id,
            "harness_id": self.harness_id,
            "status": self.status,
            "telemetry_status": self.telemetry_status,
            "artifact_count": self.artifact_count,
            "bundle_file_count": self.bundle_file_count,
            "lifecycle_event_count": self.lifecycle_event_count,
            "sse_event_count": self.sse_event_count,
        }


class FrontendSmokeClient(Protocol):
    def post(self, path: str, *, json: dict[str, Any], headers: dict[str, str]) -> Any:
        ...

    def get(self, path: str, *, params: dict[str, Any] | None = None, headers: dict[str, str]) -> Any:
        ...


def run_frontend_smoke(
    config: FrontendSmokeConfig,
    *,
    client: FrontendSmokeClient | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> FrontendSmokeResult:
    _validate_config(config)
    headers = {"X-Request-ID": f"{config.run_id}-frontend-smoke"}
    owns_client = client is None
    active_client: FrontendSmokeClient = client or httpx.Client(
        base_url=config.base_url.rstrip("/"),
        timeout=httpx.Timeout(10.0),
        follow_redirects=True,
    )
    try:
        _response_json(
            active_client.post(
                "/auth/login",
                json={"username": config.username, "password": config.password},
                headers=headers,
            ),
            expected_status=200,
            context="log in through frontend auth boundary",
        )
        _response_json(active_client.get("/auth/session", headers=headers), expected_status=200, context="read session")

        project = _select_project(
            _response_json(active_client.get("/projects", headers=headers), expected_status=200, context="list projects"),
            preferred_project_id=config.project_id,
        )
        model = _select_model(
            _response_json(active_client.get("/models", headers=headers), expected_status=200, context="list models")
        )
        harness = _select_harness(
            _response_json(active_client.get("/harnesses", headers=headers), expected_status=200, context="list harnesses"),
            preferred_harness_id=config.preferred_harness_id,
        )
        benchmark = _select_benchmark(
            _response_json(
                active_client.get("/benchmarks", headers=headers),
                expected_status=200,
                context="list benchmarks",
            )
        )
        tasks = _response_json(
            active_client.get(
                "/tasks",
                params={
                    "benchmark_suite": benchmark["suite_name"],
                    "benchmark_version": benchmark["benchmark_version"],
                },
                headers=headers,
            ),
            expected_status=200,
            context="list benchmark tasks",
        )
        task = _select_task(tasks)

        _response_json(
            active_client.post(
                "/runs",
                json=_run_create_payload(config, project=project, model=model, harness=harness, benchmark=benchmark, task=task),
                headers=headers,
            ),
            expected_status=201,
            context="launch frontend smoke run",
        )
        detail = _wait_for_terminal_run(config, active_client, headers=headers, sleep=sleep)
        lifecycle_events = _validate_event_replay(
            config,
            _response_json(
                active_client.get(
                    f"/runs/{config.run_id}/events",
                    params={"after_seq": 0, "limit": 100},
                    headers=headers,
                ),
                expected_status=200,
                context="read run lifecycle event replay",
            ),
        )
        sse_events = _validate_sse_replay(
            config,
            _response_bytes(
                active_client.get(
                    f"/runs/{config.run_id}/stream",
                    params={"after_seq": 0, "limit": 100, "once": True},
                    headers=headers,
                ),
                expected_status=200,
                context="read run lifecycle SSE replay",
            ),
            expected_events=lifecycle_events,
        )
        telemetry = _response_json(
            active_client.get(f"/runs/{config.run_id}/telemetry", headers=headers),
            expected_status=200,
            context="read run telemetry",
        )
        bundle_file_count = _validate_bundle(
            _response_bytes(
                active_client.get(f"/runs/{config.run_id}/artifact-bundle", headers=headers),
                expected_status=200,
                context="download artifact bundle",
            ),
            expected_events=lifecycle_events,
        )
        _response_json(
            active_client.get(
                "/dashboard/progress",
                params={"project_id": project["project_id"]},
                headers=headers,
            ),
            expected_status=200,
            context="read dashboard progress",
        )
        run = _dict_at(detail, "run")
        progress = _dict_at(run, "progress")
        return FrontendSmokeResult(
            run_id=str(run.get("run_id") or config.run_id),
            project_id=str(project["project_id"]),
            model_id=str(model["model_id"]),
            harness_id=str(harness["harness_id"]),
            status=str(run.get("status")),
            telemetry_status=str(_dict_at(telemetry, "run").get("status")),
            artifact_count=_int_value(progress.get("artifact_count"), "run.progress.artifact_count"),
            bundle_file_count=bundle_file_count,
            lifecycle_event_count=len(lifecycle_events),
            sse_event_count=len(sse_events),
        )
    finally:
        if owns_client and hasattr(active_client, "close"):
            active_client.close()  # type: ignore[attr-defined]


def main() -> int:
    result = run_frontend_smoke(_config_from_env())
    print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
    return 0


def _config_from_env(environ: Mapping[str, str] | None = None) -> FrontendSmokeConfig:
    values = os.environ if environ is None else environ
    return FrontendSmokeConfig(
        base_url=_env(values, "FRONTEND_SMOKE_BASE_URL", "http://127.0.0.1:8000"),
        username=_env(values, "FRONTEND_SMOKE_USERNAME", "[REDACTED_OWNER]"),
        password=_env(values, "FRONTEND_SMOKE_PASSWORD", "[REDACTED_PASSWORD]"),
        run_id=_env(values, "FRONTEND_SMOKE_RUN_ID", f"frontend_smoke_{uuid4().hex}"),
        timeout_seconds=float(_env(values, "FRONTEND_SMOKE_TIMEOUT_SECONDS", "120")),
        poll_interval_seconds=float(_env(values, "FRONTEND_SMOKE_POLL_INTERVAL_SECONDS", "5")),
        project_id=_env(values, "FRONTEND_SMOKE_PROJECT_ID", ""),
        preferred_harness_id=_env(values, "FRONTEND_SMOKE_HARNESS_ID", "harbor-local-docker"),
    )


def _wait_for_terminal_run(
    config: FrontendSmokeConfig,
    client: FrontendSmokeClient,
    *,
    headers: dict[str, str],
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    deadline = time.monotonic() + config.timeout_seconds
    detail: dict[str, Any] | None = None
    while time.monotonic() <= deadline:
        response = client.get(f"/runs/{config.run_id}", headers=headers)
        if int(getattr(response, "status_code", 0)) == 404:
            sleep(config.poll_interval_seconds)
            continue
        detail = _response_json(response, expected_status=200, context="read launched run detail")
        status = str(_dict_at(detail, "run").get("status"))
        if status in _TERMINAL_STATUSES:
            if status != "succeeded":
                raise FrontendSmokeError(f"frontend smoke run finished with non-success status: {status}")
            _validate_detail(detail)
            return detail
        sleep(config.poll_interval_seconds)
    last_status = _dict_at(detail or {}, "run").get("status") if detail else "unknown"
    raise FrontendSmokeError(
        f"frontend smoke run {config.run_id} did not reach a terminal state within "
        f"{config.timeout_seconds:g}s; last_status={last_status}"
    )


def _validate_detail(detail: dict[str, Any]) -> None:
    run = _dict_at(detail, "run")
    progress = _dict_at(run, "progress")
    artifact_count = _int_value(progress.get("artifact_count"), "run.progress.artifact_count")
    turn_count = _int_value(progress.get("turn_count"), "run.progress.turn_count")
    if artifact_count < 3:
        raise FrontendSmokeError(f"frontend smoke expected at least 3 artifacts, got {artifact_count}")
    evaluator = run.get("evaluator")
    if not isinstance(evaluator, dict) or evaluator.get("status") != "completed":
        raise FrontendSmokeError("frontend smoke evaluator output was not completed")
    if turn_count < 1 and evaluator.get("mode") != "harbor_verifier":
        raise FrontendSmokeError("frontend smoke expected at least one terminal trajectory turn")


def _validate_event_replay(config: FrontendSmokeConfig, payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("run_id") != config.run_id:
        raise FrontendSmokeError(f"frontend smoke event replay returned unexpected run_id: {payload.get('run_id')!r}")
    events = [_dict_value(event, "events[]") for event in _list_at(payload, "events")]
    return _validate_lifecycle_events(config, events, context="event replay")


def _validate_sse_replay(
    config: FrontendSmokeConfig,
    payload: bytes,
    *,
    expected_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events = _validate_lifecycle_events(config, _parse_sse_events(payload), context="SSE replay")
    if _event_pairs(events) != _event_pairs(expected_events):
        raise FrontendSmokeError("frontend smoke SSE replay did not match durable event replay")
    return events


def _parse_sse_events(payload: bytes) -> list[dict[str, Any]]:
    try:
        text = payload.decode("utf-8").replace("\r\n", "\n")
    except UnicodeDecodeError as exc:
        raise FrontendSmokeError("frontend smoke SSE replay was not valid UTF-8") from exc
    events: list[dict[str, Any]] = []
    for frame in text.split("\n\n"):
        event_id: str | None = None
        event_type: str | None = None
        data_lines: list[str] = []
        for line in frame.splitlines():
            if not line or line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if not separator:
                continue
            if value.startswith(" "):
                value = value[1:]
            if field == "id":
                event_id = value
            elif field == "event":
                event_type = value
            elif field == "data":
                data_lines.append(value)
        if not data_lines:
            continue
        try:
            event_payload = json.loads("\n".join(data_lines))
        except json.JSONDecodeError as exc:
            raise FrontendSmokeError("frontend smoke SSE replay contained invalid JSON data") from exc
        event = _dict_value(event_payload, "SSE data")
        if event_id is not None and str(event.get("seq")) != event_id:
            raise FrontendSmokeError("frontend smoke SSE replay id did not match event seq")
        if event_type is not None and str(event.get("event_type")) != event_type:
            raise FrontendSmokeError("frontend smoke SSE replay event name did not match event payload")
        events.append(event)
    if not events:
        raise FrontendSmokeError("frontend smoke SSE replay returned no events")
    return events


def _validate_lifecycle_events(
    config: FrontendSmokeConfig,
    events: list[dict[str, Any]],
    *,
    context: str,
) -> list[dict[str, Any]]:
    event_types = [str(event.get("event_type")) for event in events]
    missing = [event_type for event_type in _REQUIRED_LIFECYCLE_EVENT_TYPES if event_type not in event_types]
    if missing:
        raise FrontendSmokeError(f"frontend smoke {context} missing required lifecycle events: {', '.join(missing)}")

    previous_index = -1
    previous_seq = -1
    for event_type in _REQUIRED_LIFECYCLE_EVENT_TYPES:
        index = event_types.index(event_type)
        if index <= previous_index:
            raise FrontendSmokeError(f"frontend smoke {context} lifecycle events were not ordered")
        previous_index = index

    for event in events:
        if event.get("run_id") != config.run_id:
            raise FrontendSmokeError(f"frontend smoke {context} included unrelated run_id: {event.get('run_id')!r}")
        seq = _int_value(event.get("seq"), "events[].seq")
        if seq <= previous_seq:
            raise FrontendSmokeError(f"frontend smoke {context} seq values were not strictly increasing")
        previous_seq = seq

    expected_execution_task_id: str | None = None
    for event_type in _REQUIRED_LIFECYCLE_EVENT_TYPES[1:]:
        event = events[event_types.index(event_type)]
        metadata = _dict_value(event.get("metadata") or {}, "events[].metadata")
        execution_task_id = _required_metadata_str(metadata, "execution_task_id", context=context, event_type=event_type)
        if expected_execution_task_id is None:
            expected_execution_task_id = execution_task_id
        elif execution_task_id != expected_execution_task_id:
            raise FrontendSmokeError(f"frontend smoke {context} changed execution_task_id before terminal state")
        if event_type == "run.dispatched":
            _required_metadata_str(metadata, "scheduler_id", context=context, event_type=event_type)
            _required_metadata_str(metadata, "backend_key", context=context, event_type=event_type)
            _required_metadata_str(metadata, "project_id", context=context, event_type=event_type)
        if event_type in _WORKER_LIFECYCLE_EVENT_TYPES:
            _required_metadata_str(metadata, "worker_id", context=context, event_type=event_type)
    return events


def _required_metadata_str(metadata: dict[str, Any], key: str, *, context: str, event_type: str) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FrontendSmokeError(f"frontend smoke {context} missing {key} on {event_type}")
    return value.strip()


def _event_pairs(events: list[dict[str, Any]]) -> list[tuple[int, str]]:
    return [(_int_value(event.get("seq"), "events[].seq"), str(event.get("event_type"))) for event in events]


def _run_create_payload(
    config: FrontendSmokeConfig,
    *,
    project: dict[str, Any],
    model: dict[str, Any],
    harness: dict[str, Any],
    benchmark: dict[str, Any],
    task: dict[str, Any],
) -> dict[str, Any]:
    instruction = (
        _dict_value(task.get("metadata") or {}, "task.metadata").get("instruction")
        or f"Follow {benchmark['suite_name']} task {task['task_family']}/{task['instance_id']} from {task['instruction_ref']}."
    )
    harness_metadata = _dict_value(harness.get("metadata") or {}, "harness.metadata")
    launch_metadata: dict[str, Any] = {"launched_from": "frontend-smoke", "harness_id": harness["harness_id"]}
    if harness_metadata.get("harbor_compatible"):
        launch_metadata["harbor_run"] = _harbor_run_config_for_harness(harness)
        evaluators = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
    else:
        command = (
            "python - <<'PY'\n"
            "from pathlib import Path\n"
            f"Path('frontend-smoke-output.txt').write_text('frontend smoke {config.run_id}\\n')\n"
            "print('frontend smoke complete')\n"
            "PY"
        )
        launch_metadata["worker_commands"] = [
            {"command": command, "cwd": "/workspace", "model_call_id": "frontend-smoke-call-1"}
        ]
        evaluators = [
            {
                "evaluator_id": "mock-judge-v0",
                "mode": "llm_judge",
                "judge": {
                    "provider": "mock",
                    "model_name": "deterministic-judge",
                    "rubric_version": "frontend-e2e-v0",
                },
            }
        ]
    return {
        "run_id": config.run_id,
        "project_id": project["project_id"],
        "owner_team": project.get("owner_team_id") or project.get("name") or project["project_id"],
        "task": {
            "benchmark_suite": benchmark["suite_name"],
            "benchmark_version": benchmark["benchmark_version"],
            "task_family": task["task_family"],
            "instance_id": task["instance_id"],
            "source_uri": benchmark["source_uri"],
            "input_artifact_refs": list(task.get("input_artifact_refs") or []),
            "required_artifacts": list(task.get("required_artifacts") or ["trajectory", "workspace_snapshot", "evaluator_report"]),
            "metadata": {**dict(task.get("metadata") or {}), "instruction": instruction},
        },
        "model": {
            "provider": model["provider"],
            "model_name": model["model_id"],
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
            "provider_config_id": model.get("provider_config_id"),
            "metadata": {},
        },
        "runner": {
            "kind": harness["runner_kind"],
            "sandbox_backend": harness["sandbox_backend"],
            "image": task.get("runner_image") or harness["default_image"],
            "entrypoint": list(task.get("runner_entrypoint") or ["python", "-c"]),
            "internet_access": bool(harness["internet_access"]),
            "resource_limits": dict(harness["resource_limits"]),
            "metadata": {
                **dict(harness.get("metadata") or {}),
                "runner_contract": task.get("runner_contract")
                or _dict_value(harness.get("metadata") or {}, "harness.metadata").get("runner_contract"),
            },
        },
        "evaluators": evaluators,
        "metadata": launch_metadata,
    }


def _harbor_run_config_for_harness(harness: dict[str, Any]) -> dict[str, Any]:
    metadata = _dict_value(harness.get("metadata") or {}, "harness.metadata")
    resource_limits = _dict_value(harness.get("resource_limits") or {}, "harness.resource_limits")
    harbor_config: dict[str, Any] = {
        "task_template": _str_metadata(metadata.get("harbor_task_template"), "harbor-cli-smoke"),
        "agent": _str_metadata(metadata.get("harbor_agent"), "oracle"),
        "environment": _str_metadata(metadata.get("harbor_environment"), "docker"),
        "timeout_seconds": _int_metadata(
            metadata.get("harbor_timeout_seconds") or resource_limits.get("timeout_seconds"),
            default=600,
            field_name="harness.metadata.harbor_timeout_seconds",
        ),
        "extra_args": _str_list_metadata(
            metadata.get("harbor_extra_args"),
            default=["--n-tasks", "1", "--quiet"],
            field_name="harness.metadata.harbor_extra_args",
        ),
    }
    model_name = metadata.get("harbor_model_name")
    if isinstance(model_name, str) and model_name.strip():
        harbor_config["model_name"] = model_name.strip()
    return harbor_config


def _select_project(payload: dict[str, Any], *, preferred_project_id: str) -> dict[str, Any]:
    projects = _list_at(payload, "projects")
    for project in projects:
        item = _dict_value(project, "projects[]")
        if preferred_project_id and item.get("project_id") == preferred_project_id:
            return item
    if projects:
        return _dict_value(projects[0], "projects[0]")
    raise FrontendSmokeError("frontend smoke could not find an accessible project")


def _select_model(payload: dict[str, Any]) -> dict[str, Any]:
    for model in _list_at(payload, "models"):
        item = _dict_value(model, "models[]")
        if not item.get("disabled"):
            return item
    raise FrontendSmokeError("frontend smoke could not find an enabled API model")


def _select_harness(payload: dict[str, Any], *, preferred_harness_id: str) -> dict[str, Any]:
    harnesses = [_dict_value(item, "harnesses[]") for item in _list_at(payload, "harnesses")]
    for harness in harnesses:
        if harness.get("harness_id") == preferred_harness_id:
            return harness
    for harness in harnesses:
        if _dict_value(harness.get("metadata") or {}, "harness.metadata").get("harbor_compatible"):
            return harness
    if harnesses:
        return harnesses[0]
    raise FrontendSmokeError("frontend smoke could not find a launch harness")


def _select_benchmark(payload: dict[str, Any]) -> dict[str, Any]:
    benchmarks = _list_at(payload, "benchmarks")
    if not benchmarks:
        raise FrontendSmokeError("frontend smoke could not find benchmark catalog entries")
    return _dict_value(benchmarks[0], "benchmarks[0]")


def _select_task(payload: dict[str, Any]) -> dict[str, Any]:
    tasks = _list_at(payload, "tasks")
    if not tasks:
        raise FrontendSmokeError("frontend smoke could not find benchmark task entries")
    return _dict_value(tasks[0], "tasks[0]")


def _validate_bundle(payload: bytes, *, expected_events: list[dict[str, Any]]) -> int:
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = set(archive.namelist())
            lifecycle_payload = json.loads(archive.read("lifecycle-events.json").decode("utf-8"))
    except zipfile.BadZipFile as exc:
        raise FrontendSmokeError("artifact bundle download was not a zip archive") from exc
    except KeyError as exc:
        raise FrontendSmokeError("artifact bundle missing files: lifecycle-events.json") from exc
    except json.JSONDecodeError as exc:
        raise FrontendSmokeError("artifact bundle lifecycle-events.json was not valid JSON") from exc
    required = {
        "manifest.json",
        "run.json",
        "trajectory.jsonl",
        "evaluation.json",
        "artifact-metadata.json",
        "lifecycle-events.json",
    }
    missing = sorted(required - names)
    if missing:
        raise FrontendSmokeError(f"artifact bundle missing files: {', '.join(missing)}")
    lifecycle_events = [
        _dict_value(event, "lifecycle_events[]")
        for event in _list_at(_dict_value(lifecycle_payload, "lifecycle-events.json"), "lifecycle_events")
    ]
    if _event_pairs(lifecycle_events) != _event_pairs(expected_events):
        raise FrontendSmokeError("artifact bundle lifecycle events did not match durable event replay")
    if not any(name.startswith("artifacts/") and not name.endswith("/") for name in names):
        raise FrontendSmokeError("artifact bundle did not include any artifact payload files")
    return len(names)


def _response_json(response: Any, *, expected_status: int, context: str) -> dict[str, Any]:
    status_code = int(getattr(response, "status_code", 0))
    if status_code != expected_status:
        raise FrontendSmokeError(f"frontend smoke failed to {context}: HTTP {status_code}: {getattr(response, 'text', '')}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise FrontendSmokeError(f"frontend smoke received non-object response while trying to {context}")
    return payload


def _response_bytes(response: Any, *, expected_status: int, context: str) -> bytes:
    status_code = int(getattr(response, "status_code", 0))
    if status_code != expected_status:
        raise FrontendSmokeError(f"frontend smoke failed to {context}: HTTP {status_code}: {getattr(response, 'text', '')}")
    payload = getattr(response, "content", b"")
    if not isinstance(payload, bytes) or not payload:
        raise FrontendSmokeError(f"frontend smoke received empty bytes while trying to {context}")
    return payload


def _validate_config(config: FrontendSmokeConfig) -> None:
    if not config.base_url.strip():
        raise FrontendSmokeError("FRONTEND_SMOKE_BASE_URL must be non-empty")
    if not config.username.strip():
        raise FrontendSmokeError("FRONTEND_SMOKE_USERNAME must be non-empty")
    if not config.password:
        raise FrontendSmokeError("FRONTEND_SMOKE_PASSWORD must be non-empty")
    if config.timeout_seconds <= 0:
        raise FrontendSmokeError("FRONTEND_SMOKE_TIMEOUT_SECONDS must be positive")
    if config.poll_interval_seconds < 0:
        raise FrontendSmokeError("FRONTEND_SMOKE_POLL_INTERVAL_SECONDS cannot be negative")


def _dict_at(payload: dict[str, Any], key: str) -> dict[str, Any]:
    return _dict_value(payload.get(key), key)


def _dict_value(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FrontendSmokeError(f"frontend smoke expected object at {name}")
    return value


def _list_at(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise FrontendSmokeError(f"frontend smoke expected list at {key}")
    return value


def _int_value(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FrontendSmokeError(f"frontend smoke expected integer at {name}")
    return value


def _int_metadata(value: object, *, default: int, field_name: str) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FrontendSmokeError(f"frontend smoke expected positive integer at {field_name}")
    return value


def _str_metadata(value: object, default: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else default


def _str_list_metadata(value: object, *, default: list[str], field_name: str) -> list[str]:
    if value is None:
        return list(default)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise FrontendSmokeError(f"frontend smoke expected string list at {field_name}")
    return [item.strip() for item in value]


def _env(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default


_TERMINAL_STATUSES = {"succeeded", "failed", "canceled"}
_REQUIRED_LIFECYCLE_EVENT_TYPES = (
    "run.created",
    "run.dispatched",
    "run.claimed",
    "run.started",
    "run.evaluating",
    "evaluator.started",
    "evaluator.completed",
    "run.succeeded",
)
_WORKER_LIFECYCLE_EVENT_TYPES = {
    "run.claimed",
    "run.started",
    "run.evaluating",
    "evaluator.started",
    "evaluator.completed",
    "run.succeeded",
}


if __name__ == "__main__":
    raise SystemExit(main())
