from __future__ import annotations

import io
import json
import os
import re
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from uuid import uuid4

import httpx


class HarborAdapterMatrixSmokeError(RuntimeError):
    pass


@dataclass(frozen=True)
class HarborAdapterMatrixSmokeConfig:
    base_url: str
    username: str
    password: str
    project_id: str = ""
    harness_id: str = "harbor-local-docker"
    agent_ids: list[str] = field(default_factory=lambda: list(_MAINSTREAM_AGENT_IDS))
    model_ids: list[str] = field(default_factory=list)
    model_families: list[str] = field(default_factory=lambda: list(_MAINSTREAM_MODEL_FAMILIES))
    run_prefix: str = "adapter_matrix"
    timeout_seconds: float = 7200.0
    per_run_timeout_seconds: int = 900
    agent_timeout_multiplier: float = 1.0
    poll_interval_seconds: float = 5.0
    report_path: str = ""


class MatrixSmokeClient(Protocol):
    def post(self, path: str, *, json: dict[str, Any], headers: dict[str, str]) -> Any:
        ...

    def get(self, path: str, *, params: dict[str, Any] | None = None, headers: dict[str, str]) -> Any:
        ...


def run_harbor_adapter_matrix_smoke(
    config: HarborAdapterMatrixSmokeConfig,
    *,
    client: MatrixSmokeClient | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    _validate_config(config)
    headers = {"X-Request-ID": f"{config.run_prefix}-{uuid4().hex[:12]}"}
    owns_client = client is None
    active_client: MatrixSmokeClient = client or httpx.Client(
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
            context="log in",
        )
        project = _select_project(
            _response_json(active_client.get("/projects", headers=headers), expected_status=200, context="list projects"),
            preferred_project_id=config.project_id,
        )
        models = _select_models(
            _response_json(active_client.get("/models", headers=headers), expected_status=200, context="list models"),
            model_ids=config.model_ids,
            model_families=config.model_families,
        )
        harness = _select_harness(
            _response_json(active_client.get("/harnesses", headers=headers), expected_status=200, context="list harnesses"),
            preferred_harness_id=config.harness_id,
        )
        benchmark = _select_benchmark(
            _response_json(active_client.get("/benchmarks", headers=headers), expected_status=200, context="list benchmarks")
        )
        task = _select_task(
            _response_json(
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
        )

        started_at = _now()
        blocked_results: list[dict[str, Any]] = []
        pending_runs: list[dict[str, Any]] = []
        total = len(config.agent_ids) * len(models)
        index = 0
        for agent_id in config.agent_ids:
            for model in models:
                index += 1
                launched = _launch_one_combo(
                    config,
                    active_client,
                    headers=headers,
                    project=project,
                    harness=harness,
                    benchmark=benchmark,
                    task=task,
                    agent_id=agent_id,
                    model=model,
                    index=index,
                    total=total,
                )
                if launched.get("run_id"):
                    pending_runs.append(launched)
                else:
                    blocked_results.append(launched)
        results = [
            *blocked_results,
            *_wait_for_launched_runs(
                config,
                active_client,
                pending_runs=pending_runs,
                headers=headers,
                sleep=sleep,
            ),
        ]

        summary = _summary_payload(
            config=config,
            project=project,
            harness=harness,
            models=models,
            results=results,
            started_at=started_at,
        )
        if config.report_path:
            _write_report(Path(config.report_path), summary)
        return summary
    finally:
        if owns_client and hasattr(active_client, "close"):
            active_client.close()  # type: ignore[attr-defined]


def main() -> int:
    result = run_harbor_adapter_matrix_smoke(_config_from_env())
    print(json.dumps(result, sort_keys=True), flush=True)
    failed = [item for item in result["results"] if item["status"] != "succeeded"]
    return 1 if failed else 0


def _launch_one_combo(
    config: HarborAdapterMatrixSmokeConfig,
    client: MatrixSmokeClient,
    *,
    headers: dict[str, str],
    project: dict[str, Any],
    harness: dict[str, Any],
    benchmark: dict[str, Any],
    task: dict[str, Any],
    agent_id: str,
    model: dict[str, Any],
    index: int,
    total: int,
) -> dict[str, Any]:
    model_id = str(model["model_id"])
    provider_config_id = model.get("provider_config_id")
    preflight = _response_json(
        client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": project["project_id"],
                "harness_id": harness["harness_id"],
                "agent_id": agent_id,
                "model_id": model_id,
                "provider_config_id": provider_config_id,
            },
            headers=headers,
        ),
        expected_status=200,
        context=f"preflight {agent_id} + {model_id}",
    )
    if preflight.get("status") != "ready":
        return {
            "agent_id": agent_id,
            "model_id": model_id,
            "family": _model_family(model),
            "status": "blocked",
            "run_id": None,
            "adapter_id": _adapter_id(preflight),
            "message": _preflight_message(preflight),
            "preflight": _trim_preflight(preflight),
        }

    run_id = _run_id(config.run_prefix, index=index, agent_id=agent_id, model_id=model_id)
    _response_json(
        client.post(
            "/runs",
            json=_run_create_payload(
                config,
                project=project,
                harness=harness,
                benchmark=benchmark,
                task=task,
                agent_id=agent_id,
                model=model,
                run_id=run_id,
            ),
            headers=headers,
        ),
        expected_status=201,
        context=f"launch {index}/{total} {agent_id} + {model_id}",
    )
    return {
        "agent_id": agent_id,
        "model_id": model_id,
        "family": _model_family(model),
        "run_id": run_id,
        "adapter_id": _adapter_id(preflight),
        "launched_at": time.monotonic(),
    }


def _wait_for_launched_runs(
    config: HarborAdapterMatrixSmokeConfig,
    client: MatrixSmokeClient,
    *,
    pending_runs: list[dict[str, Any]],
    headers: dict[str, str],
    sleep: Callable[[float], None],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    pending = list(pending_runs)
    deadline = time.monotonic() + config.timeout_seconds
    while pending and time.monotonic() <= deadline:
        still_pending: list[dict[str, Any]] = []
        for pending_run in pending:
            now = time.monotonic()
            run_id = str(pending_run["run_id"])
            response = client.get(f"/runs/{run_id}", headers=headers)
            if int(getattr(response, "status_code", 0)) == 404:
                still_pending.append(pending_run)
                continue
            detail = _response_json(response, expected_status=200, context=f"read run {run_id}")
            status = str(_dict_value(detail.get("run"), "run").get("status"))
            if status in _TERMINAL_STATUSES:
                results.append(_result_from_detail(config, client, headers=headers, pending_run=pending_run, detail=detail))
            elif _active_wait_seconds(pending_run, status=status, now=now) > config.per_run_timeout_seconds + 60:
                results.append(
                    {
                        **_result_identity(pending_run),
                        "status": "timeout",
                        "failure_reason": "run did not finish before per-run matrix timeout",
                    }
                )
            else:
                still_pending.append(pending_run)
        pending = still_pending
        if pending:
            sleep(config.poll_interval_seconds)
    for pending_run in pending:
        results.append(
            {
                **_result_identity(pending_run),
                "status": "timeout",
                "failure_reason": "matrix timeout reached before terminal status",
            }
        )
    return results


def _active_wait_seconds(pending_run: dict[str, Any], *, status: str, now: float) -> float:
    if status == "queued":
        return 0.0
    active_since = pending_run.get("active_since")
    if not isinstance(active_since, (int, float)):
        pending_run["active_since"] = now
        return 0.0
    return now - float(active_since)


def _result_from_detail(
    config: HarborAdapterMatrixSmokeConfig,
    client: MatrixSmokeClient,
    *,
    headers: dict[str, str],
    pending_run: dict[str, Any],
    detail: dict[str, Any],
) -> dict[str, Any]:
    run_id = str(pending_run["run_id"])
    run = _dict_value(detail.get("run"), "run")
    status = str(run.get("status"))
    artifact_count = int(_dict_value(run.get("progress"), "run.progress").get("artifact_count") or 0)
    bundle_file_count = 0
    failure_reason = run.get("failure_reason")
    if status == "succeeded":
        bundle_file_count = _validate_bundle(
            _response_bytes(
                client.get(f"/runs/{run_id}/artifact-bundle", headers=headers),
                expected_status=200,
                context=f"download artifact bundle for {run_id}",
            )
        )
    return {
        **_result_identity(pending_run),
        "status": status,
        "artifact_count": artifact_count,
        "bundle_file_count": bundle_file_count,
        "failure_reason": failure_reason,
        "verifier_score": _verifier_score(run),
    }


def _result_identity(pending_run: dict[str, Any]) -> dict[str, Any]:
    return {
        "agent_id": pending_run["agent_id"],
        "model_id": pending_run["model_id"],
        "family": pending_run["family"],
        "run_id": pending_run.get("run_id"),
        "adapter_id": pending_run.get("adapter_id"),
    }


def _run_create_payload(
    config: HarborAdapterMatrixSmokeConfig,
    *,
    project: dict[str, Any],
    harness: dict[str, Any],
    benchmark: dict[str, Any],
    task: dict[str, Any],
    agent_id: str,
    model: dict[str, Any],
    run_id: str,
) -> dict[str, Any]:
    agent_name = agent_id.removeprefix("harbor:")
    task_metadata = dict(task.get("metadata") or {})
    task_metadata["instruction"] = "Create `/app/smoke-output.txt` containing the text `harbor-smoke-ok`."
    harness_metadata = dict(harness.get("metadata") or {})
    return {
        "run_id": run_id,
        "project_id": project["project_id"],
        "owner_team": project.get("owner_team_id") or project.get("name") or project["project_id"],
        "task": {
            "benchmark_suite": benchmark["suite_name"],
            "benchmark_version": benchmark["benchmark_version"],
            "task_family": task.get("task_family") or "harbor-cli-smoke",
            "instance_id": task.get("instance_id") or "adapter-matrix-smoke",
            "source_uri": benchmark.get("source_uri") or "harbor://local/smoke",
            "input_artifact_refs": list(task.get("input_artifact_refs") or []),
            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
            "metadata": task_metadata,
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
                **harness_metadata,
                "runner_contract": task.get("runner_contract")
                or _dict_value(harness.get("metadata") or {}, "harness.metadata").get("runner_contract"),
            },
        },
        "evaluators": [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}],
        "metadata": {
            "launched_from": "harbor-adapter-matrix-smoke",
            "harbor_run": {
                "task_template": "harbor-cli-smoke",
                "agent": agent_name,
                "model_name": model["model_id"],
                "environment": "docker",
                "timeout_seconds": config.per_run_timeout_seconds,
                "extra_args": [
                    "--n-tasks",
                    "1",
                    "--quiet",
                    "--agent-timeout-multiplier",
                    f"{config.agent_timeout_multiplier:g}",
                ],
            },
        },
    }

def _select_models(payload: dict[str, Any], *, model_ids: list[str], model_families: list[str]) -> list[dict[str, Any]]:
    models = [_dict_value(item, "models[]") for item in _list_at(payload, "models") if not item.get("disabled")]
    if not models:
        raise HarborAdapterMatrixSmokeError("model catalog did not include enabled models")
    by_id = {str(model["model_id"]): model for model in models}
    if model_ids:
        missing = [model_id for model_id in model_ids if model_id not in by_id]
        if missing:
            raise HarborAdapterMatrixSmokeError(f"requested matrix models not found: {', '.join(missing)}")
        return [by_id[model_id] for model_id in model_ids]

    selected: list[dict[str, Any]] = []
    for family in model_families:
        for model in models:
            if _model_family(model) == family and model not in selected:
                selected.append(model)
                break
    missing_families = [family for family in model_families if family not in {_model_family(model) for model in selected}]
    if missing_families:
        raise HarborAdapterMatrixSmokeError(f"model catalog is missing mainstream families: {', '.join(missing_families)}")
    return selected


def _select_project(payload: dict[str, Any], *, preferred_project_id: str) -> dict[str, Any]:
    projects = [_dict_value(item, "projects[]") for item in _list_at(payload, "projects")]
    for project in projects:
        if preferred_project_id and project.get("project_id") == preferred_project_id:
            return project
    if projects:
        return projects[0]
    raise HarborAdapterMatrixSmokeError("no accessible projects")


def _select_harness(payload: dict[str, Any], *, preferred_harness_id: str) -> dict[str, Any]:
    harnesses = [_dict_value(item, "harnesses[]") for item in _list_at(payload, "harnesses")]
    for harness in harnesses:
        if harness.get("harness_id") == preferred_harness_id:
            return harness
    raise HarborAdapterMatrixSmokeError(f"harness not found: {preferred_harness_id}")


def _select_benchmark(payload: dict[str, Any]) -> dict[str, Any]:
    benchmarks = _list_at(payload, "benchmarks")
    if not benchmarks:
        raise HarborAdapterMatrixSmokeError("benchmark catalog is empty")
    return _dict_value(benchmarks[0], "benchmarks[0]")


def _select_task(payload: dict[str, Any]) -> dict[str, Any]:
    tasks = _list_at(payload, "tasks")
    if not tasks:
        raise HarborAdapterMatrixSmokeError("task catalog is empty")
    return _dict_value(tasks[0], "tasks[0]")


def _summary_payload(
    *,
    config: HarborAdapterMatrixSmokeConfig,
    project: dict[str, Any],
    harness: dict[str, Any],
    models: list[dict[str, Any]],
    results: list[dict[str, Any]],
    started_at: str,
) -> dict[str, Any]:
    succeeded = [item for item in results if item["status"] == "succeeded"]
    failed = [item for item in results if item["status"] != "succeeded"]
    return {
        "status": "succeeded" if not failed else "failed",
        "started_at": started_at,
        "completed_at": _now(),
        "project_id": project["project_id"],
        "harness_id": harness["harness_id"],
        "agents": config.agent_ids,
        "models": [
            {"model_id": model["model_id"], "family": _model_family(model), "provider_config_id": model.get("provider_config_id")}
            for model in models
        ],
        "total": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "results": results,
    }


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = path.with_suffix(".md")
    lines = [
        "# Harbor Adapter Matrix Smoke",
        "",
        f"- Status: `{payload['status']}`",
        f"- Total: {payload['total']}",
        f"- Succeeded: {payload['succeeded']}",
        f"- Failed: {payload['failed']}",
        "",
        "| Agent | Model | Family | Status | Run ID | Adapter | Artifacts | Failure |",
        "| --- | --- | --- | --- | --- | --- | ---: | --- |",
    ]
    for item in payload["results"]:
        lines.append(
            "| {agent} | {model} | {family} | {status} | {run_id} | {adapter} | {artifacts} | {failure} |".format(
                agent=item.get("agent_id") or "",
                model=item.get("model_id") or "",
                family=item.get("family") or "",
                status=item.get("status") or "",
                run_id=item.get("run_id") or "",
                adapter=item.get("adapter_id") or "",
                artifacts=item.get("artifact_count") or 0,
                failure=str(item.get("failure_reason") or item.get("message") or "").replace("|", "\\|"),
            )
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _validate_bundle(payload: bytes) -> int:
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = set(archive.namelist())
    except zipfile.BadZipFile as exc:
        raise HarborAdapterMatrixSmokeError("artifact bundle was not a zip archive") from exc
    required = {"manifest.json", "run.json", "trajectory.jsonl", "evaluation.json", "artifact-metadata.json"}
    missing = sorted(required - names)
    if missing:
        raise HarborAdapterMatrixSmokeError(f"artifact bundle missing files: {', '.join(missing)}")
    return len(names)


def _verifier_score(run: dict[str, Any]) -> float | None:
    evaluator = run.get("evaluator")
    if not isinstance(evaluator, dict):
        return None
    score = evaluator.get("score")
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        return None
    return float(score)


def _response_json(response: Any, *, expected_status: int, context: str) -> dict[str, Any]:
    status_code = int(getattr(response, "status_code", 0))
    if status_code != expected_status:
        raise HarborAdapterMatrixSmokeError(f"failed to {context}: HTTP {status_code}: {getattr(response, 'text', '')}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise HarborAdapterMatrixSmokeError(f"received non-object response while trying to {context}")
    return payload


def _response_bytes(response: Any, *, expected_status: int, context: str) -> bytes:
    status_code = int(getattr(response, "status_code", 0))
    if status_code != expected_status:
        raise HarborAdapterMatrixSmokeError(f"failed to {context}: HTTP {status_code}: {getattr(response, 'text', '')}")
    payload = getattr(response, "content", b"")
    if not isinstance(payload, bytes) or not payload:
        raise HarborAdapterMatrixSmokeError(f"received empty bytes while trying to {context}")
    return payload


def _dict_value(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise HarborAdapterMatrixSmokeError(f"expected object at {name}")
    return value


def _list_at(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise HarborAdapterMatrixSmokeError(f"expected list at {key}")
    return value


def _model_family(model: dict[str, Any]) -> str:
    metadata = model.get("metadata") if isinstance(model.get("metadata"), dict) else {}
    family = metadata.get("family")
    return str(family) if family else "other"


def _adapter_id(preflight: dict[str, Any]) -> str:
    adapter = preflight.get("adapter")
    if isinstance(adapter, dict) and adapter.get("adapter_id"):
        return str(adapter["adapter_id"])
    return ""


def _preflight_message(preflight: dict[str, Any]) -> str:
    gaps = preflight.get("gaps")
    if isinstance(gaps, list) and gaps:
        first = gaps[0]
        if isinstance(first, dict):
            return str(first.get("message") or first.get("code") or "preflight blocked")
    return "preflight blocked"


def _trim_preflight(preflight: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": preflight.get("status"),
        "adapter": preflight.get("adapter"),
        "gaps": preflight.get("gaps"),
        "harbor_model_name": preflight.get("harbor_model_name"),
    }


def _run_id(prefix: str, *, index: int, agent_id: str, model_id: str) -> str:
    safe_agent = _safe_id(agent_id.removeprefix("harbor:"))
    safe_model = _safe_id(model_id)
    return f"{_safe_id(prefix)}_{index:03d}_{safe_agent}_{safe_model}_{uuid4().hex[:8]}"[:160]


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "item"


def _config_from_env(environ: Mapping[str, str] | None = None) -> HarborAdapterMatrixSmokeConfig:
    values = os.environ if environ is None else environ
    return HarborAdapterMatrixSmokeConfig(
        base_url=_env(values, "HARBOR_ADAPTER_MATRIX_BASE_URL", "http://127.0.0.1:8000"),
        username=_env(values, "HARBOR_ADAPTER_MATRIX_USERNAME", "[REDACTED_OWNER]"),
        password=_env(values, "HARBOR_ADAPTER_MATRIX_PASSWORD", "[REDACTED_PASSWORD]"),
        project_id=_env(values, "HARBOR_ADAPTER_MATRIX_PROJECT_ID", ""),
        harness_id=_env(values, "HARBOR_ADAPTER_MATRIX_HARNESS_ID", "harbor-local-docker"),
        agent_ids=_csv(values, "HARBOR_ADAPTER_MATRIX_AGENT_IDS", _MAINSTREAM_AGENT_IDS),
        model_ids=_csv(values, "HARBOR_ADAPTER_MATRIX_MODEL_IDS", ()),
        model_families=_csv(values, "HARBOR_ADAPTER_MATRIX_MODEL_FAMILIES", _MAINSTREAM_MODEL_FAMILIES),
        run_prefix=_env(values, "HARBOR_ADAPTER_MATRIX_RUN_PREFIX", "adapter_matrix"),
        timeout_seconds=float(_env(values, "HARBOR_ADAPTER_MATRIX_TIMEOUT_SECONDS", "7200")),
        per_run_timeout_seconds=int(_env(values, "HARBOR_ADAPTER_MATRIX_PER_RUN_TIMEOUT_SECONDS", "900")),
        agent_timeout_multiplier=float(_env(values, "HARBOR_ADAPTER_MATRIX_AGENT_TIMEOUT_MULTIPLIER", "1")),
        poll_interval_seconds=float(_env(values, "HARBOR_ADAPTER_MATRIX_POLL_INTERVAL_SECONDS", "5")),
        report_path=_env(values, "HARBOR_ADAPTER_MATRIX_REPORT_PATH", ""),
    )


def _env(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) and value.strip() else default


def _csv(values: Mapping[str, str], key: str, default: tuple[str, ...]) -> list[str]:
    raw = values.get(key, "")
    if isinstance(raw, str) and raw.strip():
        return [item.strip() for item in raw.split(",") if item.strip()]
    return list(default)


def _validate_config(config: HarborAdapterMatrixSmokeConfig) -> None:
    if not config.base_url.strip():
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_BASE_URL must be non-empty")
    if not config.username.strip():
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_USERNAME must be non-empty")
    if not config.password:
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_PASSWORD must be non-empty")
    if not config.agent_ids:
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_AGENT_IDS must not be empty")
    if not config.model_ids and not config.model_families:
        raise HarborAdapterMatrixSmokeError("set model ids or model families")
    if config.timeout_seconds <= 0:
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_TIMEOUT_SECONDS must be positive")
    if config.per_run_timeout_seconds <= 0:
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_PER_RUN_TIMEOUT_SECONDS must be positive")
    if config.agent_timeout_multiplier <= 0:
        raise HarborAdapterMatrixSmokeError("HARBOR_ADAPTER_MATRIX_AGENT_TIMEOUT_MULTIPLIER must be positive")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


_TERMINAL_STATUSES = {"succeeded", "failed", "canceled"}
_MAINSTREAM_AGENT_IDS = (
    "harbor:codex",
    "harbor:opencode",
    "harbor:claude-code",
    "harbor:gemini-cli",
    "harbor:qwen-coder",
    "harbor:aider",
    "harbor:openhands",
    "harbor:openhands-sdk",
    "harbor:swe-agent",
    "harbor:mini-swe-agent",
    "harbor:kimi-cli",
)
_MAINSTREAM_MODEL_FAMILIES = ("openai", "deepseek", "claude", "gemini", "qwen", "kimi", "glm", "grok", "minimax")


if __name__ == "__main__":
    raise SystemExit(main())
