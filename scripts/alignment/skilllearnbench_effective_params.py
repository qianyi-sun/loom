"""Build redacted SkillLearnBench effective request-parameter evidence.

The official SkillLearnBench runner can compute ``extra_flags`` that are not
actually consumed by the selected agent template. This offline helper records
the difference between computed flags and effective parameters, then compares
that official conclusion with Loom submitted/observed request-parameter audit
payloads.
"""

from __future__ import annotations

import argparse
import json
import shlex
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from loom.request_params import coerce_request_params, sanitize_request_extras

_OMIT_VALUE_OPTIONS = {
    "--api-key",
    "--env",
    "--header",
    "--input",
    "--instructions",
    "--password",
    "--prompt",
    "--secret",
    "--token",
    "-e",
}
_PATH_VALUE_OPTIONS = {
    "--artifact-dir",
    "--cwd",
    "--input-dir",
    "--output",
    "--output-dir",
    "--task",
    "--task-dir",
    "--workdir",
}
_SECRET_FRAGMENTS = (
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "credential",
    "github_token",
    "key",
    "password",
    "prompt",
    "secret",
    "token",
)


def build_evidence(
    official_plan: Mapping[str, Any],
    loom_debug: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    official_tasks = [_official_task(row) for row in _rows(official_plan, "tasks")]
    loom_trials = [_loom_trial(row) for row in _rows(loom_debug or {}, "trials")]
    loom_by_task = {row["task_id"]: row for row in loom_trials}
    comparisons = [
        _comparison(official, loom_by_task.get(official["task_id"]))
        for official in official_tasks
    ]
    counts = Counter(row["alignment_classification"] for row in comparisons)
    return {
        "schema_version": 1,
        "official_run_id": str(official_plan.get("run_id") or ""),
        "official_tasks": official_tasks,
        "loom_trials": loom_trials,
        "comparisons": comparisons,
        "summary": {
            "official_task_count": len(official_tasks),
            "loom_trial_count": len(loom_trials),
            "alignment_classification_counts": dict(sorted(counts.items())),
        },
    }


def write_markdown(evidence: Mapping[str, Any], path: Path) -> None:
    lines = [
        "# SkillLearnBench Effective Request Parameters",
        "",
        f"- Official tasks: {evidence.get('summary', {}).get('official_task_count', 0)}",
        f"- Loom trials: {evidence.get('summary', {}).get('loom_trial_count', 0)}",
        "",
        "## Alignment",
        "",
        "| Task | Agent | Template | Official conclusion | Loom trial | Classification |",
        "|---|---|---|---|---|---|",
    ]
    official_by_task = {
        row["task_id"]: row for row in evidence.get("official_tasks", [])
    }
    for row in evidence.get("comparisons", []):
        official = official_by_task.get(row["task_id"], {})
        lines.append(
            "| {task} | {agent} | {template} | {conclusion} | {trial} | {classification} |".format(
                task=row["task_id"],
                agent=official.get("agent_id", ""),
                template=official.get("template_id", ""),
                conclusion=official.get("effective_parameter_conclusion", ""),
                trial=row.get("loom_trial_id") or "missing",
                classification=row["alignment_classification"],
            ),
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build redacted SkillLearnBench effective request-parameter evidence "
            "from offline official-plan and Loom debug JSON files."
        ),
    )
    parser.add_argument("--official-plan-json", required=True, type=Path)
    parser.add_argument("--loom-debug-json", type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args(argv)

    official_plan = _read_json(args.official_plan_json)
    loom_debug = _read_json(args.loom_debug_json) if args.loom_debug_json else {}
    evidence = build_evidence(official_plan, loom_debug)

    args.out_json.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(evidence, args.out_md)
    print(json.dumps(evidence["summary"], indent=2, sort_keys=True))
    return 0


def _official_task(row: Mapping[str, Any]) -> dict[str, Any]:
    computed_extra_flags = str(row.get("computed_extra_flags") or "").strip()
    command_template = str(row.get("command_template") or "")
    settings_params = _settings_from_flags(computed_extra_flags)
    consumed = _template_consumed_extra_flags(command_template, computed_extra_flags)
    if not computed_extra_flags:
        conclusion = "provider_defaults_no_extra_flags"
        effective_params: dict[str, Any] = {}
    elif consumed and settings_params:
        conclusion = "explicit_template_params"
        effective_params = settings_params
    elif consumed:
        conclusion = "explicit_template_flags_no_safe_params"
        effective_params = {}
    else:
        conclusion = "provider_defaults_extra_flags_not_consumed"
        effective_params = {}
    return {
        "task_id": str(row.get("task_id") or ""),
        "agent_id": str(row.get("agent_id") or ""),
        "template_id": str(row.get("template_id") or ""),
        "model": str(row.get("model") or ""),
        "computed_extra_flags": _redact_command(computed_extra_flags),
        "template_has_extra_flags_placeholder": "{extra_flags}" in command_template,
        "template_consumed_extra_flags": consumed,
        "redacted_rendered_command": _redact_command(
            str(row.get("rendered_command") or ""),
        ),
        "official_effective_request_params": {
            "status": "available",
            "parameters": effective_params,
        },
        "effective_parameter_conclusion": conclusion,
    }


def _loom_trial(row: Mapping[str, Any]) -> dict[str, Any]:
    trial_config = row.get("trial_config")
    if not isinstance(trial_config, Mapping):
        trial_config = {}
    submitted = sanitize_request_extras(
        _mapping_or_empty(trial_config.get("request_params")),
    )
    provider = row.get("provider")
    if not isinstance(provider, Mapping):
        provider = {}
    observed = [
        coerce_request_params(_mapping_or_empty(call))
        for call in _rows(provider, "request_params")
    ]
    if observed:
        status_counts = Counter(str(item["status"]) for item in observed)
    else:
        status_counts = Counter(
            {
                str(k): int(v)
                for k, v in _mapping_or_empty(
                    provider.get("request_params_status_counts"),
                ).items()
            },
        )
    return {
        "task_id": str(row.get("task_id") or ""),
        "trial_id": str(row.get("trial_id") or ""),
        "submitted_request_params": {
            "status": "available",
            "parameters": submitted,
        },
        "observed_request_params_status_counts": dict(sorted(status_counts.items())),
        "observed_request_params_distinct": _distinct_params(observed),
    }


def _comparison(
    official: Mapping[str, Any],
    loom_trial: Mapping[str, Any] | None,
) -> dict[str, Any]:
    official_params = dict(
        official.get("official_effective_request_params", {}).get("parameters") or {},
    )
    if loom_trial is None:
        classification = "loom_trial_missing"
        trial_id = None
        submitted: dict[str, Any] = {}
        observed_sets: list[dict[str, Any]] = []
    else:
        trial_id = loom_trial.get("trial_id")
        submitted = dict(
            loom_trial.get("submitted_request_params", {}).get("parameters") or {},
        )
        observed_sets = list(loom_trial.get("observed_request_params_distinct") or [])
        classification = _alignment_classification(
            official_params,
            submitted,
            observed_sets,
        )
    return {
        "task_id": official["task_id"],
        "loom_trial_id": trial_id,
        "official_effective_parameters": official_params,
        "loom_submitted_parameters": submitted,
        "loom_observed_parameter_sets": observed_sets,
        "alignment_classification": classification,
    }


def _alignment_classification(
    official_params: Mapping[str, Any],
    submitted: Mapping[str, Any],
    observed_sets: Sequence[Mapping[str, Any]],
) -> str:
    observed_params = [dict(item) for item in observed_sets] or [dict(submitted)]
    if not official_params:
        if not submitted and all(not item for item in observed_params):
            return "aligned_by_provider_defaults"
        return "loom_params_not_in_official"
    if dict(submitted) == dict(official_params) and all(
        dict(item) == dict(official_params) for item in observed_params
    ):
        return "aligned_explicit_params"
    if not submitted and all(not item for item in observed_params):
        return "official_explicit_params_missing_in_loom"
    if dict(submitted) == dict(official_params):
        return "loom_observed_param_mismatch"
    return "request_param_mismatch"


def _template_consumed_extra_flags(
    command_template: str,
    computed_extra_flags: str,
) -> bool:
    return bool(computed_extra_flags.strip()) and "{extra_flags}" in command_template


def _settings_from_flags(flags: str) -> dict[str, Any]:
    tokens = _split(flags)
    params: dict[str, Any] = {}
    for index, token in enumerate(tokens):
        if token == "--settings" and index + 1 < len(tokens):
            try:
                parsed = json.loads(tokens[index + 1])
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, Mapping):
                params.update(sanitize_request_extras(parsed))
    return params


def _redact_command(command: str) -> list[str]:
    redacted: list[str] = []
    next_value: str | None = None
    for token in _split(command):
        if next_value == "omit":
            redacted.append("<omitted>")
            next_value = None
            continue
        if next_value == "path":
            redacted.append("<path>")
            next_value = None
            continue
        if _sensitive_assignment(token):
            redacted.append("<omitted>")
            continue
        if token in _OMIT_VALUE_OPTIONS:
            redacted.append(token)
            next_value = "omit"
            continue
        if token in _PATH_VALUE_OPTIONS:
            redacted.append(token)
            next_value = "path"
            continue
        if _looks_like_secret(token):
            redacted.append("<omitted>")
            continue
        if _looks_like_path(token):
            redacted.append("<path>")
            continue
        redacted.append(token)
    return redacted


def _split(value: str) -> list[str]:
    if not value:
        return []
    try:
        return shlex.split(value)
    except ValueError:
        return value.split()


def _distinct_params(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    distinct: list[dict[str, Any]] = []
    for row in rows:
        params = dict(row.get("parameters") or {})
        key = json.dumps(params, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        distinct.append(params)
    return distinct


def _rows(container: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = container.get(key)
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return []
    return [row for row in value if isinstance(row, Mapping)]


def _mapping_or_empty(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json(path: Path) -> Mapping[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping):
        raise SystemExit(f"{path} must contain a JSON object")
    return data


def _sensitive_assignment(token: str) -> bool:
    if "=" not in token:
        return False
    key = token.split("=", 1)[0].lower()
    return any(fragment in key for fragment in _SECRET_FRAGMENTS)


def _looks_like_secret(token: str) -> bool:
    lowered = token.lower()
    return any(fragment in lowered for fragment in _SECRET_FRAGMENTS)


def _looks_like_path(token: str) -> bool:
    return token.startswith(("/", "./", "../", "~")) or "/tmp/" in token


if __name__ == "__main__":
    raise SystemExit(main())
