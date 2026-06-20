"""tau2-bench default leaderboard task-set adapter.

The upstream leaderboard requires complete runs for the default `airline`,
`retail`, and `telecom` domains. `mock` is a development domain and
`telecom_full` is an extra registered task set, so this adapter publishes the
three default leaderboard task files without `num_tasks` / `task_ids` filters.
"""

from __future__ import annotations

import gzip
import json
import re
import shutil
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.util import (
    sha256_of_dir,
    structured_verifier_script,
    toml_string,
)

_DOMAINS = ("airline", "retail", "telecom")

_CHECK_PY = (
    textwrap.dedent(r'''
    import json
    import os
    import pathlib
    import re


    def load_json(path):
        if not path.is_file():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception as exc:
            return {"_error": str(exc)}


    def normalize_text(text):
        return re.sub(r"\s+", " ", str(text).lower()).strip()


    def content_words(text):
        return [word for word in re.findall(r"[a-z0-9]+", str(text).lower()) if len(word) > 2]


    def action_matches(expected, actual):
        if expected.get("name") != actual.get("name"):
            return False
        if expected.get("arguments", {}) != actual.get("arguments", {}):
            return False
        requestor = expected.get("requestor")
        if requestor and actual.get("requestor") not in (None, requestor):
            return False
        return True


    def score_actions(expected_actions, actual_actions):
        expected_actions = expected_actions or []
        actual_actions = actual_actions or []
        if not expected_actions:
            return (1.0 if not actual_actions else 0.0), []
        found = []
        cursor = 0
        for expected in expected_actions:
            matched_at = None
            for idx in range(cursor, len(actual_actions)):
                if action_matches(expected, actual_actions[idx]):
                    matched_at = idx
                    break
            if matched_at is None:
                continue
            found.append(expected.get("action_id") or expected.get("name"))
            cursor = matched_at + 1
        return len(found) / len(expected_actions), found


    def score_text_criteria(criteria, messages):
        criteria = criteria or []
        if not criteria:
            return None, []
        haystack = normalize_text("\n".join(str(m) for m in messages or []))
        matched = []
        for criterion in criteria:
            words = content_words(criterion)
            if words and all(word in haystack for word in words):
                matched.append(str(criterion))
        return len(matched) / len(criteria), matched


    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    output_path = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "agent_output.json"))
    )
    truth = json.loads((task_dir / "ground_truth.json").read_text())
    output = load_json(output_path)
    actions = output.get("actions", []) if isinstance(output, dict) else []
    messages = output.get("messages", []) if isinstance(output, dict) else []

    action_score, matched_actions = score_actions(truth.get("actions"), actions)
    scores = [action_score]
    checks = [
        {
            "name": "required_actions",
            "passed": action_score == 1.0,
            "score": action_score,
            "message": f"matched {len(matched_actions)} of {len(truth.get('actions') or [])} required actions",
        }
    ]

    for key in ("communicate_info", "nl_assertions"):
        text_score, matched = score_text_criteria(truth.get(key), messages)
        if text_score is None:
            continue
        scores.append(text_score)
        checks.append(
            {
                "name": key,
                "passed": text_score == 1.0,
                "score": text_score,
                "message": f"matched {len(matched)} of {len(truth.get(key) or [])}",
            }
        )

    score = sum(scores) / len(scores)
    result = {
        "rewards": {"score": score},
        "checks": checks,
        "structured": {
            "matched_actions": matched_actions,
            "expected_actions": len(truth.get("actions") or []),
            "env_assertions": truth.get("env_assertions") or [],
        },
        "confidence": 1.0,
    }
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(json.dumps(result))
''').strip()
    + "\n"
)

_RUN_SH = textwrap.dedent("""
    script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
    export LOOM_TASK_DIR="$task_dir"
    export LOOM_AGENT_OUTPUT="${LOOM_AGENT_OUTPUT:-$task_dir/agent_output.json}"
    "${PYTHON:-python3}" "$task_dir/verifier/check.py"
""").strip()


class Tau2BenchAdapter(CatalogBackedAdapter):
    name = "tau2-bench"

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        root = _source_root(source_dir)
        for domain in _DOMAINS:
            tasks_path = root / "domains" / domain / "tasks.json"
            tasks = json.loads(tasks_path.read_text())
            for idx, record in enumerate(tasks):
                upstream_task_id = str(record["id"])
                yield BenchmarkInstance(
                    instance_id=f"{domain}/{idx:03d}",
                    split=split,
                    raw={
                        "domain": domain,
                        "source_root": str(root),
                        "upstream_task_id": upstream_task_id,
                        "task": record,
                    },
                    tags={
                        "domain": domain,
                        "task_set": "default_leaderboard",
                        "upstream_task_id": upstream_task_id,
                    },
                )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        domain = str(instance.raw["domain"])
        source_root = Path(str(instance.raw["source_root"]))
        task = dict(instance.raw["task"])
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        domain_dir = source_root / "domains" / domain
        _copy_domain_assets(domain_dir, out_dir / "domain")
        (out_dir / "task.json").write_text(json.dumps(task, indent=2, sort_keys=True) + "\n")
        ground_truth = dict(task.get("evaluation_criteria") or {})
        (out_dir / "ground_truth.json").write_text(
            json.dumps(ground_truth, indent=2, sort_keys=True) + "\n",
        )
        (out_dir / "instruction.md").write_text(
            _render_instruction(domain=domain, task=task),
        )

        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        (verifier_dir / "check.py").write_text(_CHECK_PY)
        structured_verifier_script(_RUN_SH, out_dir=out_dir)

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        (out_dir / "task.toml").write_text(
            textwrap.dedent(f"""
            schema_version = "1"

            [task]
            id = {toml_id}
            name = {toml_name}

            [environment]
            os = "linux"
            docker_image = "python:3.11-slim"

            [agent]
            name = "oracle"

            [verifier]
            name = "script"

            [verifier.args]
            script_path = "/workspace/verifier/run.sh"

            [[steps]]
            name = "main"
            artifacts = ["agent_output.json"]
        """).strip()
            + "\n",
        )

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )


def _source_root(source_dir: Path) -> Path:
    if (source_dir / "repo" / "domains").is_dir():
        return source_dir / "repo"
    return source_dir


def _copy_domain_assets(domain_dir: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    for path in sorted(domain_dir.iterdir()):
        if not path.is_file():
            continue
        if path.name.startswith("tasks"):
            continue
        if path.suffix == ".json":
            with path.open("rb") as src, gzip.open(dest_dir / f"{path.name}.gz", "wb") as dst:
                shutil.copyfileobj(src, dst)
            continue
        if path.suffix in {".md", ".toml"}:
            shutil.copy2(path, dest_dir / path.name)


def _render_instruction(*, domain: str, task: dict[str, Any]) -> str:
    scenario = _render_scenario(task.get("user_scenario"))
    description = _render_description(task.get("description"))
    ticket = str(task.get("ticket") or "").strip()
    ticket_block = f"## Ticket\n\n{ticket}\n\n" if ticket else ""
    return (
        f"# tau2-bench {domain} task\n\n"
        f"{description}"
        f"{ticket_block}"
        f"{scenario}"
        "## Domain Assets\n\n"
        "Relevant policy, database, and support files are available under "
        "the `domain/` directory. JSON databases are gzip-compressed as "
        "`*.json.gz`.\n\n"
        "## Required output\n\n"
        "Write `agent_output.json` with this shape:\n\n"
        "```json\n"
        "{\"actions\": [{\"name\": \"tool_name\", \"arguments\": {}}], "
        "\"messages\": [\"user-facing response\"]}\n"
        "```\n\n"
        "Use the action names and arguments you would execute in the tau2 "
        "domain. Include user-facing messages needed to satisfy the task.\n"
    )


def _render_description(value: object) -> str:
    if not isinstance(value, dict):
        return ""
    lines = []
    for key in ("purpose", "relevant_policies", "notes"):
        item = value.get(key)
        if item:
            lines.append(f"- {key}: {item}")
    if not lines:
        return ""
    return "## Task Description\n\n" + "\n".join(lines) + "\n\n"


def _render_scenario(value: object) -> str:
    if not isinstance(value, dict):
        return ""
    instructions = value.get("instructions")
    persona = value.get("persona")
    lines = []
    if persona:
        lines.append(f"Persona: {persona}")
    if isinstance(instructions, dict):
        for key, item in instructions.items():
            if item:
                lines.append(f"{_titleize(key)}: {item}")
    elif instructions:
        lines.append(str(instructions))
    if not lines:
        return ""
    return "## User Scenario\n\n" + "\n\n".join(lines) + "\n\n"


def _titleize(value: str) -> str:
    return re.sub(r"[_-]+", " ", value).strip().title()
