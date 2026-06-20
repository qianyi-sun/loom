"""BFCL — Berkeley Function-Calling Leaderboard. Spec §5.2 row 8.

Each upstream row carries a `question` (list of conversation turns), a
`function` schema list, and either a `ground_truth` in the parallel
`possible_answer/` tree or an official call-presence objective for the
relevance/irrelevance categories. The agent emits a canonical
`agent_output.json`; the bundled script verifier scores that output and
writes Loom `VerifierResult` JSON.
"""

from __future__ import annotations

import json
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

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

_BFCL_CHECK_PY = textwrap.dedent(r'''
    import ast
    import json
    import os
    import pathlib
    from typing import Any


    def _attr_name(node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = _attr_name(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        return None


    def _literal(node: ast.AST) -> Any:
        try:
            return ast.literal_eval(node)
        except Exception:
            return ast.unparse(node)


    def _call_from_string(value: str) -> dict[str, Any] | None:
        text = value.strip()
        if not text or text == "[]":
            return None
        try:
            expr = ast.parse(text, mode="eval").body
        except SyntaxError:
            return None
        if not isinstance(expr, ast.Call):
            return None
        name = _attr_name(expr.func)
        if not name:
            return None
        args: dict[str, Any] = {
            f"arg{idx}": _literal(arg) for idx, arg in enumerate(expr.args)
        }
        for kw in expr.keywords:
            if kw.arg is not None:
                args[kw.arg] = _literal(kw.value)
        return {"name": name, "arguments": args}


    def _call_from_dict(value: dict[str, Any]) -> dict[str, Any] | None:
        function = value.get("function")
        if isinstance(function, dict):
            name = function.get("name") or value.get("name")
            arguments = function.get("arguments", value.get("arguments", {}))
        else:
            name = value.get("name") or value.get("function")
            arguments = value.get("arguments", value.get("args", {}))
        if not name:
            return None
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {}
        if not isinstance(arguments, dict):
            arguments = {}
        return {"name": str(name), "arguments": arguments}


    def _calls_from_value(value: Any) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, str):
            call = _call_from_string(value)
            return [call] if call else []
        if isinstance(value, dict):
            if "calls" in value:
                return _calls_from_value(value["calls"])
            if "turns" in value:
                calls: list[dict[str, Any]] = []
                for turn in value["turns"]:
                    calls.extend(_calls_from_value(turn))
                return calls
            call = _call_from_dict(value)
            return [call] if call else []
        if isinstance(value, list):
            calls = []
            for item in value:
                calls.extend(_calls_from_value(item))
            return calls
        return []


    def _turns_from_value(value: Any) -> list[list[dict[str, Any]]]:
        if isinstance(value, dict) and "turns" in value:
            return [_calls_from_value(turn) for turn in value["turns"]]
        if isinstance(value, list) and all(isinstance(turn, list) for turn in value):
            return [_calls_from_value(turn) for turn in value]
        calls = _calls_from_value(value)
        return [calls] if calls else []


    def _stable(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        return json.dumps(value, sort_keys=True, separators=(",", ":"))


    def _arg_matches(actual: Any, accepted: Any) -> bool:
        accepted_values = accepted if isinstance(accepted, list) else [accepted]
        actual_stable = _stable(actual)
        return any(actual_stable == _stable(candidate) for candidate in accepted_values)


    def _call_matches(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
        if actual["name"] != expected["name"]:
            return False
        actual_args = actual.get("arguments", {})
        expected_args = expected.get("arguments", {})
        return all(
            key in actual_args and _arg_matches(actual_args[key], accepted)
            for key, accepted in expected_args.items()
        )


    def _normalize_call(call: dict[str, Any] | None) -> str:
        if call is None:
            return ""
        return json.dumps(call, sort_keys=True, separators=(",", ":"))


    def _expected_calls(ground_truth: list[Any]) -> list[dict[str, Any]]:
        calls = []
        for item in ground_truth:
            if not isinstance(item, dict):
                continue
            for name, arguments in item.items():
                calls.append({"name": str(name), "arguments": arguments})
        return calls


    def _score(ground_truth: Any, agent_output: Any) -> tuple[bool, str, dict[str, Any]]:
        calls = _calls_from_value(agent_output)
        if isinstance(ground_truth, dict) and ground_truth.get("mode"):
            mode = str(ground_truth["mode"])
            if mode == "no_function_call":
                return len(calls) == 0, mode, {"calls": calls}
            if mode == "requires_function_call":
                return len(calls) > 0, mode, {"calls": calls}

        if (
            isinstance(ground_truth, list)
            and ground_truth
            and all(isinstance(turn, list) for turn in ground_truth)
        ):
            expected_turns = [
                [_normalize_call(_call_from_string(call)) for call in turn]
                for turn in ground_truth
            ]
            actual_turns = [
                [_normalize_call(call) for call in turn]
                for turn in _turns_from_value(agent_output)
            ]
            return expected_turns == actual_turns, "multi_turn_match", {
                "expected_turns": expected_turns,
                "actual_turns": actual_turns,
            }

        expected = _expected_calls(ground_truth if isinstance(ground_truth, list) else [])
        passed = bool(expected) and all(
            any(_call_matches(actual, expected_call) for actual in calls)
            for expected_call in expected
        )
        return passed, "function_call_match", {
            "expected": expected,
            "calls": calls,
        }


    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    agent_output_path = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "agent_output.json"))
    )
    ground_truth = json.loads((task_dir / "ground_truth.json").read_text())
    try:
        agent_output = json.loads(agent_output_path.read_text())
        parse_error = None
    except Exception as exc:
        agent_output = None
        parse_error = str(exc)

    passed, mode, structured = _score(ground_truth, agent_output)
    if parse_error is not None:
        passed = False
        structured["parse_error"] = parse_error
    score = 1.0 if passed else 0.0
    result = {
        "rewards": {"score": score},
        "checks": [
            {
                "name": "bfcl",
                "passed": passed,
                "score": score,
                "message": "BFCL output matched" if passed else "BFCL output did not match",
            }
        ],
        "structured": {"mode": mode, **structured},
        "confidence": 1.0,
    }
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(json.dumps(result))
''').strip() + "\n"


_BFCL_RUN_SH = textwrap.dedent("""
    script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
    export LOOM_TASK_DIR="$task_dir"
    export LOOM_AGENT_OUTPUT="${LOOM_AGENT_OUTPUT:-$task_dir/agent_output.json}"
    : "${LOOM_VERIFIER_OUTPUT:?LOOM_VERIFIER_OUTPUT is required}"
    "${PYTHON:-python3}" "$task_dir/verifier/check.py"
""").strip()


class BFCLAdapter(CatalogBackedAdapter):
    name = "bfcl"

    @staticmethod
    def _category_for(data_file: Path) -> str:
        category = data_file.stem
        for prefix in ("BFCL_v4_", "BFCL_"):
            if category.startswith(prefix):
                return category.removeprefix(prefix)
        return category

    @staticmethod
    def _possible_answers_for(data_file: Path) -> dict[str, object]:
        answers_file = data_file.parent / "possible_answer" / data_file.name
        if not answers_file.is_file():
            return {}
        answers: dict[str, object] = {}
        for line in answers_file.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if (
                isinstance(rec, dict)
                and "id" in rec
                and "ground_truth" in rec
            ):
                answers[str(rec["id"])] = rec["ground_truth"]
        return answers

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        # Upstream restructured circa BFCL v4: data moved from
        # `berkeley-function-call-leaderboard/data/` to
        # `berkeley-function-call-leaderboard/bfcl_eval/data/`. Try
        # both so the adapter keeps working if the repo ever moves
        # back, and fall back to a recursive search as a last resort.
        bfcl_root = source_dir / "repo" / "berkeley-function-call-leaderboard"
        candidates = [
            bfcl_root / "bfcl_eval" / "data",
            bfcl_root / "data",
        ]
        data_dir = next((c for c in candidates if c.is_dir()), None)
        if data_dir is None:
            return
        for path in sorted(data_dir.glob("BFCL_*.json")):
            category = self._category_for(path)
            possible_answers = self._possible_answers_for(path)
            # Upstream mixes JSONL task files with auxiliary index
            # files (`BFCL_v4_format_sensitivity.json` etc.) that are
            # single multi-line JSON objects mapping category names to
            # task-id lists. Skip non-JSONL files (any line that isn't
            # a parseable object with an `id` field).
            for line in path.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    rec = cast(dict[str, Any], json.loads(line))
                except json.JSONDecodeError:
                    break  # file is not JSONL; skip remainder
                if not isinstance(rec, dict) or "id" not in rec:
                    break
                ground_truth = possible_answers.get(str(rec["id"]))
                if ground_truth is not None:
                    rec["ground_truth"] = ground_truth
                elif "irrelevance" in category:
                    rec["ground_truth"] = {
                        "mode": "no_function_call",
                        "category": category,
                    }
                elif "relevance" in category:
                    rec["ground_truth"] = {
                        "mode": "requires_function_call",
                        "category": category,
                    }
                yield BenchmarkInstance(
                    instance_id=str(rec["id"]),
                    split=split,
                    raw=rec,
                    tags={"bfcl_category": category},
                )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        question_turns = []
        for idx, turns in enumerate(r["question"], start=1):
            rendered_turns = "\n".join(
                f"{turn.get('role', 'user')}: {turn['content']}"
                for turn in turns
            )
            question_turns.append(f"Turn {idx}:\n{rendered_turns}")
        question_text = "\n\n".join(question_turns)

        extra_sections = []
        if "function" in r:
            extra_sections.append(
                "## Available functions\n\n"
                f"```json\n{json.dumps(r['function'], indent=2)}\n```"
            )
        if "initial_config" in r:
            extra_sections.append(
                "## Initial state\n\n"
                f"```json\n{json.dumps(r['initial_config'], indent=2)}\n```"
            )
        if "path" in r:
            extra_sections.append(
                "## Available function path\n\n"
                f"```json\n{json.dumps(r['path'], indent=2)}\n```"
            )

        output_contract = textwrap.dedent("""
            ## Required output

            Write your answer to `agent_output.json`.

            For single-turn tasks, use this shape:

            ```json
            {"calls": [{"name": "function_name", "arguments": {"arg": "value"}}]}
            ```

            If no function should be called, write:

            ```json
            {"calls": []}
            ```

            For multi-turn tasks, use `turns`, where each item is that turn's
            call list:

            ```json
            {"turns": [[{"name": "function_name", "arguments": {}}]]}
            ```
        """).strip()

        body_sections = [question_text, *extra_sections, output_contract]
        (out_dir / "instruction.md").write_text(
            f"# BFCL {instance.instance_id}\n\n"
            + "\n\n".join(body_sections)
            + "\n",
        )

        ground_truth = r.get("ground_truth")
        if ground_truth is None:
            category = instance.tags.get("bfcl_category", "")
            if "irrelevance" in category:
                ground_truth = {
                    "mode": "no_function_call",
                    "category": category,
                }
            elif "relevance" in category:
                ground_truth = {
                    "mode": "requires_function_call",
                    "category": category,
                }
            else:
                raise ValueError(
                    "BFCL instance is missing ground_truth and is not a "
                    f"relevance category: {instance.instance_id}",
                )
        (out_dir / "ground_truth.json").write_text(
            json.dumps(ground_truth, indent=2),
        )

        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        (verifier_dir / "check.py").write_text(_BFCL_CHECK_PY)
        structured_verifier_script(_BFCL_RUN_SH, out_dir=out_dir)

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        (out_dir / "task.toml").write_text(textwrap.dedent(f"""
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
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
