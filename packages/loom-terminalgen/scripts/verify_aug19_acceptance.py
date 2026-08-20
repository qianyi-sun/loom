#!/usr/bin/env python3
"""Replay the Aug19 four-source acceptance set through the imported pipeline."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import tempfile
import tomllib
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from terminalgen.models import DatasetTask, Difficulty, GenerationMode
from terminalgen.pipeline import SyntheticTaskPipeline

SELECTED_RUNS = {
    "qemu-alpine-ssh": ("v2", 9201),
    "feal-differential-cryptanalysis": ("v3", 9302),
    "financial-document-processor": ("v2", 9203),
    "break-filter-js-from-html": ("v2", 9204),
}


@dataclass(frozen=True)
class ReplayCall:
    prompt: str
    task: DatasetTask | None
    error_message: str | None


class ReplaySynthesizer:
    """A deterministic stand-in for the already-recorded model boundary."""

    def __init__(self, calls: list[ReplayCall]) -> None:
        self._calls = deque(calls)
        self.prompt_sha256: list[str] = []
        self.call_count = 0
        self.failed_calls = 0
        self.accepted_packages = 0
        self.last_error: str | None = None
        self.prompt_exact_matches = 0
        self.prompt_whitespace_normalized_matches = 0

    def generate_task(
        self,
        *,
        spec: Any,
        domain: Any,
        system_prompt: str,
        user_prompt: str,
        base_image: str,
    ) -> DatasetTask:
        del spec, domain, system_prompt, base_image
        while True:
            if not self._calls:
                raise AssertionError("pipeline issued more calls than the Aug19 record")
            call = self._calls.popleft()
            self.call_count += 1
            if user_prompt == call.prompt:
                self.prompt_exact_matches += 1
            elif _normalize_prompt_whitespace(user_prompt) == _normalize_prompt_whitespace(
                call.prompt
            ):
                self.prompt_whitespace_normalized_matches += 1
            else:
                self.last_error = (
                    "generated prompt does not match the recorded Aug19 provider prompt: "
                    f"actual={_sha256(user_prompt.encode())} "
                    f"expected={_sha256(call.prompt.encode())}"
                )
                raise AssertionError(self.last_error)
            self.prompt_sha256.append(_sha256(user_prompt.encode()))
            if call.error_message is not None:
                self.failed_calls += 1
                continue
            if call.task is None:
                raise AssertionError("successful replay call is missing a task")
            self.accepted_packages += 1
            return call.task

    def assert_exhausted(self) -> None:
        if self._calls:
            raise AssertionError(f"{len(self._calls)} recorded calls were not replayed")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _normalize_prompt_whitespace(prompt: str) -> str:
    """Ignore only duplicate empty lines in the archived prompt evidence."""
    return re.sub(r"\n{3,}", "\n\n", prompt.strip())


def _tree_manifest(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        mode = stat.S_IMODE(path.stat().st_mode)
        payload = path.read_bytes()
        rows.append(
            {
                "path": relative,
                "bytes": len(payload),
                "mode": f"{mode:04o}",
                "sha256": _sha256(payload),
            }
        )
    return rows


def _tree_sha256(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return _sha256(canonical)


def _load_exported_task(task_dir: Path) -> DatasetTask:
    metadata = tomllib.loads((task_dir / "task.toml").read_text(encoding="utf-8"))[
        "metadata"
    ]
    requirements = [
        line.strip()
        for line in (task_dir / "tests" / "requirements.txt")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    return DatasetTask(
        task_id=task_dir.name,
        prompt=(task_dir / "instruction.md").read_text(encoding="utf-8"),
        tests=(task_dir / "tests" / "test_outputs.py").read_text(encoding="utf-8"),
        info={
            "domain": metadata["category"].replace("-", "_"),
            "difficulty": metadata["difficulty"],
        },
        workspace_dir=task_dir / "environment" / "files",
        test_requirements=requirements,
    )


def _load_calls(run_dir: Path, tasks: deque[DatasetTask]) -> list[ReplayCall]:
    calls: list[ReplayCall] = []
    for call_path in sorted((run_dir / "calls").glob("[0-9][0-9][0-9][0-9][0-9][0-9].json")):
        payload = json.loads(call_path.read_text(encoding="utf-8"))
        command = payload.get("command")
        if not isinstance(command, list) or not command or not isinstance(command[-1], str):
            raise AssertionError(f"recorded call has no provider prompt: {call_path}")
        error = payload.get("error")
        if error is None:
            if not tasks:
                raise AssertionError(f"recorded successes exceed selected tasks: {call_path}")
            calls.append(ReplayCall(prompt=command[-1], task=tasks.popleft(), error_message=None))
        else:
            message = error.get("message") if isinstance(error, dict) else str(error)
            calls.append(ReplayCall(prompt=command[-1], task=None, error_message=message))
    if tasks:
        raise AssertionError(f"{len(tasks)} selected tasks have no successful call record")
    return calls


def verify(reference_root: Path) -> dict[str, Any]:
    corpus_root = reference_root / "catalog-calibrated-synthesis 2"
    selection_path = corpus_root / "analysis" / "final-selection.json"
    run_manifest_path = corpus_root / "analysis" / "run-manifest.json"
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))

    if selection["summary"] != {
        "selected_tasks": 20,
        "source_tasks": 4,
        "tasks_per_source": 5,
        "hit": 20,
        "proxy": 0,
        "drift": 0,
        "sft_verdict_retain": 20,
    }:
        raise AssertionError("Aug19 final selection is not the canonical 4 x 5 set")
    invocation = run_manifest["invocation"]
    expected_invocation = {
        "mode": "skill-based",
        "synthesizer": "opencode-agent",
        "model": "gpt-5.6-luna",
        "difficulty": "hard",
        "count_per_run": 5,
        "workers": 1,
        "max_retries": 3,
        "temperature": 1.0,
    }
    for key, value in expected_invocation.items():
        if invocation.get(key) != value:
            raise AssertionError(f"unexpected Aug19 invocation {key}: {invocation.get(key)!r}")

    sources = {item["source_task"]: item for item in selection["sources"]}
    if set(sources) != set(SELECTED_RUNS):
        raise AssertionError("Aug19 selection does not contain the expected four source tasks")

    source_reports: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="loom-terminalgen-aug19-") as tmp:
        output_root = Path(tmp)
        for source_task, (round_name, seed) in SELECTED_RUNS.items():
            source = sources[source_task]
            expected_catalog = f"catalogs/{round_name}/{source_task}.json"
            if source["catalog_path"] != expected_catalog:
                raise AssertionError(
                    f"{source_task} catalog mismatch: {source['catalog_path']}"
                )
            selected_dirs = deque(
                _load_exported_task(corpus_root / task["path"])
                for task in sorted(source["tasks"], key=lambda item: item["sample_order"])
            )
            reference_dirs = [corpus_root / task["path"] for task in source["tasks"]]
            run_dir = corpus_root / "runs" / round_name / source_task
            replay = ReplaySynthesizer(_load_calls(run_dir, selected_dirs))
            generated_root = output_root / source_task
            pipeline = SyntheticTaskPipeline(
                replay,
                console=Console(file=None, quiet=True),
                random_seed=seed,
            )
            tasks = pipeline.generate_tasks(
                mode=GenerationMode.SKILL_BASED,
                count=5,
                output_path=generated_root,
                workers=1,
                difficulty=Difficulty.HARD,
                catalog_config=corpus_root / expected_catalog,
                benchmark_dedup_enabled=False,
            )
            replay.assert_exhausted()
            if len(tasks) != 5:
                detail = f": {replay.last_error}" if replay.last_error else ""
                raise AssertionError(
                    f"{source_task} generated {len(tasks)} tasks, expected 5{detail}"
                )

            task_reports: list[dict[str, Any]] = []
            for reference_dir in reference_dirs:
                relative = reference_dir.relative_to(run_dir)
                generated_dir = generated_root / relative
                expected_rows = _tree_manifest(reference_dir)
                actual_rows = _tree_manifest(generated_dir)
                if actual_rows != expected_rows:
                    raise AssertionError(
                        f"generated bundle differs from Aug19 reference: {source_task}/{relative}"
                    )
                task_reports.append(
                    {
                        "task_id": reference_dir.name,
                        "domain": relative.parent.as_posix(),
                        "file_count": len(expected_rows),
                        "tree_sha256": _tree_sha256(expected_rows),
                    }
                )
            source_reports.append(
                {
                    "source_task": source_task,
                    "catalog": expected_catalog,
                    "catalog_sha256": _sha256((corpus_root / expected_catalog).read_bytes()),
                    "seed": seed,
                    "recorded_calls": replay.call_count,
                    "recorded_failed_calls": replay.failed_calls,
                    "prompt_exact_matches": replay.prompt_exact_matches,
                    "prompt_whitespace_normalized_matches": (
                        replay.prompt_whitespace_normalized_matches
                    ),
                    "prompt_sha256": replay.prompt_sha256,
                    "tasks": task_reports,
                }
            )

    return {
        "schema_version": 1,
        "acceptance": "pass",
        "criterion": "four canonical source tasks reproduce all 20 selected Aug19 bundles byte-for-byte",
        "reference_selection_date": selection["selection_date"],
        "reference_selection_sha256": _sha256(selection_path.read_bytes()),
        "reference_run_manifest_sha256": _sha256(run_manifest_path.read_bytes()),
        "invocation": expected_invocation,
        "sources": source_reports,
        "summary": {
            "source_tasks": len(source_reports),
            "generated_tasks": sum(len(source["tasks"]) for source in source_reports),
            "matching_tasks": sum(len(source["tasks"]) for source in source_reports),
            "mismatching_tasks": 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.reference_root.resolve())
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
