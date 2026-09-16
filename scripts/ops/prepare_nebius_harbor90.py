#!/usr/bin/env python3
"""Prepare the existing Harbor90 catalog for x86; never publish or mutate source."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import tomllib
from collections import Counter
from pathlib import Path
from typing import Any

import tomli_w

from loom.execution_runtime_contract import TaskExecutionResourceRequestsV1
from loom.models.task import TaskConfig
from loom.models.task_checksum import task_checksum
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    automatic_service_execution_rejections,
    runtime_profile_rejections,
    validate_task_resource_requests,
)
from loom.task_bundle_compat import (
    CompatibilitySeverity,
    collect_task_dir_compatibility_issues,
    format_compatibility_issues,
)
from loom_cli.local_benchmark_validate import validate_local_benchmark

BENCHMARK_ID = "terminal-bench-2-harbor-90"
NATIVE_TASKS = frozenset({
    "cancel-async-tasks", "constraints-scheduling", "db-wal-recovery",
    "file-archive-manifest", "git-leak-recovery", "large-scale-text-editing",
    "log-summary-date-ranges", "multi-source-data-merger", "openssl-selfsigned-cert",
    "regex-log", "circuit-fibsqrt", "dna-insert", "extract-elf", "fix-git",
    "polyglot-c-py", "polyglot-rust-c", "regex-chess", "sparql-university",
    "sqlite-db-truncate", "write-compressor",
})
BOOTSTRAP_OMISSIONS = frozenset({
    "cancel-async-tasks", "file-archive-manifest", "large-scale-text-editing",
    "multi-source-data-merger",
})


def inventory(root: Path) -> dict[str, Path]:
    if not root.is_dir():
        raise ValueError(f"task source root missing: {root}")
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ValueError("task sources must not contain symlinks")
    tasks = {path.parent.name: path.parent for path in root.glob("*/task.toml")}
    if not tasks:
        raise ValueError("no task directories")
    return tasks


def x86_task_toml(body: str) -> str:
    """Change the environment declaration only, leaving task/oracle text intact."""
    section = re.search(r"(?ms)^\[environment\][ \t]*\r?\n(?P<body>.*?)(?=^\[|\Z)", body)
    if section is None:
        raise ValueError("task requires an environment section")
    environment = section.group("body")
    declaration = re.compile(r'(?m)^cpu_arch\s*=\s*[\"\'](?:arm64|x86_64|any)[\"\'][^\r\n]*')
    if len(declaration.findall(environment)) != 1:
        raise ValueError("expected one supported environment cpu_arch declaration")
    environment = declaration.sub('cpu_arch = "x86_64"', environment)
    return body[:section.start("body")] + environment + body[section.end("body"):]


def protect_assertions(original: Path, overlay: Path) -> int:
    config = tomllib.loads((original / "task.toml").read_text())
    instruction_paths = {"instruction.md", *(step.get("instruction_file", "instruction.md")
                                            for step in config.get("steps", []))}
    if any(Path(path).is_absolute() or ".." in Path(path).parts for path in instruction_paths):
        raise ValueError("instruction path escapes task root")
    protected = [*(original / path for path in instruction_paths),
                 *(original / "tests").rglob("*")]
    count = 0
    for source in protected:
        if not source.is_file():
            continue
        relative = source.relative_to(original)
        target = overlay / relative
        if (relative.as_posix() == "tests/test.sh" and original.name in BOOTSTRAP_OMISSIONS
                and not target.exists()):
            continue
        if not target.is_file() or source.read_bytes() != target.read_bytes():
            raise ValueError(f"native overlay changes protected instruction/test: {original.name}/{relative}")
        count += 1
    return count


def arm_build_references(task_dir: Path) -> list[str]:
    dockerfile = task_dir / "environment" / "Dockerfile"
    if not dockerfile.is_file():
        raise ValueError(f"missing Dockerfile: {task_dir.name}")
    return [line for line in dockerfile.read_text().splitlines()
            if not line.lstrip().startswith("#") and re.search(r"\b(?:arm64|aarch64)\b", line, re.I)]


def prepare(
    *, source_root: Path, catalog_metadata: Path, overlay_roots: list[Path],
    overlay_metadata: Path, profile_path: Path, output: Path,
) -> dict[str, Any]:
    if output.exists():
        raise ValueError("output already exists; preserve it or choose a new output directory")
    sources = inventory(source_root)
    catalog = json.loads(catalog_metadata.read_text())
    if (catalog["benchmark"]["id"] != BENCHMARK_ID
            or catalog["benchmark"]["license_spdx"] != "Apache-2.0"):
        raise ValueError("requires the existing Harbor90 ID and Apache-2.0 metadata")
    rows = {row["id"].removeprefix(BENCHMARK_ID + "/"): row for row in catalog["tasks"]}
    if len(rows) != 90 or set(rows) != set(sources):
        raise ValueError("the current catalog and source must contain the same 90 task IDs")
    for name, row in rows.items():
        if row["id"] != f"{BENCHMARK_ID}/{name}" or row["license"] != "Apache-2.0":
            raise ValueError("task identity/license changed")
        if task_checksum(sources[name]) != row["checksum"]:
            raise ValueError(f"source differs from current catalog: {name}")
    overlays: dict[str, Path] = {}
    for root in overlay_roots:
        for name, directory in inventory(root).items():
            if name in overlays:
                raise ValueError("duplicate overlay task")
            overlays[name] = directory
    if set(overlays) != NATIVE_TASKS:
        raise ValueError("native overlays must be exactly the approved 20-task cohort")
    approved = json.loads(overlay_metadata.read_text())["tasks"]
    approved_checksums = {row["name"]: row["current_task_checksum"] for row in approved}
    for name, directory in overlays.items():
        if task_checksum(directory) != approved_checksums.get(name):
            raise ValueError(f"native overlay changed since its frozen TaskSet: {name}")
        protect_assertions(sources[name], directory)
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(profile_path.read_text())
    if profile.default_task_resource_requests is None:
        raise ValueError("active runtime profile has no scheduling baseline")
    trial = TrialConfig.model_validate({"agent_name": "terminus-2", "agent_model": {
        "provider": "openai", "name": "glm-5.2", "source": "api",
    }})
    records = []
    # Validate every source/overlay before writing any output.
    for name in sorted(sources):
        directory = overlays.get(name, sources[name])
        if arm_build_references(directory):
            raise ValueError(f"ARM-specific build reference requires explicit review: {name}")
        raw = tomllib.loads(x86_task_toml((directory / "task.toml").read_bytes().decode()))
        task = TaskConfig.model_validate(raw)
        compatibility_errors = [
            issue for issue in collect_task_dir_compatibility_issues(directory, task_config=raw)
            if issue.severity == CompatibilitySeverity.ERROR
        ]
        if compatibility_errors:
            raise ValueError(f"publish compatibility failed for {name}: "
                             + format_compatibility_issues(compatibility_errors))
        reasons = list(automatic_service_execution_rejections(
            task, trial, source_provenance={}, allow_task_image_preparation=True,
        ))
        # Publication supplies the manifest after this local source preparation.
        reasons = [reason for reason in reasons if reason != "immutable_task_input_unavailable"]
        reasons.extend(runtime_profile_rejections(task, trial, profile, allow_task_image_preparation=True))
        try:
            validate_task_resource_requests(
                task=task, trial=trial, profile=profile,
                task_revision_sha256="sha256:" + rows[name]["checksum"],
                override=TaskExecutionResourceRequestsV1(
                    task_revision_sha256="sha256:" + rows[name]["checksum"],
                    requests=profile.default_task_resource_requests,
                ),
            )
        except ValueError as exc:
            reasons.append(str(exc))
        if name in overlays and reasons:
            raise ValueError(f"approved native task fails current admission: {name}: {reasons}")
        records.append({"task_id": rows[name]["id"], "name": name, "source_checksum": rows[name]["checksum"],
                        "native_overlay": name in overlays, "native_admission_blockers": sorted(set(reasons)),
                        "cpu_arch": task.environment.cpu_arch})
    output.mkdir(parents=True)
    tasks_root = output / "tasks"
    tasks_root.mkdir()
    for record in records:
        name = record["name"]
        source = overlays.get(name, sources[name])
        target = tasks_root / name
        shutil.copytree(source, target, copy_function=shutil.copy2)
        if name not in overlays:
            task_file = target / "task.toml"
            task_file.write_bytes(x86_task_toml(task_file.read_bytes().decode()).encode())
        record["prepared_checksum"] = task_checksum(target)
        record["source_relative_files_and_modes_preserved"] = all(
            (p.read_bytes() == (target / p.relative_to(source)).read_bytes()
             and (p.stat().st_mode & 0o777) == ((target / p.relative_to(source)).stat().st_mode & 0o777))
            for p in source.rglob("*") if p.is_file() and p.name != "task.toml"
        )
    (output / "benchmark.toml").write_text(tomli_w.dumps({
        "schema_version": 1, "id": BENCHMARK_ID,
        "display_name": catalog["benchmark"]["display_name"], "series": "terminal-bench",
        "license_spdx": "Apache-2.0", "source_subdir": "tasks",
    }))
    validate_local_benchmark(output)
    report = {"benchmark_id": BENCHMARK_ID, "task_count": 90, "cpu_arch": "x86_64",
              "license_spdx": "Apache-2.0", "candidate_sha": profile.candidate_sha,
              "native_admission_compatible_count": sum(not r["native_admission_blockers"] for r in records),
              "remaining_blocker_counts": dict(Counter(x for r in records for x in r["native_admission_blockers"])),
              "sources": {"root": str(source_root), "catalog_metadata": str(catalog_metadata),
                          "overlay_roots": [str(p) for p in overlay_roots], "overlay_metadata": str(overlay_metadata)},
              "publication_performed": False, "runtime_acceptance_performed": False, "tasks": records}
    (output / "migration-report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--catalog-metadata", type=Path, required=True)
    parser.add_argument("--native-overlay", type=Path, action="append", required=True)
    parser.add_argument("--overlay-metadata", type=Path, required=True)
    parser.add_argument("--runtime-profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = prepare(source_root=args.source_root, catalog_metadata=args.catalog_metadata,
                     overlay_roots=args.native_overlay, overlay_metadata=args.overlay_metadata,
                     profile_path=args.runtime_profile, output=args.output)
    print(json.dumps({k: v for k, v in report.items() if k not in {"tasks", "sources"}}))


if __name__ == "__main__":
    main()
