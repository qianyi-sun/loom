"""SkillFlow benchmark adapter. Spec §5.2 row 12.

The converter accepts both the legacy per-instance `manifest.json`
fixtures used by early Loom tests and the official task-bundle layout
published by SkillFlow / SkillLearnBench. Official bundles already ship
task instructions, Dockerfiles, and upstream `tests/test.sh`; this
adapter wraps them in Loom's `TaskConfig` and script-verifier contract.
"""

from __future__ import annotations

import json
import re
import shutil
import textwrap
import tomllib
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

_SAFE_INSTANCE_SEGMENT_RE = re.compile(r"[^A-Za-z0-9._+\-]")
_SKILLFLOW_BASE_IMAGE = "skillflow/harbor-cli-base:ubuntu24.04"
_UNPUBLISHED_HARBOR_BASE_IMAGES = {
    "skillevlove/harbor-cli-openhands:ubuntu24.04",
}
_SOLUTION_ROOT_HEADER = (
    'LOOM_TASK_ROOT="${LOOM_TASK_ROOT:-$(CDPATH= cd -- "$(dirname "$0")" '
    '&& pwd)}"\n'
    'if [ "$(basename "$LOOM_TASK_ROOT")" = "solution" ]; then\n'
    '    LOOM_TASK_ROOT="$(CDPATH= cd -- "$LOOM_TASK_ROOT/.." && pwd)"\n'
    "fi\n"
)


def _normalize_required_artifact_pattern(value: str, *, workdir: str) -> str | None:
    pattern = value.strip()
    if not pattern or "\x00" in pattern:
        return None

    workdir_prefix = workdir.rstrip("/") + "/"
    if pattern.startswith(workdir_prefix):
        pattern = pattern[len(workdir_prefix):]
    elif pattern == workdir.rstrip("/"):
        return None
    elif pattern.startswith("/"):
        pattern = pattern.lstrip("/")

    parts = [part for part in pattern.split("/") if part]
    if not parts or any(part == ".." for part in parts):
        return None
    return "/".join(parts)


class SkillFlowAdapter(CatalogBackedAdapter):
    name = "skillflow"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for path in sorted(self._manifest_roots(source_dir)):
            manifest = cast(dict[str, Any], json.loads(path.read_text()))
            yield BenchmarkInstance(
                instance_id=str(manifest["instance_id"]),
                split=split, raw=manifest,
            )
        for root in self._bundle_roots(source_dir):
            for task_toml in sorted(root.rglob("task.toml")):
                bundle_dir = task_toml.parent
                raw = cast(dict[str, Any], tomllib.loads(task_toml.read_text()))
                yield BenchmarkInstance(
                    instance_id=self._safe_instance_id(root, bundle_dir),
                    split=split,
                    raw={"__source_path": str(bundle_dir), "task_toml": raw},
                )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        if "__source_path" in instance.raw:
            return self._convert_bundle_instance(
                instance, out_dir=out_dir, task_id=task_id,
            )

        files: dict[str, str] = instance.raw["files"]
        for rel, body in files.items():
            target = out_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body)

        # Re-stamp task.toml's `id` field to the namespaced form.
        # Upstream bundles use the bare instance_id (which already
        # matches the namespaced form for our v1 bundles), but force
        # consistency so future upstream changes can't drift.
        toml_path = out_dir / "task.toml"
        if toml_path.exists():
            old = toml_path.read_text()
            # Replace any `id = "..."` value under [task]; cheap line-by-line
            # walk avoids needing tomli-w.
            new_lines: list[str] = []
            in_task = False
            replaced = False
            for line in old.splitlines():
                stripped = line.strip()
                if stripped.startswith("[task]"):
                    in_task = True
                elif stripped.startswith("[") and stripped != "[task]":
                    in_task = False
                if in_task and stripped.startswith("id =") and not replaced:
                    line = f"id = {toml_string(task_id)}"
                    replaced = True
                new_lines.append(line)
            toml_path.write_text("\n".join(new_lines) + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )

    @staticmethod
    def _manifest_roots(source_dir: Path) -> Iterator[Path]:
        for base in (source_dir / "repo" / "tasks", source_dir / "tasks"):
            if base.is_dir():
                yield from base.rglob("manifest.json")

    @staticmethod
    def _bundle_roots(source_dir: Path) -> Iterator[Path]:
        for base in (
            source_dir / "repo" / "test_tasks",
            source_dir / "test_tasks",
            source_dir / "repo" / "tasks",
            source_dir / "tasks",
        ):
            if base.is_dir() and not any(base.rglob("manifest.json")):
                yield base

    @staticmethod
    def _safe_instance_id(root: Path, bundle_dir: Path) -> str:
        parts: list[str] = []
        for part in bundle_dir.relative_to(root).parts:
            safe = _SAFE_INSTANCE_SEGMENT_RE.sub("_", part)
            if safe in {"", ".", ".."}:
                safe = "task"
            parts.append(safe)
        return "/".join(parts)

    def _convert_bundle_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
        task_id: str,
    ) -> ConvertedTask:
        source_path = Path(str(instance.raw["__source_path"]))
        self._copy_bundle(source_path, out_dir)
        self._rewrite_unpublished_base_images(out_dir)
        self._rewrite_absolute_solution_paths(out_dir)
        if self._dockerfile_uses_root_build_context(out_dir):
            self._mirror_environment_copy_sources_for_root_context(out_dir)
            skills_dir = out_dir / "skills"
            skills_dir.mkdir(exist_ok=True)
            (skills_dir / ".keep").touch()
        self._write_loom_task_toml(instance, out_dir=out_dir, task_id=task_id)
        self._write_reward_verifier(out_dir)
        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )

    @staticmethod
    def _copy_bundle(source_path: Path, out_dir: Path) -> None:
        def ignore(_dir: str, names: list[str]) -> set[str]:
            return {
                name for name in names
                if name in {".DS_Store", "__pycache__"} or name.endswith(".pyc")
            }

        for child in source_path.iterdir():
            target = out_dir / child.name
            if child.is_symlink():
                continue
            if child.is_dir():
                shutil.copytree(child, target, ignore=ignore)
            else:
                shutil.copy2(child, target)

    @staticmethod
    def _rewrite_unpublished_base_images(out_dir: Path) -> None:
        dockerfile = out_dir / "environment" / "Dockerfile"
        if not dockerfile.exists():
            return

        lines = dockerfile.read_text().splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped.startswith("FROM "):
                continue
            parts = stripped.split()
            if len(parts) >= 2 and parts[1] in _UNPUBLISHED_HARBOR_BASE_IMAGES:
                lines[idx] = line.replace(parts[1], _SKILLFLOW_BASE_IMAGE, 1)
                dockerfile.write_text("\n".join(lines) + "\n")
            return

    @staticmethod
    def _rewrite_absolute_solution_paths(out_dir: Path) -> None:
        solve_sh = out_dir / "solution" / "solve.sh"
        if not solve_sh.exists():
            return

        old = solve_sh.read_text()
        if "/solution/" not in old:
            return

        new = old.replace("/solution/", "${LOOM_TASK_ROOT}/solution/")
        if "LOOM_TASK_ROOT=" not in new:
            lines = new.splitlines()
            insert_at = 1 if lines and lines[0].startswith("#!") else 0
            lines[insert_at:insert_at] = _SOLUTION_ROOT_HEADER.splitlines()
            new = "\n".join(lines) + ("\n" if old.endswith("\n") else "")
        solve_sh.write_text(new)

    @classmethod
    def _dockerfile_uses_root_build_context(cls, out_dir: Path) -> bool:
        dockerfile = out_dir / "environment" / "Dockerfile"
        if not dockerfile.exists():
            return False
        environment_dir = out_dir / "environment"
        for source in cls._dockerfile_copy_sources(dockerfile):
            normalized = source.strip("\"'").removeprefix("./")
            if normalized == "skills" or normalized.startswith("skills/"):
                return True
            if cls._copy_source_needs_root_context(
                normalized,
                out_dir=out_dir,
                environment_dir=environment_dir,
            ):
                return True
        return False

    @staticmethod
    def _dockerfile_copy_sources(dockerfile: Path) -> Iterator[str]:
        for line in dockerfile.read_text().splitlines():
            parts = line.strip().split()
            if not parts or parts[0] not in {"COPY", "ADD"}:
                continue
            sources = parts[1:-1]
            while sources and sources[0].startswith("--"):
                sources = sources[1:]
            yield from sources

    @staticmethod
    def _copy_source_needs_root_context(
        source: str,
        *,
        out_dir: Path,
        environment_dir: Path,
    ) -> bool:
        if (
            not source
            or source.startswith("/")
            or "://" in source
            or source.startswith("$")
        ):
            return False
        env_matches = list(environment_dir.glob(source))
        if env_matches:
            return False
        root_matches = list(out_dir.glob(source))
        return bool(root_matches)

    @classmethod
    def _mirror_environment_copy_sources_for_root_context(
        cls,
        out_dir: Path,
    ) -> None:
        dockerfile = out_dir / "environment" / "Dockerfile"
        environment_dir = out_dir / "environment"
        if not dockerfile.exists() or not environment_dir.is_dir():
            return

        for source in cls._dockerfile_copy_sources(dockerfile):
            normalized = source.strip("\"'").removeprefix("./")
            if (
                not normalized
                or normalized.startswith("/")
                or "://" in normalized
                or normalized.startswith("$")
            ):
                continue
            for env_path in environment_dir.glob(normalized):
                relative = env_path.relative_to(environment_dir)
                target = out_dir / relative
                if target.exists():
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                if env_path.is_dir():
                    shutil.copytree(env_path, target)
                else:
                    shutil.copy2(env_path, target)

    def _write_loom_task_toml(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
        task_id: str,
    ) -> None:
        raw_task = cast(dict[str, Any], instance.raw.get("task_toml") or {})
        if not raw_task:
            copied_task_toml = out_dir / "task.toml"
            if copied_task_toml.exists():
                raw_task = cast(
                    dict[str, Any],
                    tomllib.loads(copied_task_toml.read_text()),
                )
        task_meta = (
            raw_task.get("task")
            if isinstance(raw_task.get("task"), dict)
            else {}
        )
        name = str(task_meta.get("name") or instance.instance_id)
        dockerfile = out_dir / "environment" / "Dockerfile"
        environment_lines = [
            "[environment]",
            'os = "linux"',
            'workdir = "/root"',
            'user = "root"',
            "build_timeout_sec = 1800",
        ]
        cpu_arch = self._task_cpu_arch()
        if cpu_arch is not None:
            environment_lines.append(f"cpu_arch = {toml_string(cpu_arch)}")
        if dockerfile.exists():
            build_context = (
                "."
                if self._dockerfile_uses_root_build_context(out_dir)
                else "environment"
            )
            environment_lines.extend([
                'dockerfile = "environment/Dockerfile"',
                f'docker_build_context = "{build_context}"',
            ])
        else:
            environment_lines.append('docker_image = "python:3.11-slim"')

        environment_block = "\n".join(environment_lines)
        display_name = f"{self.display_name} - {name}"
        required_artifacts = self._required_artifacts(raw_task, workdir="/root")
        required_artifacts_block = ""
        if required_artifacts:
            required_artifacts_block = (
                "\n            required_artifacts = [\n"
                + "".join(
                    f"              {toml_string(pattern)},\n"
                    for pattern in required_artifacts
                )
                + "            ]"
            )
        (out_dir / "task.toml").write_text(textwrap.dedent(f"""
            schema_version = "1"

            [task]
            id = {toml_string(task_id)}
            name = {toml_string(display_name)}

            {environment_block}

            [agent]
            name = "oracle"
            timeout_sec = 1800

            [verifier]
            name = "script"
            timeout_sec = 900
            user = "root"

            [verifier.args]
            script_path = "/root/verifier/run.sh"

            [[steps]]
            name = "main"
            artifacts = [
              "*.csv",
              "*.docx",
              "*.json",
              "*.md",
              "*.pdf",
              "*.png",
              "*.txt",
              "*.xlsx",
            ]{required_artifacts_block}
        """).strip() + "\n")

    @staticmethod
    def _required_artifacts(raw_task: dict[str, Any], *, workdir: str) -> list[str]:
        evaluation = raw_task.get("evaluation")
        if not isinstance(evaluation, dict):
            return []
        raw_required = evaluation.get("required_files")
        if not isinstance(raw_required, list):
            return []

        normalized: list[str] = []
        seen: set[str] = set()
        for item in raw_required:
            if not isinstance(item, str):
                continue
            pattern = _normalize_required_artifact_pattern(item, workdir=workdir)
            if pattern is None or pattern in seen:
                continue
            seen.add(pattern)
            normalized.append(pattern)
        return normalized

    def _task_cpu_arch(self) -> str | None:
        """Return explicit catalog-declared task CPU compatibility.

        Omitted means preserve the TaskConfig default (`x86_64`). Adapters must
        opt in via catalog metadata before emitting `any` or `arm64`.
        """
        cpu_arch = self._params.get("cpu_arch")
        if cpu_arch is None:
            return None
        if cpu_arch not in {"x86_64", "arm64", "any"}:
            raise ValueError(
                f"{self.name} params.cpu_arch must be x86_64, arm64, or any",
            )
        return cpu_arch

    @staticmethod
    def _write_reward_verifier(out_dir: Path) -> None:
        structured_verifier_script(
            r'''
TASK_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
mkdir -p /tests /logs/verifier "$(dirname "$LOOM_VERIFIER_OUTPUT")"
rm -f /logs/verifier/reward.txt
if [ -d "$TASK_DIR/tests" ]; then
    cp -R "$TASK_DIR/tests/." /tests/
fi
cd "$TASK_DIR"
verifier_rc=0
if [ -f "$TASK_DIR/tests/test.sh" ]; then
    if bash "$TASK_DIR/tests/test.sh"; then
        verifier_rc=0
    else
        verifier_rc=$?
    fi
else
    verifier_rc=127
    echo 0 > /logs/verifier/reward.txt
fi
python3 - "$LOOM_VERIFIER_OUTPUT" "$verifier_rc" <<'PY'
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
verifier_rc = int(sys.argv[2])
reward_path = Path("/logs/verifier/reward.txt")
raw = reward_path.read_text().strip() if reward_path.exists() else "0"
output_log_path = Path("/logs/verifier/output.log")
output_log_tail = None
if output_log_path.exists():
    output_log_tail = output_log_path.read_text(
        encoding="utf-8",
        errors="replace",
    )[-4000:]
try:
    score = float(raw)
except ValueError:
    score = 0.0
passed = score > 0.0 and verifier_rc == 0
out.write_text(json.dumps({
    "rewards": {"score": score},
    "checks": [
        {
            "name": "upstream_tests",
            "passed": passed,
            "score": score,
            "message": f"test.sh rc={verifier_rc}; reward={raw}",
        }
    ],
    "structured": {
        "reward_raw": raw,
        "test_sh_returncode": verifier_rc,
        "output_log_tail": output_log_tail,
    },
}))
PY
''',
            out_dir=out_dir,
        )
