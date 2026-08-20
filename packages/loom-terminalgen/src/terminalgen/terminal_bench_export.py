from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Mapping

from terminalgen.constants import (
    DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY,
    DEFAULT_BASE_IMAGE,
    DEFAULT_ENVIRONMENT_BUILD_TIMEOUT_SEC,
    DEFAULT_MAX_SAME_TASK_NAMES,
)
from terminalgen.models import DatasetTask

DEFAULT_AUTHOR_NAME = "terminalGen"
DEFAULT_AUTHOR_EMAIL = "noreply@terminalgen.local"
APT_MIRROR_SETUP = """RUN if [ -f /etc/apt/sources.list.d/debian.sources ]; then \\
      sed -i "s|http://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g; s|http://security.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|http://deb.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|https://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g; s|https://security.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|https://deb.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g" /etc/apt/sources.list.d/debian.sources; \\
    fi \\
 && if [ -f /etc/apt/sources.list ]; then \\
      sed -i "s|http://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g; s|http://security.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|http://deb.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|http://archive.ubuntu.com/ubuntu|https://mirrors.tuna.tsinghua.edu.cn/ubuntu|g; s|http://security.ubuntu.com/ubuntu|https://mirrors.tuna.tsinghua.edu.cn/ubuntu|g; s|https://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g; s|https://security.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|https://deb.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g; s|https://archive.ubuntu.com/ubuntu|https://mirrors.tuna.tsinghua.edu.cn/ubuntu|g; s|https://security.ubuntu.com/ubuntu|https://mirrors.tuna.tsinghua.edu.cn/ubuntu|g" /etc/apt/sources.list; \\
    fi"""


class DuplicateTaskNameLimitError(ValueError):
    pass


def export_tasks(
    output_dir: Path,
    tasks: list[DatasetTask],
    *,
    base_image: str = DEFAULT_BASE_IMAGE,
    agent_timeout_by_difficulty: Mapping[str, float] | None = None,
    max_same_task_names: int = DEFAULT_MAX_SAME_TASK_NAMES,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()
    for task in tasks:
        export_task(
            output_dir,
            task,
            used_names=used_names,
            base_image=base_image,
            agent_timeout_by_difficulty=agent_timeout_by_difficulty,
            max_same_task_names=max_same_task_names,
        )


def export_task(
    output_dir: Path,
    task: DatasetTask,
    *,
    used_names: set[str],
    base_image: str = DEFAULT_BASE_IMAGE,
    agent_timeout_by_difficulty: Mapping[str, float] | None = None,
    max_same_task_names: int = DEFAULT_MAX_SAME_TASK_NAMES,
) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    if max_same_task_names <= 0:
        raise ValueError("max_same_task_names must be > 0")
    task_dir = _task_output_dir(
        output_dir,
        task,
        used_names,
        max_same_task_names=max_same_task_names,
    )
    used_names.add(task_dir.relative_to(output_dir).as_posix())
    _export_task(
        task_dir,
        task,
        base_image=base_image,
        agent_timeout_by_difficulty=agent_timeout_by_difficulty,
    )
    return task_dir.relative_to(output_dir).as_posix()


def prune_tasks(
    output_dir: Path,
    *,
    keep_task_dirs: set[str],
    preserve_dirs: set[str] | None = None,
) -> None:
    if not output_dir.exists():
        return
    preserved = preserve_dirs or set()
    keep_paths = {Path(task_dir) for task_dir in keep_task_dirs}
    for path in output_dir.iterdir():
        if not path.is_dir():
            continue
        if path.name in preserved:
            continue
        _prune_dir(path, output_dir=output_dir, keep_paths=keep_paths)


def _task_base_dir_name(task: DatasetTask) -> str:
    base_name = task.stable_id or "task"
    return base_name.replace("-", "").replace("_", "")

def _unique_task_dir_name(
    task: DatasetTask,
    used_names: set[str],
    *,
    max_same_task_names: int,
) -> str:
    dir_name = _task_base_dir_name(task)
    sibling_matches = [
        name
        for name in used_names
        if _is_same_task_dir_name(name, dir_name)
    ]
    if len(sibling_matches) >= max_same_task_names:
        raise DuplicateTaskNameLimitError(
            f"task name {dir_name!r} already reached the maximum of {max_same_task_names}"
        )
    if dir_name not in used_names:
        return dir_name

    suffix = 2
    while f"{dir_name}{suffix}" in used_names:
        suffix += 1
    return f"{dir_name}{suffix}"


def _is_same_task_dir_name(name: str, base_name: str) -> bool:
    if name == base_name:
        return True
    if not name.startswith(base_name):
        return False
    suffix = name[len(base_name) :]
    return suffix.isdigit() and int(suffix) >= 2


def _task_output_dir(
    output_dir: Path,
    task: DatasetTask,
    used_names: set[str],
    *,
    max_same_task_names: int,
) -> Path:
    category_dir = _task_category_dir(task)
    relative_parent = Path(category_dir) if category_dir else Path()
    sibling_names = {
        path.name for name in used_names for path in [Path(name)] if path.parent == relative_parent
    }
    task_dir_name = _unique_task_dir_name(
        task,
        sibling_names,
        max_same_task_names=max_same_task_names,
    )
    return output_dir / relative_parent / task_dir_name


def _task_category_dir(task: DatasetTask) -> str | None:
    domain = str(task.extra.get("domain", "")).strip()
    return domain or None


def _prune_dir(current_dir: Path, *, output_dir: Path, keep_paths: set[Path]) -> None:
    relative_dir = current_dir.relative_to(output_dir)
    if relative_dir in keep_paths:
        return
    if not any(relative_dir in keep_path.parents for keep_path in keep_paths):
        shutil.rmtree(current_dir)
        return

    for child in current_dir.iterdir():
        if not child.is_dir():
            continue
        _prune_dir(child, output_dir=output_dir, keep_paths=keep_paths)

    if not any(current_dir.iterdir()):
        current_dir.rmdir()


def _task_dir_suffix(task: DatasetTask) -> str:
    payload = json.dumps(task.to_jsonable(), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8]


def _export_task(
    task_dir: Path,
    task: DatasetTask,
    *,
    base_image: str,
    agent_timeout_by_difficulty: Mapping[str, float] | None,
) -> None:
    if task_dir.exists():
        shutil.rmtree(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)

    environment_dir = task_dir / "environment"
    tests_dir = task_dir / "tests"
    environment_dir.mkdir(parents=True, exist_ok=True)
    tests_dir.mkdir(parents=True, exist_ok=True)

    _write_environment_files(environment_dir, task, base_image=base_image)
    _write_instruction(task_dir, task)
    _write_tests(tests_dir, task)
    _write_solution(task_dir, task)
    _write_task_toml(task_dir, task, agent_timeout_by_difficulty=agent_timeout_by_difficulty)


def _write_environment_files(environment_dir: Path, task: DatasetTask, *, base_image: str) -> None:
    dockerfile_path = environment_dir / "Dockerfile"
    dockerfile_path.write_text(_render_dockerfile(task, base_image=base_image), encoding="utf-8")
    workspace_files_dir = environment_dir / "files"
    workspace_files_dir.mkdir(parents=True, exist_ok=True)

    if task.workspace_dir is not None:
        _copy_workspace_tree(task.workspace_dir, workspace_files_dir)
        return

    directory_placeholders = task.directory_placeholder_names()
    for task_file in task.files:
        path = workspace_files_dir / task_file.name
        if task_file.name in directory_placeholders and task_file.context == "":
            path.mkdir(parents=True, exist_ok=True)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.is_dir():
            raise ValueError(f"task file path resolves to a directory: {task_file.name}")
        path.write_text(task_file.context, encoding="utf-8")


def _copy_workspace_tree(source_dir: Path, target_dir: Path) -> None:
    if not source_dir.is_dir():
        raise ValueError(f"workspace_dir must be a directory: {source_dir}")
    source_root = source_dir.resolve()
    for source_path in source_dir.rglob("*"):
        if source_path.is_symlink():
            raise ValueError(f"workspace_dir cannot contain symlinks: {source_path}")
        resolved = source_path.resolve()
        try:
            relative_path = resolved.relative_to(source_root)
        except ValueError as exc:
            raise ValueError(f"workspace_dir path escapes source root: {source_path}") from exc
        target_path = target_dir / relative_path
        if source_path.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
        elif source_path.is_file():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
        else:
            raise ValueError(f"workspace_dir can only contain files and directories: {source_path}")


def _render_dockerfile(task: DatasetTask, *, base_image: str) -> str:
    return (
        f"FROM {base_image}\n\n"
        f"{APT_MIRROR_SETUP}\n\n"
        "WORKDIR /app\n\n"
        "COPY files/ /app/\n"
    )


def _write_instruction(task_dir: Path, task: DatasetTask) -> None:
    task_dir.joinpath("instruction.md").write_text(f"{task.prompt}\n", encoding="utf-8")


def _write_tests(tests_dir: Path, task: DatasetTask) -> None:
    tests_dir.joinpath("test_outputs.py").write_text(f"{task.tests}\n", encoding="utf-8")
    requirements = "\n".join(task.test_requirements).strip()
    tests_dir.joinpath("requirements.txt").write_text(
        f"{requirements}\n" if requirements else "",
        encoding="utf-8",
    )
    test_sh_path = tests_dir / "test.sh"
    test_sh_path.write_text(_render_test_sh(), encoding="utf-8")
    test_sh_path.chmod(0o755)


def _write_solution(task_dir: Path, task: DatasetTask) -> None:
    if task.solution is None:
        return
    solution_dir = task_dir / "solution"
    solution_dir.mkdir(parents=True, exist_ok=True)
    solution_path = solution_dir / "solve.sh"
    solution_path.write_text(f"{task.solution}\n", encoding="utf-8")
    solution_path.chmod(0o755)


def _render_test_sh() -> str:
    return """#!/bin/bash
set -uo pipefail

REQUIREMENTS_FILE="/tests/requirements.txt"
TARGET_TEST_PATH="/app/.terminus2_tests/terminus2_test_task.py"

if [ -s "$REQUIREMENTS_FILE" ]; then
  python -m pip install --disable-pip-version-check -r "$REQUIREMENTS_FILE" || {
    echo 0 > /logs/verifier/reward.txt
    exit 1
  }
fi

cd /app || {
  echo 0 > /logs/verifier/reward.txt
  exit 1
}

mkdir -p "$(dirname "$TARGET_TEST_PATH")" || {
  echo 0 > /logs/verifier/reward.txt
  exit 1
}

cp /tests/test_outputs.py "$TARGET_TEST_PATH" || {
  echo 0 > /logs/verifier/reward.txt
  exit 1
}

python -m pytest -q "$TARGET_TEST_PATH"
status=$?

if [ $status -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi

exit $status
"""


def _toml_basic_string(value: str) -> str:
    escapes = {
        "\\": "\\\\",
        '"': '\\"',
        "\b": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\f": "\\f",
        "\r": "\\r",
    }
    escaped_chars: list[str] = []
    for char in value:
        if char in escapes:
            escaped_chars.append(escapes[char])
            continue
        codepoint = ord(char)
        if codepoint < 0x20 or codepoint == 0x7F:
            escaped_chars.append(f"\\u{codepoint:04X}")
            continue
        escaped_chars.append(char)
    return f'"{"".join(escaped_chars)}"'


def _toml_string_array(values: list[str]) -> str:
    items = ", ".join(_toml_basic_string(value) for value in values)
    return f"[ {items},]"


def _write_task_toml(
    task_dir: Path,
    task: DatasetTask,
    *,
    agent_timeout_by_difficulty: Mapping[str, float] | None = None,
) -> None:
    difficulty = str(task.extra.get("difficulty", "medium")).strip().lower() or "medium"
    if difficulty not in {"medium", "hard", "expert"}:
        difficulty = "medium"
    agent_timeout_sec = _resolve_agent_timeout_by_difficulty(agent_timeout_by_difficulty)[difficulty]
    domain = str(task.extra.get("domain", "synthetic_terminal_tasks")).strip() or "synthetic_terminal_tasks"
    generation_mode = str(task.extra.get("generation_mode", "generated")).strip() or "generated"
    skills = [
        str(skill).strip()
        for skill in task.extra.get("skills", [])
        if str(skill).strip()
    ]
    category = domain.replace("_", "-")
    tags = [
        "synthetic",
        f"domain:{domain}",
        f"generation-mode:{generation_mode}",
    ]
    if skills:
        tags.append(f"skill-count:{len(skills)}")

    task_toml = (
        f"version = {_toml_basic_string('1.0')}\n\n"
        "[metadata]\n"
        f"author_name = {_toml_basic_string(DEFAULT_AUTHOR_NAME)}\n"
        f"author_email = {_toml_basic_string(DEFAULT_AUTHOR_EMAIL)}\n"
        f"difficulty = {_toml_basic_string(difficulty)}\n"
        f"category = {_toml_basic_string(category)}\n"
        f"tags = {_toml_string_array(tags)}\n\n"
        "[verifier]\n"
        "timeout_sec = 1200.0\n\n"
        "[agent]\n"
        f"timeout_sec = {agent_timeout_sec:.1f}\n\n"
        "[environment]\n"
        f"build_timeout_sec = {DEFAULT_ENVIRONMENT_BUILD_TIMEOUT_SEC:.1f}\n"
        "cpus = 2\n"
        f"memory = {_toml_basic_string('4G')}\n"
        f"storage = {_toml_basic_string('10G')}\n"
    )
    task_dir.joinpath("task.toml").write_text(task_toml, encoding="utf-8")


def _resolve_agent_timeout_by_difficulty(
    agent_timeout_by_difficulty: Mapping[str, float] | None,
) -> dict[str, float]:
    resolved = dict(DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY)
    if agent_timeout_by_difficulty is None:
        return resolved

    for difficulty, timeout_sec in agent_timeout_by_difficulty.items():
        if difficulty not in resolved:
            raise ValueError(f"unsupported difficulty for agent timeout: {difficulty}")
        if timeout_sec <= 0:
            raise ValueError(f"agent timeout must be > 0 for difficulty {difficulty}")
        resolved[difficulty] = float(timeout_sec)
    return resolved
