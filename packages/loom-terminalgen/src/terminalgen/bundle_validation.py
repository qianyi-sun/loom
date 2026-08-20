from __future__ import annotations

import ast
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from terminalgen.models import prompt_test_leakage_matches

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib


GENERATION_MANIFEST_NAME = "bundle_generation_manifest.jsonl"
VALIDATION_RESULTS_NAME = "validation_results.jsonl"
VALIDATION_SUMMARY_NAME = "validation_summary.json"
CHECKSUMS_NAME = "SHA256SUMS"
ATOMIC_BATCH_CONTRACT_NAME = "atomic_batch_contract.json"


class BundleValidationResult(BaseModel):
    task_path: str
    task_id: str
    sha256: str
    static_passed: bool
    docker_executed: bool = False
    docker_build_passed: bool | None = None
    baseline_reward: int | None = None
    solution_rewards: list[int] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    provenance: dict[str, Any] = Field(default_factory=dict)

    @property
    def passed(self) -> bool:
        if self.errors or not self.static_passed:
            return False
        if not self.docker_executed:
            return True
        return (
            self.docker_build_passed is True
            and self.baseline_reward == 0
            and bool(self.solution_rewards)
            and all(reward == 1 for reward in self.solution_rewards)
        )


def write_generation_manifest(output_path: Path, tasks: list[Any]) -> Path:
    rows: list[dict[str, Any]] = []
    for task in sorted(tasks, key=lambda item: str(item.extra.get("bundle_path", ""))):
        bundle_path = str(task.extra.get("bundle_path", ""))
        if not bundle_path:
            continue
        task_root = output_path / bundle_path
        rows.append(
            {
                "task_id": task.stable_id,
                "bundle_path": bundle_path,
                "source_task": task.extra.get("source_task"),
                "capability_id": task.extra.get("capability_id"),
                "variant_bucket": task.extra.get("variant_bucket"),
                "variant_index": task.extra.get("variant_index"),
                "template_family_id": task.extra.get("template_family_id"),
                "difficulty": task.extra.get("difficulty"),
                "generation_mode": task.extra.get("generation_mode"),
                "generation_attempt": task.extra.get("attempt_index"),
                "sha256": task_tree_sha256(task_root),
            }
        )
    path = output_path / GENERATION_MANIFEST_NAME
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    return path


def write_atomic_batch_contract(output_path: Path, requests: list[Any]) -> Path:
    expected_slots = [
        {
            "template_family_id": request.template_family_id,
            "source_task": request.atomic_card.source_task if request.atomic_card else None,
            "capability_id": request.atomic_card.capability_id if request.atomic_card else None,
            "variant_bucket": request.variant_bucket.value if request.variant_bucket else None,
            "variant_index": request.variant_index,
            "domain": request.domain,
            "difficulty": request.difficulty,
        }
        for request in requests
    ]
    payload = {
        "format_version": "1.0",
        "generation_mode": "atomic-target",
        "expected_task_count": len(expected_slots),
        "expected_slots": expected_slots,
    }
    path = output_path / ATOMIC_BATCH_CONTRACT_NAME
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def validate_bundle_tree(
    tasks_root: Path,
    *,
    run_docker: bool = False,
    platform: str = "linux/arm64",
    solution_repetitions: int = 2,
    docker_timeout_sec: float = 1800.0,
) -> list[BundleValidationResult]:
    if solution_repetitions <= 0:
        raise ValueError("solution_repetitions must be > 0")
    if docker_timeout_sec <= 0:
        raise ValueError("docker_timeout_sec must be > 0")
    task_roots = sorted(path.parent for path in tasks_root.rglob("task.toml"))
    provenance = _load_generation_manifest(tasks_root)
    results = [
        validate_task_bundle(
            task_root,
            root=tasks_root,
            provenance=provenance.get(task_root.relative_to(tasks_root).as_posix(), {}),
            run_docker=run_docker,
            platform=platform,
            solution_repetitions=solution_repetitions,
            docker_timeout_sec=docker_timeout_sec,
        )
        for task_root in task_roots
    ]
    batch_errors = _audit_atomic_batch_contract(tasks_root, results)
    if batch_errors and results:
        results[0].errors.extend(f"atomic batch contract: {error}" for error in batch_errors)
    _write_validation_reports(tasks_root, results)
    return results


def validate_task_bundle(
    task_root: Path,
    *,
    root: Path | None = None,
    provenance: dict[str, Any] | None = None,
    run_docker: bool = False,
    platform: str = "linux/arm64",
    solution_repetitions: int = 2,
    docker_timeout_sec: float = 1800.0,
) -> BundleValidationResult:
    report_root = root or task_root.parent
    relative_path = task_root.relative_to(report_root).as_posix()
    errors: list[str] = []
    warnings: list[str] = []
    parsed_toml: dict[str, Any] = {}
    provenance = provenance or {}
    task_id = str(provenance.get("task_id") or task_root.name)

    required_files = [
        task_root / "instruction.md",
        task_root / "task.toml",
        task_root / "environment" / "Dockerfile",
        task_root / "tests" / "test.sh",
        task_root / "tests" / "test_outputs.py",
        task_root / "tests" / "requirements.txt",
        task_root / "solution" / "solve.sh",
    ]
    for path in required_files:
        if not path.is_file():
            errors.append(f"missing required file: {path.relative_to(task_root)}")
    files_dir = task_root / "environment" / "files"
    if not files_dir.is_dir():
        errors.append("missing required directory: environment/files")

    for path in task_root.rglob("*"):
        if path.is_symlink():
            errors.append(f"symlink is not allowed: {path.relative_to(task_root)}")

    task_toml_path = task_root / "task.toml"
    if task_toml_path.is_file():
        try:
            parsed_toml = tomllib.loads(task_toml_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"invalid task.toml: {exc}")
        else:
            _validate_task_toml(parsed_toml, errors)

    instruction_path = task_root / "instruction.md"
    if instruction_path.is_file():
        instruction = instruction_path.read_text(encoding="utf-8", errors="replace").strip()
        if not instruction:
            errors.append("instruction.md is empty")
        if not instruction.startswith("Work in /app."):
            errors.append("instruction.md must start with 'Work in /app.'")
        leakage = prompt_test_leakage_matches(instruction)
        if leakage:
            errors.append(f"instruction leaks verifier terminology: {', '.join(leakage)}")

    tests_path = task_root / "tests" / "test_outputs.py"
    if tests_path.is_file():
        try:
            ast.parse(tests_path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            errors.append(f"tests/test_outputs.py has invalid syntax: {exc}")

    test_sh = task_root / "tests" / "test.sh"
    if test_sh.is_file():
        test_text = test_sh.read_text(encoding="utf-8", errors="replace")
        if "/logs/verifier/reward.txt" not in test_text:
            errors.append("tests/test.sh does not write /logs/verifier/reward.txt")
        if "pytest" not in test_text:
            errors.append("tests/test.sh does not execute pytest")
        if not test_sh.stat().st_mode & 0o111:
            errors.append("tests/test.sh is not executable")

    solution_path = task_root / "solution" / "solve.sh"
    if solution_path.is_file():
        solution_text = solution_path.read_text(encoding="utf-8", errors="replace").strip()
        if not solution_text:
            errors.append("solution/solve.sh is empty")
        if not solution_path.stat().st_mode & 0o111:
            errors.append("solution/solve.sh is not executable")
        if not solution_text.startswith("#!"):
            warnings.append("solution/solve.sh has no shebang")

    dockerfile_path = task_root / "environment" / "Dockerfile"
    if dockerfile_path.is_file():
        _validate_dockerfile(
            dockerfile_path.read_text(encoding="utf-8", errors="replace"),
            errors,
            warnings,
        )

    actual_sha256 = task_tree_sha256(task_root)
    expected_sha256 = provenance.get("sha256")
    if expected_sha256 is not None and expected_sha256 != actual_sha256:
        errors.append(
            "bundle content changed after generation manifest was written: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

    result = BundleValidationResult(
        task_path=relative_path,
        task_id=task_id,
        sha256=actual_sha256,
        static_passed=not errors,
        errors=errors,
        warnings=warnings,
        provenance=provenance,
    )
    if run_docker and result.static_passed:
        _run_docker_validation(
            task_root,
            result,
            platform=platform,
            solution_repetitions=solution_repetitions,
            timeout_sec=docker_timeout_sec,
        )
    return result


def task_tree_sha256(task_root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(candidate for candidate in task_root.rglob("*") if candidate.is_file()):
        relative = path.relative_to(task_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def _validate_task_toml(payload: dict[str, Any], errors: list[str]) -> None:
    if payload.get("version") != "1.0":
        errors.append("task.toml version must be '1.0'")
    for section in ("metadata", "verifier", "agent", "environment"):
        if not isinstance(payload.get(section), dict):
            errors.append(f"task.toml missing [{section}] section")
    for section, field in (
        ("verifier", "timeout_sec"),
        ("agent", "timeout_sec"),
        ("environment", "build_timeout_sec"),
    ):
        value = payload.get(section, {}).get(field)
        if not isinstance(value, (int, float)) or value <= 0:
            errors.append(f"task.toml {section}.{field} must be > 0")


def _validate_dockerfile(text: str, errors: list[str], warnings: list[str]) -> None:
    forbidden_patterns = {
        r"\bx86_64\b|\bamd64\b|linux_amd64|awscli-exe-linux-x86_64": "x86-only artifact",
        r"npm@latest": "unpinned npm@latest",
        r"--platform(?:\s*=\s*|\s+)linux/amd64": "forced linux/amd64 platform",
        r"/etc/resolv\.conf|/etc/nsswitch\.conf": "system DNS or NSS mutation",
        r"\|\|\s*true": "failure-masking '|| true'",
    }
    for pattern, label in forbidden_patterns.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            errors.append(f"Dockerfile contains incompatible pattern: {label}")
    if re.search(
        r"(?<!extra-)--index-url\s+https://download\.pytorch\.org",
        text,
        flags=re.IGNORECASE,
    ):
        errors.append("Dockerfile uses the PyTorch package index as the only index")
    if "COPY files/ /app/" not in text:
        errors.append("Dockerfile must copy environment/files to /app with 'COPY files/ /app/'")
    if "WORKDIR /app" not in text:
        errors.append("Dockerfile must set WORKDIR /app")
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    if not first_line.startswith("FROM "):
        errors.append("Dockerfile must start with FROM")
    elif ":latest" in first_line:
        warnings.append("base image uses mutable :latest tag; pin a digest for final production")


def _run_docker_validation(
    task_root: Path,
    result: BundleValidationResult,
    *,
    platform: str,
    solution_repetitions: int,
    timeout_sec: float,
) -> None:
    result.docker_executed = True
    if shutil.which("docker") is None:
        result.errors.append("docker executable not found")
        result.docker_build_passed = False
        return
    image_tag = f"terminalgen-validate:{result.sha256[:16]}"
    build_command = [
        "docker",
        "buildx",
        "build",
        "--load",
        "--platform",
        platform,
        "--tag",
        image_tag,
        str(task_root / "environment"),
    ]
    try:
        build = subprocess.run(
            build_command,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
        result.docker_build_passed = build.returncode == 0
        if build.returncode != 0:
            result.errors.append(f"Docker build failed: {_tail_output(build)}")
            return
        with tempfile.TemporaryDirectory(prefix="terminalgen-validate-") as temp_dir:
            logs_dir = Path(temp_dir) / "verifier"
            logs_dir.mkdir()
            baseline = _run_bundle_container(
                image_tag,
                task_root,
                logs_dir,
                solve=False,
                platform=platform,
                timeout_sec=timeout_sec,
            )
            result.baseline_reward = _read_reward(logs_dir)
            if baseline.returncode == 0:
                result.errors.append("unsolved baseline unexpectedly exited successfully")
            if result.baseline_reward != 0:
                result.errors.append(
                    f"unsolved baseline reward must be 0, got {result.baseline_reward!r}"
                )
            if not _pytest_executed(baseline.stdout + "\n" + baseline.stderr):
                result.errors.append("unsolved baseline did not execute a pytest test set")
            for _ in range(solution_repetitions):
                reward_path = logs_dir / "reward.txt"
                if reward_path.exists():
                    reward_path.unlink()
                solved = _run_bundle_container(
                    image_tag,
                    task_root,
                    logs_dir,
                    solve=True,
                    platform=platform,
                    timeout_sec=timeout_sec,
                )
                reward = _read_reward(logs_dir)
                result.solution_rewards.append(reward if reward is not None else -1)
                if solved.returncode != 0 or reward != 1:
                    result.errors.append(
                        f"reference solution failed reward={reward!r}: {_tail_output(solved)}"
                    )
                if not _pytest_executed(solved.stdout + "\n" + solved.stderr):
                    result.errors.append("reference solution did not execute a pytest test set")
    except subprocess.TimeoutExpired as exc:
        result.errors.append(f"Docker validation timed out after {exc.timeout} seconds")
    finally:
        subprocess.run(
            ["docker", "image", "rm", "-f", image_tag],
            text=True,
            capture_output=True,
            check=False,
        )


def _run_bundle_container(
    image_tag: str,
    task_root: Path,
    logs_dir: Path,
    *,
    solve: bool,
    platform: str,
    timeout_sec: float,
) -> subprocess.CompletedProcess[str]:
    command = [
        "docker",
        "run",
        "--rm",
        "--platform",
        platform,
        "--volume",
        f"{(task_root / 'tests').resolve()}:/tests:ro",
        "--volume",
        f"{logs_dir.resolve()}:/logs/verifier",
    ]
    if solve:
        command.extend(
            [
                "--volume",
                f"{(task_root / 'solution').resolve()}:/solution:ro",
            ]
        )
    command.append(image_tag)
    if solve:
        command.extend(["bash", "-lc", "bash /solution/solve.sh && bash /tests/test.sh"])
    else:
        command.extend(["bash", "/tests/test.sh"])
    return subprocess.run(
        command,
        text=True,
        capture_output=True,
        timeout=timeout_sec,
        check=False,
    )


def _read_reward(logs_dir: Path) -> int | None:
    path = logs_dir / "reward.txt"
    if not path.is_file():
        return None
    value = path.read_text(encoding="utf-8", errors="replace").strip()
    return int(value) if value in {"0", "1"} else None


def _pytest_executed(output: str) -> bool:
    return bool(re.search(r"\b\d+\s+(?:passed|failed|error|errors)\b", output))


def _tail_output(completed: subprocess.CompletedProcess[str]) -> str:
    output = (completed.stdout + "\n" + completed.stderr).strip()
    return output[-2_000:] if output else f"exit code {completed.returncode}"


def _load_generation_manifest(tasks_root: Path) -> dict[str, dict[str, Any]]:
    path = tasks_root / GENERATION_MANIFEST_NAME
    if not path.is_file():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        bundle_path = payload.get("bundle_path")
        if not isinstance(bundle_path, str) or not bundle_path:
            raise ValueError(f"invalid bundle_path in {path} line {line_number}")
        rows[bundle_path] = payload
    return rows


def _audit_atomic_batch_contract(
    tasks_root: Path,
    results: list[BundleValidationResult],
) -> list[str]:
    path = tasks_root / ATOMIC_BATCH_CONTRACT_NAME
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"invalid {ATOMIC_BATCH_CONTRACT_NAME}: {exc}"]
    expected_slots = payload.get("expected_slots")
    expected_count = payload.get("expected_task_count")
    if not isinstance(expected_slots, list) or not isinstance(expected_count, int):
        return [f"invalid schema in {ATOMIC_BATCH_CONTRACT_NAME}"]

    errors: list[str] = []
    if expected_count != len(expected_slots):
        errors.append(
            f"contract expected_task_count={expected_count} but lists {len(expected_slots)} slots"
        )
    if len(results) != expected_count:
        errors.append(f"task count is {len(results)}, expected {expected_count}")

    expected_ids = [slot.get("template_family_id") for slot in expected_slots]
    actual_ids = [result.provenance.get("template_family_id") for result in results]
    if any(not isinstance(value, str) or not value for value in expected_ids):
        errors.append("contract contains an invalid template_family_id")
    if any(not isinstance(value, str) or not value for value in actual_ids):
        errors.append("one or more bundles lack template_family_id provenance")
    expected_counter = Counter(expected_ids)
    actual_counter = Counter(actual_ids)
    if any(count != 1 for count in expected_counter.values()):
        errors.append("contract template_family_id values are not unique")
    if actual_counter != expected_counter:
        missing = list((expected_counter - actual_counter).elements())[:5]
        unexpected = list((actual_counter - expected_counter).elements())[:5]
        errors.append(
            "quota slots do not match contract "
            f"missing={missing} unexpected={unexpected}"
        )
    return errors


def _write_validation_reports(
    tasks_root: Path,
    results: list[BundleValidationResult],
) -> None:
    rows = [
        {**result.model_dump(), "passed": result.passed}
        for result in results
    ]
    (tasks_root / VALIDATION_RESULTS_NAME).write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    summary = {
        "task_count": len(results),
        "passed": sum(result.passed for result in results),
        "failed": sum(not result.passed for result in results),
        "docker_executed": sum(result.docker_executed for result in results),
        "static_passed": sum(result.static_passed for result in results),
    }
    (tasks_root / VALIDATION_SUMMARY_NAME).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (tasks_root / CHECKSUMS_NAME).write_text(
        "".join(f"{result.sha256}  {result.task_path}\n" for result in results),
        encoding="utf-8",
    )
