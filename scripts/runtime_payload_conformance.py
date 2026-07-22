#!/usr/bin/env python3
"""Run every manifest-owned runtime payload in an isolated policy container."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.component_ownership import (
    ExecutionCase,
    ExecutionPolicy,
    ManifestError,
    _tracked_paths,
    lane_execution_plan,
    load_manifest,
    validate_manifest,
)
from scripts.runtime_payload_dispatch import _declared_tests

_CONTAINER_TIMEOUT_SEC = 45
_IMAGE_PULL_TIMEOUT_SEC = 120


def _seed_case(case: ExecutionCase, root: Path) -> None:
    root.mkdir(parents=True, mode=0o755)
    for fixture in case.fixture_files:
        target = root / fixture.path
        target.parent.mkdir(parents=True, exist_ok=True)
        if fixture.size is not None:
            with target.open("wb") as handle:
                handle.truncate(fixture.size)
        else:
            target.write_text(fixture.content or "", encoding="utf-8")
        target.chmod(0o644)
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ManifestError(f"execution case fixture contains a symlink: {case.path}")


def _container_name(path: str) -> str:
    suffix = hashlib.sha256(path.encode("utf-8")).hexdigest()[:16]
    return f"loom-runtime-payload-{suffix}"


def _pull_image(*, repo_root: Path, image: str) -> None:
    completed = subprocess.run(
        ["docker", "pull", image],
        cwd=repo_root,
        text=True,
        capture_output=True,
        timeout=_IMAGE_PULL_TIMEOUT_SEC,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"failed to pull immutable runtime payload image: {image}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )


def _run_payload(
    *,
    repo_root: Path,
    policy: ExecutionPolicy,
    fixture_root: Path,
    path: str,
) -> dict[str, Any]:
    container_name = _container_name(path)
    dispatcher = repo_root / "scripts/runtime_payload_dispatch.py"
    test_file = repo_root / path
    for label, candidate in (("dispatcher", dispatcher), ("payload", test_file)):
        if candidate.is_symlink() or not candidate.is_file():
            raise ManifestError(f"runtime payload {label} is not a regular file: {candidate}")
        if not candidate.resolve().is_relative_to(repo_root.resolve()):
            raise ManifestError(f"runtime payload {label} escapes repository: {candidate}")
    command = [
        "docker",
        "run",
        "--rm",
        "--pull",
        "never",
        "--name",
        container_name,
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "64",
        "--memory",
        "256m",
        "--cpus",
        "1",
        "--user",
        "65534:65534",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,nodev,size=16m",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        "--volume",
        f"{dispatcher}:/runner/dispatch.py:ro",
        "--volume",
        f"{test_file}:/payload/test.py:ro",
        "--volume",
        f"{fixture_root}:{policy.virtual_root}:ro",
        "--workdir",
        "/tmp",
        policy.container_image,
        "python",
        "/runner/dispatch.py",
        "--test-file",
        "/payload/test.py",
        "--logical-path",
        path,
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            text=True,
            capture_output=True,
            timeout=_CONTAINER_TIMEOUT_SEC,
            check=False,
        )
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            cwd=repo_root,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
    if completed.returncode != 0:
        raise RuntimeError(
            f"runtime payload failed: {path} policy={policy.id}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    try:
        result: Any = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"runtime payload returned invalid JSON: {path}") from exc
    if not isinstance(result, dict):
        raise RuntimeError(f"runtime payload returned non-object evidence: {path}")
    expected = {
        "schema_version": 1,
        "path": path,
        "payload_sha256": hashlib.sha256(test_file.read_bytes()).hexdigest(),
        "executed": list(
            _declared_tests(test_file.read_text(encoding="utf-8"), logical_path=path)
        ),
    }
    if result != expected:
        raise RuntimeError(
            f"runtime payload evidence differs from expected: {path}: "
            f"expected={expected!r} actual={result!r}"
        )
    return result


def run(*, repo_root: Path, manifest_path: Path) -> tuple[dict[str, Any], ...]:
    manifest = load_manifest(manifest_path)
    tracked_paths = _tracked_paths(repo_root)
    errors = validate_manifest(manifest, repo_root=repo_root, tracked_paths=tracked_paths)
    if errors:
        raise ManifestError("; ".join(errors))
    plan = lane_execution_plan(
        manifest,
        tracked_paths=tracked_paths,
        lane="runtime-payload",
    )
    planned = [case["path"] for entry in plan for case in entry["cases"]]
    evidence: list[dict[str, Any]] = []
    images = dict.fromkeys(entry["policy"]["container_image"] for entry in plan)
    for image in images:
        _pull_image(repo_root=repo_root, image=image)
    with tempfile.TemporaryDirectory(prefix="loom-runtime-payload-") as temp_dir:
        temp_root = Path(temp_dir)
        temp_root.chmod(0o755)
        for entry in plan:
            policy = manifest.execution_policy(entry["policy"]["id"])
            for planned_case in entry["cases"]:
                path = planned_case["path"]
                case = manifest.execution_case(path)
                fixture_root = temp_root / hashlib.sha256(
                    path.encode("utf-8")
                ).hexdigest()[:16]
                _seed_case(case, fixture_root)
                evidence.append(
                    _run_payload(
                        repo_root=repo_root,
                        policy=policy,
                        fixture_root=fixture_root,
                        path=path,
                    )
                )
    completed_paths = [item["path"] for item in evidence]
    if len(completed_paths) != len(set(completed_paths)) or completed_paths != planned:
        raise RuntimeError(
            f"runtime payload evidence differs from plan: "
            f"planned={planned!r} completed={completed_paths!r}"
        )
    return tuple(evidence)


def main() -> int:
    default_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=default_root)
    parser.add_argument("--manifest", type=Path, default=None)
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()
    manifest_path = args.manifest or repo_root / "config/component-ownership.toml"
    evidence = run(repo_root=repo_root, manifest_path=manifest_path)
    function_count = sum(len(item["executed"]) for item in evidence)
    print(
        f"runtime payload conformance passed: {len(evidence)} files, "
        f"{function_count} tests"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
