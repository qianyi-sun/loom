from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from scripts.component_ownership import (
    lane_execution_plan,
    load_manifest,
    select_release_image_matrix,
)
from scripts.component_ownership import (
    test_paths_for_lane as selected_tests,
)

ROOT = Path(__file__).resolve().parents[2]


def _tracked() -> tuple[str, ...]:
    return tuple(subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True).splitlines())


@pytest.mark.parametrize(
    ("lane", "existing", "retained"),
    [
        (
            "tests-root",
            "tests/loom_cli/test_end_to_end_fake_backend.py",
            "tests/unit/test_explicit_nebius_backend.py",
        ),
        (
            "tests-root",
            "tests/unit/test_control_plane_app.py",
            "tests/unit/test_worker_claim_loop.py",
        ),
        (
            "integration",
            "tests/integration/test_nebius_platform_bootstrap.py",
            "tests/integration/test_control_plane_client.py",
        ),
        (
            "cluster-smoke",
            "tests/integration/test_nebius_platform_k3s.py",
            "tests/integration/test_execution_actuator_k3s.py",
        ),
        (
            "go-checks",
            "cmd/loom-execution-runtime/input_test.go",
            "cmd/loom-execution-runtime/broker_test.go",
        ),
    ],
)
def test_dev_lane_preserves_existing_and_nebius_tests(
    lane: str, existing: str, retained: str
) -> None:
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    selected = selected_tests(manifest, tracked_paths=_tracked(), lane=lane)
    assert existing in selected
    assert retained in selected


@pytest.mark.parametrize("force_all", [False, True])
def test_dev_image_fallback_preserves_existing_images(force_all: bool) -> None:
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    images = select_release_image_matrix(
        manifest,
        changed_paths=("new-unclassified-runtime.py",),
        force_all=force_all,
        fallback_all=True,
    )
    identities = {row["image"] for row in images}
    assert {"service", "control-plane", "execution-actuator", "execution-runtime"} <= identities



def test_runtime_payload_plan_preserves_existing_catalog() -> None:
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    plan = lane_execution_plan(manifest, tracked_paths=_tracked(), lane="runtime-payload")
    paths = [case["path"] for group in plan for case in group["cases"]]
    assert paths
    assert any("tb2-task-hello-world" in path for path in paths)


def test_python_lint_cli_preserves_combined_platform_scope() -> None:
    paths = subprocess.check_output(
        [sys.executable, "scripts/component_ownership.py", "python-paths"],
        cwd=ROOT,
        text=True,
    ).splitlines()
    assert "src/loom_execution_actuator/renderer.py" in paths
    assert "tests/unit/test_worker_claim_loop.py" in paths
    assert "tests/ops/test_nebius_ci_scope.py" in paths
    # Historical migrations still form the shared application schema chain.
    assert "migrations/versions/0043_gb10_worker_lifecycle.py" in paths
