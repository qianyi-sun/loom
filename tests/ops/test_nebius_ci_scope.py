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
    ("lane", "retired", "retained"),
    [
        (
            "tests-root",
            "tests/loom_cli/rollout/operator/test_broker.py",
            "tests/unit/test_explicit_nebius_backend.py",
        ),
        (
            "tests-root",
            "tests/unit/test_slurm_worker_jobs.py",
            "tests/unit/test_worker_claim_loop.py",
        ),
        (
            "integration",
            "tests/integration/test_executable_global_capacity_bridge.py",
            "tests/integration/test_control_plane_client.py",
        ),
        (
            "cluster-smoke",
            "tests/cluster/test_staging_k3s_render_contract.py",
            "tests/integration/test_execution_actuator_k3s.py",
        ),
        (
            "go-checks",
            "cmd/loom-task-image-builder-supervisor/main_test.go",
            "cmd/loom-execution-runtime/broker_test.go",
        ),
    ],
)
def test_nebius_lane_preserves_common_tests_without_retired_pool_tests(
    lane: str, retired: str, retained: str
) -> None:
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    selected = selected_tests(manifest, tracked_paths=_tracked(), lane=lane)
    assert retired not in selected
    assert retained in selected
    assert not any("gb10" in path.lower() or "oldlab" in path.lower() for path in selected)


@pytest.mark.parametrize("force_all", [False, True])
def test_image_selection_cannot_restore_retired_images_via_fallback(force_all: bool) -> None:
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    images = select_release_image_matrix(
        manifest,
        changed_paths=("new-unclassified-runtime.py",),
        force_all=force_all,
        fallback_all=True,
    )
    identities = {row["image"] for row in images}
    assert {"service", "control-plane", "execution-actuator", "execution-runtime"} <= identities
    assert not identities.intersection(
        {"capacity-manager", "capacity-executor", "rehearsal-postgres", "staging-admin-browser-smoke"}
    )
    assert not any(identity.startswith("personal-dev-") for identity in identities)


def test_runtime_payload_plan_does_not_execute_gb10_catalog() -> None:
    manifest = load_manifest(ROOT / "config/component-ownership.toml")
    plan = lane_execution_plan(manifest, tracked_paths=_tracked(), lane="runtime-payload")
    paths = [case["path"] for group in plan for case in group["cases"]]
    assert paths
    assert not any("gb10" in path for path in paths)
    assert any("tb2-task-hello-world" in path for path in paths)


def test_python_lint_cli_uses_the_same_retirement_boundary() -> None:
    paths = subprocess.check_output(
        [sys.executable, "scripts/component_ownership.py", "python-paths"],
        cwd=ROOT,
        text=True,
    ).splitlines()
    assert "src/loom_execution_actuator/renderer.py" in paths
    assert "tests/unit/test_worker_claim_loop.py" in paths
    assert "tests/ops/test_nebius_ci_scope.py" in paths
    assert not any(path.startswith("src/loom_cli/rollout/") for path in paths)
    assert not any(path.startswith("src/loom_capacity_") for path in paths)
    # Historical migrations still form the shared application schema chain.
    assert "migrations/versions/0043_gb10_worker_lifecycle.py" in paths
    assert not any(
        "gb10" in path.lower() or "oldlab" in path.lower()
        for path in paths
        if not path.startswith("migrations/")
    )
