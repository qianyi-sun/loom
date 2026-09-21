"""Installed capacity-control-plane migration resource coverage."""

from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION_RESOURCES = {
    "capacity_migrations/__init__.py",
    "capacity_migrations/alembic.ini",
    "capacity_migrations/env.py",
    "capacity_migrations/script.py.mako",
    "capacity_migrations/versions/__init__.py",
    "capacity_migrations/versions/capacity_0001_shadow_management_schema.py",
    "capacity_migrations/versions/capacity_0002_dynamic_development_projection.py",
    "capacity_migrations/versions/capacity_0003_fenced_grant_protocol.py",
    "capacity_migrations/versions/capacity_0004_executable_bridge.py",
    "capacity_migrations/versions/capacity_0005_executable_allocation.py",
    "capacity_migrations/versions/capacity_0006_executable_work_queue.py",
    "capacity_migrations/versions/capacity_0007_protected_bootstrap_handshake.py",
    "capacity_migrations/versions/capacity_0008_executable_bridge_completion.py",
    "capacity_migrations/versions/capacity_0009_inventory_confirmation.py",
    "capacity_migrations/versions/capacity_0010_prepared_retirement_evidence.py",
    "capacity_migrations/versions/capacity_0011_retirement_heartbeat_freshness.py",
    "capacity_migrations/versions/capacity_0012_executable_intent_observed_state_check.py",
    "capacity_migrations/versions/capacity_0013_prepared_abort_evidence.py",
    "capacity_migrations/versions/capacity_0014_protected_admission_plan.py",
    "capacity_migrations/versions/capacity_0015_terminal_inventory_evidence.py",
    "capacity_migrations/versions/capacity_0016_personal_membership_events.py",
    "capacity_migrations/versions/capacity_0017_personal_membership_execution.py",
    "capacity_migrations/versions/capacity_0018_personal_build_membership.py",
    "capacity_migrations/versions/capacity_0019_typed_inventory_retirement.py",
    "capacity_migrations/versions/capacity_0020_typed_execution_readers.py",
    "capacity_migrations/versions/capacity_0021_typed_terminal_evidence.py",
    "capacity_migrations/versions/capacity_0022_chunked_inventory_retirement.py",
    "capacity_migrations/versions/capacity_0023_final_release_witness.py",
}
_GUARD_MIGRATION_RESOURCES = {
    "capacity_guard_migrations/__init__.py",
    "capacity_guard_migrations/alembic.ini",
    "capacity_guard_migrations/env.py",
    "capacity_guard_migrations/script.py.mako",
    "capacity_guard_migrations/versions/__init__.py",
    "capacity_guard_migrations/versions/guard_0001_protected_admission_foundation.py",
    "capacity_guard_migrations/versions/guard_0002_trusted_demand_agent.py",
    "capacity_guard_migrations/versions/guard_0003_prepared_admission.py",
    "capacity_guard_migrations/versions/guard_0004_disconnected_claim_guard.py",
    "capacity_guard_migrations/versions/guard_0005_inert_legacy_authority_fence.py",
    "capacity_guard_migrations/versions/guard_0006_lifecycle_demand_projection.py",
    "capacity_guard_migrations/versions/guard_0007_inert_trial_submission.py",
    "capacity_guard_migrations/versions/guard_0008_complete_mutation_inventory.py",
    "capacity_guard_migrations/versions/guard_0009_agent_reconfiguration.py",
    "capacity_guard_migrations/versions/guard_0010_protected_release_fence.py",
    "capacity_guard_migrations/versions/guard_0011_atomic_trial_submission.py",
    "capacity_guard_migrations/versions/guard_0012_protected_bootstrap_handshake.py",
    "capacity_guard_migrations/versions/guard_0013_executable_admission.py",
    "capacity_guard_migrations/versions/guard_0014_executable_intent_observation.py",
    "capacity_guard_migrations/versions/guard_0015_status_observer.py",
    "capacity_guard_migrations/versions/guard_0016_candidate_provenance.py",
    "capacity_guard_migrations/versions/guard_0017_executable_unregistered_withdrawal.py",
    "capacity_guard_migrations/versions/guard_0018_prepared_bootstrap_revocation.py",
    "capacity_guard_migrations/versions/guard_0019_executable_release_outbox.py",
    "capacity_guard_migrations/versions/guard_0020_exact_claim_assignment.py",
    "capacity_guard_migrations/versions/guard_0021_current_assignment_assertion.py",
    "capacity_guard_migrations/versions/guard_0022_staging_atomic_submission.py",
    "capacity_guard_migrations/versions/guard_0023_staging_worker_session.py",
    "capacity_guard_migrations/versions/guard_0024_protected_trial_terminal_closure.py",
    "capacity_guard_migrations/versions/guard_0025_protected_trial_retry.py",
    "capacity_guard_migrations/versions/guard_0026_protected_trial_requeue.py",
    "capacity_guard_migrations/versions/guard_0027_runtime_self_validation.py",
    "capacity_guard_migrations/versions/guard_0028_terminal_inventory_recovery.py",
    "capacity_guard_migrations/versions/guard_0029_protected_pending_cancellation.py",
    "capacity_guard_migrations/versions/guard_0030_application_trigger_schema_usage.py",
    "capacity_guard_migrations/versions/guard_0031_native_reader_fence.py",
    "capacity_guard_migrations/versions/guard_0032_typed_terminal_inventory.py",
    "capacity_guard_migrations/versions/guard_0035_trial_writer_interception.py",
    "capacity_guard_migrations/versions/guard_0033_refundable_admission_compatibility.py",
    "capacity_guard_migrations/versions/guard_0034_current_bootstrap_observation.py",
}


@pytest.fixture(scope="module")
def built_loom_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output_directory = tmp_path_factory.mktemp("capacity-wheel")
    source_directory = tmp_path_factory.mktemp("capacity-source") / "loom"
    shutil.copytree(
        _REPO_ROOT,
        source_directory,
        ignore=shutil.ignore_patterns(
            ".git",
            ".hypothesis",
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
            ".superpowers",
            ".venv",
            ".env",
            "*.egg-info",
            "__pycache__",
            "build",
            "dist",
        ),
    )
    completed = subprocess.run(
        [
            "uv",
            "build",
            "--wheel",
            "--out-dir",
            str(output_directory),
            str(source_directory),
        ],
        cwd=source_directory,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    wheels = list(output_directory.glob("loom-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_wheel_contains_complete_capacity_migration_package(
    built_loom_wheel: Path,
) -> None:
    with zipfile.ZipFile(built_loom_wheel) as wheel:
        members = set(wheel.namelist())

    assert _MIGRATION_RESOURCES <= members
    assert not any(member.endswith((".pyc", ".pyo")) for member in members)


def test_wheel_contains_complete_capacity_guard_migration_package(
    built_loom_wheel: Path,
) -> None:
    with zipfile.ZipFile(built_loom_wheel) as wheel:
        members = set(wheel.namelist())

    assert _GUARD_MIGRATION_RESOURCES <= members
