from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
UNIT_ROOT = ROOT / "deploy" / "staging-rollout"


def test_lifecycle_service_is_fixed_non_shell_and_read_only_on_data() -> None:
    service = (UNIT_ROOT / "loom-staging-data-lifecycle.service").read_text()

    assert "User=loom-rollout" in service
    assert "ProtectSystem=strict" in service
    assert "ReadOnlyPaths=/data" in service
    assert "ReadWritePaths=/data" not in service
    assert "staging_data_lifecycle_maintenance.py" in service
    assert "--namespace loom-staging" in service
    assert "--bucket trajectories --bucket artifacts" in service
    assert "/bin/sh" not in service
    assert "sudo" not in service


def test_lifecycle_timer_is_bounded_and_persistent() -> None:
    timer = (UNIT_ROOT / "loom-staging-data-lifecycle.timer").read_text()

    assert "OnBootSec=5min" in timer
    assert "OnUnitActiveSec=5min" in timer
    assert "Persistent=true" in timer
    assert "Unit=loom-staging-data-lifecycle.service" in timer
