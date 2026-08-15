from __future__ import annotations

from pathlib import Path


def test_global_dev_fleet_service_uses_independently_protected_manager_trust_files() -> None:
    service = Path("deploy/dev-fleet/loom-global-dev-fleet-autoscaler.service").read_text(
        encoding="utf-8"
    )
    env_example = Path("deploy/dev-fleet/dev-fleet-autoscaler.env.example").read_text(
        encoding="utf-8"
    )

    assert "--global-execution-witness-json ${LOOM_DEV_GLOBAL_EXECUTION_WITNESS_JSON}" in service
    assert "--manager-public-key ${LOOM_DEV_MANAGER_PUBLIC_KEY}" in service
    assert (
        "--expected-manager-public-key-sha256-file "
        "${LOOM_DEV_MANAGER_PUBLIC_KEY_SHA256_FILE}" in service
    )
    assert "LOOM_DEV_MANAGER_PUBLIC_KEY_SHA256_FILE=" in env_example
