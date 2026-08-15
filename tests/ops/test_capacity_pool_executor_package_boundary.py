"""Fail-closed source boundary for the controller-local pool executor package."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path("src/loom_capacity_pool_executor")
PREPARED_SERVICE = Path("deploy/dev-fleet/loom-capacity-pool-executor-prepared.service")
PREPARED_TIMER = Path("deploy/dev-fleet/loom-capacity-pool-executor-prepared.timer")


def test_pool_executor_observer_has_no_scheduler_mutation_surface() -> None:
    forbidden = (
        "sbatch",
        "scancel",
        "srun",
        "shell=True",
        "os.system",
        "import subprocess",
        "from subprocess",
        "pyslurm",
    )
    sources = tuple(PACKAGE_ROOT.rglob("*.py"))

    assert sources
    assert not any(
        token in source.read_text(encoding="utf-8") for source in sources for token in forbidden
    )


def test_pool_executor_observer_is_not_imported_by_the_dry_run_package() -> None:
    dry_run_sources = tuple(Path("src/loom_capacity_executor").rglob("*.py"))

    assert dry_run_sources
    assert not any(
        "loom_capacity_pool_executor" in source.read_text(encoding="utf-8")
        for source in dry_run_sources
    )


def test_prepared_executor_service_and_timer_are_read_only_nonoverlapping_packages() -> None:
    service = PREPARED_SERVICE.read_text(encoding="utf-8")
    timer = PREPARED_TIMER.read_text(encoding="utf-8")
    service_directives = "\n".join(
        line for line in service.splitlines() if line and not line.startswith("#")
    )
    timer_directives = "\n".join(
        line for line in timer.splitlines() if line and not line.startswith("#")
    )

    assert "Type=oneshot" in service_directives
    assert "User=loom_capacity_executor" in service_directives
    assert "Group=loom_capacity_executor" in service_directives
    assert "UMask=0077" in service_directives
    assert "EnvironmentFile=/etc/loom-capacity-executor/service.env" in service_directives
    assert "ConditionPathIsRegular" not in service_directives
    assert "ExecCondition=/usr/bin/test -f /etc/loom-capacity-executor/service.env" in (
        service_directives
    )
    assert "ExecCondition=/usr/bin/test ! -L /etc/loom-capacity-executor/service.env" in (
        service_directives
    )
    assert "--prepared-only" in service_directives
    assert "--inventory-policy ${LOOM_CAPACITY_EXECUTOR_INVENTORY_POLICY}" in service_directives
    assert (
        "--expected-inventory-policy-sha256 "
        "${LOOM_CAPACITY_EXECUTOR_EXPECTED_INVENTORY_POLICY_SHA256}" in service_directives
    )
    assert "NoNewPrivileges=yes" in service_directives
    assert "ProtectSystem=strict" in service_directives
    assert "ReadOnlyPaths=/etc/loom-capacity-executor /run/loom-capacity-executor" in (
        service_directives
    )
    assert "ReadWritePaths=/var/lib/loom-capacity-executor" in service_directives
    assert "TimeoutStartSec=240" in service_directives
    assert "[Install]" not in service_directives

    assert "OnUnitInactiveSec=30" in timer_directives
    assert "OnUnitActiveSec" not in timer_directives
    assert "Persistent=false" in timer_directives
    assert "Unit=loom-capacity-pool-executor-prepared.service" in timer_directives
    assert "[Install]" in timer_directives
    assert "WantedBy=timers.target" in timer_directives
    assert 30 < 120

    lowered = f"{service_directives}\n{timer_directives}".lower()
    for forbidden in (
        "--validate-only",
        "--activation-runtime-artifact",
        "sbatch",
        "scancel",
        "/bin/sh",
        "/bin/bash",
        "autoscaler",
        "readwritepaths=/run/loom-capacity-executor",
    ):
        assert forbidden not in lowered
    assert "LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING} = 0" in service_directives
    assert "LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING} = 1" not in service_directives
