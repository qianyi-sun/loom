"""Fail-closed source boundary for the controller-local pool executor package."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

PACKAGE_ROOT = Path("src/loom_capacity_pool_executor")
PREPARED_SERVICE = Path("deploy/dev-fleet/loom-capacity-pool-executor-prepared.service")
PREPARED_TIMER = Path("deploy/dev-fleet/loom-capacity-pool-executor-prepared.timer")
ACTIVE_SERVICE = Path("deploy/dev-fleet/loom-capacity-pool-executor-active.service")
ACTIVE_TIMER = Path("deploy/dev-fleet/loom-capacity-pool-executor-active.timer")
REHEARSAL_RUNBOOK = Path(
    "docs/runbooks/executable-global-capacity-bridge-rehearsal.md"
)


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


def test_active_executor_service_and_timer_require_exact_positive_runtime_artifacts() -> None:
    service = ACTIVE_SERVICE.read_text(encoding="utf-8")
    timer = ACTIVE_TIMER.read_text(encoding="utf-8")
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
    assert (
        "EnvironmentFile=/etc/loom-capacity-executor/active-service.env"
        in service_directives
    )
    for path in (
        "/etc/loom-capacity-executor/active-service.env",
        "${LOOM_CAPACITY_EXECUTOR_CONFIG}",
        "${LOOM_CAPACITY_EXECUTOR_ACTIVATION_RUNTIME_ARTIFACT}",
    ):
        assert f"ExecCondition=/usr/bin/test -f {path}" in service_directives
        assert f"ExecCondition=/usr/bin/test ! -L {path}" in service_directives
    assert (
        "ExecStart=/opt/loom-capacity-executor/venv/bin/python -I -B "
        "-m loom_capacity_executor --config ${LOOM_CAPACITY_EXECUTOR_CONFIG} "
        "--expected-manifest-sha256 "
        "${LOOM_CAPACITY_EXECUTOR_EXPECTED_MANIFEST_SHA256} "
        "--pool ${LOOM_CAPACITY_EXECUTOR_POOL} --activation-runtime-artifact "
        "${LOOM_CAPACITY_EXECUTOR_ACTIVATION_RUNTIME_ARTIFACT}"
        in service_directives
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
    assert "Unit=loom-capacity-pool-executor-active.service" in timer_directives
    assert "[Install]" in timer_directives
    assert "WantedBy=timers.target" in timer_directives

    lowered = f"{service_directives}\n{timer_directives}".lower()
    for forbidden in (
        "--prepared-only",
        "--validate-only",
        "/bin/sh",
        "/bin/bash",
        "sbatch ",
        "scancel ",
        "scontrol ",
        "sacctmgr ",
    ):
        assert forbidden not in lowered


def test_drain_only_runbook_gate_accepts_the_exact_safe_status_boundary() -> None:
    runbook = REHEARSAL_RUNBOOK.read_text(encoding="utf-8")
    drain_only_section = runbook.split(
        'executor_status="$evidence_dir/drain-only-executors.json"', maxsplit=1
    )[1].split('subject_statuses="$evidence_dir/drain-only-subjects.jsonl"', maxsplit=1)[0]
    match = re.search(
        r"jq -e '\n(?P<filter>.*?)\n' \"\$executor_status\"",
        drain_only_section,
        flags=re.DOTALL,
    )
    assert match is not None
    payload = {
        "execution_state": "drain-only",
        "executable_new_capacity_ceiling": 0,
        "blockers": ["manager-drain-only", "zero-executable-ceiling"],
        "items": [
            {
                "pool_id": pool_id,
                "blockers": [],
                "retirement_safe": True,
                "inventory_record_counts": {},
                "inventory_digest": "a" * 64,
                "inventory_observed_at": "2026-08-17T22:00:00Z",
                "last_heartbeat_at": "2026-08-17T22:00:01Z",
            }
            for pool_id in ("gb10", "oldlab")
        ],
    }

    result = subprocess.run(
        ["jq", "-e", match.group("filter")],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
