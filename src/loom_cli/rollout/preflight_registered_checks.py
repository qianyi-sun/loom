"""Concrete reusable implementations for staged rollout preflight checks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

from loom_cli.rollout.gb10_readiness import GB10ProbeTarget, probe_gb10_fleet_readonly
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.systemd_readiness import CommandRunner, probe_user_manager_readonly


def build_systemd_user_manager_check(
    run: CommandRunner,
    *,
    service_uid: int,
    monotonic: Callable[[], float] | None = None,
) -> RegisteredCheck:
    """Build the Tier 0 read-only systemd user-manager invariant."""

    def probe(context: CheckContext) -> CheckProbe:
        expected_uid = context.bindings["service.uid"]
        if type(expected_uid) is not int or expected_uid != service_uid:
            return _empty_user_manager_probe()
        evidence = (
            probe_user_manager_readonly(run, uid=service_uid)
            if monotonic is None
            else probe_user_manager_readonly(run, uid=service_uid, monotonic=monotonic)
        )
        if evidence is None:
            return _empty_user_manager_probe()
        return CheckProbe(
            passed=True,
            evidence={
                "version": evidence.version,
                "linger": evidence.linger_enabled,
                "boot-id": evidence.boot_id,
                "rpc-latency-ms": evidence.rpc_latency_ms,
                "rpc-budget-ms": evidence.rpc_budget_ms,
                "readiness-digest": evidence.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="systemd.user-manager",
            failure_code="systemd.user-manager.unavailable",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("tools.runtime",),
            mutation_class=MutationClass.NONE,
            input_keys=("runner.config.sha256", "service.uid"),
            evidence_schema=(
                EvidenceField("version", "string"),
                EvidenceField("linger", "boolean"),
                EvidenceField("boot-id", "string"),
                EvidenceField("rpc-latency-ms", "integer"),
                EvidenceField("rpc-budget-ms", "integer"),
                EvidenceField("readiness-digest", "sha256"),
            ),
            timeout_seconds=10,
            freshness_ttl_seconds=120,
            remediation="restore the rollout service user manager, linger, and bounded RPC",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_user_manager_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "version": "unavailable",
            "linger": False,
            "boot-id": "unavailable",
            "rpc-latency-ms": 0,
            "rpc-budget-ms": 0,
            "readiness-digest": "0" * 64,
        },
    )


def gb10_target_inventory_digest(targets: tuple[GB10ProbeTarget, ...]) -> str:
    """Bind the registered fleet check to the exact ordered host/service set."""
    payload = [
        {"host": target.ssh_target, "service": target.node_agent_service} for target in targets
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_gb10_host_readiness_check(
    run: CommandRunner,
    *,
    targets: tuple[GB10ProbeTarget, ...],
    ssh_config: Path,
    identity: Path,
    max_concurrency: int = 8,
    settle_attempts: int = 16,
    settle_interval_seconds: float = 2.0,
    sleep: Callable[[float], None] | None = None,
) -> RegisteredCheck:
    """Build the Tier 0 fixed-fleet readonly host readiness invariant."""
    expected_inventory_digest = gb10_target_inventory_digest(targets)

    def probe(context: CheckContext) -> CheckProbe:
        if context.bindings["gb10.inventory-digest"] != expected_inventory_digest:
            return _empty_gb10_probe(targets)
        fleet = (
            probe_gb10_fleet_readonly(
                run,
                targets,
                ssh_config=ssh_config,
                identity=identity,
                max_concurrency=max_concurrency,
                settle_attempts=settle_attempts,
                settle_interval_seconds=settle_interval_seconds,
            )
            if sleep is None
            else probe_gb10_fleet_readonly(
                run,
                targets,
                ssh_config=ssh_config,
                identity=identity,
                max_concurrency=max_concurrency,
                settle_attempts=settle_attempts,
                settle_interval_seconds=settle_interval_seconds,
                sleep=sleep,
            )
        )
        return CheckProbe(
            passed=fleet.ready,
            evidence={
                "boot-ids": dict(fleet.host_boot_ids),
                "host-digests": dict(fleet.host_evidence_digests),
                "failed-hosts": {host: "gb10.host-readiness.failed" for host in fleet.failed_hosts},
                "transient-hosts": {host: "observed" for host in fleet.transient_hosts},
                "host-count": len(targets),
                "inventory-digest": fleet.inventory_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="gb10.host-readiness",
            failure_code="gb10.host-readiness.failed",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("gb10.ssh-topology", "systemd.user-manager"),
            mutation_class=MutationClass.NONE,
            input_keys=("gb10.inventory-digest", "runner.config.sha256"),
            evidence_schema=(
                EvidenceField("boot-ids", "string-map"),
                EvidenceField("host-digests", "string-map"),
                EvidenceField("failed-hosts", "string-map"),
                EvidenceField("transient-hosts", "string-map"),
                EvidenceField("host-count", "integer"),
                EvidenceField("inventory-digest", "sha256"),
            ),
            timeout_seconds=60,
            freshness_ttl_seconds=120,
            remediation="restore GB10 user managers, linger, node-agent units, and timer readiness",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_gb10_probe(targets: tuple[GB10ProbeTarget, ...]) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "boot-ids": {},
            "host-digests": {},
            "failed-hosts": {target.ssh_target: "gb10.host-readiness.failed" for target in targets},
            "transient-hosts": {},
            "host-count": len(targets),
            "inventory-digest": "0" * 64,
        },
    )


__all__ = [
    "build_gb10_host_readiness_check",
    "build_systemd_user_manager_check",
    "gb10_target_inventory_digest",
]
