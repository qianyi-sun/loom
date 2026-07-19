"""Concrete reusable implementations for staged rollout preflight checks."""

from __future__ import annotations

from collections.abc import Callable

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


__all__ = ["build_systemd_user_manager_check"]
