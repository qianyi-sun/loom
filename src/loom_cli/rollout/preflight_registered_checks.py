"""Concrete reusable implementations for staged rollout preflight checks."""

from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
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


@dataclass(frozen=True, slots=True)
class CredentialProbeSource:
    label: str
    path: Path
    expected_content_fingerprint: str | None = None

    def __post_init__(self) -> None:
        if self.label not in {"admin", "worker", "service", "catalog"}:
            raise ValueError("credential probe label is not allowlisted")
        if not self.path.is_absolute() or ".." in self.path.parts:
            raise ValueError("credential probe path is invalid")
        if self.expected_content_fingerprint is not None and (
            not self.expected_content_fingerprint.startswith("sha256:")
            or len(self.expected_content_fingerprint) > 96
        ):
            raise ValueError("credential expected fingerprint is invalid")


def credential_source_set_digest(sources: tuple[CredentialProbeSource, ...]) -> str:
    """Bind the check to the exact allowlisted labels and absolute file paths."""
    if not sources or len({source.label for source in sources}) != len(sources):
        raise ValueError("credential probe sources must be non-empty and unique")
    payload = [
        {
            "label": source.label,
            "path": str(source.path),
        }
        for source in sorted(sources, key=lambda item: item.label)
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_credentials_metadata_check(
    *,
    sources: tuple[CredentialProbeSource, ...],
    service_uid: int,
    allow_qianyi_owner: bool = True,
) -> RegisteredCheck:
    """Build the Tier 0 protected-input authority and stability invariant."""
    if service_uid < 0:
        raise ValueError("credential service UID is invalid")
    expected_source_set_digest = credential_source_set_digest(sources)
    expected_content_fingerprints = {
        source.label: source.expected_content_fingerprint
        for source in sources
        if source.expected_content_fingerprint is not None
    }

    def probe(context: CheckContext) -> CheckProbe:
        expected_uid = context.bindings["service.uid"]
        source_set = context.bindings["protected-inputs.sha256"]
        context_fingerprints = context.bindings["secret-fingerprints"]
        valid_context_fingerprints = (
            isinstance(context_fingerprints, Mapping)
            and dict(context_fingerprints) == expected_content_fingerprints
        )
        if (
            type(expected_uid) is not int
            or expected_uid != service_uid
            or source_set != expected_source_set_digest
            or not valid_context_fingerprints
        ):
            return _empty_credentials_probe(
                source_set_digest=expected_source_set_digest,
                failed_sources={source.label: "binding-drift" for source in sources},
            )

        metadata_fingerprints: dict[str, str] = {}
        acl_fingerprints: dict[str, str] = {}
        content_fingerprints: dict[str, str] = {}
        authorities: dict[str, str] = {}
        failed_sources: dict[str, str] = {}
        for source in sources:
            try:
                trusted = read_trusted_file(
                    source.path,
                    service_uid=service_uid,
                    private=True,
                    allow_qianyi_owner=allow_qianyi_owner,
                    require_nonempty=True,
                )
                fingerprint = safe_content_fingerprint(trusted.payload)
                if (
                    source.expected_content_fingerprint is not None
                    and fingerprint != source.expected_content_fingerprint
                ):
                    failed_sources[source.label] = "content-fingerprint-mismatch"
                    continue
                metadata_fingerprints[source.label] = trusted.metadata_fingerprint
                acl_fingerprints[source.label] = trusted.acl_fingerprint
                content_fingerprints[source.label] = fingerprint
                authorities[source.label] = (
                    f"uid:{trusted.metadata.st_uid}:gid:{trusted.metadata.st_gid}:"
                    f"mode:{stat.S_IMODE(trusted.metadata.st_mode):04o}"
                )
            except (OSError, ValueError):
                failed_sources[source.label] = "authority-or-stability-failed"

        return CheckProbe(
            passed=not failed_sources and len(metadata_fingerprints) == len(sources),
            evidence={
                "metadata-fingerprints": metadata_fingerprints,
                "acl-fingerprints": acl_fingerprints,
                "content-fingerprints": content_fingerprints,
                "authorities": authorities,
                "failed-sources": failed_sources,
                "source-set-digest": expected_source_set_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="credentials.metadata",
            failure_code="credentials.metadata.unsafe",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("runner.install",),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "protected-inputs.sha256",
                "runner.config.sha256",
                "secret-fingerprints",
                "service.uid",
            ),
            evidence_schema=(
                EvidenceField("metadata-fingerprints", "string-map"),
                EvidenceField("acl-fingerprints", "string-map"),
                EvidenceField("content-fingerprints", "string-map"),
                EvidenceField("authorities", "string-map"),
                EvidenceField("failed-sources", "string-map"),
                EvidenceField("source-set-digest", "sha256"),
            ),
            timeout_seconds=10,
            freshness_ttl_seconds=120,
            remediation=(
                "restore the fixed protected files, service readability, private ACL, and exact metadata"
            ),
            secret_redaction_policy=SecretRedactionPolicy.METADATA_FINGERPRINTS_ONLY,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_credentials_probe(
    *,
    source_set_digest: str,
    failed_sources: dict[str, str],
) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "metadata-fingerprints": {},
            "acl-fingerprints": {},
            "content-fingerprints": {},
            "authorities": {},
            "failed-sources": failed_sources,
            "source-set-digest": source_set_digest,
        },
    )


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
    "CredentialProbeSource",
    "build_credentials_metadata_check",
    "build_gb10_host_readiness_check",
    "build_systemd_user_manager_check",
    "credential_source_set_digest",
    "gb10_target_inventory_digest",
]
