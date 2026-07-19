"""Concrete reusable implementations for staged rollout preflight checks."""

from __future__ import annotations

import hashlib
import importlib
import json
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.docker_readiness import CommandRunner as DockerCommandRunner
from loom_cli.rollout.docker_readiness import probe_docker_runtime
from loom_cli.rollout.gb10_readiness import GB10ProbeTarget, probe_gb10_fleet_readonly
from loom_cli.rollout.install_attestation import (
    INSTALL_ATTESTATION_PATH,
    verify_runner_install,
)
from loom_cli.rollout.operator.candidate import (
    CandidateBindingError,
    GitRunner,
    verify_bound_candidate,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.model import CandidateBinding
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
from loom_cli.rollout.runtime_readiness import (
    REQUIRED_EXECUTABLES,
    REQUIRED_IMPORTS,
    RUNTIME_REQUIREMENT_DIGEST,
    ExecutableLookup,
    ModuleImporter,
    probe_runtime_readiness,
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


def build_candidate_identity_check(
    *,
    config: OperatorConfig,
    candidate: CandidateBinding,
    run: GitRunner,
) -> RegisteredCheck:
    """Build the Tier 0 exact candidate and installed source identity invariant."""
    expected_base = candidate.approved_base_sha or "none"

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != candidate.resolved_sha
            or context.bindings["candidate.source-mode"] != candidate.source_mode
            or context.bindings["candidate.base.sha"] != expected_base
            or context.bindings["runner.config.sha256"] != config.config_sha256
        ):
            return _empty_candidate_probe(candidate)
        try:
            identity = verify_bound_candidate(config, candidate, run=run)
        except (CandidateBindingError, OSError, ValueError):
            return _empty_candidate_probe(candidate)
        return CheckProbe(
            passed=True,
            evidence={
                "resolved-sha": identity.resolved_sha,
                "resolved-tree": identity.resolved_tree,
                "source-mode": identity.source_mode,
                "approved-base": identity.approved_base_sha or "none",
                "linear-history-count": identity.linear_history_count,
                "identity-digest": identity.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="candidate.identity",
            failure_code="candidate.identity.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "candidate.base.sha",
                "candidate.sha",
                "candidate.source-mode",
                "runner.config.sha256",
            ),
            evidence_schema=(
                EvidenceField("resolved-sha", "string"),
                EvidenceField("resolved-tree", "string"),
                EvidenceField("source-mode", "string"),
                EvidenceField("approved-base", "string"),
                EvidenceField("linear-history-count", "integer"),
                EvidenceField("identity-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=300,
            remediation="restore the exact clean candidate, source tree, approved base, and install config",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_candidate_probe(candidate: CandidateBinding) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "resolved-sha": candidate.resolved_sha,
            "resolved-tree": candidate.resolved_tree or "unavailable",
            "source-mode": candidate.source_mode,
            "approved-base": candidate.approved_base_sha or "none",
            "linear-history-count": 0,
            "identity-digest": "0" * 64,
        },
    )


def build_runner_install_check(
    *,
    config: OperatorConfig,
    candidate: CandidateBinding,
    service_uid: int,
    expected_attestation_digest: str,
    attestation_path: Path = INSTALL_ATTESTATION_PATH,
    assets: Mapping[str, tuple[Path, int, bool]] | None = None,
    expected_root_uid: int = 0,
) -> RegisteredCheck:
    """Build the Tier 0 root-issued runner install and live asset invariant."""
    if len(expected_attestation_digest) != 64 or any(
        character not in "0123456789abcdef" for character in expected_attestation_digest
    ):
        raise ValueError("runner install attestation digest is invalid")
    expected_base = candidate.approved_base_sha or "none"

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != candidate.resolved_sha
            or context.bindings["candidate.source-mode"] != candidate.source_mode
            or context.bindings["candidate.base.sha"] != expected_base
            or context.bindings["runner.config.sha256"] != config.config_sha256
            or context.bindings["runner.install.sha256"] != expected_attestation_digest
            or context.bindings["service.uid"] != service_uid
        ):
            return _empty_runner_install_probe(candidate)
        try:
            verified = verify_runner_install(
                service_uid=service_uid,
                attestation_path=attestation_path,
                assets=assets,
                expected_root_uid=expected_root_uid,
            )
        except (OSError, ValueError):
            return _empty_runner_install_probe(candidate)
        attestation = verified.attestation
        source_matches = bool(
            attestation.source_sha == candidate.resolved_sha
            and attestation.source_mode == candidate.source_mode
            and attestation.source_base_sha == expected_base
            and attestation.asset_sha256["config"] == config.config_sha256
            and attestation.payload_digest == expected_attestation_digest
        )
        if candidate.source_mode == "sealed-cumulative":
            source_matches = source_matches and bool(
                candidate.resolved_tree is not None
                and attestation.source_tree_sha == candidate.resolved_tree
            )
        else:
            source_matches = source_matches and attestation.source_tree_sha == "none"
        failed_assets = {label: "asset-drift" for label in verified.failed_assets}
        if not source_matches:
            failed_assets["install-binding"] = "identity-drift"
        return CheckProbe(
            passed=verified.ready and source_matches,
            evidence={
                "source-sha": attestation.source_sha,
                "source-tree": attestation.source_tree_sha,
                "source-base": attestation.source_base_sha,
                "install-record-digest": attestation.install_record_sha256,
                "asset-digests": dict(attestation.asset_sha256),
                "failed-assets": failed_assets,
                "metadata-fingerprint": verified.metadata_fingerprint,
                "acl-fingerprint": verified.acl_fingerprint,
                "attestation-digest": attestation.payload_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="runner.install",
            failure_code="runner.install.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("candidate.identity",),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "candidate.base.sha",
                "candidate.sha",
                "candidate.source-mode",
                "runner.config.sha256",
                "runner.install.sha256",
                "service.uid",
            ),
            evidence_schema=(
                EvidenceField("source-sha", "string"),
                EvidenceField("source-tree", "string"),
                EvidenceField("source-base", "string"),
                EvidenceField("install-record-digest", "sha256"),
                EvidenceField("asset-digests", "string-map"),
                EvidenceField("failed-assets", "string-map"),
                EvidenceField("metadata-fingerprint", "sha256"),
                EvidenceField("acl-fingerprint", "sha256"),
                EvidenceField("attestation-digest", "sha256"),
            ),
            timeout_seconds=15,
            freshness_ttl_seconds=300,
            remediation="reinstall and root-verify the exact runner source, config, and assets",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_runner_install_probe(candidate: CandidateBinding) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "source-sha": candidate.resolved_sha,
            "source-tree": candidate.resolved_tree or "unavailable",
            "source-base": candidate.approved_base_sha or "none",
            "install-record-digest": "0" * 64,
            "asset-digests": {},
            "failed-assets": {"install-binding": "unavailable"},
            "metadata-fingerprint": "0" * 64,
            "acl-fingerprint": "0" * 64,
            "attestation-digest": "0" * 64,
        },
    )


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


def build_tools_runtime_check(
    *,
    runner_install_hash: str,
    executable_lookup: ExecutableLookup,
    importer: ModuleImporter = importlib.import_module,
) -> RegisteredCheck:
    """Build the Tier 0 fixed executable and Python import invariant."""
    if len(runner_install_hash) != 64 or any(
        character not in "0123456789abcdef" for character in runner_install_hash
    ):
        raise ValueError("runner install hash is invalid")

    def probe(context: CheckContext) -> CheckProbe:
        if context.bindings["runner.install.sha256"] != runner_install_hash:
            return _empty_tools_runtime_probe()
        runtime = probe_runtime_readiness(
            executable_lookup=executable_lookup,
            importer=importer,
        )
        return CheckProbe(
            passed=runtime.ready,
            evidence={
                "executables": dict(runtime.executables),
                "imports": dict(runtime.imports),
                "executable-count": len(runtime.executables),
                "import-count": len(runtime.imports),
                "requirement-digest": runtime.requirement_digest,
                "runtime-digest": runtime.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="tools.runtime",
            failure_code="tools.runtime.unavailable",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("runner.install",),
            mutation_class=MutationClass.NONE,
            input_keys=("runner.config.sha256", "runner.install.sha256"),
            evidence_schema=(
                EvidenceField("executables", "string-map"),
                EvidenceField("imports", "string-map"),
                EvidenceField("executable-count", "integer"),
                EvidenceField("import-count", "integer"),
                EvidenceField("requirement-digest", "sha256"),
                EvidenceField("runtime-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=300,
            remediation="restore the fixed rollout executables and locked Python imports",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_tools_runtime_probe() -> CheckProbe:
    executables = {name: "missing" for name in REQUIRED_EXECUTABLES}
    imports = {name: "missing" for name in REQUIRED_IMPORTS}
    payload = {
        "executables": executables,
        "imports": imports,
        "requirement_digest": RUNTIME_REQUIREMENT_DIGEST,
    }
    return CheckProbe(
        passed=False,
        evidence={
            "executables": executables,
            "imports": imports,
            "executable-count": len(executables),
            "import-count": len(imports),
            "requirement-digest": RUNTIME_REQUIREMENT_DIGEST,
            "runtime-digest": hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        },
    )


def build_docker_runtime_check(run: DockerCommandRunner) -> RegisteredCheck:
    """Build the Tier 0 daemon/buildx invariant from the shared read-only probe."""

    def probe(_context: CheckContext) -> CheckProbe:
        runtime = probe_docker_runtime(run)
        return CheckProbe(
            passed=runtime.ready,
            evidence={
                "daemon-ready": runtime.daemon_ready,
                "buildx-ready": runtime.buildx_ready,
                "runtime-digest": runtime.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="docker.runtime",
            failure_code="docker.runtime.unavailable",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("tools.runtime",),
            mutation_class=MutationClass.NONE,
            input_keys=("runner.config.sha256", "runner.install.sha256"),
            evidence_schema=(
                EvidenceField("daemon-ready", "boolean"),
                EvidenceField("buildx-ready", "boolean"),
                EvidenceField("runtime-digest", "sha256"),
            ),
            timeout_seconds=15,
            freshness_ttl_seconds=120,
            remediation="restore service Docker daemon access and the fixed buildx plugin",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
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
    "build_candidate_identity_check",
    "build_credentials_metadata_check",
    "build_docker_runtime_check",
    "build_gb10_host_readiness_check",
    "build_runner_install_check",
    "build_systemd_user_manager_check",
    "build_tools_runtime_check",
    "credential_source_set_digest",
    "gb10_target_inventory_digest",
]
