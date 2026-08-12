"""Concrete reusable implementations for staged rollout preflight checks."""

from __future__ import annotations

import hashlib
import importlib
import json
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.browser_runtime_readiness import (
    CommandRunner as BrowserCommandRunner,
)
from loom_cli.rollout.browser_runtime_readiness import (
    browser_report_schema_digest,
    probe_browser_runtime,
)
from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.docker_readiness import CommandRunner as DockerCommandRunner
from loom_cli.rollout.docker_readiness import probe_docker_runtime
from loom_cli.rollout.external_supervisor_predecessor import (
    external_supervisor_unit_directory,
)
from loom_cli.rollout.external_supervisor_readiness import (
    build_external_supervisor_artifact,
    verify_external_supervisor_artifact,
)
from loom_cli.rollout.final_gate_readiness import (
    PROTECTED_MUTATION_CHECK_IDS,
    FinalGateAction,
    FinalGateResult,
    FinalGateSession,
)
from loom_cli.rollout.gb10_readiness import (
    GB10ProbeTarget,
    GB10SharedMountReadiness,
    probe_gb10_candidate_source_readonly,
    probe_gb10_fleet_readonly,
    probe_gb10_ssh_topology,
)
from loom_cli.rollout.image_readiness import (
    BROWSER_IMAGE,
    ImageArtifactSet,
    ImageBuildSession,
    image_plan_digest,
)
from loom_cli.rollout.image_readiness import (
    DockerRunner as ImageDockerRunner,
)
from loom_cli.rollout.install_attestation import (
    INSTALL_ATTESTATION_PATH,
    verify_runner_install,
)
from loom_cli.rollout.kubernetes_readiness import CommandRunner as KubernetesCommandRunner
from loom_cli.rollout.kubernetes_readiness import probe_kubernetes_client
from loom_cli.rollout.lifecycle_protocol import (
    LifecycleSelfTestEvidence,
    lifecycle_protocol_digest,
    run_lifecycle_self_test,
)
from loom_cli.rollout.manifest_readiness import (
    ManifestArtifact,
    ManifestRenderSession,
    RenderManifest,
    ServerDryRun,
)
from loom_cli.rollout.migration_manifest_readiness import (
    MigrationManifestArtifact,
    build_migration_manifest_artifact,
)
from loom_cli.rollout.migration_readiness import (
    DEFAULT_MIGRATION_POLICY,
    MigrationPlanEvidence,
    inspect_migration_plan,
)
from loom_cli.rollout.operator.backup_lease import (
    BackupLease,
    component_set_digest,
    evaluate_backup_lease,
)
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupRotationState,
    backup_rotation_admission_blockers,
)
from loom_cli.rollout.operator.candidate import (
    CandidateBindingError,
    GitRunner,
    verify_bound_candidate,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.manifest_apply_contract import MANIFEST_APPLY_CONTRACT_DIGEST
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.operator.systemd import SystemdLaunchCancelEvidence
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    EXTERNAL_SUPERVISOR_UNIT_DIRECTORY,
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
    external_supervisor_unit_set_digest,
    external_supervisor_unit_set_digest_or_empty,
)
from loom_cli.rollout.production_defaults_readiness import (
    ProductionDefaultsArtifact,
    build_production_defaults_artifact,
)
from loom_cli.rollout.readonly_authority import (
    ReadonlyAuthorityEvidence,
    readonly_authority_policy_digest,
)
from loom_cli.rollout.rehearsal_readiness import (
    IsolatedRehearsalSession,
    RehearsalAction,
    RehearsalResult,
)
from loom_cli.rollout.runtime_readiness import (
    REQUIRED_EXECUTABLES,
    REQUIRED_IMPORTS,
    RUNTIME_REQUIREMENT_DIGEST,
    ExecutableLookup,
    ModuleImporter,
    probe_runtime_readiness,
)
from loom_cli.rollout.staging_baseline_readiness import (
    BaselineProbeResult,
    ReadonlyProbe,
    StagingBaselineSession,
)
from loom_cli.rollout.systemd_readiness import CommandRunner, probe_user_manager_readonly
from loom_cli.rollout.systemd_unit_readiness import (
    CommandRunner as SystemdAnalyzeRunner,
)
from loom_cli.rollout.systemd_unit_readiness import (
    inspect_systemd_unit_sources,
    inspect_systemd_units,
)

_CLEAR_EXTERNAL_SUPERVISOR_TRANSITION_DIGEST = hashlib.sha256(b"{}").hexdigest()


@dataclass(frozen=True, slots=True)
class CredentialProbeSource:
    label: str
    path: Path
    expected_content_fingerprint: str | None = None

    def __post_init__(self) -> None:
        if self.label not in {
            "admin",
            "worker",
            "service",
            "catalog",
            "readonly-probe",
            "readonly-kubeconfig",
            "readonly-database",
            "readonly-minio",
            "rehearsal-kubeconfig",
            "server-dry-run-kubeconfig",
        }:
            raise ValueError("credential probe label is not allowlisted")
        if not self.path.is_absolute() or ".." in self.path.parts:
            raise ValueError("credential probe path is invalid")
        if self.expected_content_fingerprint is not None and (
            not self.expected_content_fingerprint.startswith("sha256:")
            or len(self.expected_content_fingerprint) > 96
        ):
            raise ValueError("credential expected fingerprint is invalid")


@dataclass(frozen=True, slots=True)
class ExternalSupervisorPredecessorSnapshot:
    """Secret-free read-only snapshot of the active supervisor authority."""

    kind: str
    authority_digest: str
    pointer_digest: str
    unit_sha256: Mapping[str, str]
    live_evidence_digest: str
    pending_transition_digest: str
    transition_clear: bool
    runtime_ready: bool
    pool_identity_digest: str

    def __post_init__(self) -> None:
        units = dict(self.unit_sha256)
        # An absent predecessor (first introduction of the supervisor) carries no
        # units and the absent authority/pointer digests; a present predecessor
        # (legacy-manifest or canonical) carries a complete paired unit set.
        absent = self.kind == "absent"
        if not absent:
            try:
                external_supervisor_unit_set_digest(units)
            except ValueError as exc:
                raise ValueError("external supervisor predecessor snapshot is invalid") from exc
        if (
            self.kind not in {"legacy-manifest", "canonical", "absent"}
            or bool(units) == absent
            or any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for value in (
                    self.authority_digest,
                    self.pointer_digest,
                    self.live_evidence_digest,
                    self.pending_transition_digest,
                    self.pool_identity_digest,
                )
            )
            or type(self.transition_clear) is not bool
            or type(self.runtime_ready) is not bool
            or (self.authority_digest == EXTERNAL_SUPERVISOR_ABSENT_DIGEST) != absent
            or (
                self.kind == "legacy-manifest"
                and self.pointer_digest != EXTERNAL_SUPERVISOR_ABSENT_DIGEST
            )
            or (
                self.kind == "canonical"
                and self.pointer_digest == EXTERNAL_SUPERVISOR_ABSENT_DIGEST
            )
            or (absent and self.pointer_digest != EXTERNAL_SUPERVISOR_ABSENT_DIGEST)
            or (
                self.transition_clear
                and self.pending_transition_digest != _CLEAR_EXTERNAL_SUPERVISOR_TRANSITION_DIGEST
            )
        ):
            raise ValueError("external supervisor predecessor snapshot is invalid")
        object.__setattr__(
            self,
            "unit_sha256",
            MappingProxyType(dict(sorted(units.items()))),
        )

    @property
    def unit_set_digest(self) -> str:
        return external_supervisor_unit_set_digest_or_empty(self.unit_sha256)


ExternalSupervisorPredecessorSource = Callable[
    [CheckContext],
    ExternalSupervisorPredecessorSnapshot | Mapping[str, ExternalSupervisorPredecessorSnapshot],
]


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
                fingerprint_payload = (
                    trusted.payload.strip()
                    if source.expected_content_fingerprint is not None
                    else trusted.payload
                )
                fingerprint = safe_content_fingerprint(fingerprint_payload)
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
                "inotify-capacity-ready": runtime.inotify_capacity_ready,
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
                EvidenceField("inotify-capacity-ready", "boolean"),
                EvidenceField("runtime-digest", "sha256"),
            ),
            timeout_seconds=15,
            freshness_ttl_seconds=120,
            remediation=(
                "restore service Docker daemon access, the fixed buildx plugin, and managed "
                "host inotify instance headroom"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v2",
        operations={CheckOperation.PROBE: probe},
    )


def build_kubernetes_client_check(
    run: KubernetesCommandRunner,
    *,
    config: OperatorConfig,
    expected_kubeconfig_metadata_digest: str,
) -> RegisteredCheck:
    """Build the Tier 0 exact context/namespace client invariant."""
    if len(expected_kubeconfig_metadata_digest) != 64 or any(
        character not in "0123456789abcdef" for character in expected_kubeconfig_metadata_digest
    ):
        raise ValueError("kubeconfig metadata digest is invalid")

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["runner.config.sha256"] != config.config_sha256
            or context.bindings["kubeconfig.metadata.sha256"] != expected_kubeconfig_metadata_digest
        ):
            return _empty_kubernetes_probe(config.namespace)
        readiness = probe_kubernetes_client(
            run,
            kubeconfig=config.kubeconfig_path,
            cluster_name=config.cluster_name,
            namespace=config.namespace,
        )
        return CheckProbe(
            passed=readiness.ready,
            evidence={
                "current-context": readiness.current_context,
                "namespace": readiness.namespace,
                "context-ready": readiness.context_ready,
                "namespace-ready": readiness.namespace_ready,
                "client-digest": readiness.evidence_digest,
                "kubeconfig-metadata-digest": expected_kubeconfig_metadata_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="kubernetes.client",
            failure_code="kubernetes.client.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("tools.runtime",),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "kubeconfig.metadata.sha256",
                "runner.config.sha256",
                "runner.install.sha256",
            ),
            evidence_schema=(
                EvidenceField("current-context", "string"),
                EvidenceField("namespace", "string"),
                EvidenceField("context-ready", "boolean"),
                EvidenceField("namespace-ready", "boolean"),
                EvidenceField("client-digest", "sha256"),
                EvidenceField("kubeconfig-metadata-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=120,
            remediation="restore the exact kubeconfig, staging context, and namespace access",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def build_readonly_authority_check(
    source: Callable[[], ReadonlyAuthorityEvidence],
) -> RegisteredCheck:
    """Prove the Tier 2 principal cannot mutate staging or read secrets."""
    policy_digest = readonly_authority_policy_digest()

    def probe(context: CheckContext) -> CheckProbe:
        if context.bindings["readonly.principal.sha256"] != policy_digest:
            return _empty_readonly_authority_probe(policy_digest)
        try:
            evidence = source()
        except Exception:
            return _empty_readonly_authority_probe(policy_digest)
        return CheckProbe(
            passed=evidence.ready,
            evidence={
                "principal": evidence.principal,
                "mutation-denied": evidence.ready,
                "protected-read-denied": not bool(
                    set(evidence.kubernetes_resources) & {"secrets", "serviceaccounts/token"}
                ),
                "policy-digest": policy_digest,
                "authority-digest": evidence.evidence_digest,
                "capability-source-digest": evidence.capability_source_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="readonly.authority",
            failure_code="readonly.authority.unsafe",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("runner.install",),
            mutation_class=MutationClass.NONE,
            input_keys=("readonly.principal.sha256", "runner.config.sha256"),
            evidence_schema=(
                EvidenceField("principal", "string"),
                EvidenceField("mutation-denied", "boolean"),
                EvidenceField("protected-read-denied", "boolean"),
                EvidenceField("policy-digest", "sha256"),
                EvidenceField("authority-digest", "sha256"),
                EvidenceField("capability-source-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=60,
            remediation="restore the dedicated read-only principal and remove every write or secret-read grant",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_readonly_authority_probe(policy_digest: str) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "principal": "loom-rollout-readonly",
            "mutation-denied": False,
            "protected-read-denied": False,
            "policy-digest": policy_digest,
            "authority-digest": "0" * 64,
            "capability-source-digest": "0" * 64,
        },
    )


def build_external_supervisor_predecessor_check(
    source: ExternalSupervisorPredecessorSource,
) -> RegisteredCheck:
    """Bind the live #907/canonical predecessor before any protected mutation."""

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["environment"] != "staging"
            or context.bindings["external-supervisor.unit-directory"]
            != EXTERNAL_SUPERVISOR_UNIT_DIRECTORY
        ):
            return _empty_external_supervisor_predecessor_probe()
        try:
            observed = source(context)
            if isinstance(observed, ExternalSupervisorPredecessorSnapshot):
                inferred_host = (
                    "TRT-EAI-OLDLAB-1"
                    if any("oldlab" in name for name in observed.unit_sha256)
                    else "gx10-01c7"
                )
                snapshots = {inferred_host: observed}
            else:
                snapshots = dict(observed)
            if (
                not snapshots
                or len(snapshots) > 8
                or any(
                    not isinstance(host, str)
                    or not host
                    or not isinstance(snapshot, ExternalSupervisorPredecessorSnapshot)
                    for host, snapshot in snapshots.items()
                )
                or len({snapshot.pool_identity_digest for snapshot in snapshots.values()}) != 1
            ):
                raise ValueError("external supervisor controller snapshots are invalid")
        except Exception:
            return _empty_external_supervisor_predecessor_probe()
        primary_host = "gx10-01c7" if "gx10-01c7" in snapshots else min(snapshots)
        primary = snapshots[primary_host]
        controller_bindings = {
            key: value
            for host, snapshot in sorted(snapshots.items())
            for key, value in {
                f"{host}/authority-kind": snapshot.kind,
                f"{host}/authority-digest": snapshot.authority_digest,
                f"{host}/pointer-digest": snapshot.pointer_digest,
                f"{host}/unit-set-digest": snapshot.unit_set_digest,
                f"{host}/live-evidence-digest": snapshot.live_evidence_digest,
                f"{host}/pending-transition-digest": snapshot.pending_transition_digest,
                f"{host}/unit-directory": external_supervisor_unit_directory(host),
                **{f"{host}/unit/{name}": digest for name, digest in snapshot.unit_sha256.items()},
            }.items()
        }
        return CheckProbe(
            passed=(
                all(
                    snapshot.kind in {"legacy-manifest", "canonical", "absent"}
                    and snapshot.transition_clear
                    and snapshot.runtime_ready
                    for snapshot in snapshots.values()
                )
            ),
            evidence={
                "authority-kind": primary.kind,
                "authority-digest": primary.authority_digest,
                "pointer-digest": primary.pointer_digest,
                "unit-digests": dict(primary.unit_sha256),
                "unit-set-digest": primary.unit_set_digest,
                "live-evidence-digest": primary.live_evidence_digest,
                "pending-transition-digest": primary.pending_transition_digest,
                "transition-clear": all(
                    snapshot.transition_clear for snapshot in snapshots.values()
                ),
                "runtime-ready": all(snapshot.runtime_ready for snapshot in snapshots.values()),
                "pool-identity-digest": primary.pool_identity_digest,
                "controller-bindings": controller_bindings,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="external-supervisor.predecessor",
            failure_code="external-supervisor.predecessor.drift",
            tier=0,
            stage=StageCapability.BASELINE_LIVE_READONLY,
            dependencies=("candidate.identity", "systemd.user-manager"),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "candidate.sha",
                "candidate.tree",
                "environment",
                "external-supervisor.unit-directory",
                "runner.config.sha256",
                "database.schema.revision",
                "service.uid",
            ),
            evidence_schema=(
                EvidenceField("authority-kind", "string"),
                EvidenceField("authority-digest", "sha256"),
                EvidenceField("pointer-digest", "sha256"),
                EvidenceField("unit-digests", "string-map"),
                EvidenceField("unit-set-digest", "sha256"),
                EvidenceField("live-evidence-digest", "sha256"),
                EvidenceField("pending-transition-digest", "sha256"),
                EvidenceField("transition-clear", "boolean"),
                EvidenceField("runtime-ready", "boolean"),
                EvidenceField("pool-identity-digest", "sha256"),
                EvidenceField("controller-bindings", "string-map"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=120,
            remediation=(
                "restore the checked-in #907 or active canonical supervisor authority, "
                "clear every durable transition, and re-run read-only admission"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v2",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_external_supervisor_predecessor_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "authority-kind": "unavailable",
            "authority-digest": "0" * 64,
            "pointer-digest": "0" * 64,
            "unit-digests": {},
            "unit-set-digest": "0" * 64,
            "live-evidence-digest": "0" * 64,
            "pending-transition-digest": "0" * 64,
            "transition-clear": False,
            "runtime-ready": False,
            "pool-identity-digest": "0" * 64,
            "controller-bindings": {},
        },
    )


def _empty_kubernetes_probe(namespace: str) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "current-context": "unavailable",
            "namespace": namespace,
            "context-ready": False,
            "namespace-ready": False,
            "client-digest": "0" * 64,
            "kubeconfig-metadata-digest": "0" * 64,
        },
    )


def build_capacity_high_water_check(
    capacity_source: Callable[[], StagingCapacity],
) -> RegisteredCheck:
    """Build the Tier 0 staging admission check from the typed capacity policy."""
    policy_digest = staging_capacity_policy_digest()

    def probe(context: CheckContext) -> CheckProbe:
        if context.bindings["capacity.policy.sha256"] != policy_digest:
            return _empty_capacity_probe(policy_digest)
        try:
            capacity = capacity_source()
        except Exception:
            return _empty_capacity_probe(policy_digest)
        return CheckProbe(
            passed=capacity.admission_allowed,
            evidence={
                "object-count": capacity.object_count,
                "bytes-used": capacity.bytes_used,
                "disk-free-percent": capacity.disk_free_percent,
                "inode-free-percent": capacity.inode_free_percent,
                "gc-required": capacity.gc_required,
                "admission-allowed": capacity.admission_allowed,
                "policy-digest": policy_digest,
                "capacity-digest": capacity.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="capacity.high-water",
            failure_code="capacity.high-water.blocked",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("runner.install",),
            mutation_class=MutationClass.NONE,
            input_keys=("capacity.policy.sha256", "runner.config.sha256"),
            evidence_schema=(
                EvidenceField("object-count", "integer"),
                EvidenceField("bytes-used", "integer"),
                EvidenceField("disk-free-percent", "integer"),
                EvidenceField("inode-free-percent", "integer"),
                EvidenceField("gc-required", "boolean"),
                EvidenceField("admission-allowed", "boolean"),
                EvidenceField("policy-digest", "sha256"),
                EvidenceField("capacity-digest", "sha256"),
            ),
            timeout_seconds=60,
            freshness_ttl_seconds=60,
            remediation="run supported staging GC or restore object, byte, disk, and inode headroom",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_capacity_probe(policy_digest: str) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "object-count": 0,
            "bytes-used": 0,
            "disk-free-percent": 0,
            "inode-free-percent": 0,
            "gc-required": True,
            "admission-allowed": False,
            "policy-digest": policy_digest,
            "capacity-digest": "0" * 64,
        },
    )


def build_backup_lease_eligibility_check(
    lease_source: Callable[[], BackupLease | None],
    *,
    now: Callable[[], datetime],
    expected_lease_digest: str,
    source_request_id: str,
    environment: str,
    namespace: str,
    mutation_epoch: int,
    db_snapshot_identity: str,
    schema_revision: str,
    object_inventory_root: str,
    manifest_sha256: str,
    component_sha256: Mapping[str, str],
) -> RegisteredCheck:
    """Build the Tier 0 reuse-or-fresh backup admission invariant.

    Absence or expiry of a lease is not itself a rollout blocker: it selects a
    fresh critical checkpoint after the pre-backup tiers pass.  Unreadable
    authority, cross-environment state, or input drift still fails closed.
    """
    expected_component_set_digest = component_set_digest(component_sha256)
    if len(expected_lease_digest) != 64 or any(
        character not in "0123456789abcdef" for character in expected_lease_digest
    ):
        raise ValueError("backup lease digest is invalid")

    expected_bindings = {
        "backup.component-set.sha256": expected_component_set_digest,
        "backup.lease.sha256": expected_lease_digest,
        "backup.manifest.sha256": manifest_sha256,
        "backup.source-request": source_request_id,
        "db.snapshot-identity": db_snapshot_identity,
        "environment": environment,
        "namespace": namespace,
        "object.inventory-root": object_inventory_root,
        "schema.revision": schema_revision,
        "staging.mutation-epoch": mutation_epoch,
    }

    def probe(context: CheckContext) -> CheckProbe:
        if any(context.bindings[key] != value for key, value in expected_bindings.items()):
            return _empty_backup_lease_probe(
                source_request_id=source_request_id,
                manifest_sha256=manifest_sha256,
                component_digest=expected_component_set_digest,
                mutation_epoch=mutation_epoch,
                blockers=("input-binding",),
            )
        try:
            lease = lease_source()
            if lease is None:
                return _empty_backup_lease_probe(
                    source_request_id=source_request_id,
                    manifest_sha256=manifest_sha256,
                    component_digest=expected_component_set_digest,
                    mutation_epoch=mutation_epoch,
                    blockers=("lease-absent",),
                    admission_allowed=True,
                )
            eligibility = evaluate_backup_lease(
                lease,
                now=now(),
                source_request_id=source_request_id,
                environment=environment,
                namespace=namespace,
                mutation_epoch=mutation_epoch,
                db_snapshot_identity=db_snapshot_identity,
                schema_revision=schema_revision,
                object_inventory_root=object_inventory_root,
                manifest_sha256=manifest_sha256,
                component_sha256=component_sha256,
            )
        except (OSError, RuntimeError, ValueError):
            return _empty_backup_lease_probe(
                source_request_id=source_request_id,
                manifest_sha256=manifest_sha256,
                component_digest=expected_component_set_digest,
                mutation_epoch=mutation_epoch,
                blockers=("lease-unavailable",),
            )
        blockers = {blocker: "mismatch" for blocker in eligibility.blockers}
        if eligibility.lease_digest != expected_lease_digest:
            blockers["lease-digest"] = "mismatch"
        fatal = {
            blocker
            for blocker in blockers
            if blocker in {"environment", "namespace", "lease-digest"}
        }
        reusable = eligibility.eligible and not blockers
        return CheckProbe(
            passed=not fatal,
            evidence={
                "admission-allowed": not fatal,
                "eligible": reusable,
                "strategy": "reuse" if reusable else "fresh",
                "blockers": {
                    blocker: "blocked" if blocker in fatal else "fresh-required"
                    for blocker in blockers
                },
                "lease-digest": eligibility.lease_digest,
                "source-request": lease.source_request_id,
                "manifest-digest": lease.manifest_sha256,
                "component-set-digest": component_set_digest(lease.component_sha256),
                "mutation-epoch": lease.mutation_epoch,
                "expires-at": lease.expires_at.isoformat(),
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="backup.lease-eligibility",
            failure_code="backup.lease.ineligible",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(
                "backup.rotation-capacity",
                "kubernetes.client",
            ),
            mutation_class=MutationClass.NONE,
            input_keys=tuple(expected_bindings),
            evidence_schema=(
                EvidenceField("admission-allowed", "boolean"),
                EvidenceField("eligible", "boolean"),
                EvidenceField("strategy", "string"),
                EvidenceField("blockers", "string-map"),
                EvidenceField("lease-digest", "sha256"),
                EvidenceField("source-request", "string"),
                EvidenceField("manifest-digest", "sha256"),
                EvidenceField("component-set-digest", "sha256"),
                EvidenceField("mutation-epoch", "integer"),
                EvidenceField("expires-at", "string"),
            ),
            timeout_seconds=15,
            freshness_ttl_seconds=60,
            remediation=(
                "repair unreadable or cross-environment lease authority; otherwise create and "
                "restore-verify the declared fresh staging checkpoint"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v2",
        operations={CheckOperation.PROBE: probe},
    )


def build_backup_rotation_capacity_check(
    rotation_source: Callable[[], BackupRotationState],
    *,
    expected_rotation_digest: str,
    permit_reserved_candidate: bool = False,
) -> RegisteredCheck:
    """Build the Tier 0 bounded backup-rotation admission invariant.

    ``permit_reserved_candidate`` is set ONLY for the isolated restore
    rehearsal, which runs after the checkpoint coordinator has already reserved
    this backup's own in-progress (``CREATING``) candidate. There the admission
    "candidate present" blocker is the expected post-checkpoint state, not a
    concurrent/orphaned reservation, so it is tolerated when the live rotation
    state matches the pinned ``expected_rotation_digest`` exactly. Pre-backup
    admission (the gating assessment and the driver's final admission) leaves it
    ``False`` and still blocks on any candidate.
    """
    if len(expected_rotation_digest) != 64 or any(
        character not in "0123456789abcdef" for character in expected_rotation_digest
    ):
        raise ValueError("backup rotation digest is invalid")

    def failed(*, blockers: Mapping[str, str], digest: str = "0" * 64) -> CheckProbe:
        return CheckProbe(
            passed=False,
            evidence={
                "admission-allowed": False,
                "active-present": False,
                "candidate-present": False,
                "payload-count": 0,
                "retirement-count": 0,
                "rotation-digest": digest,
                "blockers": dict(blockers),
            },
        )

    def probe(context: CheckContext) -> CheckProbe:
        if context.bindings["backup.rotation.sha256"] != expected_rotation_digest:
            return failed(blockers={"input-binding": "mismatch"})
        try:
            state = rotation_source()
        except (OSError, RuntimeError, ValueError):
            return failed(blockers={"rotation-authority": "unavailable"})
        blockers = backup_rotation_admission_blockers(state)
        if (
            permit_reserved_candidate
            and blockers.get("candidate") == "present"
            and state.candidate is not None
            and state.candidate.phase
            in (BackupPayloadPhase.CREATING, BackupPayloadPhase.MANIFEST_VERIFIED)
            and state.evidence_digest == expected_rotation_digest
        ):
            # The restore rehearsal's own reserved candidate, pinned exactly into
            # expected_rotation_digest, is not an admission blocker here. The
            # coordinator records the manifest (CREATING -> MANIFEST_VERIFIED)
            # before verifying restore, so the rehearsal observes it in either of
            # those pre-restore-verified phases; a RESTORE_VERIFIED/ACTIVE/FAILED
            # candidate is never permitted.
            del blockers["candidate"]
            # The own candidate also inflates payload_count by one, so a rolling
            # backup (prior active + own candidate) legitimately reaches the
            # transient limit of two in the rehearsal. Re-evaluate that limit
            # excluding the own candidate: the coordinator promotes it and
            # retires the prior active, restoring capacity. A genuine backlog
            # (stuck retirements) still leaves >= 2 payloads once the own
            # candidate is excluded and stays blocked.
            if blockers.get("transient-limit") == "reached" and state.payload_count - 1 < 2:
                del blockers["transient-limit"]
        if state.evidence_digest != expected_rotation_digest:
            blockers["rotation-digest"] = "drifted"
        return CheckProbe(
            passed=not blockers,
            evidence={
                "admission-allowed": not blockers,
                "active-present": state.active is not None,
                "candidate-present": state.candidate is not None,
                "payload-count": state.payload_count,
                "retirement-count": len(state.retirements),
                "rotation-digest": state.evidence_digest,
                "blockers": blockers,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="backup.rotation-capacity",
            failure_code="backup.rotation-capacity.blocked",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("capacity.high-water", "lifecycle.launch-cancel"),
            mutation_class=MutationClass.NONE,
            input_keys=("backup.rotation.sha256",),
            evidence_schema=(
                EvidenceField("admission-allowed", "boolean"),
                EvidenceField("active-present", "boolean"),
                EvidenceField("candidate-present", "boolean"),
                EvidenceField("payload-count", "integer"),
                EvidenceField("retirement-count", "integer"),
                EvidenceField("rotation-digest", "sha256"),
                EvidenceField("blockers", "string-map"),
            ),
            timeout_seconds=15,
            freshness_ttl_seconds=60,
            remediation=(
                "freeze admission and use digest-approved backup rotation retirement; never "
                "delete payloads or old request evidence directly"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def build_lifecycle_launch_cancel_check(
    self_test: Callable[[], LifecycleSelfTestEvidence] = run_lifecycle_self_test,
    runtime_test: Callable[[], SystemdLaunchCancelEvidence] | None = None,
) -> RegisteredCheck:
    """Build the Tier 0 protocol and real transient-unit launch/cancel test."""
    expected_protocol_digest = lifecycle_protocol_digest()

    def probe(context: CheckContext) -> CheckProbe:
        if context.bindings["lifecycle.protocol.sha256"] != expected_protocol_digest:
            return _empty_lifecycle_probe(expected_protocol_digest)
        try:
            evidence = self_test()
            if runtime_test is None:
                return _empty_lifecycle_probe(expected_protocol_digest)
            runtime = runtime_test()
        except Exception:
            return _empty_lifecycle_probe(expected_protocol_digest)
        ready = (
            evidence.ready
            and evidence.protocol_digest == expected_protocol_digest
            and runtime.ready
            and runtime.launched
            and runtime.cancelled
            and runtime.unit_absent
        )
        return CheckProbe(
            passed=ready,
            evidence={
                "ready": ready,
                "scenario-count": evidence.scenario_count,
                "transition-count": evidence.transition_count,
                "rejection-count": evidence.rejection_count,
                "protocol-digest": evidence.protocol_digest,
                "self-test-digest": evidence.evidence_digest,
                "runtime-ready": runtime.ready,
                "launched": runtime.launched,
                "cancelled": runtime.cancelled,
                "unit-absent": runtime.unit_absent,
                "launch-latency-ms": runtime.launch_latency_ms,
                "cancel-latency-ms": runtime.cancel_latency_ms,
                "latency-budget-ms": runtime.latency_budget_ms,
                "runtime-digest": runtime.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="lifecycle.launch-cancel",
            failure_code="lifecycle.launch-cancel.failed",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("systemd.user-manager",),
            mutation_class=MutationClass.ISOLATED,
            input_keys=(
                "candidate.sha",
                "lifecycle.protocol.sha256",
                "runner.config.sha256",
            ),
            evidence_schema=(
                EvidenceField("ready", "boolean"),
                EvidenceField("scenario-count", "integer"),
                EvidenceField("transition-count", "integer"),
                EvidenceField("rejection-count", "integer"),
                EvidenceField("protocol-digest", "sha256"),
                EvidenceField("self-test-digest", "sha256"),
                EvidenceField("runtime-ready", "boolean"),
                EvidenceField("launched", "boolean"),
                EvidenceField("cancelled", "boolean"),
                EvidenceField("unit-absent", "boolean"),
                EvidenceField("launch-latency-ms", "integer"),
                EvidenceField("cancel-latency-ms", "integer"),
                EvidenceField("latency-budget-ms", "integer"),
                EvidenceField("runtime-digest", "sha256"),
            ),
            timeout_seconds=75,
            freshness_ttl_seconds=120,
            remediation=(
                "restore the reviewed short-lock lifecycle protocol and repair the systemd user "
                "manager or kernel path until the exact transient launch/cancel round trip passes"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v2",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_lifecycle_probe(protocol_digest: str) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "ready": False,
            "scenario-count": 0,
            "transition-count": 0,
            "rejection-count": 0,
            "protocol-digest": protocol_digest,
            "self-test-digest": "0" * 64,
            "runtime-ready": False,
            "launched": False,
            "cancelled": False,
            "unit-absent": False,
            "launch-latency-ms": 0,
            "cancel-latency-ms": 0,
            "latency-budget-ms": 10_000,
            "runtime-digest": "0" * 64,
        },
    )


def _empty_backup_lease_probe(
    *,
    source_request_id: str,
    manifest_sha256: str,
    component_digest: str,
    mutation_epoch: int,
    blockers: tuple[str, ...],
    admission_allowed: bool = False,
) -> CheckProbe:
    return CheckProbe(
        passed=admission_allowed,
        evidence={
            "admission-allowed": admission_allowed,
            "eligible": False,
            "strategy": "fresh" if admission_allowed else "blocked",
            "blockers": {
                blocker: "fresh-required" if admission_allowed else "blocked"
                for blocker in blockers
            },
            "lease-digest": "0" * 64,
            "source-request": source_request_id,
            "manifest-digest": manifest_sha256,
            "component-set-digest": component_digest,
            "mutation-epoch": mutation_epoch,
            "expires-at": "unavailable",
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


def build_gb10_ssh_topology_check(
    run: CommandRunner,
    *,
    targets: tuple[GB10ProbeTarget, ...],
    ssh_config: Path,
    identity: Path,
    service_uid: int,
    expected_ssh_config_sha256: str,
    expected_identity_metadata_fingerprint: str,
    max_concurrency: int = 8,
) -> RegisteredCheck:
    """Build the Tier 0 fixed SSH topology and batch-mode trust invariant."""
    expected_inventory_digest = gb10_target_inventory_digest(targets)
    for value in (expected_ssh_config_sha256, expected_identity_metadata_fingerprint):
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("GB10 SSH topology digest is invalid")

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["gb10.inventory-digest"] != expected_inventory_digest
            or context.bindings["gb10.ssh-config.sha256"] != expected_ssh_config_sha256
            or context.bindings["gb10.identity.metadata-fingerprint"]
            != expected_identity_metadata_fingerprint
            or context.bindings["service.uid"] != service_uid
        ):
            return _empty_gb10_ssh_probe(
                targets,
                expected_ssh_config_sha256,
                expected_identity_metadata_fingerprint,
            )
        try:
            config_read = read_trusted_file(
                ssh_config,
                service_uid=service_uid,
                private=False,
                require_nonempty=True,
            )
            identity_read = read_trusted_file(
                identity,
                service_uid=service_uid,
                private=True,
                allow_qianyi_owner=True,
                require_nonempty=True,
            )
            config_digest = hashlib.sha256(config_read.payload).hexdigest()
            if (
                config_digest != expected_ssh_config_sha256
                or identity_read.metadata_fingerprint != expected_identity_metadata_fingerprint
            ):
                raise ValueError("GB10 SSH input drift")
            topology = probe_gb10_ssh_topology(
                run,
                targets,
                ssh_config=ssh_config,
                identity=identity,
                max_concurrency=max_concurrency,
            )
        except (OSError, ValueError):
            return _empty_gb10_ssh_probe(
                targets,
                expected_ssh_config_sha256,
                expected_identity_metadata_fingerprint,
            )
        return CheckProbe(
            passed=topology.ready,
            evidence={
                "reachable-hosts": {host: "reachable" for host in topology.reachable_hosts},
                "failed-hosts": {host: "unreachable" for host in topology.failed_hosts},
                "host-count": len(targets),
                "ssh-config-digest": config_digest,
                "identity-metadata-fingerprint": identity_read.metadata_fingerprint,
                "topology-digest": topology.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="gb10.ssh-topology",
            failure_code="gb10.ssh-topology.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("credentials.metadata", "tools.runtime"),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "gb10.identity.metadata-fingerprint",
                "gb10.inventory-digest",
                "gb10.ssh-config.sha256",
                "runner.config.sha256",
                "service.uid",
            ),
            evidence_schema=(
                EvidenceField("reachable-hosts", "string-map"),
                EvidenceField("failed-hosts", "string-map"),
                EvidenceField("host-count", "integer"),
                EvidenceField("ssh-config-digest", "sha256"),
                EvidenceField("identity-metadata-fingerprint", "sha256"),
                EvidenceField("topology-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=120,
            remediation="restore the exact GB10 SSH config, identity authority, known hosts, and batch trust",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def gb10_mount_binding_digest(binding: Mapping[str, int]) -> str:
    required = {
        "service_uid",
        "service_primary_gid",
        "consumer_uid",
        "consumer_primary_gid",
        "shared_gid",
        "parent_device",
        "parent_inode",
        "authority_device",
        "authority_inode",
        "repository_device",
        "repository_inode",
    }
    values = dict(binding)
    if set(values) != required or any(
        type(value) is not int or value < 0 for value in values.values()
    ):
        raise ValueError("GB10 mount binding is invalid")
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_gb10_shared_mount_check(
    mount_source: Callable[[], GB10SharedMountReadiness],
    *,
    targets: tuple[GB10ProbeTarget, ...],
    expected_binding_digest: str,
) -> RegisteredCheck:
    """Build the Tier 0 exact shared mount and immutable checkout-root invariant."""
    expected_inventory_digest = gb10_target_inventory_digest(targets)
    if len(expected_binding_digest) != 64 or any(
        character not in "0123456789abcdef" for character in expected_binding_digest
    ):
        raise ValueError("GB10 mount binding digest is invalid")

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["gb10.inventory-digest"] != expected_inventory_digest
            or context.bindings["gb10.mount-binding.sha256"] != expected_binding_digest
        ):
            return _empty_gb10_mount_probe(targets, expected_binding_digest)
        try:
            evidence = mount_source()
        except Exception:
            return _empty_gb10_mount_probe(targets, expected_binding_digest)
        return CheckProbe(
            passed=evidence.ready
            and set(evidence.host_digests) == {target.ssh_target for target in targets},
            evidence={
                "host-digests": dict(evidence.host_digests),
                "failed-hosts": {host: "mount-drift" for host in evidence.failed_hosts},
                "host-count": len(targets),
                "binding-digest": expected_binding_digest,
                "mount-digest": evidence.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="gb10.shared-mount",
            failure_code="gb10.shared-mount.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("gb10.ssh-topology",),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "gb10.inventory-digest",
                "gb10.mount-binding.sha256",
                "runner.config.sha256",
            ),
            evidence_schema=(
                EvidenceField("host-digests", "string-map"),
                EvidenceField("failed-hosts", "string-map"),
                EvidenceField("host-count", "integer"),
                EvidenceField("binding-digest", "sha256"),
                EvidenceField("mount-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=120,
            remediation="restore the canonical shared mount, UID/GID authority, and immutable checkout root",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_gb10_mount_probe(
    targets: tuple[GB10ProbeTarget, ...],
    binding_digest: str,
) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "host-digests": {},
            "failed-hosts": {target.ssh_target: "mount-drift" for target in targets},
            "host-count": len(targets),
            "binding-digest": binding_digest,
            "mount-digest": "0" * 64,
        },
    )


def _empty_gb10_ssh_probe(
    targets: tuple[GB10ProbeTarget, ...],
    ssh_config_digest: str,
    identity_metadata_fingerprint: str,
) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "reachable-hosts": {},
            "failed-hosts": {target.ssh_target: "unreachable" for target in targets},
            "host-count": len(targets),
            "ssh-config-digest": ssh_config_digest,
            "identity-metadata-fingerprint": identity_metadata_fingerprint,
            "topology-digest": "0" * 64,
        },
    )


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


def build_gb10_candidate_source_check(
    run: CommandRunner,
    *,
    targets: tuple[GB10ProbeTarget, ...],
    ssh_config: Path,
    identity: Path,
    candidate_root: Path,
    expected_candidate_sha: str,
    expected_candidate_tree: str,
    image_tag: str,
    max_concurrency: int = 8,
    settle_attempts: int = 6,
    settle_interval_seconds: float = 2.0,
) -> RegisteredCheck:
    """Build the Tier 0 exact shared candidate source and unit-byte invariant."""
    expected_inventory_digest = gb10_target_inventory_digest(targets)

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != expected_candidate_sha
            or context.bindings["candidate.tree"] != expected_candidate_tree
            or context.bindings["gb10.inventory-digest"] != expected_inventory_digest
        ):
            return _empty_gb10_candidate_source_probe(targets)
        try:
            units = inspect_systemd_unit_sources(candidate_root)
            if not units.ready:
                return _empty_gb10_candidate_source_probe(targets)
            evidence = probe_gb10_candidate_source_readonly(
                run,
                targets,
                ssh_config=ssh_config,
                identity=identity,
                candidate_sha=expected_candidate_sha,
                candidate_tree=expected_candidate_tree,
                image_tag=image_tag,
                unit_sha256=units.unit_sha256,
                unit_set_digest=units.unit_set_digest,
                max_concurrency=max_concurrency,
                settle_attempts=settle_attempts,
                settle_interval_seconds=settle_interval_seconds,
            )
        except (OSError, RuntimeError, ValueError):
            return _empty_gb10_candidate_source_probe(targets)
        return CheckProbe(
            passed=evidence.ready
            and set(evidence.host_digests) == {target.ssh_target for target in targets},
            evidence={
                "host-digests": dict(evidence.host_digests),
                "failed-hosts": {
                    host: "gb10.candidate-source.drift" for host in evidence.failed_hosts
                },
                "host-count": len(targets),
                "candidate-sha": evidence.candidate_sha,
                "candidate-tree": evidence.candidate_tree,
                "unit-set-digest": evidence.unit_set_digest,
                "source-digest": evidence.evidence_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="gb10.candidate-source",
            failure_code="gb10.candidate-source.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=("candidate.identity", "gb10.shared-mount"),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "candidate.sha",
                "candidate.tree",
                "gb10.inventory-digest",
                "runner.config.sha256",
            ),
            evidence_schema=(
                EvidenceField("host-digests", "string-map"),
                EvidenceField("failed-hosts", "string-map"),
                EvidenceField("host-count", "integer"),
                EvidenceField("candidate-sha", "string"),
                EvidenceField("candidate-tree", "string"),
                EvidenceField("unit-set-digest", "sha256"),
                EvidenceField("source-digest", "sha256"),
            ),
            timeout_seconds=180,
            freshness_ttl_seconds=120,
            remediation=(
                "restore the exact immutable shared candidate checkout and candidate unit bytes"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v2",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_gb10_candidate_source_probe(
    targets: tuple[GB10ProbeTarget, ...],
) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "host-digests": {},
            "failed-hosts": {
                target.ssh_target: "gb10.candidate-source.drift" for target in targets
            },
            "host-count": len(targets),
            "candidate-sha": "unavailable",
            "candidate-tree": "unavailable",
            "unit-set-digest": "0" * 64,
            "source-digest": "0" * 64,
        },
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


def build_migration_plan_check(
    *,
    alembic_ini: Path,
    expected_candidate_sha: str,
    expected_policy_digest: str,
    policy_path: Path = DEFAULT_MIGRATION_POLICY,
    artifact_sink: Callable[[MigrationPlanEvidence], None] | None = None,
) -> RegisteredCheck:
    """Build the Tier 1 exact static migration graph and policy invariant."""
    for value in (expected_candidate_sha, expected_policy_digest):
        if len(value) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("migration plan binding is invalid")

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != expected_candidate_sha
            or context.bindings["migration.policy.sha256"] != expected_policy_digest
        ):
            return _empty_migration_plan_probe(expected_policy_digest)
        try:
            plan = inspect_migration_plan(alembic_ini, policy_path=policy_path)
        except ValueError:
            return _empty_migration_plan_probe(expected_policy_digest)
        if artifact_sink is not None:
            artifact_sink(plan)
        return CheckProbe(
            passed=plan.policy_digest == expected_policy_digest,
            evidence={
                "head": plan.head,
                "base": plan.base,
                "revision-count": plan.revision_count,
                "linear": True,
                "graph-policy": plan.graph_policy,
                "upgrade-policy": plan.upgrade_policy,
                "downgrade-policy": plan.downgrade_policy,
                "policy-digest": plan.policy_digest,
                "plan-digest": plan.plan_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="migration.plan",
            failure_code="migration.plan.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("candidate.identity",),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha", "migration.policy.sha256"),
            evidence_schema=(
                EvidenceField("head", "string"),
                EvidenceField("base", "string"),
                EvidenceField("revision-count", "integer"),
                EvidenceField("linear", "boolean"),
                EvidenceField("graph-policy", "string"),
                EvidenceField("upgrade-policy", "string"),
                EvidenceField("downgrade-policy", "string"),
                EvidenceField("policy-digest", "sha256"),
                EvidenceField("plan-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=3600,
            remediation="restore the exact single-head migration graph and reviewed staging policy",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def build_migration_manifest_check(
    server_dry_run: ServerDryRun,
    *,
    image_artifact: Callable[[], ImageArtifactSet],
    migration_plan: Callable[[], tuple[str, str]],
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    namespace: str,
    container_registry: str = "",
    artifact_sink: Callable[[MigrationManifestArtifact], None] | None = None,
) -> RegisteredCheck:
    """Render and server-validate the exact migration Job once in Tier 1."""

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != candidate_sha
            or context.bindings["candidate.tree"] != candidate_tree
        ):
            return _empty_migration_manifest_probe()
        try:
            plan_digest, target_revision = migration_plan()
            images = image_artifact()
            artifact = build_migration_manifest_artifact(
                server_dry_run,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                image_tag=image_tag,
                image_id=images.image_digests["loom-control-plane"],
                namespace=namespace,
                migration_plan_sha256=plan_digest,
                migration_target_revision=target_revision,
                container_registry=container_registry,
                registry_digest=(
                    images.registry_digests["loom-control-plane"] if container_registry else ""
                ),
            )
        except (OSError, RuntimeError, ValueError):
            return _empty_migration_manifest_probe()
        if artifact_sink is not None:
            artifact_sink(artifact)
        return CheckProbe(
            passed=True,
            evidence={
                "artifact-digest": artifact.artifact_digest,
                "manifest-digest": artifact.rendered_sha256,
                "job-name": artifact.job_name,
                "image-id": artifact.image_id,
                "target-revision": artifact.migration_target_revision,
                "server-valid": True,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="migration.manifest",
            failure_code="migration.manifest.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("images.contract", "migration.plan", "kubernetes.client"),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha", "candidate.tree"),
            evidence_schema=(
                EvidenceField("artifact-digest", "sha256"),
                EvidenceField("manifest-digest", "sha256"),
                EvidenceField("job-name", "string"),
                EvidenceField("image-id", "string"),
                EvidenceField("target-revision", "string"),
                EvidenceField("server-valid", "boolean"),
            ),
            timeout_seconds=120,
            freshness_ttl_seconds=3600,
            remediation="restore exact image, migration graph and readonly schema validation",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v2",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_migration_manifest_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "artifact-digest": "0" * 64,
            "manifest-digest": "0" * 64,
            "job-name": "unavailable",
            "image-id": "unavailable",
            "target-revision": "unavailable",
            "server-valid": False,
        },
    )


def _empty_migration_plan_probe(policy_digest: str) -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "head": "unavailable",
            "base": "unavailable",
            "revision-count": 0,
            "linear": False,
            "graph-policy": "unavailable",
            "upgrade-policy": "unavailable",
            "downgrade-policy": "unavailable",
            "policy-digest": policy_digest,
            "plan-digest": "0" * 64,
        },
    )


def build_systemd_render_check(
    run: SystemdAnalyzeRunner,
    *,
    candidate_root: Path,
    expected_candidate_sha: str,
    expected_candidate_tree: str,
    expected_image_tag: str,
    expected_environment: str,
) -> RegisteredCheck:
    """Build the Tier 1 fixed and environment-derived systemd unit invariant."""

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != expected_candidate_sha
            or context.bindings["candidate.tree"] != expected_candidate_tree
            or context.bindings["candidate.image-tag"] != expected_image_tag
            or context.bindings["environment"] != expected_environment
        ):
            return _empty_systemd_render_probe()
        try:
            fixed = inspect_systemd_units(candidate_root, run=run)
            supervisor_artifact = build_external_supervisor_artifact(
                candidate_root,
                candidate_sha=expected_candidate_sha,
                candidate_tree=expected_candidate_tree,
                image_tag=expected_image_tag,
                environment=expected_environment,
            )
            supervisor = verify_external_supervisor_artifact(supervisor_artifact, run)
            controller_artifacts = {
                execution_host: build_external_supervisor_artifact(
                    candidate_root,
                    candidate_sha=expected_candidate_sha,
                    candidate_tree=expected_candidate_tree,
                    image_tag=expected_image_tag,
                    environment=expected_environment,
                    execution_host=execution_host,
                )
                for execution_host in sorted(
                    {identity.execution_host for identity in supervisor_artifact.supervisors}
                )
            }
        except (OSError, RuntimeError, ValueError):
            return _empty_systemd_render_probe()
        controller_units = {
            f"{execution_host}/{name}": digest
            for execution_host, artifact in controller_artifacts.items()
            for name, digest in artifact.unit_sha256.items()
        }
        if (
            set(fixed.unit_sha256) & set(supervisor_artifact.unit_sha256)
            or any(
                artifact.profile_sha256 != supervisor_artifact.profile_sha256
                or dict(artifact.script_sha256) != dict(supervisor_artifact.script_sha256)
                for artifact in controller_artifacts.values()
            )
            or {
                name: digest
                for artifact in controller_artifacts.values()
                for name, digest in artifact.unit_sha256.items()
            }
            != dict(supervisor_artifact.unit_sha256)
        ):
            return _empty_systemd_render_probe()
        unit_digests = {
            **fixed.unit_sha256,
            **supervisor.unit_sha256,
        }
        failed_units = {
            **fixed.failed_units,
            **supervisor.failed_units,
        }
        unit_set_digest = hashlib.sha256(
            json.dumps(
                {"failed": failed_units, "units": unit_digests},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        supervisor_unit_set_digest = external_supervisor_unit_set_digest(
            supervisor_artifact.unit_sha256
        )
        return CheckProbe(
            passed=fixed.ready and supervisor.ready,
            evidence={
                "supervisor-artifact-digest": supervisor.artifact_digest,
                "supervisor-profile-sha256": supervisor_artifact.profile_sha256,
                "supervisor-script-digests": dict(supervisor_artifact.script_sha256),
                "supervisor-unit-digests": dict(supervisor_artifact.unit_sha256),
                "supervisor-unit-set-digest": supervisor_unit_set_digest,
                "supervisor-controller-artifact-digests": {
                    host: artifact.artifact_digest
                    for host, artifact in controller_artifacts.items()
                },
                "supervisor-controller-unit-digests": controller_units,
                "supervisor-controller-unit-set-digests": {
                    host: external_supervisor_unit_set_digest(artifact.unit_sha256)
                    for host, artifact in controller_artifacts.items()
                },
                "unit-digests": dict(unit_digests),
                "failed-units": dict(failed_units),
                "unit-count": len(unit_digests),
                "unit-set-digest": unit_set_digest,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="systemd.render",
            failure_code="systemd.render.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("candidate.identity", "systemd.user-manager"),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "candidate.image-tag",
                "candidate.sha",
                "candidate.tree",
                "environment",
                "runner.config.sha256",
            ),
            evidence_schema=(
                EvidenceField("supervisor-artifact-digest", "sha256"),
                EvidenceField("supervisor-profile-sha256", "sha256"),
                EvidenceField("supervisor-script-digests", "string-map"),
                EvidenceField("supervisor-unit-digests", "string-map"),
                EvidenceField("supervisor-unit-set-digest", "sha256"),
                EvidenceField("supervisor-controller-artifact-digests", "string-map"),
                EvidenceField("supervisor-controller-unit-digests", "string-map"),
                EvidenceField("supervisor-controller-unit-set-digests", "string-map"),
                EvidenceField("unit-digests", "string-map"),
                EvidenceField("failed-units", "string-map"),
                EvidenceField("unit-count", "integer"),
                EvidenceField("unit-set-digest", "sha256"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=3600,
            remediation=(
                "restore exact safe candidate units, staging supervisor profile and script, "
                "then pass static systemd verification"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v4",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_systemd_render_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "supervisor-artifact-digest": "0" * 64,
            "supervisor-profile-sha256": "0" * 64,
            "supervisor-script-digests": {},
            "supervisor-unit-digests": {},
            "supervisor-unit-set-digest": "0" * 64,
            "supervisor-controller-artifact-digests": {},
            "supervisor-controller-unit-digests": {},
            "supervisor-controller-unit-set-digests": {},
            "unit-digests": {},
            "failed-units": {"candidate-units": "unavailable"},
            "unit-count": 0,
            "unit-set-digest": "0" * 64,
        },
    )


def build_image_preflight_checks(
    run: ImageDockerRunner,
    *,
    candidate_root: Path,
    image_tag: str,
    expected_candidate_sha: str,
    session: ImageBuildSession | None = None,
) -> tuple[RegisteredCheck, RegisteredCheck]:
    """Build the Tier 1 build-once and immutable image contract checks."""
    expected_plan_digest = image_plan_digest()
    image_session = session or ImageBuildSession(
        run,
        candidate_root=candidate_root,
        image_tag=image_tag,
        resolved_sha=expected_candidate_sha,
    )

    def probe_build(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != expected_candidate_sha
            or context.bindings["image.plan.sha256"] != expected_plan_digest
        ):
            return _empty_image_probe()
        try:
            artifact = image_session.build()
        except (OSError, RuntimeError, ValueError):
            return _empty_image_probe()
        return _image_probe(artifact)

    def probe_contract(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != expected_candidate_sha
            or context.bindings["image.plan.sha256"] != expected_plan_digest
        ):
            return _empty_image_probe()
        try:
            artifact = image_session.verify()
        except (OSError, RuntimeError, ValueError):
            return _empty_image_probe()
        return _image_probe(artifact)

    common_inputs = ("candidate.sha", "image.plan.sha256")
    common_evidence = (
        EvidenceField("image-digests", "string-map"),
        EvidenceField("image-count", "integer"),
        EvidenceField("plan-digest", "sha256"),
        EvidenceField("artifact-digest", "sha256"),
        EvidenceField("browser-image-id", "string"),
    )
    build = RegisteredCheck(
        spec=CheckSpec(
            check_id="images.build",
            failure_code="images.build.failed",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("docker.runtime", "candidate.identity"),
            mutation_class=MutationClass.NONE,
            input_keys=common_inputs,
            evidence_schema=common_evidence,
            timeout_seconds=3600,
            freshness_ttl_seconds=86400,
            remediation="restore Docker capacity and build every exact candidate image once",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe_build},
    )
    contract = RegisteredCheck(
        spec=CheckSpec(
            check_id="images.contract",
            failure_code="images.contract.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("images.build",),
            mutation_class=MutationClass.NONE,
            input_keys=common_inputs,
            evidence_schema=common_evidence,
            timeout_seconds=60,
            freshness_ttl_seconds=3600,
            remediation="restore exact image IDs, labels, architectures and browser entrypoint",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe_contract},
    )
    return build, contract


def _image_probe(artifact: ImageArtifactSet) -> CheckProbe:
    return CheckProbe(
        passed=True,
        evidence={
            "image-digests": dict(artifact.image_digests),
            "image-count": len(artifact.descriptors),
            "plan-digest": artifact.plan_digest,
            "artifact-digest": artifact.artifact_digest,
            "browser-image-id": artifact.descriptors[BROWSER_IMAGE].image_id,
        },
    )


def _empty_image_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "image-digests": {},
            "image-count": 0,
            "plan-digest": "0" * 64,
            "artifact-digest": "0" * 64,
            "browser-image-id": "unavailable",
        },
    )


def build_manifest_preflight_checks(
    render: RenderManifest,
    server_dry_run: ServerDryRun,
    image_artifact: Callable[[], ImageArtifactSet],
    *,
    field_ownership_dry_run: ServerDryRun | None = None,
    image_tag: str,
    namespace: str,
    expected_candidate_sha: str,
    expected_config_digest: str,
    expected_image_names: frozenset[str] | None = None,
    session: ManifestRenderSession | None = None,
    artifact_sink: Callable[[ManifestArtifact], None] | None = None,
    container_registry: str = "",
) -> tuple[RegisteredCheck, RegisteredCheck, RegisteredCheck]:
    """Build independent Tier 1 render, schema and field-owner checks."""
    manifest_session: ManifestRenderSession | None = session

    def get_session() -> ManifestRenderSession:
        nonlocal manifest_session
        if manifest_session is None:
            images = image_artifact()
            manifest_session = ManifestRenderSession(
                render,
                server_dry_run,
                field_ownership_dry_run=field_ownership_dry_run,
                image_tag=image_tag,
                namespace=namespace,
                image_digests=images.image_digests,
                expected_image_names=expected_image_names,
                container_registry=container_registry,
                registry_digests=images.registry_digests,
            )
        return manifest_session

    def bindings_match(context: CheckContext) -> bool:
        return bool(
            context.bindings["candidate.sha"] == expected_candidate_sha
            and context.bindings["runner.config.sha256"] == expected_config_digest
        )

    def probe_render(context: CheckContext) -> CheckProbe:
        if not bindings_match(context):
            return _empty_manifest_probe()
        try:
            artifact = get_session().render()
        except (OSError, RuntimeError, ValueError):
            return _empty_manifest_probe()
        if artifact_sink is not None:
            artifact_sink(artifact)
        return _manifest_probe(artifact, server_valid=False)

    def probe_server_schema(context: CheckContext) -> CheckProbe:
        if not bindings_match(context):
            return _empty_manifest_probe()
        try:
            artifact = get_session().server_validate()
        except (OSError, RuntimeError, ValueError):
            return _empty_manifest_probe()
        if artifact_sink is not None:
            artifact_sink(artifact)
        return _manifest_probe(artifact, server_valid=True)

    def probe_field_ownership(context: CheckContext) -> CheckProbe:
        if not bindings_match(context):
            return _empty_manifest_ownership_probe()
        try:
            artifact = get_session().field_ownership_validate()
        except (OSError, RuntimeError, ValueError):
            return _empty_manifest_ownership_probe()
        return CheckProbe(
            passed=True,
            evidence={
                "rendered-sha256": artifact.rendered_sha256,
                "ownership-ready": True,
                "apply-contract-digest": MANIFEST_APPLY_CONTRACT_DIGEST,
            },
        )

    common_inputs = ("candidate.sha", "runner.config.sha256")
    common_evidence = (
        EvidenceField("rendered-sha256", "sha256"),
        EvidenceField("resource-count", "integer"),
        EvidenceField("resource-set-digest", "sha256"),
        EvidenceField("image-identities", "string-map"),
        EvidenceField("artifact-digest", "sha256"),
        EvidenceField("server-valid", "boolean"),
    )
    rendered = RegisteredCheck(
        spec=CheckSpec(
            check_id="manifests.render",
            failure_code="manifests.render.failed",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("candidate.identity", "images.contract"),
            mutation_class=MutationClass.NONE,
            input_keys=common_inputs,
            evidence_schema=common_evidence,
            timeout_seconds=120,
            freshness_ttl_seconds=3600,
            remediation="restore exact candidate rendering and immutable image bindings",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe_render},
    )
    server_schema = RegisteredCheck(
        spec=CheckSpec(
            check_id="manifests.server-schema",
            failure_code="manifests.server-schema.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("manifests.render", "kubernetes.client"),
            mutation_class=MutationClass.NONE,
            input_keys=common_inputs,
            evidence_schema=common_evidence,
            timeout_seconds=120,
            freshness_ttl_seconds=3600,
            remediation=("restore API-valid exact rendered resources before another rollout"),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v3",
        operations={CheckOperation.PROBE: probe_server_schema},
    )
    field_ownership = RegisteredCheck(
        spec=CheckSpec(
            check_id="manifests.field-ownership",
            failure_code="manifests.field-ownership.conflict",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("manifests.render", "kubernetes.client"),
            mutation_class=MutationClass.NONE,
            input_keys=common_inputs,
            evidence_schema=(
                EvidenceField("rendered-sha256", "sha256"),
                EvidenceField("ownership-ready", "boolean"),
                EvidenceField("apply-contract-digest", "sha256"),
            ),
            timeout_seconds=120,
            freshness_ttl_seconds=3600,
            remediation=(
                "converge recognized legacy field ownership through the reviewed protected "
                "manager before another rollout"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe_field_ownership},
    )
    return rendered, server_schema, field_ownership


def _empty_manifest_ownership_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "rendered-sha256": "0" * 64,
            "ownership-ready": False,
            "apply-contract-digest": MANIFEST_APPLY_CONTRACT_DIGEST,
        },
    )


def build_preflight_artifact_publication_check(
    *,
    store: PreflightArtifactStore,
    image_artifact: Callable[[], ImageArtifactSet],
    manifest_artifact: Callable[[], ManifestArtifact],
    migration_manifest_artifact: Callable[[], MigrationManifestArtifact],
    production_defaults_artifact: Callable[[], ProductionDefaultsArtifact],
    candidate_sha: str,
    candidate_tree: str,
    mutation_epoch: int,
    migration_artifact: Callable[[], tuple[str, str]],
    expected_migration_policy_sha256: str,
    browser_report_schema_sha256: str,
) -> RegisteredCheck:
    """Publish the exact Tier 1 outputs consumed by the detached worker.

    ``systemd.render`` stays in immutable check evidence rather than this
    artifact store: the attestation binds that evidence hash and the detached
    rehearsal/final plans consume the exact execution directly. Duplicating it
    here would create a second artifact authority without improving ordering.
    """

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != candidate_sha
            or context.bindings["candidate.tree"] != candidate_tree
            or context.bindings["staging.mutation-epoch"] != mutation_epoch
            or context.bindings["migration.policy.sha256"] != expected_migration_policy_sha256
            or context.bindings["browser.report-schema.sha256"] != browser_report_schema_sha256
        ):
            return _empty_artifact_publication_probe()
        try:
            migration_plan_sha256, migration_target_revision = migration_artifact()
            publication = store.publish(
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                mutation_epoch=mutation_epoch,
                images=image_artifact(),
                manifests=manifest_artifact(),
                migration=migration_manifest_artifact(),
                production_defaults=production_defaults_artifact(),
                migration_plan_sha256=migration_plan_sha256,
                migration_target_revision=migration_target_revision,
                browser_report_schema_sha256=browser_report_schema_sha256,
            )
        except (OSError, RuntimeError, ValueError):
            return _empty_artifact_publication_probe()
        return CheckProbe(
            passed=True,
            evidence={
                "bundle-digest": publication.bundle_digest,
                "image-artifact-digest": publication.image_artifact_sha256,
                "manifest-artifact-digest": publication.manifest_artifact_sha256,
                "rendered-manifest-digest": publication.rendered_manifest_sha256,
                "migration-manifest-digest": publication.migration_manifest_sha256,
                "migration-artifact-digest": publication.migration_manifest_artifact_sha256,
                "production-defaults-digest": publication.production_defaults_sha256,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="artifacts.publish",
            failure_code="artifacts.publish.failed",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=(
                "images.contract",
                "manifests.server-schema",
                "migration.plan",
                "migration.manifest",
                "browser.runtime",
                "production-defaults.plan",
            ),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "browser.report-schema.sha256",
                "candidate.sha",
                "candidate.tree",
                "migration.policy.sha256",
                "staging.mutation-epoch",
            ),
            evidence_schema=(
                EvidenceField("bundle-digest", "sha256"),
                EvidenceField("image-artifact-digest", "sha256"),
                EvidenceField("manifest-artifact-digest", "sha256"),
                EvidenceField("rendered-manifest-digest", "sha256"),
                EvidenceField("migration-manifest-digest", "sha256"),
                EvidenceField("migration-artifact-digest", "sha256"),
                EvidenceField("production-defaults-digest", "sha256"),
            ),
            timeout_seconds=60,
            freshness_ttl_seconds=3600,
            remediation=(
                "restore the private preflight artifact store and publish exact build outputs"
            ),
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v3",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_artifact_publication_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "bundle-digest": "0" * 64,
            "image-artifact-digest": "0" * 64,
            "manifest-artifact-digest": "0" * 64,
            "rendered-manifest-digest": "0" * 64,
            "migration-manifest-digest": "0" * 64,
            "migration-artifact-digest": "0" * 64,
            "production-defaults-digest": "0" * 64,
        },
    )


def build_production_defaults_plan_check(
    *,
    profile_path: Path,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    environment: str,
    artifact_sink: Callable[[ProductionDefaultsArtifact], None] | None = None,
) -> RegisteredCheck:
    """Build the exact secret-free production-defaults plan in Tier 1."""

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != candidate_sha
            or context.bindings["candidate.tree"] != candidate_tree
            or context.bindings["environment"] != environment
        ):
            return _empty_production_defaults_probe()
        try:
            artifact = build_production_defaults_artifact(
                profile_path,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                image_tag=image_tag,
                environment=environment,
            )
        except (OSError, ValueError):
            return _empty_production_defaults_probe()
        if artifact_sink is not None:
            artifact_sink(artifact)
        return CheckProbe(
            passed=True,
            evidence={
                "artifact-digest": artifact.artifact_digest,
                "provider-count": len(artifact.providers),
                "rate-card-sync": artifact.yibuapi_sync is not None,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="production-defaults.plan",
            failure_code="production-defaults.plan.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("candidate.identity",),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha", "candidate.tree", "environment"),
            evidence_schema=(
                EvidenceField("artifact-digest", "sha256"),
                EvidenceField("provider-count", "integer"),
                EvidenceField("rate-card-sync", "boolean"),
            ),
            timeout_seconds=30,
            freshness_ttl_seconds=3600,
            remediation="restore valid exact production defaults in the environment profile",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_production_defaults_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "artifact-digest": "0" * 64,
            "provider-count": 0,
            "rate-card-sync": False,
        },
    )


def _manifest_probe(artifact: ManifestArtifact, *, server_valid: bool) -> CheckProbe:
    return CheckProbe(
        passed=True,
        evidence={
            "rendered-sha256": artifact.rendered_sha256,
            "resource-count": artifact.resource_count,
            "resource-set-digest": artifact.resource_set_digest,
            "image-identities": dict(artifact.image_identities),
            "artifact-digest": artifact.artifact_digest,
            "server-valid": server_valid,
        },
    )


def _empty_manifest_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "rendered-sha256": "0" * 64,
            "resource-count": 0,
            "resource-set-digest": "0" * 64,
            "image-identities": {},
            "artifact-digest": "0" * 64,
            "server-valid": False,
        },
    )


def build_browser_runtime_check(
    run: BrowserCommandRunner,
    image_artifact: Callable[[], ImageArtifactSet],
    *,
    token_path: Path,
    service_uid: int,
    service_gid: int,
    expected_candidate_sha: str,
    expected_source_set_digest: str,
) -> RegisteredCheck:
    """Build the Tier 1 exact browser image/token/container invariant."""
    expected_schema_digest = browser_report_schema_digest()

    def probe(context: CheckContext) -> CheckProbe:
        if (
            context.bindings["candidate.sha"] != expected_candidate_sha
            or context.bindings["protected-inputs.sha256"] != expected_source_set_digest
            or context.bindings["browser.report-schema.sha256"] != expected_schema_digest
        ):
            return _empty_browser_runtime_probe()
        try:
            evidence = probe_browser_runtime(
                run,
                image_artifact=image_artifact(),
                token_path=token_path,
                service_uid=service_uid,
                service_gid=service_gid,
            )
        except (OSError, RuntimeError, ValueError):
            return _empty_browser_runtime_probe()
        return CheckProbe(
            passed=evidence.launch_ready,
            evidence={
                "image-id": evidence.image_id,
                "protected-file-metadata-digest": evidence.token_metadata_fingerprint,
                "protected-file-acl-digest": evidence.token_acl_fingerprint,
                "report-schema-digest": evidence.report_schema_digest,
                "launch-ready": evidence.launch_ready,
            },
        )

    return RegisteredCheck(
        spec=CheckSpec(
            check_id="browser.runtime",
            failure_code="browser.runtime.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=("images.contract", "credentials.metadata"),
            mutation_class=MutationClass.NONE,
            input_keys=(
                "browser.report-schema.sha256",
                "candidate.sha",
                "protected-inputs.sha256",
            ),
            evidence_schema=(
                EvidenceField("image-id", "string"),
                EvidenceField("protected-file-metadata-digest", "sha256"),
                EvidenceField("protected-file-acl-digest", "sha256"),
                EvidenceField("report-schema-digest", "sha256"),
                EvidenceField("launch-ready", "boolean"),
            ),
            timeout_seconds=60,
            freshness_ttl_seconds=3600,
            remediation="restore exact browser image, private token authority and launch sandbox",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _empty_browser_runtime_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "image-id": "unavailable",
            "protected-file-metadata-digest": "0" * 64,
            "protected-file-acl-digest": "0" * 64,
            "report-schema-digest": "0" * 64,
            "launch-ready": False,
        },
    )


def build_staging_baseline_checks(
    probes: Mapping[str, ReadonlyProbe],
    *,
    environment: str,
    namespace: str,
    route: str,
    mutation_epoch: int,
    baseline_probe_route: str | None = None,
) -> tuple[RegisteredCheck, ...]:
    """Build all Tier 2 current-staging readonly baseline checks.

    ``route`` is the canonical/target route bound into the plan context (it must
    equal the ``"route"`` context binding). ``baseline_probe_route`` is the route
    the live readonly probes actually hit — identical to ``route`` except during a
    declared route transition, when the live service still serves the predecessor
    route while the plan/context already carry the target (see #936). The context
    binding stays on the target so ``bindings_match`` agrees with the plan; only
    the probe session targets the predecessor.
    """
    probe_route = route if baseline_probe_route is None else baseline_probe_route
    session = StagingBaselineSession(
        probes,
        expected_environment=environment,
        expected_namespace=namespace,
        expected_route=probe_route,
        expected_mutation_epoch=mutation_epoch,
    )
    principal_digest = readonly_authority_policy_digest()
    bindings = {
        "environment": environment,
        "namespace": namespace,
        "readonly.principal.sha256": principal_digest,
        "route": route,
        "staging.mutation-epoch": mutation_epoch,
    }

    def bindings_match(context: CheckContext) -> bool:
        return all(context.bindings[key] == value for key, value in bindings.items())

    dependencies = {
        "staging.health": ("kubernetes.client", "readonly.authority"),
        "staging.auth": ("readonly.authority", "credentials.metadata"),
        "staging.catalog-task": ("readonly.authority", "credentials.metadata"),
        "staging.storage-db": ("readonly.authority",),
        "staging.network": ("readonly.authority",),
    }
    failure_codes = {
        "staging.health": "staging.health.failed",
        "staging.auth": "staging.auth.failed",
        "staging.catalog-task": "staging.catalog-task.failed",
        "staging.storage-db": "staging.storage-db.failed",
        "staging.network": "staging.network.failed",
    }
    remediations = {
        "staging.health": "restore readonly current-staging workload health",
        "staging.auth": "restore readonly current-staging authentication baseline",
        "staging.catalog-task": "restore catalog, task, provider and worker compatibility",
        "staging.storage-db": "restore PostgreSQL and MinIO readonly health and inventory",
        "staging.network": "restore canonical staging ingress, TLS and DNS",
    }
    common_inputs = tuple(sorted(bindings))
    checks: list[RegisteredCheck] = []
    for check_id in dependencies:

        def probe(context: CheckContext, *, check_id: str = check_id) -> CheckProbe:
            if not bindings_match(context):
                return _empty_baseline_probe()
            try:
                result = session.probe(check_id)
            except (OSError, RuntimeError, ValueError):
                return _empty_baseline_probe()
            return _baseline_probe(result)

        checks.append(
            RegisteredCheck(
                spec=CheckSpec(
                    check_id=check_id,
                    failure_code=failure_codes[check_id],
                    tier=2,
                    stage=StageCapability.BASELINE_LIVE_READONLY,
                    dependencies=dependencies[check_id],
                    mutation_class=MutationClass.NONE,
                    input_keys=common_inputs,
                    evidence_schema=_baseline_evidence_schema(),
                    timeout_seconds=60,
                    freshness_ttl_seconds=120,
                    remediation=remediations[check_id],
                    secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
                ),
                implementation_version="v1",
                operations={CheckOperation.PROBE: probe},
            )
        )

    def probe_release_baseline(context: CheckContext) -> CheckProbe:
        if not bindings_match(context):
            return _empty_baseline_probe()
        try:
            return _baseline_probe(session.aggregate())
        except (OSError, RuntimeError, ValueError):
            return _empty_baseline_probe()

    checks.append(
        RegisteredCheck(
            spec=CheckSpec(
                check_id="staging.release-baseline",
                failure_code="staging.release-baseline.drift",
                tier=2,
                stage=StageCapability.BASELINE_LIVE_READONLY,
                dependencies=(
                    "staging.health",
                    "staging.auth",
                    "staging.catalog-task",
                    "staging.storage-db",
                    "staging.network",
                ),
                mutation_class=MutationClass.NONE,
                input_keys=common_inputs,
                evidence_schema=_baseline_evidence_schema(),
                timeout_seconds=10,
                freshness_ttl_seconds=120,
                remediation="restore every candidate-independent current-staging baseline",
                secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
            ),
            implementation_version="v1",
            operations={CheckOperation.PROBE: probe_release_baseline},
        )
    )
    return tuple(checks)


def _baseline_evidence_schema() -> tuple[EvidenceField, ...]:
    return (
        EvidenceField("ready", "boolean"),
        EvidenceField("readonly-principal", "string"),
        EvidenceField("observed-epoch", "integer"),
        EvidenceField("resource-digest", "sha256"),
        EvidenceField("blockers", "string-map"),
    )


def _baseline_probe(result: BaselineProbeResult) -> CheckProbe:
    return CheckProbe(
        passed=result.ready,
        evidence={
            "ready": result.ready,
            "readonly-principal": result.readonly_principal,
            "observed-epoch": result.observed_mutation_epoch,
            "resource-digest": result.resource_digest,
            "blockers": dict(result.blockers),
        },
    )


def _empty_baseline_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "ready": False,
            "readonly-principal": "unavailable",
            "observed-epoch": 0,
            "resource-digest": "0" * 64,
            "blockers": {"binding": "staging-baseline-unavailable"},
        },
    )


def build_rehearsal_checks(
    actions: Mapping[str, RehearsalAction],
    *,
    isolation_id: str,
    candidate_sha: str,
    mutation_epoch: int,
    checkpoint_evidence_digest: str,
    rehearsal_plan_digest: str,
) -> tuple[RegisteredCheck, ...]:
    """Build Tier 3 against a checkpoint that has not yet become a lease."""
    session = IsolatedRehearsalSession(
        actions,
        isolation_id=isolation_id,
        candidate_sha=candidate_sha,
        mutation_epoch=mutation_epoch,
    )
    bindings = {
        "candidate.sha": candidate_sha,
        "checkpoint.evidence.sha256": checkpoint_evidence_digest,
        "rehearsal.plan.sha256": rehearsal_plan_digest,
        "staging.mutation-epoch": mutation_epoch,
    }
    dependencies = {
        "rehearsal.namespace": ("manifests.server-schema", "staging.release-baseline"),
        "rehearsal.db-clone": ("rehearsal.namespace",),
        "rehearsal.systemd-launch": (
            "rehearsal.namespace",
            "systemd.user-manager",
            "systemd.render",
            "gb10.ssh-topology",
            "gb10.shared-mount",
            "gb10.host-readiness",
            "gb10.candidate-source",
        ),
        "rehearsal.migration": ("rehearsal.db-clone", "migration.plan"),
        "rehearsal.release": (
            "rehearsal.migration",
            "rehearsal.systemd-launch",
            "images.contract",
        ),
        "rehearsal.production-defaults": ("rehearsal.release", "production-defaults.plan"),
        "rehearsal.api-smoke": ("rehearsal.production-defaults",),
        "rehearsal.browser": ("rehearsal.api-smoke", "browser.runtime"),
        "rehearsal.cleanup": (
            "rehearsal.namespace",
            "rehearsal.db-clone",
            "rehearsal.systemd-launch",
            "rehearsal.migration",
            "rehearsal.release",
            "rehearsal.production-defaults",
            "rehearsal.api-smoke",
            "rehearsal.browser",
        ),
    }
    failure_codes = {check_id: check_id + ".failed" for check_id in dependencies}
    common_inputs = tuple(sorted(bindings))

    def bindings_match(context: CheckContext) -> bool:
        return all(context.bindings[key] == value for key, value in bindings.items())

    checks: list[RegisteredCheck] = []
    for check_id, required in dependencies.items():

        def probe(context: CheckContext, *, check_id: str = check_id) -> CheckProbe:
            if not bindings_match(context):
                return _empty_rehearsal_probe()
            try:
                result = session.execute(check_id)
            except (OSError, RuntimeError, ValueError):
                return _empty_rehearsal_probe()
            return _rehearsal_probe(result)

        checks.append(
            RegisteredCheck(
                spec=CheckSpec(
                    check_id=check_id,
                    failure_code=failure_codes[check_id],
                    tier=3,
                    stage=StageCapability.ISOLATED_REHEARSAL,
                    dependencies=required,
                    mutation_class=MutationClass.ISOLATED,
                    input_keys=common_inputs,
                    evidence_schema=_rehearsal_evidence_schema(),
                    timeout_seconds=3600,
                    freshness_ttl_seconds=3600,
                    remediation=f"restore and clean the exact isolated {check_id} action",
                    secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
                    run_after_failed_dependencies=check_id == "rehearsal.cleanup",
                ),
                implementation_version="v1",
                operations={
                    CheckOperation.PROBE: probe,
                    CheckOperation.APPLY: probe,
                    CheckOperation.VERIFY: probe,
                },
            )
        )
    return tuple(checks)


def _rehearsal_evidence_schema() -> tuple[EvidenceField, ...]:
    return (
        EvidenceField("ready", "boolean"),
        EvidenceField("isolation-id", "string"),
        EvidenceField("candidate-sha", "string"),
        EvidenceField("observed-epoch", "integer"),
        EvidenceField("evidence-digest", "sha256"),
        EvidenceField("journal-digest", "sha256"),
        EvidenceField("protected-mutation", "boolean"),
        EvidenceField("cleanup-verified", "boolean"),
        EvidenceField("blockers", "string-map"),
    )


def _rehearsal_probe(result: RehearsalResult) -> CheckProbe:
    return CheckProbe(
        passed=result.ready,
        evidence={
            "ready": result.ready,
            "isolation-id": result.isolation_id,
            "candidate-sha": result.candidate_sha,
            "observed-epoch": result.mutation_epoch,
            "evidence-digest": result.evidence_digest,
            "journal-digest": result.journal_digest,
            "protected-mutation": result.protected_mutation,
            "cleanup-verified": result.cleanup_verified,
            "blockers": dict(result.blockers),
        },
    )


def _empty_rehearsal_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "ready": False,
            "isolation-id": "unavailable",
            "candidate-sha": "unavailable",
            "observed-epoch": 0,
            "evidence-digest": "0" * 64,
            "journal-digest": "0" * 64,
            "protected-mutation": False,
            "cleanup-verified": False,
            "blockers": {"binding": "rehearsal-unavailable"},
        },
    )


def build_final_gate_checks(
    actions: Mapping[str, FinalGateAction],
    *,
    candidate_sha: str,
    attestation_digest: str,
    mutation_epoch: int,
) -> tuple[RegisteredCheck, ...]:
    """Build the complete final-only gate chain from shared implementations."""
    session = FinalGateSession(
        actions,
        candidate_sha=candidate_sha,
        attestation_digest=attestation_digest,
        mutation_epoch=mutation_epoch,
    )
    bindings = {
        "candidate.sha": candidate_sha,
        "preflight.attestation.sha256": attestation_digest,
        "staging.mutation-epoch": mutation_epoch,
    }
    dependencies = {
        "final.protected-apply": ("rehearsal.cleanup", "manifests.field-ownership"),
        "final.convergence": ("final.protected-apply",),
        "final.drift": ("final.convergence",),
        "final.smoke": ("final.drift", "rehearsal.api-smoke"),
        "final.browser": ("final.smoke", "rehearsal.browser"),
        "final.summary": ("final.browser",),
    }
    failure_codes = {
        "final.protected-apply": "final.protected-apply.failed",
        "final.convergence": "final.convergence.failed",
        "final.drift": "final.attestation-drift",
        "final.smoke": "final.smoke.failed",
        "final.browser": "final.browser.failed",
        "final.summary": "final.summary.incomplete",
    }
    justifications = {
        "final.protected-apply": (
            "Exact protected staging convergence necessarily mutates the protected namespace "
            "after rehearsal."
        ),
        "final.convergence": (
            "Only the protected live namespace can prove its final observed resource versions "
            "and convergence."
        ),
        "final.drift": (
            "The post-apply observation must bind the attestation to the protected live state "
            "after mutation."
        ),
        "final.smoke": (
            "The protected route and live data path cannot be proven by an isolated rehearsal "
            "alone."
        ),
        "final.browser": (
            "The final canonical protected route needs one acceptance proof, while token and "
            "container contracts run earlier."
        ),
        "final.summary": (
            "The terminal summary necessarily consumes evidence produced after protected "
            "convergence."
        ),
    }

    def bindings_match(context: CheckContext) -> bool:
        return all(context.bindings[key] == value for key, value in bindings.items())

    checks: list[RegisteredCheck] = []
    for check_id, required in dependencies.items():

        def execute(
            context: CheckContext,
            *,
            check_id: str = check_id,
            operation: CheckOperation,
        ) -> CheckProbe:
            if not bindings_match(context):
                return _empty_final_gate_probe()
            try:
                result = session.execute(check_id, operation)
            except (OSError, RuntimeError, ValueError):
                return _empty_final_gate_probe()
            return _final_gate_probe(result)

        def operation_probe(
            operation: CheckOperation,
            *,
            check_id: str = check_id,
        ) -> Callable[[CheckContext], CheckProbe]:
            def probe(context: CheckContext) -> CheckProbe:
                return execute(context, check_id=check_id, operation=operation)

            return probe

        operations: dict[CheckOperation, Callable[[CheckContext], CheckProbe]] = {
            CheckOperation.PROBE: operation_probe(CheckOperation.PROBE),
            CheckOperation.VERIFY: operation_probe(CheckOperation.VERIFY),
        }
        if check_id in PROTECTED_MUTATION_CHECK_IDS:
            operations[CheckOperation.PLAN] = operation_probe(CheckOperation.PLAN)
            operations[CheckOperation.APPLY] = operation_probe(CheckOperation.APPLY)
        checks.append(
            RegisteredCheck(
                spec=CheckSpec(
                    check_id=check_id,
                    failure_code=failure_codes[check_id],
                    tier=4,
                    stage=StageCapability.FINAL_ONLY,
                    dependencies=required,
                    mutation_class=(
                        MutationClass.PROTECTED_STAGING
                        if check_id in PROTECTED_MUTATION_CHECK_IDS
                        else MutationClass.NONE
                    ),
                    input_keys=tuple(sorted(bindings)),
                    evidence_schema=(
                        EvidenceField("ready", "boolean"),
                        EvidenceField("candidate-sha", "string"),
                        EvidenceField("attestation-digest", "sha256"),
                        EvidenceField("observed-epoch", "integer"),
                        EvidenceField("evidence-digest", "sha256"),
                        EvidenceField("protected-mutation", "boolean"),
                        EvidenceField("blockers", "string-map"),
                    ),
                    timeout_seconds=3600,
                    freshness_ttl_seconds=3600,
                    remediation=f"restore the exact attested {check_id} invariant",
                    secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
                    final_only_justification=justifications[check_id],
                ),
                implementation_version="v1",
                operations=operations,
            )
        )
    return tuple(checks)


def _final_gate_probe(result: FinalGateResult) -> CheckProbe:
    return CheckProbe(
        passed=result.ready,
        evidence={
            "ready": result.ready,
            "candidate-sha": result.candidate_sha,
            "attestation-digest": result.attestation_digest,
            "observed-epoch": result.observed_epoch,
            "evidence-digest": result.evidence_digest,
            "protected-mutation": result.protected_mutation,
            "blockers": dict(result.blockers),
        },
    )


def _empty_final_gate_probe() -> CheckProbe:
    return CheckProbe(
        passed=False,
        evidence={
            "ready": False,
            "candidate-sha": "unavailable",
            "attestation-digest": "0" * 64,
            "observed-epoch": 0,
            "evidence-digest": "0" * 64,
            "protected-mutation": False,
            "blockers": {"binding": "final-gate-unavailable"},
        },
    )


__all__ = [
    "CredentialProbeSource",
    "ExternalSupervisorPredecessorSnapshot",
    "ExternalSupervisorPredecessorSource",
    "build_backup_lease_eligibility_check",
    "build_backup_rotation_capacity_check",
    "build_browser_runtime_check",
    "build_candidate_identity_check",
    "build_capacity_high_water_check",
    "build_credentials_metadata_check",
    "build_docker_runtime_check",
    "build_external_supervisor_predecessor_check",
    "build_final_gate_checks",
    "build_gb10_candidate_source_check",
    "build_gb10_host_readiness_check",
    "build_gb10_shared_mount_check",
    "build_gb10_ssh_topology_check",
    "build_image_preflight_checks",
    "build_kubernetes_client_check",
    "build_lifecycle_launch_cancel_check",
    "build_manifest_preflight_checks",
    "build_migration_manifest_check",
    "build_migration_plan_check",
    "build_preflight_artifact_publication_check",
    "build_production_defaults_plan_check",
    "build_rehearsal_checks",
    "build_runner_install_check",
    "build_staging_baseline_checks",
    "build_systemd_render_check",
    "build_systemd_user_manager_check",
    "build_tools_runtime_check",
    "credential_source_set_digest",
    "gb10_mount_binding_digest",
    "gb10_target_inventory_digest",
]
