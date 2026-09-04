"""Protected policy-enabled manager and private-router desired resources."""

from __future__ import annotations

import copy
import hashlib
import ipaddress
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from cryptography import x509

from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationV2,
    canonical_executable_digest,
)
from loom_cli.capacity_control_plane import (
    load_capacity_control_plane_profile,
    render_capacity_control_plane_manifests,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact

_PROFILE_PATH = Path("deploy/dev-fleet/capacity-control-plane.toml")
_COMPONENT_LABEL = "loom.carin.dev/protected-component"
_COMPONENT_VALUE = "staging-capacity-manager-policy"
_REGISTRY_ANNOTATION = "loom.yylx.dev/principal-registry-sha256"
_MAX_REGISTRY_BYTES = 1024 * 1024
_MAX_RESOURCE_BYTES = 4 * 1024 * 1024
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_UID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_RESOURCE_VERSION_RE = re.compile(r"^[1-9][0-9]{0,31}$")
_FIELD_MANAGER = "loom-staging-capacity-manager-runtime"
_REQUEST_TIMEOUT = "60s"
_QUERY_TIMEOUT_SECONDS = 30.0
_MUTATION_TIMEOUT_SECONDS = 60.0
_ROLLOUT_TIMEOUT_SECONDS = 660.0
_ROUTER_IDENTITY = (
    "Deployment",
    "loom-capacity-router",
    "loom-capacity-manager-router",
)
_MANAGER_IDENTITY = ("Deployment", "loom-dev", "loom-capacity-manager")
_MANAGER_INGRESS_IDENTITY = (
    "NetworkPolicy",
    "loom-dev",
    "capacity-manager-ingress",
)
_MANAGER_DEPLOYMENT_MANAGER_CONTRACTS = frozenset(
    {
        ("loom-capacity-control-plane", "Apply", "apps/v1", None),
        ("kubectl-client-side-apply", "Update", "apps/v1", None),
        ("kubectl-rollout", "Update", "apps/v1", None),
        (_FIELD_MANAGER, "Update", "apps/v1", None),
        ("k3s", "Update", "apps/v1", "status"),
    }
)
_STATUS_FIELDS = {
    "schema_version",
    "authority_incarnation",
    "observer_principal_id",
    "writer_epoch",
    "configuration_epoch",
    "configuration_digest",
    "report_freshness_counts",
    "latest_shadow_epoch",
    "latest_shadow_input_digest",
    "account_slots",
    "tier_slots",
    "pool_slots",
    "blocker_counts",
    "increase_freeze",
    "execution_epoch",
    "execution_state",
    "execution_manifest_sha256",
    "executable_new_capacity_ceiling",
}

ResourceIdentity = tuple[str, str, str]


class ProtectedManagerPolicyCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes: ...

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int: ...

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ManagerPolicyRuntimeAuthority:
    """Private runtime source required to render and verify the manager."""

    authority_incarnation: UUID
    principal_registry: bytes = field(repr=False)
    server_certificate: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.authority_incarnation, UUID)
            or self.authority_incarnation.int == 0
            or not isinstance(self.principal_registry, bytes)
            or not 0 < len(self.principal_registry) <= _MAX_REGISTRY_BYTES
            or not isinstance(self.server_certificate, bytes)
            or not 0 < len(self.server_certificate) <= _MAX_RESOURCE_BYTES
        ):
            raise ValueError("protected manager policy runtime authority is invalid")
        try:
            certificate = x509.load_pem_x509_certificate(self.server_certificate)
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            san = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        except (TypeError, ValueError, x509.ExtensionNotFound) as exc:
            raise ValueError("protected manager policy server certificate is invalid") from exc
        if (
            constraints.ca
            or not certificate.not_valid_before_utc
            <= datetime.now(UTC)
            <= certificate.not_valid_after_utc
            or ipaddress.ip_address("192.168.50.103") not in san.get_values_for_type(x509.IPAddress)
        ):
            raise ValueError("protected manager policy server certificate is invalid")


@dataclass(frozen=True, slots=True)
class _Sources:
    prerequisite: ProtectedExecutionPrerequisiteArtifact
    authority: ManagerPolicyRuntimeAuthority
    resources: Mapping[ResourceIdentity, dict[str, object]]
    digest: str


@dataclass(frozen=True, slots=True)
class _PolicySnapshot:
    state: ComponentState
    evidence_digest: str
    observed: Mapping[ResourceIdentity, dict[str, object] | None]
    exact: Mapping[ResourceIdentity, bool]


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityManagerPolicyComponent:
    """Converge the policy-enabled manager and its isolated private router."""

    runner: ProtectedManagerPolicyCommandRunner
    candidate_root: Path
    container_registry: str
    prerequisite_reader: Callable[[FinalGatePlan], ProtectedExecutionPrerequisiteArtifact]
    runtime_authority_reader: Callable[[], ManagerPolicyRuntimeAuthority]
    manager_status_reader: Callable[[], Mapping[str, object]]

    def __post_init__(self) -> None:
        if (
            "KUBECONFIG" not in self.runner.environment
            or not callable(self.prerequisite_reader)
            or not callable(self.runtime_authority_reader)
            or not callable(self.manager_status_reader)
        ):
            raise ValueError("protected manager policy component authority is invalid")

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            sources = self._sources(plan)
            snapshot = self._snapshot(sources)
            if self._sources(plan).digest != sources.digest:
                raise ValueError("protected manager policy source changed")
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        return snapshot.state, snapshot.evidence_digest

    def apply(self, plan: FinalGatePlan) -> None:
        sources = self._sources(plan)
        snapshot = self._snapshot(sources)
        if snapshot.state is not ComponentState.READY:
            raise RuntimeError("protected manager policy state changed before apply")
        order = (
            ("Namespace", "", "loom-capacity-router"),
            next(key for key in sources.resources if key[0] == "ConfigMap"),
            _MANAGER_INGRESS_IDENTITY,
            (
                "NetworkPolicy",
                "loom-capacity-router",
                "capacity-manager-router-default-deny",
            ),
            (
                "NetworkPolicy",
                "loom-capacity-router",
                "capacity-manager-router-ingress",
            ),
            (
                "NetworkPolicy",
                "loom-capacity-router",
                "capacity-manager-router-egress",
            ),
            _MANAGER_IDENTITY,
            _ROUTER_IDENTITY,
        )
        router_mutation_started = False
        try:
            if snapshot.observed[_ROUTER_IDENTITY] is not None:
                router_mutation_started = True
                self._require_sources(plan, sources)
                self._disable_router(sources)
                self._require_sources(plan, sources)
                snapshot = self._snapshot(sources)
                if snapshot.state is not ComponentState.READY:
                    raise RuntimeError(
                        "protected manager policy state changed after router isolation"
                    )
            for identity in order:
                if identity == _ROUTER_IDENTITY:
                    self._rollout(_MANAGER_IDENTITY)
                    self._require_sources(plan, sources)
                if snapshot.exact[identity]:
                    continue
                self._require_sources(plan, sources)
                desired = copy.deepcopy(sources.resources[identity])
                observed = snapshot.observed[identity]
                if observed is None:
                    argv = self._create_argv(identity, dry_run=False)
                else:
                    metadata = desired["metadata"]
                    observed_metadata = observed["metadata"]
                    assert isinstance(metadata, dict) and isinstance(observed_metadata, dict)
                    metadata["uid"] = observed_metadata["uid"]
                    metadata["resourceVersion"] = observed_metadata["resourceVersion"]
                    argv = self._replace_argv(identity, dry_run=False)
                normalized = self._server_normalize(
                    identity,
                    desired,
                    replacing=observed is not None,
                )
                if identity == _ROUTER_IDENTITY:
                    router_mutation_started = True
                result = self.runner.capture_stdout_with_input(
                    argv,
                    env=self.runner.environment,
                    input_payload=_encode_document(desired),
                    timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
                )
                applied = _object(result, label="applied resource")
                if not _safe_owned(
                    applied,
                    sources.resources[identity],
                    require_dedicated=True,
                ):
                    raise RuntimeError("protected manager policy resource did not converge")
                if _projection(applied) != _projection(normalized):
                    raise RuntimeError("protected manager policy resource did not converge")
                self._require_sources(plan, sources)
            self._rollout(_ROUTER_IDENTITY)
            self._require_sources(plan, sources)
            after = self._snapshot(sources)
            if after.state is not ComponentState.EXACT:
                raise RuntimeError("protected manager policy runtime did not converge")
            self._require_sources(plan, sources)
        except (OSError, RuntimeError, UnicodeError, ValueError, KeyError) as exc:
            if not router_mutation_started:
                raise
            try:
                self._disable_router(sources)
            except (OSError, RuntimeError, UnicodeError, ValueError, KeyError) as compensation:
                raise RuntimeError(
                    "protected manager policy router compensation lost its fence"
                ) from compensation
            raise RuntimeError(
                "protected manager policy router was disabled after convergence failed"
            ) from exc

    def _sources(self, plan: FinalGatePlan) -> _Sources:
        prerequisite = self.prerequisite_reader(plan)
        authority = self.runtime_authority_reader()
        if not isinstance(authority, ManagerPolicyRuntimeAuthority):
            raise ValueError("protected manager policy runtime authority is invalid")
        resources = build_manager_policy_resource_documents(
            plan,
            prerequisite=prerequisite,
            candidate_root=self.candidate_root,
            container_registry=self.container_registry,
            authority_incarnation=authority.authority_incarnation,
            principal_registry=authority.principal_registry,
        )
        digest = _hash_json(
            {
                "artifact": prerequisite.artifact_sha256,
                "authority_incarnation": str(authority.authority_incarnation),
                "principal_registry_sha256": hashlib.sha256(
                    authority.principal_registry
                ).hexdigest(),
                "server_certificate_sha256": hashlib.sha256(
                    authority.server_certificate
                ).hexdigest(),
                "resources": {
                    "/".join(identity): hashlib.sha256(_encode_document(document)).hexdigest()
                    for identity, document in resources.items()
                },
            }
        )
        return _Sources(
            prerequisite=prerequisite,
            authority=authority,
            resources=resources,
            digest=digest,
        )

    def _require_sources(self, plan: FinalGatePlan, expected: _Sources) -> None:
        if self._sources(plan).digest != expected.digest:
            raise RuntimeError("protected manager policy source changed before mutation")

    def _snapshot(self, sources: _Sources) -> _PolicySnapshot:
        desired = sources.resources
        inventory_payloads = (
            self.runner.capture_stdout(
                self._inventory_argv(namespaced=False),
                env=self.runner.environment,
                timeout_seconds=_QUERY_TIMEOUT_SECONDS,
            ),
            self.runner.capture_stdout(
                self._inventory_argv(namespaced=True),
                env=self.runner.environment,
                timeout_seconds=_QUERY_TIMEOUT_SECONDS,
            ),
        )
        inventory = [item for payload in inventory_payloads for item in _listed_resources(payload)]
        inventory_identities = [_identity(item) for item in inventory]
        if len(inventory_identities) != len(set(inventory_identities)) or not set(
            inventory_identities
        ).issubset(set(desired)):
            return _drifted_snapshot(sources, inventory_payloads)
        observed: dict[ResourceIdentity, dict[str, object] | None] = {}
        exact: dict[ResourceIdentity, bool] = {}
        for identity, desired_document in desired.items():
            payload = self.runner.capture_stdout(
                self._get_argv(identity),
                env=self.runner.environment,
                timeout_seconds=_QUERY_TIMEOUT_SECONDS,
            )
            document = None if not payload else _object(payload, label="resource")
            observed[identity] = document
            if document is not None:
                if identity not in set(inventory_identities) and _has_component_label(document):
                    return _drifted_snapshot(sources, inventory_payloads)
                if not _safe_owned(
                    document,
                    desired_document,
                    require_dedicated=False,
                ):
                    return _drifted_snapshot(sources, inventory_payloads)
                if identity == _ROUTER_IDENTITY and not _safe_owned(
                    document,
                    desired_document,
                    require_dedicated=True,
                ):
                    return _drifted_snapshot(sources, inventory_payloads)
            diff = self.runner.run_status(
                self._diff_argv(),
                env=self.runner.environment,
                input_payload=_encode_document(desired_document),
                timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
            )
            if diff not in {0, 1}:
                raise RuntimeError("protected manager policy diff failed")
            if document is None and diff == 0:
                return _drifted_snapshot(sources, inventory_payloads)
            if document is not None and identity[0] == "ConfigMap" and diff == 1:
                return _drifted_snapshot(sources, inventory_payloads)
            exact[identity] = bool(
                document is not None
                and diff == 0
                and _safe_owned(
                    document,
                    desired_document,
                    require_dedicated=True,
                )
            )
        router_present = observed[_ROUTER_IDENTITY] is not None
        if router_present and any(
            not exact[identity]
            for identity in desired
            if identity not in {_MANAGER_IDENTITY, _ROUTER_IDENTITY}
        ):
            return _drifted_snapshot(sources, inventory_payloads)
        deployments_healthy = all(
            observed[identity] is not None and _deployment_healthy(observed[identity])
            for identity in (_MANAGER_IDENTITY, _ROUTER_IDENTITY)
        )
        all_exact = all(exact.values()) and deployments_healthy
        if all_exact:
            _validate_manager_status(
                self.manager_status_reader(),
                authority_incarnation=sources.authority.authority_incarnation,
                prerequisite=sources.prerequisite,
            )
        state = ComponentState.EXACT if all_exact else ComponentState.READY
        evidence = _hash_json(
            {
                "inventory": [hashlib.sha256(item).hexdigest() for item in inventory_payloads],
                "resources": {
                    "/".join(identity): (
                        "absent"
                        if document is None
                        else hashlib.sha256(_encode_document(_projection(document))).hexdigest()
                    )
                    for identity, document in observed.items()
                },
                "sources": sources.digest,
                "state": state.value,
            }
        )
        return _PolicySnapshot(
            state=state,
            evidence_digest=evidence,
            observed=MappingProxyType(observed),
            exact=MappingProxyType(exact),
        )

    def _rollout(self, identity: ResourceIdentity) -> None:
        self.runner.run_checked(
            (
                "kubectl",
                "--namespace",
                identity[1],
                "rollout",
                "status",
                f"deployment/{identity[2]}",
                "--timeout=600s",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            input_payload=None,
            timeout_seconds=_ROLLOUT_TIMEOUT_SECONDS,
        )

    def _disable_router(self, sources: _Sources) -> None:
        payload = self.runner.capture_stdout(
            self._get_argv(_ROUTER_IDENTITY),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        if not payload:
            return
        observed = _object(payload, label="router compensation resource")
        desired = sources.resources[_ROUTER_IDENTITY]
        if not _safe_owned(observed, desired, require_dedicated=True):
            raise RuntimeError("protected manager policy router compensation is unsafe")
        disabled = copy.deepcopy(desired)
        metadata = disabled["metadata"]
        observed_metadata = observed["metadata"]
        spec = disabled["spec"]
        assert (
            isinstance(metadata, dict)
            and isinstance(observed_metadata, dict)
            and isinstance(spec, dict)
        )
        metadata["uid"] = observed_metadata["uid"]
        metadata["resourceVersion"] = observed_metadata["resourceVersion"]
        spec["replicas"] = 0
        normalized = self._server_normalize(
            _ROUTER_IDENTITY,
            disabled,
            replacing=True,
        )
        result = self.runner.capture_stdout_with_input(
            self._replace_argv(_ROUTER_IDENTITY, dry_run=False),
            env=self.runner.environment,
            input_payload=_encode_document(disabled),
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        applied = _object(result, label="disabled router resource")
        if not _safe_owned(applied, disabled, require_dedicated=True) or _projection(
            applied
        ) != _projection(normalized):
            raise RuntimeError("protected manager policy router compensation is invalid")
        self._rollout(_ROUTER_IDENTITY)
        readback = self.runner.capture_stdout(
            self._get_argv(_ROUTER_IDENTITY),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        final = _object(readback, label="disabled router readback")
        if (
            not _safe_owned(final, disabled, require_dedicated=True)
            or _projection(final) != _projection(normalized)
            or not _deployment_scaled_to_zero(final)
        ):
            raise RuntimeError("protected manager policy router compensation is invalid")

    def _server_normalize(
        self,
        identity: ResourceIdentity,
        desired: Mapping[str, object],
        *,
        replacing: bool,
    ) -> dict[str, object]:
        argv = (
            self._replace_argv(identity, dry_run=True)
            if replacing
            else self._create_argv(identity, dry_run=True)
        )
        payload = self.runner.capture_stdout_with_input(
            argv,
            env=self.runner.environment,
            input_payload=_encode_document(desired),
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        normalized = _object(payload, label="server-normalized resource")
        if _identity(normalized) != identity:
            raise RuntimeError("protected manager policy server normalization is invalid")
        return normalized

    @staticmethod
    def _inventory_argv(*, namespaced: bool) -> tuple[str, ...]:
        resource = "configmaps,deployments,networkpolicies" if namespaced else "namespaces"
        scope = ("--all-namespaces",) if namespaced else ()
        return (
            "kubectl",
            "get",
            resource,
            *scope,
            f"--selector={_COMPONENT_LABEL}={_COMPONENT_VALUE}",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _get_argv(identity: ResourceIdentity) -> tuple[str, ...]:
        kind, namespace, name = identity
        kind_name = {
            "ConfigMap": "configmap",
            "Deployment": "deployment",
            "Namespace": "namespace",
            "NetworkPolicy": "networkpolicy",
        }[kind]
        scope = ("--namespace", namespace) if namespace else ()
        return (
            "kubectl",
            *scope,
            "get",
            f"{kind_name}/{name}",
            "--ignore-not-found=true",
            "--show-managed-fields",
            "--output=json",
            f"--request-timeout={_REQUEST_TIMEOUT}",
        )

    @staticmethod
    def _diff_argv() -> tuple[str, ...]:
        return (
            "kubectl",
            "diff",
            "--server-side=true",
            f"--field-manager={_FIELD_MANAGER}",
            "--validate=strict",
            f"--request-timeout={_REQUEST_TIMEOUT}",
            "-f",
            "-",
        )

    @staticmethod
    def _create_argv(
        identity: ResourceIdentity,
        *,
        dry_run: bool,
    ) -> tuple[str, ...]:
        scope = ("--namespace", identity[1]) if identity[1] else ()
        argv = [
            "kubectl",
            *scope,
            "create",
            f"--field-manager={_FIELD_MANAGER}",
            "--show-managed-fields",
            "--output=json",
            "--validate=strict",
            f"--request-timeout={_REQUEST_TIMEOUT}",
            "-f",
            "-",
        ]
        if dry_run:
            argv.insert(argv.index("--show-managed-fields"), "--dry-run=server")
        return tuple(argv)

    @staticmethod
    def _replace_argv(
        identity: ResourceIdentity,
        *,
        dry_run: bool,
    ) -> tuple[str, ...]:
        scope = ("--namespace", identity[1]) if identity[1] else ()
        argv = [
            "kubectl",
            *scope,
            "replace",
            f"--field-manager={_FIELD_MANAGER}",
            "--show-managed-fields",
            "--output=json",
            "--validate=strict",
            f"--request-timeout={_REQUEST_TIMEOUT}",
            "-f",
            "-",
        ]
        if dry_run:
            argv.insert(argv.index("--show-managed-fields"), "--dry-run=server")
        return tuple(argv)


def build_manager_policy_resource_documents(
    plan: FinalGatePlan,
    *,
    prerequisite: ProtectedExecutionPrerequisiteArtifact,
    candidate_root: Path,
    container_registry: str,
    authority_incarnation: UUID,
    principal_registry: bytes,
) -> Mapping[ResourceIdentity, dict[str, object]]:
    """Select only the eight policy/router resources from the canonical renderer."""

    _validate_source(
        plan,
        prerequisite=prerequisite,
        candidate_root=candidate_root,
        container_registry=container_registry,
        authority_incarnation=authority_incarnation,
        principal_registry=principal_registry,
    )
    manager_image = (
        f"{container_registry}/loom-capacity-manager@{plan.image_digests['loom-capacity-manager']}"
    )
    routes = tuple(sorted(set(prerequisite.manager_client_cidrs.values())))
    rendered = render_capacity_control_plane_manifests(
        load_capacity_control_plane_profile(candidate_root / _PROFILE_PATH),
        manager_image=manager_image,
        authority_incarnation=authority_incarnation,
        execution_policy=prerequisite.execution_policy,
        execution_policy_sha256=prerequisite.execution_policy_sha256,
        external_manager_client_cidrs=routes,
    )
    policy_name = f"loom-capacity-execution-policy-{prerequisite.execution_policy_sha256[:32]}"
    expected = {
        ("Namespace", "", "loom-capacity-router"),
        ("ConfigMap", "loom-dev", policy_name),
        ("Deployment", "loom-dev", "loom-capacity-manager"),
        ("Deployment", "loom-capacity-router", "loom-capacity-manager-router"),
        ("NetworkPolicy", "loom-dev", "capacity-manager-ingress"),
        (
            "NetworkPolicy",
            "loom-capacity-router",
            "capacity-manager-router-default-deny",
        ),
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-ingress"),
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-egress"),
    }
    selected: dict[ResourceIdentity, dict[str, object]] = {}
    for raw in yaml.safe_load_all(rendered):
        if not isinstance(raw, dict):
            raise ValueError("protected manager policy render is invalid")
        document = cast(dict[str, object], raw)
        identity = _identity(document)
        if identity not in expected:
            continue
        if identity in selected:
            raise ValueError("protected manager policy render contains duplicates")
        desired = copy.deepcopy(document)
        metadata = desired["metadata"]
        assert isinstance(metadata, dict)
        labels = metadata.get("labels")
        if not isinstance(labels, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()
        ):
            raise ValueError("protected manager policy render labels are invalid")
        labels[_COMPONENT_LABEL] = _COMPONENT_VALUE
        if identity == ("Namespace", "", "loom-capacity-router"):
            labels["kubernetes.io/metadata.name"] = "loom-capacity-router"
        if identity == ("Deployment", "loom-dev", "loom-capacity-manager"):
            spec = desired.get("spec")
            template = spec.get("template") if isinstance(spec, dict) else None
            template_metadata = template.get("metadata") if isinstance(template, dict) else None
            if not isinstance(template_metadata, dict):
                raise ValueError("protected manager policy Deployment is invalid")
            annotations = template_metadata.setdefault("annotations", {})
            if not isinstance(annotations, dict):
                raise ValueError("protected manager policy Deployment is invalid")
            annotations[_REGISTRY_ANNOTATION] = hashlib.sha256(principal_registry).hexdigest()
        selected[identity] = desired
    if set(selected) != expected:
        raise ValueError("protected manager policy render is incomplete")
    return MappingProxyType(selected)


def _validate_source(
    plan: FinalGatePlan,
    *,
    prerequisite: ProtectedExecutionPrerequisiteArtifact,
    candidate_root: Path,
    container_registry: str,
    authority_incarnation: UUID,
    principal_registry: bytes,
) -> None:
    if (
        not isinstance(plan, FinalGatePlan)
        or plan.schema_version != 7
        or not isinstance(prerequisite, ProtectedExecutionPrerequisiteArtifact)
        or plan.execution_prerequisite_artifact_sha256 != prerequisite.artifact_sha256
        or plan.execution_policy_sha256 != prerequisite.execution_policy_sha256
        or plan.execution_manager_route_sha256 != prerequisite.manager_route_sha256
        or plan.execution_access_metadata_sha256 != prerequisite.credential_metadata_manifest_sha256
        or plan.execution_coexistence_witness_sha256 != prerequisite.witness_manifest_sha256
        or plan.execution_legacy_writer_sha256 != prerequisite.legacy_writer_manifest_sha256
        or plan.execution_rollback_evidence_sha256 != prerequisite.rollback_evidence_sha256
        or plan.candidate_sha != prerequisite.candidate_sha
        or plan.candidate_tree != prerequisite.candidate_tree
        or plan.artifact_bundle_digest != prerequisite.core_artifact_bundle_sha256
        or plan.backup_lease_digest != prerequisite.backup_lease_sha256
        or not candidate_root.is_absolute()
        or ".." in candidate_root.parts
        or not container_registry
        or any(character in container_registry for character in ("\r", "\n", "\x00"))
        or not isinstance(authority_incarnation, UUID)
        or authority_incarnation.int == 0
        or not isinstance(principal_registry, bytes)
        or not 0 < len(principal_registry) <= _MAX_REGISTRY_BYTES
        or _DIGEST_RE.fullmatch(prerequisite.execution_policy_sha256) is None
    ):
        raise ValueError("protected manager policy source is invalid")
    digest = plan.image_digests.get("loom-capacity-manager")
    if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ValueError("protected manager policy image is invalid")


def _identity(document: Mapping[str, object]) -> ResourceIdentity:
    kind = document.get("kind")
    metadata = document.get("metadata")
    if (
        not isinstance(kind, str)
        or not isinstance(metadata, dict)
        or not isinstance(metadata.get("name"), str)
    ):
        raise ValueError("protected manager policy render identity is invalid")
    namespace = metadata.get("namespace", "")
    if not isinstance(namespace, str):
        raise ValueError("protected manager policy render identity is invalid")
    return kind, namespace, metadata["name"]


def _has_component_label(document: Mapping[str, object]) -> bool:
    metadata = document.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    return isinstance(labels, dict) and labels.get(_COMPONENT_LABEL) == _COMPONENT_VALUE


def _safe_owned(
    observed: Mapping[str, object],
    desired: Mapping[str, object],
    *,
    require_dedicated: bool,
) -> bool:
    try:
        if _identity(observed) != _identity(desired):
            return False
    except ValueError:
        return False
    observed_metadata = observed.get("metadata")
    desired_metadata = desired.get("metadata")
    if not isinstance(observed_metadata, dict) or not isinstance(desired_metadata, dict):
        return False
    uid = observed_metadata.get("uid")
    resource_version = observed_metadata.get("resourceVersion")
    labels = observed_metadata.get("labels")
    desired_labels = desired_metadata.get("labels")
    if (
        observed.get("apiVersion") != desired.get("apiVersion")
        or observed.get("kind") != desired.get("kind")
        or not isinstance(uid, str)
        or _UID_RE.fullmatch(uid) is None
        or not isinstance(resource_version, str)
        or _RESOURCE_VERSION_RE.fullmatch(resource_version) is None
        or not isinstance(labels, dict)
        or not isinstance(desired_labels, dict)
    ):
        return False
    identity = _identity(observed)
    dedicated_label = labels.get(_COMPONENT_LABEL) == _COMPONENT_VALUE
    if dedicated_label:
        if labels != desired_labels:
            return False
    elif (
        require_dedicated
        or identity not in {_MANAGER_IDENTITY, _MANAGER_INGRESS_IDENTITY}
        or labels
        != {key: value for key, value in desired_labels.items() if key != _COMPONENT_LABEL}
    ):
        return False
    managed = observed_metadata.get("managedFields")
    if not isinstance(managed, list) or not managed:
        return False
    dedicated_owner = False
    canonical_owner = False
    for entry in managed:
        if not isinstance(entry, dict) or entry.get("fieldsType") != "FieldsV1":
            return False
        fields = entry.get("fieldsV1")
        if not isinstance(fields, dict):
            return False
        manager = entry.get("manager")
        operation = entry.get("operation")
        subresource = entry.get("subresource")
        contract = (manager, operation, entry.get("apiVersion"), subresource)
        if identity == _MANAGER_IDENTITY and contract in _MANAGER_DEPLOYMENT_MANAGER_CONTRACTS:
            dedicated_owner = dedicated_owner or manager == _FIELD_MANAGER
            canonical_owner = canonical_owner or manager == "loom-capacity-control-plane"
            continue
        if manager == _FIELD_MANAGER and operation == "Update" and subresource is None:
            dedicated_owner = True
            continue
        if (
            manager == "loom-capacity-control-plane"
            and operation == "Apply"
            and subresource is None
        ):
            canonical_owner = True
            continue
        if (
            observed.get("kind") == "Deployment"
            and manager == "k3s"
            and operation == "Update"
            and subresource == "status"
            and set(fields) == {"f:status"}
        ):
            continue
        return False
    return dedicated_owner if require_dedicated else dedicated_owner or canonical_owner


def _deployment_healthy(document: Mapping[str, object] | None) -> bool:
    if document is None:
        return False
    metadata = document.get("metadata")
    status = document.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        return False
    generation = metadata.get("generation")
    return bool(
        type(generation) is int
        and generation > 0
        and status.get("observedGeneration") == generation
        and status.get("replicas") == 1
        and status.get("updatedReplicas") == 1
        and status.get("readyReplicas") == 1
        and status.get("availableReplicas") == 1
        and status.get("unavailableReplicas") in {None, 0}
        and status.get("terminatingReplicas") in {None, 0}
    )


def _deployment_scaled_to_zero(document: Mapping[str, object]) -> bool:
    metadata = document.get("metadata")
    spec = document.get("spec")
    status = document.get("status")
    if not isinstance(metadata, dict) or not isinstance(spec, dict) or not isinstance(status, dict):
        return False
    generation = metadata.get("generation")
    return bool(
        type(generation) is int
        and generation > 0
        and spec.get("replicas") == 0
        and status.get("observedGeneration") == generation
        and status.get("replicas") in {None, 0}
        and status.get("updatedReplicas") in {None, 0}
        and status.get("readyReplicas") in {None, 0}
        and status.get("availableReplicas") in {None, 0}
        and status.get("unavailableReplicas") in {None, 0}
        and status.get("terminatingReplicas") in {None, 0}
    )


def _validate_manager_status(
    status: Mapping[str, object],
    *,
    authority_incarnation: UUID,
    prerequisite: ProtectedExecutionPrerequisiteArtifact,
) -> None:
    if not isinstance(status, Mapping) or set(status) != _STATUS_FIELDS:
        raise ValueError("protected manager policy status is not frozen")
    writer_epoch = status.get("writer_epoch")
    configuration_epoch = status.get("configuration_epoch")
    configuration_digest = status.get("configuration_digest")
    execution_epoch = status.get("execution_epoch")
    execution_state = status.get("execution_state")
    execution_manifest_sha256 = status.get("execution_manifest_sha256")
    if (
        status.get("schema_version") != 1
        or status.get("authority_incarnation") != str(authority_incarnation)
        or status.get("observer_principal_id") != "manager-read"
        or type(writer_epoch) is not int
        or writer_epoch < 0
        or type(configuration_epoch) is not int
        or configuration_epoch < 1
        or not isinstance(configuration_digest, str)
        or _DIGEST_RE.fullmatch(configuration_digest) is None
        or status.get("increase_freeze") is not True
        or status.get("executable_new_capacity_ceiling") != 0
        or any(
            not isinstance(status.get(name), Mapping)
            for name in (
                "report_freshness_counts",
                "account_slots",
                "tier_slots",
                "pool_slots",
                "blocker_counts",
            )
        )
        or (
            status.get("latest_shadow_epoch") is not None
            and type(status.get("latest_shadow_epoch")) is not int
        )
        or (
            status.get("latest_shadow_input_digest") is not None
            and (
                not isinstance(status.get("latest_shadow_input_digest"), str)
                or _DIGEST_RE.fullmatch(cast(str, status.get("latest_shadow_input_digest"))) is None
            )
        )
    ):
        raise ValueError("protected manager policy status is not frozen")
    if execution_state == "shadow":
        if execution_epoch != 0 or execution_manifest_sha256 is not None:
            raise ValueError("protected manager policy status is not frozen")
        return
    if (
        execution_state != "prepared"
        or type(execution_epoch) is not int
        or execution_epoch <= 0
        or not isinstance(execution_manifest_sha256, str)
        or _DIGEST_RE.fullmatch(execution_manifest_sha256) is None
        or writer_epoch <= 0
    ):
        raise ValueError("protected manager policy status is not frozen")
    policy = prerequisite.execution_policy
    request = ExecutionPreparationV2(
        authority_incarnation=authority_incarnation,
        expected_writer_epoch=writer_epoch,
        configuration_epoch=configuration_epoch,
        fleet_generation=prerequisite.desired_fleet_generation,
        fleet_digest=prerequisite.desired_fleet_sha256,
        trusted_fleet_release_sha256=policy.trusted_fleet_release_sha256,
        requested_ceiling=policy.executable_new_capacity_ceiling,
        requested_rate_per_minute=policy.executable_new_capacity_rate_per_minute,
        executors=policy.executors,
        subject_acknowledgements=policy.subject_acknowledgements,
        legacy_writer_fences=policy.legacy_writer_fences,
        rollback_evidence_sha256=policy.rollback_evidence_sha256,
    )
    if execution_manifest_sha256 != canonical_executable_digest(request):
        raise ValueError("protected manager policy prepared manifest is not exact")


def _projection(document: Mapping[str, object]) -> dict[str, object]:
    projected = copy.deepcopy(dict(document))
    projected.pop("status", None)
    metadata = projected.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("protected manager policy metadata is invalid")
    for field_name in (
        "creationTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "uid",
    ):
        metadata.pop(field_name, None)
    spec = projected.get("spec")
    if isinstance(spec, dict):
        template = spec.get("template")
        if isinstance(template, dict):
            template_metadata = template.get("metadata")
            if isinstance(template_metadata, dict):
                template_metadata.pop("creationTimestamp", None)
    return projected


def _drifted_snapshot(
    sources: _Sources,
    inventory_payloads: Sequence[bytes],
) -> _PolicySnapshot:
    return _PolicySnapshot(
        state=ComponentState.DRIFTED,
        evidence_digest=_hash_json(
            {
                "inventory": [hashlib.sha256(item).hexdigest() for item in inventory_payloads],
                "sources": sources.digest,
                "status": "unsafe",
            }
        ),
        observed=MappingProxyType({}),
        exact=MappingProxyType({}),
    )


def _listed_resources(payload: bytes) -> list[dict[str, object]]:
    value = _object(payload, label="inventory")
    items = value.get("items")
    if not isinstance(items, list):
        raise ValueError("protected manager policy inventory is invalid")
    result: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("protected manager policy inventory is invalid")
        result.append(cast(dict[str, object], item))
    return result


def _object(payload: bytes, *, label: str) -> dict[str, object]:
    if not payload or len(payload) > _MAX_RESOURCE_BYTES:
        raise ValueError(f"protected manager policy {label} is invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise ValueError(f"protected manager policy {label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"protected manager policy {label} is invalid")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("protected manager policy JSON is ambiguous")
        result[key] = value
    return result


def _encode_document(document: Mapping[str, object]) -> bytes:
    return cast(
        str,
        yaml.safe_dump(document, sort_keys=True, explicit_start=True),
    ).encode("ascii")


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


__all__ = [
    "KubernetesProtectedStagingCapacityManagerPolicyComponent",
    "ManagerPolicyRuntimeAuthority",
    "build_manager_policy_resource_documents",
]
