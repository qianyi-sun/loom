"""Protected staging capacity composition for the installed final gate."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)
from .protected_capacity_execution_preparation_component import (
    ExecutionPreparationDependencyGuard,
    ExecutionPreparationManagerClient,
    KubernetesProtectedCapacityExecutionPreparationComponent,
    PreparedControllerTransport,
)
from .protected_capacity_manager_client import open_protected_capacity_manager_client
from .protected_controller_prerequisite_component import (
    KubernetesProtectedControllerPrerequisiteComponent,
    ProtectedControllerPrerequisiteTransport,
)
from .protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisitePublication,
    ProtectedExecutionPrerequisiteStore,
)
from .protected_execution_prerequisites import ProtectedExecutionPrerequisiteArtifact
from .protected_pool_credential_transport import ProtectedPoolCredentialTransport
from .protected_staging_capacity_agent_component import (
    KubernetesProtectedStagingCapacityAgentComponent,
)
from .protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
)
from .protected_staging_capacity_execution_credential_component import (
    KubernetesProtectedStagingExecutionCredentialComponent,
)
from .protected_staging_capacity_execution_credentials import (
    ExecutionCredentialBundle,
    load_execution_credential_bundle,
)
from .protected_staging_capacity_manager_configuration_component import (
    ClientContext,
    KubernetesProtectedStagingCapacityManagerConfigurationComponent,
)
from .protected_staging_capacity_manager_runtime_component import (
    KubernetesProtectedStagingCapacityManagerRuntimeComponent,
)
from .protected_staging_capacity_runtime_secret_component import (
    KubernetesProtectedStagingCapacityRuntimeSecretComponent,
)

_COMPONENT_IDS = (
    "staging-capacity-credentials",
    "staging-capacity-database",
    "staging-protected-runtime-secret",
    "capacity-manager-runtime",
    "capacity-manager-configuration",
    "staging-capacity-agent",
)
_EXECUTION_CREDENTIAL_COMPONENT_ID = "staging-capacity-execution-credentials"
_EXECUTION_PREPARATION_COMPONENT_ID = "capacity-execution-preparation"
_CONTROLLER_PREREQUISITE_COMPONENT_POOLS = {
    "oldlab-controller-prerequisite": "oldlab",
    "gb10-controller-prerequisite": "gb10",
}
_CREDENTIAL_DIRECTORY_NAMES = (
    "configuration-read",
    "configuration-fleet",
    "configuration-subject",
    "configuration-activate",
    "staging-reporter",
)
_EXECUTION_CREDENTIAL_DIRECTORY_NAMES = (
    "manager-read",
    "manager-prepare",
    "manager-activate",
    "manager-drain",
    "manager-retire",
    "manager-abort",
    "pool-executor-gb10",
    "pool-executor-oldlab",
    "pool-ownership-gb10",
    "pool-ownership-oldlab",
)
_CREDENTIAL_ROOT_FILE_NAMES = ("client-ca.pem",)
_CONFIGURATION_CLIENT_FILE_NAMES = (
    "bearer-token",
    "certificate.pem",
    "manager-ca.pem",
    "private-key.pem",
)
_STAGING_REPORTER_FILE_NAMES = (
    "certificate.pem",
    "manager-ca.pem",
    "private-key.pem",
)
_CREDENTIAL_SEED_KEYS = frozenset(
    {
        "agent_database_password",
        "agent_incarnation",
        "authority_incarnation",
        "migrator_database_password",
        "observer_database_password",
        "reporter_incarnation",
        "reporter_token",
        "runtime_database_password",
        "schema_version",
        "subject_id",
        "subject_incarnation",
    }
)
_SUBJECT_ID = uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")
_SUBJECT_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1")
_AUTHORITY_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-authority:v1")
_AGENT_INCARNATION = uuid5(NAMESPACE_URL, "loom:staging:capacity-agent:v1")
_MAX_PRIVATE_FILE_BYTES = 1024 * 1024


class ProtectedStagingCapacityCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
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

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityRuntime:
    """Bind staging capacity convergence to the installed rollout authority."""

    runner: ProtectedStagingCapacityCommandRunner
    state_root: Path
    candidate_root: Path
    service_uid: int
    service_gid: int
    container_registry: str
    manager_configuration_client_context: ClientContext = open_protected_capacity_manager_client
    controller_prerequisite_transports: Mapping[str, ProtectedControllerPrerequisiteTransport] = (
        field(default_factory=dict)
    )
    pool_credential_transports: Mapping[str, ProtectedPoolCredentialTransport] = field(
        default_factory=dict
    )
    prepared_controller_transports: Mapping[str, PreparedControllerTransport] = field(
        default_factory=dict
    )
    execution_preparation_dependency_guard: ExecutionPreparationDependencyGuard | None = None

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or not self.candidate_root.is_absolute()
            or ".." in self.state_root.parts
            or ".." in self.candidate_root.parts
            or self.service_uid < 0
            or self.service_gid < 0
            or any(item in self.container_registry for item in ("\r", "\n", "\x00"))
            or any(
                pool_id not in {"gb10", "oldlab"}
                or not callable(getattr(transport, "observe", None))
                or not callable(getattr(transport, "publish", None))
                for pool_id, transport in self.pool_credential_transports.items()
            )
            or any(
                pool_id not in {"gb10", "oldlab"}
                or not callable(getattr(transport, "observe", None))
                or not callable(getattr(transport, "converge", None))
                for pool_id, transport in self.controller_prerequisite_transports.items()
            )
            or any(
                pool_id not in {"gb10", "oldlab"}
                or not callable(getattr(transport, "observe", None))
                or not callable(getattr(transport, "converge_files", None))
                or not callable(getattr(transport, "enable_timer", None))
                or not callable(getattr(transport, "run_tick", None))
                or not callable(getattr(transport, "disable_timer", None))
                for pool_id, transport in self.prepared_controller_transports.items()
            )
            or (
                self.execution_preparation_dependency_guard is not None
                and not callable(self.execution_preparation_dependency_guard)
            )
        ):
            raise ValueError("protected staging capacity runtime authority is invalid")
        object.__setattr__(
            self,
            "controller_prerequisite_transports",
            MappingProxyType(dict(self.controller_prerequisite_transports)),
        )
        object.__setattr__(
            self,
            "pool_credential_transports",
            MappingProxyType(dict(self.pool_credential_transports)),
        )
        object.__setattr__(
            self,
            "prepared_controller_transports",
            MappingProxyType(dict(self.prepared_controller_transports)),
        )

    @property
    def credentials_root(self) -> Path:
        return self.state_root / "protected-capacity" / "credentials"

    @property
    def credential_seed_path(self) -> Path:
        return self.credentials_root / "seed.json"

    def components(
        self,
        plan: FinalGatePlan,
        *,
        epoch_guard: Callable[[FinalGatePlan], ComponentObservation],
    ) -> tuple[ProtectedApplyComponent, ...]:
        if not callable(epoch_guard):
            raise ValueError("protected staging capacity epoch authority is invalid")

        def build(component_id: str) -> ProtectedApplyComponent:
            def classify(bound_plan: FinalGatePlan) -> ComponentObservation:
                epoch = epoch_guard(bound_plan)
                if epoch.state is not ComponentState.EXACT:
                    return self._observation(
                        bound_plan,
                        component_id=component_id,
                        state=ComponentState.DRIFTED,
                        epoch=epoch,
                        component_evidence="0" * 64,
                    )
                return self._classify(component_id, bound_plan, epoch)

            def apply(bound_plan: FinalGatePlan) -> None:
                epoch = epoch_guard(bound_plan)
                if epoch.state is not ComponentState.EXACT:
                    raise RuntimeError(
                        f"protected staging capacity epoch ownership changed before {component_id}"
                    )
                before = self._classify(component_id, bound_plan, epoch)
                if before.state is not ComponentState.READY:
                    raise RuntimeError(
                        f"protected staging capacity state changed before {component_id}"
                    )
                self._apply(component_id, bound_plan)

            return ProtectedApplyComponent(
                component_id=component_id,
                implementation_digest=_hash_json(
                    {
                        "component_id": component_id,
                        "implementation": "loom-protected-staging-capacity-v1",
                    }
                ),
                input_fingerprint=_hash_json(
                    {
                        "candidate_sha": plan.candidate_sha,
                        "candidate_tree": plan.candidate_tree,
                        "component_id": component_id,
                        "container_registry": self.container_registry,
                        "plan_digest": plan.plan_digest,
                        "starting_epoch": plan.starting_mutation_epoch,
                    }
                ),
                classify=classify,
                apply=apply,
            )

        component_ids: tuple[str, ...] = _COMPONENT_IDS
        if plan.schema_version == 7:
            if set(self.controller_prerequisite_transports) != {"gb10", "oldlab"}:
                raise ValueError("controller prerequisite transports are incomplete")
            if set(self.pool_credential_transports) != {"gb10", "oldlab"}:
                raise ValueError("pool credential transports are incomplete")
            if set(self.prepared_controller_transports) != {"gb10", "oldlab"}:
                raise ValueError("prepared controller transports are incomplete")
            if self.execution_preparation_dependency_guard is None:
                raise ValueError("execution preparation dependency guard is unavailable")
            component_ids = (
                *_COMPONENT_IDS[:3],
                *_CONTROLLER_PREREQUISITE_COMPONENT_POOLS,
                _EXECUTION_CREDENTIAL_COMPONENT_ID,
                *_COMPONENT_IDS[3:],
                _EXECUTION_PREPARATION_COMPONENT_ID,
            )
        return tuple(build(component_id) for component_id in component_ids)

    def _classify(
        self,
        component_id: str,
        plan: FinalGatePlan,
        epoch: ComponentObservation,
    ) -> ComponentObservation:
        if component_id == "staging-capacity-credentials":
            state, evidence = self._classify_credentials()
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == "staging-capacity-database":
            state, evidence = self._database_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == "staging-protected-runtime-secret":
            state, evidence = self._runtime_secret_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        prerequisite_pool = _CONTROLLER_PREREQUISITE_COMPONENT_POOLS.get(component_id)
        if prerequisite_pool is not None:
            state, evidence = self._controller_prerequisite_component(prerequisite_pool).classify(
                plan
            )
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == _EXECUTION_CREDENTIAL_COMPONENT_ID:
            state, evidence = self._execution_credential_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == "capacity-manager-runtime":
            state, evidence = self._manager_runtime_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == "capacity-manager-configuration":
            state, evidence = self._manager_configuration_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == "staging-capacity-agent":
            state, evidence = self._agent_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        if component_id == _EXECUTION_PREPARATION_COMPONENT_ID:
            state, evidence = self._execution_preparation_component().classify(plan)
            return self._observation(
                plan,
                component_id=component_id,
                state=state,
                epoch=epoch,
                component_evidence=evidence,
            )
        return self._observation(
            plan,
            component_id=component_id,
            state=ComponentState.DRIFTED,
            epoch=epoch,
            component_evidence=_hash_json({"status": "authority-unavailable"}),
        )

    def _apply(self, component_id: str, plan: FinalGatePlan) -> None:
        if component_id == "staging-capacity-credentials":
            self._create_credential_seed()
            return
        if component_id == "staging-capacity-database":
            self._database_component().apply(plan)
            return
        if component_id == "staging-protected-runtime-secret":
            self._runtime_secret_component().apply(plan)
            return
        prerequisite_pool = _CONTROLLER_PREREQUISITE_COMPONENT_POOLS.get(component_id)
        if prerequisite_pool is not None:
            self._controller_prerequisite_component(prerequisite_pool).apply(plan)
            return
        if component_id == _EXECUTION_CREDENTIAL_COMPONENT_ID:
            self._execution_credential_component().apply(plan)
            return
        if component_id == "capacity-manager-runtime":
            self._manager_runtime_component().apply(plan)
            return
        if component_id == "capacity-manager-configuration":
            self._manager_configuration_component().apply(plan)
            return
        if component_id == "staging-capacity-agent":
            self._agent_component().apply(plan)
            return
        if component_id == _EXECUTION_PREPARATION_COMPONENT_ID:
            self._execution_preparation_component().apply(plan)
            return
        raise RuntimeError(f"protected staging capacity mutation is unavailable for {component_id}")

    def _database_component(self) -> KubernetesProtectedStagingCapacityDatabaseComponent:
        return KubernetesProtectedStagingCapacityDatabaseComponent(
            runner=self.runner,
            container_registry=self.container_registry,
            seed_reader=self.read_credential_seed,
        )

    def _runtime_secret_component(
        self,
    ) -> KubernetesProtectedStagingCapacityRuntimeSecretComponent:
        return KubernetesProtectedStagingCapacityRuntimeSecretComponent(
            runner=self.runner,
            seed_reader=self.read_credential_seed,
        )

    def _manager_runtime_component(
        self,
    ) -> KubernetesProtectedStagingCapacityManagerRuntimeComponent:
        return KubernetesProtectedStagingCapacityManagerRuntimeComponent(
            runner=self.runner,
            candidate_root=self.candidate_root,
            container_registry=self.container_registry,
            seed_reader=self.read_credential_seed,
            prerequisite_reader=self._read_execution_prerequisite,
            manager_status_reader=self._read_manager_status,
        )

    def _controller_prerequisite_component(
        self,
        pool_id: str,
    ) -> KubernetesProtectedControllerPrerequisiteComponent:
        return KubernetesProtectedControllerPrerequisiteComponent(
            pool_id=pool_id,
            transport=self.controller_prerequisite_transports[pool_id],
            prerequisite_reader=self._read_execution_prerequisite,
        )

    def _read_manager_status(self) -> Mapping[str, object]:
        with self.manager_configuration_client_context(
            runner=self.runner,
            credentials_root=self.credentials_root,
            service_uid=self.service_uid,
            service_gid=self.service_gid,
        ) as client:
            return client.get_status()

    def _execution_credential_component(
        self,
    ) -> KubernetesProtectedStagingExecutionCredentialComponent:
        return KubernetesProtectedStagingExecutionCredentialComponent(
            runner=self.runner,
            credential_bundle_reader=self.read_execution_credential_bundle,
            prerequisite_reader=self._read_execution_prerequisite,
            pool_credential_transports=self.pool_credential_transports,
        )

    def _execution_preparation_component(
        self,
    ) -> KubernetesProtectedCapacityExecutionPreparationComponent:
        if self.execution_preparation_dependency_guard is None:
            raise ValueError("execution preparation dependency guard is unavailable")

        def client_context() -> AbstractContextManager[ExecutionPreparationManagerClient]:
            return cast(
                AbstractContextManager[ExecutionPreparationManagerClient],
                self.manager_configuration_client_context(
                    runner=self.runner,
                    credentials_root=self.credentials_root,
                    service_uid=self.service_uid,
                    service_gid=self.service_gid,
                ),
            )

        return KubernetesProtectedCapacityExecutionPreparationComponent(
            state_root=self.state_root,
            service_uid=self.service_uid,
            client_context=client_context,
            prerequisite_reader=self._read_execution_prerequisite,
            dependency_guard=self._execution_preparation_dependency,
            controller_prerequisite_transports=self.controller_prerequisite_transports,
            prepared_controller_transports=self.prepared_controller_transports,
        )

    def _execution_preparation_dependency(
        self,
        plan: FinalGatePlan,
        artifact: ProtectedExecutionPrerequisiteArtifact,
    ) -> str:
        components = (
            ("gb10-controller-prerequisite", self._controller_prerequisite_component("gb10")),
            (
                "oldlab-controller-prerequisite",
                self._controller_prerequisite_component("oldlab"),
            ),
            (_EXECUTION_CREDENTIAL_COMPONENT_ID, self._execution_credential_component()),
            ("capacity-manager-runtime", self._manager_runtime_component()),
            ("capacity-manager-configuration", self._manager_configuration_component()),
        )
        evidence: dict[str, str] = {}
        for component_id, component in components:
            state, component_evidence = component.classify(plan)
            if state is not ComponentState.EXACT:
                raise RuntimeError(
                    f"protected execution preparation dependency {component_id} is not exact"
                )
            evidence[component_id] = component_evidence

        dependency_guard = self.execution_preparation_dependency_guard
        if dependency_guard is None:
            raise ValueError("execution preparation dependency guard is unavailable")
        external_evidence = dependency_guard(plan, artifact)
        if (
            not isinstance(external_evidence, str)
            or len(external_evidence) != 64
            or any(character not in "0123456789abcdef" for character in external_evidence)
            or external_evidence == "0" * 64
        ):
            raise ValueError("execution preparation dependency evidence is invalid")
        return _hash_json(
            {
                "component_evidence": evidence,
                "external_evidence": external_evidence,
            }
        )

    def _read_execution_prerequisite(
        self,
        plan: FinalGatePlan,
    ) -> ProtectedExecutionPrerequisiteArtifact:
        path = Path(plan.execution_prerequisite_artifact_path or "")
        digest = plan.execution_prerequisite_artifact_sha256
        expected_root = self.state_root / "execution-prerequisites"
        if path.parent != expected_root or not isinstance(digest, str):
            raise ValueError("protected execution prerequisite publication is invalid")
        store = ProtectedExecutionPrerequisiteStore(
            self.state_root,
            service_uid=self.service_uid,
        )
        return store.read(
            ProtectedExecutionPrerequisitePublication(
                path=path,
                artifact_sha256=digest,
            )
        )

    def _manager_configuration_component(
        self,
    ) -> KubernetesProtectedStagingCapacityManagerConfigurationComponent:
        return KubernetesProtectedStagingCapacityManagerConfigurationComponent(
            runner=self.runner,
            credentials_root=self.credentials_root,
            service_uid=self.service_uid,
            service_gid=self.service_gid,
            seed_reader=self.read_credential_seed,
            client_context=self.manager_configuration_client_context,
        )

    def _agent_component(self) -> KubernetesProtectedStagingCapacityAgentComponent:
        return KubernetesProtectedStagingCapacityAgentComponent(
            runner=self.runner,
            container_registry=self.container_registry,
            seed_reader=self.read_credential_seed,
            reporter_tls_reader=self._read_staging_reporter_tls,
            postgres_ca_reader=self._read_postgres_ca,
        )

    def _read_staging_reporter_tls(self) -> dict[str, bytes]:
        self._read_credential_bootstrap()
        directory = self.credentials_root / "staging-reporter"
        return {
            file_name: self._read_private_file(directory / file_name)
            for file_name in _STAGING_REPORTER_FILE_NAMES
        }

    def _read_postgres_ca(self) -> bytes:
        return self._runtime_secret_component()._read_ca_certificate()

    def _classify_credentials(self) -> tuple[ComponentState, str]:
        try:
            bootstrap = self._read_credential_bootstrap()
        except (OSError, RuntimeError, ValueError):
            return ComponentState.DRIFTED, _hash_json({"status": "bootstrap-drifted"})
        if not self.credential_seed_path.exists():
            return ComponentState.READY, _hash_json(
                {"bootstrap": bootstrap, "status": "seed-absent"}
            )
        try:
            payload = self._read_private_file(self.credential_seed_path)
            self._parse_credential_seed(payload)
        except (OSError, RuntimeError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            return ComponentState.DRIFTED, _hash_json(
                {"bootstrap": bootstrap, "status": "seed-drifted"}
            )
        return ComponentState.EXACT, _hash_json(
            {
                "bootstrap": bootstrap,
                "seed_sha256": hashlib.sha256(payload).hexdigest(),
                "status": "exact",
            }
        )

    def read_credential_seed(self) -> dict[str, object]:
        """Return one strictly validated private staging credential seed."""
        return self._parse_credential_seed(self._read_private_file(self.credential_seed_path))

    def read_execution_credential_bundle(self) -> ExecutionCredentialBundle:
        """Return the strictly validated execution-only bootstrap subset."""
        return load_execution_credential_bundle(
            self.credentials_root,
            expected_uid=self.service_uid,
            expected_gid=self.service_gid,
        )

    def read_execution_credential_metadata(self) -> dict[str, str]:
        """Return exact secret-free metadata for prerequisite production."""
        return dict(self.read_execution_credential_bundle().metadata_sha256)

    def _parse_credential_seed(self, payload: bytes) -> dict[str, object]:
        seed = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        self._validate_credential_seed(seed)
        assert isinstance(seed, dict)
        return seed

    def _read_credential_bootstrap(self) -> dict[str, dict[str, str]]:
        self._validate_private_directory(self.state_root)
        protected_root = self.credentials_root.parent
        self._validate_private_directory(protected_root)
        self._validate_private_directory(self.credentials_root)
        expected_root_entries = (
            set(_CREDENTIAL_DIRECTORY_NAMES)
            | set(_EXECUTION_CREDENTIAL_DIRECTORY_NAMES)
            | set(_CREDENTIAL_ROOT_FILE_NAMES)
        )
        if self.credential_seed_path.exists():
            expected_root_entries.add(self.credential_seed_path.name)
        if {path.name for path in self.credentials_root.iterdir()} != expected_root_entries:
            raise ValueError("protected staging credential root contains unexpected entries")
        client_ca_payload = self._read_private_file(self.credentials_root / "client-ca.pem")
        client_ca = x509.load_pem_x509_certificate(client_ca_payload)
        try:
            client_ca_constraints = client_ca.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            client_ca.verify_directly_issued_by(client_ca)
        except (TypeError, ValueError, x509.ExtensionNotFound) as exc:
            raise ValueError("protected staging client CA is invalid") from exc
        now = datetime.now(UTC)
        if (
            not client_ca_constraints.ca
            or not client_ca.not_valid_before_utc <= now <= client_ca.not_valid_after_utc
        ):
            raise ValueError("protected staging client CA is invalid")
        client_ca_der = client_ca.public_bytes(serialization.Encoding.DER)
        client_ca_public_key = client_ca.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        evidence: dict[str, dict[str, str]] = {
            "client-ca": {"client-ca.pem": hashlib.sha256(client_ca_payload).hexdigest()}
        }
        manager_ca_der: bytes | None = None
        public_keys: set[bytes] = set()
        for directory_name in _CREDENTIAL_DIRECTORY_NAMES:
            directory = self.credentials_root / directory_name
            self._validate_private_directory(directory)
            expected_files = (
                _STAGING_REPORTER_FILE_NAMES
                if directory_name == "staging-reporter"
                else _CONFIGURATION_CLIENT_FILE_NAMES
            )
            if {path.name for path in directory.iterdir()} != set(expected_files):
                raise ValueError("protected staging credential client is incomplete")
            payloads = {
                file_name: self._read_private_file(directory / file_name)
                for file_name in expected_files
            }
            certificate = x509.load_pem_x509_certificate(payloads["certificate.pem"])
            manager_ca = x509.load_pem_x509_certificate(payloads["manager-ca.pem"])
            private_key = serialization.load_pem_private_key(
                payloads["private-key.pem"], password=None
            )
            try:
                manager_ca_constraints = manager_ca.extensions.get_extension_for_class(
                    x509.BasicConstraints
                ).value
                manager_ca.verify_directly_issued_by(manager_ca)
            except x509.ExtensionNotFound as exc:
                raise ValueError("protected staging manager CA is invalid") from exc
            except (TypeError, ValueError) as exc:
                raise ValueError("protected staging manager CA is invalid") from exc
            if not manager_ca_constraints.ca:
                raise ValueError("protected staging manager CA is invalid")
            if not (
                manager_ca.not_valid_before_utc <= now <= manager_ca.not_valid_after_utc
                and certificate.not_valid_before_utc <= now <= certificate.not_valid_after_utc
            ):
                raise ValueError("protected staging credential certificate is not current")
            try:
                certificate.verify_directly_issued_by(client_ca)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    "protected staging credential certificate issuer is invalid"
                ) from exc
            certificate_public_key = certificate.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            try:
                private_public_key = private_key.public_key().public_bytes(
                    serialization.Encoding.DER,
                    serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            except AttributeError as exc:
                raise ValueError("protected staging private key is invalid") from exc
            if certificate_public_key != private_public_key:
                raise ValueError("protected staging certificate and key do not match")
            ca_der = manager_ca.public_bytes(serialization.Encoding.DER)
            manager_ca_public_key = manager_ca.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            if ca_der == client_ca_der or manager_ca_public_key == client_ca_public_key:
                raise ValueError("protected staging manager and client CAs overlap")
            if manager_ca_der is None:
                manager_ca_der = ca_der
            elif ca_der != manager_ca_der:
                raise ValueError("protected staging credentials use different manager CAs")
            if certificate_public_key in public_keys:
                raise ValueError("protected staging credential private key is reused")
            public_keys.add(certificate_public_key)
            if "bearer-token" in payloads:
                self._validate_opaque_credential(payloads["bearer-token"])
            evidence[directory_name] = {
                file_name: hashlib.sha256(payload).hexdigest()
                for file_name, payload in payloads.items()
            }
        execution_bundle = self.read_execution_credential_bundle()
        evidence.update(
            {
                name: {"metadata-sha256": digest}
                for name, digest in execution_bundle.metadata_sha256.items()
            }
        )
        return evidence

    @staticmethod
    def _validate_opaque_credential(payload: bytes) -> None:
        if not 32 <= len(payload) <= 4096 or any(not 0x21 <= byte <= 0x7E for byte in payload):
            raise ValueError("protected staging opaque credential is invalid")

    def _validate_private_directory(self, path: Path) -> None:
        metadata = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != self.service_uid
            or metadata.st_gid != self.service_gid
        ):
            raise ValueError("protected staging credential directory is unsafe")

    def _read_private_file(self, path: Path) -> bytes:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_uid != self.service_uid
                or before.st_gid != self.service_gid
                or before.st_nlink != 1
                or not 0 < before.st_size <= _MAX_PRIVATE_FILE_BYTES
            ):
                raise ValueError("protected staging credential file is unsafe")
            payload = b""
            while len(payload) <= _MAX_PRIVATE_FILE_BYTES:
                chunk = os.read(descriptor, min(65536, _MAX_PRIVATE_FILE_BYTES + 1 - len(payload)))
                if not chunk:
                    break
                payload += chunk
            after = os.fstat(descriptor)
            stable_before = (
                before.st_dev,
                before.st_ino,
                before.st_mode,
                before.st_uid,
                before.st_gid,
                before.st_nlink,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            stable_after = (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_uid,
                after.st_gid,
                after.st_nlink,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            if (
                stable_before != stable_after
                or not payload
                or len(payload) > _MAX_PRIVATE_FILE_BYTES
            ):
                raise ValueError("protected staging credential file changed while reading")
            return payload
        finally:
            os.close(descriptor)

    @staticmethod
    def _validate_credential_seed(value: object) -> None:
        if not isinstance(value, dict) or set(value) != _CREDENTIAL_SEED_KEYS:
            raise ValueError("protected staging credential seed fields are invalid")
        if value["schema_version"] != 1:
            raise ValueError("protected staging credential seed schema is invalid")
        expected_ids = {
            "agent_incarnation": _AGENT_INCARNATION,
            "authority_incarnation": _AUTHORITY_INCARNATION,
            "subject_id": _SUBJECT_ID,
            "subject_incarnation": _SUBJECT_INCARNATION,
        }
        parsed_ids: dict[str, UUID] = {}
        for seed_field in (*expected_ids, "reporter_incarnation"):
            raw = value[seed_field]
            if not isinstance(raw, str):
                raise ValueError("protected staging credential seed identity is invalid")
            parsed = UUID(raw)
            if parsed.int == 0 or str(parsed) != raw:
                raise ValueError("protected staging credential seed identity is invalid")
            parsed_ids[seed_field] = parsed
        if any(parsed_ids[field] != expected for field, expected in expected_ids.items()):
            raise ValueError("protected staging credential seed binding is invalid")
        if len(set(parsed_ids.values())) != len(parsed_ids):
            raise ValueError("protected staging credential seed identities overlap")
        for credential_field in (
            "agent_database_password",
            "migrator_database_password",
            "observer_database_password",
            "reporter_token",
            "runtime_database_password",
        ):
            raw = value[credential_field]
            if (
                not isinstance(raw, str)
                or not 32 <= len(raw.encode("ascii")) <= 1024
                or any(not 0x21 <= byte <= 0x7E for byte in raw.encode("ascii"))
            ):
                raise ValueError("protected staging opaque credential is invalid")

    def _create_credential_seed(self) -> None:
        bootstrap = self._read_credential_bootstrap()
        if self.credential_seed_path.exists():
            raise RuntimeError("protected staging credential seed appeared before creation")
        seed = {
            "agent_database_password": secrets.token_urlsafe(48),
            "agent_incarnation": str(_AGENT_INCARNATION),
            "authority_incarnation": str(_AUTHORITY_INCARNATION),
            "migrator_database_password": secrets.token_urlsafe(48),
            "observer_database_password": secrets.token_urlsafe(48),
            "reporter_incarnation": str(uuid4()),
            "reporter_token": secrets.token_urlsafe(48),
            "runtime_database_password": secrets.token_urlsafe(48),
            "schema_version": 1,
            "subject_id": str(_SUBJECT_ID),
            "subject_incarnation": str(_SUBJECT_INCARNATION),
        }
        self._validate_credential_seed(seed)
        payload = (json.dumps(seed, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        temporary_name = f".seed.{uuid4().hex}.tmp"
        directory_fd = os.open(
            self.credentials_root,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            try:
                os.fchmod(descriptor, 0o600)
                offset = 0
                while offset < len(payload):
                    offset += os.write(descriptor, payload[offset:])
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.link(
                temporary_name,
                self.credential_seed_path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temporary_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        except Exception:
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            raise
        finally:
            os.close(directory_fd)
        if not bootstrap:
            raise RuntimeError("protected staging credential bootstrap disappeared")

    @staticmethod
    def _observation(
        plan: FinalGatePlan,
        *,
        component_id: str,
        state: ComponentState,
        epoch: ComponentObservation,
        component_evidence: str,
    ) -> ComponentObservation:
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "component_evidence": component_evidence,
                    "component_id": component_id,
                    "epoch_evidence": epoch.evidence_digest,
                    "state": state.value,
                }
            ),
            observed_epoch=plan.starting_mutation_epoch + 1,
        )


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("protected staging JSON contains duplicate keys")
        value[key] = item
    return value


__all__ = ["KubernetesProtectedStagingCapacityRuntime"]
