"""Journaled protected staging capacity database bootstrap."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from sqlalchemy import URL

from loom.personal_dev_capacity_identity import (
    capacity_role_names,
    capacity_runtime_database_url,
)
from loom.personal_dev_capacity_runtime import protected_capacity_database_admission_digest
from loom.staging_capacity_database_bootstrap import staging_capacity_identity
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    ReporterConfigurationV1,
)
from loom_capacity_guard.contracts import GuardFenceV1, canonical_bytes
from loom_capacity_guard.schema_startup import capacity_guard_schema_head

from .final_gate_plan import FinalGatePlan
from .postgres_sql import single_line_sql
from .protected_apply_journal import ComponentState

_NAMESPACE = "loom-staging"
_NAME = "loom-staging-capacity-database-bootstrap"
_COMPONENT_LABEL = "loom.carin.dev/protected-component"
_COMPONENT_LABEL_VALUE = "staging-capacity-database"
_MANAGED_BY = "loom-staging-rollout"
_FIELD_MANAGER = "loom-staging-capacity-database-bootstrap"
_REQUEST_TIMEOUT = "60s"
_QUERY_TIMEOUT_SECONDS = 30.0
_MUTATION_TIMEOUT_SECONDS = 60.0
_WAIT_TIMEOUT_SECONDS = 660.0
_REVISION_RE = re.compile(r"^guard_([0-9]{4})$")
_REVISION_PRESENCE_SQL = single_line_sql(
    """
    SELECT COALESCE(
      to_regclass('loom_capacity_guard.capacity_guard_alembic_version')::text,
      'absent'
    )
    """
)
_REVISION_SQL = single_line_sql(
    "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
)
_DETAIL_SQL = single_line_sql(
    """
    SELECT jsonb_build_object(
      'authority', (
        SELECT jsonb_build_object(
          'schema_version', schema_version,
          'environment_id', environment_id,
          'subject_id', subject_id,
          'subject_incarnation', subject_incarnation,
          'authority_mode', authority_mode,
          'authority_incarnation', authority_incarnation,
          'reporter_incarnation', reporter_incarnation,
          'reporter_high_water', reporter_high_water,
          'allocation_epoch', allocation_epoch,
          'deployment_generation', deployment_generation,
          'configuration_generation', configuration_generation,
          'candidate_digest', candidate_digest
        )
        FROM loom_capacity_guard.authority_state
        WHERE singleton_id = 1
      ),
      'registration', (
        SELECT jsonb_build_object(
          'schema_version', schema_version,
          'environment_id', environment_id,
          'subject_id', subject_id,
          'subject_incarnation', subject_incarnation,
          'authority_incarnation', authority_incarnation,
          'agent_incarnation', agent_incarnation,
          'reporter_incarnation', reporter_incarnation,
          'authority_mode', authority_mode,
          'allocation_epoch', allocation_epoch,
          'reporter_high_water', 0,
          'candidate_digest', candidate_digest,
          'candidate_identity_algorithm', candidate_identity_algorithm,
          'candidate_identity', candidate_identity,
          'candidate_publication_sha256', candidate_publication_sha256,
          'deployment_generation', deployment_generation,
          'configuration_generation', configuration_generation
        )
        FROM loom_capacity_guard.agent_registrations
        WHERE singleton_id = 1 AND registration_state = 'registered'
      ),
      'agent_role', (
        SELECT agent_role_name
        FROM loom_capacity_guard.agent_runtime_authority
        WHERE singleton_id = 1
      ),
      'runtime_role', (
        SELECT runtime_role_name
        FROM loom_capacity_guard.staging_worker_runtime_authority
        WHERE singleton_id = 1
      ),
      'roles', (
        SELECT COALESCE(
          jsonb_object_agg(
            role.rolname,
            jsonb_build_object(
              'can_login', role.rolcanlogin,
              'inherit', role.rolinherit,
              'superuser', role.rolsuper,
              'create_db', role.rolcreatedb,
              'create_role', role.rolcreaterole,
              'replication', role.rolreplication,
              'bypass_rls', role.rolbypassrls,
              'has_password', role.rolpassword IS NOT NULL,
              'memberships', (
                SELECT count(*)
                FROM pg_catalog.pg_auth_members AS membership
                WHERE membership.member = role.oid OR membership.roleid = role.oid
              )
            )
          ),
          '{}'::jsonb
        )
        FROM pg_catalog.pg_authid AS role
        WHERE role.rolname = ANY(ARRAY[
          'loom_cap_staging_owner',
          'loom_cap_staging_migrator',
          'loom_cap_staging_agent',
          'loom_cap_staging_executor',
          'loom_cap_staging_observer',
          'loom_cap_staging_runtime'
        ])
      )
    )
    """
)
_RUNTIME_SQL = single_line_sql(
    """
    SET SESSION AUTHORIZATION loom_cap_staging_runtime;
    SELECT loom_capacity_guard.current_protected_runtime_registration()
    """
)


class ProtectedStagingCapacityDatabaseCommandRunner(Protocol):
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


class _DatabaseState(StrEnum):
    NEEDS_CONVERGENCE = "needs-convergence"
    EXACT = "exact"
    DRIFTED = "drifted"


class _ResourceState(StrEnum):
    ABSENT = "absent"
    EXACT = "exact"
    DRIFTED = "drifted"


@dataclass(frozen=True, slots=True)
class _Snapshot:
    database: _DatabaseState
    resources: _ResourceState
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityDatabaseComponent:
    runner: ProtectedStagingCapacityDatabaseCommandRunner
    container_registry: str
    seed_reader: Callable[[], dict[str, object]]

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            snapshot = self._snapshot(plan)
        except (OSError, RuntimeError, UnicodeError, ValueError):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        if (
            snapshot.database is _DatabaseState.DRIFTED
            or snapshot.resources is _ResourceState.DRIFTED
        ):
            state = ComponentState.DRIFTED
        elif (
            snapshot.database is _DatabaseState.EXACT
            and snapshot.resources is _ResourceState.ABSENT
        ):
            state = ComponentState.EXACT
        else:
            state = ComponentState.READY
        return state, _hash_json(
            {
                "database": snapshot.database.value,
                "resources": snapshot.resources.value,
                "snapshot": snapshot.evidence_digest,
                "state": state.value,
            }
        )

    def apply(self, plan: FinalGatePlan) -> None:
        seed = self.seed_reader()
        payload = self._manifest(plan, seed)
        before = self._snapshot(plan, seed=seed, manifest=payload)
        if before.database is _DatabaseState.DRIFTED or before.resources is _ResourceState.DRIFTED:
            raise RuntimeError("protected staging capacity database state drifted")
        if before.database is _DatabaseState.EXACT:
            if before.resources is not _ResourceState.EXACT:
                raise RuntimeError(
                    "protected staging capacity database state changed before cleanup"
                )
            self._delete_bootstrap_resources()
        else:
            if before.resources is _ResourceState.ABSENT:
                self.runner.run_checked(
                    (
                        "kubectl",
                        "--namespace",
                        _NAMESPACE,
                        "create",
                        "--validate=strict",
                        f"--request-timeout={_REQUEST_TIMEOUT}",
                        "-f",
                        "-",
                    ),
                    env=self.runner.environment,
                    input_payload=payload,
                    timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
                )
            elif before.resources is not _ResourceState.EXACT:
                raise RuntimeError("protected staging capacity database bootstrap drifted")
            self.runner.run_checked(
                (
                    "kubectl",
                    "--namespace",
                    _NAMESPACE,
                    "wait",
                    "--for=condition=complete",
                    "--timeout=600s",
                    f"job/{_NAME}",
                ),
                env=self.runner.environment,
                input_payload=None,
                timeout_seconds=_WAIT_TIMEOUT_SECONDS,
            )
            if self._database_state(plan, seed) is not _DatabaseState.EXACT:
                raise RuntimeError("protected staging capacity database bootstrap was not exact")
            resources, _evidence = self._resource_state(plan, payload)
            if resources is not _ResourceState.EXACT:
                raise RuntimeError("protected staging capacity database bootstrap changed")
            self._delete_bootstrap_resources()
        after = self._snapshot(plan, seed=seed, manifest=payload)
        if (
            after.database is not _DatabaseState.EXACT
            or after.resources is not _ResourceState.ABSENT
        ):
            raise RuntimeError("protected staging capacity database did not converge")

    def _snapshot(
        self,
        plan: FinalGatePlan,
        *,
        seed: dict[str, object] | None = None,
        manifest: bytes | None = None,
    ) -> _Snapshot:
        effective_seed = self.seed_reader() if seed is None else seed
        effective_manifest = self._manifest(plan, effective_seed) if manifest is None else manifest
        database = self._database_state(plan, effective_seed)
        resources, resource_evidence = self._resource_state(plan, effective_manifest)
        return _Snapshot(
            database=database,
            resources=resources,
            evidence_digest=_hash_json(
                {
                    "database": database.value,
                    "resource_evidence": resource_evidence,
                    "resources": resources.value,
                }
            ),
        )

    def _database_state(
        self,
        plan: FinalGatePlan,
        seed: dict[str, object],
    ) -> _DatabaseState:
        presence = self._query(_REVISION_PRESENCE_SQL).decode("ascii").strip()
        if presence == "absent":
            return _DatabaseState.NEEDS_CONVERGENCE
        if presence != "loom_capacity_guard.capacity_guard_alembic_version":
            return _DatabaseState.DRIFTED
        revision = self._query(_REVISION_SQL).decode("ascii").strip()
        match = _REVISION_RE.fullmatch(revision)
        expected_revision, expected_generation = capacity_guard_schema_head()
        if match is None or int(match.group(1)) > expected_generation:
            return _DatabaseState.DRIFTED
        if revision != expected_revision:
            return _DatabaseState.NEEDS_CONVERGENCE
        configuration = build_staging_reporter_configuration(plan, seed)
        try:
            details = json.loads(
                self._query(_DETAIL_SQL),
                object_pairs_hook=_reject_duplicate_keys,
            )
            runtime = AgentRegistrationV1.model_validate_json(self._query(_RUNTIME_SQL))
        except (json.JSONDecodeError, UnicodeError, ValueError):
            return _DatabaseState.DRIFTED
        expected_registration = AgentRegistrationV1.model_validate(
            {field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields}
        )
        expected_fence = GuardFenceV1(
            environment_id=configuration.environment_id,
            subject_id=configuration.subject_id,
            subject_incarnation=configuration.subject_incarnation,
            authority_incarnation=configuration.authority_incarnation,
            reporter_incarnation=configuration.reporter_incarnation,
            candidate_digest=configuration.candidate_digest,
            deployment_generation=configuration.deployment_generation,
            configuration_generation=configuration.configuration_generation,
        )
        expected_details: dict[str, object] = {
            "agent_role": "loom_cap_staging_agent",
            "authority": expected_fence.model_dump(mode="json"),
            "registration": expected_registration.model_dump(mode="json"),
            "roles": _expected_roles(),
            "runtime_role": "loom_cap_staging_runtime",
        }
        if details == expected_details and runtime == expected_registration:
            return _DatabaseState.EXACT
        authority = details.get("authority") if isinstance(details, dict) else None
        registration = details.get("registration") if isinstance(details, dict) else None
        expected_authority = expected_details["authority"]
        expected_registration_details = expected_details["registration"]
        assert isinstance(expected_authority, dict)
        assert isinstance(expected_registration_details, dict)
        if (
            not isinstance(authority, dict)
            or not isinstance(registration, dict)
            or details.get("agent_role") != expected_details["agent_role"]
            or details.get("runtime_role") != expected_details["runtime_role"]
            or any(
                authority.get(field) != expected_authority[field]
                for field in (
                    "environment_id",
                    "subject_id",
                    "subject_incarnation",
                    "authority_incarnation",
                    "authority_mode",
                    "allocation_epoch",
                )
            )
            or any(
                registration.get(field) != expected_registration_details[field]
                for field in (
                    "environment_id",
                    "subject_id",
                    "subject_incarnation",
                    "authority_incarnation",
                    "agent_incarnation",
                    "authority_mode",
                    "allocation_epoch",
                )
            )
        ):
            return _DatabaseState.DRIFTED
        return _DatabaseState.NEEDS_CONVERGENCE

    def _resource_state(
        self,
        plan: FinalGatePlan,
        manifest: bytes,
    ) -> tuple[_ResourceState, str]:
        inventory = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                "secret,job",
                f"--selector={_COMPONENT_LABEL}={_COMPONENT_LABEL_VALUE}",
                "--output=json",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        try:
            document = json.loads(inventory, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
            raise ValueError("protected staging capacity database inventory is invalid") from exc
        if not isinstance(document, dict) or not isinstance(document.get("items"), list):
            raise ValueError("protected staging capacity database inventory is invalid")
        items = document["items"]
        if not items:
            return _ResourceState.ABSENT, hashlib.sha256(inventory).hexdigest()
        identities: list[tuple[object, object, object]] = []
        failed = False
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("metadata"), dict):
                raise ValueError("protected staging capacity database resource is invalid")
            metadata = item["metadata"]
            identities.append((item.get("kind"), metadata.get("namespace"), metadata.get("name")))
            status = item.get("status", {})
            if item.get("kind") == "Job" and (
                not isinstance(status, dict)
                or (isinstance(status.get("failed"), int) and status["failed"] > 0)
            ):
                failed = True
        if (
            set(identities)
            != {
                ("Secret", _NAMESPACE, _NAME),
                ("Job", _NAMESPACE, _NAME),
            }
            or len(identities) != 2
            or failed
        ):
            return _ResourceState.DRIFTED, hashlib.sha256(inventory).hexdigest()
        status = self.runner.run_status(
            (
                "kubectl",
                "diff",
                "--server-side=true",
                f"--field-manager={_FIELD_MANAGER}",
                "--validate=strict",
                f"--request-timeout={_REQUEST_TIMEOUT}",
                "-f",
                "-",
            ),
            env=self.runner.environment,
            input_payload=manifest,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        return (
            _ResourceState.EXACT if status == 0 else _ResourceState.DRIFTED,
            hashlib.sha256(inventory).hexdigest(),
        )

    def _delete_bootstrap_resources(self) -> None:
        for resource in ("job", "secret"):
            self.runner.run_checked(
                (
                    "kubectl",
                    "--namespace",
                    _NAMESPACE,
                    "delete",
                    resource,
                    _NAME,
                    "--wait=true",
                    f"--request-timeout={_REQUEST_TIMEOUT}",
                ),
                env=self.runner.environment,
                input_payload=None,
                timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
            )

    def _query(self, statement: str) -> bytes:
        return self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "exec",
                "service/loom-postgres-rw",
                "--",
                "sh",
                "-ceu",
                'exec psql -U postgres -d loom -qAtX -v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                statement,
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )

    def _manifest(self, plan: FinalGatePlan, seed: dict[str, object]) -> bytes:
        configuration = build_staging_reporter_configuration(plan, seed)
        labels = {
            "app.kubernetes.io/managed-by": _MANAGED_BY,
            "app.kubernetes.io/name": _NAME,
            _COMPONENT_LABEL: _COMPONENT_LABEL_VALUE,
        }
        annotations = {
            "loom.carin.dev/candidate-sha": plan.candidate_sha,
            "loom.carin.dev/candidate-tree": plan.candidate_tree,
            "loom.carin.dev/plan-digest": plan.plan_digest,
        }
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "annotations": annotations,
                "labels": labels,
                "name": _NAME,
                "namespace": _NAMESPACE,
            },
            "immutable": True,
            "type": "Opaque",
            "data": {
                "reporter-configuration.json": base64.b64encode(
                    canonical_bytes(configuration)
                ).decode("ascii"),
                "seed.json": base64.b64encode(
                    (json.dumps(seed, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
                ).decode("ascii"),
            },
        }
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "annotations": annotations,
                "labels": labels,
                "name": _NAME,
                "namespace": _NAMESPACE,
            },
            "spec": {
                "activeDeadlineSeconds": 600,
                "backoffLimit": 0,
                "completions": 1,
                "parallelism": 1,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "command": [
                                    "python",
                                    "-I",
                                    "-B",
                                    "-m",
                                    "loom.staging_capacity_database_bootstrap",
                                ],
                                "image": (
                                    f"{self.container_registry}/loom-control-plane@"
                                    f"{plan.image_digests['loom-control-plane']}"
                                ),
                                "imagePullPolicy": "IfNotPresent",
                                "name": "bootstrap",
                                "resources": {
                                    "limits": {"cpu": "1", "memory": "1Gi"},
                                    "requests": {"cpu": "100m", "memory": "256Mi"},
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                    "readOnlyRootFilesystem": True,
                                },
                                "volumeMounts": [
                                    {
                                        "mountPath": "/run/loom-staging-capacity-bootstrap",
                                        "name": "bootstrap",
                                        "readOnly": True,
                                    },
                                    {
                                        "mountPath": "/run/loom-postgres-admin",
                                        "name": "postgres-admin",
                                        "readOnly": True,
                                    },
                                    {
                                        "mountPath": "/run/loom-postgres-ca",
                                        "name": "postgres-ca",
                                        "readOnly": True,
                                    },
                                ],
                                "workingDir": "/app",
                            }
                        ],
                        "nodeSelector": {"kubernetes.io/os": "linux"},
                        "restartPolicy": "Never",
                        "securityContext": {
                            "fsGroup": 65532,
                            "fsGroupChangePolicy": "OnRootMismatch",
                            "runAsGroup": 65532,
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "volumes": [
                            {
                                "name": "bootstrap",
                                "secret": {
                                    "defaultMode": 0o440,
                                    "items": [
                                        {
                                            "key": "reporter-configuration.json",
                                            "path": "reporter-configuration.json",
                                        },
                                        {"key": "seed.json", "path": "seed.json"},
                                    ],
                                    "secretName": _NAME,
                                },
                            },
                            {
                                "name": "postgres-admin",
                                "secret": {
                                    "defaultMode": 0o440,
                                    "items": [
                                        {"key": "password", "path": "password"},
                                        {"key": "username", "path": "username"},
                                    ],
                                    "secretName": "loom-postgres-cnpg-credentials",
                                },
                            },
                            {
                                "name": "postgres-ca",
                                "secret": {
                                    "defaultMode": 0o440,
                                    "items": [{"key": "ca.crt", "path": "ca.crt"}],
                                    "secretName": "loom-postgres-ca",
                                },
                            },
                        ],
                    },
                },
            },
        }
        return cast(
            str, yaml.safe_dump_all((secret, job), sort_keys=True, explicit_start=True)
        ).encode()


def build_staging_reporter_configuration(
    plan: FinalGatePlan,
    seed: Mapping[str, object],
    *,
    protected_admission_sha256: str | None = None,
) -> ReporterConfigurationV1:
    """Build the one sealed reporter binding consumed by bootstrap and runtime."""
    return build_staging_reporter_configuration_for_candidate(
        candidate_sha=plan.candidate_sha,
        artifact_bundle_digest=plan.artifact_bundle_digest,
        mutation_epoch=plan.starting_mutation_epoch,
        seed=seed,
        protected_admission_sha256=protected_admission_sha256,
    )


def build_staging_reporter_configuration_for_candidate(
    *,
    candidate_sha: str,
    artifact_bundle_digest: str,
    mutation_epoch: int,
    seed: Mapping[str, object],
    protected_admission_sha256: str | None = None,
) -> ReporterConfigurationV1:
    """Build the shared reporter binding without requiring a final-gate plan."""
    return ReporterConfigurationV1(
        environment_id="staging",
        subject_id=UUID(str(seed["subject_id"])),
        subject_incarnation=UUID(str(seed["subject_incarnation"])),
        authority_incarnation=UUID(str(seed["authority_incarnation"])),
        agent_incarnation=UUID(str(seed["agent_incarnation"])),
        reporter_incarnation=UUID(str(seed["reporter_incarnation"])),
        candidate_digest=artifact_bundle_digest,
        candidate_identity_algorithm="git-sha1",
        candidate_identity=candidate_sha,
        candidate_publication_sha256=artifact_bundle_digest,
        deployment_generation=mutation_epoch + 1,
        configuration_generation=mutation_epoch + 1,
        protected_admission_sha256=protected_admission_sha256,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


def staging_database_protected_admission_digest(
    plan: FinalGatePlan, seed: Mapping[str, object]
) -> str:
    """Reproduce the database installer's sealed admission digest without admin credentials."""
    return staging_database_protected_admission_digest_for_candidate(
        candidate_sha=plan.candidate_sha,
        artifact_bundle_digest=plan.artifact_bundle_digest,
        mutation_epoch=plan.starting_mutation_epoch,
        seed=seed,
    )


def staging_database_protected_admission_digest_for_candidate(
    *,
    candidate_sha: str,
    artifact_bundle_digest: str,
    mutation_epoch: int,
    seed: Mapping[str, object],
) -> str:
    """Derive the shared protected-admission digest from preflight identity."""
    runtime_password = seed.get("runtime_database_password")
    if not isinstance(runtime_password, str):
        raise ValueError("protected staging runtime database password is invalid")
    identity = staging_capacity_identity()
    admin_url = URL.create(
        "postgresql+psycopg",
        username="postgres",
        host="loom-postgres-rw.loom-staging.svc.cluster.local",
        port=5432,
        database="loom",
        query={
            "sslmode": "verify-full",
            "sslrootcert": "/run/loom-postgres-ca/ca.crt",
        },
    ).render_as_string(hide_password=False)
    runtime_url = capacity_runtime_database_url(admin_url, identity, runtime_password)
    return protected_capacity_database_admission_digest(
        identity=identity,
        configuration=build_staging_reporter_configuration_for_candidate(
            candidate_sha=candidate_sha,
            artifact_bundle_digest=artifact_bundle_digest,
            mutation_epoch=mutation_epoch,
            seed=seed,
        ),
        runtime_database_url=runtime_url,
    )


def _expected_roles() -> dict[str, dict[str, object]]:
    def role(*, login: bool, inherit: bool, password: bool) -> dict[str, object]:
        return {
            "bypass_rls": False,
            "can_login": login,
            "create_db": False,
            "create_role": False,
            "has_password": password,
            "inherit": inherit,
            "memberships": 0,
            "replication": False,
            "superuser": False,
        }

    owner, migrator, agent, executor, observer, runtime = capacity_role_names(
        staging_capacity_identity()
    )
    return {
        agent: role(login=True, inherit=False, password=True),
        executor: role(login=False, inherit=False, password=False),
        migrator: role(login=False, inherit=True, password=False),
        observer: role(login=True, inherit=False, password=True),
        owner: role(login=False, inherit=False, password=False),
        runtime: role(login=True, inherit=False, password=True),
    }


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("protected staging capacity database JSON is ambiguous")
        value[key] = item
    return value


__all__ = [
    "KubernetesProtectedStagingCapacityDatabaseComponent",
    "build_staging_reporter_configuration",
    "build_staging_reporter_configuration_for_candidate",
    "staging_database_protected_admission_digest",
    "staging_database_protected_admission_digest_for_candidate",
]
