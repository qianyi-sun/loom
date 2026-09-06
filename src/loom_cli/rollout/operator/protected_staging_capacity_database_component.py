"""Journaled protected staging capacity database bootstrap."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from psycopg import sql
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
_CLEANUP_LABEL = "loom.carin.dev/protected-cleanup"
_CLEANUP_LABEL_PATH = "/metadata/labels/" + _CLEANUP_LABEL.replace("/", "~1")
_MANAGED_BY = "loom-staging-rollout"
_FIELD_MANAGER = "loom-staging-capacity-database-bootstrap"
_REQUEST_TIMEOUT = "60s"
_QUERY_TIMEOUT_SECONDS = 30.0
_MUTATION_TIMEOUT_SECONDS = 60.0
_WAIT_TIMEOUT_SECONDS = 660.0
_WAIT_SLICE_SECONDS = 5
_CLEANUP_PATCH_ATTEMPTS = 3
_CLEANUP_DELETE_ATTEMPTS = 3
_CLEANUP_DELETE_WAIT_ATTEMPTS = 3
_CLEANUP_DELETE_WAIT_SECONDS = 20
_PEER_PSQL_COMMAND = (
    "kubectl",
    "--namespace",
    _NAMESPACE,
    "exec",
    "-i",
    "service/loom-postgres-rw",
    "--",
    "sh",
    "-ceu",
    "exec psql -U postgres -d loom -qAtX -v ON_ERROR_STOP=1",
)
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
              'credential_validity', CASE
                WHEN role.rolvaliduntil IS NULL THEN 'none'
                WHEN role.rolvaliduntil = 'infinity'::timestamptz THEN 'infinite'
                WHEN role.rolvaliduntil > CURRENT_TIMESTAMP THEN 'finite-valid'
                ELSE 'expired'
              END,
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
      ),
      'database_privileges', (
        SELECT jsonb_build_object(
          'migrator_acl_count', (
            SELECT count(*)
            FROM pg_catalog.aclexplode(
              COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
            ) AS privilege
            JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = privilege.grantee
            WHERE grantee.rolname = 'loom_cap_staging_migrator'
          ),
          'migrator_connect', pg_catalog.has_database_privilege(
            'loom_cap_staging_migrator', 'loom', 'CONNECT'
          ),
          'migrator_create', pg_catalog.has_database_privilege(
            'loom_cap_staging_migrator', 'loom', 'CREATE'
          ),
          'migrator_temporary', pg_catalog.has_database_privilege(
            'loom_cap_staging_migrator', 'loom', 'TEMPORARY'
          ),
          'owner_create', pg_catalog.has_database_privilege(
            'loom_cap_staging_owner', 'loom', 'CREATE'
          )
        )
        FROM pg_catalog.pg_database AS database
        WHERE database.datname = 'loom'
      ),
      'active_migrator_sessions', (
        SELECT count(*) FROM pg_catalog.pg_stat_activity
        WHERE usename = 'loom_cap_staging_migrator'
          AND pid <> pg_catalog.pg_backend_pid()
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
    FAILED = "failed"
    RECOVERABLE = "recoverable"
    PREVIOUS_FAILED = "previous-failed"
    DRIFTED = "drifted"


@dataclass(frozen=True, slots=True)
class _Snapshot:
    database: _DatabaseState
    resources: _ResourceState
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class _CertifiedResourceIdentity:
    uid: str
    resource_version: str


@dataclass(slots=True)
class _CertifiedPreviousFailedBootstrap:
    manifest: bytes
    secret: _CertifiedResourceIdentity
    job: _CertifiedResourceIdentity
    initial_pair_validated: bool = False

    def identity(self, kind: str) -> _CertifiedResourceIdentity:
        if kind == "Secret":
            return self.secret
        if kind == "Job":
            return self.job
        raise ValueError("protected staging capacity database resource kind is invalid")


def _manifest_with_observed_cleanup_labels(
    manifest: bytes,
    observed: Mapping[str, Mapping[str, object]],
) -> bytes:
    documents: list[object] = []
    for document in yaml.safe_load_all(manifest):
        if not isinstance(document, dict):
            continue
        kind = document.get("kind")
        item = observed.get(str(kind))
        if item is None:
            continue
        metadata = item.get("metadata") if item is not None else None
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        if isinstance(labels, dict) and _CLEANUP_LABEL in labels:
            document = copy.deepcopy(document)
            document_metadata = document.get("metadata")
            if not isinstance(document_metadata, dict):
                raise ValueError("protected staging capacity database manifest is invalid")
            document_labels = document_metadata.get("labels", {})
            if not isinstance(document_labels, dict):
                raise ValueError("protected staging capacity database manifest is invalid")
            document_labels = dict(document_labels)
            document_metadata["labels"] = document_labels
            document_labels[_CLEANUP_LABEL] = labels[_CLEANUP_LABEL]
        documents.append(document)
    if len(documents) != len(observed):
        raise ValueError("protected staging capacity database manifest is invalid")
    return cast(str, yaml.safe_dump_all(documents, sort_keys=True, explicit_start=True)).encode()


def _manifest_resources(manifest: bytes) -> dict[str, dict[str, object]]:
    resources = {
        document["kind"]: document
        for document in yaml.safe_load_all(manifest)
        if isinstance(document, dict) and document.get("kind") in {"Secret", "Job"}
    }
    if set(resources) != {"Secret", "Job"}:
        raise ValueError("protected staging capacity database manifest is invalid")
    return resources


def _pop_exact(mapping: dict[str, object], key: str, value: object) -> None:
    if mapping.get(key) == value:
        mapping.pop(key)


def _resource_projection(document: Mapping[str, object]) -> dict[str, object]:
    """Remove only API/controller fields whose exact defaults are independently known."""

    value = copy.deepcopy(dict(document))
    value.pop("status", None)
    metadata = value.get("metadata")
    if not isinstance(metadata, dict):
        return value
    uid = metadata.get("uid")
    name = metadata.get("name")
    for field in (
        "creationTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "uid",
    ):
        metadata.pop(field, None)
    labels = metadata.get("labels")
    if isinstance(labels, dict):
        labels.pop(_CLEANUP_LABEL, None)
    if value.get("kind") != "Job":
        return value

    spec = value.get("spec")
    if not isinstance(spec, dict):
        return value
    for spec_field, spec_default in (
        ("completionMode", "NonIndexed"),
        ("manualSelector", False),
        ("podReplacementPolicy", "TerminatingOrFailed"),
        ("suspend", False),
    ):
        _pop_exact(spec, spec_field, spec_default)
    if isinstance(uid, str):
        _pop_exact(
            spec,
            "selector",
            {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}},
        )
    template = spec.get("template")
    if not isinstance(template, dict):
        return value
    template_metadata = template.get("metadata")
    template_labels = (
        template_metadata.get("labels") if isinstance(template_metadata, dict) else None
    )
    if isinstance(template_labels, dict) and isinstance(uid, str) and isinstance(name, str):
        for label_field, label_default in (
            ("batch.kubernetes.io/controller-uid", uid),
            ("batch.kubernetes.io/job-name", name),
            ("controller-uid", uid),
            ("job-name", name),
        ):
            _pop_exact(template_labels, label_field, label_default)
    pod_spec = template.get("spec")
    if not isinstance(pod_spec, dict):
        return value
    for pod_field, pod_default in (
        ("dnsPolicy", "ClusterFirst"),
        ("schedulerName", "default-scheduler"),
        ("terminationGracePeriodSeconds", 30),
    ):
        _pop_exact(pod_spec, pod_field, pod_default)
    for container_field in ("containers", "initContainers"):
        containers = pod_spec.get(container_field)
        if not isinstance(containers, list):
            continue
        for container in containers:
            if not isinstance(container, dict):
                continue
            _pop_exact(container, "terminationMessagePath", "/dev/termination-log")
            _pop_exact(container, "terminationMessagePolicy", "File")
    return value


def _job_failed(status: Mapping[str, object]) -> bool:
    failed = status.get("failed")
    if isinstance(failed, int) and failed > 0:
        return True
    conditions = status.get("conditions", ())
    if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes)):
        return False
    for condition in conditions:
        if not isinstance(condition, Mapping):
            continue
        if condition.get("type") in {"Failed", "FailureTarget"} and condition.get("status") == (
            "True"
        ):
            return True
    return False


def _job_terminally_failed(status: Mapping[str, object]) -> bool:
    failed = status.get("failed")
    if type(failed) is not int or failed < 1:
        return False
    for field in ("active", "ready", "terminating"):
        count = status.get(field, 0)
        if type(count) is not int or count != 0:
            return False
    conditions = status.get("conditions", ())
    if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes)):
        return False
    return any(
        isinstance(condition, Mapping)
        and condition.get("type") == "Failed"
        and condition.get("status") == "True"
        for condition in conditions
    )


def _seed_credential(seed: Mapping[str, object], key: str) -> str:
    value = seed.get(key)
    if not isinstance(value, str):
        raise ValueError("protected staging capacity database credential is invalid")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("protected staging capacity database credential is invalid") from None
    if not 32 <= len(encoded) <= 1024 or any(not 0x21 <= byte <= 0x7E for byte in encoded):
        raise ValueError("protected staging capacity database credential is invalid")
    return value


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityDatabaseComponent:
    runner: ProtectedStagingCapacityDatabaseCommandRunner
    container_registry: str
    seed_reader: Callable[[], dict[str, object]]
    recovery_plan_reader: Callable[[FinalGatePlan, str, str, str], FinalGatePlan | None] | None = (
        None
    )

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
        cleanup_manifest = payload
        cleanup_certification: _CertifiedPreviousFailedBootstrap | None = None
        if before.resources is _ResourceState.PREVIOUS_FAILED:
            cleanup_certification = self._previous_failed_bootstrap_manifest(plan, payload)
            if cleanup_certification is None:
                raise RuntimeError("protected staging capacity database recovery identity changed")
            cleanup_manifest = cleanup_certification.manifest
        if before.database is _DatabaseState.EXACT:
            if before.resources not in {
                _ResourceState.EXACT,
                _ResourceState.FAILED,
                _ResourceState.RECOVERABLE,
                _ResourceState.PREVIOUS_FAILED,
            }:
                raise RuntimeError(
                    "protected staging capacity database state changed before cleanup"
                )
            self._delete_bootstrap_resources(
                plan,
                cleanup_manifest,
                certification=cleanup_certification,
            )
        else:
            self._compensate_bootstrap(
                plan,
                cleanup_manifest,
                certification=cleanup_certification,
            )
            arm_attempted = False
            try:
                arm_attempted = True
                self._arm_transient_migrator(seed)
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
                self._wait_for_bootstrap_job(plan, payload)
                self._disable_transient_credentials(preserve_runtime_credentials=True)
                self._terminate_transient_sessions(preserve_runtime_credentials=True)
                self._delete_bootstrap_resources(plan, payload, kinds=("Job",))
                self._remove_transient_authority()
                self._verify_transient_authority_sealed(
                    preserve_runtime_credentials=True,
                    durable_runtime_credentials=False,
                )
                if (
                    self._database_state(plan, seed, durable_runtime_credentials=False)
                    is not _DatabaseState.EXACT
                ):
                    raise RuntimeError(
                        "protected staging capacity database bootstrap was not exact"
                    )
                self._finalize_runtime_credentials()
                self._verify_transient_authority_sealed(
                    preserve_runtime_credentials=True,
                    durable_runtime_credentials=True,
                )
                if self._database_state(plan, seed) is not _DatabaseState.EXACT:
                    raise RuntimeError(
                        "protected staging capacity database bootstrap was not exact"
                    )
                resources, _evidence = self._resource_state(plan, payload)
                if resources is not _ResourceState.RECOVERABLE:
                    raise RuntimeError("protected staging capacity database bootstrap changed")
                self._delete_bootstrap_resources(plan, payload, kinds=("Secret",))
            except Exception:
                if arm_attempted:
                    self._compensate_bootstrap(plan, payload)
                raise
        after = self._snapshot(plan, seed=seed, manifest=payload)
        if (
            after.database is not _DatabaseState.EXACT
            or after.resources is not _ResourceState.ABSENT
        ):
            raise RuntimeError("protected staging capacity database did not converge")

    def _compensate_bootstrap(
        self,
        plan: FinalGatePlan,
        manifest: bytes,
        *,
        certification: _CertifiedPreviousFailedBootstrap | None = None,
    ) -> None:
        credentials_disabled = self._attempt_compensation_phase(
            lambda: self._disable_transient_credentials(preserve_runtime_credentials=False)
        )
        self._attempt_compensation_phase(
            lambda: self._terminate_transient_sessions(preserve_runtime_credentials=False)
        )
        self._attempt_compensation_phase(
            lambda: self._delete_bootstrap_resources(
                plan,
                manifest,
                kinds=("Job",),
                certification=certification,
            )
        )

        credentials_disabled = (
            self._attempt_compensation_phase(
                lambda: self._disable_transient_credentials(preserve_runtime_credentials=False)
            )
            or credentials_disabled
        )
        sessions_terminated = self._attempt_compensation_phase(
            lambda: self._terminate_transient_sessions(preserve_runtime_credentials=False)
        )
        job_stopped = self._attempt_compensation_phase(
            lambda: self._delete_bootstrap_resources(
                plan,
                manifest,
                kinds=("Job",),
                certification=certification,
            )
        )
        unresolved = [
            phase
            for phase, confirmed in (
                ("credentials", credentials_disabled),
                ("sessions", sessions_terminated),
                ("job", job_stopped),
            )
            if not confirmed
        ]
        if unresolved:
            raise RuntimeError(
                "protected staging capacity database compensation could not confirm safe "
                f"shutdown: {','.join(unresolved)}"
            ) from None
        self._remove_transient_authority()
        self._verify_transient_authority_sealed(
            preserve_runtime_credentials=False,
            durable_runtime_credentials=True,
        )
        self._delete_bootstrap_resources(
            plan,
            manifest,
            kinds=("Secret",),
            certification=certification,
        )

    @staticmethod
    def _attempt_compensation_phase(operation: Callable[[], None]) -> bool:
        try:
            operation()
        except Exception:
            return False
        return True

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
        *,
        durable_runtime_credentials: bool = True,
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
            "active_migrator_sessions": 0,
            "agent_role": "loom_cap_staging_agent",
            "authority": expected_fence.model_dump(mode="json"),
            "database_privileges": {
                "migrator_acl_count": 0,
                "migrator_connect": False,
                "migrator_create": False,
                "migrator_temporary": False,
                "owner_create": False,
            },
            "registration": expected_registration.model_dump(mode="json"),
            "roles": _expected_roles(
                runtime_credential_validity=(
                    "infinite" if durable_runtime_credentials else "finite-valid"
                )
            ),
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
        observed, evidence_digest, exact = self._inventory_bootstrap_resources(manifest)
        if not observed:
            return _ResourceState.ABSENT, evidence_digest
        if not exact:
            if self._certified_previous_failed_manifest(plan, observed) is not None:
                return _ResourceState.PREVIOUS_FAILED, evidence_digest
            return _ResourceState.DRIFTED, evidence_digest
        job = observed.get("Job")
        if job is not None:
            status = job.get("status", {})
            if not isinstance(status, dict):
                return _ResourceState.DRIFTED, evidence_digest
            failed = _job_failed(status)
        else:
            failed = False
        if set(observed) != {"Secret", "Job"}:
            return _ResourceState.RECOVERABLE, evidence_digest
        return (
            _ResourceState.FAILED if failed else _ResourceState.EXACT,
            evidence_digest,
        )

    def _inventory_bootstrap_resources(
        self,
        manifest: bytes,
    ) -> tuple[dict[str, dict[str, object]], str, bool]:
        expected = _manifest_resources(manifest)
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
        listed: dict[str, dict[str, object]] = {}
        for item in document["items"]:
            if not isinstance(item, dict) or not isinstance(item.get("metadata"), dict):
                raise ValueError("protected staging capacity database resource is invalid")
            metadata = item["metadata"]
            kind = item.get("kind")
            identity = (kind, metadata.get("namespace"), metadata.get("name"))
            if (
                kind not in {"Secret", "Job"}
                or identity
                not in {
                    ("Secret", _NAMESPACE, _NAME),
                    ("Job", _NAMESPACE, _NAME),
                }
                or kind in listed
            ):
                raise RuntimeError("protected staging capacity database bootstrap drifted")
            listed[str(kind)] = item

        observed: dict[str, dict[str, object]] = {}
        direct_digests: dict[str, str] = {}
        for kind in ("Secret", "Job"):
            item, payload = self._capture_bootstrap_resource(kind)
            direct_digests[kind] = hashlib.sha256(payload).hexdigest()
            if item is None:
                continue
            observed[kind] = item
        if set(observed) != set(listed):
            raise RuntimeError("protected staging capacity database bootstrap drifted")
        for kind, item in observed.items():
            listed_metadata = listed[kind]["metadata"]
            item_metadata = item["metadata"]
            assert isinstance(listed_metadata, dict) and isinstance(item_metadata, dict)
            if listed_metadata.get("uid") != item_metadata.get("uid"):
                raise RuntimeError("protected staging capacity database bootstrap changed")
        evidence_digest = _hash_json(
            {
                "direct": direct_digests,
                "inventory": hashlib.sha256(inventory).hexdigest(),
            }
        )
        if not observed:
            return observed, evidence_digest, True
        if any(
            _resource_projection(item) != _resource_projection(expected[kind])
            for kind, item in observed.items()
        ):
            return observed, evidence_digest, False
        comparison_manifest = _manifest_with_observed_cleanup_labels(manifest, observed)
        status = self.runner.run_status(
            (
                "kubectl",
                "diff",
                "--server-side=true",
                f"--field-manager={_FIELD_MANAGER}",
                f"--request-timeout={_REQUEST_TIMEOUT}",
                "-f",
                "-",
            ),
            env=self.runner.environment,
            input_payload=comparison_manifest,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        return observed, evidence_digest, status == 0

    def _previous_failed_bootstrap_manifest(
        self,
        plan: FinalGatePlan,
        current_manifest: bytes,
    ) -> _CertifiedPreviousFailedBootstrap | None:
        observed, _evidence_digest, exact = self._inventory_bootstrap_resources(current_manifest)
        if exact:
            return None
        return self._certified_previous_failed_manifest(plan, observed)

    def _certified_previous_failed_manifest(
        self,
        plan: FinalGatePlan,
        observed: Mapping[str, Mapping[str, object]],
    ) -> _CertifiedPreviousFailedBootstrap | None:
        if self.recovery_plan_reader is None or set(observed) != {"Secret", "Job"}:
            return None
        job_status = observed["Job"].get("status")
        if not isinstance(job_status, Mapping) or not _job_terminally_failed(job_status):
            return None
        bindings: tuple[str, str, str] | None = None
        for kind in ("Secret", "Job"):
            metadata = observed[kind].get("metadata")
            annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
            if not isinstance(annotations, Mapping):
                return None
            found = (
                annotations.get("loom.carin.dev/candidate-sha"),
                annotations.get("loom.carin.dev/candidate-tree"),
                annotations.get("loom.carin.dev/plan-digest"),
            )
            if not all(isinstance(value, str) for value in found):
                return None
            typed_found = cast(tuple[str, str, str], found)
            if bindings is None:
                bindings = typed_found
            elif typed_found != bindings:
                return None
        assert bindings is not None
        prior_plan = self.recovery_plan_reader(plan, *bindings)
        if prior_plan is None:
            return None
        legacy_manifest = self._legacy_auth_manifest(prior_plan, self.seed_reader())
        expected = _manifest_resources(legacy_manifest)
        if any(
            _resource_projection(observed[kind]) != _resource_projection(expected[kind])
            for kind in ("Secret", "Job")
        ):
            return None
        comparison_manifest = _manifest_with_observed_cleanup_labels(legacy_manifest, observed)
        status = self.runner.run_status(
            (
                "kubectl",
                "diff",
                "--server-side=true",
                f"--field-manager={_FIELD_MANAGER}",
                f"--request-timeout={_REQUEST_TIMEOUT}",
                "-f",
                "-",
            ),
            env=self.runner.environment,
            input_payload=comparison_manifest,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )
        if status != 0:
            return None
        identities: dict[str, _CertifiedResourceIdentity] = {}
        for kind in ("Secret", "Job"):
            metadata = observed[kind]["metadata"]
            assert isinstance(metadata, Mapping)
            uid = metadata["uid"]
            resource_version = metadata["resourceVersion"]
            assert isinstance(uid, str) and isinstance(resource_version, str)
            identities[kind] = _CertifiedResourceIdentity(
                uid=uid,
                resource_version=resource_version,
            )
        return _CertifiedPreviousFailedBootstrap(
            manifest=legacy_manifest,
            secret=identities["Secret"],
            job=identities["Job"],
        )

    def _capture_bootstrap_resource(
        self,
        kind: str,
    ) -> tuple[dict[str, object] | None, bytes]:
        if kind not in {"Secret", "Job"}:
            raise ValueError("protected staging capacity database resource kind is invalid")
        payload = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                _NAMESPACE,
                "get",
                f"{kind.lower()}/{_NAME}",
                "--ignore-not-found=true",
                "--output=json",
                f"--request-timeout={_REQUEST_TIMEOUT}",
            ),
            env=self.runner.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        if not payload:
            return None, payload
        try:
            item = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
            raise ValueError("protected staging capacity database resource is invalid") from exc
        if not isinstance(item, dict) or not isinstance(item.get("metadata"), dict):
            raise ValueError("protected staging capacity database resource is invalid")
        metadata = item["metadata"]
        labels = metadata.get("labels")
        if (
            item.get("apiVersion") != ("v1" if kind == "Secret" else "batch/v1")
            or item.get("kind") != kind
            or metadata.get("namespace") != _NAMESPACE
            or metadata.get("name") != _NAME
            or not isinstance(labels, dict)
            or labels.get(_COMPONENT_LABEL) != _COMPONENT_LABEL_VALUE
            or not isinstance(metadata.get("uid"), str)
            or not isinstance(metadata.get("resourceVersion"), str)
        ):
            raise RuntimeError("protected staging capacity database bootstrap drifted")
        return item, payload

    def _delete_bootstrap_resources(
        self,
        plan: FinalGatePlan,
        manifest: bytes,
        *,
        kinds: tuple[str, ...] = ("Job", "Secret"),
        certification: _CertifiedPreviousFailedBootstrap | None = None,
    ) -> None:
        del plan
        target_kinds = set(kinds)
        if not target_kinds <= {"Secret", "Job"}:
            raise ValueError("protected staging capacity database cleanup target is invalid")
        observed = self._observed_bootstrap_resources(
            manifest,
            certification=certification,
        )
        cleanup_token = secrets.token_urlsafe(32)
        labelled: dict[str, dict[str, object]] = {}
        for kind in kinds:
            item = observed.get(kind)
            if item is None:
                continue
            labelled_item = self._label_bootstrap_resource_for_cleanup(
                kind=kind,
                item=item,
                cleanup_token=cleanup_token,
                manifest=manifest,
                certification=certification,
            )
            if labelled_item is not None:
                labelled[kind] = labelled_item
        for kind in kinds:
            item = labelled.get(kind)
            if item is None:
                continue
            self._delete_bootstrap_resource_by_identity(
                kind=kind,
                item=item,
                manifest=manifest,
                certification=certification,
            )
        remaining = self._observed_bootstrap_resources(
            manifest,
            certification=certification,
        )
        if any(kind in remaining for kind in target_kinds):
            raise RuntimeError(
                "protected staging capacity database bootstrap cleanup was not exact"
            )

    def _label_bootstrap_resource_for_cleanup(
        self,
        *,
        kind: str,
        item: dict[str, object],
        cleanup_token: str,
        manifest: bytes,
        certification: _CertifiedPreviousFailedBootstrap | None,
    ) -> dict[str, object] | None:
        metadata = item["metadata"]
        assert isinstance(metadata, dict)
        original_uid = metadata["uid"]
        for _attempt in range(_CLEANUP_PATCH_ATTEMPTS):
            patch = [
                {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": metadata["resourceVersion"],
                },
                {"op": "add", "path": _CLEANUP_LABEL_PATH, "value": cleanup_token},
            ]
            try:
                self.runner.run_checked(
                    (
                        "kubectl",
                        "--namespace",
                        _NAMESPACE,
                        "patch",
                        f"{kind.lower()}/{_NAME}",
                        "--type=json",
                        "--patch-file=-",
                        f"--request-timeout={_REQUEST_TIMEOUT}",
                    ),
                    env=self.runner.environment,
                    input_payload=json.dumps(patch, sort_keys=True).encode("utf-8"),
                    timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
                )
            except Exception:
                refreshed = self._observed_bootstrap_resources(
                    manifest,
                    certification=certification,
                ).get(kind)
                if refreshed is None:
                    return None
                refreshed_metadata = refreshed["metadata"]
                assert isinstance(refreshed_metadata, dict)
                if refreshed_metadata["uid"] != original_uid:
                    raise RuntimeError(
                        "protected staging capacity database bootstrap identity changed "
                        "during cleanup"
                    ) from None
                metadata = refreshed_metadata
                refreshed_labels = metadata.get("labels")
                if (
                    isinstance(refreshed_labels, dict)
                    and refreshed_labels.get(_CLEANUP_LABEL) == cleanup_token
                ):
                    return refreshed
                continue
            refreshed = self._observed_bootstrap_resources(
                manifest,
                certification=certification,
            ).get(kind)
            if refreshed is None:
                return None
            refreshed_metadata = refreshed["metadata"]
            assert isinstance(refreshed_metadata, dict)
            if refreshed_metadata["uid"] != original_uid:
                raise RuntimeError(
                    "protected staging capacity database bootstrap identity changed during cleanup"
                ) from None
            return refreshed
        raise RuntimeError(
            "protected staging capacity database bootstrap cleanup patch did not stabilize"
        ) from None

    def _delete_bootstrap_resource_by_identity(
        self,
        *,
        kind: str,
        item: dict[str, object],
        manifest: bytes,
        certification: _CertifiedPreviousFailedBootstrap | None,
    ) -> None:
        metadata = item["metadata"]
        assert isinstance(metadata, dict)
        original_uid = metadata["uid"]
        resource_path = (
            f"/api/v1/namespaces/{_NAMESPACE}/secrets/{_NAME}"
            if kind == "Secret"
            else f"/apis/batch/v1/namespaces/{_NAMESPACE}/jobs/{_NAME}"
        )
        for _attempt in range(_CLEANUP_DELETE_ATTEMPTS):
            delete_options = {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {
                    "resourceVersion": metadata["resourceVersion"],
                    "uid": original_uid,
                },
                "propagationPolicy": "Foreground",
            }
            try:
                self.runner.run_checked(
                    (
                        "kubectl",
                        "--namespace",
                        _NAMESPACE,
                        "delete",
                        "--raw",
                        resource_path,
                        "-f",
                        "-",
                        f"--request-timeout={_REQUEST_TIMEOUT}",
                    ),
                    env=self.runner.environment,
                    input_payload=json.dumps(delete_options, sort_keys=True).encode("utf-8"),
                    timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
                )
            except Exception:
                refreshed, _payload = self._capture_bootstrap_resource(kind)
                if refreshed is None:
                    return
                self._validate_certified_previous_failed_resources(
                    certification,
                    {kind: refreshed},
                )
                refreshed_metadata = refreshed["metadata"]
                assert isinstance(refreshed_metadata, dict)
                if refreshed_metadata["uid"] != original_uid:
                    raise RuntimeError(
                        "protected staging capacity database bootstrap identity changed "
                        "during cleanup"
                    ) from None
                if isinstance(refreshed_metadata.get("deletionTimestamp"), str):
                    break
                exact_refreshed = self._observed_bootstrap_resources(
                    manifest,
                    certification=certification,
                ).get(kind)
                if exact_refreshed is None:
                    return
                exact_metadata = exact_refreshed["metadata"]
                assert isinstance(exact_metadata, dict)
                if exact_metadata["uid"] != original_uid:
                    raise RuntimeError(
                        "protected staging capacity database bootstrap identity changed "
                        "during cleanup"
                    ) from None
                metadata = exact_metadata
                continue
            break
        else:
            raise RuntimeError(
                "protected staging capacity database bootstrap cleanup delete did not stabilize"
            ) from None

        for _wait_attempt in range(_CLEANUP_DELETE_WAIT_ATTEMPTS):
            wait_status = self.runner.run_status(
                (
                    "kubectl",
                    "--namespace",
                    _NAMESPACE,
                    "wait",
                    "--for=delete",
                    f"--timeout={_CLEANUP_DELETE_WAIT_SECONDS}s",
                    f"--request-timeout={_REQUEST_TIMEOUT}",
                    f"{kind.lower()}/{_NAME}",
                ),
                env=self.runner.environment,
                input_payload=None,
                timeout_seconds=_QUERY_TIMEOUT_SECONDS,
            )
            refreshed, _payload = self._capture_bootstrap_resource(kind)
            if refreshed is None:
                return
            self._validate_certified_previous_failed_resources(
                certification,
                {kind: refreshed},
            )
            refreshed_metadata = refreshed["metadata"]
            assert isinstance(refreshed_metadata, dict)
            if refreshed_metadata["uid"] != original_uid:
                raise RuntimeError(
                    "protected staging capacity database bootstrap identity changed during cleanup"
                ) from None
            if wait_status == 0 or not isinstance(refreshed_metadata.get("deletionTimestamp"), str):
                raise RuntimeError(
                    "protected staging capacity database bootstrap cleanup was not exact"
                ) from None
        raise RuntimeError(
            "protected staging capacity database bootstrap foreground deletion did not finish"
        ) from None

    def _observed_bootstrap_resources(
        self,
        manifest: bytes,
        *,
        certification: _CertifiedPreviousFailedBootstrap | None = None,
    ) -> dict[str, dict[str, object]]:
        observed, _evidence_digest, exact = self._inventory_bootstrap_resources(manifest)
        if not exact:
            raise RuntimeError("protected staging capacity database bootstrap drifted")
        self._validate_certified_previous_failed_resources(
            certification,
            observed,
        )
        return observed

    @staticmethod
    def _validate_certified_previous_failed_resources(
        certification: _CertifiedPreviousFailedBootstrap | None,
        observed: Mapping[str, Mapping[str, object]],
    ) -> None:
        if certification is None:
            return
        require_initial_pair = not certification.initial_pair_validated
        if require_initial_pair and set(observed) != {"Secret", "Job"}:
            raise RuntimeError(
                "protected staging capacity database bootstrap changed during cleanup"
            )
        for kind, item in observed.items():
            metadata = item.get("metadata")
            if not isinstance(metadata, Mapping):
                raise RuntimeError(
                    "protected staging capacity database bootstrap changed during cleanup"
                )
            identity = certification.identity(kind)
            if metadata.get("uid") != identity.uid or (
                require_initial_pair
                and metadata.get("resourceVersion") != identity.resource_version
            ):
                raise RuntimeError(
                    "protected staging capacity database bootstrap changed during cleanup"
                )
            if kind == "Job":
                status = item.get("status")
                if not isinstance(status, Mapping) or not _job_terminally_failed(status):
                    raise RuntimeError(
                        "protected staging capacity database bootstrap changed during cleanup"
                    )
        if require_initial_pair:
            certification.initial_pair_validated = True

    def _wait_for_bootstrap_job(self, plan: FinalGatePlan, manifest: bytes) -> None:
        deadline = time.monotonic() + _WAIT_TIMEOUT_SECONDS
        while True:
            completed = self.runner.run_status(
                (
                    "kubectl",
                    "--namespace",
                    _NAMESPACE,
                    "wait",
                    "--for=condition=complete",
                    f"--timeout={_WAIT_SLICE_SECONDS}s",
                    f"--request-timeout={_REQUEST_TIMEOUT}",
                    f"job/{_NAME}",
                ),
                env=self.runner.environment,
                input_payload=None,
                timeout_seconds=_QUERY_TIMEOUT_SECONDS,
            )
            if completed == 0:
                return
            resources, _evidence = self._resource_state(plan, manifest)
            if resources is _ResourceState.FAILED:
                raise RuntimeError("protected staging capacity database bootstrap job failed")
            if resources is not _ResourceState.EXACT:
                raise RuntimeError(
                    "protected staging capacity database bootstrap changed while waiting"
                )
            if time.monotonic() >= deadline:
                raise RuntimeError("protected staging capacity database bootstrap job timed out")

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

    def _arm_transient_migrator(self, seed: Mapping[str, object]) -> None:
        migrator_password = sql.Literal(
            _seed_credential(seed, "migrator_database_password")
        ).as_string()
        agent_password = sql.Literal(_seed_credential(seed, "agent_database_password")).as_string()
        observer_password = sql.Literal(
            _seed_credential(seed, "observer_database_password")
        ).as_string()
        runtime_password = sql.Literal(
            _seed_credential(seed, "runtime_database_password")
        ).as_string()
        payload = f"""\
BEGIN;
DO $loom$
DECLARE
    protected_names text[] := ARRAY[
        'loom_cap_staging_owner',
        'loom_cap_staging_migrator',
        'loom_cap_staging_agent',
        'loom_cap_staging_executor',
        'loom_cap_staging_observer',
        'loom_cap_staging_runtime'
    ];
BEGIN
    IF current_user <> 'postgres'
       OR NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_roles
           WHERE rolname = current_user AND rolsuper
       )
       OR (SELECT pg_catalog.pg_get_userbyid(datdba)
           FROM pg_catalog.pg_database WHERE datname = 'loom') <> 'loom'
       OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_roles
           WHERE rolname = 'loom' AND (rolsuper OR rolcreaterole)
       )
       OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_roles
           WHERE rolname = ANY(protected_names)
             AND (rolsuper OR rolcreatedb OR rolreplication OR rolbypassrls)
       ) THEN
        RAISE EXCEPTION 'protected staging capacity role bootstrap authority is invalid';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_owner') THEN
        CREATE ROLE loom_cap_staging_owner NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_migrator') THEN
        CREATE ROLE loom_cap_staging_migrator NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_agent') THEN
        CREATE ROLE loom_cap_staging_agent NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_executor') THEN
        CREATE ROLE loom_cap_staging_executor NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_observer') THEN
        CREATE ROLE loom_cap_staging_observer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_runtime') THEN
        CREATE ROLE loom_cap_staging_runtime NOLOGIN;
    END IF;
END
$loom$;
ALTER ROLE loom_cap_staging_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_migrator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_agent NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_executor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_observer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_owner RESET ALL;
ALTER ROLE loom_cap_staging_migrator RESET ALL;
ALTER ROLE loom_cap_staging_agent RESET ALL;
ALTER ROLE loom_cap_staging_executor RESET ALL;
ALTER ROLE loom_cap_staging_observer RESET ALL;
ALTER ROLE loom_cap_staging_runtime RESET ALL;
DO $loom$
DECLARE
    granted_name text;
    member_name text;
BEGIN
    FOR granted_name, member_name IN
        SELECT granted.rolname, member.rolname
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        WHERE member.rolname = ANY(ARRAY[
            'loom_cap_staging_owner',
            'loom_cap_staging_migrator',
            'loom_cap_staging_agent',
            'loom_cap_staging_executor',
            'loom_cap_staging_observer',
            'loom_cap_staging_runtime'
        ])
        OR granted.rolname = ANY(ARRAY[
            'loom_cap_staging_owner',
            'loom_cap_staging_migrator',
            'loom_cap_staging_agent',
            'loom_cap_staging_executor',
            'loom_cap_staging_observer',
            'loom_cap_staging_runtime'
        ])
    LOOP
        EXECUTE format('REVOKE %I FROM %I', granted_name, member_name);
    END LOOP;
END
$loom$;
COMMIT;
BEGIN;
GRANT loom TO loom_cap_staging_migrator WITH ADMIN FALSE, INHERIT TRUE, SET TRUE;
GRANT loom_cap_staging_owner TO loom_cap_staging_migrator WITH ADMIN FALSE, INHERIT TRUE, SET TRUE;
DO $loom$
DECLARE
    lease_until timestamptz := clock_timestamp() + interval '45 minutes';
BEGIN
    EXECUTE format(
        'ALTER ROLE loom_cap_staging_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L VALID UNTIL %L',
        {migrator_password},
        lease_until
    );
    EXECUTE format(
        'ALTER ROLE loom_cap_staging_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L VALID UNTIL %L',
        {agent_password},
        lease_until
    );
    EXECUTE format(
        'ALTER ROLE loom_cap_staging_observer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L VALID UNTIL %L',
        {observer_password},
        lease_until
    );
    EXECUTE format(
        'ALTER ROLE loom_cap_staging_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L VALID UNTIL %L',
        {runtime_password},
        lease_until
    );
END
$loom$;
DO $loom$
DECLARE
    protected_names text[] := ARRAY[
        'loom_cap_staging_owner',
        'loom_cap_staging_migrator',
        'loom_cap_staging_agent',
        'loom_cap_staging_executor',
        'loom_cap_staging_observer',
        'loom_cap_staging_runtime'
    ];
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_authid
        WHERE rolname = 'loom_cap_staging_migrator'
          AND rolcanlogin
          AND rolinherit
          AND NOT rolsuper
          AND NOT rolcreatedb
          AND NOT rolcreaterole
          AND NOT rolreplication
          AND NOT rolbypassrls
          AND rolpassword IS NOT NULL
          AND rolvaliduntil IS NOT NULL
          AND rolvaliduntil > CURRENT_TIMESTAMP
          AND rolvaliduntil < 'infinity'::timestamptz
    )
    OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_authid
        WHERE rolname IN (
            'loom_cap_staging_owner',
            'loom_cap_staging_agent',
            'loom_cap_staging_executor',
            'loom_cap_staging_observer',
            'loom_cap_staging_runtime'
        )
        AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
    )
    OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_authid
        WHERE rolname IN ('loom_cap_staging_owner', 'loom_cap_staging_executor')
          AND (rolcanlogin OR rolinherit OR rolpassword IS NOT NULL)
    )
    OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_authid
        WHERE rolname IN (
            'loom_cap_staging_agent',
            'loom_cap_staging_observer',
            'loom_cap_staging_runtime'
        )
        AND (
            NOT rolcanlogin
            OR rolinherit
            OR rolpassword IS NULL
            OR rolvaliduntil IS NULL
            OR rolvaliduntil <= CURRENT_TIMESTAMP
            OR rolvaliduntil >= 'infinity'::timestamptz
        )
    )
    OR (
        SELECT count(*)
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        WHERE member.rolname = ANY(protected_names)
           OR granted.rolname = ANY(protected_names)
    ) <> 2
    OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        WHERE member.rolname = 'loom_cap_staging_migrator'
          AND granted.rolname = 'loom'
          AND NOT membership.admin_option
          AND membership.inherit_option
          AND membership.set_option
    )
    OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        WHERE member.rolname = 'loom_cap_staging_migrator'
          AND granted.rolname = 'loom_cap_staging_owner'
          AND NOT membership.admin_option
          AND membership.inherit_option
          AND membership.set_option
    ) THEN
        RAISE EXCEPTION 'protected staging capacity role bootstrap did not arm exact transient authority';
    END IF;
END
$loom$;
COMMIT;
""".encode()
        self.runner.run_checked(
            _PEER_PSQL_COMMAND,
            env=self.runner.environment,
            input_payload=payload,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )

    def _disable_transient_credentials(self, *, preserve_runtime_credentials: bool) -> None:
        runtime_sql = (
            ""
            if preserve_runtime_credentials
            else """
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_agent') THEN
        ALTER ROLE loom_cap_staging_agent NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_observer') THEN
        ALTER ROLE loom_cap_staging_observer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_runtime') THEN
        ALTER ROLE loom_cap_staging_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
    END IF;
"""
        )
        payload = f"""\
BEGIN;
DO $loom$
BEGIN
    IF current_user <> 'postgres'
       OR NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_roles
           WHERE rolname = current_user AND rolsuper
       ) THEN
        RAISE EXCEPTION 'protected staging capacity seal authority is invalid';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_migrator') THEN
        ALTER ROLE loom_cap_staging_migrator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
    END IF;
{runtime_sql}
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_owner') THEN
        ALTER ROLE loom_cap_staging_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_executor') THEN
        ALTER ROLE loom_cap_staging_executor NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL VALID UNTIL 'infinity';
    END IF;
END
$loom$;
COMMIT;
""".encode()
        self._run_peer_payload(payload)

    def _terminate_transient_sessions(self, *, preserve_runtime_credentials: bool) -> None:
        session_roles = (
            "'loom_cap_staging_migrator'"
            if preserve_runtime_credentials
            else (
                "'loom_cap_staging_migrator', 'loom_cap_staging_agent', "
                "'loom_cap_staging_observer', 'loom_cap_staging_runtime'"
            )
        )
        payload = f"""\
BEGIN;
DO $loom$
BEGIN
    PERFORM pg_catalog.pg_terminate_backend(pid)
    FROM pg_catalog.pg_stat_activity
    WHERE usename = ANY(ARRAY[{session_roles}])
      AND pid <> pg_catalog.pg_backend_pid();
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_activity
        WHERE usename = ANY(ARRAY[{session_roles}])
          AND pid <> pg_catalog.pg_backend_pid()
    ) THEN
        RAISE EXCEPTION 'protected staging capacity transient sessions remain';
    END IF;
END
$loom$;
COMMIT;
""".encode()
        self._run_peer_payload(payload)

    def _remove_transient_authority(self) -> None:
        payload = b"""\
BEGIN;
DO $loom$
DECLARE
    granted_name text;
    member_name text;
BEGIN
    REVOKE ALL PRIVILEGES ON DATABASE loom FROM PUBLIC;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_migrator'
    ) THEN
        REVOKE ALL PRIVILEGES ON DATABASE loom FROM loom_cap_staging_migrator;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'loom_cap_staging_owner'
    ) THEN
        REVOKE CREATE ON DATABASE loom FROM loom_cap_staging_owner;
    END IF;
    FOR granted_name, member_name IN
        SELECT granted.rolname, member.rolname
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        WHERE member.rolname = ANY(ARRAY[
            'loom_cap_staging_owner',
            'loom_cap_staging_migrator',
            'loom_cap_staging_agent',
            'loom_cap_staging_executor',
            'loom_cap_staging_observer',
            'loom_cap_staging_runtime'
        ])
        OR granted.rolname = ANY(ARRAY[
            'loom_cap_staging_owner',
            'loom_cap_staging_migrator',
            'loom_cap_staging_agent',
            'loom_cap_staging_executor',
            'loom_cap_staging_observer',
            'loom_cap_staging_runtime'
        ])
    LOOP
        EXECUTE format('REVOKE %I FROM %I', granted_name, member_name);
    END LOOP;
END
$loom$;
COMMIT;
"""
        self._run_peer_payload(payload)

    def _verify_transient_authority_sealed(
        self,
        *,
        preserve_runtime_credentials: bool,
        durable_runtime_credentials: bool,
    ) -> None:
        if not preserve_runtime_credentials:
            runtime_condition = """
                rolcanlogin OR rolinherit OR rolpassword IS NOT NULL
                OR rolvaliduntil IS DISTINCT FROM 'infinity'::timestamptz
"""
        elif durable_runtime_credentials:
            runtime_condition = """
                NOT rolcanlogin OR rolinherit OR rolpassword IS NULL
                OR rolvaliduntil IS DISTINCT FROM 'infinity'::timestamptz
"""
        else:
            runtime_condition = """
                NOT rolcanlogin OR rolinherit OR rolpassword IS NULL
                OR rolvaliduntil IS NULL OR rolvaliduntil <= CURRENT_TIMESTAMP
                OR rolvaliduntil >= 'infinity'::timestamptz
"""
        session_roles = (
            "'loom_cap_staging_migrator'"
            if preserve_runtime_credentials
            else (
                "'loom_cap_staging_migrator', 'loom_cap_staging_agent', "
                "'loom_cap_staging_observer', 'loom_cap_staging_runtime'"
            )
        )
        payload = f"""\
BEGIN;
DO $loom$
DECLARE
    protected_names text[] := ARRAY[
        'loom_cap_staging_owner',
        'loom_cap_staging_migrator',
        'loom_cap_staging_agent',
        'loom_cap_staging_executor',
        'loom_cap_staging_observer',
        'loom_cap_staging_runtime'
    ];
    protected_count integer;
BEGIN
    SELECT count(*) INTO protected_count
    FROM pg_catalog.pg_authid WHERE rolname = ANY(protected_names);
    IF protected_count NOT IN (0, 6) THEN
        RAISE EXCEPTION 'protected staging capacity transient authority is not sealed';
    END IF;
    IF protected_count = 6 AND (
        NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_authid
            WHERE rolname = 'loom_cap_staging_migrator'
              AND NOT rolcanlogin AND rolinherit AND NOT rolsuper
              AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication
              AND NOT rolbypassrls AND rolpassword IS NULL
              AND rolvaliduntil = 'infinity'::timestamptz
        )
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_authid
            WHERE rolname IN ('loom_cap_staging_owner', 'loom_cap_staging_executor')
              AND (
                  rolcanlogin OR rolinherit OR rolsuper OR rolcreatedb OR rolcreaterole
                  OR rolreplication OR rolbypassrls OR rolpassword IS NOT NULL
                  OR rolvaliduntil IS DISTINCT FROM 'infinity'::timestamptz
              )
        )
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_authid
            WHERE rolname IN (
                'loom_cap_staging_agent',
                'loom_cap_staging_observer',
                'loom_cap_staging_runtime'
            )
              AND (
                  rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls
                  OR ({runtime_condition})
              )
        )
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_auth_members AS membership
            JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
            JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
            WHERE member.rolname = ANY(protected_names)
               OR granted.rolname = ANY(protected_names)
        )
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_stat_activity
            WHERE usename = ANY(ARRAY[{session_roles}])
              AND pid <> pg_catalog.pg_backend_pid()
        )
        OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_database AS database
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
            ) AS privilege
            JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = privilege.grantee
            WHERE database.datname = 'loom'
              AND grantee.rolname = 'loom_cap_staging_migrator'
        )
        OR pg_catalog.has_database_privilege(
            'loom_cap_staging_migrator', 'loom', 'CONNECT'
        )
        OR pg_catalog.has_database_privilege(
            'loom_cap_staging_migrator', 'loom', 'CREATE'
        )
        OR pg_catalog.has_database_privilege(
            'loom_cap_staging_migrator', 'loom', 'TEMPORARY'
        )
        OR pg_catalog.has_database_privilege(
            'loom_cap_staging_owner', 'loom', 'CREATE'
        )
    ) THEN
        RAISE EXCEPTION 'protected staging capacity transient authority is not sealed';
    END IF;
END
$loom$;
COMMIT;
""".encode()
        self._run_peer_payload(payload)

    def _finalize_runtime_credentials(self) -> None:
        payload = b"""\
BEGIN;
ALTER ROLE loom_cap_staging_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_observer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS VALID UNTIL 'infinity';
ALTER ROLE loom_cap_staging_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS VALID UNTIL 'infinity';
COMMIT;
"""
        self._run_peer_payload(payload)

    def _seal_transient_migrator(self, *, preserve_runtime_credentials: bool = True) -> None:
        self._disable_transient_credentials(
            preserve_runtime_credentials=preserve_runtime_credentials
        )
        self._terminate_transient_sessions(
            preserve_runtime_credentials=preserve_runtime_credentials
        )
        self._remove_transient_authority()
        self._verify_transient_authority_sealed(
            preserve_runtime_credentials=preserve_runtime_credentials,
            durable_runtime_credentials=not preserve_runtime_credentials,
        )
        if preserve_runtime_credentials:
            self._finalize_runtime_credentials()
            self._verify_transient_authority_sealed(
                preserve_runtime_credentials=True,
                durable_runtime_credentials=True,
            )

    def _run_peer_payload(self, payload: bytes) -> None:
        self.runner.run_checked(
            _PEER_PSQL_COMMAND,
            env=self.runner.environment,
            input_payload=payload,
            timeout_seconds=_MUTATION_TIMEOUT_SECONDS,
        )

    def _legacy_auth_manifest(self, plan: FinalGatePlan, seed: dict[str, object]) -> bytes:
        """Rebuild the exact pre-transient-role bootstrap resources for safe retirement."""

        documents = [document for document in yaml.safe_load_all(self._manifest(plan, seed))]
        resources = {
            document.get("kind"): document for document in documents if isinstance(document, dict)
        }
        if set(resources) != {"Secret", "Job"} or len(documents) != 2:
            raise ValueError("protected staging capacity legacy manifest is invalid")
        secret_data = resources["Secret"].get("data")
        job_spec = resources["Job"].get("spec")
        if not isinstance(secret_data, dict) or not isinstance(job_spec, dict):
            raise ValueError("protected staging capacity legacy manifest is invalid")
        secret_data.pop("admin-password", None)
        secret_data.pop("admin-username", None)
        template = job_spec.get("template")
        pod_spec = template.get("spec") if isinstance(template, dict) else None
        volumes = pod_spec.get("volumes") if isinstance(pod_spec, dict) else None
        if not isinstance(volumes, list):
            raise ValueError("protected staging capacity legacy manifest is invalid")
        postgres_admin = [
            volume
            for volume in volumes
            if isinstance(volume, dict) and volume.get("name") == "postgres-admin"
        ]
        if len(postgres_admin) != 1:
            raise ValueError("protected staging capacity legacy manifest is invalid")
        postgres_admin[0]["secret"] = {
            "defaultMode": 0o440,
            "items": [
                {"key": "password", "path": "password"},
                {"key": "username", "path": "username"},
            ],
            "secretName": "loom-postgres-cnpg-credentials",
        }
        return cast(
            str,
            yaml.safe_dump_all(documents, sort_keys=True, explicit_start=True),
        ).encode()

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
                "admin-password": base64.b64encode(
                    str(seed["migrator_database_password"]).encode("ascii")
                ).decode("ascii"),
                "admin-username": base64.b64encode(b"loom_cap_staging_migrator").decode("ascii"),
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
                                        {"key": "admin-password", "path": "password"},
                                        {"key": "admin-username", "path": "username"},
                                    ],
                                    "secretName": _NAME,
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


def _expected_roles(
    *,
    runtime_credential_validity: str = "infinite",
) -> dict[str, dict[str, object]]:
    def role(
        *,
        login: bool,
        inherit: bool,
        password: bool,
        credential_validity: str = "infinite",
    ) -> dict[str, object]:
        return {
            "bypass_rls": False,
            "can_login": login,
            "credential_validity": credential_validity,
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
        agent: role(
            login=True,
            inherit=False,
            password=True,
            credential_validity=runtime_credential_validity,
        ),
        executor: role(login=False, inherit=False, password=False),
        migrator: role(login=False, inherit=True, password=False),
        observer: role(
            login=True,
            inherit=False,
            password=True,
            credential_validity=runtime_credential_validity,
        ),
        owner: role(login=False, inherit=False, password=False),
        runtime: role(
            login=True,
            inherit=False,
            password=True,
            credential_validity=runtime_credential_validity,
        ),
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
