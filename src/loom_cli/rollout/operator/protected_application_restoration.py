"""Observe original database, credential, process and workload restoration together.

This is read-only evidence inside the admitted handoff. It does not establish
administrator exclusion, retire old server work, mutate SQL/workloads, publish a
terminal or release a fence. The enclosing operation preserves those boundaries.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, replace
from typing import TYPE_CHECKING, Protocol

from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_runtime_login import (
    ApplicationRuntimeLoginState,
    observe_application_runtime_login,
)
from loom.application_schema_reference import application_schema_revision
from loom.staging_mutation_coordination import rollout_guard_application_name

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import admission_record_digest
from .protected_application_credential_recovery import observe_application_runtime_credential
from .protected_application_database_completion import _STAGING_ROLE_BINDINGS
from .protected_application_owner_preparation import APPLICATION_OWNER_ROLE
from .protected_application_workload_runtime import (
    ApplicationWorkloadRunner,
    observe_recovered_application_workloads,
)
from .protected_application_workloads import _digest, validate_workload_inventory
from .protected_cnpg_manager_replacement import CNPGManagerReplacementReceipt
from .protected_cnpg_runtime_admission import observe_cnpg_primary_runtime
from .staging_mutation_guard import MutationGuardEvidence

if TYPE_CHECKING:
    from .protected_apply_journal import ApplicationRecoveryView


class ApplicationRestorationRunner(ApplicationWorkloadRunner, Protocol):
    def open_staging_peer_database(self) -> AbstractContextManager[ApplicationDatabaseConnection]: ...


@dataclass(frozen=True, slots=True)
class ApplicationRestorationEvidence:
    intent_digest: str
    admission_sha256: str
    runtime_sha256: str
    credential_sha256: str
    configuration_sha256: str
    workloads_sha256: str

    def __post_init__(self) -> None:
        if any(not isinstance(value, str) or re.fullmatch(r'[0-9a-f]{64}', value) is None for value in asdict(self).values()):
            raise ValueError('application restoration evidence is invalid')

    def to_dict(self) -> dict[str, object]:
        return {'schema_version': 1, **asdict(self)}

    @property
    def digest(self) -> str:
        return admission_record_digest(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationRestorationEvidence:
        if (set(value) != {'schema_version', *cls.__dataclass_fields__}
                or type(value['schema_version']) is not int or value['schema_version'] != 1
                or any(not isinstance(value[key], str) for key in cls.__dataclass_fields__)):
            raise ValueError('application restoration evidence fields are invalid')
        return cls(**{key: str(value[key]) for key in cls.__dataclass_fields__})


def _bound_evidence(view: ApplicationRecoveryView) -> ApplicationRestorationEvidence:
    """Check saved binding structure only; this cannot supply live restoration evidence."""
    admission, runtime, replacement = view.admission, view.cnpg_runtime, view.manager_replacement
    binding, configuration = view.credential_binding, view.cnpg_configuration
    if (admission is None or admission.coordination_guard is None or runtime is None or replacement is None
            or binding is None or configuration is None or not view.workloads or view.workloads_restoring is not True
            or not view.owner_creations or (view.handoff_recoveries and view.handoff_recoveries[-1][1] is None)):
        raise RuntimeError('application restoration lacks its original completed phase bindings')
    intent, dispatched, receipt = replacement
    if (dispatched is not True or receipt is None or intent.identity != runtime.manager
            or intent.component_intent_digest != view.intent.intent_digest
            or admission.intent_digest != view.intent.intent_digest
            or intent.admission_digest != admission_record_digest(admission.to_dict())
            or configuration.cluster_uid != runtime.cluster_uid
            or admission.target.database != 'loom' or admission.target.owner_role != 'loom'
            or admission.target.successor_role != APPLICATION_OWNER_ROLE
            or view.owner_creations[-1][1] != admission.target.successor_oid
            or any(record.coordination_guard != admission.coordination_guard for record, _ in view.owner_creations)):
        raise RuntimeError('application restoration original bindings changed')
    if receipt != CNPGManagerReplacementReceipt.validate(intent, receipt.identity):
        raise RuntimeError('application restoration manager receipt changed')
    return ApplicationRestorationEvidence(view.intent.intent_digest, admission_record_digest(admission.to_dict()),
        admission_record_digest(replace(runtime, manager=receipt.identity).to_dict()),
        admission_record_digest(asdict(binding)), admission_record_digest(asdict(configuration)),
        _digest([item.to_dict() for item in validate_workload_inventory(view.workloads)]))


def observe_application_restoration(
    plan: FinalGatePlan, *, view: ApplicationRecoveryView, runner: ApplicationRestorationRunner,
    guard: MutationGuardEvidence, service_uid: int,
) -> ApplicationRestorationEvidence:
    """Require current restored login/schema, original inputs and ready workloads.

    Credential, process and SQL observations bracket workload readiness. Results
    are non-secret and stable across identical observations, without journal writes
    or fsync. A recorded restoration intent or database-only success is insufficient.
    The installed caller independently verifies supervised guard liveness and
    continuous writer exclusion; SQL observations never reacquire its lock.
    """
    expected = _bound_evidence(view)
    admission, original = view.admission, view.cnpg_runtime
    assert admission is not None and admission.coordination_guard is not None and original is not None
    if (view.intent.plan_digest != plan.plan_digest or view.intent.request_id != plan.request_id
            or view.intent.attempt_number != plan.attempt_number
            or guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
            or guard.candidate_tree != plan.candidate_tree or guard.mutation_epoch != plan.starting_mutation_epoch
            or guard.state != 'ready' or admission.coordination_guard.backend.pid != guard.database_backend_pid
            or admission.coordination_guard.application_name != rollout_guard_application_name(
                request_id=guard.request_id, candidate_sha=guard.candidate_sha,
                candidate_tree=guard.candidate_tree, generation=guard.generation)):
        raise RuntimeError('application restoration original plan or guard changed')

    def observe_database_and_inputs() -> None:
        runtime = observe_cnpg_primary_runtime(runner, cluster_uid=original.cluster_uid, pod_name=original.manager.pod_name)
        if admission_record_digest(runtime.to_dict()) != expected.runtime_sha256:
            raise RuntimeError('application restoration original runtime changed')
        credential = observe_application_runtime_credential(plan, runner=runner, service_uid=service_uid)
        if (credential.binding != view.credential_binding or credential.configuration != view.cnpg_configuration):
            raise RuntimeError('application restoration original credential or configuration changed')
        with runner.open_staging_peer_database() as connection:
            state = observe_application_runtime_login(
                connection, owner_role=admission.target.successor_role, role_bindings=_STAGING_ROLE_BINDINGS,
                password=credential.credential.password, target=admission.target,
                schema_acl_profile='cnpg-staging',
                schema_revision=application_schema_revision(public_revision=plan.public_schema_revision, guard_revision=plan.capacity_guard_schema_revision), coordination_guard=admission.coordination_guard,
            )
        if state is not ApplicationRuntimeLoginState.RESTORED:
            raise RuntimeError('application restoration original runtime login is not restored')

    observe_database_and_inputs()
    workloads = observe_recovered_application_workloads(plan, runner=runner, guard=guard, workloads=view.workloads)
    if workloads != expected.workloads_sha256:
        raise RuntimeError('application restoration original workloads changed')
    observe_database_and_inputs()
    return expected
