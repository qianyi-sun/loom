"""Generation-bound capacity bootstrap resources beneath the retained journal.

The installed lifecycle admits the preceding ownership/migration terminals,
original role identities, credential seed and CA before constructing this adapter.
Dispatch must be durable before creation. Cleanup uses only observation and
UID/resource-version preconditioned deletion; construction grants no SQL authority.
"""

from __future__ import annotations

import base64
import copy
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

import yaml  # type: ignore[import-untyped]

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import _string
from .protected_application_migration_journal import ApplicationMigrationEvent
from .protected_application_migration_resources import ProtectedApplicationMigrationResources
from .protected_cnpg_writer_configuration import _mapping
from .protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
)
from .staging_mutation_guard import MutationGuardEvidence


def capacity_bootstrap_resources(
    *, base: KubernetesProtectedStagingCapacityDatabaseComponent, plan: FinalGatePlan,
    seed: Mapping[str, object], generation: ApplicationMigrationEvent, guard: MutationGuardEvidence,
    assert_guard: Callable[[], MutationGuardEvidence],
) -> ProtectedApplicationMigrationResources:
    job, secret = _documents(base, plan, seed, generation, guard)
    return _CapacityBootstrapResources(runner=base.runner, job=job, secret=secret, guard=guard,
        assert_guard=assert_guard, base=base, plan=plan, seed=seed, generation=generation)


@dataclass(frozen=True, slots=True)
class _CapacityBootstrapResources(ProtectedApplicationMigrationResources):
    base: KubernetesProtectedStagingCapacityDatabaseComponent
    plan: FinalGatePlan
    seed: Mapping[str, object] = field(repr=False)
    generation: ApplicationMigrationEvent

    def __post_init__(self) -> None:
        job, secret = _documents(self.base, self.plan, self.seed, self.generation, self.guard)
        if self.runner is not self.base.runner or not callable(self.assert_guard) or self.job != job or self.secret != secret:
            raise ValueError("application capacity bootstrap resource binding changed")
        object.__setattr__(self, "seed", copy.deepcopy(dict(self.seed)))
        self._freeze_documents()


def _documents(
    base: KubernetesProtectedStagingCapacityDatabaseComponent, plan: FinalGatePlan,
    seed: Mapping[str, object], generation: ApplicationMigrationEvent, guard: MutationGuardEvidence,
) -> tuple[dict[str, object], dict[str, object]]:
    if (type(base) is not KubernetesProtectedStagingCapacityDatabaseComponent
            or type(guard) is not MutationGuardEvidence or guard.state != "ready"
            or generation.phase != "generation" or generation.guard_digest != guard.evidence_digest
            or ApplicationMigrationEvent.from_dict(generation.to_dict()) != generation
            or guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
            or guard.candidate_tree != plan.candidate_tree or plan.namespace != "loom-staging"
            or plan.environment != "staging" or not base.container_registry):
        raise ValueError("application capacity bootstrap generation authority changed")
    password = _string(generation.payload, "password")
    ca = _string(generation.payload, "ca_certificate")
    if re.fullmatch(r"[A-Za-z0-9_-]{64}", password) is None:
        raise ValueError("application capacity bootstrap generation credential is invalid")
    try:
        certificate = base64.b64decode(ca, validate=True)
    except ValueError:
        raise ValueError("application capacity bootstrap CA encoding is invalid") from None
    if not 64 <= len(certificate) <= 65536:
        raise ValueError("application capacity bootstrap CA size is invalid")
    effective = {**copy.deepcopy(dict(seed)), "migrator_database_password": password}
    secret, job = (_mapping(value) for value in yaml.safe_load_all(base._manifest(plan, effective)))
    name = "loom-cap-bootstrap-" + generation.event_digest[:32]
    annotations = {"loom.carin.dev/migration-generation": generation.event_digest,
        "loom.carin.dev/request-id": guard.request_id, "loom.carin.dev/candidate-sha": plan.candidate_sha,
        "loom.carin.dev/candidate-tree": plan.candidate_tree, "loom.carin.dev/plan-digest": plan.plan_digest}
    for document in (job, secret):
        _mapping(document["metadata"]).update(name=name, annotations=annotations)
    _mapping(secret["data"])["ca.crt"] = ca
    spec = _mapping(job["spec"])
    # These values remain the API defaults; normalize desired and observed Jobs
    # identically so explicit server defaults cannot cause a cleanup mismatch.
    if spec.pop("parallelism") != 1 or spec.pop("completions") != 1:
        raise ValueError("application capacity bootstrap Job parallelism changed")
    pod = _mapping(_mapping(spec["template"])["spec"])
    pod["enableServiceLinks"] = False
    volumes = pod["volumes"]
    if (not isinstance(volumes, list)
            or [v.get("name") for v in volumes if isinstance(v, dict)] != ["bootstrap", "postgres-admin", "postgres-ca"]):
        raise ValueError("application capacity bootstrap volume contract changed")
    for volume in volumes:
        _mapping(_mapping(volume)["secret"])["secretName"] = name
    return job, secret
