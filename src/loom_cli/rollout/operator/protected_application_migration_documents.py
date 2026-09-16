"""Derive one credential generation from the immutable admitted migration Job."""

from __future__ import annotations

import base64
import copy
import hashlib
import re
from urllib.parse import quote

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.application_migration_contract import (
    APPLICATION_MIGRATION_CA_PATH,
    APPLICATION_OWNER_ROLE,
    application_migration_authority,
    application_migration_secret_name,
    require_application_migration_job,
)

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import _string
from .protected_application_migration_journal import ApplicationMigrationEvent
from .protected_cnpg_writer_configuration import _mapping
from .protected_migration_component import _verify_job
from .staging_mutation_guard import MutationGuardEvidence


def application_migration_role(generation: ApplicationMigrationEvent) -> str:
    nonce = _string(generation.payload, "nonce")
    if generation.phase != "generation" or re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
        raise ValueError("application migration generation role binding changed")
    return "loom_app_migrate_" + nonce


def application_migration_documents(
    plan: FinalGatePlan, *, template: bytes, generation: ApplicationMigrationEvent,
    guard: MutationGuardEvidence, container_registry: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Only resource names and fixed generation credentials vary from the artifact."""
    if (hashlib.sha256(template).hexdigest() != plan.migration_manifest_sha256
            or generation.guard_digest != guard.evidence_digest or guard.state != "ready"
            or guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
            or guard.candidate_tree != plan.candidate_tree or not container_registry):
        raise ValueError("application migration original artifact or guard changed")
    _verify_job(template, plan, container_registry=container_registry)
    job = _mapping(yaml.safe_load(template))
    require_application_migration_job(job, owner_role=APPLICATION_OWNER_ROLE)
    job = copy.deepcopy(job)
    name = "loom-app-migrate-" + generation.event_digest[:32]
    annotations = {"loom.carin.dev/migration-generation": generation.event_digest,
        "loom.carin.dev/request-id": guard.request_id, "loom.carin.dev/candidate-sha": guard.candidate_sha,
        "loom.carin.dev/candidate-tree": guard.candidate_tree}
    metadata = _mapping(job["metadata"])
    if "annotations" in metadata:
        raise ValueError("application migration template has unexpected annotations")
    metadata.update(name=name, annotations=annotations)
    spec = _mapping(_mapping(_mapping(job["spec"])["template"])["spec"])
    containers = spec["containers"]
    assert isinstance(containers, list)
    container = _mapping(containers[0])
    authority = application_migration_authority(name)
    container["env"] = authority["env"]
    spec["volumes"] = authority["volumes"]
    require_application_migration_job(job, owner_role=APPLICATION_OWNER_ROLE)
    password = _string(generation.payload, "password")
    if re.fullmatch(r"[A-Za-z0-9_-]{64}", password) is None:
        raise ValueError("application migration generation credential is invalid")
    ca = _string(generation.payload, "ca_certificate")
    try:
        certificate = base64.b64decode(ca, validate=True)
    except ValueError:
        raise ValueError("application migration generation CA encoding is invalid") from None
    if not 64 <= len(certificate) <= 65536:
        raise ValueError("application migration generation CA size is invalid")
    url = (f"postgresql+psycopg://{application_migration_role(generation)}:{quote(password, safe='')}"
           "@loom-postgres-rw.loom-staging.svc.cluster.local:5432/loom"
           f"?sslmode=verify-full&sslrootcert={quote(APPLICATION_MIGRATION_CA_PATH, safe='')}")
    secret: dict[str, object] = {"apiVersion": "v1", "kind": "Secret", "metadata": {
        "namespace": "loom-staging", "name": application_migration_secret_name(name), "annotations": annotations},
        "type": "Opaque", "immutable": True, "data": {"db-url": base64.b64encode(url.encode()).decode(), "ca.crt": ca}}
    return job, secret
