"""Credential-first inventory must cover uploads with no candidate callback."""

from __future__ import annotations

import hashlib
import importlib
from dataclasses import FrozenInstanceError
from datetime import timedelta
from uuid import uuid4

import pytest
import rfc8785

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageRegistryCredentialGeneration,
)
from loom.task_image_build_plan import derive_task_image_build_plan
from loom.task_image_materialization import task_image_materialization_key
from loom_task_image_authority.registry_token import publication_repository
from tests.unit.test_task_image_build_plan import _authorization, _row
from tests.unit.test_task_image_registry_contracts import (
    ATTEMPT_ID,
    GRANT_ID,
    MATERIALIZATION_ID,
    NOW,
    SESSION_ID,
    _credential,
)

ORIGIN = "https://registry.example:5443"


def inventory_module():
    name = "loom_task_image_authority.retention_inventory"
    assert importlib.util.find_spec(name) is not None, "credential-first inventory missing"
    return importlib.import_module(name)


def fixture(arch="arm64"):
    materialization = TaskImageMaterialization(
        **vars(_row(id=MATERIALIZATION_ID, cpu_arch=arch)),
        materialization_key=task_image_materialization_key(
            task_id="bench/task-1", task_checksum="4" * 64, cpu_arch=arch
        ),
    )
    plan = derive_task_image_build_plan(
        materialization,
        _authorization(
            grant_id=GRANT_ID, session_id=SESSION_ID, session_generation=1, cpu_arch=arch
        ),
    )
    public_plan = plan.model_dump(mode="json", exclude_none=False)
    attempt = TaskImageMaterializationAttempt(
        id=ATTEMPT_ID,
        materialization_id=materialization.id,
        attempt_number=4,
        lease_epoch=3,
        builder_id=plan.builder_id,
        grant_id=GRANT_ID,
        session_id=SESSION_ID,
        session_generation=1,
        claim_id=uuid4(),
        claim_plan_json=public_plan,
        claim_plan_sha256=hashlib.sha256(rfc8785.dumps(public_plan)).hexdigest(),
        claimed_at=NOW - timedelta(seconds=1),
    )
    return materialization, attempt, plan


def credential_row(plan, component="task", predecessor=None, **changes):
    generation = 1 if predecessor is None else predecessor.generation + 1
    issued_at = changes.pop("issued_at", NOW + timedelta(seconds=10 * (generation - 1)))
    credential = _credential(
        credential_id=uuid4(),
        request_id=uuid4(),
        cpu_arch=plan.cpu_arch,
        platform=plan.platform,
        component=component,
        builder_id=plan.builder_id,
        generation=generation,
        session_generation=generation,
        attestation_generation=generation,
        predecessor_credential_id=None if predecessor is None else predecessor.credential_id,
        predecessor_generation=None if predecessor is None else predecessor.generation,
        lease_heartbeat_operation_id=None if predecessor is None else uuid4(),
        issued_at=issued_at,
        expires_at=changes.pop("expires_at", issued_at + timedelta(seconds=45)),
        repository=publication_repository(
            purpose="production",
            shadow_campaign_id=None,
            cpu_arch=plan.cpu_arch,
            attempt_id=ATTEMPT_ID,
            component=component,
        ),
        **changes,
    )
    values = credential.model_dump(mode="python")
    values["materialization_attempt_id"] = values.pop("attempt_id")
    values["token_hash"] = hashlib.sha256(credential.bearer_token.encode()).digest()
    fields = set(TaskImageRegistryCredentialGeneration.__table__.columns.keys())
    row = TaskImageRegistryCredentialGeneration(**{k: v for k, v in values.items() if k in fields})
    row.response_public_json = credential.public_binding()
    row.response_sha256 = hashlib.sha256(rfc8785.dumps(row.response_public_json)).hexdigest()
    return row


def derive(materialization, attempt, credentials):
    return inventory_module().derive_attempt_repository_inventory(
        materialization=materialization,
        attempt=attempt,
        credentials=credentials,
        registry_origin=ORIGIN,
    )


@pytest.mark.parametrize("arch", ["x86_64", "arm64"])
@pytest.mark.parametrize("component", ["task", "sidecar:cache"])
def test_credential_before_any_candidate_is_sufficient_inventory(arch, component):
    materialization, attempt, plan = fixture(arch)
    credential = credential_row(plan, component)
    inventory = derive(materialization, attempt, [credential])
    assert len(inventory.repositories) == 1
    item = inventory.repositories[0]
    assert item.component == component
    assert item.repository == credential.repository
    assert item.last_credential_expires_at == credential.expires_at
    assert item.credential_count == 1
    assert inventory.attempt_id == attempt.id
    assert inventory.materialization_id == materialization.id
    assert inventory.registry_origin == ORIGIN
    with pytest.raises((FrozenInstanceError, AttributeError)):
        item.repository = "loom-trial-cache/another-repository"
    assert b"header.payload.signature" not in inventory.canonical_bytes
    assert b"secret_response_ref" not in inventory.canonical_bytes


def test_inventory_collapses_successors_and_orders_components_deterministically():
    materialization, attempt, plan = fixture()
    first = credential_row(plan)
    second = credential_row(plan, predecessor=first)
    sidecar = credential_row(plan, "sidecar:cache")
    values = [sidecar, second, first]
    result = derive(materialization, attempt, values)
    assert (
        result.canonical_bytes
        == derive(materialization, attempt, list(reversed(values))).canonical_bytes
    )
    assert [item.component for item in result.repositories] == ["task", "sidecar:cache"]
    assert result.repositories[0].credential_count == 2
    assert result.repositories[0].last_credential_expires_at == second.expires_at
    before = result.canonical_bytes
    first.response_public_json.clear()
    values.clear()
    assert result.canonical_bytes == before


def test_no_issued_credentials_produces_no_deletion_paths():
    materialization, attempt, _ = fixture()
    assert derive(materialization, attempt, []).repositories == ()


def test_historical_registry_identity_rotation_preserves_inventory():
    materialization, attempt, plan = fixture()
    first = credential_row(plan)
    successor = credential_row(
        plan,
        predecessor=first,
        registry_service="rotated-registry-service",
        registry_issuer="rotated-issuer",
        registry_key_id="L" * 43,
    )
    inventory = derive(materialization, attempt, [first, successor])
    assert len(inventory.repositories) == 1
    assert inventory.repositories[0].credential_count == 2
    assert inventory.repositories[0].last_credential_expires_at == successor.expires_at


def test_same_second_successor_preserves_inventory():
    materialization, attempt, plan = fixture()
    first = credential_row(plan)
    successor = credential_row(plan, predecessor=first, issued_at=first.issued_at)
    inventory = derive(materialization, attempt, [first, successor])
    assert inventory.repositories[0].credential_count == 2


def test_expiry_bound_covers_earlier_generation_with_longer_lifetime():
    materialization, attempt, plan = fixture()
    first = credential_row(plan)
    successor = credential_row(plan, predecessor=first, expires_at=NOW + timedelta(seconds=20))
    assert successor.expires_at < first.expires_at
    inventory = derive(materialization, attempt, [successor, first])
    assert inventory.repositories[0].last_credential_expires_at == first.expires_at


@pytest.mark.parametrize(
    "corruption",
    [
        "hash",
        "unknown_public_field",
        "row_binding",
        "origin",
        "repository",
        "component",
        "attempt",
        "plan_hash",
        "plan_binding",
        "materialization_key",
        "duplicate",
        "missing_predecessor",
        "naive_expiry",
        "purpose",
    ],
)
def test_inventory_rejects_ambiguous_or_substituted_authority(corruption):
    materialization, attempt, plan = fixture()
    credential = credential_row(plan)
    credentials = [credential]
    public = credential.response_public_json
    if corruption == "hash":
        credential.response_sha256 = "f" * 64
    elif corruption == "unknown_public_field":
        public["unexpected"] = "value"
    elif corruption == "row_binding":
        credential.lease_epoch += 1
    elif corruption in {"origin", "repository", "component", "purpose"}:
        field, value = {
            "origin": ("registry_origin", "https://another.example"),
            "repository": ("repository", "loom-trial-cache/wrong"),
            "component": ("component", "sidecar:not-in-frozen-plan"),
            "purpose": ("purpose", "shadow"),
        }[corruption]
        public[field] = value
        if hasattr(credential, field):
            setattr(credential, field, value)
    elif corruption == "attempt":
        attempt.id = uuid4()
    elif corruption == "plan_hash":
        attempt.claim_plan_sha256 = "f" * 64
    elif corruption == "plan_binding":
        attempt.claim_plan_json["task_id"] = "another/task"
        attempt.claim_plan_sha256 = hashlib.sha256(
            rfc8785.dumps(attempt.claim_plan_json)
        ).hexdigest()
    elif corruption == "materialization_key":
        materialization.materialization_key = "f" * 64
    elif corruption == "duplicate":
        credentials.append(credential)
    elif corruption == "missing_predecessor":
        credentials = [credential_row(plan, predecessor=credential)]
    elif corruption == "naive_expiry":
        credential.expires_at = credential.expires_at.replace(tzinfo=None)
        public["expires_at"] = credential.expires_at.isoformat()
    if corruption != "hash":
        credential.response_sha256 = hashlib.sha256(rfc8785.dumps(public)).hexdigest()
    with pytest.raises(ValueError):
        derive(materialization, attempt, credentials)
