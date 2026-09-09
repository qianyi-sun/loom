"""Persisted strong receipts exercise consumers, not native claim admission."""

import hashlib
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import rfc8785

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom.task_image_build_plan import parse_task_image_build_plan
from loom.task_image_materialization import task_image_materialization_key
from loom_task_image_authority.contracts import TaskImagePublicationCandidateRequestV2
from loom_task_image_authority.publication_store import submit_publication_job
from loom_task_image_authority.retirement_snapshot import prepare_attempt_retirement_inventory
from tests.integration.test_task_image_authority_materializations import (
    _active_authorization,
    _config,
)
from tests.integration.test_task_image_candidate_v2 import _record
from tests.integration.test_task_image_registry_credentials import (
    NOW,
    _candidate_request,
    _issue_first,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_store import observe
from tests.unit.test_task_image_build_plan_versions import strong_payload


async def _retained_strong_attempt(session, *, mismatched=False):
    authorization, _, _, build_session, secrets = await _active_authorization(session)
    materialization_id = uuid4()
    payload = dict(
        strong_payload(), materialization_id=str(materialization_id),
        grant_id=str(authorization.grant_id), session_id=str(authorization.session_id),
        session_generation=authorization.session_generation,
        builder_id=f"rootless:{authorization.session_id.hex}",
        authorization_expires_at=min(authorization.attestation_expires_at, authorization.session_expires_at, authorization.grant_expires_at).isoformat(),
    )
    plan = parse_task_image_build_plan(json.dumps(payload))
    row = TaskImageMaterialization(
        id=materialization_id, task_id=plan.task_id, task_checksum=plan.task_checksum,
        cpu_arch=plan.cpu_arch, bundle_content_manifest_sha256=plan.content_manifest_digest,
        materialization_key=task_image_materialization_key(
            task_id=plan.task_id, task_checksum=plan.task_checksum, cpu_arch=plan.cpu_arch,
            bundle_content_manifest_sha256=plan.content_manifest_digest,
        ),
        task_config=_config(task_id=plan.task_id),
        task_source=f"s3://{plan.bundle_bucket}/{plan.bundle_prefix}",
        task_source_provenance={
            "bundle_content_manifest_sha256": plan.content_manifest_digest,
            "bundle_file_metadata_sha256": "sha256:" + plan.bundle_file_metadata_sha256,
        },
        state="claimed", claimed_by=plan.builder_id, lease_epoch=1, attempt_count=0,
        claimed_at=NOW + timedelta(seconds=10), lease_expires_at=NOW + timedelta(seconds=310),
    )
    session.add(row)
    await session.flush()
    if mismatched:
        payload.update(bundle_content_manifest_sha256="7" * 64, bundle_prefix=f"bench/revision/{'7' * 64}/")
        plan = parse_task_image_build_plan(json.dumps(payload))
    public = plan.model_dump(mode="json")
    attempt = TaskImageMaterializationAttempt(
        materialization_id=row.id, attempt_number=1, lease_epoch=1,
        builder_id=plan.builder_id, grant_id=plan.grant_id, session_id=plan.session_id,
        session_generation=plan.session_generation, claim_id=uuid4(),
        claim_deterministic_failure_count=0, claim_lease_expires_at=row.lease_expires_at,
        claim_plan_json=public, claim_plan_sha256=hashlib.sha256(rfc8785.dumps(public)).hexdigest(),
        claimed_at=row.claimed_at,
    )
    # Direct fixture insertion is deliberate: production derivation still rejects
    # strong rows until complete capability and Go verification are implemented.
    session.add(attempt)
    await session.flush()
    return authorization, build_session, secrets, row, attempt


async def test_v2_receipt_flows_through_credentials_publication_and_retirement_snapshot(registry_authority_session, registry_issuer):
    async with registry_authority_session() as session:
        auth, build_session, secrets, row, attempt = await _retained_strong_attempt(session)
        _, credential = await _issue_first(session, authorization=auth, build_session=build_session, secrets=secrets, row=row, attempt=attempt, issuer=registry_issuer)
        legacy = _candidate_request(auth, build_session, row, attempt, credential_id=credential.credential_id, credential_generation=credential.generation)
        request = TaskImagePublicationCandidateRequestV2.model_validate(dict(
            legacy.model_dump(), schema_version=2, base_resolution={
                "schema": "loom.task-image-base-resolution/v1", "solve_ref": "same-solve_1",
                "platform": "linux/arm64", "output_digest": "sha256:" + "a" * 64,
                "observed_base_digests": [],
            },
        ))
        await _record(session, auth, request)
        job = await submit_publication_job(
            session, authorization=auth, operation_id=uuid4(), materialization_id=row.id,
            attempt_id=attempt.id, lease_epoch=1, registry_origin=registry_issuer.registry_origin,
            clock=lambda: NOW + timedelta(seconds=14),
        )
        assert job.snapshot.materialization_key == row.materialization_key
        await session.commit()
    prepared = await prepare_attempt_retirement_inventory(
        registry_authority_session.kw["bind"], attempt_id=attempt.id, registry_origin=registry_issuer.registry_origin,
    )
    assert prepared.credential_count == 1
    assert parse_task_image_build_plan(prepared.canonical_plan).content_manifest_digest == row.bundle_content_manifest_sha256
    assert prepared.materialization_values[-1] == row.bundle_content_manifest_sha256
    assert prepared.inventory.repositories[0].repository == credential.repository
    # Exercise the retirement owner's publication-snapshot parser too; preparing
    # only credential inventory would not cover its independent plan read.
    instant = max(job.deadline, row.lease_expires_at) + timedelta(seconds=1)
    assert (await observe(registry_authority_session, attempt.id, instant)).status == "observing"


async def test_v2_receipt_digest_mismatch_rejects_credential_and_detached_retirement(registry_authority_session, registry_issuer):
    async with registry_authority_session() as session:
        auth, build_session, secrets, row, attempt = await _retained_strong_attempt(session, mismatched=True)
        with pytest.raises(RuntimeError):
            await _issue_first(session, authorization=auth, build_session=build_session, secrets=secrets, row=row, attempt=attempt, issuer=registry_issuer)
        await session.commit()
    with pytest.raises(ValueError):
        await prepare_attempt_retirement_inventory(
            registry_authority_session.kw["bind"], attempt_id=attempt.id, registry_origin=registry_issuer.registry_origin,
        )
