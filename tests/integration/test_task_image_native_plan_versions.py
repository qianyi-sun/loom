"""Registered native V2 claims retain authority through downstream consumers."""

import hashlib
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import rfc8785

from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_build_plan import parse_task_image_build_plan
from loom.task_image_materialization import ensure_task_image_materializations
from loom_task_image_authority.contracts import TaskImagePublicationCandidateRequestV2
from loom_task_image_authority.materializations import claim_session_materialization
from loom_task_image_authority.publication_store import submit_publication_job
from loom_task_image_authority.retirement_snapshot import prepare_attempt_retirement_inventory
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_journal import _publish, _receipts, _upload
from tests.integration.test_task_image_authority_materializations import (
    _active_authorization,
    _attempt,
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
from tests.unit.test_task_bundle_registration import _bundle


async def _retained_strong_attempt(factory, tmp_path, *, mismatched=False):
    directory = _bundle(tmp_path)
    config = directory / "task.toml"
    config.write_text(config.read_text().replace("[environment]", '[environment]\ncpu_arch = "arm64"'))
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(directory, task_id="benchmark/" + uuid4().hex),
        bucket="task-sources",
    )
    ticket = await _upload(factory, spec)
    await _receipts(factory, ticket)
    await _publish(factory, ticket)
    # Registration and claim are separate real transactions, preserving the
    # parent/image/source lock order instead of constructing synthetic receipts.
    async with factory.begin() as session:
        authorization, _, _, build_session, secrets = await _active_authorization(session)
        image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        image_id = image.id
    async with factory.begin() as session:
        claim_id = uuid4()
        row, plan = await claim_session_materialization(
            session, authorization=authorization, claim_id=claim_id,
            now=NOW + timedelta(seconds=10), lease_seconds=300,
        )
        assert row.id == image_id
        assert plan.content_manifest_digest == spec.manifest.digest
        attempt = await _attempt(session, claim_id=claim_id)
        if mismatched:
            payload = dict(plan.model_dump(mode="json"), bundle_content_manifest_sha256="7" * 64, bundle_prefix=f"bench/revision/{'7' * 64}/")
            changed = parse_task_image_build_plan(json.dumps(payload))
            attempt.claim_plan_json = changed.model_dump(mode="json")
            attempt.claim_plan_sha256 = hashlib.sha256(rfc8785.dumps(attempt.claim_plan_json)).hexdigest()
    return authorization, build_session, secrets, row, attempt


async def test_v2_receipt_flows_through_credentials_publication_and_retirement_snapshot(registry_authority_session, registry_issuer, tmp_path):
    auth, build_session, secrets, row, attempt = await _retained_strong_attempt(registry_authority_session, tmp_path)
    async with registry_authority_session() as session:
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


async def test_v2_receipt_digest_mismatch_rejects_credential_and_detached_retirement(registry_authority_session, registry_issuer, tmp_path):
    auth, build_session, secrets, row, attempt = await _retained_strong_attempt(registry_authority_session, tmp_path, mismatched=True)
    async with registry_authority_session() as session:
        with pytest.raises(RuntimeError, match="frozen claim plan changed"):
            await _issue_first(session, authorization=auth, build_session=build_session, secrets=secrets, row=row, attempt=attempt, issuer=registry_issuer)
        await session.commit()
    with pytest.raises(ValueError):
        await prepare_attempt_retirement_inventory(
            registry_authority_session.kw["bind"], attempt_id=attempt.id, registry_origin=registry_issuer.registry_origin,
        )
