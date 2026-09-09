"""Build HTTP admission owns fresh snapshots without changing control/cleanup isolation."""

from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text

from loom_task_image_authority import api
from loom_task_image_authority.http_contracts import TaskImageMaterializationClaimResponseV1
from tests.integration.test_task_image_authority_api import (
    GRANT_ID,
    NOW,
    _claim_request,
    _operation_request,
    _post,
    _put,
    _registry_credential_request,
    _renewed_session,
    _seed_materialization,
)
from tests.integration.test_task_image_authority_api import (
    authority_api as authority_api,
)
from tests.integration.test_task_image_authority_api import (
    registry_token_issuer as registry_token_issuer,
)
from tests.integration.test_task_image_authority_api import (
    test_registry_routes_issue_exact_credentials_and_record_only_inert_candidates as _registry_flow,
)
from tests.integration.test_task_image_authority_api import (
    test_session_routes_drive_claim_bundle_and_lease_operations as _lease_flow,
)
from tests.integration.test_task_image_authority_api import (
    test_v2_candidate_route_persists_and_replays_mandatory_evidence as _candidate_v2_flow,
)


async def test_credential_route_owns_read_committed_and_restores_default_isolation(
    authority_api, isolated_migration_postgres_url, monkeypatch,
):
    observed = []
    original_claim = api.claim_session_materialization
    original_issue = api.issue_session_registry_credential
    original_release = api.release_session_materialization

    async def claim(session, **values):
        observed.append(("claim", await session.scalar(text("SHOW transaction_isolation"))))
        return await original_claim(session, **values)

    async def issue(session, **values):
        observed.append(("credential", await session.scalar(text("SHOW transaction_isolation"))))
        return await original_issue(session, **values)

    async def release(session, **values):
        observed.append(("release", await session.scalar(text("SHOW transaction_isolation"))))
        return await original_release(session, **values)

    monkeypatch.setattr(api, "claim_session_materialization", claim)
    monkeypatch.setattr(api, "issue_session_registry_credential", issue)
    monkeypatch.setattr(api, "release_session_materialization", release)
    materialization_id = await _seed_materialization(isolated_migration_postgres_url)
    build_session = _renewed_session(authority_api)
    authority_api.now[0] = NOW + timedelta(seconds=14)
    claim_path = f"/v1/projections/{GRANT_ID}/materializations/claim"
    request = _claim_request(build_session)
    claimed = _post(authority_api, claim_path, request)
    assert claimed.status_code == 200
    receipt = TaskImageMaterializationClaimResponseV1.model_validate_json(claimed.content)
    path = f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/registry-credential"
    authority_api.now[0] = NOW + timedelta(seconds=15)
    credential_request = _registry_credential_request(build_session, receipt)
    issued = _put(authority_api, path, credential_request)
    assert observed == [("claim", "read committed"), ("credential", "read committed")]
    assert issued.status_code == 200
    replayed = _put(authority_api, path, credential_request)
    assert replayed.status_code == 200
    assert replayed.content == issued.content
    replayed_claim = _post(authority_api, claim_path, request)
    assert replayed_claim.status_code == 200
    assert replayed_claim.content == claimed.content
    assert observed[-2:] == [("credential", "read committed"), ("claim", "read committed")]
    released = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/release",
        _operation_request(build_session, receipt, operation_id=uuid4()),
    )
    assert released.status_code == 200
    assert observed[-1] == ("release", "serializable")


@pytest.mark.parametrize(
    ("exercise", "required"),
    [
        (_lease_flow, {
            "claim_session_materialization", "start_session_materialization",
            "heartbeat_session_materialization", "issue_session_materialization_bundle",
            "release_session_materialization", "fail_session_materialization",
        }),
        (_registry_flow, {"issue_session_registry_credential", "record_session_publication_candidate"}),
        (_candidate_v2_flow, {"record_session_publication_candidate_v2"}),
    ],
    ids=["lease-bundle-cleanup", "credential-candidate-v1", "candidate-v2"],
)
async def test_actual_http_flows_own_admission_mode_and_preserve_control_default(
    authority_api, isolated_migration_postgres_url, monkeypatch, exercise, required,
):
    observed = set()
    admitting = {
        "claim_session_materialization", "start_session_materialization",
        "heartbeat_session_materialization", "issue_session_materialization_bundle",
        "issue_session_registry_credential", "record_session_publication_candidate",
        "record_session_publication_candidate_v2",
    }
    controls = {
        "request_task_image_projection", "complete_task_image_projection",
        "exchange_task_image_bootstrap", "renew_task_image_build_session",
        "release_session_materialization", "fail_session_materialization",
    }

    def trace(name):
        original = getattr(api, name)

        async def call(session, **values):
            expected = "read committed" if name in admitting else "serializable"
            assert await session.scalar(text("SHOW transaction_isolation")) == expected
            observed.add(name)
            return await original(session, **values)

        return call

    for name in admitting | controls:
        monkeypatch.setattr(api, name, trace(name))
    await exercise(authority_api, isolated_migration_postgres_url)
    assert required | {
        "request_task_image_projection", "complete_task_image_projection",
        "exchange_task_image_bootstrap", "renew_task_image_build_session",
    } <= observed
