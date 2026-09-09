"""Credential HTTP admission owns its required isolation without changing other routes."""

from datetime import timedelta

from sqlalchemy import text

from loom_task_image_authority import api
from loom_task_image_authority.http_contracts import TaskImageMaterializationClaimResponseV1
from tests.integration.test_task_image_authority_api import (
    GRANT_ID,
    NOW,
    _claim_request,
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


async def test_credential_route_owns_read_committed_and_restores_default_isolation(
    authority_api, isolated_migration_postgres_url, monkeypatch,
):
    observed = []
    original_claim = api.claim_session_materialization
    original_issue = api.issue_session_registry_credential

    async def claim(session, **values):
        observed.append(("claim", await session.scalar(text("SHOW transaction_isolation"))))
        return await original_claim(session, **values)

    async def issue(session, **values):
        observed.append(("credential", await session.scalar(text("SHOW transaction_isolation"))))
        return await original_issue(session, **values)

    monkeypatch.setattr(api, "claim_session_materialization", claim)
    monkeypatch.setattr(api, "issue_session_registry_credential", issue)
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
    assert observed == [("claim", "serializable"), ("credential", "read committed")]
    assert issued.status_code == 200
    replayed = _put(authority_api, path, credential_request)
    assert replayed.status_code == 200
    assert replayed.content == issued.content
    replayed_claim = _post(authority_api, claim_path, request)
    assert replayed_claim.status_code == 200
    assert replayed_claim.content == claimed.content
    assert observed[-2:] == [("credential", "read committed"), ("claim", "serializable")]
