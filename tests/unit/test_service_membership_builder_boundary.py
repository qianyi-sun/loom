"""Membership cannot inherit unaccounted legacy build execution authority."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response
from fastapi.testclient import TestClient

from loom.personal_dev_native_builder_protocol import PersonalDevNativeBuilderSigner
from loom_service.personal_dev_builder import build_personal_dev_builder_runtime
from loom_service.routes.personal_dev_candidates import create_personal_dev_candidate
from tests.unit.test_dev_instance_routes import _OWNER, _ctx, _request
from tests.unit.test_personal_dev_native_builder_routes import (
    _KEY_ID,
    _PRIVATE_KEY,
    _app,
    _poll,
    _signed_headers,
    _Store,
)
from tests.unit.test_service_membership_runtime import _configured


@pytest.mark.parametrize("native_enabled", (False, True))
def test_membership_builder_is_inert_before_loading_legacy_credentials(tmp_path, native_enabled):
    settings = _configured(tmp_path)
    settings.personal_dev_native_builder_enabled = native_enabled
    assert settings.personal_dev_builder_enabled is True
    assert build_personal_dev_builder_runtime(settings, minio_client=object()) is None


def test_membership_poll_cannot_claim_a_legacy_grant():
    now = datetime.now(UTC)
    app, sessions = _app(_Store(), now)
    app.state.personal_dev_runtime_mode = "membership-v1"
    poll = _poll(now)
    signer = PersonalDevNativeBuilderSigner(keys={_KEY_ID: _PRIVATE_KEY})
    response = TestClient(app).post(
        "/api/v1/internal/personal-dev/native-builder/poll",
        content=poll.canonical_bytes(),
        headers=_signed_headers(signer.sign_poll(poll)),
    )
    assert response.status_code == 503
    assert sessions.entries == 0
    assert "allocation" in response.json()["detail"]


@pytest.mark.parametrize("available", (None, False))
async def test_membership_intake_rejects_unavailable_builder_before_upload(available):
    request = _request(object(), configured=False)
    request.app.state.personal_dev_runtime_mode = "membership-v1"
    request.app.state.personal_dev_builder_available = available
    request.app.state.settings = SimpleNamespace(dev_instances_enabled=True)
    with pytest.raises(HTTPException) as error:
        await create_personal_dev_candidate(
            request, Response(), (object(), _ctx(_OWNER)), object(), "a" * 64, "b" * 64,
        )
    assert error.value.status_code == 503
    assert "builder" in error.value.detail
