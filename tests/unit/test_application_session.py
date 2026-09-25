"""Protected audience configuration cannot silently weaken shared-DB auth."""

from __future__ import annotations

import json
import hashlib
from uuid import UUID

import pytest

from loom_service.config import LoomServiceSettings

APPLICATION_ID = "c18a28fe-22e1-4aeb-ab82-fd496c38309e"
AUDIENCE = {
    "schema_version": "loom.application-session-audience.v1",
    "application_id": APPLICATION_ID,
    "origin": "https://alice.dev.example.com",
    "access_generation": 1,
}


def _settings(**overrides):
    values = {
        "_env_file": None,
        "db_url": "postgresql+psycopg://u:p@localhost/loom",
        "minio_access_key": "x", "minio_secret_key": "y",
        "public_base_url": "https://alice.dev.example.com",
        "auth_local_http": False,
        "auth_session_audience_json": json.dumps(AUDIENCE),
    }
    return LoomServiceSettings(**(values | overrides))


def test_audience_is_validated_and_canonicalized_without_changing_legacy_default():
    audience = _settings().session_audience
    assert audience.application_id == UUID(APPLICATION_ID)
    assert audience.origin == "https://alice.dev.example.com"
    assert audience.access_generation == 1
    with pytest.raises(ValueError):
        audience.access_generation = 2
    equivalent = _settings(auth_session_audience_json=json.dumps(AUDIENCE | {
        "origin": "https://ALICE.dev.example.com:443/",
    }))
    assert equivalent.session_audience == audience
    assert _settings(auth_session_audience_json=None).session_audience is None


@pytest.mark.parametrize("updates", [
    {"application_id": "00000000-0000-0000-0000-000000000000"},
    {"application_id": "alice"},
    {"access_generation": 0}, {"access_generation": -1},
    {"access_generation": True}, {"access_generation": "1"},
    {"schema_version": "loom.application-session-audience.v2"},
    {"extra_authority": True},
    {"origin": "http://alice.dev.example.com"},
    {"origin": "https://alice.dev.example.com/api"},
    {"origin": "https://alice.dev.example.com?next=anything"},
    {"origin": "https://alice.dev.example.com#ignored"},
    {"origin": "https://user:password@alice.dev.example.com"},
    {"origin": " https://alice.dev.example.com"},
    {"origin": "https://alice.dev.example.com\n"},
    {"origin": "https://alice.dev.example.com:0"},
])
def test_invalid_audience_is_rejected(updates):
    from loom.application_session import ApplicationSessionAudienceV1

    with pytest.raises(ValueError):
        ApplicationSessionAudienceV1.model_validate(AUDIENCE | updates)


@pytest.mark.parametrize("overrides", [
    {"public_base_url": None},
    {"public_base_url": "https://bob.dev.example.com"},
    {"public_base_url": "https://alice.dev.example.com/personal"},
    {"auth_local_http": True},
    {"managed_environment_config_file": "/tmp/legacy-child.json"},
    {"service_mode": "management"},
    {"auth_session_audience_json": "{}"},
    {"auth_session_audience_json": "null"},
    {"auth_session_audience_json": "not-json"},
])
def test_audience_requires_explicit_consistent_hosted_configuration(overrides):
    # Establish the positive control: rejection cannot be due to an unknown field.
    assert _settings().session_audience is not None
    with pytest.raises(ValueError):
        _settings(**overrides)


def test_audience_hash_separates_purpose_and_legacy_token_preimages():
    from loom_service.session_auth import hash_browser_secret

    audience = _settings().session_audience
    raw = "loom_session_test-proof"
    session_hash = hash_browser_secret(raw, audience=audience, purpose="session")
    challenge_hash = hash_browser_secret(raw, audience=audience, purpose="login_challenge")
    assert session_hash != challenge_hash
    assert hash_browser_secret(raw, audience=None, purpose="session") == hashlib.sha256(raw.encode()).digest()
    # A scoped hash must not equal a legacy hash of its serialized audience and
    # proof. Otherwise a holder could wrap the proof to authenticate in legacy mode.
    serialized = json.dumps([
        "loom.application-session-audience.v1", APPLICATION_ID,
        "https://alice.dev.example.com", 1, "session", raw,
    ], separators=(",", ":"))
    assert session_hash != hashlib.sha256(serialized.encode()).digest()
