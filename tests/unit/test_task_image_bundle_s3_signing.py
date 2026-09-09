"""Exact S3 signing timestamp, public target and credential lifetime contracts."""

import importlib
import logging
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

NOW = datetime(2026, 9, 9, 12, tzinfo=UTC)


def _module():
    return importlib.import_module("loom_task_image_authority.bundle_s3_signing")


def _sign(**changes):
    module = _module()
    values = dict(
        public_origin="https://objects.example:9443", bucket="loom-bundles",
        key="bench/revision/a space+%/café.toml", region="us-east-1",
        credentials=module.S3SigningCredentials(
            access_key="access-fixture", secret_key="private-secret-fixture",
        ),
        expires_at=NOW + timedelta(seconds=40), clock=lambda: NOW,
    )
    values.update(changes)
    return module.presign_bundle_get(**values)


def test_signs_exact_public_host_bucket_and_encoded_key_to_deadline(caplog):
    caplog.set_level(logging.DEBUG)
    url = _sign(clock=lambda: NOW + timedelta(seconds=1, microseconds=900000))
    parsed = urlsplit(url)
    assert parsed.netloc == "objects.example:9443"
    assert parsed.path == "/loom-bundles/bench/revision/a%20space%2B%25/caf%C3%A9.toml"
    query = parse_qs(parsed.query)
    assert query["X-Amz-Date"] == ["20260909T120001Z"]
    assert query["X-Amz-Expires"] == ["39"]
    assert query["X-Amz-SignedHeaders"] == ["host"]
    assert len(query["X-Amz-Signature"][0]) == 64
    assert query["X-Amz-Credential"] == ["access-fixture/20260909/us-east-1/s3/aws4_request"]
    assert "private-secret-fixture" not in caplog.text
    assert query["X-Amz-Signature"][0] not in caplog.text


def test_temporary_credentials_require_and_bind_sufficient_lifetime(caplog):
    module = _module()
    caplog.set_level(logging.DEBUG)
    credentials = module.S3SigningCredentials(
        access_key="temporary-access", secret_key="temporary-secret",
        session_token="private-session-fixture", expires_at=NOW + timedelta(seconds=40),
    )
    url = _sign(credentials=credentials)
    assert parse_qs(urlsplit(url).query)["X-Amz-Security-Token"] == ["private-session-fixture"]
    assert "private-session-fixture" not in caplog.text + repr(credentials)
    with pytest.raises(RuntimeError, match="credential"):
        _sign(credentials=credentials, expires_at=NOW + timedelta(seconds=41))
    with pytest.raises(RuntimeError, match="credential"):
        module.S3SigningCredentials(
            access_key="temporary-access", secret_key="temporary-secret", session_token="token",
        )


@pytest.mark.parametrize("changes", [
    {"public_origin": "http://objects.example"},
    {"public_origin": "https://user:private@objects.example"},
    {"public_origin": "https://objects.example/prefix"},
    {"bucket": "other/bucket"},
    {"key": "../private"}, {"key": "/private"}, {"key": "a//private"},
    {"key": "a\\private"}, {"key": "a\nprivate"}, {"key": "é" * 513},
    {"region": "us-east-1/foreign"},
    {"expires_at": NOW}, {"expires_at": NOW + timedelta(seconds=901)},
    {"expires_at": NOW + timedelta(seconds=40, microseconds=1)},
    {"expires_at": NOW.replace(tzinfo=None) + timedelta(seconds=40)},
])
def test_refuses_ambiguous_targets_or_invalid_deadlines_without_secret_echo(changes):
    with pytest.raises(RuntimeError) as error:
        _sign(**changes)
    assert "private" not in str(error.value)


def test_rejects_oversized_signed_url_without_exposing_it():
    module = _module()
    credentials = module.S3SigningCredentials(
        access_key="access-fixture", secret_key="secret-fixture",
        session_token="x" * 4096, expires_at=NOW + timedelta(seconds=60),
    )
    with pytest.raises(RuntimeError, match="limit"):
        _sign(credentials=credentials)


@pytest.mark.parametrize("changes", [
    {"access_key": None}, {"secret_key": None}, {"access_key": ""}, {"secret_key": ""},
    {"access_key": "a\nprivate"}, {"secret_key": "é"}, {"session_token": ""},
])
def test_credential_snapshot_requires_real_bounded_secret_fields(changes):
    values = dict(access_key="access", secret_key="secret")
    values.update(changes)
    with pytest.raises(RuntimeError, match="credential"):
        _module().S3SigningCredentials(**values)


def _list(**changes):
    module = _module()
    values = dict(
        public_origin="https://objects.example:9443", bucket="loom-bundles",
        prefix="bench/revision space+%/", maximum_keys=2,
        continuation_token="opaque+/=%2F", region="us-east-1",
        credentials=module.S3SigningCredentials(access_key="access-fixture", secret_key="private-secret-fixture"),
        expires_at=NOW + timedelta(seconds=40), clock=lambda: NOW,
    )
    values.update(changes)
    return module.presign_bundle_list(**values)


def test_signs_exact_list_request_without_logging_bearer_url(caplog):
    caplog.set_level(logging.DEBUG)
    calls = []

    def clock():
        calls.append(True)
        return NOW + timedelta(seconds=3, microseconds=999999)

    parsed = urlsplit(_list(clock=clock))
    assert parsed.netloc == "objects.example:9443"
    assert parsed.path == "/loom-bundles"
    query = parse_qs(parsed.query)
    assert query["list-type"] == ["2"]
    assert query["prefix"] == ["bench/revision space+%/"]
    assert query["max-keys"] == ["2"]
    assert query["encoding-type"] == ["url"]
    assert query["continuation-token"] == ["opaque+/=%2F"]
    assert query["X-Amz-Date"] == ["20260909T120003Z"]
    assert query["X-Amz-Expires"] == ["37"]
    assert query["X-Amz-SignedHeaders"] == ["host"]
    assert len(calls) == 1
    for private in ("private-secret-fixture", "opaque+/=%2F", query["X-Amz-Signature"][0]):
        assert private not in caplog.text


def test_first_list_page_has_no_continuation_parameter():
    query = parse_qs(urlsplit(_list(continuation_token=None)).query)
    assert "continuation-token" not in query


@pytest.mark.parametrize("changes", [
    {"prefix": ""}, {"prefix": "../"}, {"prefix": "revision//"},
    {"prefix": "revision"}, {"prefix": "/revision/"}, {"prefix": "revision\n/"},
    {"prefix": "é" * 512 + "/"}, {"prefix": None},
    {"maximum_keys": True}, {"maximum_keys": 0}, {"maximum_keys": 1001},
    {"continuation_token": ""}, {"continuation_token": "secret\n"},
    {"continuation_token": "é"}, {"continuation_token": "x" * 4097},
    {"public_origin": "http://objects.example"}, {"public_origin": "https://objects.example/extra"},
    {"bucket": "other/bucket"}, {"region": "foreign/region"},
    {"credentials": None}, {"expires_at": NOW}, {"expires_at": NOW + timedelta(seconds=901)},
])
def test_refuses_invalid_listing_authority_without_echo(changes):
    with pytest.raises(RuntimeError) as error:
        _list(**changes)
    assert "secret" not in str(error.value)


def test_list_rejects_oversized_final_signed_url():
    with pytest.raises(RuntimeError, match="limit"):
        _list(continuation_token="x" * 4096)


def test_list_requires_credentials_to_cover_deadline():
    module = _module()
    credentials = module.S3SigningCredentials(
        access_key="access", secret_key="secret", expires_at=NOW + timedelta(seconds=39),
    )
    with pytest.raises(RuntimeError, match="credential"):
        _list(credentials=credentials)
