"""Explicit native MinIO identity and lifespan ownership, never ambient auth."""

import json

import pytest
from pydantic import ValidationError

from loom_task_image_authority.config import TaskImageAuthoritySettings
from tests.unit.test_task_image_authority_config import _owner_only, _settings_values


def _native(tmp_path, **changes):
    options = dict(
        **_settings_values(tmp_path), bundle_backend="minio",
        bundle_public_https_origin="https://objects.example:9443", bundle_expected_bucket="loom-bundles",
        bundle_region="us-east-1", bundle_credentials_file=tmp_path / "identity.json",
        bundle_reader_ca_file=tmp_path / "ca.pem",
    )
    options.update(changes)
    return TaskImageAuthoritySettings(**options)


def test_native_configuration_is_explicit_and_disabled_by_default(tmp_path, monkeypatch):
    disabled = TaskImageAuthoritySettings(**_settings_values(tmp_path))
    assert disabled.bundle_backend == "disabled"
    assert disabled.bundle_credentials_file is None
    configured = _native(tmp_path)
    assert configured.bundle_backend == "minio"
    for name, value in _settings_values(tmp_path).items():
        monkeypatch.setenv("LOOM_TASK_IMAGE_AUTHORITY_" + name.upper(), str(value))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "unrelated-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "unrelated-secret")
    monkeypatch.setenv("AWS_REGION", "unrelated-region")
    assert TaskImageAuthoritySettings().bundle_backend == "disabled"


@pytest.mark.parametrize("changes", [
    {"bundle_public_https_origin": None}, {"bundle_expected_bucket": None},
    {"bundle_region": None}, {"bundle_credentials_file": None}, {"bundle_reader_ca_file": None},
    {"bundle_backend": "disabled"}, {"bundle_backend": "ambient"},
    {"bundle_public_https_origin": "https://objects.example/"},
    {"bundle_region": "invalid region"},
])
def test_native_configuration_rejects_partial_or_unsafe_settings(tmp_path, changes):
    with pytest.raises(ValidationError):
        _native(tmp_path, **changes)


@pytest.mark.parametrize("failure", ["duplicate", "unknown", "token", "wrong_schema", "missing", "bad_key", "mode", "symlink", "oversize"])
def test_native_credentials_reject_unsafe_files_without_disclosing_content(tmp_path, failure):
    from loom_task_image_authority.bundle_runtime import load_bundle_credentials

    document = dict(schema_version=1, access_key="fixture-access", secret_key="fixture-private-secret")
    if failure == "unknown":
        document["unknown"] = True
    elif failure == "token":
        document["session_token"] = "fixture-private-token"
    elif failure == "wrong_schema":
        document["schema_version"] = True
    elif failure == "missing":
        del document["secret_key"]
    elif failure == "bad_key":
        document["secret_key"] += "\n"
    payload = json.dumps(document).encode()
    if failure == "duplicate":
        payload = payload[:-1] + b', "access_key": "different"}'
    elif failure == "oversize":
        payload += b" " * (16 * 1024)
    path = _owner_only(tmp_path / "credentials.json", payload)
    if failure == "mode":
        path.chmod(0o644)
    elif failure == "symlink":
        link = tmp_path / "link"
        link.symlink_to(path)
        path = link
    with pytest.raises(ValueError) as caught:
        load_bundle_credentials(path)
    assert "fixture-private" not in str(caught.value)
    assert caught.value.__cause__ is None


async def test_runtime_owns_configured_backend_and_closes_on_exit(tmp_path, monkeypatch):
    from loom_task_image_authority import bundle_runtime as module

    settings = _native(tmp_path)
    _owner_only(settings.bundle_credentials_file, json.dumps(dict(schema_version=1, access_key="fixture-access", secret_key="fixture-secret")).encode())
    constructed = []

    class Backend:
        def __init__(self, **options):
            self.options, self.closed = options, False
            constructed.append(self)

        async def aclose(self):
            self.closed = True

    monkeypatch.setattr(module, "MinioTaskImageBundleBackend", Backend)
    for fail in (False, True):
        try:
            async with module.configured_bundle_provider(settings) as provider:
                assert provider is not None
                assert constructed[-1].options["origin"] == settings.bundle_public_https_origin
                assert constructed[-1].options["credentials"].access_key == "fixture-access"
                assert "fixture-secret" not in repr(constructed[-1].options["credentials"])
                assert not constructed[-1].closed
                if fail:
                    raise RuntimeError("consumer failed")
        except RuntimeError as error:
            assert str(error) == "consumer failed"
        assert constructed[-1].closed
    assert len(constructed) == 2 and constructed[0] is not constructed[1]
    async with module.configured_bundle_provider(TaskImageAuthoritySettings(**_settings_values(tmp_path))) as provider:
        assert provider is None
    assert len(constructed) == 2


async def test_runtime_closes_backend_when_provider_construction_fails(tmp_path, monkeypatch):
    from loom_task_image_authority import bundle_runtime as module

    settings = _native(tmp_path)
    _owner_only(settings.bundle_credentials_file, b'{"schema_version":1,"access_key":"fixture-access","secret_key":"fixture-secret"}')
    closed = []

    class Backend:
        def __init__(self, **options):
            pass

        async def aclose(self):
            closed.append(True)

    def fail(**options):
        raise ValueError("provider failed")

    monkeypatch.setattr(module, "MinioTaskImageBundleBackend", Backend)
    monkeypatch.setattr(module, "AsyncTaskImageBundleCapabilityProvider", fail)
    with pytest.raises(ValueError, match="provider failed"):
        async with module.configured_bundle_provider(settings):
            pytest.fail("construction unexpectedly succeeded")
    assert closed == [True]
