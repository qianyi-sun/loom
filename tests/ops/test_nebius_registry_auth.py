from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops.nebius_registry_auth import refresh_registry_auth


@pytest.fixture
def credential_file(tmp_path: Path) -> Path:
    path = tmp_path / "service-account.json"
    path.write_text(
        json.dumps(
            {
                "subject-credentials": {
                    "alg": "RS256",
                    "private-key": "private-key-fixture",
                    "kid": "publickey-fixture",
                    "iss": "serviceaccount-fixture",
                    "sub": "serviceaccount-fixture",
                }
            }
        )
    )
    path.chmod(0o600)
    return path


class FakeSDK:
    token_value = "access-token-one"
    expires = datetime.now(UTC) + timedelta(hours=1)
    closed = False

    def __init__(self, **kwargs: str):
        assert set(kwargs) == {"credentials_file_name", "user_agent_prefix"}

    async def __aenter__(self) -> FakeSDK:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def get_token(self, *, timeout: int) -> SimpleNamespace:
        assert timeout == 30
        return SimpleNamespace(token=self.token_value, expiration=self.expires)


def test_mint_and_refresh_auth_without_exposing_token(
    credential_file: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    auth = tmp_path / "docker/config.json"
    instance = FakeSDK(credentials_file_name=str(credential_file), user_agent_prefix="test")

    def factory(**kwargs: str) -> FakeSDK:
        return instance

    summary = refresh_registry_auth(
        credential_file, "cr.eu-north1.nebius.cloud/test", auth, sdk_factory=factory
    )
    value = json.loads(auth.read_text())["auths"]["cr.eu-north1.nebius.cloud"]["auth"]
    assert base64.b64decode(value).decode() == "iam:access-token-one"
    assert auth.stat().st_mode & 0o077 == 0
    assert instance.closed
    assert "access-token-one" not in json.dumps(summary)
    instance.token_value = "access-token-two"
    refresh_registry_auth(
        credential_file, "cr.eu-north1.nebius.cloud/test", auth, sdk_factory=factory
    )
    value = json.loads(auth.read_text())["auths"]["cr.eu-north1.nebius.cloud"]["auth"]
    assert base64.b64decode(value).decode().endswith("access-token-two")
    output = capsys.readouterr()
    assert "access-token" not in output.out + output.err


def test_reject_copied_or_expired_token_and_keep_existing_auth(
    credential_file: Path,
    tmp_path: Path,
) -> None:
    auth = tmp_path / "auth.json"
    auth.write_text("existing-auth")
    instance = FakeSDK(credentials_file_name=str(credential_file), user_agent_prefix="test")
    instance.expires = datetime.now(UTC) - timedelta(seconds=1)
    with pytest.raises(ValueError, match="expired"):
        refresh_registry_auth(
            credential_file,
            "cr.eu-north1.nebius.cloud/test",
            auth,
            sdk_factory=lambda **kwargs: instance,
        )
    assert auth.read_text() == "existing-auth"
    credential_file.write_text('{"access-token":"copied-token"}')
    with pytest.raises(ValueError, match="authorized service-account key"):
        refresh_registry_auth(
            credential_file,
            "cr.eu-north1.nebius.cloud/test",
            auth,
            sdk_factory=lambda **kwargs: pytest.fail("must not mint"),
        )


@pytest.mark.parametrize("failure", ["public_file", "symlink", "foreign_registry"])
def test_reject_credential_boundaries_before_mint(
    credential_file: Path,
    tmp_path: Path,
    failure: str,
) -> None:
    registry = "cr.eu-north1.nebius.cloud/test"
    path = credential_file
    if failure == "public_file":
        path.chmod(0o644)
    elif failure == "symlink":
        path = tmp_path / "link.json"
        path.symlink_to(credential_file)
    else:
        registry = "ghcr.io/qianyi-sun"
    with pytest.raises(ValueError):
        refresh_registry_auth(
            path,
            registry,
            tmp_path / "auth.json",
            sdk_factory=lambda **kwargs: pytest.fail("must not mint"),
        )
