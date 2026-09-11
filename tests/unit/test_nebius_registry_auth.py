from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom.nebius_registry_auth import mint_registry_auth

REGISTRY = "cr.eu-north1.nebius.cloud/test"


@pytest.fixture
def projected_credentials(tmp_path: Path) -> Path:
    generation = tmp_path / "..2026_09_11"
    generation.mkdir()
    target = generation / "credentials.json"
    target.write_text(
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
    target.chmod(0o440)
    (tmp_path / "..data").symlink_to(generation.name, target_is_directory=True)
    path = tmp_path / "credentials.json"
    path.symlink_to("..data/credentials.json")
    return path


class FakeSDK:
    def __init__(self, expiration: datetime | None = None):
        self.expiration = expiration
        self.closed = False

    async def __aenter__(self) -> FakeSDK:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.closed = True

    async def get_token(self, *, timeout: int) -> SimpleNamespace:
        assert timeout == 30
        return SimpleNamespace(token="ephemeral-token-fixture", expiration=self.expiration)


def test_projected_group_readable_secret_mints_private_auth_without_secret_output(
    projected_credentials: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    expires = datetime.now(UTC) + timedelta(hours=1)
    sdk = FakeSDK(expires)

    def factory(**kwargs: str) -> FakeSDK:
        assert kwargs == {
            "credentials_file_name": str(projected_credentials),
            "user_agent_prefix": "loom-nebius-publication/1.0",
        }
        return sdk

    auth = tmp_path / "private/auth.json"
    summary = mint_registry_auth(projected_credentials, REGISTRY, auth, sdk_factory=factory)
    assert summary == {
        "registry_host": "cr.eu-north1.nebius.cloud",
        "expires_at": expires.isoformat(),
    }
    value = json.loads(auth.read_text())["auths"][summary["registry_host"]]["auth"]
    assert base64.b64decode(value).decode() == "iam:ephemeral-token-fixture"
    assert auth.stat().st_mode & 0o777 == 0o600
    assert sdk.closed
    assert not list(auth.parent.glob(".nebius-auth-*"))
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""


@pytest.mark.parametrize("expiry", ["expired", "missing", "naive", "near_expiry"])
def test_invalid_expiry_preserves_existing_auth(
    projected_credentials: Path, tmp_path: Path, expiry: str
) -> None:
    expires = {
        "expired": datetime.now(UTC) - timedelta(seconds=1),
        "missing": None,
        "naive": datetime.now() + timedelta(hours=1),
        "near_expiry": datetime.now(UTC) + timedelta(seconds=30),
    }[expiry]
    sdk = FakeSDK(expires)
    auth = tmp_path / "auth.json"
    auth.write_text("previous-auth")
    with pytest.raises(ValueError, match="empty, expired, or has no bounded lifetime"):
        mint_registry_auth(
            projected_credentials, REGISTRY, auth, sdk_factory=lambda **kwargs: sdk
        )
    assert auth.read_text() == "previous-auth"
    assert sdk.closed
    assert not list(tmp_path.glob(".nebius-auth-*"))


@pytest.mark.parametrize(
    "failure", ["world_readable", "directory", "oversized", "output_symlink", "foreign_registry"]
)
def test_reject_unsafe_files_before_sdk(
    projected_credentials: Path, tmp_path: Path, failure: str
) -> None:
    credentials = projected_credentials
    auth = tmp_path / "auth.json"
    registry = REGISTRY
    if failure == "world_readable":
        credentials.chmod(0o444)
    elif failure == "directory":
        credentials = tmp_path
    elif failure == "oversized":
        credentials.chmod(0o600)
        credentials.write_bytes(b"x" * (1024 * 1024 + 1))
    elif failure == "output_symlink":
        auth.symlink_to(credentials)
    else:
        registry = "ghcr.io/example"
    with pytest.raises(ValueError):
        mint_registry_auth(
            credentials,
            registry,
            auth,
            sdk_factory=lambda **kwargs: pytest.fail("must not mint"),
        )


@pytest.mark.parametrize("document", [[], {"access-token": "copied-token"}, {"subject-credentials": []}])
def test_reject_non_authorized_key_without_echoing_input(
    projected_credentials: Path, tmp_path: Path, document: object
) -> None:
    projected_credentials.chmod(0o600)
    projected_credentials.write_text(json.dumps(document))
    with pytest.raises(
        ValueError, match=r"^registry authentication requires an authorized service-account key$"
    ):
        mint_registry_auth(
            projected_credentials,
            REGISTRY,
            tmp_path / "auth.json",
            sdk_factory=lambda **kwargs: pytest.fail("must not mint"),
        )
