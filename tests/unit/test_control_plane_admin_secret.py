from __future__ import annotations

from pathlib import Path

import pytest

from loom.admin_secret import AdminSecretConfigError, AdminSecretVerifier
from loom.auth import verify_bearer_token
from loom_control_plane.app import _load_admin_secret_verifier
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "D" * 43


def _settings(*, admin_secret_file: Path | None = None) -> ControlPlaneSettings:
    return ControlPlaneSettings(
        _env_file=None,
        db_url="postgresql+psycopg://loom:pw@localhost:5432/loom",
        minio_endpoint="http://localhost:9000",
        minio_access_key="loomdev",
        minio_secret_key="loomdev123",
        llm_gateway_url="http://gateway:9100",
        admin_secret_file=admin_secret_file,
    )


def _write_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-16T00:00:00Z\"\n"
        "version = 1\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_load_admin_secret_verifier_uses_configured_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_ENV", raising=False)
    secret_file = tmp_path / "secrets.toml"
    _write_secret(secret_file)

    verifier = _load_admin_secret_verifier(_settings(admin_secret_file=secret_file))

    assert verifier is not None
    assert verifier.verify(RAW_ADMIN_TOKEN)


def test_load_admin_secret_verifier_requires_file_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_ENV", "production")

    with pytest.raises(AdminSecretConfigError, match="admin secret"):
        _load_admin_secret_verifier(_settings(admin_secret_file=None))


async def test_file_backed_admin_secret_grants_gb10_worker_scope() -> None:
    verifier = AdminSecretVerifier.from_token(RAW_ADMIN_TOKEN)

    ctx = await verify_bearer_token(
        object(),  # type: ignore[arg-type]
        f"Bearer {RAW_ADMIN_TOKEN}",
        admin_verifier=verifier,
    )

    assert ctx is not None
    assert "admin:gb10_workers" in ctx.scopes
