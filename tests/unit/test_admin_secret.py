from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.admin_secret import (
    AdminSecretConfigError,
    AdminSecretVerifier,
    load_admin_secret_file,
)
from loom.auth import verify_bearer_token

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


def _write_secret(path: Path, token: str = RAW_ADMIN_TOKEN) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{token}\"\n"
        "created_at = \"2026-06-16T00:00:00Z\"\n"
        "version = 1\n",
        encoding="utf-8",
    )


def test_load_admin_secret_file_accepts_safe_toml(tmp_path: Path) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_secret(secret_file)
    secret_file.chmod(0o600)

    verifier = load_admin_secret_file(
        secret_file, require_safe_permissions=True,
    )

    assert verifier.verify(RAW_ADMIN_TOKEN)
    assert not verifier.verify(RAW_ADMIN_TOKEN + "wrong")


def test_load_admin_secret_file_rejects_world_readable_file(
    tmp_path: Path,
) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_secret(secret_file)
    secret_file.chmod(0o644)

    with pytest.raises(AdminSecretConfigError, match="permissions"):
        load_admin_secret_file(secret_file, require_safe_permissions=True)


def test_load_admin_secret_file_rejects_low_entropy_token(
    tmp_path: Path,
) -> None:
    secret_file = tmp_path / "secrets.toml"
    _write_secret(secret_file, token="loom_admin_short")
    secret_file.chmod(0o600)

    with pytest.raises(AdminSecretConfigError, match="entropy"):
        load_admin_secret_file(secret_file, require_safe_permissions=True)


class _ExplodingSession:
    async def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("admin secret verification must not query DB")


async def test_verify_bearer_token_accepts_singleton_admin_without_db_lookup(
) -> None:
    verifier = AdminSecretVerifier.from_token(RAW_ADMIN_TOKEN)

    ctx = await verify_bearer_token(
        _ExplodingSession(),
        f"Bearer {RAW_ADMIN_TOKEN}",
        admin_verifier=verifier,
    )

    assert ctx is not None
    assert ctx.type == "admin"
    assert ctx.team_id is None
    assert "admin:tokens" in ctx.scopes
