"""Integration tests for the deployment-managed smoke-user provisioner."""
from __future__ import annotations

import hashlib

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Team, TeamMembership, TeamQuota, Token, User
from loom_cli.smoke_credential import (
    ensure_batch_runner_token,
    ensure_smoke_user_credential,
)
from loom_service.password_auth import normalize_username


def _token_row(postgres_url: str, raw: str) -> Token | None:
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            return s.execute(
                select(Token).where(
                    Token.token_hash == hashlib.sha256(raw.encode()).digest(),
                ),
            ).scalar_one_or_none()
    finally:
        engine.dispose()


def test_provisions_user_team_and_user_owned_submit_token(
    postgres_url: str,
) -> None:
    cred = ensure_smoke_user_credential(
        postgres_url, username="loom-smoke-a", team_name="loom-smoke-a",
    )

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            user = s.get(User, cred.user_id)
            assert user is not None
            assert user.username_normalized == normalize_username("loom-smoke-a")
            assert user.status == "active"
            assert user.is_platform_admin is False
            # Non-human: no password credential.
            assert user.password_hash is None

            team = s.get(Team, cred.team_id)
            assert team is not None and team.name == "loom-smoke-a"
            assert s.get(TeamQuota, cred.team_id) is not None

            membership = s.execute(
                select(TeamMembership).where(
                    TeamMembership.team_id == cred.team_id,
                    TeamMembership.user_id == cred.user_id,
                ),
            ).scalar_one()
            assert membership.role == "owner"
    finally:
        engine.dispose()

    # The token is user-owned (created_by_user_id set) — this is exactly
    # what require_submitting_user needs to permit trial submission.
    row = _token_row(postgres_url, cred.raw_token)
    assert row is not None
    assert row.type == "team"
    assert row.created_by_user_id == cred.user_id
    assert row.team_id == cred.team_id
    assert "submit" in row.scopes
    assert row.revoked_at is None
    assert row.expires_at is not None
    assert cred.raw_token.startswith("loom_api_")


def test_idempotent_identity_and_rotates_token(postgres_url: str) -> None:
    first = ensure_smoke_user_credential(
        postgres_url, username="loom-smoke-b", team_name="loom-smoke-b",
    )
    second = ensure_smoke_user_credential(
        postgres_url, username="loom-smoke-b", team_name="loom-smoke-b",
    )

    # Same identity reused; a fresh token minted.
    assert second.user_id == first.user_id
    assert second.team_id == first.team_id
    assert second.raw_token != first.raw_token
    assert second.rotated_prior == 1

    # Rotation: the first token is revoked, the second is live.
    first_row = _token_row(postgres_url, first.raw_token)
    second_row = _token_row(postgres_url, second.raw_token)
    assert first_row is not None and first_row.revoked_at is not None
    assert second_row is not None and second_row.revoked_at is None


def test_keep_prior_leaves_existing_tokens_live(postgres_url: str) -> None:
    first = ensure_smoke_user_credential(
        postgres_url, username="loom-smoke-c", team_name="loom-smoke-c",
    )
    second = ensure_smoke_user_credential(
        postgres_url,
        username="loom-smoke-c",
        team_name="loom-smoke-c",
        revoke_prior=False,
    )
    assert second.rotated_prior == 0
    first_row = _token_row(postgres_url, first.raw_token)
    assert first_row is not None and first_row.revoked_at is None


def test_requires_submit_scope(postgres_url: str) -> None:
    with pytest.raises(ValueError, match="submit"):
        ensure_smoke_user_credential(
            postgres_url,
            username="loom-smoke-d",
            team_name="loom-smoke-d",
            scopes=("read:own",),
        )


def test_rejects_nonpositive_ttl(postgres_url: str) -> None:
    with pytest.raises(ValueError, match="ttl_days"):
        ensure_smoke_user_credential(
            postgres_url,
            username="loom-smoke-e",
            team_name="loom-smoke-e",
            ttl_days=0,
        )


def test_batch_runner_token_is_submit_batch_worker_token(postgres_url: str) -> None:
    tok = ensure_batch_runner_token(postgres_url)
    assert tok.raw_token.startswith("loom_br_")
    row = _token_row(postgres_url, tok.raw_token)
    assert row is not None
    # Mirrors POST /admin/batch-runner-tokens: non-user worker token,
    # submit:batch scope — exactly what loom-service's batch fan-out needs.
    assert row.type == "worker"
    assert list(row.scopes) == ["submit:batch"]
    assert row.team_id is None
    assert row.created_by_user_id is None
    assert row.revoked_at is None
    assert row.expires_at is not None


def test_batch_runner_token_rotates_prior(postgres_url: str) -> None:
    first = ensure_batch_runner_token(postgres_url)
    second = ensure_batch_runner_token(postgres_url)
    assert second.raw_token != first.raw_token
    assert second.rotated_prior >= 1
    first_row = _token_row(postgres_url, first.raw_token)
    second_row = _token_row(postgres_url, second.raw_token)
    assert first_row is not None and first_row.revoked_at is not None
    assert second_row is not None and second_row.revoked_at is None


def _worker_token_rows(postgres_url: str, token_hash: bytes) -> list[Token]:
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            return list(
                s.execute(
                    select(Token).where(Token.token_hash == token_hash)
                ).scalars().all()
            )
    finally:
        engine.dispose()


def test_ensure_dev_worker_token_seeds_fixed_smoke_token(
    postgres_url: str,
) -> None:
    """The seeded row matches the fixed --smoke-defaults worker-token
    (sha256 of the plaintext) with the worker:report scope, so in-cluster
    workers authenticate without a mint/patch/restart."""
    from loom_cli.smoke_credential import (
        _DEV_WORKER_TOKEN,
        ensure_dev_worker_token,
    )

    token_hash = hashlib.sha256(_DEV_WORKER_TOKEN.encode()).digest()

    res = ensure_dev_worker_token(postgres_url)
    assert res.created is True
    assert res.token_hash_prefix == token_hash.hex()[:8]

    rows = _worker_token_rows(postgres_url, token_hash)
    assert len(rows) == 1
    row = rows[0]
    assert row.type == "worker"
    assert "worker:report" in row.scopes
    assert row.team_id is None
    assert row.revoked_at is None


def test_ensure_dev_worker_token_is_idempotent(postgres_url: str) -> None:
    """Re-running is a get-or-create no-op — never a second row."""
    from loom_cli.smoke_credential import (
        _DEV_WORKER_TOKEN,
        ensure_dev_worker_token,
    )

    ensure_dev_worker_token(postgres_url)
    second = ensure_dev_worker_token(postgres_url)
    assert second.created is False

    token_hash = hashlib.sha256(_DEV_WORKER_TOKEN.encode()).digest()
    assert len(_worker_token_rows(postgres_url, token_hash)) == 1
