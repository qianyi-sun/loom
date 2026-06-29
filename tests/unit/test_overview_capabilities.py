from __future__ import annotations

from uuid import uuid4

from loom.auth import AuthContext
from loom_service.routes.overview import _capabilities


def _team_ctx(*, user_owned: bool) -> AuthContext:
    return AuthContext(
        token_hash=b"\x00" * 32,
        type="team",
        scopes=["read:own", "submit"],
        team_id=uuid4(),
        expires_at=None,
        user_id=uuid4() if user_owned else None,
    )


def test_user_owned_api_token_can_submit_in_overview() -> None:
    assert _capabilities(_team_ctx(user_owned=True))["can_submit"] is True


def test_legacy_team_token_cannot_submit_in_overview() -> None:
    assert _capabilities(_team_ctx(user_owned=False))["can_submit"] is False
