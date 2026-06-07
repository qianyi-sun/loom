"""Bearer token authentication for the Control Plane + Gateway.

Two principal branches:
- DB-backed tokens (team/worker/admin) — hashed lookup against the shared
  `tokens` table. The original v0.7 path.
- Step-scoped JWTs (`loom_step_<jwt>`) — stateless HS256 tokens minted by
  the Control Plane at the start of each trial step. The Gateway extracts
  `(team_id, trial_id, step_id)` from the JWT claims for per-call cost
  attribution.

Per Plan 9 amendment A9.0, the `AuthContext` shape PRESERVES the v0.7
field names (`token_hash`, `type`, `scopes`, `team_id`, `expires_at`) and
ADDS optional `trial_id` + `step_id`. Service-layer plans 17-20 reference
the original names; the JWT branch fills the new fields and sets a
synthetic `token_hash=b""` + `type="step_session"`.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Token

_STEP_JWT_PREFIX = "loom_step_"


@dataclass(frozen=True)
class AuthContext:
    token_hash: bytes
    type: str
    scopes: list[str]
    team_id: UUID | None
    expires_at: datetime | None
    # Plan 9 additive optional fields. Populated by the JWT branch;
    # remain None for DB-backed tokens.
    trial_id: UUID | None = None
    step_id: str | None = None


def mint_step_jwt(
    *,
    team_id: UUID,
    trial_id: UUID,
    step_id: str,
    ttl_sec: int,
    signing_key: str,
) -> str:
    """Mint a step-scoped JWT carrying `(team_id, trial_id, step_id)`.
    Used by the Control Plane's POST /admin/step-tokens (Task 4); the
    returned string is the bearer token the agent presents at the
    Gateway."""
    now = datetime.now(UTC)
    payload = {
        "iss": "loom-control-plane",
        "sub": "step-session",
        "team_id": str(team_id),
        "trial_id": str(trial_id),
        "step_id": step_id,
        "exp": int((now + timedelta(seconds=ttl_sec)).timestamp()),
        "iat": int(now.timestamp()),
        "scopes": ["llm:call"],
    }
    body = jwt.encode(payload, signing_key, algorithm="HS256")
    return _STEP_JWT_PREFIX + body


def verify_step_jwt(token: str, *, signing_key: str) -> AuthContext:
    """Verify a step-scoped JWT. Raises `jwt.PyJWTError` subclasses on
    failure (ExpiredSignatureError, InvalidSignatureError,
    InvalidTokenError, etc.) — caller decides how to surface them."""
    if not token.startswith(_STEP_JWT_PREFIX):
        raise jwt.InvalidTokenError("not a step JWT")
    body = token[len(_STEP_JWT_PREFIX):]
    payload = jwt.decode(body, signing_key, algorithms=["HS256"])
    return AuthContext(
        token_hash=b"",                       # synthetic — no DB row
        type="step_session",
        scopes=list(payload.get("scopes", [])),
        team_id=UUID(payload["team_id"]),
        expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
        trial_id=UUID(payload["trial_id"]),
        step_id=payload["step_id"],
    )


async def verify_bearer_token(
    session: AsyncSession,
    header_value: str | None,
    *,
    signing_key: str | None = None,
) -> AuthContext | None:
    """Validate a Bearer token. Returns an AuthContext or None.

    `signing_key` enables the step-JWT branch; if None, JWTs always
    return None (effectively disabling that path). DB-backed tokens
    are unaffected by `signing_key`.
    """
    if not header_value or not header_value.lower().startswith("bearer "):
        return None
    raw = header_value.split(" ", 1)[1].strip()
    if not raw:
        return None

    # JWT branch
    if raw.startswith(_STEP_JWT_PREFIX):
        if signing_key is None:
            return None
        try:
            return verify_step_jwt(raw, signing_key=signing_key)
        except jwt.PyJWTError:
            return None

    # DB-backed branch (unchanged from v0.7)
    token_hash = hashlib.sha256(raw.encode()).digest()
    row = (await session.execute(
        select(Token).where(Token.token_hash == token_hash),
    )).scalar_one_or_none()
    if row is None:
        return None
    if row.revoked_at is not None:
        return None
    if row.expires_at is not None and row.expires_at < datetime.now(UTC):
        return None

    return AuthContext(
        token_hash=row.token_hash,
        type=row.type,
        scopes=list(row.scopes),
        team_id=row.team_id,
        expires_at=row.expires_at,
    )
