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
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import Token

_STEP_JWT_PREFIX = "loom_step_"
_TOKEN_TOUCH_DEBOUNCE = timedelta(seconds=60)

_ROLE_SCOPES: dict[str, list[str]] = {
    "viewer": ["read:own"],
    "member": ["read:own", "submit"],
    "owner": [
        "read:own",
        "submit",
        "tokens:manage",
        "providers:manage",
        "team:manage",
    ],
    "platform_admin": ["admin:platform"],
}


def role_scopes(role: str) -> list[str]:
    """Return the service scopes granted by a browser user role."""
    try:
        return list(_ROLE_SCOPES[role])
    except KeyError as exc:
        raise ValueError(f"unknown user role: {role}") from exc


def is_platform_admin_role(role: str | None) -> bool:
    return role == "platform_admin"


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
    # cluster-deploy.md §Authentication / issue #72: per-trial step
    # JWTs additionally scope to a `provider_connection_id` so a
    # compromised gateway can only target the connection bound at
    # mint time. Remains None for:
    # - DB-backed tokens (team/worker/admin)
    # - Step JWTs minted before the field was added (graceful fallback)
    # - Trials whose Trial.provider_connection_id is NULL (e.g. local
    #   adapters that don't route through the gateway facade)
    provider_connection_id: UUID | None = None
    # Browser-session principal fields (#326). They stay None for
    # bearer/worker/step auth so the existing token path remains stable.
    user_id: UUID | None = None
    role: str | None = None
    session_hash: bytes | None = None
    csrf_hash: bytes | None = None
    auth_kind: str = "bearer"


def mint_step_jwt(
    *,
    team_id: UUID,
    trial_id: UUID,
    step_id: str,
    ttl_sec: int,
    signing_key: str,
    provider_connection_id: UUID | None = None,
) -> str:
    """Mint a step-scoped JWT carrying `(team_id, trial_id, step_id)`
    and optionally `provider_connection_id` (cluster-deploy.md /
    issue #72). Used by the Control Plane's POST /admin/step-tokens.

    When `provider_connection_id` is set, the JWT scope binds the
    bearer to one specific connection — the facade routes prefer the
    JWT-supplied value over the `x-loom-provider-connection-id`
    header (and 400 on mismatch). When None, the field is omitted
    from the payload so legacy verifiers see no change."""
    now = datetime.now(UTC)
    payload: dict[str, object] = {
        "iss": "loom-control-plane",
        "sub": "step-session",
        "team_id": str(team_id),
        "trial_id": str(trial_id),
        "step_id": step_id,
        "exp": int((now + timedelta(seconds=ttl_sec)).timestamp()),
        "iat": int(now.timestamp()),
        "scopes": ["llm:call"],
    }
    if provider_connection_id is not None:
        payload["provider_connection_id"] = str(provider_connection_id)
    body = jwt.encode(payload, signing_key, algorithm="HS256")
    return _STEP_JWT_PREFIX + body


def verify_step_jwt(token: str, *, signing_key: str) -> AuthContext:
    """Verify a step-scoped JWT. Raises `jwt.PyJWTError` subclasses on
    failure (ExpiredSignatureError, InvalidSignatureError,
    InvalidTokenError, etc.) — caller decides how to surface them.

    `provider_connection_id` is decoded when present; a missing
    claim leaves the field None so JWTs minted before issue #72
    keep verifying (graceful rollout)."""
    if not token.startswith(_STEP_JWT_PREFIX):
        raise jwt.InvalidTokenError("not a step JWT")
    body = token[len(_STEP_JWT_PREFIX):]
    payload = jwt.decode(body, signing_key, algorithms=["HS256"])
    raw_conn_id = payload.get("provider_connection_id")
    provider_connection_id: UUID | None = (
        UUID(raw_conn_id) if isinstance(raw_conn_id, str) else None
    )
    return AuthContext(
        token_hash=b"",                       # synthetic — no DB row
        type="step_session",
        scopes=list(payload.get("scopes", [])),
        team_id=UUID(payload["team_id"]),
        expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
        trial_id=UUID(payload["trial_id"]),
        step_id=payload["step_id"],
        provider_connection_id=provider_connection_id,
    )


async def verify_bearer_token(
    session: AsyncSession,
    header_value: str | None,
    *,
    signing_key: str | None = None,
    admin_verifier: AdminSecretVerifier | None = None,
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

    if admin_verifier is not None and admin_verifier.verify(raw):
        return AuthContext(
            token_hash=admin_verifier.token_hash,
            type="admin",
            scopes=[
                "admin:tokens",
                "admin:rate_cards",
                "admin:slurm_workers",
                "admin:gb10_workers",
                "admin:worker_pools",
            ],
            team_id=None,
            expires_at=None,
        )

    # JWT branch
    if raw.startswith(_STEP_JWT_PREFIX):
        if signing_key is None:
            return None
        try:
            return verify_step_jwt(raw, signing_key=signing_key)
        except jwt.PyJWTError:
            return None

    # DB-backed branch for team/worker credentials. Admin credentials moved to
    # the singleton secret verifier above; legacy DB admin rows are ignored so
    # production cannot accidentally authenticate through the removed path.
    token_hash = hashlib.sha256(raw.encode()).digest()
    row = (await session.execute(
        select(Token).where(Token.token_hash == token_hash),
    )).scalar_one_or_none()
    if row is None:
        return None
    if row.type == "admin" or any(
        scope.startswith("admin:") for scope in row.scopes
    ):
        return None
    if row.revoked_at is not None:
        return None
    if row.expires_at is not None and row.expires_at < datetime.now(UTC):
        return None

    token_hash = row.token_hash
    token_type = row.type
    scopes = list(row.scopes)
    team_id = row.team_id
    user_id = row.created_by_user_id
    expires_at = row.expires_at
    now = datetime.now(UTC)
    touch_cutoff = now - _TOKEN_TOUCH_DEBOUNCE
    if row.last_seen_at is None or row.last_seen_at <= touch_cutoff:
        await session.execute(
            update(Token)
            .where(Token.token_hash == token_hash)
            .where(
                (Token.last_seen_at.is_(None))
                | (Token.last_seen_at <= touch_cutoff),
            )
            .values(last_used_at=now, last_seen_at=now),
        )
    await session.commit()

    return AuthContext(
        token_hash=token_hash,
        type=token_type,
        scopes=scopes,
        team_id=team_id,
        user_id=user_id,
        expires_at=expires_at,
    )
