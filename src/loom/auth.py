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
from loom.db.schema import Token, User

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
    # True when the JWT explicitly carried the provider claim. This remains
    # meaningful when the claim value is null: an explicit null binds the
    # bearer to the platform route and must not be overridden by a header/body
    # connection id. Older JWTs with no claim keep the legacy fallback path.
    provider_connection_id_bound: bool = False
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
    provider_connection_id_bound: bool = False,
) -> str:
    """Mint a step-scoped JWT carrying `(team_id, trial_id, step_id)`
    and optionally `provider_connection_id` (cluster-deploy.md /
    issue #72). Used by the Control Plane's POST /admin/step-tokens.

    A concrete `provider_connection_id` binds the bearer to one connection.
    Set `provider_connection_id_bound=True` with None to bind it to the
    platform route. Facade routes treat either bound value as authoritative
    over header/body transport and reject mismatches."""
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
    if provider_connection_id is not None or provider_connection_id_bound:
        payload["provider_connection_id"] = (
            str(provider_connection_id) if provider_connection_id is not None else None
        )
    body = jwt.encode(payload, signing_key, algorithm="HS256")
    return _STEP_JWT_PREFIX + body


def verify_step_jwt(token: str, *, signing_key: str) -> AuthContext:
    """Verify a step-scoped JWT. Raises `jwt.PyJWTError` subclasses on
    failure (ExpiredSignatureError, InvalidSignatureError,
    InvalidTokenError, etc.) — caller decides how to surface them.

    `provider_connection_id` is decoded when present. A missing claim leaves
    the value unbound so JWTs minted before issue #72 retain their legacy
    header fallback during a rolling upgrade."""
    if not token.startswith(_STEP_JWT_PREFIX):
        raise jwt.InvalidTokenError("not a step JWT")
    body = token[len(_STEP_JWT_PREFIX) :]
    payload = jwt.decode(body, signing_key, algorithms=["HS256"])
    provider_connection_id_bound = "provider_connection_id" in payload
    raw_conn_id = payload.get("provider_connection_id")
    provider_connection_id: UUID | None = (
        UUID(raw_conn_id) if isinstance(raw_conn_id, str) else None
    )
    return AuthContext(
        token_hash=b"",  # synthetic — no DB row
        type="step_session",
        scopes=list(payload.get("scopes", [])),
        team_id=UUID(payload["team_id"]),
        expires_at=datetime.fromtimestamp(payload["exp"], tz=UTC),
        trial_id=UUID(payload["trial_id"]),
        step_id=payload["step_id"],
        provider_connection_id=provider_connection_id,
        provider_connection_id_bound=provider_connection_id_bound,
    )


async def verify_bearer_token(
    session: AsyncSession,
    header_value: str | None,
    *,
    signing_key: str | None = None,
    admin_verifier: AdminSecretVerifier | None = None,
    allow_readonly_probe: bool = False,
    allow_family_orchestrator: bool = False,
) -> AuthContext | None:
    """Validate a Bearer token. Returns an AuthContext or None.

    `signing_key` enables the step-JWT branch; if None, JWTs always
    return None (effectively disabling that path). DB-backed tokens
    are unaffected by `signing_key`. `allow_family_orchestrator` is a
    narrow opt-in for the Control Plane step-token exchange; it remains
    false for every other bearer-token consumer.
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

    # DB-backed branch for team/worker credentials plus narrowly opted-in
    # service principals. Admin credentials moved to the singleton secret
    # verifier above; legacy DB admin rows are ignored so production cannot
    # accidentally authenticate through the removed path.
    token_hash = hashlib.sha256(raw.encode()).digest()
    row = (
        await session.execute(
            select(Token).where(Token.token_hash == token_hash),
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    if row.type == "admin" or any(scope.startswith("admin:") for scope in row.scopes):
        return None
    if row.type == "readonly_probe":
        if (
            not allow_readonly_probe
            or row.team_id is None
            or list(row.scopes) != ["read:own"]
            or row.created_by_user_id is not None
        ):
            return None
    elif row.type == "family_orchestrator":
        if (
            not allow_family_orchestrator
            or row.team_id is not None
            or list(row.scopes) != ["family:evolve"]
            or row.created_by_user_id is not None
        ):
            return None
    elif row.type not in {"team", "worker"}:
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
    role = None
    if user_id is not None:
        is_platform_admin = (
            await session.execute(
                select(User.is_platform_admin).where(User.id == user_id),
            )
        ).scalar_one_or_none()
        if is_platform_admin:
            role = "platform_admin"
    expires_at = row.expires_at
    now = datetime.now(UTC)
    touch_cutoff = now - _TOKEN_TOUCH_DEBOUNCE
    if row.type != "readonly_probe" and (
        row.last_seen_at is None or row.last_seen_at <= touch_cutoff
    ):
        await session.execute(
            update(Token)
            .where(Token.token_hash == token_hash)
            .where(
                (Token.last_seen_at.is_(None)) | (Token.last_seen_at <= touch_cutoff),
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
        role=role,
        expires_at=expires_at,
        auth_kind="readonly_probe" if token_type == "readonly_probe" else "bearer",
    )
