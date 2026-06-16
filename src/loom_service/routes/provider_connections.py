"""Provider-connection CRUD (cluster-deploy.md §CLI surface + §Schema).

Routes:
- POST   /api/v1/provider-connections       — create + encrypt + store
- GET    /api/v1/provider-connections       — list (team-scoped; soft-deleted excluded)
- GET    /api/v1/provider-connections/{id}  — detail
- PATCH  /api/v1/provider-connections/{id}  — update (base_url, api_key, allowed_models, pricing)
- DELETE /api/v1/provider-connections/{id}  — soft-delete (sets deleted_at)

`test` + `models` routes land in a follow-up PR alongside the
provider_models_cache refresh contract.

Trust boundary:
- All routes require team-or-admin auth; cross-team access returns 404
  (not 403) to avoid leaking existence.
- Secret values (api_key) live in the SecretStore (loom-encrypted by
  default); the row stores only the opaque ref. Responses NEVER include
  the api_key — it's write-only.
- SSRF defense layer 3 (DNS-resolve + classify) gates creation; layers
  1+2+4 (sandbox isolation + step-JWT auth + egress proxy enforcement)
  ship separately.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import ProviderConnection, ProviderModelCache, TeamQuota
from loom.security.secret_store import LocalEncryptedSecretStore, SecretStore
from loom_service.auth_guards import is_admin
from loom_service.dependencies import SessionAndCtx
from loom_service.provider_connections_service import (
    InvalidBaseUrlError,
    InvalidPricingError,
    SsrfRejectedError,
    UpstreamModelFetchError,
    default_pricing_source_for,
    default_rate_card_provider_for,
    fetch_upstream_models,
    probe_connection,
    resolve_and_validate,
    validate_pricing,
)
from loom_service.provider_model_classifier import classify_model_id

router = APIRouter()


# ──────────────────────────────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────────────────────────────


# Matches the DB CHECK constraint domain. Kept here for the
# enum-in-error-message path; the actual validation against this set
# happens at the DB layer via the migration's CHECK constraint, so
# we don't need to re-list it in Pydantic-Literal form.
_PROVIDER_TYPES = ("openai-compatible", "anthropic", "google", "custom")


class ProviderConnectionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    type: str = Field(description=f"one of {_PROVIDER_TYPES}")
    base_url: str = Field(min_length=1)
    # api_key is write-only — never returned in responses. Accepts the
    # raw secret; the route encrypts via SecretStore before persisting.
    api_key: str = Field(min_length=1)
    allowed_models: list[str] | None = Field(default=None)
    # If omitted, defaults per provider_type:
    #   anthropic, google → rate-card
    #   openai-compatible, custom → tokens-only
    pricing_source: str | None = Field(default=None)
    # Required if pricing_source='operator-supplied'; rejected otherwise.
    pricing_data: dict[str, float] | None = Field(default=None)
    # Optional rate-card provider namespace used by facade-routed calls
    # when pricing_source='rate-card' (e.g. openai, together, fireworks).
    rate_card_provider: str | None = Field(default=None, min_length=1, max_length=128)


class ProviderConnectionUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # All fields optional — PATCH semantics. `name` and `type` are NOT
    # updatable (changing them changes which connection consumers
    # reference); operators who need a new name/type create a new
    # connection and migrate. base_url IS updatable (provider URL
    # changes are common) — the route re-derives upstream_host and
    # re-resolves IPs.
    base_url: str | None = None
    api_key: str | None = None
    allowed_models: list[str] | None = None
    pricing_source: str | None = None
    pricing_data: dict[str, float] | None = None
    rate_card_provider: str | None = Field(default=None, min_length=1, max_length=128)


class ProviderConnectionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    team_id: UUID
    name: str
    type: str
    base_url: str
    upstream_host: str
    resolved_egress_ips: list[str]
    allowed_models: list[str] | None
    status: str
    last_validated_at: datetime | None
    last_validation_error: str | None
    pricing_source: str
    pricing_data: dict[str, float] | None
    rate_card_provider: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime


class ProviderConnectionListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ProviderConnectionResponse]


class ProviderConnectionTestResponse(BaseModel):
    """Result of POST /provider-connections/{id}/test.

    Mirrors the row columns updated by the route so the CLI can render
    a useful summary without a second GET. `http_status=None` means the
    request never reached an HTTP server (DNS, connect, timeout).
    """

    model_config = ConfigDict(extra="forbid")

    connection_id: UUID
    status: str  # 'valid' | 'invalid'
    http_status: int | None
    last_validation_error: str | None
    last_validated_at: datetime


class ProviderModelCacheEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_id: str
    family: str | None
    context_length: int | None
    capabilities: dict[str, object]
    visible: bool
    hidden_reason: str | None
    last_seen_at: datetime
    upstream_present: bool
    source: str
    agent_capable: bool
    recommended: bool
    visibility: str


class ProviderModelCacheListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ProviderModelCacheEntry]


class ProviderModelRefreshResponse(BaseModel):
    """Summary of a /models/refresh: counts and the resulting cache
    state. Counts are computed against the cache BEFORE the upsert."""

    model_config = ConfigDict(extra="forbid")

    added: int
    refreshed: int
    missing: int
    items: list[ProviderModelCacheEntry]


class ProviderModelManualCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_id: str = Field(min_length=1, max_length=512)
    family: str | None = Field(default=None, max_length=128)
    context_length: int | None = Field(default=None, gt=0)
    capabilities: dict[str, object] = Field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _cache_row_to_entry(row: ProviderModelCache) -> ProviderModelCacheEntry:
    capabilities = dict(row.capabilities or {})
    source = capabilities.get("source")
    source_str = source if isinstance(source, str) else "discovered"
    classification = classify_model_id(
        row.model_id, source=source_str, family=row.family,
    )
    if not row.visible:
        visibility = "hidden"
        recommended = False
        hidden_reason = row.hidden_reason
    else:
        visibility = classification.visibility
        recommended = classification.recommended
        hidden_reason = row.hidden_reason or classification.reason
    return ProviderModelCacheEntry(
        model_id=row.model_id,
        family=row.family,
        context_length=row.context_length,
        capabilities=capabilities,
        visible=row.visible,
        hidden_reason=hidden_reason,
        last_seen_at=row.last_seen_at,
        upstream_present=row.upstream_present,
        source=source_str,
        agent_capable=classification.agent_capable,
        recommended=recommended,
        visibility=visibility,
    )


def _is_manual_model(row: ProviderModelCache) -> bool:
    source = (row.capabilities or {}).get("source")
    return source == "manual"


def _row_to_response(row: ProviderConnection) -> ProviderConnectionResponse:
    return ProviderConnectionResponse(
        id=row.id,
        team_id=row.team_id,
        name=row.display_name,
        type=row.provider_type,
        base_url=row.base_url,
        upstream_host=row.upstream_host,
        # Postgres inet[] comes back as a list of `ipaddress.IPvNAddress`
        # objects via SA; coerce to plain strings for the response.
        resolved_egress_ips=[str(ip) for ip in (row.resolved_egress_ips or [])],
        allowed_models=row.allowed_models,
        status=row.status,
        last_validated_at=row.last_validated_at,
        last_validation_error=row.last_validation_error,
        pricing_source=row.pricing_source,
        pricing_data=row.pricing_data,
        rate_card_provider=row.rate_card_provider,
        created_by=row.created_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _make_secret_store(session: AsyncSession) -> SecretStore:
    """Construct a SecretStore from the request's session.

    Today: always `LocalEncryptedSecretStore` (the data-path default
    per cluster-deploy.md). When the k8s-secret backend lands, this
    dispatches by an env var or per-deployment-mode config. The route
    code doesn't care which backend — it gets back an object satisfying
    the SecretStore Protocol.
    """
    return LocalEncryptedSecretStore(session)


async def _get_active_connection(
    session: AsyncSession, connection_id: UUID, ctx: AuthContext,
) -> ProviderConnection:
    """Lookup a connection visible to `ctx`. Returns 404 for nonexistent
    / soft-deleted / cross-team rows alike — same response so existence
    can't be probed across team boundaries. Admins (ctx.team_id is None)
    see any team's rows; team tokens see only their own."""
    row = (await session.execute(
        select(ProviderConnection)
        .where(
            ProviderConnection.id == connection_id,
            ProviderConnection.deleted_at.is_(None),
        ),
    )).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="provider_connection not found")
    if not is_admin(ctx) and row.team_id != ctx.team_id:
        raise HTTPException(status_code=404, detail="provider_connection not found")
    return row


async def _team_allows_private(session: AsyncSession, team_id: UUID) -> bool:
    """Read the team's allow_private_endpoints flag. Missing TeamQuota
    row = strict default (False). Routes that haven't seen the team
    before treat them as untrusted."""
    flag = (await session.execute(
        select(TeamQuota.allow_private_endpoints).where(
            TeamQuota.team_id == team_id,
        ),
    )).scalar_one_or_none()
    return bool(flag)


# ──────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────


@router.post(
    "/provider-connections",
    response_model=ProviderConnectionResponse,
    status_code=201,
)
async def create_connection(
    payload: ProviderConnectionCreate, sc: SessionAndCtx,
) -> ProviderConnectionResponse:
    session, ctx = sc
    if ctx.team_id is None:
        raise HTTPException(
            status_code=400,
            detail="provider_connection creation requires a team-scoped token",
        )
    team_id = ctx.team_id

    if payload.type not in _PROVIDER_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"type must be one of {_PROVIDER_TYPES}",
        )

    # Pricing defaults by provider type if not explicitly set.
    pricing_source = payload.pricing_source or default_pricing_source_for(payload.type)
    rate_card_provider = (
        payload.rate_card_provider
        if payload.rate_card_provider is not None
        else default_rate_card_provider_for(payload.type)
    )
    try:
        validate_pricing(pricing_source, payload.pricing_data)
    except InvalidPricingError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # SSRF defense layer 3: resolve + classify the upstream IPs against
    # the team's policy. Failure here is a 400; the user can fix the
    # URL or ask admin to flip the team flag.
    allow_private = await _team_allows_private(session, team_id)
    try:
        resolved = resolve_and_validate(
            payload.base_url, allow_private=allow_private,
        )
    except InvalidBaseUrlError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except SsrfRejectedError as e:
        raise HTTPException(
            status_code=400,
            detail=f"base_url rejected: {e}",
        ) from e

    # Encrypt the api_key BEFORE inserting the connection row so we
    # never have a row with a non-existent ref. If put() succeeds and
    # the INSERT fails, the orphaned secret is cleaned up by the
    # session rollback (LocalEncryptedSecretStore.put adds to session
    # via SQLAlchemy add() + flush; rollback removes it).
    secret_store = _make_secret_store(session)
    namespace = f"team:{team_id}"
    encrypted_ref = await secret_store.put(
        namespace=namespace, value=payload.api_key,
    )

    row = ProviderConnection(
        id=uuid4(),
        team_id=team_id,
        provider_type=payload.type,
        display_name=payload.name,
        base_url=payload.base_url,
        upstream_host=resolved.upstream_host,
        resolved_egress_ips=resolved.resolved_ips,
        egress_ips_refreshed_at=datetime.now(UTC),
        encrypted_api_key_ref=encrypted_ref,
        allowed_models=payload.allowed_models,
        status="pending",  # /test route flips to 'valid' / 'invalid'
        pricing_source=pricing_source,
        pricing_data=payload.pricing_data,
        rate_card_provider=rate_card_provider,
        # Per spec audit format: "<token-type>:<token-id-suffix>".
        # token_hash is bytes (sha256 digest); hex-encode the prefix
        # for a stable printable suffix without leaking the full hash.
        created_by=f"{ctx.type}:{ctx.token_hash.hex()[:16]}",
    )
    session.add(row)
    try:
        await session.flush()
    except IntegrityError as e:
        # The partial-UNIQUE-on-active-name catches duplicates here.
        # Roll back the SecretStore.put too (rolled back by session
        # rollback in the request teardown path).
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                f"a provider_connection named {payload.name!r} already "
                f"exists for this team"
            ),
        ) from e
    await session.commit()

    return _row_to_response(row)


@router.get(
    "/provider-connections",
    response_model=ProviderConnectionListResponse,
)
async def list_connections(
    sc: SessionAndCtx,
) -> ProviderConnectionListResponse:
    session, ctx = sc
    if ctx.team_id is None and not is_admin(ctx):
        raise HTTPException(
            status_code=400,
            detail="provider_connection list requires team-scoped or admin token",
        )
    stmt = select(ProviderConnection).where(
        ProviderConnection.deleted_at.is_(None),
    )
    # Admins see all teams' connections; team tokens see only their own.
    if not is_admin(ctx):
        stmt = stmt.where(ProviderConnection.team_id == ctx.team_id)
    stmt = stmt.order_by(ProviderConnection.created_at.desc())

    rows = (await session.execute(stmt)).scalars().all()
    return ProviderConnectionListResponse(
        items=[_row_to_response(r) for r in rows],
    )


@router.get(
    "/provider-connections/{connection_id}",
    response_model=ProviderConnectionResponse,
)
async def get_connection(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderConnectionResponse:
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    return _row_to_response(row)


@router.patch(
    "/provider-connections/{connection_id}",
    response_model=ProviderConnectionResponse,
)
async def update_connection(
    connection_id: UUID,
    payload: ProviderConnectionUpdate,
    sc: SessionAndCtx,
) -> ProviderConnectionResponse:
    session, ctx = sc
    if ctx.team_id is None and not is_admin(ctx):
        raise HTTPException(
            status_code=400,
            detail="PATCH requires team-scoped or admin token",
        )
    row = await _get_active_connection(session, connection_id, ctx)

    # base_url change → re-derive upstream_host + re-resolve IPs.
    # PATCH validates against the OWNER team's allow_private flag,
    # not the caller's (admin updating another team's row uses that
    # team's policy).
    if payload.base_url is not None:
        allow_private = await _team_allows_private(session, row.team_id)
        try:
            resolved = resolve_and_validate(
                payload.base_url, allow_private=allow_private,
            )
        except InvalidBaseUrlError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except SsrfRejectedError as e:
            raise HTTPException(
                status_code=400, detail=f"base_url rejected: {e}",
            ) from e
        row.base_url = payload.base_url
        row.upstream_host = resolved.upstream_host
        row.resolved_egress_ips = resolved.resolved_ips
        row.egress_ips_refreshed_at = datetime.now(UTC)
        # Connection needs re-validation against the new base_url; the
        # `test` route (Phase 2 follow-up) will flip back to 'valid'.
        row.status = "pending"

    # Pricing change: re-validate the combined source + data.
    if payload.pricing_source is not None or payload.pricing_data is not None:
        new_source = payload.pricing_source or row.pricing_source
        new_data = (
            payload.pricing_data if payload.pricing_data is not None
            else row.pricing_data
        )
        try:
            validate_pricing(new_source, new_data)
        except InvalidPricingError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        row.pricing_source = new_source
        row.pricing_data = new_data

    if payload.allowed_models is not None:
        row.allowed_models = payload.allowed_models

    if payload.rate_card_provider is not None:
        row.rate_card_provider = payload.rate_card_provider

    # API key rotation: encrypt new value, swap ref, queue old ref for
    # delete. The old ref's secret can't be deleted in this request
    # because in-flight gateway requests may still be using it (cache
    # holds the decrypted key for up to one indexed-updated_at-lookup
    # window). Phase 5 ships a cleanup job for revoked refs older than
    # the cache TTL.
    if payload.api_key is not None:
        secret_store = _make_secret_store(session)
        new_ref = await secret_store.put(
            namespace=f"team:{row.team_id}", value=payload.api_key,
        )
        # TODO Phase 5: track old refs in a `revoked_secrets` table for
        # delayed cleanup. For now, the old secret stays decryptable
        # but is no longer pointed at by any connection.
        row.encrypted_api_key_ref = new_ref
        row.status = "pending"

    await session.flush()  # trigger touches updated_at
    await session.commit()
    return _row_to_response(row)


@router.post(
    "/provider-connections/{connection_id}/test",
    response_model=ProviderConnectionTestResponse,
)
async def test_connection(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderConnectionTestResponse:
    """Probe the configured base_url with the stored (decrypted) api_key.

    Decrypts the api_key from SecretStore, issues a single short-timeout
    GET to the provider's `/models` endpoint (`?key=...` for google),
    and persists the outcome on the row: `status` ∈ {'valid', 'invalid'},
    `last_validated_at`, `last_validation_error`. Redirects are NOT
    followed — a 3xx to an internal host would re-introduce SSRF
    despite the create-time IP validation.

    The api_key never leaves loom_service; it is decrypted in-process
    and passed only in the probe request's headers.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)

    secret_store = _make_secret_store(session)
    api_key = await secret_store.get(row.encrypted_api_key_ref)

    result = await probe_connection(
        row.provider_type, row.base_url, api_key,
    )

    now = datetime.now(UTC)
    await session.execute(
        update(ProviderConnection)
        .where(ProviderConnection.id == row.id)
        .values(
            status=result.status,
            last_validated_at=now,
            last_validation_error=result.error,
        ),
    )
    await session.commit()
    return ProviderConnectionTestResponse(
        connection_id=row.id,
        status=result.status,
        http_status=result.http_status,
        last_validation_error=result.error,
        last_validated_at=now,
    )


@router.get(
    "/provider-connections/{connection_id}/models",
    response_model=ProviderModelCacheListResponse,
)
async def list_models(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderModelCacheListResponse:
    """Return every cached model row for the connection.

    No auto-refresh: the cache is populated on `POST /models/refresh`
    only. The CLI surfaces an empty list with a hint to run refresh.
    Ordering by model_id keeps the output stable for CI use.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    rows = (await session.execute(
        select(ProviderModelCache)
        .where(ProviderModelCache.provider_connection_id == row.id)
        .order_by(ProviderModelCache.model_id.asc()),
    )).scalars().all()
    return ProviderModelCacheListResponse(
        items=[_cache_row_to_entry(r) for r in rows],
    )


@router.post(
    "/provider-connections/{connection_id}/models",
    response_model=ProviderModelCacheEntry,
    status_code=201,
)
async def create_manual_model(
    connection_id: UUID,
    payload: ProviderModelManualCreate,
    sc: SessionAndCtx,
) -> ProviderModelCacheEntry:
    """Upsert an explicit model id for endpoints with no discovery.

    Manual rows share `provider_models_cache` so the selector sees one
    per-connection catalog. They remain visible even when a later
    upstream refresh does not return the id.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    now = datetime.now(UTC)
    capabilities = dict(payload.capabilities)
    capabilities["source"] = "manual"
    stmt = pg_insert(ProviderModelCache).values(
        provider_connection_id=row.id,
        model_id=payload.model_id,
        family=payload.family,
        context_length=payload.context_length,
        capabilities=capabilities,
        visible=True,
        hidden_reason=None,
        last_seen_at=now,
        upstream_present=False,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            ProviderModelCache.provider_connection_id,
            ProviderModelCache.model_id,
        ],
        set_=dict(
            family=payload.family,
            context_length=payload.context_length,
            capabilities=capabilities,
            visible=True,
            hidden_reason=None,
            last_seen_at=now,
            upstream_present=False,
        ),
    )
    await session.execute(stmt)
    await session.commit()
    cache_row = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == row.id,
            ProviderModelCache.model_id == payload.model_id,
        ),
    )).scalar_one()
    return _cache_row_to_entry(cache_row)


@router.post(
    "/provider-connections/{connection_id}/models/refresh",
    response_model=ProviderModelRefreshResponse,
)
async def refresh_models(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderModelRefreshResponse:
    """Probe the upstream `/models` endpoint and upsert the cache.

    For each id returned by the upstream:
    - existing row → bump `last_seen_at`, set `upstream_present=true`;
      if the row was marked `missing-upstream` (system-hidden), flip
      it back to visible (operator-hidden rows stay hidden — refresh
      MUST NOT clobber operator overrides).
    - new row → INSERT visible=true, upstream_present=true.

    For each existing row NOT returned by the upstream:
    - set `upstream_present=false` and (only if not operator-hidden)
      `visible=false, hidden_reason='missing-upstream'`. Operator-
      hidden rows keep their `hidden_reason='operator-hidden'`.

    Upstream errors (network / non-2xx / parse) bubble up as 502;
    the cache is left untouched (no partial commits).
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)

    secret_store = _make_secret_store(session)
    api_key = await secret_store.get(row.encrypted_api_key_ref)

    try:
        upstream_ids = await fetch_upstream_models(
            row.provider_type, row.base_url, api_key,
        )
    except UpstreamModelFetchError as e:
        # 502 — operator's upstream is the source of the failure, not
        # loom_service itself. Carry along the http_status when we have
        # one so the CLI can render a useful hint.
        detail = f"upstream /models fetch failed: {e}"
        if e.http_status is not None:
            detail += f" (upstream HTTP {e.http_status})"
        raise HTTPException(status_code=502, detail=detail) from e

    upstream_set = set(upstream_ids)
    existing_rows = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == row.id,
        ),
    )).scalars().all()
    existing_by_id = {r.model_id: r for r in existing_rows}
    existing_ids = set(existing_by_id.keys())

    now = datetime.now(UTC)

    # Upsert each upstream id. INSERT ... ON CONFLICT DO UPDATE handles
    # the new vs. seen split atomically; we count after-the-fact.
    added = 0
    refreshed = 0
    for mid in upstream_ids:
        existing = existing_by_id.get(mid)
        if existing is None:
            added += 1
        else:
            refreshed += 1
        # Determine visibility post-refresh:
        # - operator-hidden stays hidden
        # - missing-upstream → unhide (we just saw it)
        # - everything else → visible
        if existing is not None and existing.hidden_reason == "operator-hidden":
            visible = False
            hidden_reason: str | None = "operator-hidden"
        else:
            visible = True
            hidden_reason = None

        stmt = pg_insert(ProviderModelCache).values(
            provider_connection_id=row.id,
            model_id=mid,
            last_seen_at=now,
            upstream_present=True,
            visible=visible,
            hidden_reason=hidden_reason,
            capabilities={"source": "discovered"},
        )
        update_values: dict[str, object] = dict(
            last_seen_at=now,
            upstream_present=True,
            visible=visible,
            hidden_reason=hidden_reason,
        )
        if existing is None or not _is_manual_model(existing):
            update_values["capabilities"] = {"source": "discovered"}
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                ProviderModelCache.provider_connection_id,
                ProviderModelCache.model_id,
            ],
            set_=update_values,
        )
        await session.execute(stmt)

    # Mark anything we DIDN'T see as missing. Operator-hidden rows keep
    # their hidden_reason; only flip upstream_present.
    missing_ids = existing_ids - upstream_set
    missing = len(missing_ids)
    for mid in missing_ids:
        prev = existing_by_id[mid]
        if prev.hidden_reason == "operator-hidden":
            await session.execute(
                update(ProviderModelCache)
                .where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id == mid,
                )
                .values(upstream_present=False),
            )
        elif _is_manual_model(prev):
            await session.execute(
                update(ProviderModelCache)
                .where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id == mid,
                )
                .values(
                    upstream_present=False,
                    visible=True,
                    hidden_reason=None,
                ),
            )
        else:
            await session.execute(
                update(ProviderModelCache)
                .where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id == mid,
                )
                .values(
                    upstream_present=False,
                    visible=False,
                    hidden_reason="missing-upstream",
                ),
            )

    await session.commit()

    refreshed_rows = (await session.execute(
        select(ProviderModelCache)
        .where(ProviderModelCache.provider_connection_id == row.id)
        .order_by(ProviderModelCache.model_id.asc()),
    )).scalars().all()
    return ProviderModelRefreshResponse(
        added=added,
        refreshed=refreshed,
        missing=missing,
        items=[_cache_row_to_entry(r) for r in refreshed_rows],
    )


@router.post(
    "/provider-connections/{connection_id}/models/{model_id:path}/hide",
    response_model=ProviderModelCacheEntry,
)
async def hide_model(
    connection_id: UUID, model_id: str, sc: SessionAndCtx,
) -> ProviderModelCacheEntry:
    """Mark a cached model `visible=false` with reason `operator-hidden`.
    Subsequent refreshes will not flip it back to visible — the operator's
    hide overrides upstream presence.

    404 if the model isn't in the cache for this connection. The CLI
    hints to run /refresh first when this happens.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    cache_row = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == row.id,
            ProviderModelCache.model_id == model_id,
        ),
    )).scalar_one_or_none()
    if cache_row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"model {model_id!r} not in cache for this connection — "
                f"run POST /models/refresh first"
            ),
        )
    cache_row.visible = False
    cache_row.hidden_reason = "operator-hidden"
    await session.commit()
    return _cache_row_to_entry(cache_row)


@router.post(
    "/provider-connections/{connection_id}/models/{model_id:path}/unhide",
    response_model=ProviderModelCacheEntry,
)
async def unhide_model(
    connection_id: UUID, model_id: str, sc: SessionAndCtx,
) -> ProviderModelCacheEntry:
    """Reverse a previous hide. If the model is still upstream-present,
    sets `visible=true, hidden_reason=NULL`. If the model is no longer
    upstream-present, sets `visible=false, hidden_reason='missing-upstream'`
    — operators can't make a missing model visible without re-refreshing
    and finding it.

    404 if the model isn't in the cache.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    cache_row = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == row.id,
            ProviderModelCache.model_id == model_id,
        ),
    )).scalar_one_or_none()
    if cache_row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"model {model_id!r} not in cache for this connection — "
                f"run POST /models/refresh first"
            ),
        )
    if cache_row.upstream_present:
        cache_row.visible = True
        cache_row.hidden_reason = None
    else:
        cache_row.visible = False
        cache_row.hidden_reason = "missing-upstream"
    await session.commit()
    return _cache_row_to_entry(cache_row)


@router.delete(
    "/provider-connections/{connection_id}",
    status_code=204,
)
async def delete_connection(
    connection_id: UUID, sc: SessionAndCtx,
) -> None:
    """Soft-delete: sets `deleted_at = now()`. Existing Trial /
    Batch FKs (when those land in the next PR) retain attribution
    for billing/audit. Re-using the display_name is permitted after
    soft-delete (partial UNIQUE WHERE deleted_at IS NULL).

    The SecretStore ref is NOT deleted here — in-flight gateway
    requests may still need it. Phase 5 cleanup walker reclaims
    refs whose owning connection has been deleted for > cache TTL.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    await session.execute(
        update(ProviderConnection)
        .where(ProviderConnection.id == row.id)
        .values(deleted_at=datetime.now(UTC), status="disabled"),
    )
    await session.commit()
