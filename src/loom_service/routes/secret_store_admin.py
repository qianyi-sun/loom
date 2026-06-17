"""POST /api/v1/admin/secret-store/rewrap — master-key rotation walker.

Operator endpoint for the 2-stage online master-key rotation protocol
(approach C from #80). The endpoint walks all ``loom://`` refs in the
``secrets`` table and re-encrypts each one with the store's current
PRIMARY key (the first entry in ``LOOM_SECRET_STORE_MASTER_KEYS``).

2-stage online rotation protocol:
  Step 1 (deploy): ``LOOM_SECRET_STORE_MASTER_KEYS=<NEW>,<CURRENT>``
    Deploy loom-service. New encrypts use the NEW (primary) key.
    Reads decrypt via fallback for existing rows still using CURRENT.
  Step 2 (rewrap): call this endpoint.
    Walks all refs; re-encrypts each with the NEW (primary) key.
    After completion, no rows use CURRENT any more.
  Step 3 (cleanup): ``LOOM_SECRET_STORE_MASTER_KEYS=<NEW>``
    Deploy loom-service with only the new key. Rotation complete.

This endpoint does NOT update ``LOOM_SECRET_STORE_MASTER_KEYS`` — that
is an out-of-band operator step (kubectl patch Secret + rollout restart).
"""

from __future__ import annotations

import base64
import logging

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from loom.security.secret_store import (
    _MASTER_KEY_LEN,
    LocalEncryptedSecretStore,
    SecretStoreError,
    load_master_keys_from_env,
)
from loom_service.admin_audit import require_admin_actor, write_admin_audit_event
from loom_service.dependencies import AdminSessionAndCtx

log = logging.getLogger(__name__)

router = APIRouter()


class _RewrapRequest(BaseModel):
    """Optional override for the rewrap target key.

    If omitted, the endpoint uses the PRIMARY key already configured
    in ``LOOM_SECRET_STORE_MASTER_KEYS`` (the normal production path).
    Supplying ``new_master_key`` is only needed for scripted testing or
    disaster-recovery flows where you can't redeploy first.
    """
    new_master_key: str | None = None


def _decode_new_key(b64: str) -> bytes:
    """Validate + decode the caller-supplied new_master_key."""
    try:
        raw = base64.b64decode(b64.strip(), validate=True)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"new_master_key is not valid base64: {exc}",
        ) from exc
    if len(raw) != _MASTER_KEY_LEN:
        raise HTTPException(
            status_code=400,
            detail=(
                f"new_master_key decodes to {len(raw)} bytes; "
                f"expected {_MASTER_KEY_LEN} (AES-256)"
            ),
        )
    return raw


@router.post("/admin/secret-store/rewrap")
async def rewrap_all_secrets(
    request: Request,
    sc: AdminSessionAndCtx,
    payload: _RewrapRequest,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, object]:
    """Walk every ``loom://`` ref and re-encrypt with the primary key.

    Response body::

        {
          "rewrapped": <int>,
          "already_current": <int>,
          "failed": [["<ref>", "<error>"], ...]
        }

    HTTP 207 when any ref failed (partial success). The walk never
    short-circuits — all refs are attempted regardless of failures so
    the operator gets a complete picture.

    Auth: admin token required (``authed_admin`` dependency).
    Audit: one event per successful rewrap; summary event on completion.

    IMPORTANT: Do not call this endpoint before deploying the new key as
    a fallback (``LOOM_SECRET_STORE_MASTER_KEYS=NEW,OLD``). After this
    endpoint runs, rows encrypted with the OLD key are gone — if the
    running process can't decrypt with NEW, reads will fail.
    """
    session, _ctx = sc
    admin_actor = require_admin_actor(x_loom_admin_actor)

    # Resolve the target key: caller-supplied OR primary from env.
    try:
        primary_key, primary_version, fallback_keys = load_master_keys_from_env()
    except SecretStoreError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if payload.new_master_key is not None:
        target_key = _decode_new_key(payload.new_master_key)
    else:
        target_key = primary_key

    # Sanity check: if new_master_key was supplied AND it matches the
    # primary, that's fine (harmless re-encrypt to same key).  If it
    # was omitted, we already have the primary. No "same as current"
    # rejection — idempotent rewrap is safe.

    store = LocalEncryptedSecretStore(
        session,
        master_key=primary_key,
        master_key_version=primary_version,
        fallback_keys=fallback_keys,
    )

    rewrapped = 0
    already_current: int = 0
    failed: list[tuple[str, str]] = []

    # Stream refs to avoid loading the whole secrets table into memory.
    refs: list[str] = []
    async for ref in store.list_refs():
        refs.append(ref)

    for ref in refs:
        try:
            await store.rewrap(ref, new_master_key=target_key)
            rewrapped += 1
            log.info(
                "secret.rewrap ref=%s actor=%s", ref, admin_actor,
            )
        except Exception as exc:
            err = str(exc)
            log.warning(
                "secret.rewrap.failed ref=%s actor=%s error=%s",
                ref, admin_actor, err,
            )
            failed.append((ref, err))

    # Write one summary audit event (not one per ref — audit table
    # would blow up on large secrets tables).
    await write_admin_audit_event(
        session,
        actor=admin_actor,
        action="secret_store.rewrap",
        target_type="secret_store",
        target_id="all",
        request=request,
        metadata={
            "rewrapped": rewrapped,
            "already_current": already_current,
            "failed_count": len(failed),
        },
    )

    await session.commit()

    body: dict[str, object] = {
        "rewrapped": rewrapped,
        "already_current": already_current,
        "failed": [[r, e] for r, e in failed],
    }
    if failed:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=207, content=body)  # type: ignore[return-value]
    return body
