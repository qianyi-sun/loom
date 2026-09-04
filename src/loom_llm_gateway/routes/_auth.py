"""Shared HTTP bearer boundary for every user-facing LLM dialect."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException

from loom.auth import AuthContext, validate_bearer_token

logger = logging.getLogger(__name__)

_LLM_CALL_SCOPE = "llm:call"
_INVALID_BEARER_DETAIL = {"code": "invalid_or_expired_bearer"}
_INVALID_BEARER_HEADERS = {
    "WWW-Authenticate": 'Bearer error="invalid_token"',
}


async def require_llm_call_bearer(
    session: Any,
    authorization: str | None,
    *,
    signing_key: str,
) -> AuthContext:
    """Authenticate a Gateway LLM call with one dialect-neutral contract.

    The externally visible 401 intentionally does not distinguish a missing,
    malformed, invalid, revoked, or expired credential. The internal log field
    contains only the validator's bounded reason vocabulary and never includes
    the Authorization header or token.
    """
    result = await validate_bearer_token(
        session,
        authorization,
        signing_key=signing_key,
    )
    if result.context is None:
        logger.info(
            "gateway bearer authentication rejected",
            extra={"bearer_validation_reason": result.reason},
        )
        raise HTTPException(
            status_code=401,
            detail=_INVALID_BEARER_DETAIL,
            headers=_INVALID_BEARER_HEADERS,
        )
    if _LLM_CALL_SCOPE not in result.context.scopes:
        logger.info(
            "gateway bearer authorization rejected",
            extra={"bearer_validation_reason": "missing_scope"},
        )
        raise HTTPException(
            status_code=403,
            detail={
                "code": "missing_scope",
                "required_scope": _LLM_CALL_SCOPE,
            },
        )
    return result.context
