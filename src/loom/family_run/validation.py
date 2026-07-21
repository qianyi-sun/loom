"""Validation for security-sensitive family-run adapter parameters."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from loom.family_run.spec import PluginRef, ResolvedFamilyRunSpec

_SKILL_PATCHER_LLM = "skill_patcher_llm"
_SECRET_LIKE_KEY = re.compile(
    r"(?:^|_)(?:"
    r"api_?key|authorization|bearer|credentials?|headers?|password|passwd|"
    r"secrets?|token|cookies?|private_?key|access_?key"
    r")(?:$|_)",
)


def normalize_evolver_provider_connection(
    resolved: ResolvedFamilyRunSpec,
) -> tuple[ResolvedFamilyRunSpec, UUID | None]:
    """Validate and canonicalize ``skill_patcher_llm`` provider routing.

    Provider credentials must stay behind a stored provider connection. The
    adapter's free-form params therefore reject secret-like keys recursively
    and accept only an optional provider connection UUID. The returned spec is
    safe to persist and the UUID can be authorized by the service before any
    family state is materialized.
    """
    if resolved.adapter.name != _SKILL_PATCHER_LLM:
        return resolved, None

    params = dict(resolved.adapter.params)
    secret_path = _find_secret_like_key(params)
    if secret_path is not None:
        raise ValueError(
            "skill_patcher_llm adapter params contain a secret-like key at "
            f"{secret_path}; store credentials in a provider connection and "
            "pass only provider_connection_id",
        )

    if "provider_connection_id" not in params:
        return resolved, None

    raw_connection_id = params["provider_connection_id"]
    raw_text = str(raw_connection_id).strip() if raw_connection_id is not None else ""
    if not raw_text:
        raise ValueError(
            "skill_patcher_llm adapter params.provider_connection_id must be "
            "a non-empty UUID",
        )
    try:
        connection_id = UUID(raw_text)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(
            "skill_patcher_llm adapter params.provider_connection_id must be "
            "a valid UUID",
        ) from exc

    canonical_id = str(connection_id)
    if params["provider_connection_id"] == canonical_id:
        return resolved, connection_id

    params["provider_connection_id"] = canonical_id
    normalized_adapter = PluginRef(name=resolved.adapter.name, params=params)
    return (
        resolved.model_copy(update={"adapter": normalized_adapter}),
        connection_id,
    )


def _find_secret_like_key(
    value: Any,
    *,
    path: tuple[str, ...] = (),
) -> str | None:
    if isinstance(value, Mapping):
        for raw_key, nested in value.items():
            key = str(raw_key)
            key_path = (*path, key)
            if _is_secret_like_key(key):
                return ".".join(key_path)
            found = _find_secret_like_key(nested, path=key_path)
            if found is not None:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            found = _find_secret_like_key(nested, path=(*path, str(index)))
            if found is not None:
                return found
    return None


def _is_secret_like_key(key: str) -> bool:
    snake_case = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", snake_case).strip("_").lower()
    return bool(_SECRET_LIKE_KEY.search(normalized))
