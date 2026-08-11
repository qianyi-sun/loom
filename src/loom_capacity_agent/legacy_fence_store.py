"""Agent-only persistence for the inert legacy-authority fence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TypeVar

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.legacy_fence import (
    LegacyCompatibilityFreezeV1,
    LegacyCompatibilityPreparationV1,
)
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest

_SCHEMA = "loom_capacity_guard"
_BINDING_FIELDS = tuple(AgentRegistrationV1.model_fields)
_LegacyFenceT = TypeVar(
    "_LegacyFenceT",
    LegacyCompatibilityPreparationV1,
    LegacyCompatibilityFreezeV1,
)


class LegacyCompatibilityFenceError(RuntimeError):
    """Legacy compatibility evidence is not exactly bound or canonical."""


class LegacyCompatibilityFenceStore:
    """Invoke only the reviewed, non-executable legacy fence procedures."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        registration: AgentRegistrationV1,
    ) -> None:
        if not isinstance(registration, AgentRegistrationV1):
            raise TypeError("legacy compatibility fence requires a trusted agent registration")
        self._session = session
        self._registration = registration

    def _assert_binding(self, value: AgentRegistrationV1) -> None:
        mismatches = tuple(
            field
            for field in _BINDING_FIELDS
            if getattr(value, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise LegacyCompatibilityFenceError(
                f"legacy compatibility binding mismatch: {', '.join(mismatches)}"
            )

    async def _invoke(
        self,
        function_name: str,
        value: _LegacyFenceT,
        model_type: type[_LegacyFenceT],
    ) -> _LegacyFenceT:
        self._assert_binding(value)
        payload_bytes = canonical_bytes(value)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.{function_name}("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest)"
                    ),
                    {
                        "agent_incarnation": self._registration.agent_incarnation,
                        "payload": payload_bytes.decode("ascii"),
                        "canonical_payload": payload_bytes,
                        "payload_digest": canonical_digest(value),
                    },
                )
            ).scalar_one()
            if not isinstance(returned, Mapping):
                raise LegacyCompatibilityFenceError(
                    "protected legacy-fence procedure returned a non-object"
                )
            try:
                parsed = model_type.model_validate_json(
                    json.dumps(
                        returned,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        allow_nan=False,
                    ).encode("ascii")
                )
            except (ValidationError, ValueError) as exc:
                raise LegacyCompatibilityFenceError(
                    "protected legacy-fence procedure returned an invalid contract"
                ) from exc
            if parsed != value or canonical_bytes(parsed) != payload_bytes:
                raise LegacyCompatibilityFenceError(
                    "protected legacy-fence replay differs from its exact contract"
                )
        return parsed

    async def prepare(
        self,
        preparation: LegacyCompatibilityPreparationV1,
    ) -> LegacyCompatibilityPreparationV1:
        return await self._invoke(
            "prepare_inert_legacy_compatibility",
            preparation,
            LegacyCompatibilityPreparationV1,
        )

    async def freeze(
        self,
        freeze: LegacyCompatibilityFreezeV1,
    ) -> LegacyCompatibilityFreezeV1:
        return await self._invoke(
            "freeze_inert_legacy_compatibility",
            freeze,
            LegacyCompatibilityFreezeV1,
        )


__all__ = ["LegacyCompatibilityFenceError", "LegacyCompatibilityFenceStore"]
