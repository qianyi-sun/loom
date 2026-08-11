"""Agent-only persistence for disconnected unclaimed lifecycle transitions."""

from __future__ import annotations

import json
from collections.abc import Mapping

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.claim_guard import InertAttemptTransitionV1
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest

_SCHEMA = "loom_capacity_guard"
_BINDING_FIELDS = tuple(AgentRegistrationV1.model_fields)


class CapacityAttemptLifecycleError(RuntimeError):
    """An inert attempt transition is not exactly bound or canonical."""


class CapacityAttemptLifecycleStore:
    """Invoke the sole agent-only, nonclaiming lifecycle procedure."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        registration: AgentRegistrationV1,
    ) -> None:
        if not isinstance(registration, AgentRegistrationV1):
            raise TypeError("attempt lifecycle requires a trusted agent registration")
        self._session = session
        self._registration = registration

    async def apply_transition(
        self,
        transition: InertAttemptTransitionV1,
    ) -> InertAttemptTransitionV1:
        mismatches = tuple(
            field
            for field in _BINDING_FIELDS
            if getattr(transition, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise CapacityAttemptLifecycleError(
                f"attempt lifecycle binding mismatch: {', '.join(mismatches)}"
            )
        payload_bytes = canonical_bytes(transition)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.apply_inert_attempt_transition("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest)"
                    ),
                    {
                        "agent_incarnation": self._registration.agent_incarnation,
                        "payload": payload_bytes.decode("ascii"),
                        "canonical_payload": payload_bytes,
                        "payload_digest": canonical_digest(transition),
                    },
                )
            ).scalar_one()
            if not isinstance(returned, Mapping):
                raise CapacityAttemptLifecycleError(
                    "protected lifecycle procedure returned a non-object"
                )
            try:
                parsed = InertAttemptTransitionV1.model_validate_json(
                    json.dumps(
                        returned,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        allow_nan=False,
                    ).encode("ascii")
                )
            except (ValidationError, ValueError) as exc:
                raise CapacityAttemptLifecycleError(
                    "protected lifecycle procedure returned an invalid transition"
                ) from exc
            if parsed != transition or canonical_bytes(parsed) != payload_bytes:
                raise CapacityAttemptLifecycleError(
                    "protected lifecycle replay differs from its exact transition"
                )
        return parsed


__all__ = ["CapacityAttemptLifecycleError", "CapacityAttemptLifecycleStore"]
