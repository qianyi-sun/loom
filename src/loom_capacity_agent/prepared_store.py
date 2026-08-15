"""Agent-only persistence for zero-executable prepared admission records."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TypeVar

from pydantic import BaseModel, ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedProtectedReleaseV1,
    PreparedWorkerBindingV1,
)
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest

_SCHEMA = "loom_capacity_guard"
_BINDING_FIELDS = tuple(AgentRegistrationV1.model_fields)
_PreparedT = TypeVar(
    "_PreparedT",
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedProtectedReleaseV1,
    PreparedWorkerBindingV1,
)
_ResponseT = TypeVar("_ResponseT", bound=BaseModel)


class CapacityPreparedAdmissionError(RuntimeError):
    """Prepared admission state is not exactly bound or canonical."""


def parse_protected_response(
    returned: object,
    model_type: type[_ResponseT],
    *,
    label: str,
) -> _ResponseT:
    """Parse one bounded protected procedure response without type coercion."""

    if not isinstance(returned, Mapping):
        raise CapacityPreparedAdmissionError(f"protected {label} returned a non-object")
    try:
        return model_type.model_validate_json(
            json.dumps(
                returned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        )
    except (ValidationError, ValueError) as exc:
        raise CapacityPreparedAdmissionError(
            f"protected {label} returned an invalid contract"
        ) from exc


class CapacityPreparedAdmissionStore:
    """Invoke the agent's reviewed inert SECURITY DEFINER procedures."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        registration: AgentRegistrationV1,
    ) -> None:
        if not isinstance(registration, AgentRegistrationV1):
            raise TypeError("prepared admission requires a trusted agent registration")
        self._session = session
        self._registration = registration

    def _assert_binding(self, value: AgentRegistrationV1) -> None:
        mismatches = tuple(
            field
            for field in _BINDING_FIELDS
            if getattr(value, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise CapacityPreparedAdmissionError(
                f"prepared admission binding mismatch: {', '.join(mismatches)}"
            )

    async def _invoke(
        self,
        function_name: str,
        value: _PreparedT,
        model_type: type[_PreparedT],
    ) -> _PreparedT:
        self._assert_binding(value)
        payload_bytes = canonical_bytes(value)
        payload_text = payload_bytes.decode("ascii")
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
                        "payload": payload_text,
                        "canonical_payload": payload_bytes,
                        "payload_digest": canonical_digest(value),
                    },
                )
            ).scalar_one()
            parsed = parse_protected_response(
                returned,
                model_type,
                label="prepared-admission procedure",
            )
            if parsed != value or canonical_bytes(parsed) != payload_bytes:
                raise CapacityPreparedAdmissionError(
                    "protected prepared-admission replay differs from its exact contract"
                )
        return parsed

    async def prepare_plan(self, plan: PreparedAdmissionPlanV1) -> PreparedAdmissionPlanV1:
        return await self._invoke(
            "prepare_inert_admission_plan",
            plan,
            PreparedAdmissionPlanV1,
        )

    async def register_bootstrap(
        self,
        binding: PreparedBootstrapBindingV1,
    ) -> PreparedBootstrapBindingV1:
        return await self._invoke(
            "register_inert_bootstrap",
            binding,
            PreparedBootstrapBindingV1,
        )

    async def record_prepared_worker(
        self,
        binding: PreparedWorkerBindingV1,
    ) -> PreparedWorkerBindingV1:
        return await self._invoke(
            "record_inert_worker",
            binding,
            PreparedWorkerBindingV1,
        )

    async def acknowledge_protected_release(
        self,
        release: PreparedProtectedReleaseV1,
    ) -> PreparedProtectedReleaseV1:
        return await self._invoke(
            "acknowledge_inert_protected_release",
            release,
            PreparedProtectedReleaseV1,
        )


__all__ = [
    "CapacityPreparedAdmissionError",
    "CapacityPreparedAdmissionStore",
    "parse_protected_response",
]
