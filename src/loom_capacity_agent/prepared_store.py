"""Agent-only persistence for zero-executable prepared admission records."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    AbandonedAdmissionPlanV1,
    NeverConvergedAdmissionPlanV1,
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedProtectedReleaseV1,
    PreparedWorkerBindingV1,
)
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_bytes

_SCHEMA = "loom_capacity_guard"
_BINDING_FIELDS = tuple(AgentRegistrationV1.model_fields)
_PreparedT = TypeVar(
    "_PreparedT",
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedProtectedReleaseV1,
    PreparedWorkerBindingV1,
    AbandonedAdmissionPlanV1,
    NeverConvergedAdmissionPlanV1,
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

    async def assert_current_plan(
        self,
        *,
        plan_id: UUID,
        admission_incarnation: UUID,
        manager_allocation_epoch: int,
        pool_id: Literal["oldlab", "gb10"],
        prepared_plan_digest: str,
    ) -> None:
        """Lock one exact prepared plan while its acknowledgement is published."""

        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.assert_current_inert_admission_plan("
                        ":agent_incarnation, :plan_id, :admission_incarnation, "
                        ":manager_allocation_epoch, :pool_id, :prepared_plan_digest)"
                    ),
                    {
                        "agent_incarnation": self._registration.agent_incarnation,
                        "plan_id": plan_id,
                        "admission_incarnation": admission_incarnation,
                        "manager_allocation_epoch": manager_allocation_epoch,
                        "pool_id": pool_id,
                        "prepared_plan_digest": prepared_plan_digest,
                    },
                )
            ).scalar_one()
            if returned is not True:
                raise CapacityPreparedAdmissionError(
                    "protected current-plan assertion returned an invalid result"
                )

    async def abandon_plan(
        self,
        abandonment: AbandonedAdmissionPlanV1,
    ) -> AbandonedAdmissionPlanV1:
        return await self._invoke(
            "abandon_inert_admission_plan",
            abandonment,
            AbandonedAdmissionPlanV1,
        )

    async def tombstone_never_converged_plan(
        self,
        tombstone: NeverConvergedAdmissionPlanV1,
    ) -> NeverConvergedAdmissionPlanV1:
        self._assert_binding(tombstone)
        registration = AgentRegistrationV1.model_validate(
            {
                field: getattr(tombstone, field)
                for field in AgentRegistrationV1.model_fields
            }
        )
        payload_bytes = canonical_bytes(tombstone)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.tombstone_never_converged_admission_plan("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest, "
                        "CAST(:registration_payload AS bytea), :registration_digest, "
                        "CAST(:closure_payload AS bytea), :closure_digest, "
                        "CAST(:proposal_payload AS bytea), :proposal_digest)"
                    ),
                    {
                        "agent_incarnation": self._registration.agent_incarnation,
                        "payload": payload_bytes.decode("ascii"),
                        "canonical_payload": payload_bytes,
                        "payload_digest": canonical_digest(tombstone),
                        "registration_payload": canonical_bytes(registration),
                        "registration_digest": tombstone.registration_digest,
                        "closure_payload": canonical_executable_bytes(
                            tombstone.closure
                        ),
                        "closure_digest": tombstone.closure_digest,
                        "proposal_payload": canonical_executable_bytes(
                            tombstone.closure.proposal
                        ),
                        "proposal_digest": tombstone.proposal_digest,
                    },
                )
            ).scalar_one()
            parsed = parse_protected_response(
                returned,
                NeverConvergedAdmissionPlanV1,
                label="never-converged tombstone procedure",
            )
            if parsed != tombstone or canonical_bytes(parsed) != payload_bytes:
                raise CapacityPreparedAdmissionError(
                    "protected never-converged replay differs from its exact contract"
                )
        return parsed

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
