"""Agent-only persistence for zero-executable initial trial registration."""

from __future__ import annotations

import json
from collections.abc import Mapping

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.contracts import AgentRegistrationV1, InertTrialSubmissionV1
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest

_SCHEMA = "loom_capacity_guard"
_BINDING_FIELDS = tuple(AgentRegistrationV1.model_fields)


class CapacityTrialSubmissionError(RuntimeError):
    """An initial protected submission is not exactly bound or canonical."""


class CapacityTrialSubmissionStore:
    """Invoke the sole agent-only, non-executable submission procedure."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        registration: AgentRegistrationV1,
    ) -> None:
        if not isinstance(registration, AgentRegistrationV1):
            raise TypeError("trial submission requires a trusted agent registration")
        self._session = session
        self._registration = registration

    async def register_initial_submission(
        self,
        submission: InertTrialSubmissionV1,
    ) -> InertTrialSubmissionV1:
        if not isinstance(submission, InertTrialSubmissionV1):
            raise TypeError("trial submission requires an inert submission contract")
        mismatches = tuple(
            field
            for field in _BINDING_FIELDS
            if getattr(submission, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise CapacityTrialSubmissionError(
                f"trial submission binding mismatch: {', '.join(mismatches)}"
            )

        payload_bytes = canonical_bytes(submission)
        requirements_bytes = canonical_bytes(submission.requirements)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.register_inert_trial_submission("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest, "
                        "CAST(:requirements_payload AS bytea), :requirements_digest)"
                    ),
                    {
                        "agent_incarnation": self._registration.agent_incarnation,
                        "payload": payload_bytes.decode("ascii"),
                        "canonical_payload": payload_bytes,
                        "payload_digest": canonical_digest(submission),
                        "requirements_payload": requirements_bytes,
                        "requirements_digest": submission.requirements_digest,
                    },
                )
            ).scalar_one()
            if not isinstance(returned, Mapping):
                raise CapacityTrialSubmissionError(
                    "protected submission procedure returned a non-object"
                )
            try:
                parsed = InertTrialSubmissionV1.model_validate_json(
                    json.dumps(
                        returned,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        allow_nan=False,
                    ).encode("ascii")
                )
            except (ValidationError, ValueError) as exc:
                raise CapacityTrialSubmissionError(
                    "protected submission procedure returned an invalid contract"
                ) from exc
            if parsed != submission or canonical_bytes(parsed) != payload_bytes:
                raise CapacityTrialSubmissionError(
                    "protected submission replay differs from its exact contract"
                )
        return parsed


__all__ = ["CapacityTrialSubmissionError", "CapacityTrialSubmissionStore"]
