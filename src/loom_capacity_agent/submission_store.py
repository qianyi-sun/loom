"""Agent-only persistence for zero-executable initial trial registration."""

from __future__ import annotations

import json
from collections.abc import Mapping

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    AtomicTrialSubmissionReceiptV1,
    AtomicTrialSubmissionV1,
    InertTrialSubmissionV1,
    atomic_submission_bytes,
    atomic_submission_digest,
)
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

    async def create_initial_submission(
        self,
        submission: AtomicTrialSubmissionV1,
    ) -> AtomicTrialSubmissionReceiptV1:
        """Atomically create the public projection and its protected attempt."""

        if not isinstance(submission, AtomicTrialSubmissionV1):
            raise TypeError("trial creation requires an atomic submission contract")
        mismatches = tuple(
            field
            for field in _BINDING_FIELDS
            if getattr(submission, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise CapacityTrialSubmissionError(
                f"trial submission binding mismatch: {', '.join(mismatches)}"
            )
        protected = InertTrialSubmissionV1.model_validate(
            {field: getattr(submission, field) for field in InertTrialSubmissionV1.model_fields}
        )
        payload_bytes = atomic_submission_bytes(submission)
        protected_bytes = canonical_bytes(protected)
        requirements_bytes = canonical_bytes(submission.requirements)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.submit_inert_trial_projection("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :payload_digest, "
                        "CAST(:protected_payload AS jsonb), "
                        "CAST(:protected_canonical_payload AS bytea), "
                        ":protected_payload_digest, "
                        "CAST(:requirements_payload AS bytea), :requirements_digest)"
                    ),
                    {
                        "agent_incarnation": self._registration.agent_incarnation,
                        "payload": payload_bytes.decode("utf-8"),
                        "canonical_payload": payload_bytes,
                        "payload_digest": atomic_submission_digest(submission),
                        "protected_payload": protected_bytes.decode("ascii"),
                        "protected_canonical_payload": protected_bytes,
                        "protected_payload_digest": canonical_digest(protected),
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
                receipt = AtomicTrialSubmissionReceiptV1.model_validate_json(
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
                    "protected submission procedure returned an invalid receipt"
                ) from exc
            identity_mismatch = (
                receipt.trial_id != submission.trial_id
                or receipt.protected_attempt_id != submission.protected_attempt_id
            )
            if (
                receipt.requirements_digest != submission.requirements_digest
                or (identity_mismatch and not receipt.replayed)
                or (identity_mismatch and submission.idempotency_key is None)
            ):
                raise CapacityTrialSubmissionError(
                    "protected submission receipt differs from its request"
                )
        return receipt

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
