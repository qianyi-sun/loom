"""Serializable database-backed inspection for the disconnected claim guard."""

from __future__ import annotations

import json
from collections.abc import Mapping

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.claim_guard import ClaimGuardDecisionV1, ClaimProposalV1
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import canonical_bytes, canonical_digest

_SCHEMA = "loom_capacity_guard"
_BINDING_FIELDS = tuple(AgentRegistrationV1.model_fields)


class DatabaseClaimGuardError(RuntimeError):
    """The protected claim inspection did not return an exact denial."""


class DatabaseClaimGuard:
    """Inspect protected bindings without exposing claim mutation authority."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        registration: AgentRegistrationV1,
    ) -> None:
        if not isinstance(registration, AgentRegistrationV1):
            raise TypeError("database claim guard requires a trusted agent registration")
        self._session = session
        self._registration = registration

    async def evaluate(self, proposal: ClaimProposalV1) -> ClaimGuardDecisionV1:
        mismatches = tuple(
            field
            for field in _BINDING_FIELDS
            if getattr(proposal, field) != getattr(self._registration, field)
        )
        if mismatches:
            raise DatabaseClaimGuardError(
                f"claim proposal binding mismatch: {', '.join(mismatches)}"
            )
        payload_bytes = canonical_bytes(proposal)
        returned = (
            await self._session.execute(
                text(
                    f"SELECT {_SCHEMA}.inspect_inert_claim_proposal("
                    ":agent_incarnation, CAST(:payload AS jsonb), "
                    "CAST(:canonical_payload AS bytea), :payload_digest)"
                ),
                {
                    "agent_incarnation": self._registration.agent_incarnation,
                    "payload": payload_bytes.decode("ascii"),
                    "canonical_payload": payload_bytes,
                    "payload_digest": canonical_digest(proposal),
                },
            )
        ).scalar_one()
        if not isinstance(returned, Mapping):
            raise DatabaseClaimGuardError("protected claim inspection returned a non-object")
        try:
            decision = ClaimGuardDecisionV1.model_validate_json(
                json.dumps(
                    returned,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("ascii")
            )
        except (ValidationError, ValueError) as exc:
            raise DatabaseClaimGuardError(
                "protected claim inspection returned an invalid decision"
            ) from exc
        if (
            decision.proposal_id != proposal.proposal_id
            or decision.agent_incarnation != self._registration.agent_incarnation
            or decision.admitted is not False
            or decision.claim_id is not None
            or decision.concurrency_lease_id is not None
            or decision.executable is not False
        ):
            raise DatabaseClaimGuardError("protected claim inspection returned authority")
        return decision


__all__ = ["DatabaseClaimGuard", "DatabaseClaimGuardError"]
