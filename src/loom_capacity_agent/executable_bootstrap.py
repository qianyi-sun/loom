"""Protected subject-side registration of executable bootstrap proposals."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import ProtectedExecutableBootstrapRegistrationV2
from loom_capacity_agent.contracts import ReporterConfigurationV1
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)

_SCHEMA = "loom_capacity_guard"


class ProtectedExecutableBootstrapError(RuntimeError):
    """A bootstrap proposal cannot be bound to protected local evidence."""


@dataclass(frozen=True, slots=True)
class ProtectedExecutableBootstrapWork:
    """Durable local evidence and the exact manager acknowledgement it authorizes."""

    registration: ProtectedExecutableBootstrapRegistrationV2
    acknowledgement: ExecutableBootstrapAcknowledgementV2
    idempotency_key: UUID


class ProtectedExecutableBootstrapCoordinator:
    """Commit executor-proposed hashes locally before acknowledging them remotely."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        configuration: ReporterConfigurationV1,
    ) -> None:
        if not isinstance(configuration, ReporterConfigurationV1):
            raise TypeError("protected bootstrap requires a trusted reporter configuration")
        self._session = session
        self._configuration = configuration

    def _assert_binding(self, proposal: ExecutableBootstrapProposalV2) -> str:
        if not isinstance(proposal, ExecutableBootstrapProposalV2):
            raise ProtectedExecutableBootstrapError("bootstrap proposal is not schema-v2")
        binding = proposal.binding
        mismatches = tuple(
            name
            for name, actual, expected in (
                ("subject_id", binding.subject_id, self._configuration.subject_id),
                (
                    "subject_incarnation",
                    binding.subject_incarnation,
                    self._configuration.subject_incarnation,
                ),
                (
                    "deployment_generation",
                    binding.deployment_generation,
                    self._configuration.deployment_generation,
                ),
            )
            if actual != expected
        )
        if binding.pool_id not in {
            capability.pool_id for capability in self._configuration.pool_capabilities
        }:
            mismatches += ("pool_id",)
        if mismatches:
            raise ProtectedExecutableBootstrapError(
                "bootstrap proposal binding differs from trusted configuration: "
                + ", ".join(mismatches)
            )
        protected_digest = self._configuration.protected_admission_sha256
        if protected_digest is None:
            raise ProtectedExecutableBootstrapError(
                "protected admission digest is absent from trusted configuration"
            )
        return protected_digest

    async def protect(
        self,
        proposal: ExecutableBootstrapProposalV2,
    ) -> ProtectedExecutableBootstrapWork:
        """Persist one exact proposal and derive a deterministic replayable acknowledgement."""

        protected_digest = self._assert_binding(proposal)
        proposal_bytes = canonical_executable_bytes(proposal)
        proposal_digest = canonical_executable_digest(proposal)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.protect_executable_bootstrap("
                        ":agent_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :proposal_digest, "
                        ":protected_admission_sha256)"
                    ),
                    {
                        "agent_incarnation": self._configuration.agent_incarnation,
                        "payload": proposal_bytes.decode("ascii"),
                        "canonical_payload": proposal_bytes,
                        "proposal_digest": proposal_digest,
                        "protected_admission_sha256": protected_digest,
                    },
                )
            ).scalar_one()
            if not isinstance(returned, Mapping):
                raise ProtectedExecutableBootstrapError(
                    "protected bootstrap procedure returned a non-object"
                )
            try:
                registration = ProtectedExecutableBootstrapRegistrationV2.model_validate_json(
                    json.dumps(
                        returned,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        allow_nan=False,
                    ).encode("ascii")
                )
            except (ValidationError, ValueError) as exc:
                raise ProtectedExecutableBootstrapError(
                    "protected bootstrap procedure returned an invalid receipt"
                ) from exc

        if (
            registration.subject_id != proposal.binding.subject_id
            or registration.subject_incarnation != proposal.binding.subject_incarnation
            or registration.intent_id != proposal.binding.intent_id
            or registration.proposal_epoch != proposal.proposal_epoch
            or registration.proposal_digest != proposal_digest
            or registration.bootstrap_sha256 != proposal.bootstrap_sha256
            or registration.protected_admission_sha256 != protected_digest
        ):
            raise ProtectedExecutableBootstrapError(
                "protected bootstrap receipt differs from its exact proposal"
            )
        evidence_digest = canonical_executable_digest(registration)
        acknowledgement = ExecutableBootstrapAcknowledgementV2(
            binding=proposal.binding,
            proposal_epoch=proposal.proposal_epoch,
            proposal_digest=proposal_digest,
            reporter_incarnation=self._configuration.reporter_incarnation,
            bootstrap_registration_epoch=registration.bootstrap_registration_epoch,
            bootstrap_evidence_sha256=evidence_digest,
            protected_admission_sha256=protected_digest,
        )
        idempotency_key = uuid5(
            NAMESPACE_URL,
            f"loom:protected-executable-bootstrap:{evidence_digest}",
        )
        return ProtectedExecutableBootstrapWork(
            registration=registration,
            acknowledgement=acknowledgement,
            idempotency_key=idempotency_key,
        )


__all__ = [
    "ProtectedExecutableBootstrapCoordinator",
    "ProtectedExecutableBootstrapError",
    "ProtectedExecutableBootstrapWork",
]
