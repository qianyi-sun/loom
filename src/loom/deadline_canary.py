"""Secret-free contracts shared by the isolated fixture and trusted reader."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class CanaryBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    case: Literal["A", "B"]
    team_id: UUID
    provider_connection_id: UUID


class ReceiptApproval(BaseModel):
    """Operator-verified projection, not metadata supplied by a provider caller."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    receipt_id: UUID
    team_id: UUID
    trial_id: UUID
    provider_connection_id: UUID
    step_id: str = Field(min_length=1, max_length=256)
    agent_attempt_id: UUID | None = None
    service_execution_lease_id: UUID | None = None
    service_execution_generation: int | None = Field(default=None, gt=0)
    step_jwt_id: UUID
    deadline: AwareDatetime
    previous_attempt_stopped: bool = False

    @model_validator(mode="after")
    def _one_attempt_authority(self) -> "ReceiptApproval":
        native = self.service_execution_lease_id is not None
        if native != (self.service_execution_generation is not None):
            raise ValueError("native receipt requires lease and generation")
        if native == (self.agent_attempt_id is not None):
            raise ValueError("receipt requires exactly one attempt authority")
        return self

    @property
    def attempt_identity(self) -> tuple[UUID | None, UUID | None, int | None]:
        return (
            self.agent_attempt_id,
            self.service_execution_lease_id,
            self.service_execution_generation,
        )
