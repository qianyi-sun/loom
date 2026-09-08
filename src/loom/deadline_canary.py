"""Secret-free contracts shared by the isolated fixture and trusted reader."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


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
    agent_attempt_id: UUID
    step_jwt_id: UUID
    deadline: AwareDatetime
    previous_attempt_stopped: bool = False
