"""Published native agent releases and the immutable binding frozen at submission."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.execution_image_admission import SignedImageAdmissionV1

AgentVersion = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]


class AgentRuntimeBindingV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    agent_name: Literal["terminus-2"] = "terminus-2"
    agent_version: AgentVersion
    runtime_contract: Literal["loom.terminus-controller.v1"]
    agent_image_ref: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    harbor_version: str = Field(min_length=1, max_length=128)
    harbor_source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    loom_bridge_revision: str = Field(min_length=1, max_length=128)
    publisher_source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")

    def public_metadata(self) -> dict[str, str]:
        return {
            "agent_version": self.agent_version,
            "harbor_version": self.harbor_version,
            "loom_bridge_revision": self.loom_bridge_revision,
        }


class AgentRuntimeReleaseV1(AgentRuntimeBindingV1):
    schema_version: Literal["loom.agent-runtime-release.v1"]
    image_admission: SignedImageAdmissionV1

    @model_validator(mode="after")
    def matching_image(self) -> AgentRuntimeReleaseV1:
        if self.image_admission.statement.image_ref != self.agent_image_ref:
            raise ValueError("agent release admission subject differs from its image")
        return self

    def binding(self) -> AgentRuntimeBindingV1:
        return AgentRuntimeBindingV1.model_validate(
            self.model_dump(exclude={"schema_version", "image_admission"})
        )
