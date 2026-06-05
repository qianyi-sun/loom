"""NetworkPolicy tagged union — three policies the runtime knows about.

Spec §4.2 (Supporting types).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class _BasePolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Public(_BasePolicy):
    kind: Literal["public"] = "public"


class NoNetwork(_BasePolicy):
    kind: Literal["no-network"] = "no-network"


class Allowlist(_BasePolicy):
    kind: Literal["allowlist"] = "allowlist"
    domains: tuple[str, ...] = Field(..., min_length=1)
    cidrs: tuple[str, ...] = ()


NetworkPolicy = Annotated[
    Public | NoNetwork | Allowlist,
    Field(discriminator="kind"),
]
