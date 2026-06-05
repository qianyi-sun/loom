"""MCPConnection — typed channel for declaring MCP servers (spec §2.1 / §4.2)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


class MCPConnection(BaseModel):
    """Connection details for an MCP server the agent will use."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    transport: Literal["stdio", "sse", "websocket", "http"]
    command: list[str] | None = None
    url: str | None = None
    env: dict[str, str] = {}
    headers: dict[str, str] = {}

    @model_validator(mode="after")
    def _check_transport(self) -> MCPConnection:
        if self.transport == "stdio":
            if not self.command:
                raise ValueError("stdio transport requires `command`")
            if self.url is not None:
                raise ValueError("stdio transport must not set `url`")
        else:  # sse | websocket | http
            if not self.url:
                raise ValueError(f"{self.transport} transport requires `url`")
            if self.command is not None:
                raise ValueError(f"{self.transport} transport must not set `command`")
        return self
