"""Application-local browser authentication within a shared data environment.

The protected application binding supplies this identity; it is not a credential
or permission grant. Old processes must still be stopped/revoked by lifecycle
management when a generation is retired.
"""

from __future__ import annotations

from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


def canonical_session_origin(value: str) -> str:
    """Require a single HTTPS origin, never a request-derived URL or base path."""
    if value.strip() != value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("session audience requires a canonical HTTPS origin")
    url = HttpUrl(value)
    if (url.scheme != "https" or url.host is None or url.username is not None
            or url.password is not None or url.path not in {None, "", "/"}
            or url.query is not None or url.fragment is not None or url.port == 0):
        raise ValueError("session audience requires a canonical HTTPS origin")
    port = "" if url.port in {None, 443} else f":{url.port}"
    return f"https://{url.host}{port}"


class ApplicationSessionAudienceV1(BaseModel):
    """Stable app identity plus revocable access generation and public origin."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.application-session-audience.v1"] = (
        "loom.application-session-audience.v1"
    )
    application_id: UUID
    origin: str = Field(min_length=1, max_length=2048)
    access_generation: int = Field(ge=1, strict=True)

    _origin = field_validator("origin")(canonical_session_origin)

    @field_validator("application_id")
    @classmethod
    def _non_nil_id(cls: type[Self], value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("application identity must not be nil")
        return value
