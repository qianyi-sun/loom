"""Closed aggregate request renderer for the generic Pipeline acceptance fixture."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom.pipeline.keys import canonical_digest, canonical_document


class CoreFixtureTransformRefV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    shard_key: Literal["item-000", "item-001"]
    artifact_id: str
    content_sha256: str
    manifest_sha256: str

    @field_validator("content_sha256", "manifest_sha256")
    @classmethod
    def digest_is_canonical(cls, value: str) -> str:
        if len(value) != 71 or not value.startswith("sha256:"):
            raise ValueError("fixture digest is invalid")
        int(value[7:], 16)
        return value


class CoreFixtureAggregateRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["loom.pipeline-core-aggregate-request.v1"]
    transforms: list[CoreFixtureTransformRefV1] = Field(min_length=2, max_length=2)

    @field_validator("transforms")
    @classmethod
    def transforms_are_exact(
        cls, values: list[CoreFixtureTransformRefV1]
    ) -> list[CoreFixtureTransformRefV1]:
        if [item.shard_key for item in values] != ["item-000", "item-001"]:
            raise ValueError("fixture transforms must preserve canonical shard order")
        if len({item.artifact_id for item in values}) != 2:
            raise ValueError("fixture transform artifacts must be distinct")
        return values


def render(value: CoreFixtureAggregateRequestV1 | dict[str, Any]) -> bytes:
    """Validate and render the exact aggregate request as RFC8785 JCS plus LF."""

    request = (
        value
        if isinstance(value, CoreFixtureAggregateRequestV1)
        else CoreFixtureAggregateRequestV1.model_validate(value)
    )
    return canonical_document(request)


def request_digest(value: CoreFixtureAggregateRequestV1 | dict[str, Any]) -> str:
    """Return the immutable digest used by acceptance evidence."""

    request = (
        value
        if isinstance(value, CoreFixtureAggregateRequestV1)
        else CoreFixtureAggregateRequestV1.model_validate(value)
    )
    return canonical_digest(request)


__all__ = [
    "CoreFixtureAggregateRequestV1",
    "CoreFixtureTransformRefV1",
    "render",
    "request_digest",
]
