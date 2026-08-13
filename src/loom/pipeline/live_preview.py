"""Closed contracts and lifecycle helpers for ephemeral Stage 1 previews."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Annotated, Literal
from uuid import UUID

from PIL import Image, UnidentifiedImageError
from pydantic import Field, field_validator, model_validator

from loom.pipeline.spec import Digest, ExecutionSpecSnapshotV1, PipelineModel

PREVIEW_SCHEMA_VERSION = "loom.behavior-stage1-live-preview.v1"
PREVIEW_WIDTH = 672
PREVIEW_HEIGHT = 448
PREVIEW_MAX_FRAME_BYTES = 524_288
PREVIEW_MAX_FRAMES = 64
PREVIEW_MAX_BYTES = 32 * 1024 * 1024
PREVIEW_MIN_INTERVAL = timedelta(milliseconds=500)
PREVIEW_TTL = timedelta(seconds=300)
PREVIEW_TEAM_MAX_GENERATIONS = 128
PREVIEW_TEAM_MAX_BYTES = 64 * 1024 * 1024
PREVIEW_GLOBAL_MAX_GENERATIONS = 1_024
PREVIEW_GLOBAL_MAX_BYTES = 512 * 1024 * 1024


class LivePreviewContractError(ValueError):
    """A preview frame or record violates the closed non-authoritative contract."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class LivePreviewRecordV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-live-preview.v1"]
    sequence: Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)]
    step_idx: Annotated[int, Field(strict=True, ge=0, le=18_446_744_073_709_551_615)]
    jpeg_sha256: Digest
    jpeg_size_bytes: Annotated[int, Field(strict=True, ge=1, le=PREVIEW_MAX_FRAME_BYTES)]


class LivePreviewMetadataV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-live-preview.v1"]
    state: Literal["waiting", "live", "handoff", "ended"]
    attempt_id: UUID
    generation: UUID
    latest_sequence: Annotated[int, Field(strict=True, ge=0, le=9_007_199_254_740_991)] | None
    latest_step_idx: Annotated[int, Field(strict=True, ge=0, le=18_446_744_073_709_551_615)] | None
    received_at: datetime | None
    retry_after_ms: Literal[500]

    @field_validator("received_at")
    @classmethod
    def received_at_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("received_at must include a timezone")
        return value

    @model_validator(mode="after")
    def latest_group_is_exact(self) -> LivePreviewMetadataV1:
        values = (self.latest_sequence, self.latest_step_idx, self.received_at)
        if any(item is None for item in values) and not all(item is None for item in values):
            raise ValueError("latest preview fields must be present together")
        if self.state == "waiting" and any(item is not None for item in values):
            raise ValueError("waiting preview cannot expose a latest frame")
        return self


def is_stage1_live_preview_eligible(snapshot: object) -> bool:
    """Select preview UI from the frozen server-owned execution contract only."""

    try:
        parsed = ExecutionSpecSnapshotV1.model_validate(snapshot)
    except ValueError:
        return False
    node = parsed.container_node
    return (
        parsed.node_key == "rollout"
        and parsed.network_profile == "none"
        and len(node.outputs) == 1
        and node.outputs[0].name == "rollout"
        and node.outputs[0].artifact_type == "behavior_rollout_bundle.v1"
        and node.outputs[0].producer == "container"
        and node.outputs[0].required is True
    )


def preview_is_expired(*, expires_at: datetime, now: datetime | None = None) -> bool:
    observed = now or datetime.now(UTC)
    return expires_at <= observed


def validate_preview_jpeg(value: bytes) -> None:
    """Fully decode one metadata-free 672x448 baseline RGB JPEG."""

    if not 1 <= len(value) <= PREVIEW_MAX_FRAME_BYTES:
        raise LivePreviewContractError("preview_size_invalid")
    # SOF0 is baseline DCT. SOF2 is progressive and all other SOF variants are
    # outside this one-format protocol.
    non_baseline_sof_markers = tuple(
        bytes((0xFF, marker))
        for marker in (0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF)
    )
    if b"\xff\xc0" not in value or any(marker in value for marker in non_baseline_sof_markers):
        raise LivePreviewContractError("preview_jpeg_not_baseline")
    try:
        with Image.open(BytesIO(value)) as image:
            if image.format != "JPEG" or image.mode != "RGB":
                raise LivePreviewContractError("preview_jpeg_format_invalid")
            if image.size != (PREVIEW_WIDTH, PREVIEW_HEIGHT):
                raise LivePreviewContractError("preview_dimensions_invalid")
            if image.getexif() or any(
                key in image.info for key in ("exif", "icc_profile", "comment")
            ):
                raise LivePreviewContractError("preview_metadata_forbidden")
            image.load()
    except (OSError, UnidentifiedImageError) as exc:
        raise LivePreviewContractError("preview_jpeg_decode_failed") from exc


__all__ = [
    "PREVIEW_GLOBAL_MAX_BYTES",
    "PREVIEW_GLOBAL_MAX_GENERATIONS",
    "PREVIEW_HEIGHT",
    "PREVIEW_MAX_BYTES",
    "PREVIEW_MAX_FRAMES",
    "PREVIEW_MAX_FRAME_BYTES",
    "PREVIEW_MIN_INTERVAL",
    "PREVIEW_SCHEMA_VERSION",
    "PREVIEW_TEAM_MAX_BYTES",
    "PREVIEW_TEAM_MAX_GENERATIONS",
    "PREVIEW_TTL",
    "PREVIEW_WIDTH",
    "LivePreviewContractError",
    "LivePreviewMetadataV1",
    "LivePreviewRecordV1",
    "is_stage1_live_preview_eligible",
    "preview_is_expired",
    "validate_preview_jpeg",
]
