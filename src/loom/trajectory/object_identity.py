"""Immutable object identities for trajectory attempt artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

TrajectoryObjectFilename = Literal["events.jsonl", "atif.json"]


def _path_segment(value: str | UUID, *, name: str) -> str:
    text = str(value)
    if (
        not text
        or text.strip() != text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
    ):
        raise ValueError(f"{name} must be one canonical path segment")
    return text


@dataclass(frozen=True, slots=True)
class TrajectoryObjectIdentity:
    """Canonical local and S3 identities for one trial attempt.

    ``attempt_count=None`` is the explicit compatibility mode for direct and
    historical callers. Service-worker claims always provide a positive
    attempt and therefore receive immutable, attempt-scoped identities.
    """

    bucket: str
    team_id: str | UUID
    trial_id: str | UUID
    attempt_count: int | None = None

    def __post_init__(self) -> None:
        if (
            not self.bucket
            or self.bucket.strip() != self.bucket
            or "/" in self.bucket
            or "\\" in self.bucket
        ):
            raise ValueError("bucket must be one canonical S3 bucket name")
        _path_segment(self.team_id, name="team_id")
        _path_segment(self.trial_id, name="trial_id")
        if self.attempt_count is not None and (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count <= 0
        ):
            raise ValueError("attempt_count must be a positive integer")

    @property
    def prefix(self) -> str:
        base = f"{self.team_id}/{self.trial_id}"
        if self.attempt_count is None:
            return base
        return f"{base}/attempts/{self.attempt_count}"

    @property
    def events_key(self) -> str:
        return f"{self.prefix}/events.jsonl"

    @property
    def atif_key(self) -> str:
        return f"{self.prefix}/atif.json"

    @property
    def events_uri(self) -> str:
        return f"s3://{self.bucket}/{self.events_key}"

    @property
    def atif_uri(self) -> str:
        return f"s3://{self.bucket}/{self.atif_key}"

    def local_path(self, root: Path) -> Path:
        if self.attempt_count is None:
            return root / f"{self.trial_id}.jsonl"
        return root / f"{self.trial_id}.attempt-{self.attempt_count}.events.jsonl"


def resolve_trajectory_object_key(
    *,
    uri: object | None,
    expected_bucket: str,
    team_id: str | UUID,
    trial_id: str | UUID,
    filename: TrajectoryObjectFilename,
) -> str:
    """Resolve a persisted trajectory URI without crossing its trust scope.

    Missing URIs and exact historical trial-only URIs resolve to the legacy
    key. A present attempt URI must use the expected bucket, team, trial,
    positive canonical attempt number, and requested filename. Everything
    else is rejected instead of silently falling back to another object.
    """

    legacy = TrajectoryObjectIdentity(
        bucket=expected_bucket,
        team_id=team_id,
        trial_id=trial_id,
    )
    legacy_key = legacy.events_key if filename == "events.jsonl" else legacy.atif_key
    if uri is None:
        return legacy_key
    if not isinstance(uri, str) or not uri:
        raise ValueError("trajectory object URI is not a non-empty string")

    parsed = urlsplit(uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != expected_bucket
        or not parsed.path.startswith("/")
        or parsed.path.startswith("//")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("trajectory object URI has an invalid scheme or bucket")
    key = parsed.path[1:]
    if uri != f"s3://{expected_bucket}/{key}":
        raise ValueError("trajectory object URI is not canonical")
    if key == legacy_key:
        return legacy_key

    parts = key.split("/")
    expected_team = _path_segment(team_id, name="team_id")
    expected_trial = _path_segment(trial_id, name="trial_id")
    if (
        len(parts) != 5
        or parts[0] != expected_team
        or parts[1] != expected_trial
        or parts[2] != "attempts"
        or parts[4] != filename
    ):
        raise ValueError("trajectory object URI is outside the expected trial identity")
    raw_attempt = parts[3]
    try:
        attempt_count = int(raw_attempt)
    except ValueError as exc:
        raise ValueError("trajectory object URI has a noncanonical attempt") from exc
    if attempt_count <= 0 or str(attempt_count) != raw_attempt:
        raise ValueError("trajectory object URI has a noncanonical attempt")
    return key
