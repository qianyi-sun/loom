"""Loom-owned validators for the BEHAVIOR whole-episode judge outputs.

The rules in this module are a bounded port of the historical ``agentic_sweep``
validator.  Runtime code never imports or executes that source tree.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from itertools import pairwise
from typing import Annotated, Any, Literal, TypeAlias, cast

from pydantic import Field, field_validator, model_validator

from loom.integrations.behavior.canonical_json import canonical_document
from loom.integrations.behavior.errors import BehaviorContractError
from loom.pipeline.keys import MAX_SAFE_INTEGER
from loom.pipeline.spec import PipelineModel

MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_SEED_BYTES = 16 * 1024 * 1024

SKILL_LABELS = frozenset(
    {
        "move to",
        "pick up from",
        "place in",
        "place on",
        "push to",
        "open door",
        "place on next to",
        "close door",
        "close lid",
        "open lid",
        "insert",
        "tip over",
        "turn on switch",
        "hand over",
        "turn to",
        "open drawer",
        "close drawer",
        "place in next to",
        "pour",
        "press",
        "ignite",
        "turn off switch",
    }
)

UInt = Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
FrameRange = Annotated[list[UInt], Field(min_length=2, max_length=2)]


def _text(value: str, *, label: str, limit: int = 4096) -> str:
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{label} must already be NFC")
    if not value or len(value.encode("utf-8")) > limit:
        raise ValueError(f"{label} is empty or exceeds {limit} UTF-8 bytes")
    return value


class LearnChunkV1(PipelineModel):
    span: FrameRange
    learn: FrameRange
    seed: None
    reason: str

    _reason = field_validator("reason")(
        lambda value: _text(value, label="chunk reason", limit=4096)
    )

    @model_validator(mode="after")
    def ranges_are_ordered(self) -> LearnChunkV1:
        if self.span[0] > self.span[1] or self.learn[0] > self.learn[1]:
            raise ValueError("chunk ranges must be inclusive and ordered")
        if not (self.span[0] <= self.learn[0] <= self.learn[1] <= self.span[1]):
            raise ValueError("learn range must lie inside its row span")
        if len(self.reason) < 30:
            raise ValueError("chunk reason must contain at least 30 characters")
        return self


class RecoverySeedChunkV1(PipelineModel):
    span: FrameRange
    learn: None
    seed: UInt
    reason: str
    skill_label: str
    object: str
    target: str
    arm: Literal["left", "right", "either"]

    @field_validator("reason", "object", "target")
    @classmethod
    def strings_are_closed(cls, value: str) -> str:
        return _text(value, label="seed chunk text", limit=4096)

    @field_validator("skill_label")
    @classmethod
    def label_is_registered(cls, value: str) -> str:
        value = _text(value, label="skill_label", limit=128)
        if value not in SKILL_LABELS:
            raise ValueError("skill_label is outside the closed BEHAVIOR vocabulary")
        return value

    @model_validator(mode="after")
    def seed_is_in_span(self) -> RecoverySeedChunkV1:
        if self.span[0] > self.span[1] or not self.span[0] <= self.seed <= self.span[1]:
            raise ValueError("seed must lie inside its inclusive row span")
        if len(self.reason) < 30:
            raise ValueError("chunk reason must contain at least 30 characters")
        return self


SeedChunk: TypeAlias = LearnChunkV1 | RecoverySeedChunkV1


class AgenticSweepSeedV1(PipelineModel):
    chunks: Annotated[list[SeedChunk], Field(min_length=1, max_length=100_000)]
    task_id: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    episode: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    n_steps: Annotated[int, Field(strict=True, ge=1, le=MAX_SAFE_INTEGER)]
    fps: Literal[30]
    rollout: str

    @field_validator("rollout")
    @classmethod
    def rollout_is_artifact_ref(cls, value: str) -> str:
        if re.fullmatch(
            r"artifact:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            value,
        ) is None:
            raise ValueError("rollout must be a canonical artifact UUID reference")
        return value


@dataclass(frozen=True)
class TimelineRow:
    first: int
    last: int
    primitive: str | None
    object_target: str | None
    arm: str | None
    verdict: Literal["success", "execution", "ordering", "no progress"]
    learn: tuple[tuple[int, int], ...]
    seed: int | None
    why: str


@dataclass(frozen=True)
class ValidatedSweepOutputs:
    report: bytes
    seed: bytes
    rows: tuple[TimelineRow, ...]
    document: AgenticSweepSeedV1

    @property
    def seed_count(self) -> int:
        return sum(isinstance(chunk, RecoverySeedChunkV1) for chunk in self.document.chunks)

    @property
    def learn_count(self) -> int:
        return sum(isinstance(chunk, LearnChunkV1) for chunk in self.document.chunks)


_ROW_RE = re.compile(
    r"^\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|"
    r"\s*([^|]+?)\s*\|\s*(success|execution|ordering|no progress)\s*\|"
    r"\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*$"
)
_HEADING_RE = re.compile(
    r"^#\s+(.+?)/(\d+)/(\d+)\s+n_steps=(\d+)\s+fps=30\s*$", re.MULTILINE
)
_RANGE_RE = re.compile(r"^(\d+)-(\d+)$")


def parse_timeline(report: bytes, *, n_steps: int) -> tuple[TimelineRow, ...]:
    if len(report) > MAX_REPORT_BYTES:
        raise BehaviorContractError("report.md exceeds 4 MiB")
    if b"\x00" in report or b"\r" in report:
        raise BehaviorContractError("report.md contains NUL or CR")
    try:
        text = report.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BehaviorContractError("report.md is not UTF-8") from exc
    if unicodedata.normalize("NFC", text) != text:
        raise BehaviorContractError("report.md must already be NFC")
    rows: list[TimelineRow] = []
    in_timeline = False
    for line in text.splitlines():
        if line.strip() == "## Timeline":
            in_timeline = True
            continue
        if in_timeline and line.startswith("## "):
            break
        if not in_timeline or not line.lstrip().startswith("|"):
            continue
        match = _ROW_RE.match(line)
        if match is None:
            if re.search(r"\|\s*first\s*\|", line) or re.match(r"^\|[-: |]+\|$", line):
                continue
            raise BehaviorContractError("Timeline row does not use the exact nine-column shape")
        first, last = int(match.group(1)), int(match.group(2))
        if first > last or last >= n_steps:
            raise BehaviorContractError("Timeline row is reversed or outside n_steps")
        primitive_raw, target_raw, arm_raw = match.group(3), match.group(4), match.group(5)
        verdict = cast(
            Literal["success", "execution", "ordering", "no progress"], match.group(6)
        )
        learn_raw, seed_raw, why = match.group(7), match.group(8), match.group(9).strip()
        if len(why) < 30:
            raise BehaviorContractError("Timeline why must contain at least 30 characters")
        primitive = None if primitive_raw == "—" else _timeline_skill(primitive_raw)
        target = None if target_raw == "—" else target_raw
        arm = None if arm_raw == "—" else arm_raw
        if verdict == "no progress":
            if (primitive, target, arm) != (None, None, None):
                raise BehaviorContractError("no progress row must use em dashes for action fields")
        elif primitive not in SKILL_LABELS or arm not in {"left", "right", "either"}:
            raise BehaviorContractError("Timeline action uses an unknown skill label or arm")
        learns: list[tuple[int, int]] = []
        if learn_raw != "—":
            for raw in (item.strip() for item in learn_raw.split(",")):
                range_match = _RANGE_RE.fullmatch(raw)
                if range_match is None:
                    raise BehaviorContractError("Timeline learn range is malformed")
                learns.append((int(range_match.group(1)), int(range_match.group(2))))
        seed = None if seed_raw == "—" else _strict_uint(seed_raw, "Timeline seed")
        if verdict == "success":
            if not 1 <= len(learns) <= 2 or seed is not None:
                raise BehaviorContractError("success row requires one/two learns and no seed")
            if any(not first <= lo <= hi <= last for lo, hi in learns):
                raise BehaviorContractError("Timeline learn lies outside its row")
            if len(learns) == 2 and learns[0][1] >= learns[1][0]:
                raise BehaviorContractError("split Timeline learns must be ordered and disjoint")
        elif learns or seed is None or not first <= seed <= last:
            raise BehaviorContractError("failure row requires one in-span seed and no learn")
        elif verdict in {"ordering", "no progress"} and seed != first:
            raise BehaviorContractError("ordering/no progress seed must equal row start")
        rows.append(
            TimelineRow(first, last, primitive, target, arm, verdict, tuple(learns), seed, why)
        )
    if not rows:
        raise BehaviorContractError("report.md has no Timeline rows")
    if rows[0].first != 0 or rows[-1].last != n_steps - 1:
        raise BehaviorContractError("Timeline must cover exactly 0..n_steps-1")
    if any(left.last + 1 != right.first for left, right in pairwise(rows)):
        raise BehaviorContractError("Timeline rows must be gap-free and nonoverlapping")
    for row in rows:
        if row.seed is not None and re.search(rf"(?<![0-9]){row.seed}(?![0-9])", row.why) is None:
            raise BehaviorContractError("each Timeline seed frame must be cited in its why text")
    first_ordering = next((row.first for row in rows if row.verdict == "ordering"), None)
    if first_ordering is not None and any(
        row.first > first_ordering and row.learn for row in rows
    ):
        raise BehaviorContractError("a learn row may not follow the first ordering failure")
    return tuple(rows)


def validate_sweep_outputs(
    report: bytes,
    raw_seed: bytes,
    *,
    task_name: str,
    engine_task_instance_id: int,
    task_id: int,
    demo_id: int,
    n_steps: int,
    rollout_artifact_id: str,
) -> ValidatedSweepOutputs:
    rows = parse_timeline(report, n_steps=n_steps)
    heading = _HEADING_RE.search(report.decode("utf-8"))
    if heading is None or (
        heading.group(1),
        int(heading.group(2)),
        int(heading.group(3)),
        int(heading.group(4)),
    ) != (task_name, engine_task_instance_id, demo_id, n_steps):
        raise BehaviorContractError("report heading identity disagrees with signed inputs")
    if len(raw_seed) > MAX_SEED_BYTES or b"\x00" in raw_seed or b"\r" in raw_seed:
        raise BehaviorContractError("seed.json exceeds 16 MiB or contains forbidden bytes")
    try:
        decoded = json.loads(
            raw_seed,
            parse_constant=_reject_constant,
            object_pairs_hook=_object_without_duplicates,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BehaviorContractError("seed.json is not strict JSON") from exc
    if not isinstance(decoded, dict):
        raise BehaviorContractError("seed.json must be an object")
    stamped = dict(decoded)
    expected_identity = {
        "task_id": task_id,
        "episode": demo_id,
        "n_steps": n_steps,
        "fps": 30,
        "rollout": f"artifact:{rollout_artifact_id}",
    }
    for key, expected in expected_identity.items():
        if key in stamped and stamped[key] != expected:
            raise BehaviorContractError(f"agent-supplied seed {key} identity drift")
        stamped[key] = expected
    try:
        document = AgenticSweepSeedV1.model_validate(stamped)
    except ValueError as exc:
        raise BehaviorContractError("seed.json violates the closed judgement schema") from exc
    _cross_check_chunks(rows, document)
    canonical_seed = canonical_document(document)
    return ValidatedSweepOutputs(report=report, seed=canonical_seed, rows=rows, document=document)


def _cross_check_chunks(rows: tuple[TimelineRow, ...], seed: AgenticSweepSeedV1) -> None:
    spans = [tuple(chunk.span) for chunk in seed.chunks]
    collapsed = [span for index, span in enumerate(spans) if index == 0 or span != spans[index - 1]]
    expected = [(row.first, row.last) for row in rows]
    if collapsed != expected:
        raise BehaviorContractError("seed chunk spans do not match Timeline rows")
    by_span = {(row.first, row.last): row for row in rows}
    chunks_by_span: dict[tuple[int, int], list[SeedChunk]] = {}
    for chunk in seed.chunks:
        chunk_span = (chunk.span[0], chunk.span[1])
        chunks_by_span.setdefault(chunk_span, []).append(chunk)
    for row in rows:
        chunks = chunks_by_span[(row.first, row.last)]
        if row.verdict == "success":
            learns = [tuple(chunk.learn) for chunk in chunks if isinstance(chunk, LearnChunkV1)]
            if learns != list(row.learn):
                raise BehaviorContractError(
                    "success Timeline learns and seed chunks are not an exact ordered match"
                )
        elif len(chunks) != 1 or not isinstance(chunks[0], RecoverySeedChunkV1):
            raise BehaviorContractError("failure Timeline row requires exactly one seed chunk")
    first_ordering = next((row.first for row in rows if row.verdict == "ordering"), None)
    for chunk in seed.chunks:
        row = by_span[(chunk.span[0], chunk.span[1])]
        if row.verdict == "success":
            if not isinstance(chunk, LearnChunkV1) or tuple(chunk.learn) not in row.learn:
                raise BehaviorContractError("success Timeline row and learn chunk disagree")
            if first_ordering is not None and row.first > first_ordering:
                raise BehaviorContractError("learn chunk follows ordering failure")
        else:
            if not isinstance(chunk, RecoverySeedChunkV1) or chunk.seed != row.seed:
                raise BehaviorContractError("failure Timeline row and seed chunk disagree")
            if chunk.skill_label != row.primitive and row.verdict != "no progress":
                raise BehaviorContractError("seed skill_label disagrees with Timeline primitive")
            if row.object_target is not None:
                parts = [item.strip() for item in row.object_target.split("->")]
                if len(parts) != 2 or (chunk.object, chunk.target) != (parts[0], parts[1]):
                    raise BehaviorContractError(
                        "seed object/target disagrees with Timeline action"
                    )
            if row.arm is not None and chunk.arm != row.arm:
                raise BehaviorContractError("seed arm disagrees with Timeline action")


def _strict_uint(value: str, label: str) -> int:
    if not value.isascii() or not value.isdigit() or (len(value) > 1 and value.startswith("0")):
        raise BehaviorContractError(f"{label} is not a canonical uint")
    parsed = int(value)
    if parsed > MAX_SAFE_INTEGER:
        raise BehaviorContractError(f"{label} exceeds the safe integer range")
    return parsed


def _timeline_skill(value: str) -> str:
    if not value.startswith("`") or not value.endswith("`") or value.count("`") != 2:
        raise BehaviorContractError("Timeline primitive must be one exact backtick label")
    return value[1:-1]


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key: {key}")
        value[key] = item
    return value


__all__ = [
    "MAX_REPORT_BYTES",
    "MAX_SEED_BYTES",
    "SKILL_LABELS",
    "AgenticSweepSeedV1",
    "LearnChunkV1",
    "RecoverySeedChunkV1",
    "TimelineRow",
    "ValidatedSweepOutputs",
    "parse_timeline",
    "validate_sweep_outputs",
]
