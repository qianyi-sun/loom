"""Durable row-locked authority for native personal-dev build grants."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    PersonalDevCandidate,
    PersonalDevCandidateBuildAttempt,
    PersonalDevNativeBuilderAgent,
    PersonalDevNativeBuildGrant,
)
from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_PLATFORM,
    NATIVE_BUILDER_PROVIDER,
    NativeBuilderAgentStatus,
    NativeBuilderCompletion,
    NativeBuilderHeartbeatRequest,
    NativeBuilderPollRequest,
    NativeBuilderRuntimeEvidence,
)

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_IMAGE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}@sha256:[0-9a-f]{64}")
_KEY_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}")
_CONTENT_TYPE = "application/vnd.loom.personal-dev-build.v1+tar"


class NativeBuilderGrantFencedError(RuntimeError):
    """The agent, grant policy, request sequence, or parent lease was superseded."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _json_object(value: bytes) -> dict[str, object]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):  # pragma: no cover - protocol types emit objects
        raise ValueError("native builder canonical object is invalid")
    return parsed


def _validate_now(now: datetime) -> None:
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("native builder store time must include a timezone")


def _validate_digest(value: str, *, label: str) -> None:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"native builder {label} digest is invalid")


@dataclass(frozen=True, slots=True)
class NativeBuilderGrantPolicy:
    """Immutable management policy copied into every issued native grant."""

    agent_instance_id: UUID
    agent_key_id: str
    agent_image: str
    builder_image: str
    runtime_profile_sha256: str
    contract_json: str
    contract_sha256: str
    artifact_max_bytes: int
    active_deadline_seconds: int

    def __post_init__(self) -> None:
        if not isinstance(self.agent_instance_id, UUID) or self.agent_instance_id.int == 0:
            raise ValueError("native builder policy agent identity is invalid")
        if not isinstance(self.agent_key_id, str) or _KEY_ID_RE.fullmatch(self.agent_key_id) is None:
            raise ValueError("native builder policy agent key is invalid")
        for label, value in (
            ("agent", self.agent_image),
            ("builder", self.builder_image),
        ):
            if not isinstance(value, str) or _IMAGE_RE.fullmatch(value) is None:
                raise ValueError(f"native builder policy {label} image is invalid")
        _validate_digest(self.runtime_profile_sha256, label="runtime profile")
        _validate_digest(self.contract_sha256, label="contract")
        if (
            not isinstance(self.contract_json, str)
            or not 2 <= len(self.contract_json.encode("utf-8")) <= 64 * 1024
            or hashlib.sha256(self.contract_json.encode("ascii")).hexdigest()
            != self.contract_sha256
        ):
            raise ValueError("native builder policy contract is invalid")
        try:
            contract = json.loads(self.contract_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            raise ValueError("native builder policy contract is invalid") from None
        if not isinstance(contract, dict) or _canonical_bytes(contract).decode("ascii") != (
            self.contract_json
        ):
            raise ValueError("native builder policy contract is not canonical")
        if (
            type(self.artifact_max_bytes) is not int
            or not 1 <= self.artifact_max_bytes <= 16 * 1024 * 1024 * 1024
        ):
            raise ValueError("native builder policy artifact limit is invalid")
        if (
            type(self.active_deadline_seconds) is not int
            or not 300 <= self.active_deadline_seconds <= 7200
        ):
            raise ValueError("native builder policy active deadline is invalid")


@dataclass(frozen=True, slots=True)
class NativeBuilderArtifactHead:
    """Management-observed object identity used to admit a successful completion."""

    bucket: str
    object_key: str
    content_type: str
    size_bytes: int
    metadata: Mapping[str, str]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.bucket, str)
            or not self.bucket
            or self.bucket.strip() != self.bucket
            or "/" in self.bucket
            or not isinstance(self.object_key, str)
            or not self.object_key
            or len(self.object_key) > 2048
            or any(character in self.object_key for character in "\r\n\0")
        ):
            raise ValueError("native builder artifact head object binding is invalid")
        if self.content_type != _CONTENT_TYPE:
            raise ValueError("native builder artifact head content type is invalid")
        if type(self.size_bytes) is not int or self.size_bytes <= 0:
            raise ValueError("native builder artifact head size is invalid")
        values = dict(self.metadata)
        if (
            not 1 <= len(values) <= 32
            or any(
                not isinstance(key, str)
                or not isinstance(value, str)
                or not key
                or len(key) > 128
                or len(value) > 2048
                or any(character in key + value for character in "\r\n\0")
                for key, value in values.items()
            )
        ):
            raise ValueError("native builder artifact head metadata is invalid")
        object.__setattr__(self, "metadata", MappingProxyType(values))

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "bucket": self.bucket,
                "content_type": self.content_type,
                "metadata": dict(self.metadata),
                "object_key": self.object_key,
                "size_bytes": self.size_bytes,
            }
        )


@dataclass(frozen=True, slots=True)
class NativeBuilderPollResult:
    grant: PersonalDevNativeBuildGrant | None
    cancel_grant_ids: tuple[UUID, ...]


def _agent_lock_keys(key_id: str) -> tuple[int, int]:
    digest = hashlib.sha256(b"personal-dev-native-builder-agent-v1\0" + key_id.encode()).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )


def _artifact_key(registration: CandidateRegistration) -> str:
    candidate = registration.candidate
    attempt = registration.build_attempt
    if attempt is None or attempt.candidate_id != candidate.id or attempt.lease_epoch <= 0:
        raise ValueError("native builder registration attempt is invalid")
    return (
        f"personal-dev/builds/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/{attempt.id}/l{attempt.lease_epoch:016x}/"
        "arm64/artifacts.tar"
    )


def _parent_is_current(
    parent: PersonalDevCandidateBuildAttempt,
    *,
    lease_epoch: int,
    now: datetime,
) -> bool:
    return (
        parent.state == "running"
        and parent.lease_epoch == lease_epoch
        and parent.lease_expires_at is not None
        and parent.lease_expires_at > now
    )


def _grant_matches_status(
    grant: PersonalDevNativeBuildGrant,
    status: NativeBuilderAgentStatus,
) -> bool:
    return (
        grant.required_agent_instance_id == status.agent_instance_id
        and grant.required_agent_key_id == status.agent_key_id
        and grant.agent_image == status.agent_image
        and grant.builder_image == status.builder_image
        and grant.runtime_profile_sha256 == status.runtime_profile_sha256
        and grant.provider == status.provider
        and grant.platform == status.platform
    )


def _same_policy(
    grant: PersonalDevNativeBuildGrant,
    *,
    candidate_id: UUID,
    source_bucket: str,
    source_object_key: str,
    artifact_object_key: str,
    policy: NativeBuilderGrantPolicy,
) -> bool:
    return (
        grant.candidate_id == candidate_id
        and grant.provider == NATIVE_BUILDER_PROVIDER
        and grant.required_agent_instance_id == policy.agent_instance_id
        and grant.required_agent_key_id == policy.agent_key_id
        and grant.agent_image == policy.agent_image
        and grant.builder_image == policy.builder_image
        and grant.runtime_profile_sha256 == policy.runtime_profile_sha256
        and grant.contract_json == policy.contract_json
        and grant.contract_sha256 == policy.contract_sha256
        and grant.source_bucket == source_bucket
        and grant.source_object_key == source_object_key
        and grant.artifact_bucket == source_bucket
        and grant.artifact_object_key == artifact_object_key
        and grant.artifact_max_bytes == policy.artifact_max_bytes
        and grant.active_deadline_seconds == policy.active_deadline_seconds
    )


async def _lock_parent(
    session: AsyncSession,
    attempt_id: UUID,
) -> PersonalDevCandidateBuildAttempt | None:
    return (
        await session.execute(
            select(PersonalDevCandidateBuildAttempt)
            .where(PersonalDevCandidateBuildAttempt.id == attempt_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _lock_grant(
    session: AsyncSession,
    grant_id: UUID,
) -> PersonalDevNativeBuildGrant | None:
    return (
        await session.execute(
            select(PersonalDevNativeBuildGrant)
            .where(PersonalDevNativeBuildGrant.id == grant_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def issue_native_build_grant(
    session: AsyncSession,
    registration: CandidateRegistration,
    policy: NativeBuilderGrantPolicy,
    now: datetime,
) -> PersonalDevNativeBuildGrant:
    """Create or return one immutable grant under the current parent lease."""
    _validate_now(now)
    attempt = registration.build_attempt
    if attempt is None:
        raise ValueError("native builder registration has no build attempt")
    parent = await _lock_parent(session, attempt.id)
    if (
        parent is None
        or parent.candidate_id != registration.candidate.id
        or not _parent_is_current(parent, lease_epoch=attempt.lease_epoch, now=now)
    ):
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder parent attempt is not current")
    candidate = (
        await session.execute(
            select(PersonalDevCandidate)
            .where(PersonalDevCandidate.id == registration.candidate.id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if (
        candidate is None
        or candidate.status != "building"
        or candidate.owner_user_id != registration.candidate.owner_user_id
        or candidate.owner_team_id != registration.candidate.owner_team_id
        or candidate.object_bucket != registration.candidate.object_bucket
        or candidate.object_key != registration.candidate.object_key
        or candidate.candidate_sha != registration.candidate.candidate_sha
    ):
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder candidate binding is not current")
    agent = (
        await session.execute(
            select(PersonalDevNativeBuilderAgent)
            .where(PersonalDevNativeBuilderAgent.instance_id == policy.agent_instance_id)
        )
    ).scalar_one_or_none()
    if (
        agent is None
        or agent.key_id != policy.agent_key_id
        or agent.agent_image != policy.agent_image
        or agent.builder_image != policy.builder_image
        or agent.runtime_profile_sha256 != policy.runtime_profile_sha256
    ):
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder policy agent is not current")
    existing = (
        await session.execute(
            select(PersonalDevNativeBuildGrant)
            .where(
                PersonalDevNativeBuildGrant.attempt_id == attempt.id,
                PersonalDevNativeBuildGrant.attempt_lease_epoch == attempt.lease_epoch,
                PersonalDevNativeBuildGrant.platform == NATIVE_BUILDER_PLATFORM,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    artifact_object_key = _artifact_key(registration)
    if existing is not None:
        if not _same_policy(
            existing,
            candidate_id=candidate.id,
            source_bucket=candidate.object_bucket,
            source_object_key=candidate.object_key,
            artifact_object_key=artifact_object_key,
            policy=policy,
        ):
            await session.rollback()
            raise NativeBuilderGrantFencedError("native builder grant policy changed")
        await session.commit()
        return existing
    grant = PersonalDevNativeBuildGrant(
        id=uuid4(),
        candidate_id=candidate.id,
        attempt_id=attempt.id,
        attempt_lease_epoch=attempt.lease_epoch,
        platform=NATIVE_BUILDER_PLATFORM,
        provider=NATIVE_BUILDER_PROVIDER,
        required_agent_instance_id=policy.agent_instance_id,
        required_agent_key_id=policy.agent_key_id,
        agent_image=policy.agent_image,
        builder_image=policy.builder_image,
        runtime_profile_sha256=policy.runtime_profile_sha256,
        contract_json=policy.contract_json,
        contract_sha256=policy.contract_sha256,
        source_bucket=candidate.object_bucket,
        source_object_key=candidate.object_key,
        artifact_bucket=candidate.object_bucket,
        artifact_object_key=artifact_object_key,
        artifact_max_bytes=policy.artifact_max_bytes,
        active_deadline_seconds=policy.active_deadline_seconds,
        state="queued",
        queued_at=now,
        updated_at=now,
    )
    session.add(grant)
    await session.commit()
    return grant


def _check_poll_sequence(
    agent: PersonalDevNativeBuilderAgent,
    request: NativeBuilderPollRequest,
) -> None:
    if request.request_nonce == agent.last_poll_nonce:
        raise NativeBuilderGrantFencedError("native builder poll replay was rejected")
    if request.requested_at <= agent.last_poll_requested_at:
        raise NativeBuilderGrantFencedError(
            "native builder poll timestamp is not strictly monotonic"
        )


def _apply_status(
    agent: PersonalDevNativeBuilderAgent,
    request: NativeBuilderPollRequest,
    now: datetime,
) -> None:
    status = request.status
    status_json = _json_object(status.canonical_bytes())
    agent.provider = status.provider
    agent.platform = status.platform
    agent.protocol_version = status.protocol_version
    agent.host_name = status.host_name
    agent.host_architecture = status.host_architecture
    agent.host_boot_id = status.host_boot_id
    agent.agent_image = status.agent_image
    agent.builder_image = status.builder_image
    agent.runtime_profile_sha256 = status.runtime_profile_sha256
    agent.max_concurrency = status.max_concurrency
    agent.managed_grant_ids_json = [str(value) for value in status.managed_grant_ids]
    agent.active_grant_ids_json = [str(value) for value in status.active_grant_ids]
    agent.available = status.available
    agent.unavailable_reason = status.unavailable_reason
    agent.readiness_evidence_sha256 = status.readiness_evidence_sha256
    agent.status_json = status_json
    agent.status_sha256 = hashlib.sha256(status.canonical_bytes()).hexdigest()
    agent.last_poll_requested_at = request.requested_at
    agent.last_poll_nonce = request.request_nonce
    agent.last_seen_at = now
    agent.updated_at = now


async def poll_native_build_grant(
    session: AsyncSession,
    request: NativeBuilderPollRequest,
    now: datetime,
) -> NativeBuilderPollResult:
    """Persist signed readiness, reconcile inventory, and return at most one grant."""
    _validate_now(now)
    status = request.status
    lock_a, lock_b = _agent_lock_keys(status.agent_key_id)
    await session.execute(select(func.pg_advisory_xact_lock(lock_a, lock_b)))
    identity_rows = (
        await session.execute(
            select(PersonalDevNativeBuilderAgent)
            .where(
                (PersonalDevNativeBuilderAgent.instance_id == status.agent_instance_id)
                | (PersonalDevNativeBuilderAgent.key_id == status.agent_key_id)
            )
            .with_for_update()
        )
    ).scalars().all()
    if any(
        row.instance_id != status.agent_instance_id or row.key_id != status.agent_key_id
        for row in identity_rows
    ):
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder agent identity changed")
    if identity_rows:
        agent = identity_rows[0]
        try:
            _check_poll_sequence(agent, request)
        except NativeBuilderGrantFencedError:
            await session.rollback()
            raise
        _apply_status(agent, request, now)
    else:
        status_json = _json_object(status.canonical_bytes())
        agent = PersonalDevNativeBuilderAgent(
            instance_id=status.agent_instance_id,
            key_id=status.agent_key_id,
            provider=status.provider,
            platform=status.platform,
            protocol_version=status.protocol_version,
            host_name=status.host_name,
            host_architecture=status.host_architecture,
            host_boot_id=status.host_boot_id,
            agent_image=status.agent_image,
            builder_image=status.builder_image,
            runtime_profile_sha256=status.runtime_profile_sha256,
            max_concurrency=status.max_concurrency,
            managed_grant_ids_json=[str(value) for value in status.managed_grant_ids],
            active_grant_ids_json=[str(value) for value in status.active_grant_ids],
            available=status.available,
            unavailable_reason=status.unavailable_reason,
            readiness_evidence_sha256=status.readiness_evidence_sha256,
            status_json=status_json,
            status_sha256=hashlib.sha256(status.canonical_bytes()).hexdigest(),
            last_poll_requested_at=request.requested_at,
            last_poll_nonce=request.request_nonce,
            first_seen_at=now,
            last_seen_at=now,
            updated_at=now,
        )
        session.add(agent)
        try:
            await session.flush()
        except IntegrityError:
            await session.rollback()
            raise NativeBuilderGrantFencedError("native builder agent identity changed") from None

    managed_ids = tuple(status.managed_grant_ids)
    reported_rows: dict[UUID, PersonalDevNativeBuildGrant] = {}
    if managed_ids:
        rows = (
            await session.execute(
                select(PersonalDevNativeBuildGrant).where(
                    PersonalDevNativeBuildGrant.id.in_(managed_ids)
                )
            )
        ).scalars().all()
        reported_rows = {row.id: row for row in rows}
    parent_ids = {
        row.attempt_id
        for row in reported_rows.values()
        if row.state == "running"
    }
    parents: dict[UUID, PersonalDevCandidateBuildAttempt] = {}
    if parent_ids:
        parents = {
            row.id: row
            for row in (
                await session.execute(
                    select(PersonalDevCandidateBuildAttempt).where(
                        PersonalDevCandidateBuildAttempt.id.in_(parent_ids)
                    )
                )
            ).scalars()
        }
    cancellations: list[UUID] = []
    for grant_id in managed_ids:
        row = reported_rows.get(grant_id)
        parent = parents.get(row.attempt_id) if row is not None else None
        if (
            row is None
            or row.state != "running"
            or row.running_agent_instance_id != status.agent_instance_id
            or not _grant_matches_status(row, status)
            or parent is None
            or not _parent_is_current(
                parent,
                lease_epoch=row.attempt_lease_epoch,
                now=now,
            )
        ):
            cancellations.append(grant_id)

    grant_to_return: PersonalDevNativeBuildGrant | None = None
    running_rows: Sequence[PersonalDevNativeBuildGrant] = ()
    reported_running_to_redeliver: list[PersonalDevNativeBuildGrant] = []
    if status.available:
        running_rows = (
            await session.execute(
                select(PersonalDevNativeBuildGrant)
                .where(
                    PersonalDevNativeBuildGrant.running_agent_instance_id
                    == status.agent_instance_id,
                    PersonalDevNativeBuildGrant.state == "running",
                )
                .order_by(
                    PersonalDevNativeBuildGrant.heartbeat_at,
                    PersonalDevNativeBuildGrant.started_at,
                    PersonalDevNativeBuildGrant.id,
                )
            )
        ).scalars().all()
        for row in running_rows:
            if not _grant_matches_status(row, status):
                continue
            parent = await session.get(PersonalDevCandidateBuildAttempt, row.attempt_id)
            if parent is not None and _parent_is_current(
                parent,
                lease_epoch=row.attempt_lease_epoch,
                now=now,
            ):
                if row.id in status.active_grant_ids:
                    reported_running_to_redeliver.append(row)
                else:
                    grant_to_return = row
                    break

    occupied_grant_ids = set(status.active_grant_ids)
    occupied_grant_ids.update(row.id for row in running_rows)
    if grant_to_return is None and status.available and (
        len(occupied_grant_ids) < status.max_concurrency
    ):
        queued_rows = (
            await session.execute(
                select(PersonalDevNativeBuildGrant)
                .where(
                    PersonalDevNativeBuildGrant.required_agent_instance_id
                    == status.agent_instance_id,
                    PersonalDevNativeBuildGrant.state == "queued",
                )
                .order_by(
                    PersonalDevNativeBuildGrant.queued_at,
                    PersonalDevNativeBuildGrant.id,
                )
                .limit(64)
            )
        ).scalars().all()
        for queued in queued_rows:
            if not _grant_matches_status(queued, status):
                continue
            parent = await _lock_parent(session, queued.attempt_id)
            if parent is None:
                continue
            locked = await _lock_grant(session, queued.id)
            if locked is None or locked.state != "queued":
                continue
            if not _parent_is_current(
                parent,
                lease_epoch=locked.attempt_lease_epoch,
                now=now,
            ):
                locked.state = "cancelled"
                locked.failure_reason = "coordinator_lease_lost"
                locked.finished_at = now
                locked.updated_at = now
                continue
            locked.state = "running"
            locked.running_agent_instance_id = status.agent_instance_id
            locked.started_at = now
            locked.heartbeat_at = now
            locked.updated_at = now
            grant_to_return = locked
            break

    if grant_to_return is None and reported_running_to_redeliver:
        grant_to_return = reported_running_to_redeliver[0]

    await session.commit()
    return NativeBuilderPollResult(
        grant=grant_to_return,
        cancel_grant_ids=tuple(sorted(cancellations, key=str)),
    )


def _validate_grant_request(
    grant: PersonalDevNativeBuildGrant,
    *,
    agent_instance_id: UUID,
    agent_key_id: str,
    attempt_id: UUID,
    attempt_lease_epoch: int,
) -> None:
    if (
        grant.attempt_id != attempt_id
        or grant.attempt_lease_epoch != attempt_lease_epoch
        or grant.required_agent_instance_id != agent_instance_id
        or grant.required_agent_key_id != agent_key_id
        or grant.running_agent_instance_id != agent_instance_id
    ):
        raise NativeBuilderGrantFencedError("native builder grant identity is fenced")


def _validate_request_sequence(
    grant: PersonalDevNativeBuildGrant,
    *,
    requested_at: datetime,
    request_nonce: UUID,
) -> None:
    if grant.last_request_nonce == request_nonce:
        raise NativeBuilderGrantFencedError("native builder grant request replay was rejected")
    if grant.last_request_at is not None and requested_at <= grant.last_request_at:
        raise NativeBuilderGrantFencedError(
            "native builder grant request timestamp is not strictly monotonic"
        )


async def heartbeat_native_build_grant(
    session: AsyncSession,
    request: NativeBuilderHeartbeatRequest,
    now: datetime,
) -> bool:
    """Refresh one running grant only while the parent whole-attempt lease is current."""
    _validate_now(now)
    parent = await _lock_parent(session, request.attempt_id)
    grant = await _lock_grant(session, request.grant_id)
    if parent is None or grant is None:
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder grant is unavailable")
    try:
        _validate_grant_request(
            grant,
            agent_instance_id=request.agent_instance_id,
            agent_key_id=request.agent_key_id,
            attempt_id=request.attempt_id,
            attempt_lease_epoch=request.attempt_lease_epoch,
        )
        _validate_request_sequence(
            grant,
            requested_at=request.requested_at,
            request_nonce=request.request_nonce,
        )
    except NativeBuilderGrantFencedError:
        await session.rollback()
        raise
    grant.last_request_at = request.requested_at
    grant.last_request_nonce = request.request_nonce
    grant.updated_at = now
    if grant.state != "running":
        await session.commit()
        return False
    if not _parent_is_current(
        parent,
        lease_epoch=request.attempt_lease_epoch,
        now=now,
    ):
        grant.state = "cancelled"
        grant.failure_reason = "coordinator_lease_lost"
        grant.finished_at = now
        await session.commit()
        return False
    grant.heartbeat_at = now
    await session.commit()
    return True


def _stable_completion(value: Mapping[str, object]) -> dict[str, object]:
    result = dict(value)
    result.pop("request_nonce", None)
    result.pop("requested_at", None)
    return result


def _artifact_head_matches(
    head: NativeBuilderArtifactHead,
    grant: PersonalDevNativeBuildGrant,
    candidate: PersonalDevCandidate,
) -> bool:
    expected_metadata = {
        "attestation-scope": "personal-dev-only",
        "build-attempt-id": str(grant.attempt_id),
        "build-lease-epoch": str(grant.attempt_lease_epoch),
        "candidate-sha256": candidate.candidate_sha,
        "platform": grant.platform,
    }
    return (
        head.bucket == grant.artifact_bucket
        and head.object_key == grant.artifact_object_key
        and head.content_type == _CONTENT_TYPE
        and 0 < head.size_bytes <= grant.artifact_max_bytes
        and dict(head.metadata) == expected_metadata
    )


def _runtime_evidence_matches(
    evidence: NativeBuilderRuntimeEvidence,
    grant: PersonalDevNativeBuildGrant,
) -> bool:
    return (
        evidence.agent_instance_id == grant.required_agent_instance_id
        and evidence.agent_instance_id == grant.running_agent_instance_id
        and evidence.grant_id == grant.id
        and evidence.attempt_id == grant.attempt_id
        and evidence.attempt_lease_epoch == grant.attempt_lease_epoch
        and evidence.provider == grant.provider
        and evidence.platform == grant.platform
        and evidence.agent_image == grant.agent_image
        and evidence.builder_image == grant.builder_image
        and evidence.runtime_profile_sha256 == grant.runtime_profile_sha256
        and evidence.contract_sha256 == grant.contract_sha256
    )


async def complete_native_build_grant(
    session: AsyncSession,
    completion: NativeBuilderCompletion,
    now: datetime,
    artifact_head: NativeBuilderArtifactHead | None,
) -> PersonalDevNativeBuildGrant:
    """Commit one signed terminal outcome after parent and artifact fencing."""
    _validate_now(now)
    parent = await _lock_parent(session, completion.attempt_id)
    grant = await _lock_grant(session, completion.grant_id)
    if parent is None or grant is None:
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder grant is unavailable")
    try:
        _validate_grant_request(
            grant,
            agent_instance_id=completion.agent_instance_id,
            agent_key_id=completion.agent_key_id,
            attempt_id=completion.attempt_id,
            attempt_lease_epoch=completion.attempt_lease_epoch,
        )
        _validate_request_sequence(
            grant,
            requested_at=completion.requested_at,
            request_nonce=completion.request_nonce,
        )
    except NativeBuilderGrantFencedError:
        await session.rollback()
        raise
    completion_json = _json_object(completion.canonical_bytes())
    completion_sha256 = hashlib.sha256(completion.canonical_bytes()).hexdigest()

    if grant.state in {"succeeded", "failed"}:
        expected_state = "succeeded" if completion.outcome == "succeeded" else "failed"
        head_sha256 = (
            hashlib.sha256(artifact_head.canonical_bytes()).hexdigest()
            if artifact_head is not None
            else None
        )
        if (
            grant.state != expected_state
            or grant.completion_json is None
            or _stable_completion(grant.completion_json)
            != _stable_completion(completion_json)
            or grant.artifact_head_sha256 != head_sha256
        ):
            await session.rollback()
            raise NativeBuilderGrantFencedError("native builder completion changed")
        grant.last_request_at = completion.requested_at
        grant.last_request_nonce = completion.request_nonce
        grant.updated_at = now
        await session.commit()
        return grant
    if grant.state != "running":
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder grant is not running")
    if not _parent_is_current(
        parent,
        lease_epoch=completion.attempt_lease_epoch,
        now=now,
    ):
        grant.state = "cancelled"
        grant.failure_reason = "coordinator_lease_lost"
        grant.last_request_at = completion.requested_at
        grant.last_request_nonce = completion.request_nonce
        grant.finished_at = now
        grant.updated_at = now
        await session.commit()
        raise NativeBuilderGrantFencedError("native builder parent attempt is not current")

    candidate = (
        await session.execute(
            select(PersonalDevCandidate)
            .where(PersonalDevCandidate.id == grant.candidate_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if candidate is None:
        await session.rollback()
        raise NativeBuilderGrantFencedError("native builder candidate is unavailable")
    if completion.outcome == "succeeded":
        if artifact_head is None or not _artifact_head_matches(artifact_head, grant, candidate):
            await session.rollback()
            raise NativeBuilderGrantFencedError("native builder artifact head is invalid")
        evidence = completion.evidence
        assert evidence is not None  # protocol model enforces success evidence
        if not _runtime_evidence_matches(evidence, grant):
            await session.rollback()
            raise NativeBuilderGrantFencedError("native builder runtime evidence is invalid")
        evidence_json = _json_object(evidence.canonical_bytes())
        head_json = _json_object(artifact_head.canonical_bytes())
        grant.state = "succeeded"
        grant.runtime_evidence_json = evidence_json
        grant.runtime_evidence_sha256 = hashlib.sha256(evidence.canonical_bytes()).hexdigest()
        grant.artifact_head_json = head_json
        grant.artifact_head_sha256 = hashlib.sha256(artifact_head.canonical_bytes()).hexdigest()
    else:
        if artifact_head is not None:
            await session.rollback()
            raise NativeBuilderGrantFencedError(
                "native builder failed completion supplied an artifact"
            )
        grant.state = "failed"
        grant.failure_reason = completion.failure_reason
    grant.completion_json = completion_json
    grant.completion_sha256 = completion_sha256
    grant.last_request_at = completion.requested_at
    grant.last_request_nonce = completion.request_nonce
    grant.finished_at = now
    grant.updated_at = now
    await session.commit()
    return grant


async def cancel_native_build_grant(
    session: AsyncSession,
    attempt_id: UUID,
    attempt_lease_epoch: int,
    platform: PersonalDevPlatform,
    now: datetime,
) -> bool:
    """Idempotently cancel only one exact whole-attempt/platform grant."""
    _validate_now(now)
    if platform != NATIVE_BUILDER_PLATFORM or attempt_lease_epoch <= 0:
        raise ValueError("native builder cancellation binding is invalid")
    parent = await _lock_parent(session, attempt_id)
    grant = (
        await session.execute(
            select(PersonalDevNativeBuildGrant)
            .where(
                PersonalDevNativeBuildGrant.attempt_id == attempt_id,
                PersonalDevNativeBuildGrant.attempt_lease_epoch == attempt_lease_epoch,
                PersonalDevNativeBuildGrant.platform == platform,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if parent is None or grant is None:
        await session.rollback()
        return False
    if grant.state in {"succeeded", "failed", "cancelled"}:
        await session.commit()
        return False
    grant.state = "cancelled"
    grant.failure_reason = "coordinator_cancelled"
    grant.finished_at = now
    grant.updated_at = now
    await session.commit()
    return True


async def get_native_build_grant(
    session: AsyncSession,
    attempt_id: UUID,
    attempt_lease_epoch: int,
    platform: PersonalDevPlatform,
) -> PersonalDevNativeBuildGrant | None:
    if platform != NATIVE_BUILDER_PLATFORM or attempt_lease_epoch <= 0:
        raise ValueError("native builder lookup binding is invalid")
    return (
        await session.execute(
            select(PersonalDevNativeBuildGrant).where(
                PersonalDevNativeBuildGrant.attempt_id == attempt_id,
                PersonalDevNativeBuildGrant.attempt_lease_epoch == attempt_lease_epoch,
                PersonalDevNativeBuildGrant.platform == platform,
            )
        )
    ).scalar_one_or_none()


__all__ = [
    "NativeBuilderArtifactHead",
    "NativeBuilderGrantFencedError",
    "NativeBuilderGrantPolicy",
    "NativeBuilderPollResult",
    "cancel_native_build_grant",
    "complete_native_build_grant",
    "get_native_build_grant",
    "heartbeat_native_build_grant",
    "issue_native_build_grant",
    "poll_native_build_grant",
]
