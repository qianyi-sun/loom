"""Project management-owned platform requests into ordinary allocator demand.

This is not admission or a grant. The caller must read exact current owned lease
records and unassigned platform requests in its fenced management transaction.
Assignments and cleanup-unproven commitments belong in the same demand snapshot;
omitting a cancelled request here does not release its physical allocation.
Membership build intake remains disabled until that durable integration exists.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom_capacity_manager.contracts import MAX_DEMAND_BUCKETS_PER_REPORT, DemandBucketV1

_PLATFORM_PLACEMENT = {
    "linux/arm64": ("gb10", "cpu_arch.arm64"),
    "linux/amd64": ("oldlab", "cpu_arch.x86_64"),
}
_DIGEST = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class PersonalDevBuildDemandRequest:
    registration: CandidateRegistration
    platform: PersonalDevPlatform


def _uuid(value: UUID) -> str:
    if not isinstance(value, UUID) or value.int == 0:
        raise ValueError("personal build demand identity must be nonzero")
    return str(value)


def _time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("personal build demand time must be timezone-aware")
    return value.astimezone(UTC)


def _positive_epoch(value: int) -> int:
    if type(value) is not int or not 0 < value <= 2**63 - 1:
        raise ValueError("personal build demand epoch must be positive")
    return value


def _work_binding(registration: CandidateRegistration) -> bytes:
    candidate, attempt = registration.candidate, registration.build_attempt
    if attempt is None or attempt.candidate_id != candidate.id:
        raise ValueError("personal build work has no exact parent attempt")
    digests = (candidate.candidate_sha, candidate.source_sha256, candidate.archive_sha256,
               candidate.build_contract_sha256)
    if any(not isinstance(value, str) or not _DIGEST.fullmatch(value) or value == "0" * 64 for value in digests):
        raise ValueError("personal build demand source identity is invalid")
    return json.dumps([
        "personal-build-worker", _uuid(candidate.owner_user_id), _uuid(candidate.owner_team_id),
        _uuid(candidate.id), _uuid(candidate.source_generation_id), *digests,
        _uuid(attempt.id), _positive_epoch(attempt.lease_epoch),
        _uuid(attempt.subject_id), _uuid(attempt.subject_incarnation),
        _uuid(attempt.operation_id), _positive_epoch(attempt.operation_epoch),
    ], separators=(",", ":")).encode("ascii")


def personal_build_work_identity(registration: CandidateRegistration, platform: PersonalDevPlatform) -> tuple[str, UUID]:
    """Name immutable work even after expiry, for cancellation tombstones only.

    Identity alone does not authorize demand; the projector still requires a
    current owner-matching live lease and usable source.
    """
    if not isinstance(registration, CandidateRegistration) or not isinstance(platform, str) or platform not in _PLATFORM_PLACEMENT:
        raise ValueError("personal build work requires a native platform and source")
    work_id = "build-" + hashlib.sha256(_work_binding(registration) + b"\n" + platform.encode("ascii")).hexdigest()
    return work_id, uuid5(NAMESPACE_URL, f"loom:personal-build-work:{work_id}")


def project_personal_build_demand(
    *, owner_user_id: UUID, requests: tuple[PersonalDevBuildDemandRequest, ...], now: datetime,
) -> tuple[DemandBucketV1, ...]:
    """Use one cold slot per exact native-platform attempt, never user priorities."""
    _uuid(owner_user_id)
    now = _time(now)
    if not isinstance(requests, tuple) or len(requests) > MAX_DEMAND_BUCKETS_PER_REPORT:
        raise ValueError("personal build demand batch exceeds its bound")
    seen: set[tuple[UUID, str]] = set()
    whole_attempts: dict[UUID, bytes] = {}
    buckets = []
    for request in requests:
        if (
            not isinstance(request, PersonalDevBuildDemandRequest)
            or not isinstance(request.platform, str)
            or request.platform not in _PLATFORM_PLACEMENT
            or not isinstance(request.registration, CandidateRegistration)
        ):
            raise ValueError("personal build demand requires an exact native platform")
        candidate, attempt = request.registration.candidate, request.registration.build_attempt
        if (
            candidate.owner_user_id != owner_user_id
            or candidate.status != "building"
            or candidate.artifact_state != "retained"
            or attempt is None or attempt.candidate_id != candidate.id
            or attempt.state != "running" or attempt.finished_at is not None
            or not isinstance(attempt.claimed_by, str) or not attempt.claimed_by.strip()
            or attempt.lease_expires_at is None or _time(attempt.lease_expires_at) <= now
        ):
            raise ValueError("personal build demand owner or live lease is unavailable")
        submitted = _time(attempt.created_at)
        if submitted > now:
            raise ValueError("personal build demand submission is in the future")
        # Never replace the build service's trusted-runtime candidate with these
        # private source identities. They identify management platform work only.
        binding = _work_binding(request.registration)
        if attempt.id in whole_attempts and whole_attempts[attempt.id] != binding:
            raise ValueError("personal build demand whole-attempt records conflict")
        whole_attempts[attempt.id] = binding
        key = (attempt.id, request.platform)
        if key in seen:
            raise ValueError("personal build demand repeats a platform attempt")
        seen.add(key)
        work_id, attempt_id = personal_build_work_identity(request.registration, request.platform)
        pool, architecture = _PLATFORM_PLACEMENT[request.platform]
        buckets.append(DemandBucketV1(
            bucket_id=work_id, requested_slots=1, local_priority=0,
            oldest_submitted_at=submitted, eligible_pool_ids=(pool,),
            required_capabilities=(architecture, "personal-build-worker"), attempt_ids=(str(attempt_id),),
        ))
    return tuple(sorted(buckets, key=lambda item: item.bucket_id))
