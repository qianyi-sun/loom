"""Validate retained publication candidates against immutable row bindings."""

from __future__ import annotations

import hashlib
import hmac
import json

import rfc8785
from pydantic import ValidationError

from loom.db.schema import (
    TaskImagePublicationCandidate,
)
from loom_task_image_authority.http_contracts import (
    TaskImagePublicationCandidateResponseV1,
    TaskImagePublicationCandidateResponseV2,
)


class TaskImageSessionMaterializationConflictError(RuntimeError):
    """Retained candidate no longer matches its durable binding."""


def _candidate_response_from_row(
    row: TaskImagePublicationCandidate,
    *,
    credential_generation: int,
    response_model: type[
        TaskImagePublicationCandidateResponseV1
    ] = TaskImagePublicationCandidateResponseV1,
) -> TaskImagePublicationCandidateResponseV1:
    try:
        response = response_model.model_validate_json(json.dumps(row.response_json))
    except ValidationError:
        raise TaskImageSessionMaterializationConflictError(
            "stored task-image publication candidate changed"
        ) from None
    response_json = response.model_dump(mode="json", exclude_none=False)
    payload = rfc8785.dumps(response_json)
    if (
        (
            isinstance(response, TaskImagePublicationCandidateResponseV2)
            and response_json != row.response_json
        )
        or response.candidate_id != row.candidate_id
        or response.operation_id != row.operation_id
        or response.credential_id != row.credential_id
        or response.credential_generation != credential_generation
        or response.grant_id != row.grant_id
        or response.session_id != row.session_id
        or response.session_generation != row.session_generation
        or response.materialization_id != row.materialization_id
        or response.attempt_id != row.materialization_attempt_id
        or response.attempt_number != row.attempt_number
        or response.lease_epoch != row.lease_epoch
        or response.builder_id != row.builder_id
        or response.component != row.component
        or response.repository != row.repository
        or response.manifest_digest != row.manifest_digest
        or response.manifest_size != row.manifest_size
        or response.oci_file_sha256 != row.oci_file_sha256
        or response.oci_file_size != row.oci_file_size
        or response.platform != row.platform
        or response.recorded_at != row.recorded_at
        or not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), row.response_sha256)
    ):
        raise TaskImageSessionMaterializationConflictError(
            "stored task-image publication candidate changed"
        )
    return response


def parse_stored_publication_candidate_v2(
    row: TaskImagePublicationCandidate,
    *,
    credential_generation: int,
) -> TaskImagePublicationCandidateResponseV2:
    """Parse a stored V2 candidate after validating its complete row binding."""

    response = _candidate_response_from_row(
        row,
        credential_generation=credential_generation,
        response_model=TaskImagePublicationCandidateResponseV2,
    )
    assert isinstance(response, TaskImagePublicationCandidateResponseV2)
    return response
