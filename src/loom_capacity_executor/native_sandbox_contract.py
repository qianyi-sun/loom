"""Render existing authority-free sandbox input from authenticated native context."""

import json

from loom.personal_dev_candidate import PERSONAL_DEV_BUILD_CONTRACT_SHA256, PERSONAL_DEV_COMPONENTS
from loom.personal_dev_sandbox_builder import PersonalDevSandboxBuildContract
from loom_capacity_agent.build_admission import BuildSourceContextV1

_CONTEXT_FIELDS = {
    "archive_sha256", "archive_size_bytes", "attempt_id", "attempt_sequence",
    "build_contract_sha256", "candidate_id", "candidate_sha", "lease_epoch",
    "operation_epoch", "operation_id", "platform", "source_commit", "source_sha256",
    "subject_id", "subject_incarnation",
}


def render_native_sandbox_contract(context: BuildSourceContextV1, *, max_artifact_bytes: int,
    max_image_archive_bytes: int,
) -> bytes:
    context = BuildSourceContextV1.model_validate_json(context.model_dump_json())
    if context.build_contract_sha256 != PERSONAL_DEV_BUILD_CONTRACT_SHA256:
        raise ValueError("native sandbox build contract changed")
    value = {**context.model_dump(mode="json", include=_CONTEXT_FIELDS), "schema_version": 1,
        "scope": "personal-dev-only", "components": list(PERSONAL_DEV_COMPONENTS),
        "max_artifact_bytes": max_artifact_bytes, "max_image_archive_bytes": max_image_archive_bytes}
    wire = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    PersonalDevSandboxBuildContract.parse(wire)
    return wire
