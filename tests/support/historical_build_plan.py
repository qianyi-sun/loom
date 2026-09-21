from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from loom.task_image_build_plan import (
    TaskImageBuildComponentV1,
    TaskImageBuildPlanV1,
)

NOW = datetime(2026, 9, 3, 14, 0, tzinfo=UTC)
CAPABILITY_ID = UUID("44444444-4444-4444-4444-444444444444")


def _plan(**changes: object) -> TaskImageBuildPlanV1:
    values: dict[str, object] = {
        "grant_id": UUID("11111111-1111-1111-1111-111111111111"),
        "session_id": UUID("22222222-2222-2222-2222-222222222222"),
        "session_generation": 3,
        "materialization_id": UUID("33333333-3333-3333-3333-333333333333"),
        "builder_id": "rootless:22222222222222222222222222222222",
        "task_id": "bench/task-1",
        "task_checksum": "4" * 64,
        "cpu_arch": "arm64",
        "platform": "linux/arm64",
        "bundle_bucket": "loom-bundles",
        "bundle_prefix": "bench/revision/task-1/",
        "bundle_file_metadata_sha256": "5" * 64,
        "bundle_file_limit": 2_000,
        "bundle_byte_limit": 512 * 1024 * 1024,
        "build_timeout_seconds": 900.0,
        "authorization_expires_at": NOW + timedelta(seconds=40),
        "components": (
            TaskImageBuildComponentV1(
                name="task",
                dockerfile_path="environment/Dockerfile",
                context_path=".",
                oci_output_path="oci/0000.tar",
            ),
        ),
    }
    values.update(changes)
    return TaskImageBuildPlanV1.model_validate(values)
