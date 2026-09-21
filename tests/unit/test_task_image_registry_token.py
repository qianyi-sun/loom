from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID

import pytest

from loom_task_image_authority.registry_token import publication_repository

ATTEMPT_ID = UUID("11111111-1111-4111-8111-111111111111")
CREDENTIAL_ID = UUID("22222222-2222-4222-8222-222222222222")
NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def test_publication_repository_derives_exact_task_and_sidecar_paths() -> None:
    assert publication_repository(
        purpose="production",
        shadow_campaign_id=None,
        cpu_arch="x86_64",
        attempt_id=ATTEMPT_ID,
        component="task",
    ) == f"loom-task-image-attempts/x86_64/{ATTEMPT_ID}/task"

    sidecar_digest = hashlib.sha256(b"sidecar:Redis_cache.1").hexdigest()
    assert publication_repository(
        purpose="production",
        shadow_campaign_id=None,
        cpu_arch="arm64",
        attempt_id=ATTEMPT_ID,
        component="sidecar:Redis_cache.1",
    ) == (
        f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/"
        f"sidecar-sha256-{sidecar_digest}"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"purpose": "shadow"},
        {"purpose": "Production"},
        {"shadow_campaign_id": UUID("33333333-3333-4333-8333-333333333333")},
        {"cpu_arch": "amd64"},
        {"cpu_arch": "ARM64"},
        {"attempt_id": UUID(int=0)},
        {"attempt_id": str(ATTEMPT_ID)},
        {"component": ""},
        {"component": "sidecar:"},
        {"component": "sidecar:bad/name"},
        {"component": "sidecar:" + "x" * 129},
        {"component": "TASK"},
    ],
)
def test_publication_repository_rejects_unavailable_or_noncanonical_inputs(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "purpose": "production",
        "shadow_campaign_id": None,
        "cpu_arch": "arm64",
        "attempt_id": ATTEMPT_ID,
        "component": "task",
    }
    values.update(changes)
    with pytest.raises((TypeError, ValueError)):
        publication_repository(**values)  # type: ignore[arg-type]
