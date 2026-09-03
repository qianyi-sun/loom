from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.task_image_build_plan import (
    TaskImageBuildComponentV1,
    TaskImageBuildPlanV1,
)
from loom_task_image_authority.bundle_capability import (
    MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES,
    TaskImageBundleCapabilityError,
    TaskImageBundleCapabilityProvider,
    TaskImageBundleObject,
    TaskImageBundleObjectCapabilityV1,
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


class _FakeBundleBackend:
    def __init__(
        self,
        objects: tuple[TaskImageBundleObject, ...],
        *,
        url: Callable[[str], str] | None = None,
    ) -> None:
        self.objects = objects
        self.url = url or (
            lambda key: (
                f"https://objects.example/{key}"
                "?X-Amz-Date=20260903T140000Z&X-Amz-Expires=40"
                "&X-Amz-Signature=secret"
            )
        )
        self.list_bounds: list[int] = []
        self.presign_expiries: list[int] = []

    def list_objects(
        self,
        *,
        bucket: str,
        prefix: str,
        maximum_objects: int,
    ) -> tuple[TaskImageBundleObject, ...]:
        assert bucket == "loom-bundles"
        assert prefix == "bench/revision/task-1/"
        self.list_bounds.append(maximum_objects)
        return self.objects

    def presign_get(
        self,
        *,
        bucket: str,
        key: str,
        expires_in_seconds: int,
    ) -> str:
        assert bucket == "loom-bundles"
        self.presign_expiries.append(expires_in_seconds)
        return self.url(key)


def _provider(
    backend: _FakeBundleBackend,
    **changes: object,
) -> TaskImageBundleCapabilityProvider:
    values: dict[str, object] = {
        "backend": backend,
        "public_https_origin": "https://objects.example",
        "expected_bucket": "loom-bundles",
        "maximum_objects": 2_000,
        "maximum_bytes": 512 * 1024 * 1024,
        "url_expiry_seconds": 600,
        "capability_id_factory": lambda: CAPABILITY_ID,
    }
    values.update(changes)
    return TaskImageBundleCapabilityProvider(**values)  # type: ignore[arg-type]


def _objects() -> tuple[TaskImageBundleObject, ...]:
    prefix = "bench/revision/task-1/"
    return (
        TaskImageBundleObject(key=f"{prefix}z-empty.txt", size_bytes=0),
        TaskImageBundleObject(key=f"{prefix}task.toml", size_bytes=20),
        TaskImageBundleObject(
            key=f"{prefix}.loom-bundle-file-metadata.json",
            size_bytes=42,
        ),
    )


def test_issues_sorted_single_object_urls_bounded_by_current_session() -> None:
    backend = _FakeBundleBackend(_objects())
    capability = _provider(backend).issue(_plan(), now=NOW)

    assert capability.model_dump(mode="json") == {
        "schema_version": "loom.task-image-bundle-capability.v1",
        "capability_id": "44444444-4444-4444-4444-444444444444",
        "grant_id": "11111111-1111-1111-1111-111111111111",
        "session_id": "22222222-2222-2222-2222-222222222222",
        "session_generation": 3,
        "materialization_id": "33333333-3333-3333-3333-333333333333",
        "task_checksum": "4" * 64,
        "bundle_file_metadata_sha256": "5" * 64,
        "file_count": 3,
        "total_bytes": 62,
        "issued_at": "2026-09-03T14:00:00Z",
        "expires_at": "2026-09-03T14:00:40Z",
        "objects": [
            {
                "relative_path": ".loom-bundle-file-metadata.json",
                "size_bytes": 42,
                "url": (
                        "https://objects.example/bench/revision/task-1/"
                        ".loom-bundle-file-metadata.json"
                        "?X-Amz-Date=20260903T140000Z&X-Amz-Expires=40"
                        "&X-Amz-Signature=secret"
                ),
            },
            {
                "relative_path": "task.toml",
                "size_bytes": 20,
                "url": (
                        "https://objects.example/bench/revision/task-1/task.toml"
                        "?X-Amz-Date=20260903T140000Z&X-Amz-Expires=40"
                        "&X-Amz-Signature=secret"
                ),
            },
            {
                "relative_path": "z-empty.txt",
                "size_bytes": 0,
                "url": (
                        "https://objects.example/bench/revision/task-1/"
                        "z-empty.txt?X-Amz-Date=20260903T140000Z&X-Amz-Expires=40"
                        "&X-Amz-Signature=secret"
                ),
            },
        ],
    }
    assert backend.list_bounds == [2_001]
    assert backend.presign_expiries == [40, 40, 40]


def test_secret_capability_representation_never_contains_presigned_urls() -> None:
    backend = _FakeBundleBackend(
        _objects(),
        url=lambda key: (
            f"https://objects.example/{key}"
            "?X-Amz-Date=20260903T140000Z&X-Amz-Expires=40"
            "&X-Amz-Signature=TOPSECRET"
        ),
    )
    capability = _provider(backend).issue(_plan(), now=NOW)

    for rendered in (repr(capability), str(capability)):
        assert "TOPSECRET" not in rendered
        assert "https://" not in rendered


@pytest.mark.parametrize(
    "objects",
    [
        (),
        (
            TaskImageBundleObject(
                key="foreign/task.toml",
                size_bytes=1,
            ),
        ),
        (
            TaskImageBundleObject(
                key="bench/revision/task-1/",
                size_bytes=1,
            ),
        ),
        (
            TaskImageBundleObject(
                key="bench/revision/task-1/a/../task.toml",
                size_bytes=1,
            ),
        ),
        (
            TaskImageBundleObject(
                key="bench/revision/task-1/task.toml",
                size_bytes=1,
            ),
            TaskImageBundleObject(
                key="bench/revision/task-1/task.toml",
                size_bytes=1,
            ),
        ),
        (
            TaskImageBundleObject(
                key="bench/revision/task-1/task.toml",
                size_bytes=1,
                redirect=True,
            ),
        ),
    ],
)
def test_rejects_empty_cross_prefix_traversing_duplicate_or_redirected_objects(
    objects: tuple[TaskImageBundleObject, ...],
) -> None:
    with pytest.raises(TaskImageBundleCapabilityError):
        _provider(_FakeBundleBackend(objects)).issue(_plan(), now=NOW)


def test_rejects_object_count_and_aggregate_byte_overflow() -> None:
    objects = _objects()
    with pytest.raises(TaskImageBundleCapabilityError, match="limits"):
        _provider(
            _FakeBundleBackend(objects),
            maximum_objects=2,
        ).issue(_plan(), now=NOW)
    with pytest.raises(TaskImageBundleCapabilityError, match="limits"):
        _provider(
            _FakeBundleBackend(objects),
            maximum_bytes=61,
        ).issue(_plan(), now=NOW)


@pytest.mark.parametrize(
    "url",
    [
        "http://objects.example/private-key?signature=secret",
        "https://attacker.example/private-key?signature=secret",
        "https://user:password@objects.example/private-key?signature=secret",
        "https://objects.example/private-key?signature=secret#fragment",
        "https://objects.example.evil/private-key?signature=secret",
        "https://objects.example////private-key?signature=secret",
    ],
)
def test_rejects_non_https_or_cross_origin_presigned_urls_without_echo(url: str) -> None:
    backend = _FakeBundleBackend(
        _objects(),
        url=lambda _key: url,
    )

    with pytest.raises(TaskImageBundleCapabilityError) as caught:
        _provider(backend).issue(_plan(), now=NOW)

    assert url not in str(caught.value)
    assert "private-key" not in str(caught.value)
    assert "secret" not in str(caught.value)


def test_rejects_an_ambiguous_multi_slash_path_for_the_exact_object_key() -> None:
    backend = _FakeBundleBackend(
        _objects(),
        url=lambda key: f"https://objects.example////{key}?signature=secret",
    )

    with pytest.raises(TaskImageBundleCapabilityError, match="presigned URL"):
        _provider(backend).issue(_plan(), now=NOW)


def test_rejects_a_presigned_url_that_outlives_the_capability() -> None:
    backend = _FakeBundleBackend(
        _objects(),
        url=lambda key: (
            f"https://objects.example/{key}"
            "?X-Amz-Date=20260903T140000Z&X-Amz-Expires=604800"
            "&X-Amz-Signature=secret"
        ),
    )

    with pytest.raises(TaskImageBundleCapabilityError, match="presigned URL"):
        _provider(backend).issue(_plan(), now=NOW)


def test_rejects_wrong_bucket_and_expired_plan_before_listing() -> None:
    backend = _FakeBundleBackend(_objects())
    with pytest.raises(TaskImageBundleCapabilityError, match="source"):
        _provider(backend, expected_bucket="other-bucket").issue(_plan(), now=NOW)
    with pytest.raises(TaskImageBundleCapabilityError, match="expired"):
        _provider(backend).issue(
            _plan(authorization_expires_at=NOW),
            now=NOW,
        )

    assert backend.list_bounds == []


@pytest.mark.parametrize(
    "changes",
    [
        {"public_https_origin": "http://objects.example"},
        {"public_https_origin": "https://user@objects.example"},
        {"public_https_origin": "https://objects.example/path"},
        {"expected_bucket": "LOOM-BUNDLES"},
        {"maximum_objects": 0},
        {"maximum_objects": 2_001},
        {"maximum_bytes": 0},
        {"maximum_bytes": 512 * 1024 * 1024 + 1},
        {"url_expiry_seconds": 0},
        {"url_expiry_seconds": 901},
    ],
)
def test_provider_rejects_unsafe_configuration(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _provider(_FakeBundleBackend(_objects()), **changes)


def test_contract_rejects_unsorted_duplicate_or_mismatched_object_totals() -> None:
    capability = _provider(_FakeBundleBackend(_objects())).issue(_plan(), now=NOW)
    payload = capability.model_dump(mode="python")
    objects = list(capability.objects)
    assert all(isinstance(item, TaskImageBundleObjectCapabilityV1) for item in objects)

    for changed_objects, file_count, total_bytes in (
        (tuple(reversed(objects)), 3, 62),
        ((objects[0], objects[0], objects[2]), 3, 84),
        (tuple(objects), 2, 62),
        (tuple(objects), 3, 63),
    ):
        changed = dict(payload)
        changed.update(
            objects=changed_objects,
            file_count=file_count,
            total_bytes=total_bytes,
        )
        with pytest.raises(ValidationError):
            type(capability).model_validate(changed)


def test_rejects_capability_response_larger_than_fixed_ceiling() -> None:
    long_query = "x" * 512
    backend = _FakeBundleBackend(
        _objects(),
        url=lambda key: (
            f"https://objects.example/{key}"
            "?X-Amz-Date=20260903T140000Z&X-Amz-Expires=40"
            f"&X-Amz-Signature={long_query}"
        ),
    )

    with pytest.raises(TaskImageBundleCapabilityError, match="response"):
        _provider(
            backend,
            maximum_capability_bytes=256,
        ).issue(_plan(), now=NOW)
    assert MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES >= 256
