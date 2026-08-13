from __future__ import annotations

import hashlib
import io
import os
import time
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from PIL import Image

from loom.integrations.behavior.stages.rollout_engine import best_effort_live_preview
from loom.pipeline.keys import canonical_document
from loom_worker.control_plane_client import ExecutionAttemptClaimHeaders, HttpControlPlaneClient
from loom_worker.pipeline_live_preview import (
    LivePreviewRecordV1,
    PipelineLivePreviewError,
    PipelineLivePreviewProducer,
    PipelineLivePreviewPublisher,
    purge_live_preview,
    scan_live_preview_frames,
)


def _jpeg(
    *,
    progressive: bool = False,
    metadata: bool = False,
    color: str = "navy",
    size: tuple[int, int] = (672, 448),
) -> bytes:
    image = Image.new("RGB", size, color=color)
    output = io.BytesIO()
    options: dict[str, Any] = {"format": "JPEG", "quality": 70, "progressive": progressive}
    if metadata:
        options["comment"] = b"SECRET_CANARY_DO_NOT_PERSIST"
    image.save(output, **options)
    return output.getvalue()


def _claim() -> ExecutionAttemptClaimHeaders:
    return ExecutionAttemptClaimHeaders(
        claim_id=UUID(int=2),
        lease_epoch=7,
        lease_token="lease-secret-canary",
    )


def _producer(tmp_path: Path) -> PipelineLivePreviewProducer:
    return PipelineLivePreviewProducer(
        preview_root=(tmp_path / "live-preview").resolve(),
        episode_bound=100,
    )


def test_stage1_best_effort_sink_contains_preview_failure(tmp_path: Path) -> None:
    producer = _producer(tmp_path)
    sink = best_effort_live_preview(producer)

    sink.offer(step_idx=0, jpeg=b"not-a-jpeg")
    sink.offer(step_idx=1, jpeg=_jpeg())

    assert list((tmp_path / "live-preview").iterdir()) == []


def test_producer_writes_private_atomic_canonical_pair_and_enforces_cadence(
    tmp_path: Path,
) -> None:
    producer = _producer(tmp_path)
    root = tmp_path / "live-preview"
    first = producer.emit(sequence=0, step_idx=0, jpeg=_jpeg(), monotonic_now=10.0)
    assert first.accepted is True
    assert (
        producer.emit(sequence=1, step_idx=1, jpeg=_jpeg(color="red"), monotonic_now=10.49).reason
        == "cadence_drop"
    )
    producer.emit(sequence=1, step_idx=1, jpeg=_jpeg(color="red"), monotonic_now=10.5)

    assert os.stat(root).st_mode & 0o777 == 0o700
    assert sorted(path.name for path in root.iterdir()) == [
        "00000000000000000000.jpg",
        "00000000000000000000.json",
        "00000000000000000001.jpg",
        "00000000000000000001.json",
    ]
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in root.iterdir())
    frames = scan_live_preview_frames(root, episode_bound=100)
    assert [frame.record.sequence for frame in frames] == [0, 1]
    assert [frame.record.step_idx for frame in frames] == [0, 1]
    assert frames[0].record_bytes == canonical_document(frames[0].record.model_dump(mode="json"))
    assert frames[0].record_bytes.endswith(b"\n")


def test_bounded_producer_and_watcher_performance(tmp_path: Path) -> None:
    producer = PipelineLivePreviewProducer(
        preview_root=(tmp_path / "live-preview").resolve(),
        episode_bound=1_000,
    )
    value = _jpeg()
    started = time.perf_counter()
    for sequence in range(64):
        producer.emit(
            sequence=sequence,
            step_idx=sequence,
            jpeg=value,
            monotonic_now=sequence * 0.5,
        )
    emit_seconds = time.perf_counter() - started
    started = time.perf_counter()
    frames = scan_live_preview_frames(tmp_path / "live-preview", episode_bound=1_000)
    scan_seconds = time.perf_counter() - started

    assert len(frames) == 64
    assert sum(len(frame.jpeg) + len(frame.record_bytes) for frame in frames) < 32 * 1024 * 1024
    # Wide machine-independent guardrails catch accidental quadratic work or
    # blocking encoder/network additions without treating wall time as a SLA.
    assert emit_seconds < 10
    assert scan_seconds < 5


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (_jpeg(progressive=True), "preview_jpeg_not_baseline"),
        (_jpeg(metadata=True), "preview_metadata_forbidden"),
        (_jpeg(size=(671, 448)), "preview_dimensions_invalid"),
        (_jpeg()[:-2], "preview_jpeg_decode_failed"),
        (b"not-a-jpeg", "preview_jpeg_not_baseline"),
    ],
)
def test_producer_rejects_non_baseline_metadata_or_invalid_jpeg(
    tmp_path: Path, value: bytes, reason: str
) -> None:
    producer = _producer(tmp_path)
    with pytest.raises(PipelineLivePreviewError, match=reason):
        producer.emit(sequence=0, step_idx=0, jpeg=value, monotonic_now=1.0)
    assert list((tmp_path / "live-preview").iterdir()) == []


def test_watcher_rejects_hardlink_mode_digest_noncanonical_and_step_drift(
    tmp_path: Path,
) -> None:
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=2, jpeg=_jpeg(), monotonic_now=1.0)
    root = tmp_path / "live-preview"
    jpeg = root / "00000000000000000000.jpg"
    (tmp_path / "outside").hardlink_to(jpeg)
    with pytest.raises(PipelineLivePreviewError, match="preview_file_not_private"):
        scan_live_preview_frames(root, episode_bound=100)
    (tmp_path / "outside").unlink()

    record_path = root / "00000000000000000000.json"
    record = LivePreviewRecordV1.model_validate_json(record_path.read_bytes())
    record_path.write_text(record.model_dump_json(indent=2) + "\n")
    with pytest.raises(PipelineLivePreviewError, match="preview_record_not_canonical"):
        scan_live_preview_frames(root, episode_bound=100)

    record_path.write_bytes(canonical_document(record.model_dump(mode="json")))
    os.chmod(record_path, 0o644)
    with pytest.raises(PipelineLivePreviewError, match="preview_file_not_private"):
        scan_live_preview_frames(root, episode_bound=100)
    os.chmod(record_path, 0o600)

    changed = record.model_copy(update={"step_idx": 100})
    record_path.write_bytes(canonical_document(changed.model_dump(mode="json")))
    with pytest.raises(PipelineLivePreviewError, match="preview_step_invalid"):
        scan_live_preview_frames(root, episode_bound=100)


def test_producer_rejects_non_contiguous_sequence_and_step_regression(tmp_path: Path) -> None:
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=2, jpeg=_jpeg(), monotonic_now=0.0)
    with pytest.raises(PipelineLivePreviewError, match="preview_sequence_not_contiguous"):
        producer.emit(sequence=2, step_idx=3, jpeg=_jpeg(), monotonic_now=0.5)
    with pytest.raises(PipelineLivePreviewError, match="preview_step_regressed"):
        producer.emit(sequence=1, step_idx=1, jpeg=_jpeg(), monotonic_now=0.5)


def test_producer_restart_discards_partial_and_resumes_unpublished_sequence(
    tmp_path: Path,
) -> None:
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=2, jpeg=_jpeg(), monotonic_now=0.0)
    partial = tmp_path / "live-preview" / ".00000000000000000001.jpg.partial"
    partial.write_bytes(b"incomplete")
    os.chmod(partial, 0o600)

    restarted = _producer(tmp_path)
    assert not partial.exists()
    restarted.emit(sequence=1, step_idx=2, jpeg=_jpeg(), monotonic_now=0.0)
    assert [
        frame.record.sequence
        for frame in scan_live_preview_frames(tmp_path / "live-preview", episode_bound=100)
    ] == [0, 1]


@pytest.mark.asyncio
async def test_local_pressure_evicts_only_oldest_and_gap_closes_optional_channel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import loom_worker.pipeline_live_preview as preview

    monkeypatch.setattr(preview, "PREVIEW_MAX_LOCAL_FRAMES", 2)
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=0, jpeg=_jpeg(), monotonic_now=0.0)
    producer.emit(sequence=1, step_idx=1, jpeg=_jpeg(), monotonic_now=0.5)
    result = producer.emit(sequence=2, step_idx=2, jpeg=_jpeg(), monotonic_now=1.0)
    assert result.evicted_sequences == (0,)
    assert [
        frame.record.sequence
        for frame in scan_live_preview_frames(tmp_path / "live-preview", episode_bound=100)
    ] == [1, 2]

    class UnusedControlPlane:
        async def publish_live_preview_frame(self, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("gap must close before HTTP")

    publisher = PipelineLivePreviewPublisher(
        preview_root=(tmp_path / "live-preview").resolve(),
        attempt_id=UUID(int=1),
        claim=_claim(),
        control_plane=UnusedControlPlane(),
        episode_bound=100,
    )
    outcome = await publisher.publish_if_due(monotonic_now=2.0)
    assert outcome.state == "closed"
    assert outcome.reason == "preview_sequence_gap"
    assert list((tmp_path / "live-preview").iterdir()) == []


@pytest.mark.asyncio
async def test_publisher_retries_identically_then_releases_and_is_rate_limited(
    tmp_path: Path,
) -> None:
    producer = _producer(tmp_path)
    value = _jpeg()
    producer.emit(sequence=0, step_idx=4, jpeg=value, monotonic_now=0.0)

    class FlakyControlPlane:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def publish_live_preview_frame(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                raise httpx.ConnectError("SECRET_CANARY transport")
            return {"state": "live", "sequence": kwargs["sequence"]}

    control_plane = FlakyControlPlane()
    publisher = PipelineLivePreviewPublisher(
        preview_root=(tmp_path / "live-preview").resolve(),
        attempt_id=UUID(int=1),
        claim=_claim(),
        control_plane=control_plane,
        episode_bound=100,
    )
    failed = await publisher.publish_if_due(monotonic_now=10.0)
    assert failed.state == "retrying"
    assert failed.reason == "control_plane_publish_failed"
    assert "SECRET_CANARY" not in repr(failed)
    assert (await publisher.publish_if_due(monotonic_now=10.49)).state == "idle"
    accepted = await publisher.publish_if_due(monotonic_now=10.5)
    assert accepted.state == "published"
    assert len(control_plane.calls) == 2
    assert control_plane.calls[0]["jpeg"] == control_plane.calls[1]["jpeg"] == value
    assert list((tmp_path / "live-preview").iterdir()) == []


@pytest.mark.asyncio
async def test_changed_retry_closes_as_equivocation_without_stage_exception(tmp_path: Path) -> None:
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=0, jpeg=_jpeg(), monotonic_now=0.0)

    class OfflineControlPlane:
        async def publish_live_preview_frame(self, **kwargs: Any) -> dict[str, Any]:
            raise httpx.ConnectError("offline")

    publisher = PipelineLivePreviewPublisher(
        preview_root=(tmp_path / "live-preview").resolve(),
        attempt_id=UUID(int=1),
        claim=_claim(),
        control_plane=OfflineControlPlane(),
        episode_bound=100,
    )
    assert (await publisher.publish_if_due(monotonic_now=1.0)).state == "retrying"
    root = tmp_path / "live-preview"
    replacement = _jpeg(color="red")
    jpeg_path = root / "00000000000000000000.jpg"
    jpeg_path.write_bytes(replacement)
    record_path = root / "00000000000000000000.json"
    record = LivePreviewRecordV1.model_validate_json(record_path.read_bytes()).model_copy(
        update={
            "jpeg_sha256": f"sha256:{hashlib.sha256(replacement).hexdigest()}",
            "jpeg_size_bytes": len(replacement),
        }
    )
    record_path.write_bytes(canonical_document(record.model_dump(mode="json")))
    closed = await publisher.publish_if_due(monotonic_now=1.5)
    assert closed.state == "closed"
    assert closed.reason == "preview_sequence_equivocation"
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_control_plane_4xx_closes_and_purges_without_leaking_response(
    tmp_path: Path,
) -> None:
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=0, jpeg=_jpeg(), monotonic_now=0.0)

    class RejectingControlPlane:
        async def publish_live_preview_frame(self, **kwargs: Any) -> dict[str, Any]:
            request = httpx.Request("PUT", "https://control.invalid/preview")
            response = httpx.Response(
                409,
                request=request,
                text="SECRET_CANARY /private/worker/path",
            )
            raise httpx.HTTPStatusError("rejected", request=request, response=response)

    publisher = PipelineLivePreviewPublisher(
        preview_root=(tmp_path / "live-preview").resolve(),
        attempt_id=UUID(int=1),
        claim=_claim(),
        control_plane=RejectingControlPlane(),
        episode_bound=100,
    )
    result = await publisher.publish_if_due(monotonic_now=1.0)
    assert result.reason == "control_plane_preview_rejected"
    assert "SECRET_CANARY" not in repr(result)
    assert "/private/worker/path" not in repr(result)
    assert list((tmp_path / "live-preview").iterdir()) == []


@pytest.mark.asyncio
async def test_http_publish_uses_only_claim_bound_closed_headers_and_bytes() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"state": "live", "sequence": 3})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://control.invalid"
    ) as http:
        client = HttpControlPlaneClient(
            base_url="https://control.invalid", token="worker-secret-canary", _client=http
        )
        value = _jpeg()
        response = await client.publish_live_preview_frame(
            attempt_id=UUID(int=1),
            sequence=3,
            step_idx=9,
            jpeg_sha256=f"sha256:{hashlib.sha256(value).hexdigest()}",
            jpeg=value,
            claim=_claim(),
        )

    assert response == {"state": "live", "sequence": 3}
    request = seen[0]
    assert request.method == "PUT"
    assert request.url.path.endswith(
        "/execution-attempts/00000000-0000-0000-0000-000000000001/live-preview/frames/3"
    )
    assert request.headers["content-type"] == "image/jpeg"
    assert request.headers["x-loom-claim-id"] == str(UUID(int=2))
    assert request.headers["x-loom-lease-epoch"] == "7"
    assert request.headers["x-loom-lease-token"] == "lease-secret-canary"
    assert request.headers["idempotency-key"].endswith(":3")
    assert request.headers["if-match"].startswith('"sha256:')
    assert request.headers["x-loom-preview-step"] == "9"
    assert request.content == value
    assert b"worker-secret-canary" not in request.content
    assert b"lease-secret-canary" not in request.content


def test_stop_purges_idempotently_and_exposes_no_path_or_secret(tmp_path: Path) -> None:
    producer = _producer(tmp_path)
    producer.emit(sequence=0, step_idx=0, jpeg=_jpeg(), monotonic_now=0.0)
    root = (tmp_path / "live-preview").resolve()
    assert purge_live_preview(root) == 2
    assert purge_live_preview(root) == 0
    assert list(root.iterdir()) == []
