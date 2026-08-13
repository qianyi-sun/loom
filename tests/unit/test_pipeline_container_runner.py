from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID

import pytest

from loom_worker.pipeline_container_runner import (
    ArtifactCommitResult,
    ArtifactCommitter,
    ArtifactInputMaterializer,
    ContainerProcessResult,
    ExecutionCancellation,
    FakeArtifactCommitter,
    FakeArtifactInputMaterializer,
    FakeExecutionCancellation,
    FakePipelineContainerBackend,
    MaterializedInputView,
    PipelineCancelledError,
    PipelineContainerRunner,
    PipelineExecutionError,
    PipelineExecutionRequest,
    PipelineLivePreviewLifecycle,
    PipelineProcessFailedError,
    build_pipeline_container_spec,
)

ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000008")
IMAGE = "registry.example/loom/behavior@sha256:" + "8" * 64
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


def _runner(
    tmp_path: Path,
    *,
    process: ContainerProcessResult | None = None,
    cancellation: FakeExecutionCancellation | None = None,
    materialized_root: Path | None = None,
) -> tuple[
    PipelineContainerRunner,
    FakeArtifactInputMaterializer,
    FakeArtifactCommitter,
    FakeExecutionCancellation,
    FakePipelineContainerBackend,
]:
    inputs = tmp_path / "inputs"
    outputs = tmp_path / "outputs"
    scratch = tmp_path / "scratch"
    for path in (inputs, outputs, scratch):
        path.mkdir()
    spec = build_pipeline_container_spec(
        image=IMAGE,
        argv=["/app/run", "/inputs/stage-request.json"],
        workdir="/workspace",
        uid=1000,
        gid=1000,
        input_dir=inputs,
        outputs_dir=outputs,
        scratch_dir=scratch,
        network_profile="none",
        cpus=1,
        memory_bytes=1024,
        pids=32,
        scratch_bytes=4096,
    )
    view = MaterializedInputView(
        root=materialized_root or inputs,
        input_view_digest=DIGEST_A,
    )
    materializer = FakeArtifactInputMaterializer(result=view)
    committer = FakeArtifactCommitter(result=ArtifactCommitResult(manifest_digest=DIGEST_B))
    cancellation = cancellation or FakeExecutionCancellation()
    backend = FakePipelineContainerBackend(
        result=process
        or ContainerProcessResult(
            exit_code=0,
            stage_result={"schema_version": "loom.stage-result.v1"},
            stage_result_digest=DIGEST_B,
        )
    )
    return (
        PipelineContainerRunner(
            spec=spec,
            materializer=materializer,
            committer=committer,
            cancellation=cancellation,
            backend=backend,
        ),
        materializer,
        committer,
        cancellation,
        backend,
    )


def _request() -> PipelineExecutionRequest:
    return PipelineExecutionRequest(
        attempt_id=ATTEMPT_ID,
        bindings=({"binding_name": "dataset", "items": []},),
        stage_request=b'{"schema_version":"behavior.stage-request.v1"}\n',
    )


async def test_runner_materializes_runs_commits_and_tears_down(tmp_path: Path) -> None:
    runner, materializer, committer, cancellation, backend = _runner(tmp_path)

    result = await runner.run(_request())

    assert result.attempt_id == ATTEMPT_ID
    assert result.input_view_digest == DIGEST_A
    assert result.stage_result_digest == DIGEST_B
    assert result.commit.manifest_digest == DIGEST_B
    assert materializer.calls == [(ATTEMPT_ID, tmp_path / "inputs")]
    assert materializer.releases == [(ATTEMPT_ID, tmp_path / "inputs")]
    assert committer.calls == [(ATTEMPT_ID, tmp_path / "outputs", DIGEST_B)]
    assert cancellation.acknowledged == []
    assert backend.calls == ["run", "teardown"]


async def test_preview_failure_is_isolated_and_runner_always_stops_it(tmp_path: Path) -> None:
    runner, _, _, _, backend = _runner(tmp_path)

    class _FailingPreview:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.stopped = 0

        async def run_until(self, stop: asyncio.Event) -> None:
            del stop
            self.started.set()
            raise RuntimeError("SECRET_CANARY preview failure")

        def stop(self, *, reason: str = "preview_lifecycle_ended") -> object:
            assert reason == "preview_lifecycle_ended"
            self.stopped += 1
            return None

    preview = _FailingPreview()
    assert isinstance(preview, PipelineLivePreviewLifecycle)
    runner.live_preview = preview
    result = await runner.run(_request())

    assert result.attempt_id == ATTEMPT_ID
    assert preview.started.is_set()
    assert preview.stopped == 1
    assert backend.calls == ["run", "teardown"]


async def test_runner_attests_preflight_before_stage_argv(tmp_path: Path) -> None:
    runner, _, _, _, backend = _runner(tmp_path)
    calls: list[str] = []

    class _Preflight:
        async def attest(self, *, attempt_id, spec, input_view) -> None:  # type: ignore[no-untyped-def]
            del attempt_id, spec, input_view
            assert backend.calls == []
            calls.append("preflight")

    runner.preflight = _Preflight()
    await runner.run(_request())
    assert calls == ["preflight"]
    assert backend.calls == ["run", "teardown"]


async def test_nonzero_exit_never_commits_and_still_tears_down(tmp_path: Path) -> None:
    runner, _, committer, _, backend = _runner(
        tmp_path,
        process=ContainerProcessResult(
            exit_code=17,
            stage_result=None,
            stage_result_digest=None,
        ),
    )

    with pytest.raises(PipelineProcessFailedError) as caught:
        await runner.run(_request())

    assert caught.value.exit_code == 17
    assert committer.calls == []
    assert backend.calls == ["run", "teardown"]


async def test_rc_zero_without_stage_result_fails_before_commit(tmp_path: Path) -> None:
    runner, _, committer, _, backend = _runner(
        tmp_path,
        process=ContainerProcessResult(
            exit_code=0,
            stage_result=None,
            stage_result_digest=None,
        ),
    )

    with pytest.raises(PipelineExecutionError, match="requires a canonical StageResult"):
        await runner.run(_request())

    assert committer.calls == []
    assert backend.calls == ["run", "teardown"]


async def test_cancellation_before_materialization_starts_nothing(tmp_path: Path) -> None:
    cancellation = FakeExecutionCancellation(observations=[True])
    runner, materializer, committer, cancellation, backend = _runner(
        tmp_path,
        cancellation=cancellation,
    )

    with pytest.raises(PipelineCancelledError):
        await runner.run(_request())

    assert materializer.calls == []
    assert materializer.releases == []
    assert committer.calls == []
    assert backend.calls == []
    assert cancellation.acknowledged == [(ATTEMPT_ID, False, True)]


async def test_cancellation_after_exit_terminates_and_acks_after_teardown(
    tmp_path: Path,
) -> None:
    cancellation = FakeExecutionCancellation(observations=[False, False, True])
    runner, materializer, committer, cancellation, backend = _runner(
        tmp_path,
        cancellation=cancellation,
    )
    backend.terminate_forced = True

    with pytest.raises(PipelineCancelledError):
        await runner.run(_request())

    assert committer.calls == []
    assert backend.calls == ["run", "terminate", "teardown"]
    assert materializer.releases == [(ATTEMPT_ID, tmp_path / "inputs")]
    assert cancellation.acknowledged == [(ATTEMPT_ID, True, True)]


async def test_materializer_cannot_substitute_a_different_input_root(tmp_path: Path) -> None:
    other = tmp_path / "other"
    other.mkdir()
    runner, materializer, committer, _, backend = _runner(tmp_path, materialized_root=other)

    with pytest.raises(PipelineExecutionError, match="different input root"):
        await runner.run(_request())

    assert committer.calls == []
    assert backend.calls == []
    assert materializer.releases == [(ATTEMPT_ID, other)]


def test_fakes_implement_only_the_strict_worker_protocols(tmp_path: Path) -> None:
    runner, materializer, committer, cancellation, _ = _runner(tmp_path)

    assert isinstance(materializer, ArtifactInputMaterializer)
    assert isinstance(committer, ArtifactCommitter)
    assert isinstance(cancellation, ExecutionCancellation)
    assert not hasattr(materializer, "get_object_store_credentials")
    assert not hasattr(committer, "put_object")
    assert runner.spec.limits.scratch_bytes == 4096
