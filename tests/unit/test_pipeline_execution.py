from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.work_protocol import ExecutionAttemptClaimV1, WorkerCleanupProofV1
from loom_worker import main_loop as worker_main_loop
from loom_worker.pipeline_container_runner import PipelineProcessFailedError
from loom_worker.pipeline_execution import (
    HttpFinalOutputCommitter,
    PipelineAttemptPaths,
    PipelineCleanupJournal,
    _container_spec,
    _execution_failure,
    _install_runtime_contract,
    _observed_cleanup_proof,
    _require_terminalgen_claim,
    _validate_complete_marker,
    production_pipeline_enabled,
)
from tests.pipeline_input_helpers import claim, scalar_artifact
from tests.unit.test_pipeline_work_protocol import terminalgen_validation_claim


def _claim():  # type: ignore[no-untyped-def]
    _payload, _manifest, binding = scalar_artifact()
    return claim(binding)


def test_production_pipeline_is_confined_to_closed_enabled_pools() -> None:
    identity = {
        "require_cgroup_parent": True,
        "cgroup_parent": "loom-job-42.slice",
        "slurm_job_id": "42",
        "sandbox_identity": "stage1",
        "candidate_sha": "a" * 40,
        "compose_project": "loom-stage1",
    }
    for name in ("behavior-gpu-oldlab", "behavior-gpu-gb10"):
        assert production_pipeline_enabled(  # type: ignore[arg-type]
            SimpleNamespace(pool_name=name, **identity)
        )
    for name in ("default", "behavior-cpu-data", "remote-worker"):
        assert not production_pipeline_enabled(  # type: ignore[arg-type]
            SimpleNamespace(pool_name=name, **identity)
        )
    assert not production_pipeline_enabled(  # type: ignore[arg-type]
        SimpleNamespace(pool_name="behavior-gpu-oldlab")
    )
    terminalgen = SimpleNamespace(
        pool_name="terminalgen-validate-none",
        pipeline_terminalgen_authoring_enabled=False,
        **identity,
    )
    assert not production_pipeline_enabled(terminalgen)  # type: ignore[arg-type]
    terminalgen.pipeline_terminalgen_authoring_enabled = True
    assert production_pipeline_enabled(terminalgen)  # type: ignore[arg-type]


def test_terminalgen_worker_admission_binds_pool_node_and_validation_backend() -> None:
    parsed = ExecutionAttemptClaimV1.model_validate(terminalgen_validation_claim())
    settings = SimpleNamespace(
        pool_name="terminalgen-validate-none",
        pipeline_terminalgen_authoring_enabled=True,
        require_cgroup_parent=True,
        cgroup_parent="loom-job-42.slice",
        slurm_job_id="42",
        sandbox_identity="terminalgen-authoring",
        candidate_sha="a" * 40,
        compose_project="loom-terminalgen-validate-none",
    )

    _require_terminalgen_claim(parsed, settings)  # type: ignore[arg-type]

    settings.pool_name = "terminalgen-plan-none"
    with pytest.raises(RuntimeError, match="claim_not_eligible"):
        _require_terminalgen_claim(parsed, settings)  # type: ignore[arg-type]

    settings.pool_name = "terminalgen-validate-none"
    settings.pipeline_terminalgen_authoring_enabled = False
    with pytest.raises(RuntimeError, match="pool_not_enabled"):
        _require_terminalgen_claim(parsed, settings)  # type: ignore[arg-type]


def test_terminalgen_validation_pool_rejects_unattested_backend() -> None:
    value = terminalgen_validation_claim()
    value["worker_capability_snapshot"]["container_runtime_features"] = [
        "loom-terminalgen-authoring-worker-v1"
    ]
    value["worker_capability_snapshot_digest"] = digest_bytes(
        canonical_document(value["worker_capability_snapshot"])
    )
    with pytest.raises(ValueError, match="worker capability does not satisfy"):
        ExecutionAttemptClaimV1.model_validate(value)


def test_container_pid_limit_comes_from_the_frozen_resource_profile(tmp_path: Path) -> None:
    attempt_id = UUID("00000000-0000-0000-0000-000000000173")
    payload = SimpleNamespace(
        execution_attempt_id=attempt_id,
        resource_profile_snapshot=SimpleNamespace(
            cpu_cores=2,
            memory_bytes=1 << 30,
            scratch_bytes=2 << 30,
            pids_limit=173,
            execution_variants=[
                SimpleNamespace(
                    variant_id="pipeline-test-cpu-x86_64",
                    container_memory_bytes_override=None,
                )
            ],
        ),
        execution_spec_snapshot=SimpleNamespace(execution_variant_id="pipeline-test-cpu-x86_64"),
        image="registry.invalid/loom/test@sha256:" + "a" * 64,
        argv=["python", "-m", "example"],
        workdir="/workspace",
        network_profile="none",
        slurm_gpu_allocation_evidence=None,
        resume_checkpoint=None,
    )
    root = tmp_path / "attempt"
    inputs = tmp_path / "inputs"
    outputs = root / "outputs"
    scratch = root / "scratch"
    for path in (inputs, outputs, scratch):
        path.mkdir(parents=True, exist_ok=True)
    paths = PipelineAttemptPaths(root=root, inputs=inputs, outputs=outputs, scratch=scratch)
    assert _container_spec(payload, paths).limits.pids == 173  # type: ignore[arg-type]


def test_stage1_runtime_registration_advertises_dedicated_claim_feature(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = SimpleNamespace(
        pool_name="behavior-gpu-oldlab",
        max_concurrent=1,
        trajectory_cache_dir=tmp_path,
        require_cgroup_parent=True,
        cgroup_parent="loom-job-42.slice",
        slurm_job_id="42",
        sandbox_identity="stage1",
        candidate_sha="a" * 40,
        compose_project="loom-stage1",
    )
    capability = SimpleNamespace(
        model_dump=lambda **_kwargs: {
            "schema_version": "loom.worker-capabilities.v1",
            "cpu_arch": "x86_64",
            "cpu_cores": 8,
            "memory_bytes": 64 << 30,
            "scratch_bytes": 100 << 30,
            "network_profiles": ["none"],
            "container_runtime_features": [
                "egl",
                "loom-secret-tmpfs-v1",
                "loom-stage1-smoke-worker-v1",
                "nvidia-container-runtime",
            ],
            "gpu_devices": [],
            "input_cache_capacity_bytes": 1,
            "input_cache_reserved_bytes": 0,
            "input_cache_ready_bytes": 0,
        },
        digest="sha256:" + "1" * 64,
        cpu_arch="x86_64",
        gpu_devices=(),
        input_cache_capacity_bytes=1,
        input_cache_reserved_bytes=0,
        input_cache_ready_bytes=0,
    )
    monkeypatch.setenv("LOOM_SLURM_CLUSTER_ID", "oldlab")
    monkeypatch.setattr(worker_main_loop, "_host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(worker_main_loop, "_host_memory_bytes", lambda: 64 << 30)
    monkeypatch.setattr(worker_main_loop, "discover_slurm_gpu_allocation", lambda **_kw: ((), None))
    monkeypatch.setattr(
        worker_main_loop,
        "build_worker_capability_snapshot",
        lambda **kw: (
            capability
            if "loom-stage1-smoke-worker-v1" in kw["container_runtime_features"]
            else (_ for _ in ()).throw(AssertionError("Stage1 feature was not advertised"))
        ),
    )
    monkeypatch.setattr(
        worker_main_loop.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=""),
    )
    monkeypatch.setattr(Path, "read_text", lambda *_args, **_kwargs: "MemTotal: 1 kB\n")

    payload = worker_main_loop._pipeline_registration_payload(settings)  # type: ignore[arg-type]

    assert payload["capability_snapshot"]["container_runtime_features"] == [
        "egl",
        "loom-secret-tmpfs-v1",
        "loom-stage1-smoke-worker-v1",
        "nvidia-container-runtime",
    ]


@pytest.mark.parametrize(
    "pool_name,enabled,advertised",
    [
        ("terminalgen-plan-none", False, False),
        ("terminalgen-plan-none", True, True),
        ("terminalgen-package-none", True, True),
        ("terminalgen-generate-gateway", True, False),
        ("terminalgen-validate-none", True, False),
    ],
)
def test_terminalgen_registration_feature_is_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    pool_name: str,
    enabled: bool,
    advertised: bool,
) -> None:
    settings = SimpleNamespace(
        pool_name=pool_name,
        max_concurrent=1,
        trajectory_cache_dir=tmp_path,
        pipeline_terminalgen_authoring_enabled=enabled,
        require_cgroup_parent=True,
        cgroup_parent="loom-job-42.slice",
        slurm_job_id="42",
        sandbox_identity="terminalgen-authoring",
        candidate_sha="a" * 40,
        compose_project=f"loom-{pool_name}",
    )
    seen_features: list[str] = []
    capability = SimpleNamespace(
        model_dump=lambda **_kwargs: {
            "schema_version": "loom.worker-capabilities.v1",
            "cpu_arch": "x86_64",
            "cpu_cores": 8,
            "memory_bytes": 64 << 30,
            "scratch_bytes": 100 << 30,
            "network_profiles": ["gateway", "none"],
            "container_runtime_features": seen_features,
            "gpu_devices": [],
            "input_cache_capacity_bytes": 1,
            "input_cache_reserved_bytes": 0,
            "input_cache_ready_bytes": 0,
        },
        digest="sha256:" + "2" * 64,
        cpu_arch="x86_64",
        gpu_devices=(),
        input_cache_capacity_bytes=1,
        input_cache_reserved_bytes=0,
        input_cache_ready_bytes=0,
    )

    monkeypatch.setattr(worker_main_loop, "_host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(worker_main_loop, "_host_memory_bytes", lambda: 64 << 30)
    monkeypatch.setattr(worker_main_loop, "validate_oldlab_cpu_allocation", lambda _env: None)

    def _capability(**kwargs: Any) -> Any:
        seen_features.extend(kwargs["container_runtime_features"])
        return capability

    monkeypatch.setattr(worker_main_loop, "build_worker_capability_snapshot", _capability)

    payload = worker_main_loop._pipeline_registration_payload(settings)  # type: ignore[arg-type]

    assert (
        "loom-terminalgen-authoring-worker-v1"
        in payload["capability_snapshot"]["container_runtime_features"]
    ) is advertised


class _FinalOutputControlPlane:
    def __init__(self) -> None:
        self.prepared: dict[str, Any] | None = None
        self.parts: list[tuple[int, int, bytes]] = []
        self.completed: list[int] = []
        self.session_id = UUID(int=44)

    async def prepare_final_output(self, **kwargs: Any) -> dict[str, Any]:
        self.prepared = kwargs
        container_files = [
            {
                "file_index": index,
                "preallocated_artifact_id": str(UUID(int=index + 100)),
                "artifact_name": item["output_name"],
                "artifact_type": "behavior_rollout_bundle.v1",
                "producer": "container",
                "media_type": (
                    "application/json"
                    if item["relative_path"].endswith("/artifact.json")
                    else "application/octet-stream"
                ),
                "relative_path": item["relative_path"].removeprefix(
                    f"artifacts/{item['output_name']}/"
                ),
                "role": (
                    "semantic_document"
                    if item["relative_path"].endswith("/artifact.json")
                    else "payload"
                ),
                "archive_format": "none",
                "expected_max_bytes": 1024,
                "expected_size": item["size_bytes"],
                "expected_sha256": item["sha256"],
            }
            for index, item in enumerate(kwargs["payload"]["files"])
        ]
        return {
            "schema_version": "loom.upload-session-grant.v1",
            "upload_session_id": str(self.session_id),
            "state": "uploading",
            "upload_token": "u" * 48,
            "token_expires_at": (datetime.now(UTC) + timedelta(minutes=15)).isoformat(),
            "files": [
                *container_files,
                {
                    "file_index": len(container_files),
                    "preallocated_artifact_id": str(UUID(int=999)),
                    "artifact_name": "platform_manifest",
                    "artifact_type": "loom.fanout-manifest.v1",
                    "producer": "platform",
                    "media_type": "application/json",
                    "relative_path": "artifact.json",
                    "role": "semantic_document",
                    "archive_format": "none",
                    "expected_max_bytes": 1024,
                    "expected_size": None,
                    "expected_sha256": None,
                },
            ],
        }

    async def upload_final_output_part(self, **kwargs: Any) -> dict[str, Any]:
        self.parts.append((kwargs["file_index"], kwargs["part_number"], kwargs["content"]))
        return {
            "file_index": kwargs["file_index"],
            "part_number": kwargs["part_number"],
            "size_bytes": len(kwargs["content"]),
            "sha256": kwargs["content_sha256"],
        }

    async def complete_final_output_file(self, **kwargs: Any) -> dict[str, Any]:
        self.completed.append(kwargs["file_index"])
        assert self.prepared is not None
        item = self.prepared["payload"]["files"][kwargs["file_index"]]
        return {
            "file_index": kwargs["file_index"],
            "size_bytes": item["size_bytes"],
            "sha256": item["sha256"],
            "state": "verified",
        }

    async def commit_final_output_session(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["session_id"] == self.session_id
        return {
            "upload_session_id": str(self.session_id),
            "state": "committed_ready",
            "manifest_sha256": "sha256:" + "c" * 64,
            "committed_marker_sha256": "sha256:" + "d" * 64,
        }

    async def abort_final_output_session(self, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError(f"unexpected abort: {kwargs}")


async def test_final_output_committer_streams_closed_inventory_and_returns_session(
    tmp_path: Path,
) -> None:
    output = tmp_path / "outputs" / "artifacts" / "rollout"
    (output / "payload").mkdir(parents=True)
    (output / "artifact.json").write_bytes(b'{"schema_version":"behavior_rollout_bundle.v1"}\n')
    (output / "payload" / "empty.bin").write_bytes(b"")
    (output / "payload" / "video.mp4").write_bytes(b"abcdefg")
    control = _FinalOutputControlPlane()
    execution = _claim().model_copy(update={"network_profile": "none"})
    committer = HttpFinalOutputCommitter(execution, control)  # type: ignore[arg-type]
    stage_result = {
        "schema_version": "loom.stage-result.v1",
        "domain_outcome": "complete",
        "reason_code": "completed",
        "retry_class": "none",
        "inputs": [],
        "outputs": [{"name": "rollout", "artifact_type": "behavior_rollout_bundle.v1"}],
        "metrics": {},
        "provenance": {
            "pipeline_run_id": str(UUID(int=1)),
            "stage_run_id": str(UUID(int=2)),
            "execution_attempt_id": str(UUID(int=3)),
            "recipe_digest": "sha256:" + "1" * 64,
            "execution_spec_digest": "sha256:" + "2" * 64,
            "image_digest": "sha256:" + "3" * 64,
        },
        "error": None,
    }
    stage_result_digest = digest_bytes(canonical_document(stage_result))

    result = await committer.commit(
        attempt_id=execution.execution_attempt_id,
        outputs_dir=tmp_path / "outputs",
        stage_result=stage_result,
        stage_result_digest=stage_result_digest,
    )

    assert result.upload_session_id == control.session_id
    assert result.manifest_digest == "sha256:" + "c" * 64
    assert control.prepared is not None
    files = control.prepared["payload"]["files"]
    assert [item["relative_path"] for item in files] == [
        "artifacts/rollout/artifact.json",
        "artifacts/rollout/payload/empty.bin",
        "artifacts/rollout/payload/video.mp4",
    ]
    assert control.completed == [0, 1, 2]
    assert [value for index, _part, value in control.parts if index == 1] == [b""]
    assert b"".join(value for index, _part, value in control.parts if index == 2) == b"abcdefg"
    assert committer.active_session_id is None


class _Container:
    labels: dict[str, str]

    def __init__(self, attempt_id: UUID) -> None:
        self.labels = {"loom.execution_attempt_id": str(attempt_id)}
        self.removed = False

    def remove(self, *, force: bool) -> None:
        assert force is True
        self.removed = True


class _Containers:
    def __init__(self, container: _Container) -> None:
        self.container = container

    def get(self, container_id: str) -> _Container:
        assert container_id == "container-1"
        return self.container


def test_cleanup_journal_reaps_only_exact_recorded_attempt(tmp_path: Path) -> None:
    execution = _claim()
    attempt_id = execution.execution_attempt_id
    journal = PipelineCleanupJournal(tmp_path / "journal")
    journal.record(execution, container_id="container-1")
    attempts = tmp_path / "attempts"
    inputs = tmp_path / "input-views"
    (attempts / str(attempt_id)).mkdir(parents=True)
    (inputs / str(attempt_id)).mkdir(parents=True)
    container = _Container(attempt_id)
    docker_client = SimpleNamespace(containers=_Containers(container))

    cleaned = journal.cleanup_orphans(
        docker_client=docker_client,
        attempts_root=attempts,
        input_views_root=inputs,
    )

    assert cleaned == [attempt_id]
    assert container.removed is True
    assert not (attempts / str(attempt_id)).exists()
    assert not (inputs / str(attempt_id)).exists()
    assert list((tmp_path / "journal").glob("*.json")) != []
    journal.clear(attempt_id)
    assert list((tmp_path / "journal").glob("*.json")) == []


def test_complete_marker_binds_exact_final_output_inventory(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    artifact = outputs / "artifacts" / "rollout"
    payload = artifact / "payload" / "video.mp4"
    payload.parent.mkdir(parents=True)
    artifact_document = b'{"schema_version":"behavior_rollout_bundle.v1"}\n'
    artifact_file = artifact / "artifact.json"
    artifact_file.write_bytes(artifact_document)
    payload.write_bytes(b"video")
    stage_result = canonical_document({"schema_version": "loom.stage-result.v1"})
    (outputs / "stage_result.json").write_bytes(stage_result)
    (outputs / "COMPLETE.json").write_bytes(
        canonical_document(
            {
                "schema_version": "loom.attempt-complete.v1",
                "idempotency_key": "attempt:1",
                "stage_result_sha256": digest_bytes(stage_result),
                "outputs": [
                    {
                        "name": "rollout",
                        "artifact_json_sha256": digest_bytes(artifact_document),
                        "files": [
                            {
                                "relative_path": "payload/video.mp4",
                                "sha256": digest_bytes(b"video"),
                                "size_bytes": 5,
                            }
                        ],
                    }
                ],
            }
        )
    )

    _validate_complete_marker(outputs, stage_result_digest=digest_bytes(stage_result))

    payload.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="payload_inventory_mismatch"):
        _validate_complete_marker(outputs, stage_result_digest=digest_bytes(stage_result))


def test_runtime_contract_is_claim_derived_exclusive_and_read_only(tmp_path: Path) -> None:
    execution = SimpleNamespace(
        image_runtime_contract_snapshot=SimpleNamespace(
            platform="linux/arm64",
            gpu_vendor="nvidia",
        ),
        worker_capability_snapshot=SimpleNamespace(
            gpu_devices=[
                SimpleNamespace(
                    device_uuid="GPU-gb10-1",
                    model="NVIDIA GB10",
                )
            ]
        ),
        slurm_gpu_allocation_evidence=SimpleNamespace(
            slurm_cluster_id="gb10",
            device_uuids=["GPU-gb10-1"],
        ),
        resource_profile_snapshot=SimpleNamespace(
            execution_variants=[
                SimpleNamespace(
                    variant_id="gb10-shared-1gpu",
                    device_roles=SimpleNamespace(sim_gpu_index=0, vla_gpu_index=0),
                )
            ]
        ),
        execution_spec_snapshot=SimpleNamespace(execution_variant_id="gb10-shared-1gpu"),
    )

    target = _install_runtime_contract(tmp_path, execution)  # type: ignore[arg-type]

    assert target.read_bytes() == canonical_document(
        {
            "platform": "gb10",
            "devices": [
                {
                    "logical_index": 0,
                    "model": "GB10",
                    "roles": ["sim", "vla"],
                }
            ],
            "system_env": {},
        }
    )
    assert target.stat().st_mode & 0o777 == 0o444
    with pytest.raises(FileExistsError):
        _install_runtime_contract(tmp_path, execution)  # type: ignore[arg-type]


def test_nonzero_container_exit_has_stable_secret_free_failure() -> None:
    proof = WorkerCleanupProofV1(
        container_absent=True,
        cgroup_empty=True,
        network_absent=True,
        step_jwt_revoked=True,
        runtime_secret_mount_absent=True,
        scratch_absent=True,
        outputs_absent=True,
        input_views_absent=True,
        active_upload_session_ids=[],
    )
    failure = _execution_failure(PipelineProcessFailedError(17), resources=proof)
    assert failure.exit_code == 17
    assert failure.reason_code == "container_exit_nonzero"
    assert failure.retry_class.value == "internal_defect"
    assert failure.teardown_observed is True
    assert failure.resources == proof


class _AbsentBackend:
    container_absent = True

    async def expected_process_group_present(self, *, attempt_id: UUID) -> bool:
        del attempt_id
        return False


async def test_failure_cleanup_proof_is_built_only_after_local_removal(
    tmp_path: Path,
) -> None:
    execution = _claim().model_copy(update={"network_profile": "none"})
    root = tmp_path / "attempt"
    outputs = root / "outputs"
    scratch = root / "scratch"
    outputs.mkdir(parents=True)
    scratch.mkdir()
    (outputs / "partial").write_bytes(b"not committed")
    paths = PipelineAttemptPaths(
        root=root,
        inputs=tmp_path / "absent-input-view",
        outputs=outputs,
        scratch=scratch,
    )
    proof = await _observed_cleanup_proof(
        claim=execution,
        paths=paths,
        backend=_AbsentBackend(),  # type: ignore[arg-type]
        committer=SimpleNamespace(active_session_id=None),  # type: ignore[arg-type]
    )

    assert proof.active_upload_session_ids == []
    assert not outputs.exists()
    assert not scratch.exists()


async def test_failure_cleanup_proof_rejects_active_upload(tmp_path: Path) -> None:
    execution = _claim().model_copy(update={"network_profile": "none"})
    root = tmp_path / "attempt"
    outputs = root / "outputs"
    scratch = root / "scratch"
    outputs.mkdir(parents=True)
    scratch.mkdir()

    with pytest.raises(RuntimeError, match="cleanup_proof_incomplete"):
        await _observed_cleanup_proof(
            claim=execution,
            paths=PipelineAttemptPaths(
                root=root,
                inputs=tmp_path / "absent-input-view",
                outputs=outputs,
                scratch=scratch,
            ),
            backend=_AbsentBackend(),  # type: ignore[arg-type]
            committer=SimpleNamespace(active_session_id=UUID(int=99)),  # type: ignore[arg-type]
        )
