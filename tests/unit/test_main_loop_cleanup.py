"""Regression: _spawn_trial's per-trial tempdir is cleaned up after the
trial body finishes — both on success and on exception.

Bug 4 from the post-Plan-7 review: long-running workers leak one mkdtemp
dir per claim until the host runs out of inodes / the trajectory PV
fills up.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path, PurePosixPath
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest

from loom.models.result import FailureReason
from loom.models.trial import BackoffSpec, RetryPolicy, RetryReason
from loom.verifier.script_verifier import ScriptVerifier
from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool


def test_runtime_identity_labels_allow_legacy_settings() -> None:
    """Pre-containment workers remain valid and intentionally unlabelled."""
    assert ml._runtime_identity_labels(_FakeSettings()) == ()  # type: ignore[arg-type]
    assert ml._slurm_gpu_device_ids(_FakeSettings()) == ()  # type: ignore[arg-type]
    assert ml._worker_cgroup_parent(_FakeSettings()) is None  # type: ignore[arg-type]


def test_required_worker_cgroup_parent_fails_closed_when_missing() -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = ""

    with pytest.raises(RuntimeError, match="requires an allocation cgroup"):
        ml._worker_cgroup_parent(settings)  # type: ignore[arg-type]


def test_required_worker_cgroup_parent_accepts_slurm_scope() -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = "/system.slice/slurmstepd.scope/job_123"
    settings.slurm_job_id = "123"

    assert ml._worker_cgroup_parent(settings) == ("/system.slice/slurmstepd.scope/job_123")  # type: ignore[arg-type]


def test_required_worker_cgroup_parent_accepts_guard_slice() -> None:
    # Docker's systemd driver takes the guard-owned slice unit, not a path.
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = "loom-job-123.slice"
    settings.slurm_job_id = "123"

    assert ml._worker_cgroup_parent(settings) == "loom-job-123.slice"  # type: ignore[arg-type]


def test_required_worker_cgroup_parent_accepts_guard_slice_for_array_base() -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = "loom-job-123.slice"
    settings.slurm_job_id = "123_7"

    assert ml._worker_cgroup_parent(settings) == "loom-job-123.slice"  # type: ignore[arg-type]


def test_required_worker_cgroup_parent_rejects_mismatched_slice() -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = "loom-job-456.slice"
    settings.slurm_job_id = "123"

    with pytest.raises(RuntimeError, match="slice does not match the Slurm job ID"):
        ml._worker_cgroup_parent(settings)  # type: ignore[arg-type]


def test_required_worker_cgroup_parent_rejects_missing_slurm_job_id() -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = "/system.slice/slurmstepd.scope/job_123"
    settings.slurm_job_id = ""

    with pytest.raises(RuntimeError, match="needs a valid Slurm job ID"):
        ml._worker_cgroup_parent(settings)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("parent", "job_id", "match"),
    [
        (
            "/system.slice/slurmstepd.scope/job_456",
            "123",
            "does not match the Slurm job ID",
        ),
        (
            "/evil/job_123",
            "123",
            "no identifiable Slurm scope",
        ),
        (
            "/system.slice/slurmstepd.scope/step_batch",
            "123",
            "no job scope after the Slurm marker",
        ),
        (
            "/evil/job_123/system.slice/slurmstepd.scope",
            "123",
            "no job scope after the Slurm marker",
        ),
    ],
)
def test_required_worker_cgroup_parent_rejects_unbound_scope(
    parent: str,
    job_id: str,
    match: str,
) -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = parent
    settings.slurm_job_id = job_id

    with pytest.raises(RuntimeError, match=match):
        ml._worker_cgroup_parent(settings)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("parent", "job_id"),
    [
        ("/system.slice/slurmstepd.scope/job_123_4", "123_4"),
        ("/system.slice/slurmstepd.scope/job_123", "123_4"),
    ],
)
def test_required_worker_cgroup_parent_accepts_array_job_binding(
    parent: str,
    job_id: str,
) -> None:
    settings = _FakeSettings()
    settings.require_cgroup_parent = True
    settings.cgroup_parent = parent
    settings.slurm_job_id = job_id

    assert ml._worker_cgroup_parent(settings) == parent  # type: ignore[arg-type]


def test_setup_failure_classifier_recognizes_task_image_build_timeout() -> None:
    detail = (
        "building Docker image 'loom-task:405adf85aa0c5227b5fdf74f916f6b9c' "
        "from 'environment/Dockerfile' exceeded 1800s"
    )

    assert ml._classify_setup_failure(detail) == FailureReason.TASK_IMAGE_BUILD_TIMEOUT


def test_setup_failure_classifier_recognizes_task_dns_compatibility() -> None:
    detail = (
        "failed to build layered image 'loom-trial-cache:abc123': "
        "The command '/bin/sh -c bash /tmp/install.sh' returned a non-zero code: 2\n"
        "build log (last 20 lines):\n"
        "Ign:1 http://deb.debian.org/debian bookworm InRelease\n"
        "Err:1 http://deb.debian.org/debian bookworm InRelease\n"
        "  Temporary failure resolving 'deb.debian.org'\n"
    )

    assert ml._classify_setup_failure(detail) == FailureReason.TASK_COMPATIBILITY


def test_setup_failure_classifier_recognizes_preflight_task_compatibility() -> None:
    detail = (
        "TASK_COMPAT_DNS_MUTATION environment/Dockerfile:3: "
        "Dockerfile mutates DNS/NSS configuration before Loom installs the "
        "service-mode agent layer."
    )

    assert ml._classify_setup_failure(detail) == FailureReason.TASK_COMPATIBILITY


def test_setup_failure_classifier_recognizes_node_health_guard() -> None:
    detail = (
        "SETUP_ADMISSION_BLOCKED reason=node_io_pressure "
        "operation=trial-cache-build io.full.avg10=76.15 max=50.0"
    )

    assert ml._classify_setup_failure(detail) == FailureReason.NODE_SETUP_HEALTH


async def test_setup_failure_patch_uses_task_image_build_timeout_reason() -> None:
    cp = _FakeCPClient()
    trial_id = uuid4()
    worker_id = uuid4()

    await ml._mark_setup_failed(
        cp_client=cp,  # type: ignore[arg-type]
        trial_id=trial_id,
        worker_id=worker_id,
        detail=(
            "building Docker image 'loom-task:405adf85aa0c5227b5fdf74f916f6b9c' "
            "from 'environment/Dockerfile' exceeded 1800s"
        ),
    )

    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.TASK_IMAGE_BUILD_TIMEOUT.value,
            "failure_message": (
                "building Docker image 'loom-task:405adf85aa0c5227b5fdf74f916f6b9c' "
                "from 'environment/Dockerfile' exceeded 1800s"
            ),
        }
    ]


async def test_setup_failure_patch_preserves_long_docker_build_error_tail() -> None:
    cp = _FakeCPClient()
    trial_id = uuid4()
    worker_id = uuid4()
    decisive_error = "dpkg: error processing package nodejs (--configure): post-installation script returned error exit status 1"
    build_lines = [f"Removing noisy-package-{idx} (1.0) ..." for idx in range(120)]
    build_lines.append(decisive_error)
    detail = (
        "failed to build layered image 'loom-trial-cache:abc123': "
        "The command '/bin/sh -c bash /tmp/install.sh' returned a non-zero code: 1\n"
        "build log (last 121 lines):\n" + "\n".join(build_lines)
    )

    await ml._mark_setup_failed(
        cp_client=cp,  # type: ignore[arg-type]
        trial_id=trial_id,
        worker_id=worker_id,
        detail=detail,
    )

    failure_message = str(cp.patch_calls[0]["failure_message"])
    assert len(failure_message) <= 1000
    assert "failed to build layered image" in failure_message
    assert decisive_error in failure_message
    assert "Removing noisy-package-0" not in failure_message


class _FakeCPClient:
    def __init__(self) -> None:
        self.patch_calls: list[dict[str, object]] = []
        self.retry_calls: list[dict[str, object]] = []
        self.pre_start_heartbeat_calls: list[dict[str, object]] = []
        self.bundle = {
            "id": "fake",
            "checksum": "0" * 64,
            "config": {
                "schema_version": "1",
                "task": {"id": "fake", "name": "fake"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            "source": None,
        }

    async def get_task_bundle(self, _task_id: str) -> dict:
        return self.bundle

    async def get_trial_llm_calls(self, _trial_id) -> list:  # type: ignore[no-untyped-def]
        return []

    async def pre_start_heartbeat(self, *, trial_id, worker_id) -> bool:  # type: ignore[no-untyped-def]
        self.pre_start_heartbeat_calls.append(
            {
                "trial_id": trial_id,
                "worker_id": worker_id,
            }
        )
        return True

    async def patch_state(
        self,
        *,
        trial_id,
        worker_id,
        state: str,
        failure_reason: str | None = None,
        failure_message: str | None = None,
    ) -> bool:  # type: ignore[no-untyped-def]
        patch = {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": state,
            "failure_reason": failure_reason,
        }
        if failure_message is not None:
            patch["failure_message"] = failure_message
        self.patch_calls.append(patch)
        return True

    async def requeue_trial_retry(
        self,
        *,
        trial_id,
        worker_id,
        failure_reason: str,
        failure_message: str | None,
        retry_after_sec: float,
    ) -> bool:  # type: ignore[no-untyped-def]
        self.retry_calls.append(
            {
                "trial_id": trial_id,
                "worker_id": worker_id,
                "failure_reason": failure_reason,
                "failure_message": failure_message,
                "retry_after_sec": retry_after_sec,
            }
        )
        return True


class _RetryRaisingCPClient(_FakeCPClient):
    async def requeue_trial_retry(
        self,
        *,
        trial_id,
        worker_id,
        failure_reason: str,
        failure_message: str | None,
        retry_after_sec: float,
    ) -> bool:  # type: ignore[no-untyped-def]
        self.retry_calls.append(
            {
                "trial_id": trial_id,
                "worker_id": worker_id,
                "failure_reason": failure_reason,
                "failure_message": failure_message,
                "retry_after_sec": retry_after_sec,
            }
        )
        request = httpx.Request("POST", f"http://cp/trials/{trial_id}/retry")
        raise httpx.ConnectError("control-plane retry report failed", request=request)


class _RetryFalseCPClient(_FakeCPClient):
    async def requeue_trial_retry(
        self,
        *,
        trial_id,
        worker_id,
        failure_reason: str,
        failure_message: str | None,
        retry_after_sec: float,
    ) -> bool:  # type: ignore[no-untyped-def]
        self.retry_calls.append(
            {
                "trial_id": trial_id,
                "worker_id": worker_id,
                "failure_reason": failure_reason,
                "failure_message": failure_message,
                "retry_after_sec": retry_after_sec,
            }
        )
        return False


class _Bundle404CPClient(_FakeCPClient):
    async def get_task_bundle(self, task_id: str) -> dict:
        request = httpx.Request(
            "GET",
            f"http://cp/tasks/{task_id}/bundle",
        )
        response = httpx.Response(
            404,
            request=request,
            json={"detail": "task not found"},
        )
        raise httpx.HTTPStatusError(
            "404 task bundle not found",
            request=request,
            response=response,
        )


class _BlockingBundleCPClient(_FakeCPClient):
    def __init__(self) -> None:
        super().__init__()
        self.bundle_requested = asyncio.Event()
        self.release_bundle = asyncio.Event()

    async def get_task_bundle(self, _task_id: str) -> dict:
        self.bundle_requested.set()
        await self.release_bundle.wait()
        return self.bundle


class _BlockingFailingBundleCPClient(_FakeCPClient):
    def __init__(self) -> None:
        super().__init__()
        self.bundle_requested = asyncio.Event()
        self.release_bundle = asyncio.Event()

    async def get_task_bundle(self, task_id: str) -> dict:
        self.bundle_requested.set()
        await self.release_bundle.wait()
        request = httpx.Request(
            "GET",
            f"http://cp/tasks/{task_id}/bundle",
        )
        response = httpx.Response(
            500,
            request=request,
            json={"detail": "storage timeout"},
        )
        raise httpx.HTTPStatusError(
            "500 task bundle unavailable",
            request=request,
            response=response,
        )


class _FakeSettings:
    trajectory_cache_dir = Path("/tmp/loom-test-cleanup-cache")
    gateway_url = "http://gw:9100"
    fixtures_root = None  # disables the fixture:// resolver path
    benchmark_cache = None  # use HF's default cache
    task_materialize_timeout_sec = 300.0
    # Phase D: required even when isolation is off because the worker
    # reads it to construct the LocalTrialRunner.
    sandbox_step_jwt_ttl_sec = 600
    docker_api_timeout_sec = 1800
    # #896: per-container caps threaded into LocalTrialRunner (0 = unbounded).
    container_cpus = 0.0
    container_memory_mib = 0
    container_pids = 0
    # #360 / #378: cancellation watchdog config. Fast poll and permissive
    # deadline so tests don't trigger the backstop.
    trial_cancel_poll_interval_sec = 0.05
    trial_hard_deadline_multiplier = 3.0
    trial_hard_deadline_grace_sec = 600.0
    pre_start_heartbeat_interval_sec = 0.01
    # Shared with control-plane / loom-service bucket settings (#1100).
    trajectories_bucket = "trajectories"
    artifacts_bucket = "artifacts"


async def _drive_spawn(runner_target: object) -> Path:
    """Spawn one fake trial through _spawn_trial; capture the task_dir
    it created (via the patched mkdtemp), return its Path so the caller
    can assert it was cleaned up."""
    captured: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def capture_mkdtemp(**kwargs: str) -> str:
        d = real_mkdtemp(**kwargs)
        captured.append(Path(d))
        return d

    settings = _FakeSettings()
    cp = _FakeCPClient()
    payload = {
        "trial_id": str(uuid4()),
        "team_id": str(uuid4()),
        "task_id": "fake",
        # Plan 23: TrialConfig requires agent_name + agent_model.
        # `agent_model: None` is allowed for agents that don't call an LLM.
        "config": {"agent_name": "oracle", "agent_model": None},
    }

    pool = RunnerPool(max_concurrent=1)

    with (
        patch.object(ml, "tempfile") as fake_tempfile,
        patch.object(ml, "LocalTrialRunner") as fake_runner_cls,
    ):
        fake_tempfile.mkdtemp.side_effect = capture_mkdtemp
        fake_runner_cls.return_value = runner_target
        from loom_worker.vllm_registry import WorkerVLLMRegistry

        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload=payload,
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    assert captured, "_spawn_trial should have called tempfile.mkdtemp once"
    return captured[0]


class _SucceedingRunner:
    async def run(self) -> None:
        return None


class _FailingRunner:
    async def run(self) -> None:
        raise RuntimeError("simulated agent error")


async def test_tempdir_cleaned_on_success() -> None:
    task_dir = await _drive_spawn(_SucceedingRunner())
    assert not task_dir.exists(), f"task_dir {task_dir} leaked after a successful trial"


async def test_tempdir_cleaned_on_runner_exception() -> None:
    task_dir = await _drive_spawn(_FailingRunner())
    assert not task_dir.exists(), f"task_dir {task_dir} leaked after a failing trial"


async def test_runner_exception_marks_claimed_trial_failed() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _FakeCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    with patch.object(ml, "LocalTrialRunner") as fake_runner_cls:
        fake_runner_cls.return_value = _FailingRunner()
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    assert {
        "trial_id": trial_id,
        "worker_id": worker_id,
        "state": "failed",
        "failure_reason": FailureReason.INTERNAL_ERROR.value,
        "failure_message": "simulated agent error",
    } in cp.patch_calls


async def test_spawn_uses_script_verifier_from_task_config() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _FakeCPClient()
    cp.bundle["config"]["verifier"] = {
        "name": "script",
        "args": {"script_path": "/loom/verifier/run.sh"},
    }
    pool = RunnerPool(max_concurrent=1)
    captured: dict[str, object] = {}

    class _CapturingRunner:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def run(self) -> None:
            return None

    with patch.object(ml, "LocalTrialRunner", _CapturingRunner):
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload={
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    verifier_factory = captured["verifier_factory"]
    verifier = verifier_factory()  # type: ignore[operator]
    assert isinstance(verifier, ScriptVerifier)
    assert verifier.script_path == PurePosixPath("/loom/verifier/run.sh")


async def test_spawn_uses_resolved_task_image_for_dockerfile_task() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _FakeCPClient()
    cp.bundle["config"]["environment"] = {
        "os": "linux",
        "dockerfile": "environment/Dockerfile",
    }
    pool = RunnerPool(max_concurrent=1)
    captured: dict[str, object] = {}

    class _CapturingRunner:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def run(self) -> None:
            return None

    async def fake_resolve_task_image(**kwargs: object) -> str:
        captured["resolve_kwargs"] = kwargs
        return "loom-task:resolved"

    with (
        patch.object(ml, "LocalTrialRunner", _CapturingRunner),
        patch.object(ml, "resolve_task_image", fake_resolve_task_image),
    ):
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload={
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    driver_factory = captured["driver_factory"]
    assert callable(driver_factory)
    driver = driver_factory()
    assert driver.image == "loom-task:resolved"
    resolve_kwargs = captured["resolve_kwargs"]
    assert resolve_kwargs["task_checksum"] == "0" * 64
    assert resolve_kwargs["task_config"].environment.dockerfile.as_posix() == (
        "environment/Dockerfile"
    )


async def test_task_image_setup_failure_records_diagnostic_message() -> None:
    from loom_worker.task_image import TaskImageBuildError
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _FakeCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    async def fail_resolve_task_image(**_kwargs: object) -> str:
        raise TaskImageBuildError("Docker build context exceeds operator file limit")

    with patch.object(ml, "resolve_task_image", fail_resolve_task_image):
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    assert {
        "trial_id": trial_id,
        "worker_id": worker_id,
        "state": "failed",
        "failure_reason": FailureReason.INTERNAL_ERROR.value,
        "failure_message": "Docker build context exceeds operator file limit",
    } in cp.patch_calls


async def test_long_setup_failure_writes_redacted_diagnostic_file(
    tmp_path: Path,
) -> None:
    from loom_worker.task_image import TaskImageBuildError
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    settings.trajectory_cache_dir = tmp_path / "trajectory-cache"
    cp = _FakeCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()
    decisive_error = (
        "dpkg: error processing package nodejs (--configure): "
        "post-installation script returned error exit status 1"
    )
    full_detail = (
        "failed to build Docker image 'loom-task:abc123' from "
        "'environment/Dockerfile': The command '/bin/sh -c apt-get install nodejs' "
        "returned a non-zero code: 1\n"
        "build log (last 200 lines):\n"
        "Bearer super-secret-token\n"
        + "\n".join(f"Removing noisy-package-{idx} (1.0) ..." for idx in range(160))
        + f"\n{decisive_error}\n"
        + "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    )

    async def fail_resolve_task_image(**_kwargs: object) -> str:
        raise TaskImageBuildError(full_detail)

    with patch.object(ml, "resolve_task_image", fail_resolve_task_image):
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    diagnostic_path = settings.trajectory_cache_dir / "setup-diagnostics" / f"{trial_id}.log"
    failure_message = str(cp.patch_calls[0]["failure_message"])
    assert len(failure_message) <= 1000
    assert str(diagnostic_path) in failure_message
    artifact_text = diagnostic_path.read_text(encoding="utf-8")
    assert decisive_error in artifact_text
    assert "Bearer super-secret-token" not in artifact_text
    assert "Bearer [REDACTED:bearer]" in artifact_text
    assert "hf_abcdefghijklmnopqrstuvwxyz1234567890" not in artifact_text
    assert "[REDACTED:hf-token]" in artifact_text


async def test_setup_transport_disconnect_requeues_when_trial_policy_allows() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _FakeCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    async def fail_layered_image(**_kwargs: object) -> str:
        raise httpx.RemoteProtocolError("Server disconnected without sending a response.")

    with patch.object(ml, "_resolve_layered_trial_image", fail_layered_image):
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "attempt_count": 1,
                "config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "retry": {
                        "max_attempts": 3,
                        "retry_on": ["provider_transport_disconnect"],
                        "backoff": {
                            "base_sec": 30,
                            "max_sec": 600,
                            "multiplier": 2.0,
                            "jitter": 0.0,
                        },
                    },
                },
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    assert cp.patch_calls == []
    assert cp.retry_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "failure_reason": FailureReason.PROVIDER_TRANSPORT_DISCONNECT.value,
            "failure_message": (
                "Provider transport disconnected before returning a response. "
                "Server disconnected without sending a response."
            ),
            "retry_after_sec": 30.0,
        }
    ]


async def test_setup_node_health_requeues_without_user_retry_budget() -> None:
    cp = _FakeCPClient()
    trial_id = uuid4()
    worker_id = uuid4()

    await ml._mark_setup_failed(
        cp_client=cp,  # type: ignore[arg-type]
        trial_id=trial_id,
        worker_id=worker_id,
        detail=(
            "SETUP_ADMISSION_BLOCKED reason=node_io_pressure "
            "operation=setup-build io.full.avg10=70 max=50"
        ),
        retry_policy=RetryPolicy(
            max_attempts=1,
            retry_on=frozenset(),
            backoff=BackoffSpec(
                base_sec=30,
                max_sec=600,
                multiplier=2.0,
                jitter=0.0,
            ),
        ),
        attempt_count=1,
    )

    assert cp.patch_calls == []
    assert cp.retry_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "failure_reason": FailureReason.NODE_SETUP_HEALTH.value,
            "failure_message": (
                "SETUP_ADMISSION_BLOCKED reason=node_io_pressure "
                "operation=setup-build io.full.avg10=70 max=50"
            ),
            "retry_after_sec": 30.0,
        }
    ]


async def test_setup_retry_report_exception_falls_back_to_terminal_patch() -> None:
    cp = _RetryRaisingCPClient()
    trial_id = uuid4()
    worker_id = uuid4()

    await ml._mark_setup_failed(
        cp_client=cp,  # type: ignore[arg-type]
        trial_id=trial_id,
        worker_id=worker_id,
        detail="Server disconnected without sending a response.",
        retry_policy=RetryPolicy(
            max_attempts=3,
            retry_on=frozenset({RetryReason.PROVIDER_TRANSPORT_DISCONNECT}),
            backoff=BackoffSpec(base_sec=30, max_sec=600, multiplier=2.0, jitter=0.0),
        ),
        attempt_count=1,
    )

    assert len(cp.retry_calls) == 1
    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.PROVIDER_TRANSPORT_DISCONNECT.value,
            "failure_message": (
                "Provider transport disconnected before returning a response. "
                "Server disconnected without sending a response."
            ),
        }
    ]


async def test_setup_retry_report_false_falls_back_to_terminal_patch() -> None:
    cp = _RetryFalseCPClient()
    trial_id = uuid4()
    worker_id = uuid4()

    await ml._mark_setup_failed(
        cp_client=cp,  # type: ignore[arg-type]
        trial_id=trial_id,
        worker_id=worker_id,
        detail="Server disconnected without sending a response.",
        retry_policy=RetryPolicy(
            max_attempts=3,
            retry_on=frozenset({RetryReason.PROVIDER_TRANSPORT_DISCONNECT}),
            backoff=BackoffSpec(base_sec=30, max_sec=600, multiplier=2.0, jitter=0.0),
        ),
        attempt_count=1,
    )

    assert len(cp.retry_calls) == 1
    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.PROVIDER_TRANSPORT_DISCONNECT.value,
            "failure_message": (
                "Provider transport disconnected before returning a response. "
                "Server disconnected without sending a response."
            ),
        }
    ]


async def test_tempdir_cleaned_on_cancellation() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    class _SlowRunner:
        async def run(self) -> None:
            await asyncio.sleep(10.0)

    settings = _FakeSettings()
    cp = _FakeCPClient()
    captured: list[Path] = []
    real_mkdtemp = tempfile.mkdtemp

    def capture_mkdtemp(**kwargs: str) -> str:
        d = real_mkdtemp(**kwargs)
        captured.append(Path(d))
        return d

    pool = RunnerPool(max_concurrent=1)
    with (
        patch.object(ml, "tempfile") as fake_tempfile,
        patch.object(ml, "LocalTrialRunner") as fake_runner_cls,
    ):
        fake_tempfile.mkdtemp.side_effect = capture_mkdtemp
        fake_runner_cls.return_value = _SlowRunner()
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=uuid4(),
            payload={
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": "fake",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await asyncio.sleep(0.05)
        pool.cancel_all()
        await pool.wait_all(timeout=2.0)

    assert captured
    assert not captured[0].exists(), f"task_dir {captured[0]} leaked after cancellation"


async def test_bundle_lookup_failure_marks_trial_failed_without_spawning() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _Bundle404CPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    await ml._spawn_trial(
        pool=pool,
        settings=settings,  # type: ignore[arg-type]
        cp_client=cp,  # type: ignore[arg-type]
        gateway_client=None,  # type: ignore[arg-type]
        object_store=None,  # type: ignore[arg-type]
        worker_id=worker_id,
        payload={
            "trial_id": str(trial_id),
            "team_id": str(uuid4()),
            "task_id": "humaneval/HumanEval/26",
            "config": {"agent_name": "oracle", "agent_model": None},
        },
        vllm_registry=WorkerVLLMRegistry(enabled=False),
    )

    await pool.wait_all(timeout=0.1)
    assert pool.in_flight == 0
    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.INTERNAL_ERROR.value,
            "failure_message": "404 task bundle not found",
        }
    ]


async def test_claimed_trial_counts_in_flight_before_setup_finishes() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _BlockingBundleCPClient()
    pool = RunnerPool(max_concurrent=1)

    with patch.object(ml, "LocalTrialRunner") as fake_runner_cls:
        fake_runner_cls.return_value = _SucceedingRunner()
        spawn_task = asyncio.create_task(
            ml._spawn_trial(
                pool=pool,
                settings=settings,  # type: ignore[arg-type]
                cp_client=cp,  # type: ignore[arg-type]
                gateway_client=None,  # type: ignore[arg-type]
                object_store=None,  # type: ignore[arg-type]
                worker_id=uuid4(),
                payload={
                    "trial_id": str(uuid4()),
                    "team_id": str(uuid4()),
                    "task_id": "fake",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
                vllm_registry=WorkerVLLMRegistry(enabled=False),
            )
        )
        await cp.bundle_requested.wait()
        assert pool.in_flight == 1
        cp.release_bundle.set()
        await spawn_task
        await pool.wait_all(timeout=2.0)


async def test_pre_start_heartbeat_runs_while_setup_waits() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _BlockingBundleCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    with patch.object(ml, "LocalTrialRunner") as fake_runner_cls:
        fake_runner_cls.return_value = _SucceedingRunner()
        spawn_task = asyncio.create_task(
            ml._spawn_trial(
                pool=pool,
                settings=settings,  # type: ignore[arg-type]
                cp_client=cp,  # type: ignore[arg-type]
                gateway_client=None,  # type: ignore[arg-type]
                object_store=None,  # type: ignore[arg-type]
                worker_id=worker_id,
                payload={
                    "trial_id": str(trial_id),
                    "team_id": str(uuid4()),
                    "task_id": "fake",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
                vllm_registry=WorkerVLLMRegistry(enabled=False),
            )
        )
        await cp.bundle_requested.wait()
        for _ in range(100):
            if cp.pre_start_heartbeat_calls:
                break
            await asyncio.sleep(0.005)
        assert cp.pre_start_heartbeat_calls == [{"trial_id": trial_id, "worker_id": worker_id}]
        cp.release_bundle.set()
        await spawn_task
        await pool.wait_all(timeout=2.0)


async def test_setup_failure_inside_pool_marks_claimed_trial_failed() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    settings = _FakeSettings()
    cp = _BlockingFailingBundleCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    spawn_task = asyncio.create_task(
        ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "humaneval/HumanEval/26",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
    )

    await cp.bundle_requested.wait()
    assert pool.in_flight == 1
    cp.release_bundle.set()
    await spawn_task
    await pool.wait_all(timeout=2.0)

    assert pool.in_flight == 0
    assert cp.patch_calls == [
        {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "state": "failed",
            "failure_reason": FailureReason.INTERNAL_ERROR.value,
            "failure_message": "500 task bundle unavailable",
        }
    ]


async def test_materialize_timeout_marks_claimed_trial_failed() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    class _HangingMaterializer:
        def matches(self, source: str | None) -> bool:
            return source == "hf://PRHW/private@rev/task-1"

        async def materialize(
            self,
            *,
            source: str,
            task_dir: Path,
            trial_id,  # type: ignore[no-untyped-def]
        ) -> Path:
            await asyncio.Event().wait()
            return task_dir

    settings = _FakeSettings()
    settings.task_materialize_timeout_sec = 0.01
    cp = _FakeCPClient()
    cp.bundle["source"] = "hf://PRHW/private@rev/task-1"
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    with patch.object(ml, "build_default_materializers") as build_materializers:
        build_materializers.return_value = (_HangingMaterializer(),)
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "skilllearnbench/private-task",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        try:
            await pool.wait_all(timeout=1.0)
            assert pool.in_flight == 0
        finally:
            pool.cancel_all()
            await pool.wait_all(timeout=1.0)

    assert len(cp.patch_calls) == 1
    patch_call = cp.patch_calls[0]
    assert patch_call["trial_id"] == trial_id
    assert patch_call["worker_id"] == worker_id
    assert patch_call["state"] == "failed"
    assert patch_call["failure_reason"] == FailureReason.INTERNAL_ERROR.value
    failure_message = str(patch_call["failure_message"])
    assert "task materialization timed out after 0.01s" in failure_message
    assert "source_scheme=hf" in failure_message
    assert "PRHW/private" not in failure_message


async def test_setup_failure_redacts_secret_detail_before_state_patch() -> None:
    from loom_worker.vllm_registry import WorkerVLLMRegistry

    async def fail_materialize(**_kwargs: object) -> Path:
        raise RuntimeError(
            "hf auth failed with Authorization: Bearer hf_abcdefghijklmnopqrstuvwxyz1234567890"
        )

    settings = _FakeSettings()
    cp = _FakeCPClient()
    pool = RunnerPool(max_concurrent=1)
    trial_id = uuid4()
    worker_id = uuid4()

    with patch.object(ml, "_materialize_task_dir", fail_materialize):
        await ml._spawn_trial(
            pool=pool,
            settings=settings,  # type: ignore[arg-type]
            cp_client=cp,  # type: ignore[arg-type]
            gateway_client=None,  # type: ignore[arg-type]
            object_store=None,  # type: ignore[arg-type]
            worker_id=worker_id,
            payload={
                "trial_id": str(trial_id),
                "team_id": str(uuid4()),
                "task_id": "skilllearnbench/private-task",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            vllm_registry=WorkerVLLMRegistry(enabled=False),
        )
        await pool.wait_all(timeout=2.0)

    assert len(cp.patch_calls) == 1
    failure_message = str(cp.patch_calls[0]["failure_message"])
    assert "hf_abcdefghijklmnopqrstuvwxyz1234567890" not in failure_message
    assert "Bearer [REDACTED:bearer]" in failure_message


async def test_runtime_bucket_bootstrap_creates_required_runtime_buckets() -> None:
    class _RuntimeBucketStore:
        def __init__(self) -> None:
            self.created: list[str] = []

        async def ensure_bucket(self, bucket: str) -> None:
            if bucket not in self.created:
                self.created.append(bucket)

    store = _RuntimeBucketStore()

    await ml._ensure_runtime_buckets(store)
    await ml._ensure_runtime_buckets(store)

    assert store.created == ["trajectories", "artifacts"]


# Suppress pytest's "unused" warning on the helper.
_ = pytest
