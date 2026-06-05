from datetime import UTC, datetime
from uuid import uuid4

from loom.models.trajectory import (
    EnvExecEvent,
    EnvReadyEvent,
    EnvStartEvent,
    EnvStopEvent,
    EventKind,
    FileDownloadEvent,
    FileUploadEvent,
    StepEndEvent,
    StepStartEvent,
    TrialCancelledEvent,
    TrialEndEvent,
    TrialErrorEvent,
)


def _env(**overrides):
    base = {
        "emitted_at": datetime.now(UTC),
        "trial_id": uuid4(),
        "step_id": "main",
        "seq": 0,
    }
    base.update(overrides)
    return base


def test_trial_end_event():
    e = TrialEndEvent(**_env(), final_state="succeeded", reward={"passed": 1.0})
    assert e.kind == EventKind.TRIAL_END


def test_trial_error_event():
    e = TrialErrorEvent(**_env(), error_type="DriverError", message="boom", traceback="t")
    assert e.kind == EventKind.TRIAL_ERROR


def test_trial_cancelled_event():
    e = TrialCancelledEvent(
        **_env(),
        cancellation_requested_at=datetime.now(UTC),
        observed_at=datetime.now(UTC),
    )
    assert e.kind == EventKind.TRIAL_CANCELLED


def test_step_lifecycle_events():
    s = StepStartEvent(**_env(), instruction_excerpt="Do thing")
    assert s.kind == EventKind.STEP_START
    e = StepEndEvent(**_env(), summary={"reward": 1.0})
    assert e.kind == EventKind.STEP_END


def test_env_lifecycle_events():
    a = EnvStartEvent(**_env(), image_ref="alpine:3.19", build_time_sec=4.2)
    assert a.kind == EventKind.ENV_START
    b = EnvReadyEvent(**_env(), healthcheck_attempts=2)
    assert b.kind == EventKind.ENV_READY
    c = EnvStopEvent(**_env(), duration_sec=80.0, exit_status=0)
    assert c.kind == EventKind.ENV_STOP


def test_env_exec_event():
    e = EnvExecEvent(
        **_env(),
        cmd="ls -la",
        user="root",
        cwd="/workspace",
        return_code=0,
        stdout_bytes=128,
        stderr_bytes=0,
        truncated=False,
        duration_sec=0.05,
    )
    assert e.return_code == 0


def test_file_events():
    u = FileUploadEvent(**_env(), src_size_bytes=1024, dst_path="/x/y", duration_sec=0.1)
    assert u.kind == EventKind.FILE_UPLOAD
    d = FileDownloadEvent(**_env(), src_path="/x/y", dst_size_bytes=1024, duration_sec=0.1)
    assert d.kind == EventKind.FILE_DOWNLOAD
