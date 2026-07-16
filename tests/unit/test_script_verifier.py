import json
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.verifier.script_verifier import ScriptVerifier


def _ok_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
    return ExecResult(
        return_code=0,
        stdout=b"",
        stderr=b"",
        truncated=False,
        duration_sec=0.01,
    )


@pytest.fixture
async def fake_with_script_output() -> FakeDriver:
    fake = FakeDriver(exec_handler=_ok_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = json.dumps(
        {
            "rewards": {"score": 0.85},
            "checks": [
                {"name": "linter", "passed": True, "score": 1.0},
                {
                    "name": "format",
                    "passed": False,
                    "score": 0.0,
                    "message": "bad indent",
                    "detail": {"exit_code": 1},
                },
            ],
        }
    ).encode()
    return fake


async def test_script_verifier_reads_output(fake_with_script_output: FakeDriver):
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))
    result = await v.verify(
        task=None,  # type: ignore[arg-type]
        env=fake_with_script_output,
        artifacts_dir=PurePosixPath("/loom/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.rewards == {"score": 0.85}
    assert len(result.checks) == 2
    assert result.checks[1].name == "format"
    assert result.checks[1].detail == {"exit_code": 1}


async def test_script_verifier_missing_output_returns_error(tmp_path: Path):
    fake = FakeDriver(exec_handler=_ok_handler)
    await fake.start(options=StartOptions())
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))
    result = await v.verify(
        task=None,
        env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is not None
    # #380: script exited 0 but no output.json → distinct kind so
    # operators can filter for this specific failure mode.
    assert result.error.kind == "missing_output"
    # #380: post-mortem includes the output-dir path + probe output so
    # operators can distinguish script-side no-op from a permission or
    # env-var bug without a rerun.
    assert result.error.detail["output_dir"] == "/loom/verifier"
    assert "output_dir_probe" in result.error.detail
    assert "output_dir_probe_return_code" in result.error.detail


async def test_script_verifier_invalid_json_returns_parse_error():
    fake = FakeDriver(exec_handler=_ok_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b"not json"
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))
    result = await v.verify(
        task=None,
        env=fake,  # type: ignore[arg-type]
        artifacts_dir=PurePosixPath("/x"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is not None
    assert result.error.kind == "parse_failure"


async def test_script_verifier_missing_output_preserves_exec_diagnostics():
    captured_env = {}

    def _failing_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        captured_env.update(env or {})
        return ExecResult(
            return_code=2,
            stdout=b"setup started\nlast stdout line\n",
            stderr=b"permission denied\n",
            truncated=False,
            duration_sec=1.25,
        )

    fake = FakeDriver(exec_handler=_failing_handler)
    await fake.start(options=StartOptions())
    v = ScriptVerifier(script_path=PurePosixPath("/tests/check.sh"))

    result = await v.verify(
        task=None,  # type: ignore[arg-type]
        env=fake,
        artifacts_dir=PurePosixPath("/x"),
        trajectory=None,  # type: ignore[arg-type]
    )

    assert captured_env["LOOM_VERIFIER_OUTPUT"] == "/loom/verifier/output.json"
    assert result.error is not None
    assert result.error.kind == "exec_failure"
    assert result.error.detail["return_code"] == 2
    assert result.error.detail["output_path"] == "/loom/verifier/output.json"
    assert result.error.detail["script_path"] == "/tests/check.sh"
    assert result.error.detail["stdout_tail"] == "setup started\nlast stdout line\n"
    assert result.error.detail["stderr_tail"] == "permission denied\n"
    assert result.error.detail["duration_sec"] == 1.25


# ---- #688: standard task-context env vars ----


def _task_with_single_artifact(artifact: str):
    from loom.models.task import (
        AgentDefaults,
        EnvironmentConfig,
        StepConfig,
        TaskConfig,
        TaskMetadata,
        VerifierDefaults,
    )

    return TaskConfig(
        task=TaskMetadata(id="t/1", name="t 1"),
        environment=EnvironmentConfig(os="linux", docker_image="python:3.11-slim"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="script", args={"script_path": "/x/run.sh"}),
        steps=[StepConfig(name="main", artifacts=[artifact])],
    )


async def test_script_verifier_exports_loom_task_dir():
    captured_env: dict[str, str] = {}

    def _capture_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        captured_env.update(env or {})
        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.01,
        )

    fake = FakeDriver(exec_handler=_capture_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b'{"rewards": {}}'

    v = ScriptVerifier(script_path=PurePosixPath("/workspace/verifier/run.sh"))
    await v.verify(
        task=None,  # type: ignore[arg-type]
        env=fake,
        artifacts_dir=PurePosixPath("/workspace/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    # Workspace derives from artifacts_dir.parent.
    assert captured_env["LOOM_TASK_DIR"] == "/workspace"
    # LOOM_AGENT_OUTPUT stays unset when task is None (no artifact
    # convention to resolve).
    assert "LOOM_AGENT_OUTPUT" not in captured_env


async def test_script_verifier_exports_loom_agent_output_for_single_file_artifact():
    captured_env: dict[str, str] = {}

    def _capture_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        captured_env.update(env or {})
        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.01,
        )

    fake = FakeDriver(exec_handler=_capture_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b'{"rewards": {}}'

    task = _task_with_single_artifact("final_answer.txt")
    v = ScriptVerifier(script_path=PurePosixPath("/workspace/verifier/run.sh"))
    await v.verify(
        task=task,
        env=fake,
        artifacts_dir=PurePosixPath("/workspace/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert captured_env["LOOM_AGENT_OUTPUT"] == "/workspace/final_answer.txt"


async def test_script_verifier_uses_task_workdir_when_artifacts_dir_is_workspace():
    captured_env: dict[str, str] = {}

    def _capture_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        captured_env.update(env or {})
        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.01,
        )

    fake = FakeDriver(exec_handler=_capture_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b'{"rewards": {}}'

    task = _task_with_single_artifact("answer.txt")
    v = ScriptVerifier(script_path=PurePosixPath("/workspace/verifier/run.sh"))
    await v.verify(
        task=task,
        env=fake,
        artifacts_dir=PurePosixPath("/workspace"),
        trajectory=None,  # type: ignore[arg-type]
    )

    assert captured_env["LOOM_TASK_DIR"] == "/workspace"
    assert captured_env["LOOM_AGENT_OUTPUT"] == "/workspace/answer.txt"


async def test_script_verifier_skips_agent_output_for_glob_artifact():
    captured_env: dict[str, str] = {}

    def _capture_handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        captured_env.update(env or {})
        return ExecResult(
            return_code=0,
            stdout=b"",
            stderr=b"",
            truncated=False,
            duration_sec=0.01,
        )

    fake = FakeDriver(exec_handler=_capture_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b'{"rewards": {}}'

    task = _task_with_single_artifact("outputs/*.json")
    v = ScriptVerifier(script_path=PurePosixPath("/workspace/verifier/run.sh"))
    await v.verify(
        task=task,
        env=fake,
        artifacts_dir=PurePosixPath("/workspace/artifacts"),
        trajectory=None,  # type: ignore[arg-type]
    )
    # LOOM_AGENT_OUTPUT is unset for glob-based artifacts — the verifier
    # script has to walk LOOM_TASK_DIR itself.
    assert "LOOM_AGENT_OUTPUT" not in captured_env
    assert captured_env["LOOM_TASK_DIR"] == "/workspace"


# ---- #865: retain capped verifier audit logs under .loom/verifier/ ----


async def test_script_verifier_writes_audit_log_on_success():
    def _handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        return ExecResult(
            return_code=0,
            stdout=b"pytest collected 3 items\n3 passed\n",
            stderr=b"",
            truncated=False,
            duration_sec=0.2,
        )

    fake = FakeDriver(exec_handler=_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = json.dumps(
        {"rewards": {"passed": 1.0}, "checks": [{"name": "ok", "passed": True}]}
    ).encode()

    from loom.models.task import (
        AgentDefaults,
        EnvironmentConfig,
        StepConfig,
        TaskConfig,
        TaskMetadata,
        VerifierDefaults,
    )
    from loom.verifier.script_verifier import (
        _VERIFIER_LOG_NAME,
        _VERIFIER_META_NAME,
    )

    task = TaskConfig(
        task=TaskMetadata(id="t/1", name="t 1"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="python:3.11-slim",
            workdir=PurePosixPath("/app"),
        ),
        agent=AgentDefaults(name="terminus-2"),
        verifier=VerifierDefaults(name="script", args={"script_path": "/x/run.sh"}),
        steps=[StepConfig(name="main")],
    )
    v = ScriptVerifier(script_path=PurePosixPath("/app/verifier/loom_verify.sh"))
    result = await v.verify(
        task=task,
        env=fake,
        artifacts_dir=PurePosixPath("/app"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.rewards == {"passed": 1.0}
    assert result.error is None

    log_path = PurePosixPath("/app/.loom/verifier") / _VERIFIER_LOG_NAME
    meta_path = PurePosixPath("/app/.loom/verifier") / _VERIFIER_META_NAME
    assert log_path in fake.filesystem
    assert meta_path in fake.filesystem
    log_text = fake.filesystem[log_path].decode()
    assert "3 passed" in log_text
    meta = json.loads(fake.filesystem[meta_path].decode())
    assert meta["truncated"] is False
    assert meta["return_code"] == 0
    assert meta["kept_bytes"] == len(fake.filesystem[log_path])


async def test_script_verifier_truncates_oversized_audit_log():
    from loom.verifier.script_verifier import (
        MAX_VERIFIER_LOG_BYTES,
        _VERIFIER_LOG_NAME,
        _VERIFIER_META_NAME,
        _VERIFIER_LOG_TRUNCATION_MARKER,
    )

    huge = b"HEAD" + (b"x" * (MAX_VERIFIER_LOG_BYTES + 5000)) + b"TAIL_MARKER"

    def _handler(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        return ExecResult(
            return_code=0,
            stdout=huge,
            stderr=b"",
            truncated=False,
            duration_sec=0.5,
        )

    fake = FakeDriver(exec_handler=_handler)
    await fake.start(options=StartOptions())
    fake.filesystem[PurePosixPath("/loom/verifier/output.json")] = b'{"rewards": {"passed": 1.0}}'

    from loom.models.task import (
        AgentDefaults,
        EnvironmentConfig,
        StepConfig,
        TaskConfig,
        TaskMetadata,
        VerifierDefaults,
    )

    task = TaskConfig(
        task=TaskMetadata(id="t/1", name="t 1"),
        environment=EnvironmentConfig(
            os="linux",
            docker_image="python:3.11-slim",
            workdir=PurePosixPath("/app"),
        ),
        agent=AgentDefaults(name="terminus-2"),
        verifier=VerifierDefaults(name="script"),
        steps=[StepConfig(name="main")],
    )
    v = ScriptVerifier(script_path=PurePosixPath("/app/verifier/loom_verify.sh"))
    result = await v.verify(
        task=task,
        env=fake,
        artifacts_dir=PurePosixPath("/app"),
        trajectory=None,  # type: ignore[arg-type]
    )
    assert result.error is None
    log_path = PurePosixPath("/app/.loom/verifier") / _VERIFIER_LOG_NAME
    meta_path = PurePosixPath("/app/.loom/verifier") / _VERIFIER_META_NAME
    kept = fake.filesystem[log_path]
    assert len(kept) <= MAX_VERIFIER_LOG_BYTES
    assert kept.startswith(b"--- stdout ---\nHEAD")
    assert _VERIFIER_LOG_TRUNCATION_MARKER in kept
    assert kept.endswith(b"TAIL_MARKER") or kept.rstrip().endswith(b"TAIL_MARKER")
    meta = json.loads(fake.filesystem[meta_path].decode())
    assert meta["truncated"] is True
    assert meta["original_bytes"] > MAX_VERIFIER_LOG_BYTES
