from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.models.exec import ExecResult
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom.trajectory.reader import TrajectoryReader
from loom.verifier.pytest_verifier import (
    PytestVerifier,
    build_pytest_install_command,
)


def _task() -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="livecodebench/example", name="example"),
        environment=EnvironmentConfig(os="linux", docker_image="python:3.11-slim"),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="pytest"),
    )


def _trajectory(tmp_path: Path) -> TrajectoryReader:
    path = tmp_path / "events.jsonl"
    path.write_text("", encoding="utf-8")
    return TrajectoryReader(path)


@pytest.mark.asyncio
async def test_missing_junit_includes_pytest_exec_diagnostics(
    tmp_path: Path,
) -> None:
    def _exec(cmd, user, cwd, env):  # type: ignore[no-untyped-def]
        if cmd == "mkdir -p /loom/verifier":
            return ExecResult(
                return_code=0,
                stdout=b"",
                stderr=b"",
                duration_sec=0.01,
            )
        if cmd == build_pytest_install_command():
            return ExecResult(
                return_code=0,
                stdout=b"",
                stderr=b"",
                duration_sec=0.02,
            )
        assert "pytest --junitxml=/loom/verifier/junit.xml" in cmd
        return ExecResult(
            return_code=4,
            stdout=b"collected 0 items\n",
            stderr=b"ERROR: file or directory not found: /workspace/tests\n",
            truncated=True,
            duration_sec=1.25,
        )

    driver = FakeDriver(exec_handler=_exec)
    await driver.start(options=StartOptions())

    result = await PytestVerifier().verify(
        task=_task(),
        env=driver,
        artifacts_dir=PurePosixPath("/workspace"),
        trajectory=_trajectory(tmp_path),
    )

    assert result.rewards == {}
    assert result.error is not None
    assert result.error.kind == "missing_tests"
    assert result.error.detail["phase"] == "pytest"
    assert result.error.detail["return_code"] == 4
    assert result.error.detail["stdout_tail"] == "collected 0 items\n"
    assert (
        result.error.detail["stderr_tail"]
        == "ERROR: file or directory not found: /workspace/tests\n"
    )
    assert result.error.detail["driver_truncated"] is True
    assert result.error.detail["duration_sec"] == pytest.approx(1.25)
    assert result.structured is not None
    assert result.structured["pytest_exec"]["phase"] == "pytest"


@pytest.mark.asyncio
async def test_pytest_command_timeout_returns_scored_diagnostic(
    tmp_path: Path,
) -> None:
    class _TimeoutPytestDriver(FakeDriver):
        seen_pytest_timeout: float | None = None

        async def exec(  # type: ignore[override]
            self,
            cmd: str,
            *,
            user=None,  # type: ignore[no-untyped-def]
            cwd=None,  # type: ignore[no-untyped-def]
            env=None,  # type: ignore[no-untyped-def]
            timeout_sec=None,  # type: ignore[no-untyped-def]
        ) -> ExecResult:
            if cmd == "mkdir -p /loom/verifier":
                return ExecResult(
                    return_code=0,
                    stdout=b"",
                    stderr=b"",
                    duration_sec=0.01,
                )
            if cmd == build_pytest_install_command():
                return ExecResult(
                    return_code=0,
                    stdout=b"",
                    stderr=b"",
                    duration_sec=0.02,
                )
            assert "pytest --junitxml=/loom/verifier/junit.xml" in cmd
            self.seen_pytest_timeout = timeout_sec
            raise TimeoutError

    driver = _TimeoutPytestDriver()
    await driver.start(options=StartOptions())

    result = await PytestVerifier(pytest_timeout_sec=12.5).verify(
        task=_task(),
        env=driver,
        artifacts_dir=PurePosixPath("/workspace"),
        trajectory=_trajectory(tmp_path),
    )

    assert driver.seen_pytest_timeout == 12.5
    assert result.rewards == {"passed": 0.0, "pytest_pass_rate": 0.0}
    assert result.error is not None
    assert result.error.kind == "timeout"
    assert result.error.detail == {
        "phase": "pytest",
        "timeout_sec": 12.5,
        "junit_xml_path": "/loom/verifier/junit.xml",
    }
    assert result.structured == {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "pytest_timeout": {
            "phase": "pytest",
            "timeout_sec": 12.5,
            "junit_xml_path": "/loom/verifier/junit.xml",
        },
    }


@pytest.mark.asyncio
async def test_pytest_install_timeout_is_unscored_infrastructure_failure(
    tmp_path: Path,
) -> None:
    class _TimeoutInstallDriver(FakeDriver):
        seen_install_timeout: float | None = None

        async def exec(  # type: ignore[override]
            self,
            cmd: str,
            *,
            user=None,  # type: ignore[no-untyped-def]
            cwd=None,  # type: ignore[no-untyped-def]
            env=None,  # type: ignore[no-untyped-def]
            timeout_sec=None,  # type: ignore[no-untyped-def]
        ) -> ExecResult:
            if cmd == "mkdir -p /loom/verifier":
                return ExecResult(
                    return_code=0,
                    stdout=b"",
                    stderr=b"",
                    duration_sec=0.01,
                )
            assert cmd == build_pytest_install_command()
            self.seen_install_timeout = timeout_sec
            raise TimeoutError

    driver = _TimeoutInstallDriver()
    await driver.start(options=StartOptions())

    result = await PytestVerifier(install_timeout_sec=7.0).verify(
        task=_task(),
        env=driver,
        artifacts_dir=PurePosixPath("/workspace"),
        trajectory=_trajectory(tmp_path),
    )

    assert driver.seen_install_timeout == 7.0
    assert result.rewards == {}
    assert result.error is not None
    assert result.error.kind == "timeout"
    assert result.error.detail == {
        "phase": "install",
        "timeout_sec": 7.0,
        "junit_xml_path": "/loom/verifier/junit.xml",
    }
