from __future__ import annotations

import subprocess
import traceback
from pathlib import Path
from typing import cast

import pytest

from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.systemd import (
    JournalStreamRunner,
    SystemdOperationError,
    SystemdQueryError,
    SystemdUnitStatus,
    SystemdUserManager,
    UnitLaunchError,
)

SERVICE_UID = 2222


def make_config() -> OperatorConfig:
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=Path("/opt/loom-staging-runner/repo"),
        state_root=Path("/var/lib/loom-staging-rollout"),
        runtime_root=Path("/run/loom-staging-rollout"),
        rollout_root=Path("/data/loom-staging"),
        kubeconfig_path=Path("/var/lib/loom-staging-rollout/kubeconfig"),
        cluster_config_path=Path(
            "/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml"
        ),
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=Path("/etc/loom/staging-rollout.toml"),
        config_sha256="1" * 64,
    )


class RecordingRunner:
    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.argvs: list[list[str]] = []
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.argvs.append(list(argv))
        return subprocess.CompletedProcess(argv, self.returncode, self.stdout, self.stderr)


class RecordingLineStream:
    def __init__(
        self,
        *,
        lines: tuple[str, ...],
        iteration_error: Exception | None,
        close_error: Exception | None,
    ) -> None:
        self._lines = iter(lines)
        self._iteration_error = iteration_error
        self._close_error = close_error
        self.closed = False
        self.close_calls = 0

    def __iter__(self) -> RecordingLineStream:
        return self

    def __next__(self) -> str:
        if self.closed:
            raise StopIteration
        try:
            return next(self._lines)
        except StopIteration:
            if self._iteration_error is None:
                raise
            error = self._iteration_error
            self._iteration_error = None
            raise error from None

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self._close_error is not None:
            error = self._close_error
            self._close_error = None
            raise error from None


class RecordingStreamRunner:
    def __init__(
        self,
        *,
        lines: tuple[str, ...] = (),
        iteration_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.argvs: list[list[str]] = []
        self.lines = lines
        self.iteration_error = iteration_error
        self.close_error = close_error
        self.opened_stream: RecordingLineStream | None = None

    def __call__(self, argv: list[str]) -> RecordingLineStream:
        self.argvs.append(list(argv))
        self.opened_stream = RecordingLineStream(
            lines=self.lines,
            iteration_error=self.iteration_error,
            close_error=self.close_error,
        )
        return self.opened_stream

    @property
    def closed(self) -> bool:
        return self.opened_stream is not None and self.opened_stream.closed

    @property
    def close_calls(self) -> int:
        return 0 if self.opened_stream is None else self.opened_stream.close_calls


def make_manager(
    runner: RecordingRunner | None = None,
    *,
    stream_runner: JournalStreamRunner | None = None,
) -> SystemdUserManager:
    return SystemdUserManager(
        make_config(),
        service_uid=SERVICE_UID,
        run=runner or RecordingRunner(),
        stream=stream_runner,
    )


def assert_sanitized_operation_error(
    error: SystemdOperationError,
    sentinel: str,
) -> None:
    assert sentinel not in str(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = "".join(
        traceback.format_exception(
            type(error),
            error,
            error.__traceback__,
        )
    )
    assert sentinel not in rendered


def test_start_argv_is_fixed_and_uses_the_sanitized_environment() -> None:
    manager = make_manager()
    envelope = Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json")

    assert manager.start_argv(
        envelope,
        "loom-staging-rollout-req-alpha-1.service",
    ) == [
        "systemd-run",
        "--user",
        "--collect",
        "--service-type=exec",
        "--unit",
        "loom-staging-rollout-req-alpha-1.service",
        "--property",
        "UMask=0077",
        "--property",
        "WorkingDirectory=/opt/loom-staging-runner/repo",
        "/usr/bin/env",
        "-i",
        "HOME=/var/lib/loom-staging-rollout",
        "USER=loom-rollout",
        "LOGNAME=loom-rollout",
        "PATH=/opt/loom-staging-runner/venv/bin:/usr/local/bin:/usr/bin:/bin",
        f"XDG_RUNTIME_DIR=/run/user/{SERVICE_UID}",
        f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{SERVICE_UID}/bus",
        "KUBECONFIG=/var/lib/loom-staging-rollout/kubeconfig",
        "LC_ALL=C.UTF-8",
        "GIT_CONFIG_NOSYSTEM=1",
        "GIT_CONFIG_GLOBAL=/dev/null",
        "GIT_TERMINAL_PROMPT=0",
        "LOOM_STAGING_ROLLOUT_CONFIG=/etc/loom/staging-rollout.toml",
        "/opt/loom-staging-runner/venv/bin/python",
        "-m",
        "loom_cli.rollout.operator.worker",
        "run-attempt",
        "--envelope",
        str(envelope),
    ]


def test_backup_start_argv_is_fixed_to_one_preflight_job() -> None:
    manager = make_manager()
    job = Path("/var/lib/loom-staging-rollout/requests/req-alpha/preflight-backup/job.json")

    argv = manager.start_backup_argv(
        job,
        "loom-staging-backup-req-alpha.service",
    )

    assert argv[-5:] == [
        "-m",
        "loom_cli.rollout.operator.worker",
        "run-backup",
        "--job",
        str(job),
    ]
    assert "--collect" in argv
    assert "UMask=0077" in argv


@pytest.mark.parametrize(
    ("job", "unit"),
    [
        (
            Path("requests/req-alpha/preflight-backup/job.json"),
            "loom-staging-backup-req-alpha.service",
        ),
        (
            Path("/tmp/requests/req-alpha/preflight-backup/job.json"),
            "loom-staging-backup-req-alpha.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-other/preflight-backup/job.json"),
            "loom-staging-backup-req-alpha.service",
        ),
    ],
)
def test_backup_start_rejects_path_or_identity_escape(job: Path, unit: str) -> None:
    with pytest.raises(UnitLaunchError):
        make_manager().start_backup_argv(job, unit)


def test_start_timeout_is_a_fail_closed_launch_error() -> None:
    def timeout_runner(argv: list[str]) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(argv, timeout=120)

    manager = SystemdUserManager(
        make_config(),
        service_uid=SERVICE_UID,
        run=timeout_runner,
    )
    envelope = Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json")

    with pytest.raises(UnitLaunchError) as captured:
        manager.start_attempt(envelope, "loom-staging-rollout-req-alpha-1.service")

    assert str(captured.value) == "transient rollout unit could not be started"


def test_show_timeout_is_a_fail_closed_query_error() -> None:
    def timeout_runner(argv: list[str]) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(argv, timeout=120)

    manager = SystemdUserManager(
        make_config(),
        service_uid=SERVICE_UID,
        run=timeout_runner,
    )

    with pytest.raises(SystemdQueryError) as captured:
        manager.show("loom-staging-rollout-req-alpha-1.service")

    assert str(captured.value) == "systemd unit status could not be queried"


@pytest.mark.parametrize(
    ("envelope", "unit"),
    [
        (
            Path("requests/req-alpha/attempts/1/envelope.json"),
            "loom-staging-rollout-req-alpha-1.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/../envelope.json"),
            "loom-staging-rollout-req-alpha-1.service",
        ),
        (
            Path("/tmp/requests/req-alpha/attempts/1/envelope.json"),
            "loom-staging-rollout-req-alpha-1.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-a/attempts/1/envelope.json"),
            "loom-staging-rollout-req-a-1.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/0/envelope.json"),
            "loom-staging-rollout-req-alpha-0.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/not-envelope.json"),
            "loom-staging-rollout-req-alpha-1.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json"),
            "generic-safe.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json"),
            "loom-staging-rollout-req-bravo-1.service",
        ),
        (
            Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json"),
            "loom-staging-rollout-req-alpha-2.service",
        ),
    ],
)
def test_start_rejects_unsafe_or_mismatched_envelope_and_unit(
    envelope: Path,
    unit: str,
) -> None:
    runner = RecordingRunner()
    manager = make_manager(runner)

    with pytest.raises(UnitLaunchError):
        manager.start_attempt(envelope, unit)

    assert runner.argvs == []


def test_start_rejects_unit_longer_than_systemd_name_limit_before_runner() -> None:
    runner = RecordingRunner()
    attempt = "1" * 230
    envelope = Path(
        f"/var/lib/loom-staging-rollout/requests/req-alpha/attempts/{attempt}/envelope.json"
    )
    unit = f"loom-staging-rollout-req-alpha-{attempt}.service"
    assert len(unit) > 255

    with pytest.raises(UnitLaunchError):
        make_manager(runner).start_attempt(envelope, unit)

    assert runner.argvs == []


def test_start_wraps_oversized_attempt_number_as_unit_launch_error() -> None:
    runner = RecordingRunner()
    attempt = "1" * 5000
    envelope = Path(
        f"/var/lib/loom-staging-rollout/requests/req-alpha/attempts/{attempt}/envelope.json"
    )
    unit = f"loom-staging-rollout-req-alpha-{attempt}.service"

    with pytest.raises(UnitLaunchError):
        make_manager(runner).start_attempt(envelope, unit)

    assert runner.argvs == []


def test_start_attempt_invokes_the_approved_argv() -> None:
    runner = RecordingRunner()
    manager = make_manager(runner)
    envelope = Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json")
    unit = "loom-staging-rollout-req-alpha-1.service"

    manager.start_attempt(envelope, unit)

    assert runner.argvs == [manager.start_argv(envelope, unit)]


def test_start_failure_does_not_expose_captured_output() -> None:
    runner = RecordingRunner(returncode=1, stderr="SECRET=do-not-leak")
    manager = make_manager(runner)
    envelope = Path("/var/lib/loom-staging-rollout/requests/req-alpha/attempts/1/envelope.json")

    with pytest.raises(UnitLaunchError) as caught:
        manager.start_attempt(
            envelope,
            "loom-staging-rollout-req-alpha-1.service",
        )

    assert "SECRET" not in str(caught.value)
    assert "do-not-leak" not in str(caught.value)


def test_show_requests_only_the_allowlisted_properties_and_parses_typed_status() -> None:
    runner = RecordingRunner(
        stdout=(
            "ActiveState=active\n"
            "SubState=running\n"
            "Result=success\n"
            "ExecMainStatus=0\n"
            "MainPID=4321\n"
            "ExecMainStartTimestamp=Mon 2026-07-13 20:00:00 UTC\n"
            "ExecMainExitTimestamp=\n"
        )
    )
    manager = make_manager(runner)

    status = manager.show("loom-staging-rollout-req-alpha-1.service")

    assert status == SystemdUnitStatus(
        unit_name="loom-staging-rollout-req-alpha-1.service",
        active_state="active",
        sub_state="running",
        result="success",
        exec_main_status=0,
        main_pid=4321,
        exec_main_start_timestamp="Mon 2026-07-13 20:00:00 UTC",
        exec_main_exit_timestamp=None,
    )
    assert status.is_running
    assert runner.argvs == [
        [
            "systemctl",
            "--user",
            "show",
            "--no-pager",
            "--property=ActiveState",
            "--property=SubState",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=MainPID",
            "--property=ExecMainStartTimestamp",
            "--property=ExecMainExitTimestamp",
            "loom-staging-rollout-req-alpha-1.service",
        ]
    ]


def test_show_returns_none_only_for_systemctl_not_found_status() -> None:
    runner = RecordingRunner(returncode=4, stderr="Unit does not exist")

    assert make_manager(runner).show("loom-staging-rollout-req-alpha-1.service") is None


@pytest.mark.parametrize(
    "stdout",
    [
        "",
        (
            "ActiveState=active\nSubState=running\nResult=success\n"
            "ExecMainStatus=0\nMainPID=1\n"
            "ExecMainStartTimestamp=now\nUnknownProperty=value\n"
        ),
        (
            "ActiveState=active\nActiveState=inactive\nSubState=running\n"
            "Result=success\nExecMainStatus=0\nMainPID=1\n"
            "ExecMainStartTimestamp=now\nExecMainExitTimestamp=\n"
        ),
        (
            "ActiveState=active\nSubState=running\nResult=success\n"
            "ExecMainStatus=not-an-int\nMainPID=1\n"
            "ExecMainStartTimestamp=now\nExecMainExitTimestamp=\n"
        ),
    ],
)
def test_show_rejects_missing_malformed_or_extra_properties(stdout: str) -> None:
    with pytest.raises(SystemdQueryError):
        make_manager(RecordingRunner(stdout=stdout)).show(
            "loom-staging-rollout-req-alpha-1.service"
        )


def test_show_wraps_oversized_numeric_property_as_query_error() -> None:
    stdout = (
        "ActiveState=active\n"
        "SubState=running\n"
        "Result=success\n"
        "ExecMainStatus=0\n"
        f"MainPID={'1' * 5000}\n"
        "ExecMainStartTimestamp=now\n"
        "ExecMainExitTimestamp=\n"
    )

    with pytest.raises(SystemdQueryError):
        make_manager(RecordingRunner(stdout=stdout)).show(
            "loom-staging-rollout-req-alpha-1.service"
        )


def test_show_runner_failure_does_not_expose_captured_output() -> None:
    runner = RecordingRunner(
        returncode=1,
        stdout="TOKEN=stdout-secret",
        stderr="TOKEN=stderr-secret",
    )

    with pytest.raises(SystemdQueryError) as caught:
        make_manager(runner).show("loom-staging-rollout-req-alpha-1.service")

    assert "secret" not in str(caught.value)


def test_terminate_sends_only_version_independent_normal_sigterm() -> None:
    runner = RecordingRunner()

    make_manager(runner).terminate("loom-staging-rollout-req-alpha-1.service")

    assert runner.argvs == [
        [
            "systemctl",
            "--user",
            "kill",
            "--signal=SIGTERM",
            "loom-staging-rollout-req-alpha-1.service",
        ]
    ]


def test_stream_journal_non_follow_uses_bounded_fixed_argv() -> None:
    runner = RecordingRunner(stdout="safe journal output\n")

    lines = list(
        make_manager(runner).stream_journal(
            "loom-staging-rollout-req-alpha-1.service",
            follow=False,
        )
    )

    expected = [
        "journalctl",
        "--user",
        "--unit",
        "loom-staging-rollout-req-alpha-1.service",
        "--no-pager",
        "--lines=200",
        "--output=short-iso",
    ]
    assert runner.argvs == [expected]
    assert lines == ["safe journal output\n"]


def test_stream_journal_follow_uses_injected_line_stream_and_fixed_argv() -> None:
    runner = RecordingRunner(stdout="must not be captured")
    stream_runner = RecordingStreamRunner(lines=("first\n", "second\n"))

    lines = list(
        make_manager(runner, stream_runner=stream_runner).stream_journal(
            "loom-staging-rollout-req-alpha-1.service",
            follow=True,
        )
    )

    assert runner.argvs == []
    assert stream_runner.argvs == [
        [
            "journalctl",
            "--user",
            "--unit",
            "loom-staging-rollout-req-alpha-1.service",
            "--no-pager",
            "--lines=200",
            "--output=short-iso",
            "--follow",
        ]
    ]
    assert lines == ["first\n", "second\n"]
    assert stream_runner.close_calls == 1


def test_stream_journal_accepts_the_declared_positional_follow_parameter() -> None:
    runner = RecordingRunner(stdout="journal\n")

    assert list(
        make_manager(runner).stream_journal(
            "loom-staging-rollout-req-alpha-1.service",
            False,
        )
    ) == ["journal\n"]


def test_stream_journal_follow_requires_configured_stream_runner() -> None:
    with pytest.raises(SystemdOperationError):
        make_manager().stream_journal(
            "loom-staging-rollout-req-alpha-1.service",
            follow=True,
        )


def test_stream_journal_open_failure_has_no_secret_exception_chain() -> None:
    sentinel = "traceback-open-secret"

    def fail_open(argv: list[str]) -> RecordingLineStream:
        raise OSError(sentinel)

    with pytest.raises(SystemdOperationError) as caught:
        make_manager(stream_runner=fail_open).stream_journal(
            "loom-staging-rollout-req-alpha-1.service",
            follow=True,
        )

    assert_sanitized_operation_error(caught.value, sentinel)


def test_stream_journal_wraps_iteration_failure_without_exposing_output() -> None:
    sentinel = "traceback-iteration-secret"
    stream_runner = RecordingStreamRunner(
        lines=("safe line\n",),
        iteration_error=OSError(sentinel),
    )
    lines = make_manager(stream_runner=stream_runner).stream_journal(
        "loom-staging-rollout-req-alpha-1.service",
        follow=True,
    )

    assert next(lines) == "safe line\n"
    with pytest.raises(SystemdOperationError) as caught:
        next(lines)

    assert_sanitized_operation_error(caught.value, sentinel)


def test_stream_journal_rewraps_source_operation_error_without_exposing_output() -> None:
    stream_runner = RecordingStreamRunner(
        iteration_error=SystemdOperationError("TOKEN=iteration-secret"),
    )
    lines = make_manager(stream_runner=stream_runner).stream_journal(
        "loom-staging-rollout-req-alpha-1.service",
        follow=True,
    )

    with pytest.raises(SystemdOperationError) as caught:
        next(lines)

    assert "secret" not in str(caught.value)


def test_stream_journal_closes_underlying_follow_stream_on_early_stop() -> None:
    stream_runner = RecordingStreamRunner(lines=("first\n", "second\n"))
    lines = make_manager(stream_runner=stream_runner).stream_journal(
        "loom-staging-rollout-req-alpha-1.service",
        follow=True,
    )

    assert next(lines) == "first\n"
    assert not stream_runner.closed
    lines.close()

    assert stream_runner.closed
    assert stream_runner.close_calls == 1
    lines.close()
    assert stream_runner.close_calls == 1


def test_stream_journal_close_before_first_line_closes_underlying_stream_once() -> None:
    stream_runner = RecordingStreamRunner(lines=("first\n",))
    lines = make_manager(stream_runner=stream_runner).stream_journal(
        "loom-staging-rollout-req-alpha-1.service",
        follow=True,
    )

    lines.close()

    assert stream_runner.closed
    assert stream_runner.close_calls == 1
    lines.close()
    assert stream_runner.close_calls == 1


def test_stream_journal_close_failure_is_sanitized_without_double_close() -> None:
    sentinel = "traceback-close-secret"
    stream_runner = RecordingStreamRunner(
        lines=("first\n",),
        close_error=OSError(sentinel),
    )
    lines = make_manager(stream_runner=stream_runner).stream_journal(
        "loom-staging-rollout-req-alpha-1.service",
        follow=True,
    )

    with pytest.raises(SystemdOperationError) as caught:
        lines.close()

    assert_sanitized_operation_error(caught.value, sentinel)
    assert stream_runner.close_calls == 1
    lines.close()
    assert stream_runner.close_calls == 1


def test_stream_journal_rejects_non_boolean_follow_without_running_a_command() -> None:
    runner = RecordingRunner()

    with pytest.raises(SystemdOperationError):
        make_manager(runner).stream_journal(
            "loom-staging-rollout-req-alpha-1.service",
            cast(bool, "--follow --since=forever"),
        )

    assert runner.argvs == []


@pytest.mark.parametrize("operation", ["terminate", "journal"])
def test_control_runner_failures_do_not_expose_captured_output(operation: str) -> None:
    runner = RecordingRunner(
        returncode=1,
        stdout="TOKEN=stdout-secret",
        stderr="TOKEN=stderr-secret",
    )
    manager = make_manager(runner)

    with pytest.raises(SystemdOperationError) as caught:
        if operation == "terminate":
            manager.terminate("loom-staging-rollout-req-alpha-1.service")
        else:
            manager.stream_journal(
                "loom-staging-rollout-req-alpha-1.service",
                follow=False,
            )

    assert "secret" not in str(caught.value)
