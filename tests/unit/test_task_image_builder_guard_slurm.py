from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from uuid import UUID

import pytest

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import (
    CommandConfig,
    CommandIdentity,
    IdentityConfig,
    SlurmConfig,
)
from loom_task_image_builder_guard.slurm import (
    CommandResult,
    PinnedCommandRunner,
    SlurmInspector,
)

GRANT = UUID("11111111-1111-4111-8111-111111111111")
COMMENT = f"loom-task-builder-v1:grant={GRANT}"
DIGEST = "a" * 64


def _identity(path: Path) -> CommandIdentity:
    return CommandIdentity(path=path, sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def test_pinned_runner_executes_opened_binary_without_shell(tmp_path: Path) -> None:
    marker = tmp_path / "must-not-exist"
    command = tmp_path / "command"
    command.write_text("#!/bin/sh\nprintf '%s' \"$1\"\n", encoding="ascii")
    command.chmod(0o555)
    runner = PinnedCommandRunner(trusted_uid=os.geteuid(), timeout_seconds=2)

    result = runner.run(_identity(command), (f"$(touch {marker})",))

    assert result == CommandResult(0, f"$(touch {marker})", "")
    assert not marker.exists()


def test_pinned_runner_reports_main_thread_progress_between_output_chunks(
    tmp_path: Path,
) -> None:
    command = tmp_path / "command"
    command.write_text(
        "#!/bin/sh\nprintf first\nsleep 0.05\nprintf second\n",
        encoding="ascii",
    )
    command.chmod(0o555)
    progress: list[float] = []
    runner = PinnedCommandRunner(
        trusted_uid=os.geteuid(),
        timeout_seconds=2,
        progress=lambda: progress.append(time.monotonic()),
    )

    result = runner.run(_identity(command), ())

    assert result == CommandResult(0, "firstsecond", "")
    assert len(progress) >= 2
    assert progress[-1] - progress[0] >= 0.03


def test_pinned_runner_does_not_signal_an_already_reaped_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command = tmp_path / "command"
    command.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    command.chmod(0o555)
    runner = PinnedCommandRunner(trusted_uid=os.geteuid(), timeout_seconds=2)

    def unexpected_killpg(_process_group: int, _signal: int) -> None:
        raise AssertionError("a reaped child PID must not be signalled")

    monkeypatch.setattr("loom_task_image_builder_guard.slurm.os.killpg", unexpected_killpg)

    assert runner.run(_identity(command), ()) == CommandResult(0, "", "")


def test_pinned_runner_rejects_changed_or_writable_command(tmp_path: Path) -> None:
    command = tmp_path / "command"
    command.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    command.chmod(0o555)
    identity = _identity(command)
    runner = PinnedCommandRunner(trusted_uid=os.geteuid(), timeout_seconds=2)

    command.chmod(0o755)
    command.write_text("#!/bin/sh\nexit 1\n", encoding="ascii")
    command.chmod(0o555)
    with pytest.raises(GuardError) as caught:
        runner.run(identity, ())
    assert caught.value.code == "command_identity_invalid"

    command.chmod(0o755)
    command.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    command.chmod(0o775)
    with pytest.raises(GuardError) as caught:
        runner.run(_identity(command), ())
    assert caught.value.code == "command_identity_invalid"


def test_pinned_runner_terminates_a_command_at_the_output_limit(tmp_path: Path) -> None:
    marker = tmp_path / "must-not-be-created"
    command = tmp_path / "command"
    command.write_text(
        "#!/bin/sh\nhead -c 1048576 /dev/zero\ntouch \"$1\"\n",
        encoding="ascii",
    )
    command.chmod(0o555)
    runner = PinnedCommandRunner(
        trusted_uid=os.geteuid(),
        timeout_seconds=5,
        max_stdout_bytes=1024,
    )

    with pytest.raises(GuardError) as caught:
        runner.run(_identity(command), (str(marker),))

    assert caught.value.code == "command_output_invalid"
    assert not marker.exists()


def test_pinned_runner_terminates_descendants_after_the_group_leader_exits(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "must-not-be-created"
    command = tmp_path / "command"
    command.write_text(
        "#!/bin/sh\n"
        "( sleep 0.2; head -c 1048576 /dev/zero; sleep 0.2; touch \"$1\" ) &\n"
        "exit 0\n",
        encoding="ascii",
    )
    command.chmod(0o555)
    runner = PinnedCommandRunner(
        trusted_uid=os.geteuid(),
        timeout_seconds=5,
        max_stdout_bytes=1024,
    )

    with pytest.raises(GuardError) as caught:
        runner.run(_identity(command), (str(marker),))
    time.sleep(0.5)

    assert caught.value.code == "command_output_invalid"
    assert not marker.exists()


def test_pinned_runner_rejects_a_lingering_descendant_that_closed_its_pipes(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "must-not-be-created"
    command = tmp_path / "command"
    command.write_text(
        "#!/bin/sh\n"
        "( exec </dev/null >/dev/null 2>/dev/null; sleep 0.2; touch \"$1\" ) &\n"
        "exit 0\n",
        encoding="ascii",
    )
    command.chmod(0o555)
    runner = PinnedCommandRunner(trusted_uid=os.geteuid(), timeout_seconds=2)

    with pytest.raises(GuardError) as caught:
        runner.run(_identity(command), (str(marker),))
    time.sleep(0.3)

    assert caught.value.code == "command_descendants_invalid"
    assert not marker.exists()


def test_pinned_runner_kills_a_same_session_descendant_in_a_new_process_group(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "must-not-be-created"
    ready = tmp_path / "child-ready"
    child = tmp_path / "child"
    child.write_text(
        "#!/usr/bin/python3\n"
        "import os, pathlib, sys, time\n"
        "os.setpgid(0, 0)\n"
        "pathlib.Path(sys.argv[2]).touch()\n"
        "time.sleep(0.2)\n"
        "pathlib.Path(sys.argv[1]).touch()\n",
        encoding="ascii",
    )
    child.chmod(0o555)
    command = tmp_path / "command"
    command.write_text(
        "#!/bin/sh\n"
        '"$2" "$1" "$3" </dev/null >/dev/null 2>/dev/null &\n'
        'while [ ! -e "$3" ]; do :; done\n'
        "exit 0\n",
        encoding="ascii",
    )
    command.chmod(0o555)
    runner = PinnedCommandRunner(trusted_uid=os.geteuid(), timeout_seconds=2)

    with pytest.raises(GuardError) as caught:
        runner.run(_identity(command), (str(marker), str(child), str(ready)))
    time.sleep(0.3)

    assert caught.value.code == "command_descendants_invalid"
    assert not marker.exists()


def test_pinned_runner_rejects_final_pathname_replacement(tmp_path: Path) -> None:
    command = tmp_path / "command"
    command.write_text(
        "#!/bin/sh\ncp \"$1\" \"$1.replacement\"\n"
        "chmod 0555 \"$1.replacement\"\n"
        "mv \"$1.replacement\" \"$1\"\n",
        encoding="ascii",
    )
    command.chmod(0o555)
    runner = PinnedCommandRunner(trusted_uid=os.geteuid(), timeout_seconds=2)

    with pytest.raises(GuardError) as caught:
        runner.run(_identity(command), (str(command),))

    assert caught.value.code == "command_identity_changed"


def _scontrol(**changes: str) -> str:
    values = {
        "JobId": "123",
        "UserId": "loom-builder(993)",
        "GroupId": "loom-task-builder(980)",
        "JobState": "RUNNING",
        "Account": "loom-task-builder",
        "QOS": "loom-task-image-builder-rootless-oldlab",
        "Partition": "loom-task-builder",
        "BatchHost": "trt-eai-oldlab-3",
        "NodeList": "trt-eai-oldlab-3",
        "NumNodes": "1",
        "NumCPUs": "8",
        "MinMemoryNode": "32768M",
        "TimeLimit": "02:00:00",
        "Features": "loom_rootless_buildkit",
        "Comment": COMMENT,
        "Requeue": "0",
        "Restarts": "0",
    }
    values.update(changes)
    return " ".join(f"{key}={value}" for key, value in values.items()) + "\n"


def _sacct(**changes: str) -> str:
    values = {
        "job": "123",
        "state": "RUNNING",
        "user": "loom-builder",
        "group": "loom-task-builder",
        "account": "loom-task-builder",
        "cluster": "trt-oldlab",
        "partition": "loom-task-builder",
        "qos": "loom-task-image-builder-rootless-oldlab",
        "cpus": "8",
        "memory": "32768M",
        "tres": "cpu=8,mem=32G,node=1,billing=8",
        "nodes": "trt-eai-oldlab-3",
        "comment": COMMENT,
    }
    values.update(changes)
    return "|".join(values.values()) + "|\n"


class _Runner:
    def __init__(self, control: str, accounting: str) -> None:
        self.control = control
        self.accounting = accounting
        self.calls: list[tuple[Path, tuple[str, ...]]] = []

    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult:
        self.calls.append((command.path, argv))
        if command.path.name == "scontrol" and argv[:2] == ("show", "job"):
            return CommandResult(0, self.control, "")
        if command.path.name == "sacct":
            return CommandResult(0, self.accounting, "")
        if command.path.name == "scontrol" and argv[:1] == ("update",):
            return CommandResult(0, "", "")
        return CommandResult(2, "", "unexpected")


def _inspector(runner: _Runner) -> SlurmInspector:
    commands = CommandConfig(
        scontrol=CommandIdentity(Path("/usr/bin/scontrol"), DIGEST),
        sacct=CommandIdentity(Path("/usr/bin/sacct"), DIGEST),
        bpftool=CommandIdentity(Path("/usr/bin/bpftool"), DIGEST),
    )
    return SlurmInspector(
        cluster_id="oldlab",
        node_name="trt-eai-oldlab-3",
        identity=IdentityConfig(
            uid=993,
            gid=980,
            forbidden_supplementary_gids=(0, 27, 128),
            supervisor_path=Path("/usr/local/libexec/loom-task-builder-supervisor"),
            supervisor_sha256=DIGEST,
        ),
        policy=SlurmConfig(
            cluster_name="trt-oldlab",
            request_sha256=DIGEST,
            account="loom-task-builder",
            partition="loom-task-builder",
            qos="loom-task-image-builder-rootless-oldlab",
            feature="loom_rootless_buildkit",
            cpus=8,
            memory_mib=32768,
            wall_time="02:00:00",
        ),
        commands=commands,
        runner=runner,
    )


def test_observe_requires_matching_live_controller_and_accounting_facts() -> None:
    runner = _Runner(_scontrol(), _sacct())

    facts = _inspector(runner).observe(job_id="123", grant_id=GRANT)

    assert facts.job_id == "123"
    assert facts.node_name == "trt-eai-oldlab-3"
    assert facts.comment == COMMENT
    assert facts.cpus == 8
    assert facts.memory_mib == 32768
    assert [path.name for path, _argv in runner.calls] == ["scontrol", "sacct"]


@pytest.mark.parametrize(
    ("control", "accounting", "code"),
    [
        (_scontrol(JobState="PENDING"), _sacct(), "slurm_job_not_running"),
        (_scontrol(UserId="other(993)"), _sacct(), "slurm_identity_invalid"),
        (_scontrol(QOS="normal"), _sacct(), "slurm_policy_invalid"),
        (_scontrol(Comment="foreign"), _sacct(), "slurm_grant_invalid"),
        (_scontrol(NumCPUs="7"), _sacct(), "slurm_resources_invalid"),
        (_scontrol(BatchHost="other-node"), _sacct(), "slurm_node_invalid"),
        (_scontrol(Requeue="1"), _sacct(), "slurm_lifecycle_invalid"),
        (_scontrol(), _sacct(cluster="foreign"), "slurm_accounting_invalid"),
        (_scontrol(), _sacct(state="COMPLETED"), "slurm_accounting_invalid"),
        (_scontrol(), _sacct(tres="cpu=8,mem=31G,node=1"), "slurm_accounting_invalid"),
    ],
)
def test_observe_rejects_each_changed_live_job_authority(
    control: str,
    accounting: str,
    code: str,
) -> None:
    with pytest.raises(GuardError) as caught:
        _inspector(_Runner(control, accounting)).observe(job_id="123", grant_id=GRANT)

    assert caught.value.code == code


def test_observe_rejects_multiple_controller_or_accounting_records() -> None:
    for control, accounting in (
        (_scontrol() + _scontrol(), _sacct()),
        (_scontrol(), _sacct() + _sacct()),
    ):
        with pytest.raises(GuardError) as caught:
            _inspector(_Runner(control, accounting)).observe(job_id="123", grant_id=GRANT)
        assert caught.value.code in {"slurm_controller_invalid", "slurm_accounting_invalid"}


def test_quarantine_removes_only_builder_active_feature() -> None:
    runner = _Runner(_scontrol(), _sacct())

    _inspector(runner).quarantine_capability()

    assert runner.calls == [
        (
            Path("/usr/bin/scontrol"),
            (
                "update",
                "NodeName=trt-eai-oldlab-3",
                "ActiveFeatures-=loom_rootless_buildkit",
            ),
        )
    ]
    assert all("State=" not in item for item in runner.calls[0][1])
