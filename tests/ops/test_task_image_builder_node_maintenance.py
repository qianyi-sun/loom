from __future__ import annotations

import json
import os
import shlex
import shutil
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_node_maintenance as maintenance

OPERATION_ID = "00000000-0000-4000-8000-000000000021"
LOOM_REASON = f"loom-task-builder-phase1/host-release-v1/{OPERATION_ID}"


@dataclass
class FakeController:
    candidate_root: Path
    state: str = "IDLE"
    reason: str = "none"
    allocated_tres: str = "cpu=0,mem=0M"
    active_jobs: list[str] = field(default_factory=list)
    smoke_state: str | None = None
    smoke_reason: str | None = None
    smoke_released: bool = False
    sacct_rows: str | None = None
    reservation: str | None = None
    legacy_reservation: str = "legacy_operator_hold"
    smoke_evidence: dict[str, object] = field(
        default_factory=lambda: {
            "schema": "loom.task-image-builder-maintenance-smoke/v1",
            "operation_id": OPERATION_ID,
            "job_id": "101",
            "cgroup_path": "/slurm/uid_993/job_101/step_batch",
            "controls": {
                "cpuset_cpus_effective": "0-7",
                "cpuset_cpu_count": 8,
                "memory_max": 34359738368,
                "memory_swap_max": 0,
                "devices": {
                    "cgroup_path": "/slurm/uid_993/job_101/step_batch",
                    "programs": [
                        {
                            "id": 19,
                            "attach_type": "cgroup_device",
                            "attach_flags": "multi",
                            "name": "loom_devices",
                        }
                    ],
                },
            },
        }
    )
    fail_at: str | None = None
    commands: list[tuple[str, ...]] = field(default_factory=list)
    remote_actions: list[str] = field(default_factory=list)
    drain_reasons: list[str] = field(default_factory=list)
    clock: float = 0.0

    def monotonic(self) -> float:
        self.clock += 0.1
        return self.clock

    def run(self, args: tuple[str, ...]) -> maintenance.CommandResult:
        command = tuple(args)
        self.commands.append(command)
        if command == ("/usr/bin/scontrol", "show", "node", "node-1", "-o"):
            return maintenance.CommandResult(
                0,
                f"NodeName=node-1 State={self.state} Reason={self.reason} "
                f"AllocTRES={self.allocated_tres}\n",
                "",
            )
        if command[:3] == ("/usr/bin/scontrol", "update", "NodeName=node-1"):
            fields = dict(part.split("=", 1) for part in command[3:] if "=" in part)
            if fields.get("State") == "DRAIN":
                self.state = "DRAIN"
                self.reason = fields["Reason"]
                self.drain_reasons.append(self.reason)
                return maintenance.CommandResult(0, "", "")
            if fields.get("State") == "RESUME":
                if self.reason != LOOM_REASON:
                    return maintenance.CommandResult(1, "", "foreign drain")
                self.state = "IDLE"
                self.reason = "none"
                self.smoke_state = "RUNNING"
                self.smoke_reason = "None"
                return maintenance.CommandResult(0, "", "")
        if command[:3] == ("/usr/bin/scontrol", "create", "reservation"):
            name = next(item[5:] for item in command if item.startswith("Name="))
            if self.reservation == name or self.legacy_reservation == name:
                return maintenance.CommandResult(1, "", "reservation already exists")
            self.reservation = name
            return maintenance.CommandResult(0, "", "")
        if command[:3] == ("/usr/bin/scontrol", "delete", "reservation"):
            assert (
                next(item[5:] for item in command if item.startswith("Name=")) == self.reservation
            )
            self.reservation = None
            return maintenance.CommandResult(0, "", "")
        if command == ("/usr/bin/scontrol", "show", "reservation", "--oneliner"):
            rows = [
                "ReservationName=legacy_operator_hold Nodes=legacy-node Users=operator State=ACTIVE"
            ]
            if self.reservation is not None:
                rows.append(
                    f"ReservationName={self.reservation} Nodes=node-1 "
                    "Users=loom-builder State=ACTIVE"
                )
            return maintenance.CommandResult(0, "\n".join(rows) + "\n", "")
        if command[:3] == ("/usr/bin/squeue", "--nodelist", "node-1"):
            if self.smoke_state == "RUNNING":
                return maintenance.CommandResult(0, "101\n", "")
            return maintenance.CommandResult(0, "\n".join(self.active_jobs), "")
        if command[:2] == ("/usr/bin/squeue", "--job"):
            if self.smoke_state is None:
                return maintenance.CommandResult(1, "", "unknown job")
            fields = dict(item.split("=", 1) for item in command if item.startswith("--format="))
            format_value = fields["--format"]
            values = {
                "%T": self.smoke_state,
                "%R": self.smoke_reason,
                "%N": "node-1" if self.smoke_state == "RUNNING" else "",
            }
            return maintenance.CommandResult(0, values[format_value] + "\n", "")
        if command[:3] == ("/usr/bin/sacct", "--noheader", "--parsable2"):
            if self.sacct_rows is not None:
                return maintenance.CommandResult(0, self.sacct_rows, "")
            return maintenance.CommandResult(
                0,
                "101|FAILED|1:0\n" if self.fail_at == "smoke-failure" else "101|COMPLETED|0:0\n",
                "",
            )
        if command and command[0] == "/usr/bin/ssh":
            return self._ssh(command)
        if command[:4] == ("/usr/sbin/runuser", "--user", "loom-builder", "--"):
            return self._builder_sbatch(command)
        if command[:4] == ("/usr/sbin/runuser", "--user", "loom-rollout", "--"):
            if "--test-only" in command:
                if "--partition=trial" in command:
                    return maintenance.CommandResult(0, "", "")
                return maintenance.CommandResult(1, "", "invalid account or QoS")
        raise AssertionError(f"unexpected command: {command!r}")

    def _ssh(self, command: tuple[str, ...]) -> maintenance.CommandResult:
        remote = tuple(shlex.split(command[-1]))
        assert remote[:2] == ("sudo", "--")
        action = remote[2]
        if action == str(self.candidate_root / "scripts/ops/task_image_builder_host_converge.py"):
            host_action = remote[3]
            self.remote_actions.append(host_action)
            if self.fail_at == f"remote-{host_action}":
                return maintenance.CommandResult(1, "", f"injected {host_action} failure")
            return maintenance.CommandResult(
                0,
                json.dumps(
                    {
                        "state": (
                            "planned"
                            if host_action == "plan"
                            else "host_prepared"
                            if host_action != "rollback"
                            else "rolled_back"
                        ),
                        "production_certification_allowed": False,
                        "certified_nodes": [],
                        "blockers": ["phase2_guard_provider_release_missing"],
                    }
                ),
                "",
            )
        if action == str(
            self.candidate_root / "scripts/ops/task_image_builder_node_maintenance.py"
        ):
            if remote[3] == "--internal-node-daemon":
                assert remote[4] in {"restart", "check"}
                self.remote_actions.append("slurmd-" + remote[4])
                if self.fail_at == "daemon" and remote[4] == "restart":
                    self.fail_at = None
                    return maintenance.CommandResult(1, "", "injected daemon failure")
                daemon = {
                    "state": "active",
                    "cgroup_config": {
                        "path": "/etc/slurm/cgroup.conf",
                        "contents": "CgroupPlugin=autodetect\n",
                    },
                }
                daemon["cgroup_config"]["sha256"] = sha256(
                    daemon["cgroup_config"]["contents"].encode("utf-8")
                ).hexdigest()
                return maintenance.CommandResult(0, json.dumps(daemon), "")
            assert remote[3] == "--internal-smoke"
            smoke_action, job_id, operation_id = remote[4:7]
            assert job_id == "101"
            assert operation_id == OPERATION_ID
            self.remote_actions.append("smoke-" + smoke_action)
            if smoke_action == "observe":
                if self.fail_at == "smoke-observe-release-disconnect":
                    raise OSError("injected observation transport disconnect")
                if self.fail_at == "smoke-observe-disconnect":
                    self.fail_at = None
                    raise OSError("injected transport disconnect")
                return maintenance.CommandResult(
                    0,
                    maintenance._canonical(
                        {"state": "observed", "evidence": self.smoke_evidence}
                    ).decode("utf-8")
                    + "\n",
                    "",
                )
            if smoke_action == "release":
                if self.fail_at == "smoke-observe-release-disconnect":
                    self.sacct_rows = "101|TIMEOUT|0:0\n"
                    self.smoke_state = "TIMEOUT"
                    raise maintenance.MaintenanceError("injected release transport failure")
                self.smoke_released = True
                self.smoke_state = "COMPLETED"
                return maintenance.CommandResult(0, '{"state":"released"}\n', "")
            if smoke_action == "cleanup":
                return maintenance.CommandResult(
                    0,
                    (
                        '{"job_directory_absent":true,"mounts_absent":true,'
                        '"processes_absent":true,"state":"absent"}\n'
                    ),
                    "",
                )
            raise AssertionError(smoke_action)
        raise AssertionError(f"unexpected remote command: {remote!r}")

    def _builder_sbatch(self, command: tuple[str, ...]) -> maintenance.CommandResult:
        sbatch = command[4:]
        assert sbatch[0] == "/usr/bin/sbatch"
        if "--test-only" in sbatch:
            return maintenance.CommandResult(0, "", "")
        assert "--parsable" in sbatch
        assert "--nodelist=node-1" in sbatch
        assert self.state == "DRAIN" and self.reason == LOOM_REASON
        self.smoke_state = "PENDING"
        self.smoke_reason = "ReqNodeNotAvail, UnavailableNodes:node-1"
        if self.fail_at == "smoke-submit":
            return maintenance.CommandResult(1, "", "injected smoke submission failure")
        return maintenance.CommandResult(0, "101\n", "")


@dataclass(frozen=True)
class Fixture:
    candidate_root: Path
    bundle: Path
    receipt_root: Path
    runner: FakeController


def _fixture(tmp_path: Path) -> Fixture:
    candidate_root = tmp_path / "candidate"
    binding_manifest = json.loads(
        maintenance.authority.DEFAULT_MANIFEST.read_text(encoding="utf-8")
    )
    manifest_destination = candidate_root / maintenance.authority.MANIFEST_RELATIVE_PATH
    manifest_destination.parent.mkdir(parents=True)
    shutil.copyfile(maintenance.authority.DEFAULT_MANIFEST, manifest_destination)
    for component in binding_manifest["components"]:
        source = maintenance.ROOT / component["path"]
        destination = candidate_root / component["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    policy = candidate_root / "deploy/task-image-builder/prerequisites-v1.toml"
    host = candidate_root / "scripts/ops/task_image_builder_host_converge.py"
    policy.parent.mkdir(parents=True, exist_ok=True)
    host.parent.mkdir(parents=True, exist_ok=True)
    policy.write_text(
        """
schema = "loom.task-image-builder-prerequisites/v1"
production_certification_allowed = false
certified_nodes = []
unconditional_blockers = ["phase2_guard_provider_release_missing"]

[resource_profile]
cpus = 8
memory_mib = 32768
pids = 4096
wall_time = "02:00:00"
swap_bytes = 0

[[clusters]]
id = "test"
controller = "test-controller"
builder_nodes = ["node-1"]
trial_partition = "trial"
builder_partition = "loom-task-builder"
slurm_account = "loom-task-builder"
slurm_qos = "loom-task-image-builder-rootless-test"
""".lstrip(),
        encoding="utf-8",
    )
    host.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    host.chmod(0o755)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    receipt_root = tmp_path / "receipts"
    receipt_root.mkdir(mode=0o700)
    return Fixture(candidate_root, bundle, receipt_root, FakeController(candidate_root))


def _maintain(fixture: Fixture, action: str = "apply") -> dict[str, object]:
    return maintenance.maintain_node(
        action,
        "test",
        "node-1",
        fixture.candidate_root,
        fixture.bundle,
        fixture.receipt_root,
        fixture.runner,
        operation_id=OPERATION_ID,
        effective_uid=os.geteuid(),
        required_owner=os.geteuid(),
        monotonic=fixture.runner.monotonic,
        poll_interval=0,
    )


def _receipt(fixture: Fixture) -> dict[str, object]:
    paths = list(fixture.receipt_root.glob("*.json"))
    assert len(paths) == 1
    return json.loads(paths[0].read_text(encoding="utf-8"))


def test_apply_records_prestate_drains_then_preflights_and_activates_only_target(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.active_jobs = ["44"]
    fixture.runner.allocated_tres = "cpu=8,mem=32768M"
    first_poll = True
    original = fixture.runner.run

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        nonlocal first_poll
        if command[:3] == ("/usr/bin/squeue", "--nodelist", "node-1") and first_poll:
            first_poll = False
            result = original(command)
            fixture.runner.active_jobs = []
            fixture.runner.allocated_tres = "cpu=0,mem=0M"
            return result
        return original(command)

    fixture.runner.run = run  # type: ignore[method-assign]
    result = _maintain(fixture)
    receipt = _receipt(fixture)

    assert result["state"] == "prepared"
    assert receipt["schema"] == "loom.task-image-builder-node-maintenance/v1"
    assert receipt["pre_state"] == {
        "allocated_tres": "cpu=8,mem=32768M",
        "reason": "none",
        "state": "IDLE",
    }
    assert fixture.runner.drain_reasons == [LOOM_REASON]
    assert fixture.runner.remote_actions[:4] == ["plan", "apply", "slurmd-restart", "slurmd-check"]
    assert fixture.runner.remote_actions[-1] == "check"
    assert fixture.runner.state == "IDLE"
    assert "scancel" not in " ".join(" ".join(item) for item in fixture.runner.commands)


def test_foreign_drain_is_blocked_without_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.state = "DRAIN"
    fixture.runner.reason = "operator-maintenance"

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert _receipt(fixture)["pre_state"]["reason"] == "operator-maintenance"
    assert _receipt(fixture)["terminal_state"] == "blocked"
    assert fixture.runner.drain_reasons == []
    assert fixture.runner.remote_actions == []


@pytest.mark.parametrize("reason", ["", "None"])
def test_drained_node_without_the_exact_loom_reason_is_foreign(
    tmp_path: Path,
    reason: str,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.state = "DRAIN*"
    fixture.runner.reason = reason

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert fixture.runner.drain_reasons == []
    assert fixture.runner.remote_actions == []


def test_present_empty_allocated_tres_is_canonical_zero() -> None:
    assert maintenance._is_zero_tres("") is True


@pytest.mark.parametrize(
    "value",
    ["cpu", "=0", "cpu=unknown", "cpu=0garbage", "cpu=1", "cpu=0,mem=1M"],
)
def test_malformed_or_nonzero_allocated_tres_is_not_idle(value: str) -> None:
    assert maintenance._is_zero_tres(value) is False


def test_missing_allocated_tres_remains_incomplete() -> None:
    class MissingAllocTres:
        @staticmethod
        def run(args: tuple[str, ...]) -> maintenance.CommandResult:
            assert args == ("/usr/bin/scontrol", "show", "node", "node-1", "-o")
            return maintenance.CommandResult(
                0,
                "NodeName=node-1 State=IDLE Reason=none\n",
                "",
            )

    with pytest.raises(maintenance.MaintenanceError, match="incomplete"):
        maintenance._snapshot(MissingAllocTres(), "node-1")


def test_admission_and_smoke_are_exactly_scoped_and_resume_owned_drain_last(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    _maintain(fixture)

    builder_tests = [
        command
        for command in fixture.runner.commands
        if command[:4] == ("/usr/sbin/runuser", "--user", "loom-builder", "--")
        and "--test-only" in command
    ]
    assert len(builder_tests) == 1
    assert builder_tests[0][4:] == (
        "/usr/bin/sbatch",
        "--test-only",
        "--account=loom-task-builder",
        "--qos=loom-task-image-builder-rootless-test",
        "--partition=loom-task-builder",
        "--cpus-per-task=8",
        "--mem=32768M",
        "--time=02:00:00",
        "--wrap=/usr/bin/true",
    )
    assert any(
        command[:4] == ("/usr/sbin/runuser", "--user", "loom-rollout", "--")
        and "--test-only" in command
        and "--wrap=/usr/bin/true" in command
        for command in fixture.runner.commands
    )
    smoke = next(command for command in fixture.runner.commands if "--parsable" in command)
    smoke_script = next(value for value in smoke if value.startswith("--wrap="))
    # The fixed evidence schema contains "image-builder"; this smoke must still
    # not invoke image, credential, or mount tooling.
    for forbidden in ("buildkit", "docker", "credential", "mount"):
        assert forbidden not in smoke_script.casefold()
    for required in (
        "cpuset.cpus.effective",
        "memory.max",
        "memory.swap.max",
        "devices",
    ):
        assert required in smoke_script
    assert "cpu.max" not in smoke_script
    assert "pids.max" not in smoke_script
    resume_index = next(
        index
        for index, command in enumerate(fixture.runner.commands)
        if command[:3] == ("/usr/bin/scontrol", "update", "NodeName=node-1")
        and "State=RESUME" in command
    )
    pending_index = next(
        index
        for index, command in enumerate(fixture.runner.commands)
        if command[:2] == ("/usr/bin/squeue", "--job")
    )
    assert pending_index < resume_index
    job_readbacks = [
        command
        for command in fixture.runner.commands
        if command[:2] == ("/usr/bin/squeue", "--job")
    ]
    assert [command[-1] for command in job_readbacks] == [
        "--format=%T",
        "--format=%R",
        "--format=%T",
        "--format=%N",
    ]
    assert any(command[0] == "/usr/bin/sacct" for command in fixture.runner.commands)
    receipt = _receipt(fixture)
    smoke_facts = receipt["observations"]["smoke"]
    assert smoke_facts["cgroup"] == fixture.runner.smoke_evidence["controls"]
    assert smoke_facts["cgroup_path"] == "/slurm/uid_993/job_101/step_batch"
    assert smoke_facts["cleanup"] == {
        "processes_absent": True,
        "mounts_absent": True,
        "job_directory_absent": True,
    }
    assert fixture.runner.remote_actions[-4:] == [
        "smoke-observe",
        "smoke-release",
        "smoke-cleanup",
        "check",
    ]


def test_controller_rejects_smoke_evidence_that_differs_from_observed_policy(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    controls = dict(fixture.runner.smoke_evidence["controls"])
    controls["memory_max"] = 1
    fixture.runner.smoke_evidence = {
        **fixture.runner.smoke_evidence,
        "controls": controls,
    }

    result = _maintain(fixture)

    assert result["state"] == "rolled_back"
    assert _receipt(fixture)["terminal_state"] == "rolled_back"
    assert fixture.runner.remote_actions.index(
        "smoke-release"
    ) < fixture.runner.remote_actions.index("rollback")


def test_smoke_script_keeps_awk_fields_literal_and_queries_kernel_device_programs(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = maintenance._load_policy(fixture.candidate_root, "test", "node-1")

    smoke = maintenance._smoke_script(policy, OPERATION_ID)

    assert "'$1 == 0 {print $3}'" in smoke
    assert "bpftool -j cgroup show" in smoke
    assert "devices.json" in smoke


def test_real_bpftool_string_attach_flags_are_preserved_in_the_receipt(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = _maintain(fixture)

    assert result["state"] == "prepared"
    programs = _receipt(fixture)["observations"]["smoke"]["cgroup"]["devices"]["programs"]
    assert programs == [
        {
            "id": 19,
            "attach_type": "cgroup_device",
            "attach_flags": "multi",
            "name": "loom_devices",
        }
    ]


def test_internal_smoke_parses_documented_bpftool_json_shape(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jobs_root = tmp_path / "jobs"
    job_root = jobs_root / "101"
    job_root.mkdir(parents=True, mode=0o700)
    evidence = {
        "schema": "loom.task-image-builder-maintenance-smoke/v1",
        "operation_id": OPERATION_ID,
        "job_id": "101",
        "cgroup_path": "/slurm/uid_993/job_101/step_batch",
        "controls": {
            "cpuset_cpus_effective": "0-7",
            "cpuset_cpu_count": 8,
            "memory_max": 34359738368,
            "memory_swap_max": 0,
        },
    }
    evidence_path = job_root / "evidence.json"
    evidence_path.write_bytes(maintenance._canonical(evidence) + b"\n")
    evidence_path.chmod(0o600)
    devices_path = job_root / "devices.json"
    devices_path.write_bytes(
        maintenance._canonical(
            [
                {
                    "id": 19,
                    "attach_type": "cgroup_device",
                    "attach_flags": "multi",
                    "name": "loom_devices",
                }
            ]
        )
        + b"\n"
    )
    devices_path.chmod(0o600)

    result = maintenance._internal_smoke(
        "observe",
        "101",
        OPERATION_ID,
        jobs_root=jobs_root,
        smoke_owner=os.geteuid(),
    )

    assert result == 0
    observed = json.loads(capsys.readouterr().out)
    assert observed["evidence"]["controls"]["devices"]["programs"] == json.loads(
        devices_path.read_text(encoding="utf-8")
    )


def test_internal_smoke_cleanup_reports_process_mount_and_directory_absence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jobs_root = tmp_path / "jobs"
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text("", encoding="utf-8")

    result = maintenance._internal_smoke(
        "cleanup",
        "101",
        OPERATION_ID,
        jobs_root=jobs_root,
        smoke_owner=os.geteuid(),
        proc_root=proc_root,
        mountinfo_path=mountinfo,
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out) == {
        "state": "absent",
        "processes_absent": True,
        "mounts_absent": True,
        "job_directory_absent": True,
    }


def test_internal_smoke_cleanup_detects_a_surviving_job_process(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jobs_root = tmp_path / "jobs"
    proc_root = tmp_path / "proc"
    process = proc_root / "42"
    process.mkdir(parents=True)
    (process / "cgroup").write_text(
        "0::/slurm/uid_993/job_101/step_batch\n",
        encoding="utf-8",
    )
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text("", encoding="utf-8")

    result = maintenance._internal_smoke(
        "cleanup",
        "101",
        OPERATION_ID,
        jobs_root=jobs_root,
        smoke_owner=os.geteuid(),
        proc_root=proc_root,
        mountinfo_path=mountinfo,
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out) == {"state": "present"}


def test_internal_smoke_cleanup_detects_a_surviving_job_mount(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    jobs_root = tmp_path / "jobs"
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        f"36 25 0:32 / {jobs_root / '101'} rw,relatime - tmpfs tmpfs rw\n",
        encoding="utf-8",
    )

    result = maintenance._internal_smoke(
        "cleanup",
        "101",
        OPERATION_ID,
        jobs_root=jobs_root,
        smoke_owner=os.geteuid(),
        proc_root=proc_root,
        mountinfo_path=mountinfo,
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out) == {"state": "present"}


@pytest.mark.parametrize(
    "programs",
    [
        [],
        [
            {
                "id": 19,
                "attach_type": "cgroup_device",
                "attach_flags": 0,
                "name": "loom_devices",
            }
        ],
    ],
)
def test_empty_or_malformed_device_program_evidence_is_rejected(
    tmp_path: Path,
    programs: list[dict[str, object]],
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = maintenance._load_policy(fixture.candidate_root, "test", "node-1")
    evidence = json.loads(json.dumps(fixture.runner.smoke_evidence))
    evidence["controls"]["devices"]["programs"] = programs

    with pytest.raises(
        maintenance.MaintenanceError,
        match="maintenance smoke control evidence does not match policy",
    ):
        maintenance._validate_smoke_evidence(evidence, policy, "101", OPERATION_ID)


def test_phase1_smoke_binds_cpuset_cardinality_without_phase2_cpu_or_pid_limits(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = maintenance._load_policy(fixture.candidate_root, "test", "node-1")
    evidence = json.loads(json.dumps(fixture.runner.smoke_evidence))
    evidence["controls"] = {
        "cpuset_cpus_effective": "0-3,8-11",
        "cpuset_cpu_count": 8,
        "memory_max": 34359738368,
        "memory_swap_max": 0,
        "devices": evidence["controls"]["devices"],
    }

    observed = maintenance._validate_smoke_evidence(
        evidence,
        policy,
        "101",
        OPERATION_ID,
    )
    smoke = maintenance._smoke_script(policy, OPERATION_ID)

    assert observed["controls"] == evidence["controls"]
    assert "cpuset.cpus.effective" in smoke
    assert "cpu.max" not in smoke
    assert "pids.max" not in smoke


@pytest.mark.parametrize(
    "terminal_state",
    ["FAILED", "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED"],
)
def test_terminal_smoke_state_before_running_fails_without_retrying_until_timeout(
    tmp_path: Path,
    terminal_state: str,
) -> None:
    fixture = _fixture(tmp_path)
    original = fixture.runner.run

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        result = original(command)
        if (
            command[:3] == ("/usr/bin/scontrol", "update", "NodeName=node-1")
            and "State=RESUME" in command
        ):
            fixture.runner.smoke_state = terminal_state
        return result

    fixture.runner.run = run  # type: ignore[method-assign]
    result = _maintain(fixture)

    assert result["state"] == "rolled_back"
    assert (
        _receipt(fixture)["failure"] == f"maintenance smoke entered terminal state {terminal_state}"
    )


def test_sacct_step_rows_do_not_make_successful_smoke_accounting_ambiguous(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.sacct_rows = (
        "101|COMPLETED|0:0\n101.batch|COMPLETED|0:0\n101.extern|COMPLETED|0:0\n"
    )

    result = _maintain(fixture)

    assert result["state"] == "prepared"


def test_ownership_loss_after_host_apply_prevents_daemon_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    original_ssh = fixture.runner._ssh

    def ssh(command: tuple[str, ...]) -> maintenance.CommandResult:
        result = original_ssh(command)
        remote = tuple(shlex.split(command[-1]))
        if remote[2:4] == (
            str(fixture.candidate_root / "scripts/ops/task_image_builder_host_converge.py"),
            "apply",
        ):
            fixture.runner.reason = "operator-maintenance"
        return result

    fixture.runner._ssh = ssh  # type: ignore[method-assign]
    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert "slurmd-restart" not in fixture.runner.remote_actions


def test_observe_disconnect_releases_smoke_before_rollback(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "smoke-observe-disconnect"

    result = _maintain(fixture)

    assert result["state"] == "rolled_back"
    assert fixture.runner.smoke_released is True
    assert fixture.runner.remote_actions.index(
        "smoke-release"
    ) < fixture.runner.remote_actions.index("rollback")


def test_emergency_path_receipt_binds_self_termination_and_containment_before_rollback(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "smoke-observe-release-disconnect"

    result = _maintain(fixture)
    receipt = _receipt(fixture)

    assert result["state"] == "rolled_back"
    containment = receipt["observations"]["emergency_containment"]
    assert containment["job_id"] == "101"
    assert containment["release"] == {
        "command": [
            str(fixture.candidate_root / "scripts/ops/task_image_builder_node_maintenance.py"),
            "--internal-smoke",
            "release",
            "101",
            OPERATION_ID,
        ],
        "error": "injected release transport failure",
        "outcome": "transport_unavailable",
    }
    assert containment["accounting"]["top_level"] == {
        "exit_code": "0:0",
        "job_id": "101",
        "state": "TIMEOUT",
    }
    assert {
        key: containment["cleanup"][key]
        for key in ("processes_absent", "mounts_absent", "job_directory_absent")
    } == {
        "processes_absent": True,
        "mounts_absent": True,
        "job_directory_absent": True,
    }
    assert containment["owned_drain"] == {
        "allocated_tres": "cpu=0,mem=0M",
        "reason": LOOM_REASON,
        "state": "DRAIN",
    }
    assert containment["idle"]["zero_active_jobs"] is True
    assert containment["idle"]["zero_allocated_tres"] is True
    event_types = [event["type"] for event in receipt["events"]]
    assert event_types.index("emergency_contained") < event_types.index("rolled_back")
    assert fixture.runner.remote_actions.index(
        "smoke-cleanup"
    ) < fixture.runner.remote_actions.index("rollback")
    assert "scancel" not in " ".join(" ".join(command) for command in fixture.runner.commands)


def test_unverifiable_emergency_accounting_blocks_without_host_or_daemon_rollback(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    controls = dict(fixture.runner.smoke_evidence["controls"])
    controls["memory_max"] = 1
    fixture.runner.smoke_evidence = {**fixture.runner.smoke_evidence, "controls": controls}
    fixture.runner.sacct_rows = ""

    result = _maintain(fixture)
    receipt = _receipt(fixture)

    assert result["state"] == "blocked"
    assert receipt["terminal_state"] == "blocked"
    assert "containment" in receipt["failure"]
    assert "rollback" not in fixture.runner.remote_actions
    assert fixture.runner.state == "DRAIN"
    assert fixture.runner.reason == LOOM_REASON


def test_smoke_uses_and_cleans_up_an_operation_scoped_scheduler_reservation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = _maintain(fixture)

    assert result["state"] == "prepared"
    reservation = "loom_task_builder_maintenance_00000000000040008000000000000021"
    assert (
        "/usr/bin/scontrol",
        "create",
        "reservation",
        f"Name={reservation}",
        "Nodes=node-1",
        "Users=loom-builder",
        "StartTime=now",
        "Duration=00:15:00",
    ) in fixture.runner.commands
    smoke = next(command for command in fixture.runner.commands if "--parsable" in command)
    assert f"--reservation={reservation}" in smoke
    assert (
        "/usr/bin/scontrol",
        "delete",
        "reservation",
        f"Name={reservation}",
    ) in fixture.runner.commands
    receipt = _receipt(fixture)
    reservation_facts = receipt["observations"]["reservation"]
    assert reservation_facts["name"] == reservation
    assert reservation_facts["create"] == {
        "command": [
            "/usr/bin/scontrol",
            "create",
            "reservation",
            f"Name={reservation}",
            "Nodes=node-1",
            "Users=loom-builder",
            "StartTime=now",
            "Duration=00:15:00",
        ],
        "returncode": 0,
        "stdout": "",
        "stderr": "",
    }
    assert reservation_facts["binding"] == {
        "name": reservation,
        "node": "node-1",
        "state": "ACTIVE",
        "user": "loom-builder",
    }
    assert reservation_facts["create_readback"]["command"] == [
        "/usr/bin/scontrol",
        "show",
        "reservation",
        "--oneliner",
    ]
    assert reservation_facts["delete"]["command"] == [
        "/usr/bin/scontrol",
        "delete",
        "reservation",
        f"Name={reservation}",
    ]
    assert reservation_facts["absence"] == {"name": reservation, "absent": True}
    assert reservation_facts["delete_readback"]["command"] == [
        "/usr/bin/scontrol",
        "show",
        "reservation",
        "--oneliner",
    ]
    readback_indexes = [
        index
        for index, command in enumerate(fixture.runner.commands)
        if command == ("/usr/bin/scontrol", "show", "reservation", "--oneliner")
    ]
    resume_index = next(
        index
        for index, command in enumerate(fixture.runner.commands)
        if command[:3] == ("/usr/bin/scontrol", "update", "NodeName=node-1")
        and "State=RESUME" in command
    )
    delete_index = fixture.runner.commands.index(
        ("/usr/bin/scontrol", "delete", "reservation", f"Name={reservation}")
    )
    ordinary_index = next(
        index
        for index, command in enumerate(fixture.runner.commands)
        if command[:4] == ("/usr/sbin/runuser", "--user", "loom-rollout", "--")
        and "--partition=trial" in command
    )
    assert readback_indexes[0] < resume_index
    assert delete_index < readback_indexes[-1] < ordinary_index
    assert fixture.runner.legacy_reservation == "legacy_operator_hold"
    assert not any(
        command[:3] == ("/usr/bin/scontrol", "delete", "reservation")
        and "Name=legacy_operator_hold" in command
        for command in fixture.runner.commands
    )


def test_reservation_exclusion_outlives_maximum_pending_and_running_poll_budgets(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    original = fixture.runner.run
    reservation_expires_at: float | None = None
    competing_allocation_started = False
    pending_delayed = False
    running_delayed = False

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        nonlocal competing_allocation_started, pending_delayed, reservation_expires_at
        nonlocal running_delayed
        result = original(command)
        if command[:3] == ("/usr/bin/scontrol", "create", "reservation"):
            duration = next(item[9:] for item in command if item.startswith("Duration="))
            hours, minutes, seconds = (int(field) for field in duration.split(":"))
            reservation_expires_at = fixture.runner.clock + hours * 3600 + minutes * 60 + seconds
        if command[:4] == ("/usr/sbin/runuser", "--user", "loom-builder", "--") and (
            "--parsable" in command
        ):
            fixture.runner.clock += 60.0
        if command[:2] == ("/usr/bin/squeue", "--job"):
            field = next(item[9:] for item in command if item.startswith("--format="))
            if field == "%R" and not pending_delayed:
                pending_delayed = True
                fixture.runner.clock += maintenance.DEFAULT_TIMEOUT_SECONDS - 1.0
                return maintenance.CommandResult(0, "Resources\n", "")
            if field == "%T" and fixture.runner.smoke_state == "RUNNING" and not running_delayed:
                running_delayed = True
                fixture.runner.clock += maintenance.DEFAULT_TIMEOUT_SECONDS - 1.0
                return maintenance.CommandResult(0, "PENDING\n", "")
        if (
            command[:3] == ("/usr/bin/squeue", "--nodelist", "node-1")
            and fixture.runner.smoke_state == "RUNNING"
            and reservation_expires_at is not None
            and fixture.runner.clock >= reservation_expires_at
        ):
            competing_allocation_started = True
            return maintenance.CommandResult(0, "101\n999\n", "")
        return result

    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert competing_allocation_started is False
    assert result["state"] == "prepared"


def test_existing_operation_named_reservation_is_not_modified_or_deleted(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    reservation = "loom_task_builder_maintenance_00000000000040008000000000000021"
    fixture.runner.reservation = reservation

    result = _maintain(fixture)

    assert result["state"] == "rolled_back"
    assert fixture.runner.reservation == reservation
    assert not any(
        command[:3]
        in {
            ("/usr/bin/scontrol", "create", "reservation"),
            ("/usr/bin/scontrol", "delete", "reservation"),
        }
        for command in fixture.runner.commands
    )


def test_unverifiable_reservation_absence_blocks_before_rollback(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    reservation = "loom_task_builder_maintenance_00000000000040008000000000000021"
    original = fixture.runner.run
    deleted = False

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        nonlocal deleted
        result = original(command)
        if command == (
            "/usr/bin/scontrol",
            "delete",
            "reservation",
            f"Name={reservation}",
        ):
            deleted = True
        if deleted and command == (
            "/usr/bin/scontrol",
            "show",
            "reservation",
            "--oneliner",
        ):
            return maintenance.CommandResult(
                0,
                f"ReservationName={reservation} Nodes=node-1 Users=loom-builder State=ACTIVE\n",
                "",
            )
        return result

    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert "reservation cleanup" in _receipt(fixture)["failure"]
    assert "rollback" not in fixture.runner.remote_actions


@pytest.mark.parametrize("failure", ["remote-plan", "remote-apply", "daemon", "smoke-failure"])
def test_failure_never_resumes_unverified_node_and_rolls_back_after_apply(
    tmp_path: Path,
    failure: str,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = failure

    result = _maintain(fixture)
    receipt = _receipt(fixture)

    assert result["state"] in {"blocked", "rolled_back"}
    if failure != "smoke-failure":
        assert not any("State=RESUME" in command for command in fixture.runner.commands)
    if failure in {"remote-apply", "daemon", "smoke-failure"}:
        assert receipt["terminal_state"] == "rolled_back"
        assert fixture.runner.remote_actions[-3:] == ["rollback", "slurmd-restart", "slurmd-check"]
        assert fixture.runner.state == "DRAIN"
        assert fixture.runner.reason == LOOM_REASON
    else:
        assert receipt["terminal_state"] == "blocked"


def test_rollback_failure_keeps_node_drained_with_receipt_digest(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "remote-apply"
    original_ssh = fixture.runner._ssh

    def ssh(command: tuple[str, ...]) -> maintenance.CommandResult:
        remote = tuple(shlex.split(command[-1]))
        if remote[2:4] == (
            str(fixture.candidate_root / "scripts/ops/task_image_builder_host_converge.py"),
            "rollback",
        ):
            return maintenance.CommandResult(1, "", "injected rollback failure")
        return original_ssh(command)

    fixture.runner._ssh = ssh  # type: ignore[method-assign]

    result = _maintain(fixture)
    receipt = _receipt(fixture)

    assert result["state"] == "drained_rollback_failed"
    assert receipt["terminal_state"] == "drained_rollback_failed"
    assert fixture.runner.state == "DRAIN"
    assert fixture.runner.reason.startswith(LOOM_REASON + "/rollback-failed/")
    assert len(fixture.runner.reason.rsplit("/", 1)[1]) == 64


def test_ownership_loss_prevents_resume(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    original = fixture.runner.run

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        if (
            command == ("/usr/bin/scontrol", "show", "node", "node-1", "-o")
            and fixture.runner.smoke_state == "PENDING"
        ):
            fixture.runner.reason = "another-operator"
        return original(command)

    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert not any("State=RESUME" in command for command in fixture.runner.commands)


def test_plan_and_check_are_read_only(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    planned = _maintain(fixture, "plan")
    checked = _maintain(fixture, "check")

    assert planned["state"] == "planned"
    assert checked["state"] == "checked"
    assert fixture.runner.drain_reasons == []
    assert fixture.runner.remote_actions == ["plan", "check"]
    assert list(fixture.receipt_root.iterdir()) == []


def test_receipt_events_are_hash_chained_and_inert(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    _maintain(fixture)
    receipt = _receipt(fixture)

    assert receipt["production_certification_allowed"] is False
    assert receipt["certified_nodes"] == []
    assert receipt["blockers"] == ["phase2_guard_provider_release_missing"]
    assert [event["sequence"] for event in receipt["events"]] == list(range(len(receipt["events"])))
    assert receipt["events"][-1]["type"] == "prepared"


def test_remote_payload_is_one_shell_quoted_allowlisted_command(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    command = (
        str(fixture.candidate_root / "scripts/ops/task_image_builder_host_converge.py"),
        "plan",
        "--bundle",
        "/safe path/with;metacharacters",
    )

    maintenance._remote(
        fixture.runner,
        "node-1",
        command,
        ssh_config=None,
        candidate_root=fixture.candidate_root,
    )

    ssh = fixture.runner.commands[-1]
    assert ssh[-1] == shlex.join(("sudo", "--", *command))
    assert ssh.count(ssh[-1]) == 1
    with pytest.raises(maintenance.MaintenanceError, match="remote command is unsafe"):
        maintenance._remote(
            fixture.runner,
            "node-1\nunsafe",
            command,
            ssh_config=None,
            candidate_root=fixture.candidate_root,
        )


def test_accounting_lag_is_polled_until_successful_smoke_record(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    original = fixture.runner.run
    accounting_reads = 0

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        nonlocal accounting_reads
        if command[:3] == ("/usr/bin/sacct", "--noheader", "--parsable2"):
            accounting_reads += 1
            if accounting_reads < 3:
                return maintenance.CommandResult(0, "", "")
        return original(command)

    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "prepared"
    assert accounting_reads == 3


def test_competing_job_after_resume_fails_closed_and_rolls_back(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    original = fixture.runner.run

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        if (
            command[:3] == ("/usr/bin/squeue", "--nodelist", "node-1")
            and fixture.runner.smoke_state == "RUNNING"
        ):
            return maintenance.CommandResult(0, "101\n999\n", "")
        return original(command)

    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "rolled_back"
    assert fixture.runner.state == "DRAIN"
    assert sum("State=RESUME" in command for command in fixture.runner.commands) == 1


def test_timeout_before_host_mutation_remains_blocked(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.active_jobs = ["44"]
    clock_values = iter((0.0, 301.0, 302.0))

    result = maintenance.maintain_node(
        "apply",
        "test",
        "node-1",
        fixture.candidate_root,
        fixture.bundle,
        fixture.receipt_root,
        fixture.runner,
        operation_id=OPERATION_ID,
        effective_uid=os.geteuid(),
        required_owner=os.geteuid(),
        monotonic=lambda: next(clock_values),
        poll_interval=0,
    )

    assert result["state"] == "blocked"
    assert fixture.runner.remote_actions == []


def test_post_apply_receipt_replace_failure_rolls_back_without_corrupting_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    original_replace = maintenance.os.replace
    replacements = 0

    def replace(source: Path, target: Path) -> None:
        nonlocal replacements
        replacements += 1
        if replacements == 4:
            raise OSError("injected replacement failure")
        original_replace(source, target)

    monkeypatch.setattr(maintenance.os, "replace", replace)

    result = _maintain(fixture)
    receipt_path = next(fixture.receipt_root.glob("*.json"))

    assert result["state"] == "rolled_back"
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["terminal_state"] == "rolled_back"


def test_rollback_failed_reason_matches_the_final_persisted_receipt(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "remote-apply"
    original_ssh = fixture.runner._ssh

    def ssh(command: tuple[str, ...]) -> maintenance.CommandResult:
        remote = tuple(shlex.split(command[-1]))
        if remote[2:4] == (
            str(fixture.candidate_root / "scripts/ops/task_image_builder_host_converge.py"),
            "rollback",
        ):
            return maintenance.CommandResult(1, "", "injected rollback failure")
        return original_ssh(command)

    fixture.runner._ssh = ssh  # type: ignore[method-assign]

    result = _maintain(fixture)
    receipt_path = next(fixture.receipt_root.glob("*.json"))

    assert result["state"] == "drained_rollback_failed"
    assert fixture.runner.reason.rsplit("/", 1)[1] == sha256(receipt_path.read_bytes()).hexdigest()


def test_rollback_failed_drain_update_failure_is_blocked_not_claimed_safe(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "remote-apply"
    original_ssh = fixture.runner._ssh
    original_run = fixture.runner.run

    def ssh(command: tuple[str, ...]) -> maintenance.CommandResult:
        remote = tuple(shlex.split(command[-1]))
        if remote[2:4] == (
            str(fixture.candidate_root / "scripts/ops/task_image_builder_host_converge.py"),
            "rollback",
        ):
            return maintenance.CommandResult(1, "", "injected rollback failure")
        return original_ssh(command)

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        if command[:3] == ("/usr/bin/scontrol", "update", "NodeName=node-1") and any(
            value.startswith("Reason=" + LOOM_REASON + "/rollback-failed/") for value in command
        ):
            return maintenance.CommandResult(1, "", "injected drain update failure")
        return original_run(command)

    fixture.runner._ssh = ssh  # type: ignore[method-assign]
    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert _receipt(fixture)["terminal_state"] == "blocked"
    assert fixture.runner.reason == LOOM_REASON


def test_rollback_failed_drain_readback_failure_is_blocked_not_claimed_safe(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "remote-apply"
    original_ssh = fixture.runner._ssh
    original_run = fixture.runner.run

    def ssh(command: tuple[str, ...]) -> maintenance.CommandResult:
        remote = tuple(shlex.split(command[-1]))
        if remote[2:4] == (
            str(fixture.candidate_root / "scripts/ops/task_image_builder_host_converge.py"),
            "rollback",
        ):
            return maintenance.CommandResult(1, "", "injected rollback failure")
        return original_ssh(command)

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        result = original_run(command)
        if command[:3] == ("/usr/bin/scontrol", "update", "NodeName=node-1") and any(
            value.startswith("Reason=" + LOOM_REASON + "/rollback-failed/") for value in command
        ):
            fixture.runner.reason = LOOM_REASON
        return result

    fixture.runner._ssh = ssh  # type: ignore[method-assign]
    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert _receipt(fixture)["terminal_state"] == "blocked"


def test_ownership_loss_during_rollback_is_blocked_without_new_reason(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = "remote-apply"
    original = fixture.runner.run

    def run(command: tuple[str, ...]) -> maintenance.CommandResult:
        if (
            command == ("/usr/bin/scontrol", "show", "node", "node-1", "-o")
            and "rollback" in fixture.runner.remote_actions
        ):
            fixture.runner.reason = "other-operator"
        return original(command)

    fixture.runner.run = run  # type: ignore[method-assign]

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert fixture.runner.reason == "other-operator"


def test_receipt_update_failure_blocks_before_remote_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    original = maintenance._write_document

    def fail_update(*args: object, **kwargs: object) -> None:
        if kwargs.get("exclusive") is False:
            raise maintenance.MaintenanceError("injected receipt update failure")
        original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(maintenance, "_write_document", fail_update)

    result = _maintain(fixture)

    assert result["state"] == "blocked"
    assert fixture.runner.remote_actions == []
    assert not any("State=RESUME" in command for command in fixture.runner.commands)
