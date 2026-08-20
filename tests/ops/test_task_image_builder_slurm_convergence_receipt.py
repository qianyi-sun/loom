from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_slurm_converge as converge

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ops/task_image_builder_slurm_converge.py"

TRIAL_LINE = (
    "PartitionName=trial Nodes=node-[1-2] Default=NO MaxTime=1-00:00:00 "
    "State=UP PriorityTier=100 OverSubscribe=NO"
)
BUILDER_LINE = (
    "PartitionName=loom-task-builder Nodes=node-[1-2] Default=NO "
    "MaxTime=02:00:00 State=UP PriorityTier=200 "
    "AllowAccounts=loom-task-builder AllowGroups=loom-task-builder OverSubscribe=NO"
)
LEGACY_QOS = "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00|\n"
LEGACY_ASSOCIATION = (
    "test-cluster|loom-staging|loom-rollout|loom-task-image-builder,normal|normal\n"
)
LEGACY_RESERVATION = (
    "ReservationName=loom-task-image-builder Nodes=legacy-node NodeCnt=1 "
    "PartitionName=trial Users=loom-rollout Accounts=loom-staging "
    "State=ACTIVE Flags=IGNORE_JOBS,SPEC_NODES\n"
)
ROOTLESS_QOS = (
    "loom-task-image-builder-rootless-test|DenyOnLimit|0|1|1|02:00:00|"
    "cpu=8,mem=32768M,node=1|\n"
)
ROOTLESS_ASSOCIATION = (
    "test-cluster|loom-task-builder|loom-builder|loom-task-builder|"
    "loom-task-image-builder-rootless-test|loom-task-image-builder-rootless-test|\n"
)


@dataclass
class FakeSlurmRunner:
    config: Path
    converger: Path
    backup: Path
    account: bool = False
    qos: bool = False
    association: bool = False
    fail_at: str | None = None
    legacy_qos: str = LEGACY_QOS
    commands: list[tuple[str, ...]] = field(default_factory=list)
    delegate_calls: int = 0
    delegate_checks: int = 0
    after_delegate: bool = False

    def run(self, args: tuple[str, ...]) -> converge.CommandResult:
        command = tuple(args)
        self.commands.append(command)
        if command == ("/usr/bin/scontrol", "show", "config"):
            return converge.CommandResult(
                0,
                "ClusterName = test-cluster\n"
                "SlurmctldHost[0] = test-controller(127.0.0.1)\n",
                "",
            )
        if command[:3] == ("/usr/bin/sacctmgr", "--noheader", "--parsable2"):
            joined = " ".join(command)
            if "name=loom-task-image-builder-rootless-test" in joined:
                if self.after_delegate and self.fail_at == "post-readback":
                    return converge.CommandResult(1, "", "injected readback failure")
                return converge.CommandResult(0, ROOTLESS_QOS if self.qos else "", "")
            if "name=loom-task-image-builder" in joined:
                return converge.CommandResult(0, self.legacy_qos, "")
            if "account=loom-staging" in joined:
                return converge.CommandResult(0, LEGACY_ASSOCIATION, "")
            if "account=loom-task-builder" in joined:
                return converge.CommandResult(
                    0,
                    ROOTLESS_ASSOCIATION if self.association else "",
                    "",
                )
            if "show account" in joined:
                return converge.CommandResult(0, "loom-task-builder|\n" if self.account else "", "")
        if command == (
            "/usr/bin/scontrol",
            "show",
            "reservation",
            "loom-task-image-builder",
            "-o",
        ):
            return converge.CommandResult(0, LEGACY_RESERVATION, "")
        if command == (str(self.converger), "check", "test"):
            self.delegate_checks += 1
            converged = (
                BUILDER_LINE in self.config.read_text(encoding="utf-8")
                and self.account
                and self.qos
                and self.association
            )
            return converge.CommandResult(
                0 if converged else 1,
                '{"state":"prerequisites_converged"}\n' if converged else "",
                "" if converged else "prerequisites incomplete",
            )
        if command == (str(self.converger), "apply", "test"):
            self.delegate_calls += 1
            if self.fail_at == "execution":
                self.after_delegate = True
                raise converge.ConvergenceError("injected delegate execution failure")
            if BUILDER_LINE not in self.config.read_text(encoding="utf-8"):
                if not self.backup.exists():
                    self.backup.write_bytes(self.config.read_bytes())
                self.config.write_text(
                    self.config.read_text(encoding="utf-8") + BUILDER_LINE + "\n",
                    encoding="utf-8",
                )
            if self.fail_at == "partition":
                self.after_delegate = True
                return converge.CommandResult(1, "", "injected partition failure")
            self.account = True
            if self.fail_at == "qos":
                self.after_delegate = True
                return converge.CommandResult(1, "", "injected QoS failure")
            self.qos = True
            if self.fail_at == "association":
                self.after_delegate = True
                return converge.CommandResult(1, "", "injected association failure")
            self.association = True
            self.after_delegate = True
            return converge.CommandResult(0, '{"state":"prerequisites_converged"}\n', "")
        raise AssertionError(f"unexpected command: {command!r}")


@dataclass(frozen=True)
class Fixture:
    policy: Path
    config: Path
    backup: Path
    receipt_dir: Path
    paths: converge.ReleasePaths
    runner: FakeSlurmRunner


def _fixture(tmp_path: Path) -> Fixture:
    config = tmp_path / "slurm.conf"
    config.write_text(f"ClusterName=test-cluster\n{TRIAL_LINE}\n", encoding="utf-8")
    policy = tmp_path / "prerequisites-v1.toml"
    policy.write_text(
        f"""
schema = "loom.task-image-builder-prerequisites/v1"
policy_version = "task-image-builder-prerequisites-v1"
production_certification_allowed = false
certified_nodes = []
unconditional_blockers = ["phase2_guard_provider_release_missing"]

[identity]
user = "loom-builder"
group = "loom-task-builder"
uid = 993
gid = 980
subid_start = 3000000
subid_count = 65536
home = "/nonexistent"
shell = "/usr/sbin/nologin"
forbidden_supplementary_groups = ["docker", "root", "sudo"]

[resource_profile]
cpus = 8
memory_mib = 32768
wall_time = "02:00:00"
max_jobs_per_user = 1
max_submit_jobs_per_user = 1

[legacy_guard]
qos = "loom-task-image-builder"
reservation = "loom-task-image-builder"
account = "loom-staging"
user = "loom-rollout"
max_jobs_per_user = 1
max_submit_jobs_per_user = 1
max_wall = "04:00:00"

[[clusters]]
id = "test"
slurm_cluster = "test-cluster"
architecture = "x86_64"
controller = "test-controller"
trial_partition = "trial"
trial_priority_tier = 100
builder_partition = "loom-task-builder"
builder_priority_tier = 200
builder_nodes = ["node-1", "node-2"]
builder_nodes_expression = "node-[1-2]"
trial_partition_anchor = "{TRIAL_LINE}"
builder_partition_line = "{BUILDER_LINE}"
slurm_account = "loom-task-builder"
slurm_qos = "loom-task-image-builder-rootless-test"
legacy_base_qos = "normal"
legacy_reservation_node = "legacy-node"
legacy_reservation_partition = "trial"
slurm_config = "{config}"
slurm_config_owner = "root"
slurm_config_group = "root"
slurm_config_mode = "0644"
""".lstrip(),
        encoding="utf-8",
    )
    converger = tmp_path / "delegate"
    controller_installer = tmp_path / "controller-installer"
    readback = tmp_path / "readback.py"
    wrapper = tmp_path / "wrapper.py"
    for path, payload in (
        (converger, b"delegate-v1\n"),
        (controller_installer, b"controller-installer-v1\n"),
        (readback, b"readback-v1\n"),
        (wrapper, b"wrapper-v1\n"),
    ):
        path.write_bytes(payload)
        path.chmod(0o755 if path == converger else 0o644)
    backup = tmp_path / "slurm.conf.backup"
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir(mode=0o700)
    paths = converge.ReleasePaths(
        policy=policy,
        converger=converger,
        controller_installer=controller_installer,
        readback=readback,
        wrapper=wrapper,
        durable_backup=backup,
    )
    return Fixture(
        policy,
        config,
        backup,
        receipt_dir,
        paths,
        FakeSlurmRunner(config, converger, backup),
    )


def _converge(
    fixture: Fixture,
    action: str,
    *,
    operation_id: str = "00000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    return converge.converge_slurm(
        action,
        "test",
        fixture.receipt_dir,
        fixture.runner,
        fixture.paths,
        operation_id=operation_id,
        effective_uid=os.geteuid(),
        controller_host="test-controller",
        host_arch="x86_64",
        required_receipt_owner=os.geteuid(),
    )


def _receipt(fixture: Fixture) -> tuple[Path, dict[str, object]]:
    receipts = list(fixture.receipt_dir.iterdir())
    assert len(receipts) == 1
    path = receipts[0]
    return path, json.loads(path.read_text(encoding="utf-8"))


def _assert_event_chain(receipt: dict[str, object]) -> None:
    previous = "0" * 64
    events = receipt["events"]
    assert isinstance(events, list)
    for sequence, event_raw in enumerate(events):
        assert isinstance(event_raw, dict)
        event = dict(event_raw)
        digest = event.pop("event_hash")
        assert event["sequence"] == sequence
        assert event["previous_hash"] == previous
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
        assert digest == hashlib.sha256(encoded).hexdigest()
        previous = str(digest)


def _journal_document() -> dict[str, object]:
    return {"schema": "fixture", "events": []}


def test_direct_cli_loads_before_argument_parsing() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_plan_and_check_are_read_only(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    config_before = fixture.config.read_bytes()

    planned = _converge(fixture, "plan")
    with pytest.raises(converge.ConvergenceError, match="not converged"):
        _converge(fixture, "check")

    assert planned["changes"] == ["partition", "account", "qos", "association"]
    assert fixture.config.read_bytes() == config_before
    assert fixture.runner.delegate_calls == 0
    assert list(fixture.receipt_dir.iterdir()) == []


def test_apply_records_exact_created_objects_and_hash_chain(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = _converge(fixture, "apply")

    path, receipt = _receipt(fixture)
    assert result["state"] == "converged"
    assert result["receipt"] == str(path)
    assert result["production_certification_allowed"] is False
    assert result["certified_nodes"] == []
    assert result["blockers"] == ["phase2_guard_provider_release_missing"]
    assert receipt["schema"] == "loom.task-image-builder-slurm-receipt/v1"
    assert receipt["production_certification_allowed"] is False
    assert receipt["certified_nodes"] == []
    assert receipt["blockers"] == ["phase2_guard_provider_release_missing"]
    assert receipt["terminal_state"] == "converged"
    assert receipt["created_objects"] == [
        {"kind": "partition", "name": "loom-task-builder"},
        {"kind": "account", "name": "loom-task-builder"},
        {"kind": "qos", "name": "loom-task-image-builder-rootless-test"},
        {
            "kind": "association",
            "name": "test-cluster/loom-task-builder/loom-builder/loom-task-builder",
        },
    ]
    assert receipt["legacy_pre_fingerprint"] == receipt["legacy_post_fingerprint"]
    assert receipt["durable_config_backup_digest"] == hashlib.sha256(
        f"ClusterName=test-cluster\n{TRIAL_LINE}\n".encode()
    ).hexdigest()
    assert all(
        isinstance(receipt[field], str) and len(receipt[field]) == 64
        for field in ("candidate_digest", "policy_digest", "controller_digest", "cluster_digest")
    )
    assert path.stat().st_mode & 0o777 == 0o600
    _assert_event_chain(receipt)


def test_receipt_revision_file_fsync_failure_preserves_previous_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "receipt.json"
    journal = converge.ReceiptJournal(path, _journal_document())
    previous = path.read_bytes()

    with monkeypatch.context() as scoped:
        scoped.setattr(converge.os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError("fsync")))
        with pytest.raises(OSError, match="fsync"):
            journal.append("intent", {"action": "apply"})

    journal.close()
    assert path.read_bytes() == previous
    assert list(tmp_path.iterdir()) == [path]


def test_receipt_revision_replace_failure_preserves_previous_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "receipt.json"
    journal = converge.ReceiptJournal(path, _journal_document())
    previous = path.read_bytes()

    with monkeypatch.context() as scoped:
        scoped.setattr(
            converge.os,
            "replace",
            lambda source, target: (_ for _ in ()).throw(OSError("replace")),
            raising=False,
        )
        with pytest.raises(OSError, match="replace"):
            journal.append("intent", {"action": "apply"})

    journal.close()
    assert path.read_bytes() == previous
    assert list(tmp_path.iterdir()) == [path]


def test_receipt_revision_directory_fsync_failure_leaves_new_valid_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "receipt.json"
    journal = converge.ReceiptJournal(path, _journal_document())
    real_fsync = os.fsync

    def fail_directory_fsync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("directory fsync")
        real_fsync(descriptor)

    with monkeypatch.context() as scoped:
        scoped.setattr(converge.os, "fsync", fail_directory_fsync)
        with pytest.raises(OSError, match="directory fsync"):
            journal.append("intent", {"action": "apply"})

    journal.close()
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert [event["type"] for event in persisted["events"]] == ["intent"]
    _assert_event_chain(persisted)
    assert list(tmp_path.iterdir()) == [path]


def test_idempotent_apply_records_no_created_objects(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.config.write_text(
        fixture.config.read_text(encoding="utf-8") + BUILDER_LINE + "\n",
        encoding="utf-8",
    )
    fixture.runner.account = True
    fixture.runner.qos = True
    fixture.runner.association = True

    _converge(fixture, "apply")

    _, receipt = _receipt(fixture)
    assert receipt["terminal_state"] == "converged"
    assert receipt["created_objects"] == []


def test_converged_check_delegates_full_read_only_validation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.config.write_text(
        fixture.config.read_text(encoding="utf-8") + BUILDER_LINE + "\n",
        encoding="utf-8",
    )
    fixture.runner.account = True
    fixture.runner.qos = True
    fixture.runner.association = True

    checked = _converge(fixture, "check")

    assert checked["state"] == "converged"
    assert fixture.runner.delegate_checks == 1
    assert fixture.runner.delegate_calls == 0
    assert list(fixture.receipt_dir.iterdir()) == []


@pytest.mark.parametrize(
    "failure",
    ["execution", "partition", "qos", "association", "post-readback"],
)
def test_partial_failure_still_produces_durable_exact_receipt(
    tmp_path: Path,
    failure: str,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.fail_at = failure

    with pytest.raises(converge.ConvergenceError):
        _converge(fixture, "apply")

    path, receipt = _receipt(fixture)
    assert receipt["terminal_state"] == "failed"
    assert receipt["command_outcome"]["returncode"] in {0, 1, 127}
    assert receipt["pre_state"] is not None
    assert receipt["post_state"] is not None
    assert path.stat().st_mode & 0o777 == 0o600
    _assert_event_chain(receipt)
    if failure == "post-readback":
        post_state = receipt["post_state"]
        assert post_state["partition"] is not None
        assert post_state["account"] is not None
        assert post_state["qos"] is None
        assert post_state["association"] is not None
        assert post_state["legacy"] is not None
        assert receipt["post_readback_error"]["qos"]
        assert receipt["legacy_post_fingerprint"] == receipt["legacy_pre_fingerprint"]


def test_legacy_drift_prevents_delegation_and_receipt_creation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.runner.legacy_qos = LEGACY_QOS.replace("04:00:00", "08:00:00")

    with pytest.raises(converge.ConvergenceError, match="legacy"):
        _converge(fixture, "apply")

    assert fixture.runner.delegate_calls == 0
    assert list(fixture.receipt_dir.iterdir()) == []


def test_wrapper_never_emits_modify_or_delete(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    _converge(fixture, "apply")

    emitted = "\n".join(" ".join(command) for command in fixture.runner.commands).casefold()
    assert " modify " not in f" {emitted} "
    assert " delete " not in f" {emitted} "
