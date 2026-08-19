from __future__ import annotations

import grp
import os
import pwd
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-task-image-builder-prerequisites.sh"
TRIAL_LINE = (
    "PartitionName=trial Nodes=node-[1-2] Default=NO MaxTime=1-00:00:00 "
    "State=UP PriorityTier=100 OverSubscribe=NO"
)
BUILDER_LINE = (
    "PartitionName=loom-task-builder Nodes=node-[1-2] Default=NO "
    "MaxTime=02:00:00 State=UP PriorityTier=200 "
    "AllowAccounts=loom-task-builder AllowGroups=loom-task-builder OverSubscribe=NO"
)
INITIAL_CONFIG = f"ClusterName=test-cluster\n{TRIAL_LINE}\n"
EXPECTED_CONFIG = f"{INITIAL_CONFIG}{BUILDER_LINE}\n"
TRIAL_STATE = (
    "PartitionName=trial Default=NO MaxTime=1-00:00:00 Nodes=node-[1-2] "
    "PriorityTier=100 OverSubscribe=NO State=UP"
)
BUILDER_STATE = (
    "PartitionName=loom-task-builder AllowAccounts=loom-task-builder "
    "AllowGroups=loom-task-builder Default=NO MaxTime=02:00:00 "
    "Nodes=node-[1-2] PriorityTier=200 OverSubscribe=NO State=UP"
)
LEGACY_QOS = "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00||\n"
DESIRED_QOS = (
    "loom-task-image-builder|DenyOnLimit|0|1|1|02:00:00|"
    "cpu=8,mem=32768M,node=1|\n"
)
DESIRED_ASSOCIATION = (
    "test-cluster|loom-task-builder|loom-builder|loom-task-builder|"
    "loom-task-image-builder|loom-task-image-builder|\n"
)


@dataclass(frozen=True)
class Fixture:
    policy: Path
    config: Path
    state_root: Path
    account: Path
    qos: Path
    association: Path
    scontrol_log: Path
    sacctmgr_log: Path
    reconfigure_count: Path
    fake_bin: Path


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(0o755)


def _fixture(tmp_path: Path) -> Fixture:
    owner = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    config = tmp_path / "slurm.conf"
    config.write_text(INITIAL_CONFIG, encoding="utf-8")
    config.chmod(0o644)
    policy = tmp_path / "prerequisites-v1.toml"
    policy.write_text(
        f"""
schema = "loom.task-image-builder-prerequisites/v1"
policy_version = "task-image-builder-prerequisites-v1"
production_certification_allowed = false
certified_nodes = []

[identity]
user = "loom-builder"
group = "loom-task-builder"

[resource_profile]
cpus = 8
memory_mib = 32768
wall_time = "02:00:00"
max_jobs_per_user = 1
max_submit_jobs_per_user = 1

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
slurm_qos = "loom-task-image-builder"
slurm_config = "{config}"
slurm_config_owner = "{owner}"
slurm_config_group = "{group}"
slurm_config_mode = "0644"
""".lstrip(),
        encoding="utf-8",
    )
    account = tmp_path / "account"
    qos = tmp_path / "qos"
    association = tmp_path / "association"
    account.write_text("", encoding="utf-8")
    qos.write_text(LEGACY_QOS, encoding="utf-8")
    association.write_text("", encoding="utf-8")
    scontrol_log = tmp_path / "scontrol.log"
    sacctmgr_log = tmp_path / "sacctmgr.log"
    reconfigure_count = tmp_path / "reconfigure-count"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "scontrol",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$LOOM_SCONTROL_LOG"
case "$*" in
  "show config")
    printf 'ClusterName             = test-cluster\n'
    printf 'SlurmctldHost[0]        = test-controller(127.0.0.1)\n'
    ;;
  "reconfigure")
    count=0
    if [[ -f "$LOOM_RECONFIGURE_COUNT" ]]; then read -r count < "$LOOM_RECONFIGURE_COUNT"; fi
    count=$((count + 1))
    printf '%s\n' "$count" > "$LOOM_RECONFIGURE_COUNT"
    if [[ ",${LOOM_RECONFIGURE_FAIL_AT:-}," == *",$count,"* ]]; then exit 1; fi
    ;;
  "show partition trial -o")
    printf '%s\n' "$LOOM_TRIAL_STATE"
    ;;
  "show partition loom-task-builder -o")
    if grep -qxF "$LOOM_BUILDER_LINE" "$LOOM_TEST_CONFIG"; then
      printf '%s\n' "$LOOM_BUILDER_STATE"
    else
      exit 1
    fi
    ;;
  "show hostnames node-[1-2]")
    printf 'node-1\nnode-2\n'
    ;;
  *)
    printf 'unexpected scontrol command: %s\n' "$*" >&2
    exit 2
    ;;
esac
""",
    )
    _write_executable(
        fake_bin / "sacctmgr",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$LOOM_SACCTMGR_LOG"
case "$*" in
  "--noheader --parsable2 show account where name=loom-task-builder format=Account")
    cat "$LOOM_ACCOUNT_STATE"
    ;;
  "--noheader --parsable2 show qos where name=loom-task-image-builder format=Name,Flags,Priority,MaxJobsPU,MaxSubmitJobsPU,MaxWall,GrpTRES")
    cat "$LOOM_QOS_STATE"
    ;;
  "--noheader --parsable2 show association where cluster=test-cluster account=loom-task-builder user=loom-builder partition=loom-task-builder format=Cluster,Account,User,Partition,QOS,DefaultQOS")
    cat "$LOOM_ASSOCIATION_STATE"
    ;;
  "--immediate add account name=loom-task-builder cluster=test-cluster description=Loom allocation-scoped task image builders organization=loom")
    printf 'loom-task-builder|\n' > "$LOOM_ACCOUNT_STATE"
    ;;
  "--immediate modify qos where name=loom-task-image-builder set Flags=DenyOnLimit Priority=0 MaxJobsPU=1 MaxSubmitJobsPU=1 MaxWall=02:00:00 GrpTRES=cpu=8,mem=32768M,node=1")
    printf '%s' "$LOOM_DESIRED_QOS" > "$LOOM_QOS_STATE"
    ;;
  "--immediate add user name=loom-builder account=loom-task-builder cluster=test-cluster partition=loom-task-builder qos=loom-task-image-builder defaultqos=loom-task-image-builder")
    printf '%s' "$LOOM_DESIRED_ASSOCIATION" > "$LOOM_ASSOCIATION_STATE"
    ;;
  *)
    printf 'unexpected sacctmgr command: %s\n' "$*" >&2
    exit 2
    ;;
esac
""",
    )
    return Fixture(
        policy=policy,
        config=config,
        state_root=tmp_path / "state",
        account=account,
        qos=qos,
        association=association,
        scontrol_log=scontrol_log,
        sacctmgr_log=sacctmgr_log,
        reconfigure_count=reconfigure_count,
        fake_bin=fake_bin,
    )


def _run(
    fixture: Fixture,
    action: str,
    *,
    reconfigure_fail_at: str = "",
    builder_state: str = BUILDER_STATE,
) -> subprocess.CompletedProcess[str]:
    owner = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    environment = {
        **os.environ,
        "PATH": f"{fixture.fake_bin}:{os.environ['PATH']}",
        "LOOM_POLICY_PATH": str(fixture.policy),
        "LOOM_STATE_ROOT": str(fixture.state_root),
        "LOOM_STATE_OWNER": owner,
        "LOOM_STATE_GROUP": group,
        "LOOM_CONTROLLER_HOST": "test-controller",
        "LOOM_HOST_ARCH": "x86_64",
        "LOOM_TEST_CONFIG": str(fixture.config),
        "LOOM_SCONTROL_LOG": str(fixture.scontrol_log),
        "LOOM_SACCTMGR_LOG": str(fixture.sacctmgr_log),
        "LOOM_RECONFIGURE_COUNT": str(fixture.reconfigure_count),
        "LOOM_RECONFIGURE_FAIL_AT": reconfigure_fail_at,
        "LOOM_TRIAL_STATE": TRIAL_STATE,
        "LOOM_BUILDER_STATE": builder_state,
        "LOOM_BUILDER_LINE": BUILDER_LINE,
        "LOOM_ACCOUNT_STATE": str(fixture.account),
        "LOOM_QOS_STATE": str(fixture.qos),
        "LOOM_ASSOCIATION_STATE": str(fixture.association),
        "LOOM_DESIRED_QOS": DESIRED_QOS,
        "LOOM_DESIRED_ASSOCIATION": DESIRED_ASSOCIATION,
    }
    return subprocess.run(
        [
            shutil.which("bash") or "bash",
            "-c",
            'source "$1"; "loom_builder_slurm_$2" test',
            "builder-slurm-test",
            str(CONVERGER),
            action,
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _logs(fixture: Fixture) -> str:
    return "\n".join(
        path.read_text(encoding="utf-8") if path.exists() else ""
        for path in (fixture.scontrol_log, fixture.sacctmgr_log)
    )


def test_converger_parses_and_check_mode_is_read_only(tmp_path: Path) -> None:
    parsed = subprocess.run(
        [shutil.which("bash") or "bash", "-n", str(CONVERGER)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert parsed.returncode == 0, parsed.stderr
    fixture = _fixture(tmp_path)
    before = {
        fixture.config: fixture.config.read_bytes(),
        fixture.account: fixture.account.read_bytes(),
        fixture.qos: fixture.qos.read_bytes(),
        fixture.association: fixture.association.read_bytes(),
    }

    result = _run(fixture, "check")

    assert result.returncode == 1
    assert "not converged" in result.stderr
    assert {path: path.read_bytes() for path in before} == before
    assert "reconfigure" not in _logs(fixture)
    assert "--immediate" not in _logs(fixture)


def test_first_apply_converges_and_second_apply_is_idempotent(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    first = _run(fixture, "apply")

    assert first.returncode == 0, first.stderr
    assert fixture.config.read_text(encoding="utf-8") == EXPECTED_CONFIG
    assert fixture.account.read_text(encoding="utf-8") == "loom-task-builder|\n"
    assert fixture.qos.read_text(encoding="utf-8") == DESIRED_QOS
    assert fixture.association.read_text(encoding="utf-8") == DESIRED_ASSOCIATION
    assert fixture.reconfigure_count.read_text(encoding="utf-8") == "1\n"
    before = {
        path: (path.stat().st_mtime_ns, path.read_bytes())
        for path in (fixture.config, fixture.account, fixture.qos, fixture.association)
    }

    second = _run(fixture, "apply")
    checked = _run(fixture, "check")

    assert second.returncode == 0, second.stderr
    assert checked.returncode == 0, checked.stderr
    assert {
        path: (path.stat().st_mtime_ns, path.read_bytes()) for path in before
    } == before
    assert fixture.reconfigure_count.read_text(encoding="utf-8") == "1\n"
    report = checked.stdout.strip()
    assert '"production_certification_allowed":false' in report
    assert '"certified_nodes":[]' in report


def test_historical_backup_does_not_freeze_unrelated_config_after_convergence(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    assert _run(fixture, "apply").returncode == 0
    fixture.scontrol_log.unlink()
    fixture.sacctmgr_log.unlink()
    fixture.config.write_text(
        EXPECTED_CONFIG + "SchedulerParameters=bf_continue\n",
        encoding="utf-8",
    )

    checked = _run(fixture, "check")

    assert checked.returncode == 0, checked.stderr
    assert "--immediate" not in _logs(fixture)


@pytest.mark.parametrize("drift", ["qos", "partition", "backup"])
def test_unknown_existing_drift_fails_before_mutation(tmp_path: Path, drift: str) -> None:
    fixture = _fixture(tmp_path)
    if drift == "qos":
        fixture.qos.write_text(
            "loom-task-image-builder|DenyOnLimit|0|2|2|08:00:00||\n",
            encoding="utf-8",
        )
    elif drift == "partition":
        fixture.config.write_text(
            INITIAL_CONFIG + BUILDER_LINE.replace("PriorityTier=200", "PriorityTier=100") + "\n",
            encoding="utf-8",
        )
    else:
        fixture.state_root.mkdir(mode=0o755)
        (fixture.state_root / "slurm.conf.before-loom-task-builder").write_text(
            "stale\n", encoding="utf-8"
        )

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "drift" in result.stderr or "backup" in result.stderr
    assert "--immediate" not in _logs(fixture)
    assert "reconfigure" not in _logs(fixture)


def test_unsafe_authority_state_directory_fails_before_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.state_root.mkdir(mode=0o777)
    fixture.state_root.chmod(0o777)

    result = _run(fixture, "apply")

    assert result.returncode == 1
    assert "state directory" in result.stderr
    assert fixture.state_root.stat().st_mode & 0o777 == 0o777
    assert fixture.config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert fixture.qos.read_text(encoding="utf-8") == LEGACY_QOS
    assert "--immediate" not in _logs(fixture)
    assert "reconfigure" not in _logs(fixture)


def test_reconfigure_failure_restores_exact_prechange_config(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = _run(fixture, "apply", reconfigure_fail_at="1")

    assert result.returncode == 1
    assert "restored backup" in result.stderr
    assert fixture.config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert fixture.reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_live_partition_readback_drift_restores_backup(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    drifted_state = BUILDER_STATE.replace("PriorityTier=200", "PriorityTier=100")

    result = _run(fixture, "apply", builder_state=drifted_state)

    assert result.returncode == 1
    assert "readback" in result.stderr
    assert fixture.config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert fixture.reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_convergence_never_touches_reservations_features_or_jobs(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    result = _run(fixture, "apply")

    assert result.returncode == 0, result.stderr
    operations = _logs(fixture).lower()
    for forbidden in (
        "reservation",
        "exclusive",
        "scancel",
        "update nodename",
        "features=",
        "delete",
    ):
        assert forbidden not in operations
