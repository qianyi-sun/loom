"""Behavioral contracts for canonical GB10 Slurm partition membership."""

from __future__ import annotations

import grp
import os
import pwd
import stat
import subprocess
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-gb10-slurm-partition.sh"
SHARED_LINE = (
    "PartitionName=gb10 Nodes=trt-gb10-[1-9,11-16] Default=YES "
    "MaxTime=1-00:00:00 State=UP PriorityTier=100"
)
DEDICATED_LINE = (
    "PartitionName=loom-staging Nodes=trt-gb10-[1-15] Default=NO "
    "MaxTime=1-00:00:00 State=UP PriorityTier=100 "
    "AllowAccounts=loom-staging AllowQos=loom-staging OverSubscribe=NO"
)
OTHER_PARTITIONS = "\n".join(
    (
        "PartitionName=debug Nodes=trt-gb10-10,trt-gb10-11 Default=NO "
        "State=UP MaxTime=02:00:00 DefaultTime=02:00:00 OverSubscribe=NO "
        "DefMemPerNode=24000 MaxMemPerNode=24000",
        "PartitionName=rao Nodes=trt-gb10-12 Default=NO MaxTime=INFINITE "
        "State=UP PriorityTier=1000 AllowGroups=rao PreemptMode=REQUEUE",
        "PartitionName=cheryl Nodes=trt-gb10-13 Default=NO MaxTime=INFINITE "
        "State=UP PriorityTier=1000 AllowGroups=cheryl PreemptMode=REQUEUE",
        "PartitionName=xuan Nodes=trt-gb10-[14-15] Default=NO MaxTime=INFINITE "
        "State=UP PriorityTier=1000 AllowGroups=xuan PreemptMode=REQUEUE",
        "PartitionName=energy Nodes=trt-gb10-[12-13] Default=NO MaxTime=INFINITE "
        "State=UP PriorityTier=1000 AllowGroups=energy PreemptMode=REQUEUE",
    )
)
INITIAL_CONFIG = f"ClusterName=trt-gb10\n{SHARED_LINE}\n{OTHER_PARTITIONS}\n"
EXPECTED_CONFIG = (
    f"ClusterName=trt-gb10\n{SHARED_LINE}\n{DEDICATED_LINE}\n{OTHER_PARTITIONS}\n"
)
LIVE_PARTITION = (
    "PartitionName=loom-staging AllowAccounts=loom-staging AllowQos=loom-staging "
    "Default=NO MaxTime=1-00:00:00 Nodes=trt-gb10-[1-15] "
    "OverSubscribe=NO PriorityTier=100 State=UP"
)
LIVE_NODES = "\n".join(f"trt-gb10-{number}" for number in range(1, 16))


def _write_executable(path: Path, source: str) -> None:
    path.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
    path.chmod(0o755)


def _fake_scontrol(fake_bin: Path) -> None:
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "scontrol",
        r"""
        #!/usr/bin/env bash
        set -euo pipefail
        printf '%s\n' "$*" >>"$FAKE_SCONTROL_LOG"
        case "$*" in
          "show config")
            printf 'ClusterName             = trt-gb10\n'
            ;;
          "reconfigure")
            count=0
            if [ -f "$FAKE_RECONFIGURE_COUNT" ]; then
              read -r count <"$FAKE_RECONFIGURE_COUNT"
            fi
            count=$((count + 1))
            printf '%s\n' "$count" >"$FAKE_RECONFIGURE_COUNT"
            case ",${FAKE_RECONFIGURE_FAIL_AT:-}," in
              *",$count,"*) exit 1 ;;
            esac
            ;;
          "show partition loom-staging -o")
            printf '%s\n' "$FAKE_PARTITION_STATE"
            ;;
          "show hostnames "*)
            printf '%s\n' "$FAKE_HOSTNAMES"
            ;;
          "show node trt-gb10-10 -o")
            printf 'NodeName=trt-gb10-10 Partitions=%s State=MIXED\n' \
              "$FAKE_NODE10_PARTITIONS"
            ;;
          "show node trt-gb10-16 -o")
            printf 'NodeName=trt-gb10-16 Partitions=%s State=MIXED\n' \
              "$FAKE_NODE16_PARTITIONS"
            ;;
          *)
            printf 'unexpected fake scontrol invocation: %s\n' "$*" >&2
            exit 2
            ;;
        esac
        """,
    )


def _run_converger(
    tmp_path: Path,
    *,
    config_text: str = INITIAL_CONFIG,
    backup_text: str | None = None,
    backup_mode: int = 0o600,
    hostnames: str = LIVE_NODES,
    node10_partitions: str = "debug,loom-staging",
    node16_partitions: str = "gb10",
    partition_state: str = LIVE_PARTITION,
    reconfigure_fail_at: str = "",
    runs: int = 1,
) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path, Path]:
    fake_bin = tmp_path / "bin"
    _fake_scontrol(fake_bin)
    config = tmp_path / "etc" / "slurm.conf"
    config.parent.mkdir()
    config.write_text(config_text, encoding="utf-8")
    config.chmod(0o644)
    authority = tmp_path / "authority"
    if backup_text is not None:
        authority.mkdir()
        backup = authority / "slurm.conf.before-loom-staging-partition"
        backup.write_text(backup_text, encoding="utf-8")
        backup.chmod(backup_mode)
    reconfigure_count = tmp_path / "reconfigure-count"
    scontrol_log = tmp_path / "scontrol.log"
    owner = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_RECONFIGURE_COUNT": str(reconfigure_count),
        "FAKE_RECONFIGURE_FAIL_AT": reconfigure_fail_at,
        "FAKE_SCONTROL_LOG": str(scontrol_log),
        "FAKE_PARTITION_STATE": partition_state,
        "FAKE_HOSTNAMES": hostnames,
        "FAKE_NODE10_PARTITIONS": node10_partitions,
        "FAKE_NODE16_PARTITIONS": node16_partitions,
    }
    result = subprocess.run(
        [
            "bash",
            "-c",
            r"""
            source "$1"
            CONFIG="$2"
            STATE_ROOT="$3"
            BACKUP="$STATE_ROOT/slurm.conf.before-loom-staging-partition"
            CONFIG_OWNER="$4"
            CONFIG_GROUP="$5"
            STATE_OWNER="$4"
            STATE_GROUP="$5"
            for ((run = 0; run < $6; run++)); do
              loom_gb10_converge_partition
            done
            """,
            "gb10-converger-test",
            str(CONVERGER),
            str(config),
            str(authority),
            owner,
            group,
            str(runs),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    return result, config, authority, reconfigure_count, scontrol_log


def test_convergence_adds_dedicated_partition_without_rewriting_shared_partition(
    tmp_path: Path,
) -> None:
    result, config, authority, reconfigure_count, _scontrol_log = _run_converger(tmp_path)

    assert result.returncode == 0, result.stderr
    assert config.read_text(encoding="utf-8") == EXPECTED_CONFIG
    backup = authority / "slurm.conf.before-loom-staging-partition"
    assert backup.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_second_convergence_is_idempotent(tmp_path: Path) -> None:
    result, config, authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        runs=2,
    )

    assert result.returncode == 0, result.stderr
    assert config.read_text(encoding="utf-8") == EXPECTED_CONFIG
    backup = authority / "slurm.conf.before-loom-staging-partition"
    assert backup.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_stale_backup_fails_before_partition_mutation(tmp_path: Path) -> None:
    stale_backup = f"{INITIAL_CONFIG}# stale\n"
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        backup_text=stale_backup,
    )

    assert result.returncode == 1
    assert "backup is unsafe or stale" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert not reconfigure_count.exists()


def test_unsafe_backup_mode_fails_before_partition_mutation(tmp_path: Path) -> None:
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        backup_text=INITIAL_CONFIG,
        backup_mode=0o644,
    )

    assert result.returncode == 1
    assert "backup is unsafe or stale" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert not reconfigure_count.exists()


def test_drifted_shared_partition_anchor_fails_without_mutation(tmp_path: Path) -> None:
    drifted = INITIAL_CONFIG.replace("PriorityTier=100", "PriorityTier=99", 1)
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        config_text=drifted,
    )

    assert result.returncode == 1
    assert "shared GB10 partition anchor is not exact" in result.stderr
    assert config.read_text(encoding="utf-8") == drifted
    assert not reconfigure_count.exists()


def test_duplicate_shared_partition_authority_fails_without_mutation(tmp_path: Path) -> None:
    duplicate = INITIAL_CONFIG.replace(
        SHARED_LINE,
        f"{SHARED_LINE}\nPartitionName=gb10 Nodes=trt-gb10-1 Default=NO State=DOWN",
        1,
    )
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        config_text=duplicate,
    )

    assert result.returncode == 1
    assert "shared GB10 partition anchor is not exact" in result.stderr
    assert config.read_text(encoding="utf-8") == duplicate
    assert not reconfigure_count.exists()


def test_rejected_reconfigure_restores_exact_backup(tmp_path: Path) -> None:
    result, config, authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        reconfigure_fail_at="1",
    )

    assert result.returncode == 1
    assert "Slurm rejected the dedicated GB10 staging partition" in result.stderr
    assert "restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    backup = authority / "slurm.conf.before-loom-staging-partition"
    assert backup.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_rejected_backup_reconfigure_reports_durable_and_live_state(
    tmp_path: Path,
) -> None:
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        reconfigure_fail_at="1,2",
    )

    assert result.returncode == 1
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert (
        "restored backup on disk, but Slurm rejected the restored backup reconfigure"
        in result.stderr
    )
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_extra_live_partition_node_restores_fresh_mutation(tmp_path: Path) -> None:
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        hostnames=f"{LIVE_NODES}\ntrt-gb10-16",
    )

    assert result.returncode == 1
    assert "live GB10 partition node set is not exact" in result.stderr
    assert "restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_missing_node10_membership_restores_fresh_mutation(tmp_path: Path) -> None:
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        node10_partitions="debug",
    )

    assert result.returncode == 1
    assert "did not converge for trt-gb10-10" in result.stderr
    assert "restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_node16_in_dedicated_partition_restores_fresh_mutation(tmp_path: Path) -> None:
    result, config, _authority, reconfigure_count, _scontrol_log = _run_converger(
        tmp_path,
        node16_partitions="gb10,loom-staging",
    )

    assert result.returncode == 1
    assert "dedicated GB10 staging partition still includes trt-gb10-16" in result.stderr
    assert "restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_partition_converger_parses_as_bash() -> None:
    result = subprocess.run(
        ["bash", "-n", str(CONVERGER)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_partition_converger_rejects_arguments_before_mutation() -> None:
    result = subprocess.run(
        ["bash", str(CONVERGER), "unexpected"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert result.stderr.startswith("usage: sudo ")


def test_partition_converger_never_preempts_or_mutates_jobs() -> None:
    source = CONVERGER.read_text(encoding="utf-8").lower()

    assert "scancel" not in source
    assert "scontrol update job" not in source
    assert "scontrol hold" not in source
    assert "scontrol release" not in source
