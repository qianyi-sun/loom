"""Contracts for the dedicated non-preemptive OLDLAB staging partition."""

from __future__ import annotations

import grp
import os
import pwd
import shutil
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONVERGER = ROOT / "deploy/slurm/converge-loom-oldlab-slurm-partition.sh"
ANCHOR_LINE = "PartitionName=all Nodes=ALL Default=YES MaxTime=INFINITE State=UP OverSubscribe=NO"
PARTITION_LINE = (
    "PartitionName=loom-staging Nodes=trt-eai-oldlab-[3-5] Default=NO "
    "MaxTime=2-00:00:00 State=UP PriorityTier=100 "
    "AllowGroups=loom-rollout OverSubscribe=NO"
)
INITIAL_CONFIG = f"ClusterName=trt-oldlab\n{ANCHOR_LINE}\n"
LIVE_PARTITION = (
    "PartitionName=loom-staging AllowGroups=loom-rollout "
    "Default=NO MaxTime=2-00:00:00 PriorityTier=100 "
    "OverSubscribe=NO State=UP Nodes=trt-eai-oldlab-[3-5]"
)
LIVE_NODES = "\n".join(
    (
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5",
    )
)


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
        case "$*" in
          "show config")
            printf 'ClusterName             = trt-oldlab\n'
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
            read_count=0
            if [ -f "$FAKE_PARTITION_READ_COUNT" ]; then
              read -r read_count <"$FAKE_PARTITION_READ_COUNT"
            fi
            read_count=$((read_count + 1))
            printf '%s\n' "$read_count" >"$FAKE_PARTITION_READ_COUNT"
            if [ -n "${FAKE_PARTITION_UNAVAILABLE_AFTER_READ:-}" ] \
              && [ "$read_count" -ge "$FAKE_PARTITION_UNAVAILABLE_AFTER_READ" ]; then
              exit 1
            fi
            if [ "${FAKE_PARTITION_UNAVAILABLE_UNTIL_RECONFIGURE:-}" = "1" ] \
              && [ ! -f "$FAKE_RECONFIGURE_COUNT" ]; then
              exit 1
            fi
            if [ -n "${FAKE_PARTITION_STATE_BEFORE_RECONFIGURE:-}" ] \
              && [ ! -f "$FAKE_RECONFIGURE_COUNT" ]; then
              printf '%s\n' "$FAKE_PARTITION_STATE_BEFORE_RECONFIGURE"
              exit 0
            fi
            printf '%s\n' "$FAKE_PARTITION_STATE"
            ;;
          "show hostnames "*)
            printf '%s\n' "$FAKE_HOSTNAMES"
            ;;
          show\ node\ *\ -o)
            if [ "${FAKE_NODE_WITHOUT_PARTITION:-}" = "$3" ]; then
              printf 'NodeName=%s Partitions=all State=IDLE\n' "$3"
            else
              printf 'NodeName=%s Partitions=all,loom-staging State=IDLE\n' "$3"
            fi
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
    partition_state: str = LIVE_PARTITION,
    hostnames: str = LIVE_NODES,
    reconfigure_fail_at: str = "",
    runs: int = 1,
    config_text: str = INITIAL_CONFIG,
    backup_text: str | None = None,
    backup_mode: int = 0o600,
    partition_unavailable_until_reconfigure: bool = False,
    partition_state_before_reconfigure: str = "",
    partition_unavailable_after_read: int | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path]:
    fake_bin = tmp_path / "bin"
    _fake_scontrol(fake_bin)
    config = tmp_path / "etc" / "slurm.conf"
    config.parent.mkdir()
    config.write_text(config_text, encoding="utf-8")
    config.chmod(0o664)
    authority = tmp_path / "authority"
    if backup_text is not None:
        authority.mkdir()
        backup = authority / "slurm.conf.before-loom-staging-partition"
        backup.write_text(backup_text, encoding="utf-8")
        backup.chmod(backup_mode)
    reconfigure_count = tmp_path / "reconfigure-count"
    partition_read_count = tmp_path / "partition-read-count"
    owner = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_RECONFIGURE_COUNT": str(reconfigure_count),
        "FAKE_RECONFIGURE_FAIL_AT": reconfigure_fail_at,
        "FAKE_PARTITION_STATE": partition_state,
        "FAKE_PARTITION_UNAVAILABLE_UNTIL_RECONFIGURE": (
            "1" if partition_unavailable_until_reconfigure else ""
        ),
        "FAKE_PARTITION_STATE_BEFORE_RECONFIGURE": partition_state_before_reconfigure,
        "FAKE_PARTITION_READ_COUNT": str(partition_read_count),
        "FAKE_PARTITION_UNAVAILABLE_AFTER_READ": (
            str(partition_unavailable_after_read)
            if partition_unavailable_after_read is not None
            else ""
        ),
        "FAKE_HOSTNAMES": hostnames,
    }
    result = subprocess.run(
        [
            shutil.which("bash") or "bash",
            "-c",
            r"""
            source "$1"
            sleep() { :; }
            CONFIG="$2"
            STATE_ROOT="$3"
            BACKUP="$STATE_ROOT/slurm.conf.before-loom-staging-partition"
            CONFIG_OWNER="$4"
            CONFIG_GROUP="$5"
            STATE_OWNER="$4"
            STATE_GROUP="$5"
            for ((run = 0; run < $6; run++)); do
              loom_oldlab_converge_partition
            done
            """,
            "oldlab-converger-test",
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
    return result, config, authority, reconfigure_count


def test_first_convergence_inserts_partition_and_preserves_exact_backup(
    tmp_path: Path,
) -> None:
    result, config, authority, reconfigure_count = _run_converger(tmp_path)

    assert result.returncode == 0, result.stderr
    assert config.read_text(encoding="utf-8") == f"{INITIAL_CONFIG}{PARTITION_LINE}\n"
    backup = authority / "slurm.conf.before-loom-staging-partition"
    assert backup.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_second_convergence_is_idempotent(tmp_path: Path) -> None:
    result, config, authority, reconfigure_count = _run_converger(tmp_path, runs=2)

    assert result.returncode == 0, result.stderr
    assert config.read_text(encoding="utf-8") == f"{INITIAL_CONFIG}{PARTITION_LINE}\n"
    backup = authority / "slurm.conf.before-loom-staging-partition"
    assert backup.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_canonical_durable_partition_reloads_missing_live_state(tmp_path: Path) -> None:
    canonical_config = f"{INITIAL_CONFIG}{PARTITION_LINE}\n"
    result, config, authority, reconfigure_count = _run_converger(
        tmp_path,
        config_text=canonical_config,
        partition_unavailable_until_reconfigure=True,
    )

    assert result.returncode == 0, result.stderr
    assert config.read_text(encoding="utf-8") == canonical_config
    assert not authority.exists()
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_canonical_durable_partition_reloads_drifted_live_state(tmp_path: Path) -> None:
    canonical_config = f"{INITIAL_CONFIG}{PARTITION_LINE}\n"
    stale_live_partition = LIVE_PARTITION.replace("PriorityTier=100", "PriorityTier=1")
    result, config, authority, reconfigure_count = _run_converger(
        tmp_path,
        config_text=canonical_config,
        partition_state_before_reconfigure=stale_live_partition,
    )

    assert result.returncode == 0, result.stderr
    assert config.read_text(encoding="utf-8") == canonical_config
    assert not authority.exists()
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_canonical_reload_rejects_transient_one_sample_convergence(
    tmp_path: Path,
) -> None:
    canonical_config = f"{INITIAL_CONFIG}{PARTITION_LINE}\n"
    result, config, authority, reconfigure_count = _run_converger(
        tmp_path,
        config_text=canonical_config,
        partition_unavailable_until_reconfigure=True,
        partition_unavailable_after_read=3,
    )

    assert result.returncode == 1
    assert "did not remain exact through stable readback" in result.stderr
    assert config.read_text(encoding="utf-8") == canonical_config
    assert not authority.exists()
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


def test_canonical_durable_partition_reload_failure_is_explicit(tmp_path: Path) -> None:
    canonical_config = f"{INITIAL_CONFIG}{PARTITION_LINE}\n"
    result, config, authority, reconfigure_count = _run_converger(
        tmp_path,
        config_text=canonical_config,
        partition_unavailable_until_reconfigure=True,
        reconfigure_fail_at="1",
    )

    assert result.returncode == 1
    assert "rejected the canonical durable OLDLAB staging partition reload" in result.stderr
    assert config.read_text(encoding="utf-8") == canonical_config
    assert not authority.exists()
    assert reconfigure_count.read_text(encoding="utf-8") == "1\n"


@pytest.mark.parametrize(
    ("backup_text", "backup_mode"),
    (
        (f"{INITIAL_CONFIG}# stale authority\n", 0o600),
        (INITIAL_CONFIG, 0o644),
    ),
    ids=("stale-content", "unsafe-mode"),
)
def test_existing_backup_must_be_exact_and_safe_before_mutation(
    tmp_path: Path,
    backup_text: str,
    backup_mode: int,
) -> None:
    result, config, _authority, reconfigure_count = _run_converger(
        tmp_path,
        backup_text=backup_text,
        backup_mode=backup_mode,
    )

    assert result.returncode == 1
    assert "backup is unsafe or stale" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert not reconfigure_count.exists()


@pytest.mark.parametrize(
    "config_text",
    (
        f"{INITIAL_CONFIG}{PARTITION_LINE}\n{PARTITION_LINE}\n",
        (
            f"{INITIAL_CONFIG}PartitionName=loom-staging "
            "Nodes=trt-eai-oldlab-[3-5] Default=NO MaxTime=2-00:00:00 "
            "State=UP PriorityTier=1 AllowGroups=loom-rollout OverSubscribe=NO\n"
        ),
    ),
    ids=("duplicate", "drifted"),
)
def test_duplicate_or_drifted_partition_fails_without_mutation(
    tmp_path: Path,
    config_text: str,
) -> None:
    result, config, _authority, reconfigure_count = _run_converger(
        tmp_path,
        config_text=config_text,
    )

    assert result.returncode == 1
    assert "partition line does not match authority" in result.stderr
    assert config.read_text(encoding="utf-8") == config_text
    assert not reconfigure_count.exists()


def test_rejected_reconfigure_restores_exact_backup(tmp_path: Path) -> None:
    result, config, authority, reconfigure_count = _run_converger(
        tmp_path,
        reconfigure_fail_at="1",
    )

    assert result.returncode == 1
    assert "Slurm rejected the OLDLAB staging partition; restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    backup = authority / "slurm.conf.before-loom-staging-partition"
    assert backup.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_rejected_backup_reconfigure_reports_durable_and_live_state(
    tmp_path: Path,
) -> None:
    result, config, _authority, reconfigure_count = _run_converger(
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


def test_missing_live_field_restores_freshly_applied_partition(tmp_path: Path) -> None:
    missing_default = LIVE_PARTITION.replace(" Default=NO", "")
    result, config, _authority, reconfigure_count = _run_converger(
        tmp_path,
        partition_state=missing_default,
    )

    assert result.returncode == 1
    assert "live OLDLAB staging partition readback is incomplete" in result.stderr
    assert "restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_extra_live_partition_node_restores_freshly_applied_partition(
    tmp_path: Path,
) -> None:
    extra_node_partition = LIVE_PARTITION.replace(
        "Nodes=trt-eai-oldlab-[3-5]",
        "Nodes=trt-eai-oldlab-[3-6]",
    )
    result, config, _authority, reconfigure_count = _run_converger(
        tmp_path,
        partition_state=extra_node_partition,
        hostnames=f"{LIVE_NODES}\ntrt-eai-oldlab-6",
    )

    assert result.returncode == 1
    assert "live OLDLAB staging partition node set is not exact" in result.stderr
    assert "restored backup" in result.stderr
    assert config.read_text(encoding="utf-8") == INITIAL_CONFIG
    assert reconfigure_count.read_text(encoding="utf-8") == "2\n"


def test_oldlab_partition_converger_parses_as_bash() -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    result = subprocess.run(
        [bash, "-n", str(CONVERGER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_oldlab_partition_converger_rejects_arguments_before_mutation() -> None:
    result = subprocess.run(
        [shutil.which("bash") or "bash", str(CONVERGER), "unexpected"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert result.stderr.startswith("usage: sudo ")


def test_oldlab_partition_converger_never_preempts_or_mutates_jobs() -> None:
    source = CONVERGER.read_text(encoding="utf-8").lower()

    assert "preempt" not in source
    assert "scancel" not in source
    assert "scontrol update job" not in source
    assert "scontrol hold" not in source
    assert "scontrol release" not in source
