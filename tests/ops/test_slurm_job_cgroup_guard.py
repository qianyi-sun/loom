from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_slurm_policy as policy
from scripts.ops import slurm_job_cgroup_guard as guard


def _config() -> guard.GuardConfig:
    return guard.GuardConfig(
        cluster="trt-oldlab",
        pids_max=32768,
        allowed_accounts=frozenset(
            {
                "loom-dev-qianyi",
                "loom-dev-hongjian",
                "loom-dev-devansh",
            },
        ),
        poll_interval_seconds=0.2,
    )


def _job(tmp_path: Path, job_id: str = "123") -> tuple[Path, Path]:
    root = tmp_path / "cgroup"
    job = root / "system.slice/node_slurmstepd.scope" / f"job_{job_id}"
    (job / "step_extern").mkdir(parents=True)
    (job / "cgroup.controllers").write_text("cpu memory pids")
    (job / "cgroup.subtree_control").write_text("cpu memory")
    (job / "cgroup.procs").write_text("")
    (job / "pids.max").write_text("max\n")
    return root, job


def test_discovers_only_exact_job_cgroup_below_slurm_scope(tmp_path: Path) -> None:
    root, job = _job(tmp_path)
    (root / "user.slice/job_456").mkdir(parents=True)
    (root / "system.slice/node_slurmstepd.scope/job_bad").mkdir(parents=True)
    target = root / "system.slice/node_slurmstepd.scope/job_999"
    target.symlink_to(job, target_is_directory=True)

    assert guard.discover_job_cgroups(root) == (("123", job),)


def test_scan_applies_exact_fixed_limit_and_delegation(tmp_path: Path) -> None:
    root, job = _job(tmp_path)

    result = guard.scan_once(
        _config(),
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-dev-qianyi",
            comment="loom-cgroup-v1:pids=32768",
        ),
    )

    assert result == {"discovered": 1, "converged": 1, "unrelated": 0, "failed": 0}
    assert (job / "pids.max").read_text().strip() == "32768"
    assert (job / "cgroup.subtree_control").read_text() == "+pids"


def test_unrelated_job_is_unchanged(tmp_path: Path) -> None:
    root, job = _job(tmp_path)

    result = guard.scan_once(
        _config(),
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="normal",
            comment="ordinary-job",
        ),
    )

    assert result["unrelated"] == 1
    assert (job / "pids.max").read_text() == "max\n"


def test_job_identity_cache_is_bounded_and_reuses_record() -> None:
    calls: list[str] = []

    def lookup(job_id: str) -> guard.JobRecord:
        calls.append(job_id)
        return guard.JobRecord(job_id=job_id, account="normal", comment="ordinary-job")

    cached = guard.BoundedJobLookup(lookup)

    assert cached("123") == cached("123")
    assert calls == ["123"]
    cached.retain(set())
    cached("123")
    assert calls == ["123", "123"]


@pytest.mark.parametrize(
    ("account", "comment"),
    [
        ("normal", "loom-cgroup-v1:pids=32768"),
        ("loom-dev-qianyi", "loom-cgroup-v1:pids=32767"),
        ("loom-dev-qianyi", "loom-cgroup-v1:pids=max"),
        ("loom-dev-qianyi", "loom-cgroup-v2:pids=32768"),
    ],
)
def test_malformed_or_unreviewed_loom_job_fails_closed(
    tmp_path: Path,
    account: str,
    comment: str,
) -> None:
    root, job = _job(tmp_path)

    result = guard.scan_once(
        _config(),
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account=account,
            comment=comment,
        ),
    )

    assert result["failed"] == 1
    assert (job / "pids.max").read_text() == "max\n"


def test_host_converger_installs_exact_guard_contract(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    profile = policy.load_profile(
        repo_root / "deploy/slurm/developer-sandboxes/oldlab.toml",
    )
    root = tmp_path / "root"
    (root / "etc/slurm").mkdir(parents=True)
    (root / "etc/docker").mkdir(parents=True)
    (root / "etc/slurm/slurm.conf").write_text("ClusterName=trt-oldlab\n")
    (root / "etc/docker/daemon.json").write_text("{}\n")

    policy.apply(root, profile, restart=False, apply_accounting=False)

    installed = root / "usr/libexec/loom-slurm-job-cgroup-guard"
    assert installed.is_file()
    assert stat.S_IMODE(installed.stat().st_mode) == 0o755
    payload = json.loads((root / "etc/loom/slurm-job-cgroup-guard.json").read_text())
    assert payload["pids_max"] == 32768
    assert payload["cluster"] == "trt-oldlab"
    unit = (root / "etc/systemd/system/loom-slurm-job-cgroup-guard.service").read_text()
    assert "ReadWritePaths=/sys/fs/cgroup" in unit
    assert "PrivateNetwork=true" not in unit
    assert "Prolog=" not in (root / "etc/slurm/slurm.conf").read_text()
