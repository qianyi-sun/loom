from __future__ import annotations

import json
import stat
from dataclasses import replace
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_slurm_policy as policy
from scripts.ops import slurm_job_cgroup_guard as guard


def _config() -> guard.GuardConfig:
    return guard.GuardConfig(
        cluster="trt-oldlab",
        controller="oldlab-1",
        submit_host="oldlab-2",
        allowed_nodes=frozenset({"oldlab-1"}),
        candidate_sha="a" * 40,
        config_sha256="b" * 64,
        pids_max=32768,
        allowed_accounts=frozenset(
            {
                "loom-dev-qianyi",
                "loom-dev-hongjian",
                "loom-dev-devansh",
            },
        ),
        poll_interval_seconds=0.2,
        require_gpu_probe=False,
    )


def _job(tmp_path: Path, job_id: str = "123") -> tuple[Path, Path]:
    root = tmp_path / "cgroup"
    job = root / "system.slice/node_slurmstepd.scope" / f"job_{job_id}"
    (job / "step_extern").mkdir(parents=True)
    (job / "cgroup.controllers").write_text("cpu memory pids")
    (job / "cgroup.subtree_control").write_text("cpu memory")
    (job / "cgroup.procs").write_text("")
    (job / "cpu.max").write_text("200000 100000\n")
    (job / "memory.max").write_text("8388608000\n")
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
            job_name="loom-qianyi-aaaaaaaaaaaa-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
        ),
    )

    assert result["scanned"] == 1
    assert result["verified"] == 1
    assert result["unrelated"] == 0
    assert result["failed"] == 0
    assert result["failures"] == []
    assert result["resource_probe"]["pids_max"] == "32768"
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


def test_candidate_or_allocation_route_mismatch_fails_closed(tmp_path: Path) -> None:
    root, job = _job(tmp_path)

    result = guard.scan_once(
        _config(),
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-dev-qianyi",
            comment="loom-cgroup-v1:pids=32768",
            job_name="loom-qianyi-bbbbbbbbbbbb-oldlab-9",
            batch_host="oldlab-9",
            node_list="oldlab-9",
        ),
    )

    assert result["failed"] == 1
    assert result["verified"] == 0
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
    config_path = root / "etc/loom/slurm-job-cgroup-guard.json"
    payload = json.loads(config_path.read_text())
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert payload["pids_max"] == 32768
    assert payload["cluster"] == "trt-oldlab"
    assert payload["controller"] == "TRT-EAI-OLDLAB-1"
    assert payload["submit_host"] == "trt-EAI-OLDLAB-2"
    assert "trt-eai-oldlab-5" in payload["allowed_nodes"]
    unit = (root / "etc/systemd/system/loom-slurm-job-cgroup-guard.service").read_text()
    assert "ReadWritePaths=/sys/fs/cgroup" in unit
    assert "PrivateNetwork=true" not in unit
    assert "Prolog=" not in (root / "etc/slurm/slurm.conf").read_text()


def test_gpu_profile_requires_positive_allocated_tres_probe(tmp_path: Path) -> None:
    root, _job_path = _job(tmp_path)
    config = replace(_config(), require_gpu_probe=True)

    failed = guard.scan_once(
        config,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-dev-qianyi",
            comment="loom-cgroup-v1:pids=32768",
            alloc_tres="cpu=2,mem=11500M",
            job_name="loom-qianyi-aaaaaaaaaaaa-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
        ),
    )
    assert failed["failed"] == 1
    assert failed["verified"] == 0

    passed = guard.scan_once(
        config,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-dev-qianyi",
            comment="loom-cgroup-v1:pids=32768",
            alloc_tres="cpu=2,mem=11500M,gres/gpu=1",
            job_name="loom-qianyi-aaaaaaaaaaaa-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
        ),
    )
    assert passed["failed"] == 0
    assert passed["verified"] == 1
    assert passed["resource_probe"]["gpu_verified"] is True


def test_status_is_atomic_private_and_candidate_bound(tmp_path: Path) -> None:
    status = tmp_path / "state" / "guard.json"
    result = {
        "scanned": 1,
        "verified": 1,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probe": {"job_id": "123"},
    }

    guard.write_status(status, config=_config(), result=result)

    payload = json.loads(status.read_text())
    assert payload["candidate_sha"] == "a" * 40
    assert payload["config_sha256"] == "b" * 64
    assert stat.S_IMODE(status.stat().st_mode) == 0o600
    assert stat.S_IMODE(status.parent.stat().st_mode) == 0o700


def test_daemon_iteration_records_guard_error_without_exiting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status = tmp_path / "guard.json"

    def failed_cluster() -> str:
        raise guard.GuardError("controller unavailable")

    monkeypatch.setattr(guard, "_cluster_name", failed_cluster)
    result = guard.daemon_iteration(
        _config(),
        status_path=status,
        job_lookup=lambda _job_id: pytest.fail("job lookup must not run"),
        cgroup_root=tmp_path,
    )

    assert result["failed"] == 1
    assert "controller unavailable" in result["failures"][0]["reason"]
    assert json.loads(status.read_text())["failed"] == 1
