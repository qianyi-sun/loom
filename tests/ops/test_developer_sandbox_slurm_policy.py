from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_slurm_policy as policy

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy/slurm/developer-sandboxes/oldlab.toml"
GB10_PROFILE = REPO_ROOT / "deploy/slurm/developer-sandboxes/gb10.toml"


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "root"
    (root / "etc/slurm").mkdir(parents=True)
    (root / "etc/docker").mkdir(parents=True)
    (root / "etc/slurm/slurm.conf").write_text(
        "\n".join(
            (
                "ClusterName=trt-oldlab",
                "ProctrackType=proctrack/linuxproc",
                "TaskPlugin=task/none",
                "PriorityType=priority/basic",
                "PriorityWeightFairshare=0",
                "",
            ),
        ),
        encoding="utf-8",
    )
    (root / "etc/slurm/cgroup.conf").write_text(
        "CgroupPlugin=autodetect\nConstrainCores=yes\nConstrainRAMSpace=no\n",
        encoding="utf-8",
    )
    (root / "etc/docker/daemon.json").write_text(
        json.dumps(
            {
                "features": {"containerd-snapshotter": False},
                "exec-opts": ["native.cgroupdriver=systemd"],
            },
        ),
        encoding="utf-8",
    )
    return root


def test_profile_is_exact_three_sandbox_fairshare_contract() -> None:
    loaded = policy.load_profile(PROFILE)

    assert loaded.cluster == "trt-oldlab"
    assert loaded.child_accounts == (
        "loom-dev-qianyi",
        "loom-dev-hongjian",
        "loom-dev-devansh",
    )
    assert loaded.users == ("qianyi", "hongjian", "devansh")
    assert loaded.docker_cgroup_driver == "cgroupfs"
    assert loaded.slurm["accounting_storage_enforce"] == ("associations,limits,qos,safe")


def test_gb10_profile_maps_connection_aliases_to_canonical_hosts() -> None:
    loaded = policy.load_profile(GB10_PROFILE)

    assert policy._slurm_node_for_host(loaded, "gx10-01c7") == "trt-gb10-1"
    assert policy._slurm_node_for_host(loaded, "trt-gb10-1") is None
    assert "trt-gb10-7" not in loaded.host_aliases


def test_render_preserves_unrelated_settings_and_removes_duplicate_keys(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    slurm = root / "etc/slurm/slurm.conf"
    slurm.write_text(
        slurm.read_text() + "TaskPlugin=task/affinity\n" + "SlurmctldHost=TRT-EAI-OLDLAB-1\n",
        encoding="utf-8",
    )

    rendered = policy.desired_files(root, loaded)[slurm]

    assert rendered.count("TaskPlugin=") == 1
    assert "TaskPlugin=task/cgroup,task/affinity" in rendered
    assert "SlurmctldHost=TRT-EAI-OLDLAB-1" in rendered
    assert "PriorityWeightFairshare=10000" in rendered


def test_daemon_merge_preserves_existing_keys_and_replaces_driver(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)

    rendered = policy.desired_files(root, loaded)[root / "etc/docker/daemon.json"]
    payload = json.loads(rendered)

    assert payload["features"] == {"containerd-snapshotter": False}
    assert payload["exec-opts"] == ["native.cgroupdriver=cgroupfs"]


def test_apply_to_offline_root_is_idempotent_and_snapshots(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)

    first = policy.apply(root, loaded, restart=False, apply_accounting=False)
    second = policy.apply(root, loaded, restart=False, apply_accounting=False)

    assert first["mutation_authorized"] is True
    assert second["mutation_authorized"] is True
    assert all(row["converged"] for row in second["files"])
    assert Path(first["snapshot"]).is_dir()
    assert Path(second["snapshot"]).is_dir()
    assert (root / "etc/slurm/cgroup.conf").read_text() == policy.render_cgroup_conf(loaded)


def test_invalid_or_weakened_profile_fails_closed(tmp_path: Path) -> None:
    raw = PROFILE.read_text(encoding="utf-8").replace(
        "constrain_devices = true",
        "constrain_devices = false",
    )
    invalid = tmp_path / "invalid.toml"
    invalid.write_text(raw, encoding="utf-8")

    with pytest.raises(policy.PolicyError, match="constrain_devices"):
        policy.load_profile(invalid)


def test_accounting_plan_has_parent_budget_and_one_child_per_user() -> None:
    loaded = policy.load_profile(PROFILE)

    commands = policy.accounting_commands(loaded)
    flattened = [" ".join(command) for command in commands]

    assert any(
        "account=loom-dev set Fairshare=1 GrpTRES=cpu=40,mem=160G" in row for row in flattened
    )
    for user in loaded.users:
        assert any(f"add user {user}" in row for row in flattened)
    for account in loaded.child_accounts:
        assert any(f"add account {account} Parent=loom-dev" in row for row in flattened)
