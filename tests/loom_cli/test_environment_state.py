from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom_cli.environment_state import (
    EnvironmentStateProfileError,
    apply_external_slurm_autoscaler_supervisors,
    diff_environment_state,
    diff_external_slurm_autoscaler_supervisors,
    diff_external_slurm_runner_prerequisites,
    load_environment_state_profile,
)


def _write_profile(path: Path) -> None:
    path.write_text(
        """
environment = "public-beta"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10-arm64"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = 150
scale_up_threshold_slots = 1
scale_down_idle_seconds = 600
scale_up_cooldown_seconds = 60
scale_down_cooldown_seconds = 300
drain_timeout_seconds = 600
force = false

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "arm64"
partition = "gb10"
allowed_nodes = ["trt-gb10-1", "trt-gb10-2"]
requested_concurrency = 10
max_jobs = 15
pending_job_cap = 2

[[gb10_worker_pool_desired_states]]
pool_name = "gb10-arm64"
image_tag = "${IMAGE_TAG}"
max_concurrent = 10
env_config_version = "${ENV_CONFIG_VERSION}"
target_slots = 150

[gb10_worker_pool_desired_states.host_intents]
trt-gb10-1 = "active"
trt-gb10-2 = "active"

[gb10_worker_pool_desired_states.rollout_policy]
mode = "all"

[catalog_provisioning]
required = true
command = "loom datasets provision-public-beta-catalog"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_load_environment_state_profile_normalizes_payloads_and_variables(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)

    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "public-beta-57a7509",
            "ENV_CONFIG_VERSION": "public-beta-57a7509",
        },
        expected_environment="public-beta",
    )

    assert profile.environment == "public-beta"
    assert profile.control_plane_environment == "public-beta"
    assert profile.autoscaler_policies == [
        {
            "environment": "public-beta",
            "pool_name": "gb10-arm64",
            "actuator": "slurm",
            "enabled": True,
            "min_slots": 0,
            "max_slots": 150,
            "scale_up_threshold_slots": 1,
            "scale_down_idle_seconds": 600,
            "scale_up_cooldown_seconds": 60,
            "scale_down_cooldown_seconds": 300,
            "drain_timeout_seconds": 600,
            "force": False,
            "actuator_config": {
                "backend": "docker",
                "cpu_arch": "arm64",
                "partition": "gb10",
                "allowed_nodes": ["trt-gb10-1", "trt-gb10-2"],
                "requested_concurrency": 10,
                "max_jobs": 15,
                "pending_job_cap": 2,
            },
        },
    ]
    assert profile.gb10_desired_states[0]["image_tag"] == "public-beta-57a7509"
    assert profile.gb10_desired_states[0]["env_config_version"] == "public-beta-57a7509"
    assert profile.catalog_provisioning["required"] is True


def test_load_environment_state_profile_requires_placeholder_values(tmp_path: Path) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)

    with pytest.raises(EnvironmentStateProfileError, match="IMAGE_TAG"):
        load_environment_state_profile(profile_path, variables={})


def test_diff_environment_state_reports_policy_and_desired_state_drift(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "public-beta-57a7509",
            "ENV_CONFIG_VERSION": "public-beta-57a7509",
        },
    )

    live: dict[str, Any] = {
        "autoscaler_status": {
            "policies": [
                {
                    "environment": "public-beta",
                    "pool_name": "gb10-arm64",
                    "actuator": "gb10",
                    "enabled": True,
                    "min_slots": 0,
                    "max_slots": 150,
                    "scale_up_threshold_slots": 1,
                    "scale_down_idle_seconds": 600,
                    "scale_up_cooldown_seconds": 60,
                    "scale_down_cooldown_seconds": 300,
                    "drain_timeout_seconds": 600,
                    "force": False,
                    "actuator_config": {"backend": "docker", "cpu_arch": "arm64"},
                },
            ],
        },
        "gb10_status": {
            "desired_states": [
                {
                    "environment": "public-beta",
                    "pool_name": "gb10-arm64",
                    "image_tag": "public-beta-old",
                    "max_concurrent": 10,
                    "env_config_version": "public-beta-old",
                    "target_slots": 150,
                    "host_intents": {
                        "trt-gb10-1": "active",
                        "trt-gb10-2": "active",
                    },
                    "rollout_policy": {"mode": "all"},
                    "env": {},
                },
            ],
        },
    }

    drift = diff_environment_state(profile, live)

    assert [item.path for item in drift] == [
        "worker_pool_autoscaler_policies[public-beta/gb10-arm64].actuator",
        "worker_pool_autoscaler_policies[public-beta/gb10-arm64].actuator_config",
        "gb10_worker_pool_desired_states[public-beta/gb10-arm64].image_tag",
        "gb10_worker_pool_desired_states[public-beta/gb10-arm64].env_config_version",
    ]
    assert drift[0].desired == "slurm"
    assert drift[0].live == "gb10"


def test_diff_environment_state_reports_missing_live_policy(tmp_path: Path) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "public-beta-57a7509",
            "ENV_CONFIG_VERSION": "public-beta-57a7509",
        },
    )

    drift = diff_environment_state(
        profile,
        {"autoscaler_status": {"policies": []}, "gb10_status": {"desired_states": []}},
    )

    assert drift[0].path == "worker_pool_autoscaler_policies[public-beta/gb10-arm64]"
    assert drift[0].live is None
    assert drift[1].path == "gb10_worker_pool_desired_states[public-beta/gb10-arm64]"
    assert drift[1].live is None


def test_diff_environment_state_reports_active_slurm_job_runtime_drift(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    profile_path.write_text(
        """
environment = "public-beta"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "x86_64"
env_file = "/shared_work/qianyi/loom-worker-capacity/public-beta-oldlab-worker.env"
repo_dir = "/shared_work/qianyi/loom-remote-worker"
requested_cpus = 2
requested_memory_mib = 8192
requested_concurrency = 1
external_runner = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_environment_state(
        profile,
        {
            "autoscaler_status": {
                "policies": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "actuator": "slurm",
                        "enabled": True,
                        "min_slots": 1,
                        "max_slots": 40,
                        "scale_up_threshold_slots": 1,
                        "scale_down_idle_seconds": 600,
                        "scale_up_cooldown_seconds": 60,
                        "scale_down_cooldown_seconds": 300,
                        "drain_timeout_seconds": 600,
                        "force": False,
                        "actuator_config": {
                            "backend": "docker",
                            "cpu_arch": "x86_64",
                            "env_file": "/shared_work/qianyi/loom-worker-capacity/public-beta-oldlab-worker.env",
                            "repo_dir": "/shared_work/qianyi/loom-remote-worker",
                            "requested_cpus": 2,
                            "requested_memory_mib": 8192,
                            "requested_concurrency": 1,
                            "external_runner": True,
                        },
                    },
                ],
            },
            "gb10_status": {"desired_states": []},
            "slurm_status": {
                "jobs": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "job_id": "14893",
                        "state": "running",
                        "redacted_env": {
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/shared_work/qianyi/loom-worker-capacity/issue45-oldlab-4-warm-1608b05.env",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/shared_work/qianyi/loom-remote-worker-1608b05",
                            "LOOM_WORKER_MAX_CONCURRENT": "1",
                        },
                    },
                ],
            },
        },
    )

    assert [item.path for item in drift] == [
        "slurm_worker_jobs[production/oldlab/14893].LOOM_REMOTE_WORKER_ENV_FILE",
        "slurm_worker_jobs[production/oldlab/14893].LOOM_REMOTE_WORKER_REPO_DIR",
    ]
    assert drift[0].desired.endswith("public-beta-oldlab-worker.env")
    assert drift[0].live.endswith("issue45-oldlab-4-warm-1608b05.env")


def test_external_slurm_runner_prerequisite_check_reports_missing_env_and_dirty_repo(
    tmp_path: Path,
) -> None:
    repo_dir = tmp_path / "loom-remote-worker"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    profile_path = tmp_path / "public-beta.state.toml"
    profile_path.write_text(
        f"""
environment = "public-beta"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "x86_64"
env_file = "{tmp_path / 'missing.env'}"
repo_dir = "{repo_dir}"
requested_concurrency = 1
external_runner = true

[external_slurm_runner_prerequisites]
expected_repo_ref = "public-beta-57a7509"
require_clean_repo = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    def _runner(command: list[str], *, cwd: Path | None = None) -> tuple[int, str, str]:
        assert command[:2] == ["git", "-C"]
        if command[-2:] == ["rev-parse", "HEAD"]:
            return 0, "62eb0a6d0000000000000000000000000000000\n", ""
        if command[-3:] == ["status", "--short", "--untracked-files=no"]:
            return 0, " M src/loom/trial/step_runner.py\n", ""
        raise AssertionError(command)

    drift = diff_external_slurm_runner_prerequisites(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_runner_prerequisites[production/oldlab].env_file",
        "external_slurm_runner_prerequisites[production/oldlab].repo_dir.git_head",
        "external_slurm_runner_prerequisites[production/oldlab].repo_dir.git_status",
    ]
    assert drift[0].desired == str(tmp_path / "missing.env")
    assert drift[0].live == "missing"
    assert drift[1].desired == "public-beta-57a7509"
    assert drift[1].live.startswith("62eb0a6")
    assert drift[2].desired == "clean"
    assert "step_runner.py" in drift[2].live


def test_external_slurm_autoscaler_supervisor_profile_is_normalized(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    profile_path.write_text(
        """
environment = "public-beta"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}"
python_path = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}/.venv/bin/python"
script_path = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--pool-name", "oldlab", "--namespace", "loom-public-beta"]
requires = ["network-online.target", "loom-public-beta-postgres-port-forward.service"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    profile = load_environment_state_profile(
        profile_path,
        variables={"IMAGE_TAG": "public-beta-052e420"},
    )

    assert profile.external_slurm_autoscaler_supervisors == [
        {
            "environment": "public-beta",
            "name": "oldlab",
            "pool_name": "oldlab",
            "service_name": "loom-oldlab-autoscaler.service",
            "timer_name": "loom-oldlab-autoscaler.timer",
            "working_directory": "/home/qianyi/dev/loom-worktrees/public-beta-052e420",
            "python_path": (
                "/home/qianyi/dev/loom-worktrees/"
                "public-beta-052e420/.venv/bin/python"
            ),
            "script_path": (
                "/home/qianyi/dev/loom-worktrees/public-beta-052e420/"
                "scripts/ops/worker_pool_autoscaler_external_once.py"
            ),
            "args": ["--pool-name", "oldlab", "--namespace", "loom-public-beta"],
            "requires": [
                "network-online.target",
                "loom-public-beta-postgres-port-forward.service",
            ],
            "timer_on_boot_sec": "45",
            "timer_on_unit_active_sec": "30",
            "timer_accuracy_sec": "5",
            "enabled": True,
            "active": True,
        },
    ]


def test_external_slurm_autoscaler_supervisor_check_reports_stale_inactive_unit(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    profile_path.write_text(
        """
environment = "public-beta"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}"
python_path = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}/.venv/bin/python"
script_path = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--pool-name", "oldlab", "--namespace", "loom-public-beta"]
requires = ["network-online.target", "loom-public-beta-postgres-port-forward.service"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(
        profile_path,
        variables={"IMAGE_TAG": "public-beta-052e420"},
    )
    stale_service_unit = """
[Service]
Type=oneshot
WorkingDirectory=/home/qianyi/dev/loom-worktrees/public-beta-b453057
Environment=PYTHONPATH=/home/qianyi/dev/loom-worktrees/public-beta-b453057/src
ExecStart=/home/qianyi/dev/loom-worktrees/public-beta-b453057/.venv/bin/python /home/qianyi/dev/loom-ops/oldlab_autoscaler_external_once.py
""".strip()
    desired_timer_unit = """
[Unit]
Description=Run Loom oldlab external autoscaler reconcile

[Timer]
OnBootSec=45
OnUnitActiveSec=30
AccuracySec=5
Unit=loom-oldlab-autoscaler.service

[Install]
WantedBy=timers.target
""".strip()

    def _runner(command: list[str]) -> tuple[int, str, str]:
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.service"]:
            return 0, stale_service_unit, ""
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.timer"]:
            return 0, desired_timer_unit, ""
        if command == ["systemctl", "--user", "is-enabled", "loom-oldlab-autoscaler.timer"]:
            return 0, "enabled\n", ""
        if command == ["systemctl", "--user", "is-active", "loom-oldlab-autoscaler.timer"]:
            return 3, "inactive\n", ""
        raise AssertionError(command)

    drift = diff_external_slurm_autoscaler_supervisors(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_autoscaler_supervisors[public-beta/oldlab].service_unit",
        "external_slurm_autoscaler_supervisors[public-beta/oldlab].timer_active",
    ]
    assert "--pool-name oldlab" in drift[0].desired
    assert "public-beta-b453057" in drift[0].live
    assert drift[1].desired == "active"
    assert drift[1].live == "inactive"


def test_external_slurm_autoscaler_supervisor_apply_writes_units_and_starts_timer(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    profile_path.write_text(
        """
environment = "public-beta"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/public-beta-052e420"
python_path = "/srv/loom/public-beta-052e420/.venv/bin/python"
script_path = "/srv/loom/public-beta-052e420/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--pool-name", "oldlab"]
requires = ["network-online.target"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    unit_dir = tmp_path / "systemd-user"
    commands: list[list[str]] = []

    def _runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        return 0, "", ""

    applied = apply_external_slurm_autoscaler_supervisors(
        profile,
        unit_dir=unit_dir,
        runner=_runner,
    )

    service_unit = (unit_dir / "loom-oldlab-autoscaler.service").read_text(
        encoding="utf-8",
    )
    timer_unit = (unit_dir / "loom-oldlab-autoscaler.timer").read_text(
        encoding="utf-8",
    )
    assert "WorkingDirectory=/srv/loom/public-beta-052e420" in service_unit
    assert "--pool-name oldlab" in service_unit
    assert "Unit=loom-oldlab-autoscaler.service" in timer_unit
    assert commands == [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", "--now", "loom-oldlab-autoscaler.timer"],
        ["systemctl", "--user", "restart", "loom-oldlab-autoscaler.timer"],
    ]
    assert applied == [
        {
            "kind": "external_slurm_autoscaler_supervisor",
            "service": "loom-oldlab-autoscaler.service",
            "timer": "loom-oldlab-autoscaler.timer",
        },
    ]


@pytest.mark.parametrize(
    ("path", "environment"),
    [
        (Path("deploy/environment-state/public-beta.toml"), "public-beta"),
        (Path("deploy/environment-state/staging.toml"), "staging"),
    ],
)
def test_committed_environment_state_profiles_cover_gb10_slurm_policy(
    path: Path,
    environment: str,
) -> None:
    profile = load_environment_state_profile(
        path,
        variables={
            "IMAGE_TAG": "public-beta-test",
            "ENV_CONFIG_VERSION": "public-beta-test",
        },
        expected_environment=environment,
    )

    gb10_policy = next(
        policy
        for policy in profile.autoscaler_policies
        if policy["pool_name"] == "gb10-arm64"
    )
    expected_cp_environment = "production" if environment == "public-beta" else environment
    assert profile.environment == environment
    assert gb10_policy["environment"] == expected_cp_environment
    assert gb10_policy["actuator"] == "slurm"
    assert gb10_policy["max_slots"] == 150
    assert gb10_policy["actuator_config"]["backend"] == "docker"
    assert gb10_policy["actuator_config"]["cpu_arch"] == "arm64"
    assert gb10_policy["actuator_config"]["partition"] == "gb10"
    assert len(gb10_policy["actuator_config"]["allowed_nodes"]) == 15

    gb10_state = next(
        state
        for state in profile.gb10_desired_states
        if state["pool_name"] == "gb10-arm64"
    )
    assert gb10_state["environment"] == expected_cp_environment
    assert gb10_state["image_tag"] == "public-beta-test"
    assert gb10_state["env_config_version"] == "public-beta-test"
    assert gb10_state["max_concurrent"] == 10
    assert gb10_state["target_slots"] == 150
    assert profile.catalog_provisioning["required"] is True
    command = profile.catalog_provisioning["command"]
    assert "loom datasets provision-public-beta-catalog" in command
    assert "loom datasets register skilllearnbench" in command
    assert "--mirror-to-object-store" in command
    assert "loom datasets audit --all --verify-bundles" in command
    assert profile.catalog_provisioning["required_env"] == [
        "HF_TOKEN",
        "LOOM_SVC_DB_URL",
        "LOOM_SVC_MINIO_ENDPOINT",
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
    ]


def test_public_beta_oldlab_policy_allows_all_five_oldlab_submit_nodes() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/public-beta.toml"),
        variables={
            "IMAGE_TAG": "public-beta-test",
            "ENV_CONFIG_VERSION": "public-beta-test",
        },
        expected_environment="public-beta",
    )

    oldlab_policy = next(
        policy
        for policy in profile.autoscaler_policies
        if policy["pool_name"] == "oldlab"
    )

    assert oldlab_policy["environment"] == "production"
    assert oldlab_policy["actuator_config"]["allowed_nodes"] == [
        "TRT-EAI-OLDLAB-1",
        "trt-EAI-OLDLAB-2",
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5",
    ]
    assert oldlab_policy["actuator_config"]["max_jobs"] == 5
