"""Contracts for the root-installed GB10 Slurm acceptance authority."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
AUTHORITY = ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py"
INSTALLER = ROOT / "deploy/slurm/install-loom-gb10-acceptance-authority.sh"


def _authority_source() -> str:
    return AUTHORITY.read_text(encoding="utf-8")


def _installer_source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def _load_authority() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gb10_acceptance_authority_test", AUTHORITY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_authority_compiles_and_installer_parses() -> None:
    compile_result = subprocess.run(
        ["python3", "-m", "py_compile", str(AUTHORITY)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert compile_result.returncode == 0, compile_result.stderr
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    parse_result = subprocess.run(
        [bash, "-n", str(INSTALLER)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert parse_result.returncode == 0, parse_result.stderr


def test_authority_is_fixed_to_service_identity_and_real_slurm_partition() -> None:
    authority = _load_authority()
    assert authority.SERVICE_USER == "loom-rollout"
    assert authority.SERVICE_UID == 995
    assert authority.SERVICE_GID == 2007
    assert authority.SLURM_ACCOUNT == "loom-staging"
    assert authority.SLURM_QOS == "loom-staging"
    assert authority.CLUSTER_NAME == "trt-gb10"
    assert authority.CONTROLLER_HOST == "gx10-01c7"
    assert authority.SLURM_NODES == (
        "trt-gb10-1",
        "trt-gb10-2",
        "trt-gb10-3",
        "trt-gb10-4",
        "trt-gb10-5",
        "trt-gb10-6",
        "trt-gb10-7",
        "trt-gb10-8",
        "trt-gb10-9",
        "trt-gb10-11",
        "trt-gb10-12",
        "trt-gb10-13",
        "trt-gb10-14",
        "trt-gb10-15",
    )
    assert authority.LEGACY_AGENT_NODES == tuple(f"trt-gb10-{number}" for number in range(1, 16))


def test_candidate_contract_separates_slurm_nodes_from_retired_agent_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / candidate_sha / "repo/deploy/environment-state/staging.toml"
    profile.parent.mkdir(parents=True)
    profile.write_text(
        """
environment = "staging"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = 140

[worker_pool_autoscaler_policies.actuator_config]
slurm_cluster_name = "trt-gb10"
slurm_controller_host = "gx10-01c7"
partition = "gb10"
external_runner = true
slurm_account = "loom-staging"
qos_normal = "loom-staging"
candidate_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
max_jobs = 14
allowed_nodes = [
  "trt-gb10-1", "trt-gb10-2", "trt-gb10-3", "trt-gb10-4",
  "trt-gb10-5", "trt-gb10-6", "trt-gb10-7", "trt-gb10-8",
  "trt-gb10-9", "trt-gb10-11", "trt-gb10-12", "trt-gb10-13",
  "trt-gb10-14", "trt-gb10-15",
]
repo_dir = "/shared_work2/loom-staging-rollout/worker-repos/exact"
env_file = "/shared_work2/loom-staging-rollout/worker-envs/exact.env"

[[gb10_worker_pool_desired_states]]
pool_name = "gb10"
target_slots = 0

[gb10_worker_pool_desired_states.host_intents]
trt-gb10-1 = "stopped"
trt-gb10-2 = "stopped"
trt-gb10-3 = "stopped"
trt-gb10-4 = "stopped"
trt-gb10-5 = "stopped"
trt-gb10-6 = "stopped"
trt-gb10-7 = "stopped"
trt-gb10-8 = "stopped"
trt-gb10-9 = "stopped"
trt-gb10-10 = "stopped"
trt-gb10-11 = "stopped"
trt-gb10-12 = "stopped"
trt-gb10-13 = "stopped"
trt-gb10-14 = "stopped"
trt-gb10-15 = "stopped"

[external_slurm_runner_prerequisites]
materialize = true
require_external_allocation_authority = true
pools = ["gb10"]

[external_slurm_runner_prerequisites.worker_service_env.gb10]
LOOM_WORKER_CONTROL_PLANE_URL = "http://192.168.50.103:18081"
LOOM_WORKER_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_SUBPROCESS_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_MINIO_ENDPOINT = "http://192.168.50.103:19000"
LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"

[external_slurm_runner_prerequisites.worker_service_env.oldlab]
LOOM_WORKER_CONTROL_PLANE_URL = "http://192.168.50.103:18081"
LOOM_WORKER_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_SUBPROCESS_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_MINIO_ENDPOINT = "http://192.168.50.103:19000"
LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"

[[external_slurm_autoscaler_supervisors]]
pool_name = "gb10"
execution_host = "gx10-01c7"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    contract = authority._load_contract(candidate_sha, "staging-aaaaaaa")

    assert contract["repo_dir"] == Path("/shared_work2/loom-staging-rollout/worker-repos/exact")
    assert contract["env_file"] == Path("/shared_work2/loom-staging-rollout/worker-envs/exact.env")


def test_candidate_contract_rejects_retired_worker_service_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / candidate_sha / "repo/deploy/environment-state/staging.toml"
    profile.parent.mkdir(parents=True)
    payload = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    profile.write_text(
        payload.replace(
            "http://192.168.50.103:18081",
            "http://192.168.50.13:18081",
            1,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    with pytest.raises(authority.AcceptanceError, match="prerequisites are incomplete"):
        authority._load_contract(candidate_sha, "staging-aaaaaaa")


def test_candidate_contract_rejects_job_ceiling_beyond_accepted_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / candidate_sha / "repo/deploy/environment-state/staging.toml"
    profile.parent.mkdir(parents=True)
    payload = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    profile.write_text(
        payload.replace("max_jobs = 14", "max_jobs = 15", 1),
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    with pytest.raises(authority.AcceptanceError, match="accepted contract"):
        authority._load_contract(candidate_sha, "staging-aaaaaaa")


def test_authority_runs_real_service_user_allocations_on_each_exact_node() -> None:
    source = _authority_source()
    assert '"runuser", "-u", SERVICE_USER, "--"' in source
    assert '"srun"' in source
    assert 'f"--nodelist={node}"' in source
    assert '"--immediate=15"' in source
    assert 'f"--job-name={job_name}"' in source
    assert '"/usr/bin/scancel"' in source
    assert 'f"--account={SLURM_ACCOUNT}"' in source
    assert 'f"--qos={SLURM_QOS}"' in source
    assert "loom-slurm-job-cgroup-guard.service" in source
    assert '"docker", "info"' in source


def test_busy_deferral_requires_a_real_busy_node_and_scheduler_error() -> None:
    authority = _load_authority()
    busy = subprocess.CompletedProcess(
        args=["srun"],
        returncode=1,
        stdout="",
        stderr="srun: error: Unable to allocate resources: Requested nodes are busy\n",
    )
    unrelated = subprocess.CompletedProcess(
        args=["srun"], returncode=1, stdout="", stderr="invalid qos\n"
    )

    assert authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED ",
        result=busy,
    )
    assert not authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=0 AllocMem=0 State=IDLE ",
        result=busy,
    )
    assert not authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED ",
        result=unrelated,
    )


def test_allocation_probe_requires_candidate_registry_pull_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    repo = Path("/shared_work2/loom-staging-rollout/worker-repos/exact")
    env_file = Path("/shared_work2/loom-staging-rollout/worker-envs/exact.env")
    monkeypatch.setattr(authority, "SLURM_NODES", ("trt-gb10-1",))
    commands: list[list[str]] = []

    def _run(
        argv: list[str],
        *,
        timeout: float = 30,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check
        commands.append(argv)
        if argv[:4] == ["/usr/bin/scontrol", "show", "node", "trt-gb10-1"]:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=("NodeName=trt-gb10-1 CPUAlloc=0 AllocMem=0 State=IDLE Partitions=gb10 "),
                stderr="",
            )
        if "/usr/bin/squeue" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        if "srun" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "candidate_sha": candidate_sha,
                        "node": "trt-gb10-1",
                        "trial_cache_registry": {
                            "ca_sha256": authority.TRIAL_CACHE_CA_SHA256,
                            "registry_image": (
                                f"{authority.TRIAL_CACHE_REGISTRY_REPO}:"
                                f"{authority.TRIAL_CACHE_CANARY_TAG}"
                            ),
                            "repo_digest": (
                                f"{authority.TRIAL_CACHE_REGISTRY_REPO}@"
                                f"{authority.TRIAL_CACHE_CANARY_DIGEST}"
                            ),
                        },
                    },
                    sort_keys=True,
                )
                + "\n",
                stderr="",
            )
        raise AssertionError(argv)

    monkeypatch.setattr(authority, "_run", _run)

    passed, deferred = authority._probe_nodes(
        candidate_sha,
        {"repo_dir": repo, "env_file": env_file},
    )

    assert passed == ["trt-gb10-1"]
    assert deferred == []
    srun = next(command for command in commands if "srun" in command)
    assert str(repo / "scripts/ops/staging_trial_cache_registry_node_probe.py") in srun
    assert str(repo / "deploy/worker-pools/trial-cache/staging-ca.crt") in srun
    assert authority.TRIAL_CACHE_REGISTRY_REPO in srun
    assert authority.TRIAL_CACHE_CA_SHA256 in srun
    assert authority.TRIAL_CACHE_CANARY_DIGEST in srun


def test_authority_binds_candidate_profile_repo_env_and_short_expiry() -> None:
    source = _authority_source()
    assert "/opt/loom-staging-runner/candidates" in source
    assert "deploy/environment-state/staging.toml" in source
    assert "profile_sha256" in source
    assert "candidate_tree" in source
    assert "timedelta(minutes=30)" in source
    assert '"kind": "loom_gb10_slurm_acceptance"' in source
    assert '"result": "pass"' in source
    assert '"probed_nodes": probed_nodes' in source
    assert '"deferred_busy_nodes": deferred_busy_nodes' in source
    assert "git_timeout = 120 if uid == SERVICE_UID else 30" in source
    assert "command timed out safely" in source
    assert "/home/qianyi" not in source
    assert "/shared_work2/qianyi" not in source


def test_installer_publishes_only_root_owned_fixed_authority() -> None:
    source = _installer_source()
    assert 'CONTROLLER="gx10-01c7"' in source
    assert "grep -Eq" not in source
    assert 'INSTALL_PATH="/usr/local/libexec/loom-gb10-slurm-acceptance-authority"' in source
    assert 'STATE_ROOT="/var/lib/loom-gb10-slurm-authority"' in source
    assert "install -o root -g root -m 0755" in source
    assert "install -d -o root -g root -m 0755" in source
    assert "/home/qianyi" not in source
