"""Contracts for the root-installed GB10 Slurm acceptance authority."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace

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


def _parse_test_contract(
    authority: ModuleType,
    profile: Path,
    candidate_sha: str,
) -> dict[str, object]:
    return authority._parse_contract(
        candidate_sha=candidate_sha,
        image_tag=f"staging-{candidate_sha[:7]}",
        profile_path=profile,
        profile_bytes=profile.read_bytes(),
    )


def _node_probe_registry_payload(authority: ModuleType) -> dict[str, str]:
    return {
        "ca_sha256": authority.TRIAL_CACHE_CA_SHA256,
        "registry_image": (
            f"{authority.TRIAL_CACHE_REGISTRY_REPO}:{authority.TRIAL_CACHE_CANARY_TAG}"
        ),
        "repo_digest": (
            f"{authority.TRIAL_CACHE_REGISTRY_REPO}@{authority.TRIAL_CACHE_CANARY_DIGEST}"
        ),
    }


def _trusted_test_registry_probe(authority: ModuleType) -> bytes:
    encoded = json.dumps(_node_probe_registry_payload(authority), sort_keys=True)
    return f"print({encoded!r})\n".encode()


def _execute_embedded_node_probe(
    authority: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    service_probe: bytes,
    service_ca: bytes,
    trusted_probe: bytes,
    trusted_ca: bytes,
    replace_snapshot_before_execution: bool = False,
) -> list[list[str]]:
    candidate_sha = "a" * 40
    candidate_tree = "b" * 40
    node = "trt-gb10-1"
    repo = tmp_path / "worker-repo"
    probe_path = repo / authority.TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH
    ca_path = repo / authority.TRIAL_CACHE_CA_RELATIVE_PATH
    probe_path.parent.mkdir(parents=True)
    ca_path.parent.mkdir(parents=True)
    probe_path.write_bytes(service_probe)
    ca_path.write_bytes(service_ca)
    env_file = tmp_path / "worker.env"
    env_file.write_text(
        f"LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO={authority.TRIAL_CACHE_REGISTRY_REPO}\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    executed: list[list[str]] = []
    real_run = subprocess.run

    def check_output(argv: list[str], **kwargs: object) -> str:
        if argv[:3] == ["/usr/bin/scontrol", "show", "hostnames"]:
            return node + "\n"
        if argv == ["/usr/bin/id", "-nG"]:
            return "loom-rollout docker\n"
        if argv[-2:] == ["rev-parse", "HEAD"]:
            return candidate_sha + "\n"
        if argv[-2:] == ["rev-parse", "HEAD^{tree}"]:
            return candidate_tree + "\n"
        if argv[-3:] == ["status", "--porcelain", "--untracked-files=no"]:
            return ""
        if argv[:1] == ["/usr/bin/python3"]:
            executed.append(argv)
            snapshot_probe = Path(argv[3] if argv[1] == "-c" else argv[1])
            snapshot_ca = Path(argv[argv.index("--ca-file") + 1])
            assert snapshot_probe != probe_path
            assert snapshot_ca != ca_path
            assert snapshot_probe.read_bytes() == trusted_probe
            assert snapshot_ca.read_bytes() == trusted_ca
            if replace_snapshot_before_execution:
                snapshot_probe.chmod(0o600)
                snapshot_probe.write_bytes(_trusted_test_registry_probe(authority))
            result = real_run(
                argv,
                capture_output=True,
                check=True,
                text=bool(kwargs.get("text")),
                timeout=kwargs.get("timeout"),
            )
            return str(result.stdout)
        raise AssertionError(argv)

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setenv("SLURM_JOB_NODELIST", node)
    monkeypatch.setattr(subprocess, "check_output", check_output)
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "gb10-node-probe",
            node,
            str(repo),
            str(env_file),
            candidate_sha,
            candidate_tree,
            authority.TRIAL_CACHE_REGISTRY_REPO,
            authority.TRIAL_CACHE_CA_SHA256,
            authority.TRIAL_CACHE_CANARY_DIGEST,
            str(probe_path),
            str(ca_path),
            hashlib.sha256(trusted_probe).hexdigest(),
            hashlib.sha256(trusted_ca).hexdigest(),
            str(os.geteuid()),
            str(os.getegid()),
        ],
    )
    exec(compile(authority._NODE_PROBE, "<gb10-node-probe>", "exec"), {})
    return executed


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
    assert authority.SLURM_PARTITION == "loom-staging"
    assert authority.CLUSTER_NAME == "trt-gb10"
    assert authority.CONTROLLER_HOST == "gx10-01c7"
    assert authority.SLURM_NODES == tuple(f"trt-gb10-{number}" for number in range(1, 16))
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
max_slots = 150

[worker_pool_autoscaler_policies.actuator_config]
slurm_cluster_name = "trt-gb10"
slurm_controller_host = "gx10-01c7"
partition = "loom-staging"
external_runner = true
slurm_account = "loom-staging"
qos_normal = "loom-staging"
candidate_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
max_jobs = 15
allowed_nodes = [
  "trt-gb10-1", "trt-gb10-2", "trt-gb10-3", "trt-gb10-4",
  "trt-gb10-5", "trt-gb10-6", "trt-gb10-7", "trt-gb10-8",
  "trt-gb10-9", "trt-gb10-10", "trt-gb10-11", "trt-gb10-12", "trt-gb10-13",
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
LOOM_WORKER_TRAJECTORIES_BUCKET = "loom-staging-trajectories"
LOOM_WORKER_ARTIFACTS_BUCKET = "loom-staging-artifacts"
LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"

[external_slurm_runner_prerequisites.worker_service_env.oldlab]
LOOM_WORKER_CONTROL_PLANE_URL = "http://192.168.50.103:18081"
LOOM_WORKER_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_SUBPROCESS_GATEWAY_URL = "http://192.168.50.103:19100"
LOOM_WORKER_MINIO_ENDPOINT = "http://192.168.50.103:19000"
LOOM_WORKER_TRAJECTORIES_BUCKET = "loom-staging-trajectories"
LOOM_WORKER_ARTIFACTS_BUCKET = "loom-staging-artifacts"
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

    contract = _parse_test_contract(authority, profile, candidate_sha)

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
        _parse_test_contract(authority, profile, candidate_sha)


def test_candidate_contract_accepts_exact_manager_witness_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / candidate_sha / "repo/deploy/environment-state/staging.toml"
    profile.parent.mkdir(parents=True)
    profile.write_text(
        Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    contract = _parse_test_contract(authority, profile, candidate_sha)

    assert contract["repo_dir"].name == "loom-remote-worker-staging-aaaaaaa"
    assert contract["env_file"].name == "staging-gb10-worker-staging-aaaaaaa.env"


def test_candidate_contract_rejects_profile_symlink_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    runtime_repo = tmp_path / candidate_sha / "repo"
    profile = runtime_repo / "deploy/environment-state/staging.toml"
    profile.parent.mkdir(parents=True)
    outside_profile = tmp_path / "outside-staging.toml"
    outside_profile.write_text(
        Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    profile.symlink_to(outside_profile)
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)
    monkeypatch.setattr(authority, "_git_identity", lambda *_args, **_kwargs: "b" * 40)

    with pytest.raises(authority.AcceptanceError, match="candidate asset"):
        authority._load_contract(candidate_sha, "staging-aaaaaaa")


def test_candidate_contract_rejects_active_non_gb10_supervisor_during_manager_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / candidate_sha / "repo/deploy/environment-state/staging.toml"
    profile.parent.mkdir(parents=True)
    payload = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    prerequisites = "[external_slurm_runner_prerequisites]\n"
    assert payload.count(prerequisites) == 1
    payload = payload.replace(
        prerequisites,
        prerequisites + "manager_witness_export_bootstrap = true\n",
        1,
    )
    for name in ("gb10-staging", "oldlab-staging"):
        prefix, marker, suffix = payload.partition(f'name = "{name}"')
        assert marker
        section, next_section, tail = suffix.partition(
            "\n[[external_slurm_autoscaler_supervisors]]"
        )
        active = "enabled = true\nactive = true"
        assert section.count(active) == 1
        section = section.replace(active, "enabled = false\nactive = false", 1)
        payload = prefix + marker + section + next_section + tail
    prefix, marker, suffix = payload.partition('name = "task-image-builder-oldlab-staging"')
    assert marker
    suffix = suffix.replace(
        "enabled = false\nactive = false",
        "enabled = true\nactive = true",
        1,
    )
    profile.write_text(prefix + marker + suffix, encoding="utf-8")
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    with pytest.raises(authority.AcceptanceError, match="bootstrap supervisors are not inert"):
        _parse_test_contract(authority, profile, candidate_sha)


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
        payload.replace("max_jobs = 15", "max_jobs = 16", 1),
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    with pytest.raises(authority.AcceptanceError, match="accepted contract"):
        _parse_test_contract(authority, profile, candidate_sha)


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
    assert 'f"--partition={SLURM_PARTITION}"' in source
    assert "loom-slurm-job-cgroup-guard.service" in source
    assert '"docker", "info"' in source


def test_candidate_inputs_accept_hardened_and_legacy_root_runtime_modes_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    runtime_repo = tmp_path / "candidates" / candidate_sha / "repo"
    trusted_probe = ROOT / authority.TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH
    trusted_ca = ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH
    runtime_probe = runtime_repo / authority.TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH
    runtime_ca = runtime_repo / authority.TRIAL_CACHE_CA_RELATIVE_PATH
    runtime_probe.parent.mkdir(parents=True)
    runtime_ca.parent.mkdir(parents=True)
    runtime_probe.write_bytes(trusted_probe.read_bytes())
    runtime_ca.write_bytes(trusted_ca.read_bytes())
    worker_repo = tmp_path / "worker-repo"
    worker_repo.mkdir(mode=0o750)
    env_file = tmp_path / "worker.env"
    env_file.write_text("SAFE=value\n", encoding="utf-8")
    env_file.chmod(0o600)
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path / "candidates")
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())
    calls: list[dict[str, object]] = []

    def git_identity(_repo: Path, **kwargs: object) -> str:
        calls.append(kwargs)
        return "b" * 40

    monkeypatch.setattr(authority, "_git_identity", git_identity)

    def run(
        argv: list[str],
        *,
        timeout: float = 30,
        check: bool = True,
        deadline: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check
        assert deadline is None
        tree_path = argv[-1].partition(":")[2]
        payload = (runtime_repo / tree_path).read_bytes()
        blob = hashlib.sha1(
            f"blob {len(payload)}\0".encode() + payload,
            usedforsecurity=False,
        ).hexdigest()
        return subprocess.CompletedProcess(argv, 0, stdout=blob + "\n", stderr="")

    monkeypatch.setattr(authority, "_run", run)

    verified = authority._verify_inputs(
        candidate_sha,
        {"repo_dir": worker_repo, "env_file": env_file},
    )
    assert verified.candidate_tree == "b" * 40
    assert verified.registry_probe_sha256 == hashlib.sha256(trusted_probe.read_bytes()).hexdigest()
    assert verified.registry_ca_sha256 == authority.TRIAL_CACHE_CA_SHA256
    assert calls == [
        {
            "uid": 0,
            "gid": 0,
            "modes": frozenset({0o555, 0o755}),
            "sha": candidate_sha,
            "deadline": None,
        },
        {
            "uid": os.geteuid(),
            "gid": os.getegid(),
            "modes": frozenset({0o750}),
            "sha": candidate_sha,
            "deadline": None,
        },
    ]


def test_node_probe_executes_only_a_verified_job_private_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    trusted_probe = _trusted_test_registry_probe(authority)
    trusted_ca = (ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH).read_bytes()

    executed = _execute_embedded_node_probe(
        authority,
        monkeypatch,
        tmp_path,
        service_probe=trusted_probe,
        service_ca=trusted_ca,
        trusted_probe=trusted_probe,
        trusted_ca=trusted_ca,
    )

    assert len(executed) == 1


def test_node_probe_rejects_replaced_probe_that_can_print_expected_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    trusted_probe = _trusted_test_registry_probe(authority)
    trusted_ca = (ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH).read_bytes()

    with pytest.raises(RuntimeError, match="registry probe identity"):
        _execute_embedded_node_probe(
            authority,
            monkeypatch,
            tmp_path,
            service_probe=b"print the expected JSON without pulling\n",
            service_ca=trusted_ca,
            trusted_probe=trusted_probe,
            trusted_ca=trusted_ca,
        )


def test_node_probe_rejects_replaced_ca_that_can_print_expected_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    trusted_probe = _trusted_test_registry_probe(authority)
    trusted_ca = (ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH).read_bytes()

    with pytest.raises(RuntimeError, match="registry CA identity"):
        _execute_embedded_node_probe(
            authority,
            monkeypatch,
            tmp_path,
            service_probe=trusted_probe,
            service_ca=b"replacement public CA\n",
            trusted_probe=trusted_probe,
            trusted_ca=trusted_ca,
        )


def test_node_probe_rechecks_the_job_private_snapshot_at_execution_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    trusted_probe = _trusted_test_registry_probe(authority) + b"# trusted identity\n"
    trusted_ca = (ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH).read_bytes()

    with pytest.raises(subprocess.CalledProcessError):
        _execute_embedded_node_probe(
            authority,
            monkeypatch,
            tmp_path,
            service_probe=trusted_probe,
            service_ca=trusted_ca,
            trusted_probe=trusted_probe,
            trusted_ca=trusted_ca,
            replace_snapshot_before_execution=True,
        )


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


def test_command_deadline_kills_the_whole_slurm_launcher_process_group(
    tmp_path: Path,
) -> None:
    authority = _load_authority()
    orphan_marker = tmp_path / "escaped-child"
    child = (
        "import pathlib,sys,time; time.sleep(0.3); pathlib.Path(sys.argv[1]).write_text('escaped')"
    )
    parent = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable, '-c', {child!r}, sys.argv[1]]); "
        "time.sleep(5)"
    )

    started = time.monotonic()
    with pytest.raises(authority.AcceptanceError, match="timed out safely"):
        authority._run(
            [sys.executable, "-c", parent, str(orphan_marker)],
            timeout=5,
            deadline=started + 0.05,
        )
    assert time.monotonic() - started < 1
    time.sleep(0.4)
    assert not orphan_marker.exists()


def test_overall_timeout_cancels_and_verifies_current_job_before_next_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    verified = SimpleNamespace(
        candidate_tree="b" * 40,
        registry_probe_sha256="c" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
    )
    monkeypatch.setattr(authority, "SLURM_NODES", ("trt-gb10-1", "trt-gb10-2"))
    commands: list[list[str]] = []
    queue_reads = 0

    def _run(
        argv: list[str],
        *,
        timeout: float = 30,
        check: bool = True,
        deadline: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal queue_reads
        del timeout, check
        commands.append(argv)
        if argv[:4] == ["/usr/bin/scontrol", "show", "node", "trt-gb10-1"]:
            assert deadline == 10.0
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    "NodeName=trt-gb10-1 CPUAlloc=0 AllocMem=0 State=IDLE "
                    "Partitions=gb10,loom-staging "
                ),
                stderr="",
            )
        if "srun" in argv:
            assert deadline == 10.0
            raise authority.AcceptanceError("authority overall time budget exhausted safely")
        if "/usr/bin/squeue" in argv:
            assert deadline == 70.0
            queue_reads += 1
            job_name = next(item.removeprefix("--name=") for item in argv if "--name=" in item)
            stdout = f"123|{job_name}\n" if queue_reads == 1 else ""
            return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")
        if "/usr/bin/scancel" in argv:
            assert deadline == 70.0
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        raise AssertionError(argv)

    monkeypatch.setattr(authority, "_run", _run)

    with pytest.raises(authority.AcceptanceError, match="overall time budget"):
        authority._probe_nodes(
            candidate_sha,
            verified,
            {"repo_dir": Path("/worker/repo"), "env_file": Path("/worker/env")},
            work_deadline=10.0,
            cleanup_deadline=70.0,
        )

    assert sum("srun" in command for command in commands) == 1
    assert sum("/usr/bin/scancel" in command for command in commands) == 1
    assert queue_reads == 2
    assert not any("trt-gb10-2" in command for command in commands)


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
        deadline: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check, deadline
        commands.append(argv)
        if argv[:4] == ["/usr/bin/scontrol", "show", "node", "trt-gb10-1"]:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    "NodeName=trt-gb10-1 CPUAlloc=0 AllocMem=0 State=IDLE "
                    "Partitions=gb10,loom-staging "
                ),
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

    verified = authority.VerifiedCandidateInputs(
        candidate_tree="b" * 40,
        registry_probe_sha256="c" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
    )
    passed, deferred = authority._probe_nodes(
        candidate_sha,
        verified,
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
    assert verified.candidate_tree in srun
    assert verified.registry_probe_sha256 in srun
    assert "--partition=loom-staging" in srun


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


def test_authority_artifact_writer_retries_short_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    state_root = tmp_path / "authority-state"
    artifact_path = state_root / "current.json"
    monkeypatch.setattr(authority, "STATE_ROOT", state_root)
    monkeypatch.setattr(authority, "ARTIFACT_PATH", artifact_path)
    real_lstat = Path.lstat

    def trusted_state_root_lstat(path: Path) -> object:
        metadata = real_lstat(path)
        if path == state_root:
            return SimpleNamespace(st_mode=metadata.st_mode, st_uid=0, st_gid=0)
        return metadata

    monkeypatch.setattr(Path, "lstat", trusted_state_root_lstat)
    real_write = os.write
    write_count = 0

    def short_write(descriptor: int, payload: bytes) -> int:
        nonlocal write_count
        write_count += 1
        return real_write(descriptor, payload[: max(1, len(payload) // 2)])

    monkeypatch.setattr(authority.os, "write", short_write)
    payload = {"kind": "loom_gb10_slurm_acceptance", "result": "pass"}

    authority._write_artifact(payload)

    assert json.loads(artifact_path.read_text(encoding="utf-8")) == payload
    assert write_count > 1


def test_authority_emits_the_canonical_artifact_consumed_by_the_broker(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    candidate_tree = "b" * 40
    written: list[dict[str, object]] = []
    monkeypatch.setattr(
        authority,
        "_parser",
        lambda: SimpleNamespace(
            parse_args=lambda: SimpleNamespace(
                candidate_sha=candidate_sha,
                image_tag="staging-aaaaaaa",
            )
        ),
    )
    monkeypatch.setattr(authority, "_verify_installed_authority", lambda: None)
    monkeypatch.setattr(authority, "_verify_controller", lambda *, deadline: None)
    monkeypatch.setattr(
        authority,
        "_load_contract",
        lambda _sha, _tag, *, deadline: {
            "profile_sha256": "c" * 64,
            "repo_dir": Path("/worker/repo"),
            "env_file": Path("/worker/env"),
        },
    )
    verified = authority.VerifiedCandidateInputs(
        candidate_tree=candidate_tree,
        registry_probe_sha256="d" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
    )
    monkeypatch.setattr(
        authority,
        "_verify_inputs",
        lambda _sha, _contract, *, deadline: verified,
    )
    monkeypatch.setattr(
        authority,
        "_probe_nodes",
        lambda _sha, _verified, _contract, *, work_deadline, cleanup_deadline: (
            list(authority.SLURM_NODES),
            [],
        ),
    )
    monkeypatch.setattr(authority, "_write_artifact", lambda payload: written.append(payload))

    assert authority.main() == 0

    output = capsys.readouterr().out
    assert output.endswith("\n")
    emitted = json.loads(output)
    assert output == json.dumps(emitted, sort_keys=True, separators=(",", ":")) + "\n"
    assert written == [emitted]
    assert emitted["candidate_sha"] == candidate_sha
    assert emitted["candidate_tree"] == candidate_tree
    assert emitted["profile_sha256"] == "c" * 64
    assert emitted["nodes"] == list(authority.SLURM_NODES)


def test_installer_publishes_only_root_owned_fixed_authority() -> None:
    source = _installer_source()
    assert 'CONTROLLER="gx10-01c7"' in source
    assert "grep -Eq" not in source
    assert 'INSTALL_PATH="/usr/local/libexec/loom-gb10-slurm-acceptance-authority"' in source
    assert 'STATE_ROOT="/var/lib/loom-gb10-slurm-authority"' in source
    assert "install -o root -g root -m 0755" in source
    assert "install -d -o root -g root -m 0755" in source
    assert "/home/qianyi" not in source
