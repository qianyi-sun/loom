"""Contracts for the root-installed GB10 Slurm acceptance authority."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from tests.ops.test_staging_rollout_shared_repo_consumer import _checkout, _normalize

ROOT = Path(__file__).resolve().parents[2]
AUTHORITY = ROOT / "scripts/ops/gb10_slurm_acceptance_authority.py"
INSTALLER = ROOT / "deploy/slurm/install-loom-gb10-acceptance-authority.sh"
TMPFILES = ROOT / "deploy/slurm/loom-gb10-slurm-authority.tmpfiles"


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


def _canonical_worker_env(authority: ModuleType, *, image_tag: str) -> bytes:
    values = {
        "IMAGE_TAG": image_tag,
        "ENV_CONFIG_VERSION": image_tag,
        "LOOM_IMAGE_TAG": image_tag,
        "LOOM_WORKER_ENV_CONFIG_VERSION": image_tag,
        "LOOM_WORKER_POOL_NAME": "gb10",
        "LOOM_WORKER_MAX_CONCURRENT": "10",
        "LOOM_WORKER_TOKEN": "secret-token",
        "LOOM_WORKER_MINIO_ACCESS_KEY": "secret-access",
        "LOOM_WORKER_MINIO_SECRET_KEY": "secret-key",
        **authority.PRIVATE_WORKER_SERVICE_ENV["gb10"],
    }
    return "".join(f"{key}={value}\n" for key, value in values.items()).encode()


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
    mutate_worker_input: str | None = None,
) -> list[list[str]]:
    node = "trt-gb10-1"
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())
    _root, repo, _initial_sha = _checkout(tmp_path)
    probe_path = repo / authority.TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH
    ca_path = repo / authority.TRIAL_CACHE_CA_RELATIVE_PATH
    probe_path.parent.mkdir(parents=True)
    ca_path.parent.mkdir(parents=True)
    probe_path.write_bytes(service_probe)
    ca_path.write_bytes(service_ca)
    subprocess.run(
        ["/usr/bin/git", "-C", str(repo), "add", "."],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(repo),
            "-c",
            "user.email=test@example.com",
            "-c",
            "user.name=Test",
            "commit",
            "--quiet",
            "-m",
            "probe assets",
        ],
        check=True,
        capture_output=True,
    )
    candidate_sha = subprocess.run(
        ["/usr/bin/git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_tree = subprocess.run(
        ["/usr/bin/git", "-C", str(repo), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (repo / ".git/config").write_bytes(
        authority._WORKER_VERIFIER_NAMESPACE["WORKER_CANONICAL_GIT_CONFIG"]
    )
    _normalize(repo)
    image_tag = f"staging-{candidate_sha[:7]}"
    env_file = tmp_path / "worker.env"
    env_file.write_bytes(_canonical_worker_env(authority, image_tag=image_tag))
    env_file.chmod(0o600)
    worker_inputs = authority._verify_worker_inputs(
        repo_dir=repo,
        env_file=env_file,
        candidate_sha=candidate_sha,
        image_tag=image_tag,
        requested_concurrency=10,
    )
    if mutate_worker_input == "checkout":
        (repo / "README.md").write_text("allocation-time drift\n", encoding="utf-8")
        (repo / "README.md").chmod(0o640)
    elif mutate_worker_input == "environment":
        env_file.write_bytes(env_file.read_bytes() + b"EXTRA_DRIFT=1\n")
    executed: list[list[str]] = []
    real_run = subprocess.run

    def check_output(argv: list[str], **kwargs: object) -> str:
        if argv[:3] == ["/usr/bin/scontrol", "show", "hostnames"]:
            return node + "\n"
        if argv == ["/usr/bin/id", "-nG"]:
            return "loom-rollout docker\n"
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
        if argv[:1] == ["/usr/bin/git"]:
            return real_run(argv, **_kwargs)
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
            image_tag,
            "10",
            json.dumps(worker_inputs, sort_keys=True, separators=(",", ":")),
            json.dumps(
                authority.PRIVATE_WORKER_SERVICE_ENV["gb10"],
                sort_keys=True,
                separators=(",", ":"),
            ),
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
    assert authority.LEGACY_AGENT_NODES == tuple(f"trt-gb10-{number}" for number in range(1, 16))


def test_exclusive_builder_node_is_outside_normal_slurm_acceptance_boundary() -> None:
    authority = _load_authority()

    assert authority.SLURM_NODES == (
        "trt-gb10-1",
        "trt-gb10-3",
        "trt-gb10-4",
        "trt-gb10-5",
        "trt-gb10-6",
        "trt-gb10-7",
        "trt-gb10-8",
        "trt-gb10-9",
        "trt-gb10-10",
        "trt-gb10-11",
        "trt-gb10-12",
        "trt-gb10-13",
        "trt-gb10-14",
        "trt-gb10-15",
    )
    assert "trt-gb10-2" in authority.LEGACY_AGENT_NODES


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
partition = "loom-staging"
external_runner = true
slurm_account = "loom-staging"
qos_normal = "loom-staging"
candidate_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
max_jobs = 14
allowed_nodes = [
  "trt-gb10-1", "trt-gb10-3", "trt-gb10-4",
  "trt-gb10-5", "trt-gb10-6", "trt-gb10-7", "trt-gb10-8",
  "trt-gb10-9", "trt-gb10-10", "trt-gb10-11", "trt-gb10-12", "trt-gb10-13",
  "trt-gb10-14", "trt-gb10-15",
]
repo_dir = "/shared_work2/loom-staging-rollout/worker-repos/loom-remote-worker-staging-aaaaaaa"
env_file = "/shared_work2/loom-staging-rollout/worker-envs/staging-gb10-worker-staging-aaaaaaa.env"
requested_concurrency = 10

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

    assert contract["repo_dir"] == Path(
        "/shared_work2/loom-staging-rollout/worker-repos/loom-remote-worker-staging-aaaaaaa"
    )
    assert contract["env_file"] == Path(
        "/shared_work2/loom-staging-rollout/worker-envs/staging-gb10-worker-staging-aaaaaaa.env"
    )
    assert contract["requested_concurrency"] == 10


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
        payload.replace("max_jobs = 14", "max_jobs = 15", 1),
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path)

    with pytest.raises(authority.AcceptanceError, match="accepted contract"):
        _parse_test_contract(authority, profile, candidate_sha)


@pytest.mark.parametrize(
    ("before", "after"),
    (
        ("min_slots = 0", "min_slots = false"),
        ("target_slots = 0", "target_slots = false"),
        ("external_runner = true", "external_runner = 1"),
        ('pools = ["gb10", "oldlab"]', 'pools = "gb10"'),
    ),
    ids=("min-slots-bool", "target-slots-bool", "external-runner-int", "pools-string"),
)
def test_candidate_contract_rejects_cross_type_scalar_and_list_values(
    tmp_path: Path,
    before: str,
    after: str,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / "staging.toml"
    payload = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    assert before in payload
    profile.write_text(payload.replace(before, after, 1), encoding="utf-8")

    with pytest.raises(authority.AcceptanceError):
        _parse_test_contract(authority, profile, candidate_sha)


@pytest.mark.parametrize(
    ("before", "after"),
    (
        (
            'repo_dir = "/shared_work2/loom-staging-rollout/worker-repos/'
            'loom-remote-worker-${IMAGE_TAG}"',
            'repo_dir = "/shared_work2/loom-staging-rollout/worker-repos/exact"',
        ),
        (
            'env_file = "/shared_work2/loom-staging-rollout/worker-envs/'
            'staging-gb10-worker-${IMAGE_TAG}.env"',
            'env_file = "/shared_work2/loom-staging-rollout/worker-envs/exact.env"',
        ),
        ("requested_concurrency = 10", "requested_concurrency = false"),
        ("requested_concurrency = 10", "requested_concurrency = 9"),
        ("requested_concurrency = 10", 'requested_concurrency = "10"'),
    ),
    ids=("repo-path", "env-path", "concurrency-bool", "concurrency-value", "concurrency-string"),
)
def test_candidate_contract_requires_canonical_worker_paths_and_concurrency(
    tmp_path: Path,
    before: str,
    after: str,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / "staging.toml"
    payload = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    assert before in payload
    profile.write_text(payload.replace(before, after, 1), encoding="utf-8")

    with pytest.raises(authority.AcceptanceError, match="accepted contract"):
        _parse_test_contract(authority, profile, candidate_sha)


@pytest.mark.parametrize(
    "malformed_row",
    (
        "\n[[worker_pool_autoscaler_policies]]\npool_name = 7\n",
        "\n[[gb10_worker_pool_desired_states]]\npool_name = false\n",
        '\n[[external_slurm_autoscaler_supervisors]]\npool_name = ["gb10"]\n',
    ),
    ids=("policy", "desired-state", "supervisor"),
)
def test_candidate_contract_rejects_malformed_nested_rows(
    tmp_path: Path,
    malformed_row: str,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    profile = tmp_path / "staging.toml"
    payload = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    profile.write_text(payload + malformed_row, encoding="utf-8")

    with pytest.raises(authority.AcceptanceError):
        _parse_test_contract(authority, profile, candidate_sha)


def test_authority_runs_real_service_user_allocations_on_each_exact_node() -> None:
    authority = _load_authority()

    assert authority._service_command(authority.SRUN) == [
        "/usr/sbin/runuser",
        "-u",
        authority.SERVICE_USER,
        "--",
        "/usr/bin/srun",
    ]
    assert authority.SRUN == "/usr/bin/srun"


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
    env_file = tmp_path / "worker.env"
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path / "candidates")
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())
    calls: list[dict[str, object]] = []

    def git_identity(_repo: Path, **kwargs: object) -> str:
        calls.append(kwargs)
        return "b" * 40

    monkeypatch.setattr(authority, "_git_identity", git_identity)
    worker_inputs = {"head": candidate_sha, "env_sha256": "d" * 64}
    monkeypatch.setattr(
        authority,
        "_verify_worker_inputs",
        lambda **_kwargs: worker_inputs,
    )

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
        {
            "repo_dir": worker_repo,
            "env_file": env_file,
            "image_tag": "staging-aaaaaaa",
            "requested_concurrency": 10,
        },
    )
    assert verified.candidate_tree == "b" * 40
    assert verified.registry_probe_sha256 == hashlib.sha256(trusted_probe.read_bytes()).hexdigest()
    assert verified.registry_ca_sha256 == authority.TRIAL_CACHE_CA_SHA256
    assert verified.worker_inputs == worker_inputs
    assert calls == [
        {
            "uid": 0,
            "gid": 0,
            "modes": frozenset({0o555, 0o755}),
            "sha": candidate_sha,
            "deadline": None,
        },
    ]


def test_candidate_inputs_bind_physical_worker_checkout_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    _root, worker_repo, candidate_sha = _checkout(tmp_path / "shared")
    candidate_tree = subprocess.run(
        ["/usr/bin/git", "-C", str(worker_repo), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    runtime_repo = tmp_path / "candidates" / candidate_sha / "repo"
    runtime_probe = runtime_repo / authority.TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH
    runtime_ca = runtime_repo / authority.TRIAL_CACHE_CA_RELATIVE_PATH
    runtime_probe.parent.mkdir(parents=True)
    runtime_ca.parent.mkdir(parents=True)
    runtime_probe.write_bytes((ROOT / authority.TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH).read_bytes())
    runtime_ca.write_bytes((ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH).read_bytes())
    image_tag = f"staging-{candidate_sha[:7]}"
    env_file = tmp_path / "worker.env"
    env_file.write_bytes(_canonical_worker_env(authority, image_tag=image_tag))
    env_file.chmod(0o600)
    monkeypatch.setattr(authority, "CANDIDATE_ROOT", tmp_path / "candidates")
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())

    def git_identity(repo: Path, **_kwargs: object) -> str:
        assert repo == runtime_repo, "worker checkout must use the physical verifier"
        return candidate_tree

    monkeypatch.setattr(authority, "_git_identity", git_identity)

    def run(
        argv: list[str],
        *,
        timeout: float = 30,
        check: bool = True,
        deadline: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check, deadline
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
        {
            "repo_dir": worker_repo,
            "env_file": env_file,
            "image_tag": image_tag,
            "requested_concurrency": 10,
        },
    )

    assert verified.candidate_tree == candidate_tree
    assert verified.worker_inputs["head"] == candidate_sha
    assert verified.worker_inputs["target_inode"] == worker_repo.stat().st_ino
    assert verified.worker_inputs["env_sha256"] == hashlib.sha256(env_file.read_bytes()).hexdigest()


def test_worker_input_verifier_rejects_git_ignored_physical_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    _root, repo, candidate_sha = _checkout(tmp_path)
    env_file = tmp_path / "worker.env"
    env_file.write_bytes(_canonical_worker_env(authority, image_tag="staging-aaaaaaa"))
    env_file.chmod(0o600)
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())

    verified = authority._verify_worker_inputs(
        repo_dir=repo,
        env_file=env_file,
        candidate_sha=candidate_sha,
        image_tag="staging-aaaaaaa",
        requested_concurrency=10,
    )

    assert verified["head"] == candidate_sha
    assert verified["target_inode"] == repo.stat().st_ino
    assert verified["env_sha256"] == hashlib.sha256(env_file.read_bytes()).hexdigest()

    exclude = repo / ".git/info/exclude"
    exclude.write_text("ignored-entry/\n", encoding="utf-8")
    exclude.chmod(0o640)
    (repo / "ignored-entry").mkdir(mode=0o750)
    (repo / "ignored-entry/payload").write_text("ignored\n", encoding="utf-8")
    (repo / "ignored-entry/payload").chmod(0o640)

    with pytest.raises(authority.AcceptanceError, match="worker input verification"):
        authority._verify_worker_inputs(
            repo_dir=repo,
            env_file=env_file,
            candidate_sha=candidate_sha,
            image_tag="staging-aaaaaaa",
            requested_concurrency=10,
        )


def test_worker_input_evidence_excludes_mount_local_device_numbers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    _root, repo, candidate_sha = _checkout(tmp_path)
    env_file = tmp_path / "worker.env"
    env_file.write_bytes(_canonical_worker_env(authority, image_tag="staging-aaaaaaa"))
    env_file.chmod(0o600)
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())

    verified = authority._verify_worker_inputs(
        repo_dir=repo,
        env_file=env_file,
        candidate_sha=candidate_sha,
        image_tag="staging-aaaaaaa",
        requested_concurrency=10,
    )

    assert set(verified).isdisjoint({"root_device", "target_device", "git_device", "env_device"})
    assert verified["target_inode"] == repo.stat().st_ino
    assert verified["env_inode"] == env_file.stat().st_ino


@pytest.mark.parametrize(
    "mutation",
    ("content", "mode", "hardlink", "commondir"),
)
def test_worker_input_verifier_rejects_tree_metadata_and_git_redirection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    authority = _load_authority()
    _root, repo, candidate_sha = _checkout(tmp_path)
    env_file = tmp_path / "worker.env"
    env_file.write_bytes(_canonical_worker_env(authority, image_tag="staging-aaaaaaa"))
    env_file.chmod(0o600)
    monkeypatch.setattr(authority, "SERVICE_UID", os.geteuid())
    monkeypatch.setattr(authority, "SERVICE_GID", os.getegid())
    if mutation == "content":
        (repo / "other.txt").write_text("drift\n", encoding="utf-8")
    elif mutation == "mode":
        (repo / "other.txt").chmod(0o600)
    elif mutation == "hardlink":
        outside = tmp_path / "outside"
        outside.write_text("outside\n", encoding="utf-8")
        (repo / "other.txt").unlink()
        os.link(outside, repo / "other.txt")
        (repo / "other.txt").chmod(0o640)
    else:
        (repo / ".git/commondir").write_text("../external.git\n", encoding="utf-8")
        (repo / ".git/commondir").chmod(0o640)

    with pytest.raises(authority.AcceptanceError, match="worker input verification"):
        authority._verify_worker_inputs(
            repo_dir=repo,
            env_file=env_file,
            candidate_sha=candidate_sha,
            image_tag="staging-aaaaaaa",
            requested_concurrency=10,
        )


@pytest.mark.parametrize(
    "payload",
    (
        b"LOOM_WORKER_TOKEN=one\nLOOM_WORKER_TOKEN=two\n",
        b"LOOM_WORKER_POOL_NAME=oldlab\n",
        b"malformed-line\n",
    ),
    ids=("duplicate", "candidate-drift", "malformed"),
)
def test_worker_environment_rejects_noncanonical_or_ambiguous_values(
    tmp_path: Path,
    payload: bytes,
) -> None:
    authority = _load_authority()
    env_file = tmp_path / "worker.env"
    env_file.write_bytes(_canonical_worker_env(authority, image_tag="staging-aaaaaaa") + payload)
    env_file.chmod(0o600)

    with pytest.raises(authority.AcceptanceError, match="worker environment"):
        authority._verify_worker_environment(
            env_file,
            image_tag="staging-aaaaaaa",
            requested_concurrency=10,
            uid=os.geteuid(),
            gid=os.getegid(),
        )


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


@pytest.mark.parametrize("mutation", ("checkout", "environment"))
def test_node_probe_rechecks_bound_worker_inputs_before_trusted_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    authority = _load_authority()
    trusted_probe = _trusted_test_registry_probe(authority)
    trusted_ca = (ROOT / authority.TRIAL_CACHE_CA_RELATIVE_PATH).read_bytes()

    with pytest.raises(RuntimeError, match="worker input evidence mismatched"):
        _execute_embedded_node_probe(
            authority,
            monkeypatch,
            tmp_path,
            service_probe=trusted_probe,
            service_ca=trusted_ca,
            trusted_probe=trusted_probe,
            trusted_ca=trusted_ca,
            mutate_worker_input=mutation,
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
    started_busy = subprocess.CompletedProcess(
        args=["srun"],
        returncode=1,
        stdout=authority.ALLOCATION_START_MARKER + "\n",
        stderr=busy.stderr,
    )

    assert authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED ",
        result=busy,
        scheduler_never_started=True,
    )
    assert not authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=0 AllocMem=0 State=IDLE ",
        result=busy,
        scheduler_never_started=True,
    )
    assert not authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED ",
        result=unrelated,
        scheduler_never_started=True,
    )
    assert not authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED ",
        result=busy,
        scheduler_never_started=False,
    )
    assert not authority._node_is_deferred_busy(
        node_config="NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED ",
        result=started_busy,
        scheduler_never_started=True,
    )


@pytest.mark.parametrize(
    ("stdout", "expected"),
    (
        ("123|loom-job|CANCELLED by 995|Unknown|trt-gb10-1\n", True),
        ("38794|loom-job|CANCELLED by 995|None|None assigned\n", True),
        ("123|loom-job|RUNNING|2026-08-27T12:00:00|trt-gb10-1\n", False),
        ("123|other-job|CANCELLED|Unknown|trt-gb10-1\n", False),
        ("", False),
    ),
)
def test_busy_deferral_requires_structured_never_started_job_readback(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str,
    expected: bool,
) -> None:
    authority = _load_authority()
    commands: list[list[str]] = []

    def run(
        argv: list[str],
        *,
        timeout: float = 30,
        check: bool = True,
        deadline: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check
        assert deadline == 70.0
        commands.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(authority, "_run", run)

    assert (
        authority._allocation_never_started(
            "loom-job",
            node="trt-gb10-1",
            deadline=70.0,
        )
        is expected
    )
    assert commands and "/usr/bin/sacct" in commands[0]
    assert "--name=loom-job" in commands[0]
    assert "--starttime=now-10minutes" in commands[0]


def test_busy_deferral_preserves_padded_full_nonce_bound_job_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    job_name = "loom-accept-abcdef0-1-0123456789abcdef01234567"
    node = "trt-gb10-1"
    commands: list[list[str]] = []

    def run(
        argv: list[str],
        *,
        timeout: float = 30,
        check: bool = True,
        deadline: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout, check
        assert deadline == 70.0
        commands.append(argv)
        stdout = f"123|{job_name:<128}|CANCELLED by 995|Unknown|{node:<128}\n"
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(authority, "_run", run)

    assert authority._allocation_never_started(job_name, node=node, deadline=70.0)
    assert "--format=JobIDRaw,JobName%128,State,Start,NodeList%128" in commands[0]


def test_busy_accounting_polls_empty_until_full_nonce_row_is_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    job_name = "loom-accept-abcdef0-1-0123456789abcdef01234567"
    node = "trt-gb10-1"
    observations = iter(
        (
            "",
            f"123|{job_name:<128}|CANCELLED by 995|Unknown|{node:<128}\n",
        )
    )
    commands: list[list[str]] = []
    clock = [0.0]

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=next(observations), stderr="")

    monkeypatch.setattr(authority, "_run", run)
    monkeypatch.setattr(authority.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        authority.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    assert authority._allocation_never_started(job_name, node=node, deadline=1.0)
    assert len(commands) == 2
    assert clock[0] <= 1.0


@pytest.mark.parametrize(
    ("persisted_busy_job_id", "expected_deferred"),
    (("456", True), ("123", False), (None, False)),
)
def test_busy_deferral_requires_the_exact_persisted_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    persisted_busy_job_id: str | None,
    expected_deferred: bool,
) -> None:
    authority = _load_authority()
    authority.ROOT_UID = os.geteuid()
    authority.ROOT_GID = os.getegid()
    candidate_sha = "a" * 40
    first_node = "trt-gb10-1"
    busy_node = "trt-gb10-3"
    monkeypatch.setattr(authority, "SLURM_NODES", (first_node, busy_node))
    state_root = tmp_path / "jobs"
    state_root.mkdir(mode=0o700)
    state_path = state_root / "active.json"
    busy_job_name = ""
    accounting_commands: list[list[str]] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[:3] == ["/usr/bin/scontrol", "show", "node"]:
            node = argv[3]
            allocation = "0" if node == first_node else "20"
            state = "IDLE" if node == first_node else "ALLOCATED"
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=(
                    f"NodeName={node} CPUAlloc={allocation} AllocMem=0 State={state} "
                    "Partitions=gb10,loom-staging "
                ),
                stderr="",
            )
        if "/usr/bin/sacct" in argv:
            accounting_commands.append(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=f"456|{busy_job_name}|CANCELLED by 995|None|None assigned\n",
                stderr="",
            )
        raise AssertionError(argv)

    def launch(
        argv: list[str],
        *,
        job_name: str,
        state_path: Path,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal busy_job_name
        is_busy = f"--nodelist={busy_node}" in argv
        job_id = persisted_busy_job_id if is_busy else "101"
        authority._write_active_job_state(state_path, job_name=job_name, job_id=job_id)
        if is_busy:
            busy_job_name = job_name
            return subprocess.CompletedProcess(
                argv,
                1,
                stdout="",
                stderr="srun: Requested nodes are busy\n",
            )
        payload = {
            "candidate_sha": candidate_sha,
            "node": first_node,
            "trial_cache_registry": _node_probe_registry_payload(authority),
        }
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=authority.ALLOCATION_START_MARKER + "\n" + json.dumps(payload) + "\n",
            stderr="",
        )

    monkeypatch.setattr(authority, "_run", run)
    monkeypatch.setattr(authority, "_run_allocation_launcher", launch)
    monkeypatch.setattr(
        authority,
        "_cleanup_persisted_probe_job",
        lambda _job_name, *, state_path, **_kwargs: authority._clear_active_job_state(state_path),
    )
    verified = authority.VerifiedCandidateInputs(
        candidate_tree="b" * 40,
        registry_probe_sha256="c" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
        worker_inputs={"head": candidate_sha, "env_sha256": "d" * 64},
    )

    def probe() -> tuple[list[str], list[str]]:
        return authority._probe_nodes(
            candidate_sha,
            verified,
            {
                "repo_dir": Path("/worker/repo"),
                "env_file": Path("/worker/env"),
                "image_tag": "staging-aaaaaaa",
                "requested_concurrency": 10,
            },
            job_state_path=state_path,
            request_nonce="0123456789abcdef01234567",
        )

    if expected_deferred:
        passed, deferred = probe()
        assert passed == [first_node]
        assert deferred == [busy_node]
        assert len(accounting_commands) == 1
        assert "--jobs=456" in accounting_commands[0]
    else:
        with pytest.raises(
            authority.AcceptanceError, match=f"node allocation failed safely: {busy_node}"
        ):
            probe()


@pytest.mark.parametrize(
    "stdout",
    (
        "malformed\n",
        "123|loom-job|CANCELLED|Unknown|trt-gb10-1\n456|loom-job|CANCELLED|Unknown|trt-gb10-1\n",
    ),
)
def test_busy_accounting_rejects_nonempty_malformed_or_ambiguous_rows_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str,
) -> None:
    authority = _load_authority()
    commands: list[list[str]] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(authority, "_run", run)

    assert not authority._allocation_never_started(
        "loom-job",
        node="trt-gb10-1",
        deadline=time.monotonic() + 1.0,
    )
    assert len(commands) == 1


def test_started_allocation_with_busy_phrase_is_a_capacity_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    monkeypatch.setattr(authority, "SLURM_NODES", ("trt-gb10-1",))
    commands: list[list[str]] = []

    def run(
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
                    "NodeName=trt-gb10-1 CPUAlloc=20 AllocMem=115000 State=ALLOCATED "
                    "Partitions=gb10,loom-staging "
                ),
                stderr="",
            )
        if "/usr/bin/srun" in argv:
            return subprocess.CompletedProcess(
                argv,
                1,
                stdout=authority.ALLOCATION_START_MARKER + "\n",
                stderr="srun: Requested nodes are busy\n",
            )
        if "/usr/bin/squeue" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        raise AssertionError(argv)

    monkeypatch.setattr(authority, "_run", run)
    verified = authority.VerifiedCandidateInputs(
        candidate_tree="b" * 40,
        registry_probe_sha256="c" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
        worker_inputs={"head": candidate_sha, "env_sha256": "d" * 64},
    )

    with pytest.raises(authority.AcceptanceError, match="node allocation failed safely"):
        authority._probe_nodes(
            candidate_sha,
            verified,
            {
                "repo_dir": Path("/worker/repo"),
                "env_file": Path("/worker/env"),
                "image_tag": "staging-aaaaaaa",
                "requested_concurrency": 10,
            },
        )

    assert not any("/usr/bin/sacct" in command for command in commands)


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


def test_exact_job_cleanup_defers_sigterm_through_empty_readback(tmp_path: Path) -> None:
    job_state = tmp_path / "active-job.json"
    cleanup_log = tmp_path / "cleanup.log"
    script = r"""
import importlib.util
import os
import pathlib
import signal
import subprocess
import sys

authority_path, state_path, log_path = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("cleanup_signal_authority", authority_path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
authority.ROOT_UID = os.geteuid()
authority.ROOT_GID = os.getegid()
events = []


def run(argv, *, timeout=30, check=True, deadline=None):
    del timeout, check, deadline
    if "/usr/bin/scancel" in argv:
        events.append("scancel:123")
        os.kill(os.getpid(), signal.SIGTERM)
        return subprocess.CompletedProcess(argv, 0, "", "")
    if "/usr/bin/squeue" in argv:
        events.append("squeue-empty")
        return subprocess.CompletedProcess(argv, 0, "", "")
    raise AssertionError(argv)


authority._run = run
authority._install_signal_handlers()
authority._write_active_job_state(state_path, job_name="loom-test-job", job_id="123")
try:
    authority._cleanup_exact_probe_job("123", state_path=state_path, deadline=None)
except authority.AuthorityInterruptedError:
    log_path.write_text("\n".join(events) + "\n")
    raise SystemExit(42)
raise SystemExit(3)
"""
    process = subprocess.run(
        [sys.executable, "-c", script, str(AUTHORITY), str(job_state), str(cleanup_log)],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert process.returncode == 42, (process.stdout, process.stderr)
    assert cleanup_log.read_text(encoding="utf-8").splitlines() == [
        "scancel:123",
        "squeue-empty",
        "squeue-empty",
    ]
    assert not job_state.exists()


def test_exact_job_scancel_timeout_still_requires_empty_readback(tmp_path: Path) -> None:
    authority = _load_authority()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    state_path = state_root / "active-job.json"
    authority.ROOT_UID = os.geteuid()
    authority.ROOT_GID = os.getegid()
    authority._write_active_job_state(state_path, job_name="loom-cancel-timeout", job_id="123")
    events: list[str] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if "/usr/bin/scancel" in argv:
            events.append("scancel-timeout")
            raise authority.AcceptanceError("command timed out safely: scancel")
        if "/usr/bin/squeue" in argv:
            events.append("squeue-empty")
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    authority._run = run

    authority._cleanup_exact_probe_job("123", state_path=state_path, deadline=None)

    assert events == ["scancel-timeout", "squeue-empty", "squeue-empty"]
    assert not state_path.exists()


def test_exact_job_cleanup_polls_until_two_consecutive_empty_reads(tmp_path: Path) -> None:
    authority = _load_authority()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    state_path = state_root / "active-job.json"
    job_name = "loom-accept-abcdef0-1-0123456789abcdef01234567"
    authority.ROOT_UID = os.geteuid()
    authority.ROOT_GID = os.getegid()
    authority._write_active_job_state(state_path, job_name=job_name, job_id="123")
    observations = iter((f"123|{job_name}\n", "", ""))
    events: list[str] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if "/usr/bin/scancel" in argv:
            events.append("scancel-123")
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv:
            events.append("squeue")
            return subprocess.CompletedProcess(argv, 0, next(observations), "")
        raise AssertionError(argv)

    authority._run = run

    authority._cleanup_exact_probe_job(
        "123",
        state_path=state_path,
        deadline=time.monotonic() + 1.0,
    )

    assert events == ["scancel-123", "squeue", "squeue", "squeue"]
    assert not state_path.exists()


def test_exact_job_cleanup_fails_when_job_persists_until_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    state_path = state_root / "active-job.json"
    job_name = "loom-accept-abcdef0-1-0123456789abcdef01234567"
    authority.ROOT_UID = os.geteuid()
    authority.ROOT_GID = os.getegid()
    authority._write_active_job_state(state_path, job_name=job_name, job_id="123")
    clock = [0.0]
    name_queries = 0

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal name_queries
        if "/usr/bin/scancel" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv:
            name_queries += 1
            return subprocess.CompletedProcess(argv, 0, f"123|{job_name}\n", "")
        raise AssertionError(argv)

    authority._run = run
    monkeypatch.setattr(authority.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        authority.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    with pytest.raises(authority.AcceptanceError, match="did not converge"):
        authority._cleanup_exact_probe_job("123", state_path=state_path, deadline=0.11)

    assert name_queries >= 2
    assert clock[0] <= 0.11
    assert state_path.exists()


def test_pre_id_cleanup_waits_for_quiescent_empty_before_exact_cancel(
    tmp_path: Path,
) -> None:
    authority = _load_authority()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    state_path = state_root / "active-job.json"
    authority.ROOT_UID = os.geteuid()
    authority.ROOT_GID = os.getegid()
    authority._write_active_job_state(state_path, job_name="loom-late-job")
    events: list[str] = []
    name_queries = 0

    def observe(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal name_queries
        name_queries += 1
        events.append(f"name-query-{name_queries}")
        stdout = "" if name_queries == 1 else "123|loom-late-job\n"
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if "/usr/bin/scancel" in argv:
            events.append("scancel-123")
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "/usr/bin/squeue" in argv:
            events.append("exact-readback-empty")
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    authority._run_job_observer = observe
    authority._run = run

    authority._cleanup_persisted_probe_job(
        "loom-late-job",
        state_path=state_path,
        deadline=None,
    )

    assert events == [
        "name-query-1",
        "name-query-2",
        "scancel-123",
        "exact-readback-empty",
        "exact-readback-empty",
    ]
    assert not state_path.exists()


def test_launcher_persists_exact_id_before_wait_and_termination(tmp_path: Path) -> None:
    job_state = tmp_path / "active-job.json"
    launcher_started = tmp_path / "launcher-started"
    discovery_log = tmp_path / "discovery.log"
    script = r"""
import importlib.util
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

authority_path, state_path, started_path, discovery_path = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("launcher_state_authority", authority_path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
authority.ROOT_UID = os.geteuid()
authority.ROOT_GID = os.getegid()
authority._install_signal_handlers()


def observe(argv, *, timeout=30, check=True, deadline=None):
    del timeout, check, deadline
    assert "/usr/bin/squeue" in argv
    deadline = time.monotonic() + 2
    while not started_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert started_path.exists()
    assert json.loads(state_path.read_text()) == {
        "job_name": "loom-test-job",
        "schema_version": 1,
    }
    discovery_path.write_text("unique-name-query-before-wait\n")
    deadline = time.monotonic() + 5
    while authority._DEFERRED_SIGNAL != signal.SIGTERM and time.monotonic() < deadline:
        time.sleep(0.01)
    assert authority._DEFERRED_SIGNAL == signal.SIGTERM
    return subprocess.CompletedProcess(argv, 0, "123|loom-test-job\n", "")


authority._run_job_observer = observe
launcher = (
    "import pathlib,sys,time; "
    "pathlib.Path(sys.argv[1]).write_text('started'); "
    "time.sleep(30)"
)
try:
    authority._run_allocation_launcher(
        [sys.executable, "-c", launcher, str(started_path)],
        job_name="loom-test-job",
        state_path=state_path,
        timeout=30,
        deadline=None,
    )
except authority.AuthorityInterruptedError:
    raise SystemExit(42)
raise SystemExit(3)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(AUTHORITY),
            str(job_state),
            str(launcher_started),
            str(discovery_log),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while (
            (not job_state.exists() or not launcher_started.exists() or not discovery_log.exists())
            and process.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert job_state.exists() and launcher_started.exists() and discovery_log.exists(), (
            process.communicate(timeout=1)
        )
        assert process.poll() is None
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=15)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()

    assert process.returncode == 42, (stdout, stderr)
    assert discovery_log.read_text(encoding="utf-8") == "unique-name-query-before-wait\n"
    assert json.loads(job_state.read_text(encoding="utf-8")) == {
        "job_id": "123",
        "job_name": "loom-test-job",
        "schema_version": 1,
    }
    metadata = job_state.stat()
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert (metadata.st_uid, metadata.st_gid) == (os.geteuid(), os.getegid())


def test_launcher_preserves_immediate_busy_result_without_exact_job_row(tmp_path: Path) -> None:
    authority = _load_authority()
    monkeypatch_state_root = tmp_path / "state"
    monkeypatch_state_root.mkdir(mode=0o700)
    authority.ROOT_UID = os.geteuid()
    authority.ROOT_GID = os.getegid()
    authority._run_job_observer = lambda argv, **kwargs: subprocess.CompletedProcess(
        argv,
        0,
        "",
        "",
    )
    result = authority._run_allocation_launcher(
        [
            sys.executable,
            "-c",
            "import sys; print('srun: Requested nodes are busy', file=sys.stderr); sys.exit(1)",
        ],
        job_name="loom-test-busy",
        state_path=monkeypatch_state_root / "active.json",
        timeout=5,
        deadline=None,
    )

    assert result.returncode == 1
    assert "Requested nodes are busy" in result.stderr
    assert json.loads((monkeypatch_state_root / "active.json").read_text(encoding="utf-8")) == {
        "job_name": "loom-test-busy",
        "schema_version": 1,
    }


def test_probe_job_name_is_bound_to_broker_request_nonce(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    request_nonce = "0123456789abcdef01234567"
    monkeypatch.setattr(authority, "SLURM_NODES", ("trt-gb10-1",))
    launched_names: list[str] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        assert argv[:4] == ["/usr/bin/scontrol", "show", "node", "trt-gb10-1"]
        return subprocess.CompletedProcess(
            argv,
            0,
            "NodeName=trt-gb10-1 CPUAlloc=0 AllocMem=0 State=IDLE Partitions=gb10,loom-staging ",
            "",
        )

    def launch(
        argv: list[str],
        *,
        job_name: str,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        launched_names.append(job_name)
        payload = {
            "candidate_sha": candidate_sha,
            "node": "trt-gb10-1",
            "trial_cache_registry": _node_probe_registry_payload(authority),
        }
        return subprocess.CompletedProcess(
            argv,
            0,
            authority.ALLOCATION_START_MARKER + "\n" + json.dumps(payload) + "\n",
            "",
        )

    monkeypatch.setattr(authority, "_run", run)
    monkeypatch.setattr(authority, "_run_allocation_launcher", launch)
    monkeypatch.setattr(authority, "_cleanup_persisted_probe_job", lambda *_args, **_kwargs: None)
    verified = authority.VerifiedCandidateInputs(
        candidate_tree="b" * 40,
        registry_probe_sha256="c" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
        worker_inputs={"head": candidate_sha, "env_sha256": "d" * 64},
    )

    passed, deferred = authority._probe_nodes(
        candidate_sha,
        verified,
        {
            "repo_dir": Path("/worker/repo"),
            "env_file": Path("/worker/env"),
            "image_tag": "staging-aaaaaaa",
            "requested_concurrency": 10,
        },
        job_state_path=tmp_path / "active.json",
        request_nonce=request_nonce,
    )

    assert passed == ["trt-gb10-1"]
    assert deferred == []
    assert launched_names == [f"loom-accept-aaaaaaa-1-{request_nonce}"]


def test_authority_fixed_paths_resist_path_interception(tmp_path: Path) -> None:
    attacker = tmp_path / "bin"
    attacker.mkdir()
    marker = tmp_path / "intercepted"
    for name in ("python3", "docker", "srun", "squeue", "scancel", "runuser"):
        executable = attacker / name
        executable.write_text(
            f"#!/bin/sh\nprintf intercepted > {marker}\nexit 77\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)
    script = f"""
import importlib.util
import json
path = {str(AUTHORITY)!r}
spec = importlib.util.spec_from_file_location("fixed_path_authority", path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
result = authority._run([authority.SYSTEM_PYTHON, "-c", "print('fixed')"])
print(json.dumps({{"docker": authority.DOCKER, "stdout": result.stdout.strip()}}))
"""
    result = subprocess.run(
        ["/usr/bin/python3", "-c", script],
        capture_output=True,
        text=True,
        env={
            "HOME": str(tmp_path),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": str(attacker),
        },
        timeout=10,
    )

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {"docker": "/usr/bin/docker", "stdout": "fixed"}
    assert not marker.exists()


def test_authority_signal_kills_child_group_and_cleans_only_current_job(
    tmp_path: Path,
) -> None:
    started_marker = tmp_path / "launcher-started"
    escaped_marker = tmp_path / "escaped-child"
    cleanup_log = tmp_path / "cleanup.log"
    script = r"""
import importlib.util
import pathlib
import subprocess
import sys

authority_path, started_path, escaped_path, cleanup_path = sys.argv[1:]
spec = importlib.util.spec_from_file_location("signal_authority", authority_path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
authority.SLURM_NODES = ("trt-gb10-1",)
real_run = authority._run
queue_reads = 0


def append_log(value):
    with open(cleanup_path, "a", encoding="utf-8") as stream:
        stream.write(value + "\n")


def run(argv, *, timeout=30, check=True, deadline=None):
    global queue_reads
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
    if "/usr/bin/srun" in argv:
        descendant = (
            "import pathlib,sys,time; time.sleep(0.4); "
            "pathlib.Path(sys.argv[1]).write_text('escaped')"
        )
        launcher = (
            "import pathlib,subprocess,sys,time; "
            "pathlib.Path(sys.argv[1]).write_text('started'); "
            f"subprocess.Popen([sys.executable, '-c', {descendant!r}, sys.argv[2]]); "
            "time.sleep(30)"
        )
        return real_run(
            [sys.executable, "-c", launcher, started_path, escaped_path],
            timeout=timeout,
            check=check,
            deadline=deadline,
        )
    if "/usr/bin/squeue" in argv:
        queue_reads += 1
        append_log("squeue")
        job_name = next(
            item.removeprefix("--name=") for item in argv if item.startswith("--name=")
        )
        stdout = f"123|{job_name}\n" if queue_reads == 1 else ""
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")
    if "/usr/bin/scancel" in argv:
        append_log("scancel:" + ",".join(argv[argv.index("/usr/bin/scancel") + 1 :]))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
    raise AssertionError(argv)


authority._run = run
authority._install_signal_handlers()
verified = authority.VerifiedCandidateInputs(
    candidate_tree="b" * 40,
    registry_probe_sha256="c" * 64,
    registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
    worker_inputs={"head": "a" * 40, "env_sha256": "d" * 64},
)
try:
    authority._probe_nodes(
        "a" * 40,
        verified,
        {
            "repo_dir": pathlib.Path("/worker/repo"),
            "env_file": pathlib.Path("/worker/env"),
            "image_tag": "staging-aaaaaaa",
            "requested_concurrency": 10,
        },
    )
except authority.AcceptanceError as exc:
    if "interrupted safely" not in str(exc):
        raise
    raise SystemExit(42)
raise SystemExit(3)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(AUTHORITY),
            str(started_marker),
            str(escaped_marker),
            str(cleanup_log),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while (
            not started_marker.exists() and process.poll() is None and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert started_marker.exists(), process.communicate(timeout=1)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()

    assert process.returncode == 42, (stdout, stderr)
    time.sleep(0.5)
    assert not escaped_marker.exists()
    assert cleanup_log.read_text(encoding="utf-8").splitlines() == [
        "squeue",
        "scancel:123",
        "squeue",
    ]


def test_overall_timeout_cancels_and_verifies_current_job_before_next_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    candidate_sha = "a" * 40
    verified = SimpleNamespace(
        candidate_tree="b" * 40,
        registry_probe_sha256="c" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
        worker_inputs={"head": candidate_sha, "env_sha256": "d" * 64},
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
        if "/usr/bin/srun" in argv:
            assert deadline == 10.0
            raise authority.AcceptanceError("authority overall time budget exhausted safely")
        if "/usr/bin/squeue" in argv:
            assert deadline == 70.0
            queue_reads += 1
            job_name = next(
                item.removeprefix("--name=") for item in argv if item.startswith("--name=")
            )
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
            {
                "repo_dir": Path("/worker/repo"),
                "env_file": Path("/worker/env"),
                "image_tag": "staging-aaaaaaa",
                "requested_concurrency": 10,
            },
            work_deadline=10.0,
            cleanup_deadline=70.0,
        )

    assert sum("/usr/bin/srun" in command for command in commands) == 1
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
        if "/usr/bin/srun" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=authority.ALLOCATION_START_MARKER
                + "\n"
                + json.dumps(
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
        worker_inputs={"head": candidate_sha, "env_sha256": "d" * 64},
    )
    passed, deferred = authority._probe_nodes(
        candidate_sha,
        verified,
        {
            "repo_dir": repo,
            "env_file": env_file,
            "image_tag": "staging-aaaaaaa",
            "requested_concurrency": 10,
        },
    )

    assert passed == ["trt-gb10-1"]
    assert deferred == []
    srun = next(command for command in commands if "/usr/bin/srun" in command)
    assert str(repo / "scripts/ops/staging_trial_cache_registry_node_probe.py") in srun
    assert str(repo / "deploy/worker-pools/trial-cache/staging-ca.crt") in srun
    assert authority.TRIAL_CACHE_REGISTRY_REPO in srun
    assert authority.TRIAL_CACHE_CA_SHA256 in srun
    assert authority.TRIAL_CACHE_CANARY_DIGEST in srun
    assert verified.candidate_tree in srun
    assert verified.registry_probe_sha256 in srun
    assert json.dumps(verified.worker_inputs, sort_keys=True, separators=(",", ":")) in srun
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
    monkeypatch.setattr(authority, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(authority, "ROOT_GID", os.getegid())
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


def test_authority_rejects_real_evidence_directory_ownership_drift(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir(mode=0o755)
    script = r"""
import importlib.util
import os
import pathlib
import sys

authority_path, evidence_path = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("ownership_authority", authority_path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
authority.ROOT_UID = os.geteuid() + 1
authority.ROOT_GID = os.getegid() + 1
try:
    authority._validate_root_directory(evidence_path, mode=0o755, label="authority evidence root")
except authority.AcceptanceError as exc:
    if "metadata" not in str(exc):
        raise
    raise SystemExit(42)
raise SystemExit(3)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(AUTHORITY), str(evidence_root)],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 42, (result.stdout, result.stderr)


def test_authority_rejects_real_published_artifact_ownership_drift(tmp_path: Path) -> None:
    artifact = tmp_path / "evidence.json"
    expected = b'{"kind":"loom_gb10_slurm_acceptance"}\n'
    artifact.write_bytes(expected)
    artifact.chmod(0o644)
    script = r"""
import importlib.util
import os
import pathlib
import sys

authority_path, artifact_path = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("artifact_ownership_authority", authority_path)
assert spec is not None and spec.loader is not None
authority = importlib.util.module_from_spec(spec)
spec.loader.exec_module(authority)
authority.ROOT_UID = os.geteuid() + 1
authority.ROOT_GID = os.getegid() + 1
try:
    authority._read_published_artifact(
        artifact_path,
        b'{"kind":"loom_gb10_slurm_acceptance"}\n',
    )
except authority.AcceptanceError as exc:
    if "ownership" not in str(exc) and "collision" not in str(exc):
        raise
    raise SystemExit(42)
raise SystemExit(3)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(AUTHORITY), str(artifact)],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 42, (result.stdout, result.stderr)


def test_authority_publishes_digest_addressed_evidence_before_current_and_fsyncs_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    state_root = tmp_path / "authority-state"
    artifact_path = state_root / "current.json"
    monkeypatch.setattr(authority, "STATE_ROOT", state_root)
    monkeypatch.setattr(authority, "ARTIFACT_PATH", artifact_path)
    monkeypatch.setattr(authority, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(authority, "ROOT_GID", os.getegid())
    real_fsync = os.fsync
    synced: list[str] = []

    def record_fsync(descriptor: int) -> None:
        synced.append(os.readlink(f"/proc/self/fd/{descriptor}"))
        real_fsync(descriptor)

    monkeypatch.setattr(authority.os, "fsync", record_fsync)
    payload = {"kind": "loom_gb10_slurm_acceptance", "result": "pass"}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    immutable = state_root / "evidence" / f"{digest}.json"

    published = authority._write_artifact(payload)

    assert published == immutable
    assert immutable.read_text(encoding="utf-8") == canonical + "\n"
    assert artifact_path.read_bytes() == immutable.read_bytes()
    assert synced.index(str(immutable)) < synced.index(str(artifact_path))
    assert str(immutable.parent) in synced
    assert str(state_root) in synced


def test_authority_artifact_publication_is_idempotent_and_rejects_digest_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _load_authority()
    state_root = tmp_path / "authority-state"
    artifact_path = state_root / "current.json"
    monkeypatch.setattr(authority, "STATE_ROOT", state_root)
    monkeypatch.setattr(authority, "ARTIFACT_PATH", artifact_path)
    monkeypatch.setattr(authority, "ROOT_UID", os.geteuid())
    monkeypatch.setattr(authority, "ROOT_GID", os.getegid())
    payload = {"kind": "loom_gb10_slurm_acceptance", "result": "pass"}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    immutable = state_root / "evidence" / f"{digest}.json"

    first = authority._write_artifact(payload)
    first_inode = immutable.stat().st_ino
    second = authority._write_artifact(payload)

    assert first == second == immutable
    assert immutable.stat().st_ino == first_inode
    immutable.write_text("collision\n", encoding="utf-8")
    with pytest.raises(authority.AcceptanceError, match="collision"):
        authority._write_artifact(payload)


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
                job_state_path=(
                    authority.JOB_STATE_ROOT
                    / "loom-gb10-capacity-0123456789abcdef01234567.service.json"
                ),
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
            "image_tag": "staging-aaaaaaa",
            "requested_concurrency": 10,
        },
    )
    verified = authority.VerifiedCandidateInputs(
        candidate_tree=candidate_tree,
        registry_probe_sha256="d" * 64,
        registry_ca_sha256=authority.TRIAL_CACHE_CA_SHA256,
        worker_inputs={"head": candidate_sha, "env_sha256": "e" * 64},
    )
    monkeypatch.setattr(
        authority,
        "_verify_inputs",
        lambda _sha, _contract, *, deadline: verified,
    )
    monkeypatch.setattr(
        authority,
        "_probe_nodes",
        lambda _sha, _verified, _contract, *, work_deadline, cleanup_deadline, job_state_path, request_nonce: (
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
    assert 'RUNTIME_ROOT="/run/loom-gb10-slurm-authority"' in source
    assert 'TMPFILES_PATH="/etc/tmpfiles.d/loom-gb10-slurm-authority.conf"' in source
    assert "install -o root -g root -m 0755" in source
    assert "install -o root -g root -m 0644" in source
    assert "install -d -o root -g root -m 0755" in source
    assert '/usr/bin/systemd-tmpfiles --create "$TMPFILES_PATH"' in source
    assert '"root:root:700:directory"' in source
    assert "/home/qianyi" not in source


def test_acceptance_tmpfiles_provisions_strict_volatile_job_state() -> None:
    assert TMPFILES.read_text(encoding="utf-8").splitlines() == [
        "d /run/loom-gb10-slurm-authority 0700 root root -",
        "d /run/loom-gb10-slurm-authority/jobs 0700 root root -",
        "f /run/loom-gb10-slurm-authority/acceptance.lock 0600 root root -",
    ]
