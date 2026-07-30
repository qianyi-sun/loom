from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_slurm_policy as policy
from scripts.ops import slurm_job_cgroup_guard as guard


def _binding(
    account: str,
    sandbox: str,
    service_user: str,
    candidate_sha: str,
    candidate_tree: str,
) -> guard.CandidateBinding:
    identity = hashlib.sha256(f"{account}:{sandbox}".encode("ascii")).hexdigest()
    return guard.CandidateBinding(
        account=account,
        env_id=f"denv-{identity[:8]}",
        resource_generation=1,
        sandbox=sandbox,
        service_user=service_user,
        slurm_qos=account.replace("loom-dev-", "loom-dev-").replace("lda-", "ldq-"),
        candidate_id=f"cand-{identity[:40]}",
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )


def _candidate_set_sha256(
    bindings: dict[str, guard.CandidateBinding],
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                account: {
                    "env_id": binding.env_id,
                    "resource_generation": binding.resource_generation,
                    "sandbox": binding.sandbox,
                    "service_user": binding.service_user,
                    "slurm_qos": binding.slurm_qos,
                    "candidate_id": binding.candidate_id,
                    "candidate_sha": binding.candidate_sha,
                    "candidate_tree": binding.candidate_tree,
                }
                for account, binding in bindings.items()
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii"),
    ).hexdigest()


def _config(
    bindings: dict[str, guard.CandidateBinding] | None = None,
) -> guard.GuardConfig:
    configured_bindings = (
        bindings
        if bindings is not None
        else {
            "loom-dev-qianyi": _binding(
                "loom-dev-qianyi",
                "qianyi",
                "loom-sandbox-qianyi",
                "a" * 40,
                "b" * 40,
            ),
            "loom-dev-hongjian": _binding(
                "loom-dev-hongjian",
                "hongjian",
                "loom-sandbox-hongjian",
                "c" * 40,
                "b" * 40,
            ),
            "loom-dev-devansh": _binding(
                "loom-dev-devansh",
                "devansh",
                "loom-sandbox-devansh",
                "d" * 40,
                "b" * 40,
            ),
        }
    )
    return guard.GuardConfig(
        cluster="trt-oldlab",
        controller="oldlab-1",
        submit_host="oldlab-2",
        allowed_nodes=frozenset({"oldlab-1"}),
        candidate_bindings=configured_bindings,
        candidate_set_sha256=_candidate_set_sha256(configured_bindings),
        config_sha256="b" * 64,
        pids_max=32768,
        poll_interval_seconds=0.2,
        require_gpu_probe=False,
        docker_cgroup_driver="cgroupfs",
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
    (job / "memory.swap.max").write_text("max\n")
    (job / "pids.max").write_text("max\n")
    (job / "cpuset.cpus.effective").write_text("0-1\n")
    (job / "cpuset.mems.effective").write_text("0\n")
    return root, job


def _config_payload(
    config: guard.GuardConfig | None = None,
) -> dict[str, object]:
    config = config or _config()
    bindings = {
        account: {
            "env_id": binding.env_id,
            "resource_generation": binding.resource_generation,
            "sandbox": binding.sandbox,
            "service_user": binding.service_user,
            "slurm_qos": binding.slurm_qos,
            "candidate_id": binding.candidate_id,
            "candidate_sha": binding.candidate_sha,
            "candidate_tree": binding.candidate_tree,
        }
        for account, binding in config.candidate_bindings.items()
    }
    return {
        "schema_version": 3,
        "cluster": config.cluster,
        "controller": config.controller,
        "submit_host": config.submit_host,
        "allowed_nodes": sorted(config.allowed_nodes),
        "candidate_bindings": bindings,
        "candidate_set_sha256": config.candidate_set_sha256,
        "pids_max": config.pids_max,
        "poll_interval_seconds": config.poll_interval_seconds,
        "require_gpu_probe": config.require_gpu_probe,
        "docker_cgroup_driver": config.docker_cgroup_driver,
    }


def _write_config(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n")
    path.chmod(0o600)


def _refresh_candidate_set_digest(payload: dict[str, object]) -> None:
    bindings = payload["candidate_bindings"]
    assert isinstance(bindings, dict)
    payload["candidate_set_sha256"] = hashlib.sha256(
        json.dumps(bindings, sort_keys=True, separators=(",", ":")).encode("ascii"),
    ).hexdigest()


def test_dynamic_registry_bindings_accept_custom_accounts_and_service_users(
    tmp_path: Path,
) -> None:
    bindings = dict(_config().candidate_bindings)
    bindings.update(
        {
            "lda-a1b2c3d4": _binding(
                "lda-a1b2c3d4",
                "research-4",
                "lds-a1b2c3d4",
                "e" * 40,
                "f" * 40,
            ),
            "lda-e5f6a7b8": _binding(
                "lda-e5f6a7b8",
                "team-27",
                "lds-e5f6a7b8",
                "f" * 40,
                "e" * 40,
            ),
        },
    )
    path = tmp_path / "guard.json"
    _write_config(path, _config_payload(_config(bindings)))

    loaded = guard.load_config(path)

    assert len(loaded.candidate_bindings) == 5
    assert loaded.candidate_bindings["lda-a1b2c3d4"] == bindings["lda-a1b2c3d4"]
    root, job = _job(tmp_path)
    result = guard.scan_once(
        loaded,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="lda-a1b2c3d4",
            comment="loom-cgroup-v1:pids=32768",
            job_name="loom-sandbox-research-4-eeeeeeeeeeee-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
            user="lds-a1b2c3d4",
        ),
    )
    assert result["verified"] == 1
    assert result["resource_probes"]["lda-a1b2c3d4"]["service_user"] == "lds-a1b2c3d4"
    assert (job / "pids.max").read_text() == "32768\n"


def _staging_binding() -> guard.CandidateBinding:
    return guard.CandidateBinding(
        account="loom-staging",
        env_id="denv-staging-" + "a" * 40,
        resource_generation=7,
        sandbox="staging",
        service_user="loom-staging-worker",
        slurm_qos="loom-staging",
        candidate_id="cand-" + "a" * 40,
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("env_id", "denv-staging-wrong"),
        ("sandbox", "staging-other"),
        ("service_user", "loom-staging-worker-other"),
        ("slurm_qos", "loom-staging-other"),
        ("candidate_id", "cand-" + "c" * 40),
    ),
)
def test_guard_rejects_noncanonical_fixed_staging_binding(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    staging = replace(_staging_binding(), **{field: value})
    config = _config({"loom-staging": staging})
    path = tmp_path / "guard.json"
    _write_config(path, _config_payload(config))

    with pytest.raises(guard.GuardError, match="fixed staging"):
        guard.load_config(path)


@pytest.mark.parametrize("mutation", ("generation", "digest", "regular-name", "user"))
def test_fixed_staging_job_name_is_candidate_set_and_generation_bound(
    tmp_path: Path,
    mutation: str,
) -> None:
    staging = _staging_binding()
    config = replace(
        _config({"loom-staging": staging}),
        cluster="trt-gb10",
        controller="trt-gb10-1",
        submit_host="trt-gb10-1",
        allowed_nodes=frozenset({"trt-gb10-2"}),
    )
    root, _job_path = _job(tmp_path)
    job_name = (
        f"loom827-staging-{staging.candidate_sha[:12]}-trt-gb10-2-"
        f"g{config.candidate_set_sha256}-a{staging.resource_generation}"
    )
    user = staging.service_user
    if mutation == "generation":
        job_name = job_name.rsplit("-a", 1)[0] + "-a8"
    elif mutation == "digest":
        job_name = job_name.replace(config.candidate_set_sha256, "f" * 64)
    elif mutation == "regular-name":
        job_name = "loom-sandbox-staging-aaaaaaaaaaaa-trt-gb10-2"
    elif mutation == "user":
        user = "loom-staging-worker-other"

    result = guard.scan_once(
        config,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-staging",
            comment="loom-cgroup-v1:pids=32768",
            job_name=job_name,
            batch_host="trt-gb10-2",
            node_list="trt-gb10-2",
            user=user,
            start_time="2026-07-29T12:00:00",
        ),
    )

    assert result["failed"] == 1
    assert result["verified"] == 0


def test_fixed_staging_broker_job_name_is_request_comment_bound(tmp_path: Path) -> None:
    staging = _staging_binding()
    config = replace(
        _config({"loom-staging": staging}),
        cluster="trt-gb10",
        controller="trt-gb10-1",
        submit_host="trt-gb10-1",
        allowed_nodes=frozenset({"trt-gb10-2"}),
    )
    request_id = "6" * 64
    identity = hashlib.sha256(
        (
            f"{staging.candidate_sha}|trt-gb10-2|{config.candidate_set_sha256}|"
            f"{staging.resource_generation}|{request_id}"
        ).encode("ascii"),
    ).hexdigest()
    root, _job_path = _job(tmp_path)

    result = guard.scan_once(
        config,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-staging",
            comment=f"loom-cgroup-v1:pids=32768:r={request_id}",
            job_name=f"loom827-staging-aaaaaaaaaaaa-trt-gb10-2-x{identity}",
            batch_host="trt-gb10-2",
            node_list="trt-gb10-2",
            user=staging.service_user,
            start_time="2026-07-30T12:00:00",
        ),
    )

    assert result["verified"] == 1
    assert result["failed"] == 0


@pytest.mark.parametrize("mutation", ("missing-request", "wrong-request", "wrong-name"))
def test_fixed_staging_broker_job_rejects_request_identity_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    staging = _staging_binding()
    config = replace(
        _config({"loom-staging": staging}),
        cluster="trt-gb10",
        controller="trt-gb10-1",
        submit_host="trt-gb10-1",
        allowed_nodes=frozenset({"trt-gb10-2"}),
    )
    request_id = "6" * 64
    identity = hashlib.sha256(
        (
            f"{staging.candidate_sha}|trt-gb10-2|{config.candidate_set_sha256}|"
            f"{staging.resource_generation}|{request_id}"
        ).encode("ascii"),
    ).hexdigest()
    comment = f"loom-cgroup-v1:pids=32768:r={request_id}"
    job_name = f"loom827-staging-aaaaaaaaaaaa-trt-gb10-2-x{identity}"
    if mutation == "missing-request":
        comment = "loom-cgroup-v1:pids=32768"
    elif mutation == "wrong-request":
        comment = f"loom-cgroup-v1:pids=32768:r={'7' * 64}"
    else:
        job_name = job_name[:-1] + ("0" if job_name[-1] != "0" else "1")
    root, _job_path = _job(tmp_path)

    result = guard.scan_once(
        config,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="loom-staging",
            comment=comment,
            job_name=job_name,
            batch_host="trt-gb10-2",
            node_list="trt-gb10-2",
            user=staging.service_user,
            start_time="2026-07-30T12:00:00",
        ),
    )

    assert result["verified"] == 0
    assert result["failed"] == 1


@pytest.mark.parametrize(
    "mutation",
    (
        "legacy",
        "hybrid",
        "empty",
        "invalid-account",
        "invalid-service-user",
        "root-service-user",
        "invalid-sandbox",
        "duplicate-sandbox",
        "duplicate-service-user",
        "invalid-sha",
        "invalid-tree",
    ),
)
def test_config_rejects_invalid_dynamic_candidate_maps(
    tmp_path: Path,
    mutation: str,
) -> None:
    payload = _config_payload()
    bindings = payload["candidate_bindings"]
    assert isinstance(bindings, dict)
    if mutation == "legacy":
        payload.pop("candidate_bindings")
        payload.pop("candidate_set_sha256")
        payload["schema_version"] = 1
        payload["candidate_sha"] = "a" * 40
        payload["allowed_accounts"] = sorted(_config().allowed_accounts)
    elif mutation == "hybrid":
        payload["candidate_sha"] = "a" * 40
    elif mutation == "empty":
        bindings.clear()
    elif mutation == "invalid-account":
        bindings["Bad Account"] = bindings.pop("loom-dev-devansh")
    elif mutation == "invalid-service-user":
        bindings["loom-dev-devansh"]["service_user"] = "Bad User"
    elif mutation == "root-service-user":
        bindings["loom-dev-devansh"]["service_user"] = "root"
    elif mutation == "invalid-sandbox":
        bindings["loom-dev-devansh"]["sandbox"] = "Devansh"
    elif mutation == "duplicate-sandbox":
        bindings["loom-dev-devansh"]["sandbox"] = "qianyi"
    elif mutation == "duplicate-service-user":
        bindings["loom-dev-devansh"]["service_user"] = "loom-sandbox-qianyi"
    elif mutation == "invalid-sha":
        bindings["loom-dev-devansh"]["candidate_sha"] = "abc"
    else:
        bindings["loom-dev-devansh"]["candidate_tree"] = "abc"
    if mutation not in {"legacy", "hybrid"}:
        _refresh_candidate_set_digest(payload)
    path = tmp_path / "guard.json"
    _write_config(path, payload)

    with pytest.raises(guard.GuardError):
        guard.load_config(path)


@pytest.mark.parametrize(
    "field",
    ("account", "sandbox", "service_user", "candidate_sha", "candidate_tree"),
)
def test_candidate_set_digest_binds_every_dynamic_binding_field(
    tmp_path: Path,
    field: str,
) -> None:
    payload = _config_payload()
    bindings = payload["candidate_bindings"]
    assert isinstance(bindings, dict)
    binding = bindings["loom-dev-qianyi"]
    if field == "account":
        bindings["lda-a1b2c3d4"] = bindings.pop("loom-dev-qianyi")
    elif field == "sandbox":
        binding["sandbox"] = "runtime-x"
    elif field == "service_user":
        binding["service_user"] = "lds-a1b2c3d4"
    elif field == "candidate_sha":
        binding["candidate_sha"] = "e" * 40
    else:
        binding["candidate_tree"] = "f" * 40
    path = tmp_path / f"{field}.json"
    _write_config(path, payload)

    with pytest.raises(guard.GuardError, match="candidate-set digest"):
        guard.load_config(path)


def test_config_rejects_duplicate_json_account_keys(tmp_path: Path) -> None:
    payload = _config_payload()
    bindings = payload["candidate_bindings"]
    assert isinstance(bindings, dict)
    serialized_bindings = json.dumps(bindings, sort_keys=True)
    duplicated_bindings = (
        serialized_bindings[:-1]
        + ',"loom-dev-qianyi":'
        + json.dumps(bindings["loom-dev-qianyi"], sort_keys=True)
        + "}"
    )
    serialized = json.dumps(payload, sort_keys=True).replace(
        serialized_bindings,
        duplicated_bindings,
        1,
    )
    path = tmp_path / "duplicate-account.json"
    path.write_text(serialized + "\n")
    path.chmod(0o600)

    with pytest.raises(guard.GuardError, match="duplicate fields"):
        guard.load_config(path)


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
            job_name="loom827-qianyi-aaaaaaaaaaaa-oldlab-1-g" + "0" * 64 + "-a1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
            user="loom-sandbox-qianyi",
        ),
    )

    assert result["scanned"] == 1
    assert result["verified"] == 1
    assert result["unrelated"] == 0
    assert result["failed"] == 0
    assert result["failures"] == []
    assert result["resource_probes"]["loom-dev-qianyi"]["pids_max"] == "32768"
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
            job_name="loom-sandbox-qianyi-bbbbbbbbbbbb-oldlab-9",
            batch_host="oldlab-9",
            node_list="oldlab-9",
            user="loom-sandbox-qianyi",
        ),
    )

    assert result["failed"] == 1
    assert result["verified"] == 0
    assert (job / "pids.max").read_text() == "max\n"


def test_cross_account_candidate_or_user_binding_fails_closed(tmp_path: Path) -> None:
    root, job = _job(tmp_path)
    config = _config(
        {
            "lda-a1b2c3d4": _binding(
                "lda-a1b2c3d4",
                "runtime-a",
                "lds-a1b2c3d4",
                "a" * 40,
                "b" * 40,
            ),
            "lda-e5f6a7b8": _binding(
                "lda-e5f6a7b8",
                "runtime-b",
                "lds-e5f6a7b8",
                "c" * 40,
                "d" * 40,
            ),
        },
    )

    for user, job_name in (
        (
            "lds-e5f6a7b8",
            "loom-sandbox-runtime-a-aaaaaaaaaaaa-oldlab-1",
        ),
        (
            "lds-a1b2c3d4",
            "loom-sandbox-runtime-b-cccccccccccc-oldlab-1",
        ),
        (
            "lds-a1b2c3d4",
            "loom-sandbox-runtime-a-cccccccccccc-oldlab-1",
        ),
    ):
        result = guard.scan_once(
            config,
            cgroup_root=root,
            job_lookup=lambda job_id, user=user, job_name=job_name: guard.JobRecord(
                job_id=job_id,
                account="lda-a1b2c3d4",
                comment="loom-cgroup-v1:pids=32768",
                job_name=job_name,
                batch_host="oldlab-1",
                node_list="oldlab-1",
                user=user,
            ),
        )
        assert result["failed"] == 1
        assert result["verified"] == 0
        assert (job / "pids.max").read_text() == "max\n"

    account_drift = replace(
        config,
        candidate_bindings={
            **config.candidate_bindings,
            "lda-a1b2c3d4": replace(
                config.candidate_bindings["lda-a1b2c3d4"],
                account="lda-e5f6a7b8",
            ),
        },
    )
    result = guard.scan_once(
        account_drift,
        cgroup_root=root,
        job_lookup=lambda job_id: guard.JobRecord(
            job_id=job_id,
            account="lda-a1b2c3d4",
            comment="loom-cgroup-v1:pids=32768",
            job_name="loom-sandbox-runtime-a-aaaaaaaaaaaa-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
            user="lds-a1b2c3d4",
        ),
    )
    assert result["failed"] == 1
    assert "account binding" in result["failures"][0]["reason"]


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
            job_name="loom-sandbox-qianyi-aaaaaaaaaaaa-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
            user="loom-sandbox-qianyi",
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
            gres_detail="gpu(IDX:0)",
            job_name="loom-sandbox-qianyi-aaaaaaaaaaaa-oldlab-1",
            batch_host="oldlab-1",
            node_list="oldlab-1",
            user="loom-sandbox-qianyi",
        ),
    )
    assert passed["failed"] == 0
    assert passed["verified"] == 1
    assert passed["resource_probes"]["loom-dev-qianyi"]["gpu_verified"] is True


def test_status_is_atomic_private_and_candidate_bound(tmp_path: Path) -> None:
    status = tmp_path / "state" / "guard.json"
    result = {
        "scanned": 1,
        "verified": 1,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probes": {"loom-dev-qianyi": {"job_id": "123"}},
    }

    guard.write_status(status, config=_config(), result=result)

    payload = json.loads(status.read_text())
    assert payload["candidate_set_sha256"] == _config().candidate_set_sha256
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


def _systemd_record(
    config: guard.GuardConfig,
    *,
    job_id: str = "123",
    start_time: str = "2026-07-30T00:00:00",
) -> guard.JobRecord:
    binding = config.candidate_bindings["loom-dev-qianyi"]
    return guard.JobRecord(
        job_id=job_id,
        account=binding.account,
        comment=f"loom-cgroup-v1:pids={config.pids_max}",
        alloc_tres="cpu=2,mem=8000M",
        job_name=(f"loom-sandbox-{binding.sandbox}-{binding.candidate_sha[:12]}-oldlab-1"),
        batch_host="oldlab-1",
        node_list="oldlab-1",
        user=binding.service_user,
        start_time=start_time,
        gres_detail="(null)",
    )


def _systemd_scan_fixture(
    tmp_path: Path,
) -> tuple[
    guard.GuardConfig,
    Path,
    Path,
    Path,
    guard.JobRecord,
    list[tuple[str, ...]],
    dict[str, object],
]:
    config = replace(_config(), docker_cgroup_driver="systemd")
    cgroup_root, _job_path = _job(tmp_path)
    unit_root = tmp_path / "units"
    receipt_root = tmp_path / "receipts"
    unit_root.mkdir(mode=0o755)
    record = _systemd_record(config)
    commands: list[tuple[str, ...]] = []
    result = guard.scan_once(
        config,
        cgroup_root=cgroup_root,
        job_lookup=lambda _job_id: record,
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda argv: commands.append(tuple(argv)),
    )
    return (
        config,
        cgroup_root,
        unit_root,
        receipt_root,
        record,
        commands,
        result,
    )


def test_systemd_scan_publishes_allocation_slice_without_peer_mutation(
    tmp_path: Path,
) -> None:
    unit_root = tmp_path / "units"
    unit_root.mkdir(mode=0o755)
    peer = unit_root / "peer-production.slice"
    peer.write_text("[Slice]\nMemoryMax=1G\n", encoding="ascii")
    peer.chmod(0o640)
    peer_before = (peer.read_bytes(), peer.stat().st_mode, unit_root.stat().st_mode)
    config = replace(_config(), docker_cgroup_driver="systemd")
    cgroup_root, _job_path = _job(tmp_path)
    receipt_root = tmp_path / "receipts"
    record = _systemd_record(config)
    commands: list[tuple[str, ...]] = []

    result = guard.scan_once(
        config,
        cgroup_root=cgroup_root,
        job_lookup=lambda _job_id: record,
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda argv: commands.append(tuple(argv)),
    )

    probe = result["resource_probes"][record.account]
    unit = str(probe["systemd_slice"])
    assert result["failed"] == 0
    assert (unit_root / unit).is_file()
    assert (receipt_root / f"{unit}.json").is_file()
    assert peer_before == (peer.read_bytes(), peer.stat().st_mode, unit_root.stat().st_mode)
    flattened = " ".join(" ".join(command) for command in commands)
    assert "daemon-reload" in flattened
    for forbidden in (" stop ", " kill ", " scancel ", "restart docker", "restart slurm"):
        assert forbidden not in f" {flattened} "


def test_systemd_scan_clamps_finite_source_swap_to_zero(tmp_path: Path) -> None:
    config = replace(_config(), docker_cgroup_driver="systemd")
    cgroup_root, job_path = _job(tmp_path)
    (job_path / "memory.swap.max").write_text("4294967296\n", encoding="ascii")
    unit_root = tmp_path / "units"
    receipt_root = tmp_path / "receipts"
    unit_root.mkdir(mode=0o755)
    record = _systemd_record(config)

    result = guard.scan_once(
        config,
        cgroup_root=cgroup_root,
        job_lookup=lambda _job_id: record,
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda _argv: None,
    )

    probe = result["resource_probes"][record.account]
    unit = str(probe["systemd_slice"])
    receipt = json.loads((receipt_root / f"{unit}.json").read_text(encoding="ascii"))
    assert receipt["memory_swap_max_source"] == "4294967296"
    assert receipt["memory_swap_max_effective"] == "0"
    assert "MemorySwapMax=0\n" in (unit_root / unit).read_text(encoding="ascii")


def test_systemd_scan_restart_reuses_exact_unit_and_receipt(tmp_path: Path) -> None:
    (
        config,
        cgroup_root,
        unit_root,
        receipt_root,
        record,
        _commands,
        first,
    ) = _systemd_scan_fixture(tmp_path)
    probe = first["resource_probes"][record.account]
    unit = str(probe["systemd_slice"])
    unit_before = (unit_root / unit).read_bytes()
    receipt_before = (receipt_root / f"{unit}.json").read_bytes()
    commands: list[tuple[str, ...]] = []

    second = guard.scan_once(
        config,
        cgroup_root=cgroup_root,
        job_lookup=lambda _job_id: record,
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda argv: commands.append(tuple(argv)),
    )

    assert second["failed"] == 0
    assert (unit_root / unit).read_bytes() == unit_before
    assert (receipt_root / f"{unit}.json").read_bytes() == receipt_before
    assert not any("daemon-reload" in command for command in commands)


def test_systemd_expired_empty_slice_is_removed_without_stop_or_kill(
    tmp_path: Path,
) -> None:
    (
        config,
        _active_cgroup_root,
        unit_root,
        receipt_root,
        record,
        _commands,
        first,
    ) = _systemd_scan_fixture(tmp_path)
    unit = str(first["resource_probes"][record.account]["systemd_slice"])
    empty_root = tmp_path / "empty-cgroup"
    empty_root.mkdir()
    commands: list[tuple[str, ...]] = []

    result = guard.scan_once(
        config,
        cgroup_root=empty_root,
        job_lookup=lambda _job_id: pytest.fail("there is no live job"),
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda argv: commands.append(tuple(argv)),
    )

    assert result["failed"] == 0
    assert not (unit_root / unit).exists()
    assert not (receipt_root / f"{unit}.json").exists()
    assert commands == [("/usr/bin/systemctl", "daemon-reload")]


@pytest.mark.parametrize("residue", ("scope", "process"))
def test_systemd_live_residue_is_preserved_and_fails_closed(
    tmp_path: Path,
    residue: str,
) -> None:
    (
        config,
        _active_cgroup_root,
        unit_root,
        receipt_root,
        record,
        _commands,
        first,
    ) = _systemd_scan_fixture(tmp_path)
    unit = str(first["resource_probes"][record.account]["systemd_slice"])
    empty_root = tmp_path / "empty-cgroup"
    empty_root.mkdir()
    slice_cgroup = guard._slice_cgroup_path(empty_root, unit)
    slice_cgroup.mkdir(parents=True)
    (slice_cgroup / "cgroup.procs").write_text(
        "4242\n" if residue == "process" else "",
        encoding="ascii",
    )
    if residue == "scope":
        (slice_cgroup / "docker-deadbeef.scope").mkdir()
    commands: list[tuple[str, ...]] = []

    result = guard.scan_once(
        config,
        cgroup_root=empty_root,
        job_lookup=lambda _job_id: pytest.fail("there is no live job"),
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda argv: commands.append(tuple(argv)),
    )

    assert result["failed"] == 1
    assert (unit_root / unit).is_file()
    assert (receipt_root / f"{unit}.json").is_file()
    assert commands == []


def test_foreign_same_prefix_unit_is_preserved_and_fails_closed(
    tmp_path: Path,
) -> None:
    config = replace(_config(), docker_cgroup_driver="systemd")
    cgroup_root = tmp_path / "cgroup"
    unit_root = tmp_path / "units"
    receipt_root = tmp_path / "receipts"
    cgroup_root.mkdir()
    unit_root.mkdir(mode=0o755)
    receipt_root.mkdir(mode=0o755)
    foreign = unit_root / f"loom-job-999-{'f' * 40}.slice"
    foreign.write_text("[Slice]\nMemoryMax=1G\n", encoding="ascii")
    foreign.chmod(0o640)
    before = (foreign.read_bytes(), foreign.stat().st_mode)
    commands: list[tuple[str, ...]] = []

    result = guard.scan_once(
        config,
        cgroup_root=cgroup_root,
        unit_root=unit_root,
        receipt_root=receipt_root,
        systemd_runner=lambda argv: commands.append(tuple(argv)),
    )

    assert result["failed"] == 1
    assert before == (foreign.read_bytes(), foreign.stat().st_mode)
    assert commands == []
