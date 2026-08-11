from __future__ import annotations

import pytest

from loom_control_plane import elastic_slurm_worker_controller as controller
from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmNodeCapacityPlan,
    SlurmNodeResource,
    SlurmWorkerCapacitySnapshot,
    SlurmWorkerControllerDecision,
    SubprocessSlurmCommandRunner,
    _resolve_allowed_node_case,
    build_controller_config,
    build_sbatch_request,
    compute_controller_decision,
    parse_sinfo_node_resources,
    slurm_submission_config_for_node,
    with_node_resource_snapshot,
)


def _config(**overrides: object) -> ElasticSlurmWorkerControllerConfig:
    values: dict[str, object] = {
        "environment": "production",
        "pool_name": "oldlab",
        "allowed_nodes": ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5"),
        "env_file": "/secure/.env.remote-worker",
        "repo_dir": "/opt/loom",
        "partition": "",
        "time_limit": "7-00:00:00",
        "requested_cpus": 12,
        "requested_memory_mib": 58000,
        "requested_concurrency": 6,
        "max_jobs": 5,
        "pending_job_cap": 2,
        "min_queued_trials": 1,
        "stale_after_seconds": 300,
        "sbatch_path": "sbatch",
        "squeue_path": "squeue",
        "sacct_path": "sacct",
        "scancel_path": "scancel",
        "command_timeout_seconds": 20.0,
        # #896: non-exclusive workers require a positive job cgroup PID ceiling
        # (>= container_pids * concurrency). Default to a value the standard
        # config satisfies; tests exercising the guard override it explicitly.
        "job_pids_max": 3072,
    }
    values.update(overrides)
    return ElasticSlurmWorkerControllerConfig(**values)  # type: ignore[arg-type]


def test_decision_submits_missing_capacity_without_reusing_active_nodes() -> None:
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=20,
        running_trials=3,
        pending_jobs=1,
        running_jobs=1,
        active_slots=6,
        pending_slots=6,
        active_nodes={"oldlab-1", "oldlab-2"},
        cancellable_pending_job_ids=(),
        active_job_ids=("101", "102"),
    )

    decision = compute_controller_decision(_config(), snapshot)

    assert decision == SlurmWorkerControllerDecision(
        submit_nodes=("oldlab-3", "oldlab-4"),
        cancel_job_ids=(),
        reason="queued_backlog",
    )


def test_decision_respects_pending_cap_before_submitting_more() -> None:
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=36,
        running_trials=0,
        pending_jobs=2,
        running_jobs=0,
        active_slots=0,
        pending_slots=12,
        active_nodes={"oldlab-1", "oldlab-2"},
        cancellable_pending_job_ids=("201", "202"),
        active_job_ids=("201", "202"),
    )

    decision = compute_controller_decision(_config(), snapshot)

    assert decision == SlurmWorkerControllerDecision(
        submit_nodes=(),
        cancel_job_ids=(),
        reason="pending_cap_reached",
    )


def test_decision_cancels_pending_jobs_when_queue_drains() -> None:
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=0,
        running_trials=12,
        pending_jobs=2,
        running_jobs=1,
        active_slots=6,
        pending_slots=12,
        active_nodes={"oldlab-1", "oldlab-2", "oldlab-3"},
        cancellable_pending_job_ids=("301", "302"),
        active_job_ids=("301", "302", "303"),
    )

    decision = compute_controller_decision(_config(), snapshot)

    assert decision == SlurmWorkerControllerDecision(
        submit_nodes=(),
        cancel_job_ids=("301", "302"),
        reason="queue_drained",
    )


def test_resource_aware_decision_filters_unsafe_nodes_and_uses_safe_slots() -> None:
    config = _config(
        requested_concurrency=1,
        resource_aware=True,
        cpu_per_slot=2,
        memory_mib_per_slot=8192,
        reserved_cpus=4,
        reserved_memory_mib=24_576,
        max_concurrency_per_node=8,
        max_cpu_load_ratio=1.0,
    )
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=30,
        running_trials=0,
        pending_jobs=0,
        running_jobs=0,
        active_slots=0,
        pending_slots=0,
        active_nodes=set(),
        cancellable_pending_job_ids=(),
        active_job_ids=(),
        node_resources={
            "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 120_000, 12.0),
            "oldlab-2": SlurmNodeResource("oldlab-2", "mixed", 24, 8_000, 12.0),
            "oldlab-3": SlurmNodeResource("oldlab-3", "drain", 24, 120_000, 12.0),
            "oldlab-4": SlurmNodeResource("oldlab-4", "mixed", 24, 120_000, 30.0),
            "oldlab-5": SlurmNodeResource("oldlab-5", "mixed", 24, 120_000, 12.0),
        },
    )

    decision = compute_controller_decision(config, snapshot)

    assert decision.submit_nodes == ("oldlab-1", "oldlab-5")
    assert decision.node_capacity["oldlab-1"].safe_slots == 8
    assert decision.node_capacity["oldlab-1"].reason == "eligible"
    assert decision.node_capacity["oldlab-2"].reason == "insufficient_memory"
    assert decision.node_capacity["oldlab-3"].reason == "unsafe_state"
    assert decision.node_capacity["oldlab-4"].reason == "cpu_load_high"


def test_resource_aware_decision_does_not_reuse_active_nodes() -> None:
    config = _config(
        resource_aware=True,
        cpu_per_slot=2,
        memory_mib_per_slot=8192,
        reserved_cpus=4,
        reserved_memory_mib=24_576,
        max_concurrency_per_node=8,
    )
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=8,
        running_trials=0,
        pending_jobs=0,
        running_jobs=1,
        active_slots=8,
        pending_slots=0,
        active_nodes={"oldlab-1"},
        cancellable_pending_job_ids=(),
        active_job_ids=("401",),
        node_resources={
            "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 120_000, 4.0),
            "oldlab-2": SlurmNodeResource("oldlab-2", "mixed", 24, 120_000, 4.0),
        },
    )

    decision = compute_controller_decision(config, snapshot)

    assert decision.submit_nodes == ()
    assert decision.node_capacity["oldlab-1"].reason == "active_loom_job"


def test_parse_sinfo_node_resources_reads_state_memory_load_and_idle_cpus() -> None:
    resources = parse_sinfo_node_resources(
        "\n".join(
            (
                "trt-eai-oldlab-1|mixed|24|120000|56273|28.96|8/16/0/24",
                "trt-eai-oldlab-2|idle|24|120000|119000|0.25|0/24/0/24",
            )
        ),
    )

    assert resources["trt-eai-oldlab-1"] == SlurmNodeResource(
        hostname="trt-eai-oldlab-1",
        state="mixed",
        cpus_total=24,
        free_memory_mib=56_273,
        cpu_load=28.96,
        idle_cpus=16,
    )
    assert resources["trt-eai-oldlab-2"].idle_cpus == 24


async def test_with_node_resource_snapshot_queries_runner_when_resource_aware() -> None:
    class FakeRunner:
        def __init__(self) -> None:
            self.queried_nodes: tuple[str, ...] = ()

        async def query_node_resources(
            self,
            nodes: tuple[str, ...],
        ) -> dict[str, SlurmNodeResource]:
            self.queried_nodes = nodes
            return {
                "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 120_000, 1.0),
            }

    runner = FakeRunner()
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=1,
        running_trials=0,
        pending_jobs=0,
        running_jobs=0,
        active_slots=0,
        pending_slots=0,
        active_nodes=set(),
        cancellable_pending_job_ids=(),
        active_job_ids=(),
    )

    updated = await with_node_resource_snapshot(
        snapshot,
        config=_config(resource_aware=True, allowed_nodes=("oldlab-1", "oldlab-2")),
        runner=runner,
    )

    assert runner.queried_nodes == ("oldlab-1", "oldlab-2")
    assert updated.node_resources == {
        "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 120_000, 1.0),
    }


def test_slurm_submission_config_for_node_uses_capacity_plan() -> None:
    decision = SlurmWorkerControllerDecision(
        submit_nodes=("oldlab-1",),
        cancel_job_ids=(),
        reason="queued_backlog",
        node_capacity={
            "oldlab-1": SlurmNodeCapacityPlan(
                hostname="oldlab-1",
                safe_slots=8,
                reason="eligible",
            ),
        },
    )

    node_config = slurm_submission_config_for_node(
        _config(
            resource_aware=True,
            requested_concurrency=1,
            requested_cpus=2,
            requested_memory_mib=8192,
            cpu_per_slot=2,
            memory_mib_per_slot=8192,
        ),
        decision,
        node="oldlab-1",
    )

    assert node_config.requested_concurrency == 8
    assert node_config.requested_cpus == 16
    assert node_config.requested_memory_mib == 65_536


def test_build_sbatch_request_uses_environment_specific_worker_settings() -> None:
    request = build_sbatch_request(_config(partition="cpu"), node="oldlab-4")

    assert request.args == (
        "sbatch",
        "--parsable",
        "--job-name=loom-production-legacy-oldlab-4",
        "--nodelist=oldlab-4",
        "--chdir=/opt/loom",
        "--comment=loom-cgroup-v1:pids=3072",
        "--time=7-00:00:00",
        "--partition=cpu",
        "--cpus-per-task=12",
        "--mem=58000M",
        "--export=ALL,LOOM_WORKER_MAX_CONCURRENT=6,LOOM_WORKER_POOL_NAME=oldlab,LOOM_WORKER_HOSTNAME=oldlab-4,LOOM_REMOTE_WORKER_ENV_FILE=/secure/.env.remote-worker,LOOM_REMOTE_WORKER_REPO_DIR=/opt/loom,LOOM_WORKER_SANDBOX_IDENTITY=production,LOOM_WORKER_CANDIDATE_SHA=legacy,LOOM_WORKER_SLURM_ALLOCATED_GPUS=0,LOOM_WORKER_RESTART_POLICY=no,LOOM_WORKER_REQUIRE_CGROUP_PARENT=1,LOOM_WORKER_JOB_PIDS_MAX=3072",
    )
    assert "compose_files=(-f deploy/docker-compose.remote-worker.yml)" in (request.stdin)
    assert (
        'compose_args=(--project-name "$LOOM_WORKER_COMPOSE_PROJECT" '
        '--env-file "$LOOM_REMOTE_WORKER_ENV_FILE" "${compose_files[@]}")'
    ) in request.stdin
    assert 'export LOOM_WORKER_SLURM_JOB_ID="$SLURM_JOB_ID"' in request.stdin
    assert (
        'export LOOM_WORKER_COMPOSE_PROJECT="loom-${LOOM_WORKER_SANDBOX_IDENTITY}-'
        '${project_candidate}-${project_job}"'
    ) in request.stdin
    assert 'docker compose "${compose_args[@]}" up --build &' in request.stdin
    assert 'docker compose "${compose_args[@]}" down --remove-orphans' in request.stdin
    assert 'cd "$LOOM_REMOTE_WORKER_REPO_DIR"' in request.stdin


def test_build_sbatch_request_rejects_exclusive_node_allocation() -> None:
    with pytest.raises(ValueError, match="exclusive Loom Slurm workers"):
        build_sbatch_request(_config(exclusive=True), node="oldlab-4")


def test_build_sbatch_request_can_disable_exclusive_node_allocation() -> None:
    request = build_sbatch_request(
        _config(
            exclusive=False,
            container_pids=512,
            job_pids_max=3072,
        ),
        node="oldlab-4",
    )

    assert "--exclusive" not in request.args
    assert "--comment=loom-cgroup-v1:pids=3072" in request.args
    export_arg = next(a for a in request.args if a.startswith("--export="))
    assert "LOOM_WORKER_REQUIRE_CGROUP_PARENT=1" in export_arg
    assert "LOOM_WORKER_JOB_PIDS_MAX=3072" in export_arg
    assert "-m loom_control_plane.slurm_job_cgroup" in request.stdin
    assert '--job-id "$SLURM_JOB_ID"' in request.stdin
    assert '--pids-max "$LOOM_WORKER_JOB_PIDS_MAX"' in request.stdin
    assert "--wait-seconds 30" in request.stdin
    # The parent must be discovered against Docker's live cgroup driver so the
    # systemd driver receives a ``.slice`` and cgroupfs the raw job cgroup.
    assert "docker info --format '{{.CgroupDriver}}'" in request.stdin
    assert '--docker-driver "$worker_cgroup_driver"' in request.stdin
    assert "docker-compose.remote-worker.cgroup-parent.yml" in request.stdin
    # The generated batch script must be valid shell (a stray quote in an error
    # message once broke `${var:?...}` parsing and failed the job in ~1s).
    import shutil
    import subprocess

    bash = shutil.which("bash")
    if bash is not None:
        syntax = subprocess.run(
            [bash, "-n"],
            input=request.stdin,
            capture_output=True,
            text=True,
            check=False,
        )
        assert syntax.returncode == 0, syntax.stderr


def test_nonexclusive_sbatch_rejects_missing_job_pids_max() -> None:
    with pytest.raises(ValueError, match="job_pids_max is required"):
        build_sbatch_request(
            _config(exclusive=False, container_pids=512, job_pids_max=0),
            node="oldlab-4",
        )


def test_build_sbatch_request_emits_account_qos_reservation_when_set() -> None:
    request = build_sbatch_request(
        _config(
            slurm_account="loom-staging",
            slurm_qos="loom-boost",
            slurm_reservation="loom-staging-min",
        ),
        node="oldlab-4",
    )

    assert "--account=loom-staging" in request.args
    assert "--qos=loom-boost" in request.args
    assert "--reservation=loom-staging-min" in request.args


def test_build_sbatch_request_omits_account_qos_reservation_when_empty() -> None:
    request = build_sbatch_request(_config(), node="oldlab-4")

    assert not any(arg.startswith("--account=") for arg in request.args)
    assert not any(arg.startswith("--qos=") for arg in request.args)
    assert not any(arg.startswith("--reservation=") for arg in request.args)


def test_build_sbatch_request_uses_shared_job_output_directory() -> None:
    config = build_controller_config(
        **_controller_config_kwargs(  # type: ignore[arg-type]
            job_output_dir="/shared_work/loom/staging-rollout/job-output/",
        )
    )

    assert config is not None
    assert config.job_output_dir == "/shared_work/loom/staging-rollout/job-output"
    request = build_sbatch_request(config, node="oldlab-1")
    assert (
        "--output=/shared_work/loom/staging-rollout/job-output/slurm-%j.out"
        in request.args
    )


@pytest.mark.parametrize(
    "job_output_dir",
    ["relative/output", "/shared_work/../private", "/shared_work/output\n--error=x"],
)
def test_build_controller_config_rejects_unsafe_job_output_directory(
    job_output_dir: str,
) -> None:
    with pytest.raises(ValueError, match="job_output_dir"):
        build_controller_config(
            **_controller_config_kwargs(  # type: ignore[arg-type]
                job_output_dir=job_output_dir,
            )
        )


def test_build_sbatch_request_exports_container_caps_when_set() -> None:
    # #896: per-container caps (>0) are exported into the worker env so the
    # packed compose worker + trial/sidecar containers get a hard ceiling.
    request = build_sbatch_request(
        _config(
            container_cpus=4.0,
            container_memory_mib=8192,
            container_pids=512,
        ),
        node="oldlab-4",
    )

    export_arg = next(a for a in request.args if a.startswith("--export="))
    assert "LOOM_WORKER_CONTAINER_CPUS=4.0" in export_arg
    assert "LOOM_WORKER_CONTAINER_MEMORY_MIB=8192" in export_arg
    assert "LOOM_WORKER_CONTAINER_PIDS=512" in export_arg


def test_build_sbatch_request_omits_container_caps_when_default() -> None:
    # Non-Slurm callers may still omit caps; Loom Slurm admission rejects this.
    request = build_sbatch_request(_config(), node="oldlab-4")

    export_arg = next(a for a in request.args if a.startswith("--export="))
    assert "LOOM_WORKER_CONTAINER_CPUS" not in export_arg
    assert "LOOM_WORKER_CONTAINER_MEMORY_MIB" not in export_arg
    assert "LOOM_WORKER_CONTAINER_PIDS" not in export_arg


def _controller_config_kwargs(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "enabled": True,
        "environment": "staging",
        "pool_name": "oldlab",
        "allowed_nodes_csv": "oldlab-1",
        "env_file": "/secure/staging.env",
        "repo_dir": "/srv/loom",
        "partition": "",
        "time_limit": "2:00:00",
        "requested_cpus": 12,
        "requested_memory_mib": 58000,
        "requested_concurrency": 6,
        "max_jobs": 1,
        "pending_job_cap": 1,
        "min_queued_trials": 1,
        "stale_after_seconds": 300,
        "sbatch_path": "sbatch",
        "squeue_path": "squeue",
        "sacct_path": "sacct",
        "scancel_path": "scancel",
        "command_timeout_seconds": 20.0,
        "exclusive": False,
        "container_cpus": 2.0,
        "container_memory_mib": 4096,
        "container_pids": 512,
        "candidate_sha": "a" * 40,
        # #896: satisfies job_pids_max >= container_pids * concurrency (512*6).
        "job_pids_max": 3072,
    }
    values.update(overrides)
    return values


def test_build_controller_config_threads_container_caps() -> None:
    # #896: caps flow from the actuator policy through build_controller_config.
    config = build_controller_config(
        **_controller_config_kwargs(  # type: ignore[arg-type]
            container_cpus=2.5,
            container_memory_mib=4096,
            container_pids=256,
            job_pids_max=1536,
        )
    )
    assert config is not None
    assert config.container_cpus == 2.5
    assert config.container_memory_mib == 4096
    assert config.container_pids == 256
    assert config.job_pids_max == 1536


@pytest.mark.parametrize(
    "field",
    ["container_cpus", "container_memory_mib", "container_pids"],
)
def test_build_controller_config_rejects_negative_container_caps(field: str) -> None:
    with pytest.raises(ValueError):
        build_controller_config(
            **_controller_config_kwargs(**{field: -1}),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("container_cpus", 0.0),
        ("container_memory_mib", 0),
        ("container_pids", 0),
        ("job_pids_max", 0),
        ("candidate_sha", ""),
    ],
)
def test_nonexclusive_admission_requires_complete_containment(
    field: str,
    value: object,
) -> None:
    overrides: dict[str, object] = {
        "exclusive": False,
        "environment": "dev-a",
        "container_cpus": 2.0,
        "container_memory_mib": 4096,
        "container_pids": 512,
        "job_pids_max": 3072,
        "candidate_sha": "a" * 40,
    }
    overrides[field] = value

    with pytest.raises(ValueError):
        build_controller_config(
            **_controller_config_kwargs(**overrides),  # type: ignore[arg-type]
        )


def test_build_controller_config_rejects_exclusive_workers() -> None:
    with pytest.raises(ValueError, match="exclusive Loom Slurm workers"):
        build_controller_config(
            **_controller_config_kwargs(exclusive=True),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("job_pids_max", "match"),
    [
        (511, "must not be lower than container_pids"),
        (1024, "configured concurrency ceiling"),
        (3072.0, "non-negative integer"),
    ],
)
def test_nonexclusive_admission_rejects_unsafe_job_pids_max(
    job_pids_max: object,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        build_controller_config(
            **_controller_config_kwargs(  # type: ignore[arg-type]
                exclusive=False,
                environment="dev-a",
                container_cpus=2.0,
                container_memory_mib=4096,
                container_pids=512,
                job_pids_max=job_pids_max,
                candidate_sha="a" * 40,
            ),
        )


def test_resource_aware_job_pids_max_covers_maximum_node_concurrency() -> None:
    base = {
        "exclusive": False,
        "environment": "dev-a",
        "resource_aware": True,
        "requested_concurrency": 1,
        "max_concurrency_per_node": 8,
        "container_cpus": 2.0,
        "container_memory_mib": 4096,
        "container_pids": 512,
        "candidate_sha": "a" * 40,
    }

    with pytest.raises(ValueError, match="configured concurrency ceiling"):
        build_controller_config(
            **_controller_config_kwargs(  # type: ignore[arg-type]
                **base,
                job_pids_max=4095,
            ),
        )

    config = build_controller_config(
        **_controller_config_kwargs(  # type: ignore[arg-type]
            **base,
            job_pids_max=4096,
        ),
    )
    assert config is not None
    assert config.job_pids_max == 4096


def test_gpu_tres_is_validated_and_emitted_in_sbatch_request() -> None:
    config = build_controller_config(
        **_controller_config_kwargs(  # type: ignore[arg-type]
            gpu_tres="gpu:a100:2",
        )
    )

    assert config is not None
    assert config.requested_gpus == 2
    request = build_sbatch_request(config, node="oldlab-1")
    assert "--gres=gpu:a100:2" in request.args
    assert "LOOM_WORKER_SLURM_ALLOCATED_GPUS=2" in request.args[-1]
    assert "SLURM_JOB_GPUS is empty" in request.stdin


@pytest.mark.parametrize("gpu_tres", ["gpu:0", "gpu:a100", "tesla:1", "gpu::2"])
def test_gpu_tres_rejects_ambiguous_requests(gpu_tres: str) -> None:
    with pytest.raises(ValueError, match="gpu_tres"):
        build_controller_config(
            **_controller_config_kwargs(gpu_tres=gpu_tres),  # type: ignore[arg-type]
        )


def test_build_controller_config_threads_account_qos_reservation() -> None:
    config = build_controller_config(
        enabled=True,
        environment="staging",
        pool_name="oldlab",
        allowed_nodes_csv="oldlab-1",
        env_file="/secure/staging.env",
        repo_dir="/srv/loom",
        partition="",
        time_limit="2:00:00",
        requested_cpus=12,
        requested_memory_mib=58000,
        requested_concurrency=6,
        max_jobs=1,
        pending_job_cap=1,
        min_queued_trials=1,
        stale_after_seconds=300,
        sbatch_path="sbatch",
        squeue_path="squeue",
        sacct_path="sacct",
        scancel_path="scancel",
        command_timeout_seconds=20.0,
        slurm_account="loom-staging",
        slurm_qos="loom-staging-normal",
        slurm_reservation="loom-staging-min",
        container_cpus=2.0,
        container_memory_mib=4096,
        container_pids=512,
        candidate_sha="a" * 40,
        job_pids_max=3072,
    )

    assert config is not None
    assert config.slurm_account == "loom-staging"
    assert config.slurm_qos == "loom-staging-normal"
    assert config.slurm_reservation == "loom-staging-min"


def test_build_sbatch_request_cleans_up_compose_on_exit() -> None:
    request = build_sbatch_request(_config(), node="oldlab-4")

    assert "trap cleanup EXIT" in request.stdin
    assert "trap 'cleanup 130' INT" in request.stdin
    assert "trap 'cleanup 143' TERM" in request.stdin
    assert 'docker compose "${compose_args[@]}" up --build &' in request.stdin
    assert "compose_pid=$!" in request.stdin
    assert 'wait "$compose_pid"' in request.stdin
    assert 'docker compose "${compose_args[@]}" down --remove-orphans' in request.stdin


def test_build_controller_config_rejects_enabled_missing_required_settings() -> None:
    with pytest.raises(ValueError, match="allowed nodes"):
        build_controller_config(
            enabled=True,
            environment="production",
            pool_name="oldlab",
            allowed_nodes_csv="",
            env_file="/secure/.env.remote-worker",
            repo_dir="/opt/loom",
            partition="",
            time_limit="7-00:00:00",
            requested_cpus=12,
            requested_memory_mib=58000,
            requested_concurrency=6,
            max_jobs=5,
            pending_job_cap=2,
            min_queued_trials=1,
            stale_after_seconds=300,
            sbatch_path="sbatch",
            squeue_path="squeue",
            sacct_path="sacct",
            scancel_path="scancel",
            command_timeout_seconds=20.0,
        )


def test_build_controller_config_parses_allowed_nodes_and_caps() -> None:
    config = build_controller_config(
        enabled=True,
        environment="staging",
        pool_name="oldlab",
        allowed_nodes_csv=" oldlab-1,oldlab-2 ,, oldlab-3 ",
        env_file="/secure/staging.env",
        repo_dir="/srv/loom",
        partition="",
        time_limit="2:00:00",
        requested_cpus=12,
        requested_memory_mib=58000,
        requested_concurrency=6,
        max_jobs=3,
        pending_job_cap=1,
        min_queued_trials=1,
        stale_after_seconds=300,
        sbatch_path="sbatch",
        squeue_path="squeue",
        sacct_path="sacct",
        scancel_path="scancel",
        command_timeout_seconds=20.0,
        container_cpus=2.0,
        container_memory_mib=4096,
        container_pids=512,
        candidate_sha="a" * 40,
        job_pids_max=3072,
    )

    assert config.allowed_nodes == ("oldlab-1", "oldlab-2", "oldlab-3")
    assert config.max_jobs == 3


class _ResolvingRunner:
    """Runner double exposing only case-insensitive node resolution."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self._mapping = mapping

    async def resolve_node_names(self, nodes: tuple[str, ...]) -> dict[str, str]:
        return {n: self._mapping[n] for n in nodes if n in self._mapping}


class _RaisingRunner:
    async def resolve_node_names(self, nodes: tuple[str, ...]) -> dict[str, str]:
        raise RuntimeError("sinfo unavailable")


class _NoResolveRunner:
    """A legacy runner without resolve_node_names (resolution is optional)."""


async def test_resolve_allowed_node_case_maps_to_canonical() -> None:
    cfg = _config(allowed_nodes=("trt-eai-oldlab-1", "trt-eai-oldlab-2"))
    runner = _ResolvingRunner(
        {"trt-eai-oldlab-1": "TRT-EAI-OLDLAB-1", "trt-eai-oldlab-2": "trt-EAI-OLDLAB-2"},
    )

    out = await _resolve_allowed_node_case(cfg, runner)  # type: ignore[arg-type]

    assert out.allowed_nodes == ("TRT-EAI-OLDLAB-1", "trt-EAI-OLDLAB-2")


async def test_resolve_allowed_node_case_drops_unknown_nodes() -> None:
    cfg = _config(allowed_nodes=("trt-eai-oldlab-1", "ghost-node"))
    runner = _ResolvingRunner({"trt-eai-oldlab-1": "TRT-EAI-OLDLAB-1"})

    out = await _resolve_allowed_node_case(cfg, runner)  # type: ignore[arg-type]

    assert out.allowed_nodes == ("TRT-EAI-OLDLAB-1",)


async def test_resolve_allowed_node_case_falls_back_when_resolution_empty() -> None:
    # A transient sinfo hiccup must not disable the pool by emptying allowed_nodes.
    cfg = _config(allowed_nodes=("trt-eai-oldlab-1",))

    out = await _resolve_allowed_node_case(cfg, _ResolvingRunner({}))  # type: ignore[arg-type]

    assert out.allowed_nodes == ("trt-eai-oldlab-1",)


async def test_resolve_allowed_node_case_falls_back_on_error() -> None:
    cfg = _config(allowed_nodes=("trt-eai-oldlab-1",))

    out = await _resolve_allowed_node_case(cfg, _RaisingRunner())  # type: ignore[arg-type]

    assert out.allowed_nodes == ("trt-eai-oldlab-1",)


async def test_resolve_allowed_node_case_noop_without_resolver() -> None:
    cfg = _config(allowed_nodes=("trt-eai-oldlab-1",))

    out = await _resolve_allowed_node_case(cfg, _NoResolveRunner())  # type: ignore[arg-type]

    assert out is cfg


async def test_query_jobs_falls_back_to_sacct_when_squeue_rejects_terminal_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []

    async def fake_run_command(
        args: tuple[str, ...],
        *,
        timeout: float,
        stdin: str | None = None,
    ) -> controller._CommandResult:
        del timeout, stdin
        commands.append(args)
        if args[0] == "squeue":
            raise RuntimeError(
                "Slurm command failed (1): squeue "
                "slurm_load_jobs error: Invalid job id specified",
            )
        return controller._CommandResult(
            stdout="31619|FAILED|trt-eai-oldlab-5|NonZeroExitCode\n",
            stderr="",
        )

    monkeypatch.setattr(controller, "_run_command", fake_run_command)
    runner = SubprocessSlurmCommandRunner().bind_config(_config())

    observations = await runner.query_jobs(("31619",))

    assert [(item.job_id, item.slurm_state, item.nodelist) for item in observations] == [
        ("31619", "FAILED", "trt-eai-oldlab-5"),
    ]
    assert [command[0] for command in commands] == ["squeue", "sacct"]


async def test_query_jobs_keeps_non_terminal_squeue_errors_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_command(
        args: tuple[str, ...],
        *,
        timeout: float,
        stdin: str | None = None,
    ) -> controller._CommandResult:
        del args, timeout, stdin
        raise RuntimeError("Slurm command failed (1): squeue Unable to contact slurm controller")

    monkeypatch.setattr(controller, "_run_command", fake_run_command)
    runner = SubprocessSlurmCommandRunner().bind_config(_config())

    with pytest.raises(RuntimeError, match="Unable to contact slurm controller"):
        await runner.query_jobs(("31619",))
