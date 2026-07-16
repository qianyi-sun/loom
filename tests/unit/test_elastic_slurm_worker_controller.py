from __future__ import annotations

import pytest

from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmNodeCapacityPlan,
    SlurmNodeResource,
    SlurmWorkerCapacitySnapshot,
    SlurmWorkerControllerDecision,
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
        "--job-name=loom-worker-oldlab-4",
        "--nodelist=oldlab-4",
        "--exclusive",
        "--time=7-00:00:00",
        "--partition=cpu",
        "--cpus-per-task=12",
        "--mem=58000M",
        "--export=ALL,LOOM_WORKER_MAX_CONCURRENT=6,LOOM_WORKER_POOL_NAME=oldlab,LOOM_REMOTE_WORKER_ENV_FILE=/secure/.env.remote-worker,LOOM_REMOTE_WORKER_REPO_DIR=/opt/loom",
    )
    assert (
        'compose_args=(--env-file "$LOOM_REMOTE_WORKER_ENV_FILE" '
        "-f deploy/docker-compose.remote-worker.yml)"
    ) in request.stdin
    assert 'docker compose "${compose_args[@]}" up --build' in request.stdin
    assert 'cd "$LOOM_REMOTE_WORKER_REPO_DIR"' in request.stdin


def test_build_sbatch_request_can_disable_exclusive_node_allocation() -> None:
    request = build_sbatch_request(_config(exclusive=False), node="oldlab-4")

    assert "--exclusive" not in request.args


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
    )

    assert config.allowed_nodes == ("oldlab-1", "oldlab-2", "oldlab-3")
    assert config.max_jobs == 3
