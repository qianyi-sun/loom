from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from loom_control_plane.elastic_slurm_worker_controller import (
    FIXED_EXTERNAL_RUNNER_SAFETY_PROTOCOL,
    FIXED_EXTERNAL_RUNNER_SAFETY_TAG,
    ElasticSlurmWorkerControllerConfig,
    FixedExternalSlurmBrokerRunner,
    SlurmNodeCapacityPlan,
    SlurmNodeResource,
    SlurmWorkerCapacitySnapshot,
    SlurmWorkerControllerDecision,
    SubprocessSlurmCommandRunner,
    build_controller_config,
    build_sbatch_request,
    compute_controller_decision,
    fixed_external_slurm_broker_safety_binding,
    parse_sinfo_node_resources,
    require_fixed_external_slurm_broker_runner,
    run_elastic_slurm_worker_controller_once,
    slurm_compose_project_identity,
    slurm_submission_config_for_node,
    with_node_resource_snapshot,
)
from loom_control_plane.slurm_worker_jobs import SlurmWorkerJobObservation


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


@pytest.mark.asyncio
async def test_fixed_external_broker_routes_query_submit_cancel_without_local_slurm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    tree = "b" * 40
    config = _config(
        environment="staging",
        pool_name="gb10",
        allowed_nodes=tuple(f"trt-gb10-{index}" for index in range(1, 16)),
        repo_dir="/srv/loom/staging-shared/candidates/loom-remote-worker-staging-aaaaaaa",
        partition="gb10",
        slurm_account="loom-staging",
        slurm_qos="loom-staging",
        slurm_cluster="trt-gb10",
        candidate_sha=sha,
        candidate_tree=tree,
        external_broker="staging-gb10-v1",
        max_jobs=15,
        pending_job_cap=3,
    )
    calls: list[tuple[str, ...]] = []

    async def fake_run_command(args, *, timeout, stdin=None):
        del timeout, stdin
        argv = tuple(args)
        calls.append(argv)
        assert argv[:3] == (
            "/usr/bin/sudo",
            "-n",
            "/usr/local/libexec/loom-staging-external-slurm-authority",
        )
        assert not any(
            executable in argv for executable in ("sbatch", "squeue", "sacct", "sinfo", "scancel")
        )
        command = argv[3]
        if command == "broker-query" and "--job-id" in argv:
            payload = {
                "schema_version": 1,
                "kind": "staging_external_slurm_broker_query",
                "candidate_sha": sha,
                "candidate_tree": tree,
                "cluster": "trt-gb10",
                "controller": "trt-gb10-1",
                "submit_host": "trt-gb10-1",
                "jobs": [
                    {
                        "job_id": "31415",
                        "submit_request_id": "c" * 64,
                        "state": "RUNNING",
                        "nodelist": "trt-gb10-7",
                        "pending_reason": None,
                        "observed_at": "2026-07-30T12:00:00Z",
                    }
                ],
                "nodes": [],
            }
        elif command == "broker-query":
            payload = {
                "schema_version": 1,
                "kind": "staging_external_slurm_broker_query",
                "candidate_sha": sha,
                "candidate_tree": tree,
                "cluster": "trt-gb10",
                "controller": "trt-gb10-1",
                "submit_host": "trt-gb10-1",
                "jobs": [],
                "nodes": [
                    {
                        "hostname": "trt-gb10-7",
                        "state": "idle",
                        "cpus_total": 20,
                        "free_memory_mib": 110000,
                        "cpu_load": 0.25,
                        "idle_cpus": 20,
                    }
                ],
            }
        elif command == "broker-submit":
            payload = {
                "schema_version": 1,
                "kind": "staging_external_slurm_broker_submission",
                "candidate_sha": sha,
                "candidate_tree": tree,
                "cluster": "trt-gb10",
                "controller": "trt-gb10-1",
                "submit_host": "trt-gb10-1",
                "request_id": "d" * 64,
                "job_id": "27182",
                "node": "trt-gb10-8",
            }
        else:
            assert command == "broker-cancel"
            payload = {
                "schema_version": 1,
                "kind": "staging_external_slurm_broker_cancellation",
                "candidate_sha": sha,
                "candidate_tree": tree,
                "cluster": "trt-gb10",
                "controller": "trt-gb10-1",
                "submit_host": "trt-gb10-1",
                "request_id": "e" * 64,
                "submit_request_id": "c" * 64,
                "job_id": "31415",
                "node": "trt-gb10-7",
                "state": "CANCELLED",
            }
        return type("Result", (), {"stdout": json.dumps(payload), "stderr": ""})()

    monkeypatch.setattr(
        "loom_control_plane.elastic_slurm_worker_controller._run_command",
        fake_run_command,
    )
    runner = FixedExternalSlurmBrokerRunner().bind_config(config)

    assert await runner.query_jobs(("31415",)) == [
        SlurmWorkerJobObservation(
            job_id="31415",
            slurm_state="RUNNING",
            nodelist="trt-gb10-7",
            pending_reason=None,
            observed_at=datetime(2026, 7, 30, 12, 0, tzinfo=UTC),
        )
    ]
    assert await runner.query_node_resources(("trt-gb10-7",)) == {
        "trt-gb10-7": SlurmNodeResource(
            hostname="trt-gb10-7",
            state="idle",
            cpus_total=20,
            free_memory_mib=110000,
            cpu_load=0.25,
            idle_cpus=20,
        )
    }
    assert await runner.submit_worker(node="trt-gb10-8", config=config) == "27182"
    await runner.cancel_job("31415")
    assert [call[3] for call in calls] == [
        "broker-query",
        "broker-query",
        "broker-submit",
        "broker-cancel",
    ]


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"environment": "production"}, "staging"),
        ({"pool_name": "oldlab"}, "GB10"),
        ({"slurm_cluster": "trt-oldlab"}, "trt-gb10"),
        ({"external_broker": ""}, "staging-gb10-v1"),
    ],
)
def test_fixed_external_broker_rejects_any_pool_or_controller_fallback(
    overrides: dict[str, object],
    match: str,
) -> None:
    values: dict[str, object] = {
        "environment": "staging",
        "pool_name": "gb10",
        "slurm_cluster": "trt-gb10",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "external_broker": "staging-gb10-v1",
    }
    values.update(overrides)
    config = _config(**values)

    with pytest.raises(ValueError, match=match):
        FixedExternalSlurmBrokerRunner().bind_config(config)


@pytest.mark.asyncio
async def test_fixed_external_broker_rejects_success_with_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(
        environment="staging",
        pool_name="gb10",
        allowed_nodes=tuple(f"trt-gb10-{index}" for index in range(1, 16)),
        slurm_cluster="trt-gb10",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        external_broker="staging-gb10-v1",
        max_jobs=15,
        pending_job_cap=3,
    )

    async def fake_run_command(args, *, timeout, stdin=None):
        del args, timeout, stdin
        return type(
            "Result",
            (),
            {
                "stdout": json.dumps(
                    {
                        "schema_version": 1,
                        "kind": "staging_external_slurm_broker_query",
                        "candidate_sha": "a" * 40,
                        "candidate_tree": "b" * 40,
                        "cluster": "trt-gb10",
                        "controller": "trt-gb10-1",
                        "submit_host": "trt-gb10-1",
                        "jobs": [],
                        "nodes": [],
                    }
                ),
                "stderr": "unexpected diagnostic",
            },
        )()

    monkeypatch.setattr(
        "loom_control_plane.elastic_slurm_worker_controller._run_command",
        fake_run_command,
    )

    with pytest.raises(RuntimeError, match="identity mismatch"):
        await (
            FixedExternalSlurmBrokerRunner()
            .bind_config(config)
            .query_node_resources(("trt-gb10-7",))
        )


def test_fixed_external_broker_injected_runner_requires_explicit_exact_safety_binding() -> None:
    config = _config(
        environment="staging",
        pool_name="gb10",
        allowed_nodes=tuple(f"trt-gb10-{index}" for index in range(1, 16)),
        slurm_cluster="trt-gb10",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        external_broker="staging-gb10-v1",
    )

    class ExplicitSafeFake:
        external_runner_safety_protocol = FIXED_EXTERNAL_RUNNER_SAFETY_PROTOCOL
        external_runner_safety_tag = FIXED_EXTERNAL_RUNNER_SAFETY_TAG

        def external_runner_safety_binding(self):
            return fixed_external_slurm_broker_safety_binding(config)

    runner = ExplicitSafeFake()
    assert require_fixed_external_slurm_broker_runner(runner, config) is runner

    runner.external_runner_safety_tag = "unsafe-local-slurm"
    with pytest.raises(ValueError, match="safety protocol"):
        require_fixed_external_slurm_broker_runner(runner, config)


@pytest.mark.asyncio
async def test_external_controller_reconcile_rejects_subprocess_runner_before_query_submit_cancel() -> (
    None
):
    config = _config(
        environment="staging",
        pool_name="gb10",
        allowed_nodes=tuple(f"trt-gb10-{index}" for index in range(1, 16)),
        slurm_cluster="trt-gb10",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        external_broker="staging-gb10-v1",
    )

    with pytest.raises(ValueError, match="refuses the subprocess runner"):
        await run_elastic_slurm_worker_controller_once(
            object(),  # type: ignore[arg-type]
            config=config,
            runner=SubprocessSlurmCommandRunner().bind_config(config),
        )


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
        "--exclusive",
        "--time=7-00:00:00",
        "--partition=cpu",
        "--cpus-per-task=12",
        "--mem=58000M",
        "--export=ALL,LOOM_WORKER_MAX_CONCURRENT=6,LOOM_WORKER_POOL_NAME=oldlab,LOOM_REMOTE_WORKER_ENV_FILE=/secure/.env.remote-worker,LOOM_REMOTE_WORKER_REPO_DIR=/opt/loom,LOOM_WORKER_SANDBOX_IDENTITY=production,LOOM_WORKER_CANDIDATE_SHA=legacy,LOOM_WORKER_SLURM_ALLOCATED_GPUS=0,LOOM_WORKER_RESTART_POLICY=no",
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
    assert 'docker compose "${compose_args[@]}" up --no-build &' in request.stdin
    assert 'docker compose "${compose_args[@]}" config --format json' in request.stdin
    assert 're.fullmatch(r"sha256:[0-9a-f]{64}",str(image))' in request.stdin
    assert "docker inspect --format '{{.Image}}'" in request.stdin
    assert 'if [[ "$running_worker_image_id" != "$expected_worker_image_id" ]]' in request.stdin
    assert "worker container did not start with the exact image config ID" in request.stdin
    assert 'docker compose "${compose_args[@]}" down --remove-orphans' in request.stdin
    assert 'cd "$LOOM_REMOTE_WORKER_REPO_DIR"' in request.stdin


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
    assert "docker-compose.remote-worker.cgroup-parent.yml" in request.stdin


def _systemd_nonexclusive_overrides() -> dict[str, object]:
    return {
        "exclusive": False,
        "environment": "sandbox-dev-a",
        "container_cpus": 2.0,
        "container_memory_mib": 4096,
        "container_pids": 512,
        "job_pids_max": 3072,
        "docker_cgroup_driver": "systemd",
        "slurm_cluster": "trt-oldlab",
        "slurm_account": "loom-dev-a",
        "env_id": "denv-developer01",
        "resource_generation": 7,
        "runtime_id": "dev-a",
        "candidate_id": "cand-" + "b" * 40,
        "candidate_sha": "a" * 40,
        "candidate_tree": "c" * 40,
    }


def test_systemd_nonexclusive_sbatch_binds_exact_allocation_and_reads_back_pid() -> None:
    request = build_sbatch_request(
        _config(**_systemd_nonexclusive_overrides()),
        node="oldlab-4",
    )

    export_arg = next(arg for arg in request.args if arg.startswith("--export="))
    for expected in (
        "LOOM_WORKER_SANDBOX_IDENTITY=dev-a",
        "LOOM_WORKER_DOCKER_CGROUP_DRIVER=systemd",
        "LOOM_WORKER_SLURM_CLUSTER=trt-oldlab",
        "LOOM_WORKER_SCONTROL_PATH=/usr/bin/scontrol",
        "LOOM_WORKER_SLURM_ACCOUNT=loom-dev-a",
        "LOOM_WORKER_ENV_ID=denv-developer01",
        "LOOM_WORKER_RESOURCE_GENERATION=7",
        "LOOM_WORKER_RUNTIME_ID=dev-a",
        f"LOOM_WORKER_CANDIDATE_ID=cand-{'b' * 40}",
        f"LOOM_WORKER_CANDIDATE_TREE={'c' * 40}",
    ):
        assert expected in export_arg
    assert '"$LOOM_WORKER_SCONTROL_PATH" show job --oneliner --details' in request.stdin
    assert 'required=("JobId","Account","NodeList","StartTime")' in request.stdin
    assert '--docker-driver "$LOOM_WORKER_DOCKER_CGROUP_DRIVER"' in request.stdin
    assert '--job-start-time "${slurm_identity[3]}"' in request.stdin
    assert 'docker compose "${compose_args[@]}" ps -q worker' in request.stdin
    assert "docker inspect --format '{{.Image}}'" in request.stdin
    assert "docker inspect --format '{{.State.Pid}}'" in request.stdin
    assert 'Path(f"/proc/{pid}/cgroup")' in request.stdin
    assert "if parent not in child.parents" in request.stdin
    assert 'value("memory.swap.max")' in request.stdin
    assert "worker container did not enter the exact bounded systemd slice" in request.stdin
    assert (
        "unset LOOM_WORKER_REGISTRY_GENERATION LOOM_WORKER_REGISTRY_PAYLOAD_SHA256"
    ) in request.stdin
    assert 'docker compose "${compose_args[@]}" down --remove-orphans' in request.stdin
    assert slurm_compose_project_identity(
        _config(**_systemd_nonexclusive_overrides()),
        "123",
    ).startswith("loom-dev-a-")


def test_systemd_job_name_preserves_guard_contract_and_lowercases_node_only() -> None:
    request = build_sbatch_request(
        _config(**_systemd_nonexclusive_overrides()),
        node="TRT-EAI-OLDLAB-4",
    )

    assert "--nodelist=TRT-EAI-OLDLAB-4" in request.args
    assert "--job-name=loom-sandbox-dev-a-aaaaaaaaaaaa-trt-eai-oldlab-4" in request.args


def _embedded_scontrol_parser(stdin: str) -> str:
    marker = "/usr/bin/python3 -I -B -c '"
    start = stdin.index(marker) + len(marker)
    end = stdin.index("' ", start)
    return stdin[start:end]


def _embedded_post_start_verifier(stdin: str) -> str:
    marker = "/usr/bin/python3 -I -B -c '"
    first = stdin.index(marker)
    start = stdin.index(marker, first + len(marker)) + len(marker)
    return stdin[start : stdin.index("' ", start)]


def test_post_start_verifier_rejects_noncanonical_systemd_control_group() -> None:
    request = build_sbatch_request(
        _config(**_systemd_nonexclusive_overrides()),
        node="oldlab-4",
    )
    unit = f"loom-job-123-{'a' * 40}.slice"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            _embedded_post_start_verifier(request.stdin),
            unit,
            "1",
            f"/foreign.slice/{unit}",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert (
        'f"/loom.slice/loom-job.slice/loom-job-{unit_match.group(1)}.slice/{unit}"' in request.stdin
    )


def test_embedded_scontrol_parser_returns_exact_allocation_fields() -> None:
    request = build_sbatch_request(
        _config(**_systemd_nonexclusive_overrides()),
        node="oldlab-4",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            _embedded_scontrol_parser(request.stdin),
            "123",
            "loom-dev-a",
            "oldlab-4",
        ],
        input=(
            "JobId=123 JobName=loom Account=loom-dev-a QOS=normal "
            "NodeList=oldlab-4 StartTime=2026-07-30T12:00:00 "
            "Command=/srv/loom --flag value"
        ),
        text=True,
        capture_output=True,
        check=True,
    )

    assert result.stdout.splitlines() == [
        "123",
        "loom-dev-a",
        "oldlab-4",
        "2026-07-30T12:00:00",
    ]


@pytest.mark.parametrize(
    "record",
    [
        "JobId=999 Account=loom-dev-a NodeList=oldlab-4 StartTime=2026-07-30T12:00:00",
        "JobId=123 Account=foreign NodeList=oldlab-4 StartTime=2026-07-30T12:00:00",
        "JobId=123 Account=loom-dev-a NodeList=oldlab-5 StartTime=2026-07-30T12:00:00",
        "JobId=123 Account=loom-dev-a NodeList=oldlab-4 StartTime=Unknown",
        "JobId=123 Account=loom-dev-a NodeList=oldlab-4",
    ],
)
def test_embedded_scontrol_parser_rejects_foreign_or_incomplete_identity(
    record: str,
) -> None:
    request = build_sbatch_request(
        _config(**_systemd_nonexclusive_overrides()),
        node="oldlab-4",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            _embedded_scontrol_parser(request.stdin),
            "123",
            "loom-dev-a",
            "oldlab-4",
        ],
        input=record,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_systemd_sbatch_script_is_valid_bash_and_cleanup_is_project_scoped() -> None:
    request = build_sbatch_request(
        _config(**_systemd_nonexclusive_overrides()),
        node="oldlab-4",
    )

    subprocess.run(
        ["/bin/bash", "-n"],
        input=request.stdin,
        text=True,
        check=True,
    )
    assert "docker stop" not in request.stdin
    assert "docker kill" not in request.stdin
    assert 'docker compose "${compose_args[@]}" down --remove-orphans' in request.stdin


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("slurm_cluster", ""),
        ("slurm_account", ""),
        ("env_id", ""),
        ("resource_generation", 0),
        ("runtime_id", ""),
        ("candidate_id", ""),
        ("candidate_sha", ""),
        ("candidate_tree", ""),
    ],
)
def test_systemd_nonexclusive_rejects_incomplete_registry_binding(
    field: str,
    value: object,
) -> None:
    overrides = _systemd_nonexclusive_overrides()
    overrides[field] = value
    with pytest.raises(ValueError):
        build_sbatch_request(_config(**overrides), node="oldlab-4")


def test_systemd_nonexclusive_accepts_registry_maximum_env_id_only() -> None:
    valid = _systemd_nonexclusive_overrides()
    valid["env_id"] = "denv-" + "a" * 64
    assert build_sbatch_request(_config(**valid), node="oldlab-4")

    invalid = dict(valid)
    invalid["env_id"] = "denv-" + "a" * 65
    with pytest.raises(ValueError, match="closed cluster"):
        build_sbatch_request(_config(**invalid), node="oldlab-4")


@pytest.mark.parametrize("path", ["scontrol", "/usr/bin/../bin/scontrol", "/usr/bin/scontrol,x"])
def test_controller_rejects_unsafe_scontrol_path(path: str) -> None:
    with pytest.raises(ValueError, match="safe absolute path"):
        build_controller_config(
            **_controller_config_kwargs(scontrol_path=path),  # type: ignore[arg-type]
        )


def test_exclusive_sbatch_does_not_require_cgroup_parent() -> None:
    request = build_sbatch_request(_config(exclusive=True), node="oldlab-4")

    export_arg = next(a for a in request.args if a.startswith("--export="))
    assert not any(arg.startswith("--comment=") for arg in request.args)
    assert "LOOM_WORKER_REQUIRE_CGROUP_PARENT" not in export_arg
    assert "unset LOOM_WORKER_CGROUP_PARENT" in request.stdin


def test_nonexclusive_sbatch_rejects_missing_job_pids_max() -> None:
    with pytest.raises(ValueError, match="job_pids_max is required"):
        build_sbatch_request(
            _config(exclusive=False, container_pids=512),
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
    # #896: 0/unset caps must NOT be exported so exclusive pools are unchanged.
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
    assert 'docker compose "${compose_args[@]}" up --no-build &' in request.stdin
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
