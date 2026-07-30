from __future__ import annotations

import io
import json
import subprocess
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergencePlan,
    GB10ConvergenceState,
    GB10HostMutation,
    GB10MutationKind,
)
from loom_cli.rollout.operator.protected_gb10_transport import (
    FixedGB10SSHTransport,
    GB10FleetApplyError,
    GB10TransportTarget,
    _native_worker_build_source,
    _remote_apply_source,
    _remote_observation_source,
    build_fixed_gb10_ssh_transport,
    native_worker_build_ssh_argv,
    retirement_worker_image_observation_source,
)
from tests.loom_cli.rollout.operator.test_protected_migration_component import _published_plan


def _target(host: str = "trt-gb10-1") -> GB10TransportTarget:
    return GB10TransportTarget(
        ssh_target=host,
        repo_path=None,
        env_file_path=None,
        node_agent_service="loom-gb10-node-agent.service",
        retirement_only=True,
    )


def _legacy_target(host: str = "trt-gb10-1") -> GB10TransportTarget:
    return GB10TransportTarget(
        ssh_target=host,
        repo_path=PurePosixPath("/home/qianyi/loom-worker-build-staging"),
        env_file_path=PurePosixPath("/home/qianyi/loom-worker-build-staging/.env"),
        node_agent_service="loom-gb10-node-agent.service",
    )


def _plan(tmp_path: Path, *hosts: str):
    return replace(
        _published_plan(tmp_path),
        gb10_boot_ids={host: f"boot-{index}" for index, host in enumerate(hosts, 1)},
    )


def _transport(run, *targets: GB10TransportTarget) -> FixedGB10SSHTransport:
    return FixedGB10SSHTransport(
        targets=targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        run=run,
        max_concurrency=2,
    )


def _execute_generated_legacy_observer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    service_overrides: dict[str, str] | None = None,
    timer_overrides: dict[str, str] | None = None,
    timer_enabled: bool = True,
) -> tuple[dict[str, object], list[tuple[str, ...]]]:
    service_properties = {
        "LoadState": "loaded",
        "Type": "oneshot",
        "Result": "success",
        "ExecMainStatus": "0",
        "ActiveState": "inactive",
        "SubState": "dead",
        "NeedDaemonReload": "no",
    }
    service_properties.update(service_overrides or {})
    timer_properties = {
        "LoadState": "loaded",
        "ActiveState": "active",
        "SubState": "waiting",
        "Unit": "loom-gb10-node-agent.service",
        "NeedDaemonReload": "no",
    }
    timer_properties.update(timer_overrides or {})
    calls: list[tuple[str, ...]] = []

    def completed(
        argv: list[str],
        *,
        returncode: int = 0,
        stdout: str = "",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, returncode, stdout, "")

    def fake_run(argv, **_kwargs):
        argv = list(argv)
        calls.append(tuple(argv))
        if argv[:3] == ["systemctl", "--user", "show"]:
            if argv[3] == "loom-gb10-node-agent.service":
                stdout = "".join(f"{key}={value}\n" for key, value in service_properties.items())
                return completed(argv, stdout=stdout)
            if argv[3] == "loom-gb10-node-agent.timer":
                stdout = "".join(f"{key}={value}\n" for key, value in timer_properties.items())
                return completed(argv, stdout=stdout)
            if argv[3] == "loom-gb10-worker.service":
                return completed(
                    argv,
                    stdout="LoadState=not-found\nActiveState=inactive\nSubState=dead\n",
                )
            return completed(argv, stdout="255\n")
        if argv[:3] == ["systemctl", "--user", "is-enabled"]:
            if argv[3] == "loom-gb10-node-agent.timer":
                return completed(
                    argv,
                    returncode=0 if timer_enabled else 1,
                    stdout="enabled\n" if timer_enabled else "disabled\n",
                )
            return completed(argv, returncode=1, stdout="not-found\n")
        if argv[:2] == ["loginctl", "show-user"]:
            return completed(argv, stdout="yes\n")
        return completed(argv, returncode=1)

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(subprocess, "run", fake_run)
    target = _legacy_target()
    source = _remote_observation_source(target, _plan(tmp_path, target.ssh_target))
    stdout = io.StringIO()
    with redirect_stdout(stdout):
        exec(compile(source, "<generated-legacy-gb10-observer>", "exec"), {})
    return json.loads(stdout.getvalue()), calls


def test_fixed_transport_observes_exact_candidate_without_remote_mutation(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls: list[tuple[str, ...]] = []
    payload = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": True,
        "environment_exact": True,
        "legacy_absent": False,
        "service_timer_exact": False,
        "service_timer_transient": False,
        "units_exact": True,
    }

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _transport(run, _target()).observe(plan)

    assert set(observed.hosts) == {"trt-gb10-1"}
    assert observed.hosts["trt-gb10-1"].applicable
    assert not observed.hosts["trt-gb10-1"].exact
    assert observed.candidate_source_digest == plan.gb10_unit_digest
    assert len(calls) == 1
    argv = calls[0]
    assert argv[-2] == "trt-gb10-1"
    assert plan.candidate_sha in argv[-1]
    assert plan.plan_digest in argv[-1]
    assert "/srv/loom/staging-shared/candidates" not in argv[-1]
    assert "/home/qianyi/loom-worker-build-staging" not in argv[-1]
    assert "gb10-authority-retirement.json" in argv[-1]
    assert 'systemctl", "--user", "is-active' in argv[-1]


def test_fixed_transport_applies_only_typed_operations_to_planned_host(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")
    calls: list[tuple[str, ...]] = []

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(
            GB10HostMutation(
                "trt-gb10-2",
                (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
            ),
        ),
        blockers={},
        evidence_digest="1" * 64,
    )
    _transport(run, _target(), _target("trt-gb10-2")).apply(plan, convergence)

    assert len(calls) == 1
    assert calls[0][-2] == "trt-gb10-2"
    assert "legacy-retire" in calls[0][-1]
    assert "service-timer" in calls[0][-1]
    assert plan.candidate_sha in calls[0][-1]
    assert "loom-gb10-worker.service" in calls[0][-1]
    assert 'run(["systemctl", "--user", "disable", "--now", unit])' in calls[0][-1]
    assert "git" not in calls[0][-1]


def _retrying_transport(
    run, *targets: GB10TransportTarget, sleeps: list[float], attempts: int = 5
) -> FixedGB10SSHTransport:
    return FixedGB10SSHTransport(
        targets=targets,
        ssh_config=Path("/fixed/ssh-config"),
        identity=Path("/fixed/identity"),
        run=run,
        max_concurrency=2,
        settle_attempts=attempts,
        settle_interval_seconds=0.5,
        sleep=sleeps.append,
    )


def test_fixed_transport_retries_transient_observe_failure(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    payload = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": True,
        "environment_exact": True,
        "legacy_absent": True,
        "service_timer_exact": True,
        "service_timer_transient": False,
        "units_exact": True,
    }

    def run(argv):
        nonlocal calls
        calls += 1
        # The single bastion drops the first two connections (ssh exit 255).
        if calls < 3:
            return subprocess.CompletedProcess(argv, 255, "", "kex_exchange_identification")
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    host = observed.hosts["trt-gb10-1"]
    assert host.exact
    assert host.boot_id == "boot-1"
    assert calls == 3
    assert sleeps == [0.5, 0.5]


def test_fixed_transport_observe_fails_closed_after_exhausting_retries(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []

    def run(argv):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 255, "", "connection reset by peer")

    observed = _retrying_transport(run, _target(), sleeps=sleeps, attempts=4).observe(plan)

    host = observed.hosts["trt-gb10-1"]
    assert host.boot_id == "unavailable"
    assert not host.applicable
    assert calls == 4
    assert sleeps == [0.5, 0.5, 0.5]


def test_fixed_transport_does_not_retry_a_valid_drifted_observation(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    payload = {
        "baseline_ready": False,
        "boot_id": "boot-1",
        "candidate_source_exact": False,
        "checkout_exact": False,
        "environment_exact": False,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": False,
        "units_exact": False,
    }

    def run(argv):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    # A well-formed observation is authoritative -- genuine drift is never retried.
    assert not observed.hosts["trt-gb10-1"].applicable
    assert observed.hosts["trt-gb10-1"].boot_id == "boot-1"
    assert calls == 1
    assert sleeps == []


def test_fixed_transport_settles_a_firing_node_agent_to_exact(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    firing = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": True,
        "environment_exact": True,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": True,
        "units_exact": True,
    }
    settled = {**firing, "service_timer_exact": True, "service_timer_transient": False}

    def run(argv):
        nonlocal calls
        calls += 1
        # The node-agent oneshot fires on the first two observes; then the timer
        # returns to "waiting" and the host reads exact.
        return subprocess.CompletedProcess(
            argv, 0, json.dumps(firing if calls < 3 else settled), ""
        )

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    assert observed.hosts["trt-gb10-1"].exact
    assert calls == 3
    assert sleeps == [0.5, 0.5]


def test_fixed_transport_does_not_settle_a_durably_non_exact_host(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    calls = 0
    sleeps: list[float] = []
    # checkout_exact is False -- a real convergence mutation is required -- so even
    # with the timer firing the observation is authoritative and returned at once.
    payload = {
        "baseline_ready": True,
        "boot_id": "boot-1",
        "candidate_source_exact": True,
        "checkout_exact": False,
        "environment_exact": True,
        "legacy_absent": True,
        "service_timer_exact": False,
        "service_timer_transient": True,
        "units_exact": True,
    }

    def run(argv):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    observed = _retrying_transport(run, _target(), sleeps=sleeps).observe(plan)

    host = observed.hosts["trt-gb10-1"]
    assert not host.exact
    assert host.applicable
    assert calls == 1
    assert sleeps == []


def test_fixed_transport_apply_retries_transient_failure(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")
    calls = 0
    sleeps: list[float] = []

    def run(argv):
        nonlocal calls
        calls += 1
        if calls < 2:
            return subprocess.CompletedProcess(argv, 255, "", "reset")
        return subprocess.CompletedProcess(argv, 0, "", "")

    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(GB10HostMutation("trt-gb10-2", (GB10MutationKind.LEGACY_RETIRE,)),),
        blockers={},
        evidence_digest="1" * 64,
    )
    # A retried transient apply failure must converge without raising.
    _retrying_transport(run, _target(), _target("trt-gb10-2"), sleeps=sleeps, attempts=3).apply(
        plan, convergence
    )

    assert calls == 2
    assert sleeps == [0.5]


def test_fixed_transport_reports_only_validated_failed_hosts(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")

    def run(argv):
        host = argv[-2]
        return subprocess.CompletedProcess(
            argv,
            0 if host == "trt-gb10-1" else 255,
            "",
            "" if host == "trt-gb10-1" else "private transport detail",
        )

    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(
            GB10HostMutation("trt-gb10-1", (GB10MutationKind.LEGACY_RETIRE,)),
            GB10HostMutation("trt-gb10-2", (GB10MutationKind.LEGACY_RETIRE,)),
        ),
        blockers={},
        evidence_digest="1" * 64,
    )

    with pytest.raises(GB10FleetApplyError) as caught:
        _transport(run, _target(), _target("trt-gb10-2")).apply(plan, convergence)

    assert caught.value.failed_hosts == ("trt-gb10-2",)
    assert "private transport detail" not in str(caught.value)


def test_fixed_remote_programs_compile_and_apply_rejects_noncanonical_order(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    target = _target()
    compile(_remote_observation_source(target, plan), "<gb10-observe>", "exec")
    compile(
        _remote_apply_source(
            target,
            plan,
            (GB10MutationKind.CHECKOUT, GB10MutationKind.SERVICE_TIMER),
        ),
        "<gb10-apply>",
        "exec",
    )
    transport = _transport(
        lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
        target,
    )
    convergence = GB10ConvergencePlan(
        state=GB10ConvergenceState.READY,
        mutations=(
            GB10HostMutation(
                "trt-gb10-1",
                (GB10MutationKind.UNITS, GB10MutationKind.ENVIRONMENT),
            ),
        ),
        blockers={},
        evidence_digest="2" * 64,
    )
    with pytest.raises(RuntimeError, match="failed safely"):
        transport.apply(plan, convergence)


def test_remote_retirement_program_has_no_candidate_or_git_dependency(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    observation = _remote_observation_source(_target(), plan)
    apply = _remote_apply_source(
        _target(),
        plan,
        (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
    )

    assert "/srv/loom/staging-shared/candidates" not in observation
    assert plan.candidate_sha in observation
    assert plan.plan_digest in observation
    assert "git" not in observation
    assert "git" not in apply
    assert "gb10-authority-retirement.lock" in apply
    assert 'committed["phase"] = "committed"' in apply


def test_remote_retirement_never_updates_human_checkout_or_starts_timer(tmp_path) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")
    apply = _remote_apply_source(
        _target(),
        plan,
        (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER),
    )

    assert "/home/qianyi/loom-worker-build-staging" not in apply
    assert "/srv/loom/staging-shared/candidates" not in apply
    assert '"start"' not in apply
    assert '"enable"' not in apply
    assert '"disable", "--now"' in apply
    assert "daemon-reload" in apply


def test_same_candidate_second_attempt_reuses_durable_retirement_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    boot_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    original_read_text = Path.read_text

    def read_text(path: Path, *args, **kwargs):
        if path == Path("/proc/sys/kernel/random/boot_id"):
            return boot_id + "\n"
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    first = replace(
        _plan(tmp_path, target.ssh_target),
        gb10_boot_ids={target.ssh_target: boot_id},
    )
    second = replace(
        first,
        attempt_number=first.attempt_number + 1,
        plan_digest="f" * 64,
    )
    mutation_calls: list[tuple[str, ...]] = []

    def run(argv, **_kwargs):
        command = tuple(argv)
        if command[:3] == ("systemctl", "--user", "is-active"):
            return subprocess.CompletedProcess(argv, 3, "inactive\n", "")
        if command[:3] == ("systemctl", "--user", "is-enabled"):
            return subprocess.CompletedProcess(argv, 1, "disabled\n", "")
        if command[:2] == ("loginctl", "show-user"):
            return subprocess.CompletedProcess(argv, 0, "yes\n", "")
        if command[:3] == ("systemctl", "--user", "disable") or command[:3] in {
            ("systemctl", "--user", "stop"),
            ("systemctl", "--user", "reset-failed"),
            ("systemctl", "--user", "daemon-reload"),
        }:
            mutation_calls.append(command)
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(subprocess, "run", run)
    operations = (GB10MutationKind.LEGACY_RETIRE, GB10MutationKind.SERVICE_TIMER)
    exec(
        compile(
            _remote_apply_source(target, first, operations),
            "<first-gb10-retirement>",
            "exec",
        ),
        {},
    )
    first_mutation_count = len(mutation_calls)
    assert first_mutation_count > 0

    with pytest.raises(SystemExit) as stopped:
        exec(
            compile(
                _remote_apply_source(target, second, operations),
                "<second-gb10-retirement>",
                "exec",
            ),
            {},
        )

    assert stopped.value.code == 0
    assert len(mutation_calls) == first_mutation_count
    receipt = json.loads(
        (tmp_path / ".local/state/loom-staging-rollout/gb10-authority-retirement.json").read_bytes()
    )
    assert receipt["plan_digest"] == first.plan_digest
    assert receipt["candidate_sha"] == second.candidate_sha
    assert receipt["candidate_tree"] == second.candidate_tree


def test_fixed_transport_aggregates_unavailable_hosts_and_rejects_inventory_drift(
    tmp_path,
) -> None:
    targets = (_target(), _target("trt-gb10-2"))
    plan = _plan(tmp_path, "trt-gb10-1", "trt-gb10-2")
    transport = _transport(
        lambda argv: subprocess.CompletedProcess(argv, 255, "", "unavailable"),
        *targets,
    )

    observed = transport.observe(plan)
    assert not any(host.applicable for host in observed.hosts.values())
    assert {host.boot_id for host in observed.hosts.values()} == {"unavailable"}

    with pytest.raises(ValueError, match="inventory drifted"):
        transport.observe(replace(plan, gb10_boot_ids={"trt-gb10-1": "boot-1"}))


@pytest.mark.parametrize(
    ("repo", "env_file"),
    (
        ("relative", "/home/qianyi/loom-worker-build-staging/.env"),
        ("/home/qianyi/repo/../escape", "/home/qianyi/escape/.env"),
        ("/home/qianyi/repo", "/tmp/.env"),
    ),
)
def test_fixed_transport_target_rejects_path_authority_drift(repo, env_file) -> None:
    with pytest.raises(ValueError, match="outside fixed authority"):
        GB10TransportTarget(
            ssh_target="trt-gb10-1",
            repo_path=PurePosixPath(repo),
            env_file_path=PurePosixPath(env_file),
            node_agent_service="loom-gb10-node-agent.service",
        )


def test_fixed_transport_factory_binds_checked_in_staging_inventory() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    hosts = tuple(f"trt-gb10-{index}" for index in range(1, 16))
    transport = build_fixed_gb10_ssh_transport(
        repo_root / "deploy/environments/staging.cluster.toml",
        expected_hosts=hosts,
        run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
        max_concurrency=4,
    )

    assert tuple(target.ssh_target for target in transport.targets) == hosts
    assert all(target.retirement_only for target in transport.targets)
    assert all(
        target.repo_path is None and target.env_file_path is None for target in transport.targets
    )
    assert "/home/qianyi" not in repr(transport.targets)
    assert transport.identity == Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
    assert transport.ssh_config == (repo_root / "deploy/worker-pools/gb10/ssh_config").resolve()


@pytest.mark.parametrize("forbidden_field", ("repo_path", "env_file_path", "repo_url"))
def test_external_transport_factory_rejects_legacy_candidate_authority(
    tmp_path: Path,
    forbidden_field: str,
) -> None:
    repo_root = Path(__file__).resolve().parents[4]
    source = repo_root / "deploy/environments/staging.cluster.toml"
    raw = source.read_text(encoding="utf-8")
    raw = raw.replace(
        '{ ssh_target = "trt-gb10-1",',
        f'{{ ssh_target = "trt-gb10-1", {forbidden_field} = "/forbidden",',
        1,
    )
    config = tmp_path / "staging.cluster.toml"
    config.write_text(
        raw.replace(
            'env_state_profile = "../environment-state/staging.toml"',
            f'env_state_profile = "{repo_root / "deploy/environment-state/staging.toml"}"',
        ).replace(
            'ssh_config = "../worker-pools/gb10/ssh_config"',
            f'ssh_config = "{repo_root / "deploy/worker-pools/gb10/ssh_config"}"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="host authority is invalid"):
        build_fixed_gb10_ssh_transport(
            config,
            expected_hosts=tuple(f"trt-gb10-{index}" for index in range(1, 16)),
            run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
            max_concurrency=4,
        )


def test_legacy_transport_explicitly_retains_existing_user_path_contract(tmp_path: Path) -> None:
    target = _legacy_target()
    plan = _plan(tmp_path, target.ssh_target)

    observation = _remote_observation_source(target, plan)
    apply = _remote_apply_source(
        target,
        plan,
        (
            GB10MutationKind.CHECKOUT,
            GB10MutationKind.ENVIRONMENT,
            GB10MutationKind.UNITS,
            GB10MutationKind.LEGACY_RETIRE,
            GB10MutationKind.SERVICE_TIMER,
        ),
    )

    assert "/home/qianyi/loom-worker-build-staging" in observation
    assert "/shared_work2/qianyi/.loom-staging-rollout/worker-repos" in observation
    assert "/home/qianyi/loom-worker-build-staging" in apply
    assert '"checkout"' in apply
    assert '"environment"' in apply
    assert '"units"' in apply


@pytest.mark.parametrize(
    (
        "service_overrides",
        "timer_overrides",
        "timer_enabled",
        "expected_exact",
        "expected_transient",
    ),
    (
        ({}, {}, True, True, False),
        (
            {"ActiveState": "active", "SubState": "running"},
            {"SubState": "running"},
            True,
            False,
            True,
        ),
        (
            {"ActiveState": "active", "SubState": "running"},
            {},
            True,
            False,
            False,
        ),
        (
            {
                "Result": "",
                "ExecMainStatus": "",
                "ActiveState": "activating",
                "SubState": "start",
            },
            {"SubState": "running"},
            True,
            False,
            True,
        ),
        (
            {},
            {"SubState": "running"},
            True,
            False,
            False,
        ),
        (
            {
                "Result": "failed",
                "ExecMainStatus": "1",
                "ActiveState": "failed",
                "SubState": "failed",
            },
            {"SubState": "running"},
            True,
            False,
            False,
        ),
        (
            {
                "Result": "",
                "ExecMainStatus": "",
                "ActiveState": "activating",
                "SubState": "start",
                "NeedDaemonReload": "yes",
            },
            {"SubState": "running"},
            True,
            False,
            False,
        ),
        (
            {
                "Result": "",
                "ExecMainStatus": "",
                "ActiveState": "activating",
                "SubState": "start",
            },
            {"SubState": "running", "NeedDaemonReload": "yes"},
            True,
            False,
            False,
        ),
        (
            {
                "Result": "",
                "ExecMainStatus": "",
                "ActiveState": "activating",
                "SubState": "start",
            },
            {"SubState": "running"},
            False,
            False,
            False,
        ),
        ({"Type": "simple"}, {"SubState": "running"}, True, False, False),
    ),
)
def test_generated_legacy_observer_classifies_complete_service_timer_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    service_overrides: dict[str, str],
    timer_overrides: dict[str, str],
    timer_enabled: bool,
    expected_exact: bool,
    expected_transient: bool,
) -> None:
    payload, calls = _execute_generated_legacy_observer(
        tmp_path,
        monkeypatch,
        service_overrides=service_overrides,
        timer_overrides=timer_overrides,
        timer_enabled=timer_enabled,
    )

    assert payload["service_timer_exact"] is expected_exact
    assert payload["service_timer_transient"] is expected_transient
    service_show = next(
        argv
        for argv in calls
        if argv[:4]
        == (
            "systemctl",
            "--user",
            "show",
            "loom-gb10-node-agent.service",
        )
    )
    assert {
        "--property=Type",
        "--property=ActiveState",
        "--property=SubState",
        "--property=NeedDaemonReload",
    } <= set(service_show)


def test_generated_legacy_observer_rejects_unclassified_timer_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    elapsed, _ = _execute_generated_legacy_observer(
        tmp_path,
        monkeypatch,
        timer_overrides={"SubState": "elapsed"},
    )

    assert elapsed["service_timer_exact"] is False
    assert elapsed["service_timer_transient"] is False


def test_external_transport_target_rejects_any_legacy_human_path() -> None:
    with pytest.raises(ValueError, match="outside fixed authority"):
        GB10TransportTarget(
            ssh_target="trt-gb10-1",
            repo_path=PurePosixPath("/home/qianyi/loom-worker-build-staging"),
            env_file_path=PurePosixPath("/home/qianyi/loom-worker-build-staging/.env"),
            node_agent_service="loom-gb10-node-agent.service",
            retirement_only=True,
        )


def test_worker_image_observer_binds_fixed_env_and_classic_store(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path, "trt-gb10-1")

    source = retirement_worker_image_observation_source(plan)

    assert (
        "/srv/loom/staging-shared/generated/"
        f"staging-gb10-worker-staging-{plan.candidate_sha[:7]}.env"
    ) in source
    assert '["docker", "info", "--format", "{{.Driver}}"]' in source
    assert 'driver.stdout.strip() != "overlay2"' in source
    assert 'row.get("Id") == image_id' in source
    assert 'config.get("Cmd") == ["python", "-m", "loom_worker"]' in source


def test_native_worker_build_is_fixed_qianyi_direct_docker_transport() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    candidate_sha = "b" * 40
    source_sha256 = "c" * 64

    argv = native_worker_build_ssh_argv(
        (repo_root / "deploy/environments/staging.cluster.toml").resolve(),
        candidate_sha=candidate_sha,
        image_tag="staging-bbbbbbb",
        source_sha256=source_sha256,
    )
    source = _native_worker_build_source(
        candidate_sha=candidate_sha,
        image_tag="staging-bbbbbbb",
        source_sha256=source_sha256,
    )
    compile(source, "<native-worker-build>", "exec")

    assert argv[-2] == "trt-gb10-1"
    assert "UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts" in argv
    assert "/var/lib/loom-staging-rollout/gb10-deploy-ed25519" in argv
    assert '"/usr/bin/docker", "info"' in source
    assert '"/usr/bin/docker", "buildx", "build"' in source
    assert "sudo" not in source
    assert "overlay2 arm64" in source
    assert "expected_source_sha256" in source
    assert "max_source_bytes = 1024 * 1024 * 1024" in source
    assert "diagnostic_size > 65536" in source
    assert "fcntl.LOCK_EX | fcntl.LOCK_NB" in source
    assert 'state_root.glob("work-*")' in source
    assert "shutil.rmtree(work, ignore_errors=True)" in source
