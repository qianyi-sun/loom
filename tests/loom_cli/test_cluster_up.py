"""`loom cluster up` — orchestration tests (#76 Phase 3).

The unit under test composes `collect_preflight`, `render_manifests`,
`apply_manifests` (kubectl subprocess), and `wait_for_ready` (status
polling loop). We mock the subprocess + the k8s clients to drive
each branch of the orchestration without needing a real cluster.
"""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from loom_cli import cluster_cmd
from loom_cli.__main__ import main
from loom_cli.cluster_cmd import (
    ApplyResult,
    ClusterStatus,
    ComponentStatus,
    DeploymentImageCheck,
    apply_manifests,
    recover_sandbox_deadline_pods,
    wait_for_ready,
)

# ──────────────────────────────────────────────────────────────────────
# apply_manifests — subprocess wrapper
# ──────────────────────────────────────────────────────────────────────


def test_apply_manifests_invokes_kubectl_with_correct_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The kubectl invocation MUST include `apply -f - -n NS` plus
    `--context X` only when context is non-None. Pipes the YAML
    text via stdin."""
    captured: dict[str, Any] = {}

    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = list(cmd)
        captured["input"] = kwargs.get("input")
        # Simulate kubectl reporting one configured object.
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout="deployment.apps/loom-service configured\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr(
        "shutil.which",
        lambda _bin: "/usr/local/bin/kubectl",
    )

    result = apply_manifests(
        "apiVersion: v1\nkind: ConfigMap\n",
        "loom",
        context="prod",
    )
    assert result.returncode == 0
    assert result.summary_lines == [
        "deployment.apps/loom-service configured",
    ]
    assert captured["cmd"][:5] == [
        "kubectl",
        "apply",
        "-n",
        "loom",
        "-f",
    ]
    assert "--context" in captured["cmd"]
    assert "prod" in captured["cmd"]
    assert "ConfigMap" in captured["input"]


def test_apply_manifests_omits_context_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = list(cmd)
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda _bin: "/x/kubectl")

    apply_manifests("---", "loom", context=None)
    assert "--context" not in captured["cmd"]


def test_apply_manifests_propagates_nonzero_returncode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stdout="",
            stderr='error: secret "loom-secrets" not found\n',
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda _bin: "/x/kubectl")

    result = apply_manifests("---", "loom", context=None)
    assert result.returncode == 1
    assert "loom-secrets" in result.stderr


def test_apply_manifests_raises_when_kubectl_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If kubectl isn't on PATH, surface a friendly install hint
    instead of a cryptic FileNotFoundError from subprocess."""
    monkeypatch.setattr("shutil.which", lambda _bin: None)
    with pytest.raises(RuntimeError, match="kubectl is required"):
        apply_manifests("---", "loom", context=None)


# ──────────────────────────────────────────────────────────────────────
# wait_for_ready — polling loop
# ──────────────────────────────────────────────────────────────────────


class _FakeApi:
    """Stand-in for the apps/networking/core clients. The polling
    loop only calls `collect_status`, which only touches the clients
    via the workload methods. We patch `collect_status` directly so
    the fake API just needs to exist."""


class _FakeApiException(Exception):  # noqa: N818
    def __init__(self, status: int) -> None:
        super().__init__(f"status={status}")
        self.status = status


_FakeApiException.__name__ = "ApiException"


class _FakeAppsForPrune:
    def __init__(
        self,
        deployments: set[str],
        stateful_sets: set[str] | None = None,
    ) -> None:
        self.deployments = set(deployments)
        self.stateful_sets: set[str] = set(stateful_sets or ())
        self.deleted_deployments: list[str] = []
        self.deleted_stateful_sets: list[str] = []

    def delete_namespaced_deployment(self, *, name: str, namespace: str) -> None:
        if name not in self.deployments:
            raise _FakeApiException(404)
        self.deleted_deployments.append(f"{namespace}/{name}")
        self.deployments.remove(name)

    def delete_namespaced_stateful_set(self, *, name: str, namespace: str) -> None:
        if name not in self.stateful_sets:
            raise _FakeApiException(404)
        self.deleted_stateful_sets.append(f"{namespace}/{name}")
        self.stateful_sets.remove(name)


class _FakeNetworkingForPrune:
    def __init__(self, network_policies: set[str]) -> None:
        self.network_policies = set(network_policies)
        self.deleted_network_policies: list[str] = []

    def delete_namespaced_network_policy(self, *, name: str, namespace: str) -> None:
        if name not in self.network_policies:
            raise _FakeApiException(404)
        self.deleted_network_policies.append(f"{namespace}/{name}")
        self.network_policies.remove(name)


class _FakeCoreForPrune:
    def __init__(self, pvcs: set[str], services: set[str] | None = None) -> None:
        self.pvcs = set(pvcs)
        self.services: set[str] = set(services or ())
        self.deleted_pvcs: list[str] = []
        self.deleted_services: list[str] = []

    def read_namespaced_persistent_volume_claim(self, *, name: str, namespace: str) -> object:
        if name not in self.pvcs:
            raise _FakeApiException(404)
        return object()

    def delete_namespaced_persistent_volume_claim(self, *, name: str, namespace: str) -> None:
        self.deleted_pvcs.append(f"{namespace}/{name}")

    def delete_namespaced_service(self, *, name: str, namespace: str) -> None:
        if name not in self.services:
            raise _FakeApiException(404)
        self.deleted_services.append(f"{namespace}/{name}")
        self.services.remove(name)


class _FakePruneResult:
    def __init__(
        self,
        *,
        deleted: list[str],
        retained: list[str],
        not_found: list[str],
        failed: list[tuple[str, str]],
    ) -> None:
        self.deleted = deleted
        self.retained = retained
        self.not_found = not_found
        self.failed = failed

    @property
    def has_evidence(self) -> bool:
        return bool(self.deleted or self.retained or self.not_found or self.failed)

    @property
    def ok(self) -> bool:
        return not self.failed


def test_prune_disabled_worker_resources_deletes_workload_and_policy_but_retains_pvc() -> None:
    # Legacy Deployment shape (static-host-path profile).
    apps = _FakeAppsForPrune({"loom-worker"})
    net = _FakeNetworkingForPrune({"loom-worker"})
    core = _FakeCoreForPrune({"loom-worker-trajectories"})

    result = cluster_cmd.prune_disabled_profile_resources(
        apps,
        net,
        core,
        cluster_cmd.ClusterConfig(),
        namespace="loom-staging",
    )

    assert apps.deleted_deployments == ["loom-staging/loom-worker"]
    assert apps.deleted_stateful_sets == []
    assert net.deleted_network_policies == ["loom-staging/loom-worker"]
    assert core.deleted_pvcs == []
    assert core.deleted_services == []
    assert result.deleted == [
        "deployment.apps/loom-worker",
        "networkpolicy.networking.k8s.io/loom-worker",
    ]
    assert result.retained == ["persistentvolumeclaim/loom-worker-trajectories"]
    assert result.failed == []


def test_prune_disabled_worker_resources_deletes_statefulset_and_headless_service() -> None:
    """Dynamic-storage profiles render loom-worker as a StatefulSet
    with a headless Service (#673). Prune must clean both — otherwise
    disabled-worker profiles leak the StatefulSet and its per-pod PVCs
    stay bound."""
    apps = _FakeAppsForPrune(set(), stateful_sets={"loom-worker"})
    net = _FakeNetworkingForPrune({"loom-worker"})
    core = _FakeCoreForPrune(
        {"loom-worker-trajectories"},
        services={"loom-worker"},
    )

    result = cluster_cmd.prune_disabled_profile_resources(
        apps,
        net,
        core,
        cluster_cmd.ClusterConfig(),
        namespace="loom-staging",
    )

    assert apps.deleted_deployments == []
    assert apps.deleted_stateful_sets == ["loom-staging/loom-worker"]
    assert core.deleted_services == ["loom-staging/loom-worker"]
    assert net.deleted_network_policies == ["loom-staging/loom-worker"]
    assert core.deleted_pvcs == []
    assert result.deleted == [
        "statefulset.apps/loom-worker",
        "service/loom-worker",
        "networkpolicy.networking.k8s.io/loom-worker",
    ]
    assert result.retained == ["persistentvolumeclaim/loom-worker-trajectories"]
    assert result.failed == []


def test_wait_for_ready_returns_on_first_pass_when_already_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: collect_status reports all_ready=True on the first
    iteration; no sleeps."""
    sleeps: list[float] = []
    nows: list[float] = [0.0]  # never advances

    def _collect(apps, net, core, ns, *, context):  # type: ignore[no-untyped-def]
        return ClusterStatus(
            namespace=ns,
            context=context,
            components=[
                ComponentStatus(
                    name="loom-service",
                    kind="Deployment",
                    ready=2,
                    desired=2,
                    available=True,
                    generation=1,
                    observed_generation=1,
                    updated=2,
                )
            ],
            ingresses=[],
            warnings=[],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_status", _collect)

    status = wait_for_ready(
        _FakeApi(),
        _FakeApi(),
        _FakeApi(),
        namespace="loom",
        context=None,
        timeout_sec=60,
        _sleep=lambda s: sleeps.append(s),
        _now=lambda: nows[0],
    )
    assert status.all_ready
    assert sleeps == []  # didn't sleep


def test_wait_for_ready_polls_until_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First two polls return not-ready, third returns ready. Verify
    we sleep between polls and stop on first ready result."""
    sleeps: list[float] = []
    call_count = {"n": 0}

    def _collect(apps, net, core, ns, *, context):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        ready_replicas = 2 if call_count["n"] >= 3 else 0
        return ClusterStatus(
            namespace=ns,
            context=context,
            components=[
                ComponentStatus(
                    name="loom-service",
                    kind="Deployment",
                    ready=ready_replicas,
                    desired=2,
                    available=ready_replicas > 0,
                    generation=1,
                    observed_generation=1,
                    updated=2 if ready_replicas == 2 else 0,
                )
            ],
            ingresses=[],
            warnings=[],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_status", _collect)

    nows = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
    status = wait_for_ready(
        _FakeApi(),
        _FakeApi(),
        _FakeApi(),
        namespace="loom",
        context=None,
        timeout_sec=60,
        poll_interval_sec=1.0,
        _sleep=lambda s: sleeps.append(s),
        _now=lambda: next(nows),
    )
    assert status.all_ready
    assert call_count["n"] == 3
    # Two sleeps between three polls.
    assert sleeps == [1.0, 1.0]


def test_wait_for_ready_returns_unready_status_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Components never become ready; the wait returns the last
    observed status at deadline rather than raising."""
    sleeps: list[float] = []

    def _collect(apps, net, core, ns, *, context):  # type: ignore[no-untyped-def]
        return ClusterStatus(
            namespace=ns,
            context=context,
            components=[
                ComponentStatus(
                    name="loom-service",
                    kind="Deployment",
                    ready=0,
                    desired=2,
                    available=False,
                    generation=1,
                    observed_generation=1,
                    updated=0,
                )
            ],
            ingresses=[],
            warnings=[],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_status", _collect)

    # Clock advances past the 5s deadline on the second `_now` call.
    nows = iter([0.0, 0.0, 6.0, 6.0, 6.0])
    status = wait_for_ready(
        _FakeApi(),
        _FakeApi(),
        _FakeApi(),
        namespace="loom",
        context=None,
        timeout_sec=5,
        poll_interval_sec=1.0,
        _sleep=lambda s: sleeps.append(s),
        _now=lambda: next(nows),
    )
    assert not status.all_ready


def test_recover_sandbox_deadline_pods_is_bounded_and_classified_only() -> None:
    class _Core:
        def __init__(self) -> None:
            self.deleted: list[tuple[str, str]] = []

        def delete_namespaced_pod(self, *, name: str, namespace: str) -> None:
            self.deleted.append((namespace, name))

    worker = ComponentStatus(
        name="loom-worker",
        kind="Deployment",
        ready=5,
        desired=6,
        available=True,
        generation=1,
        observed_generation=1,
        updated=5,
    )
    worker.failure_class = "node_runtime_sandbox_deadline"
    worker.runtime_failure_diagnostics = [
        {"pod": "loom-worker-a"},
        {"pod": "loom-worker-b"},
        {"pod": "loom-worker-c"},
    ]
    service = ComponentStatus(
        name="loom-service",
        kind="Deployment",
        ready=0,
        desired=1,
        available=False,
        generation=1,
        observed_generation=1,
        updated=0,
        note="pod-health: loom-service CrashLoopBackOff",
    )
    service.runtime_failure_diagnostics = [{"pod": "loom-service-bad"}]
    status = ClusterStatus(
        namespace="loom",
        context=None,
        components=[worker, service],
        ingresses=[],
        warnings=[],
    )
    core = _Core()

    recovered = recover_sandbox_deadline_pods(
        core,
        "loom",
        status,
        max_pods=2,
        dry_run=False,
    )

    assert recovered == ["loom-worker-a", "loom-worker-b"]
    assert core.deleted == [
        ("loom", "loom-worker-a"),
        ("loom", "loom-worker-b"),
    ]

    dry_run = recover_sandbox_deadline_pods(
        core,
        "loom",
        status,
        max_pods=3,
        dry_run=True,
    )

    assert dry_run == ["loom-worker-a", "loom-worker-b", "loom-worker-c"]
    assert core.deleted == [
        ("loom", "loom-worker-a"),
        ("loom", "loom-worker-b"),
    ]


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch — orchestration
# ──────────────────────────────────────────────────────────────────────


def _all_ready_status(ns: str = "loom") -> ClusterStatus:
    """Helper: a fully-ready snapshot with one Deployment."""
    return ClusterStatus(
        namespace=ns,
        context=None,
        components=[
            ComponentStatus(
                name="loom-service",
                kind="Deployment",
                ready=2,
                desired=2,
                available=True,
                generation=1,
                observed_generation=1,
                updated=2,
            )
        ],
        ingresses=[],
        warnings=[],
    )


def _patch_full_up_path(
    monkeypatch: pytest.MonkeyPatch,
    *,
    preflight_any_fail: bool = False,
    target_doctor_fail: bool = False,
    apply_returncode: int = 0,
    final_ready: bool = True,
) -> dict[str, Any]:
    """Stub every external dependency: k8s clients (4-tuple),
    collect_preflight, apply_manifests, collect_status. Returns a
    `captures` dict tests can inspect for what got called with what."""
    captures: dict[str, Any] = {}
    apps_client = object()

    # k8s clients — opaque sentinels; preflight/status are patched.
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (apps_client, object(), object(), object()),
    )

    from loom_cli.cluster_cmd import PreflightCheck, PreflightReport

    preflight_report = PreflightReport(
        namespace="loom",
        context=None,
        checks=[
            PreflightCheck(
                name="namespace-exists",
                outcome="fail" if preflight_any_fail else "pass",
                detail="ok",
                remediation="kubectl create namespace loom" if preflight_any_fail else None,
            )
        ],
    )

    def _collect_preflight(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["preflight_called"] = True
        captures["preflight_kwargs"] = kwargs
        return preflight_report

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_preflight",
        _collect_preflight,
    )

    def _append_target_schema_doctor_check(report, **kwargs):  # type: ignore[no-untyped-def]
        captures["target_doctor_called"] = True
        from loom_cli.cluster_cmd import PreflightCheck

        report.checks.append(
            PreflightCheck(
                name="schema-doctor",
                outcome="fail" if target_doctor_fail else "pass",
                detail=(
                    "1 schema violation(s)" if target_doctor_fail else "schema reconciliation clean"
                ),
                remediation=(
                    "  - missing_env: LOOM_CP_EXAMPLE: Rendered Deployment missing env"
                    if target_doctor_fail
                    else None
                ),
            )
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._append_target_schema_doctor_check",
        _append_target_schema_doctor_check,
    )

    def _apply(yaml_text, ns, *, context, extra_args=(), apps_v1=None):  # type: ignore[no-untyped-def]
        captures["apply_yaml_len"] = len(yaml_text)
        captures["apply_yaml_text"] = yaml_text
        captures["apply_ns"] = ns
        captures["apply_apps_v1"] = apps_v1
        return ApplyResult(
            returncode=apply_returncode,
            summary_lines=(
                ["deployment.apps/loom-service configured"] if apply_returncode == 0 else []
            ),
            stderr="error: x" if apply_returncode != 0 else "",
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.apply_manifests", _apply)

    final_status = (
        _all_ready_status()
        if final_ready
        else ClusterStatus(
            namespace="loom",
            context=None,
            components=[
                ComponentStatus(
                    name="loom-service",
                    kind="Deployment",
                    ready=0,
                    desired=2,
                    available=False,
                    generation=1,
                    observed_generation=1,
                    updated=0,
                )
            ],
            ingresses=[],
            warnings=[],
        )
    )

    def _wait(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["waited"] = True
        return final_status

    monkeypatch.setattr("loom_cli.cluster_cmd.wait_for_ready", _wait)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.rendered_image_checks",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.prune_disabled_profile_resources",
        lambda *_args, **_kwargs: _FakePruneResult(
            deleted=[],
            retained=[],
            not_found=[],
            failed=[],
        ),
    )
    return captures


def test_cli_up_happy_path(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preflight pass → render → apply → wait → all_ready → exit 0."""
    captures = _patch_full_up_path(monkeypatch)
    rc = main(["cluster", "up"])
    assert rc == 0
    assert captures.get("preflight_called") is True
    assert captures.get("target_doctor_called") is True
    assert captures.get("apply_apps_v1") is not None
    assert captures.get("waited") is True
    out = capsys.readouterr().out
    assert "Preflight" in out
    assert "loom-service configured" in out


def test_cli_up_prunes_disabled_worker_resources_before_wait(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    prune_calls: list[dict[str, Any]] = []

    def _prune(apps, net, core, config, *, namespace):  # type: ignore[no-untyped-def]
        prune_calls.append(
            {
                "apps": apps,
                "net": net,
                "core": core,
                "namespace": namespace,
                "k8s_worker_enabled": config.k8s_worker.enabled,
            }
        )
        return _FakePruneResult(
            deleted=[
                "deployment.apps/loom-worker",
                "networkpolicy.networking.k8s.io/loom-worker",
            ],
            retained=["persistentvolumeclaim/loom-worker-trajectories"],
            not_found=[],
            failed=[],
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.prune_disabled_profile_resources",
        _prune,
        raising=False,
    )

    rc = main(["cluster", "up"])

    assert rc == 0
    assert captures.get("waited") is True
    assert prune_calls == [
        {
            "apps": prune_calls[0]["apps"],
            "net": prune_calls[0]["net"],
            "core": prune_calls[0]["core"],
            "namespace": "loom",
            "k8s_worker_enabled": False,
        }
    ]
    out = capsys.readouterr().out
    assert "Pruned disabled-profile resources:" in out
    assert "deployment.apps/loom-worker deleted" in out
    assert "persistentvolumeclaim/loom-worker-trajectories retained" in out


def test_cli_up_prints_deployment_image_convergence_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_up_path(monkeypatch)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.rendered_image_checks",
        lambda *_args, **_kwargs: [
            DeploymentImageCheck(
                deployment="loom-worker",
                container="worker",
                expected_image="loom-worker:staging-expected",
                live_image="loom-worker:staging-expected",
            ),
        ],
    )

    rc = main(["cluster", "up"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Deployment image convergence verified" in out
    assert (
        "loom-worker/worker: "
        "rendered=loom-worker:staging-expected "
        "live=loom-worker:staging-expected"
    ) in out


def test_cli_up_preflight_fail_blocks_apply(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preflight any_fail=True → refuse to apply; exit 1."""
    captures = _patch_full_up_path(
        monkeypatch,
        preflight_any_fail=True,
    )
    rc = main(["cluster", "up"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "preflight checks failed" in err
    # Apply MUST NOT have been called.
    assert "apply_ns" not in captures


def test_cli_up_target_schema_doctor_fail_blocks_apply(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_up_path(monkeypatch, target_doctor_fail=True)

    rc = main(["cluster", "up"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "preflight checks failed" in err
    assert "schema-doctor" in err
    assert captures.get("target_doctor_called") is True
    assert "apply_ns" not in captures


def test_cli_up_skip_preflight_bypasses_check(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--skip-preflight is the escape hatch for known-transient
    preflight failures (e.g. operator just created a Secret that
    hasn't propagated)."""
    captures = _patch_full_up_path(
        monkeypatch,
        preflight_any_fail=True,  # would fail
    )
    rc = main(["cluster", "up", "--skip-preflight"])
    assert rc == 0
    assert captures.get("preflight_called") is None
    assert captures.get("apply_ns") == "loom"


@pytest.mark.parametrize(
    ("namespace", "skip_preflight"),
    [
        ("loom-staging", False),
        ("loom-staging", True),
        ("loom-production", False),
        ("loom-production", True),
    ],
)
def test_cli_up_rejects_protected_namespace_environment_downgrade_before_lock_or_apply(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    namespace: str,
    skip_preflight: bool,
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    lock_attempts: list[str] = []

    def _unexpected_lock(*args, **kwargs):  # type: ignore[no-untyped-def]
        lock_attempts.append("called")
        return None

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._acquire_protected_rollout_lock",
        _unexpected_lock,
    )

    command = [
        "cluster",
        "up",
        "--namespace",
        namespace,
        "--environment",
        "development",
        "--no-wait",
    ]
    if skip_preflight:
        command.append("--skip-preflight")

    rc = main(command)

    assert rc == 1
    assert lock_attempts == []
    assert "apply_ns" not in captures
    assert "protected-target-environment" in capsys.readouterr().err


def test_cli_up_apply_failure_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """kubectl apply non-zero → exit 1 with stderr surfaced."""
    _patch_full_up_path(monkeypatch, apply_returncode=1)
    rc = main(["cluster", "up"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "kubectl apply failed" in err


def test_cli_up_no_wait_skips_readiness_check(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    rc = main(["cluster", "up", "--no-wait"])
    assert rc == 0
    assert "Skipping readiness wait" in capsys.readouterr().out
    assert captures.get("waited") is None


def test_cli_up_timeout_returns_1_when_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_up_path(monkeypatch, final_ready=False)
    rc = main(["cluster", "up", "--timeout", "30"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "did not reach ready state" in err


def test_cli_up_retries_once_after_bounded_sandbox_deadline_recovery(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#206: protected rollout recovery is automated and bounded.

    Operators opt in to pod sandbox deadline recovery; after the normal
    preflight/apply path, `cluster up` should delete only classified
    sandbox-deadline pods, then run one more readiness wait.
    """
    _patch_full_up_path(monkeypatch, final_ready=False)
    stalled = ClusterStatus(
        namespace="loom",
        context=None,
        components=[
            ComponentStatus(
                name="loom-worker",
                kind="Deployment",
                ready=5,
                desired=6,
                available=True,
                generation=1,
                observed_generation=1,
                updated=5,
                note=("node-runtime-sandbox-deadline: loom-worker-old FailedKillPod"),
            ),
        ],
        ingresses=[],
        warnings=[],
    )
    stalled.components[0].failure_class = "node_runtime_sandbox_deadline"
    stalled.components[0].runtime_failure_diagnostics = [
        {
            "pod": "loom-worker-old",
            "reason": "FailedKillPod",
            "operation": "kill",
            "target_generation": False,
        },
    ]
    ready = _all_ready_status()
    wait_results = iter([stalled, ready])
    waits: list[int] = []

    def _wait(*args, **kwargs):  # type: ignore[no-untyped-def]
        waits.append(kwargs["timeout_sec"])
        return next(wait_results)

    recovered: dict[str, Any] = {}

    def _recover(core_v1, namespace, status, *, max_pods, dry_run):  # type: ignore[no-untyped-def]
        recovered["namespace"] = namespace
        recovered["status"] = status
        recovered["max_pods"] = max_pods
        recovered["dry_run"] = dry_run
        return ["loom-worker-old"]

    monkeypatch.setattr("loom_cli.cluster_cmd.wait_for_ready", _wait)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.recover_sandbox_deadline_pods",
        _recover,
        raising=False,
    )

    rc = main(
        [
            "cluster",
            "up",
            "--recover-sandbox-deadlines",
            "--sandbox-deadline-max-pods",
            "2",
        ]
    )

    assert rc == 0
    assert waits == [600, 600]
    assert recovered["namespace"] == "loom"
    assert recovered["max_pods"] == 2
    assert recovered["dry_run"] is False
    assert recovered["status"] is stalled
    out = capsys.readouterr().out
    assert "Recovered 1 pod sandbox deadline stall(s)" in out
    assert "loom-worker-old" in out


def test_cli_up_fails_when_live_deployment_image_drifts_after_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    rendered = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-worker
spec:
  template:
    spec:
      containers:
        - name: worker
          image: loom-worker:staging-expected
"""
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.render_manifests",
        lambda _config: rendered,
    )

    def _image_checks(apps, namespace, rendered_manifests):  # type: ignore[no-untyped-def]
        assert namespace == "loom"
        assert rendered_manifests == rendered
        return [
            DeploymentImageCheck(
                deployment="loom-worker",
                container="worker",
                expected_image="loom-worker:staging-expected",
                live_image="loom-worker:debug-tip",
            ),
        ]

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.rendered_image_checks",
        _image_checks,
        raising=False,
    )

    rc = main(["cluster", "up"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "deployment image drift" in err
    assert "loom-worker:debug-tip" in err
    assert captures.get("waited") is True


def test_cli_up_cluster_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(_: str | None) -> object:
        raise OSError("kubeconfig not found")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise)
    rc = main(["cluster", "up"])
    assert rc == 2
    assert "cannot connect to cluster" in capsys.readouterr().err


def test_cli_up_kubectl_missing_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_up_path(monkeypatch)

    def _apply_raise(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError(
            "kubectl is required for `loom cluster up`. install from ...",
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.apply_manifests",
        _apply_raise,
    )
    rc = main(["cluster", "up"])
    assert rc == 2
    assert "kubectl is required" in capsys.readouterr().err


def test_cli_up_namespace_flag_threads_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    main(
        [
            "cluster",
            "up",
            "--namespace",
            "loom-custom",
            "--environment",
            "development",
        ]
    )
    assert captures["apply_ns"] == "loom-custom"


def test_cli_up_backup_guard_flags_thread_to_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    manifest = tmp_path / "backup-manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    config = tmp_path / "production.cluster.toml"
    config.write_text(
        'namespace = "loom-production"\n'
        'runtime_environment = "production"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    captures = _patch_full_up_path(monkeypatch)

    rc = main(
        [
            "cluster",
            "up",
            "--namespace",
            "loom-production",
            "--environment",
            "production",
            "--config",
            str(config),
            "--backup-manifest",
            str(manifest),
            "--backup-max-age-hours",
            "12",
        ]
    )

    assert rc == 0
    assert captures["preflight_kwargs"]["environment"] == "production"
    assert captures["preflight_kwargs"]["backup_manifest"] == manifest.resolve()
    assert captures["preflight_kwargs"]["backup_max_age_hours"] == 12


def test_cli_up_config_file_threads_static_host_path_to_preflight_and_render(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    cfg = tmp_path / "cluster.toml"
    cfg.write_text(
        'namespace = "loom-dev"\n'
        'runtime_environment = "development"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/tmp/loom-development"\n',
        encoding="utf-8",
    )
    captures = _patch_full_up_path(monkeypatch)

    rc = main(
        [
            "cluster",
            "up",
            "--config",
            str(cfg),
            "--namespace",
            "loom-dev",
            "--environment",
            "development",
        ]
    )

    assert rc == 0
    assert (
        captures["preflight_kwargs"]["cluster_config"].persistent_storage_backend
        == "static-host-path"
    )
    assert "kind: PersistentVolume" in captures["apply_yaml_text"]
    assert 'path: "/tmp/loom-development/postgres"' in captures["apply_yaml_text"]


def test_cli_up_config_file_invalid_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A typo in cluster-config.toml shouldn't reach apply."""
    cfg = tmp_path / "bad.toml"
    cfg.write_text('imag_tag = "1.2.3"\n', encoding="utf-8")
    _patch_full_up_path(monkeypatch)
    rc = main(["cluster", "up", "--config", str(cfg)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "render failed" in err
