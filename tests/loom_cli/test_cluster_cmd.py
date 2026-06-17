"""`loom cluster status` unit tests.

Use fake k8s client classes (not the real kubernetes lib) so the
tests don't need a cluster or the python `kubernetes` package
installed in the test env. The CLI module's `collect_status`
function takes the API clients as args specifically for this seam.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import (
    ClusterStatus,
    ComponentStatus,
    IngressEndpoint,
    _format_json,
    _format_table,
    collect_status,
)

# ──────────────────────────────────────────────────────────────────────
# Fake k8s clients
# ──────────────────────────────────────────────────────────────────────


class _FakeApiException(Exception):  # noqa: N818 — name must end "Exception" so `_exception_to_note` matches on type name
    """Mimics kubernetes.client.exceptions.ApiException for the
    `_exception_to_note` 404 branch."""

    def __init__(self, status: int, body: str = "") -> None:
        super().__init__(body or f"status={status}")
        self.status = status


# Override the class name so `_exception_to_note` matches on "ApiException".
_FakeApiException.__name__ = "ApiException"


class _Spec:
    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


class _Workload:
    def __init__(
        self, *, spec: object | None = None, status: object | None = None,
    ) -> None:
        self.spec = spec
        self.status = status


class _FakeAppsV1:
    """In-memory deployments / daemonsets / statefulsets. Tests
    populate the dicts with the workloads they want to exist;
    missing entries surface as ApiException(404)."""

    def __init__(self) -> None:
        self.deployments: dict[str, _Workload] = {}
        self.daemonsets: dict[str, _Workload] = {}
        self.statefulsets: dict[str, _Workload] = {}

    def read_namespaced_deployment(
        self, *, name: str, namespace: str,
    ) -> _Workload:
        if name not in self.deployments:
            raise _FakeApiException(404)
        return self.deployments[name]

    def read_namespaced_daemon_set(
        self, *, name: str, namespace: str,
    ) -> _Workload:
        if name not in self.daemonsets:
            raise _FakeApiException(404)
        return self.daemonsets[name]

    def read_namespaced_stateful_set(
        self, *, name: str, namespace: str,
    ) -> _Workload:
        if name not in self.statefulsets:
            raise _FakeApiException(404)
        return self.statefulsets[name]


class _IngressList:
    def __init__(self, items: Iterable[Any]) -> None:
        self.items = list(items)


class _FakeNetworkingV1:
    def __init__(self, ingresses: list[Any] | None = None) -> None:
        self.ingresses = ingresses or []

    def list_namespaced_ingress(self, *, namespace: str) -> _IngressList:
        return _IngressList(self.ingresses)


class _FakeCoreV1:
    def __init__(self, secrets: set[str] | None = None) -> None:
        self.secrets = secrets or set()

    def read_namespaced_secret(self, *, name: str, namespace: str) -> object:
        if name not in self.secrets:
            raise _FakeApiException(404)
        return _Spec(name=name)


class _FakeStorageV1:
    """Stub for `_load_clients`'s 4-tuple. Status doesn't read from
    it; preflight does. Tests in this file pass an empty fake."""

    def list_storage_class(self) -> object:
        return _Spec(items=[])


# ──────────────────────────────────────────────────────────────────────
# Helpers to build a "happy" cluster snapshot
# ──────────────────────────────────────────────────────────────────────


def _make_deployment(ready: int, desired: int, available: int | None = None) -> _Workload:
    return _Workload(
        spec=_Spec(replicas=desired),
        status=_Spec(
            ready_replicas=ready,
            available_replicas=available if available is not None else ready,
        ),
    )


def _make_daemonset(ready: int, desired: int) -> _Workload:
    return _Workload(
        spec=None,
        status=_Spec(desired_number_scheduled=desired, number_ready=ready),
    )


def _make_statefulset(ready: int, desired: int) -> _Workload:
    return _Workload(
        spec=_Spec(replicas=desired),
        status=_Spec(ready_replicas=ready),
    )


def _fully_ready_apps() -> _FakeAppsV1:
    apps = _FakeAppsV1()
    apps.deployments["loom-service"] = _make_deployment(2, 2)
    apps.deployments["loom-control-plane"] = _make_deployment(2, 2)
    apps.deployments["loom-llm-gateway"] = _make_deployment(2, 2)
    apps.deployments["loom-web"] = _make_deployment(0, 0)  # paused by default
    apps.daemonsets["loom-worker"] = _make_daemonset(3, 3)
    apps.statefulsets["postgres"] = _make_statefulset(1, 1)
    apps.statefulsets["minio"] = _make_statefulset(1, 1)
    return apps


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


def test_collect_status_happy_path_marks_all_healthy() -> None:
    """Every component running at desired count + secret present →
    `all_ready` is True. Web has 0/0 (paused-by-default per
    cluster-deploy.md); that should NOT count as healthy since
    `healthy` requires desired > 0."""
    apps = _fully_ready_apps()
    net = _FakeNetworkingV1()
    core = _FakeCoreV1(secrets={"loom-secrets"})
    status = collect_status(apps, net, core, "loom", context=None)
    assert status.namespace == "loom"
    assert status.warnings == []
    # The 0/0 web deployment is NOT healthy (desired must be > 0).
    web = next(c for c in status.components if c.name == "loom-web")
    assert not web.healthy
    # So all_ready is False because web counts.
    assert not status.all_ready
    # Other components ARE healthy.
    svc = next(c for c in status.components if c.name == "loom-service")
    assert svc.healthy


def test_collect_status_with_web_scaled_up_is_all_ready() -> None:
    """When operator scales web up, all components become healthy
    → all_ready=True."""
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    net = _FakeNetworkingV1()
    core = _FakeCoreV1(secrets={"loom-secrets"})
    status = collect_status(apps, net, core, "loom", context=None)
    assert status.all_ready


def test_collect_status_missing_component_surfaces_as_not_found() -> None:
    """A deployment that hasn't been applied yet appears in the
    output with ready=0/desired=0 and a 'not-found' note. Operators
    expect to see what's missing, not have it silently hidden."""
    apps = _FakeAppsV1()
    # Only loom-service is deployed.
    apps.deployments["loom-service"] = _make_deployment(2, 2)
    status = collect_status(
        apps, _FakeNetworkingV1(), _FakeCoreV1(secrets={"loom-secrets"}),
        "loom", context=None,
    )
    cp = next(c for c in status.components if c.name == "loom-control-plane")
    assert cp.note == "not-found"
    assert cp.ready == 0
    assert cp.desired == 0


def test_collect_status_warns_when_loom_secrets_missing() -> None:
    """The Secret 'loom-secrets' carries DB URLs + minio creds + JWT
    keys. If it's missing, every component fails to start. Surface
    this as a warning so operators don't waste time reading pod
    events one by one."""
    apps = _fully_ready_apps()
    status = collect_status(
        apps, _FakeNetworkingV1(), _FakeCoreV1(secrets=set()),
        "loom", context=None,
    )
    assert any("loom-secrets" in w for w in status.warnings)


def test_collect_status_renders_ingress_endpoints_with_tls() -> None:
    """The status output should surface ingress URLs so operators
    immediately know where to point a browser. https:// when the
    host appears in spec.tls; http:// otherwise."""
    ingress = _Spec(
        spec=_Spec(
            tls=[_Spec(hosts=["loom.example.com"])],
            rules=[
                _Spec(host="loom.example.com",
                      http=_Spec(paths=[_Spec(path="/")])),
            ],
        ),
    )
    apps = _fully_ready_apps()
    status = collect_status(
        apps, _FakeNetworkingV1([ingress]),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom", context=None,
    )
    assert len(status.ingresses) == 1
    ep = status.ingresses[0]
    assert ep.host == "loom.example.com"
    assert ep.tls is True
    assert ep.paths == ["/"]


def test_format_table_includes_every_expected_component_row() -> None:
    """The table renderer should produce one row per expected
    component, even when some are missing — operators rely on the
    presence of the row to know what wasn't deployed."""
    apps = _FakeAppsV1()
    apps.deployments["loom-service"] = _make_deployment(1, 1)
    status = collect_status(
        apps, _FakeNetworkingV1(), _FakeCoreV1(secrets={"loom-secrets"}),
        "loom", context=None,
    )
    table = _format_table(status)
    for name in (
        "loom-service", "loom-control-plane", "loom-llm-gateway",
        "loom-worker", "postgres", "minio",
    ):
        assert name in table


def test_format_json_is_stable_and_parseable() -> None:
    status = ClusterStatus(
        namespace="loom", context="prod",
        components=[ComponentStatus(
            name="loom-service", kind="Deployment",
            ready=2, desired=2, available=True,
        )],
        ingresses=[IngressEndpoint(
            host="loom.example.com", paths=["/"], tls=True,
        )],
        warnings=["sample warning"],
    )
    body = _format_json(status)
    parsed = json.loads(body)
    assert parsed["namespace"] == "loom"
    assert parsed["all_ready"] is True
    assert parsed["components"][0]["healthy"] is True
    assert parsed["ingresses"][0]["tls"] is True
    assert parsed["warnings"] == ["sample warning"]


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch
# ──────────────────────────────────────────────────────────────────────


def test_cli_status_returns_2_when_kubernetes_lib_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Lazy-import path: if the kubernetes package isn't installed,
    the CLI surfaces a friendly install hint and exits 2."""
    def _raise_module_not_found(_: str | None) -> object:
        raise RuntimeError(
            "the 'kubernetes' package is required for `loom cluster` "
            "commands. install it with `pip install loom[cluster]`.",
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients", _raise_module_not_found,
    )
    rc = main(["cluster", "status"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "pip install loom[cluster]" in err


def test_cli_status_returns_2_when_cluster_unreachable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A kubeconfig error → exit 2 with a `cannot connect to
    cluster` message."""
    def _raise(_: str | None) -> object:
        raise OSError("kubeconfig not found")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise)
    rc = main(["cluster", "status"])
    assert rc == 2
    assert "cannot connect to cluster" in capsys.readouterr().err


def test_cli_status_returns_0_when_all_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (apps, _FakeNetworkingV1(),
                       _FakeCoreV1(secrets={"loom-secrets"}),
                       _FakeStorageV1()),
    )
    rc = main(["cluster", "status"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "loom-service" in out
    assert "ready" in out


def test_cli_status_returns_1_when_a_component_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Exit code 1 specifically when the cluster is reachable but
    not fully ready — distinguishes 'cluster down' (2) from
    'cluster present but broken' (1) for CI scripts."""
    apps = _fully_ready_apps()
    # Postgres down.
    apps.statefulsets["postgres"] = _make_statefulset(0, 1)
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (apps, _FakeNetworkingV1(),
                       _FakeCoreV1(secrets={"loom-secrets"}),
                       _FakeStorageV1()),
    )
    rc = main(["cluster", "status"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "not-ready" in out


def test_cli_status_json_format(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (apps, _FakeNetworkingV1(),
                       _FakeCoreV1(secrets={"loom-secrets"}),
                       _FakeStorageV1()),
    )
    rc = main(["cluster", "status", "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["all_ready"] is True


def test_cli_status_namespace_flag_passed_to_collector(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--namespace` should flow through to the collector so a
    multi-tenant cluster can be queried for different deployments."""
    captured_ns: dict[str, str] = {}

    class _CapturingApps:
        def read_namespaced_deployment(self, *, name: str, namespace: str) -> Any:
            captured_ns["ns"] = namespace
            raise _FakeApiException(404)
        def read_namespaced_daemon_set(self, *, name: str, namespace: str) -> Any:
            raise _FakeApiException(404)
        def read_namespaced_stateful_set(self, *, name: str, namespace: str) -> Any:
            raise _FakeApiException(404)

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (_CapturingApps(),
                       _FakeNetworkingV1(),
                       _FakeCoreV1(secrets=set()),
                       _FakeStorageV1()),
    )
    main(["cluster", "status", "--namespace", "loom-stage"])
    assert captured_ns["ns"] == "loom-stage"
