"""`loom cluster preflight` unit tests (#76 Phase 2A).

Uses fake k8s client classes — same pattern as test_cluster_cmd.py.
Each check is exercised independently against a minimal fake; the
end-to-end CLI tests pin the dispatch + format paths.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import (
    PreflightCheck,
    PreflightReport,
    _check_default_storage_class,
    _check_ingress_class_installed,
    _check_namespace_exists,
    _check_pss_enforce,
    _check_required_secrets,
    _format_preflight_json,
    _format_preflight_table,
    collect_preflight,
)


class _FakeApiException(Exception):  # noqa: N818
    def __init__(self, status: int) -> None:
        super().__init__(f"status={status}")
        self.status = status


_FakeApiException.__name__ = "ApiException"


class _Spec:
    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeCoreV1:
    """Minimal Core API stub. `namespace_present` + `namespace_labels`
    drive the namespace check; `secrets` drives the secret check."""

    def __init__(
        self, *, namespace_present: bool = True,
        namespace_labels: dict[str, str] | None = None,
        secrets: set[str] | None = None,
    ) -> None:
        self.namespace_present = namespace_present
        self.namespace_labels = namespace_labels or {}
        self.secrets = secrets or set()

    def read_namespace(self, *, name: str) -> Any:
        if not self.namespace_present:
            raise _FakeApiException(404)
        return _Spec(metadata=_Spec(labels=self.namespace_labels))

    def read_namespaced_secret(self, *, name: str, namespace: str) -> Any:
        if name not in self.secrets:
            raise _FakeApiException(404)
        return _Spec(metadata=_Spec(name=name))


class _FakeNetworkingV1:
    def __init__(self, ingress_classes: list[str] | None = None) -> None:
        self.ingress_classes = ingress_classes or []

    def list_ingress_class(self) -> Any:
        items = [_Spec(metadata=_Spec(name=n)) for n in self.ingress_classes]
        return _Spec(items=items)


class _FakeStorageV1:
    """Stub for the storage check. `classes` is a list of (name,
    is_default) tuples; the fake builds the right annotation shape."""

    def __init__(
        self, classes: list[tuple[str, bool]] | None = None,
    ) -> None:
        self.classes = classes or []

    def list_storage_class(self) -> Any:
        items = []
        for name, is_default in self.classes:
            anns = (
                {"storageclass.kubernetes.io/is-default-class": "true"}
                if is_default else {}
            )
            items.append(_Spec(metadata=_Spec(name=name, annotations=anns)))
        return _Spec(items=items)


# ──────────────────────────────────────────────────────────────────────
# Individual check functions
# ──────────────────────────────────────────────────────────────────────


def test_namespace_check_pass_when_present() -> None:
    core = _FakeCoreV1(namespace_present=True)
    check = _check_namespace_exists(core, "loom")
    assert check.outcome == "pass"
    assert "loom" in check.detail


def test_namespace_check_fail_when_missing_includes_remediation() -> None:
    core = _FakeCoreV1(namespace_present=False)
    check = _check_namespace_exists(core, "loom")
    assert check.outcome == "fail"
    assert "loom" in check.detail
    assert check.remediation is not None
    assert "kubectl create namespace loom" in check.remediation


def test_required_secrets_check_all_present() -> None:
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )
    checks = _check_required_secrets(core, "loom")
    assert all(c.outcome == "pass" for c in checks)
    assert len(checks) == 2


def test_required_secrets_check_each_missing_emits_remediation() -> None:
    core = _FakeCoreV1(secrets=set())
    checks = _check_required_secrets(core, "loom")
    assert all(c.outcome == "fail" for c in checks)
    for c in checks:
        assert c.remediation is not None
        assert "kubectl create secret" in c.remediation


def test_ingress_class_check_pass_when_classes_present() -> None:
    net = _FakeNetworkingV1(ingress_classes=["nginx"])
    check = _check_ingress_class_installed(net)
    assert check.outcome == "pass"
    assert "nginx" in check.detail


def test_ingress_class_check_fail_when_no_classes() -> None:
    net = _FakeNetworkingV1(ingress_classes=[])
    check = _check_ingress_class_installed(net)
    assert check.outcome == "fail"
    assert check.remediation is not None
    assert "ingress-nginx" in check.remediation


def test_default_storage_class_check_pass_when_default_marked() -> None:
    storage = _FakeStorageV1(classes=[
        ("standard", False),
        ("ssd", True),
    ])
    check = _check_default_storage_class(storage)
    assert check.outcome == "pass"
    assert "ssd" in check.detail


def test_default_storage_class_check_warn_when_none_marked_default() -> None:
    """PVCs without an explicit storageClassName fail to bind when
    no default exists. Surface as warn (operators may have set
    storageClassName explicitly per StatefulSet, in which case the
    warn is informational)."""
    storage = _FakeStorageV1(classes=[("standard", False)])
    check = _check_default_storage_class(storage)
    assert check.outcome == "warn"
    assert check.remediation is not None


def test_default_storage_class_check_fail_when_no_classes() -> None:
    storage = _FakeStorageV1(classes=[])
    check = _check_default_storage_class(storage)
    assert check.outcome == "fail"
    assert check.remediation is not None


def test_pss_enforce_warn_when_restricted() -> None:
    """Worker Deployment bind-mounts docker.sock; PSS restricted
    blocks it. Warn (not fail) so non-Docker driver users aren't
    forced to relax PSS."""
    core = _FakeCoreV1(
        namespace_present=True,
        namespace_labels={
            "pod-security.kubernetes.io/enforce": "restricted",
        },
    )
    check = _check_pss_enforce(core, "loom")
    assert check.outcome == "warn"
    assert check.remediation is not None


def test_pss_enforce_pass_when_no_label() -> None:
    core = _FakeCoreV1(namespace_present=True, namespace_labels={})
    check = _check_pss_enforce(core, "loom")
    assert check.outcome == "pass"


def test_pss_enforce_pass_when_baseline_or_privileged() -> None:
    for level in ("baseline", "privileged"):
        core = _FakeCoreV1(
            namespace_present=True,
            namespace_labels={
                "pod-security.kubernetes.io/enforce": level,
            },
        )
        check = _check_pss_enforce(core, "loom")
        assert check.outcome == "pass", f"level={level}"


# ──────────────────────────────────────────────────────────────────────
# collect_preflight aggregation
# ──────────────────────────────────────────────────────────────────────


def test_collect_preflight_skips_namespace_scoped_checks_when_ns_missing() -> None:
    """When the namespace doesn't exist, the secret + PSS checks
    can't run (they're namespace-scoped). The cluster-scoped checks
    (ingress class, storage class) still run because they're useful
    diagnostics on their own."""
    core = _FakeCoreV1(namespace_present=False)
    report = collect_preflight(
        core, _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([("standard", True)]),
        "missing-ns", context=None,
    )
    names = [c.name for c in report.checks]
    # Always-on:
    assert "namespace-exists" in names
    assert "ingress-class-installed" in names
    assert "default-storage-class" in names
    # Skipped when namespace missing:
    assert "secret-loom-secrets" not in names
    assert "pss-enforce" not in names
    assert not report.all_pass
    assert report.any_fail


def test_collect_preflight_all_pass_happy_path() -> None:
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )
    report = collect_preflight(
        core, _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([("standard", True)]),
        "loom", context=None,
    )
    assert report.all_pass
    assert not report.any_fail


def test_collect_preflight_warn_does_not_set_any_fail() -> None:
    """Warns alone keep exit code 0 — CI scripts don't have to
    special-case warns. Only explicit fails flip any_fail."""
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
        namespace_labels={"pod-security.kubernetes.io/enforce": "restricted"},
    )
    report = collect_preflight(
        core, _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([("standard", True)]),
        "loom", context=None,
    )
    assert not report.any_fail
    # all_pass is False because PSS is warn (not pass).
    assert not report.all_pass


# ──────────────────────────────────────────────────────────────────────
# Format
# ──────────────────────────────────────────────────────────────────────


def test_format_table_includes_remediation_indented() -> None:
    report = PreflightReport(
        namespace="loom", context=None,
        checks=[
            PreflightCheck(
                name="secret-loom-secrets", outcome="fail",
                detail="Secret 'loom-secrets' missing in loom",
                remediation="kubectl create secret generic loom-secrets ...",
            ),
        ],
    )
    table = _format_preflight_table(report)
    assert "fail" in table
    assert "    kubectl create secret" in table  # indented


def test_format_table_omits_remediation_on_pass() -> None:
    report = PreflightReport(
        namespace="loom", context=None,
        checks=[
            PreflightCheck(
                name="namespace-exists", outcome="pass",
                detail="namespace 'loom' present",
                # Even if remediation is set (shouldn't be), pass
                # rows don't surface it.
                remediation="this should not appear",
            ),
        ],
    )
    table = _format_preflight_table(report)
    assert "this should not appear" not in table


def test_format_json_is_stable_and_parseable() -> None:
    report = PreflightReport(
        namespace="loom", context="prod",
        checks=[
            PreflightCheck(
                name="namespace-exists", outcome="pass", detail="ok",
            ),
        ],
    )
    parsed = json.loads(_format_preflight_json(report))
    assert parsed["namespace"] == "loom"
    assert parsed["context"] == "prod"
    assert parsed["all_pass"] is True
    assert parsed["any_fail"] is False
    assert parsed["checks"][0]["name"] == "namespace-exists"


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch
# ──────────────────────────────────────────────────────────────────────


def _patch_clients(monkeypatch, *, core, net, storage, apps=None) -> None:
    """Helper: wire fake clients into the CLI's lazy loader. `apps`
    is unused by preflight but must be present so the 4-tuple
    destructure works."""
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (apps or object(), net, core, storage),
    )


def test_cli_preflight_all_pass_returns_0(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )
    rc = main(["cluster", "preflight"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "namespace-exists" in out
    assert "pass" in out


def test_cli_preflight_any_fail_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No IngressClass installed → fail → exit 1."""
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1([]),  # no classes
        storage=_FakeStorageV1([("standard", True)]),
    )
    rc = main(["cluster", "preflight"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "ingress-class-installed" in out
    assert "fail" in out


def test_cli_preflight_cluster_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(_: str | None) -> object:
        raise OSError("kubeconfig not found")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise)
    rc = main(["cluster", "preflight"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "cannot connect to cluster" in err


def test_cli_preflight_kubernetes_lib_missing_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise_runtime(_: str | None) -> object:
        raise RuntimeError(
            "the 'kubernetes' package is required for `loom cluster` "
            "commands. install it with `pip install loom[cluster]`.",
        )

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise_runtime)
    rc = main(["cluster", "preflight"])
    assert rc == 2
    assert "pip install loom[cluster]" in capsys.readouterr().err


def test_cli_preflight_json_format(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )
    rc = main(["cluster", "preflight", "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["all_pass"] is True


def test_cli_preflight_warn_alone_returns_0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PSS restricted → warn; no other failures → exit 0."""
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(
            secrets={"loom-secrets", "loom-admin-secret"},
            namespace_labels={
                "pod-security.kubernetes.io/enforce": "restricted",
            },
        ),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )
    rc = main(["cluster", "preflight"])
    assert rc == 0


def test_cli_preflight_namespace_flag_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    class _CapturingCore(_FakeCoreV1):
        def read_namespace(self, *, name: str) -> Any:
            captured["ns"] = name
            return super().read_namespace(name=name)

    _patch_clients(
        monkeypatch,
        core=_CapturingCore(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )
    main(["cluster", "preflight", "--namespace", "loom-stage"])
    assert captured["ns"] == "loom-stage"
