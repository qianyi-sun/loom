"""`loom cluster preflight` unit tests (#76 Phase 2A).

Uses fake k8s client classes — same pattern as test_cluster_cmd.py.
Each check is exercised independently against a minimal fake; the
end-to-end CLI tests pin the dispatch + format paths.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

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
    render_manifests,
)
from loom_cli.cluster_config import ClusterConfig
from loom_config.loader import load_schema


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
        secret_data: dict[str, str] | None = None,
        pod_envs: dict[str, set[str]] | None = None,
        persistent_volume_claims: list[Any] | None = None,
        persistent_volumes: list[Any] | None = None,
    ) -> None:
        self.namespace_present = namespace_present
        self.namespace_labels = namespace_labels or {}
        self.secrets = secrets or set()
        self.secret_data = (
            _all_schema_secret_data()
            if secret_data is None and "loom-secrets" in self.secrets
            else secret_data or {}
        )
        self.pod_envs = pod_envs or {}
        self.persistent_volume_claims = persistent_volume_claims or []
        self.persistent_volumes = persistent_volumes or []

    def read_namespace(self, *, name: str) -> Any:
        if not self.namespace_present:
            raise _FakeApiException(404)
        return _Spec(metadata=_Spec(labels=self.namespace_labels))

    def read_namespaced_secret(self, *, name: str, namespace: str) -> Any:
        if name not in self.secrets:
            raise _FakeApiException(404)
        return _Spec(metadata=_Spec(name=name), data=self.secret_data)

    def list_namespaced_pod(self, *, namespace: str) -> Any:
        pods = []
        for pod_name, env_names in self.pod_envs.items():
            env = [_Spec(name=name) for name in env_names]
            pods.append(
                _Spec(
                    metadata=_Spec(name=pod_name),
                    spec=_Spec(containers=[_Spec(env=env)]),
                ),
            )
        return _Spec(items=pods)

    def list_namespaced_persistent_volume_claim(self, *, namespace: str) -> Any:
        return _Spec(items=list(self.persistent_volume_claims))

    def list_persistent_volume(self) -> Any:
        return _Spec(items=list(self.persistent_volumes))


class _FakeNetworkingV1:
    def __init__(self, ingress_classes: list[str] | None = None) -> None:
        self.ingress_classes = ingress_classes or []

    def list_ingress_class(self) -> Any:
        items = [_Spec(metadata=_Spec(name=n)) for n in self.ingress_classes]
        return _Spec(items=items)


_StorageClassSpec = tuple[str, bool] | tuple[str, bool, str, str]


class _FakeStorageV1:
    """Stub for the storage check. `classes` is a list of (name,
    is_default) tuples, or (name, is_default, provisioner, reclaim_policy)
    tuples; the fake builds the right annotation shape."""

    def __init__(
        self, classes: list[_StorageClassSpec] | None = None,
    ) -> None:
        self.classes = classes or []

    def list_storage_class(self) -> Any:
        items = []
        for item in self.classes:
            name = item[0]
            is_default = item[1]
            provisioner = item[2] if len(item) > 2 else "example.com/csi"
            reclaim_policy = item[3] if len(item) > 3 else "Retain"
            anns = (
                {"storageclass.kubernetes.io/is-default-class": "true"}
                if is_default else {}
            )
            items.append(_Spec(
                metadata=_Spec(name=name, annotations=anns),
                provisioner=provisioner,
                reclaim_policy=reclaim_policy,
            ))
        return _Spec(items=items)


def _bound_pvc(name: str, volume_name: str, storage_class_name: str = "") -> Any:
    return _Spec(
        metadata=_Spec(name=name),
        spec=_Spec(
            volume_name=volume_name,
            storage_class_name=storage_class_name,
        ),
        status=_Spec(phase="Bound"),
    )


def _host_path_pv(
    *,
    name: str,
    namespace: str,
    claim_name: str,
    path: str,
    reclaim_policy: str = "Retain",
    storage_class_name: str = "",
) -> Any:
    return _Spec(
        metadata=_Spec(name=name),
        spec=_Spec(
            storage_class_name=storage_class_name,
            persistent_volume_reclaim_policy=reclaim_policy,
            host_path=_Spec(path=path),
            claim_ref=_Spec(namespace=namespace, name=claim_name),
        ),
    )


def _write_recent_manifest(tmp_path: Path, *, environment: str, namespace: str) -> Path:
    from datetime import UTC, datetime

    from loom_cli.cluster_backup_guard import write_backup_manifest

    postgres = tmp_path / "postgres.dump"
    postgres.write_text("pg", encoding="utf-8")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "object").write_text("obj", encoding="utf-8")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_text("redacted", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    write_backup_manifest(
        environment=environment,
        namespace=namespace,
        output_path=manifest,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
        now=datetime.now(UTC),
    )
    return manifest


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


def test_collect_preflight_flags_public_beta_local_path_delete_storage() -> None:
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )
    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
        "loom-public-beta",
        context=None,
        environment="public-beta",
        backup_manifest=None,
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["protected-storage-boundary"].outcome == "fail"
    assert "local-path" in by_name["protected-storage-boundary"].detail
    assert by_name["backup-manifest"].outcome == "fail"
    assert report.any_fail


def test_collect_preflight_passes_protected_storage_with_recent_manifest(
    tmp_path: Path,
) -> None:
    from datetime import UTC, datetime

    from loom_cli.cluster_backup_guard import write_backup_manifest

    postgres = tmp_path / "postgres.dump"
    postgres.write_text("pg", encoding="utf-8")
    minio = tmp_path / "minio"
    minio.mkdir()
    (minio / "object").write_text("obj", encoding="utf-8")
    secrets = tmp_path / "secrets.yaml"
    secrets.write_text("redacted", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    write_backup_manifest(
        environment="staging",
        namespace="loom-staging",
        output_path=manifest,
        components={
            "postgres": postgres,
            "minio": minio,
            "k8s_secrets": secrets,
        },
        now=datetime.now(UTC),
    )
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )

    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([("fast", True, "example.com/csi", "Retain")]),
        "loom-staging",
        context=None,
        environment="staging",
        backup_manifest=manifest,
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["protected-storage-boundary"].outcome == "pass"
    assert by_name["backup-manifest"].outcome == "pass"
    assert not report.any_fail


def test_collect_preflight_passes_static_retain_pvs_despite_local_path_default(
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
        persistent_volume_claims=[
            _bound_pvc(
                "data-loom-postgres-0",
                "loom-public-beta-postgres-data",
            ),
            _bound_pvc(
                "data-loom-minio-0",
                "loom-public-beta-minio-data",
            ),
            _bound_pvc(
                "loom-worker-trajectories",
                "loom-public-beta-worker-trajectories-data",
            ),
        ],
        persistent_volumes=[
            _host_path_pv(
                name="loom-public-beta-postgres-data",
                namespace=namespace,
                claim_name="data-loom-postgres-0",
                path="/data/loom-public-beta/postgres",
            ),
            _host_path_pv(
                name="loom-public-beta-minio-data",
                namespace=namespace,
                claim_name="data-loom-minio-0",
                path="/data/loom-public-beta/minio",
            ),
            _host_path_pv(
                name="loom-public-beta-worker-trajectories-data",
                namespace=namespace,
                claim_name="loom-worker-trajectories",
                path="/data/loom-public-beta/trajectories",
            ),
        ],
    )

    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
        namespace,
        context=None,
        environment="public-beta",
        backup_manifest=manifest,
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["protected-storage-boundary"].outcome == "pass"
    assert "critical PVCs" in by_name["protected-storage-boundary"].detail
    assert not report.any_fail


def test_collect_preflight_passes_static_host_path_config_before_pvcs_exist(
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )
    cfg = ClusterConfig(
        namespace=namespace,
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-public-beta",
    )

    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
        namespace,
        context=None,
        environment="public-beta",
        backup_manifest=manifest,
        cluster_config=cfg,
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["protected-storage-boundary"].outcome == "pass"
    assert "static-host-path" in by_name["protected-storage-boundary"].detail
    assert "/data/loom-public-beta" in by_name["protected-storage-boundary"].detail
    assert not report.any_fail


def test_collect_preflight_fails_kind_static_host_path_without_host_bind_mount(
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )
    cfg = ClusterConfig(
        namespace=namespace,
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-public-beta",
    )

    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
        namespace,
        context="kind-loom-public-beta",
        environment="public-beta",
        backup_manifest=manifest,
        cluster_config=cfg,
        kind_node_mounts=[
            {"Type": "volume", "Source": "kind-var", "Destination": "/var"},
        ],
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["kind-host-storage-mount"].outcome == "fail"
    assert "/data/loom-public-beta" in by_name["kind-host-storage-mount"].detail
    assert report.any_fail


def test_collect_preflight_passes_kind_static_host_path_with_host_bind_mount(
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
    )
    cfg = ClusterConfig(
        namespace=namespace,
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-public-beta",
    )

    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
        namespace,
        context="kind-loom-public-beta",
        environment="public-beta",
        backup_manifest=manifest,
        cluster_config=cfg,
        kind_node_mounts=[
            {
                "Type": "bind",
                "Source": "/data/loom-public-beta",
                "Destination": "/data/loom-public-beta",
            },
        ],
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["kind-host-storage-mount"].outcome == "pass"
    assert "/data/loom-public-beta" in by_name["kind-host-storage-mount"].detail
    assert not report.any_fail


def test_collect_preflight_fails_when_critical_pv_uses_delete_reclaim(
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    core = _FakeCoreV1(
        secrets={"loom-secrets", "loom-admin-secret"},
        persistent_volume_claims=[
            _bound_pvc("data-loom-postgres-0", "pv-postgres"),
            _bound_pvc("data-loom-minio-0", "pv-minio"),
            _bound_pvc("loom-worker-trajectories", "pv-trajectories"),
        ],
        persistent_volumes=[
            _host_path_pv(
                name="pv-postgres",
                namespace=namespace,
                claim_name="data-loom-postgres-0",
                path="/data/loom-public-beta/postgres",
                reclaim_policy="Delete",
            ),
            _host_path_pv(
                name="pv-minio",
                namespace=namespace,
                claim_name="data-loom-minio-0",
                path="/data/loom-public-beta/minio",
            ),
            _host_path_pv(
                name="pv-trajectories",
                namespace=namespace,
                claim_name="loom-worker-trajectories",
                path="/data/loom-public-beta/trajectories",
            ),
        ],
    )

    report = collect_preflight(
        core,
        _FakeNetworkingV1(["nginx"]),
        _FakeStorageV1([("fast", True, "example.com/csi", "Retain")]),
        namespace,
        context=None,
        environment="public-beta",
        backup_manifest=manifest,
    )

    by_name = {check.name: check for check in report.checks}
    assert by_name["protected-storage-boundary"].outcome == "fail"
    assert "reclaimPolicy=Delete" in by_name["protected-storage-boundary"].detail
    assert report.any_fail


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


def _patch_clients(
    monkeypatch: pytest.MonkeyPatch, *, core: Any, net: Any, storage: Any,
    apps: Any | None = None,
) -> None:
    """Helper: wire fake clients into the CLI's lazy loader. `apps`
    is unused by preflight but must be present so the 4-tuple
    destructure works."""
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (apps or object(), net, core, storage),
    )


def _rendered_deployment_env_names(deployment_name: str) -> set[str]:
    # #383: default schema disables the k8s worker Deployment. This
    # helper inspects the worker/service env for boundary checks, so
    # opt into the worker-enabled shape used by development profile.
    k8s_worker_cls = type(ClusterConfig().k8s_worker)
    cfg = ClusterConfig(k8s_worker=k8s_worker_cls(enabled=True))
    docs = list(yaml.safe_load_all(render_manifests(cfg)))
    deployment = next(
        doc
        for doc in docs
        if doc
        and doc.get("kind") == "Deployment"
        and doc.get("metadata", {}).get("name") == deployment_name
    )
    env = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    return {entry["name"] for entry in env}


def _all_schema_secret_data() -> dict[str, str]:
    schema = load_schema(Path("config/loom-schema.toml"))
    secret_keys = set(schema.infra_secrets)
    for entry in schema.service_config.values():
        if entry.secret is None:
            continue
        for svc in entry.used_by:
            secret_keys.add(entry.secret_key_for(svc))
    return {key: "AAA=" for key in secret_keys}


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


def test_cli_preflight_schema_doctor_accepts_default_rendered_worker_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Default k8s render omits optional worker env that the worker can
    derive at runtime, but schema-doctor should still accept the rendered
    pod shape and keep checking env that templates explicitly inject.
    """
    worker_env = _rendered_deployment_env_names("loom-worker")
    assert "LOOM_WORKER_SUBPROCESS_GATEWAY_URL" in worker_env
    for optional_env in (
        "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS",
        "LOOM_WORKER_HOSTNAME",
        "LOOM_WORKER_BLOCKING_IO_MAX_WORKERS",
        "LOOM_WORKER_FIXTURES_ROOT",
        "LOOM_WORKER_BENCHMARK_CACHE",
    ):
        assert optional_env not in worker_env

    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(
            secrets={"loom-secrets", "loom-admin-secret"},
            secret_data=_all_schema_secret_data(),
            pod_envs={"loom-worker-abc": worker_env},
        ),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )

    rc = main(["cluster", "preflight"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "schema-doctor" in out
    assert "schema reconciliation clean" in out
    assert "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS" not in out


def test_cli_preflight_schema_doctor_accepts_target_env_added_during_rollout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preflight runs before apply during a rollout.

    Live pods may still be on the previous image/env surface, so schema-doctor
    must check the target rendered Deployment instead of rejecting the old pod.
    """
    control_plane_env = _rendered_deployment_env_names("loom-control-plane")
    assert "LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC" in control_plane_env
    old_live_control_plane_env = control_plane_env - {
        "LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC",
    }

    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(
            secrets={"loom-secrets", "loom-admin-secret"},
            secret_data=_all_schema_secret_data(),
            pod_envs={"loom-control-plane-old": old_live_control_plane_env},
        ),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )

    rc = main(["cluster", "preflight"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "schema-doctor" in out
    assert "schema reconciliation clean" in out
    assert "LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC" not in out


def test_cli_preflight_schema_doctor_fails_when_target_render_omits_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original_render = render_manifests

    def _broken_render(config: ClusterConfig) -> str:
        return original_render(config).replace(
            "LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC",
            "LOOM_CP_BROKEN_CLAIMED_WITHOUT_START_EXPIRY_SEC",
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.render_manifests", _broken_render)
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(
            secrets={"loom-secrets", "loom-admin-secret"},
            secret_data=_all_schema_secret_data(),
        ),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([("standard", True)]),
    )

    rc = main(["cluster", "preflight"])

    assert rc == 1
    out = capsys.readouterr().out
    assert "schema-doctor" in out
    assert "Rendered Deployment loom-control-plane" in out
    assert "LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC" in out


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
    rc = main(["cluster", "preflight", "--format", "json", "--no-doctor"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["all_pass"] is True


def test_cli_preflight_config_allows_static_host_path_before_pvcs_exist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    cfg = tmp_path / "cluster.toml"
    cfg.write_text(
        'namespace = "loom-public-beta"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-public-beta"\n',
        encoding="utf-8",
    )
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
    )

    rc = main([
        "cluster", "preflight",
        "--namespace", namespace,
        "--environment", "public-beta",
        "--backup-manifest", str(manifest),
        "--config", str(cfg),
        "--no-doctor",
    ])

    assert rc == 0


def test_cli_preflight_threads_kind_node_mounts_for_static_host_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    cfg = tmp_path / "cluster.toml"
    cfg.write_text(
        'namespace = "loom-public-beta"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-public-beta"\n',
        encoding="utf-8",
    )
    mounts = [
        {
            "Type": "bind",
            "Source": "/data/loom-public-beta",
            "Destination": "/data/loom-public-beta",
        },
    ]
    captures: dict[str, Any] = {}
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._read_kind_node_mounts",
        lambda context: mounts,
        raising=False,
    )

    def _collect_preflight(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["preflight_kwargs"] = kwargs
        return PreflightReport(
            namespace=namespace,
            context=kwargs["context"],
            checks=[
                PreflightCheck(
                    name="kind-host-storage-mount",
                    outcome="pass",
                    detail="mounted",
                ),
            ],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_preflight", _collect_preflight)

    rc = main([
        "cluster", "preflight",
        "--context", "kind-loom-public-beta",
        "--namespace", namespace,
        "--environment", "public-beta",
        "--backup-manifest", str(manifest),
        "--config", str(cfg),
        "--no-doctor",
    ])

    assert rc == 0
    assert captures["preflight_kwargs"]["kind_node_mounts"] == mounts


def test_cli_preflight_uses_current_kind_context_for_static_host_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    cfg = tmp_path / "cluster.toml"
    cfg.write_text(
        'namespace = "loom-public-beta"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-public-beta"\n',
        encoding="utf-8",
    )
    mounts = [
        {
            "Type": "bind",
            "Source": "/data/loom-public-beta",
            "Destination": "/data/loom-public-beta",
        },
    ]
    captures: dict[str, Any] = {}
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._effective_kube_context",
        lambda context: "kind-loom-public-beta",
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._read_kind_node_mounts",
        lambda context: mounts,
        raising=False,
    )

    def _collect_preflight(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["preflight_kwargs"] = kwargs
        return PreflightReport(
            namespace=namespace,
            context=kwargs["context"],
            checks=[
                PreflightCheck(
                    name="kind-host-storage-mount",
                    outcome="pass",
                    detail="mounted",
                ),
            ],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_preflight", _collect_preflight)

    rc = main([
        "cluster", "preflight",
        "--namespace", namespace,
        "--environment", "public-beta",
        "--backup-manifest", str(manifest),
        "--config", str(cfg),
        "--no-doctor",
    ])

    assert rc == 0
    assert captures["preflight_kwargs"]["context"] == "kind-loom-public-beta"
    assert captures["preflight_kwargs"]["kind_node_mounts"] == mounts


def test_cli_up_threads_kind_node_mounts_for_static_host_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    namespace = "loom-public-beta"
    manifest = _write_recent_manifest(
        tmp_path,
        environment="public-beta",
        namespace=namespace,
    )
    cfg = tmp_path / "cluster.toml"
    cfg.write_text(
        'namespace = "loom-public-beta"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-public-beta"\n',
        encoding="utf-8",
    )
    mounts = [
        {
            "Type": "bind",
            "Source": "/data/loom-public-beta",
            "Destination": "/data/loom-public-beta",
        },
    ]
    captures: dict[str, Any] = {}
    _patch_clients(
        monkeypatch,
        core=_FakeCoreV1(secrets={"loom-secrets", "loom-admin-secret"}),
        net=_FakeNetworkingV1(["nginx"]),
        storage=_FakeStorageV1([
            ("standard", True, "rancher.io/local-path", "Delete"),
        ]),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._effective_kube_context",
        lambda context: "kind-loom-public-beta",
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._read_kind_node_mounts",
        lambda context: mounts,
        raising=False,
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._append_target_schema_doctor_check",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.apply_manifests",
        lambda *args, **kwargs: _Spec(returncode=0, summary_lines=[], stderr=""),
    )

    def _collect_preflight(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["preflight_kwargs"] = kwargs
        return PreflightReport(
            namespace=namespace,
            context=kwargs["context"],
            checks=[
                PreflightCheck(
                    name="kind-host-storage-mount",
                    outcome="pass",
                    detail="mounted",
                ),
            ],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_preflight", _collect_preflight)

    rc = main([
        "cluster", "up",
        "--namespace", namespace,
        "--environment", "public-beta",
        "--backup-manifest", str(manifest),
        "--config", str(cfg),
        "--no-wait",
    ])

    assert rc == 0
    assert captures["preflight_kwargs"]["context"] == "kind-loom-public-beta"
    assert captures["preflight_kwargs"]["kind_node_mounts"] == mounts


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
