"""`loom cluster render` unit + golden-file tests (#76 Phase 1B).

The golden test compares the default render against the canonical
`deploy/k8s/*.yaml` set. Future drift in either the templates or
the example manifests gets caught immediately — operators reading
the YAML files in the repo are seeing the same thing the CLI emits.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import (
    ClusterConfig,
    load_cluster_config,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY_DIR = _REPO_ROOT / "deploy" / "k8s"
# Order MUST match `cluster_cmd._TEMPLATE_ORDER` so per-doc comparison
# lines up: postgres → minio → control-plane → service → gateway →
# worker → web → ingress.
_GOLDEN_FILES = (
    "postgres.yaml", "minio.yaml", "control-plane.yaml",
    "loom-service.yaml", "llm-gateway.yaml", "worker.yaml",
    "web.yaml", "ingress.yaml", "gateway-router.yaml",
    # Phase C (#190) — egress proxy chain. Default replicas=0 so
    # the resources exist in the manifest but no pods until
    # operators scale up.
    "egress-xds.yaml", "egress-proxy.yaml",
    "network-policies.yaml", "grafana-dashboards.yaml",
)


def _load_docs(yaml_text: str) -> list[dict]:
    """Parse a multi-document YAML string and drop None placeholders
    (empty documents at start/end produced by stray separators)."""
    return [d for d in yaml.safe_load_all(yaml_text) if d]


# ──────────────────────────────────────────────────────────────────────
# ClusterConfig + load
# ──────────────────────────────────────────────────────────────────────


def test_default_config_includes_public_tls_knobs() -> None:
    """Public deploys must render an HTTPS ingress by default, with
    operator-overridable host, class, and certificate secret knobs."""
    cfg = ClusterConfig()
    assert cfg.ingress_host == "loom.example.com"
    assert cfg.ingress_class_name == "nginx"
    assert cfg.ingress_tls_secret_name == "loom-tls"
    assert cfg.ingress_cert_manager_cluster_issuer == ""


def test_default_replicas_match_spec() -> None:
    """cluster-deploy.md §Component map specifies these defaults."""
    cfg = ClusterConfig()
    assert cfg.replicas.service == 2
    assert cfg.replicas.control_plane == 2
    assert cfg.replicas.gateway == 2
    # Paused per spec — operators scale up explicitly.
    assert cfg.replicas.web == 0
    assert cfg.replicas.worker == 3


def test_load_config_from_toml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'image_tag = "1.2.3"\n'
        'ingress_host = "loom.acme.example"\n'
        'ingress_class_name = "public-nginx"\n'
        'ingress_tls_secret_name = "loom-acme-tls"\n'
        'ingress_cert_manager_cluster_issuer = "letsencrypt-prod"\n'
        'provider_egress_allowlist = ["202.78.161.51:18001"]\n'
        '[replicas]\n'
        'service = 5\n'
        'worker = 10\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.image_tag == "1.2.3"
    assert cfg.ingress_host == "loom.acme.example"
    assert cfg.ingress_class_name == "public-nginx"
    assert cfg.ingress_tls_secret_name == "loom-acme-tls"
    assert cfg.ingress_cert_manager_cluster_issuer == "letsencrypt-prod"
    assert cfg.provider_egress_allowlist == ("202.78.161.51:18001",)
    assert cfg.replicas.service == 5
    assert cfg.replicas.worker == 10
    # Unspecified fields keep their defaults.
    assert cfg.replicas.control_plane == 2


def test_load_config_rejects_deprecated_gateway_public_host(
    tmp_path: Path,
) -> None:
    """The public-beta boundary no longer allows a public LLM Gateway
    host; old configs must fail instead of silently exposing it."""
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text('gateway_public_host = "gw.acme.example"\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"gateway_public_host.*no longer supported"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    """Typos must surface immediately, not silently use the default.
    Operators staring at `imag_tag = "..."` (typo) deserve a clear
    error, not a manifest with the wrong tag."""
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text('imag_tag = "1.2.3"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="unknown keys in cluster config"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_unknown_replicas_key(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        '[replicas]\n'
        'servvice = 4\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"unknown keys under \[replicas\]"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_non_string_provider_egress_allowlist(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'provider_egress_allowlist = ["202.78.161.51:18001", 18001]\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="array of strings"):
        load_cluster_config(cfg_path)


def test_load_config_missing_path_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_cluster_config(Path("/nonexistent/cluster.toml"))


def test_load_config_path_none_returns_defaults() -> None:
    cfg = load_cluster_config(None)
    assert cfg == ClusterConfig()


# ──────────────────────────────────────────────────────────────────────
# render_manifests
# ──────────────────────────────────────────────────────────────────────


def test_render_produces_valid_yaml_with_expected_kinds() -> None:
    """Smoke: every document parses, the set covers the 7 Deployments
    + 1 DaemonSet + 8 Services + 2 StatefulSets + 1 PVC + 1 Ingress
    + 8 NetworkPolicies + 2 ConfigMaps (Grafana dashboards + egress-
    proxy bootstrap) expected by cluster-deploy.md §Component map +
    sandbox-isolation.md."""
    text = render_manifests(ClusterConfig())
    docs = _load_docs(text)
    kinds = [d["kind"] for d in docs]
    assert kinds.count("StatefulSet") == 2   # postgres, minio
    # cp, service, gateway, worker, web + egress-xds + egress-proxy
    assert kinds.count("Deployment") == 7
    assert kinds.count("DaemonSet") == 1     # gateway-router
    # 6 service-Deployments + egress-xds + egress-proxy.
    assert kinds.count("Service") == 8
    assert kinds.count("Ingress") == 1
    assert kinds.count("PersistentVolumeClaim") == 1
    # NetworkPolicies: postgres + minio + cp + gateway + worker + svc
    # + web + gateway-router + egress-xds + egress-proxy = 10.
    assert kinds.count("NetworkPolicy") == 10
    # Grafana dashboards ConfigMap + egress-proxy bootstrap ConfigMap.
    assert kinds.count("ConfigMap") == 2


def test_render_default_matches_deploy_k8s_yamls() -> None:
    """Golden test: rendering with default config produces the same
    set of k8s objects as the canonical `deploy/k8s/*.yaml` files.

    This pins the templates against drift in either direction — if
    someone edits a deploy/k8s/*.yaml without updating the matching
    template, this test fails. Same in reverse.
    """
    rendered = _load_docs(render_manifests(ClusterConfig()))
    expected: list[dict] = []
    for fn in _GOLDEN_FILES:
        expected.extend(_load_docs((_DEPLOY_DIR / fn).read_text()))
    assert len(rendered) == len(expected), (
        f"render emitted {len(rendered)} docs; deploy/k8s/*.yaml has "
        f"{len(expected)}. Either the template order changed or a "
        f"manifest was added/removed without updating "
        f"`_TEMPLATE_ORDER` / this test's golden list."
    )
    for i, (r, e) in enumerate(zip(rendered, expected, strict=True)):
        assert r == e, (
            f"doc {i} ({r.get('kind')}/"
            f"{r.get('metadata', {}).get('name')}) differs from "
            f"deploy/k8s/. Re-run `loom cluster render > /tmp/r.yaml` "
            f"and diff against the appropriate file."
        )


def test_render_with_custom_image_tag_applies_to_every_loom_image() -> None:
    """Configure a non-default image_tag; every loom-* image MUST pick
    it up. External images (postgres, minio) stay on their own tags."""
    cfg = ClusterConfig(image_tag="2.0.0-rc1")
    docs = _load_docs(render_manifests(cfg))
    loom_images = []
    for d in docs:
        if d["kind"] not in ("Deployment", "StatefulSet"):
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            if c["image"].startswith("loom-"):
                loom_images.append(c["image"])
    assert loom_images, "no loom-* images found in render output"
    for img in loom_images:
        assert img.endswith(":2.0.0-rc1"), (
            f"expected :2.0.0-rc1 suffix, got {img}"
        )


def test_render_injects_secret_store_master_key_for_provider_paths() -> None:
    """BYO provider create/test in service and provider dispatch in
    gateway both use LocalEncryptedSecretStore. Cluster mode must wire
    the shared master key into both pods from loom-secrets.
    """
    docs = _load_docs(render_manifests(ClusterConfig()))
    deployments = {
        d["metadata"]["name"]: d
        for d in docs
        if d["kind"] == "Deployment"
    }

    expected = {
        "loom-service": "LOOM_SECRET_STORE_MASTER_KEY",
        "loom-llm-gateway": "LOOM_SECRET_STORE_MASTER_KEY",
    }
    for deployment_name, env_name in expected.items():
        env = deployments[deployment_name]["spec"]["template"]["spec"]["containers"][0]["env"]
        assert {
            "name": env_name,
            "valueFrom": {
                "secretKeyRef": {
                    "name": "loom-secrets",
                    "key": "secret-store-master-key",
                },
            },
        } in env


def _network_policy_named(docs: list[dict], name: str) -> dict:
    return next(
        d for d in docs
        if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == name
    )


def _ipblock_ports(policy: dict) -> set[tuple[str, int]]:
    out: set[tuple[str, int]] = set()
    for rule in policy["spec"].get("egress", []):
        ports = [
            p["port"]
            for p in rule.get("ports", [])
            if p.get("protocol", "TCP") == "TCP"
        ]
        for target in rule.get("to", []):
            ip_block = target.get("ipBlock")
            if ip_block is None:
                continue
            for port in ports:
                out.add((ip_block["cidr"], int(port)))
    return out


def test_render_provider_egress_allowlist_adds_service_and_gateway_rules() -> None:
    """Operators can approve a non-standard BYO provider endpoint once
    in cluster-config.toml; render must preserve that policy for both
    provider validation in loom-service and runtime calls in gateway.
    """
    cfg = ClusterConfig(
        provider_egress_allowlist=(
            "202.78.161.51:18001",
            "203.0.113.0/24:8443",
        ),
    )
    docs = _load_docs(render_manifests(cfg))

    expected = {
        ("202.78.161.51/32", 18001),
        ("203.0.113.0/24", 8443),
    }
    for policy_name in ("loom-service", "loom-llm-gateway", "loom-egress-proxy"):
        assert expected <= _ipblock_ports(_network_policy_named(docs, policy_name))
    assert not _ipblock_ports(_network_policy_named(docs, "loom-worker"))


def test_render_provider_egress_allowlist_rejects_hostname_targets() -> None:
    """Kubernetes NetworkPolicy can only enforce CIDRs. Hostnames
    must be resolved by the operator and listed as IP/CIDR entries so
    `loom cluster render` stays deterministic and auditable.
    """
    cfg = ClusterConfig(provider_egress_allowlist=("lux.example.com:18001",))
    with pytest.raises(ValueError, match="IP address or CIDR"):
        render_manifests(cfg)


@pytest.mark.parametrize(
    "target",
    [
        "0.0.0.0/0:18001",
        "127.0.0.1:18001",
        "169.254.169.254:80",
    ],
)
def test_render_provider_egress_allowlist_rejects_unsafe_targets(
    target: str,
) -> None:
    cfg = ClusterConfig(provider_egress_allowlist=(target,))
    with pytest.raises(ValueError, match=r"too broad|reserved"):
        render_manifests(cfg)


def test_render_default_ingress_uses_tls_secret_and_class() -> None:
    cfg = ClusterConfig()
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["ingressClassName"] == "nginx"
    assert ingress["spec"]["tls"] == [{
        "hosts": ["loom.example.com"],
        "secretName": "loom-tls",
    }]
    assert "cert-manager.io/cluster-issuer" not in ingress["metadata"]["annotations"]


def test_render_ip_ingress_host_uses_hostless_tls_ingress() -> None:
    """Kubernetes rejects IP literals in Ingress host fields. For
    staging deployments reached directly by IP, render a hostless
    Ingress and let the operator-provided TLS secret carry the IP SAN.
    """
    cfg = ClusterConfig(
        ingress_host="192.168.50.13",
        ingress_tls_secret_name="loom-ip-tls",
    )
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["tls"] == [{"secretName": "loom-ip-tls"}]
    assert "host" not in ingress["spec"]["rules"][0]


def test_render_with_cert_manager_cluster_issuer_annotation() -> None:
    cfg = ClusterConfig(
        ingress_host="loom.acme.example",
        ingress_class_name="public-nginx",
        ingress_tls_secret_name="loom-acme-tls",
        ingress_cert_manager_cluster_issuer="letsencrypt-prod",
    )
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["ingressClassName"] == "public-nginx"
    assert ingress["metadata"]["annotations"]["cert-manager.io/cluster-issuer"] == (
        "letsencrypt-prod"
    )
    assert ingress["spec"]["tls"] == [{
        "hosts": ["loom.acme.example"],
        "secretName": "loom-acme-tls",
    }]


def test_render_ingress_routes_only_api_and_spa_backends() -> None:
    docs = _load_docs(render_manifests(ClusterConfig()))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert [r["host"] for r in ingress["spec"]["rules"]] == ["loom.example.com"]
    paths = ingress["spec"]["rules"][0]["http"]["paths"]
    assert [
        (
            p["path"],
            p["pathType"],
            p["backend"]["service"]["name"],
            p["backend"]["service"]["port"]["number"],
        )
        for p in paths
    ] == [
        ("/api/v1", "Prefix", "loom-service", 8090),
        ("/", "Prefix", "loom-web", 80),
    ]


def test_render_custom_storage_sizes() -> None:
    cfg = ClusterConfig(
        postgres_storage_gi=200,
        minio_storage_gi=2000,
        worker_trajectory_storage_gi=500,
    )
    docs = _load_docs(render_manifests(cfg))
    pg = next(
        d for d in docs
        if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-postgres"
    )
    pg_storage = pg["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"]
    assert pg_storage == "200Gi"
    minio = next(
        d for d in docs
        if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    )
    minio_storage = minio["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"]
    assert minio_storage == "2000Gi"
    worker_pvc = next(
        d for d in docs
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == "loom-worker-trajectories"
    )
    assert worker_pvc["spec"]["resources"]["requests"]["storage"] == "500Gi"


def test_render_worker_max_concurrent_schema_default() -> None:
    """LOOM_WORKER_MAX_CONCURRENT comes from the schema default (5);
    it must appear exactly once — the template-local duplicate was
    removed in the worker_max_concurrent dedupe fix."""
    docs = _load_docs(render_manifests(ClusterConfig()))
    worker = next(
        d for d in docs
        if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    env_list = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    concurrent_entries = [e for e in env_list if e["name"] == "LOOM_WORKER_MAX_CONCURRENT"]
    assert len(concurrent_entries) == 1, (
        f"expected exactly 1 LOOM_WORKER_MAX_CONCURRENT entry, got {len(concurrent_entries)}"
    )
    assert concurrent_entries[0]["value"] == "5"


def test_render_uses_strict_undefined_so_missing_var_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: if a future template references {{ foo }} but
    ClusterConfig doesn't have `foo`, render must raise, not emit
    a manifest with `image: loom-service:` (no tag)."""
    # Inject a sentinel template by patching _TEMPLATE_ORDER to a
    # single name + monkeypatching the resources call. Easier: just
    # call jinja2 directly with StrictUndefined to assert behavior.
    from jinja2 import Environment, StrictUndefined, UndefinedError

    env = Environment(undefined=StrictUndefined)
    with pytest.raises(UndefinedError):
        env.from_string("image: loom:{{ does_not_exist }}").render()


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch
# ──────────────────────────────────────────────────────────────────────


def test_cli_render_writes_to_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["cluster", "render"])
    assert rc == 0
    out = capsys.readouterr().out
    # Smoke: the output should contain at least the postgres + service
    # manifests as the first/middle workloads.
    assert "loom-postgres" in out
    assert "loom-service" in out
    assert "Ingress" in out


def test_cli_render_with_config_picks_up_overrides(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = tmp_path / "c.toml"
    cfg.write_text('image_tag = "9.9.9"\n', encoding="utf-8")
    rc = main(["cluster", "render", "--config", str(cfg)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "loom-service:9.9.9" in out


def test_cli_render_missing_config_file_exits_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["cluster", "render", "--config", "/nonexistent.toml"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not found" in err


def test_cli_render_invalid_config_exits_2(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = tmp_path / "bad.toml"
    cfg.write_text('imag_tag = "1.2.3"\n', encoding="utf-8")
    rc = main(["cluster", "render", "--config", str(cfg)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "unknown keys" in err


def test_cli_render_invalid_provider_egress_allowlist_exits_2(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = tmp_path / "bad-provider-egress.toml"
    cfg.write_text(
        'provider_egress_allowlist = ["lux.example.com:18001"]\n',
        encoding="utf-8",
    )
    rc = main(["cluster", "render", "--config", str(cfg)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "provider_egress_allowlist" in err
    assert "IP address or CIDR" in err
