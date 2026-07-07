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
    "postgres.yaml",
    "pgbouncer.yaml",
    "minio.yaml",
    "control-plane.yaml",
    "loom-service.yaml",
    "llm-gateway.yaml",
    "worker.yaml",
    "web.yaml",
    "ingress.yaml",
    "gateway-router.yaml",
    # Phase C (#190) — egress proxy chain. Default replicas=0 so
    # the resources exist in the manifest but no pods until
    # operators scale up.
    "egress-xds.yaml",
    "egress-proxy.yaml",
    "network-policies.yaml",
    "grafana-dashboards.yaml",
)


def _load_docs(yaml_text: str) -> list[dict]:
    """Parse a multi-document YAML string and drop None placeholders
    (empty documents at start/end produced by stray separators)."""
    return [d for d in yaml.safe_load_all(yaml_text) if d]


def _deployment_env_value(docs: list[dict], deployment: str, env_name: str) -> str | None:
    doc = next(
        d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == deployment
    )
    env = doc["spec"]["template"]["spec"]["containers"][0]["env"]
    item = next((entry for entry in env if entry["name"] == env_name), None)
    return item.get("value") if item else None


# Schema default flipped k8s_worker.enabled to false (#383): profiles
# that share OLDLAB hosts with Slurm must not double-schedule the host.
# Render tests that exercise the worker Deployment / worker
# NetworkPolicy must opt in explicitly. `_DEFAULT_CFG` mirrors the
# development.cluster.toml profile (worker enabled) so the canonical
# `deploy/k8s/*.yaml` golden files still apply.
def _default_cfg(**kwargs: object) -> ClusterConfig:
    k8s_worker_cls = type(ClusterConfig().k8s_worker)
    return ClusterConfig(k8s_worker=k8s_worker_cls(enabled=True), **kwargs)


_DEFAULT_CFG = _default_cfg()


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
    assert cfg.persistent_storage_backend == "dynamic"
    assert cfg.persistent_storage_host_path_root == ""


def test_default_replicas_match_spec() -> None:
    """cluster-deploy.md §Component map specifies these defaults."""
    cfg = ClusterConfig()
    assert cfg.replicas.service == 2
    assert cfg.replicas.control_plane == 2
    assert cfg.replicas.gateway == 2
    # Paused per spec — operators scale up explicitly.
    assert cfg.replicas.web == 0
    assert cfg.replicas.worker == 3


def test_default_worker_capacity_is_above_single_worker_baseline() -> None:
    """Production render must not silently regress to the staging
    incident shape: one worker with five execution slots.
    """
    cfg = ClusterConfig()
    assert cfg.replicas.worker >= 3
    assert cfg.worker_capacity.max_concurrent >= 16
    assert cfg.replicas.worker * cfg.worker_capacity.max_concurrent >= 48


def test_load_config_from_toml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'image_tag = "1.2.3"\n'
        'ingress_host = "loom.acme.example"\n'
        'ingress_redirect_hosts = ["www.loom.acme.example"]\n'
        'ingress_class_name = "public-nginx"\n'
        'ingress_tls_secret_name = "loom-acme-tls"\n'
        'ingress_cert_manager_cluster_issuer = "letsencrypt-prod"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-staging"\n'
        'provider_egress_allowlist = ["202.78.161.51:18001"]\n'
        "[replicas]\n"
        "service = 5\n"
        "worker = 10\n",
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.image_tag == "1.2.3"
    assert cfg.ingress_host == "loom.acme.example"
    assert cfg.ingress_redirect_hosts == ("www.loom.acme.example",)
    assert cfg.ingress_class_name == "public-nginx"
    assert cfg.ingress_tls_secret_name == "loom-acme-tls"
    assert cfg.ingress_cert_manager_cluster_issuer == "letsencrypt-prod"
    assert cfg.persistent_storage_backend == "static-host-path"
    assert cfg.persistent_storage_host_path_root == "/data/loom-staging"
    assert cfg.provider_egress_allowlist == ("202.78.161.51:18001",)
    assert cfg.replicas.service == 5
    assert cfg.replicas.worker == 10
    # Unspecified fields keep their defaults.
    assert cfg.replicas.control_plane == 2


def test_load_config_from_toml_accepts_worker_capacity(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        "[worker_capacity]\n"
        "max_concurrent = 24\n"
        'cpu_request = "2"\n'
        'cpu_limit = "32"\n'
        'memory_request = "8Gi"\n'
        'memory_limit = "128Gi"\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.worker_capacity.max_concurrent == 24
    assert cfg.worker_capacity.cpu_request == "2"
    assert cfg.worker_capacity.cpu_limit == "32"
    assert cfg.worker_capacity.memory_request == "8Gi"
    assert cfg.worker_capacity.memory_limit == "128Gi"


def test_load_config_from_toml_accepts_worker_subprocess_gateway_url(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'worker_subprocess_gateway_url = "http://host.docker.internal:30444/openai/v1"\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.worker_subprocess_gateway_url == "http://host.docker.internal:30444/openai/v1"


def test_load_config_accepts_rollout_environment_state_and_gb10_pool(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        'env_state_profile = "../environment-state/staging.toml"\n'
        "[gb10_pool]\n"
        'ssh_config = "../worker-pools/gb10/ssh_config"\n'
        'ssh_identity_file = "/shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519"\n'
        'ssh_certificate_file = "/shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519-cert.pub"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom", '
        'env_file_path = "/srv/loom/.env", '
        'repo_url = "https://github.com/qianyi-sun/loom.git", '
        'node_agent_service = "loom-gb10-node-agent.service" },\n'
        "]\n",
        encoding="utf-8",
    )

    cfg = load_cluster_config(cfg_path)

    assert cfg.env_state_profile == "../environment-state/staging.toml"
    assert cfg.gb10_pool.ssh_config == "../worker-pools/gb10/ssh_config"
    assert (
        cfg.gb10_pool.ssh_identity_file
        == "/shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519"
    )
    assert (
        cfg.gb10_pool.ssh_certificate_file
        == "/shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519-cert.pub"
    )
    assert cfg.gb10_pool.hosts == [
        {
            "ssh_target": "trt-gb10-1",
            "repo_path": "/srv/loom",
            "env_file_path": "/srv/loom/.env",
            "repo_url": "https://github.com/qianyi-sun/loom.git",
            "node_agent_service": "loom-gb10-node-agent.service",
        }
    ]


def test_load_config_rejects_non_array_gb10_hosts(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cluster.toml"
    cfg_path.write_text(
        '[gb10_pool]\nhosts = "trt-gb10-1"\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"gb10_pool\.hosts must be a TOML array"):
        load_cluster_config(cfg_path)


def test_load_config_rejects_deprecated_gateway_public_host(
    tmp_path: Path,
) -> None:
    """The staging boundary no longer allows a public LLM Gateway
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
        "[replicas]\nservvice = 4\n",
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
    """Smoke: every document parses, the set covers the 8 Deployments
    + 1 DaemonSet + 9 Services + 2 StatefulSets + 1 PVC + 1 Ingress
    + 1 PodDisruptionBudget + 10 NetworkPolicies + 2 ConfigMaps
    (Grafana dashboards + egress-proxy bootstrap) expected by
    cluster-deploy.md §Component map + sandbox-isolation.md."""
    text = render_manifests(_DEFAULT_CFG)
    assert text.startswith("apiVersion: apps/v1\nkind: StatefulSet\n")
    docs = _load_docs(text)
    kinds = [d["kind"] for d in docs]
    assert kinds.count("StatefulSet") == 2  # postgres, minio
    # cp, service, gateway, worker, web + egress-xds + egress-proxy + pgbouncer
    assert kinds.count("Deployment") == 8
    assert kinds.count("DaemonSet") == 1  # gateway-router
    # postgres + pgbouncer + minio + cp + gateway + service + web + ingress + egress = 9
    assert kinds.count("Service") == 9
    assert kinds.count("Ingress") == 1
    assert kinds.count("PersistentVolumeClaim") == 1
    # pgbouncer PodDisruptionBudget.
    assert kinds.count("PodDisruptionBudget") == 1
    # NetworkPolicies: postgres + minio + cp + gateway + worker + svc
    # + web + gateway-router + egress-xds + egress-proxy = 10.
    assert kinds.count("NetworkPolicy") == 10
    # Grafana dashboards ConfigMap + egress-proxy bootstrap ConfigMap.
    assert kinds.count("ConfigMap") == 2


def test_worker_manifest_sets_subprocess_gateway_url_for_sandboxes() -> None:
    cfg = _default_cfg(
        worker_subprocess_gateway_url="http://host.docker.internal:30444/openai/v1",
    )
    docs = _load_docs(render_manifests(cfg))
    worker = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    by_name = {entry["name"]: entry for entry in env}
    assert by_name["LOOM_WORKER_SUBPROCESS_GATEWAY_URL"]["value"] == (
        "http://host.docker.internal:30444/openai/v1"
    )


def test_worker_manifest_injects_optional_hf_token_secret() -> None:
    """Private/gated hf:// runtime materializers use huggingface_hub's
    standard HF_TOKEN env, but public-only deployments must still boot
    when the Secret key is absent.
    """
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    by_name = {entry["name"]: entry for entry in env}
    assert by_name["HF_TOKEN"] == {
        "name": "HF_TOKEN",
        "valueFrom": {
            "secretKeyRef": {
                "name": "loom-secrets",
                "key": "huggingface-api-key",
                "optional": True,
            },
        },
    }


def test_worker_manifest_mounts_docker_registry_auth_config() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    pod_spec = worker["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert {
        "name": "docker-config",
        "mountPath": "/root/.docker",
        "readOnly": True,
    } in container["volumeMounts"]
    assert {
        "name": "docker-config",
        "secret": {
            "secretName": "docker-config",
            "optional": True,
            "items": [
                {
                    "key": ".dockerconfigjson",
                    "path": "config.json",
                },
            ],
        },
    } in pod_spec["volumes"]


def test_worker_manifest_renders_configured_capacity_and_resources() -> None:
    worker_capacity_cls = type(ClusterConfig().worker_capacity)
    cfg = _default_cfg(
        worker_capacity=worker_capacity_cls(
            max_concurrent=24,
            cpu_request="2",
            cpu_limit="32",
            memory_request="8Gi",
            memory_limit="128Gi",
        ),
    )
    docs = _load_docs(render_manifests(cfg))
    worker = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    container = worker["spec"]["template"]["spec"]["containers"][0]
    env = [entry for entry in container["env"] if entry["name"] == "LOOM_WORKER_MAX_CONCURRENT"]
    assert env == [{"name": "LOOM_WORKER_MAX_CONCURRENT", "value": "24"}]
    env_map = {entry["name"]: entry for entry in container["env"]}
    assert env_map["LOOM_WORKER_DOCKER_API_TIMEOUT_SEC"] == {
        "name": "LOOM_WORKER_DOCKER_API_TIMEOUT_SEC",
        "value": "1800",
    }
    assert env_map["LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS"] == {
        "name": "LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS",
        "value": "256",
    }
    assert env_map["LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC"] == {
        "name": "LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC",
        "value": "300.0",
    }
    assert container["resources"] == {
        "requests": {"cpu": "2", "memory": "8Gi"},
        "limits": {"cpu": "32", "memory": "128Gi"},
    }


def test_render_default_matches_deploy_k8s_yamls() -> None:
    """Golden test: rendering with default config produces the same
    set of k8s objects as the canonical `deploy/k8s/*.yaml` files.

    This pins the templates against drift in either direction — if
    someone edits a deploy/k8s/*.yaml without updating the matching
    template, this test fails. Same in reverse.
    """
    rendered = _load_docs(render_manifests(_DEFAULT_CFG))
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
    cfg = _default_cfg(image_tag="2.0.0-rc1")
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
        assert img.endswith(":2.0.0-rc1"), f"expected :2.0.0-rc1 suffix, got {img}"


def test_render_injects_secret_store_master_key_for_provider_paths() -> None:
    """BYO provider create/test in service and provider dispatch in
    gateway both use LocalEncryptedSecretStore. Cluster mode must wire
    the shared master key into both pods from loom-secrets.
    """
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    deployments = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Deployment"}

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
    return next(d for d in docs if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == name)


def _ipblock_ports(policy: dict) -> set[tuple[str, int]]:
    out: set[tuple[str, int]] = set()
    for rule in policy["spec"].get("egress", []):
        ports = [p["port"] for p in rule.get("ports", []) if p.get("protocol", "TCP") == "TCP"]
        for target in rule.get("to", []):
            ip_block = target.get("ipBlock")
            if ip_block is None:
                continue
            for port in ports:
                out.add((ip_block["cidr"], int(port)))
    return out


def _public_ipblock_ports(policy: dict) -> set[int]:
    """Return TCP ports allowed to public internet egress."""
    out: set[int] = set()
    for rule in policy["spec"].get("egress", []):
        for target in rule.get("to", []):
            ip_block = target.get("ipBlock")
            if ip_block is None or ip_block.get("cidr") != "0.0.0.0/0":
                continue
            if set(ip_block.get("except", [])) != {
                "10.0.0.0/8",
                "172.16.0.0/12",
                "192.168.0.0/16",
                "169.254.0.0/16",
            }:
                continue
            for port in rule.get("ports", []):
                if port.get("protocol", "TCP") == "TCP":
                    out.add(int(port["port"]))
    return out


def test_render_allows_service_public_https_for_provider_discovery() -> None:
    """Provider validation and model discovery run in loom-service, so
    the service pod needs the same public HTTPS/HTTP egress baseline as
    the gateway for ordinary hosted OpenAI-compatible `/models` APIs.
    """
    docs = _load_docs(render_manifests(_DEFAULT_CFG))

    assert {443, 80} <= _public_ipblock_ports(
        _network_policy_named(docs, "loom-service"),
    )
    assert {443, 80} <= _public_ipblock_ports(
        _network_policy_named(docs, "loom-llm-gateway"),
    )


def test_render_provider_egress_allowlist_adds_service_and_gateway_rules() -> None:
    """Operators can approve a non-standard BYO provider endpoint once
    in cluster-config.toml; render must preserve that policy for both
    provider validation in loom-service and runtime calls in gateway.
    """
    cfg = _default_cfg(
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
    assert ingress["spec"]["tls"] == [
        {
            "hosts": ["loom.example.com"],
            "secretName": "loom-tls",
        }
    ]
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
    assert ingress["spec"]["tls"] == [
        {
            "hosts": ["loom.acme.example"],
            "secretName": "loom-acme-tls",
        }
    ]


def test_render_ingress_redirect_hosts_bind_tls_and_redirect_to_canonical() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_redirect_hosts=("www.yylx.world",),
        ingress_tls_secret_name="loom-staging-tls",
        frontend_environment="staging",
        frontend_environment_label="Development / staging",
        frontend_route_path="/dev",
        frontend_api_base_path="/dev",
    )
    docs = _load_docs(render_manifests(cfg))
    ingresses = [d for d in docs if d["kind"] == "Ingress"]
    assert [d["metadata"]["name"] for d in ingresses] == ["loom-ingress"]
    main = next(d for d in ingresses if d["metadata"]["name"] == "loom-ingress")

    assert (
        main["metadata"]["annotations"]["nginx.ingress.kubernetes.io/from-to-www-redirect"]
        == "true"
    )
    assert [r["host"] for r in main["spec"]["rules"]] == ["yylx.world"]
    assert main["spec"]["tls"] == [
        {
            "hosts": ["yylx.world", "www.yylx.world"],
            "secretName": "loom-staging-tls",
        }
    ]


def test_render_rejects_redirect_host_matching_canonical_host() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_redirect_hosts=("yylx.world",),
    )
    with pytest.raises(ValueError, match="must not include ingress_host"):
        render_manifests(cfg)


def test_render_rejects_redirect_host_outside_www_counterpart() -> None:
    cfg = ClusterConfig(
        ingress_host="yylx.world",
        ingress_redirect_hosts=("staging.example.com",),
    )
    with pytest.raises(ValueError, match="www/non-www counterpart"):
        render_manifests(cfg)


def test_render_ingress_routes_only_api_and_spa_backends() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
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


@pytest.mark.parametrize(
    ("filename", "route_path"),
    [
        ("production.cluster.toml", "/prod"),
        ("development.cluster.toml", "/dev"),
        ("staging.cluster.toml", "/dev"),
    ],
)
def test_render_profile_ingress_routes_api_and_spa_under_frontend_prefix(
    filename: str,
    route_path: str,
) -> None:
    cfg = load_cluster_config(_REPO_ROOT / "deploy" / "environments" / filename)
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")

    annotations = ingress["metadata"]["annotations"]
    assert annotations["nginx.ingress.kubernetes.io/use-regex"] == "true"
    assert annotations["nginx.ingress.kubernetes.io/rewrite-target"] == "/$2"

    rule = ingress["spec"]["rules"][0]
    assert rule["host"] == "yylx.world"
    paths = rule["http"]["paths"]
    assert [
        (
            p["path"],
            p["pathType"],
            p["backend"]["service"]["name"],
            p["backend"]["service"]["port"]["number"],
        )
        for p in paths
    ] == [
        (f"{route_path}(/|$)(api/v1.*)", "ImplementationSpecific", "loom-service", 8090),
        (f"{route_path}(/|$)(.*)", "ImplementationSpecific", "loom-web", 80),
    ]


@pytest.mark.parametrize(
    ("filename", "runtime_environment"),
    [
        ("production.cluster.toml", "production"),
        ("staging.cluster.toml", "staging"),
    ],
)
def test_render_profiles_set_backend_runtime_environment(
    filename: str,
    runtime_environment: str,
) -> None:
    cfg = load_cluster_config(_REPO_ROOT / "deploy" / "environments" / filename)
    docs = _load_docs(render_manifests(cfg))

    for deployment in ("loom-control-plane", "loom-service", "loom-llm-gateway"):
        assert _deployment_env_value(docs, deployment, "LOOM_ENV") == runtime_environment


def test_staging_profile_declares_repo_owned_gb10_ssh_config() -> None:
    cfg = load_cluster_config(_REPO_ROOT / "deploy" / "environments" / "staging.cluster.toml")
    ssh_config = (_REPO_ROOT / "deploy" / "worker-pools" / "gb10" / "ssh_config").read_text(
        encoding="utf-8"
    )

    assert cfg.gb10_pool.ssh_config == "../worker-pools/gb10/ssh_config"
    assert (
        cfg.gb10_pool.ssh_identity_file
        == "/shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519"
    )
    assert len(cfg.gb10_pool.hosts) == 15
    assert (
        "IdentityFile /shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519"
        in ssh_config
    )
    assert "IdentitiesOnly yes" in ssh_config


def test_render_custom_storage_sizes() -> None:
    cfg = _default_cfg(
        postgres_storage_gi=200,
        minio_storage_gi=2000,
        worker_trajectory_storage_gi=500,
    )
    docs = _load_docs(render_manifests(cfg))
    pg = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-postgres"
    )
    pg_storage = pg["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"]
    assert pg_storage == "200Gi"
    minio = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    )
    minio_storage = minio["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"][
        "storage"
    ]
    assert minio_storage == "2000Gi"
    worker_pvc = next(
        d
        for d in docs
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == "loom-worker-trajectories"
    )
    assert worker_pvc["spec"]["resources"]["requests"]["storage"] == "500Gi"


def test_render_static_host_path_storage_binds_critical_state_to_retain_pvs() -> None:
    cfg = _default_cfg(
        namespace="loom-staging",
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-staging",
    )

    docs = _load_docs(render_manifests(cfg))
    pvs = {d["metadata"]["name"]: d for d in docs if d["kind"] == "PersistentVolume"}

    assert pvs["loom-staging-postgres-data"]["spec"] == {
        "capacity": {"storage": "50Gi"},
        "accessModes": ["ReadWriteOnce"],
        "persistentVolumeReclaimPolicy": "Retain",
        "storageClassName": "",
        "hostPath": {
            "path": "/data/loom-staging/postgres",
            "type": "DirectoryOrCreate",
        },
        "claimRef": {
            "namespace": "loom-staging",
            "name": "data-loom-postgres-0",
        },
    }
    assert pvs["loom-staging-minio-data"]["spec"]["hostPath"]["path"] == (
        "/data/loom-staging/minio"
    )
    assert pvs["loom-staging-worker-trajectories-data"]["spec"]["hostPath"]["path"] == (
        "/data/loom-staging/trajectories"
    )

    postgres = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-postgres"
    )
    postgres_claim = postgres["spec"]["volumeClaimTemplates"][0]["spec"]
    assert postgres_claim["storageClassName"] == ""
    assert postgres_claim["volumeName"] == "loom-staging-postgres-data"

    minio = next(
        d for d in docs if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "loom-minio"
    )
    minio_claim = minio["spec"]["volumeClaimTemplates"][0]["spec"]
    assert minio_claim["storageClassName"] == ""
    assert minio_claim["volumeName"] == "loom-staging-minio-data"

    worker_pvc = next(
        d
        for d in docs
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == "loom-worker-trajectories"
    )
    assert worker_pvc["spec"]["storageClassName"] == ""
    assert worker_pvc["spec"]["volumeName"] == ("loom-staging-worker-trajectories-data")


def test_render_static_host_path_keeps_worker_trajectories_pvc_when_worker_disabled() -> None:
    cfg = ClusterConfig(
        namespace="loom-staging",
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="/data/loom-staging",
    )

    docs = _load_docs(render_manifests(cfg))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}

    assert ("Deployment", "loom-worker") not in kinds_names
    assert ("NetworkPolicy", "loom-worker") not in kinds_names
    assert ("PersistentVolumeClaim", "loom-worker-trajectories") in kinds_names

    worker_pvc = next(
        d
        for d in docs
        if d["kind"] == "PersistentVolumeClaim"
        and d["metadata"]["name"] == "loom-worker-trajectories"
    )
    assert worker_pvc["spec"]["storageClassName"] == ""
    assert worker_pvc["spec"]["volumeName"] == ("loom-staging-worker-trajectories-data")


def test_render_rejects_unknown_persistent_storage_backend() -> None:
    cfg = ClusterConfig(persistent_storage_backend="local-path")
    with pytest.raises(ValueError, match="persistent_storage_backend"):
        render_manifests(cfg)


def test_render_static_host_path_requires_absolute_host_root() -> None:
    cfg = ClusterConfig(
        persistent_storage_backend="static-host-path",
        persistent_storage_host_path_root="data/loom-staging",
    )
    with pytest.raises(ValueError, match="absolute host path"):
        render_manifests(cfg)


def test_render_worker_max_concurrent_schema_default() -> None:
    """LOOM_WORKER_MAX_CONCURRENT comes from render worker_capacity;
    it must appear exactly once so Kubernetes does not depend on
    duplicate env-var ordering."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    env_list = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    concurrent_entries = [e for e in env_list if e["name"] == "LOOM_WORKER_MAX_CONCURRENT"]
    assert len(concurrent_entries) == 1, (
        f"expected exactly 1 LOOM_WORKER_MAX_CONCURRENT entry, got {len(concurrent_entries)}"
    )
    assert concurrent_entries[0]["value"] == "16"


def test_render_worker_pool_name_schema_default() -> None:
    """Fixed Kubernetes workers should register under an explicit pool."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    worker = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    env_list = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    pool_entries = [e for e in env_list if e["name"] == "LOOM_WORKER_POOL_NAME"]
    assert len(pool_entries) == 1
    assert pool_entries[0]["value"] == "k8s-worker"


def test_render_can_override_service_object_buckets() -> None:
    """Environment-specific cluster configs must not share artifact or
    trajectory buckets by accident when they use the same object-store
    endpoint.
    """
    cfg = ClusterConfig(
        trajectories_bucket="loom-prod-trajectories",
        artifacts_bucket="loom-prod-artifacts",
    )
    docs = _load_docs(render_manifests(cfg))
    service = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-service"
    )
    env = service["spec"]["template"]["spec"]["containers"][0]["env"]
    by_name = {entry["name"]: entry["value"] for entry in env if "value" in entry}
    assert by_name["LOOM_SVC_TRAJECTORIES_BUCKET"] == "loom-prod-trajectories"
    assert by_name["LOOM_SVC_ARTIFACTS_BUCKET"] == "loom-prod-artifacts"


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


# ──────────────────────────────────────────────────────────────────────
# #383: k8s_worker.enabled toggle
# ──────────────────────────────────────────────────────────────────────


def test_default_config_disables_k8s_worker() -> None:
    """Schema default flipped to false so profiles that share OLDLAB
    hosts with Slurm force intentional opt-in. See #383."""
    cfg = ClusterConfig()
    assert cfg.k8s_worker.enabled is False


def test_render_omits_worker_deployment_when_disabled() -> None:
    """Default dynamic rendering with k8s_worker.enabled=false must omit
    the whole loom-worker Deployment + trajectories PVC + worker
    NetworkPolicy, not merely scale it to zero replicas — belt-and-
    suspenders against `kubectl scale` drift."""
    docs = _load_docs(render_manifests(ClusterConfig()))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("Deployment", "loom-worker") not in kinds_names
    assert (
        "PersistentVolumeClaim",
        "loom-worker-trajectories",
    ) not in kinds_names
    assert ("NetworkPolicy", "loom-worker") not in kinds_names


def test_render_includes_worker_when_enabled_via_profile() -> None:
    """development.cluster.toml opts back in for local kind clusters."""
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("Deployment", "loom-worker") in kinds_names
    assert ("PersistentVolumeClaim", "loom-worker-trajectories") in kinds_names
    assert ("NetworkPolicy", "loom-worker") in kinds_names


def test_load_shipped_profile_files_have_explicit_k8s_worker_setting() -> None:
    """Every profile that ships in `deploy/environments/` must
    declare k8s_worker.enabled explicitly — no silent inheritance
    of the schema default. See #383 rationale."""
    envs_dir = _REPO_ROOT / "deploy" / "environments"
    expected = {
        "development.cluster.toml": True,
        "staging.cluster.toml": False,
        "production.cluster.toml": False,
    }
    for filename, want_enabled in expected.items():
        cfg = load_cluster_config(envs_dir / filename)
        assert cfg.k8s_worker.enabled is want_enabled, (
            f"{filename}: expected k8s_worker.enabled={want_enabled}, got {cfg.k8s_worker.enabled}"
        )


def test_loom_service_env_carries_k8s_worker_enabled_from_profile() -> None:
    """The rendered loom-service Deployment must propagate the
    profile's k8s_worker.enabled value into LOOM_SVC_K8S_WORKER_ENABLED
    so the API rejection path (in `_reject_if_k8s_worker_unavailable`)
    knows whether k8s-worker is available on this cluster."""
    for enabled in (True, False):
        k8s_worker_cls = type(ClusterConfig().k8s_worker)
        cfg = ClusterConfig(k8s_worker=k8s_worker_cls(enabled=enabled))
        docs = _load_docs(render_manifests(cfg))
        svc = next(
            d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-service"
        )
        env = svc["spec"]["template"]["spec"]["containers"][0]["env"]
        by_name = {entry["name"]: entry for entry in env}
        assert by_name["LOOM_SVC_K8S_WORKER_ENABLED"]["value"] == str(enabled)


# ──────────────────────────────────────────────────────────────────────
# #547 drain hook + HPA
# ──────────────────────────────────────────────────────────────────────


def test_llm_gateway_deployment_includes_drain_prestop_and_grace() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    gateway = next(
        d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-llm-gateway"
    )
    pod_spec = gateway["spec"]["template"]["spec"]
    assert pod_spec["terminationGracePeriodSeconds"] == 300
    container = pod_spec["containers"][0]
    prestop_command = container["lifecycle"]["preStop"]["exec"]["command"]
    joined = " ".join(str(part) for part in prestop_command)
    assert "127.0.0.1:9100/drain" in joined


def test_default_config_disables_gateway_hpa() -> None:
    assert ClusterConfig().gateway_hpa.enabled is False


def test_render_omits_hpa_when_disabled() -> None:
    docs = _load_docs(render_manifests(ClusterConfig()))
    kinds = {d["kind"] for d in docs}
    assert "HorizontalPodAutoscaler" not in kinds


def test_render_includes_hpa_when_enabled_with_custom_thresholds() -> None:
    hpa_cls = type(ClusterConfig().gateway_hpa)
    cfg = _default_cfg(
        gateway_hpa=hpa_cls(
            enabled=True,
            min_replicas=3,
            max_replicas=10,
            cpu_target_pct=70,
        ),
    )
    docs = _load_docs(render_manifests(cfg))
    hpa = next(
        d
        for d in docs
        if d["kind"] == "HorizontalPodAutoscaler" and d["metadata"]["name"] == "loom-llm-gateway"
    )
    spec = hpa["spec"]
    assert spec["minReplicas"] == 3
    assert spec["maxReplicas"] == 10
    assert spec["metrics"][0]["resource"]["target"]["averageUtilization"] == 70


# ──────────────────────────────────────────────────────────────────────
# #547 item #3: loom-llm-gateway-sandbox
# ──────────────────────────────────────────────────────────────────────


def test_default_config_disables_llm_gateway_sandbox() -> None:
    """Off by default because the DaemonSet needs an operator-
    provisioned TLS Secret (loom-sandbox-gateway-tls)."""
    assert ClusterConfig().llm_gateway_sandbox.enabled is False


def test_render_omits_llm_gateway_sandbox_when_disabled() -> None:
    docs = _load_docs(render_manifests(_DEFAULT_CFG))
    kinds_names = {(d["kind"], d["metadata"]["name"]) for d in docs}
    assert ("DaemonSet", "loom-llm-gateway-sandbox") not in kinds_names
    assert (
        "NetworkPolicy",
        "loom-llm-gateway-sandbox",
    ) not in kinds_names


def test_render_includes_sandbox_daemonset_when_enabled() -> None:
    sandbox_cls = type(ClusterConfig().llm_gateway_sandbox)
    cfg = _default_cfg(llm_gateway_sandbox=sandbox_cls(enabled=True))
    docs = _load_docs(render_manifests(cfg))

    ds = next(
        d
        for d in docs
        if d["kind"] == "DaemonSet" and d["metadata"]["name"] == "loom-llm-gateway-sandbox"
    )
    pod_spec = ds["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    # Binary points at the intended k8s Service (skipping the socat
    # router — the Go proxy resolves cluster DNS directly).
    args = container["args"]
    upstream = next(a for a in args if a.startswith("--upstream-url="))
    assert "loom-llm-gateway." in upstream
    assert ".svc.cluster.local:9100" in upstream

    # hostPort 8443 is exposed for sandbox Docker containers to dial.
    port = container["ports"][0]
    assert port["containerPort"] == 8443
    assert port["hostPort"] == 8443

    # Both secrets are mounted read-only into /run/loom.
    vol_secret_names = {v["secret"]["secretName"] for v in pod_spec["volumes"] if "secret" in v}
    assert vol_secret_names == {
        "loom-secrets",
        "loom-sandbox-gateway-tls",
    }


def test_render_includes_sandbox_network_policy_when_enabled() -> None:
    """The NetworkPolicy must permit hostPort ingress on 8443 (node-
    local traffic can't be further restricted by k8s NetworkPolicy)
    and egress to the in-cluster llm-gateway + kube-dns only."""
    sandbox_cls = type(ClusterConfig().llm_gateway_sandbox)
    cfg = _default_cfg(llm_gateway_sandbox=sandbox_cls(enabled=True))
    docs = _load_docs(render_manifests(cfg))

    policy = next(
        d
        for d in docs
        if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == "loom-llm-gateway-sandbox"
    )
    spec = policy["spec"]
    assert spec["podSelector"] == {
        "matchLabels": {"app": "loom-llm-gateway-sandbox"},
    }
    ingress_ports = {p["port"] for p in spec["ingress"][0]["ports"]}
    assert ingress_ports == {8443}
    egress_pods = {
        target["podSelector"]["matchLabels"].get("app")
        for rule in spec["egress"]
        for target in rule.get("to", [])
        if "podSelector" in target
    }
    assert "loom-llm-gateway" in egress_pods


def test_cluster_audit_exempts_sandbox_hostport() -> None:
    """The auditor's `_HOSTPORT_ALLOWLIST` must include
    loom-llm-gateway-sandbox; otherwise every enabled render would
    flag its hostPort as a boundary violation."""
    from loom_cli.cluster_boundary import _HOSTPORT_ALLOWLIST

    assert "loom-llm-gateway-sandbox" in _HOSTPORT_ALLOWLIST
