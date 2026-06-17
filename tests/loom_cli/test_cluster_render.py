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
    "network-policies.yaml",
)


def _load_docs(yaml_text: str) -> list[dict]:
    """Parse a multi-document YAML string and drop None placeholders
    (empty documents at start/end produced by stray separators)."""
    return [d for d in yaml.safe_load_all(yaml_text) if d]


# ──────────────────────────────────────────────────────────────────────
# ClusterConfig + load
# ──────────────────────────────────────────────────────────────────────


def test_default_config_omits_gateway_public_host() -> None:
    """Issue #77 boundary: default install MUST NOT expose the gateway
    publicly. If this changes, the operator-facing security posture
    changes too — needs a deliberate decision, not a default flip."""
    cfg = ClusterConfig()
    assert cfg.gateway_public_host == ""


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
        'gateway_public_host = "gw.acme.example"\n'
        'worker_max_concurrent = 12\n'
        '[replicas]\n'
        'service = 5\n'
        'worker = 10\n',
        encoding="utf-8",
    )
    cfg = load_cluster_config(cfg_path)
    assert cfg.image_tag == "1.2.3"
    assert cfg.ingress_host == "loom.acme.example"
    assert cfg.gateway_public_host == "gw.acme.example"
    assert cfg.worker_max_concurrent == 12
    assert cfg.replicas.service == 5
    assert cfg.replicas.worker == 10
    # Unspecified fields keep their defaults.
    assert cfg.replicas.control_plane == 2


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
    """Smoke: every document parses, the set covers the 5 Deployments
    + 1 DaemonSet + 6 Services + 2 StatefulSets + 1 PVC + 1 Ingress
    + 8 NetworkPolicies expected by cluster-deploy.md §Component
    map + sandbox-isolation.md."""
    text = render_manifests(ClusterConfig())
    docs = _load_docs(text)
    kinds = [d["kind"] for d in docs]
    assert kinds.count("StatefulSet") == 2   # postgres, minio
    assert kinds.count("Deployment") == 5    # cp, service, gateway, worker, web
    assert kinds.count("DaemonSet") == 1     # gateway-router
    assert kinds.count("Service") == 6
    assert kinds.count("Ingress") == 1
    assert kinds.count("PersistentVolumeClaim") == 1
    # NetworkPolicies: postgres + minio + cp + gateway + worker + svc
    # + web + gateway-router = 8.
    assert kinds.count("NetworkPolicy") == 8


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


def test_render_with_gateway_public_host_emits_second_ingress_rule() -> None:
    """Opt-in: setting gateway_public_host adds a second host rule
    to the Ingress without disturbing the primary one."""
    cfg = ClusterConfig(gateway_public_host="gw.example.com")
    docs = _load_docs(render_manifests(cfg))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    hosts = [r["host"] for r in ingress["spec"]["rules"]]
    assert "loom.example.com" in hosts
    assert "gw.example.com" in hosts
    # The gateway rule routes to the right service.
    gw_rule = next(
        r for r in ingress["spec"]["rules"] if r["host"] == "gw.example.com"
    )
    backend = gw_rule["http"]["paths"][0]["backend"]["service"]
    assert backend["name"] == "loom-llm-gateway"
    assert backend["port"]["number"] == 9100


def test_render_without_gateway_public_host_emits_only_primary_host() -> None:
    docs = _load_docs(render_manifests(ClusterConfig()))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    hosts = {r["host"] for r in ingress["spec"]["rules"]}
    assert hosts == {"loom.example.com"}


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


def test_render_custom_worker_max_concurrent() -> None:
    """Cluster operators must be able to size worker throughput
    explicitly instead of relying on the worker process default."""
    docs = _load_docs(render_manifests(ClusterConfig(worker_max_concurrent=12)))
    worker = next(
        d for d in docs
        if d["kind"] == "Deployment" and d["metadata"]["name"] == "loom-worker"
    )
    env = {
        item["name"]: item
        for item in worker["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["LOOM_WORKER_MAX_CONCURRENT"] == {
        "name": "LOOM_WORKER_MAX_CONCURRENT",
        "value": "12",
    }


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
