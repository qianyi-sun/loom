from __future__ import annotations

import tomllib
from importlib import resources
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNTIME_ASSETS = {
    Path("config/loom-schema.toml"): ("loom-schema.toml",),
    Path("deploy/grafana/dashboards/operator-overview.json"): (
        "grafana",
        "operator-overview.json",
    ),
    Path("deploy/grafana/dashboards/control-plane.json"): (
        "grafana",
        "control-plane.json",
    ),
    Path("deploy/grafana/dashboards/llm-gateway.json"): (
        "grafana",
        "llm-gateway.json",
    ),
    Path("deploy/grafana/dashboards/loom-service.json"): (
        "grafana",
        "loom-service.json",
    ),
    Path("deploy/grafana/dashboards/worker.json"): ("grafana", "worker.json"),
    Path("deploy/envoy/egress-proxy.yaml"): ("envoy", "egress-proxy.yaml"),
}


def test_bundled_cluster_runtime_assets_match_canonical_sources() -> None:
    package_root = resources.files("loom_cli.data")

    for canonical_path, resource_parts in _RUNTIME_ASSETS.items():
        assert (
            package_root.joinpath(*resource_parts).read_bytes()
            == (_REPO_ROOT / canonical_path).read_bytes()
        )


def test_bundled_cluster_runtime_assets_are_declared_as_wheel_package_data() -> None:
    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    package_data = pyproject["tool"]["setuptools"]["package-data"]

    assert set(package_data["loom_cli.data"]) >= {
        "*.toml",
        "grafana/*.json",
        "envoy/*.yaml",
    }
    assert set(package_data["loom_cli.templates.k8s"]) >= {"*.yaml.j2", "_env.j2"}
    assert "*.json" in package_data["loom_cli.data"]
    assert (
        resources.files("loom_cli.data")
        .joinpath("staging-rollout-preflight-coverage.json")
        .is_file()
    )
