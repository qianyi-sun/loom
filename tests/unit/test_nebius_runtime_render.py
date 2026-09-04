from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]
from scripts.ops.render_nebius_runtime import main as render_main

import loom.nebius_runtime_render as runtime_render
from loom.nebius_runtime_render import NebiusRuntimeRenderError, render_nebius_runtime

_IMAGE = "registry.example/loom-execution-actuator@sha256:" + "a" * 64


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _policy(tmp_path: Path, target_id: str) -> Path:
    path = tmp_path / "capacity.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": f"loom.nebius-{target_id.rsplit('-', 1)[-1]}-capacity.v1",
                "target_id": target_id,
                "accepted_concurrency": 56,
                "target_concurrency": 200,
                "policy": {
                    "enabled": True,
                    "max_nodes": 8,
                    "max_vcpu_millis": 128_000,
                    "max_memory_mib": 524_288,
                },
                "admission_policies": [],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_render_staging_binds_exact_topology_and_physical_ids(tmp_path: Path) -> None:
    root = _repo_root()
    output = tmp_path / "rendered"
    manifest = render_nebius_runtime(
        repo_root=root,
        environment="staging",
        image=_IMAGE,
        topology_path=root / "config/service-execution-topology.json",
        physical_binding_path=root / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=_policy(tmp_path, "nebius-eu-north1-staging"),
        output_dir=output,
    )

    assert manifest["target_id"] == "nebius-eu-north1-staging"
    assert manifest["namespace"] == "loom-nebius-staging"
    actuator = (output / "nebius-execution-actuator.yaml").read_text()
    collector = (output / "nebius-capacity-collector.yaml").read_text()
    control_plane = (output / "nebius-control-plane-staging-patch.yaml").read_text()
    assert "loom-nebius-development" not in actuator + collector
    assert "nebius-eu-north1-development" not in actuator + collector
    assert actuator.count(_IMAGE) == 1
    actuator_documents = list(yaml.safe_load_all(actuator))
    quota = next(row for row in actuator_documents if row["kind"] == "ResourceQuota")
    assert quota["metadata"]["namespace"] == "loom-nebius-staging"
    assert quota["spec"]["hard"] == {
        "pods": "72",
        "requests.cpu": "128",
        "requests.memory": "512Gi",
    }
    assert collector.count(_IMAGE) == 2
    assert "project-e00ksehzpr00ftw5pe61gt" in collector
    assert "mk8snodegroup-e00n6mbxcz8jgp8bat" in collector
    assert "value: staging" in control_plane
    assert "value: development" not in control_plane
    for path in output.glob("*.yaml"):
        assert list(yaml.safe_load_all(path.read_text()))

    manifest_bytes = (output / "render-manifest.json").read_bytes()
    assert (output / "render-manifest.json.sha256").read_text() == (
        f"{hashlib.sha256(manifest_bytes).hexdigest()}  render-manifest.json\n"
    )
    assert {row["path"] for row in manifest["files"]} == {
        "nebius-execution-actuator.yaml",
        "nebius-capacity-collector.yaml",
        "nebius-control-plane-staging-patch.yaml",
        "nebius-service-staging-patch.yaml",
        "nebius-gateway-staging-patch.yaml",
        "nebius-staging-capacity-policy.json",
    }


def test_render_refuses_cross_environment_capacity_policy(tmp_path: Path) -> None:
    root = _repo_root()
    with pytest.raises(NebiusRuntimeRenderError, match="capacity policy"):
        render_nebius_runtime(
            repo_root=root,
            environment="production",
            image=_IMAGE,
            topology_path=root / "config/service-execution-topology.json",
            physical_binding_path=root / "config/nebius-runtime-physical-binding.json",
            capacity_policy_path=_policy(tmp_path, "nebius-eu-north1-staging"),
            output_dir=tmp_path / "rendered",
        )


def test_render_refuses_mutating_nonempty_output_directory(tmp_path: Path) -> None:
    root = _repo_root()
    output = tmp_path / "rendered"
    output.mkdir()
    (output / "owned.txt").write_text("preserve", encoding="utf-8")
    with pytest.raises(NebiusRuntimeRenderError, match="not empty"):
        render_nebius_runtime(
            repo_root=root,
            environment="development",
            image=_IMAGE,
            topology_path=root / "config/service-execution-topology.json",
            physical_binding_path=root / "config/nebius-runtime-physical-binding.json",
            capacity_policy_path=root / "deploy/k8s/nebius-development-capacity-policy.json",
            output_dir=output,
        )
    assert (output / "owned.txt").read_text() == "preserve"


def test_render_derives_namespace_quota_from_capacity_policy(tmp_path: Path) -> None:
    root = _repo_root()
    policy = _policy(tmp_path, "nebius-eu-north1-production")
    document = json.loads(policy.read_text(encoding="utf-8"))
    document["accepted_concurrency"] = 200
    document["policy"]["max_vcpu_millis"] = 480_500
    document["policy"]["max_memory_mib"] = 1_966_080
    policy.write_text(json.dumps(document), encoding="utf-8")

    output = tmp_path / "production"
    render_nebius_runtime(
        repo_root=root,
        environment="production",
        image=_IMAGE,
        topology_path=root / "config/service-execution-topology.json",
        physical_binding_path=root / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=policy,
        output_dir=output,
    )

    documents = list(yaml.safe_load_all((output / "nebius-execution-actuator.yaml").read_text()))
    quota = next(row for row in documents if row["kind"] == "ResourceQuota")
    assert quota["spec"]["hard"] == {
        "pods": "216",
        "requests.cpu": "480500m",
        "requests.memory": "1920Gi",
    }


def test_render_refuses_output_file(tmp_path: Path) -> None:
    root = _repo_root()
    output = tmp_path / "rendered"
    output.write_text("owned", encoding="utf-8")

    with pytest.raises(NebiusRuntimeRenderError, match="not a directory"):
        render_nebius_runtime(
            repo_root=root,
            environment="development",
            image=_IMAGE,
            topology_path=root / "config/service-execution-topology.json",
            physical_binding_path=root / "config/nebius-runtime-physical-binding.json",
            capacity_policy_path=root / "deploy/k8s/nebius-development-capacity-policy.json",
            output_dir=output,
        )


def test_render_cli_defaults_are_repo_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    output = tmp_path / "rendered"

    assert (
        render_main(
            [
                "--environment",
                "development",
                "--image",
                _IMAGE,
                "--capacity-policy",
                str(_repo_root() / "deploy/k8s/nebius-development-capacity-policy.json"),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert (output / "render-manifest.json").is_file()


def test_minimal_gateway_render_does_not_require_pyyaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo_root()
    monkeypatch.setattr(runtime_render, "yaml", None)

    manifest = render_nebius_runtime(
        repo_root=root,
        environment="development",
        image=_IMAGE,
        topology_path=root / "config/service-execution-topology.json",
        physical_binding_path=root / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=root / "deploy/k8s/nebius-development-capacity-policy.json",
        output_dir=tmp_path / "rendered",
    )

    assert manifest["environment"] == "development"
