from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from scripts.check_nebius_iac import ContractError, check_nebius_iac

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = REPO_ROOT / "deploy" / "terraform" / "nebius"


def _copy_contract(tmp_path: Path) -> tuple[Path, Path]:
    repo_root = tmp_path / "repo"
    nebius_root = repo_root / "deploy" / "terraform" / "nebius"
    shutil.copytree(SOURCE_ROOT, nebius_root, ignore=shutil.ignore_patterns(".terraform"))
    (repo_root / "config").mkdir(parents=True)
    shutil.copy2(REPO_ROOT / "config" / "service-execution-topology.json", repo_root / "config")
    (repo_root / "deploy" / "k8s").mkdir(parents=True)
    shutil.copy2(
        REPO_ROOT / "deploy" / "k8s" / "nebius-development-capacity-policy.json",
        repo_root / "deploy" / "k8s",
    )
    (repo_root / "scripts" / "ops").mkdir(parents=True)
    shutil.copy2(
        REPO_ROOT / "scripts" / "ops" / "with_nebius_terraform_state_credentials.sh",
        repo_root / "scripts" / "ops",
    )
    return repo_root, nebius_root


def test_repository_nebius_contract_passes() -> None:
    check_nebius_iac()


def test_topology_drift_is_rejected(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = nebius_root / "targets" / "development-eu-north1.tfvars.json.example"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["target"]["environment_bindings"]["development"]["namespace_name"] = "wrong-namespace"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="namespace_name must match"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_public_world_control_plane_is_rejected(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = nebius_root / "targets" / "development-eu-north1.tfvars.json.example"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["target"]["public_control_plane_cidrs"] = ["0.0.0.0/0"]
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="may never allow"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_backend_credentials_are_rejected(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = nebius_root / "backends" / "development-eu-north1.s3.tfbackend.example"
    path.write_text(
        path.read_text(encoding="utf-8") + '\nsecret_key = "not-allowed"\n', encoding="utf-8"
    )

    with pytest.raises(ContractError, match="credentials may not be stored"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_human_nebius_profile_is_rejected(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = nebius_root / "targets" / "development-eu-north1.tfvars.json.example"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["nebius_profile"] = "loom-development-eu-north1"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="non-expiring service-account profile"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_missing_persistent_deployment_access_is_rejected(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = nebius_root / "targets" / "development-eu-north1.tfvars.json.example"
    document = json.loads(path.read_text(encoding="utf-8"))
    del document["deployment_access_public_pool_id"]
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="deployment_access_public_pool_id is required"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_execution_nodes_remain_private_with_public_deployment_gateway(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    module_path = nebius_root / "modules" / "execution-target" / "main.tf"
    text = module_path.read_text(encoding="utf-8")
    marker = 'resource "nebius_mk8s_v1_node_group" "execution"'
    text = text.replace(
        marker,
        f"{marker}\n# drift\npublic_ip_address = {{}}",
        1,
    )
    module_path.write_text(text, encoding="utf-8")

    with pytest.raises(ContractError, match="nodes must not assign public IP"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_capacity_policy_rejects_less_than_80_two_vcpu_tasks(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = repo_root / "deploy" / "k8s" / "nebius-development-capacity-policy.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["accepted_concurrency"] = 79
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="80 concurrent 2-vCPU tasks"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_capacity_policy_rejects_lost_200_task_target(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = repo_root / "deploy" / "k8s" / "nebius-development-capacity-policy.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["target_concurrency"] = 199
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="retain 200-task capacity"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_capacity_policy_rejects_terraform_shape_drift(tmp_path: Path) -> None:
    repo_root, nebius_root = _copy_contract(tmp_path)
    path = nebius_root / "targets" / "development-eu-north1.tfvars.json.example"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["target"]["execution_preset"] = "32vcpu-128gb"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ContractError, match="policy node shape must match Terraform"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)


def test_gateway_runtime_apply_carries_and_converges_capacity_policy() -> None:
    inner = (REPO_ROOT / "scripts" / "ops" / "apply_nebius_development_runtime.sh").read_text(
        encoding="utf-8"
    )
    gateway = (
        REPO_ROOT / "scripts" / "ops" / "apply_nebius_development_runtime_via_gateway.sh"
    ).read_text(encoding="utf-8")

    assert (
        'capacity_policy="$repo_root/deploy/k8s/nebius-development-capacity-policy.json"' in inner
    )
    assert "/admin/execution-capacity-policies/" in inner
    assert 'open("/var/run/loom/admin/secrets.toml", "rb")' in inner
    assert '"max_nodes": observed["max_nodes"]' in inner
    assert "deploy/k8s/nebius-development-capacity-policy.json" in gateway
