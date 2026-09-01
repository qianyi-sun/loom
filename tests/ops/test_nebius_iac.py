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
        f'{marker}\n# drift\npublic_ip_address = {{}}',
        1,
    )
    module_path.write_text(text, encoding="utf-8")

    with pytest.raises(ContractError, match="nodes must not assign public IP"):
        check_nebius_iac(repo_root=repo_root, nebius_root=nebius_root)
