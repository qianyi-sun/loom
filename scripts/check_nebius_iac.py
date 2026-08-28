from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
NEBIUS_ROOT = REPO_ROOT / "deploy" / "terraform" / "nebius"
TOPOLOGY_PATH = REPO_ROOT / "config" / "service-execution-topology.json"

class ContractError(RuntimeError):
    pass


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContractError(f"{path}: expected a JSON object")
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _backend_value(text: str, name: str, path: Path) -> str:
    match = re.search(rf'^\s*{re.escape(name)}\s*=\s*"([^"]+)"\s*$', text, re.MULTILINE)
    if match is None:
        raise ContractError(f"{path}: missing quoted {name}")
    return match.group(1)


def check_nebius_iac(
    *,
    repo_root: Path = REPO_ROOT,
    nebius_root: Path | None = None,
) -> None:
    root = nebius_root or repo_root / "deploy" / "terraform" / "nebius"
    topology = _load_json(repo_root / "config" / "service-execution-topology.json")
    targets = topology.get("targets")
    if not isinstance(targets, list):
        raise ContractError("execution topology must contain a targets list")
    topology_targets = {
        str(target["target_id"]): target
        for target in targets
        if isinstance(target, dict) and target.get("provider") == "nebius"
    }

    target_paths = sorted((root / "targets").glob("*.tfvars.json.example"))
    _require(len(target_paths) == 1, f"expected exactly 1 shared-cluster example, found {len(target_paths)}")
    _require(len(topology_targets) == 3, f"expected exactly 3 environment bindings, found {len(topology_targets)}")
    path = target_paths[0]
    _require(
        path.name == "development-eu-north1.tfvars.json.example",
        f"{path}: the live development state anchor must be retained during convergence",
    )
    document = _load_json(path)
    target = document.get("target")
    if not isinstance(target, dict):
        raise ContractError(f"{path}: target must be an object")
    _require(
        target.get("target_id") == "nebius-eu-north1-development",
        f"{path}: target_id must retain the live state/resource anchor",
    )
    cluster_scope_ids = {str(item.get("cluster_scope_id", "")) for item in topology_targets.values()}
    _require(len(cluster_scope_ids) == 1 and "" not in cluster_scope_ids, "topology targets must share one cluster_scope_id")
    cluster_scope_id = next(iter(cluster_scope_ids))
    _require(target.get("cluster_scope_id") == cluster_scope_id, f"{path}: cluster_scope_id must match topology")
    regions = {str(item.get("region", "")) for item in topology_targets.values()}
    failure_domains = {str(item.get("failure_domain", "")) for item in topology_targets.values()}
    _require(regions == {target.get("region")}, f"{path}: all bindings must use the shared cluster region")
    _require(failure_domains == {target.get("failure_domain")}, f"{path}: all bindings must use one failure domain")

    bindings = target.get("environment_bindings")
    if not isinstance(bindings, dict):
        raise ContractError(f"{path}: environment_bindings must be an object")
    _require(set(bindings) == {"development", "staging", "production"}, f"{path}: all three environment bindings are required")
    for environment, binding in bindings.items():
        if not isinstance(binding, dict):
            raise ContractError(f"{path}: {environment} binding must be an object")
        target_id = str(binding.get("target_id", ""))
        topology_target = topology_targets.get(target_id)
        if topology_target is None:
            raise ContractError(f"{path}: target_id {target_id!r} is absent from topology")
        _require(topology_target.get("environment") == environment, f"{path}: {environment} target environment must match topology")
        _require(binding.get("namespace_name") == topology_target.get("namespace_name"), f"{path}: {environment} namespace_name must match topology")
        _require(binding.get("evidence_prefix") == target_id, f"{path}: {environment} evidence prefix must equal target_id")

    _require(document.get("tenant_id") == "tenant-REPLACE", f"{path}: example tenant must remain a placeholder")
    _require(
        str(document.get("project_id", "")).startswith("project-REPLACE-"),
        f"{path}: example project must remain a placeholder",
    )
    for name in ("nebius_profile", "project_id", "evidence_bucket_name"):
        _require(bool(str(document.get(name, ""))), f"{path}: {name} is required")
    for name in ("network_cidr", "service_cidr"):
        _require(bool(str(target.get(name, ""))), f"{path}: {name} is required")
    cidrs = target.get("public_control_plane_cidrs")
    if not isinstance(cidrs, list):
        raise ContractError(f"{path}: public_control_plane_cidrs must be a list")
    _require("0.0.0.0/0" not in cidrs, f"{path}: public control plane may never allow 0.0.0.0/0")
    _require(int(target.get("execution_min_nodes", -1)) == 0, f"{path}: shared execution baseline must scale to zero")

    backend_paths = sorted((root / "backends").glob("*.s3.tfbackend.example"))
    _require(len(backend_paths) == 1, f"expected exactly 1 shared-cluster backend, found {len(backend_paths)}")
    _require(
        backend_paths[0].name == "development-eu-north1.s3.tfbackend.example",
        "the existing development backend anchor must be reused, not forked",
    )
    for path in backend_paths:
        text = path.read_text(encoding="utf-8")
        key = _backend_value(text, "key", path)
        _require(key == "nebius/development/eu-north1/terraform.tfstate", f"{path}: existing remote state key must be retained")
        _require(re.search(r"^\s*use_lockfile\s*=\s*true\s*$", text, re.MULTILINE) is not None, f"{path}: use_lockfile must be true")
        _require(re.search(r"(?i)(access_key|secret_key|token)\s*=", text) is None, f"{path}: credentials may not be stored in backend files")

    versions = (root / "modules" / "execution-target" / "versions.tf").read_text(encoding="utf-8")
    stack_versions = (root / "stack" / "versions.tf").read_text(encoding="utf-8")
    for path, text in (
        (root / "modules" / "execution-target" / "versions.tf", versions),
        (root / "stack" / "versions.tf", stack_versions),
    ):
        _require('required_version = "= 1.16.0"' in text, f"{path}: Terraform must be pinned to 1.16.0")
        _require('source  = "nebius/nebius"' in text, f"{path}: provider source must be nebius/nebius")
        _require('version = "= 0.6.46"' in text, f"{path}: Nebius provider must be pinned to 0.6.46")
        _require(path.with_name(".terraform.lock.hcl").is_file(), f"{path}: provider lock file is required")

    module = (root / "modules" / "execution-target" / "main.tf").read_text(encoding="utf-8")
    _require("public_ip_address" not in module, "execution target must not assign node public IP addresses")
    _require(module.count('policy = "FORBID"') == 2, "both node groups must forbid capacity reservations")
    _require('key    = "loom.nebius/execution"' in module, "execution node taint is required")
    _require("audit_logs        = {}" in module, "managed control-plane audit logging is required")
    _require('role        = "viewer"' in module, "registry pull access must remain resource-scoped viewer")
    _require(
        "for_each = var.environment_bindings" in module,
        "evidence writer groups must remain environment-local",
    )
    _require(
        'nebius_iam_v1_group.evidence_writers[environment].id' in module,
        "every evidence prefix must bind its environment-local writer group",
    )
    _require('roles    = ["storage.object-editor"]' in module, "evidence writers need object-only access")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the repository-owned Nebius IaC contract.")
    parser.parse_args()
    try:
        check_nebius_iac()
    except (ContractError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Nebius IaC contract failed: {exc}") from exc
    print("Nebius IaC contract passed for 1 shared cluster and 3 environment bindings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
