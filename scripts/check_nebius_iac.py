from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
NEBIUS_ROOT = REPO_ROOT / "deploy" / "terraform" / "nebius"
TOPOLOGY_PATH = REPO_ROOT / "config" / "service-execution-topology.json"

TOPOLOGY_FIELDS = (
    "target_id",
    "environment",
    "region",
    "failure_domain",
    "namespace_name",
)


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
    expected = {
        str(target["target_id"]): target
        for target in targets
        if isinstance(target, dict) and target.get("provider") == "nebius"
    }

    target_paths = sorted((root / "targets").glob("*.tfvars.json.example"))
    _require(len(target_paths) == 4, f"expected exactly 4 target examples, found {len(target_paths)}")
    _require(len(expected) == 4, f"expected exactly 4 Nebius topology targets, found {len(expected)}")

    seen_ids: set[str] = set()
    unique_values: dict[str, set[str]] = {
        "nebius_profile": set(),
        "project_id": set(),
        "evidence_bucket_name": set(),
        "network_cidr": set(),
        "service_cidr": set(),
    }
    expected_stems: set[str] = set()

    for path in target_paths:
        document = _load_json(path)
        target = document.get("target")
        if not isinstance(target, dict):
            raise ContractError(f"{path}: target must be an object")
        target_id = str(target.get("target_id", ""))
        _require(target_id in expected, f"{path}: target_id {target_id!r} is absent from topology")
        _require(target_id not in seen_ids, f"{path}: duplicate target_id {target_id}")
        seen_ids.add(target_id)

        topology_target = expected[target_id]
        for field in TOPOLOGY_FIELDS:
            _require(
                target.get(field) == topology_target.get(field),
                f"{path}: {field} must match config/service-execution-topology.json",
            )

        stem = f"{target['environment']}-{target['region']}"
        expected_stems.add(stem)
        _require(path.name == f"{stem}.tfvars.json.example", f"{path}: filename must be {stem}.tfvars.json.example")
        _require(document.get("tenant_id") == "tenant-REPLACE", f"{path}: example tenant must remain a placeholder")
        _require(
            str(document.get("project_id", "")).startswith("project-REPLACE-"),
            f"{path}: example project must remain a placeholder",
        )

        for name in ("nebius_profile", "project_id", "evidence_bucket_name"):
            value = str(document.get(name, ""))
            _require(bool(value), f"{path}: {name} is required")
            _require(value not in unique_values[name], f"{path}: {name} must be target-unique")
            unique_values[name].add(value)
        for name in ("network_cidr", "service_cidr"):
            value = str(target.get(name, ""))
            _require(value not in unique_values[name], f"{path}: {name} must be target-unique")
            unique_values[name].add(value)

        cidrs = target.get("public_control_plane_cidrs")
        if not isinstance(cidrs, list):
            raise ContractError(f"{path}: public_control_plane_cidrs must be a list")
        _require("0.0.0.0/0" not in cidrs, f"{path}: public control plane may never allow 0.0.0.0/0")
        if target.get("environment") == "production":
            _require(target.get("control_plane_etcd_size") == 3, f"{path}: production requires HA etcd")
            _require(int(target.get("execution_min_nodes", 0)) >= 1, f"{path}: production requires a warm execution node")

    _require(seen_ids == set(expected), "target examples and topology targets differ")

    backend_paths = sorted((root / "backends").glob("*.s3.tfbackend.example"))
    _require(len(backend_paths) == 4, f"expected exactly 4 backend examples, found {len(backend_paths)}")
    _require(
        {path.name.removesuffix(".s3.tfbackend.example") for path in backend_paths} == expected_stems,
        "backend examples must match target environment-region names",
    )
    state_keys: set[str] = set()
    for path in backend_paths:
        text = path.read_text(encoding="utf-8")
        key = _backend_value(text, "key", path)
        _require(key not in state_keys, f"{path}: state key must be target-unique")
        state_keys.add(key)
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
    _require('roles    = ["storage.object-editor"]' in module, "evidence writers need object-only access")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the repository-owned Nebius IaC contract.")
    parser.parse_args()
    try:
        check_nebius_iac()
    except (ContractError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Nebius IaC contract failed: {exc}") from exc
    print("Nebius IaC contract passed for 4 isolated execution targets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
