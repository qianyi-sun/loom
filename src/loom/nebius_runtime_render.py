"""Deterministic multi-environment renderer for the Nebius runtime manifests."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, cast

try:
    import yaml  # type: ignore[import-untyped]
except ModuleNotFoundError:  # Minimal deployment gateway; kubectl validates after render.
    yaml = None

_ENVIRONMENTS = frozenset({"development", "staging", "production"})
_IMAGE = re.compile(r"[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}\Z")
_ID = re.compile(r"[a-z][a-z0-9-]{2,159}\Z")
_SOURCE_ENVIRONMENT = "development"
_SOURCE_TARGET = "nebius-eu-north1-development"
_SOURCE_NAMESPACE = "loom-nebius-development"
_SOURCE_PROJECT = "project-e00ksehzpr00ftw5pe61gt"
_SOURCE_QUOTA_PARENT = "tenant-e00zcze7mmwb61vk7e"
_SOURCE_NODE_GROUP = "mk8snodegroup-e00n6mbxcz8jgp8bat"
_SOURCE_REGION = "eu-north1"
_ACTUATOR_TEMPLATE = "nebius-execution-actuator.yaml"
_COLLECTOR_TEMPLATE = "nebius-capacity-collector.yaml"
_PATCH_TEMPLATES = (
    "nebius-control-plane-development-patch.yaml",
    "nebius-service-development-patch.yaml",
    "nebius-gateway-development-patch.yaml",
)


class NebiusRuntimeRenderError(ValueError):
    """A source binding or rendered manifest is unsafe/inconsistent."""


def _read_object(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise NebiusRuntimeRenderError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise NebiusRuntimeRenderError(f"{label} must be a JSON object")
    return cast(dict[str, Any], value), raw


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _binding_value(binding: dict[str, Any], name: str) -> str:
    value = binding.get(name)
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise NebiusRuntimeRenderError(f"physical binding {name} is invalid")
    return value


def _load_target(topology: dict[str, Any], environment: str) -> dict[str, Any]:
    if topology.get("schema_version") != "loom.execution-topology.v1":
        raise NebiusRuntimeRenderError("execution topology schema is invalid")
    rows = topology.get("targets")
    matches = [
        row for row in rows or [] if isinstance(row, dict) and row.get("environment") == environment
    ]
    if len(matches) != 1:
        raise NebiusRuntimeRenderError(
            f"execution topology must contain exactly one {environment!r} target"
        )
    target = cast(dict[str, Any], matches[0])
    if (
        target.get("provider") != "nebius"
        or target.get("logical_pool_id") != topology.get("logical_pool_id")
        or target.get("region") != "eu-north1"
    ):
        raise NebiusRuntimeRenderError("execution target is not the Nebius EU pool")
    for name in ("target_id", "namespace_name", "cluster_scope_id"):
        if not isinstance(target.get(name), str) or _ID.fullmatch(target[name]) is None:
            raise NebiusRuntimeRenderError(f"execution target {name} is invalid")
    return target


def _replace_exact(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count == 0:
        raise NebiusRuntimeRenderError(f"{label} does not contain expected token {old!r}")
    return text.replace(old, new)


def _resource_quota_values(policy: dict[str, Any]) -> tuple[str, str, str]:
    capacity = policy.get("policy")
    if not isinstance(capacity, dict) or capacity.get("enabled") is not True:
        raise NebiusRuntimeRenderError("capacity policy must contain an enabled policy")
    try:
        accepted = int(policy["accepted_concurrency"])
        cpu_millis = int(capacity["max_vcpu_millis"])
        memory_mib = int(capacity["max_memory_mib"])
    except (KeyError, TypeError, ValueError) as exc:
        raise NebiusRuntimeRenderError("capacity policy resource maxima are invalid") from exc
    if accepted <= 0 or cpu_millis <= 0 or memory_mib <= 0:
        raise NebiusRuntimeRenderError("capacity policy resource maxima must be positive")
    pod_limit = str(accepted + 16)
    cpu_limit = str(cpu_millis // 1_000) if cpu_millis % 1_000 == 0 else f"{cpu_millis}m"
    memory_limit = f"{memory_mib // 1_024}Gi" if memory_mib % 1_024 == 0 else f"{memory_mib}Mi"
    return pod_limit, cpu_limit, memory_limit


def _render_resource_quota(text: str, policy: dict[str, Any], *, label: str) -> str:
    pods, cpu, memory = _resource_quota_values(policy)
    replacements = {
        '    pods: "72"': f'    pods: "{pods}"',
        '    requests.cpu: "128"': f'    requests.cpu: "{cpu}"',
        "    requests.memory: 512Gi": f"    requests.memory: {memory}",
    }
    for old, new in replacements.items():
        text = _replace_exact(text, old, new, label=label)
    return text


def _render_yaml(
    source: Path,
    *,
    target: dict[str, Any],
    binding: dict[str, Any],
    policy: dict[str, Any],
    image: str,
    replace_physical: bool,
) -> bytes:
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise NebiusRuntimeRenderError(f"cannot read runtime template {source}: {exc}") from exc
    if source.name in {_ACTUATOR_TEMPLATE, _COLLECTOR_TEMPLATE}:
        text = _replace_exact(
            text,
            _SOURCE_NAMESPACE,
            cast(str, target["namespace_name"]),
            label=source.name,
        )
    if source.name == _ACTUATOR_TEMPLATE:
        text = _render_resource_quota(text, policy, label=source.name)
    if _SOURCE_TARGET in text:
        text = _replace_exact(
            text,
            _SOURCE_TARGET,
            cast(str, target["target_id"]),
            label=source.name,
        )
    if source.name in _PATCH_TEMPLATES:
        text, environment_count = re.subn(
            r"(?m)^(\s+value:\s+)development\s*$",
            rf"\g<1>{target['environment']}",
            text,
        )
        if environment_count == 0:
            raise NebiusRuntimeRenderError(f"{source.name} does not contain an environment value")
    if replace_physical:
        replacements = {
            _SOURCE_PROJECT: _binding_value(binding, "project_id"),
            _SOURCE_QUOTA_PARENT: _binding_value(binding, "quota_parent_id"),
            _SOURCE_NODE_GROUP: _binding_value(binding, "execution_node_group_id"),
            _SOURCE_REGION: _binding_value(binding, "region"),
        }
        for old, new in replacements.items():
            text = _replace_exact(text, old, new, label=source.name)
    image_lines = re.findall(r"(?m)^(\s+image:\s+)(\S+)\s*$", text)
    if source.name == _ACTUATOR_TEMPLATE and len(image_lines) != 1:
        raise NebiusRuntimeRenderError("actuator template must contain exactly one image")
    if source.name == _COLLECTOR_TEMPLATE and len(image_lines) != 2:
        raise NebiusRuntimeRenderError("collector template must contain exactly two images")
    if image_lines:
        text = re.sub(r"(?m)^(\s+image:\s+)\S+\s*$", rf"\g<1>{image}", text)
    if yaml is None:
        documents = [item for item in re.split(r"(?m)^---\s*$", text) if item.strip()]
        invalid_patch = source.name in _PATCH_TEMPLATES and (
            len(documents) != 1 or re.search(r"(?m)^spec:\s*$", documents[0]) is None
        )
        invalid_manifest = source.name not in _PATCH_TEMPLATES and any(
            re.search(r"(?m)^apiVersion:\s*\S+\s*$", item) is None
            or re.search(r"(?m)^kind:\s*\S+\s*$", item) is None
            for item in documents
        )
        if invalid_patch or invalid_manifest:
            raise NebiusRuntimeRenderError(
                f"rendered {source.name} lacks the expected Kubernetes structure"
            )
    else:
        try:
            documents = [item for item in yaml.safe_load_all(text) if item is not None]
        except yaml.YAMLError as exc:
            raise NebiusRuntimeRenderError(
                f"rendered {source.name} is invalid YAML: {exc}"
            ) from exc
    if not documents:
        raise NebiusRuntimeRenderError(f"rendered {source.name} contains no YAML objects")
    if target["environment"] != _SOURCE_ENVIRONMENT and (
        _SOURCE_NAMESPACE in text or _SOURCE_TARGET in text
    ):
        raise NebiusRuntimeRenderError(f"rendered {source.name} retains development identity")
    return text.encode()


def render_nebius_runtime(
    *,
    repo_root: Path,
    environment: str,
    image: str,
    topology_path: Path,
    physical_binding_path: Path,
    capacity_policy_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    if environment not in _ENVIRONMENTS:
        raise NebiusRuntimeRenderError(f"unsupported environment {environment!r}")
    if _IMAGE.fullmatch(image) is None:
        raise NebiusRuntimeRenderError("execution actuator image must be digest-pinned")
    if output_dir.exists():
        if not output_dir.is_dir():
            raise NebiusRuntimeRenderError(f"output path is not a directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise NebiusRuntimeRenderError(f"output directory is not empty: {output_dir}")
    topology, topology_raw = _read_object(topology_path, label="execution topology")
    binding, binding_raw = _read_object(physical_binding_path, label="physical binding")
    policy, policy_raw = _read_object(capacity_policy_path, label="capacity policy")
    if binding.get("schema_version") != "loom.nebius-runtime-physical-binding.v1":
        raise NebiusRuntimeRenderError("physical binding schema is invalid")
    target = _load_target(topology, environment)
    if policy.get("schema_version") != f"loom.nebius-{environment}-capacity.v1":
        raise NebiusRuntimeRenderError("capacity policy schema does not match selected environment")
    if target["cluster_scope_id"] != binding.get("cluster_scope_id"):
        raise NebiusRuntimeRenderError("target and physical binding cluster scopes differ")
    if policy.get("target_id") != target["target_id"]:
        raise NebiusRuntimeRenderError("capacity policy target does not match selected environment")

    template_dir = repo_root / "deploy" / "k8s"
    output_dir.mkdir(parents=True, exist_ok=True)
    rendered: dict[str, bytes] = {}
    for name in (_ACTUATOR_TEMPLATE, _COLLECTOR_TEMPLATE, *_PATCH_TEMPLATES):
        output_name = name.replace("-development-patch", f"-{environment}-patch")
        rendered[output_name] = _render_yaml(
            template_dir / name,
            target=target,
            binding=binding,
            policy=policy,
            image=image,
            replace_physical=name == _COLLECTOR_TEMPLATE,
        )
    rendered[f"nebius-{environment}-capacity-policy.json"] = policy_raw
    for name, payload in rendered.items():
        (output_dir / name).write_bytes(payload)

    manifest: dict[str, Any] = {
        "schema_version": "loom.nebius-runtime-render.v1",
        "environment": environment,
        "target_id": target["target_id"],
        "namespace": target["namespace_name"],
        "cluster_scope_id": target["cluster_scope_id"],
        "logical_pool_id": topology["logical_pool_id"],
        "image": image,
        "source_sha256": {
            "topology": _sha256(topology_raw),
            "physical_binding": _sha256(binding_raw),
            "capacity_policy": _sha256(policy_raw),
        },
        "files": [
            {"path": name, "sha256": _sha256(payload), "size_bytes": len(payload)}
            for name, payload in sorted(rendered.items())
        ],
    }
    manifest_bytes = (
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode()
    (output_dir / "render-manifest.json").write_bytes(manifest_bytes)
    (output_dir / "render-manifest.json.sha256").write_text(
        f"{_sha256(manifest_bytes)}  render-manifest.json\n",
        encoding="utf-8",
    )
    return manifest


__all__ = ["NebiusRuntimeRenderError", "render_nebius_runtime"]
