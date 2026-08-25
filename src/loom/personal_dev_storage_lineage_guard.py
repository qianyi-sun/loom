"""Fail-closed preflight for personal-dev StatefulSet storage upgrades."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

_EXPECTED_STATEFUL_SETS = {"loom-dev-minio", "loom-dev-postgres"}
_EXPECTED_GENERATED_CLAIMS = {
    "data-loom-dev-minio-0": "loom-dev-minio",
    "data-loom-dev-postgres-0": "loom-dev-postgres",
}
_SCANNER_CACHE_CLAIM = "loom-personal-dev-scanner-cache"
_MANAGED_BY = "loom-personal-dev-control-plane"
_ACCEPTANCE_PLAN_KEY = "loom.dev/acceptance-plan-sha256"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_STORAGE = re.compile(r"[1-9][0-9]*(?:Mi|Gi)")
_VOLUME_NAME = re.compile(
    r"pvc-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_MAX_LIVE_ITEMS = 16
_BINDING_ANNOTATIONS = {
    "pv.kubernetes.io/bind-completed": "yes",
    "pv.kubernetes.io/bound-by-controller": "yes",
    "volume.beta.kubernetes.io/storage-provisioner": "driver.longhorn.io",
    "volume.kubernetes.io/storage-provisioner": "driver.longhorn.io",
}


class StorageLineageGuardError(ValueError):
    """Raised when reviewed or live storage lacks the exact contract."""


def _bounded_bytes(path: Path, *, subject: str) -> bytes:
    try:
        with path.open("rb") as source:
            payload = source.read(_MAX_MANIFEST_BYTES + 1)
    except OSError:
        raise StorageLineageGuardError(f"{subject} is invalid") from None
    if not 0 < len(payload) <= _MAX_MANIFEST_BYTES:
        raise StorageLineageGuardError(f"{subject} size is invalid")
    return payload


def _canonical(value: object, *, subject: str) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (RecursionError, TypeError, UnicodeError, ValueError):
        raise StorageLineageGuardError(f"{subject} is invalid") from None


def _claim_template_is_valid(template: object) -> bool:
    if not isinstance(template, dict) or set(template) != {"metadata", "spec"}:
        return False
    metadata = template.get("metadata")
    spec = template.get("spec")
    if (
        not isinstance(metadata, dict)
        or set(metadata) != {"name", "labels", "annotations"}
        or metadata.get("name") != "data"
        or not isinstance(spec, dict)
        or set(spec) != {"accessModes", "resources", "storageClassName"}
        or spec.get("accessModes") != ["ReadWriteOnce"]
        or not isinstance(spec.get("storageClassName"), str)
        or not spec["storageClassName"]
    ):
        return False
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    resources = spec.get("resources")
    if (
        not isinstance(labels, dict)
        or set(labels)
        != {
            "app.kubernetes.io/managed-by",
            "app.kubernetes.io/part-of",
            "loom.dev/render-input",
            "loom.dev/trusted-release",
        }
        or labels.get("app.kubernetes.io/managed-by") != _MANAGED_BY
        or labels.get("app.kubernetes.io/part-of") != "loom"
        or not isinstance(annotations, dict)
        or set(annotations)
        != {
            "loom.dev/render-input-sha256",
            "loom.dev/trusted-release-sha256",
        }
        or _ACCEPTANCE_PLAN_KEY in labels
        or _ACCEPTANCE_PLAN_KEY in annotations
        or not isinstance(resources, dict)
        or set(resources) != {"requests"}
        or not isinstance(resources.get("requests"), dict)
        or set(resources["requests"]) != {"storage"}
        or not isinstance(resources["requests"].get("storage"), str)
        or _STORAGE.fullmatch(resources["requests"]["storage"]) is None
    ):
        return False
    render_digest = annotations.get("loom.dev/render-input-sha256")
    release_digest = annotations.get("loom.dev/trusted-release-sha256")
    return (
        isinstance(render_digest, str)
        and isinstance(release_digest, str)
        and _DIGEST.fullmatch(render_digest) is not None
        and _DIGEST.fullmatch(release_digest) is not None
        and render_digest != "0" * 64
        and release_digest != "0" * 64
        and labels.get("loom.dev/render-input") == render_digest[:32]
        and labels.get("loom.dev/trusted-release") == release_digest[:32]
    )


def _claim_template_has_acceptance_metadata(template: object) -> bool:
    if not isinstance(template, dict) or not isinstance(template.get("metadata"), dict):
        return False
    metadata = template["metadata"]
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    return (
        (isinstance(labels, dict) and _ACCEPTANCE_PLAN_KEY in labels)
        or (
            isinstance(annotations, dict)
            and _ACCEPTANCE_PLAN_KEY in annotations
        )
    )


def _claim_templates(path: Path) -> dict[str, dict[str, Any]]:
    payload = _bounded_bytes(path, subject="manifest")
    try:
        documents = yaml.safe_load_all(payload.decode("utf-8"))
        stateful_sets: dict[str, dict[str, Any]] = {}
        for document in documents:
            if not isinstance(document, dict) or document.get("kind") != "StatefulSet":
                continue
            metadata = document.get("metadata")
            spec = document.get("spec")
            if not isinstance(metadata, dict) or not isinstance(spec, dict):
                raise StorageLineageGuardError("StatefulSet shape is invalid")
            name = metadata.get("name")
            templates = spec.get("volumeClaimTemplates")
            if (
                document.get("apiVersion") != "apps/v1"
                or metadata.get("namespace") != "loom-dev"
                or name not in _EXPECTED_STATEFUL_SETS
                or name in stateful_sets
                or not isinstance(templates, list)
                or len(templates) != 1
                or not _claim_template_is_valid(templates[0])
            ):
                message = (
                    "claim template metadata is invalid"
                    if isinstance(templates, list)
                    and len(templates) == 1
                    and _claim_template_has_acceptance_metadata(templates[0])
                    else "StatefulSet storage contract is invalid"
                )
                raise StorageLineageGuardError(message)
            stateful_sets[name] = templates[0]
    except StorageLineageGuardError:
        raise
    except (RecursionError, UnicodeError, yaml.YAMLError):
        raise StorageLineageGuardError("manifest is invalid") from None
    if set(stateful_sets) != _EXPECTED_STATEFUL_SETS:
        raise StorageLineageGuardError("StatefulSet storage inventory is incomplete")
    if len({_canonical(value["metadata"], subject="claim template metadata") for value in stateful_sets.values()}) != 1:
        raise StorageLineageGuardError("StatefulSet storage lineage is inconsistent")
    _canonical(stateful_sets, subject="StatefulSet claim template")
    return stateful_sets


def _live_inventory(path: Path) -> list[dict[str, Any]]:
    payload = _bounded_bytes(path, subject="live storage inventory")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (RecursionError, UnicodeError, json.JSONDecodeError):
        raise StorageLineageGuardError("live storage inventory is invalid") from None
    items = document.get("items") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or not set(document).issubset({"apiVersion", "kind", "metadata", "items"})
        or document.get("apiVersion") != "v1"
        or document.get("kind") != "List"
        or not isinstance(items, list)
        or len(items) > _MAX_LIVE_ITEMS
        or not all(isinstance(item, dict) for item in items)
    ):
        raise StorageLineageGuardError("live storage inventory is invalid")
    _canonical(document, subject="live storage inventory")
    return items


def _live_identity(item: dict[str, Any]) -> tuple[str, str]:
    metadata = item.get("metadata")
    kind = item.get("kind")
    if (
        not isinstance(metadata, dict)
        or kind not in {"StatefulSet", "PersistentVolumeClaim"}
        or item.get("apiVersion") != ("apps/v1" if kind == "StatefulSet" else "v1")
        or metadata.get("namespace") != "loom-dev"
        or not isinstance(metadata.get("name"), str)
        or not isinstance(metadata.get("labels"), dict)
        or metadata["labels"].get("app.kubernetes.io/managed-by") != _MANAGED_BY
    ):
        raise StorageLineageGuardError("live storage identity is invalid")
    return kind, metadata["name"]


def _expected_live_template(template: dict[str, Any]) -> dict[str, Any]:
    expected = copy.deepcopy(template)
    expected["apiVersion"] = "v1"
    expected["kind"] = "PersistentVolumeClaim"
    expected["spec"]["volumeMode"] = "Filesystem"
    expected["status"] = {"phase": "Pending"}
    return expected


def _normalized_live_template(template: object) -> object:
    normalized = copy.deepcopy(template)
    if not isinstance(normalized, dict) or not isinstance(normalized.get("metadata"), dict):
        return normalized
    metadata = normalized["metadata"]
    if metadata.get("creationTimestamp") is None:
        metadata.pop("creationTimestamp", None)
    return normalized


def _live_stateful_matches(item: dict[str, Any], template: dict[str, Any]) -> bool:
    spec = item.get("spec")
    templates = spec.get("volumeClaimTemplates") if isinstance(spec, dict) else None
    return (
        isinstance(templates, list)
        and len(templates) == 1
        and _canonical(
            _normalized_live_template(templates[0]),
            subject="live stateful claim template",
        )
        == _canonical(_expected_live_template(template), subject="expected live claim template")
    )


def _live_claim_matches(
    item: dict[str, Any],
    *,
    stateful_name: str,
    template: dict[str, Any],
) -> bool:
    metadata = item.get("metadata")
    spec = item.get("spec")
    status = item.get("status")
    if not isinstance(metadata, dict) or not isinstance(spec, dict) or not isinstance(status, dict):
        return False
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    expected_labels = {**template["metadata"]["labels"], "app": stateful_name}
    expected_annotations = {**template["metadata"]["annotations"], **_BINDING_ANNOTATIONS}
    volume_name = spec.get("volumeName")
    normalized_spec = {key: value for key, value in spec.items() if key != "volumeName"}
    expected_spec = copy.deepcopy(template["spec"])
    expected_spec["volumeMode"] = "Filesystem"
    expected_status = {
        "accessModes": copy.deepcopy(template["spec"]["accessModes"]),
        "capacity": copy.deepcopy(template["spec"]["resources"]["requests"]),
        "phase": "Bound",
    }
    return (
        labels == expected_labels
        and annotations == expected_annotations
        and _ACCEPTANCE_PLAN_KEY not in labels
        and _ACCEPTANCE_PLAN_KEY not in annotations
        and isinstance(volume_name, str)
        and _VOLUME_NAME.fullmatch(volume_name) is not None
        and _canonical(normalized_spec, subject="live generated claim spec")
        == _canonical(expected_spec, subject="expected generated claim spec")
        and _canonical(status, subject="live generated claim status")
        == _canonical(expected_status, subject="expected generated claim status")
    )


def _validate_live_upgrade(
    items: list[dict[str, Any]],
    templates: dict[str, dict[str, Any]],
) -> None:
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        identity = _live_identity(item)
        if identity in indexed:
            raise StorageLineageGuardError("live storage identity is duplicated")
        indexed[identity] = item
    expected_identities = {
        *(('StatefulSet', name) for name in _EXPECTED_STATEFUL_SETS),
        *(('PersistentVolumeClaim', name) for name in _EXPECTED_GENERATED_CLAIMS),
        ("PersistentVolumeClaim", _SCANNER_CACHE_CLAIM),
    }
    if set(indexed) != expected_identities:
        raise StorageLineageGuardError("live storage inventory is incomplete or unexpected")
    for name, template in templates.items():
        if not _live_stateful_matches(indexed[("StatefulSet", name)], template):
            raise StorageLineageGuardError(f"live stateful {name} storage lineage differs")
    for name, stateful_name in _EXPECTED_GENERATED_CLAIMS.items():
        if not _live_claim_matches(
            indexed[("PersistentVolumeClaim", name)],
            stateful_name=stateful_name,
            template=templates[stateful_name],
        ):
            raise StorageLineageGuardError(f"live claim {name} storage lineage differs")
    scanner = indexed[("PersistentVolumeClaim", _SCANNER_CACHE_CLAIM)]
    scanner_status = scanner.get("status")
    if not isinstance(scanner_status, dict) or scanner_status.get("phase") != "Bound":
        raise StorageLineageGuardError("live scanner cache is not bound")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate immutable personal-dev StatefulSet claim templates."
    )
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--live-inventory", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        current = _claim_templates(arguments.current)
        live = _live_inventory(arguments.live_inventory)
        if arguments.previous is None:
            if live:
                raise StorageLineageGuardError(
                    "first install requires absent live storage resources"
                )
        else:
            previous = _claim_templates(arguments.previous)
            if _canonical(current, subject="current claim templates") != _canonical(
                previous,
                subject="previous claim templates",
            ):
                raise StorageLineageGuardError(
                    "StatefulSet claim templates differ from installed storage lineage"
                )
            _validate_live_upgrade(live, previous)
    except StorageLineageGuardError as exc:
        sys.stderr.write(f"personal-dev storage lineage rejected: {exc}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
