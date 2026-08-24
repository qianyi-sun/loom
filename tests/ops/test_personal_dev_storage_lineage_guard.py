from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

_RENDER_LINEAGE = "a" * 64
_RELEASE_LINEAGE = "b" * 64
_MANAGED_BY = "loom-personal-dev-control-plane"
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024


def _lineage_metadata(render_lineage: str) -> dict[str, Any]:
    return {
        "name": "data",
        "labels": {
            "app.kubernetes.io/managed-by": _MANAGED_BY,
            "app.kubernetes.io/part-of": "loom",
            "loom.dev/render-input": render_lineage[:32],
            "loom.dev/trusted-release": _RELEASE_LINEAGE[:32],
        },
        "annotations": {
            "loom.dev/render-input-sha256": render_lineage,
            "loom.dev/trusted-release-sha256": _RELEASE_LINEAGE,
        },
    }


def _stateful_set(name: str, *, release: str, lineage: str) -> dict[str, Any]:
    storage = "100Gi" if name == "loom-dev-minio" else "20Gi"
    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": name,
            "namespace": "loom-dev",
            "labels": {"app.kubernetes.io/managed-by": _MANAGED_BY},
            "annotations": {"loom.dev/trusted-release-sha256": release},
        },
        "spec": {
            "serviceName": name,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "app", "image": release}]},
            },
            "volumeClaimTemplates": [
                {
                    "metadata": _lineage_metadata(lineage),
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "longhorn",
                        "resources": {"requests": {"storage": storage}},
                    },
                }
            ],
        },
    }


def _manifest_documents(*, release: str, lineage: str) -> list[dict[str, Any]]:
    return [
        _stateful_set("loom-dev-postgres", release=release, lineage=lineage),
        _stateful_set("loom-dev-minio", release=release, lineage=lineage),
    ]


def _write_manifest(
    path: Path,
    *,
    release: str,
    lineage: str,
    documents: list[dict[str, Any]] | None = None,
) -> None:
    path.write_text(
        yaml.safe_dump_all(
            _manifest_documents(release=release, lineage=lineage)
            if documents is None
            else documents,
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _live_inventory(*, lineage: str = _RENDER_LINEAGE) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for stateful_set in _manifest_documents(release="live-release", lineage=lineage):
        live_stateful_set = copy.deepcopy(stateful_set)
        template = live_stateful_set["spec"]["volumeClaimTemplates"][0]
        template["apiVersion"] = "v1"
        template["kind"] = "PersistentVolumeClaim"
        template["spec"]["volumeMode"] = "Filesystem"
        template["status"] = {"phase": "Pending"}
        items.append(live_stateful_set)

        name = live_stateful_set["metadata"]["name"]
        claim_metadata = copy.deepcopy(template["metadata"])
        claim_metadata.update(
            {
                "name": f"data-{name}-0",
                "namespace": "loom-dev",
            }
        )
        claim_metadata["labels"]["app"] = name
        claim_metadata["annotations"].update(
            {
                "pv.kubernetes.io/bind-completed": "yes",
                "pv.kubernetes.io/bound-by-controller": "yes",
                "volume.beta.kubernetes.io/storage-provisioner": "driver.longhorn.io",
                "volume.kubernetes.io/storage-provisioner": "driver.longhorn.io",
            }
        )
        claim_spec = copy.deepcopy(template["spec"])
        claim_spec["volumeName"] = (
            "pvc-c918defb-b136-4109-8678-86dec259e2b9"
            if name == "loom-dev-minio"
            else "pvc-0af9ec8d-04fd-4f53-9032-dd949ee21d8f"
        )
        items.append(
            {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": claim_metadata,
                "spec": claim_spec,
                "status": {
                    "accessModes": copy.deepcopy(claim_spec["accessModes"]),
                    "capacity": copy.deepcopy(claim_spec["resources"]["requests"]),
                    "phase": "Bound",
                },
            }
        )
    items.append(
        {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": "loom-personal-dev-scanner-cache",
                "namespace": "loom-dev",
                "labels": {"app.kubernetes.io/managed-by": _MANAGED_BY},
            },
            "spec": {},
            "status": {"phase": "Bound"},
        }
    )
    return {"apiVersion": "v1", "kind": "List", "items": items}


def _write_live_inventory(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _run_guard(
    current: Path,
    live_inventory: Path,
    previous: Path | None,
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-m",
        "loom.personal_dev_storage_lineage_guard",
        "--current",
        str(current),
        "--live-inventory",
        str(live_inventory),
    ]
    if previous is not None:
        command.extend(("--previous", str(previous)))
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )


def _upgrade_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    current = tmp_path / "current.yaml"
    previous = tmp_path / "previous.yaml"
    live_inventory = tmp_path / "live.json"
    _write_manifest(current, release="current-release", lineage=_RENDER_LINEAGE)
    _write_manifest(previous, release="previous-release", lineage=_RENDER_LINEAGE)
    _write_live_inventory(live_inventory, _live_inventory())
    return current, previous, live_inventory


def test_guard_accepts_release_fresh_outer_stateful_metadata_and_exact_live_lineage(
    tmp_path: Path,
) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_guard_accepts_first_install_only_when_live_storage_is_absent(tmp_path: Path) -> None:
    current = tmp_path / "current.yaml"
    live_inventory = tmp_path / "live.json"
    _write_manifest(current, release="current-release", lineage=_RENDER_LINEAGE)
    _write_live_inventory(live_inventory, {"apiVersion": "v1", "kind": "List", "items": []})

    result = _run_guard(current, live_inventory, None)

    assert result.returncode == 0, result.stderr


def test_guard_rejects_first_install_over_existing_live_storage(tmp_path: Path) -> None:
    current = tmp_path / "current.yaml"
    live_inventory = tmp_path / "live.json"
    _write_manifest(current, release="current-release", lineage=_RENDER_LINEAGE)
    _write_live_inventory(live_inventory, _live_inventory())

    result = _run_guard(current, live_inventory, None)

    assert result.returncode == 1
    assert "first install requires absent live storage" in result.stderr


def test_guard_rejects_claim_template_lineage_drift(tmp_path: Path) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    _write_manifest(current, release="current-release", lineage="c" * 64)

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "claim templates differ from installed storage lineage" in result.stderr


def test_guard_rejects_forbidden_acceptance_metadata_in_equal_templates(
    tmp_path: Path,
) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    documents = _manifest_documents(release="release", lineage=_RENDER_LINEAGE)
    for document in documents:
        metadata = document["spec"]["volumeClaimTemplates"][0]["metadata"]
        metadata["labels"]["loom.dev/acceptance-plan-sha256"] = "d" * 32
        metadata["annotations"]["loom.dev/acceptance-plan-sha256"] = "d" * 64
    _write_manifest(
        current,
        release="current-release",
        lineage=_RENDER_LINEAGE,
        documents=documents,
    )
    _write_manifest(
        previous,
        release="previous-release",
        lineage=_RENDER_LINEAGE,
        documents=documents,
    )

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert "claim template metadata is invalid" in result.stderr


@pytest.mark.parametrize("target", ["stateful", "claim"])
def test_guard_rejects_live_storage_drift(tmp_path: Path, target: str) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    live = _live_inventory()
    if target == "stateful":
        item = next(item for item in live["items"] if item["kind"] == "StatefulSet")
        item["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"][
            "storage"
        ] = "101Gi"
    else:
        item = next(
            item
            for item in live["items"]
            if item["kind"] == "PersistentVolumeClaim"
            and item["metadata"]["name"].startswith("data-")
        )
        item["metadata"]["annotations"]["loom.dev/render-input-sha256"] = "c" * 64
    _write_live_inventory(live_inventory, live)

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert f"live {target}" in result.stderr


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "api", "namespace"])
def test_guard_rejects_invalid_stateful_inventory(tmp_path: Path, mutation: str) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    documents = _manifest_documents(release="release", lineage=_RENDER_LINEAGE)
    if mutation == "missing":
        documents.pop()
    elif mutation == "duplicate":
        documents.append(copy.deepcopy(documents[0]))
    elif mutation == "api":
        documents[0]["apiVersion"] = "apps/v1beta1"
    else:
        documents[0]["metadata"]["namespace"] = "other"
    _write_manifest(
        current,
        release="current-release",
        lineage=_RENDER_LINEAGE,
        documents=documents,
    )

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert "StatefulSet storage" in result.stderr


@pytest.mark.parametrize("payload_kind", ["malformed", "oversized"])
def test_guard_rejects_malformed_or_oversized_manifest(
    tmp_path: Path,
    payload_kind: str,
) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    payload = b"[" if payload_kind == "malformed" else b"x" * (_MAX_MANIFEST_BYTES + 1)
    current.write_bytes(payload)

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert "manifest" in result.stderr


def test_guard_rejects_invalid_live_inventory(tmp_path: Path) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    live_inventory.write_text('{"apiVersion":"v1","kind":"List"}', encoding="utf-8")

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert "live storage inventory is invalid" in result.stderr


def test_guard_rejects_malformed_claim_metadata_without_traceback(tmp_path: Path) -> None:
    current, previous, live_inventory = _upgrade_inputs(tmp_path)
    documents = _manifest_documents(release="release", lineage=_RENDER_LINEAGE)
    documents[0]["spec"]["volumeClaimTemplates"][0]["metadata"] = "invalid"
    _write_manifest(
        current,
        release="current-release",
        lineage=_RENDER_LINEAGE,
        documents=documents,
    )

    result = _run_guard(current, live_inventory, previous)

    assert result.returncode == 1
    assert "StatefulSet storage contract is invalid" in result.stderr
    assert "Traceback" not in result.stderr
