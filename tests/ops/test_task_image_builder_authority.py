from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_authority as authority

ROOT = Path(__file__).resolve().parents[2]


def _candidate_copy(tmp_path: Path) -> Path:
    manifest = json.loads(authority.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    candidate = tmp_path / "candidate"
    destination_manifest = candidate / authority.MANIFEST_RELATIVE_PATH
    destination_manifest.parent.mkdir(parents=True)
    shutil.copyfile(authority.DEFAULT_MANIFEST, destination_manifest)
    for component in manifest["components"]:
        source = ROOT / component["path"]
        destination = candidate / component["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    return candidate


def test_authority_binding_changes_when_omitted_security_producers_change(
    tmp_path: Path,
) -> None:
    candidate = _candidate_copy(tmp_path)
    original = authority.load_authority_binding(candidate)

    host_release = candidate / "scripts/ops/task_image_builder_host_release.py"
    host_release.write_bytes(host_release.read_bytes() + b"\n# reviewed change\n")
    changed_release = authority.load_authority_binding(candidate)
    assert changed_release.digest != original.digest

    candidate = _candidate_copy(tmp_path / "second")
    original = authority.load_authority_binding(candidate)
    maintenance = candidate / "scripts/ops/task_image_builder_node_maintenance.py"
    maintenance.write_bytes(maintenance.read_bytes() + b"\n# reviewed change\n")
    changed_maintenance = authority.load_authority_binding(candidate)
    assert changed_maintenance.digest != original.digest


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unsafe"])
def test_authority_manifest_rejects_incomplete_duplicate_or_unsafe_components(
    tmp_path: Path,
    mutation: str,
) -> None:
    candidate = _candidate_copy(tmp_path)
    manifest_path = candidate / authority.MANIFEST_RELATIVE_PATH
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if mutation == "missing":
        manifest["components"].pop()
    elif mutation == "duplicate":
        manifest["components"].append(dict(manifest["components"][0]))
    else:
        manifest["components"][0]["path"] = "../outside.py"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(authority.AuthorityError):
        authority.load_authority_binding(candidate)


@pytest.mark.parametrize("mutation", ["missing", "extra", "changed"])
def test_receipt_authority_component_map_must_be_exact(
    tmp_path: Path,
    mutation: str,
) -> None:
    binding = authority.load_authority_binding(ROOT)
    receipt = binding.as_dict()
    components = dict(receipt["authority_component_digests"])
    if mutation == "missing":
        components.pop(next(iter(components)))
    elif mutation == "extra":
        components["unexpected"] = "0" * 64
    else:
        first = next(iter(components))
        components[first] = "0" * 64
    receipt["authority_component_digests"] = components

    with pytest.raises(authority.AuthorityError):
        authority.validate_authority_binding(receipt, binding)
