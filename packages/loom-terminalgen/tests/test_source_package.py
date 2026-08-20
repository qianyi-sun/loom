from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path

import pytest

from terminalgen.agent_skills import load_agent_skill_catalog
from terminalgen.atomic import (
    build_atomic_generation_requests,
    load_atomic_weakness_cards,
)
from terminalgen.catalog import get_domain_specs
from terminalgen.models import Difficulty, TaskFile

PACKAGE_ROOT = Path(__file__).parents[1]
SOURCE_ROOT = PACKAGE_ROOT / "src" / "terminalgen"
REPOSITORY_ROOT = PACKAGE_ROOT.parents[1]


def test_imported_source_matches_manifest() -> None:
    manifest = json.loads((PACKAGE_ROOT / "SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    rows = manifest["files"]

    assert [row["path"] for row in rows] == sorted(row["path"] for row in rows)
    assert sorted(path.name for path in SOURCE_ROOT.iterdir() if path.is_file()) == [
        row["path"] for row in rows
    ]
    for row in rows:
        payload = (SOURCE_ROOT / row["path"]).read_bytes()
        assert len(payload) == row["bytes"]
        assert hashlib.sha256(payload).hexdigest() == row["sha256"]

    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(canonical).hexdigest() == manifest["source_snapshot_sha256"]


def test_sbom_matches_workspace_lock() -> None:
    sbom = json.loads((PACKAGE_ROOT / "sbom.cdx.json").read_text(encoding="utf-8"))
    lock = tomllib.loads((REPOSITORY_ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked_versions = {package["name"]: package["version"] for package in lock["package"]}

    assert sbom["bomFormat"] == "CycloneDX"
    assert sbom["specVersion"] == "1.5"
    for component in sbom["components"]:
        assert locked_versions[component["name"]] == component["version"]


def test_atomic_catalog_constructs_exact_9000_unique_slots() -> None:
    cards = load_atomic_weakness_cards()
    requests = build_atomic_generation_requests(
        cards,
        per_card_count=500,
        difficulty=Difficulty.MIXED,
        random_seed=20260820,
    )

    assert len(cards) == 18
    assert len(requests) == 9_000
    assert len({request.template_family_id for request in requests}) == 9_000
    assert [request.sample_index for request in requests] == list(range(9_000))


def test_aug19_acceptance_evidence_has_20_unique_matching_task_trees() -> None:
    evidence = json.loads(
        (PACKAGE_ROOT / "acceptance" / "AUG19_ACCEPTANCE.json").read_text(encoding="utf-8")
    )
    tree_hashes = [
        tree_hash
        for source in evidence["sources"]
        for tree_hash in source["tasks"].values()
    ]

    assert evidence["acceptance"] == "pass"
    assert evidence["summary"] == {
        "source_tasks": 4,
        "generated_tasks": 20,
        "matching_tasks": 20,
        "mismatching_tasks": 0,
    }
    assert len(tree_hashes) == 20
    assert len(set(tree_hashes)) == 20
    assert all(len(tree_hash) == 64 for tree_hash in tree_hashes)


def test_realistic_catalog_is_packaged_and_nonempty() -> None:
    domains = get_domain_specs()

    assert len(domains) >= 18
    assert all(domain.skills for domain in domains)


def test_bulk_agent_skill_data_is_external_and_missing_input_fails_closed() -> None:
    assert not (SOURCE_ROOT / "agent_skill_plans.jsonl").exists()

    with pytest.raises(ValueError, match="agent skill plans file not found"):
        load_agent_skill_catalog()


@pytest.mark.parametrize(
    "unsafe_path",
    ["../secret", "nested/../../secret"],
)
def test_task_file_rejects_workspace_escape(unsafe_path: str) -> None:
    with pytest.raises(ValueError, match="workspace-relative"):
        TaskFile(name=unsafe_path, context="")
