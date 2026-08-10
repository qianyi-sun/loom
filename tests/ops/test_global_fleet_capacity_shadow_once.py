"""Offline Package 1 shadow-evidence driver safety tests."""

from __future__ import annotations

import json
import stat
from pathlib import Path

from scripts.ops.global_fleet_capacity_shadow_once import run_shadow_once

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    PoolObservationV1,
    canonical_digest,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "capacity"


def _run(output: Path) -> int:
    return run_shadow_once(
        fleet=FIXTURES / "fleet-v1.toml",
        subjects=FIXTURES / "subjects-v1.toml",
        snapshot=FIXTURES / "snapshot-v1.json",
        output=output,
    )


def test_shadow_once_emits_diagnostic_not_grant(tmp_path: Path) -> None:
    output = tmp_path / "shadow.json"

    assert _run(output) == 0

    document = json.loads(output.read_text(encoding="utf-8"))
    assert document["mode"] == "shadow"
    assert document["executable"] is False
    assert document["executable_new_capacity_ceiling"] == 0
    assert document["configuration_epoch"] == 1
    assert document["input_digest"] == document["shadow_epoch"]["input_digest"]
    assert "grants" not in document
    assert "launch_permits" not in document


def test_output_is_owner_only_atomic_and_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    assert _run(first) == 0
    assert _run(second) == 0

    assert stat.S_IMODE(first.stat().st_mode) == 0o600
    assert first.read_bytes() == second.read_bytes()
    assert not list(tmp_path.glob(f".{first.name}.*"))
    assert not list(tmp_path.glob(f".{second.name}.*"))


def test_output_path_must_be_absolute_and_cannot_be_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("do-not-replace\n", encoding="utf-8")
    link = tmp_path / "shadow.json"
    link.symlink_to(target)

    assert _run(Path("relative-shadow.json")) == 2
    assert _run(link) == 2
    assert target.read_text(encoding="utf-8") == "do-not-replace\n"


def test_unknown_snapshot_field_fails_without_publishing(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot.json"
    document = json.loads((FIXTURES / "snapshot-v1.json").read_text(encoding="utf-8"))
    document["database_url"] = "postgresql://forbidden"
    snapshot.write_text(json.dumps(document), encoding="utf-8")
    output = tmp_path / "shadow.json"

    result = run_shadow_once(
        fleet=FIXTURES / "fleet-v1.toml",
        subjects=FIXTURES / "subjects-v1.toml",
        snapshot=snapshot,
        output=output,
    )

    assert result == 2
    assert not output.exists()


def test_snapshot_symlink_and_oversized_input_fail_without_publishing(tmp_path: Path) -> None:
    output = tmp_path / "shadow.json"
    linked = tmp_path / "linked-snapshot.json"
    linked.symlink_to((FIXTURES / "snapshot-v1.json").resolve())

    assert (
        run_shadow_once(
            fleet=FIXTURES / "fleet-v1.toml",
            subjects=FIXTURES / "subjects-v1.toml",
            snapshot=linked,
            output=output,
        )
        == 2
    )
    oversized = tmp_path / "oversized-snapshot.json"
    oversized.write_bytes(b" " * (MAX_CONTRACT_BYTES + 1))
    assert (
        run_shadow_once(
            fleet=FIXTURES / "fleet-v1.toml",
            subjects=FIXTURES / "subjects-v1.toml",
            snapshot=oversized,
            output=output,
        )
        == 2
    )
    assert not output.exists()


def test_reported_commitment_cannot_be_omitted_from_retained_capacity(tmp_path: Path) -> None:
    document = json.loads((FIXTURES / "snapshot-v1.json").read_text(encoding="utf-8"))
    pool = document["pools"][0]
    pool["last_observation"]["commitments"] = [
        {
            "schema_version": 1,
            "kind": "physical",
            "commitment_id": "reported-worker",
            "physical_identity": "reported-worker",
            "attempt_id": None,
            "concurrency_slots": None,
            "subject_id": "00000000-0000-4000-8000-000000000005",
            "subject_incarnation": "00000000-0000-4000-8000-000000000006",
            "deployment_generation": 1,
            "pool_id": "gb10",
            "pool_generation": 1,
            "profile_id": "gb10-profile",
            "profile_generation": 1,
            "profile_digest": "e487af4177c617decec46586a276fb50798edeb90556a7a38b78c5e21505268d",
            "shape_id": "gb10-one-slot",
            "resources": {
                "schema_version": 1,
                "slots": 1,
                "cpu_millicores": 1000,
                "memory_bytes": 1073741824,
                "gpu_count": 0,
                "generic": {},
            },
            "state": "observed",
            "node_ids": ["synthetic-gb10-node"],
        }
    ]
    observation = PoolObservationV1.model_validate_json(json.dumps(pool["last_observation"]))
    pool["freshness"]["last_payload_digest"] = canonical_digest(observation)
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(document), encoding="utf-8")
    output = tmp_path / "shadow.json"

    result = run_shadow_once(
        fleet=FIXTURES / "fleet-v1.toml",
        subjects=FIXTURES / "subjects-v1.toml",
        snapshot=snapshot,
        output=output,
    )

    assert result == 2
    assert not output.exists()


def test_zero_search_bound_fails_closed_without_publishing(tmp_path: Path) -> None:
    output = tmp_path / "shadow.json"

    result = run_shadow_once(
        fleet=FIXTURES / "fleet-v1.toml",
        subjects=FIXTURES / "subjects-v1.toml",
        snapshot=FIXTURES / "snapshot-v1.json",
        output=output,
        max_allocation_decisions=0,
    )

    assert result == 2
    assert not output.exists()
