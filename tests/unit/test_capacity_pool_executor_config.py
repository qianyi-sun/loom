"""Stable, exact loading of controller-local Slurm inventory policy."""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import NodeEnvelopeV1, ResourceVectorV1
from loom_capacity_pool_executor.slurm_inventory import SlurmInventoryPolicy


def _config_module():  # type: ignore[no-untyped-def]
    return importlib.import_module("loom_capacity_pool_executor.config")


def inventory_policy_payload(
    *,
    pool_id: str = "gb10",
    pool_generation: int = 3,
    query_uid: int = 1001,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "pool_id": pool_id,
        "pool_generation": pool_generation,
        "reporter_incarnation": "10000000-0000-4000-8000-000000000001",
        "nodes": [
            {
                "pool_id": pool_id,
                "node_id": f"{pool_id}-node-a",
                "allocatable": {
                    "schema_version": 1,
                    "slots": 4,
                    "cpu_millicores": 8_000,
                    "memory_bytes": 68_719_476_736,
                    "gpu_count": 1,
                    "generic": {},
                },
                "features": ["arm64" if pool_id == "gb10" else "x86_64"],
            }
        ],
        "relevant_partitions": [f"{pool_id}-workers"],
        "slot_resources": {
            "schema_version": 1,
            "slots": 1,
            "cpu_millicores": 2_000,
            "memory_bytes": 17_179_869_184,
            "gpu_count": 0,
            "generic": {},
        },
        "controller_cluster": pool_id,
        "slurm_version": [23, 11, 4],
        "data_parser": "data_parser/v0.0.40",
        "query_principal": "loom-capacity-slurm-reader",
        "query_uid": query_uid,
        "job_visibility_evidence_sha256": "a" * 64,
        "scontrol_sha256": "b" * 64,
        "squeue_sha256": "c" * 64,
        "slurm_conf_sha256": "d" * 64,
    }


def _write_policy(path: Path, payload: dict[str, object]) -> tuple[Path, str, bytes]:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    path.write_bytes(encoded)
    path.chmod(0o600)
    return path, hashlib.sha256(encoded).hexdigest(), encoded


def test_exact_inventory_policy_round_trips_from_a_digest_pinned_file(
    tmp_path: Path,
) -> None:
    module = _config_module()
    path, digest, _encoded = _write_policy(
        tmp_path / "inventory-policy.json",
        inventory_policy_payload(),
    )

    loaded = module.load_slurm_inventory_policy(path, expected_sha256=digest)

    assert loaded == SlurmInventoryPolicy(
        pool_id="gb10",
        pool_generation=3,
        reporter_incarnation=UUID("10000000-0000-4000-8000-000000000001"),
        nodes=(
            NodeEnvelopeV1(
                node_id="gb10-node-a",
                allocatable=ResourceVectorV1(
                    slots=4,
                    cpu_millicores=8_000,
                    memory_bytes=68_719_476_736,
                    gpu_count=1,
                ),
                features=("arm64",),
            ),
        ),
        relevant_partitions=("gb10-workers",),
        slot_resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=2_000,
            memory_bytes=17_179_869_184,
        ),
        controller_cluster="gb10",
        slurm_version=(23, 11, 4),
        data_parser="data_parser/v0.0.40",
        query_principal="loom-capacity-slurm-reader",
        query_uid=1001,
        job_visibility_evidence_sha256="a" * 64,
        scontrol_sha256="b" * 64,
        squeue_sha256="c" * 64,
        slurm_conf_sha256="d" * 64,
    )


@pytest.mark.parametrize(
    "digest",
    ("", "a" * 63, "A" * 64, "g" * 64, "0" * 64),
)
def test_inventory_policy_loader_rejects_a_noncanonical_expected_digest(
    tmp_path: Path,
    digest: str,
) -> None:
    module = _config_module()
    path, _valid_digest, _encoded = _write_policy(
        tmp_path / "inventory-policy.json",
        inventory_policy_payload(),
    )

    with pytest.raises(module.SlurmInventoryPolicyError) as caught:
        module.load_slurm_inventory_policy(path, expected_sha256=digest)

    assert str(caught.value) == "Slurm inventory policy is invalid"


def test_inventory_policy_loader_rejects_missing_symlink_fifo_and_writable_files(
    tmp_path: Path,
) -> None:
    module = _config_module()
    target, digest, _encoded = _write_policy(
        tmp_path / "target.json",
        inventory_policy_payload(),
    )
    link = tmp_path / "link.json"
    link.symlink_to(target)
    hardlink = tmp_path / "hardlink.json"
    hardlink.hardlink_to(target)
    fifo = tmp_path / "policy.fifo"
    os.mkfifo(fifo, mode=0o600)

    for path in (tmp_path / "missing.json", link, hardlink, fifo):
        with pytest.raises(module.SlurmInventoryPolicyError) as caught:
            module.load_slurm_inventory_policy(path, expected_sha256=digest)
        assert str(path) not in str(caught.value)

    hardlink.unlink()
    target.chmod(0o620)
    with pytest.raises(module.SlurmInventoryPolicyError):
        module.load_slurm_inventory_policy(target, expected_sha256=digest)


def test_inventory_policy_loader_rejects_wrong_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _config_module()
    path, digest, _encoded = _write_policy(
        tmp_path / "inventory-policy.json",
        inventory_policy_payload(),
    )
    real_fstat = os.fstat

    def wrong_owner(descriptor: int):  # type: ignore[no-untyped-def]
        result = real_fstat(descriptor)
        values = list(result)
        values[4] = result.st_uid + 1
        return os.stat_result(values)

    monkeypatch.setattr(module.os, "fstat", wrong_owner)

    with pytest.raises(module.SlurmInventoryPolicyError):
        module.load_slurm_inventory_policy(path, expected_sha256=digest)


def test_inventory_policy_loader_rejects_metadata_changed_while_reading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _config_module()
    path, digest, _encoded = _write_policy(
        tmp_path / "inventory-policy.json",
        inventory_policy_payload(),
    )
    real_fstat = os.fstat
    calls = 0

    def changed_metadata(descriptor: int):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        result = real_fstat(descriptor)
        if calls == 1:
            return result
        return SimpleNamespace(
            **{
                field: getattr(result, field) + (1 if field == "st_mtime_ns" else 0)
                for field in (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_uid",
                    "st_gid",
                    "st_nlink",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
            }
        )

    monkeypatch.setattr(module.os, "fstat", changed_metadata)

    with pytest.raises(module.SlurmInventoryPolicyError):
        module.load_slurm_inventory_policy(path, expected_sha256=digest)


def test_inventory_policy_loader_rejects_oversize_malformed_unknown_and_mismatched_bytes(
    tmp_path: Path,
) -> None:
    module = _config_module()
    path = tmp_path / "do-not-echo-policy.json"
    secret = "do-not-echo-this-policy-value"
    cases = (
        b"x" * (module.MAX_SLURM_INVENTORY_POLICY_BYTES + 1),
        b"{malformed-json}",
        json.dumps(inventory_policy_payload() | {"unexpected": secret}).encode("utf-8"),
    )
    for encoded in cases:
        path.write_bytes(encoded)
        path.chmod(0o600)
        with pytest.raises(module.SlurmInventoryPolicyError) as caught:
            module.load_slurm_inventory_policy(
                path,
                expected_sha256=hashlib.sha256(encoded).hexdigest(),
            )
        assert str(caught.value) == "Slurm inventory policy is invalid"
        assert str(path) not in str(caught.value)
        assert secret not in str(caught.value)

    path, _digest, _encoded = _write_policy(path, inventory_policy_payload())
    with pytest.raises(module.SlurmInventoryPolicyError):
        module.load_slurm_inventory_policy(path, expected_sha256="f" * 64)


def _set_node_pool(payload: dict[str, object], value: str) -> None:
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    node = nodes[0]
    assert isinstance(node, dict)
    node["pool_id"] = value


def _set_node_resource(payload: dict[str, object], field: str, value: object) -> None:
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    node = nodes[0]
    assert isinstance(node, dict)
    resources = node["allocatable"]
    assert isinstance(resources, dict)
    resources[field] = value


def _duplicate_node(payload: dict[str, object], *, uppercase: bool = False) -> None:
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    duplicate = copy.deepcopy(nodes[0])
    assert isinstance(duplicate, dict)
    if uppercase:
        duplicate["node_id"] = str(duplicate["node_id"]).upper()
    nodes.append(duplicate)


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value.update(pool_id="another-pool"),
        lambda value: value.update(pool_generation=0),
        lambda value: value.update(reporter_incarnation=str(UUID(int=0))),
        lambda value: value.update(nodes=[]),
        lambda value: _duplicate_node(value),
        lambda value: _duplicate_node(value, uppercase=True),
        lambda value: _set_node_pool(value, "oldlab"),
        lambda value: _set_node_resource(value, "slots", 0),
        lambda value: _set_node_resource(value, "generic", {"licenses": 1}),
        lambda value: value["slot_resources"].update(slots=0),  # type: ignore[union-attr]
        lambda value: value["slot_resources"].update(generic={"licenses": 1}),  # type: ignore[union-attr]
        lambda value: value.update(slurm_version=[24, 5, 0]),
        lambda value: value.update(data_parser="json/v1"),
        lambda value: value.update(query_uid=0),
        lambda value: value.update(query_uid=True),
        lambda value: value.update(job_visibility_evidence_sha256="0" * 64),
        lambda value: value.update(relevant_partitions=["gb10-workers", "gb10-workers"]),
        lambda value: value.update(scontrol_path="/tmp/scontrol"),
        lambda value: value.update(command=["squeue", "--json"]),
    ),
)
def test_inventory_policy_rejects_unsafe_or_ambiguous_controller_facts(
    tmp_path: Path,
    mutate,  # type: ignore[no-untyped-def]
) -> None:
    module = _config_module()
    payload = inventory_policy_payload()
    mutate(payload)
    path, digest, _encoded = _write_policy(tmp_path / "inventory-policy.json", payload)

    with pytest.raises(module.SlurmInventoryPolicyError):
        module.load_slurm_inventory_policy(path, expected_sha256=digest)
