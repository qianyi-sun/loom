from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.operator import protected_secret_inventory as inventory_module
from loom_cli.rollout.operator.protected_secret_inventory import (
    PROTECTED_SECRET_SPECS,
    SecretInventoryError,
    build_secret_inventory,
    inspect_secret_inventory,
)


def _secret(
    namespace: str,
    name: str,
    *,
    uid: str = "11111111-1111-4111-8111-111111111111",
    resource_version: str = "7",
) -> bytes:
    return json.dumps(
        {
            "apiVersion": "v1",
            "data": {"token": "c2Vuc2l0aXZl"},
            "kind": "Secret",
            "metadata": {
                "creationTimestamp": "2026-09-03T12:00:00Z",
                "name": name,
                "namespace": namespace,
                "resourceVersion": resource_version,
                "uid": uid,
            },
            "type": "Opaque",
        },
        separators=(",", ":"),
    ).encode()


def _observations(
    *, runtime: bool, agent: bool
) -> dict[tuple[str, str], tuple[bytes | None, bytes | None]]:
    result: dict[tuple[str, str], tuple[bytes | None, bytes | None]] = {}
    for spec in PROTECTED_SECRET_SPECS:
        present = (
            spec.required
            or (spec.name == "loom-protected-worker-runtime" and runtime)
            or (spec.name == "loom-capacity-agent" and agent)
        )
        payload = _secret(spec.namespace, spec.name) if present else None
        result[(spec.namespace, spec.name)] = (payload, payload)
    return result


def _persist_inventory(root: Path, *, runtime: bool, agent: bool):
    inventory = build_secret_inventory(_observations(runtime=runtime, agent=agent))
    root.mkdir(mode=0o700)
    for filename, payload in inventory.exported_objects.items():
        path = root / filename
        path.write_bytes(payload)
        path.chmod(0o600)
    inventory_path = root / "protected-capacity-secret-inventory.json"
    inventory_path.write_bytes(inventory.inventory_payload)
    inventory_path.chmod(0o600)
    for name in ("loom-admin-secret", "loom-secrets", "loom-staging-tls"):
        path = root / f"{name}.yaml"
        path.write_text(
            json.dumps(
                {
                    "apiVersion": "v1",
                    "data": {"token": "c2Vuc2l0aXZl"},
                    "kind": "Secret",
                    "metadata": {"name": name, "namespace": "loom-staging"},
                    "type": "Opaque",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        path.chmod(0o600)
    return inventory, inventory_path


@pytest.mark.parametrize(
    "runtime,agent", [(False, False), (True, False), (False, True), (True, True)]
)
def test_inventory_records_required_and_optional_secret_presence(
    runtime: bool,
    agent: bool,
) -> None:
    inventory = build_secret_inventory(_observations(runtime=runtime, agent=agent))
    record = json.loads(inventory.inventory_payload)

    assert [(item["namespace"], item["name"], item["present"]) for item in record["secrets"]] == [
        ("loom-dev", "loom-capacity-manager", True),
        ("loom-staging", "loom-capacity-agent", agent),
        ("loom-staging", "loom-protected-worker-runtime", runtime),
        ("loom-dev", "loom-capacity-execution-operator", False),
        ("loom-dev", "loom-capacity-executor-gb10", False),
        ("loom-dev", "loom-capacity-executor-oldlab", False),
    ]
    assert set(inventory.exported_objects) == {
        item["filename"] for item in record["secrets"] if item["present"]
    }
    assert all(item["sha256"] is None for item in record["secrets"] if not item["present"])


def test_inventory_rejects_absent_required_or_changing_identity() -> None:
    observations = _observations(runtime=False, agent=False)
    required = ("loom-dev", "loom-capacity-manager")
    observations[required] = (None, None)
    with pytest.raises(SecretInventoryError, match="required protected Secret is absent"):
        build_secret_inventory(observations)

    observations = _observations(runtime=False, agent=False)
    observations[required] = (
        _secret(*required),
        _secret(*required, uid="22222222-2222-4222-8222-222222222222"),
    )
    with pytest.raises(SecretInventoryError, match="changed during acquisition"):
        build_secret_inventory(observations)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"apiVersion":"v1","kind":"Secret","metadata":',
        b'{"apiVersion":"v1","apiVersion":"v1","kind":"Secret","metadata":{}}',
        b"x" * (1024 * 1024 + 1),
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "loom-capacity-manager",
                    "namespace": "loom-dev",
                    "uid": "11111111-1111-4111-8111-111111111111",
                    "resourceVersion": "7",
                    "ownerReferences": [{"name": "unsafe"}],
                },
                "data": {"token": "c2Vuc2l0aXZl"},
                "type": "Opaque",
            }
        ).encode(),
        json.dumps(
            {
                "apiVersion": "v1",
                "data": {"token": ""},
                "kind": "Secret",
                "metadata": {
                    "name": "loom-capacity-manager",
                    "namespace": "loom-dev",
                    "resourceVersion": "7",
                    "uid": "11111111-1111-4111-8111-111111111111",
                },
                "type": "Opaque",
            }
        ).encode(),
    ],
)
def test_inventory_rejects_malformed_oversized_or_unsafe_secret(payload: bytes) -> None:
    observations = _observations(runtime=False, agent=False)
    observations[("loom-dev", "loom-capacity-manager")] = (payload, payload)

    with pytest.raises(SecretInventoryError):
        build_secret_inventory(observations)


def test_inventory_rejects_mismatched_identity_without_leaking_payload() -> None:
    marker = "bearer-token-private-key-password-database-url"
    payload = json.dumps(
        {
            "apiVersion": "v1",
            "data": {"token": marker},
            "kind": "Secret",
            "metadata": {
                "name": "other",
                "namespace": "loom-dev",
                "resourceVersion": "7",
                "uid": "11111111-1111-4111-8111-111111111111",
            },
            "type": "Opaque",
        }
    ).encode()
    observations = _observations(runtime=False, agent=False)
    observations[("loom-dev", "loom-capacity-manager")] = (payload, payload)

    with pytest.raises(SecretInventoryError) as caught:
        build_secret_inventory(observations)

    assert marker not in str(caught.value)
    assert marker not in repr(caught.value)


def test_inspection_rejects_undeclared_duplicate_and_digest_mismatched_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "secrets"
    inventory, inventory_path = _persist_inventory(root, runtime=True, agent=False)

    inspected = inspect_secret_inventory(root, expected_owner_uid=os.geteuid())
    assert inspected.inventory_sha256 == inventory.inventory_sha256

    staging = root / "loom-secrets.yaml"
    staging_payload = staging.read_bytes()
    staging.write_bytes(b'{"kind":"Secret"}\n')
    with pytest.raises(SecretInventoryError, match="persisted Secret"):
        inspect_secret_inventory(root, expected_owner_uid=os.geteuid())
    staging.write_bytes(staging_payload)

    undeclared = root / "undeclared.json"
    undeclared.write_bytes(b"{}")
    undeclared.chmod(0o600)
    with pytest.raises(SecretInventoryError, match="file set is invalid"):
        inspect_secret_inventory(root, expected_owner_uid=os.geteuid())
    undeclared.unlink()

    present = next(iter(inventory.exported_objects))
    changed = json.loads(inventory.exported_objects[present])
    changed["data"]["token"] = "Y2hhbmdlZA=="
    (root / present).write_text(
        json.dumps(changed, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(SecretInventoryError, match="digest does not match"):
        inspect_secret_inventory(root, expected_owner_uid=os.geteuid())
    (root / present).write_bytes(inventory.exported_objects[present])

    record = json.loads(inventory.inventory_payload)
    record["secrets"].append(dict(record["secrets"][0]))
    inventory_path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(SecretInventoryError, match="duplicate identity"):
        inspect_secret_inventory(root, expected_owner_uid=os.geteuid())


def test_inspection_rejects_a_short_read_that_hides_trailing_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "secrets"
    inventory, inventory_path = _persist_inventory(root, runtime=False, agent=False)
    inventory_path.write_bytes(inventory.inventory_payload + b"trailing-data")
    target_inode = inventory_path.stat().st_ino
    original_read = os.read

    def short_inventory_read(fd: int, count: int) -> bytes:
        if os.fstat(fd).st_ino == target_inode and os.lseek(fd, 0, os.SEEK_CUR) == 0:
            return original_read(fd, len(inventory.inventory_payload))
        return original_read(fd, count)

    monkeypatch.setattr(inventory_module.os, "read", short_inventory_read)

    with pytest.raises(SecretInventoryError):
        inspect_secret_inventory(root, expected_owner_uid=os.geteuid())
