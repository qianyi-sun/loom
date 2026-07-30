from __future__ import annotations

import json
import os
import socket
import sqlite3
import threading
import types
from collections.abc import Callable, Set
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_registry as registry


def _missing_identity(_value: object) -> None:
    raise KeyError


def _register(
    principal: str,
    key: str,
    *,
    display_name: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": registry.SCHEMA_VERSION,
        "kind": registry.REGISTER_KIND,
        "principal_id": principal,
        "idempotency_key": key,
        "display_name": display_name or principal,
    }


def _candidate(
    environment: registry.EnvironmentRecord,
    key: str,
    *,
    principal: str | None = None,
    sha: str = "a" * 40,
) -> dict[str, Any]:
    return {
        "schema_version": registry.SCHEMA_VERSION,
        "kind": registry.CANDIDATE_KIND,
        "principal_id": principal or environment.principal_id,
        "idempotency_key": key,
        "env_id": environment.env_id,
        "candidate_sha": sha,
        "candidate_tree": "b" * 40,
        "bundle_sha256": "c" * 64,
        "bundle_size": 1024,
        "image_digests": {
            "amd64": "sha256:" + "d" * 64,
            "arm64": "sha256:" + "e" * 64,
        },
    }


def _deploy(
    environment: registry.EnvironmentRecord,
    candidate: registry.CandidateRecord,
    key: str,
    *,
    principal: str | None = None,
    generation: int | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": registry.SCHEMA_VERSION,
        "kind": registry.DEPLOY_KIND,
        "principal_id": principal or environment.principal_id,
        "idempotency_key": key,
        "env_id": environment.env_id,
        "candidate_id": candidate.candidate_id,
        "expected_resource_generation": (
            environment.resource_generation if generation is None else generation
        ),
    }


def _finalization_evidence() -> dict[str, str]:
    return {
        "capacity_finalize_receipt_sha256": "1" * 64,
        "capacity_finalize_check_receipt_sha256": "2" * 64,
        "runtime_reconcile_receipt_sha256": "3" * 64,
        "runtime_prepare_check_receipt_sha256": "4" * 64,
        "acceptance_probe_receipt_sha256": "5" * 64,
    }


def _new_registry(
    tmp_path: Path,
    *,
    policy: registry.AllocationPolicy | None = None,
    port_inventory_collector: Callable[[], Set[int]] | None = None,
) -> registry.DeveloperEnvironmentRegistry:
    return registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry.sqlite3",
        policy=policy,
        port_inventory_collector=port_inventory_collector,
    )


def _legacy_deployment_table(path: Path, *, committed: bool) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE deployments (
                deployment_id TEXT PRIMARY KEY,
                principal_id TEXT NOT NULL,
                env_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                expected_resource_generation INTEGER NOT NULL,
                phase TEXT NOT NULL,
                previous_candidate_id TEXT,
                request_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        if committed:
            connection.execute(
                "INSERT INTO deployments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "dep-" + "1" * 32,
                    "oidc:example:legacy",
                    "denv-" + "1" * 32,
                    "cand-" + "1" * 40,
                    1,
                    "committed",
                    None,
                    "1" * 64,
                    "2026-07-29T12:00:00Z",
                    "2026-07-29T12:01:00Z",
                ),
            )
        connection.commit()
    finally:
        connection.close()
    path.chmod(0o600)


def test_legacy_empty_deployment_table_adds_applied_binding_columns(
    tmp_path: Path,
) -> None:
    database = tmp_path / "registry.sqlite3"
    _legacy_deployment_table(database, committed=False)

    registry.DeveloperEnvironmentRegistry(database)

    connection = sqlite3.connect(database)
    try:
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(deployments)")}
    finally:
        connection.close()
    assert {
        "applied_resource_generation",
        "applied_registry_generation",
        "applied_registry_payload_sha256",
    }.issubset(columns)


def test_legacy_committed_deployment_fails_closed_without_partial_backfill(
    tmp_path: Path,
) -> None:
    database = tmp_path / "registry.sqlite3"
    _legacy_deployment_table(database, committed=True)

    with pytest.raises(
        registry.RegistryError,
        match="requires explicit migration",
    ):
        registry.DeveloperEnvironmentRegistry(database)

    connection = sqlite3.connect(database)
    try:
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(deployments)")}
        finalizations = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'deployment_finalizations'
            """
        ).fetchone()
        row = connection.execute("SELECT deployment_id, phase FROM deployments").fetchone()
    finally:
        connection.close()
    assert not {
        "applied_resource_generation",
        "applied_registry_generation",
        "applied_registry_payload_sha256",
    }.intersection(columns)
    assert finalizations is None
    assert row == ("dep-" + "1" * 32, "committed")


def _resign_snapshot(payload: dict[str, Any]) -> bytes:
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    payload["payload_sha256"] = registry._digest(unsigned)
    return registry._canonical(payload)


def _fleet_identity_inventory(
    path: Path,
    policy: registry.AllocationPolicy,
    *,
    occupied_by_node: dict[str, list[int]] | None = None,
    collected_at: datetime | None = None,
) -> bytes:
    observed = collected_at or datetime.now(UTC)
    observed_at = observed.isoformat().replace("+00:00", "Z")
    nodes = [
        {
            "schema_version": registry.SCHEMA_VERSION,
            "kind": registry.NODE_IDENTITY_INVENTORY_KIND,
            "node": node,
            "domain": "oldlab" if node.startswith("oldlab-") else "gb10",
            "uid_start": policy.uid_start,
            "uid_end": policy.uid_end,
            "occupied_ids": (occupied_by_node or {}).get(node, []),
            "identity_inventory_sha256": (f"{index:064x}"[-64:]),
            "checked_at": observed_at,
        }
        for index, node in enumerate(registry.FLEET_NODES, start=1)
    ]
    unsigned = {
        "schema_version": registry.SCHEMA_VERSION,
        "kind": registry.FLEET_IDENTITY_INVENTORY_KIND,
        "registry_generation": 0,
        "registry_payload_sha256": "f" * 64,
        "uid_start": policy.uid_start,
        "uid_end": policy.uid_end,
        "collected_at": observed_at,
        "expires_at": (observed + timedelta(seconds=registry.FLEET_IDENTITY_MAX_AGE_SECONDS))
        .isoformat()
        .replace("+00:00", "Z"),
        "node_set_sha256": registry._digest({"nodes": list(registry.FLEET_NODES)}),
        "nodes": nodes,
    }
    raw = registry._canonical(
        {**unsigned, "payload_sha256": registry._digest(unsigned)},
    )
    path.write_bytes(raw)
    path.chmod(0o600)
    return raw


def test_registration_is_stable_across_idempotent_replay_and_display_change(
    tmp_path: Path,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    principal = "oidc:example:subject-123"
    first_request = _register(principal, "registration-key-0001", display_name="Old Name")

    first = authority.register(first_request)
    replay = authority.register(first_request)
    renamed = authority.register(
        _register(principal, "registration-key-0002", display_name="New Name"),
    )

    assert replay == first
    assert renamed.env_id == first.env_id
    assert renamed.runtime_id == first.runtime_id
    assert renamed.uid == first.uid
    assert renamed.ports == first.ports
    assert renamed.display_name == "New Name"
    assert len(authority.list_environments(principal_id=principal)) == 1

    changed_replay = {**first_request, "display_name": "conflicting replay"}
    with pytest.raises(registry.RegistryError, match="idempotency key conflicts"):
        authority.register(changed_replay)


def test_revived_lifecycle_rejects_old_candidate_content_and_accepts_new_candidate(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    principal = "oidc:example:revived-candidate"
    environment = authority.register(
        _register(principal, "registration-revived-candidate"),
    )
    old_request = _candidate(
        environment,
        "candidate-before-retirement",
        sha="1" * 40,
    )
    old_candidate = authority.import_candidate(old_request)
    quarantined = authority.begin_retirement(
        environment.env_id,
        principal_id=principal,
        expected_resource_generation=environment.resource_generation,
    )
    retired = authority.retire_environment(
        environment.env_id,
        principal_id=principal,
        expected_resource_generation=quarantined.resource_generation,
    )
    revived = authority.revive_environment(
        environment.env_id,
        principal_id=principal,
        expected_resource_generation=retired.resource_generation,
    )

    with pytest.raises(registry.RegistryError, match="retired lifecycle"):
        authority.import_candidate(old_request)
    with pytest.raises(registry.RegistryError, match="retired lifecycle"):
        authority.import_candidate(
            {
                **old_request,
                "idempotency_key": "candidate-after-retirement-same",
            }
        )
    with pytest.raises(registry.RegistryError, match="candidate lifecycle is stale"):
        authority.begin_deployment(
            _deploy(
                revived,
                old_candidate,
                "deploy-old-candidate-after-revive",
            )
        )

    new_candidate = authority.import_candidate(
        {
            **_candidate(
                revived,
                "candidate-after-retirement-new",
                sha="2" * 40,
            ),
            "candidate_tree": "3" * 40,
            "bundle_sha256": "4" * 64,
        }
    )
    assert new_candidate.lifecycle_epoch == revived.lifecycle_epoch
    assert new_candidate.lifecycle_epoch == old_candidate.lifecycle_epoch + 1
    assert (
        authority.begin_deployment(
            _deploy(
                revived,
                new_candidate,
                "deploy-new-candidate-after-revive",
            )
        ).candidate_id
        == new_candidate.candidate_id
    )


def test_one_hundred_concurrent_registrations_have_no_resource_collisions(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)

    def register(index: int) -> registry.EnvironmentRecord:
        return authority.register(
            _register(
                f"oidc:example:subject-{index:03d}",
                f"registration-key-{index:04d}",
            ),
        )

    with ThreadPoolExecutor(max_workers=32) as pool:
        environments = list(pool.map(register, range(100)))

    assert len({item.env_id for item in environments}) == 100
    assert len({item.runtime_id for item in environments}) == 100
    assert len({item.uid for item in environments}) == 100
    assert len({item.gid for item in environments}) == 100
    assert len({item.compose_project for item in environments}) == 100
    assert len({item.database_name for item in environments}) == 100
    assert len({item.postgres_volume for item in environments}) == 100
    assert len({item.minio_volume for item in environments}) == 100
    assert len({item.slurm_user for item in environments}) == 100
    assert len({item.slurm_account for item in environments}) == 100
    assert len({item.slurm_qos for item in environments}) == 100
    assert len({item.cgroup_slice for item in environments}) == 100
    ports = [port for item in environments for port in item.ports.values()]
    assert len(ports) == 1300
    assert len(set(ports)) == 1300
    assert all(
        item.candidate_root.startswith("/shared_work/loom/candidates/environments/")
        for item in environments
    )
    assert all(
        item.state_root.startswith("/srv/loom/developer-environments/") for item in environments
    )
    assert authority.snapshot()["generation"] == 100
    assert authority.snapshot_path.read_bytes() == authority.snapshot_bytes()


def test_parallel_short_connections_tolerate_safe_sqlite_sidecar_churn(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    authority.register(
        _register("oidc:example:baseline", "registration-key-baseline"),
    )

    def read_snapshot(_index: int) -> int:
        return int(authority.snapshot()["generation"])

    for _round in range(8):
        with ThreadPoolExecutor(max_workers=32) as pool:
            generations = list(pool.map(read_snapshot, range(128)))
        assert generations == [1] * 128


def test_legacy_seed_import_is_exact_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    owner_uids = {"qianyi": 501, "hongjian": 502, "devansh": 503}

    def getpwnam(username: str) -> types.SimpleNamespace:
        if username not in owner_uids:
            raise KeyError(username)
        return types.SimpleNamespace(
            pw_name=username,
            pw_uid=owner_uids[username],
        )

    monkeypatch.setattr(
        registry.pwd,
        "getpwnam",
        getpwnam,
    )
    monkeypatch.setattr(registry.pwd, "getpwuid", _missing_identity)
    monkeypatch.setattr(registry.grp, "getgrnam", _missing_identity)
    monkeypatch.setattr(registry.grp, "getgrgid", _missing_identity)
    seed = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/developer-environment-registry-seed.toml"
    )

    first = authority.import_legacy_seed(seed)
    second = authority.import_legacy_seed(seed)

    assert second == first
    assert [item.runtime_id for item in first] == ["qianyi", "hongjian", "devansh"]
    assert [item.uid for item in first] == [31021, 31022, 31023]
    assert [item.principal_id for item in first] == [
        "unix-uid:501",
        "unix-uid:502",
        "unix-uid:503",
    ]
    qianyi, hongjian, devansh = first
    listeners.update(qianyi.ports.values())
    assert (
        authority.reconcile_predeployment_ports(
            qianyi.env_id,
            principal_id=qianyi.principal_id,
            expected_resource_generation=qianyi.resource_generation,
        )
        == qianyi
    )
    listeners.clear()
    assert qianyi.ports["control_plane"] == 20080
    assert hongjian.ports["control_plane"] == 21080
    assert devansh.ports["control_plane"] == 22080
    assert (
        qianyi.ports["relay_control_plane"],
        qianyi.ports["relay_gateway"],
        qianyi.ports["relay_minio"],
    ) == (26080, 26100, 26900)
    assert (
        hongjian.ports["relay_control_plane"],
        hongjian.ports["relay_gateway"],
        hongjian.ports["relay_minio"],
    ) == (27080, 27100, 27900)
    assert (
        devansh.ports["relay_control_plane"],
        devansh.ports["relay_gateway"],
        devansh.ports["relay_minio"],
    ) == (28080, 28100, 28900)
    assert qianyi.compose_project == "loom-sandbox-qianyi"
    assert qianyi.candidate_root == "/shared_work/loom/candidates/sandboxes/qianyi"
    assert len({item.slurm_qos for item in first}) == 3
    snapshot = authority.snapshot()
    assert snapshot["generation"] == 1
    snapshot_qianyi = next(
        item for item in snapshot["environments"] if item["runtime_id"] == "qianyi"
    )
    assert snapshot_qianyi["ports"]["relay_control_plane"] == 26080
    assert snapshot_qianyi["ports"]["relay_gateway"] == 26100
    assert snapshot_qianyi["ports"]["relay_minio"] == 26900
    assert authority.snapshot_path.read_bytes() == authority.snapshot_bytes()

    for username, owner_uid in owner_uids.items():
        replay = authority.register(
            _register(
                f"unix-uid:{owner_uid}",
                f"registration-key-{username}",
                display_name=username,
            )
        )
        assert replay.runtime_id == username
    assert len(authority.list_environments()) == 3

    fourth = authority.register(
        _register("oidc:example:fourth", "registration-key-fourth"),
    )
    assert fourth.layout_version == "dynamic-v1"
    assert fourth.runtime_id not in {"qianyi", "hongjian", "devansh"}
    assert fourth.uid == 32000
    assert min(fourth.ports.values()) == 23000
    assert not set(fourth.ports.values()) & {
        port for environment in first for port in environment.ports.values()
    }


def test_legacy_seed_rejects_host_service_identity_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _new_registry(tmp_path)
    seed = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/developer-environment-registry-seed.toml"
    )
    owner_uids = {"qianyi": 501, "hongjian": 502, "devansh": 503}

    def getpwnam(username: str) -> types.SimpleNamespace:
        if username in owner_uids:
            return types.SimpleNamespace(
                pw_name=username,
                pw_uid=owner_uids[username],
            )
        raise KeyError(username)

    def getpwuid(identity: int) -> types.SimpleNamespace:
        if identity == 31_021:
            return types.SimpleNamespace(
                pw_name="unrelated-service",
                pw_uid=identity,
            )
        raise KeyError(identity)

    monkeypatch.setattr(registry.pwd, "getpwnam", getpwnam)
    monkeypatch.setattr(registry.pwd, "getpwuid", getpwuid)
    monkeypatch.setattr(registry.grp, "getgrnam", _missing_identity)
    monkeypatch.setattr(registry.grp, "getgrgid", _missing_identity)

    with pytest.raises(registry.RegistryError, match="user identity conflicts"):
        authority.import_legacy_seed(seed)


def test_legacy_seed_accepts_exact_preinstalled_service_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _new_registry(tmp_path)
    seed = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/developer-environment-registry-seed.toml"
    )
    owner_uids = {"qianyi": 501, "hongjian": 502, "devansh": 503}
    service_uids = {
        "loom-sandbox-qianyi": 31_021,
        "loom-sandbox-hongjian": 31_022,
        "loom-sandbox-devansh": 31_023,
    }
    users = {**owner_uids, **service_uids}

    def getpwnam(username: str) -> types.SimpleNamespace:
        if username not in users:
            raise KeyError(username)
        return types.SimpleNamespace(pw_name=username, pw_uid=users[username])

    def getpwuid(identity: int) -> types.SimpleNamespace:
        matches = [name for name, uid in service_uids.items() if uid == identity]
        if not matches:
            raise KeyError(identity)
        return types.SimpleNamespace(pw_name=matches[0], pw_uid=identity)

    def getgrnam(group_name: str) -> types.SimpleNamespace:
        if group_name not in service_uids:
            raise KeyError(group_name)
        return types.SimpleNamespace(
            gr_name=group_name,
            gr_gid=service_uids[group_name],
        )

    def getgrgid(identity: int) -> types.SimpleNamespace:
        matches = [name for name, gid in service_uids.items() if gid == identity]
        if not matches:
            raise KeyError(identity)
        return types.SimpleNamespace(gr_name=matches[0], gr_gid=identity)

    monkeypatch.setattr(registry.pwd, "getpwnam", getpwnam)
    monkeypatch.setattr(registry.pwd, "getpwuid", getpwuid)
    monkeypatch.setattr(registry.grp, "getgrnam", getgrnam)
    monkeypatch.setattr(registry.grp, "getgrgid", getgrgid)

    imported = authority.import_legacy_seed(seed)
    assert [item.runtime_id for item in imported] == [
        "qianyi",
        "hongjian",
        "devansh",
    ]


def test_uid_allocator_skips_host_passwd_and_group_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        registry.pwd,
        "getpwall",
        lambda: [types.SimpleNamespace(pw_uid=32_000)],
    )
    monkeypatch.setattr(
        registry.grp,
        "getgrall",
        lambda: [types.SimpleNamespace(gr_gid=32_001)],
    )
    authority = _new_registry(
        tmp_path,
        policy=registry.AllocationPolicy(uid_start=32_000, uid_end=32_002),
    )

    allocated = authority.register(_register("oidc:example:owner", "registration-key-owner"))
    assert allocated.uid == 32_002
    with pytest.raises(registry.RegistryError, match="UID allocation is exhausted"):
        authority.register(_register("oidc:example:other", "registration-key-other"))


def test_uid_allocator_uses_only_identity_free_across_all_twenty_nodes(
    tmp_path: Path,
) -> None:
    policy = registry.AllocationPolicy(uid_start=32_000, uid_end=32_003)
    inventory = tmp_path / "fleet-identity-inventory.json"
    _fleet_identity_inventory(
        inventory,
        policy,
        occupied_by_node={
            "oldlab-1": [32_000],
            "trt-gb10-7": [32_001],
        },
    )
    authority = registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry.sqlite3",
        policy=policy,
        fleet_identity_inventory_path=inventory,
    )

    first = authority.register(
        _register("oidc:example:first", "fleet-registration-key-first"),
    )
    second = authority.register(
        _register("oidc:example:second", "fleet-registration-key-second"),
    )

    assert len(registry.FLEET_NODES) == 20
    assert "trt-gb10-7" in registry.FLEET_NODES
    assert first.uid == first.gid == 32_002
    assert second.uid == second.gid == 32_003


def test_uid_allocator_fails_closed_without_fleet_inventory(tmp_path: Path) -> None:
    authority = registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry.sqlite3",
        fleet_identity_inventory_path=tmp_path / "missing-inventory.json",
    )

    with pytest.raises(registry.RegistryError, match="inventory is unavailable"):
        authority.register(
            _register("oidc:example:missing", "fleet-registration-key-missing"),
        )
    assert authority.list_environments() == ()


@pytest.mark.parametrize(
    "tamper",
    ["missing-node", "stale", "digest", "node-order", "occupied-order"],
)
def test_fleet_identity_inventory_is_complete_fresh_canonical_and_digest_bound(
    tmp_path: Path,
    tamper: str,
) -> None:
    policy = registry.AllocationPolicy(uid_start=32_000, uid_end=32_003)
    path = tmp_path / "fleet-identity-inventory.json"
    raw = _fleet_identity_inventory(path, policy)
    payload = json.loads(raw)
    if tamper == "missing-node":
        payload["nodes"].pop()
    elif tamper == "stale":
        observed = datetime.now(UTC) - timedelta(minutes=10)
        payload["collected_at"] = observed.isoformat().replace("+00:00", "Z")
        payload["expires_at"] = (
            (observed + timedelta(seconds=registry.FLEET_IDENTITY_MAX_AGE_SECONDS))
            .isoformat()
            .replace("+00:00", "Z")
        )
        for node in payload["nodes"]:
            node["checked_at"] = payload["collected_at"]
    elif tamper == "digest":
        payload["registry_payload_sha256"] = "e" * 64
    elif tamper == "node-order":
        payload["nodes"][0], payload["nodes"][1] = payload["nodes"][1], payload["nodes"][0]
    else:
        payload["nodes"][0]["occupied_ids"] = [32_001, 32_000]
    if tamper != "digest":
        unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
        payload["payload_sha256"] = registry._digest(unsigned)

    with pytest.raises(registry.RegistryError, match="fleet identity"):
        registry.DeveloperEnvironmentRegistry.verify_fleet_identity_inventory(
            registry._canonical(payload),
            policy=policy,
        )


def test_legacy_seed_reader_rejects_symlink(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    seed = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/developer-environment-registry-seed.toml"
    )
    linked = tmp_path / "seed.toml"
    linked.symlink_to(seed)

    with pytest.raises(registry.RegistryError, match="seed is unavailable or invalid"):
        authority.import_legacy_seed(linked)


def test_regular_seed_reader_returns_real_bytes(tmp_path: Path) -> None:
    seed = tmp_path / "seed.toml"
    seed.write_bytes(b"schema_version = 1\n")

    assert registry._read_regular(seed, limit=1024) == b"schema_version = 1\n"


@pytest.mark.parametrize("suffix", ["-wal", "-shm"])
def test_registry_rejects_symlinked_sqlite_sidecars(
    tmp_path: Path,
    suffix: str,
) -> None:
    authority = _new_registry(tmp_path)
    target = tmp_path / f"attacker{suffix}"
    target.write_bytes(b"not sqlite")
    Path(f"{authority.database}{suffix}").symlink_to(target)

    with pytest.raises(registry.RegistryError, match="storage metadata is unsafe"):
        authority.snapshot()


def test_registry_rejects_symlinked_database_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(registry.RegistryError, match="storage root is unsafe"):
        registry.DeveloperEnvironmentRegistry(linked_parent / "registry.sqlite3")


@pytest.mark.parametrize("operation", ["register", "candidate", "deploy"])
def test_public_request_schemas_reject_unknown_resource_fields(
    tmp_path: Path,
    operation: str,
) -> None:
    authority = _new_registry(tmp_path)
    environment = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    candidate = authority.import_candidate(
        _candidate(environment, "candidate-key-owner"),
    )
    request = {
        "register": _register("oidc:example:other", "registration-key-other"),
        "candidate": _candidate(environment, "candidate-key-extra"),
        "deploy": _deploy(environment, candidate, "deployment-key-extra"),
    }[operation]
    request["state_root"] = "/root/caller-selected"

    with pytest.raises(registry.RegistryError, match="request binding is invalid"):
        {
            "register": authority.register,
            "candidate": authority.import_candidate,
            "deploy": authority.begin_deployment,
        }[operation](request)


def test_candidate_import_is_owner_bound_and_idempotent(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    owner = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    other = authority.register(
        _register("oidc:example:other", "registration-key-other"),
    )
    request = _candidate(owner, "candidate-key-owner")

    first = authority.import_candidate(request)
    assert authority.import_candidate(request) == first
    assert first.env_id == owner.env_id
    assert first.principal_id == owner.principal_id

    stolen = _candidate(owner, "candidate-key-stolen", principal=other.principal_id)
    with pytest.raises(registry.RegistryError, match="candidate ownership is invalid"):
        authority.import_candidate(stolen)

    changed_replay = {**request, "candidate_sha": "f" * 40}
    with pytest.raises(registry.RegistryError, match="idempotency key conflicts"):
        authority.import_candidate(changed_replay)

    metadata_drift = {
        **request,
        "idempotency_key": "candidate-key-metadata-drift",
        "bundle_size": 2048,
    }
    with pytest.raises(registry.RegistryError, match="metadata conflicts"):
        authority.import_candidate(metadata_drift)


def test_deployment_phases_replay_and_generation_fence(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    environment = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    candidate = authority.import_candidate(
        _candidate(environment, "candidate-key-owner"),
    )
    request = _deploy(environment, candidate, "deployment-key-owner")

    deployment = authority.begin_deployment(request)
    assert authority.begin_deployment(request) == deployment
    current = deployment
    precommit: dict[str, Any] | None = None
    for expected, following in zip(
        registry.DEPLOY_PHASES[:-1],
        registry.DEPLOY_PHASES[1:],
        strict=True,
    ):
        if following == "committed":
            precommit = authority.snapshot()
            prepared = authority.prepare_deployment_finalization(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_resource_generation=1,
            )
            assert prepared.phase == "verified"
            assert prepared.applied_resource_generation == 2
            assert prepared.applied_registry_generation == precommit["generation"]
            assert prepared.applied_registry_payload_sha256 == precommit["payload_sha256"]
            authority.record_deployment_finalization(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_resource_generation=1,
                evidence=_finalization_evidence(),
            )
        current = authority.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=1,
        )
        assert current.phase == following
        replay = authority.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=1,
        )
        assert replay == current

    assert precommit is not None
    assert current.applied_resource_generation == 2
    assert current.applied_registry_generation == precommit["generation"]
    assert current.applied_registry_payload_sha256 == precommit["payload_sha256"]
    committed = authority.lookup(environment.env_id, principal_id=environment.principal_id)
    assert committed.current_candidate_id == candidate.candidate_id
    assert committed.resource_generation == 2
    assert committed.state == "active"
    assert authority.begin_deployment(request).phase == "committed"

    stale = _deploy(
        committed,
        candidate,
        "deployment-key-stale",
        generation=1,
    )
    with pytest.raises(registry.RegistryError, match="generation is stale"):
        authority.begin_deployment(stale)


def test_deployment_rejects_foreign_candidate_and_owner(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    first = authority.register(
        _register("oidc:example:first", "registration-key-first"),
    )
    second = authority.register(
        _register("oidc:example:second", "registration-key-second"),
    )
    second_candidate = authority.import_candidate(
        _candidate(second, "candidate-key-second"),
    )

    foreign_candidate = _deploy(first, second_candidate, "deployment-key-foreign")
    with pytest.raises(registry.RegistryError, match="deployment ownership is invalid"):
        authority.begin_deployment(foreign_candidate)

    forged_owner = _deploy(
        second,
        second_candidate,
        "deployment-key-forged",
        principal=first.principal_id,
    )
    with pytest.raises(registry.RegistryError, match="deployment ownership is invalid"):
        authority.begin_deployment(forged_owner)


def test_failed_deployment_is_terminal_replayable_and_releases_environment(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    environment = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    candidate = authority.import_candidate(
        _candidate(environment, "candidate-key-owner"),
    )
    deployment = authority.begin_deployment(
        _deploy(environment, candidate, "deployment-key-owner"),
    )

    failed = authority.fail_deployment(
        deployment.deployment_id,
        principal_id=environment.principal_id,
        expected_phase="requested",
        expected_resource_generation=1,
    )
    assert failed.phase == "failed"
    assert (
        authority.fail_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase="requested",
            expected_resource_generation=1,
        )
        == failed
    )
    assert authority.lookup(environment.env_id).state == "ready"

    replacement = authority.begin_deployment(
        _deploy(environment, candidate, "deployment-key-replacement"),
    )
    assert replacement.phase == "requested"


def test_retirement_is_owner_scoped_generation_fenced_and_replayable(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    environment = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    other = authority.register(
        _register("oidc:example:other", "registration-key-other"),
    )

    with pytest.raises(registry.RegistryError, match="ownership is invalid"):
        authority.begin_retirement(
            environment.env_id,
            principal_id=other.principal_id,
            expected_resource_generation=environment.resource_generation,
        )
    with pytest.raises(registry.RegistryError, match="generation is stale"):
        authority.begin_retirement(
            environment.env_id,
            principal_id=environment.principal_id,
            expected_resource_generation=environment.resource_generation + 1,
        )

    quarantined = authority.begin_retirement(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    assert quarantined.state == "quarantined"
    assert (
        authority.begin_retirement(
            environment.env_id,
            principal_id=environment.principal_id,
            expected_resource_generation=environment.resource_generation,
        )
        == quarantined
    )
    retired = authority.retire_environment(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    assert retired.state == "retired"
    assert retired.resource_generation == environment.resource_generation + 1
    assert (
        authority.retire_environment(
            environment.env_id,
            principal_id=environment.principal_id,
            expected_resource_generation=environment.resource_generation,
        )
        == retired
    )
    verified = authority.verify_snapshot(authority.snapshot_bytes())
    retired_snapshot = next(
        row for row in verified["environments"] if row["env_id"] == environment.env_id
    )
    assert retired_snapshot["state"] == "retired"
    assert retired_snapshot["resource_generation"] == retired.resource_generation


def test_retired_environment_revives_same_identity_and_clears_candidate(
    tmp_path: Path,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    original = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    candidate = authority.import_candidate(
        _candidate(original, "candidate-key-owner"),
    )
    deployment = authority.begin_deployment(
        _deploy(original, candidate, "deployment-key-owner"),
    )
    for expected, following in zip(
        registry.DEPLOY_PHASES[:-1],
        registry.DEPLOY_PHASES[1:],
        strict=True,
    ):
        if following == "committed":
            authority.prepare_deployment_finalization(
                deployment.deployment_id,
                principal_id=original.principal_id,
                expected_resource_generation=original.resource_generation,
            )
            authority.record_deployment_finalization(
                deployment.deployment_id,
                principal_id=original.principal_id,
                expected_resource_generation=original.resource_generation,
                evidence=_finalization_evidence(),
            )
        authority.advance_deployment(
            deployment.deployment_id,
            principal_id=original.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=original.resource_generation,
        )
    active = authority.lookup(original.env_id)
    listeners.update(active.ports.values())
    active_readback = authority.reconcile_predeployment_ports(
        active.env_id,
        principal_id=active.principal_id,
        expected_resource_generation=active.resource_generation,
    )
    assert active_readback.ports == original.ports
    authority.begin_retirement(
        active.env_id,
        principal_id=active.principal_id,
        expected_resource_generation=active.resource_generation,
    )
    retired = authority.retire_environment(
        active.env_id,
        principal_id=active.principal_id,
        expected_resource_generation=active.resource_generation,
    )

    revived = authority.revive_environment(
        retired.env_id,
        principal_id=retired.principal_id,
        expected_resource_generation=retired.resource_generation,
    )

    assert revived.state == "ready"
    assert revived.current_candidate_id is None
    assert revived.resource_generation == retired.resource_generation + 1
    revived_readback = authority.reconcile_predeployment_ports(
        revived.env_id,
        principal_id=revived.principal_id,
        expected_resource_generation=revived.resource_generation,
    )
    assert revived_readback.ports == original.ports
    assert revived_readback.resource_generation == revived.resource_generation
    for field in (
        "env_id",
        "runtime_id",
        "uid",
        "gid",
        "candidate_root",
        "runtime_root",
        "state_root",
        "service_user",
        "service_group",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
    ):
        assert getattr(revived, field) == getattr(original, field)
    registry.DeveloperEnvironmentRegistry.verify_snapshot(authority.snapshot_bytes())


def test_retirement_rejects_an_active_deployment(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    environment = authority.register(
        _register("oidc:example:owner", "registration-key-owner"),
    )
    candidate = authority.import_candidate(
        _candidate(environment, "candidate-key-owner"),
    )
    authority.begin_deployment(
        _deploy(environment, candidate, "deployment-key-owner"),
    )

    with pytest.raises(registry.RegistryError, match="state is invalid"):
        authority.retire_environment(
            environment.env_id,
            principal_id=environment.principal_id,
            expected_resource_generation=environment.resource_generation,
        )


def test_uid_and_port_allocator_exhaustion_fail_closed(tmp_path: Path) -> None:
    uid_authority = _new_registry(
        tmp_path / "uid",
        policy=registry.AllocationPolicy(
            uid_start=40_000,
            uid_end=40_000,
            port_start=30_000,
            port_end=30_100,
            port_block_size=16,
        ),
    )
    uid_authority.register(_register("oidc:example:one", "registration-key-one"))
    with pytest.raises(registry.RegistryError, match="UID allocation is exhausted"):
        uid_authority.register(_register("oidc:example:two", "registration-key-two"))

    port_authority = _new_registry(
        tmp_path / "port",
        policy=registry.AllocationPolicy(
            uid_start=41_000,
            uid_end=41_100,
            port_start=31_000,
            port_end=31_012,
            port_block_size=16,
        ),
    )
    port_authority.register(_register("oidc:example:one", "registration-key-one"))
    with pytest.raises(registry.RegistryError, match="port allocation is exhausted"):
        port_authority.register(_register("oidc:example:two", "registration-key-two"))


@pytest.mark.parametrize(
    "listeners",
    [
        frozenset({23_003}),
        frozenset(range(23_000, 23_013)),
    ],
)
def test_dynamic_port_allocator_skips_partial_or_full_listener_blocks(
    tmp_path: Path,
    listeners: frozenset[int],
) -> None:
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: listeners,
    )

    environment = authority.register(
        _register("oidc:example:listener-skip", "registration-listener-skip"),
    )

    assert min(environment.ports.values()) == 23_016
    assert not set(environment.ports.values()) & listeners


def test_listener_inventory_exhaustion_is_atomic(
    tmp_path: Path,
) -> None:
    authority = _new_registry(
        tmp_path,
        policy=registry.AllocationPolicy(
            port_start=23_000,
            port_end=23_031,
            port_block_size=16,
        ),
        port_inventory_collector=lambda: frozenset({23_003, 23_019}),
    )

    with pytest.raises(registry.RegistryError, match="port allocation is exhausted"):
        authority.register(
            _register("oidc:example:listener-exhaustion", "registration-listener-exhaustion"),
        )

    assert authority.snapshot()["environments"] == []


def test_registration_port_decision_is_persisted_across_inventory_drift(
    tmp_path: Path,
) -> None:
    listeners: set[int] = {23_003}
    calls = 0

    def collect() -> frozenset[int]:
        nonlocal calls
        calls += 1
        return frozenset(listeners)

    authority = _new_registry(tmp_path, port_inventory_collector=collect)
    request = _register("oidc:example:stable-ports", "registration-stable-ports")
    first = authority.register(request)
    listeners.update(first.ports.values())
    replay = authority.register(request)
    restarted = registry.DeveloperEnvironmentRegistry(
        authority.database,
        port_inventory_collector=collect,
    )

    assert replay.ports == first.ports
    assert restarted.lookup(first.env_id, principal_id=first.principal_id).ports == first.ports
    assert calls == 1


@pytest.mark.parametrize(
    "inventory",
    [
        {"not-a-port"},
        {0},
        {65_536},
        {True},
    ],
)
def test_malformed_host_port_inventory_fails_closed(
    tmp_path: Path,
    inventory: set[object],
) -> None:
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: inventory,  # type: ignore[arg-type,return-value]
    )

    with pytest.raises(registry.RegistryError, match="host port inventory"):
        authority.register(
            _register("oidc:example:malformed-listeners", "registration-malformed-listeners"),
        )

    assert authority.snapshot()["environments"] == []


def test_proc_listener_inventory_parses_listen_only_and_rejects_malformed(
    tmp_path: Path,
) -> None:
    table = tmp_path / "tcp"
    table.write_text(
        "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt "
        "uid timeout inode\n"
        "   0: 00000000:59D8 00000000:0000 0A 00000000:00000000 00:00000000 "
        "00000000 0 0 1 1\n"
        "   1: 0100007F:59D9 00000000:0000 01 00000000:00000000 00:00000000 "
        "00000000 0 0 2 1\n",
        encoding="ascii",
    )

    assert registry._proc_listener_ports(table) == {23_000}

    table.write_text(
        "sl local_address rem_address st\n0: malformed\n",
        encoding="ascii",
    )
    with pytest.raises(registry.RegistryError, match="malformed"):
        registry._proc_listener_ports(table)


def _query_fake_docker_socket(
    tmp_path: Path,
    response: bytes,
    query: Callable[[Path], Any],
    *,
    drift_socket: bool = False,
) -> Any:
    del tmp_path
    path = Path("/tmp") / f"loom-docker-{os.getpid()}-{id(response):x}.sock"
    path.unlink(missing_ok=True)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    path.chmod(0o660)
    server.listen(1)
    requests: list[bytes] = []

    def serve() -> None:
        replacement: socket.socket | None = None
        try:
            connection, _address = server.accept()
            with connection:
                raw = b""
                while b"\r\n\r\n" not in raw and len(raw) <= 65_536:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    raw += chunk
                requests.append(raw)
                if drift_socket:
                    path.unlink()
                    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    replacement.bind(str(path))
                    path.chmod(0o660)
                try:
                    connection.sendall(response)
                except BrokenPipeError:
                    pass
        finally:
            if replacement is not None:
                replacement.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        result = query(path)
    finally:
        thread.join(timeout=5)
        server.close()
        path.unlink(missing_ok=True)
    assert len(requests) == 1
    assert requests[0].startswith(b"GET /containers/json?all=1 HTTP/1.1\r\n")
    return result


def _docker_http_response(
    payload: object,
    *,
    status: str = "200 OK",
    content_length: int | None = None,
) -> bytes:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    length = len(body) if content_length is None else content_length
    return (
        f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {length}\r\nConnection: close\r\n\r\n"
    ).encode("ascii") + body


def _docker_chunked_http_response(
    payload: object,
    *,
    split_at: int | None = None,
) -> bytes:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    boundary = max(1, len(body) // 2) if split_at is None else split_at
    chunks = (body[:boundary], body[boundary:])
    encoded = b"".join(
        f"{len(chunk):x}\r\n".encode("ascii") + chunk + b"\r\n" for chunk in chunks if chunk
    )
    return (
        b"HTTP/1.1 200 OK\r\n"
        b"Api-Version: 1.52\r\n"
        b"Content-Type: application/json\r\n"
        b"Docker-Experimental: false\r\n"
        b"Ostype: linux\r\n"
        b"Server: Docker/29.1.3 (linux)\r\n"
        b"Transfer-Encoding: chunked\r\n\r\n" + encoded + b"0\r\n\r\n"
    )


def test_docker_published_port_inventory_covers_proxyless_reservations(
    tmp_path: Path,
) -> None:
    payload = [
        {
            "Id": "a" * 64,
            "Ports": [
                {
                    "IP": "0.0.0.0",
                    "PrivatePort": 8000,
                    "PublicPort": 23_003,
                    "Type": "tcp",
                },
                {"PrivatePort": 9000, "Type": "tcp"},
            ],
        }
    ]

    for response in (
        _docker_http_response(payload),
        _docker_chunked_http_response(payload),
    ):
        published = _query_fake_docker_socket(
            tmp_path,
            response,
            lambda path: registry._docker_published_ports(
                path,
                expected_uid=os.getuid(),
            ),
        )

        assert published == {23_003}


@pytest.mark.parametrize(
    "response",
    [
        _docker_http_response([], status="500 Internal Server Error"),
        _docker_http_response([], content_length=999),
        _docker_http_response(
            [
                {
                    "Id": "a" * 64,
                    "Ports": [
                        {
                            "IP": "0.0.0.0",
                            "PrivatePort": 8000,
                            "PublicPort": 23_003,
                            "Type": "tcp",
                            "extra": True,
                        }
                    ],
                }
            ],
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n[]"
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\n\r\nz\r\n[]\r\n0\r\n\r\n"
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n2\r\n[]\r\n"
        ),
        (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: gzip\r\n\r\n[]"),
        b"not-http",
    ],
)
def test_docker_published_port_inventory_rejects_malformed_responses(
    tmp_path: Path,
    response: bytes,
) -> None:
    with pytest.raises(registry.RegistryError, match="Docker port inventory"):
        _query_fake_docker_socket(
            tmp_path,
            response,
            lambda path: registry._docker_published_ports(
                path,
                expected_uid=os.getuid(),
            ),
        )


def test_docker_published_port_inventory_rejects_oversize_and_socket_drift(
    tmp_path: Path,
) -> None:
    oversized = (
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
        + str(registry.DOCKER_API_MAX_BYTES + 1).encode("ascii")
        + b"\r\nConnection: close\r\n\r\n"
        + b" " * (registry.DOCKER_API_MAX_BYTES + 1)
    )
    with pytest.raises(registry.RegistryError, match="Docker port inventory"):
        _query_fake_docker_socket(
            tmp_path,
            oversized,
            lambda path: registry._docker_published_ports(
                path,
                expected_uid=os.getuid(),
            ),
        )

    oversized_chunk = (
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
        b"Transfer-Encoding: chunked\r\n\r\n"
        + f"{registry.DOCKER_API_MAX_BYTES + 1:x}\r\n".encode("ascii")
        + b"0\r\n\r\n"
    )
    with pytest.raises(registry.RegistryError, match="size bound"):
        _query_fake_docker_socket(
            tmp_path,
            oversized_chunk,
            lambda path: registry._docker_published_ports(
                path,
                expected_uid=os.getuid(),
            ),
        )

    with pytest.raises(registry.RegistryError, match="identity drifted"):
        _query_fake_docker_socket(
            tmp_path,
            _docker_http_response([]),
            lambda path: registry._docker_published_ports(
                path,
                expected_uid=os.getuid(),
            ),
            drift_socket=True,
        )


def test_combined_reserved_port_collector_merges_proc_and_docker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        registry,
        "collect_host_listener_ports",
        lambda: frozenset({23_003}),
    )
    monkeypatch.setattr(
        registry,
        "_docker_published_ports",
        lambda path, *, expected_uid: (
            {23_019}
            if path == registry.DOCKER_SOCKET and expected_uid == 0
            else pytest.fail("collector escaped its fixed Docker socket")
        ),
    )

    assert registry.collect_host_reserved_ports() == frozenset({23_003, 23_019})


def test_pristine_dynamic_ports_reallocate_per_generation_and_reject_stale_fence(
    tmp_path: Path,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    environment = authority.register(
        _register("oidc:example:port-race", "registration-port-race"),
    )
    authority.import_candidate(_candidate(environment, "candidate-port-race"))

    before_clear = authority.snapshot()
    clear = authority.reconcile_predeployment_ports(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    assert clear == environment
    assert authority.snapshot() == before_clear

    listeners.add(environment.ports["control_plane"])
    first = authority.reconcile_predeployment_ports(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    assert first.resource_generation == environment.resource_generation + 1
    assert min(first.ports.values()) == 23_016

    with pytest.raises(registry.RegistryError, match="generation is stale"):
        authority.reconcile_predeployment_ports(
            environment.env_id,
            principal_id=environment.principal_id,
            expected_resource_generation=environment.resource_generation,
        )

    listeners.add(first.ports["control_plane"])
    second = authority.reconcile_predeployment_ports(
        first.env_id,
        principal_id=first.principal_id,
        expected_resource_generation=first.resource_generation,
    )
    assert second.resource_generation == first.resource_generation + 1
    assert min(second.ports.values()) == 23_032

    connection = sqlite3.connect(authority.database)
    try:
        rows = connection.execute(
            """
            SELECT action, expected_resource_generation, applied_resource_generation
            FROM port_allocation_journal WHERE env_id = ? ORDER BY journal_id
            """,
            (environment.env_id,),
        ).fetchall()
    finally:
        connection.close()
    assert rows == [
        ("initial", 1, 1),
        ("pre-deployment-reallocation", 1, 2),
        ("pre-deployment-reallocation", 2, 3),
    ]


def test_deployment_history_freezes_dynamic_ports_without_self_conflict(
    tmp_path: Path,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    environment = authority.register(
        _register("oidc:example:port-freeze", "registration-port-freeze"),
    )
    candidate = authority.import_candidate(_candidate(environment, "candidate-port-freeze"))
    deployment = authority.begin_deployment(
        _deploy(environment, candidate, "deployment-port-freeze"),
    )
    listeners.update(environment.ports.values())

    deploying = authority.reconcile_predeployment_ports(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )

    assert deployment.phase == "requested"
    assert deploying.state == "deploying"
    assert deploying.ports == environment.ports
    assert deploying.resource_generation == environment.resource_generation


def test_requested_phase_failure_can_recover_ports_before_retry(
    tmp_path: Path,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    environment = authority.register(
        _register("oidc:example:failed-port-bind", "registration-failed-port-bind"),
    )
    candidate = authority.import_candidate(
        _candidate(environment, "candidate-failed-port-bind"),
    )
    deployment = authority.begin_deployment(
        _deploy(environment, candidate, "deployment-failed-port-bind"),
    )
    authority.fail_deployment(
        deployment.deployment_id,
        principal_id=environment.principal_id,
        expected_phase="requested",
        expected_resource_generation=environment.resource_generation,
    )
    listeners.add(environment.ports["control_plane"])

    rebound = authority.reconcile_predeployment_ports(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    retried = authority.begin_deployment(
        _deploy(
            rebound,
            candidate,
            "deployment-failed-port-bind-retry",
        ),
    )

    assert rebound.resource_generation == environment.resource_generation + 1
    assert rebound.ports != environment.ports
    assert retried.expected_resource_generation == rebound.resource_generation


def test_post_materialization_failure_cannot_reallocate_ports(
    tmp_path: Path,
) -> None:
    listeners: set[int] = set()
    authority = _new_registry(
        tmp_path,
        port_inventory_collector=lambda: frozenset(listeners),
    )
    environment = authority.register(
        _register("oidc:example:unsafe-port-repair", "registration-unsafe-port-repair"),
    )
    candidate = authority.import_candidate(
        _candidate(environment, "candidate-unsafe-port-repair"),
    )
    deployment = authority.begin_deployment(
        _deploy(environment, candidate, "deployment-unsafe-port-repair"),
    )
    current = deployment
    for expected, following in (
        ("requested", "resources-verified"),
        ("resources-verified", "candidate-materialized"),
    ):
        current = authority.advance_deployment(
            current.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )
    authority.fail_deployment(
        deployment.deployment_id,
        principal_id=environment.principal_id,
        expected_phase="candidate-materialized",
        expected_resource_generation=environment.resource_generation,
    )
    listeners.add(environment.ports["control_plane"])

    unchanged = authority.reconcile_predeployment_ports(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )

    assert unchanged.ports == environment.ports
    assert unchanged.resource_generation == environment.resource_generation


def test_lookup_list_and_snapshot_are_owner_scoped_and_canonical(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    first = authority.register(
        _register("oidc:example:first", "registration-key-first"),
    )
    second = authority.register(
        _register("oidc:example:second", "registration-key-second"),
    )

    assert authority.lookup(first.env_id, principal_id=first.principal_id) == first
    with pytest.raises(registry.RegistryError, match="ownership is invalid"):
        authority.lookup(first.env_id, principal_id=second.principal_id)
    assert authority.list_environments(principal_id=first.principal_id) == (first,)
    assert {item.env_id for item in authority.list_environments()} == {
        first.env_id,
        second.env_id,
    }

    raw = authority.snapshot_bytes()
    verified = authority.verify_snapshot(raw)
    assert verified["generation"] == 2
    assert verified["payload_sha256"] == authority.snapshot()["payload_sha256"]


def test_snapshot_tamper_and_noncanonical_encoding_are_rejected(tmp_path: Path) -> None:
    authority = _new_registry(tmp_path)
    authority.register(_register("oidc:example:owner", "registration-key-owner"))
    payload = json.loads(authority.snapshot_bytes())
    payload["environments"][0]["display_name"] = "tampered"
    tampered = registry._canonical(payload)

    with pytest.raises(registry.RegistryError, match="snapshot digest is invalid"):
        authority.verify_snapshot(tampered)

    valid = json.loads(authority.snapshot_bytes())
    pretty = (json.dumps(valid, indent=2, sort_keys=True) + "\n").encode()
    with pytest.raises(registry.RegistryError, match="snapshot binding is invalid"):
        authority.verify_snapshot(pretty)


def test_snapshot_verifier_rejects_resigned_tampered_and_inactive_rows(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    first = authority.register(_register("oidc:example:first", "registration-key-first"))
    authority.register(_register("oidc:example:second", "registration-key-second"))
    candidate = authority.import_candidate(_candidate(first, "candidate-key-first"))
    authority.begin_deployment(_deploy(first, candidate, "deployment-key-first"))
    original = authority.snapshot()

    def copy() -> dict[str, Any]:
        value = json.loads(registry._canonical(original))
        assert isinstance(value, dict)
        return value

    tampered_payloads: list[dict[str, Any]] = []

    extra_field = copy()
    extra_field["environments"][0]["caller_uid"] = 0
    tampered_payloads.append(extra_field)

    inactive_state = copy()
    inactive_state["environments"][0]["state"] = "active"
    tampered_payloads.append(inactive_state)

    duplicate_principal = copy()
    duplicate_principal["environments"][1]["principal_id"] = duplicate_principal["environments"][0][
        "principal_id"
    ]
    tampered_payloads.append(duplicate_principal)

    duplicate_port = copy()
    duplicate_port["environments"][1]["ports"]["postgres"] = duplicate_port["environments"][0][
        "ports"
    ]["postgres"]
    tampered_payloads.append(duplicate_port)

    candidate_owner = copy()
    candidate_owner["candidates"][0]["principal_id"] = next(
        item["principal_id"]
        for item in candidate_owner["environments"]
        if item["principal_id"] != candidate_owner["candidates"][0]["principal_id"]
    )
    tampered_payloads.append(candidate_owner)

    candidate_path = copy()
    candidate_path["candidates"][0]["bundle_path"] = "/tmp/candidate.bundle"
    tampered_payloads.append(candidate_path)

    deployment_relation = copy()
    deployment_relation["deployments"][0]["phase"] = "committed"
    tampered_payloads.append(deployment_relation)

    reordered = copy()
    reordered["environments"].reverse()
    tampered_payloads.append(reordered)

    for index, payload in enumerate(tampered_payloads):
        try:
            authority.verify_snapshot(_resign_snapshot(payload))
        except registry.RegistryError:
            pass
        else:
            pytest.fail(f"tampered snapshot {index} was accepted")


def test_current_snapshot_is_published_after_every_mutation_phase(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)

    def assert_current() -> None:
        raw = authority.snapshot_path.read_bytes()
        assert raw == authority.snapshot_bytes()
        assert authority.verify_snapshot(raw)["generation"] == authority.snapshot()["generation"]
        assert authority.snapshot_path.stat().st_mode & 0o777 == 0o600

    assert authority.snapshot_path == tmp_path / "current-snapshot.json"
    assert registry.CURRENT_SNAPSHOT_PATH == Path(
        "/var/lib/loom-developer-environment-registry/current-snapshot.json"
    )
    assert_current()
    environment = authority.register(_register("oidc:example:owner", "registration-key-owner"))
    assert_current()
    candidate = authority.import_candidate(_candidate(environment, "candidate-key-owner"))
    assert_current()
    deployment = authority.begin_deployment(_deploy(environment, candidate, "deployment-key-owner"))
    assert_current()
    advanced = authority.advance_deployment(
        deployment.deployment_id,
        principal_id=environment.principal_id,
        expected_phase="requested",
        next_phase="resources-verified",
        expected_resource_generation=environment.resource_generation,
    )
    assert advanced.phase == "resources-verified"
    assert_current()
    failed = authority.fail_deployment(
        deployment.deployment_id,
        principal_id=environment.principal_id,
        expected_phase="resources-verified",
        expected_resource_generation=environment.resource_generation,
    )
    assert failed.phase == "failed"
    assert_current()


def test_current_snapshot_restart_validation_and_tamper_healing(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    authority.register(_register("oidc:example:owner", "registration-key-owner"))
    expected = authority.snapshot_bytes()
    before = authority.snapshot_path.stat()

    restarted = registry.DeveloperEnvironmentRegistry(authority.database)
    after = restarted.snapshot_path.stat()
    assert restarted.snapshot_path.read_bytes() == expected
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)

    restarted.snapshot_path.write_bytes(b'{"tampered":true}\n')
    healed = registry.DeveloperEnvironmentRegistry(authority.database)
    assert healed.snapshot_path.read_bytes() == healed.snapshot_bytes()
    assert healed.verify_snapshot(healed.snapshot_path.read_bytes())["generation"] == 1


def test_snapshot_atomic_publish_failure_preserves_old_file_and_restart_heals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _new_registry(tmp_path)
    old_raw = authority.snapshot_path.read_bytes()
    real_replace = registry.os.replace

    def fail_replace(source: object, destination: object) -> None:
        if Path(destination) == authority.snapshot_path:
            raise OSError("injected atomic replace failure")
        real_replace(source, destination)

    monkeypatch.setattr(registry.os, "replace", fail_replace)
    with pytest.raises(registry.RegistryError, match="publication failed"):
        authority.register(_register("oidc:example:owner", "registration-key-owner"))
    assert authority.snapshot_path.read_bytes() == old_raw
    assert authority.snapshot_dirty is True
    assert list(tmp_path.glob(".current-snapshot-*.tmp")) == []

    monkeypatch.setattr(registry.os, "replace", real_replace)
    restarted = registry.DeveloperEnvironmentRegistry(authority.database)
    assert len(restarted.list_environments()) == 1
    assert restarted.snapshot_path.read_bytes() == restarted.snapshot_bytes()
    assert restarted.verify_snapshot(restarted.snapshot_path.read_bytes())["generation"] == 1


def test_resource_records_never_contain_caller_selected_root_or_runtime_names(
    tmp_path: Path,
) -> None:
    authority = _new_registry(tmp_path)
    environment = authority.register(
        _register(
            "oidc:example:../../root",
            "registration-key-path-safe",
            display_name="../../root",
        ),
    )
    encoded = json.dumps(asdict(environment), sort_keys=True)

    assert "../../root" not in environment.env_id
    assert "../../root" not in environment.runtime_id
    assert "../../root" not in environment.service_user
    assert "../../root" not in environment.compose_project
    assert environment.principal_id in encoded
    assert environment.display_name in encoded


def test_registry_admin_parser_has_no_developer_actions(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = registry._admin_parser()
    assert parser.parse_args(["init"]).command == "init"
    assert parser.parse_args(["import-seed"]).command == "import-seed"
    assert parser.parse_args(["export-snapshot"]).command == "export-snapshot"
    assert parser.parse_args(["status"]).command == "status"
    with pytest.raises(SystemExit):
        parser.parse_args(["register"])

    monkeypatch.setattr(registry.os, "getuid", lambda: 1000)
    monkeypatch.setattr(registry.os, "geteuid", lambda: 1000)
    assert registry.main(["status"]) == 1
    assert "requires root" in capsys.readouterr().err
