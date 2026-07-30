from __future__ import annotations

import array
import base64
import hashlib
import json
import os
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_authority as authority
from scripts.ops import developer_environment_registry as registry


def _canonical(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"


def _peer(
    *,
    uid: int | None = None,
    groups: frozenset[str] = frozenset({"loom-developers"}),
) -> authority.PeerIdentity:
    effective_uid = os.getuid() if uid is None else uid
    return authority.PeerIdentity(
        pid=1234,
        uid=effective_uid,
        gid=effective_uid,
        username=f"developer-{effective_uid}",
        principal_id=f"unix-uid:{effective_uid}",
        groups=groups,
    )


def _call(
    registry_authority: registry.DeveloperEnvironmentRegistry,
    payload: dict[str, Any],
    tmp_path: Path,
    *,
    peer: authority.PeerIdentity | None = None,
    descriptors: list[int] | None = None,
    deployer: Any | None = None,
) -> dict[str, Any]:
    try:
        server, client = socket.socketpair(
            socket.AF_UNIX,
            socket.SOCK_SEQPACKET,
        )
    except OSError:
        # Darwin cannot create AF_UNIX SOCK_SEQPACKET pairs. SOCK_DGRAM keeps
        # the one-message and SCM_RIGHTS semantics exercised by this unit test;
        # the production activation path separately pins SOCK_SEQPACKET.
        server, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    ancillary: list[tuple[int, int, bytes]] = []
    if descriptors is not None:
        packed = array.array("i", descriptors)
        ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, packed.tobytes())]
    selected_peer = peer or _peer()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            handled = pool.submit(
                authority.handle_connection,
                server,
                registry_authority,
                peer_resolver=lambda _connection: selected_peer,
                stage_root=tmp_path / "imports",
                deployer=deployer,
            )
            client.sendmsg([_canonical(payload)], ancillary)
            raw = client.recv(4 * 1024 * 1024)
            handled.result(timeout=10)
    finally:
        server.close()
        client.close()
    response = json.loads(raw)
    assert raw == _canonical(response)
    return response


def _register_request(key: str = "registration-key-0001") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": authority.REGISTER_KIND,
        "idempotency_key": key,
        "display_name": "Developer",
    }


def _git(
    arguments: list[str],
    *,
    cwd: Path,
) -> str:
    completed = subprocess.run(
        ["/usr/bin/git", *arguments],
        cwd=cwd,
        check=True,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _bundle(
    tmp_path: Path,
    *,
    content: str = "candidate\n",
) -> tuple[Path, str, str, str, int]:
    source = tmp_path / "source"
    source.mkdir()
    _git(["init"], cwd=source)
    _git(["config", "user.name", "Authority Test"], cwd=source)
    _git(["config", "user.email", "authority@example.invalid"], cwd=source)
    (source / "README.md").write_text(content, encoding="utf-8")
    _git(["add", "README.md"], cwd=source)
    _git(["commit", "-m", "candidate"], cwd=source)
    bundle = tmp_path / "candidate.bundle"
    _git(["bundle", "create", str(bundle), "HEAD"], cwd=source)
    candidate_sha = _git(["rev-parse", "HEAD"], cwd=source)
    candidate_tree = _git(["rev-parse", "HEAD^{tree}"], cwd=source)
    content = bundle.read_bytes()
    return (
        bundle,
        candidate_sha,
        candidate_tree,
        hashlib.sha256(content).hexdigest(),
        len(content),
    )


def _import_request(
    environment: dict[str, Any],
    bundle: tuple[Path, str, str, str, int],
    *,
    key: str = "candidate-import-key-0001",
) -> dict[str, Any]:
    _path, candidate_sha, candidate_tree, digest, size = bundle
    return {
        "schema_version": 1,
        "kind": authority.IMPORT_KIND,
        "idempotency_key": key,
        "env_id": environment["env_id"],
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "bundle_sha256": digest,
        "bundle_size": size,
        "image_digests": {
            "amd64": "sha256:" + "a" * 64,
            "arm64": "sha256:" + "b" * 64,
        },
    }


def _new_registry(tmp_path: Path) -> registry.DeveloperEnvironmentRegistry:
    return registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")


class _FakeDeployer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def converge(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("converge", kwargs))
        return {
            "status": "committed",
            "operation": kwargs["operation"],
            "env_id": kwargs["env_id"],
            "candidate_id": kwargs["candidate_id"],
        }

    def check(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("check", kwargs))
        return {"status": "verified", "env_id": kwargs["env_id"]}

    def rollback(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("rollback", kwargs))
        return {"status": "committed", "env_id": kwargs["env_id"]}

    def retire(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("retire", kwargs))
        return {"status": "retired", "env_id": kwargs["env_id"]}

    def revive(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("revive", kwargs))
        return {"status": "ready", "env_id": kwargs["env_id"]}


class _FakeFleetRegistry:
    system_mode = True
    policy = registry.AllocationPolicy()

    def __init__(self) -> None:
        self.published: bytes | None = None
        self._snapshot = {
            "schema_version": 1,
            "kind": registry.SNAPSHOT_KIND,
            "generation": 17,
            "payload_sha256": "f" * 64,
            "environments": [
                {
                    "env_id": "denv-legacy-0000000000000000",
                    "principal_id": "unix-uid:501",
                    "runtime_id": "qianyi",
                    "state": "active",
                    "resource_generation": 1,
                    "current_candidate_id": "cand-" + "a" * 40,
                },
            ],
            "candidates": [
                {
                    "candidate_id": "cand-" + "a" * 40,
                    "env_id": "denv-legacy-0000000000000000",
                    "principal_id": "unix-uid:501",
                    "candidate_sha": "a" * 40,
                    "candidate_tree": "b" * 40,
                },
            ],
            "deployments": [
                {
                    "env_id": "denv-legacy-0000000000000000",
                    "candidate_id": "cand-" + "a" * 40,
                    "phase": "committed",
                },
            ],
        }

    def snapshot(self) -> dict[str, Any]:
        return self._snapshot

    def publish_fleet_identity_inventory(
        self,
        node_results: list[dict[str, Any]],
        *,
        registry_generation: int,
        registry_payload_sha256: str,
    ) -> None:
        self.published = registry.build_fleet_identity_inventory(
            node_results,
            registry_generation=registry_generation,
            registry_payload_sha256=registry_payload_sha256,
            policy=self.policy,
        )


def test_installed_node_policy_readback_is_canonical_owner_bound_and_nofollow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = tmp_path / "node-authority.json"
    payload = {
        "schema_version": 1,
        "source_sha": "a" * 40,
        "source_tree": "b" * 40,
        "node": "oldlab-1",
        "asset_sha256": {"sealed": "c" * 64},
    }
    policy.write_bytes(authority._canonical(payload))
    policy.chmod(0o600)
    monkeypatch.setattr(authority, "NODE_AUTHORITY_POLICY", policy)

    assert authority._read_installed_node_policy() == payload

    target = tmp_path / "replacement.json"
    target.write_bytes(authority._canonical(payload))
    target.chmod(0o600)
    policy.unlink()
    policy.symlink_to(target)
    with pytest.raises(authority.AuthorityError, match=r"unavailable|unsafe"):
        authority._read_installed_node_policy()


def test_root_authority_collects_all_nodes_through_fixed_sudo_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fleet = _FakeFleetRegistry()
    calls: list[tuple[list[str], dict[str, Any], dict[str, Any]]] = []
    monkeypatch.setattr(
        authority,
        "_read_installed_node_policy",
        lambda: {
            "schema_version": 1,
            "source_sha": "a" * 40,
            "source_tree": "b" * 40,
            "node": "oldlab-1",
            "asset_sha256": {"sealed": "c" * 64},
        },
    )

    def run(
        argv: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[bytes]:
        outer = json.loads(kwargs["input"])
        inner = json.loads(base64.b64decode(outer["payload_base64"], validate=True))
        calls.append((argv, outer, inner))
        node = str(outer["node"])
        result = {
            "schema_version": 1,
            "kind": registry.NODE_IDENTITY_INVENTORY_KIND,
            "node": node,
            "domain": "oldlab" if node.startswith("oldlab-") else "gb10",
            "uid_start": 32_000,
            "uid_end": 60_000,
            "occupied_ids": [32_000] if node == "trt-gb10-7" else [],
            "identity_inventory_sha256": hashlib.sha256(node.encode("ascii")).hexdigest(),
            "checked_at": registry._timestamp(),
        }
        response = {
            "schema_version": 1,
            "request_id": outer["request_id"],
            "status": "succeeded",
            "result": result,
        }
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=authority._canonical(response),
            stderr=b"",
        )

    authority._refresh_fleet_identity_inventory(fleet, run=run)

    assert len(calls) == len(registry.FLEET_NODES) == 20
    assert {outer["node"] for _argv, outer, _inner in calls} == set(
        registry.FLEET_NODES,
    )
    assert all(
        argv
        == [
            str(authority.NODE_TRANSPORT),
            "invoke",
            "--node",
            outer["node"],
            "--verb",
            "check",
        ]
        for argv, outer, _inner in calls
    )
    assert all(
        inner
        == {
            "schema_version": 1,
            "kind": "loom.developer-environment.identity-inventory-request",
            "uid_start": 32_000,
            "uid_end": 60_000,
            "registry_generation": 17,
            "registry_payload_sha256": "f" * 64,
        }
        for _argv, _outer, inner in calls
    )
    assert fleet.published is not None
    published = registry.DeveloperEnvironmentRegistry.verify_fleet_identity_inventory(
        fleet.published,
        policy=fleet.policy,
    )
    assert published["nodes"][11]["node"] == "trt-gb10-7"
    assert published["nodes"][11]["occupied_ids"] == [32_000]


def test_seed_only_registry_bootstraps_inventory_before_first_dynamic_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fleet = _FakeFleetRegistry()
    fleet._snapshot["environments"][0]["state"] = "ready"
    fleet._snapshot["environments"][0]["current_candidate_id"] = None
    fleet._snapshot["candidates"] = []
    fleet._snapshot["deployments"] = []
    monkeypatch.setattr(
        authority,
        "_read_installed_node_policy",
        lambda: {
            "schema_version": 1,
            "source_sha": "a" * 40,
            "source_tree": "b" * 40,
            "node": "oldlab-1",
            "asset_sha256": {"sealed": "c" * 64},
        },
    )
    scopes: list[str] = []

    def run(
        argv: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[bytes]:
        outer = json.loads(kwargs["input"])
        scopes.append(str(outer["sandbox"]))
        node = str(outer["node"])
        response = {
            "schema_version": 1,
            "request_id": outer["request_id"],
            "status": "succeeded",
            "result": {
                "schema_version": 1,
                "kind": registry.NODE_IDENTITY_INVENTORY_KIND,
                "node": node,
                "domain": "oldlab" if node.startswith("oldlab-") else "gb10",
                "uid_start": 32_000,
                "uid_end": 60_000,
                "occupied_ids": [32_000],
                "identity_inventory_sha256": hashlib.sha256(node.encode("ascii")).hexdigest(),
                "checked_at": registry._timestamp(),
            },
        }
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=authority._canonical(response),
            stderr=b"",
        )

    authority._refresh_fleet_identity_inventory(fleet, run=run)

    assert scopes == [authority.FLEET_BOOTSTRAP_SCOPE] * len(registry.FLEET_NODES)
    assert fleet.published is not None
    inventory = tmp_path / "fleet-identity-inventory.json"
    inventory.write_bytes(fleet.published)
    inventory.chmod(0o600)
    fresh = registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry.sqlite3",
        fleet_identity_inventory_path=inventory,
    )
    created = fresh.register(
        {
            "schema_version": 1,
            "kind": registry.REGISTER_KIND,
            "principal_id": "oidc:example:first-dynamic",
            "idempotency_key": "first-dynamic-create-key",
            "display_name": "First Dynamic",
        },
    )
    assert created.uid == created.gid == 32_001


def test_fleet_inventory_source_is_installed_authority_not_active_candidates() -> None:
    fleet = _FakeFleetRegistry()
    fleet._snapshot["environments"].append(
        {
            **fleet._snapshot["environments"][0],
            "env_id": "denv-dynamic-second-00000000",
            "principal_id": "oidc:example:second",
            "runtime_id": "e-second",
            "current_candidate_id": "cand-" + "c" * 40,
        }
    )
    fleet._snapshot["candidates"].append(
        {
            **fleet._snapshot["candidates"][0],
            "candidate_id": "cand-" + "c" * 40,
            "env_id": "denv-dynamic-second-00000000",
            "principal_id": "oidc:example:second",
            "candidate_sha": "c" * 40,
            "candidate_tree": "d" * 40,
        }
    )
    fleet._snapshot["deployments"].append(
        {
            "env_id": "denv-dynamic-second-00000000",
            "candidate_id": "cand-" + "c" * 40,
            "phase": "committed",
        }
    )

    assert authority._fleet_inventory_source(
        fleet.snapshot(),
        {
            "source_sha": "e" * 40,
            "source_tree": "f" * 40,
        },
    ) == (
        authority.FLEET_BOOTSTRAP_SCOPE,
        "e" * 40,
        "f" * 40,
    )


def test_register_status_and_snapshot_derive_identity_from_peer(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    registered = _call(registry_authority, _register_request(), tmp_path)
    environment = registered["result"]

    assert registered["status"] == "succeeded"
    assert environment["principal_id"] == f"unix-uid:{os.getuid()}"
    assert "principal_id" not in _register_request()

    status = _call(
        registry_authority,
        {
            "schema_version": 1,
            "kind": authority.STATUS_KIND,
            "env_id": environment["env_id"],
        },
        tmp_path,
    )
    snapshot = _call(
        registry_authority,
        {"schema_version": 1, "kind": authority.SNAPSHOT_KIND},
        tmp_path,
    )

    assert status["result"] == environment
    assert snapshot["result"]["environments"] == [environment]
    assert snapshot["result"]["candidates"] == []
    assert snapshot["result"]["deployments"] == []


def test_self_service_create_check_rollback_and_destroy_are_peer_scoped(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    bundle = _bundle(tmp_path)
    request = _import_request(
        {"env_id": "not-sent"},
        bundle,
        key="self-service-key-0001",
    )
    request.pop("env_id")
    request["kind"] = authority.CREATE_KIND
    request["display_name"] = "Developer"
    deployer = _FakeDeployer()
    descriptor = os.open(bundle[0], os.O_RDONLY)
    try:
        created = _call(
            registry_authority,
            request,
            tmp_path,
            descriptors=[descriptor],
            deployer=deployer,
        )
    finally:
        os.close(descriptor)

    assert created["status"] == "succeeded"
    environment = registry_authority.list_environments(
        principal_id=f"unix-uid:{os.getuid()}",
    )[0]
    assert created["result"]["env_id"] == environment.env_id
    assert deployer.calls[0][0] == "converge"
    assert deployer.calls[0][1]["principal_id"] == environment.principal_id
    assert deployer.calls[0][1]["operation"] == "create"
    assert "env_id" not in request
    assert "principal_id" not in request

    for kind, expected_call in (
        (authority.CHECK_KIND, "check"),
        (authority.ROLLBACK_KIND, "rollback"),
        (authority.DESTROY_KIND, "retire"),
    ):
        payload: dict[str, Any] = {"schema_version": 1, "kind": kind}
        if kind != authority.CHECK_KIND:
            payload["idempotency_key"] = f"{expected_call}-key-0000001"
        response = _call(
            registry_authority,
            payload,
            tmp_path,
            deployer=deployer,
        )
        assert response["status"] == "succeeded"
        assert response["result"]["env_id"] == environment.env_id
        assert deployer.calls[-1][0] == expected_call
        assert deployer.calls[-1][1]["principal_id"] == environment.principal_id
        if kind == authority.DESTROY_KIND:
            assert deployer.calls[-1][1]["idempotency_key"].startswith("self-")


def test_retired_create_requires_a_new_key_before_reviving_same_identity(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    bundle = _bundle(tmp_path)
    first = _import_request({"env_id": "not-sent"}, bundle, key="create-old-key-0001")
    first.pop("env_id")
    first["kind"] = authority.CREATE_KIND
    first["display_name"] = "Developer"
    initial_deployer = _FakeDeployer()
    descriptor = os.open(bundle[0], os.O_RDONLY)
    try:
        created = _call(
            registry_authority,
            first,
            tmp_path,
            descriptors=[descriptor],
            deployer=initial_deployer,
        )
    finally:
        os.close(descriptor)
    assert created["status"] == "succeeded"
    environment = registry_authority.list_environments(
        principal_id=f"unix-uid:{os.getuid()}",
    )[0]
    registry_authority.begin_retirement(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )
    registry_authority.retire_environment(
        environment.env_id,
        principal_id=environment.principal_id,
        expected_resource_generation=environment.resource_generation,
    )

    descriptor = os.open(bundle[0], os.O_RDONLY)
    try:
        replay = _call(
            registry_authority,
            first,
            tmp_path,
            descriptors=[descriptor],
            deployer=_FakeDeployer(),
        )
    finally:
        os.close(descriptor)
    assert replay["status"] == "failed"
    assert (
        registry_authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        ).state
        == "retired"
    )
    before_old_content_retry = registry_authority.snapshot()
    same_old_content = {
        **first,
        "idempotency_key": "create-new-key-same-old-content",
    }
    descriptor = os.open(bundle[0], os.O_RDONLY)
    try:
        old_content_retry = _call(
            registry_authority,
            same_old_content,
            tmp_path,
            descriptors=[descriptor],
            deployer=_FakeDeployer(),
        )
    finally:
        os.close(descriptor)
    after_old_content_retry = registry_authority.snapshot()
    assert old_content_retry["status"] == "failed"
    assert after_old_content_retry == before_old_content_retry
    unchanged = registry_authority.lookup(
        environment.env_id,
        principal_id=environment.principal_id,
    )
    assert unchanged.state == "retired"
    assert unchanged.resource_generation == environment.resource_generation + 1
    assert unchanged.lifecycle_epoch == environment.lifecycle_epoch

    class RevivingDeployer(_FakeDeployer):
        def revive(self, **kwargs: Any) -> dict[str, Any]:
            current = registry_authority.lookup(
                kwargs["env_id"],
                principal_id=kwargs["principal_id"],
            )
            registry_authority.revive_environment(
                kwargs["env_id"],
                principal_id=kwargs["principal_id"],
                expected_resource_generation=current.resource_generation,
            )
            return super().revive(**kwargs)

    second_root = tmp_path / "second"
    second_root.mkdir()
    second_bundle = _bundle(second_root, content="genuinely new candidate\n")
    second = _import_request(
        {"env_id": "not-sent"},
        second_bundle,
        key="create-new-key-0002",
    )
    second.pop("env_id")
    second["kind"] = authority.CREATE_KIND
    second["display_name"] = "Developer"
    selected = RevivingDeployer()
    descriptor = os.open(second_bundle[0], os.O_RDONLY)
    try:
        revived = _call(
            registry_authority,
            second,
            tmp_path,
            descriptors=[descriptor],
            deployer=selected,
        )
    finally:
        os.close(descriptor)
    current = registry_authority.lookup(
        environment.env_id,
        principal_id=environment.principal_id,
    )
    assert revived["status"] == "succeeded"
    assert current.env_id == environment.env_id
    assert current.uid == environment.uid
    assert current.gid == environment.gid
    assert [call[0] for call in selected.calls] == ["revive", "converge"]


def test_unauthorized_group_and_unknown_identity_field_are_rejected(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    unauthorized = _call(
        registry_authority,
        _register_request(),
        tmp_path,
        peer=_peer(groups=frozenset({"other"})),
    )
    extra = {
        **_register_request("registration-key-extra"),
        "uid": os.getuid(),
    }
    unknown_field = _call(registry_authority, extra, tmp_path)

    assert unauthorized["status"] == "failed"
    assert unknown_field["status"] == "failed"
    assert registry_authority.list_environments() == ()


def test_environment_status_is_owner_scoped(tmp_path: Path) -> None:
    registry_authority = _new_registry(tmp_path)
    owner = _peer(uid=os.getuid())
    other = _peer(uid=os.getuid() + 1000)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
        peer=owner,
    )["result"]

    response = _call(
        registry_authority,
        {
            "schema_version": 1,
            "kind": authority.STATUS_KIND,
            "env_id": environment["env_id"],
        },
        tmp_path,
        peer=other,
    )

    assert response["status"] == "failed"
    assert response["error"] == "request failed safely"


@pytest.mark.parametrize("descriptor_count", [0, 2])
def test_candidate_import_requires_exactly_one_descriptor(
    tmp_path: Path,
    descriptor_count: int,
) -> None:
    registry_authority = _new_registry(tmp_path)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
    )["result"]
    candidate_bundle = _bundle(tmp_path)
    descriptor = os.open(candidate_bundle[0], os.O_RDONLY)
    try:
        response = _call(
            registry_authority,
            _import_request(environment, candidate_bundle),
            tmp_path,
            descriptors=[descriptor] * descriptor_count,
        )
    finally:
        os.close(descriptor)

    assert response["status"] == "failed"
    assert registry_authority.snapshot()["candidates"] == []


def test_candidate_bundle_owner_is_peer_bound(tmp_path: Path) -> None:
    registry_authority = _new_registry(tmp_path)
    synthetic_peer = _peer(uid=os.getuid() + 1000)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
        peer=synthetic_peer,
    )["result"]
    candidate_bundle = _bundle(tmp_path)
    descriptor = os.open(candidate_bundle[0], os.O_RDONLY)
    try:
        response = _call(
            registry_authority,
            _import_request(environment, candidate_bundle),
            tmp_path,
            peer=synthetic_peer,
            descriptors=[descriptor],
        )
    finally:
        os.close(descriptor)

    assert response["status"] == "failed"


@pytest.mark.parametrize("mismatch", ["size", "digest", "head", "tree"])
def test_candidate_bundle_content_bindings_are_verified(
    tmp_path: Path,
    mismatch: str,
) -> None:
    registry_authority = _new_registry(tmp_path)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
    )["result"]
    candidate_bundle = _bundle(tmp_path)
    request = _import_request(environment, candidate_bundle)
    if mismatch == "size":
        request["bundle_size"] += 1
    elif mismatch == "digest":
        request["bundle_sha256"] = "0" * 64
    elif mismatch == "head":
        request["candidate_sha"] = "0" * 40
    else:
        request["candidate_tree"] = "0" * 40
    descriptor = os.open(candidate_bundle[0], os.O_RDONLY)
    try:
        response = _call(
            registry_authority,
            request,
            tmp_path,
            descriptors=[descriptor],
        )
    finally:
        os.close(descriptor)

    assert response["status"] == "failed"
    assert registry_authority.snapshot()["candidates"] == []


def test_candidate_import_and_begin_deploy_are_durable_idempotent_transactions(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
    )["result"]
    candidate_bundle = _bundle(tmp_path)
    request = _import_request(environment, candidate_bundle)

    imports = []
    for _attempt in range(2):
        descriptor = os.open(candidate_bundle[0], os.O_RDONLY)
        try:
            imports.append(
                _call(
                    registry_authority,
                    request,
                    tmp_path,
                    descriptors=[descriptor],
                )
            )
        finally:
            os.close(descriptor)
    assert imports[0]["status"] == "succeeded"
    assert imports[1]["result"] == imports[0]["result"]
    persisted_bundle = Path(imports[0]["result"]["bundle_path"])
    assert persisted_bundle.is_file()
    assert persisted_bundle.read_bytes() == candidate_bundle[0].read_bytes()
    assert persisted_bundle.stat().st_mode & 0o777 == 0o600

    # A fresh authority instance can start the durable transaction from the
    # root-owned candidate store; no process-local staged bundle is required.
    restarted = registry.DeveloperEnvironmentRegistry(
        registry_authority.database,
    )

    deploy_request = {
        "schema_version": 1,
        "kind": authority.BEGIN_DEPLOY_KIND,
        "idempotency_key": "deployment-key-0001",
        "env_id": environment["env_id"],
        "candidate_id": imports[0]["result"]["candidate_id"],
        "expected_resource_generation": environment["resource_generation"],
    }
    first = _call(restarted, deploy_request, tmp_path)
    replay = _call(restarted, deploy_request, tmp_path)

    assert replay == first
    assert first["result"]["phase"] == "requested"
    assert first["result"]["deployed"] is False
    assert first["result"]["mutation_started"] is False
    assert len(restarted.snapshot()["deployments"]) == 1


def test_candidate_and_environment_cannot_cross_principals(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    owner = _peer(uid=os.getuid())
    other = _peer(uid=os.getuid() + 1000)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
        peer=owner,
    )["result"]
    candidate_bundle = _bundle(tmp_path)
    descriptor = os.open(candidate_bundle[0], os.O_RDONLY)
    try:
        response = _call(
            registry_authority,
            _import_request(environment, candidate_bundle),
            tmp_path,
            peer=other,
            descriptors=[descriptor],
        )
    finally:
        os.close(descriptor)

    assert response["status"] == "failed"
    assert registry_authority.snapshot()["candidates"] == []


def test_begin_deploy_rejects_tampered_persisted_candidate(
    tmp_path: Path,
) -> None:
    registry_authority = _new_registry(tmp_path)
    environment = _call(
        registry_authority,
        _register_request(),
        tmp_path,
    )["result"]
    candidate_bundle = _bundle(tmp_path)
    descriptor = os.open(candidate_bundle[0], os.O_RDONLY)
    try:
        imported = _call(
            registry_authority,
            _import_request(environment, candidate_bundle),
            tmp_path,
            descriptors=[descriptor],
        )["result"]
    finally:
        os.close(descriptor)
    Path(imported["bundle_path"]).write_bytes(b"tampered")

    response = _call(
        registry_authority,
        {
            "schema_version": 1,
            "kind": authority.BEGIN_DEPLOY_KIND,
            "idempotency_key": "deployment-key-tampered",
            "env_id": environment["env_id"],
            "candidate_id": imported["candidate_id"],
            "expected_resource_generation": environment["resource_generation"],
        },
        tmp_path,
    )

    assert response["status"] == "failed"
    assert registry_authority.snapshot()["deployments"] == []
