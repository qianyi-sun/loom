from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import pwd
import stat
import tarfile
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_host as host
from scripts.ops import developer_sandbox_node_authority as authority

SHA = "a" * 40
TREE = "b" * 40


def _staging_infrastructure_receipt(
    *,
    generation: int,
    convergence_id: str,
    now: datetime | None = None,
) -> dict[str, object]:
    observed = now or datetime.now(UTC)
    requested_at = authority._timestamp(observed - timedelta(seconds=30))
    operation_specs = [
        ("staging-shared-source-bootstrap", "trt-gb10-2"),
        ("staging-slurm-accounting-converge", "trt-gb10-1"),
        *[
            ("staging-allocation-bootstrap", node)
            for node in authority.STAGING_INFRASTRUCTURE_NODES
        ],
    ]
    operations: list[dict[str, object]] = []
    for index, (action, node) in enumerate(operation_specs):
        envelope = json.loads(
            authority._staging_infrastructure_operation_envelope(
                action=action,
                node=node,
                candidate_sha=SHA,
                candidate_tree=TREE,
                convergence_id=convergence_id,
                requested_at=requested_at,
            ),
        )
        inner = json.loads(
            base64.b64decode(envelope["payload_base64"], validate=True),
        )
        operations.append(
            {
                "schema_version": 1,
                "request_id": envelope["request_id"],
                "action": action,
                "node": node,
                "domain": "gb10",
                "sandbox": "staging",
                "candidate_sha": SHA,
                "candidate_tree": TREE,
                "payload_sha256": envelope["payload_sha256"],
                "result_sha256": hashlib.sha256(f"{action}:{node}".encode()).hexdigest(),
                "inner_receipt": (
                    f"staging-accounting/v1/{inner['request_id']}"
                    if action == "staging-slurm-accounting-converge"
                    else (
                        f"staging-shared-source-bootstrap/v1/{'a' * 64}"
                        if action == "staging-shared-source-bootstrap"
                        else (
                            "staging-allocation-bootstrap/v1/"
                            f"{int(node.rsplit('-', 1)[1]):08x}-0000-4000-8000-"
                            f"{int(node.rsplit('-', 1)[1]):012x}/"
                            f"{hashlib.sha256(f'mount:{node}'.encode()).hexdigest()}"
                        )
                    )
                ),
                "completed_at": authority._timestamp(
                    observed - timedelta(seconds=20 - index),
                ),
                "status": "succeeded",
            },
        )
    converge_request = {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-converge-request",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "convergence_id": convergence_id,
        "requested_at": requested_at,
    }
    return {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-receipt",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "generation": generation,
        "convergence_id": convergence_id,
        "requested_at": requested_at,
        "request_sha256": hashlib.sha256(
            authority._canonical(converge_request),
        ).hexdigest(),
        "source_controller": "oldlab-2",
        "source_controller_host": "trt-eai-oldlab-2",
        "created_at": authority._timestamp(observed - timedelta(seconds=1)),
        "expires_at": authority._timestamp(observed + timedelta(seconds=300)),
        "source_bootstrap": operations[0],
        "accounting": operations[1],
        "node_bootstraps": operations[2:],
        "mount_contract": authority._staging_infrastructure_mount_contract(),
        "result": "pass",
    }


def test_staging_infrastructure_receipt_binds_request_and_inner_receipts() -> None:
    receipt = _staging_infrastructure_receipt(
        generation=1,
        convergence_id="d" * 64,
    )
    authority._validate_staging_infrastructure_receipt(
        receipt,
        candidate_sha=SHA,
        candidate_tree=TREE,
    )
    tampered = json.loads(json.dumps(receipt))
    tampered["node_bootstraps"][0]["inner_receipt"] = "staging-accounting/v1/" + "f" * 64
    with pytest.raises(authority.NodeAuthorityError, match="operation receipt"):
        authority._validate_staging_infrastructure_receipt(
            tampered,
            candidate_sha=SHA,
            candidate_tree=TREE,
        )
    tampered = json.loads(json.dumps(receipt))
    tampered["request_sha256"] = "f" * 64
    with pytest.raises(authority.NodeAuthorityError, match="binding"):
        authority._validate_staging_infrastructure_receipt(
            tampered,
            candidate_sha=SHA,
            candidate_tree=TREE,
        )


def test_staging_infrastructure_install_rolls_forward_exact_crash_but_rejects_regression(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = tmp_path / "staging-infrastructure"
    generations = state / "generations"
    generations.mkdir(parents=True)
    lock = state / "install.lock"
    lock.write_bytes(b"")
    monkeypatch.setattr(authority, "STAGING_INFRASTRUCTURE_RECEIPT_ROOT", state)
    monkeypatch.setattr(authority, "STAGING_INFRASTRUCTURE_INSTALL_GENERATIONS", generations)
    monkeypatch.setattr(authority, "STAGING_INFRASTRUCTURE_INSTALL_LOCK", lock)
    monkeypatch.setattr(
        authority,
        "STAGING_INFRASTRUCTURE_INSTALL_HIGH_WATER",
        state / "high-water.json",
    )
    monkeypatch.setattr(
        authority,
        "STAGING_INFRASTRUCTURE_INSTALL_JOURNAL",
        state / "install-journal.json",
    )
    monkeypatch.setattr(
        authority,
        "_open_named_lock",
        lambda *_args, **_kwargs: os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, **_kwargs: path.read_bytes(),
    )

    def replace(path: Path, payload: bytes, *_args: object, **_kwargs: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)

    def install(path: Path, payload: bytes, *_args: object, **_kwargs: object) -> bool:
        if path.exists():
            assert path.read_bytes() == payload
            return False
        replace(path, payload)
        return True

    monkeypatch.setattr(authority, "_atomic_replace", replace)
    monkeypatch.setattr(authority, "_atomic_install", install)
    first = _staging_infrastructure_receipt(
        generation=1,
        convergence_id="c" * 64,
        now=datetime.now(UTC) - timedelta(seconds=40),
    )
    first_raw = authority._canonical(first)
    (state / "high-water.json").write_bytes(
        authority._canonical(
            {
                "schema_version": 1,
                "generation": 1,
                "convergence_id": first["convergence_id"],
                "requested_at": first["requested_at"],
                "request_sha256": hashlib.sha256(first_raw).hexdigest(),
            },
        ),
    )
    (generations / "1.json").write_bytes(first_raw)
    second = _staging_infrastructure_receipt(
        generation=2,
        convergence_id="d" * 64,
    )
    second_raw = authority._canonical(second)
    (generations / "2.json").write_bytes(second_raw)
    request = authority.Request(
        payload={
            "action": "staging-infrastructure-install",
            "candidate_sha": SHA,
            "candidate_tree": TREE,
        },
        payload_bytes=second_raw,
    )
    result, inner = authority._staging_infrastructure_install(request)
    assert result["generation"] == 2
    assert inner == "staging-infrastructure-install/v1/2"
    assert json.loads((state / "high-water.json").read_bytes())["generation"] == 2

    regressed = authority.Request(
        payload=request.payload,
        payload_bytes=first_raw,
    )
    with pytest.raises(authority.NodeAuthorityError, match="regressed"):
        authority._staging_infrastructure_install(regressed)


def _slurm_snapshot_manifest(
    *,
    present_path: str | None = None,
    present_bytes: bytes = b"",
) -> bytes:
    rows: list[dict[str, object]] = []
    for relative in authority.SLURM_SNAPSHOT_RELATIVE_PATHS:
        if relative == present_path:
            rows.append(
                {
                    "path": relative,
                    "present": True,
                    "mode": 0o644,
                    "uid": 0,
                    "gid": 0,
                    "nlink": 1,
                    "size": len(present_bytes),
                    "sha256": hashlib.sha256(present_bytes).hexdigest(),
                },
            )
        else:
            rows.append(
                {
                    "path": relative,
                    "present": False,
                    "mode": None,
                    "uid": None,
                    "gid": None,
                    "nlink": None,
                    "size": None,
                    "sha256": None,
                },
            )
    return (json.dumps({"schema_version": 1, "files": rows}, sort_keys=True) + "\n").encode(
        "ascii",
    )


def _slurm_policy_journal(
    snapshot: Path,
    *,
    node: str,
    operation: str = "apply",
    accounting: bool = False,
    rollback_target: Path | None = None,
) -> bytes:
    domain = "oldlab" if node.startswith("oldlab-") else "gb10"
    action = (
        "slurm-rollback"
        if operation == "rollback"
        else (
            "slurm-controller-converge"
            if node == authority.SLURM_CONTROLLER[domain]
            else "slurm-node-converge"
        )
    )
    request = authority._parse_request(
        _request(
            action=action,
            node=node,
            domain=domain,
            prior_request_id="c" * 64 if operation == "rollback" else None,
        ),
        verb="transact",
        policy=_policy(node),
    )
    candidate_set = json.loads(request.payload_bytes)
    payload: dict[str, object] = {
        "schema_version": 1,
        "operation": operation,
        "cluster": authority.SLURM_CLUSTER[domain],
        "host": authority.NODE_HOSTNAMES[node],
        "slurm_node": node,
        "candidate_sha": SHA,
        "candidate_set_sha256": candidate_set["candidate_set_sha256"],
        "candidate_bindings": candidate_set["candidate_bindings"],
        "transaction_id": request.request_id,
        "candidate_set_generation": candidate_set["generation"],
        "candidate_set_convergence_id": candidate_set["convergence_id"],
        "candidate_set_payload_sha256": request.payload["payload_sha256"],
        "snapshot": str(snapshot),
        "accounting_snapshot": str(snapshot / "accounting-cas.json") if accounting else None,
        "restart": True,
        "apply_accounting": accounting,
        "phase": "committed",
        "created_at": "2026-07-28T01:02:03+00:00",
        "updated_at": "2026-07-28T01:03:04+00:00",
    }
    if operation == "rollback":
        payload["rollback_target"] = str(rollback_target)
    return authority._canonical(payload)


def _slurm_archive_identity(
    manifest: bytes,
    *,
    accounting: bytes | None = None,
) -> str:
    return hashlib.sha256(
        authority._canonical(
            {
                "schema_version": 1,
                "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
                "accounting_sha256": (
                    hashlib.sha256(accounting).hexdigest() if accounting is not None else None
                ),
            },
        ),
    ).hexdigest()


def _policy(node: str = "oldlab-1") -> authority.AuthorityPolicy:
    return authority.AuthorityPolicy(
        source_sha=SHA,
        source_tree=TREE,
        node=node,
        asset_sha256={str(path): "c" * 64 for path in authority.SOURCE_ASSETS},
    )


def _request(
    *,
    action: str = "host-converge",
    node: str = "oldlab-1",
    domain: str = "oldlab",
    sandbox: str = "qianyi",
    payload_kind: str = "none",
    payload_bytes: bytes = b"",
    prior_request_id: str | None = None,
) -> bytes:
    if action in {
        "slurm-node-converge",
        "slurm-controller-converge",
        "slurm-rollback",
        "slurm-check",
    } and payload_kind == "none":
        bindings = {
            f"loom-dev-{sandbox_name}": {
                "sandbox": sandbox_name,
                "service_user": f"loom-sandbox-{sandbox_name}",
                "candidate_sha": candidate_sha,
                "candidate_tree": TREE,
            }
            for sandbox_name, candidate_sha in (
                ("qianyi", SHA),
                ("hongjian", "c" * 40),
                ("devansh", "d" * 40),
            )
        }
        payload_kind = "slurm-candidate-set-json"
        payload_bytes = authority._canonical(
            {
                "schema_version": 2,
                "kind": "loom.developer-sandbox.slurm-candidate-set",
                "candidate_set_sha256": hashlib.sha256(
                    json.dumps(
                        bindings,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("ascii"),
                ).hexdigest(),
                "candidate_bindings": bindings,
                "generation": 1,
                "convergence_id": "e" * 64,
            },
        )
    if action in {
        "export-runtime-proof-artifact",
        "staging-pressure-reclaim-observe",
    }:
        body: dict[str, object] = {
            "schema_version": authority.SCHEMA_VERSION,
            "action": action,
            "node": node,
            "domain": domain,
            "sandbox": sandbox,
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "payload_kind": payload_kind,
            "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "payload_base64": base64.b64encode(payload_bytes).decode("ascii"),
            "prior_request_id": prior_request_id,
        }
        body["request_id"] = authority._request_digest(body)
        return authority._canonical(body)
    return host._node_authority_envelope(
        action=action,
        node=node,
        domain=domain,
        sandbox=sandbox,
        sha=SHA,
        tree=TREE,
        payload_kind=payload_kind,
        payload_bytes=payload_bytes,
        prior_request_id=prior_request_id,
    )


def _rebind_request(
    raw: bytes,
    *,
    qianyi_tree: str | None = None,
    candidate_shas: dict[str, str] | None = None,
) -> bytes:
    body = json.loads(raw)
    if body["payload_kind"] == "slurm-candidate-set-json":
        candidate_set = json.loads(
            base64.b64decode(body["payload_base64"], validate=True),
        )
        bindings = candidate_set["candidate_bindings"]
        if qianyi_tree is not None:
            bindings["loom-dev-qianyi"]["candidate_tree"] = qianyi_tree
        for sandbox, candidate_sha in (candidate_shas or {}).items():
            bindings[f"loom-dev-{sandbox}"]["candidate_sha"] = candidate_sha
        candidate_set["candidate_set_sha256"] = hashlib.sha256(
            json.dumps(bindings, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
        payload_bytes = authority._canonical(candidate_set)
        body["payload_base64"] = base64.b64encode(payload_bytes).decode("ascii")
        body["payload_sha256"] = hashlib.sha256(payload_bytes).hexdigest()
        body["candidate_sha"] = bindings["loom-dev-qianyi"]["candidate_sha"]
        body["candidate_tree"] = bindings["loom-dev-qianyi"]["candidate_tree"]
    elif qianyi_tree is not None:
        body["candidate_tree"] = qianyi_tree
    body["request_id"] = authority._request_digest(body)
    return authority._canonical(body)


def test_sudoers_exposes_only_two_exact_no_environment_commands() -> None:
    assert Path(authority.__file__).read_bytes().startswith(b"#!/usr/bin/python3 -I\n")
    lines = (
        Path(
            "deploy/developer-sandboxes/loom-developer-sandbox-node-authority.sudoers",
        )
        .read_text(encoding="ascii")
        .splitlines()
    )

    assert lines == [
        "qianyi ALL=(root) NOPASSWD:NOSETENV: "
        "/usr/local/libexec/loom-developer-sandbox-node-authority transact",
        "qianyi ALL=(root) NOPASSWD:NOSETENV: "
        "/usr/local/libexec/loom-developer-sandbox-node-authority check",
    ]
    payload = "\n".join(lines)
    assert "*" not in payload
    for forbidden in ("install", "tar", "rm", "chown", "chmod", "python3"):
        assert f" {forbidden} " not in f" {payload} "


@pytest.mark.parametrize(
    "argv",
    [
        ["transact", "extra"],
        ["check", "--candidate-sha", SHA],
        ["shell"],
        ["bootstrap", "--candidate-sha", SHA, "--candidate-tree", TREE, "extra"],
        ["upgrade", "--candidate-sha", SHA, "--candidate-tree", TREE, "extra"],
    ],
)
def test_parser_rejects_every_nonfixed_runtime_surface(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        authority._parser().parse_args(argv)


def test_host_and_authority_share_one_canonical_closed_envelope() -> None:
    raw = _request()
    parsed = authority._parse_request(raw, verb="transact", policy=_policy())

    assert parsed.action == "host-converge"
    assert parsed.payload_bytes == b""
    assert parsed.request_id == authority._request_digest(parsed.payload)
    assert raw == authority._canonical(parsed.payload)


def test_envelope_binds_exact_tree_action_payload_and_closed_schema() -> None:
    payload = b"bundle-bytes"
    raw = _request(
        action="materialize",
        payload_kind="git-bundle",
        payload_bytes=payload,
    )
    parsed = authority._parse_request(raw, verb="transact", policy=_policy())
    assert parsed.payload_bytes == payload

    for mutate in (
        lambda value: value.__setitem__("candidate_tree", "d" * 40),
        lambda value: value.__setitem__("payload_sha256", "e" * 64),
        lambda value: value.__setitem__("unknown", True),
    ):
        changed = json.loads(raw)
        mutate(changed)
        changed["request_id"] = authority._request_digest(changed)
        with pytest.raises(authority.NodeAuthorityError, match="binding"):
            authority._parse_request(
                authority._canonical(changed),
                verb="transact",
                policy=_policy(),
            )


def test_check_accepts_only_the_closed_read_only_actions() -> None:
    for action in (
        "inspect-candidate",
        "inspect-local",
        "inspect-link-client",
        "export-domain-attestation",
    ):
        parsed = authority._parse_request(
            _request(action=action),
            verb="check",
            policy=_policy(),
        )
        assert parsed.action == action
    server = authority._parse_request(
        _request(action="inspect-link-server", node="oldlab-2"),
        verb="check",
        policy=_policy("oldlab-2"),
    )
    assert server.action == "inspect-link-server"
    artifact_id = f"runtime-proof/v1/qianyi/{SHA}/{TREE}/artifact/oldlab.json"
    parsed = authority._parse_request(
        _request(
            action="export-runtime-proof-artifact",
            payload_kind="runtime-proof-artifact-id",
            payload_bytes=artifact_id.encode("ascii"),
        ),
        verb="check",
        policy=_policy(),
    )
    assert parsed.payload_bytes == artifact_id.encode("ascii")
    with pytest.raises(authority.NodeAuthorityError, match="binding"):
        authority._parse_request(_request(), verb="check", policy=_policy())


def test_slurm_authority_assets_and_node_inventory_are_closed() -> None:
    assert {
        Path("scripts/ops/developer_sandbox_slurm_policy.py"),
        Path("scripts/ops/slurm_job_cgroup_guard.py"),
        Path("deploy/slurm/developer-sandboxes/oldlab.toml"),
        Path("deploy/slurm/developer-sandboxes/gb10.toml"),
        Path("deploy/slurm/loom-slurm-job-cgroup-guard.service"),
        authority.SLURM_RECOVERY_SERVICE_RELATIVE,
        authority.SLURM_RECOVERY_TIMER_RELATIVE,
    }.issubset(set(authority.SOURCE_ASSETS))
    assert authority.NODE_HOSTNAMES["trt-gb10-7"] == "gx10-0faf"
    assert authority.STAGING_INFRASTRUCTURE_NODES == tuple(
        f"trt-gb10-{index}" for index in range(1, 16)
    )
    assert {
        authority.SLURM_RECOVERY_SERVICE_RELATIVE,
        authority.SLURM_RECOVERY_TIMER_RELATIVE,
    }.issubset(authority.MIGRATABLE_EXTERNAL_SOURCE_ASSETS)


def test_slurm_recovery_systemd_contract_is_root_persistent_and_has_no_kill_surface() -> None:
    service = authority.SLURM_RECOVERY_SERVICE_RELATIVE.read_text(encoding="utf-8")
    timer = authority.SLURM_RECOVERY_TIMER_RELATIVE.read_text(encoding="utf-8")

    assert "User=root" in service
    assert "Group=root" in service
    assert "UMask=0077" in service
    assert (
        "ExecStart=/usr/bin/python3 -I -B "
        "/usr/local/libexec/loom-developer-sandbox-slurm-recovery "
        "recover-drain --execute"
    ) in service
    assert "OnBootSec=45s" in timer
    assert "OnUnitInactiveSec=30s" in timer
    assert "Persistent=true" in timer
    assert "WantedBy=timers.target" in timer
    for forbidden in ("scancel", "docker stop", "docker kill", "State=DOWN"):
        assert forbidden not in service
        assert forbidden not in timer


def test_live_authority_system_install_mapping_is_fixed_and_complete() -> None:
    expected_prefix = (
        (
            Path("scripts/ops/developer_sandbox_slurm_policy.py"),
            authority.SLURM_RECOVERY_LIBEXEC,
            0o755,
            0o755,
        ),
        (
            authority.SLURM_RECOVERY_SERVICE_RELATIVE,
            authority.SLURM_RECOVERY_SERVICE,
            0o644,
            0o755,
        ),
        (
            authority.SLURM_RECOVERY_TIMER_RELATIVE,
            authority.SLURM_RECOVERY_TIMER,
            0o644,
            0o755,
        ),
        (
            authority.PLATFORM_HEALTH_AUTHORITY_RELATIVE,
            Path("/usr/local/libexec/loom-developer-sandbox-platform-health-authority"),
            0o755,
            0o755,
        ),
        (
            authority.CAPACITY_CONTRACT_RELATIVE,
            Path("/usr/local/libexec/scripts/ops/developer_sandbox_capacity_contract.py"),
            0o644,
            0o755,
        ),
        (
            authority.PLATFORM_HEALTH_SERVICE_RELATIVE,
            Path(
                "/etc/systemd/system/loom-developer-sandbox-platform-health-authority.service",
            ),
            0o644,
            0o755,
        ),
        (
            authority.PLATFORM_HEALTH_SUDOERS_RELATIVE,
            Path("/etc/sudoers.d/loom-developer-sandbox-platform-health-authority"),
            0o440,
            0o755,
        ),
        (
            authority.STAGING_PRESSURE_AUTHORITY_RELATIVE,
            Path("/usr/local/libexec/loom-staging-pressure-reclaim-authority"),
            0o755,
            0o755,
        ),
        (
            authority.STAGING_PRESSURE_CONFIG_RELATIVE,
            Path("/etc/loom/staging-pressure-reclaim-authority.toml"),
            0o600,
            0o755,
        ),
        (
            authority.STAGING_PRESSURE_SERVICE_RELATIVE,
            Path("/etc/systemd/system/loom-staging-pressure-reclaim-authority.service"),
            0o644,
            0o755,
        ),
        (
            authority.STAGING_PRESSURE_SUDOERS_RELATIVE,
            Path("/etc/sudoers.d/loom-staging-pressure-reclaim-authority"),
            0o440,
            0o755,
        ),
    )
    assert authority.SYSTEM_INSTALL_ASSETS[: len(expected_prefix)] == expected_prefix
    assert authority.SYSTEM_INSTALL_ASSETS[len(expected_prefix) :] == (
        (
            authority.STAGING_EXTERNAL_AUTHORITY_RELATIVE,
            authority.STAGING_EXTERNAL_SOURCE,
            0o644,
            0o755,
        ),
        (
            authority.STAGING_EXTERNAL_CONSUMER_RELATIVE,
            authority.STAGING_EXTERNAL_CONSUMER,
            0o644,
            0o755,
        ),
        (
            authority.STAGING_EXTERNAL_WRAPPER_RELATIVE,
            authority.STAGING_EXTERNAL_WRAPPER,
            0o755,
            0o755,
        ),
        (
            authority.STAGING_EXTERNAL_CONFIG_RELATIVE,
            authority.STAGING_EXTERNAL_CONFIG,
            0o600,
            0o700,
        ),
        (
            authority.STAGING_EXTERNAL_SERVICE_RELATIVE,
            authority.STAGING_EXTERNAL_SERVICE,
            0o644,
            0o755,
        ),
        (
            authority.STAGING_EXTERNAL_SUDOERS_RELATIVE,
            authority.STAGING_EXTERNAL_SUDOERS,
            0o440,
            0o755,
        ),
        (
            Path(r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount"),
            authority.STAGING_EXTERNAL_MOUNT,
            0o644,
            0o755,
        ),
        (
            Path("deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf"),
            authority.STAGING_EXTERNAL_TMPFILES,
            0o644,
            0o755,
        ),
        (
            authority.NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
            authority.NODE_AUTHORITY_TMPFILES,
            0o644,
            0o755,
        ),
    )
    assert {
        relative for relative, _target, _mode, _parent in authority.SYSTEM_INSTALL_ASSETS
    }.issubset(set(authority.SOURCE_ASSETS))
    managed = {path: (mode, parent_mode) for path, mode, parent_mode in authority._managed_assets()}
    for _relative, target, mode, parent_mode in authority.SYSTEM_INSTALL_ASSETS:
        assert managed[target] == (mode, parent_mode)


def test_policy_allows_only_the_closed_external_asset_upgrade_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    digests = {
        str(relative): "c" * 64
        for relative in authority.SOURCE_ASSETS
        if relative not in authority.MIGRATABLE_EXTERNAL_SOURCE_ASSETS
    }
    payload = authority._canonical(
        {
            "schema_version": 1,
            "source_sha": SHA,
            "source_tree": TREE,
            "node": "oldlab-1",
            "asset_sha256": digests,
        },
    )
    monkeypatch.setattr(authority, "_safe_root_file", lambda *_args, **_kwargs: payload)

    assert authority._read_policy().asset_sha256 == digests

    missing_legacy = dict(digests)
    missing_legacy.pop("scripts/ops/developer_sandbox_node_authority.py")
    invalid = authority._canonical(
        {
            "schema_version": 1,
            "source_sha": SHA,
            "source_tree": TREE,
            "node": "oldlab-1",
            "asset_sha256": missing_legacy,
        },
    )
    monkeypatch.setattr(authority, "_safe_root_file", lambda *_args, **_kwargs: invalid)
    with pytest.raises(authority.NodeAuthorityError, match="asset identity"):
        authority._read_policy()


def test_system_install_orders_validation_activation_reload_and_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    assets = {
        str(relative): f"asset:{relative}".encode()
        for relative, _target, _mode, _parent in authority.SYSTEM_INSTALL_ASSETS
    }
    readback = tuple(
        {
            "path": str(target),
            "mode": f"{mode:04o}",
            "sha256": hashlib.sha256(assets[str(relative)]).hexdigest(),
        }
        for relative, target, mode, _parent in authority.SYSTEM_INSTALL_ASSETS
    )
    monkeypatch.setattr(
        authority,
        "_validate_system_install_sources",
        lambda: events.append("validate-sudoers-sources"),
    )
    monkeypatch.setattr(authority, "_ensure_system_install_directories", lambda: None)
    monkeypatch.setattr(
        authority,
        "_validate_tmpfiles",
        lambda path, *, apply, expected_directories: events.append(
            f"tmpfiles:{apply}:{path}:{expected_directories}",
        ),
    )
    monkeypatch.setattr(
        authority,
        "_validate_systemd_service",
        lambda path, *, label: events.append(f"systemd:{label}:{path}"),
    )
    monkeypatch.setattr(
        authority,
        "_validate_sudoers",
        lambda path, *, label: events.append(f"sudoers:{label}:{path}"),
    )
    monkeypatch.setattr(
        authority,
        "_atomic_replace",
        lambda path, *_args, **_kwargs: events.append(f"replace:{path}"),
    )
    monkeypatch.setattr(
        authority,
        "_systemd_daemon_reload",
        lambda: events.append("daemon-reload"),
    )
    monkeypatch.setattr(
        authority,
        "_systemd_enable_recovery_timer",
        lambda *, start: events.append(f"recovery-enabled:{start}"),
    )
    monkeypatch.setattr(
        authority,
        "_validate_system_install_assets",
        lambda **_kwargs: events.append("readback") or readback,
    )

    assert authority._system_install_assets(assets, replace=True) == readback

    first_service = min(
        events.index(f"replace:{service}") for service in authority._system_service_paths()
    )
    last_libexec_or_config = max(
        events.index(f"replace:{target}")
        for _relative, target, _mode, _parent in authority.SYSTEM_INSTALL_ASSETS
        if target not in authority._system_service_paths()
        and target not in authority._system_sudoers_paths()
    )
    first_sudoers = min(
        events.index(f"replace:{sudoers}") for sudoers in authority._system_sudoers_paths()
    )
    assert last_libexec_or_config < first_service < first_sudoers
    assert events[-5:] == [
        "tmpfiles:True:"
        f"{authority.NODE_AUTHORITY_TMPFILES}:"
        f"{authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES}",
        "tmpfiles:True:"
        f"{authority.STAGING_EXTERNAL_TMPFILES}:"
        f"{authority.STAGING_SHARED_TMPFILES_DIRECTORIES}",
        "daemon-reload",
        "recovery-enabled:False",
        "readback",
    ]


def test_node_authority_tmpfiles_asset_is_exact_upgrade_managed_and_persistent() -> None:
    assert authority.NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE.read_text(encoding="utf-8") == (
        "d /run/loom-developer-sandbox-node-authority 0700 root root -\n"
    )
    assert authority.NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE in authority.SOURCE_ASSETS
    assert authority.NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE in (
        authority.MIGRATABLE_EXTERNAL_SOURCE_ASSETS
    )
    assert (
        authority.NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
        authority.NODE_AUTHORITY_TMPFILES,
        0o644,
        0o755,
    ) in authority.SYSTEM_INSTALL_ASSETS
    assert authority.NODE_AUTHORITY_TMPFILES == Path(
        "/etc/tmpfiles.d/loom-developer-sandbox-node-authority.conf",
    )
    assert (authority.NODE_AUTHORITY_TMPFILES, 0o644, 0o755) in authority._managed_assets()


def test_system_install_validates_both_tmpfiles_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked: list[tuple[Path, bool, tuple[tuple[Path, int], ...]]] = []
    monkeypatch.setattr(authority, "_validate_sudoers", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_validate_tmpfiles",
        lambda path, *, apply, expected_directories: checked.append(
            (path, apply, expected_directories),
        ),
    )

    authority._validate_system_install_sources()

    assert checked == [
        (
            authority.SOURCE_ROOT / Path(
                "deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf",
            ),
            False,
            authority.STAGING_SHARED_TMPFILES_DIRECTORIES,
        ),
        (
            authority.SOURCE_ROOT / authority.NODE_AUTHORITY_TMPFILES_SOURCE_RELATIVE,
            False,
            authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES,
        ),
    ]


def test_tmpfiles_source_validation_uses_an_isolated_root_and_cleans_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_root = tmp_path / "stage"
    policy = tmp_path / "policy.conf"
    policy.write_text(
        "d /srv/loom 0755 root root -\nd /srv/loom/staging-shared 0755 root root -\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)
    ensured: list[tuple[Path, int, int]] = []

    def ensure(path: Path, *, mode: int, parent_mode: int) -> bool:
        ensured.append((path, mode, parent_mode))
        if path.exists():
            return False
        path.mkdir(mode=mode)
        return True

    monkeypatch.setattr(authority, "_ensure_root_directory", ensure)
    checked: list[tuple[Path, int]] = []
    monkeypatch.setattr(
        authority,
        "_safe_root_directory",
        lambda path, *, mode: checked.append((path, mode)),
    )

    def run(
        argv: Sequence[str],
        **_kwargs: object,
    ) -> object:
        assert "--dry-run" not in argv
        roots = [value for value in argv if value.startswith("--root=")]
        assert len(roots) == 1
        validation_root = Path(roots[0].partition("=")[2])
        (validation_root / "srv/loom/staging-shared").mkdir(parents=True)
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(authority.subprocess, "run", run)

    authority._validate_tmpfiles(
        policy,
        apply=False,
        expected_directories=authority.STAGING_SHARED_TMPFILES_DIRECTORIES,
    )

    assert ensured[0] == (stage_root, 0o700, 0o755)
    assert checked[-1][0].parent == stage_root
    assert checked[-1][1] == 0o700
    assert not tuple(stage_root.iterdir())


@pytest.mark.parametrize(
    "invalid",
    ("extra", "duplicate", "wrong-type"),
    ids=("extra", "duplicate", "wrong-type"),
)
@pytest.mark.parametrize(
    "expected_directories",
    (
        authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES,
        authority.STAGING_SHARED_TMPFILES_DIRECTORIES,
    ),
    ids=("node", "staging"),
)
def test_tmpfiles_apply_rejects_non_closed_policy_before_execution(
    monkeypatch: pytest.MonkeyPatch,
    invalid: str,
    expected_directories: tuple[tuple[Path, int], ...],
) -> None:
    exact_lines = [
        f"d /{relative.as_posix()} {mode:04o} root root -\n"
        for relative, mode in expected_directories
    ]
    if invalid == "extra":
        exact_lines.append("d /tmp/extra 0700 root root -\n")
    elif invalid == "duplicate":
        exact_lines.append(exact_lines[0])
    else:
        exact_lines[0] = f"D{exact_lines[0][1:]}"
    policy = "".join(exact_lines).encode("ascii")
    installed = Path("/etc/tmpfiles.d") / (
        "loom-developer-sandbox-node-authority.conf"
        if expected_directories == authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES
        else "loom-staging-shared.conf"
    )
    monkeypatch.setattr(authority, "_safe_root_file", lambda *_args, **_kwargs: policy)
    monkeypatch.setattr(
        authority,
        "_ensure_stage_root",
        lambda: pytest.fail("closed policy must be checked before stage creation"),
    )
    monkeypatch.setattr(
        authority.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail(
            "non-closed policy must not reach systemd-tmpfiles",
        ),
    )

    with pytest.raises(authority.NodeAuthorityError, match="exact closed policy"):
        authority._validate_tmpfiles(
            installed,
            apply=True,
            expected_directories=expected_directories,
        )


def test_tmpfiles_apply_uses_boot_resolution_and_exact_etc_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = b"d /run/loom-developer-sandbox-node-authority 0700 root root -\n"
    installed = Path("/etc/tmpfiles.d/loom-developer-sandbox-node-authority.conf")
    readbacks: list[tuple[Path, int, int]] = []
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, *, mode, limit: (
            readbacks.append((path, mode, limit)) or policy
        ),
    )
    monkeypatch.setattr(authority, "_ensure_stage_root", lambda: None)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)

    def run(argv: Sequence[str], **_kwargs: object) -> object:
        commands.append(tuple(argv))
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(authority.subprocess, "run", run)

    authority._validate_tmpfiles(
        installed,
        apply=True,
        expected_directories=authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES,
    )

    assert readbacks == [(installed, 0o644, 4096)]
    assert commands == [
        (
            "/usr/bin/systemd-tmpfiles",
            "--create",
            "loom-developer-sandbox-node-authority.conf",
        ),
    ]


def test_tmpfiles_apply_rejects_non_etc_boot_policy_before_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = b"d /run/loom-developer-sandbox-node-authority 0700 root root -\n"
    monkeypatch.setattr(authority, "_safe_root_file", lambda *_args, **_kwargs: policy)
    monkeypatch.setattr(
        authority,
        "_ensure_stage_root",
        lambda: pytest.fail("boot path must be checked before stage creation"),
    )
    monkeypatch.setattr(
        authority.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail(
            "unsafe boot path must not reach systemd-tmpfiles",
        ),
    )

    with pytest.raises(authority.NodeAuthorityError, match="boot policy path"):
        authority._validate_tmpfiles(
            Path("/usr/lib/tmpfiles.d/loom-developer-sandbox-node-authority.conf"),
            apply=True,
            expected_directories=authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES,
        )


def test_tmpfiles_source_validation_cleans_isolated_root_after_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_root = tmp_path / "stage"
    stage_root.mkdir()
    policy = tmp_path / "policy.conf"
    policy.write_text("invalid\n", encoding="utf-8")
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)
    monkeypatch.setattr(
        authority,
        "_ensure_root_directory",
        lambda path, **_kwargs: False if path.exists() else path.mkdir() is None,
    )
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority.subprocess,
        "run",
        lambda *_args, **_kwargs: type("Result", (), {"returncode": 1})(),
    )

    with pytest.raises(authority.NodeAuthorityError, match="tmpfiles policy"):
        authority._validate_tmpfiles(
            policy,
            apply=False,
            expected_directories=authority.STAGING_SHARED_TMPFILES_DIRECTORIES,
        )

    assert not tuple(stage_root.iterdir())


def test_node_tmpfiles_source_validation_recreates_stage_root_after_reboot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_root = tmp_path / "run" / "loom-developer-sandbox-node-authority"
    stage_root.parent.mkdir(mode=0o755)
    policy = tmp_path / "node-authority.conf"
    policy.write_text(
        "d /run/loom-developer-sandbox-node-authority 0700 root root -\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)
    original_safe = authority._safe_root_directory

    def safe_directory(path: Path, *, mode: int) -> None:
        if tmp_path in path.parents or path == tmp_path:
            try:
                metadata = path.lstat()
            except OSError as exc:
                raise authority.NodeAuthorityError("test directory is unavailable") from exc
            if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != mode:
                raise authority.NodeAuthorityError("test directory metadata is unsafe")
            return
        original_safe(path, mode=mode)

    def run(argv: Sequence[str], **_kwargs: object) -> object:
        validation_root = Path(
            next(value for value in argv if value.startswith("--root=")).partition("=")[2],
        )
        target = validation_root / authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES[0][0]
        target.mkdir(parents=True, mode=0o700)
        target.chmod(0o700)
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(authority, "_safe_root_directory", safe_directory)
    monkeypatch.setattr(authority.os, "chown", lambda *_args: None)
    monkeypatch.setattr(authority.subprocess, "run", run)

    authority._validate_tmpfiles(
        policy,
        apply=False,
        expected_directories=authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES,
    )

    assert stage_root.is_dir()
    assert stat.S_IMODE(stage_root.stat().st_mode) == 0o700
    assert not tuple(stage_root.iterdir())


@pytest.mark.parametrize("invalid", ("missing", "wrong-mode"))
def test_node_tmpfiles_source_validation_rejects_missing_or_unsafe_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: str,
) -> None:
    stage_root = tmp_path / "run" / "loom-developer-sandbox-node-authority"
    stage_root.parent.mkdir(mode=0o755)
    policy = tmp_path / "node-authority.conf"
    policy.write_text(
        "d /run/loom-developer-sandbox-node-authority 0700 root root -\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)

    def safe_directory(path: Path, *, mode: int) -> None:
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise authority.NodeAuthorityError("test directory is unavailable") from exc
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != mode:
            raise authority.NodeAuthorityError("test directory metadata is unsafe")

    def run(argv: Sequence[str], **_kwargs: object) -> object:
        validation_root = Path(
            next(value for value in argv if value.startswith("--root=")).partition("=")[2],
        )
        if invalid == "wrong-mode":
            target = validation_root / authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES[0][0]
            target.mkdir(parents=True, mode=0o755)
            target.chmod(0o755)
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(authority, "_safe_root_directory", safe_directory)
    monkeypatch.setattr(authority.os, "chown", lambda *_args: None)
    monkeypatch.setattr(authority.subprocess, "run", run)

    with pytest.raises(authority.NodeAuthorityError, match=r"unavailable|unsafe"):
        authority._validate_tmpfiles(
            policy,
            apply=False,
            expected_directories=authority.NODE_AUTHORITY_TMPFILES_DIRECTORIES,
        )

    assert stage_root.is_dir()
    assert not tuple(stage_root.iterdir())


def _platform_health_payload(
    *,
    node: str = "oldlab-1",
    host_name: str = "trt-eai-oldlab-1",
) -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.platform-health-node-request",
            "session_id": "1" * 32,
            "checkpoint": "mixed_non_loom",
            "checkpoint_group": "during",
            "expected_node": node,
            "expected_host": host_name,
            "since_at": "2026-07-29T00:00:00Z",
            "candidates": {
                "qianyi": {"sha": SHA, "tree": TREE},
                "hongjian": {"sha": "c" * 40, "tree": "d" * 40},
                "devansh": {"sha": "e" * 40, "tree": "f" * 40},
            },
        },
    )


def _staging_pressure_payload(*, phase: str = "before") -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "loom.staging-pressure-reclaim.observe-request",
            "source_host": "trt-eai-oldlab-1",
            "submit_host": "trt-gb10-1",
            "environment": "staging",
            "pool": "gb10",
            "partition": "gb10",
            "account": "loom-staging",
            "qos": "loom-staging",
            "phase": phase,
            "session_id": "00000000-0000-0000-0000-000000000001",
            "acceptance_session_id": "1" * 32,
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "owned_jobs": [
                {
                    "job_id": "12345",
                    "user": "loom-staging-worker",
                    "account": "loom-staging",
                    "qos": "loom-staging",
                    "name": "loom-pressure",
                },
            ],
        },
    )


def _staging_pressure_result(*, phase: str = "before") -> dict[str, object]:
    jobs: list[dict[str, str]] = [
        {
            "job_id": "99999",
            "user": "researcher",
            "account": "research",
            "qos": "normal",
            "state": "RUNNING",
            "nodes": "trt-gb10-2",
            "name": "peer",
        },
    ]
    if phase == "before":
        jobs.append(
            {
                "job_id": "12345",
                "user": "loom-staging-worker",
                "account": "loom-staging",
                "qos": "loom-staging",
                "state": "RUNNING",
                "nodes": "trt-gb10-1",
                "name": "loom-pressure",
            },
        )
    result: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.staging-pressure-reclaim.observe-result",
        "submit_host": "trt-gb10-1",
        "environment": "staging",
        "pool": "gb10",
        "partition": "gb10",
        "account": "loom-staging",
        "qos": "loom-staging",
        "phase": phase,
        "session_id": "00000000-0000-0000-0000-000000000001",
        "acceptance_session_id": "1" * 32,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "observed_at": "2026-07-29T12:00:00Z",
        "jobs": jobs,
    }
    result["snapshot_sha256"] = hashlib.sha256(authority._canonical(result)).hexdigest()
    return result


def test_staging_pressure_observer_envelope_parse_dispatch_and_readback_are_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _staging_pressure_payload()
    raw = _request(
        action="staging-pressure-reclaim-observe",
        node="trt-gb10-1",
        domain="gb10",
        sandbox="staging",
        payload_kind="staging-pressure-reclaim-observe-request",
        payload_bytes=payload,
    )
    parsed = authority._parse_request(
        raw,
        verb="check",
        policy=_policy("trt-gb10-1"),
    )
    expected_result = _staging_pressure_result()
    monkeypatch.setattr(
        authority,
        "_run_fixed_input",
        lambda argv, body: (
            expected_result
            if argv == authority._staging_pressure_authority_argv(parsed) and body == payload
            else pytest.fail("pressure observer dispatch drifted")
        ),
    )
    descriptor = os.open("/dev/null", os.O_RDONLY)
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority, "_open_lock", lambda **_kwargs: descriptor)
    monkeypatch.setattr(authority, "_reject_active_upgrade", lambda: None)
    monkeypatch.setattr(authority, "_read_policy", lambda: _policy("trt-gb10-1"))
    monkeypatch.setattr(authority, "_validate_runtime_assets", lambda _policy: None)

    response = authority.dispatch("check", raw)

    assert response == {
        "schema_version": 1,
        "request_id": parsed.request_id,
        "status": "succeeded",
        "result": expected_result,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partition", "prod"),
        ("account", "research"),
        ("qos", "normal"),
        ("submit_host", "trt-gb10-2"),
    ],
)
def test_staging_pressure_observer_rejects_caller_scope(
    field: str,
    value: str,
) -> None:
    payload = json.loads(_staging_pressure_payload())
    payload[field] = value
    raw_payload = authority._canonical(payload)
    raw = _request(
        action="staging-pressure-reclaim-observe",
        node="trt-gb10-1",
        domain="gb10",
        sandbox="staging",
        payload_kind="staging-pressure-reclaim-observe-request",
        payload_bytes=raw_payload,
    )
    with pytest.raises(authority.NodeAuthorityError, match="pressure observe"):
        authority._parse_request(
            raw,
            verb="check",
            policy=_policy("trt-gb10-1"),
        )


def test_platform_health_node_action_is_closed_and_source_installed() -> None:
    payload = _platform_health_payload()
    request = authority._parse_request(
        _request(
            action="observe-platform-health-node",
            payload_kind="platform-health-node-json",
            payload_bytes=payload,
        ),
        verb="check",
        policy=_policy(),
    )

    assert request.action == "observe-platform-health-node"
    assert authority.PLATFORM_HEALTH_AUTHORITY_RELATIVE in authority.SOURCE_ASSETS
    assert authority.PLATFORM_HEALTH_CONFIG_RELATIVE in authority.SOURCE_ASSETS
    assert authority._platform_health_authority_argv(request, "observe-node") == (
        "/usr/bin/python3",
        "-I",
        "/opt/loom-developer-sandbox-node-authority/source/"
        "scripts/ops/developer_sandbox_platform_health_authority.py",
        "observe-node",
    )


@pytest.mark.parametrize(
    ("node", "host_name", "mutate"),
    [
        ("oldlab-1", "trt-eai-oldlab-1", lambda item: item.update({"unknown": True})),
        ("oldlab-1", "foreign", lambda _item: None),
        (
            "oldlab-1",
            "trt-eai-oldlab-1",
            lambda item: item["candidates"]["qianyi"].update({"sha": "9" * 40}),
        ),
        (
            "oldlab-1",
            "trt-eai-oldlab-1",
            lambda item: item.update({"checkpoint_group": "after"}),
        ),
    ],
)
def test_platform_health_node_action_rejects_foreign_or_cross_candidate_payload(
    node: str,
    host_name: str,
    mutate: object,
) -> None:
    payload = json.loads(_platform_health_payload(node=node, host_name=host_name))
    mutate(payload)  # type: ignore[operator]
    with pytest.raises(authority.NodeAuthorityError, match="platform-health"):
        authority._parse_request(
            _request(
                action="observe-platform-health-node",
                node=node,
                payload_kind="platform-health-node-json",
                payload_bytes=authority._canonical(payload),
            ),
            verb="check",
            policy=_policy(node),
        )


def _staging_probe_payload() -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "staging_external_slurm_allocation_probe_request",
            "request_id": "1" * 64,
            "candidate_sha": SHA,
            "candidate_tree": TREE,
        },
    )


def test_staging_probe_is_fixed_to_gb10_submit_host_and_closed_argv() -> None:
    request = authority._parse_request(
        _request(
            action="staging-allocation-probe",
            node="trt-gb10-1",
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-allocation-probe-request",
            payload_bytes=_staging_probe_payload(),
        ),
        verb="transact",
        policy=_policy("trt-gb10-1"),
    )

    assert authority._staging_allocation_probe_argv(request) == (
        "/usr/bin/python3",
        "-I",
        "/opt/loom-developer-sandbox-node-authority/source/scripts/ops/developer_sandbox_host.py",
        "staging-allocation-probe",
        "--candidate-sha",
        SHA,
        "--candidate-tree",
        TREE,
        "--request-id",
        "1" * 64,
        "--execute",
    )
    with pytest.raises(authority.NodeAuthorityError, match="submit host"):
        authority._parse_request(
            _request(
                action="staging-allocation-probe",
                node="trt-gb10-2",
                domain="gb10",
                sandbox="staging",
                payload_kind="staging-allocation-probe-request",
                payload_bytes=_staging_probe_payload(),
            ),
            verb="transact",
            policy=_policy("trt-gb10-2"),
        )


def _staging_submit_payload(node: str = "trt-gb10-8") -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "staging_external_slurm_allocation_submit_request",
            "request_id": "2" * 64,
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "requested_node": node,
        },
    )


def test_staging_broker_submit_is_staging_scoped_and_fixed() -> None:
    request = authority._parse_request(
        _request(
            action="staging-allocation-submit",
            node="trt-gb10-1",
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-allocation-submit-request",
            payload_bytes=_staging_submit_payload(),
        ),
        verb="transact",
        policy=_policy("trt-gb10-1"),
    )

    assert authority._staging_broker_argv(request) == (
        "/usr/bin/python3",
        "-I",
        "/opt/loom-developer-sandbox-node-authority/source/scripts/ops/developer_sandbox_host.py",
        "staging-allocation-submit",
        "--candidate-sha",
        SHA,
        "--candidate-tree",
        TREE,
        "--request-id",
        "2" * 64,
        "--requested-node",
        "trt-gb10-8",
        "--execute",
    )
    outer = json.loads(
        _request(
            action="staging-allocation-submit",
            node="trt-gb10-1",
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-allocation-submit-request",
            payload_bytes=_staging_submit_payload(),
        ),
    )
    outer["sandbox"] = "qianyi"
    outer["request_id"] = authority._request_digest(outer)
    with pytest.raises(authority.NodeAuthorityError, match="binding"):
        authority._parse_request(
            authority._canonical(outer),
            verb="transact",
            policy=_policy("trt-gb10-1"),
        )
    node_seven = authority._parse_request(
        _request(
            action="staging-allocation-submit",
            node="trt-gb10-1",
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-allocation-submit-request",
            payload_bytes=_staging_submit_payload("trt-gb10-7"),
        ),
        verb="transact",
        policy=_policy("trt-gb10-1"),
    )
    assert json.loads(node_seven.payload_bytes)["requested_node"] == "trt-gb10-7"


def test_staging_broker_cancel_uses_the_same_fixed_controller_route() -> None:
    payload = authority._canonical(
        {
            "schema_version": 1,
            "kind": "staging_external_slurm_allocation_cancel_request",
            "request_id": "5" * 64,
            "submit_request_id": "2" * 64,
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "requested_node": "trt-gb10-8",
            "job_id": "31415",
        },
    )
    request = authority._parse_request(
        _request(
            action="staging-allocation-cancel",
            node="trt-gb10-1",
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-allocation-cancel-request",
            payload_bytes=payload,
        ),
        verb="transact",
        policy=_policy("trt-gb10-1"),
    )

    assert authority._staging_broker_argv(request) == (
        "/usr/bin/python3",
        "-I",
        "/opt/loom-developer-sandbox-node-authority/source/scripts/ops/developer_sandbox_host.py",
        "staging-allocation-cancel",
        "--candidate-sha",
        SHA,
        "--candidate-tree",
        TREE,
        "--request-id",
        "5" * 64,
        "--requested-node",
        "trt-gb10-8",
        "--submit-request-id",
        "2" * 64,
        "--job-id",
        "31415",
        "--execute",
    )
    assert authority._staging_broker_submission_path("2" * 64) == (
        authority.STAGING_BROKER_ROOT / f"{'2' * 64}.json"
    )


def test_staging_accounting_action_has_no_caller_policy_surface() -> None:
    payload = authority._staging_infrastructure_operation_envelope(
        action="staging-slurm-accounting-converge",
        node="trt-gb10-1",
        candidate_sha=SHA,
        candidate_tree=TREE,
        convergence_id="3" * 64,
        requested_at="2026-07-29T12:00:00Z",
    )
    payload = base64.b64decode(json.loads(payload)["payload_base64"], validate=True)
    request = authority._parse_request(
        _request(
            action="staging-slurm-accounting-converge",
            node="trt-gb10-1",
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-infrastructure-operation-request",
            payload_bytes=payload,
        ),
        verb="transact",
        policy=_policy("trt-gb10-1"),
    )

    assert json.loads(request.payload_bytes) == {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-operation-request",
        "request_id": hashlib.sha256(
            authority._canonical(
                {
                    "schema_version": 1,
                    "kind": "loom.staging-external-slurm.infrastructure-operation-request",
                    "action": "staging-slurm-accounting-converge",
                    "node": "trt-gb10-1",
                    "candidate_sha": SHA,
                    "candidate_tree": TREE,
                    "convergence_id": "3" * 64,
                    "requested_at": "2026-07-29T12:00:00Z",
                },
            ),
        ).hexdigest(),
        "action": "staging-slurm-accounting-converge",
        "node": "trt-gb10-1",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "convergence_id": "3" * 64,
        "requested_at": "2026-07-29T12:00:00Z",
    }
    with pytest.raises(authority.NodeAuthorityError, match="accounting"):
        wrong_node_envelope = authority._staging_infrastructure_operation_envelope(
            action="staging-slurm-accounting-converge",
            node="trt-gb10-2",
            candidate_sha=SHA,
            candidate_tree=TREE,
            convergence_id="3" * 64,
            requested_at="2026-07-29T12:00:00Z",
        )
        authority._parse_request(
            _request(
                action="staging-slurm-accounting-converge",
                node="trt-gb10-2",
                domain="gb10",
                sandbox="staging",
                payload_kind="staging-infrastructure-operation-request",
                payload_bytes=base64.b64decode(
                    json.loads(wrong_node_envelope)["payload_base64"],
                    validate=True,
                ),
            ),
            verb="transact",
            policy=_policy("trt-gb10-2"),
        )


def _staging_accounting_journal(*, phase: str = "qos") -> dict[str, object]:
    snapshot: dict[str, object] = {
        "account": [],
        "qos": [],
        "association": [],
        "user": [],
    }
    return {
        "schema_version": 1,
        "kind": "loom.staging-slurm-accounting-transaction",
        "authority_request_id": "4" * 64,
        "request_id": "3" * 64,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "snapshot": snapshot,
        "snapshot_sha256": hashlib.sha256(authority._canonical(snapshot)).hexdigest(),
        "phase": phase,
        "created_at": "2026-07-29T12:00:00+00:00",
        "updated_at": "2026-07-29T12:00:01+00:00",
    }


def test_staging_accounting_recovery_rolls_back_bound_incomplete_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal_path = tmp_path / "journal.json"
    journal_path.write_bytes(authority._canonical(_staging_accounting_journal()))
    writes: list[dict[str, object]] = []
    rollbacks: list[dict[str, object]] = []
    monkeypatch.setattr(authority, "STAGING_ACCOUNTING_JOURNAL", journal_path)
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, **_kwargs: path.read_bytes(),
    )
    monkeypatch.setattr(
        authority,
        "_staging_accounting_rollback",
        lambda snapshot: rollbacks.append(dict(snapshot)),
    )
    monkeypatch.setattr(
        authority,
        "_atomic_replace",
        lambda _path, raw, _mode, **_kwargs: writes.append(json.loads(raw)),
    )

    assert authority._staging_accounting_recover() is True
    assert rollbacks == [
        {"account": [], "qos": [], "association": [], "user": []},
    ]
    assert writes[-1]["phase"] == "rolled-back"


def test_staging_accounting_recovery_rejects_snapshot_digest_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = _staging_accounting_journal()
    journal["snapshot_sha256"] = "f" * 64
    journal_path = tmp_path / "journal.json"
    journal_path.write_bytes(authority._canonical(journal))
    monkeypatch.setattr(authority, "STAGING_ACCOUNTING_JOURNAL", journal_path)
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, **_kwargs: path.read_bytes(),
    )
    monkeypatch.setattr(
        authority,
        "_staging_accounting_rollback",
        lambda _snapshot: pytest.fail("unbound snapshot must not be applied"),
    )

    with pytest.raises(authority.NodeAuthorityError, match="binding"):
        authority._staging_accounting_recover()


@pytest.mark.parametrize(
    ("node", "canonical_host"),
    [
        ("trt-gb10-2", "gx10-0fca"),
        ("trt-gb10-7", "gx10-0faf"),
    ],
)
def test_staging_bootstrap_covers_infrastructure_nodes_and_returns_fixed_roots(
    monkeypatch: pytest.MonkeyPatch,
    node: str,
    canonical_host: str,
) -> None:
    operation_envelope = authority._staging_infrastructure_operation_envelope(
        action="staging-allocation-bootstrap",
        node=node,
        candidate_sha=SHA,
        candidate_tree=TREE,
        convergence_id="3" * 64,
        requested_at="2026-07-29T12:00:00Z",
    )
    operation_payload = base64.b64decode(
        json.loads(operation_envelope)["payload_base64"],
        validate=True,
    )
    request = authority._parse_request(
        _request(
            action="staging-allocation-bootstrap",
            node=node,
            domain="gb10",
            sandbox="staging",
            payload_kind="staging-infrastructure-operation-request",
            payload_bytes=operation_payload,
        ),
        verb="transact",
        policy=_policy(node),
    )
    account = pwd.struct_passwd(
        (
            "loom-staging-worker",
            "x",
            31024,
            31024,
            "",
            "/nonexistent",
            "/usr/sbin/nologin",
        ),
    )
    monkeypatch.setattr(
        authority,
        "_staging_service_identity",
        lambda: (account, ("docker",)),
    )
    monkeypatch.setattr(
        authority,
        "_staging_mount_readback",
        lambda: {
            "unit": r"srv-loom-staging\x2dshared.mount",
            "source": "192.168.20.12:/shared_work2/loom/staging",
            "filesystem_type": "nfs4",
            "target": "/srv/loom/staging-shared",
            "device": 1,
            "inode": 2,
            "active": True,
        },
    )
    monkeypatch.setattr(authority, "_staging_accounting_readback", lambda: None)
    monkeypatch.setattr(
        authority,
        "_staging_boot_id",
        lambda: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    )
    monkeypatch.setattr(
        authority,
        "_run_fixed",
        lambda _argv: {
            "schema_version": 1,
            "kind": "staging_external_slurm_identity_bootstrap",
            "node": node,
            "canonical_host": canonical_host,
            "service_identity": {
                "username": "loom-staging-worker",
                "group": "loom-staging-worker",
                "uid": 31024,
                "gid": 31024,
                "home": "/nonexistent",
                "shell": "/usr/sbin/nologin",
                "supplementary_groups": ["docker"],
            },
            "namespace": {
                "root": "/srv/loom/staging-shared",
                "mount_source": "192.168.20.12:/shared_work2/loom/staging",
                "mount_fstype": "nfs4",
                "mount_device": 1,
                "mount_inode": 2,
                "repository_root": "/srv/loom/staging-shared/candidates",
                "worker_env_root": "/srv/loom/staging-shared/generated",
                "result_root": "/srv/loom/staging-shared/results",
                "service_uid": 31024,
                "service_gid": 31024,
                "root_mode": "0o750",
                "repository_root_mode": "0o750",
                "worker_env_root_mode": "0o750",
                "result_root_mode": "0o2770",
            },
            "result": "pass",
        },
    )

    result = authority._staging_allocation_bootstrap(request)

    assert result["canonical_host"] == canonical_host
    assert result["boot_id"] == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    assert result["mount_digest"] == hashlib.sha256(
        authority._canonical(result["mount"])
    ).hexdigest()
    assert result["supplementary_groups"] == ["docker"]
    assert result["repository_root"] == "/srv/loom/staging-shared/candidates"
    assert result["env_root"] == "/srv/loom/staging-shared/generated"
    assert result["result_root"] == "/srv/loom/staging-shared/results"
    assert result["status"] == "converged"


def test_slurm_actions_bind_domain_and_controller_role() -> None:
    compute = authority._parse_request(
        _request(action="slurm-node-converge", node="oldlab-2"),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    controller = authority._parse_request(
        _request(action="slurm-controller-converge"),
        verb="transact",
        policy=_policy(),
    )
    checked = authority._parse_request(
        _request(action="slurm-check", node="trt-gb10-2", domain="gb10"),
        verb="check",
        policy=_policy("trt-gb10-2"),
    )
    assert compute.action == "slurm-node-converge"
    assert controller.action == "slurm-controller-converge"
    assert checked.action == "slurm-check"

    for raw, verb, policy in (
        (
            _request(action="slurm-node-converge"),
            "transact",
            _policy(),
        ),
        (
            _request(action="slurm-controller-converge", node="oldlab-2"),
            "transact",
            _policy("oldlab-2"),
        ),
        (
            _request(action="slurm-check", node="trt-gb10-2", domain="oldlab"),
            "check",
            _policy("trt-gb10-2"),
        ),
    ):
        with pytest.raises(authority.NodeAuthorityError, match="Slurm"):
            authority._parse_request(raw, verb=verb, policy=policy)


def test_slurm_request_allows_new_tree_but_other_actions_remain_exact_tree_bound() -> None:
    new_tree = "f" * 40
    parsed = authority._parse_request(
        _rebind_request(
            _request(action="slurm-node-converge", node="oldlab-2"),
            qianyi_tree=new_tree,
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )

    assert parsed.payload["candidate_tree"] == new_tree
    with pytest.raises(authority.NodeAuthorityError, match="request binding"):
        authority._parse_request(
            _rebind_request(_request(action="host-converge"), qianyi_tree=new_tree),
            verb="transact",
            policy=_policy(),
        )


def test_slurm_request_rejects_colliding_job_label_prefixes() -> None:
    with pytest.raises(authority.NodeAuthorityError, match="candidate-set"):
        authority._parse_request(
            _rebind_request(
                _request(action="slurm-node-converge", node="oldlab-2"),
                candidate_shas={
                    "qianyi": "1" * 12 + "a" * 28,
                    "hongjian": "1" * 12 + "b" * 28,
                    "devansh": "2" * 40,
                },
            ),
            verb="transact",
            policy=_policy("oldlab-2"),
        )


def test_slurm_candidate_surface_must_equal_installed_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    new_tree = "f" * 40
    request = authority._parse_request(
        _rebind_request(
            _request(action="slurm-node-converge", node="oldlab-2"),
            qianyi_tree=new_tree,
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    surface = (
        "scripts/ops/developer_sandbox_slurm_policy.py",
        "scripts/ops/slurm_job_cgroup_guard.py",
        "deploy/slurm/developer-sandboxes/oldlab.toml",
        "deploy/slurm/loom-slurm-job-cgroup-guard.service",
    )
    blobs = {relative: f"installed:{relative}".encode("ascii") for relative in surface}
    asset_sha256 = dict(_policy("oldlab-2").asset_sha256)
    asset_sha256.update(
        {
            relative: hashlib.sha256(content).hexdigest()
            for relative, content in blobs.items()
        },
    )
    installed = authority.AuthorityPolicy(
        source_sha=SHA,
        source_tree=TREE,
        node="oldlab-2",
        asset_sha256=asset_sha256,
    )

    def run_fixed(argv: tuple[str, ...]) -> dict[str, object]:
        arguments = list(argv)

        def value(flag: str) -> str:
            return arguments[arguments.index(flag) + 1]

        return {
            "operation": "inspect-candidate",
            "domain": value("--domain"),
            "sandbox": value("--sandbox"),
            "candidate_sha": value("--candidate-sha"),
            "candidate_tree": value("--candidate-tree"),
            "candidate_clean": True,
        }

    def run(argv: tuple[str, ...], **_kwargs: object) -> object:
        relative = argv[-1].split(":", 1)[1]
        stdout = (
            f"{len(blobs[relative])}\n".encode("ascii")
            if "cat-file" in argv
            else blobs[relative]
        )
        return type(
            "Completed",
            (),
            {"returncode": 0, "stderr": b"", "stdout": stdout},
        )()

    monkeypatch.setattr(authority, "_run_fixed", run_fixed)
    monkeypatch.setattr(authority.subprocess, "run", run)

    authority._validate_slurm_candidate(request, installed)

    incompatible = authority.AuthorityPolicy(
        source_sha=installed.source_sha,
        source_tree=installed.source_tree,
        node=installed.node,
        asset_sha256={**installed.asset_sha256, surface[0]: "0" * 64},
    )
    with pytest.raises(authority.NodeAuthorityError, match="installed authority"):
        authority._validate_slurm_candidate(request, incompatible)


def test_slurm_policy_argv_is_fully_derived_and_has_no_override_surface() -> None:
    compute = authority._parse_request(
        _request(action="slurm-node-converge", node="oldlab-2"),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    controller = authority._parse_request(
        _request(action="slurm-controller-converge"),
        verb="transact",
        policy=_policy(),
    )
    checked = authority._parse_request(
        _request(action="slurm-check", node="trt-gb10-2", domain="gb10"),
        verb="check",
        policy=_policy("trt-gb10-2"),
    )

    candidate = f"/shared_work/loom/candidates/sandboxes/qianyi/{SHA}"
    bindings_json = json.dumps(
        json.loads(compute.payload_bytes)["candidate_bindings"],
        sort_keys=True,
        separators=(",", ":"),
    )
    candidate_set = json.loads(compute.payload_bytes)
    assert authority._slurm_policy_argv(compute, "apply") == (
        "/usr/bin/python3",
        "-I",
        f"{candidate}/scripts/ops/developer_sandbox_slurm_policy.py",
        "apply",
        "--profile",
        f"{candidate}/deploy/slurm/developer-sandboxes/oldlab.toml",
        "--candidate-sha",
        SHA,
        "--candidate-bindings-json",
        bindings_json,
        "--transaction-id",
        compute.request_id,
        "--candidate-set-generation",
        "1",
        "--candidate-set-convergence-id",
        str(candidate_set["convergence_id"]),
        "--candidate-set-payload-sha256",
        str(compute.payload["payload_sha256"]),
        "--execute",
        "--restart",
    )
    assert authority._slurm_policy_argv(controller, "apply")[-2:] == (
        "--restart",
        "--apply-accounting",
    )
    check_argv = authority._slurm_policy_argv(checked, "node-check")
    assert check_argv[4:8] == (
        "--profile",
        f"{candidate}/deploy/slurm/developer-sandboxes/gb10.toml",
        "--candidate-sha",
        SHA,
    )
    assert check_argv[8:10] == ("--candidate-bindings-json", bindings_json)
    assert check_argv[-2:] == ("--sandbox", "qianyi")
    for argv in (
        authority._slurm_policy_argv(compute, "apply"),
        authority._slurm_policy_argv(controller, "apply"),
        check_argv,
    ):
        assert "--root" not in argv
        assert "--account" not in argv
        assert "--path" not in argv
    with pytest.raises(authority.NodeAuthorityError, match="command binding"):
        authority._slurm_policy_argv(compute, "rollback")
    with pytest.raises(authority.NodeAuthorityError, match="command binding"):
        authority._slurm_policy_argv(compute, "shell")


def test_slurm_rollback_requires_exact_current_journal_and_snapshot_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transactions = tmp_path / "transactions"
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    journal_path = transactions / "trt-oldlab.json"
    journal = _slurm_policy_journal(snapshot, node="oldlab-2")
    manifest = _slurm_snapshot_manifest()
    snapshot.mkdir(parents=True)
    (snapshot / "manifest.json").write_bytes(manifest)
    binding = (
        "slurm-policy-v1:trt-oldlab:"
        + hashlib.sha256(journal).hexdigest()
        + ":"
        + _slurm_archive_identity(manifest)
    )
    parsed = authority._parse_request(
        _request(
            action="slurm-rollback",
            node="oldlab-2",
            prior_request_id="c" * 64,
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    monkeypatch.setattr(authority, "SLURM_TRANSACTION_ROOT", transactions)
    monkeypatch.setattr(authority, "SLURM_SNAPSHOT_ROOT", snapshot.parent)
    monkeypatch.setattr(authority, "SLURM_STATE_ROOT", tmp_path)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    payloads = {
        journal_path: journal,
        snapshot / "manifest.json": manifest,
    }
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, **_kwargs: payloads[path],
    )
    apply_request = authority._parse_request(
        _request(action="slurm-node-converge", node="oldlab-2"),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    assert (
        authority._slurm_policy_binding(
            apply_request,
            {
                "cluster": "trt-oldlab",
                "phase": "committed",
                "journal": str(journal_path),
                "snapshot": str(snapshot),
            },
            snapshot_field="snapshot",
        )
        == binding
    )
    authority._validate_prior_slurm_binding(
        parsed,
        {"request_id": apply_request.request_id, "inner_receipt": binding},
    )

    payloads[journal_path] = journal.replace(b'"phase":"committed"', b'"phase":"verified"')
    with pytest.raises(authority.NodeAuthorityError, match="advanced"):
        authority._validate_prior_slurm_binding(
            parsed,
            {"request_id": apply_request.request_id, "inner_receipt": binding},
        )


def test_slurm_rollback_accepts_exact_rolled_back_journal_for_safe_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transactions = tmp_path / "transactions"
    recovery = tmp_path / "snapshots" / "20260728T010204.000000Z"
    target = tmp_path / "snapshots" / "20260728T010203.000000Z"
    journal_path = transactions / "trt-oldlab.json"
    transactions.mkdir()
    recovery.mkdir(parents=True)
    target.mkdir()
    manifest = _slurm_snapshot_manifest()
    (recovery / "manifest.json").write_bytes(manifest)
    (target / "manifest.json").write_bytes(manifest)
    request = authority._parse_request(
        _request(
            action="slurm-rollback",
            node="oldlab-2",
            prior_request_id="c" * 64,
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    journal = _slurm_policy_journal(
        recovery,
        node="oldlab-2",
        operation="rollback",
        rollback_target=target,
    ).replace(b'"phase":"committed"', b'"phase":"rolled_back"')
    journal_path.write_bytes(journal)
    binding = (
        "slurm-policy-v1:trt-oldlab:"
        + "f" * 64
        + ":"
        + _slurm_archive_identity(manifest)
    )
    monkeypatch.setattr(authority, "SLURM_TRANSACTION_ROOT", transactions)
    monkeypatch.setattr(authority, "SLURM_SNAPSHOT_ROOT", target.parent)
    monkeypatch.setattr(authority, "SLURM_STATE_ROOT", tmp_path)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, **_kwargs: path.read_bytes(),
    )

    authority._validate_prior_slurm_binding(
        request,
        {"request_id": "d" * 64, "inner_receipt": binding},
    )

    drifted = json.loads(journal)
    drifted["candidate_set_generation"] = 2
    journal_path.write_bytes(authority._canonical(drifted))
    with pytest.raises(authority.NodeAuthorityError, match="binding"):
        authority._validate_prior_slurm_binding(
            request,
            {"request_id": "d" * 64, "inner_receipt": binding},
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "host",
        "slurm-node",
        "restart",
        "snapshot",
        "accounting",
        "transaction-id",
        "generation",
        "convergence-id",
        "payload-digest",
        "timestamp",
        "noncanonical",
    ],
)
def test_slurm_policy_journal_binding_is_canonical_closed_and_host_bound(
    tmp_path: Path,
    mutation: str,
) -> None:
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    request = authority._parse_request(
        _request(action="slurm-node-converge", node="oldlab-2"),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    payload = json.loads(_slurm_policy_journal(snapshot, node="oldlab-2"))
    if mutation == "extra":
        payload["foreign"] = True
    elif mutation == "host":
        payload["host"] = authority.NODE_HOSTNAMES["oldlab-3"]
    elif mutation == "slurm-node":
        payload["slurm_node"] = "oldlab-3"
    elif mutation == "restart":
        payload["restart"] = False
    elif mutation == "snapshot":
        payload["snapshot"] = str(snapshot.parent / "foreign")
    elif mutation == "accounting":
        payload["apply_accounting"] = True
        payload["accounting_snapshot"] = str(snapshot / "accounting-cas.json")
    elif mutation == "transaction-id":
        payload["transaction_id"] = "f" * 64
    elif mutation == "generation":
        payload["candidate_set_generation"] = 2
    elif mutation == "convergence-id":
        payload["candidate_set_convergence_id"] = "f" * 64
    elif mutation == "payload-digest":
        payload["candidate_set_payload_sha256"] = "f" * 64
    elif mutation == "timestamp":
        payload["updated_at"] = "2026-07-28T00:00:00+00:00"
    raw = (
        json.dumps(payload, sort_keys=True).encode("ascii") + b"\n"
        if mutation == "noncanonical"
        else authority._canonical(payload)
    )

    with pytest.raises(authority.NodeAuthorityError, match="Slurm policy"):
        authority._slurm_policy_journal_payload(
            request,
            raw,
            operation="apply",
            snapshot=snapshot,
            require_accounting=False,
        )


def test_slurm_policy_rollback_journal_binds_exact_restored_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    target = tmp_path / "snapshots" / "20260727T010203.000000Z"
    request = authority._parse_request(
        _request(
            action="slurm-rollback",
            node="oldlab-2",
            prior_request_id="c" * 64,
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    raw = _slurm_policy_journal(
        snapshot,
        node="oldlab-2",
        operation="rollback",
        rollback_target=target,
    )
    monkeypatch.setattr(
        authority,
        "_validated_slurm_snapshot_path",
        lambda value: Path(str(value)),
    )

    authority._slurm_policy_journal_payload(
        request,
        raw,
        operation="rollback",
        snapshot=snapshot,
        require_accounting=False,
        rollback_target=target,
    )
    with pytest.raises(authority.NodeAuthorityError, match="rollback target"):
        authority._slurm_policy_journal_payload(
            request,
            raw,
            operation="rollback",
            snapshot=snapshot,
            require_accounting=False,
            rollback_target=target.parent / "foreign",
        )


def test_slurm_snapshot_manifest_is_closed_and_archive_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    relative = authority.SLURM_SNAPSHOT_RELATIVE_PATHS[0]
    archive = b"exact archived bytes\n"
    manifest = _slurm_snapshot_manifest(
        present_path=relative,
        present_bytes=archive,
    )
    reads: list[tuple[Path, int]] = []
    payloads = {
        snapshot / "manifest.json": manifest,
        snapshot / relative: archive,
    }

    def safe_file(path: Path, *, mode: int, **_kwargs: object) -> bytes:
        reads.append((path, mode))
        return payloads[path]

    monkeypatch.setattr(authority, "_safe_root_file", safe_file)

    assert authority._slurm_snapshot_manifest_bytes(snapshot) == manifest
    assert reads == [
        (snapshot / "manifest.json", 0o600),
        (snapshot / relative, 0o600),
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-row",
        "foreign-key",
        "absent-metadata",
        "wrong-size",
        "wrong-digest",
        "writable-mode",
        "foreign-owner",
    ],
)
def test_slurm_snapshot_manifest_rejects_nonclosed_or_drifted_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    relative = authority.SLURM_SNAPSHOT_RELATIVE_PATHS[0]
    archive = b"exact archived bytes\n"
    manifest = json.loads(
        _slurm_snapshot_manifest(
            present_path=relative,
            present_bytes=archive,
        ),
    )
    if mutation == "missing-row":
        manifest["files"].pop()
    elif mutation == "foreign-key":
        manifest["files"][0]["foreign"] = True
    elif mutation == "absent-metadata":
        row = manifest["files"][1]
        row["mode"] = 0o644
    elif mutation == "wrong-size":
        manifest["files"][0]["size"] = len(archive) + 1
    elif mutation == "wrong-digest":
        manifest["files"][0]["sha256"] = "f" * 64
    elif mutation == "writable-mode":
        manifest["files"][0]["mode"] = 0o666
    else:
        manifest["files"][0]["uid"] = 1000
    encoded = (json.dumps(manifest, sort_keys=True) + "\n").encode("ascii")
    payloads = {
        snapshot / "manifest.json": encoded,
        snapshot / relative: archive,
    }
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda path, **_kwargs: payloads[path],
    )

    with pytest.raises(authority.NodeAuthorityError, match=r"snapshot|archive"):
        authority._slurm_snapshot_manifest_bytes(snapshot)


def test_slurm_snapshot_accounting_archive_is_controller_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    snapshot.mkdir(parents=True)
    manifest = _slurm_snapshot_manifest()
    (snapshot / "manifest.json").write_bytes(manifest)
    accounting = authority._canonical(
        {
            "schema_version": 1,
            "cluster": "trt-oldlab",
            "before": {},
            "desired": {},
        },
    )
    (snapshot / "accounting-cas.json").write_bytes(accounting)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)

    def safe_file(path: Path, **_kwargs: object) -> bytes:
        try:
            return path.read_bytes()
        except OSError as exc:
            raise authority.NodeAuthorityError("test snapshot is unavailable") from exc

    monkeypatch.setattr(authority, "_safe_root_file", safe_file)
    authority._validate_slurm_snapshot_archive_inventory(
        snapshot,
        manifest,
        journal={
            "apply_accounting": True,
            "accounting_snapshot": str(snapshot / "accounting-cas.json"),
        },
        cluster="trt-oldlab",
        require_accounting=True,
    )

    with pytest.raises(authority.NodeAuthorityError, match="unexpected"):
        authority._validate_slurm_snapshot_archive_inventory(
            snapshot,
            manifest,
            journal={
                "apply_accounting": False,
                "accounting_snapshot": None,
            },
            cluster="trt-oldlab",
            require_accounting=False,
        )

    (snapshot / "accounting-cas.json").unlink()
    with pytest.raises(authority.NodeAuthorityError, match="unavailable"):
        authority._validate_slurm_snapshot_archive_inventory(
            snapshot,
            manifest,
            journal={
                "apply_accounting": True,
                "accounting_snapshot": str(snapshot / "accounting-cas.json"),
            },
            cluster="trt-oldlab",
            require_accounting=True,
        )


def test_slurm_receipt_cas_rejects_replaced_accounting_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transactions = tmp_path / "transactions"
    snapshot = tmp_path / "snapshots" / "20260728T010203.000000Z"
    journal_path = transactions / "trt-oldlab.json"
    transactions.mkdir()
    snapshot.mkdir(parents=True)
    manifest = _slurm_snapshot_manifest()
    accounting = authority._canonical(
        {
            "schema_version": 1,
            "cluster": "trt-oldlab",
            "before": {},
            "desired": {},
        },
    )
    journal = _slurm_policy_journal(
        snapshot,
        node="oldlab-1",
        accounting=True,
    )
    journal_path.write_bytes(journal)
    (snapshot / "manifest.json").write_bytes(manifest)
    (snapshot / "accounting-cas.json").write_bytes(accounting)
    monkeypatch.setattr(authority, "SLURM_TRANSACTION_ROOT", transactions)
    monkeypatch.setattr(authority, "SLURM_SNAPSHOT_ROOT", snapshot.parent)
    monkeypatch.setattr(authority, "SLURM_STATE_ROOT", tmp_path)
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)

    def safe_file(path: Path, **_kwargs: object) -> bytes:
        try:
            return path.read_bytes()
        except OSError as exc:
            raise authority.NodeAuthorityError("test snapshot is unavailable") from exc

    monkeypatch.setattr(authority, "_safe_root_file", safe_file)
    apply_request = authority._parse_request(
        _request(action="slurm-controller-converge", node="oldlab-1"),
        verb="transact",
        policy=_policy("oldlab-1"),
    )
    binding = authority._slurm_policy_binding(
        apply_request,
        {
            "cluster": "trt-oldlab",
            "phase": "committed",
            "journal": str(journal_path),
            "snapshot": str(snapshot),
        },
        snapshot_field="snapshot",
    )
    assert binding.endswith(_slurm_archive_identity(manifest, accounting=accounting))
    rollback_request = authority._parse_request(
        _request(
            action="slurm-rollback",
            node="oldlab-1",
            prior_request_id="c" * 64,
        ),
        verb="transact",
        policy=_policy("oldlab-1"),
    )
    authority._validate_prior_slurm_binding(
        rollback_request,
        {"request_id": apply_request.request_id, "inner_receipt": binding},
    )

    replacement = authority._canonical(
        {
            "schema_version": 1,
            "cluster": "trt-oldlab",
            "before": {},
            "desired": {"foreign": {}},
        },
    )
    (snapshot / "accounting-cas.json").write_bytes(replacement)
    with pytest.raises(authority.NodeAuthorityError, match="snapshot identity drifted"):
        authority._validate_prior_slurm_binding(
            rollback_request,
            {"request_id": apply_request.request_id, "inner_receipt": binding},
        )


def test_link_authority_actions_are_node_domain_and_payload_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fleet_digest = "sha256:" + "a" * 64
    fleet = authority._canonical(
        {
            "schema_version": 1,
            "fleet": "proof",
            "payload_sha256": fleet_digest,
        },
    )
    persisted = authority._parse_request(
        _request(
            action="persist-fleet-attestation",
            node="oldlab-2",
            payload_kind="fleet-attestation-json",
            payload_bytes=fleet,
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    calls: list[tuple[tuple[str, ...], bytes]] = []
    monkeypatch.setattr(
        authority,
        "_run_fixed_input",
        lambda argv, payload: (
            calls.append((tuple(argv), payload))
            or {
                "schema_version": 1,
                "sandbox": "qianyi",
                "candidate_sha": SHA,
                "path": (
                    f"/var/lib/loom-developer-sandbox-links/attestations/qianyi/{SHA}/fleet.json"
                ),
                "payload_sha256": fleet_digest,
            }
        ),
    )
    result, receipt = authority._execute_request(
        persisted,
        _policy(str(persisted.payload["node"])),
    )
    assert receipt is None
    assert result["payload_sha256"] == fleet_digest
    assert calls == [
        (
            (
                "/usr/bin/python3",
                str(authority.SOURCE_ROOT / authority.REMOTE_LINK_HOST_RELATIVE),
                "persist-attestation",
                "--sandbox",
                "qianyi",
                "--candidate-sha",
                SHA,
                "--execute",
            ),
            fleet,
        ),
    ]
    monkeypatch.setattr(
        authority,
        "_run_fixed_input",
        lambda _argv, _payload: {
            "schema_version": 1,
            "sandbox": "qianyi",
            "candidate_sha": SHA,
            "path": (f"/var/lib/loom-developer-sandbox-links/attestations/qianyi/{SHA}/fleet.json"),
            "payload_sha256": "sha256:" + "b" * 64,
        },
    )
    with pytest.raises(authority.NodeAuthorityError, match="readback"):
        authority._execute_request(
            persisted,
            _policy(str(persisted.payload["node"])),
        )

    noncanonical = json.dumps(
        {"schema_version": 1, "fleet": "proof"},
        indent=2,
    ).encode()
    with pytest.raises(authority.NodeAuthorityError, match="fleet attestation"):
        authority._parse_request(
            _request(
                action="persist-fleet-attestation",
                node="oldlab-2",
                payload_kind="fleet-attestation-json",
                payload_bytes=noncanonical,
            ),
            verb="transact",
            policy=_policy("oldlab-2"),
        )
    oversized = authority._canonical(
        {"fleet": "x" * authority.MAX_FLEET_ATTESTATION_BYTES},
    )
    with pytest.raises(authority.NodeAuthorityError, match="fleet attestation"):
        authority._parse_request(
            _request(
                action="persist-fleet-attestation",
                node="oldlab-2",
                payload_kind="fleet-attestation-json",
                payload_bytes=oversized,
            ),
            verb="transact",
            policy=_policy("oldlab-2"),
        )
    with pytest.raises(authority.NodeAuthorityError, match="server authority"):
        authority._parse_request(
            _request(
                action="persist-fleet-attestation",
                node="oldlab-1",
                payload_kind="fleet-attestation-json",
                payload_bytes=fleet,
            ),
            verb="transact",
            policy=_policy("oldlab-1"),
        )
    with pytest.raises(authority.NodeAuthorityError, match="client inspection"):
        authority._parse_request(
            _request(
                action="inspect-link-client",
                node="trt-gb10-1",
                domain="oldlab",
            ),
            verb="check",
            policy=_policy("trt-gb10-1"),
        )


@pytest.mark.parametrize(
    ("action", "expected_command"),
    [
        ("inspect-link-client", "check-client"),
        ("inspect-link-server", "check-server"),
    ],
)
def test_link_inspections_call_only_the_fixed_local_helper(
    action: str,
    expected_command: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = "oldlab-2"
    request = authority._parse_request(
        _request(action=action, node=node),
        verb="check",
        policy=_policy(node),
    )
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        authority,
        "_run_fixed",
        lambda argv: calls.append(tuple(argv)) or {"status": "checked"},
    )

    assert authority._execute_check(
        request,
        _policy(str(request.payload["node"])),
    ) == {"status": "checked"}
    expected = [
        "/usr/bin/python3",
        str(authority.SOURCE_ROOT / authority.REMOTE_LINK_HOST_RELATIVE),
        expected_command,
        "--sandbox",
        "qianyi",
        "--candidate-sha",
        SHA,
    ]
    if action == "inspect-link-client":
        expected.extend(("--node", node))
    assert calls == [tuple(expected)]


def test_runtime_proof_check_rejects_artifact_source_cross_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id = f"runtime-proof/v1/qianyi/{SHA}/{TREE}/artifact/oldlab.json"
    request = authority._parse_request(
        _request(
            action="export-runtime-proof-artifact",
            node="oldlab-2",
            payload_kind="runtime-proof-artifact-id",
            payload_bytes=artifact_id.encode("ascii"),
        ),
        verb="check",
        policy=_policy("oldlab-2"),
    )
    monkeypatch.setattr(
        authority,
        "_run_fixed",
        lambda _argv: pytest.fail("cross-bound artifact reached helper"),
    )

    with pytest.raises(authority.NodeAuthorityError, match="artifact identity"):
        authority._execute_check(
            request,
            _policy(str(request.payload["node"])),
        )


def _live_collection_payload() -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.live-overlap-collection",
            "collection_id": "00000000-0000-0000-0000-000000000001",
            "candidate_tree": "d" * 40,
            "job_id": "1234",
        },
    )


def _live_job_payload(*, user: str = "loom-sandbox-qianyi") -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.live-slurm-request",
            "source_host": "trt-gb10-1",
            "sandbox": "qianyi",
            "pool": "gb10",
            "candidate_sha": SHA,
            "candidate_tree": "d" * 40,
            "job_id": "1234",
            "account": "loom-dev-qianyi",
            "user": user,
            "job_name": f"loom-sandbox-qianyi-{SHA[:12]}-trt-gb10-1",
            "node": "trt-gb10-1",
            "requested_cpus": 20,
            "requested_memory_mib": 115000,
            "job_pids_max": 65536,
            "requested_gpus": 1,
            "requested_gpu_tres": "gpu:1",
        },
    )


def test_live_overlap_collection_is_only_a_closed_oldlab2_transaction() -> None:
    request = authority._parse_request(
        _request(
            action="collect-live-overlap",
            node="oldlab-2",
            domain="gb10",
            payload_kind="live-overlap-collection-json",
            payload_bytes=_live_collection_payload(),
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    assert request.action == "collect-live-overlap"

    with pytest.raises(authority.NodeAuthorityError, match="OLDLAB2-only"):
        authority._parse_request(
            _request(
                action="collect-live-overlap",
                node="oldlab-1",
                domain="oldlab",
                payload_kind="live-overlap-collection-json",
                payload_bytes=_live_collection_payload(),
            ),
            verb="transact",
            policy=_policy("oldlab-1"),
        )


def test_live_overlap_slurm_readback_is_controller_and_service_user_bound() -> None:
    request = authority._parse_request(
        _request(
            action="observe-live-overlap-job",
            node="trt-gb10-1",
            domain="gb10",
            payload_kind="live-overlap-job-json",
            payload_bytes=_live_job_payload(),
        ),
        verb="check",
        policy=_policy("trt-gb10-1"),
    )
    assert request.action == "observe-live-overlap-job"

    with pytest.raises(authority.NodeAuthorityError, match="Slurm payload"):
        authority._parse_request(
            _request(
                action="observe-live-overlap-job",
                node="trt-gb10-1",
                domain="gb10",
                payload_kind="live-overlap-job-json",
                payload_bytes=_live_job_payload(user="qianyi"),
            ),
            verb="check",
            policy=_policy("trt-gb10-1"),
        )
    with pytest.raises(authority.NodeAuthorityError, match="source-host-only"):
        authority._parse_request(
            _request(
                action="observe-live-overlap-job",
                node="trt-gb10-2",
                domain="gb10",
                payload_kind="live-overlap-job-json",
                payload_bytes=_live_job_payload(),
            ),
            verb="check",
            policy=_policy("trt-gb10-2"),
        )


def test_live_overlap_actions_call_only_the_fixed_installed_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collection = authority._parse_request(
        _request(
            action="collect-live-overlap",
            node="oldlab-2",
            domain="gb10",
            payload_kind="live-overlap-collection-json",
            payload_bytes=_live_collection_payload(),
        ),
        verb="transact",
        policy=_policy("oldlab-2"),
    )
    observed = authority._parse_request(
        _request(
            action="observe-live-overlap-job",
            node="trt-gb10-1",
            domain="gb10",
            payload_kind="live-overlap-job-json",
            payload_bytes=_live_job_payload(),
        ),
        verb="check",
        policy=_policy("trt-gb10-1"),
    )
    calls: list[tuple[tuple[str, ...], bytes]] = []

    def fixed_input(argv: Sequence[str], payload: bytes) -> dict[str, object]:
        calls.append((tuple(argv), payload))
        if "collect" in argv:
            return {
                "schema_version": 1,
                "kind": "loom.developer-sandbox.live-overlap-result",
                "path": f"/var/lib/loom-developer-sandbox-live-authority/overlap/gb10/qianyi/{SHA}/1234.json",
                "payload_sha256": "e" * 64,
                "job_id": "1234",
                "observation_sequence": 7,
                "observed_at": "2026-07-29T12:00:00Z",
            }
        return {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.live-slurm-observation",
            "source_host": "trt-gb10-1",
            "sandbox": "qianyi",
            "pool": "gb10",
            "candidate_sha": SHA,
        }

    monkeypatch.setattr(authority, "_run_fixed_input", fixed_input)
    result, inner = authority._execute_request(
        collection,
        _policy(str(collection.payload["node"])),
    )
    assert inner == result["path"]
    assert authority._execute_check(
        observed,
        _policy(str(observed.payload["node"])),
    )["kind"] == (
        "loom.developer-sandbox.live-slurm-observation"
    )
    assert calls[0][0] == (
        "/usr/bin/python3",
        "-I",
        str(authority.SOURCE_ROOT / authority.LIVE_AUTHORITY_RELATIVE),
        "collect",
        "--sandbox",
        "qianyi",
        "--pool",
        "gb10",
        "--candidate-sha",
        SHA,
        "--authority-tree",
        TREE,
    )
    assert calls[1][0] == (
        "/usr/bin/python3",
        "-I",
        str(authority.SOURCE_ROOT / authority.LIVE_AUTHORITY_RELATIVE),
        "observe-slurm-job",
    )


def test_runtime_invoker_requires_exact_sudo_identity_and_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Account:
        pw_uid = 1000
        pw_gid = 1001

    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    monkeypatch.setattr(authority.pwd, "getpwnam", lambda _name: Account())
    approved = {
        "SUDO_USER": "qianyi",
        "SUDO_UID": "1000",
        "SUDO_GID": "1001",
        "SUDO_COMMAND": f"{authority.LIBEXEC} transact",
    }
    authority._validate_invoker("transact", approved)

    for key, value in (
        ("SUDO_USER", "root"),
        ("SUDO_UID", "0"),
        ("SUDO_COMMAND", f"{authority.LIBEXEC} transact extra"),
    ):
        changed = dict(approved)
        changed[key] = value
        with pytest.raises(authority.NodeAuthorityError, match="not approved"):
            authority._validate_invoker("transact", changed)


def test_persistent_root_gate_accepts_docker_chroot_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid1_comm = tmp_path / "comm"
    pid1_comm.write_text("systemd\n", encoding="ascii")
    monkeypatch.setattr(authority.os, "getuid", lambda: 0)
    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    monkeypatch.setenv("SUDO_USER", "qianyi")
    monkeypatch.setenv("container", "docker")
    monkeypatch.setattr(
        authority,
        "_git",
        lambda *args: SHA if args[-1] == "HEAD" else TREE if args[-1] == "HEAD^{tree}" else "",
    )
    monkeypatch.setattr(authority, "_hostname", lambda: "trt-eai-oldlab-1")
    monkeypatch.setattr(authority, "_node_for_hostname", lambda _host: "oldlab-1")

    assert (
        authority._validate_persistent_root_source(
            SHA,
            TREE,
            root_path=tmp_path,
            pid1_root_path=tmp_path,
            pid1_comm_path=pid1_comm,
        )
        == "oldlab-1"
    )


def test_persistent_root_gate_rejects_nonroot_or_nonhost_systemd_view(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid1_comm = tmp_path / "comm"
    pid1_comm.write_text("systemd\n", encoding="ascii")
    other_root = tmp_path / "other-root"
    other_root.mkdir()
    monkeypatch.setattr(authority.os, "getuid", lambda: 1000)
    monkeypatch.setattr(authority.os, "geteuid", lambda: 1000)
    with pytest.raises(authority.NodeAuthorityError, match="host-root authority"):
        authority._validate_persistent_root_source(
            SHA,
            TREE,
            root_path=tmp_path,
            pid1_root_path=tmp_path,
            pid1_comm_path=pid1_comm,
        )

    monkeypatch.setattr(authority.os, "getuid", lambda: 0)
    monkeypatch.setattr(authority.os, "geteuid", lambda: 0)
    with pytest.raises(authority.NodeAuthorityError, match="systemd view is invalid"):
        authority._validate_persistent_root_source(
            SHA,
            TREE,
            root_path=tmp_path,
            pid1_root_path=other_root,
            pid1_comm_path=pid1_comm,
        )


def test_validate_install_reuses_persistent_root_gate_and_exact_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy()
    installs = ({"path": "/etc/systemd/system/example.service"},)
    monkeypatch.setattr(authority, "_require_persistent_root_view", lambda: None)
    monkeypatch.setattr(authority, "_read_policy", lambda: policy)
    monkeypatch.setattr(authority, "_validate_runtime_assets", lambda current: installs)

    assert authority.validate_install() == {
        "schema_version": authority.SCHEMA_VERSION,
        "action": "validate-install",
        "node": policy.node,
        "source_sha": policy.source_sha,
        "source_tree": policy.source_tree,
        "system_installs": list(installs),
        "status": "succeeded",
    }


def test_source_asset_rejects_unsafe_parent_and_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unsafe = tmp_path / "unsafe"
    unsafe.mkdir(mode=0o777)
    unsafe.chmod(0o777)
    repo = unsafe / "repo"
    asset = repo / "scripts/asset.py"
    asset.parent.mkdir(parents=True)
    asset.write_bytes(b"payload")
    asset.chmod(0o600)
    monkeypatch.setattr(authority, "REPO_ROOT", repo)
    with pytest.raises(authority.NodeAuthorityError, match="parent is unsafe"):
        authority._source_asset(Path("scripts/asset.py"), expected_uid=os.getuid())

    safe = tmp_path / "safe"
    real = safe / "real"
    real.mkdir(parents=True)
    linked = safe / "linked"
    linked.symlink_to(real, target_is_directory=True)
    linked_asset = real / "asset.py"
    linked_asset.write_bytes(b"payload")
    linked_asset.chmod(0o600)
    monkeypatch.setattr(authority, "REPO_ROOT", linked)
    with pytest.raises(authority.NodeAuthorityError):
        authority._source_asset(Path("asset.py"), expected_uid=os.getuid())


@pytest.mark.parametrize("mutation", ["rename", "rewrite"])
def test_source_asset_rejects_post_read_identity_or_byte_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir(mode=0o700)
    asset = repo / "asset.py"
    asset.write_bytes(b"original")
    asset.chmod(0o600)
    replacement = repo / "replacement.py"
    replacement.write_bytes(b"changed!")
    replacement.chmod(0o600)
    monkeypatch.setattr(authority, "REPO_ROOT", repo)
    original_read = authority._read_fd_twice
    original_safe_source_directory = authority._safe_source_directory
    trusted_test_ancestors = frozenset(
        authority._metadata_identity(parent.stat()) for parent in repo.absolute().parents
    )
    monkeypatch.setattr(
        authority,
        "_safe_source_directory",
        lambda metadata, *, expected_uid: (
            original_safe_source_directory(metadata, expected_uid=expected_uid)
            or authority._metadata_identity(metadata) in trusted_test_ancestors
        ),
    )

    def mutate_after_read(
        descriptor: int,
        *,
        limit: int,
        error: str,
    ) -> bytes:
        payload = original_read(descriptor, limit=limit, error=error)
        if mutation == "rename":
            replacement.replace(asset)
        else:
            asset.write_bytes(b"rewritten-content")
            asset.chmod(0o600)
        return payload

    monkeypatch.setattr(authority, "_read_fd_twice", mutate_after_read)
    with pytest.raises(
        authority.NodeAuthorityError,
        match=r"metadata is unsafe|changed during verification",
    ):
        authority._source_asset(Path("asset.py"), expected_uid=os.getuid())


def test_exact_source_assets_bind_copied_bytes_to_commit_blob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authority, "SOURCE_ASSETS", (Path("asset.py"),))
    monkeypatch.setattr(authority, "_source_asset", lambda _relative: b"changed")
    monkeypatch.setattr(authority, "_git_bytes", lambda *_args: b"committed")

    with pytest.raises(authority.NodeAuthorityError, match="exact candidate"):
        authority._exact_source_assets(SHA, TREE)

    source = Path(authority.__file__).read_text(encoding="utf-8")
    assert "docker run" not in source
    assert "--privileged" not in source


def _upgrade_snapshot(tmp_path: Path, new_tree: str = "d" * 40) -> authority.UpgradeSnapshot:
    return authority.UpgradeSnapshot(
        upgrade_id="upgrade-test",
        root=tmp_path / "snapshot",
        manifest=tmp_path / "snapshot/manifest.json",
        entries=(),
        old_source_sha=SHA,
        old_source_tree=TREE,
        new_source_sha="e" * 40,
        new_source_tree=new_tree,
        high_value_state={
            "journal_sha256": "1" * 64,
            "receipts": {},
        },
    )


def _prepare_upgrade_mocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    authority.AuthorityPolicy,
    authority.AuthorityPolicy,
    authority.UpgradeSnapshot,
    list[str],
]:
    old = _policy()
    new = authority.AuthorityPolicy(
        source_sha="e" * 40,
        source_tree="d" * 40,
        node="oldlab-1",
        asset_sha256={str(path): "f" * 64 for path in authority.SOURCE_ASSETS},
    )
    snapshot = _upgrade_snapshot(tmp_path)
    events: list[str] = []
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    policies = iter((old, new))
    monkeypatch.setattr(
        authority,
        "_validate_persistent_root_source",
        lambda _sha, _tree: "oldlab-1",
    )
    monkeypatch.setattr(
        authority,
        "_exact_source_assets",
        lambda _sha, _tree: {
            str(relative): f"new:{relative}".encode() for relative in authority.SOURCE_ASSETS
        },
    )
    monkeypatch.setattr(
        authority,
        "_open_lock",
        lambda **_kwargs: os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(
        authority,
        "_ensure_upgrade_state",
        lambda: events.append("upgrade-state"),
    )
    monkeypatch.setattr(authority, "_recover_upgrade_if_needed", lambda: None)
    monkeypatch.setattr(authority, "_read_policy", lambda: next(policies))
    monkeypatch.setattr(
        authority,
        "_validate_runtime_assets",
        lambda policy, **_kwargs: (
            events.append(f"validate:{policy.source_tree}")
            or tuple({} for _item in authority.SYSTEM_INSTALL_ASSETS)
        ),
    )
    monkeypatch.setattr(
        authority,
        "_high_value_state_identity",
        lambda: {"journal_sha256": "1", "receipts": {"r": "2"}},
    )
    monkeypatch.setattr(
        authority,
        "_prepare_upgrade_snapshot",
        lambda *_args, **_kwargs: events.append("snapshot") or snapshot,
    )
    monkeypatch.setattr(
        authority,
        "_write_upgrade_active",
        lambda _snapshot, phase: events.append(f"active:{phase}"),
    )
    monkeypatch.setattr(
        authority,
        "_upgrade_journal_append",
        lambda record: events.append(f"journal:{record['phase']}"),
    )
    monkeypatch.setattr(
        authority,
        "_unlink_root_file",
        lambda *_args, **_kwargs: events.append("admission-disabled") or b"old",
    )
    monkeypatch.setattr(
        authority,
        "_system_sudoers_paths",
        lambda: frozenset(
            {
                tmp_path / "platform-health.sudoers",
                tmp_path / "staging-pressure.sudoers",
                tmp_path / "staging-external.sudoers",
            },
        ),
    )
    monkeypatch.setattr(
        authority,
        "_ensure_root_directory",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        authority,
        "_atomic_replace",
        lambda path, *_args, **_kwargs: events.append(f"replace:{path}"),
    )
    monkeypatch.setattr(
        authority,
        "_atomic_install",
        lambda path, *_args, **_kwargs: events.append(f"install:{path}") or True,
    )
    monkeypatch.setattr(
        authority,
        "_system_install_assets",
        lambda _assets, *, replace: (
            events.append(f"system-install:{replace}")
            or tuple(
                {
                    "path": str(target),
                    "mode": f"{mode:04o}",
                    "sha256": "9" * 64,
                }
                for _relative, target, mode, _parent_mode in authority.SYSTEM_INSTALL_ASSETS
            )
        ),
    )
    monkeypatch.setattr(
        authority.subprocess,
        "run",
        lambda *_args, **_kwargs: type("Result", (), {"returncode": 0})(),
    )
    monkeypatch.setattr(
        authority,
        "_remove_upgrade_active",
        lambda: events.append("active-removed"),
    )
    monkeypatch.setattr(
        authority,
        "_restore_upgrade_snapshot",
        lambda _snapshot: events.append("restored"),
    )
    return old, new, snapshot, events


def test_upgrade_disables_admission_replaces_atomically_and_preserves_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old, _new, snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)

    report = authority.upgrade("e" * 40, "d" * 40)

    assert report["changed"] is True
    assert report["snapshot"] == str(snapshot.root)
    disabled = events.index("admission-disabled")
    replaced = [index for index, event in enumerate(events) if event.startswith("replace:")]
    sudoers_install = events.index(f"install:{authority.SUDOERS}")
    assert replaced and disabled < min(replaced) < max(replaced) < sudoers_install
    assert events[-3:] == ["active:committed", "journal:committed", "active-removed"]
    assert "restored" not in events


def test_upgrade_after_reboot_recreates_private_stage_root_after_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    stage_root = tmp_path / "run" / "loom-developer-sandbox-node-authority"
    stage_root.parent.mkdir(mode=0o755)
    lock = tmp_path / "lock"
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)
    monkeypatch.setattr(
        authority,
        "_open_lock",
        lambda **_kwargs: events.append("lock") or os.open(lock, os.O_RDONLY),
    )

    def ensure_directory(
        path: Path,
        *,
        mode: int,
        parent_mode: int,
    ) -> bool:
        if path != stage_root:
            return False
        events.append("stage-root")
        assert mode == 0o700
        assert parent_mode == 0o755
        path.mkdir(mode=mode)
        path.chmod(mode)
        return True

    monkeypatch.setattr(authority, "_ensure_root_directory", ensure_directory)

    report = authority.upgrade("e" * 40, "d" * 40)

    assert report["changed"] is True
    assert events[:3] == ["lock", "stage-root", "upgrade-state"]
    assert stage_root.is_dir()
    assert stat.S_IMODE(stage_root.stat().st_mode) == 0o700


def test_upgrade_failure_restores_snapshot_and_records_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)

    def replace(path: Path, *_args: object, **_kwargs: object) -> None:
        events.append(f"replace:{path}")
        if path == authority.LIBEXEC:
            raise authority.NodeAuthorityError("injected replacement failure")

    monkeypatch.setattr(authority, "_atomic_replace", replace)

    with pytest.raises(authority.NodeAuthorityError, match="rolled back"):
        authority.upgrade("e" * 40, "d" * 40)

    assert "restored" in events
    assert "journal:rolled-back" in events
    assert events[-1] == "active-removed"
    assert f"install:{authority.SUDOERS}" not in events


def test_upgrade_creates_all_source_parents_and_removes_them_after_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_parents = (
        Path("src"),
        Path("src/loom_cli"),
        Path("src/loom_cli/rollout"),
    )
    monkeypatch.setattr(authority, "SOURCE_ROOT", source_root)
    monkeypatch.setattr(authority, "SOURCE_ASSET_PARENT_PATHS", source_parents)

    ensured: list[Path] = []

    def ensure_directory(
        path: Path,
        *,
        mode: int,
        parent_mode: int,
    ) -> bool:
        if path == authority.STAGE_ROOT:
            assert mode == 0o700
            assert parent_mode == 0o755
            return False
        assert mode == 0o755
        assert parent_mode == 0o755
        path.mkdir()
        ensured.append(path)
        return True

    monkeypatch.setattr(authority, "_ensure_root_directory", ensure_directory)

    def replace(path: Path, *_args: object, **_kwargs: object) -> None:
        events.append(f"replace:{path}")
        if path == authority.LIBEXEC:
            raise authority.NodeAuthorityError("injected replacement failure")

    monkeypatch.setattr(authority, "_atomic_replace", replace)

    with pytest.raises(authority.NodeAuthorityError, match="rolled back"):
        authority.upgrade("e" * 40, "d" * 40)

    expected = [source_root / relative for relative in source_parents]
    assert ensured == expected
    assert all(not path.exists() for path in expected)


def test_upgrade_system_install_failure_restores_the_combined_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    monkeypatch.setattr(
        authority,
        "_system_install_assets",
        lambda _assets, *, replace: (_ for _ in ()).throw(
            authority.NodeAuthorityError(
                f"injected system install failure replace={replace}",
            ),
        ),
    )

    with pytest.raises(authority.NodeAuthorityError, match="rolled back"):
        authority.upgrade("e" * 40, "d" * 40)

    assert "restored" in events
    assert "journal:rolled-back" in events
    assert events[-1] == "active-removed"
    assert f"install:{authority.SUDOERS}" not in events


def test_upgrade_high_value_state_mismatch_forces_snapshot_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    identities = iter(
        (
            {"journal_sha256": "before", "receipts": {}},
            {"journal_sha256": "after", "receipts": {}},
            {"journal_sha256": "before", "receipts": {}},
        ),
    )
    monkeypatch.setattr(
        authority,
        "_high_value_state_identity",
        lambda: next(identities),
    )

    with pytest.raises(authority.NodeAuthorityError, match="rolled back"):
        authority.upgrade("e" * 40, "d" * 40)

    assert "restored" in events
    assert "journal:rolled-back" in events


def test_upgrade_rejects_foreign_installed_drift_before_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    monkeypatch.setattr(authority, "_read_policy", lambda: old)
    monkeypatch.setattr(
        authority,
        "_validate_runtime_assets",
        lambda _policy, **_kwargs: (_ for _ in ()).throw(
            authority.NodeAuthorityError("installed source drifted"),
        ),
    )

    with pytest.raises(authority.NodeAuthorityError, match="drifted"):
        authority.upgrade("e" * 40, "d" * 40)

    assert "snapshot" not in events
    assert "admission-disabled" not in events


def test_upgrade_journal_rejects_noncanonical_foreign_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        authority,
        "_safe_root_file",
        lambda *_args, **_kwargs: b'{"phase":"foreign"}\n',
    )
    with pytest.raises(authority.NodeAuthorityError, match="upgrade journal"):
        authority._validate_upgrade_journal()


def test_upgrade_same_sha_and_tree_is_idempotent_and_never_disables_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old, _new, _snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    monkeypatch.setattr(authority, "_read_policy", lambda: old)

    report = authority.upgrade(SHA, TREE)

    assert report["changed"] is False
    assert report["source_sha"] == SHA
    assert report["source_tree"] == TREE
    assert "snapshot" not in events
    assert "admission-disabled" not in events


def test_upgrade_same_tree_new_sha_rebinds_candidate_transactionally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old, _new, snapshot, events = _prepare_upgrade_mocks(tmp_path, monkeypatch)
    rebound = authority.AuthorityPolicy(
        source_sha="e" * 40,
        source_tree=TREE,
        node=old.node,
        asset_sha256=old.asset_sha256,
    )
    policies = iter((old, rebound))
    monkeypatch.setattr(authority, "_read_policy", lambda: next(policies))

    report = authority.upgrade("e" * 40, TREE)

    assert report["changed"] is True
    assert report["source_sha"] == "e" * 40
    assert report["source_tree"] == TREE
    assert report["snapshot"] == str(snapshot.root)
    assert "snapshot" in events
    assert "admission-disabled" in events
    assert events[-3:] == ["active:committed", "journal:committed", "active-removed"]


@pytest.mark.parametrize(
    ("phase", "expected"),
    [("assets-replaced", "rolled-back"), ("committed", "committed")],
)
def test_interrupted_upgrade_recovery_is_snapshot_and_state_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    expected: str,
) -> None:
    snapshot = _upgrade_snapshot(tmp_path)
    events: list[str] = []
    monkeypatch.setattr(
        authority,
        "_read_upgrade_active",
        lambda: (snapshot, phase),
    )
    monkeypatch.setattr(
        authority,
        "_restore_upgrade_snapshot",
        lambda _snapshot: events.append("restored"),
    )
    monkeypatch.setattr(
        authority,
        "_read_policy",
        lambda: authority.AuthorityPolicy(
            source_sha=snapshot.new_source_sha,
            source_tree=snapshot.new_source_tree,
            node="oldlab-1",
            asset_sha256={},
        ),
    )
    monkeypatch.setattr(authority, "_validate_runtime_assets", lambda _policy: None)
    monkeypatch.setattr(
        authority,
        "_high_value_state_identity",
        lambda: dict(snapshot.high_value_state),
    )
    monkeypatch.setattr(
        authority,
        "_upgrade_journal_append",
        lambda record: events.append(str(record["phase"])),
    )
    monkeypatch.setattr(
        authority,
        "_remove_upgrade_active",
        lambda: events.append("active-removed"),
    )

    assert authority._recover_upgrade_if_needed() == expected
    if phase == "committed":
        assert events == ["recovered-committed", "active-removed"]
    else:
        assert events == ["restored", "recovered-rolled-back", "active-removed"]


@pytest.mark.parametrize(
    ("phase", "verb", "stage_present"),
    [
        (phase, verb, stage_present)
        for phase in ("prepared", "admission-disabled", "assets-replaced", "committed")
        for verb in ("transact", "check")
        for stage_present in (False, True)
    ],
)
def test_runtime_fails_closed_for_every_active_upgrade_phase_without_state_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    verb: str,
    stage_present: bool,
) -> None:
    active = tmp_path / "upgrade-active.json"
    active.write_bytes(authority._canonical({"phase": phase}))
    active.chmod(0o600)
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    stage_root = tmp_path / "run" / "loom-developer-sandbox-node-authority"
    stage_root.parent.mkdir(mode=0o755)
    stage_before: tuple[int, bytes] | None = None
    if stage_present:
        stage_root.mkdir(mode=0o700)
        sentinel = stage_root / "sentinel"
        sentinel.write_bytes(b"unchanged")
        stage_before = (stat.S_IMODE(stage_root.stat().st_mode), sentinel.read_bytes())
    high_value = {
        "journal_sha256": "1" * 64,
        "receipts": {"request.json": "2" * 64},
    }
    before = json.loads(json.dumps(high_value))
    monkeypatch.setattr(authority, "UPGRADE_ACTIVE", active)
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args: None)
    monkeypatch.setattr(
        authority,
        "_open_lock",
        lambda **_kwargs: os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(
        authority,
        "_read_policy",
        lambda: pytest.fail("active upgrade must reject before reading policy"),
    )

    def execute_request(_request: authority.Request) -> tuple[dict[str, object], None]:
        high_value["journal_sha256"] = "3" * 64
        return {}, None

    monkeypatch.setattr(
        authority,
        "_execute_request",
        execute_request,
    )

    with pytest.raises(authority.NodeAuthorityError, match="admission"):
        authority.dispatch(verb, b"request", environ={})
    assert high_value == before
    if stage_before is None:
        assert not stage_root.exists()
    else:
        assert (
            stat.S_IMODE(stage_root.stat().st_mode),
            (stage_root / "sentinel").read_bytes(),
        ) == stage_before
        assert {path.name for path in stage_root.iterdir()} == {"sentinel"}


def test_runtime_takes_authority_lock_before_policy_and_request_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(),
        verb="transact",
        policy=_policy(),
    )
    receipt = {
        "schema_version": 1,
        "request_id": request.request_id,
        "action": request.action,
        "status": "succeeded",
    }
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    order: list[str] = []
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args: None)
    monkeypatch.setattr(
        authority,
        "_open_lock",
        lambda **_kwargs: order.append("lock") or os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(
        authority,
        "_reject_active_upgrade",
        lambda: order.append("active"),
    )
    monkeypatch.setattr(
        authority,
        "_read_policy",
        lambda: order.append("policy") or _policy(),
    )
    monkeypatch.setattr(
        authority,
        "_validate_runtime_assets",
        lambda _policy: order.append("assets"),
    )
    monkeypatch.setattr(
        authority,
        "_parse_request",
        lambda *_args, **_kwargs: order.append("request") or request,
    )
    monkeypatch.setattr(authority, "_read_receipt", lambda _request_id: receipt)
    monkeypatch.setattr(authority, "_journal_contains", lambda _receipt: True)

    assert authority.dispatch("transact", b"request", environ={}) == receipt
    assert order == ["lock", "active", "policy", "assets", "request"]


def test_private_state_writers_require_private_parent_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[Path, int, int]] = []

    def fake_install(
        path: Path,
        payload: bytes,
        mode: int,
        *,
        parent_mode: int = 0o755,
    ) -> bool:
        del payload
        calls.append((path, mode, parent_mode))
        return True

    monkeypatch.setattr(authority, "_atomic_install", fake_install)
    monkeypatch.setattr(authority, "RECEIPT_ROOT", tmp_path / "receipts")

    authority._write_receipt({"request_id": "a" * 64})
    authority._write_stage_file(tmp_path / "stage" / "payload", b"x", 0o600)

    assert calls == [
        (tmp_path / "receipts" / f"{'a' * 64}.json", 0o600, 0o700),
        (tmp_path / "stage" / "payload", 0o600, 0o700),
    ]


def test_transact_stage_after_reboot_recreates_private_runtime_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(),
        verb="transact",
        policy=_policy(),
    )
    stage_root = tmp_path / "run" / "loom-developer-sandbox-node-authority"
    stage_root.parent.mkdir(mode=0o755)
    monkeypatch.setattr(authority, "STAGE_ROOT", stage_root)
    ensured: list[tuple[Path, int, int]] = []

    def ensure_directory(
        path: Path,
        *,
        mode: int,
        parent_mode: int,
    ) -> bool:
        ensured.append((path, mode, parent_mode))
        path.mkdir(mode=mode)
        path.chmod(mode)
        return True

    def safe_directory(path: Path, *, mode: int) -> None:
        metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != mode:
            raise authority.NodeAuthorityError("test directory metadata is unsafe")

    monkeypatch.setattr(authority, "_ensure_root_directory", ensure_directory)
    monkeypatch.setattr(authority, "_safe_root_directory", safe_directory)
    monkeypatch.setattr(authority.os, "chown", lambda *_args: None)

    stage = authority._prepare_stage(request)

    assert ensured == [(stage_root, 0o700, 0o755)]
    assert stage == stage_root / request.request_id
    assert stat.S_IMODE(stage_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(stage.stat().st_mode) == 0o700


def test_private_child_directory_accepts_private_parent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parent = tmp_path / "state"
    child = parent / "receipts"
    parent.mkdir()
    checked: list[tuple[Path, int]] = []

    def fake_safe_directory(path: Path, *, mode: int) -> None:
        checked.append((path, mode))
        if path == child and not child.exists():
            raise authority.NodeAuthorityError("missing")

    monkeypatch.setattr(authority, "_safe_root_directory", fake_safe_directory)
    monkeypatch.setattr(authority.os, "chown", lambda *_args: None)
    monkeypatch.setattr(authority.os, "chmod", lambda *_args: None)

    assert authority._ensure_root_directory(
        child,
        mode=0o700,
        parent_mode=0o700,
    )
    assert checked[0] == (child, 0o700)
    assert checked[1] == (parent, 0o700)
    assert checked[-1] == (child, 0o700)


def test_bootstrap_directory_plan_preserves_private_parent_modes() -> None:
    plan = {
        path: (mode, parent_mode) for path, mode, parent_mode in authority.BOOTSTRAP_DIRECTORIES
    }

    assert plan[authority.STATE_ROOT] == (0o700, 0o755)
    assert plan[authority.STAGING_INFRASTRUCTURE_PRODUCER_ROOT] == (0o700, 0o700)
    assert plan[authority.STAGING_INFRASTRUCTURE_PRODUCER_RECEIPTS] == (0o700, 0o700)
    assert plan[authority.STAGING_INFRASTRUCTURE_RECEIPT_ROOT] == (0o700, 0o700)
    assert plan[authority.STAGING_INFRASTRUCTURE_INSTALL_GENERATIONS] == (0o700, 0o700)
    assert plan[authority.STAGE_ROOT] == (0o700, 0o755)
    assert {
        authority.SOURCE_ROOT / parent for parent in authority.SOURCE_ASSET_PARENT_PATHS
    }.issubset(plan)
    assert all((authority.SOURCE_ROOT / asset).parent in plan for asset in authority.SOURCE_ASSETS)


def _archive(members: list[tuple[tarfile.TarInfo, bytes]]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for info, content in members:
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def _valid_client_archive() -> bytes:
    rows = []
    for name in sorted(authority.CLIENT_ARCHIVE_FILES):
        info = tarfile.TarInfo(name)
        rows.append((info, f"{name}-value\n".encode()))
    return _archive(rows)


def test_client_archive_extracts_only_the_fixed_regular_file_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        authority,
        "_write_stage_file",
        lambda path, payload, mode: path.write_bytes(payload),
    )
    authority._extract_client_archive(_valid_client_archive(), tmp_path)
    assert {path.name for path in tmp_path.iterdir()} == authority.CLIENT_ARCHIVE_FILES

    symlink = tarfile.TarInfo("ca.pem")
    symlink.type = tarfile.SYMTYPE
    symlink.linkname = "/etc/shadow"
    members = [(symlink, b"")]
    for name in sorted(authority.CLIENT_ARCHIVE_FILES - {"ca.pem"}):
        members.append((tarfile.TarInfo(name), b"value\n"))
    with pytest.raises(authority.NodeAuthorityError, match="shape"):
        authority._extract_client_archive(_archive(members), tmp_path)

    traversal = [(tarfile.TarInfo("../ca.pem"), b"value\n")]
    for name in sorted(authority.CLIENT_ARCHIVE_FILES - {"ca.pem"}):
        traversal.append((tarfile.TarInfo(name), b"value\n"))
    with pytest.raises(authority.NodeAuthorityError, match="shape"):
        authority._extract_client_archive(_archive(traversal), tmp_path)


def test_attestation_archive_extracts_only_worker_and_fleet_seeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        authority,
        "_write_stage_file",
        lambda path, payload, mode: path.write_bytes(payload),
    )
    valid = _archive(
        [
            (tarfile.TarInfo("worker.env"), b"references\n"),
            (tarfile.TarInfo("fleet.json"), b'{"proof":true}\n'),
        ],
    )

    authority._extract_attestation_archive(valid, tmp_path)

    assert {path.name for path in tmp_path.iterdir()} == {"worker.env", "fleet.json"}
    traversal = _archive(
        [
            (tarfile.TarInfo("../worker.env"), b"references\n"),
            (tarfile.TarInfo("fleet.json"), b'{"proof":true}\n'),
        ],
    )
    with pytest.raises(authority.NodeAuthorityError, match="shape"):
        authority._extract_attestation_archive(traversal, tmp_path)


@pytest.mark.parametrize(
    "action",
    ["inspect-candidate", "inspect-local", "export-domain-attestation"],
)
def test_check_dispatches_only_fixed_domain_runtime_actions(
    action: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(action=action),
        verb="check",
        policy=_policy(),
    )
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        authority,
        "_run_fixed",
        lambda argv: calls.append(tuple(argv)) or {"operation": action},
    )

    assert authority._execute_check(
        request,
        _policy(str(request.payload["node"])),
    ) == {"operation": action}
    assert calls[0][0:3] == (
        "/usr/bin/python3",
        str(authority.SOURCE_ROOT / authority.DOMAIN_RUNTIME_RELATIVE),
        action,
    )


def test_fixed_helpers_never_execute_a_candidate_or_request_supplied_program(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(
            action="materialize",
            payload_kind="git-bundle",
            payload_bytes=b"bundle",
        ),
        verb="transact",
        policy=_policy(),
    )
    monkeypatch.setattr(authority, "_prepare_stage", lambda _request: tmp_path)
    monkeypatch.setattr(
        authority,
        "_write_stage_file",
        lambda path, payload, mode: path.write_bytes(payload),
    )
    monkeypatch.setattr(authority, "_safe_root_directory", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(authority.shutil, "rmtree", lambda _path: None)
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...]) -> dict[str, object]:
        calls.append(tuple(argv))
        return {"operation": "materialize", "receipt": "/var/lib/fixed/receipt.json"}

    monkeypatch.setattr(authority, "_run_fixed", run)
    authority._execute_request(
        request,
        _policy(str(request.payload["node"])),
    )

    assert calls[0][0:3] == (
        "/usr/bin/python3",
        str(authority.SOURCE_ROOT / authority.DOMAIN_RUNTIME_RELATIVE),
        "materialize",
    )
    assert str(Path("/shared_work")) not in calls[0][1]
    assert str(tmp_path / "candidate.bundle") in calls[0]


def test_transact_replay_returns_the_root_owned_receipt_without_reexecution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(),
        verb="transact",
        policy=_policy(),
    )
    receipt = {
        "schema_version": 1,
        "request_id": request.request_id,
        "action": request.action,
        "status": "succeeded",
    }
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args: None)
    monkeypatch.setattr(authority, "_read_policy", _policy)
    monkeypatch.setattr(authority, "_validate_runtime_assets", lambda _policy: None)
    monkeypatch.setattr(
        authority,
        "_parse_request",
        lambda *_args, **_kwargs: request,
    )
    monkeypatch.setattr(
        authority,
        "_open_lock",
        lambda **_kwargs: os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(authority, "_read_receipt", lambda _request_id: receipt)
    monkeypatch.setattr(authority, "_journal_contains", lambda _receipt: True)
    monkeypatch.setattr(
        authority,
        "_execute_request",
        lambda _request: pytest.fail("replayed request must not execute"),
    )

    assert authority.dispatch("transact", b"request", environ={}) == receipt


def test_transact_replay_repairs_a_missing_journal_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(),
        verb="transact",
        policy=_policy(),
    )
    receipt = {
        "schema_version": 1,
        "request_id": request.request_id,
        "action": request.action,
        "status": "succeeded",
    }
    lock = tmp_path / "lock"
    lock.write_bytes(b"")
    appended: list[dict[str, object]] = []
    monkeypatch.setattr(authority, "_validate_invoker", lambda *_args: None)
    monkeypatch.setattr(authority, "_read_policy", _policy)
    monkeypatch.setattr(authority, "_validate_runtime_assets", lambda _policy: None)
    monkeypatch.setattr(
        authority,
        "_parse_request",
        lambda *_args, **_kwargs: request,
    )
    monkeypatch.setattr(
        authority,
        "_open_lock",
        lambda **_kwargs: os.open(lock, os.O_RDONLY),
    )
    monkeypatch.setattr(authority, "_read_receipt", lambda _request_id: receipt)
    monkeypatch.setattr(authority, "_journal_contains", lambda _receipt: False)
    monkeypatch.setattr(authority, "_append_journal", appended.append)
    monkeypatch.setattr(
        authority,
        "_execute_request",
        lambda _request: pytest.fail("replayed request must not execute"),
    )

    assert authority.dispatch("transact", b"request", environ={}) == receipt
    assert appended == [receipt]


def test_rollback_is_bound_to_a_prior_root_receipt_and_fixed_state_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = authority._parse_request(
        _request(
            action="rollback",
            prior_request_id="d" * 64,
        ),
        verb="transact",
        policy=_policy(),
    )
    monkeypatch.setattr(
        authority,
        "_read_receipt",
        lambda _request_id: {
            "node": "oldlab-1",
            "domain": "oldlab",
            "sandbox": "qianyi",
            "candidate_sha": SHA,
            "candidate_tree": TREE,
            "action": "materialize",
            "inner_receipt": "/tmp/operator-selected.json",
        },
    )
    with pytest.raises(authority.NodeAuthorityError, match=r"unavailable|path"):
        authority._execute_request(
            request,
            _policy(str(request.payload["node"])),
        )
