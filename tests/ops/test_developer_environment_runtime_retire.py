from __future__ import annotations

import base64
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_registry as registry
from scripts.ops import developer_environment_runtime_retire as retire


@dataclass(frozen=True)
class Prepared:
    deployment_id: str
    env_id: str
    operation_sha256: str
    runtime_root: Path
    snapshot_path: Path


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(retire._canonical(payload))
    path.chmod(0o600)


def _prepare(tmp_path: Path) -> Prepared:
    registry_root = tmp_path / "registry"
    authority = registry.DeveloperEnvironmentRegistry(
        registry_root / "registry.sqlite3",
    )
    principal = "oidc:example:runtime-retire"
    environment = authority.register(
        {
            "schema_version": 1,
            "kind": registry.REGISTER_KIND,
            "principal_id": principal,
            "idempotency_key": "runtime-retire-register",
            "display_name": "Runtime Retire",
        }
    )
    current = authority.import_candidate(
        {
            "schema_version": 1,
            "kind": registry.CANDIDATE_KIND,
            "principal_id": principal,
            "idempotency_key": "runtime-retire-current",
            "env_id": environment.env_id,
            "candidate_sha": "1" * 40,
            "candidate_tree": "2" * 40,
            "bundle_sha256": "3" * 64,
            "bundle_size": 1024,
            "image_digests": {
                "amd64": "sha256:" + "4" * 64,
                "arm64": "sha256:" + "5" * 64,
            },
        }
    )
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": principal,
            "idempotency_key": "runtime-retire-deploy",
            "env_id": environment.env_id,
            "candidate_id": current.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    for expected, following in zip(
        registry.DEPLOY_PHASES[:-2],
        registry.DEPLOY_PHASES[1:-1],
        strict=True,
    ):
        deployment = authority.advance_deployment(
            deployment.deployment_id,
            principal_id=principal,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )
    authority.prepare_deployment_finalization(
        deployment.deployment_id,
        principal_id=principal,
        expected_resource_generation=environment.resource_generation,
    )
    authority.record_deployment_finalization(
        deployment.deployment_id,
        principal_id=principal,
        expected_resource_generation=environment.resource_generation,
        evidence={
            "capacity_finalize_receipt_sha256": "6" * 64,
            "capacity_finalize_check_receipt_sha256": "7" * 64,
            "runtime_reconcile_receipt_sha256": "8" * 64,
            "runtime_prepare_check_receipt_sha256": "9" * 64,
            "acceptance_probe_receipt_sha256": "a" * 64,
        },
    )
    authority.advance_deployment(
        deployment.deployment_id,
        principal_id=principal,
        expected_phase="verified",
        next_phase="committed",
        expected_resource_generation=environment.resource_generation,
    )
    active = authority.lookup(environment.env_id, principal_id=principal)
    failed_candidate = authority.import_candidate(
        {
            "schema_version": 1,
            "kind": registry.CANDIDATE_KIND,
            "principal_id": principal,
            "idempotency_key": "runtime-retire-failed-candidate",
            "env_id": environment.env_id,
            "candidate_sha": "b" * 40,
            "candidate_tree": "c" * 40,
            "bundle_sha256": "d" * 64,
            "bundle_size": 2048,
            "image_digests": {
                "amd64": "sha256:" + "e" * 64,
                "arm64": "sha256:" + "f" * 64,
            },
        }
    )
    failed = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": principal,
            "idempotency_key": "runtime-retire-failed-deploy",
            "env_id": environment.env_id,
            "candidate_id": failed_candidate.candidate_id,
            "expected_resource_generation": active.resource_generation,
        }
    )
    authority.fail_deployment(
        failed.deployment_id,
        principal_id=principal,
        expected_phase="requested",
        expected_resource_generation=active.resource_generation,
    )
    authority.begin_retirement(
        environment.env_id,
        principal_id=principal,
        expected_resource_generation=active.resource_generation,
    )
    snapshot = authority.snapshot()
    environment_row = next(
        row for row in snapshot["environments"] if row["env_id"] == environment.env_id
    )
    runtime_root = tmp_path / "runtime"
    unsigned_wal = {
        "schema_version": 1,
        "kind": retire.RETIRE_WAL_KIND,
        "phase": "capacity-retired",
        "env_id": environment.env_id,
        "principal_id": principal,
        "runtime_id": environment_row["runtime_id"],
        "uid": environment_row["uid"],
        "gid": environment_row["gid"],
        "service_user": environment_row["service_user"],
        "service_group": environment_row["service_group"],
        "slurm_user": environment_row["slurm_user"],
        "slurm_account": environment_row["slurm_account"],
        "slurm_qos": environment_row["slurm_qos"],
        "expected_resource_generation": environment_row["resource_generation"],
        "current_candidate_id": current.candidate_id,
        "idempotency_key": "runtime-retire-operation",
        "evidence": {
            "admission_fence": "f" * 64,
            "capacity_retire": "0" * 64,
        },
        "object_checkpoints": {},
        "created_at": "2026-07-29T20:00:00Z",
        "updated_at": "2026-07-29T20:01:00Z",
    }
    wal = {**unsigned_wal, "payload_sha256": retire._digest(unsigned_wal)}
    _write(
        runtime_root / "lifecycle/retire" / f"{environment.env_id}.json",
        wal,
    )
    return Prepared(
        deployment_id=deployment.deployment_id,
        env_id=environment.env_id,
        operation_sha256=wal["payload_sha256"],
        runtime_root=runtime_root,
        snapshot_path=registry_root / "current-snapshot.json",
    )


def _transport(
    calls: list[str],
    *,
    fail_once_at: str | None = None,
    omit_at: str | None = None,
) -> Any:
    failed = False

    def run(
        args: tuple[str, ...],
        *,
        input: bytes,
        check: bool,
        capture_output: bool,
        timeout: int,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal failed
        assert check is False
        assert capture_output is True
        assert timeout == 180
        assert env["PATH"].startswith("/usr/local/sbin:")
        envelope = json.loads(input)
        node = envelope["node"]
        calls.append(node)
        if node == fail_once_at and not failed:
            failed = True
            return subprocess.CompletedProcess(args, 1, b"", b"")
        request_raw = base64.b64decode(envelope["payload_base64"], validate=True)
        request = json.loads(request_raw)
        assert request_raw == retire._canonical(request)
        assert set(request) == retire.REQUEST_FIELDS
        assert request["node"] == node
        assert request["foreign_path_action"] == "preserve"
        assert request["audit_action"] == "append-only-preserve"
        assert "bundle_path" not in request_raw.decode("ascii")
        unsigned_receipt = {
            "schema_version": 1,
            "kind": retire.NODE_RECEIPT_KIND,
            "status": "cleaned",
            "action": retire.ACTION,
            "node": node,
            "domain": request["domain"],
            "deployment_id": request["deployment_id"],
            "env_id": request["env_id"],
            "principal_id": request["principal_id"],
            "runtime_id": request["runtime_id"],
            "resource_generation": request["resource_generation"],
            "registry_generation": request["registry_generation"],
            "registry_snapshot_sha256": request["registry_snapshot_sha256"],
            "retire_operation_sha256": request["retire_operation_sha256"],
            "request_sha256": request["payload_sha256"],
            "transport_request_id": envelope["request_id"],
            "candidate_bindings": request["candidate_bindings"],
            "absent": {field: True for field in retire.ABSENCE_FIELDS},
            "tombstone": {
                "path": (
                    "/var/lib/loom-developer-environment-runtime-retire/tombstones/"
                    f"{node}/{request['runtime_id']}/"
                    f"{request['retire_operation_sha256']}.json"
                ),
                "payload_sha256": "1" * 64,
                "persisted": True,
            },
            "peer_digest_before": "2" * 64,
            "peer_digest_after": "2" * 64,
            "foreign_path_action": "preserve",
            "audit_action": "append-only-preserve",
            "completed_at": "2026-07-29T20:02:00Z",
        }
        if node == omit_at:
            del unsigned_receipt["absent"]["token_files"]
        receipt = {
            **unsigned_receipt,
            "payload_sha256": retire._digest(unsigned_receipt),
        }
        current = next(
            item
            for item in request["candidate_bindings"]
            if item["candidate_id"] == request["current_candidate_id"]
        )
        unsigned_response = {
            "schema_version": 1,
            "request_id": envelope["request_id"],
            "status": "succeeded",
            "action": retire.ACTION,
            "node": node,
            "domain": request["domain"],
            "sandbox": request["runtime_id"],
            "candidate_sha": current["candidate_sha"],
            "candidate_tree": current["candidate_tree"],
            "payload_sha256": envelope["payload_sha256"],
            "result": receipt,
            "result_sha256": retire._digest(receipt),
            "completed_at": "2026-07-29T20:02:00Z",
        }
        return subprocess.CompletedProcess(
            args,
            0,
            retire._canonical(unsigned_response),
            b"",
        )

    return run


def _execute(prepared: Prepared, calls: list[str], **kwargs: Any) -> dict[str, Any]:
    return retire.execute(
        prepared.deployment_id,
        prepared.env_id,
        prepared.operation_sha256,
        runtime_root=prepared.runtime_root,
        registry_snapshot=prepared.snapshot_path,
        transport_program=Path("/fixed/node-transport"),
        transport=_transport(calls, **kwargs),
        require_root_ownership=False,
    )


def test_all_twenty_nodes_are_cleaned_once_and_combined_receipt_replays(
    tmp_path: Path,
) -> None:
    prepared = _prepare(tmp_path)
    calls: list[str] = []

    first = _execute(prepared, calls)
    second = _execute(prepared, calls)

    assert first == second
    assert set(first["nodes"]) == set(retire.NODES)
    assert calls == list(retire.NODES)
    assert len(calls) == 20
    assert "trt-gb10-7" in calls
    assert len(first["candidate_bindings"]) == 2


def test_crash_after_subset_resumes_without_duplicate_cleanup(tmp_path: Path) -> None:
    prepared = _prepare(tmp_path)
    calls: list[str] = []

    with pytest.raises(retire.RuntimeRetireError, match="failed safely"):
        _execute(prepared, calls, fail_once_at="trt-gb10-3")
    first_attempt = list(calls)
    receipt = _execute(prepared, calls)

    assert receipt["status"] == "cleaned"
    completed_before_failure = first_attempt[:-1]
    for node in completed_before_failure:
        assert calls.count(node) == 1
    assert calls.count("trt-gb10-3") == 2
    for node in retire.NODES[retire.NODES.index("trt-gb10-3") + 1 :]:
        assert calls.count(node) == 1


def test_partial_node_receipt_fails_closed(tmp_path: Path) -> None:
    prepared = _prepare(tmp_path)
    with pytest.raises(retire.RuntimeRetireError, match="receipt binding"):
        _execute(prepared, [], omit_at="trt-gb10-7")


def test_active_snapshot_wrong_wal_and_candidate_injection_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepare(tmp_path)
    verified = retire._snapshot(
        prepared.snapshot_path,
        require_root_ownership=False,
    )
    active = json.loads(json.dumps(verified))
    active["environments"][0]["state"] = "active"
    monkeypatch.setattr(retire, "_snapshot", lambda *_args, **_kwargs: active)
    with pytest.raises(retire.RuntimeRetireError, match="registry intent"):
        _execute(prepared, [])

    injected = json.loads(json.dumps(verified))
    monkeypatch.setattr(retire, "_snapshot", lambda *_args, **_kwargs: injected)
    current_id = injected["environments"][0]["current_candidate_id"]
    next(row for row in injected["candidates"] if row["candidate_id"] == current_id)[
        "candidate_sha"
    ] = "../../candidate"
    with pytest.raises(retire.RuntimeRetireError, match="candidate binding"):
        _execute(prepared, [])

    monkeypatch.setattr(
        retire,
        "_snapshot",
        lambda *_args, **_kwargs: verified,
    )
    with pytest.raises(retire.RuntimeRetireError, match="WAL binding"):
        retire.execute(
            prepared.deployment_id,
            prepared.env_id,
            "f" * 64,
            runtime_root=prepared.runtime_root,
            registry_snapshot=prepared.snapshot_path,
            transport_program=Path("/fixed/node-transport"),
            transport=_transport([]),
            require_root_ownership=False,
        )


def test_symlinked_or_tampered_wal_is_rejected(tmp_path: Path) -> None:
    prepared = _prepare(tmp_path)
    wal_path = prepared.runtime_root / "lifecycle/retire" / f"{prepared.env_id}.json"
    target = wal_path.with_suffix(".safe")
    wal_path.rename(target)
    wal_path.symlink_to(target)
    with pytest.raises(retire.RuntimeRetireError, match="WAL is unavailable"):
        _execute(prepared, [])

    wal_path.unlink()
    target.rename(wal_path)
    payload = json.loads(wal_path.read_bytes())
    payload["current_candidate_id"] = "cand-" + "f" * 40
    _write(wal_path, payload)
    with pytest.raises(retire.RuntimeRetireError, match="WAL binding"):
        _execute(prepared, [])


def test_symlinked_node_receipt_and_one_node_combined_receipt_are_rejected(
    tmp_path: Path,
) -> None:
    prepared = _prepare(tmp_path)
    _execute(prepared, [])
    receipt_root = (
        prepared.runtime_root / "runtime-retire" / prepared.env_id / prepared.operation_sha256
    )
    node_path = receipt_root / "oldlab-1.json"
    node_target = node_path.with_suffix(".safe")
    node_path.rename(node_target)
    node_path.symlink_to(node_target)
    with pytest.raises(retire.RuntimeRetireError, match="receipt is unavailable"):
        _execute(prepared, [])

    node_path.unlink()
    node_target.rename(node_path)
    combined_path = receipt_root / "combined.json"
    combined = json.loads(combined_path.read_bytes())
    del combined["nodes"]["trt-gb10-7"]
    unsigned = {key: value for key, value in combined.items() if key != "payload_sha256"}
    combined["payload_sha256"] = retire._digest(unsigned)
    _write(combined_path, combined)
    with pytest.raises(retire.RuntimeRetireError, match=r"combined.*drifted"):
        _execute(prepared, [])


def test_contract_has_no_candidate_path_or_source_code_inputs() -> None:
    assert retire.NODES == (
        "oldlab-1",
        "oldlab-2",
        "oldlab-3",
        "oldlab-4",
        "oldlab-5",
        *(f"trt-gb10-{index}" for index in range(1, 16)),
    )
    assert not {
        "path",
        "candidate_root",
        "bundle_path",
        "source_repo",
        "program",
    }.intersection(retire.REQUEST_FIELDS)
    assert retire.ACTION == "developer-environment-runtime-retire"
    assert retire.PAYLOAD_KIND == "developer-environment-runtime-retire-json"
