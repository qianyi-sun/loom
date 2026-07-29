from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_sandbox_live_authority as authority

SHA = "a" * 40
TREE = "b" * 40
AUTHORITY_TREE = "c" * 40
REQUEST_ID = "00000000-0000-0000-0000-000000000001"


def _slurm_request(*, pool: str = "oldlab") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.live-slurm-request",
        "source_host": authority.SOURCE_HOSTS[pool],
        "sandbox": "qianyi",
        "pool": pool,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "job_id": "1234",
        "account": "loom-dev-qianyi",
        "user": "loom-sandbox-qianyi",
        "job_name": f"loom-sandbox-qianyi-{SHA[:12]}-trt-eai-oldlab-1",
        "node": "trt-eai-oldlab-1",
        "requested_cpus": 8,
        "requested_memory_mib": 16384,
        "job_pids_max": 65536,
        "requested_gpus": 0,
        "requested_gpu_tres": "",
    }


def _scontrol(request: dict[str, Any], **replacements: str) -> str:
    fields = {
        "JobId": request["job_id"],
        "JobName": request["job_name"],
        "UserId": f"{request['user']}(31021)",
        "Account": request["account"],
        "JobState": "RUNNING",
        "NodeList": request["node"],
        "NumNodes": "1",
        "NumCPUs": str(request["requested_cpus"]),
        "MinMemoryNode": f"{request['requested_memory_mib']}M",
        "Comment": f"loom-cgroup-v1:pids={request['job_pids_max']}",
        "Shared": "OK",
        "AllocTRES": "billing=8,cpu=8,mem=16G,node=1",
    }
    fields.update(replacements)
    return " ".join(f"{key}={value}" for key, value in fields.items()) + "\n"


def _run_result(stdout: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=(), returncode=0, stdout=stdout, stderr="")


def test_observe_slurm_job_returns_only_exact_sanitized_allocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _slurm_request()
    monkeypatch.setattr(authority, "REQUIRED_UID", os.getuid())
    result = authority.observe_slurm_job(
        authority._canonical(request),
        run=lambda *_args, **_kwargs: _run_result(_scontrol(request)),
        clock=lambda: datetime(2026, 7, 29, 12, 0, 1, tzinfo=UTC),
        hostname=lambda: "trt-eai-oldlab-2",
    )

    assert set(result) == authority.SLURM_RESULT_FIELDS
    assert result["user"] == "loom-sandbox-qianyi"
    assert result["state"] == "RUNNING"
    assert result["allocation"] == {
        "cpu_cores": 8,
        "memory_bytes": 16384 * 1024 * 1024,
        "pids": 65536,
        "gpu_count": 0,
        "tres": "billing=8,cpu=8,mem=16G,node=1",
        "exclusive": False,
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"user": "qianyi"}, "request binding"),
        ({"node": "trt-gb10-7"}, "request binding"),
        ({"candidate_tree": "../tree"}, "request binding"),
    ],
)
def test_observe_rejects_personal_user_forbidden_node_and_unbound_tree(
    monkeypatch: pytest.MonkeyPatch,
    mutation: dict[str, Any],
    message: str,
) -> None:
    request = _slurm_request()
    request.update(mutation)
    monkeypatch.setattr(authority, "REQUIRED_UID", os.getuid())
    with pytest.raises(authority.LiveAuthorityError, match=message):
        authority.observe_slurm_job(
            authority._canonical(request),
            run=lambda *_args, **_kwargs: pytest.fail("invalid request reached Slurm"),
            hostname=lambda: "trt-eai-oldlab-2",
        )


@pytest.mark.parametrize(
    "replacement",
    [
        {"JobState": "PENDING"},
        {"UserId": "qianyi(1000)"},
        {"Shared": "NO"},
        {"Comment": "loom-cgroup-v1:pids=1"},
        {"NumCPUs": "7"},
    ],
)
def test_observe_rejects_every_slurm_identity_or_containment_drift(
    monkeypatch: pytest.MonkeyPatch,
    replacement: dict[str, str],
) -> None:
    request = _slurm_request()
    monkeypatch.setattr(authority, "REQUIRED_UID", os.getuid())
    with pytest.raises(authority.LiveAuthorityError, match="does not match"):
        authority.observe_slurm_job(
            authority._canonical(request),
            run=lambda *_args, **_kwargs: _run_result(_scontrol(request, **replacement)),
            hostname=lambda: "trt-eai-oldlab-2",
        )


def _capacity(observed_at: str, sequence: int = 7) -> dict[str, Any]:
    unsigned = {
        "sandbox": "qianyi",
        "pool_name": "gb10",
        "candidate_sha": SHA,
        "request_id": REQUEST_ID,
        "lease_epoch": 3,
        "capacity_lease_state": "active",
        "observed_at": observed_at,
        "observation_sequence": sequence,
        "pending_slots": 1,
        "active_slots": 10,
        "draining_slots": 0,
        "terminal_slots": 2,
    }
    return {
        **unsigned,
        "payload_sha256": authority.hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode(),
        ).hexdigest(),
    }


def _sandbox_state(observed_at: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "sandbox": "qianyi",
        "compose_project": "loom-sandbox-qianyi",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "source_repo": "/srv/loom/source",
        "updated_at": observed_at,
    }


def _policy() -> dict[str, Any]:
    return {
        "environment": "sandbox-qianyi",
        "pool_name": "gb10",
        "actuator": "slurm",
        "enabled": True,
        "max_slots": 140,
        "actuator_config": {
            "shared_capacity_managed": True,
            "candidate_sha": SHA,
            "slurm_account": "loom-dev-qianyi",
            "exclusive": False,
            "allowed_nodes": ["trt-gb10-1"],
            "job_pids_max": 65536,
        },
        "capacity_lease_state": {
            "schema_version": 1,
            "request_id": REQUEST_ID,
            "lease_epoch": 3,
            "candidate_sha": SHA,
            "preemptible": True,
            "state": "active",
            "activated_at": "2026-07-29T12:00:00Z",
        },
    }


def _registry() -> dict[str, Any]:
    return {
        "summary": [],
        "jobs": [
            {
                "environment": "sandbox-qianyi",
                "pool_name": "gb10",
                "sandbox_identity": "sandbox-qianyi",
                "candidate_sha": SHA,
                "state": "running",
                "slurm_state": "RUNNING",
                "job_id": "4321",
                "nodelist": "trt-gb10-1",
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "requested_concurrency": 10,
                "requested_gpus": 1,
                "requested_gpu_tres": "gpu:1",
                "compose_project": f"loom-sandbox-qianyi-{SHA[:12]}-4321",
                "submission_error": None,
            },
        ],
    }


def _collection_request(collection_id: str | None = None) -> bytes:
    return authority._canonical(
        {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.live-overlap-collection",
            "collection_id": collection_id or str(uuid.UUID(int=2)),
            "candidate_tree": TREE,
            "job_id": "4321",
        },
    )


def _prepare_collect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    sequence: int = 7,
) -> tuple[bytes, bytes, list[datetime]]:
    state_root = tmp_path / "authority"
    monkeypatch.setattr(authority, "STATE_ROOT", state_root)
    monkeypatch.setattr(authority, "LOCK", state_root / "authority.lock")
    monkeypatch.setattr(authority, "HIGH_WATER_ROOT", state_root / "high-water")
    monkeypatch.setattr(authority, "TRANSACTION_ROOT", state_root / "transactions")
    monkeypatch.setattr(authority, "OVERLAP_ROOT", state_root / "overlap")
    monkeypatch.setattr(authority, "REQUIRED_UID", os.getuid())
    monkeypatch.setattr(authority, "REQUIRED_GID", os.getgid())
    capacity = _capacity("2026-07-29T12:00:00Z", sequence=sequence)
    capacity_raw = authority._canonical([capacity])
    sandbox_state = _sandbox_state("2026-07-29T11:59:00Z")
    sandbox_raw = authority._canonical(sandbox_state)
    config = authority.AdapterConfig(
        sandbox="qianyi",
        environment="sandbox-qianyi",
        pool="gb10",
        control_plane_url="http://127.0.0.1:20080",
        admin_secret_file=Path("/fixed/admin.toml"),
        observation_path=Path("/fixed/capacity.json"),
        sandbox_state_path=Path("/fixed/sandbox-state.json"),
        max_slots_bound=140,
        timeout_seconds=10,
    )
    monkeypatch.setattr(authority, "_load_adapter_config", lambda *_args: config)
    monkeypatch.setattr(
        authority,
        "_capacity_observation",
        lambda *_args, **_kwargs: (capacity, capacity_raw),
    )
    monkeypatch.setattr(
        authority,
        "_sandbox_state",
        lambda *_args, **_kwargs: (sandbox_state, sandbox_raw),
    )
    monkeypatch.setattr(authority, "_load_admin_token", lambda _path: "opaque-test-token")
    monkeypatch.setattr(
        authority,
        "_read_secure_bytes",
        lambda path, **_kwargs: (
            capacity_raw
            if path == config.observation_path
            else (sandbox_raw if path == config.sandbox_state_path else path.read_bytes())
        ),
    )
    times = [
        datetime(2026, 7, 29, 12, 0, 5, tzinfo=UTC),
        datetime(2026, 7, 29, 12, 0, 7, tzinfo=UTC),
        datetime(2026, 7, 29, 12, 0, 8, tzinfo=UTC),
    ]
    return capacity_raw, sandbox_raw, times


def _http(**kwargs: Any) -> dict[str, Any]:
    return _registry() if kwargs["path"].endswith("/status") else _policy()


def _service_result() -> subprocess.CompletedProcess[str]:
    return _run_result(
        "Id=loom-developer-sandbox-qianyi.service\n"
        "LoadState=loaded\n"
        "ActiveState=active\n"
        "SubState=running\n",
    )


def _transport(_node: str, envelope: bytes) -> dict[str, Any]:
    outer = json.loads(envelope)
    request = json.loads(authority.base64.b64decode(outer["payload_base64"]))
    assert request["user"] == "loom-sandbox-qianyi"
    return {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.live-slurm-observation",
        "source_host": "trt-gb10-1",
        "sandbox": "qianyi",
        "pool": "gb10",
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "job_id": "4321",
        "account": "loom-dev-qianyi",
        "user": "loom-sandbox-qianyi",
        "job_name": f"loom-sandbox-qianyi-{SHA[:12]}-trt-gb10-1",
        "node": "trt-gb10-1",
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": 20,
            "memory_bytes": 115000 * 1024 * 1024,
            "pids": 65536,
            "gpu_count": 1,
            "tres": "cpu=20,mem=115000M,gres/gpu=1",
            "exclusive": False,
        },
        "observed_at": "2026-07-29T12:00:06Z",
    }


def test_collect_binds_real_capacity_policy_job_service_and_remote_slurm(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _capacity_raw, _sandbox_raw, times = _prepare_collect(monkeypatch, tmp_path)
    result = authority.collect(
        "qianyi",
        "gb10",
        SHA,
        AUTHORITY_TREE,
        _collection_request(),
        http_json=_http,
        service_run=lambda *_args, **_kwargs: _service_result(),
        transport=_transport,
        clock=lambda: times.pop(0),
        hostname=lambda: authority.COLLECT_HOST,
    )

    receipt_path = Path(result["path"])
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt_path == (tmp_path / "authority/overlap/gb10/qianyi" / SHA / "4321.json")
    assert receipt["capacity_sample"]["request_id"] == REQUEST_ID
    assert receipt["capacity_sample"]["lease_epoch"] == 3
    assert receipt["capacity_sample"]["user"] == "loom-sandbox-qianyi"
    assert receipt["capacity_sample"]["observed_at"] == "2026-07-29T12:00:00Z"
    assert receipt["job_readback"]["observed_at"] == "2026-07-29T12:00:06Z"
    assert receipt["service_readback"]["observed_at"] == "2026-07-29T12:00:07Z"
    assert receipt["observed_at"] == "2026-07-29T12:00:08Z"
    assert receipt_path.stat().st_mode & 0o777 == 0o600
    assert receipt_path.stat().st_nlink == 1
    assert receipt_path.read_bytes() == authority._canonical(receipt)


def test_collect_same_collection_is_idempotent_without_querying_live_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _capacity_raw, _sandbox_raw, times = _prepare_collect(monkeypatch, tmp_path)
    request = _collection_request()
    first = authority.collect(
        "qianyi",
        "gb10",
        SHA,
        AUTHORITY_TREE,
        request,
        http_json=_http,
        service_run=lambda *_args, **_kwargs: _service_result(),
        transport=_transport,
        clock=lambda: times.pop(0),
        hostname=lambda: authority.COLLECT_HOST,
    )
    second = authority.collect(
        "qianyi",
        "gb10",
        SHA,
        AUTHORITY_TREE,
        request,
        http_json=lambda **_kwargs: pytest.fail("idempotent replay queried CP"),
        service_run=lambda *_args, **_kwargs: pytest.fail("idempotent replay queried systemd"),
        transport=lambda *_args: pytest.fail("idempotent replay queried Slurm"),
        hostname=lambda: authority.COLLECT_HOST,
    )
    assert second == first


def test_collect_recovers_receipt_written_transaction_after_high_water_crash(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _capacity_raw, _sandbox_raw, times = _prepare_collect(monkeypatch, tmp_path)
    request = _collection_request()
    original_replace = authority._atomic_replace
    failed = False

    def crash_once(path: Path, payload: Any) -> None:
        nonlocal failed
        if path.parent == authority.HIGH_WATER_ROOT and not failed:
            failed = True
            raise OSError("simulated crash")
        original_replace(path, payload)

    monkeypatch.setattr(authority, "_atomic_replace", crash_once)
    with pytest.raises(OSError, match="simulated crash"):
        authority.collect(
            "qianyi",
            "gb10",
            SHA,
            AUTHORITY_TREE,
            request,
            http_json=_http,
            service_run=lambda *_args, **_kwargs: _service_result(),
            transport=_transport,
            clock=lambda: times.pop(0),
            hostname=lambda: authority.COLLECT_HOST,
        )
    monkeypatch.setattr(authority, "_atomic_replace", original_replace)
    recovered = authority.collect(
        "qianyi",
        "gb10",
        SHA,
        AUTHORITY_TREE,
        request,
        http_json=lambda **_kwargs: pytest.fail("recovery queried CP"),
        hostname=lambda: authority.COLLECT_HOST,
    )
    assert Path(recovered["path"]).is_file()
    transaction = json.loads(next(authority.TRANSACTION_ROOT.iterdir()).read_bytes())
    assert transaction["phase"] == "committed"
    assert (authority.HIGH_WATER_ROOT / "qianyi-gb10.json").is_file()


def test_collect_rejects_stale_capacity_before_any_control_plane_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    capacity_observation = authority._capacity_observation
    _prepare_collect(monkeypatch, tmp_path)
    config = authority._load_adapter_config("qianyi", "gb10")
    stale = _capacity("2026-07-29T11:00:00Z")
    stale_raw = authority._canonical([stale])
    monkeypatch.setattr(
        authority,
        "_capacity_observation",
        lambda *_args, **_kwargs: capacity_observation(
            config,
            candidate_sha=SHA,
            now=datetime(2026, 7, 29, 12, 0, 5, tzinfo=UTC),
        ),
    )
    monkeypatch.setattr(
        authority,
        "_secure_json",
        lambda *_args, **_kwargs: ([stale], stale_raw),
    )
    with pytest.raises(authority.LiveAuthorityError, match="stale"):
        authority.collect(
            "qianyi",
            "gb10",
            SHA,
            AUTHORITY_TREE,
            _collection_request(),
            http_json=lambda **_kwargs: pytest.fail("stale source reached CP"),
            clock=lambda: datetime(2026, 7, 29, 12, 0, 5, tzinfo=UTC),
            hostname=lambda: authority.COLLECT_HOST,
        )


def test_collect_rejects_capacity_source_swap_after_live_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    capacity_raw, sandbox_raw, times = _prepare_collect(monkeypatch, tmp_path)
    reads = 0

    def changed_source(path: Path, **_kwargs: Any) -> bytes:
        nonlocal reads
        if path.name == "capacity.json":
            reads += 1
            return capacity_raw + (b" " if reads else b"")
        return sandbox_raw

    monkeypatch.setattr(authority, "_read_secure_bytes", changed_source)
    with pytest.raises(authority.LiveAuthorityError, match="changed during collection"):
        authority.collect(
            "qianyi",
            "gb10",
            SHA,
            AUTHORITY_TREE,
            _collection_request(),
            http_json=_http,
            service_run=lambda *_args, **_kwargs: _service_result(),
            transport=_transport,
            clock=lambda: times.pop(0),
            hostname=lambda: authority.COLLECT_HOST,
        )
