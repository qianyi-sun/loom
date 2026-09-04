from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom.models.batch import Combination
from loom_cli.nebius_acceptance import (
    NebiusAcceptanceError,
    _batch_shape,
    acceptance_stages,
    load_capacity_policy,
    run_acceptance,
    run_cli,
    validate_trial_bundle,
)

_CANDIDATE = "a" * 40
_CONNECTION_ID = "00000000-0000-0000-0000-0000000000aa"
_BATCH_ID = "00000000-0000-0000-0000-0000000000bb"
_TRIAL_ID = "00000000-0000-0000-0000-0000000000cc"


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _bundle(
    *,
    trial_id: str = _TRIAL_ID,
    answer: bytes = b"complete\n",
    omit: str | None = None,
) -> bytes:
    outputs = {
        "artifacts/answer.txt": answer,
        "artifacts/reasoning.md": b"reasoning\n",
        "trajectory/events.jsonl": b'{"seq":1}\n',
        "accounting/usage.json": b'{"call_count":1}\n',
        "verifier/output.json": b'{"rewards":{"artifact_complete":1.0}}\n',
    }
    if omit is not None:
        del outputs[omit]
    manifest = {
        "schema_version": "loom.canonical-trial-bundle-export.v1",
        "artifact_id": "00000000-0000-0000-0000-0000000000dd",
        "trial_id": trial_id,
        "task_id": "ts/test/task-1",
        "attempt": 1,
        "manifest_sha256": "sha256:" + "b" * 64,
        "content_sha256": "sha256:" + "c" * 64,
        "files": [
            {
                "relative_path": name,
                "size_bytes": len(payload),
                "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
                "media_type": "application/octet-stream",
            }
            for name, payload in outputs.items()
        ],
    }
    files = {"bundle.json": _canonical(manifest), **outputs}
    ledger = "".join(
        f"{hashlib.sha256(payload).hexdigest()}  {name}\n" for name, payload in files.items()
    ).encode()
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz:
        with tarfile.open(fileobj=gz, mode="w") as archive:
            for name, payload in {**files, "checksums/SHA256SUMS": ledger}.items():
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                info.mtime = 0
                archive.addfile(info, io.BytesIO(payload))
    return raw.getvalue()


def _policy(tmp_path: Path, *, accepted: int = 56) -> dict[str, Any]:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "loom.nebius-development-capacity.v1",
                "target_id": "nebius-eu-north1-development",
                "accepted_concurrency": accepted,
                "target_concurrency": 200,
                "admission_policies": [
                    {
                        "scope_kind": "global",
                        "scope_key": "*",
                        "max_concurrent": accepted,
                        "enabled": True,
                    },
                    {
                        "scope_kind": "pool",
                        "scope_key": "nebius-cpu",
                        "max_concurrent": accepted,
                        "enabled": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return load_capacity_policy(path)


def _monitor(*, nodes: int, slots: int, occupied: int = 0) -> dict[str, Any]:
    return {
        "resources": {
            "pools": [
                {
                    "pool_name": "nebius-cpu",
                    "backend": "nebius",
                    "current_active_slots": slots,
                    "pending_slots": 0,
                    "desired_slots": slots,
                    "occupied_slots": occupied,
                    "running_tasks": occupied,
                    "starting_tasks": 0,
                    "queued_tasks": 0,
                }
            ]
        },
        "service_execution": {
            "targets": [
                {
                    "provider": "nebius",
                    "pool_id": "nebius-cpu",
                    "environment": "development",
                    "health_status": "healthy",
                    "command_backlog": 0,
                    "observation": {
                        "is_fresh": True,
                        "active_nodes": nodes,
                        "node_states": {
                            "desired": nodes,
                            "creating": 0,
                            "ready": nodes,
                            "failed": 0,
                            "deleting": 0,
                        },
                        "provider_capacity_state": "available",
                        "autoscaler_state": "ready",
                    },
                }
            ]
        },
    }


def test_acceptance_stages_follow_current_and_target_envelopes() -> None:
    assert acceptance_stages(56, []) == [1, 20, 40, 56]
    assert acceptance_stages(200, []) == [1, 20, 50, 100, 150, 200]
    with pytest.raises(NebiusAcceptanceError, match="exceeds persisted"):
        acceptance_stages(56, [57])


def test_batch_shape_supports_200_trials_without_exceeding_api_sample_limit() -> None:
    trial_config, n_per_task, combinations = _batch_shape(
        trials_per_task=200,
        agent_name="litellm",
        agent_model={"provider": "openai", "name": "model", "source": "api"},
        provider_connection_id=_CONNECTION_ID,
        provider_model_id="model",
    )
    assert trial_config == {}
    assert n_per_task == 1
    assert [row["n_per_task"] for row in combinations] == [100, 100]
    assert [row["label"] for row in combinations] == [
        "nebius-acceptance-1",
        "nebius-acceptance-2",
    ]
    assert [Combination.model_validate(row).n_per_task for row in combinations] == [100, 100]


def test_capacity_policy_requires_matching_admission_limits(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy["accepted_concurrency"] == 56
    assert len(policy["_sha256"]) == 64
    policy_path = tmp_path / "policy.json"
    body = json.loads(policy_path.read_text())
    body["admission_policies"][1]["max_concurrent"] = 55
    policy_path.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(NebiusAcceptanceError, match="admission limits"):
        load_capacity_policy(policy_path)


def test_validate_trial_bundle_checks_manifest_and_every_member() -> None:
    payload = _bundle()
    manifest, manifest_sha256 = validate_trial_bundle(payload, trial_id=_TRIAL_ID)
    assert manifest["files"][0]["relative_path"] == "artifacts/answer.txt"
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        manifest_stream = archive.extractfile("bundle.json")
        assert manifest_stream is not None
        assert manifest_sha256 == hashlib.sha256(manifest_stream.read()).hexdigest()
    with pytest.raises(NebiusAcceptanceError, match="identity"):
        validate_trial_bundle(payload, trial_id="different")
    with pytest.raises(NebiusAcceptanceError, match="missing required outputs"):
        validate_trial_bundle(
            _bundle(omit="trajectory/events.jsonl"),
            trial_id=_TRIAL_ID,
            required_outputs={"trajectory/events.jsonl"},
        )


def test_run_acceptance_uses_public_api_and_persists_complete_evidence(tmp_path: Path) -> None:
    payload = _bundle()
    calls: list[tuple[str, str]] = []
    batch_reads = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal batch_reads
        calls.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/api/v1/monitor/summary":
            if request.url.params.get("batch_id"):
                return httpx.Response(200, json=_monitor(nodes=1, slots=1, occupied=1))
            if any(path.endswith("/bundle/download") for _, path in calls):
                return httpx.Response(200, json=_monitor(nodes=0, slots=0))
            return httpx.Response(200, json=_monitor(nodes=0, slots=0))
        if request.method == "POST" and request.url.path == "/api/v1/batches":
            submitted = json.loads(request.content)
            assert submitted["backend"] == "nebius"
            assert submitted["n_per_task"] == 1
            return httpx.Response(
                201,
                json={
                    "batch_id": _BATCH_ID,
                    "name": submitted["name"],
                    "backend": "nebius",
                    "expected_trial_count": 1,
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/batches/{_BATCH_ID}":
            batch_reads += 1
            terminal = batch_reads > 1
            return httpx.Response(
                200,
                json={
                    "id": _BATCH_ID,
                    "state": "finished" if terminal else "running",
                    "result_status": "succeeded" if terminal else None,
                    "trial_summary": {
                        "queued": 0,
                        "claimed": 0,
                        "running": 0 if terminal else 1,
                        "materializing": 0,
                        "succeeded": 1 if terminal else 0,
                        "failed": 0,
                        "cancelled": 0,
                    },
                    "service_execution_runtime_profile": {
                        "candidate_sha": _CANDIDATE,
                        "sha256": "d" * 64,
                    },
                    "service_execution_summary": {"canonical_ready_count": 1},
                },
            )
        if request.method == "GET" and request.url.path == "/api/v1/trials":
            assert request.url.params["batch_id"] == _BATCH_ID
            return httpx.Response(
                200,
                json={"items": [{"id": _TRIAL_ID, "state": "succeeded"}], "next_cursor": None},
            )
        if request.method == "GET" and request.url.path == f"/api/v1/trials/{_TRIAL_ID}":
            return httpx.Response(
                200,
                json={
                    "id": _TRIAL_ID,
                    "task_id": "ts/test/task-1",
                    "state": "succeeded",
                    "materialization": {
                        "canonical_ready": True,
                        "source_cleanup_state": "complete",
                        "bundle": {"file_count": 1},
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == f"/api/v1/trials/{_TRIAL_ID}/bundle/download"
        ):
            return httpx.Response(
                200,
                content=payload,
                headers={"X-Content-SHA256": "sha256:" + hashlib.sha256(payload).hexdigest()},
            )
        return httpx.Response(404, json={"detail": "unhandled test route"})

    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        evidence = run_acceptance(
            client=client,
            output_dir=tmp_path / "evidence",
            capacity_policy=_policy(tmp_path),
            task_set_id="ts/test",
            task_count=1,
            provider_connection={"id": _CONNECTION_ID},
            provider_model_id="model",
            agent_name="litellm",
            agent_provider="openai",
            candidate_sha=_CANDIDATE,
            environment="development",
            pool_id="nebius-cpu",
            stages=[1],
            poll_seconds=0,
            stage_timeout_seconds=1,
            scale_down_timeout_seconds=1,
            sleeper=lambda _: None,
        )

    output = tmp_path / "evidence"
    assert evidence["accepted"] is True
    assert evidence["stages"][0]["max_overlapping_execution_units"] == 1
    assert evidence["stages"][0]["max_node_backed_overlapping_execution_units"] == 1
    assert (output / "stage-1" / "trials" / f"{_TRIAL_ID}.tar.gz").read_bytes() == payload
    evidence_bytes = (output / "acceptance.json").read_bytes()
    sidecar = (output / "acceptance.json.sha256").read_text()
    assert sidecar == f"{hashlib.sha256(evidence_bytes).hexdigest()}  acceptance.json\n"
    assert not any(path.startswith("/admin/") for _, path in calls)


def test_run_acceptance_refuses_non_idle_shared_baseline(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_monitor(nodes=1, slots=8))

    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(NebiusAcceptanceError, match="idle 0-node"):
            run_acceptance(
                client=client,
                output_dir=tmp_path / "evidence",
                capacity_policy=_policy(tmp_path),
                task_set_id="ts/test",
                task_count=1,
                provider_connection={"id": "connection"},
                provider_model_id="model",
                agent_name="litellm",
                agent_provider="openai",
                candidate_sha=_CANDIDATE,
                environment="development",
                pool_id="nebius-cpu",
                stages=[1],
                poll_seconds=0,
                stage_timeout_seconds=1,
                scale_down_timeout_seconds=1,
                sleeper=lambda _: None,
            )


def test_run_acceptance_refuses_output_file_before_api_use(tmp_path: Path) -> None:
    output = tmp_path / "evidence"
    output.write_text("owned", encoding="utf-8")

    with httpx.Client(base_url="https://loom.test") as client:
        with pytest.raises(NebiusAcceptanceError, match="not a directory"):
            run_acceptance(
                client=client,
                output_dir=output,
                capacity_policy=_policy(tmp_path),
                task_set_id="ts/test",
                task_count=1,
                provider_connection={"id": "connection"},
                provider_model_id="model",
                agent_name="litellm",
                agent_provider="openai",
                candidate_sha=_CANDIDATE,
                environment="development",
                pool_id="nebius-cpu",
                stages=[1],
                poll_seconds=0,
                stage_timeout_seconds=1,
                scale_down_timeout_seconds=1,
                sleeper=lambda _: None,
            )
    assert output.read_text(encoding="utf-8") == "owned"


def test_run_acceptance_cancels_nonterminal_batch_on_timeout(tmp_path: Path) -> None:
    cancelled = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancelled
        if request.method == "GET" and request.url.path == "/api/v1/monitor/summary":
            return httpx.Response(
                200,
                json=(
                    _monitor(nodes=1, slots=1, occupied=1)
                    if request.url.params.get("batch_id")
                    else _monitor(nodes=0, slots=0)
                ),
            )
        if request.method == "POST" and request.url.path == "/api/v1/batches":
            return httpx.Response(
                201,
                json={
                    "batch_id": _BATCH_ID,
                    "backend": "nebius",
                    "expected_trial_count": 1,
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/batches/{_BATCH_ID}":
            return httpx.Response(
                200,
                json={
                    "state": "running",
                    "trial_summary": {"running": 1},
                },
            )
        if request.method == "POST" and request.url.path == f"/api/v1/batches/{_BATCH_ID}/cancel":
            cancelled = True
            return httpx.Response(202, json={"state": "cancelling"})
        return httpx.Response(404)

    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(NebiusAcceptanceError, match="timed out"):
            run_acceptance(
                client=client,
                output_dir=tmp_path / "evidence",
                capacity_policy=_policy(tmp_path),
                task_set_id="ts/test",
                task_count=1,
                provider_connection={"id": "connection"},
                provider_model_id="model",
                agent_name="litellm",
                agent_provider="openai",
                candidate_sha=_CANDIDATE,
                environment="development",
                pool_id="nebius-cpu",
                stages=[1],
                poll_seconds=0,
                stage_timeout_seconds=0,
                scale_down_timeout_seconds=1,
                sleeper=lambda _: None,
            )
    assert cancelled is True


def test_cli_reports_local_taskset_error_before_login(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "loom.nebius-development-capacity.v1",
                "target_id": "nebius-eu-north1-development",
                "accepted_concurrency": 1,
                "target_concurrency": 200,
                "admission_policies": [
                    {
                        "scope_kind": "global",
                        "scope_key": "*",
                        "max_concurrent": 1,
                        "enabled": True,
                    },
                    {
                        "scope_kind": "pool",
                        "scope_key": "nebius-cpu",
                        "max_concurrent": 1,
                        "enabled": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        capacity_policy=str(policy_path),
        stage=[],
        taskset_dir=str(tmp_path / "missing"),
        task_set=None,
        http_timeout=1,
    )

    assert run_cli(args) == 1
    assert "manifest.yaml not found" in capsys.readouterr().err
