"""Delivery bundle export for completed batch families (#390)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import tarfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    Batch,
    LlmCall,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.head_errors: dict[tuple[str, str], str] = {}
        self.get_errors: dict[tuple[str, str], str] = {}
        self.heads: list[tuple[str, str]] = []
        self.put_body_types: list[type[object]] = []
        self.put_body_types_by_key: dict[tuple[str, str], type[object]] = {}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        self.heads.append((Bucket, Key))
        if code := self.head_errors.get((Bucket, Key)):
            raise ClientError(
                {"Error": {"Code": code, "Message": code}},
                "HeadObject",
            )
        body = self.objects.get((Bucket, Key))
        if body is None:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )
        return {"ContentLength": len(body)}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        if code := self.get_errors.get((Bucket, Key)):
            raise ClientError(
                {"Error": {"Code": code, "Message": code}},
                "GetObject",
            )
        body = self.objects.get((Bucket, Key))
        if body is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                "GetObject",
            )
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}

    def put_object(self, *, Bucket: str, Key: str, Body: object) -> dict[str, str]:  # noqa: N803
        self.put_body_types.append(type(Body))
        self.put_body_types_by_key[(Bucket, Key)] = type(Body)
        if isinstance(Body, bytes):
            data = Body
        elif isinstance(Body, str):
            data = Body.encode()
        else:
            read = getattr(Body, "read", None)
            if not callable(read):
                raise TypeError("Body must be bytes, str, or file-like")
            data = read()
        self.objects[(Bucket, Key)] = bytes(data)
        return {"ETag": hashlib.md5(bytes(data), usedforsecurity=False).hexdigest()}


@pytest.fixture
async def delivery_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[dict[str, object]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.minio_client = _FakeS3Client()
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    read_only_raw = f"loom_team_{uuid4().hex}"
    now = datetime.now(UTC)
    main_batch_id = uuid4()
    supplemental_batch_id = uuid4()
    targeted_batch_id = uuid4()
    task_ids = [f"source-useful-5003/task-{i:04d}" for i in range(1, 5)]

    selected_trials: dict[str, UUID] = {
        task_ids[0]: uuid4(),
        task_ids[1]: uuid4(),
        task_ids[2]: uuid4(),
        task_ids[3]: uuid4(),
    }
    failed_trial = uuid4()
    cancelled_trial = uuid4()
    still_failed_trial = uuid4()

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"Delivery Team {team_id.hex[:8]}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                issued_at=now,
                expires_at=None,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(read_only_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=now,
                expires_at=None,
            )
        )
        for task_id in task_ids:
            source = None
            if task_id == task_ids[0]:
                source = f"s3://task-bundles/{task_id}/"
            s.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="d" * 64,
                    config={"task": {"id": task_id, "name": task_id}},
                    source=source or "local",
                )
            )
        common_batch = {
            "team_id": team_id,
            "description": None,
            "task_filter": {"subset_kind": "explicit", "task_ids": task_ids},
            "trial_config": {
                "agent_name": "opencode",
                "agent_model": {"provider": "yibu", "name": "glm-5.1-thinking"},
            },
            "state": "finished",
            "created_by_token_prefix": "test",
            "expected_trial_count": 4,
            "n_per_task": 1,
            "backend": "gb10",
            "combinations": [],
            "provider_model_id": "glm-5.1-thinking",
            "finished_at": now,
        }
        s.execute(
            insert(Batch).values(
                id=main_batch_id,
                name="source-useful-5003-opencode-glm51-gb10",
                result_status="partial_failed",
                created_at=now,
                **common_batch,
            )
        )
        s.execute(
            insert(Batch).values(
                id=supplemental_batch_id,
                name="source-useful-5003-primary32",
                result_status="partial_failed",
                rerun_of_batch_id=main_batch_id,
                created_at=now + timedelta(minutes=1),
                **common_batch,
            )
        )
        s.execute(
            insert(Batch).values(
                id=targeted_batch_id,
                name="source-useful-5003-http-parser-timeout1",
                result_status="succeeded",
                rerun_of_batch_id=main_batch_id,
                created_at=now + timedelta(minutes=2),
                **common_batch,
            )
        )

        def add_trial(
            *,
            trial_id: UUID,
            batch_id: UUID,
            task_id: str,
            state: str,
            reward: float | None,
            failure_reason: str | None = None,
            submitted_offset: int = 0,
        ) -> None:
            result = {"aggregate_reward": reward} if reward is not None else None
            trajectory_index = None
            if state == "succeeded":
                prefix = f"{team_id}/{trial_id}"
                trajectory_index = {
                    "trajectory_uri": f"s3://{settings.trajectories_bucket}/{prefix}/events.jsonl",
                    "atif_uri": f"s3://{settings.trajectories_bucket}/{prefix}/atif.json",
                }
                app.state.minio_client.objects[
                    (settings.trajectories_bucket, f"{prefix}/events.jsonl")
                ] = (json.dumps({"trial_id": str(trial_id), "task_id": task_id}) + "\n").encode()
                app.state.minio_client.objects[
                    (settings.trajectories_bucket, f"{prefix}/atif.json")
                ] = json.dumps({"version": "1.7", "trial_id": str(trial_id)}).encode()
            s.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    batch_id=batch_id,
                    state=state,
                    failure_reason=failure_reason,
                    failure_message=failure_reason,
                    config=common_batch["trial_config"],
                    requires_caps={},
                    submitted_at=now + timedelta(seconds=submitted_offset),
                    started_at=now + timedelta(seconds=submitted_offset) if state == "succeeded" else None,
                    finished_at=now + timedelta(seconds=submitted_offset + 1),
                    sample_idx=0,
                    combination_idx=0,
                    result=result,
                    trajectory_index=trajectory_index,
                    provider_model_id="glm-5.1-thinking",
                    visibility="org",
                    share_status="shared",
                )
            )

        add_trial(
            trial_id=selected_trials[task_ids[0]],
            batch_id=main_batch_id,
            task_id=task_ids[0],
            state="succeeded",
            reward=1.0,
        )
        add_trial(
            trial_id=failed_trial,
            batch_id=main_batch_id,
            task_id=task_ids[1],
            state="failed",
            reward=None,
            failure_reason="gateway_error",
        )
        add_trial(
            trial_id=cancelled_trial,
            batch_id=main_batch_id,
            task_id=task_ids[2],
            state="cancelled",
            reward=None,
            failure_reason="cancelled",
        )
        add_trial(
            trial_id=selected_trials[task_ids[3]],
            batch_id=main_batch_id,
            task_id=task_ids[3],
            state="succeeded",
            reward=0.0,
        )
        add_trial(
            trial_id=selected_trials[task_ids[1]],
            batch_id=supplemental_batch_id,
            task_id=task_ids[1],
            state="succeeded",
            reward=0.0,
            submitted_offset=10,
        )
        add_trial(
            trial_id=still_failed_trial,
            batch_id=supplemental_batch_id,
            task_id=task_ids[2],
            state="failed",
            reward=None,
            failure_reason="retry_exhausted",
            submitted_offset=11,
        )
        add_trial(
            trial_id=selected_trials[task_ids[2]],
            batch_id=targeted_batch_id,
            task_id=task_ids[2],
            state="succeeded",
            reward=1.0,
            submitted_offset=20,
        )
        s.commit()

    try:
        yield {
            "app": app,
            "raw": raw,
            "read_only_raw": read_only_raw,
            "team_id": team_id,
            "main_batch_id": main_batch_id,
            "supplemental_batch_id": supplemental_batch_id,
            "targeted_batch_id": targeted_batch_id,
            "selected_trials": selected_trials,
            "task_ids": task_ids,
            "settings": settings,
            "fake_s3": app.state.minio_client,
        }
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(ArtifactLineageEdge))
            s.execute(delete(Artifact))
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            s.execute(delete(Team).where(Team.id == team_id))
            s.commit()
        sync_engine.dispose()


async def test_delivery_export_creates_5003_style_bundle_and_records_artifact(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    fake_s3 = delivery_setup["fake_s3"]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["status"] == "ready"
    assert len(body["sha256"]) == 64
    assert body["archive_filename"].endswith(".tar.gz")
    assert body["manifest"]["schema_version"] == "1"
    assert body["manifest"]["selection_rule"] == (
        "highest_priority_succeeded_by_task_sample_combination"
    )
    assert body["manifest"]["batch_family"] == {
        "main_batch_id": str(main_batch_id),
        "supplemental_batch_ids": [str(supplemental_batch_id), str(targeted_batch_id)],
    }
    assert body["manifest"]["task_count"] == 4
    assert body["manifest"]["trial_count"] == 4
    assert body["manifest"]["source_counts"] == {
        str(main_batch_id): 2,
        str(supplemental_batch_id): 1,
        str(targeted_batch_id): 1,
    }
    assert body["manifest"]["reward_distribution"] == {"0.0": 2, "1.0": 2}
    assert body["manifest"]["model_provider"] == "yibu"
    assert body["manifest"]["model_name"] == "glm-5.1-thinking"
    assert body["manifest"]["object_counts"] == {"atif": 4, "trajectory": 4}
    assert body["manifest"]["archive_sha256"] == body["sha256"]

    # HEAD validation must happen before the export is declared ready.
    assert len(fake_s3.heads) == 8  # type: ignore[attr-defined]
    assert body["object_validation"]["checked"] == 8
    assert body["object_validation"]["missing"] == []

    archive_key = body["storage"]["key"]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], archive_key)]  # type: ignore[attr-defined]
    assert hashlib.sha256(archive_bytes).hexdigest() == body["sha256"]

    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        names = sorted(tar.getnames())
        assert "manifest.json" in names
        assert "summary.json" in names
        assert "ledger/trials.jsonl" in names
        assert "ledger/trials.csv" in names
        assert "checksums/SHA256SUMS" in names
        assert sum(name.startswith("trajectories/") for name in names) == 4
        assert sum(name.startswith("atif/") for name in names) == 4
        manifest = json.load(tar.extractfile("manifest.json"))  # type: ignore[arg-type]
        assert manifest["task_count"] == 4
        assert "archive_sha256" not in manifest
        assert manifest["payload_checksums"] == {
            "algorithm": "sha256",
            "file": "checksums/SHA256SUMS",
            "scope": "archive payload files excluding checksums/SHA256SUMS",
        }
        checksums = tar.extractfile("checksums/SHA256SUMS").read().decode().splitlines()  # type: ignore[union-attr]
        manifest_checksum = hashlib.sha256(
            tar.extractfile("manifest.json").read()  # type: ignore[union-attr]
        ).hexdigest()
        assert f"{manifest_checksum}  manifest.json" in checksums
        ledger_lines = tar.extractfile("ledger/trials.jsonl").read().decode().splitlines()  # type: ignore[union-attr]
        ledger = [json.loads(line) for line in ledger_lines]
        assert [row["task_id"] for row in ledger] == sorted(task_ids)
        assert {row["selected_trial_id"] for row in ledger} == {
            str(value) for value in selected_trials.values()
        }
        csv_rows = list(
            csv.DictReader(
                io.StringIO(
                    tar.extractfile("ledger/trials.csv").read().decode(),  # type: ignore[union-attr]
                )
            )
        )
        assert len(csv_rows) == 4
        assert {row["selection_source"] for row in csv_rows} == {
            "main",
            "supplemental",
        }

    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as s:
            artifact = (
                s.execute(
                    select(Artifact).where(
                        Artifact.batch_id == main_batch_id,
                        Artifact.artifact_type == "trajectory_bundle",
                    )
                )
                .scalars()
                .one()
            )
            assert artifact.content_hash == f"sha256:{body['sha256']}"
            assert artifact.artifact_metadata["delivery_export"]["status"] == "ready"
            assert artifact.artifact_metadata["delivery_export"]["task_count"] == 4
            assert artifact.artifact_metadata["delivery_export"]["manifest"][
                "archive_sha256"
            ] == body["sha256"]
            assert artifact.provenance["source_batch_ids"] == [
                str(main_batch_id),
                str(supplemental_batch_id),
                str(targeted_batch_id),
            ]
    finally:
        sync_engine.dispose()


async def test_raw_harbor_tb2_delivery_export_streams_sample_compatible_bundle(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    fake_s3 = delivery_setup["fake_s3"]

    task_bundle_key = f"{task_ids[0]}/task.toml"
    fake_s3.objects[("task-bundles", task_bundle_key)] = (  # type: ignore[attr-defined]
        f"id = \"{task_ids[0]}\"\n"
    ).encode()
    fake_s3.objects[("task-bundles", f"{task_ids[0]}/instruction.md")] = (  # type: ignore[attr-defined]
        b"solve the task\n"
    )

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            captured_at_base = datetime.now(UTC)
            first_assistant_content = ""
            for index, task_id in enumerate(task_ids, start=1):
                trial_id = selected_trials[task_id]
                assistant_content = f"answer {index}"
                if index == 1:
                    assistant_content = json.dumps(
                        {
                            "state_analysis": "fresh shell",
                            "explanation": "inspect files",
                            "commands": [
                                {
                                    "keystrokes": "ls -la\n",
                                    "is_blocking": True,
                                    "timeout_sec": 5,
                                }
                            ],
                            "is_task_complete": False,
                        }
                    )
                    first_assistant_content = assistant_content
                conn.execute(
                    insert(LlmCall).values(
                        team_id=delivery_setup["team_id"],
                        trial_id=trial_id,
                        step_id=f"step-{index}",
                        dialect="openai_facade",
                        model="gpt-4o",
                        input_tokens=10,
                        output_tokens=5,
                        provider_extras={
                            "_loom_raw_provider_log": {
                                "schema_version": "1",
                                "ref": (
                                    "llm_calls/"
                                    f"raw-{index}/provider_extras/_loom_raw_provider_log"
                                ),
                                "request": {
                                    "body": {
                                        "messages": [
                                            {
                                                "role": "user",
                                                "content": f"prompt {index}",
                                            }
                                        ]
                                    }
                                },
                                "response": {
                                    "body": {
                                        "choices": [
                                            {
                                                "message": {
                                                    "role": "assistant",
                                                    "content": assistant_content,
                                                }
                                            }
                                        ]
                                    }
                                },
                            }
                        },
                        request_params={"status": "available", "parameters": {}},
                        cost_usd=0,
                        rate_card_hash="facade:operator-supplied",
                        captured_at=captured_at_base + timedelta(seconds=index),
                    )
                )
            conn.execute(
                insert(LlmCall).values(
                    team_id=delivery_setup["team_id"],
                    trial_id=selected_trials[task_ids[0]],
                    step_id="step-1-followup",
                    dialect="openai_facade",
                    model="gpt-4o",
                    input_tokens=12,
                    output_tokens=6,
                    provider_extras={
                        "_loom_raw_provider_log": {
                            "schema_version": "1",
                            "ref": (
                                "llm_calls/raw-1b/provider_extras/"
                                "_loom_raw_provider_log"
                            ),
                            "request": {
                                "body": {
                                    "messages": [
                                        {"role": "user", "content": "prompt 1"},
                                        {
                                            "role": "assistant",
                                            "content": first_assistant_content,
                                        },
                                        {"role": "user", "content": "total 1\n"},
                                    ]
                                }
                            },
                            "response": {
                                "body": {
                                    "choices": [
                                        {
                                            "message": {
                                                "role": "assistant",
                                                "content": "answer after ls",
                                            }
                                        }
                                    ]
                                }
                            },
                        }
                    },
                    request_params={"status": "available", "parameters": {}},
                    cost_usd=0,
                    rate_card_hash="facade:operator-supplied",
                    captured_at=captured_at_base + timedelta(seconds=100),
                )
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor-tb2-v1",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["manifest"]["mode"] == "raw-harbor-tb2-v1"
    assert body["manifest"]["export_profile"] == {
        "name": "raw-harbor-tb2",
        "version": "1",
        "source_of_truth": "provider_logs",
        "audit_spine": "loom_trajectory.jsonl",
    }
    assert body["manifest"]["object_counts"]["provider_logs"] == 5
    assert body["manifest"]["object_counts"]["task_bundle_files"] == 2
    assert body["archive_filename"].endswith("-raw-harbor-tb2-v1.tar.gz")

    archive_key = body["storage"]["key"]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], archive_key)]  # type: ignore[attr-defined]
    assert hashlib.sha256(archive_bytes).hexdigest() == body["sha256"]
    # The archive body must be uploaded from a file-like spool, not held as one
    # fully materialized bytes object before put_object.
    assert fake_s3.put_body_types_by_key[(body["storage"]["bucket"], archive_key)] is not bytes  # type: ignore[attr-defined]

    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        names = set(tar.getnames())
        assert "manifest.json" in names
        assert "summary.json" in names
        assert "provider_logs/manifest.json" in names
        assert "derived/sft_messages.jsonl" in names
        assert f"task_bundles/{first_task}/task.toml" in names
        assert f"task_bundles/{first_task}/instruction.md" in names
        assert f"agent_runs/{first_task}/{first_trial}/execution_result.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/metrics.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/artifact_manifest.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/verifier_output.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/provider_logs_manifest.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/trajectory.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/loom_trajectory.jsonl" in names
        manifest = json.load(tar.extractfile("manifest.json"))  # type: ignore[arg-type]
        assert manifest["mode"] == "raw-harbor-tb2-v1"
        assert manifest["layout"] == {
            "top_level_manifests": ["manifest.json", "summary.json"],
            "task_bundles": "task_bundles/<task_id>/...",
            "agent_runs": "agent_runs/<task_id>/<trial_id>/...",
            "derived": "derived/sft_messages.jsonl",
        }
        provider_manifest = json.load(
            tar.extractfile("provider_logs/manifest.json")  # type: ignore[arg-type]
        )
        assert len(provider_manifest["logs"]) == 5
        first_provider_log = json.load(
            tar.extractfile(provider_manifest["logs"][0]["archive_path"])  # type: ignore[arg-type]
        )
        rendered_provider_log = json.dumps(first_provider_log)
        assert "prompt " in rendered_provider_log
        assert "state_analysis" in rendered_provider_log
        assert "sk-" not in rendered_provider_log
        sft_lines = (
            tar.extractfile("derived/sft_messages.jsonl")  # type: ignore[union-attr]
            .read()
            .decode()
            .splitlines()
        )
        assert len(sft_lines) == 5
        first_sft_row = json.loads(sft_lines[0])
        assert first_sft_row["reward"] == 1.0
        assert first_sft_row["reward_positive"] is True
        assert first_sft_row["source"] == "provider_logs"
        assert first_sft_row["messages"] == [
            {"role": "user", "content": "prompt 1"},
            {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "analysis": "fresh shell",
                        "plan": "inspect files",
                        "commands": [{"keystrokes": "ls -la\n", "duration": 5}],
                        "task_complete": False,
                    }
                ),
            },
        ]
        artifact_manifest = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/artifact_manifest.json"
            )
        )
        assert {
            artifact["kind"]: artifact["path"]
            for artifact in artifact_manifest["artifacts"]
        }["trajectory"] == "trajectory.json"
        assert {
            artifact["kind"]: artifact["path"]
            for artifact in artifact_manifest["artifacts"]
        }["agent_native_trajectory"] == "loom_trajectory.jsonl"
        trajectory = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/trajectory.json"
            )
        )
        assert trajectory["schema_version"] == "ATIF-v1.7"
        assert trajectory["agent"]["name"] == "opencode"
        assert trajectory["agent"]["model_name"] == "gpt-4o"
        assert trajectory["steps"][0]["source"] == "user"
        assert trajectory["steps"][1]["source"] == "agent"
        assert trajectory["steps"][1]["message"] == "Analysis: fresh shell\nPlan: inspect files"
        assert trajectory["steps"][1]["observation"] == {
            "results": [{"content": "total 1\n"}]
        }
        assert trajectory["steps"][1]["tool_calls"] == [
            {
                "tool_call_id": "call-1-1",
                "function_name": "bash_command",
                "arguments": {"keystrokes": "ls -la\n", "duration": 5},
            }
        ]


async def test_raw_harbor_delivery_export_preserves_loom_native_trajectory(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    fake_s3 = delivery_setup["fake_s3"]
    assistant_content = json.dumps(
        {
            "state_analysis": "fresh shell",
            "explanation": "inspect files",
            "commands": [{"keystrokes": "ls -la\n", "timeout_sec": 5}],
            "is_task_complete": False,
        }
    )

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            for index, task_id in enumerate(task_ids, start=1):
                conn.execute(
                    insert(LlmCall).values(
                        team_id=delivery_setup["team_id"],
                        trial_id=selected_trials[task_id],
                        step_id=f"raw-step-{index}",
                        dialect="openai_facade",
                        model="gpt-4o",
                        input_tokens=10,
                        output_tokens=5,
                        provider_extras={
                            "_loom_raw_provider_log": {
                                "schema_version": "1",
                                "request": {
                                    "body": {
                                        "messages": [
                                            {
                                                "role": "user",
                                                "content": f"prompt {index}",
                                            }
                                        ]
                                    }
                                },
                                "response": {
                                    "body": {
                                        "choices": [
                                            {
                                                "message": {
                                                    "role": "assistant",
                                                    "content": assistant_content,
                                                }
                                            }
                                        ]
                                    }
                                },
                            }
                        },
                        request_params={"status": "available", "parameters": {}},
                        cost_usd=0,
                        rate_card_hash="facade:operator-supplied",
                    )
                )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["manifest"]["mode"] == "raw-harbor"
    assert "export_profile" not in body["manifest"]
    assert body["archive_filename"].endswith("-raw-harbor.tar.gz")

    archive_key = body["storage"]["key"]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], archive_key)]  # type: ignore[attr-defined]
    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        names = set(tar.getnames())
        assert f"agent_runs/{first_task}/{first_trial}/trajectory.jsonl" in names
        assert f"agent_runs/{first_task}/{first_trial}/trajectory.json" not in names
        assert f"agent_runs/{first_task}/{first_trial}/loom_trajectory.jsonl" not in names
        sft_lines = (
            tar.extractfile("derived/sft_messages.jsonl")  # type: ignore[union-attr]
            .read()
            .decode()
            .splitlines()
        )
        first_sft_row = json.loads(sft_lines[0])
        assert first_sft_row["messages"][1]["content"] == assistant_content
        assert "state_analysis" in first_sft_row["messages"][1]["content"]


async def test_delivery_export_create_requires_submit_scope(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["read_only_raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 403


async def test_delivery_export_fails_clearly_when_referenced_object_missing(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    settings = delivery_setup["settings"]
    fake_s3 = delivery_setup["fake_s3"]

    missing_trial = selected_trials[task_ids[0]]
    del fake_s3.objects[  # type: ignore[attr-defined]
        (
            settings.trajectories_bucket,  # type: ignore[attr-defined]
            f"{delivery_setup['team_id']}/{missing_trial}/atif.json",
        )
    ]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )
    assert response.status_code == 409
    body = response.json()
    assert body["detail"]["code"] == "delivery_export_objects_missing"
    assert body["detail"]["missing_objects"] == [
        {
            "kind": "atif",
            "trial_id": str(missing_trial),
            "bucket": settings.trajectories_bucket,  # type: ignore[attr-defined]
            "key": f"{delivery_setup['team_id']}/{missing_trial}/atif.json",
        }
    ]


async def test_delivery_export_fails_clearly_when_head_object_forbidden(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    settings = delivery_setup["settings"]
    fake_s3 = delivery_setup["fake_s3"]

    forbidden_trial = selected_trials[task_ids[0]]
    forbidden_key = f"{delivery_setup['team_id']}/{forbidden_trial}/atif.json"
    fake_s3.head_errors[  # type: ignore[attr-defined]
        (settings.trajectories_bucket, forbidden_key)  # type: ignore[attr-defined]
    ] = "AccessDenied"

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_objects_unreadable"
    assert detail["unreadable_objects"] == [
        {
            "kind": "atif",
            "trial_id": str(forbidden_trial),
            "bucket": settings.trajectories_bucket,  # type: ignore[attr-defined]
            "key": forbidden_key,
            "operation": "HeadObject",
            "error_code": "AccessDenied",
        }
    ]


async def test_delivery_export_fails_clearly_when_get_object_forbidden(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    settings = delivery_setup["settings"]
    fake_s3 = delivery_setup["fake_s3"]

    forbidden_trial = selected_trials[task_ids[0]]
    forbidden_key = f"{delivery_setup['team_id']}/{forbidden_trial}/atif.json"
    fake_s3.get_errors[  # type: ignore[attr-defined]
        (settings.trajectories_bucket, forbidden_key)  # type: ignore[attr-defined]
    ] = "AccessDenied"

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_objects_unreadable"
    assert detail["unreadable_objects"] == [
        {
            "kind": "atif",
            "trial_id": str(forbidden_trial),
            "bucket": settings.trajectories_bucket,  # type: ignore[attr-defined]
            "key": forbidden_key,
            "operation": "GetObject",
            "error_code": "AccessDenied",
        }
    ]
    assert not any(
        bucket == settings.artifacts_bucket and key.startswith("delivery-exports/")  # type: ignore[attr-defined]
        for bucket, key in fake_s3.objects  # type: ignore[attr-defined]
    )


async def test_delivery_export_rejects_unlinked_supplemental_batch(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    team_id = delivery_setup["team_id"]
    main_batch_id = delivery_setup["main_batch_id"]
    task_ids = delivery_setup["task_ids"]
    unlinked_batch_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            conn.execute(
                insert(Batch).values(
                    id=unlinked_batch_id,
                    team_id=team_id,
                    name="unlinked-supplemental",
                    description=None,
                    task_filter={"subset_kind": "explicit", "task_ids": task_ids},
                    trial_config={"agent_name": "opencode"},
                    state="finished",
                    result_status="succeeded",
                    created_at=now,
                    finished_at=now,
                    created_by_token_prefix="test",
                    expected_trial_count=0,
                    n_per_task=1,
                    backend="gb10",
                    combinations=[],
                )
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"supplemental_batch_ids": [str(unlinked_batch_id)]},
        )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_invalid_batch_family"
    assert "linked rerun" in detail["message"]


async def test_delivery_export_rejects_non_terminal_batch_family(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            conn.execute(
                update(Batch).where(Batch.id == main_batch_id).values(state="running")
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_invalid_batch_family"
    assert "terminal" in detail["message"]


async def test_delivery_export_rejects_unresolved_platform_failures(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"supplemental_batch_ids": [str(supplemental_batch_id)]},
        )
    assert response.status_code == 409
    body = response.json()
    assert body["detail"]["code"] == "delivery_export_unresolved_trials"
    assert body["detail"]["unresolved_trials"][0]["task_id"].startswith("source-useful-5003/")
