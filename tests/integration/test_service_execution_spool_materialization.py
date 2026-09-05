"""Real two-endpoint spool/canonical persistence proof for #1765.

This exercises the existing persisted materializer, not the application settings
wiring. Only disposable testcontainers are stopped; no shared service is used.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer

from loom.db.schema import Artifact, ServiceExecutionLease, Task, Trial, TrialEvent
from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    finalize_committed_service_execution,
    record_execution_event,
)
from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer
from loom_control_plane.service_execution_output import (
    ServiceExecutionOutputFileV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionOutputRouteService,
    ServiceExecutionPeerV1,
)
from tests.integration.test_service_execution_leases import (
    _complete_output_contract,
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)


@pytest.fixture
def independent_minio_endpoints() -> Iterator[tuple[MinioContainer, MinioContainer]]:
    label = {"loom.test": "service-execution-spool-materialization"}
    with (
        MinioContainer().with_kwargs(labels=label) as spool,
        MinioContainer().with_kwargs(labels=label) as canonical,
    ):
        assert spool.get_config()["endpoint"] != canonical.get_config()["endpoint"]
        spool.get_client().make_bucket("artifacts")
        canonical.get_client().make_bucket("artifacts")
        canonical.get_client().make_bucket("trajectories")
        yield spool, canonical


def _store(container: MinioContainer) -> MinioObjectStore:
    config = container.get_config()
    return MinioObjectStore(
        endpoint_url=f"http://{config['endpoint']}",
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        connect_timeout=0.2,
        read_timeout=0.2,
        operation_timeout=10,
        operation_attempts=1,
    )


async def test_independent_spool_survives_outage_restart_and_ack_gated_gc(
    isolated_migration_postgres_url: str,
    independent_minio_endpoints: tuple[MinioContainer, MinioContainer],
) -> None:
    spool_container, canonical_container = independent_minio_endpoints
    source_store, canonical_store = _store(spool_container), _store(canonical_container)
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    plan = _complete_output_contract(now=now)

    def materializer() -> ServiceExecutionMaterializer:
        # Zero retention/claim TTL advances time locally without waiting a day.
        # GC must still require a persisted canonical ACK, not just copied files.
        return ServiceExecutionMaterializer(
            session_factory=sessions,
            source_store=source_store,
            source_bucket="artifacts",
            canonical_store=canonical_store,
            artifacts_bucket="artifacts",
            trajectories_bucket="trajectories",
            retry_base_seconds=0,
            source_retention_seconds=0,
            claim_ttl_seconds=0,
        )

    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            task = await session.get(Task, trial.task_id)
            assert task is not None
            task.config = {
                "schema_version": "1",
                "task": {"id": task.id, "name": "Independent spool materialization"},
                "environment": {
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "docker_image": plan.task_image_ref,
                    "network_policies_supported": ["gateway-only"],
                    "baseline_network_policy": {"kind": "gateway-only"},
                },
                "agent": {"name": "direct-completion", "version": "1.0"},
                "verifier": {"name": "script", "env_mode": "shared"},
                "steps": [{"name": "main", "artifacts": ["answer.txt"]}],
            }
            trial.config = {
                "schema_version": "1",
                "agent_name": "direct-completion",
                "agent_model": {"provider": "openai", "name": "gpt-5"},
            }
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                runtime_contract=plan,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-spool",
                    "pod_uid": "pod-spool",
                    "pod_ip": "10.24.7.21",
                    "resource_version": "1",
                },
                observed_at=now,
            )
            await session.commit()
            session.expunge(lease)

        usage = {
            "rate_card_hash": "rate-v1",
            "gateway_request_id": "gateway-spool",
            "finish_reason": "stop",
            "input_tokens": 1,
            "cached_input_tokens": 0,
            "cache_write_tokens": 0,
            "output_tokens": 1,
            "thinking_tokens": 0,
            "provider_extras": {},
            "cost_usd": 0.01,
            "duration_sec": 1.0,
            "streamed": False,
            "time_to_first_token_sec": None,
            "attempt": 1,
        }
        trace = canonical_document(
            {
                "schema_version": "loom.service-execution-llm-call.v1",
                "turn": 0,
                "started_at": now.isoformat(),
                "finished_at": (now + timedelta(seconds=1)).isoformat(),
                "model": "openai/gpt-5",
                "request": {
                    "messages": [{"role": "user", "content": "answer"}],
                    "request_params": {},
                },
                "response": {"role": "assistant", "content": "42"},
                "usage": usage,
            }
        )
        payloads = {
            "01-agent.stdout": b"42\n",
            "01-agent.stderr": b"",
            "02-verifier.stdout": b"passed\n",
            "02-verifier.stderr": b"",
            "artifacts/answer.txt": b"42\n",
            "trajectory/events.jsonl": trace,
            "accounting/usage.json": canonical_document(
                {
                    "schema_version": "loom.service-execution-usage.v1",
                    "model": "openai/gpt-5",
                    "call_count": 1,
                    "totals": {
                        key: usage[key]
                        for key in (
                            "input_tokens",
                            "cached_input_tokens",
                            "cache_write_tokens",
                            "output_tokens",
                            "thinking_tokens",
                            "cost_usd",
                            "duration_sec",
                        )
                    },
                    "calls": [usage],
                }
            ),
            "verifier/output.json": b'{"rewards":{"passed":1.0}}',
        }
        result = _runtime_result_payload(lease, started_at=now)
        result.update(
            outputs=[
                {
                    **declaration.model_dump(mode="json"),
                    "state": "captured",
                    "size_bytes": len(payloads[declaration.relative_path]),
                    "sha256": digest_bytes(payloads[declaration.relative_path]),
                }
                for declaration in plan.output_declarations
            ],
            verifier_rewards={"passed": 1.0},
        )
        payloads = {"result.json": canonical_document(result), **dict(sorted(payloads.items()))}
        repository = SqlArtifactCommitRepository(
            session_factory=sessions,
            store=source_store,
            bucket="artifacts",
        )
        route = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(
                store=source_store,
                bucket="artifacts",
                repository=repository,
            ),
            session_factory=sessions,
        )
        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        grant = await route.prepare(
            lease=lease,
            request=ServiceExecutionOutputPrepareV1(
                schema_version="loom.service-execution-output-prepare.v1",
                request_id=uuid4(),
                **identity.model_dump(),
                files=tuple(
                    ServiceExecutionOutputFileV1(
                        relative_path=path,
                        media_type="application/octet-stream",
                        size_bytes=len(body),
                        sha256=digest_bytes(body),
                    )
                    for path, body in payloads.items()
                ),
            ),
        )
        upload_id, upload_token = UUID(grant["upload_session_id"]), str(grant["upload_token"])
        for index, payload in enumerate(payloads.values()):

            async def body(payload: bytes = payload):  # type: ignore[no-untyped-def]
                yield payload

            receipt = PartReceiptV1.model_validate(
                await route.put_part(
                    lease=lease,
                    session_id=upload_id,
                    file_index=index,
                    part_number=1,
                    content_length=len(payload),
                    content_sha256=digest_bytes(payload),
                    upload_token=upload_token,
                    body=body(),
                )
            )
            await route.complete_file(
                lease=lease,
                session_id=upload_id,
                file_index=index,
                ordered_parts=(receipt,),
                upload_token=upload_token,
            )
        await route.commit(lease=lease, session_id=upload_id, upload_token=upload_token)

        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            assert current is not None
            current.observed_state = "finalizing"
            assert await finalize_committed_service_execution(
                session,
                lease_id=lease.id,
                observed_at=now + timedelta(seconds=4),
            )
            # Compute deletion is already observed; storage recovery must not undo it.
            current.desired_state = current.observed_state = "deleted"
            current.cleanup_state = "complete"
            current.deleted_at = now + timedelta(seconds=5)
            await session.commit()

        source_prefix = f"service-executions/{lease.team_id}/{lease.id}/1/output/"
        source_keys = [
            obj.object_name
            for obj in spool_container.get_client().list_objects(
                "artifacts",
                prefix=source_prefix,
                recursive=True,
            )
        ]
        assert source_keys
        source_snapshot = {
            key: await source_store.get_object(bucket="artifacts", key=key) for key in source_keys
        }
        # Same bucket AND key exist in both endpoints: source cleanup must not
        # accidentally delete this object in the canonical endpoint.
        sentinel_key = source_keys[0]
        await canonical_store.put_object(
            bucket="artifacts",
            key=sentinel_key,
            body=b"canonical-endpoint-sentinel",
        )
        canonical_docker = canonical_container.get_wrapped_container()
        await asyncio.to_thread(canonical_docker.stop, timeout=1)
        try:
            assert await materializer().run_once()
            assert not await materializer().cleanup_source_once()
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                assert current is not None and trial is not None
                assert current.materialization_state == "pending"
                assert current.materialization_error_code == "transient_materialization_error"
                assert current.cleanup_state == "complete"
                assert current.deleted_at is not None
                assert trial.state == "materializing"
            for key, expected in source_snapshot.items():
                assert await source_store.get_object(bucket="artifacts", key=key) == expected
        finally:
            await asyncio.to_thread(canonical_docker.start)
        for attempt in range(100):
            try:
                if await asyncio.to_thread(
                    canonical_container.get_client().bucket_exists, "artifacts"
                ):
                    break
            except Exception:
                if attempt == 99:
                    raise
            await asyncio.sleep(0.1)
        else:
            pytest.fail("disposable canonical MinIO did not restart")
        # Docker may allocate a new ephemeral host port on container restart;
        # reconnect the fresh worker to that same canonical container/storage.
        canonical_store = _store(canonical_container)

        # Crash after object copies but before DB ACK. A fresh worker reclaims the
        # expired persisted claim, re-copies idempotently, and owns the only ACK.
        interrupted = materializer()
        stale_claim = await interrupted.claim_one()
        assert stale_claim is not None
        stale_result = await interrupted._load_and_materialize(stale_claim)
        assert not await materializer().cleanup_source_once()
        async with sessions() as session:
            pending = await session.get(ServiceExecutionLease, lease.id)
            assert pending is not None and pending.materialization_state == "running"
            assert pending.canonical_trajectory_sha256 is None
        restarted = materializer()
        assert await restarted.run_once()
        assert not await interrupted._commit(stale_claim, stale_result)
        assert not await restarted.run_once()

        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert current is not None and trial is not None
            assert current.materialization_state == "committed"
            assert current.materialization_attempts == 3
            assert current.source_cleanup_state == "retained"
            assert trial.state == "succeeded"
            artifact = (
                await session.scalars(
                    select(Artifact).where(
                        Artifact.control_producer_id == lease.id,
                    )
                )
            ).one()
            assert artifact.lifecycle_authority_id is not None
            files = artifact.storage["files"]
            evidence = artifact.storage["source_evidence"]
            assert {item["relative_path"] for item in files} == set(payloads)
            assert len(evidence) == 3
            assert trial.trajectory_index is not None
            trajectory_index = trial.trajectory_index
            events = list(
                await session.scalars(
                    select(TrialEvent).where(
                        TrialEvent.trial_id == trial_id,
                    )
                )
            )
            assert len(events) == 7
            assert len({event.seq for event in events}) == 7

        # Compute is gone and source GC is now ACK-authorized. Canonical files,
        # raw trace/accounting, source evidence, and derived ATIF remain readable.
        assert await restarted.cleanup_source_once()
        assert not await restarted.cleanup_source_once()
        assert not list(
            spool_container.get_client().list_objects(
                "artifacts",
                prefix=source_prefix,
                recursive=True,
            )
        )
        for item in [*files, *evidence]:
            downloaded = await canonical_store.get_object(bucket=item["bucket"], key=item["key"])
            assert digest_bytes(downloaded) == item["sha256"]
            assert len(downloaded) == item["size_bytes"]
            if item["relative_path"] in payloads:
                assert downloaded == payloads[item["relative_path"]]
        for name in ("trajectory", "atif"):
            key = trajectory_index[f"{name}_uri"].removeprefix("s3://trajectories/")
            downloaded = await canonical_store.get_object(bucket="trajectories", key=key)
            if name == "atif":
                assert json.loads(downloaded)["schema_version"] == "1.7"
            else:
                assert b'"kind":"llm_call"' in downloaded
        assert (
            await canonical_store.get_object(
                bucket="artifacts",
                key=sentinel_key,
            )
            == b"canonical-endpoint-sentinel"
        )
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.source_cleanup_state == "complete"
    finally:
        await engine.dispose()
