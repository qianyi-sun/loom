"""Exercise signed source-to-start consumption, not a fabricated verified object."""

import asyncio
import importlib
import json
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
import rfc8785

from loom.models.task import TaskConfig
from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
from tests.unit.test_task_image_execution_grant import fixture
from tests.unit.test_task_image_publication_signing import NOW


def module():
    name = "loom_worker.task_image_execution"
    assert importlib.util.find_spec(name) is not None, "worker source-to-start composition missing"
    return importlib.import_module(name)


def evidence(tmp_path):
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    (tmp_path / "db").mkdir()
    (tmp_path / "db/Dockerfile").write_text("FROM scratch\n")
    manifest = capture_task_image_bundle_manifest(tmp_path)

    def plan_change(plan):
        plan.update(
            task_checksum=manifest.task_checksum,
            bundle_content_manifest_sha256=manifest.digest,
            bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
            bundle_prefix=f"bench/revision/{manifest.digest}/",
        )

    payload, _, kwargs = fixture(
        arch="x86_64", plan_change=plan_change,
        publication_change=lambda value: value.update(task_checksum=manifest.task_checksum),
    )
    return payload, kwargs


def consumer(tmp_path, payload, kwargs, consume, *, clock=lambda: NOW):
    m = module()
    values = dict(kwargs)
    values.pop("now")
    return m.WorkerTaskImageExecution(
        **values, task_dir=tmp_path,
        task_config=TaskConfig.model_validate(json.loads(payload["canonical_task_config"])),
        task_checksum=payload["task_checksum"], cpu_arch="x86_64",
        task_image=payload["components"][0]["image"],
        consume=consume, clock=clock, timeout_seconds=1.0,
    )


def accepting():
    async def accept(request):
        m = module()
        return m.ExecutionStartReceipt.model_validate(dict(
            schema="loom.task-image-execution-start-receipt/v1",
            start_id="55555555-5555-4555-8555-555555555555",
            request_sha256=request.digest,
            consumed_at=NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            expires_at=(NOW + timedelta(seconds=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))
    return AsyncMock(side_effect=accept)


async def test_signed_source_and_full_set_precede_exact_online_start(tmp_path):
    payload, kwargs = evidence(tmp_path)
    consume = accepting()
    subject = consumer(tmp_path, payload, kwargs, consume)
    assert await subject.authorize() is True
    request = consume.call_args.args[0]
    assert request.grant_id == payload["grant_id"]
    assert request.revision == payload["revision"]
    assert request.claim == kwargs["expected_claim"]
    assert request.keyset_sha256 == payload["keyset_sha256"]
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.authorize()
    assert consume.await_count == 1


@pytest.mark.parametrize("change", ["source", "mode", "plan", "missing_component", "signature"])
async def test_invalid_local_or_signed_evidence_never_calls_start(tmp_path, change):
    payload, kwargs = evidence(tmp_path)
    if change == "source":
        (tmp_path / "Dockerfile").write_text("FROM untrusted\n")
    elif change == "mode":
        (tmp_path / "Dockerfile").chmod(0o755)
    elif change == "plan":
        kwargs["plan_wire"] += b" "
    elif change == "missing_component":
        kwargs["publication_wires"] = kwargs["publication_wires"][:1]
    else:
        envelope = json.loads(kwargs["wire"])
        envelope["signature"] = "a" * 86
        kwargs["wire"] = rfc8785.dumps(envelope)
    consume = accepting()
    with pytest.raises(ValueError):
        subject = consumer(tmp_path, payload, kwargs, consume)
        await subject.authorize()
    consume.assert_not_called()


@pytest.mark.parametrize("change", ["image", "task", "arch", "checksum"])
async def test_verified_grant_cannot_run_a_different_runtime(tmp_path, change):
    payload, kwargs = evidence(tmp_path)
    consume = accepting()
    subject = consumer(tmp_path, payload, kwargs, consume)
    if change == "image":
        subject.task_image = "local-agent-layer:latest"
    elif change == "task":
        subject.task_config.environment.workdir = "/untrusted"
    elif change == "arch":
        subject.cpu_arch = "arm64"
    else:
        subject.task_checksum = "2" * 64
    with pytest.raises(ValueError):
        await subject.authorize()
    consume.assert_not_called()


@pytest.mark.parametrize("change", ["source", "expired", "receipt"])
async def test_changes_during_online_call_fail_closed(tmp_path, change):
    payload, kwargs = evidence(tmp_path)
    now = NOW
    accept = accepting()

    async def consume(request):
        nonlocal now
        receipt = await accept(request)
        if change == "source":
            (tmp_path / "Dockerfile").write_text("FROM changed\n")
        elif change == "expired":
            now = NOW + timedelta(minutes=3)
        else:
            receipt = receipt.model_copy(update={"request_sha256": "1" * 64})
        return receipt

    subject = consumer(tmp_path, payload, kwargs, consume, clock=lambda: now)
    with pytest.raises(ValueError):
        await subject.authorize()
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.authorize()


@pytest.mark.parametrize("error", [TimeoutError, ConnectionError, asyncio.CancelledError])
async def test_lost_or_cancelled_online_response_cannot_be_retried(tmp_path, error):
    payload, kwargs = evidence(tmp_path)
    consume = AsyncMock(side_effect=error)
    subject = consumer(tmp_path, payload, kwargs, consume)
    with pytest.raises(error):
        await subject.authorize()
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.authorize()
    consume.assert_awaited_once()
