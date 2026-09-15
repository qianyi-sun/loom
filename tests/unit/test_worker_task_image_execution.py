"""Exercise signed source-to-start consumption, not a fabricated verified object."""

import asyncio
import importlib
import hashlib
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


def signed_evidence(tmp_path):
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

    payload, private, kwargs = fixture(
        arch="x86_64", plan_change=plan_change,
        publication_change=lambda value: value.update(task_checksum=manifest.task_checksum),
    )
    return payload, private, kwargs


def evidence(tmp_path):
    payload, _, kwargs = signed_evidence(tmp_path)
    return payload, kwargs


def refreshed(payload, private, kwargs, now, *, rollover=False, **changes):
    from loom_task_image_authority.execution_delivery import TaskImageExecutionDelivery
    from tests.unit.test_task_image_execution_grant import sign_grant
    from tests.unit.test_task_image_publication_keyset import _sign, _time

    keyset = kwargs["keyset_wire"]
    if rollover:
        raw = json.loads(json.loads(keyset)["canonical_keyset"])
        raw.update(keyset_version=raw["keyset_version"] + 1, issued_at=_time(now), expires_at=_time(now + timedelta(minutes=5)))
        keyset = _sign(raw, private)
        changes.update(keyset_sha256=hashlib.sha256(keyset).hexdigest(), keyset_version=raw["keyset_version"])

    current = dict(payload, revision=payload["revision"] + 1,
                   issued_at=_time(now), expires_at=_time(now + timedelta(seconds=120)), **changes)
    return TaskImageExecutionDelivery.model_validate(dict(
        schema="loom.task-image-execution-delivery/v2", claim=kwargs["expected_claim"],
        grant_envelope=sign_grant(current, private).decode(), frozen_plan=kwargs["plan_wire"].decode(),
        publications=tuple(item.decode() for item in kwargs["publication_wires"]), keyset=keyset.decode(),
    ))


@pytest.mark.parametrize("change", ["valid", "early-rollover", "claim", "source", "publication", "lost-start"])
async def test_expired_preparation_refreshes_fresh_authority_without_retrying_start(tmp_path, change):
    payload, private, kwargs = signed_evidence(tmp_path)
    now = NOW + timedelta(seconds=5 if change == "early-rollover" else 121)
    delivery = refreshed(payload, private, kwargs, now, rollover=change == "early-rollover",
                         **({"task_source": "s3://another/source"} if change == "source" else {}))
    if change == "claim":
        delivery = delivery.model_copy(update={"claim": delivery.claim.model_copy(update={"claim_id": "9" * 36})})
    elif change == "publication":
        delivery = delivery.model_copy(update={"publications": delivery.publications[:1]})

    async def accept(request):
        if change == "lost-start":
            raise ConnectionError("acknowledgement lost after commit")
        return module().ExecutionStartReceipt.model_validate(dict(
            schema="loom.task-image-execution-start-receipt/v1", start_id=payload["grant_id"],
            request_sha256=request.digest, consumed_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            expires_at=(now + timedelta(seconds=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))

    consume = AsyncMock(side_effect=accept)
    subject = consumer(tmp_path, payload, kwargs, consume, clock=lambda: now)
    subject.refresh = AsyncMock(return_value=delivery)
    if change in {"valid", "early-rollover"}:
        assert await subject.authorize()
        assert consume.call_args.args[0].revision == 2
    else:
        with pytest.raises((ValueError, ConnectionError)):
            await subject.authorize()
    subject.refresh.assert_awaited_once()
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.authorize()
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.prepare()
    assert consume.await_count == (1 if change in {"valid", "early-rollover", "lost-start"} else 0)


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
        raw = subject.task_config.model_dump(mode="json")
        raw["environment"]["workdir"] = "/untrusted"
        subject.task_config = TaskConfig.model_validate(raw)
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
