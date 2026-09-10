"""Typed runtime rendering and journal replay preserve original launch authority."""

import json
from uuid import UUID

import pytest

from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import JournalRegressionError
from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_capacity_manager.launch_subject_contracts import ExecutableLaunchSubjectV3
from tests.unit.test_capacity_executor_executable import executor_fixture
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def typed_executor(tmp_path, *, pool="oldlab", purpose="personal-build-worker", policy_change=None):
    legacy, journal, manager, admission, slurm, _ = executor_fixture(tmp_path, work=None)
    context = typed_context(pool=pool, purpose=purpose)
    registration = legacy.registration.model_copy(update={
        "execution": ExecutionContextV2.model_validate(context.binding.execution.model_dump(exclude={"allocation_epoch", "executable"})),
        "pool_id": pool, "pool_generation": context.binding.pool_generation,
        "controller_authority_sha256": context.controller_authority.controller_authority_sha256})
    policy = context.policy if policy_change is None else policy_change(context.policy)
    try:
        runtime = ExecutablePoolExecutor(registration, journal, manager, admission, slurm,
            profile=context.profiles[0], profiles=context.profiles, typed_policy=policy,
            controller_authority=context.controller_authority, ownership_key=context.ownership_key,
            now=lambda: context.submitted_at, bootstrap_handoff_store=legacy._bootstrap_handoff_store)
    except BaseException:
        journal.__exit__(None, None, None)
        raise
    return runtime, journal, context


def facts(context):
    return ExecutableLaunchSubjectV3(binding=context.binding, configuration=context.subject.configuration,
        acknowledgement=context.subject.acknowledgement, authority=context.subject.authority)


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
def test_typed_runtime_renders_and_replays_retained_facts(tmp_path, pool, purpose):
    runtime, journal, context = typed_executor(tmp_path, pool=pool, purpose=purpose)
    try:
        subject = facts(context)
        rendered = runtime.render_launch(context.binding, launch_subject=subject)
        runtime._remember_launch(rendered, launch_subject=subject,
            bootstrap_registration_epoch=1, event="slurm-submit-requested")
        envelope = runtime._load_launch(context.binding.intent_id)
        assert envelope.rendered == rendered
        assert envelope.launch_subject == subject
        assert envelope.rendered.ownership_proof.metadata.subject_authority.purpose == purpose
        assert journal.pending_requests()[-1].event_kind == "slurm-submit-requested"
    finally:
        journal.__exit__(None, None, None)


def test_typed_runtime_refuses_missing_or_wrong_intent_facts(tmp_path):
    runtime, journal, context = typed_executor(tmp_path)
    try:
        with pytest.raises(ValueError, match="subject"):
            runtime.render_launch(context.binding)
        subject = facts(context).model_copy(update={"binding": context.binding.model_copy(update={"intent_id": UUID(int=777)})})
        with pytest.raises(ValueError):
            runtime.render_launch(context.binding, launch_subject=subject)
        assert journal.head.sequence == 0
    finally:
        journal.__exit__(None, None, None)


def test_typed_runtime_refuses_changed_complete_policy_root(tmp_path):
    with pytest.raises(ValueError):
        typed_executor(tmp_path, policy_change=lambda policy: policy.model_copy(update={
            "entries": (policy.entries[0].model_copy(update={"purpose": "application-worker"}),)}))


async def test_typed_launch_preparation_fetches_and_retains_before_consumption(tmp_path):
    from unittest.mock import AsyncMock
    runtime, journal, context = typed_executor(tmp_path)
    try:
        runtime.client.launch_subject = AsyncMock(return_value=facts(context))
        envelope = await runtime._prepare_launch(context.binding, bootstrap_registration_epoch=1)
        runtime.client.launch_subject.assert_awaited_once_with(context.binding)
        assert envelope.launch_subject == facts(context)
        assert envelope.rendered.ownership_proof.metadata.binding == context.binding
        assert journal.head.sequence > 0
        assert runtime.client.central_requests == []
        assert journal.pending_requests() == ()
    finally:
        journal.__exit__(None, None, None)


async def test_typed_runtime_operations_remain_closed_until_consumers_are_ready(tmp_path):
    from loom_capacity_executor.runtime_profiles import RuntimeAssemblyError
    from tests.unit.test_capacity_executor_executable import permit_fixture
    runtime, journal, context = typed_executor(tmp_path)
    try:
        with pytest.raises(RuntimeAssemblyError, match="consumers"):
            await runtime._apply_one(permit_fixture(context.binding), await runtime.client.executable_checkpoint())
        assert journal.head.sequence == 0
        assert runtime.client.central_requests == []
    finally:
        journal.__exit__(None, None, None)


@pytest.mark.parametrize("tamper", ("request", "reference", "proof", "schema"))
def test_typed_runtime_replay_rejects_tampered_envelope(tmp_path, tamper):
    from hashlib import sha256
    runtime, journal, context = typed_executor(tmp_path)
    try:
        subject = facts(context)
        rendered = runtime.render_launch(context.binding, launch_subject=subject)
        runtime._remember_launch(rendered, launch_subject=subject,
            bootstrap_registration_epoch=1, event="slurm-submit-requested")
        record = journal.latest("job", str(context.binding.intent_id))
        value = json.loads(record.durable_payload())
        if tamper == "request":
            value["request"]["ownership_token"] = "changed"
        elif tamper == "reference":
            value["launch_facts"]["sha256"] = "f" * 64
        elif tamper == "proof":
            value["ownership_proof"]["metadata"]["subject_authority"]["membership"]["head_sha256"] = "f" * 64
        else:
            value["ownership_proof"]["schema_version"] = 2
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
        record = journal.append("slurm-submit-requested", sha256(payload).hexdigest(),
            object_kind="job", object_id=str(context.binding.intent_id), payload=payload)
        with pytest.raises((ValueError, JournalRegressionError)):
            runtime._launch_envelope_from_record(context.binding.intent_id, record)
    finally:
        journal.__exit__(None, None, None)
