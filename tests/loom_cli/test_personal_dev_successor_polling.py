"""Deployment polling follows only the exact internal continuation of owner intent."""

import json
from uuid import uuid4

import httpx
import pytest

from loom_cli.personal_dev_deploy import PersonalDevDeployClient, PersonalDevDeployError
from tests.loom_cli.test_personal_dev_deploy import (
    _CANDIDATE_ID,
    _CANDIDATE_SHA,
    _OPERATION_ID,
    _environment,
    _operation,
)


@pytest.mark.parametrize("tamper", (
    None, "predecessor", "candidate", "subject", "incarnation", "epoch", "intent", "cycle",
))
def test_wait_ready_follows_exact_successor_chain(tamper):
    child_id, subject, incarnation = (str(uuid4()) for _ in range(3))
    shared = dict(candidate_id=_CANDIDATE_ID, subject_id=subject, subject_incarnation=incarnation)
    parent = _operation(state="superseded") | shared | {
        "kind": "capacity", "checkpoint": "membership_successor_created",
        "membership_successor_operation_id": child_id,
    }
    child = _operation(state="succeeded") | shared | {
        "id": child_id, "kind": "update", "operation_epoch": 2, "expected_operation_epoch": 1,
        "membership_predecessor_operation_id": _OPERATION_ID,
        "membership_continuation_kind": "capacity",
        "deployment_generation": 2, "idempotency_key": str(uuid4()), "attempt_id": str(uuid4()),
    }
    if tamper == "predecessor":
        child["membership_predecessor_operation_id"] = str(uuid4())
    elif tamper == "candidate":
        child["candidate_id"] = str(uuid4())
    elif tamper == "subject":
        child["subject_id"] = str(uuid4())
    elif tamper == "incarnation":
        child["subject_incarnation"] = str(uuid4())
    elif tamper == "epoch":
        child["operation_epoch"] = 3
    elif tamper == "intent":
        child["membership_continuation_kind"] = "destroy"
    elif tamper == "cycle":
        parent["membership_successor_operation_id"] = _OPERATION_ID
    paths = []

    def handler(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/operations/" + _OPERATION_ID):
            return httpx.Response(200, json=parent)
        if request.url.path.endswith("/operations/" + child_id):
            return httpx.Response(200, json=child)
        assert request.url.path == "/api/v1/dev-instances/alice"
        return httpx.Response(200, json=_environment(status="ready", epoch=2) | shared | {
            "operation_id": child_id, "deployment_generation": 2,
        })

    with httpx.Client(base_url="https://loom.example", transport=httpx.MockTransport(handler)) as client:
        args = dict(
            operation_id=_OPERATION_ID, candidate_sha=_CANDIDATE_SHA, min_slots=0, max_slots=2,
            operation_epoch=1, timeout=1, poll_interval=0.01, sleep=lambda _: None,
        )
        if tamper is None:
            result = PersonalDevDeployClient(client).wait_ready("alice", **args)
            assert result["operation_epoch"] == 2
            assert len(paths) == 3
        else:
            with pytest.raises(PersonalDevDeployError):
                PersonalDevDeployClient(client).wait_ready("alice", **args)


@pytest.mark.parametrize("field,value", (
    ("operation_id", str(uuid4())), ("subject_id", str(uuid4())),
    ("subject_incarnation", str(uuid4())), ("candidate_id", str(uuid4())),
    ("deployment_generation", 2), ("operation_step", "candidate_build"),
))
def test_ready_projection_must_identify_terminal_operation(field, value):
    def handler(request):
        body = _operation(state="succeeded") if "/operations/" in request.url.path else (
            _environment(status="ready", epoch=1) | {field: value}
        )
        return httpx.Response(200, json=body)

    with httpx.Client(base_url="https://loom.example", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(PersonalDevDeployError):
            PersonalDevDeployClient(client).wait_ready(
                "alice", operation_id=_OPERATION_ID, candidate_sha=_CANDIDATE_SHA,
                min_slots=0, max_slots=2, operation_epoch=1, timeout=1, poll_interval=0.01,
            )


@pytest.mark.parametrize("superseded", (False, True))
def test_apply_accepts_retained_previous_target_while_original_intent_is_pending(superseded):
    def handler(request):
        payload = json.loads(request.content)
        operation = _operation(state="superseded" if superseded else "running") | {
            "idempotency_key": payload["idempotency_key"],
        }
        if superseded:
            operation.update(checkpoint="membership_successor_created", membership_successor_operation_id=str(uuid4()))
        environment = _environment(status="updating", epoch=2 if superseded else 1) | {
            "candidate_sha": "e" * 64, "max_slots": 1,
        }
        return httpx.Response(202, json={"environment": environment, "operation": operation})

    with httpx.Client(base_url="https://loom.example", transport=httpx.MockTransport(handler)) as client:
        environment, operation = PersonalDevDeployClient(client).apply(
            name="alice", candidate={"id": _CANDIDATE_ID, "candidate_sha": _CANDIDATE_SHA},
            min_slots=0, max_slots=2, expected_operation_epoch=0,
        )
        assert operation["max_slots"] == 2
        assert environment["max_slots"] == 1
