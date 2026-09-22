from __future__ import annotations

import socket
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from loom.execution_contract import workload_requirements_from_task
from loom.execution_runtime_contract import validate_runtime_plan_requirements
from loom.models.networking import WebAllowlist, WebDestination
from loom.service_execution_materialization import compile_service_execution_plan
from loom_llm_gateway.task_egress import EgressDenied, resolve_destination
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def policy() -> WebAllowlist:
    return WebAllowlist(destinations=(WebDestination(host="registry.npmjs.org", protocol="https"),))


def test_egress_policy_bound_to_plan_and_runtime_capability() -> None:
    task, trial, profile = _inputs()
    assert "task_egress" not in workload_requirements_from_task(task).model_dump(mode="json")
    kwargs = dict(task=task, trial=trial, profile=profile, source_provenance=_provenance(),
                  task_revision_sha256=_REVISION)
    assert "task_egress" not in compile_service_execution_plan(**kwargs).canonical_payload()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "baseline_network_policy": policy(), "network_policies_supported": frozenset({"web-allowlist"}),
    })})
    kwargs["task"] = task
    with pytest.raises(ValueError, match="task_egress_runtime_unavailable"):
        compile_service_execution_plan(**kwargs)
    kwargs["profile"] = profile.model_copy(update={"supports_task_web_egress": True})
    plan = compile_service_execution_plan(**kwargs)
    requirements = workload_requirements_from_task(task)
    assert plan.task_egress == requirements.task_egress == policy()
    validate_runtime_plan_requirements(plan, requirements)
    with pytest.raises(ValueError, match="network"):
        validate_runtime_plan_requirements(plan.model_copy(update={"task_egress": None}), requirements)


@pytest.mark.parametrize("host", ["*.example.org", "localhost", "127.0.0.1", "[::1]", "EXAMPLE.org", "example.org.",
    "user@example.org", "example.org:443", "a.svc.cluster.local", "example.org/path", "0177.0.0.1"])
def test_destination_rejects_ambiguous_or_internal_hosts(host: str) -> None:
    with pytest.raises(ValidationError):
        WebDestination(host=host, protocol="https")


@pytest.mark.parametrize("address", ["127.0.0.1", "10.1.2.3", "169.254.169.254", "100.64.0.1", "::1",
    "::ffff:127.0.0.1", "64:ff9b::a00:1", "2001:db8::1", "224.0.0.1", "192.0.2.1"])
async def test_dns_mixed_answer_fails_closed(address: str) -> None:
    resolver = AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
        (socket.AF_INET6 if ":" in address else socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))])
    with pytest.raises(EgressDenied, match="destination_address_denied"):
        await resolve_destination(WebDestination(host="example.org", protocol="https"), (), resolver=resolver)


async def test_dns_is_pinned_and_deployment_protected_addresses_are_denied() -> None:
    resolver = AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))])
    destination = WebDestination(host="example.org", protocol="https")
    assert await resolve_destination(destination, (), resolver=resolver) == ("8.8.8.8",)
    with pytest.raises(EgressDenied, match="destination_address_denied"):
        await resolve_destination(destination, ("8.8.8.8/32",), resolver=resolver)
