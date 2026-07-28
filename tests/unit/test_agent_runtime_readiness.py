from __future__ import annotations

import json

from loom_cli.agent_runtime_readiness import (
    RuntimeCheck,
    build_runtime_audit_items,
    render_runtime_audit_json,
    render_runtime_audit_table,
)
from loom_service.agent_catalog import list_agents


def test_runtime_audit_covers_every_displayed_agent_and_reports_missing_deps() -> None:
    checks: list[RuntimeCheck] = []

    def fake_runner(check: RuntimeCheck) -> bool:
        checks.append(check)
        return check.kind == "executable" and check.name in {"echo", "sh"}

    items = build_runtime_audit_items(
        image="loom-sandbox:test",
        check_runner=fake_runner,
    )

    assert {item.name for item in items} == {agent.name for agent in list_agents()}
    by_name = {item.name: item for item in items}
    assert by_name["oracle"].readiness_state == "ready"
    assert "hello" not in by_name
    assert by_name["opencode"].dependency_state == "missing"
    assert by_name["opencode"].blocker_reason == "missing_runtime_dependency"
    assert RuntimeCheck("executable", "opencode") in checks
    assert RuntimeCheck("python_module", "sweagent.run.run_single") in checks


def test_runtime_audit_marks_present_catalog_ready_agents_ready() -> None:
    items = build_runtime_audit_items(
        image="loom-sandbox:test",
        check_runner=lambda _check: True,
    )

    by_name = {item.name: item for item in items}
    assert by_name["opencode"].dependency_state == "satisfied"
    assert by_name["opencode"].catalog_ready is True
    assert by_name["opencode"].readiness_state == "ready"
    assert by_name["opencode"].blocker_reason is None


def test_runtime_audit_renderers_are_stable() -> None:
    items = build_runtime_audit_items(
        image="loom-sandbox:test",
        check_runner=lambda check: check.name in {"echo", "sh"},
    )

    payload = json.loads(render_runtime_audit_json(items))
    assert payload["image"] == "loom-sandbox:test"
    assert payload["count"] == len(list_agents())
    assert any(item["name"] == "opencode" for item in payload["items"])

    table = render_runtime_audit_table(items)
    assert "AGENT" in table
    assert "opencode" in table
    assert "missing_runtime_dependency" in table
