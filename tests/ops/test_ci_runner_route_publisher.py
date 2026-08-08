from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml
from scripts.ops import ci_runner_route_publisher as publisher

HEAD_SHA = "a" * 40
REQUEST_SHA = "b" * 64
PUBLISHER_KEY = "route-publisher-test-key-with-32-bytes"


def _payload() -> dict[str, object]:
    summary = {
        "schema_version": 1,
        "repository": "qianyi-sun/loom",
        "workflow_name": "CI",
        "workflow_id": 302898379,
        "workflow_run_id": 31242905537,
        "run_attempt": 2,
        "head_sha": HEAD_SHA,
        "request_sha256": REQUEST_SHA,
        "assignments": [{"job_key": "lint-and-static"}],
        "oldlab_eligible": True,
    }
    return {
        "name": "loom-ci-route-v1/CI/31242905537/2",
        "head_sha": HEAD_SHA,
        "external_id": f"loom-ci-route-v1:302898379:31242905537:2:{REQUEST_SHA}",
        "status": "completed",
        "conclusion": "success",
        "output": {
            "title": "oldlab-first route assignment",
            "summary": json.dumps(summary, sort_keys=True, separators=(",", ":")),
        },
    }


def _environment(payload: Mapping[str, object], *, key: str = PUBLISHER_KEY) -> dict[str, str]:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return {
        "ROUTE_PUBLISH_PAYLOAD_B64": base64.b64encode(raw).decode(),
        "ROUTE_PUBLISH_SIGNATURE": hmac.new(key.encode(), raw, hashlib.sha256).hexdigest(),
        "LOOM_CI_ROUTE_PUBLISH_HMAC_KEY": key,
    }


class FakePublisherAPI:
    def __init__(self) -> None:
        self.checks: list[dict[str, object]] = []
        self.create_calls = 0

    def check_runs(self, head_sha: str, name: str) -> list[dict[str, object]]:
        return [
            item for item in self.checks if item["head_sha"] == head_sha and item["name"] == name
        ]

    def create_check_run(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        self.create_calls += 1
        check = {**payload, "app": {"id": publisher.GITHUB_ACTIONS_APP_ID}}
        self.checks.append(check)
        return check


def test_signed_route_payload_creates_once_and_replays_exactly() -> None:
    payload = publisher._load_signed_payload(_environment(_payload()))
    api = FakePublisherAPI()

    assert publisher.publish(api, payload) == "created"
    assert publisher.publish(api, payload) == "replayed"
    assert api.create_calls == 1


def test_signature_and_canonical_payload_fail_closed() -> None:
    environment = _environment(_payload())
    environment["ROUTE_PUBLISH_SIGNATURE"] = "0" * 64
    with pytest.raises(publisher.RoutePublisherError, match="does not match"):
        publisher._load_signed_payload(environment)

    raw = json.dumps(_payload(), indent=2).encode()
    environment = {
        "ROUTE_PUBLISH_PAYLOAD_B64": base64.b64encode(raw).decode(),
        "ROUTE_PUBLISH_SIGNATURE": hmac.new(
            PUBLISHER_KEY.encode(), raw, hashlib.sha256
        ).hexdigest(),
        "LOOM_CI_ROUTE_PUBLISH_HMAC_KEY": PUBLISHER_KEY,
    }
    with pytest.raises(publisher.RoutePublisherError, match="not canonical"):
        publisher._load_signed_payload(environment)


def test_check_identity_must_match_the_signed_summary() -> None:
    payload = _payload()
    payload["name"] = "repository-checks"

    with pytest.raises(publisher.RoutePublisherError, match="does not match its summary"):
        publisher.publish(FakePublisherAPI(), payload)


def test_existing_foreign_or_duplicate_check_fails_closed() -> None:
    payload = _payload()
    api = FakePublisherAPI()
    api.checks.append({**payload, "app": {"id": 1}})
    with pytest.raises(publisher.RoutePublisherError, match="does not match"):
        publisher.publish(api, payload)

    api.checks.append({**payload, "app": {"id": publisher.GITHUB_ACTIONS_APP_ID}})
    with pytest.raises(publisher.RoutePublisherError, match="ambiguous"):
        publisher.publish(api, payload)


def test_route_publisher_workflow_is_dispatch_only_and_candidate_pinned() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load(
        (repo_root / ".github/workflows/ci-runner-route-publisher.yml").read_text(encoding="utf-8")
    )
    trigger = workflow.get("on", workflow.get(True))
    assert set(trigger) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read", "checks": "write"}
    publish_job = workflow["jobs"]["publish"]
    checkout = publish_job["steps"][0]
    assert checkout["uses"] == ("actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd")
    assert checkout["with"] == {
        "ref": "${{ inputs.candidate_sha }}",
        "sparse-checkout": "scripts/ops/ci_runner_route_publisher.py",
        "persist-credentials": False,
    }
    publish_step = publish_job["steps"][1]
    assert publish_step["env"]["GITHUB_TOKEN"] == "${{ github.token }}"
    assert publish_step["env"]["LOOM_CI_ROUTE_PUBLISH_HMAC_KEY"] == (
        "${{ secrets.LOOM_CI_ROUTE_PUBLISH_HMAC_KEY }}"
    )
