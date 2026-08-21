from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from loom_control_plane import ci_runner_lease_broker as leases
from loom_control_plane import ci_runner_route_controller as controller

HEAD_SHA = "a" * 40


def _module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / ".github/actions/ci-runner-route/route.py"
    spec = importlib.util.spec_from_file_location("ci_runner_route_action", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _environment(tmp_path: Path, *, mode: str) -> dict[str, str]:
    return {
        "ROUTE_MODE": mode,
        "ROUTE_REPOSITORY": "qianyi-sun/loom",
        "ROUTE_WORKFLOW_NAME": "CI",
        "ROUTE_WORKFLOW_ID": "302898379",
        "ROUTE_WORKFLOW_RUN_ID": "40000",
        "ROUTE_RUN_ATTEMPT": "1",
        "ROUTE_HEAD_SHA": HEAD_SHA,
        "ROUTE_JOB_KEYS_JSON": json.dumps(
            [
                "lint-and-static",
                "tests-root-1-of-2",
                "tests-root-2-of-2",
                "tests-packages",
                "runtime-payload",
                "go-checks",
                "web-checks",
            ]
        ),
        "ROUTE_REQUEST_PATH": str(tmp_path / "request.json"),
    }


def _config() -> leases.LeaseBrokerConfig:
    return leases.LeaseBrokerConfig(
        repository="qianyi-sun/loom",
        oldlab_labels=(
            "self-hosted",
            "linux",
            "x64",
            "loom-ci",
            "oldlab-5",
            "ephemeral-kvm",
        ),
        capacities={"normal": 5, "image": 4, "smoke": 2},
    )


def test_disabled_action_emits_exact_hosted_routes_without_request(tmp_path: Path) -> None:
    action = _module()
    output = tmp_path / "output"
    environment = _environment(tmp_path, mode="disabled")

    routes = action.prepare(environment, output)

    assert set(routes) == set(json.loads(environment["ROUTE_JOB_KEYS_JSON"]))
    assert set(map(tuple, routes.values())) == {("ubuntu-latest",)}
    assert not Path(environment["ROUTE_REQUEST_PATH"]).exists()
    assert output.read_text(encoding="utf-8").startswith("active=false\n")


def test_active_action_and_broker_share_exact_request_and_assignment_contract(
    tmp_path: Path,
) -> None:
    action = _module()
    output = tmp_path / "output"
    environment = _environment(tmp_path, mode="oldlab-preferred-v1")
    request_value = action.prepare(environment, output)
    request = leases.RouteRequest.from_mapping(request_value)
    broker = leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", _config())
    document = broker.allocate_route(request)
    response = document.public_dict()
    response["oldlab_eligible"] = True

    routes = action.validate_assignment(request_value, response)

    assert list(routes) == request_value["job_keys"]
    assert (
        list(map(tuple, routes.values())).count(
            (
                "self-hosted",
                "linux",
                "x64",
                "loom-ci",
                "oldlab-5",
                "ephemeral-kvm",
                "loom-ci-normal",
            )
        )
        == 5
    )
    assert list(map(tuple, routes.values())).count(("ubuntu-latest",)) == 2
    expected_artifact = "loom-ci-route-request-v1-302898379-40000-1"
    assert f"artifact-name={expected_artifact}" in output.read_text(encoding="utf-8")


def test_action_rejects_unknown_jobs_and_tampered_oldlab_slots(tmp_path: Path) -> None:
    action = _module()
    environment = _environment(tmp_path, mode="oldlab-preferred-v1")
    environment["ROUTE_JOB_KEYS_JSON"] = '["invented-job"]'
    with pytest.raises(action.RouteActionError, match="outside the workflow contract"):
        action.prepare(environment, tmp_path / "output")

    environment = _environment(tmp_path, mode="oldlab-preferred-v1")
    request_value = action.prepare(environment, tmp_path / "output")
    request = leases.RouteRequest.from_mapping(request_value)
    broker = leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", _config())
    response = broker.allocate_route(request).public_dict()
    response["oldlab_eligible"] = True
    response["assignments"][1]["slot"] = response["assignments"][0]["slot"]
    with pytest.raises(action.RouteActionError, match="duplicated or out of range"):
        action.validate_assignment(request_value, response)


@pytest.mark.parametrize(
    "job_key",
    ("pipeline-orchestrator", "behavior-stage1-sim"),
)
def test_images_contract_allows_manifest_owned_jobs(job_key: str) -> None:
    action = _module()
    request, mode = action.build_request(
        {
            "ROUTE_MODE": "disabled",
            "ROUTE_REPOSITORY": "qianyi-sun/loom",
            "ROUTE_WORKFLOW_NAME": "images",
            "ROUTE_WORKFLOW_ID": "302898384",
            "ROUTE_WORKFLOW_RUN_ID": "40001",
            "ROUTE_RUN_ATTEMPT": "1",
            "ROUTE_HEAD_SHA": HEAD_SHA,
            "ROUTE_JOB_KEYS_JSON": json.dumps([job_key]),
        }
    )

    assert request["job_keys"] == [job_key]
    assert mode == "disabled"
    assert leases.RouteRequest.from_mapping(request).job_keys == (job_key,)


def test_poll_accepts_only_the_exact_digest_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    action = _module()
    environment = _environment(tmp_path, mode="oldlab-preferred-v1")
    request_value = action.prepare(environment, tmp_path / "request-output")
    request = leases.RouteRequest.from_mapping(request_value)
    broker = leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", _config())
    response = broker.allocate_route(request).public_dict()
    response["oldlab_eligible"] = True
    external_id = (
        f"{action.CHECK_PREFIX}:{request.workflow_id}:{request.workflow_run_id}:"
        f"{request.run_attempt}:{action._canonical_sha(request_value)}"
    )
    payload = {
        "check_runs": [
            {
                "external_id": external_id,
                "status": "completed",
                "conclusion": "success",
                "output": {"summary": json.dumps(response, sort_keys=True, separators=(",", ":"))},
            }
        ]
    }
    monkeypatch.setattr(action, "_get_json", lambda _url, _token: payload)
    poll_output = tmp_path / "poll-output"

    routes = action.poll(
        {
            "GITHUB_TOKEN": "opaque-token",
            "ROUTE_REQUEST_PATH": environment["ROUTE_REQUEST_PATH"],
        },
        poll_output,
    )

    assert list(routes) == request_value["job_keys"]
    assert poll_output.read_text(encoding="utf-8").startswith("routes={")


def test_poll_timeout_freezes_exact_hosted_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    action = _module()
    environment = _environment(tmp_path, mode="oldlab-preferred-v1")
    request_value = action.prepare(environment, tmp_path / "request-output")
    monkeypatch.setattr(action, "POLL_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(
        action,
        "_get_json",
        lambda *_args: pytest.fail("a zero-length deadline must not query GitHub"),
    )
    poll_output = tmp_path / "poll-output"

    routes = action.poll(
        {
            "GITHUB_TOKEN": "opaque-token",
            "ROUTE_REQUEST_PATH": environment["ROUTE_REQUEST_PATH"],
        },
        poll_output,
    )

    assert list(routes) == request_value["job_keys"]
    assert set(map(tuple, routes.values())) == {("ubuntu-latest",)}
    assert poll_output.read_text(encoding="utf-8").startswith("routes={")


def test_poll_never_hides_an_ambiguous_trusted_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    action = _module()
    environment = _environment(tmp_path, mode="oldlab-preferred-v1")
    request_value = action.prepare(environment, tmp_path / "request-output")
    request = leases.RouteRequest.from_mapping(request_value)
    external_id = (
        f"{action.CHECK_PREFIX}:{request.workflow_id}:{request.workflow_run_id}:"
        f"{request.run_attempt}:{action._canonical_sha(request_value)}"
    )
    check = {"external_id": external_id}
    monkeypatch.setattr(action, "_get_json", lambda *_args: {"check_runs": [check, check]})

    with pytest.raises(action.RouteActionError, match="identity is ambiguous"):
        action.poll(
            {
                "GITHUB_TOKEN": "opaque-token",
                "ROUTE_REQUEST_PATH": environment["ROUTE_REQUEST_PATH"],
            },
            tmp_path / "poll-output",
        )

    assert not (tmp_path / "poll-output").exists()


def test_hosted_deadline_exceeds_the_oldlab_publish_window() -> None:
    action = _module()
    controller_window = (
        controller.OLDLAB_REQUEST_MAX_AGE_SECONDS
        + controller.PUBLISHER_POLL_SECONDS * controller.PUBLISHER_POLL_ATTEMPTS
    )

    assert controller_window < action.POLL_TIMEOUT_SECONDS < 5 * 60


def test_composite_action_pins_artifact_upload_and_exposes_only_routes() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    action = yaml.safe_load(
        (repo_root / ".github/actions/ci-runner-route/action.yml").read_text(encoding="utf-8")
    )

    assert set(action["outputs"]) == {"routes"}
    upload = next(
        step for step in action["runs"]["steps"] if step["name"] == "Upload bounded route request"
    )
    assert upload["uses"] == ("actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a")
    assert upload["with"]["retention-days"] == 1
