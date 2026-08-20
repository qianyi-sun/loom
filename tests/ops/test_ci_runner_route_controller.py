from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import urllib.error
import zipfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_control_plane import ci_runner_lease_broker as leases
from loom_control_plane import ci_runner_route_controller as routes

HEAD_SHA = "a" * 40
CANDIDATE_SHA = "c" * 40
WORKFLOW_BLOB_SHA = "d" * 40
PUBLISHER_KEY = b"route-publisher-test-key-with-32-bytes"
NOW = datetime(2026, 8, 20, 18, 0, tzinfo=UTC)


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


def _request(
    *, workflow_name: str = "CI", run_id: int = 30_000, job_count: int = 7
) -> leases.RouteRequest:
    workflow_id, _, allowed_jobs, _ = leases.WORKFLOW_CLASS_CONTRACTS[workflow_name]
    return leases.RouteRequest(
        repository="qianyi-sun/loom",
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        workflow_run_id=run_id,
        run_attempt=1,
        head_sha=HEAD_SHA,
        job_keys=tuple(allowed_jobs[:job_count]),
    )


def _zip_request(
    request: leases.RouteRequest,
    *,
    filename: str = routes.ROUTE_REQUEST_FILENAME,
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, json.dumps(request.public_dict()))
    return buffer.getvalue()


class FakeRouteAPI:
    def __init__(self, request: leases.RouteRequest) -> None:
        self.request = request
        self.artifacts: list[dict[str, object]] = [
            {
                "id": 71,
                "name": (
                    f"{routes.ARTIFACT_PREFIX}{request.workflow_id}-"
                    f"{request.workflow_run_id}-{request.run_attempt}"
                ),
                "expired": False,
                "created_at": NOW.isoformat().replace("+00:00", "Z"),
                "workflow_run": {
                    "id": request.workflow_run_id,
                    "head_sha": request.head_sha,
                },
            }
        ]
        self.archives = {71: _zip_request(request)}
        self.runs: dict[int, dict[str, object]] = {
            request.workflow_run_id: {
                "id": request.workflow_run_id,
                "run_attempt": request.run_attempt,
                "workflow_id": request.workflow_id,
                "head_sha": request.head_sha,
                "name": request.workflow_name,
                "event": "pull_request",
                "status": "in_progress",
                "repository": {"full_name": request.repository},
            }
        }
        workflow_path = routes.WORKFLOW_PATHS[request.workflow_name]
        self.blobs = {
            (workflow_path, request.head_sha): WORKFLOW_BLOB_SHA,
            (workflow_path, CANDIDATE_SHA): WORKFLOW_BLOB_SHA,
        }
        self.checks: dict[tuple[str, str], list[dict[str, object]]] = {}
        self.created_checks: list[dict[str, object]] = []
        self.dispatches: list[dict[str, str]] = []
        self.publish_dispatched_check = True
        self.jobs: dict[tuple[int, int], list[dict[str, object]]] = {
            (request.workflow_run_id, request.run_attempt): []
        }

    def latest_artifact_id(self) -> int:
        return max((int(item["id"]) for item in self.artifacts), default=0)

    def list_route_artifacts(self, after_id: int) -> tuple[int, list[dict[str, object]]]:
        highwater = self.latest_artifact_id()
        return highwater, [item for item in self.artifacts if int(item["id"]) > after_id]

    def download_artifact(self, artifact_id: int) -> bytes:
        return self.archives[artifact_id]

    def workflow_run(self, run_id: int) -> dict[str, object]:
        return self.runs[run_id]

    def content_blob_sha(self, path: str, ref: str) -> str:
        return self.blobs[(path, ref)]

    def check_runs(self, head_sha: str, name: str) -> list[dict[str, object]]:
        return self.checks.get((head_sha, name), [])

    def dispatch_route_publisher(
        self, *, candidate_sha: str, payload_b64: str, signature: str
    ) -> None:
        raw = base64.b64decode(payload_b64, validate=True)
        assert candidate_sha == CANDIDATE_SHA
        assert hmac.compare_digest(
            signature,
            hmac.new(PUBLISHER_KEY, raw, hashlib.sha256).hexdigest(),
        )
        self.dispatches.append(
            {
                "candidate_sha": candidate_sha,
                "payload_b64": payload_b64,
                "signature": signature,
            }
        )
        if self.publish_dispatched_check:
            payload = json.loads(raw)
            payload["app"] = {"id": routes.GITHUB_ACTIONS_APP_ID}
            self.created_checks.append(payload)
            self.checks.setdefault((payload["head_sha"], payload["name"]), []).append(payload)

    def workflow_jobs(self, run_id: int, attempt: int) -> list[dict[str, object]]:
        return self.jobs[(run_id, attempt)]


def _controller(
    tmp_path: Path, request: leases.RouteRequest
) -> tuple[routes.CiRunnerRouteController, FakeRouteAPI, leases.CiRunnerLeaseBroker]:
    api = FakeRouteAPI(request)
    broker = leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", _config())
    cursor_file = tmp_path / "route-controller-cursor.json"
    routes._write_artifact_cursor(cursor_file, 0)
    controller = routes.CiRunnerRouteController(
        api=api,
        broker=broker,
        candidate_sha=CANDIDATE_SHA,
        cursor_file=cursor_file,
        publisher_key=PUBLISHER_KEY,
        publisher_poll_seconds=0,
        now=lambda: NOW,
    )
    return controller, api, broker


def test_controller_publishes_exact_oldlab_first_route(tmp_path: Path) -> None:
    request = _request(job_count=7)
    controller, api, broker = _controller(tmp_path, request)

    result = controller.reconcile()

    assert result.public_dict() == {
        "artifacts_seen": 1,
        "routes_published": 1,
        "routes_replayed": 0,
        "assignments_released": 0,
    }
    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["request_sha256"] in api.created_checks[0]["external_id"]
    assert summary["oldlab_eligible"] is True
    assert [item["target"] for item in summary["assignments"]].count("oldlab") == 5
    assert [item["target"] for item in summary["assignments"]].count("github_hosted") == 2
    assert broker.status()["classes"]["normal"]["oldlab_assigned"] == 5
    assert len(api.dispatches) == 1


def test_dynamic_run_name_does_not_replace_workflow_identity(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, _ = _controller(tmp_path, request)
    api.runs[request.workflow_run_id]["name"] = (
        "gate=full / head=abc / action=ready_for_review / pull=1245"
    )

    result = controller.reconcile()

    assert result.routes_published == 1


def test_changed_workflow_blob_forces_every_job_to_hosted(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    api.blobs[(routes.WORKFLOW_PATHS["CI"], HEAD_SHA)] = "e" * 40

    controller.reconcile()

    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert {item["target"] for item in summary["assignments"]} == {"github_hosted"}
    assert broker.status()["classes"]["normal"]["available"] == 5


def test_stale_request_forces_hosted_without_consuming_oldlab(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts[0]["created_at"] = (
        (NOW - timedelta(seconds=routes.OLDLAB_REQUEST_MAX_AGE_SECONDS + 1))
        .isoformat()
        .replace("+00:00", "Z")
    )

    controller.reconcile()

    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert {item["target"] for item in summary["assignments"]} == {"github_hosted"}
    assert broker.status()["classes"]["normal"]["available"] == 5


def test_future_request_fails_safe_to_hosted(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts[0]["created_at"] = (NOW + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")

    controller.reconcile()

    summary = json.loads(api.created_checks[0]["output"]["summary"])
    assert summary["oldlab_eligible"] is False
    assert summary["assignments"][0]["target"] == "github_hosted"
    assert broker.status()["classes"]["normal"]["available"] == 5


def test_invalid_artifact_time_fails_before_cursor_or_capacity_mutation(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.artifacts[0]["created_at"] = "not-a-timestamp"

    with pytest.raises(routes.RouteControllerError, match=r"artifact\.created_at"):
        controller.reconcile()

    assert routes._read_artifact_cursor(controller.cursor_file) == 0
    assert broker.active_assignments() == ()


def test_cursor_skips_processed_artifact_and_cursor_loss_replays_safely(
    tmp_path: Path,
) -> None:
    request = _request(job_count=2)
    controller, api, _ = _controller(tmp_path, request)
    first = controller.reconcile()
    skipped = controller.reconcile()
    controller.cursor_file.unlink()
    routes._write_artifact_cursor(controller.cursor_file, 0)
    replay = controller.reconcile()

    assert first.routes_published == 1
    assert skipped.artifacts_seen == 0
    assert replay.routes_published == 0
    assert replay.routes_replayed == 1
    assert len(api.created_checks) == 1


def test_missing_relay_result_fails_before_advancing_cursor(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, _ = _controller(tmp_path, request)
    api.publish_dispatched_check = False
    controller.publisher_poll_attempts = 1

    with pytest.raises(routes.RouteControllerError, match="did not create"):
        controller.reconcile()

    assert routes._read_artifact_cursor(controller.cursor_file) == 0
    assert len(api.dispatches) == 1


def test_missing_cursor_requires_explicit_highwater_initialization(
    tmp_path: Path,
) -> None:
    request = _request(job_count=1)
    controller, _, _ = _controller(tmp_path, request)
    controller.cursor_file.unlink()

    with pytest.raises(routes.RouteControllerError, match="initialize before routing"):
        controller.reconcile()

    assert controller.initialize_cursor() == 71
    assert routes._read_artifact_cursor(controller.cursor_file) == 71
    assert controller.reconcile().artifacts_seen == 0


def test_terminal_jobs_release_exact_leases(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    controller.reconcile()
    api.runs[request.workflow_run_id]["status"] = "completed"
    api.jobs[(request.workflow_run_id, 1)] = [
        {
            "name": routes.JOB_NAMES["CI"][job_key],
            "status": "completed",
            "conclusion": "success",
        }
        for job_key in request.job_keys
    ]

    result = controller.reconcile()

    assert result.assignments_released == 3
    assert broker.active_assignments() == ()


def test_new_workflow_attempt_releases_superseded_assignments(tmp_path: Path) -> None:
    request = _request(job_count=3)
    controller, api, broker = _controller(tmp_path, request)
    controller.reconcile()
    api.runs[request.workflow_run_id]["run_attempt"] = 2
    api.runs[request.workflow_run_id]["status"] = "queued"

    result = controller.reconcile()

    assert result.assignments_released == 3
    assert broker.active_assignments() == ()


def test_workflow_attempt_regression_fails_closed(tmp_path: Path) -> None:
    request = replace(_request(job_count=1), run_attempt=2)
    controller, api, broker = _controller(tmp_path, request)
    controller.reconcile()
    api.runs[request.workflow_run_id]["run_attempt"] = 1

    with pytest.raises(routes.RouteControllerError, match="attempt does not match"):
        controller.reconcile()

    assert len(broker.active_assignments()) == 1


def test_terminal_hosted_fallback_without_a_route_advances_cursor(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, broker = _controller(tmp_path, request)
    api.runs[request.workflow_run_id]["status"] = "completed"

    result = controller.reconcile()

    assert result.artifacts_seen == 1
    assert result.routes_published == 0
    assert routes._read_artifact_cursor(controller.cursor_file) == 71
    assert broker.active_assignments() == ()


def test_route_artifact_identity_and_shape_fail_closed(tmp_path: Path) -> None:
    request = _request(job_count=1)
    controller, api, _ = _controller(tmp_path, request)
    api.artifacts[0]["name"] = f"{routes.ARTIFACT_PREFIX}{request.workflow_id}-999-1"

    with pytest.raises(routes.RouteControllerError, match="name does not match"):
        controller.reconcile()

    api.artifacts[0]["name"] = (
        f"{routes.ARTIFACT_PREFIX}{request.workflow_id}-{request.workflow_run_id}-"
        f"{request.run_attempt}"
    )
    api.archives[71] = _zip_request(request, filename="wrong.json")
    with pytest.raises(
        routes.RouteControllerError,
        match=r"only loom-ci-route-request\.json",
    ):
        controller.reconcile()


def test_controller_filename_matches_the_pinned_route_action() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    action = (repo_root / ".github/actions/ci-runner-route/action.yml").read_text(encoding="utf-8")

    expected_path = "${{ runner.temp }}/loom-ci-route-request.json"
    assert routes.ROUTE_REQUEST_FILENAME == "loom-ci-route-request.json"
    assert f"ROUTE_REQUEST_PATH: {expected_path}" in action
    assert f"path: {expected_path}" in action


def test_artifact_redirect_never_forwards_github_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _zip_request(_request(job_count=1))
    redirected_to = "https://example.blob.core.windows.net/result/archive.zip?sig=fake"

    class RedirectOpener:
        @staticmethod
        def open(request: object, timeout: int) -> None:
            assert timeout == 20
            assert request.get_header("Authorization") == "Bearer top-secret"
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": redirected_to},
                None,
            )

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

        @staticmethod
        def read(_: int) -> bytes:
            return payload

    def urlopen(request: object, timeout: int) -> Response:
        assert timeout == 20
        assert request.full_url == redirected_to
        assert request.get_header("Authorization") is None
        return Response()

    monkeypatch.setattr(routes.urllib.request, "build_opener", lambda *_: RedirectOpener())
    monkeypatch.setattr(routes.urllib.request, "urlopen", urlopen)

    api = routes.GitHubRouteAPI(repository="qianyi-sun/loom", token="top-secret")
    assert api.download_artifact(71) == payload


def test_root_owned_token_and_cursor_files_fail_closed(tmp_path: Path) -> None:
    token = tmp_path / "github-token"
    token.write_text("opaque-token\n", encoding="utf-8")
    token.chmod(0o600)
    assert routes._read_token(token) == "opaque-token"

    token.chmod(0o644)
    with pytest.raises(routes.RouteControllerError, match="group or other"):
        routes._read_token(token)

    cursor = tmp_path / "state" / "cursor.json"
    routes._write_artifact_cursor(cursor, 123)
    assert routes._read_artifact_cursor(cursor) == 123
    assert cursor.stat().st_mode & 0o777 == 0o600

    cursor.unlink()
    cursor.symlink_to(token)
    with pytest.raises(routes.RouteControllerError, match="must not be a symlink"):
        routes._read_artifact_cursor(cursor)

    publisher_key = tmp_path / "route-publisher-hmac"
    publisher_key.write_text(PUBLISHER_KEY.decode() + "\n", encoding="utf-8")
    publisher_key.chmod(0o600)
    assert routes._read_publisher_key(publisher_key) == PUBLISHER_KEY

    publisher_key.chmod(0o640)
    with pytest.raises(routes.RouteControllerError, match="group or other"):
        routes._read_publisher_key(publisher_key)


def test_pool_timer_runs_the_route_controller_with_systemd_credential() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    service = (repo_root / "deploy/ci-runners/loom-ci-runner-pool.service").read_text(
        encoding="utf-8"
    )

    assert "ExecStart=/usr/local/libexec/loom-ci-runner-route-controller" in service
    assert "--token-file ${CREDENTIALS_DIRECTORY}/github-token" in service
    assert "LoadCredential=route-publisher-hmac:" in service
    assert "--publisher-secret-file ${CREDENTIALS_DIRECTORY}/route-publisher-hmac" in service
    assert "--candidate-sha ${LOOM_CI_RUNNER_CANDIDATE_SHA}" in service
    assert "Environment=GITHUB_TOKEN" not in service
