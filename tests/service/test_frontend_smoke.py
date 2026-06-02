import io
import zipfile
import unittest

from agentic_data_platform.service.frontend_smoke import (
    FrontendSmokeConfig,
    FrontendSmokeError,
    run_frontend_smoke,
)


class FrontendSmokeTest(unittest.TestCase):
    def test_frontend_smoke_logs_in_launches_run_monitors_and_downloads_bundle(self):
        client = FakeFrontendSmokeClient(
            run_details=[
                _run_detail("queued", artifact_count=0, turn_count=0),
                _run_detail("running", artifact_count=0, turn_count=1),
                _run_detail("succeeded", artifact_count=3, turn_count=1),
            ],
            telemetry=_telemetry("succeeded"),
            bundle=_bundle(),
        )

        result = run_frontend_smoke(
            FrontendSmokeConfig(
                base_url="http://api:8000",
                username="[REDACTED_OWNER]",
                password="[REDACTED_PASSWORD]",
                run_id="frontend_smoke_001",
                poll_interval_seconds=0,
            ),
            client=client,
            sleep=lambda _: None,
        )

        self.assertEqual(result.run_id, "frontend_smoke_001")
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.project_id, "pilot-project")
        self.assertEqual(result.model_id, "scripted-terminal-agent")
        self.assertEqual(result.harness_id, "harbor-local-docker")
        self.assertEqual(result.artifact_count, 3)
        self.assertEqual(result.telemetry_status, "succeeded")
        self.assertEqual(result.bundle_file_count, 7)
        self.assertEqual(result.lifecycle_event_count, 7)
        self.assertEqual(result.sse_event_count, 7)
        self.assertEqual(client.requests[0]["path"], "/auth/login")
        self.assertIn("/runs/frontend_smoke_001/artifact-bundle", [request["path"] for request in client.requests])
        self.assertNotIn("Authorization", str(client.requests))
        run_launch = next(request for request in client.requests if request["method"] == "POST" and request["path"] == "/runs")
        launch_metadata = run_launch["json"]["metadata"]
        self.assertEqual(launch_metadata["harness_id"], "harbor-local-docker")
        self.assertIn("harbor_run", launch_metadata)
        self.assertNotIn("worker_commands", launch_metadata)
        self.assertEqual(
            launch_metadata["harbor_run"],
            {
                "task_template": "harbor-cli-smoke",
                "agent": "oracle",
                "model_name": "smoke/noop",
                "environment": "docker",
                "timeout_seconds": 60,
                "extra_args": ["--n-tasks", "1", "--quiet"],
            },
        )
        self.assertEqual(run_launch["json"]["evaluators"], [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}])
        event_reads = [request for request in client.requests if request["path"] == "/runs/frontend_smoke_001/events"]
        self.assertEqual(
            event_reads,
            [
                {
                    "method": "GET",
                    "path": "/runs/frontend_smoke_001/events",
                    "params": {"after_seq": 0, "limit": 100},
                    "headers": {"X-Request-ID": "frontend_smoke_001-frontend-smoke"},
                }
            ],
        )
        stream_reads = [request for request in client.requests if request["path"] == "/runs/frontend_smoke_001/stream"]
        self.assertEqual(
            stream_reads,
            [
                {
                    "method": "GET",
                    "path": "/runs/frontend_smoke_001/stream",
                    "params": {"after_seq": 0, "limit": 100, "once": True},
                    "headers": {"X-Request-ID": "frontend_smoke_001-frontend-smoke"},
                }
            ],
        )

    def test_frontend_smoke_retries_transient_run_detail_404_after_launch(self):
        client = FakeFrontendSmokeClient(
            run_details=[
                FakeResponse(404, {"error": {"code": "not_found", "message": "Unknown run"}}),
                _run_detail("queued", artifact_count=0, turn_count=0),
                _run_detail("succeeded", artifact_count=3, turn_count=1),
            ],
            telemetry=_telemetry("succeeded"),
            bundle=_bundle(),
        )

        result = run_frontend_smoke(
            FrontendSmokeConfig(
                base_url="http://api:8000",
                username="[REDACTED_OWNER]",
                password="[REDACTED_PASSWORD]",
                run_id="frontend_smoke_001",
                poll_interval_seconds=0,
            ),
            client=client,
            sleep=lambda _: None,
        )

        self.assertEqual(result.status, "succeeded")
        detail_reads = [request for request in client.requests if request["path"] == "/runs/frontend_smoke_001"]
        self.assertEqual(len(detail_reads), 3)

    def test_frontend_smoke_accepts_harbor_verifier_without_terminal_turns(self):
        client = FakeFrontendSmokeClient(
            run_details=[_run_detail("succeeded", artifact_count=5, turn_count=0, evaluator_mode="harbor_verifier")],
            telemetry=_telemetry("succeeded"),
            bundle=_bundle(),
        )

        result = run_frontend_smoke(
            FrontendSmokeConfig(
                base_url="http://api:8000",
                username="[REDACTED_OWNER]",
                password="[REDACTED_PASSWORD]",
                run_id="frontend_smoke_001",
                poll_interval_seconds=0,
            ),
            client=client,
            sleep=lambda _: None,
        )

        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.artifact_count, 5)

    def test_frontend_smoke_rejects_incomplete_event_replay(self):
        client = FakeFrontendSmokeClient(
            run_details=[_run_detail("succeeded", artifact_count=3, turn_count=1)],
            telemetry=_telemetry("succeeded"),
            bundle=_bundle(),
            events={
                "run_id": "frontend_smoke_001",
                "next_after_seq": 1,
                "events": [_event(1, "run.created", None, "queued", {})],
            },
        )

        with self.assertRaisesRegex(FrontendSmokeError, "event replay missing required lifecycle events"):
            run_frontend_smoke(
                FrontendSmokeConfig(
                    base_url="http://api:8000",
                    username="[REDACTED_OWNER]",
                    password="[REDACTED_PASSWORD]",
                    run_id="frontend_smoke_001",
                    poll_interval_seconds=0,
                ),
                client=client,
                sleep=lambda _: None,
            )

    def test_frontend_smoke_rejects_incomplete_sse_replay(self):
        client = FakeFrontendSmokeClient(
            run_details=[_run_detail("succeeded", artifact_count=3, turn_count=1)],
            telemetry=_telemetry("succeeded"),
            bundle=_bundle(),
            sse_content=_sse_from_events(
                {
                    "run_id": "frontend_smoke_001",
                    "next_after_seq": 1,
                    "events": [_event(1, "run.created", None, "queued", {})],
                }
            ),
        )

        with self.assertRaisesRegex(FrontendSmokeError, "SSE replay missing required lifecycle events"):
            run_frontend_smoke(
                FrontendSmokeConfig(
                    base_url="http://api:8000",
                    username="[REDACTED_OWNER]",
                    password="[REDACTED_PASSWORD]",
                    run_id="frontend_smoke_001",
                    poll_interval_seconds=0,
                ),
                client=client,
                sleep=lambda _: None,
            )

    def test_frontend_smoke_rejects_empty_bundle_download(self):
        client = FakeFrontendSmokeClient(
            run_details=[_run_detail("succeeded", artifact_count=3, turn_count=1)],
            telemetry=_telemetry("succeeded"),
            bundle=b"not a zip",
        )

        with self.assertRaisesRegex(FrontendSmokeError, "artifact bundle"):
            run_frontend_smoke(
                FrontendSmokeConfig(
                    base_url="http://api:8000",
                    username="[REDACTED_OWNER]",
                    password="[REDACTED_PASSWORD]",
                    run_id="frontend_smoke_bad_bundle",
                    poll_interval_seconds=0,
                ),
                client=client,
                sleep=lambda _: None,
            )

    def test_frontend_smoke_rejects_metadata_only_bundle(self):
        client = FakeFrontendSmokeClient(
            run_details=[_run_detail("succeeded", artifact_count=3, turn_count=1)],
            telemetry=_telemetry("succeeded"),
            bundle=_metadata_only_bundle(),
        )

        with self.assertRaisesRegex(FrontendSmokeError, "artifact payload"):
            run_frontend_smoke(
                FrontendSmokeConfig(
                    base_url="http://api:8000",
                    username="[REDACTED_OWNER]",
                    password="[REDACTED_PASSWORD]",
                    run_id="frontend_smoke_bad_bundle",
                    poll_interval_seconds=0,
                ),
                client=client,
                sleep=lambda _: None,
            )


class FakeFrontendSmokeClient:
    def __init__(self, *, run_details, telemetry, bundle, events=None, sse_content=None):
        self.run_details = list(run_details)
        self.telemetry = telemetry
        self.bundle = bundle
        self.events = events
        self.sse_content = sse_content
        self.requests = []

    def post(self, path, *, json=None, headers=None):
        self.requests.append({"method": "POST", "path": path, "json": json, "headers": headers or {}})
        if path == "/auth/login":
            return FakeResponse(200, {"user": {"user_id": "[REDACTED_OWNER]"}})
        if path == "/runs":
            return FakeResponse(201, _run_detail("queued", artifact_count=0, turn_count=0))
        raise AssertionError(f"unexpected POST {path}")

    def get(self, path, *, params=None, headers=None):
        self.requests.append({"method": "GET", "path": path, "params": params or {}, "headers": headers or {}})
        if path == "/auth/session":
            return FakeResponse(200, {"user": {"user_id": "[REDACTED_OWNER]"}})
        if path == "/projects":
            return FakeResponse(
                200,
                {
                    "projects": [
                        {
                            "project_id": "pilot-project",
                            "name": "pilot group",
                            "owner_team_id": "pilot-project",
                        }
                    ]
                },
            )
        if path == "/models":
            return FakeResponse(
                200,
                {
                    "models": [
                        {
                            "provider": "mock-api",
                            "provider_config_id": "default-agent-model",
                            "model_id": "scripted-terminal-agent",
                            "disabled": False,
                        }
                    ]
                },
            )
        if path == "/harnesses":
            return FakeResponse(
                200,
                {
                    "harnesses": [
                        {
                            "harness_id": "docker-terminal",
                            "runner_kind": "original_benchmark",
                            "sandbox_backend": "docker_terminal",
                            "default_image": "python:3.12-slim",
                            "internet_access": True,
                            "resource_limits": {"cpu": 1, "memory_mb": 512, "timeout_seconds": 60},
                            "metadata": {"harbor_compatible": False},
                        },
                        {
                            "harness_id": "harbor-local-docker",
                            "runner_kind": "original_benchmark",
                            "sandbox_backend": "docker_terminal",
                            "default_image": "python:3.12-slim",
                            "internet_access": True,
                            "resource_limits": {"cpu": 1, "memory_mb": 512, "timeout_seconds": 60},
                            "metadata": {
                                "harbor_compatible": True,
                                "runner_contract": "harbor-local-docker-v0",
                                "status": "ready",
                                "harbor_task_template": "harbor-cli-smoke",
                                "harbor_agent": "oracle",
                                "harbor_model_name": "smoke/noop",
                                "harbor_environment": "docker",
                                "harbor_extra_args": ["--n-tasks", "1", "--quiet"],
                            },
                        },
                    ]
                },
            )
        if path == "/benchmarks":
            return FakeResponse(
                200,
                {
                    "benchmarks": [
                        {
                            "suite_name": "SkillLearnBench",
                            "benchmark_version": "git:cxcscmu/SkillLearnBench@test",
                            "source_uri": "https://github.com/cxcscmu/SkillLearnBench",
                        }
                    ]
                },
            )
        if path == "/tasks":
            return FakeResponse(
                200,
                {
                    "tasks": [
                        {
                            "task_family": "organize-messy-files",
                            "instance_id": "organize-messy-files-1",
                            "instruction_ref": "tasks/organize-messy-files/instruction.md",
                            "input_artifact_refs": [],
                            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
                            "runner_image": "python:3.12-slim",
                            "runner_entrypoint": ["python", "-c"],
                            "runner_contract": "skilllearnbench-original-wrapper-v0",
                            "metadata": {},
                        }
                    ]
                },
            )
        if path == "/runs/frontend_smoke_001" or path == "/runs/frontend_smoke_bad_bundle":
            detail = self.run_details.pop(0) if self.run_details else _run_detail("succeeded")
            if isinstance(detail, FakeResponse):
                return detail
            return FakeResponse(200, detail)
        if path.endswith("/events"):
            run_id = path.split("/")[2]
            return FakeResponse(200, self.events or _events(run_id))
        if path.endswith("/stream"):
            run_id = path.split("/")[2]
            content = self.sse_content if self.sse_content is not None else _sse_from_events(self.events or _events(run_id))
            return FakeResponse(200, content=content.encode("utf-8"), headers={"content-type": "text/event-stream"})
        if path.endswith("/telemetry"):
            return FakeResponse(200, self.telemetry)
        if path.endswith("/artifact-bundle"):
            return FakeResponse(200, content=self.bundle, headers={"content-type": "application/zip"})
        if path == "/dashboard/progress":
            return FakeResponse(200, {"summary": {"total_runs": 1, "runs_by_status": {"succeeded": 1}}})
        raise AssertionError(f"unexpected GET {path}")


class FakeResponse:
    def __init__(self, status_code, payload=None, *, content=b"", headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content
        self.headers = headers or {"content-type": "application/json"}
        self.text = str(self._payload)

    def json(self):
        return self._payload


def _run_detail(status="succeeded", *, artifact_count=3, turn_count=1, evaluator_mode="llm_judge"):
    return {
        "run": {
            "run_id": "frontend_smoke_001",
            "project": {"project_id": "pilot-project"},
            "status": status,
            "progress": {"artifact_count": artifact_count, "turn_count": turn_count},
            "evaluator": {"mode": evaluator_mode, "status": "completed", "verbal_feedback_summary": "ok"}
            if status == "succeeded"
            else None,
        },
        "trajectory": [
            {"turn_index": 0, "command": "python solve.py", "exit_code": 0, "stdout": "ok", "stderr": ""}
        ][:turn_count],
    }


def _telemetry(status):
    return {"run": {"run_id": "frontend_smoke_001", "status": status}, "sandbox": {"status": "exited"}}


def _events(run_id="frontend_smoke_001"):
    return {
        "run_id": run_id,
        "next_after_seq": 6,
        "events": [
            _event(1, "run.created", None, "queued", {}, run_id=run_id),
            _event(
                2,
                "run.dispatched",
                "queued",
                "dispatched",
                {
                    "scheduler_id": "scheduler-dev-1",
                    "execution_task_id": f"{run_id}:attempt:1",
                    "backend_key": "harbor-local-docker",
                    "project_id": "pilot-project",
                },
                run_id=run_id,
            ),
            _event(
                3,
                "run.claimed",
                "dispatched",
                "provisioning",
                {"worker_id": "worker-dev-1", "execution_task_id": f"{run_id}:attempt:1"},
                run_id=run_id,
            ),
            _event(
                4,
                "run.started",
                "provisioning",
                "running",
                {"worker_id": "worker-dev-1", "execution_task_id": f"{run_id}:attempt:1"},
                run_id=run_id,
            ),
            _event(
                5,
                "run.evaluating",
                "running",
                "evaluating",
                {"worker_id": "worker-dev-1", "execution_task_id": f"{run_id}:attempt:1"},
                run_id=run_id,
            ),
            _event(
                6,
                "evaluator.completed",
                "evaluating",
                "evaluating",
                {
                    "worker_id": "worker-dev-1",
                    "execution_task_id": f"{run_id}:attempt:1",
                    "evaluator_id": "harbor-verifier",
                    "mode": "harbor_verifier",
                    "status": "completed",
                    "score": 1.0,
                    "artifact_refs": [f"minio://runs/{run_id}/evaluation/verifier.json"],
                },
                run_id=run_id,
            ),
            _event(
                7,
                "run.succeeded",
                "evaluating",
                "succeeded",
                {"worker_id": "worker-dev-1", "execution_task_id": f"{run_id}:attempt:1"},
                run_id=run_id,
            ),
        ],
    }


def _event(seq, event_type, from_status, to_status, metadata, *, run_id="frontend_smoke_001"):
    return {
        "event_id": f"evt_{seq}",
        "seq": seq,
        "run_id": run_id,
        "attempt_id": f"{run_id}:attempt:1",
        "event_type": event_type,
        "from_status": from_status,
        "to_status": to_status,
        "reason": None,
        "actor_user_id": None,
        "request_id": f"{run_id}-frontend-smoke",
        "metadata": metadata,
        "created_at": "2026-06-02T00:00:00Z",
    }


def _sse_from_events(payload):
    frames = []
    for event in payload["events"]:
        frames.append(
            "\n".join(
                [
                    f"id: {event['seq']}",
                    f"event: {event['event_type']}",
                    f"data: {json_dumps(event)}",
                    "",
                    "",
                ]
            )
        )
    return "".join(frames)


def json_dumps(payload):
    import json

    return json.dumps(payload, sort_keys=True)


def _bundle():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("manifest.json", "{}")
        archive.writestr("run.json", "{}")
        archive.writestr("trajectory.jsonl", "{}\n")
        archive.writestr("evaluation.json", "{}")
        archive.writestr("artifact-metadata.json", "{}")
        archive.writestr("lifecycle-events.json", json_dumps({"lifecycle_events": _events()["events"]}))
        archive.writestr("artifacts/trajectory/frontend_smoke_001-trajectory.jsonl", "{}\n")
    return buffer.getvalue()


def _metadata_only_bundle():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("manifest.json", "{}")
        archive.writestr("run.json", "{}")
        archive.writestr("trajectory.jsonl", "{}\n")
        archive.writestr("evaluation.json", "{}")
        archive.writestr("artifact-metadata.json", "{}")
        archive.writestr("lifecycle-events.json", json_dumps({"lifecycle_events": _events()["events"]}))
    return buffer.getvalue()
