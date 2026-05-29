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
        self.assertEqual(client.requests[0]["path"], "/auth/login")
        self.assertIn("/runs/frontend_smoke_001/artifact-bundle", [request["path"] for request in client.requests])
        self.assertNotIn("Authorization", str(client.requests))

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
    def __init__(self, *, run_details, telemetry, bundle):
        self.run_details = list(run_details)
        self.telemetry = telemetry
        self.bundle = bundle
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
                            "metadata": {"harbor_compatible": True, "runner_contract": "harbor-local-docker-v0"},
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


def _run_detail(status="succeeded", *, artifact_count=3, turn_count=1):
    return {
        "run": {
            "run_id": "frontend_smoke_001",
            "project": {"project_id": "pilot-project"},
            "status": status,
            "progress": {"artifact_count": artifact_count, "turn_count": turn_count},
            "evaluator": {"status": "completed", "verbal_feedback_summary": "ok"} if status == "succeeded" else None,
        },
        "trajectory": [
            {"turn_index": 0, "command": "python solve.py", "exit_code": 0, "stdout": "ok", "stderr": ""}
        ][:turn_count],
    }


def _telemetry(status):
    return {"run": {"run_id": "frontend_smoke_001", "status": status}, "sandbox": {"status": "exited"}}


def _bundle():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("manifest.json", "{}")
        archive.writestr("run.json", "{}")
        archive.writestr("trajectory.jsonl", "{}\n")
        archive.writestr("evaluation.json", "{}")
        archive.writestr("artifact-metadata.json", "{}")
        archive.writestr("lifecycle-events.json", "{}")
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
        archive.writestr("lifecycle-events.json", "{}")
    return buffer.getvalue()
