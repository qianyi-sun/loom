import unittest

from agentic_data_platform.service.api_smoke import ApiSmokeConfig, ApiSmokeError, run_api_smoke


class ApiSmokeTest(unittest.TestCase):
    def test_api_smoke_creates_run_waits_for_worker_and_checks_dashboard(self):
        client = FakeApiClient(
            run_details=[
                _run_detail(status="queued", artifact_count=0, turn_count=0),
                _run_detail(status="running", artifact_count=0, turn_count=0),
                _run_detail(status="succeeded", artifact_count=3, turn_count=1),
            ],
            dashboard=_dashboard_progress(total_runs=1, succeeded_runs=1),
        )

        result = run_api_smoke(
            ApiSmokeConfig(
                base_url="http://api:8000",
                auth_token="[REDACTED_TOKEN]",
                run_id="api_smoke_test_001",
                timeout_seconds=30,
                poll_interval_seconds=0,
            ),
            client=client,
            sleep=lambda _: None,
        )

        self.assertEqual(result.run_id, "api_smoke_test_001")
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.artifact_count, 3)
        self.assertEqual(result.turn_count, 1)
        self.assertEqual(result.evaluator_status, "completed")
        self.assertEqual(result.dashboard_total_runs, 1)
        self.assertEqual(result.dashboard_succeeded_runs, 1)
        self.assertEqual(client.requests[0]["method"], "POST")
        self.assertEqual(client.requests[0]["path"], "/runs")
        self.assertEqual(client.requests[0]["headers"]["Authorization"], "Bearer [REDACTED_TOKEN]")
        self.assertEqual(client.requests[0]["json"]["project_id"], "pilot-project")
        self.assertEqual(client.requests[0]["json"]["run_id"], "api_smoke_test_001")
        self.assertIn("worker_commands", client.requests[0]["json"]["metadata"])
        self.assertEqual(client.requests[-1]["path"], "/dashboard/progress")

    def test_api_smoke_fails_when_run_detail_leaks_local_paths(self):
        client = FakeApiClient(
            run_details=[
                _run_detail(
                    status="succeeded",
                    artifact_count=3,
                    turn_count=1,
                    leaked_uri="file:///workspace/.runtime/sandbox-workspaces/api_smoke/output.txt",
                )
            ],
            dashboard=_dashboard_progress(total_runs=1, succeeded_runs=1),
        )

        with self.assertRaisesRegex(ApiSmokeError, "leaked internal"):
            run_api_smoke(
                ApiSmokeConfig(
                    base_url="http://api:8000",
                    auth_token="[REDACTED_TOKEN]",
                    run_id="api_smoke_test_002",
                    timeout_seconds=30,
                    poll_interval_seconds=0,
                ),
                client=client,
                sleep=lambda _: None,
            )

    def test_api_smoke_retries_run_detail_404_after_create(self):
        client = FakeApiClient(
            run_details=[
                {"error": {"code": "not_found", "message": "Unknown run: api_smoke_test_003"}},
                _run_detail(status="succeeded", artifact_count=3, turn_count=1, run_id="api_smoke_test_003"),
            ],
            run_detail_statuses=[404, 200],
            dashboard=_dashboard_progress(total_runs=1, succeeded_runs=1),
        )

        result = run_api_smoke(
            ApiSmokeConfig(
                base_url="http://api:8000",
                auth_token="[REDACTED_TOKEN]",
                run_id="api_smoke_test_003",
                timeout_seconds=30,
                poll_interval_seconds=0,
            ),
            client=client,
            sleep=lambda _: None,
        )

        run_detail_requests = [request for request in client.requests if request["path"].startswith("/runs/")]
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(len(run_detail_requests), 2)


class FakeApiClient:
    def __init__(
        self,
        *,
        run_details: list[dict],
        dashboard: dict,
        run_detail_statuses: list[int] | None = None,
    ) -> None:
        self.run_details = list(run_details)
        self.run_detail_statuses = list(run_detail_statuses or [200 for _ in run_details])
        self.dashboard = dashboard
        self.requests: list[dict] = []

    def post(self, path: str, *, json: dict, headers: dict) -> "FakeResponse":
        self.requests.append({"method": "POST", "path": path, "json": json, "headers": headers})
        return FakeResponse(201, self.run_details[0])

    def get(self, path: str, *, params: dict | None = None, headers: dict) -> "FakeResponse":
        self.requests.append({"method": "GET", "path": path, "params": params or {}, "headers": headers})
        if path.startswith("/runs/"):
            payload = self.run_details.pop(0) if len(self.run_details) > 1 else self.run_details[0]
            status_code = (
                self.run_detail_statuses.pop(0)
                if len(self.run_detail_statuses) > 1
                else self.run_detail_statuses[0]
            )
            return FakeResponse(status_code, payload)
        if path == "/dashboard/progress":
            return FakeResponse(200, self.dashboard)
        return FakeResponse(404, {"detail": "not found"})


class FakeResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


def _run_detail(
    *,
    status: str,
    artifact_count: int,
    turn_count: int,
    run_id: str = "api_smoke_test_001",
    leaked_uri: str | None = None,
) -> dict:
    artifact_uri = leaked_uri or f"minio://runs/{run_id}/workspace/workspace.tar.zst"
    return {
        "run": {
            "run_id": run_id,
            "status": status,
            "progress": {
                "status": status,
                "is_terminal": status in {"succeeded", "failed", "canceled"},
                "artifact_count": artifact_count,
                "turn_count": turn_count,
            },
            "artifacts": [
                {"kind": "trajectory", "uri": f"minio://runs/{run_id}/trajectory.jsonl"},
                {"kind": "workspace_snapshot", "uri": artifact_uri},
                {"kind": "evaluator_report", "uri": f"minio://runs/{run_id}/evaluator.json"},
            ][:artifact_count],
            "evaluator": (
                {
                    "evaluator_id": "mock-judge-v0",
                    "status": "completed",
                    "score": 0.75,
                    "verbal_feedback_summary": "Smoke evaluator completed.",
                }
                if status == "succeeded"
                else None
            ),
        },
        "trajectory": [
            {
                "turn_index": 0,
                "command": "python -c \"from pathlib import Path; Path('smoke-output.txt').write_text('api smoke ok')\"",
                "stdout": "api smoke ok\n",
                "stderr": "",
                "exit_code": 0,
                "changed_paths": ["smoke-output.txt"],
            }
        ][:turn_count],
        "lifecycle_events": [],
    }


def _dashboard_progress(*, total_runs: int, succeeded_runs: int) -> dict:
    return {
        "summary": {
            "total_runs": total_runs,
            "runs_by_status": {"succeeded": succeeded_runs},
            "artifact_count": 3,
            "turn_count": 1,
            "evaluator_completed_count": 1,
        },
        "projects": [
            {
                "project_id": "pilot-project",
                "runs_by_status": {"succeeded": succeeded_runs},
                "total_runs": total_runs,
            }
        ],
    }


if __name__ == "__main__":
    unittest.main()
