from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from loom_cli.rollout.browser_report_contract import (
    BROWSER_REPORT_CHECK_IDS,
    browser_report_schema_digest,
)
from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.operator.final_browser_executor import FinalBrowserExecutor
from loom_cli.rollout.preflight_contract import CheckOperation
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


def _authority(tmp_path: Path, *, run):
    token = tmp_path / "admin-token"
    token.write_bytes(b"admin-token\n")
    token.chmod(0o600)
    trusted = read_trusted_file(
        token,
        service_uid=os.geteuid(),
        private=True,
        max_bytes=1024,
        require_nonempty=True,
    )
    plan = replace(
        _plan(tmp_path),
        request_id="req-1111111111111111",
        browser_report_schema=browser_report_schema_digest(),
        secret_metadata_fingerprints={"admin": f"sha256:{trusted.metadata_fingerprint}"},
    )
    attempt = (
        tmp_path / "state" / "requests" / plan.request_id / "attempts" / str(plan.attempt_number)
    )
    attempt.mkdir(parents=True, mode=0o700)
    return (
        plan,
        FinalBrowserExecutor(
            state_root=tmp_path / "state",
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
            token_path=token,
            expected_token_fingerprint=safe_content_fingerprint(b"admin-token"),
            run=run,
        ),
    )


def _report(plan) -> dict[str, object]:
    return {
        "schema_version": 4,
        "status": "pass",
        "failure_code": None,
        "deployment_identity": {
            "expected_deployed_sha": plan.candidate_sha,
            "observed_deployed_sha": plan.candidate_sha,
            "matched": True,
        },
        "route": plan.route,
        "request_id": plan.request_id,
        "rollout_binding": {
            "request_id": plan.request_id,
            "attempt_number": plan.attempt_number,
            "request_envelope_sha256": plan.request_envelope_sha256,
            "resolved_sha": plan.candidate_sha,
        },
        "target": {"username": "qianyi", "user_id": "user-qianyi"},
        "audit_event_id": "audit-event",
        "browser": {"name": "chromium", "version": "1.2.3"},
        "checks": {check_id: True for check_id in BROWSER_REPORT_CHECK_IDS},
        "cleanup": {"logout_status": 204, "auth_me_after_logout_status": 401},
    }


def test_final_browser_runs_exact_image_and_shared_report_contract(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    plan = None

    def run(argv):
        calls.append(tuple(argv))
        assert plan is not None
        mount = next(
            item for item in argv if item.startswith("type=bind,src=") and "/evidence" in item
        )
        directory = Path(
            next(part for part in mount.split(",") if part.startswith("src=")).removeprefix("src=")
        )
        report = directory / "staging-admin-browser-acceptance.json"
        report.write_text(json.dumps(_report(plan)), encoding="utf-8")
        report.chmod(0o600)
        return SimpleNamespace(returncode=0)

    plan, executor = _authority(tmp_path, run=run)

    result = executor("final.browser", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.protected_mutation
    assert len(calls) == 1
    command = calls[0]
    assert plan.browser_image_digest in command
    assert plan.request_envelope_sha256 in command
    assert command[command.index("--route") + 1] == "https://yylx.world/dev"


def test_final_browser_reuses_valid_report_without_second_session(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    plan, executor = _authority(
        tmp_path,
        run=lambda argv: calls.append(tuple(argv)) or SimpleNamespace(returncode=0),
    )
    directory = executor._directory(plan)
    directory.mkdir(mode=0o700)
    report = directory / "staging-admin-browser-acceptance.json"
    report.write_text(json.dumps(_report(plan)), encoding="utf-8")
    report.chmod(0o600)

    result = executor("final.browser", CheckOperation.APPLY, plan)

    assert result.ready
    assert calls == []


def test_final_browser_rejects_partial_evidence_without_rerun(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    plan, executor = _authority(
        tmp_path,
        run=lambda argv: calls.append(tuple(argv)) or SimpleNamespace(returncode=0),
    )
    executor._directory(plan).mkdir(mode=0o700)

    result = executor("final.browser", CheckOperation.APPLY, plan)

    assert result.blockers == {"browser": "browser-existing-evidence-invalid"}
    assert not result.protected_mutation
    assert calls == []


def test_final_browser_rejects_token_content_drift_before_run(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []
    plan, executor = _authority(
        tmp_path,
        run=lambda argv: calls.append(tuple(argv)) or SimpleNamespace(returncode=0),
    )
    executor.token_path.write_bytes(b"changed\n")

    result = executor("final.browser", CheckOperation.APPLY, plan)

    assert result.blockers == {"browser": "browser-token-binding-drift"}
    assert not result.protected_mutation
    assert calls == []
