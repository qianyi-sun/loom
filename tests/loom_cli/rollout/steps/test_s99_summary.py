"""Security-focused summary rendering tests for rollout step 99."""

from __future__ import annotations

import json
from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.operator.redaction import rollout_redaction_scope
from loom_cli.rollout.steps.s99_summary import SummaryStep


def test_summary_allowlists_attribution_and_sanitizes_tampered_result(
    tmp_path: Path,
) -> None:
    secret = "opaque-summary-secret"
    token_source = "file:/var/lib/loom-staging-rollout/private/admin-token"
    ev = EvidenceDirectory(tmp_path, "request|rollout`id")
    ev.ensure()
    tampered = ev.step_dir(7, "tampered")
    tampered.result_path().write_text(
        json.dumps(
            {
                "number": "7|8`\n9",
                "name": "render|name`\r\nnext",
                "state": "failed|state`\nnext",
                "summary": f"summary {secret}|`\nnext",
                "error": f"password: raw-password {secret}",
                "environment": {"TOKEN": "must-never-render"},
                "artifacts": {"private": token_source},
            }
        ),
        encoding="utf-8",
    )
    summary_dir = ev.step_dir(99, "summary")
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123|`\nnext",
        admin_token_source=token_source,
        request_id="stg-request|`\nnext",
        initiating_operator="hongjian|`\nnext",
        initiating_uid=2002,
        attempt_number=2,
        attempt_operator="devansh|`\nnext",
        attempt_uid=2501,
    )

    with rollout_redaction_scope((secret, token_source)):
        result = SummaryStep().run(ctx, summary_dir)

    assert result.exit_code == 0
    summary = summary_dir.artifact_path("summary.md").read_text(encoding="utf-8")
    assert "Request id:" in summary
    assert "Initiating operator:" in summary
    assert "Attempt:" in summary
    assert "Resolved SHA:" in summary
    assert "staging-abc123" in summary
    assert secret not in summary
    assert "raw-password" not in summary
    assert token_source not in summary
    assert "must-never-render" not in summary
    assert "\\|" in summary
    assert "\\`" in summary
    assert "\\n" in summary
    assert "\\r" in summary


def test_summary_skips_non_mapping_and_malformed_result_documents(
    tmp_path: Path,
) -> None:
    ev = EvidenceDirectory(tmp_path, "rid")
    ev.ensure()
    ev.step_dir(1, "list-result").result_path().write_text("[]\n", encoding="utf-8")
    ev.step_dir(2, "bad-json").result_path().write_text("{bad\n", encoding="utf-8")
    summary_dir = ev.step_dir(99, "summary")

    result = SummaryStep().run(make_ctx(tmp_path), summary_dir)

    assert result.exit_code == 0
    summary = summary_dir.artifact_path("summary.md").read_text(encoding="utf-8")
    assert "list-result" not in summary
    assert "bad-json" not in summary
