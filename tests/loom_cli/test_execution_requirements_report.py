"""Explicit capability gaps remain visible even when later intake fails."""

from pathlib import Path

import pytest

from tests.loom_cli.test_local_compatibility_report import _report, _write_bundle


def test_report_keeps_capabilities_and_prerequisites_when_bootstrap_is_unsupported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    declaration = {
        "capabilities": ["external_cluster"],
        "prerequisites": [
            {"name": "cluster", "kind": "endpoint"},
            {"name": "auth", "kind": "managed_secret", "reference": "k8s-secret://team/auth"},
        ],
    }
    bundle = _write_bundle(tmp_path, "cluster", execution_requirements=declaration)
    (bundle / "tests/test.sh").write_text("#!/bin/sh\nunknown-bootstrap\n")
    original = {str(path): path.read_bytes() for path in bundle.rglob("*") if path.is_file()}

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["original_requirements"]["environment"]["execution_requirements"] == declaration
    diagnostics = {item["code"]: item for item in report["diagnostics"]}
    assert {
        "external_cluster_unqualified", "execution_prerequisite_missing",
        "execution_prerequisite_unverified", "profile_adaptation_failed",
    } <= diagnostics.keys()
    assert diagnostics["execution_prerequisite_missing"]["source_location"].endswith(
        "task.toml#environment.execution_requirements.prerequisites.cluster"
    )
    assert diagnostics["execution_prerequisite_unverified"]["category"] == "execution_prerequisite"
    assert {str(path): path.read_bytes() for path in bundle.rglob("*") if path.is_file()} == original


@pytest.mark.parametrize("field", ["reference", "value"])
def test_invalid_prerequisite_literal_is_not_echoed_into_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], field: str,
) -> None:
    secret = "must-not-echo-private-key-material"
    _write_bundle(tmp_path, "secret", execution_requirements={
        "prerequisites": [{"name": "auth", "kind": "managed_secret", field: secret}],
    })

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    assert secret not in str(payload)
    report = payload["compatibility_report"]["tasks"][0]
    assert any(item["code"] == "invalid_task_config" for item in report["diagnostics"])
    assert report["original_requirements"]["environment"]["execution_requirements"]["redacted"] is True
