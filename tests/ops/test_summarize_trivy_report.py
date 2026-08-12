from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest
from scripts import summarize_trivy_report as summary

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/summarize_trivy_report.py"


def test_summary_exposes_bounded_critical_remediation_details(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "Results": [
                    {
                        "Target": "image\nlog-injection",
                        "Vulnerabilities": [
                            {
                                "VulnerabilityID": "CVE-2026-13221",
                                "PkgName": "perl-base",
                                "InstalledVersion": "5.40.1-6",
                                "FixedVersion": "",
                                "Status": "affected",
                                "Severity": "CRITICAL",
                            },
                            {
                                "VulnerabilityID": "CVE-LOW",
                                "PkgName": "ignored",
                                "Severity": "LOW",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(SCRIPT), str(report)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == (
        "Trivy scan blocked by 1 CRITICAL finding(s):\n"
        "- CVE-2026-13221 package=perl-base installed=5.40.1-6 "
        "fixed=unavailable status=affected target=image log-injection\n"
    )


def test_summary_does_not_mask_an_invalid_or_missing_report(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text("not-json", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(SCRIPT), str(report)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == "Trivy scan failed; its JSON report could not be summarized.\n"


def test_summary_limits_untrusted_findings_written_to_the_ci_log(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "Results": [
                    {
                        "Target": "image",
                        "Vulnerabilities": [
                            {
                                "VulnerabilityID": f"CVE-2026-{index:04d}",
                                "PkgName": "package",
                                "Severity": "CRITICAL",
                            }
                            for index in range(21)
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(SCRIPT), str(report)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stderr.count("\n- CVE-") == 20
    assert result.stderr.endswith("- ... 1 additional CRITICAL finding(s) omitted\n")


def test_summary_rejects_oversized_report_before_reading_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = tmp_path / "report.json"
    with report.open("wb") as handle:
        handle.truncate(16 * 1024 * 1024 + 1)

    def fail_if_read(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("oversized report was read")

    monkeypatch.setattr(Path, "read_text", fail_if_read)
    output = io.StringIO()

    summary.summarize_trivy_report(report, output=output)

    assert output.getvalue() == (
        "Trivy scan failed; its JSON report could not be summarized.\n"
    )
