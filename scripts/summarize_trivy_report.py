#!/usr/bin/env python3
"""Print a bounded, log-safe summary of a failed Trivy JSON report."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import TextIO

MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_REPORTED_FINDINGS = 20
_UNSAFE_LOG_CHARACTER = re.compile(r"[^\x20-\x7e]")


def _log_value(value: object, *, fallback: str = "unknown") -> str:
    if not isinstance(value, str) or not value:
        return fallback
    cleaned = _UNSAFE_LOG_CHARACTER.sub(" ", value)
    return cleaned[:160]


def summarize_trivy_report(report: Path, *, output: TextIO) -> None:
    """Write critical findings without allowing report data to forge CI log lines."""

    try:
        if report.stat().st_size > MAX_REPORT_BYTES:
            raise ValueError("Trivy report exceeds the diagnostic size limit")
        payload = json.loads(report.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not isinstance(payload.get("Results"), list):
            raise ValueError("invalid Trivy report structure")
    except (OSError, UnicodeError, ValueError):
        output.write("Trivy scan failed; its JSON report could not be summarized.\n")
        return

    findings: list[tuple[str, str, str, str, str, str]] = []
    finding_count = 0
    for result in payload["Results"]:
        if not isinstance(result, dict):
            continue
        target = _log_value(result.get("Target"))
        vulnerabilities = result.get("Vulnerabilities")
        if not isinstance(vulnerabilities, list):
            continue
        for vulnerability in vulnerabilities:
            if not isinstance(vulnerability, dict):
                continue
            if vulnerability.get("Severity") != "CRITICAL":
                continue
            finding_count += 1
            if len(findings) < MAX_REPORTED_FINDINGS:
                findings.append(
                    (
                        _log_value(vulnerability.get("VulnerabilityID")),
                        _log_value(vulnerability.get("PkgName")),
                        _log_value(vulnerability.get("InstalledVersion")),
                        _log_value(
                            vulnerability.get("FixedVersion"), fallback="unavailable"
                        ),
                        _log_value(vulnerability.get("Status")),
                        target,
                    )
                )

    if finding_count == 0:
        output.write("Trivy scan failed with no CRITICAL findings in its JSON report.\n")
        return

    output.write(f"Trivy scan blocked by {finding_count} CRITICAL finding(s):\n")
    for vulnerability_id, package, installed, fixed, status, target in findings:
        output.write(
            f"- {vulnerability_id} package={package} installed={installed} "
            f"fixed={fixed} status={status} target={target}\n"
        )
    omitted = finding_count - len(findings)
    if omitted > 0:
        output.write(f"- ... {omitted} additional CRITICAL finding(s) omitted\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    arguments = parser.parse_args()
    summarize_trivy_report(arguments.report, output=sys.stderr)


if __name__ == "__main__":
    main()


__all__ = ["MAX_REPORTED_FINDINGS", "MAX_REPORT_BYTES", "summarize_trivy_report"]
