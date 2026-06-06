"""PytestVerifier — runs pytest in the sandbox, parses junit XML output.

Spec §2.4 (Verifier framework), §6.1 (testing tiers).

Output pattern: pytest writes junit XML to /loom/verifier/junit.xml; the
verifier downloads the file and parses it locally. We do NOT parse stdout
— the 10 MB cap could truncate large reports mid-document.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from xml.etree import ElementTree

from loom.driver.base import Driver
from loom.models.verifier import CheckResult, VerifierError, VerifierResult

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader

_JUNIT_PATH = PurePosixPath("/loom/verifier/junit.xml")


@dataclass
class PytestVerifier:
    name: str = "pytest"
    tests_dir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/tests"),
    )
    pytest_args: tuple[str, ...] = field(
        default_factory=lambda: ("--maxfail=0", "-q"),
    )
    user: str | int | None = None

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        await env.exec("mkdir -p /loom/verifier", user="root")
        cmd = (
            f"cd {self.tests_dir.as_posix()} && "
            f"pytest --junitxml={_JUNIT_PATH.as_posix()} "
            + " ".join(self.pytest_args)
            + " || true"   # exec non-zero is expected on failing tests; we read XML
        )
        await env.exec(cmd, user=self.user)

        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "junit.xml"
            try:
                await env.download(_JUNIT_PATH, local)
            except FileNotFoundError:
                return VerifierResult(
                    rewards={},
                    error=VerifierError(
                        kind="missing_tests",
                        message=f"pytest did not produce {_JUNIT_PATH}",
                    ),
                )
            try:
                parsed = parse_junit_xml(local.read_text(encoding="utf-8"))
            except ElementTree.ParseError as exc:
                return VerifierResult(
                    rewards={},
                    error=VerifierError(
                        kind="parse_failure",
                        message=f"junit XML parse failed: {exc}",
                    ),
                )

        return VerifierResult(
            rewards={
                "passed": float(
                    parsed["passed"] > 0 and parsed["passed"] == parsed["total"]
                ),
                "pytest_pass_rate": parsed["pass_rate"],
            },
            checks=parsed["checks"],
            structured={
                "total": parsed["total"],
                "passed": parsed["passed"],
                "failed": parsed["total"] - parsed["passed"],
            },
        )


def parse_junit_xml(xml_text: str) -> dict[str, Any]:
    """Parse junit XML into checks + counts. Pure function for testability."""
    root = ElementTree.fromstring(xml_text)
    checks: list[CheckResult] = []
    total = 0
    passed = 0
    for suite in root.findall(".//testsuite"):
        for case in suite.findall("testcase"):
            total += 1
            failure = case.find("failure")
            error = case.find("error")
            ok = failure is None and error is None
            if ok:
                passed += 1
            message: str | None = None
            if failure is not None:
                message = failure.get("message")
            elif error is not None:
                message = error.get("message")
            checks.append(CheckResult(
                name=case.get("name", "<unnamed>"),
                passed=ok,
                score=1.0 if ok else 0.0,
                message=message,
                duration_sec=float(case.get("time", 0.0)),
            ))
    pass_rate = (passed / total) if total else 0.0
    return {
        "total": total,
        "passed": passed,
        "pass_rate": pass_rate,
        "checks": checks,
    }
