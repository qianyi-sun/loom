"""Exercise expired image-build dependencies through broker and installer boundaries."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_host as host

from loom_cli.rollout.operator.broker import main as broker_main
from loom_cli.rollout.preflight_contract import (
    CheckOperation,
    CheckProbe,
    DependencyExpiredError,
    PreflightDag,
    StageCapability,
)
from tests.loom_cli.rollout.operator.test_broker import NOW, FakeMutationGuard, fakes
from tests.loom_cli.rollout.test_preflight_contract import _check, _context


@pytest.mark.parametrize("argv", [("preflight",), ("start",), ("start", "--dry-run")])
@pytest.mark.parametrize(
    "consumer_id,dependency_id,ttl",
    [
        ("browser.runtime", "credentials.metadata", 120),
        ("manifests.render", "candidate.identity", 300),
    ],
)
def test_long_image_build_reports_expired_dependency_to_installer(
    tmp_path: Path, consumer_id: str, dependency_id: str, ttl: int, argv: tuple[str, ...]
) -> None:
    bundle = fakes(tmp_path)
    guard = FakeMutationGuard(bundle.order)
    clock = [NOW]
    consumer_calls: list[str] = []

    def build(_context):  # type: ignore[no-untyped-def]
        clock[0] += timedelta(seconds=600)
        return CheckProbe(passed=True, evidence={"status.value": "ready"})

    def consume(_context):  # type: ignore[no-untyped-def]
        consumer_calls.append(consumer_id)
        return CheckProbe(passed=True, evidence={"status.value": "ready"})

    dependency = _check(dependency_id, freshness_ttl_seconds=ttl)
    image = replace(
        _check("images.build", dependencies=(dependency_id,)),
        operations={CheckOperation.PROBE: build},
    )
    consumer = replace(
        _check(consumer_id, dependencies=(dependency_id, "images.build")),
        operations={CheckOperation.PROBE: consume},
    )

    def assess(_candidate, _epoch):  # type: ignore[no-untyped-def]
        PreflightDag((dependency, image, consumer)).run(_context(), now=lambda: clock[0])
        raise AssertionError("expired dependency must refuse admission")

    dependencies = replace(
        bundle.dependencies,
        assess_preflight=assess,
        read_mutation_epoch=lambda: 7,
        mutation_guard=guard,
    )
    result_code = broker_main(argv, dependencies=dependencies)
    assert result_code == 1
    report = json.loads(bundle.stderr.getvalue())
    assert report["passed"] is False
    assert report["assessment_complete"] is False
    assert report["checks"][0]["name"] == "preflight-dependency-expired"
    assert report["checks"][0]["check_id"] == consumer_id
    assert report["checks"][0]["dependency_ids"] == [dependency_id]
    assert report["checks"][0]["stage"] == "static"
    assert "assessment_digest" not in report
    assert bundle.stdout.getvalue() == ""
    assert consumer_calls == []
    assert bundle.store.requests == {}
    assert bundle.store.preflight_requests == {}
    assert bundle.backup.create_count == 0
    assert bundle.systemd.start_count == 0
    assert bundle.systemd.backup_starts == []
    assert guard.acquired == []
    assert guard.released == []

    class BrokerRunner:
        def run(self, argv, **_kwargs):  # type: ignore[no-untyped-def]
            return subprocess.CompletedProcess(
                argv, result_code, bundle.stdout.getvalue(), bundle.stderr.getvalue()
            )

    normalized = host.HostSystem(BrokerRunner()).run_post_install_preflight()
    assert normalized["status"] == "blocked"
    assert normalized["blocker_codes"] == ["preflight-dependency-expired"]


def test_untyped_preflight_error_does_not_expose_exception_text(tmp_path: Path) -> None:
    bundle = fakes(tmp_path)

    def assess(_candidate, _epoch):  # type: ignore[no-untyped-def]
        raise ValueError("provider Authorization: Bearer known-secret")

    dependencies = replace(
        bundle.dependencies, assess_preflight=assess, read_mutation_epoch=lambda: 7
    )
    assert broker_main(["preflight"], dependencies=dependencies) == 1
    assert bundle.stderr.getvalue() == "error: request authorization or validation failed\n"


@pytest.mark.parametrize(
    "check_id,dependency_ids",
    [
        ("Authorization: Bearer known-secret", ("candidate.identity",)),
        ("a" * 97, ("candidate.identity",)),
        ("browser.runtime", ("Authorization: Bearer known-secret",)),
        ("browser.runtime", ("a" * 97,)),
        ("browser.runtime", ()),
        ("browser.runtime", ("candidate.identity", "candidate.identity")),
        ("browser.runtime", tuple(f"check.{index}" for index in range(65))),
    ],
)
def test_dependency_diagnostic_rejects_unbounded_or_untrusted_identities(
    check_id: str, dependency_ids: tuple[str, ...]
) -> None:
    with pytest.raises(ValueError, match="expired dependency identities are invalid"):
        DependencyExpiredError(check_id, dependency_ids, StageCapability.STATIC)


def test_dependency_diagnostic_requires_declared_stage_enum() -> None:
    with pytest.raises(ValueError, match="expired dependency identities are invalid"):
        DependencyExpiredError(
            "browser.runtime",
            ("credentials.metadata",),
            "static",  # type: ignore[arg-type]
        )
