from __future__ import annotations

from dataclasses import replace

import pytest

from loom_cli.rollout.gb10_convergence import (
    GB10ConvergenceState,
    GB10FleetCandidateObservation,
    GB10HostCandidateObservation,
    GB10MutationKind,
    plan_gb10_candidate_convergence,
)

SOURCE_DIGEST = "a" * 64
BOOT_IDS = {"trt-gb10-1": "boot-1", "trt-gb10-2": "boot-2"}


def _host(host: str, *, exact: bool = False, **changes) -> GB10HostCandidateObservation:
    value = GB10HostCandidateObservation(
        host=host,
        boot_id=BOOT_IDS[host],
        baseline_ready=True,
        candidate_source_exact=True,
        checkout_exact=exact,
        environment_exact=exact,
        units_exact=exact,
        legacy_absent=exact,
        service_timer_exact=exact,
        evidence_digest=("1" if host.endswith("1") else "2") * 64,
    )
    return replace(value, **changes)


def _fleet(*hosts: GB10HostCandidateObservation) -> GB10FleetCandidateObservation:
    return GB10FleetCandidateObservation(
        hosts={host.host: host for host in hosts},
        candidate_source_digest=SOURCE_DIGEST,
    )


def test_convergence_plan_is_exact_only_when_every_predicate_is_exact() -> None:
    result = plan_gb10_candidate_convergence(
        _fleet(_host("trt-gb10-1", exact=True), _host("trt-gb10-2", exact=True)),
        expected_boot_ids=BOOT_IDS,
        expected_candidate_source_digest=SOURCE_DIGEST,
    )

    assert result.state is GB10ConvergenceState.EXACT
    assert result.mutations == ()
    assert result.blockers == {}


def test_convergence_plan_returns_all_minimal_host_operations() -> None:
    result = plan_gb10_candidate_convergence(
        _fleet(
            _host("trt-gb10-1"),
            _host(
                "trt-gb10-2",
                checkout_exact=True,
                environment_exact=True,
                units_exact=True,
                legacy_absent=True,
                service_timer_exact=False,
            ),
        ),
        expected_boot_ids=BOOT_IDS,
        expected_candidate_source_digest=SOURCE_DIGEST,
    )

    assert result.state is GB10ConvergenceState.READY
    assert result.blockers == {}
    assert result.mutations[0].host == "trt-gb10-1"
    assert result.mutations[0].operations == tuple(GB10MutationKind)
    assert result.mutations[1].operations == (GB10MutationKind.SERVICE_TIMER,)


def test_convergence_plan_reports_all_boot_source_and_baseline_blockers() -> None:
    observation = replace(
        _fleet(
            _host("trt-gb10-1", boot_id="changed-boot"),
            _host("trt-gb10-2", baseline_ready=False),
        ),
        candidate_source_digest="b" * 64,
    )

    result = plan_gb10_candidate_convergence(
        observation,
        expected_boot_ids=BOOT_IDS,
        expected_candidate_source_digest=SOURCE_DIGEST,
    )

    assert result.state is GB10ConvergenceState.DRIFTED
    assert result.mutations == ()
    assert result.blockers == {
        "candidate-source": "gb10-candidate-source-drift",
        "trt-gb10-1": "gb10-host-boot-drift",
        "trt-gb10-2": "gb10-host-not-safely-applicable",
    }


def test_convergence_rejects_missing_or_extra_host_identity() -> None:
    result = plan_gb10_candidate_convergence(
        _fleet(_host("trt-gb10-1")),
        expected_boot_ids=BOOT_IDS,
        expected_candidate_source_digest=SOURCE_DIGEST,
    )

    assert result.state is GB10ConvergenceState.DRIFTED
    assert result.blockers["fleet-identity"] == "gb10-fleet-identity-drift"

    with pytest.raises(ValueError, match="observation is invalid"):
        GB10FleetCandidateObservation(hosts={}, candidate_source_digest=SOURCE_DIGEST)
