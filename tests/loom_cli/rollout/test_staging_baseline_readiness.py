from __future__ import annotations

from loom_cli.rollout.staging_baseline_readiness import (
    BaselineProbeResult,
    StagingBaselineSession,
)

CHECK_IDS = (
    "staging.health",
    "staging.auth",
    "staging.catalog-task",
    "staging.storage-db",
    "staging.network",
)


def _result(check_id: str, *, blockers: dict[str, str] | None = None) -> BaselineProbeResult:
    return BaselineProbeResult(
        check_id=check_id,
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        readonly_principal="loom-rollout-readonly",
        observed_mutation_epoch=8,
        resource_digest=check_id.encode().hex().ljust(64, "0")[:64],
        blockers=blockers or {},
    )


def test_baseline_session_caches_exact_readonly_probes_and_aggregates() -> None:
    calls: list[str] = []
    session = StagingBaselineSession(
        {
            check_id: lambda check_id=check_id: calls.append(check_id) or _result(check_id)
            for check_id in CHECK_IDS
        },
        expected_environment="staging",
        expected_namespace="loom-staging",
        expected_route="https://yylx.world/dev",
        expected_mutation_epoch=8,
    )

    for check_id in CHECK_IDS:
        assert session.probe(check_id).ready
        assert session.probe(check_id).ready
    aggregate = session.aggregate()

    assert calls == list(CHECK_IDS)
    assert aggregate.ready
    assert len(aggregate.resource_digest) == 64


def test_baseline_aggregate_preserves_normalized_blockers() -> None:
    session = StagingBaselineSession(
        {
            check_id: (
                lambda check_id=check_id: _result(
                    check_id,
                    blockers={"postgres": "connection-refused"}
                    if check_id == "staging.storage-db"
                    else {},
                )
            )
            for check_id in CHECK_IDS
        },
        expected_environment="staging",
        expected_namespace="loom-staging",
        expected_route="https://yylx.world/dev",
        expected_mutation_epoch=8,
    )
    for check_id in CHECK_IDS:
        session.probe(check_id)

    aggregate = session.aggregate()

    assert not aggregate.ready
    assert aggregate.blockers == {"staging.storage-db:postgres": "connection-refused"}
