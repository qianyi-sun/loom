from loom.pipeline.core_acceptance import PIPELINE_CORE_FAULT_MATRIX


def test_fault_arms_are_one_shot_and_cleanup_explicit() -> None:
    scenarios = {row.scenario: row for row in PIPELINE_CORE_FAULT_MATRIX}
    assert scenarios["distributed_fault_arms"].expected.cleanup == "clean"
    assert scenarios["final_output_commit_races"].expected.committed_artifacts
    assert scenarios["worker_lost_before_start"].expected.cleanup == (
        "cleanup_pending_then_clean"
    )
