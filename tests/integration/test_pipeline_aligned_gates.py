from loom.pipeline.gates import GateSelection, strict_and_gate_target


def test_strict_and_gate_waits_then_fails_closed() -> None:
    assert strict_and_gate_target([GateSelection.SELECTED, GateSelection.PENDING]) == "pending"
    assert (
        strict_and_gate_target([GateSelection.PENDING, GateSelection.SUBJECT_NOT_SUCCEEDED])
        == "not_selected"
    )
    assert strict_and_gate_target([GateSelection.SELECTED, GateSelection.SELECTED]) == "selected"
