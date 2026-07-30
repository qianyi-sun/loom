from loom_cli.rollout.operator.failure_diagnostics import (
    unclassified_failure_diagnostic,
    unclassified_failure_location,
)


def test_diagnostic_surfaces_type_and_raise_site_but_not_the_message() -> None:
    try:
        raise ValueError("secret-bearing detail must never appear")
    except ValueError as error:
        diagnostic = unclassified_failure_diagnostic(error, activity="backup")
    # class + raise-site are surfaced (secret-safe)...
    assert diagnostic.startswith("unclassified backup failure: ValueError at ")
    assert "test_failure_diagnostics.py" in diagnostic
    assert "in test_diagnostic_surfaces_type_and_raise_site_but_not_the_message" in diagnostic
    # ...but the arbitrary message is withheld (the #1077 lesson).
    assert "secret-bearing" not in diagnostic


def test_activity_label_is_reflected() -> None:
    try:
        raise RuntimeError("x")
    except RuntimeError as error:
        diagnostic = unclassified_failure_diagnostic(error, activity="run-attempt")
    assert diagnostic.startswith("unclassified run-attempt failure: RuntimeError at ")


def test_location_is_empty_and_safe_without_a_traceback() -> None:
    # An exception that was never raised has no traceback; still safe, no crash.
    error = ValueError("x")
    assert unclassified_failure_location(error) == ""
    assert (
        unclassified_failure_diagnostic(error, activity="worker")
        == "unclassified worker failure: ValueError"
    )
