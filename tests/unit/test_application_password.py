"""Only the original bounded SCRAM credential may survive a NOLOGIN handoff."""

import pytest

from loom.application_password import application_scram_verifier, matches_application_scram


def test_original_password_matches_independently_salted_verifiers() -> None:
    first = application_scram_verifier("original-password")
    second = application_scram_verifier("original-password")
    assert first != second
    assert matches_application_scram("original-password", first)
    assert matches_application_scram("original-password", second)
    assert not matches_application_scram("different-password", first)


@pytest.mark.parametrize("value", [None, "", "has space", "unicode-å", "x" * 1025, 7])
def test_invalid_password_is_rejected_without_exposing_it(value) -> None:
    with pytest.raises(ValueError, match="application password is invalid"):
        application_scram_verifier(value)
    assert not matches_application_scram(value, "SCRAM-SHA-256$4096:salt$stored:server")


@pytest.mark.parametrize(
    "verifier", [None, "", "md5" + "a" * 32, "SCRAM-SHA-256$999999:salt$stored:server", "x" * 1025]
)
def test_unknown_or_unbounded_verifier_is_not_the_original_password(verifier) -> None:
    assert not matches_application_scram("original-password", verifier)
