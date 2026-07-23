"""Disposable live acceptance probe for issue #833."""


def test_authoritative_repository_failure_remains_red() -> None:
    """The first probe generation must fail closed before the real fix."""
    raise AssertionError("intentional issue #833 authoritative-gate acceptance failure")
