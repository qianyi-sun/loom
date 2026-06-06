import pytest

from loom.errors import VerifierError
from loom.verifier.base import Verifier, VerifierFactory


class _MyVerifier:
    name = "my-fake"

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        from loom.models.verifier import VerifierResult
        return VerifierResult(rewards={"x": 1.0})


def test_factory_register_and_create():
    factory = VerifierFactory()
    factory.register("my-fake", _MyVerifier)
    impl = factory.create("my-fake", args={})
    assert impl.name == "my-fake"


def test_factory_unknown_raises():
    factory = VerifierFactory()
    with pytest.raises(VerifierError, match="unknown verifier"):
        factory.create("nope", args={})


def test_factory_duplicate_register_raises():
    factory = VerifierFactory()
    factory.register("dup", _MyVerifier)
    with pytest.raises(ValueError, match="already registered"):
        factory.register("dup", _MyVerifier)


def test_verifier_protocol_attrs():
    """name is an annotation; verify is a method."""
    assert "name" in Verifier.__annotations__
    assert "verify" in dir(Verifier)
