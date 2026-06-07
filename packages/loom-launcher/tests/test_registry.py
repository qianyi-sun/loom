"""Registry register_adapter + get_adapter + collision detection."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec
from loom_launcher.adapters.hello import HelloAdapter
from loom_launcher.registry import (
    all_adapters,
    register_adapter,
)


def test_hello_adapter_self_registers_on_import() -> None:
    """Importing loom_launcher triggers the adapters package which
    self-registers HelloAdapter at module load."""
    adapter = get_adapter("hello")
    assert adapter is not None
    assert isinstance(adapter, HelloAdapter)
    assert adapter.endpoint_dialect == "openai_chat"


def test_get_adapter_returns_none_for_unknown() -> None:
    assert get_adapter("definitely-not-real") is None


def test_register_adapter_returns_input() -> None:
    """register_adapter is decorator-friendly: it returns its input
    unmodified so `_ = register_adapter(MyAdapter())` works at module top."""
    @dataclass(frozen=True)
    class _Adapter:
        name: str = "test-register-returns-input"
        supports_os: frozenset[str] = frozenset({"linux"})
        endpoint_dialect: str = "openai_chat"
        api_key_env: str = "X"
        base_url_env: str = "Y"
        model_name_template: str = "{model_id}"
        supports_multi_turn: bool = False
        additional_egress: frozenset[str] = frozenset()

        def build_invocation(self, *, instruction, workdir, model, env):
            return ["true"]

        async def capture_events(self, *, exec_handle, step_id, trial_id):
            if False:
                yield None

    instance = _Adapter()
    returned = register_adapter(instance)
    assert returned is instance
    assert get_adapter("test-register-returns-input") is instance


def test_register_adapter_rejects_collision() -> None:
    """Two different instances claiming the same name → ValueError. The
    SAME instance being registered twice is silently OK (idempotent)."""
    @dataclass(frozen=True)
    class _A:
        name: str = "test-collision"
        supports_os: frozenset[str] = frozenset({"linux"})
        endpoint_dialect: str = "openai_chat"
        api_key_env: str = "X"
        base_url_env: str = "Y"
        model_name_template: str = "{model_id}"
        supports_multi_turn: bool = False
        additional_egress: frozenset[str] = frozenset()

        def build_invocation(self, *, instruction, workdir, model, env):
            return ["true"]

        async def capture_events(self, *, exec_handle, step_id, trial_id):
            if False:
                yield None

    a = _A()
    register_adapter(a)
    # Re-registering the SAME instance is fine.
    register_adapter(a)
    # A DIFFERENT instance with the same name raises.
    b = _A()
    with pytest.raises(ValueError, match="already registered"):
        register_adapter(b)


def test_all_adapters_returns_snapshot() -> None:
    snapshot = all_adapters()
    names = {a.name for a in snapshot}
    assert "hello" in names


def test_hello_build_invocation_includes_instruction() -> None:
    adapter = get_adapter("hello")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=__import__("pathlib").PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv[0] == "echo"
    assert "solve fizzbuzz" in argv[1]
