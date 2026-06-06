from loom.agent.base import AgentRuntime, InBoxAgentRuntime


def test_agent_runtime_required_attrs():
    """Type-hint attributes live in __annotations__; methods live in dir()."""
    annotations = set(AgentRuntime.__annotations__)
    for attr in ("mode", "name", "version", "supports_os", "model"):
        assert attr in annotations, f"missing annotation {attr}"
    assert "run" in dir(AgentRuntime)


def test_in_box_agent_runtime_extends_with_setup():
    assert "setup" in dir(InBoxAgentRuntime)
    # In-box still must satisfy AgentRuntime's method surface.
    assert "run" in dir(InBoxAgentRuntime)
    # And inherit AgentRuntime's annotations.
    inherited = set()
    for base in InBoxAgentRuntime.__mro__:
        inherited.update(getattr(base, "__annotations__", {}))
    for attr in ("mode", "name", "version", "supports_os", "model"):
        assert attr in inherited, f"missing inherited annotation {attr}"
