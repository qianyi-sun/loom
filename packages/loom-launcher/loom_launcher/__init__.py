"""loom-launcher — agent adapter registry + sandbox capture utilities."""

# Import adapter modules so they self-register on package import.
from loom_launcher import adapters  # noqa: F401
from loom_launcher.adapter import (
    AgentAdapter,
    ExecHandle,
    ModelSpec,
    SandboxAccess,
)
from loom_launcher.registry import get_adapter, register_adapter

__all__ = [
    "AgentAdapter",
    "ExecHandle",
    "ModelSpec",
    "SandboxAccess",
    "get_adapter",
    "register_adapter",
]

__version__ = "0.1.0"
