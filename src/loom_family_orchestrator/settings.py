"""Orchestrator settings alias.

The orchestrator runs with the control-plane's env prefix and reads
the same schema-driven ``ControlPlaneSettings`` model. Consolidating
here keeps ``main_loop.py`` free of cross-service imports.
"""

from __future__ import annotations

from loom_control_plane.config import ControlPlaneSettings

OrchestratorSettings = ControlPlaneSettings

__all__ = ["OrchestratorSettings"]
