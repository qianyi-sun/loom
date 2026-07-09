"""Terminus2Adapter — deprecated (#744).

``terminus-2`` is a native builtin runtime (``loom.agent.terminus2``).
This module is retained for import stability but no longer registers
an adapter — the service catalog lists terminus-2 under ``_BUILTIN``.
"""

from __future__ import annotations

# Intentionally does NOT call register_adapter().
