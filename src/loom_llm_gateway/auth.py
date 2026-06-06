"""Backward-compat re-export of `loom.auth`.

The auth helper lives in `loom.auth` so every Loom HTTP service (Gateway
+ Control Plane + …) can share it. This module is kept so existing
imports (`from loom_llm_gateway.auth import …`) continue to work.
"""

from loom.auth import AuthContext, verify_bearer_token

__all__ = ["AuthContext", "verify_bearer_token"]
