"""User-visible backend identities for durable service execution."""

import os

NEBIUS_BACKEND = "nebius"
NEBIUS_LOGICAL_POOL_ID = "nebius-cpu"

def local_execution_enabled() -> bool:
    """Worker execution is limited to explicitly disposable local environments."""
    return (
        os.environ.get("LOOM_LOCAL_EXECUTION", "") == "1"
        and os.environ.get("LOOM_ENV", "development").strip().lower() == "development"
    )


__all__ = ["NEBIUS_BACKEND", "NEBIUS_LOGICAL_POOL_ID"]
