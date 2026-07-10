"""Reserved database identities owned by Loom deployments.

These values are deliberately stable across releases. Callers must match the
identifier and expected name, not discover a system identity by display name.
"""

from __future__ import annotations

from uuid import UUID

TASKSET_FENCE_CANARY_TEAM_ID = UUID("2c9506e1-7d5e-4b49-b532-4b8f0a3f5ea9")
TASKSET_FENCE_CANARY_TEAM_NAME = "loom-system-taskset-fence-canary"
