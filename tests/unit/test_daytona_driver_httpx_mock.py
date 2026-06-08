"""Contract test for Daytona SDK wire format via httpx MockTransport.

The plan-doc strategy was to patch AsyncDaytona's `_http_client`
attribute to inject a MockTransport. Empirical probe shows the
SDK exposes `_api_client` (an OpenAPI-generated client) instead,
which wraps multiple httpx clients under the hood — substituting
just the transport requires reaching into the generated code and
is fragile across minor SDK bumps.

Deferred to a follow-up task with a smaller-surface approach: the
DaytonaClient (Task 5 wrapper) is the seam we'd actually want to
mock; live SDK contract verification belongs in the Task 17
integration test (gated on LOOM_RUN_DAYTONA_INTEGRATION=1 +
DAYTONA_API_KEY).
"""

from __future__ import annotations

import pytest

pytest.skip(
    "Daytona SDK MockTransport contract test deferred — _api_client "
    "internals are OpenAPI-generated, not easily transport-swappable. "
    "See module docstring + plan-26 Task 14 outcome.",
    allow_module_level=True,
)
