"""Shared test stub for TrialConfig.

Plan 23 made `TrialConfig.agent_name` + `agent_model` required (every
trial submission must explicitly state which agent + model run, no
fallback to TaskConfig.agent.*). Tests that don't care about agent
identity use this helper to construct a TrialConfig with reasonable
stubs — keeps test bodies focused on what they're actually
exercising.
"""

from __future__ import annotations

from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec


def stub_trial_config(**overrides: object) -> TrialConfig:
    """TrialConfig with agent_name='oracle' + a local stub model.

    Pass `**overrides` to swap any field for the test at hand.
    """
    defaults: dict[str, object] = {
        "agent_name": "oracle",
        "agent_model": ModelSpec(provider="local", name="stub"),
    }
    defaults.update(overrides)
    return TrialConfig(**defaults)  # type: ignore[arg-type]
