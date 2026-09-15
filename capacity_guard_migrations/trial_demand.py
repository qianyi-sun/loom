"""Observe protected pending demand without rewriting public trial state."""

from collections.abc import Callable

_WRAPPER = "loom_capacity_guard.capture_lifecycle_demand_observation(uuid,bigint,integer)"
_CAPTURE = "loom_capacity_guard.capture_lifecycle_demand_observation_v2_queued(uuid,bigint,integer)"


def _wrapper_replacements() -> list[tuple[str, str]]:
    replacements = []
    for state in ("queued", "protected-pending"):
        replacements.append((
            "          UPDATE public.trials AS trial\n"
            f"             SET state = '{state}'\n"
            "            FROM loom_capacity_guard.atomic_trial_submissions AS submission,",
            f"          -- Observe the {state} boundary without changing public state.\n"
            "          PERFORM 1\n"
            "            FROM public.trials AS trial,\n"
            "                 loom_capacity_guard.atomic_trial_submissions AS submission,",
        ))
    replacements.append((
        "             AND trial.state = 'queued'\n",
        "             -- Retain protected state for the restoration boundary.\n"
        "             AND trial.state = 'protected-pending'\n",
    ))
    return replacements


_CAPTURE_REPLACEMENTS = [
    (
        "              t.state AS public_state,",
        """              CASE WHEN t.state = 'protected-pending' AND EXISTS (
                SELECT 1 FROM loom_capacity_guard.atomic_trial_submissions AS origin
                 WHERE origin.trial_id = t.id
              ) THEN 'queued' ELSE t.state END AS public_state,""",
    ),
    (
        "             AND (trial.state <> 'queued'\n",
        "             AND (trial.state NOT IN ('queued', 'protected-pending')\n",
    ),
]


def install_trial_demand(rewrite: Callable[..., None]) -> None:
    rewrite(_WRAPPER, _wrapper_replacements(), upgrading=True)
    rewrite(_CAPTURE, _CAPTURE_REPLACEMENTS, upgrading=True)


def uninstall_trial_demand(rewrite: Callable[..., None]) -> None:
    rewrite(_CAPTURE, _CAPTURE_REPLACEMENTS, upgrading=False)
    rewrite(_WRAPPER, _wrapper_replacements(), upgrading=False)
