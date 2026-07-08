"""Unit tests for worker family-state mount plumbing (#672 PR-3).

The full download-and-run path is exercised in the SkillFlow end-to-end
integration test. This file locks the two shape-level contracts that
the worker main loop depends on:

1. ``LocalTrialRunner.family_state_volumes`` is an appendable tuple
   dataclass field that defaults to empty (backward-compat with
   trials that don't opt into family-run mode).
2. When populated, the runner concatenates it onto ``sandbox_volumes``
   passed to the TrialContext so the driver's StartOptions mount the
   shared state directory alongside the JWT rotator mount.

Shape-level rather than end-to-end because ``LocalTrialRunner.run``
requires a live Driver + Agent + Verifier + TrialContext to stand up;
mocking the whole surface obscures what the test is actually
guarding.
"""

from __future__ import annotations

from dataclasses import fields

from loom_worker.trial_runner import LocalTrialRunner


def test_family_state_volumes_field_present_and_default_empty() -> None:
    """LocalTrialRunner exposes ``family_state_volumes`` as a
    tuple[tuple[str, str, str], ...] dataclass field with an empty
    default. Missing this field would break the worker main loop's
    ``LocalTrialRunner(..., family_state_volumes=…)`` construction."""
    field_names = {f.name for f in fields(LocalTrialRunner)}
    assert "family_state_volumes" in field_names
    field = next(f for f in fields(LocalTrialRunner) if f.name == "family_state_volumes")
    # dataclasses stores the default separately from the annotation.
    assert field.default == ()


def test_family_state_volumes_concatenated_in_run_source() -> None:
    """Guardrail against a future refactor that silently drops the
    concatenation. Instead of exercising the full run() flow we assert
    the source-level pattern is in place; the integration test in
    tests/integration/test_skillflow_iterative_end_to_end.py covers
    the runtime path.
    """
    import inspect

    src = inspect.getsource(LocalTrialRunner.run)
    assert "family_state_volumes" in src
    # The concatenation onto sandbox_volumes must be present so the
    # container mount tuple actually reaches ctx.sandbox_volumes.
    assert "sandbox_volumes + self.family_state_volumes" in src
