"""Family-run orchestrator service (#672 PR-2).

A long-running CP-sibling service that owns the ``adapting -> pending``
transition for family-run batches. On each poll cadence, picks the
oldest ``batch_family_state`` row in ``state='adapting'``, evaluates
the batch's resolved adapter's ``evolve()`` against the just-completed
trial, and applies the resulting ``NextFamilyState`` (bump index +
transition to pending/done on success, or the batch's failure_policy's
FailureAction on exception).

See ``docs/architecture/family-runs.md`` and ``main_loop.py`` for the
per-iteration semantics.
"""
