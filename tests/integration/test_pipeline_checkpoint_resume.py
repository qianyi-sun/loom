from __future__ import annotations

from loom.pipeline.checkpoint import checkpoint_is_resume_compatible, resume_compatibility_key

_RECIPE = "sha256:" + "1" * 64
_BINDINGS = "sha256:" + "2" * 64
_EXECUTION = "sha256:" + "3" * 64
_IMAGE = "sha256:" + "4" * 64


def _compatible(reason: str | None, **overrides: str) -> bool:
    observed = {
        "observed_recipe_digest": _RECIPE,
        "observed_input_bindings_digest": _BINDINGS,
        "observed_execution_spec_digest": _EXECUTION,
        "observed_image_digest": _IMAGE,
        "observed_resume_compatibility_key": resume_compatibility_key(
            recipe_digest=_RECIPE,
            resolved_input_bindings_digest=_BINDINGS,
            execution_spec_digest=_EXECUTION,
            image_digest=_IMAGE,
        ),
    }
    observed.update(overrides)
    return checkpoint_is_resume_compatible(
        previous_reason=reason,
        recipe_digest=_RECIPE,
        resolved_input_bindings_digest=_BINDINGS,
        execution_spec_digest=_EXECUTION,
        image_digest=_IMAGE,
        **observed,
    )


def test_only_infrastructure_retry_can_resume_the_exact_five_key_identity() -> None:
    assert _compatible("worker_lost")
    assert _compatible("object_store_transport")
    assert not _compatible("provider_429")
    assert not _compatible("user_cancel")
    assert not _compatible(None)


def test_any_frozen_identity_drift_starts_from_scratch() -> None:
    assert not _compatible("worker_lost", observed_image_digest="sha256:" + "f" * 64)
    assert not _compatible(
        "worker_lost", observed_resume_compatibility_key="sha256:" + "e" * 64
    )
