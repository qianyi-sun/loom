from __future__ import annotations

import hashlib

import pytest

from loom.pipeline.checkpoint import resume_compatibility_key
from loom.pipeline.keys import canonical_identity

_RECIPE = "sha256:" + "1" * 64
_INPUT = "sha256:" + "2" * 64
_EXECUTION = "sha256:" + "3" * 64
_IMAGE = "sha256:" + "4" * 64


def test_resume_key_is_raw_jcs_of_exact_five_field_identity() -> None:
    preimage = {
        "checkpoint_schema": "loom.execution-checkpoint.v1",
        "execution_spec_digest": _EXECUTION,
        "image_digest": _IMAGE,
        "resolved_input_bindings_digest": _INPUT,
        "recipe_digest": _RECIPE,
    }
    expected = "sha256:" + hashlib.sha256(canonical_identity(preimage)).hexdigest()
    assert resume_compatibility_key(
        recipe_digest=_RECIPE,
        resolved_input_bindings_digest=_INPUT,
        execution_spec_digest=_EXECUTION,
        image_digest=_IMAGE,
    ) == expected
    assert canonical_identity(preimage)[-1:] != b"\n"


def test_resume_key_has_no_second_or_raw_input_identity() -> None:
    with pytest.raises(TypeError):
        resume_compatibility_key(  # type: ignore[call-arg]
            recipe_digest=_RECIPE,
            resolved_input_bindings_digest=_INPUT,
            execution_spec_digest=_EXECUTION,
            image_digest=_IMAGE,
            input_binding_digest=_INPUT,
        )
