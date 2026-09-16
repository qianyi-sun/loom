"""A caller cannot replace the bundled schema reference with an observed digest."""

from dataclasses import replace

import pytest

from loom.application_schema_inventory import ApplicationSchemaInventory, ApplicationSchemaObject
from loom.application_schema_reference import (
    ApplicationSchemaReferenceError,
    application_schema_profile,
    application_schema_reference,
    require_application_schema_reference,
)


def test_bundled_reference_is_immutable_and_has_no_caller_digest() -> None:
    reference = application_schema_reference()
    assert (reference.application_head, reference.guard_head) == ("0148", "guard_0036")
