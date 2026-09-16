"""Generate the bundled reference through actual isolated provisioning."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
import scripts.build_application_schema_reference as builder
from alembic.config import Config
from alembic.script import ScriptDirectory
from scripts.build_application_schema_reference import build_application_schema_reference

from loom.application_schema_reference import (
    ApplicationSchemaReferenceError,
    application_schema_reference,
    require_application_schema_reference,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", ["0148/guard_0036", "0149/guard_0035", "0148/guard_0035", "0147/guard_0036", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"])
