"""Manifest upload size cap tests (#242 sub-plan 8)."""

from __future__ import annotations

from io import BytesIO

import pytest
from fastapi import HTTPException, UploadFile

from loom_service.taskset_intake import parse_manifest_upload

_MINIMAL_MANIFEST = b"""\
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: my-tasks
  display_name: My Tasks
source:
  type: hf
  locator: org/dataset
instance_mapping:
  prompt: row.question
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: t
  environment:
    os: linux
  agent:
    name: default
  steps:
    - artifacts: [out.txt]
"""


@pytest.mark.asyncio
async def test_manifest_exceeding_cap_returns_413() -> None:
    upload = UploadFile(filename="manifest.yaml", file=BytesIO(b"x" * 100))
    with pytest.raises(HTTPException) as exc_info:
        await parse_manifest_upload(upload, max_bytes=50)
    assert exc_info.value.status_code == 413
    assert exc_info.value.detail == "manifest_too_large"


@pytest.mark.asyncio
async def test_manifest_within_cap_accepted() -> None:
    upload = UploadFile(filename="manifest.yaml", file=BytesIO(_MINIMAL_MANIFEST))
    model, _raw = await parse_manifest_upload(upload, max_bytes=1_048_576)
    assert model.slug == "my-tasks"
