"""Refresh identification is not start authority, even for an expired grant."""

from typing import Literal

from pydantic import Field

from loom_task_image_authority.execution_start import ExecutionStartRequest
from loom_task_image_authority.publication_contracts import _ClosedPublicationModel

MAX_EXECUTION_DELIVERY_BYTES = 4 * 1024 * 1024
REFRESH_MINIMUM_REMAINING_SECONDS = 30


class ExecutionRefreshRequest(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-execution-refresh-request/v1"] = Field(alias="schema")
    previous: ExecutionStartRequest
