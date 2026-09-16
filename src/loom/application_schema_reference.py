"""Expected application shape carried by the protected source/image release.

Regenerate with scripts/build_application_schema_reference.py in an isolated
trusted checkout. Never replace this pin with an observation of a live database.
The installed code must itself have passed protected release admission; this
module neither authenticates its own installation nor authorizes a transfer.
"""

from dataclasses import dataclass
from typing import Literal

from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_schema_inventory import ApplicationSchemaInventory


@dataclass(frozen=True, slots=True)
class ApplicationSchemaReference:
    format_version: int
    profile: str
    application_head: str
    guard_head: str
    postgres_image: str
    postgres_major: int
    object_count: int
    inventory_sha256: str


class ApplicationSchemaReferenceError(RuntimeError):
    """An observation does not match the bundled protected-release reference."""


ApplicationSchemaProfile = Literal[
    "legacy-owner",
    "sealed-owner",
    "staging-readonly-legacy-owner",
    "staging-readonly-sealed-owner",
    "cnpg-staging-legacy-owner",
    "cnpg-staging-sealed-owner",
    "cnpg-staging-executor-admission",
]
ApplicationSchemaRevision = Literal["0148/guard_0036", "0149/guard_0035", "0148/guard_0035", "0147/guard_0036", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"]
