"""Read-only PostgreSQL authority for critical rollout object inventories."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol

from .config import OperatorConfig
from .rollout_checkpoint import (
    ImmutableObjectInventory,
    ImmutableObjectReference,
    build_immutable_inventory,
)

_INVENTORY_TIMEOUT_SECONDS = 60.0
_INVENTORY_SQL = """
WITH epoch AS (
  SELECT epoch
  FROM staging_mutation_epochs
  WHERE environment = 'staging' AND namespace = 'loom-staging'
), inventory AS (
  SELECT jsonb_build_object(
    'bucket', obj.bucket,
    'object_key', obj.object_key,
    'version_id', obj.version_id,
    'content_sha256', obj.content_sha256,
    'size_bytes', obj.size_bytes,
    'data_class', auth.data_class,
    'authoritative_source', auth.metadata ->> 'authoritative_source'
  ) AS item
  FROM data_lifecycle_objects AS obj
  JOIN data_lifecycle_authorities AS auth ON auth.id = obj.authority_id
  WHERE obj.environment = 'staging'
    AND obj.namespace = 'loom-staging'
    AND obj.state = 'active'
    AND auth.environment = obj.environment
    AND auth.namespace = obj.namespace
    AND auth.state = 'active'
    AND auth.pinned
    AND auth.data_class IN ('benchmark', 'catalog', 'system')
)
SELECT jsonb_build_object(
  'mutation_epoch', (SELECT epoch FROM epoch),
  'schema_revision', (SELECT version_num FROM alembic_version),
  'objects', COALESCE(
    (SELECT jsonb_agg(item ORDER BY item->>'bucket', item->>'object_key', item->>'version_id')
     FROM inventory),
    '[]'::jsonb
  )
)::text;
""".strip()


class InventoryCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float | None = None,
    ) -> bytes: ...


class KubernetesLifecycleInventoryProvider:
    """Read exact pinned-object versions from the DB snapshot authority."""

    def __init__(
        self,
        config: OperatorConfig,
        *,
        runner: InventoryCommandRunner,
        environment: Mapping[str, str],
    ) -> None:
        if config.environment != "staging" or config.namespace != "loom-staging":
            raise ValueError("critical checkpoint inventory is staging-only")
        self._config = config
        self._runner = runner
        self._environment = dict(environment)

    def __call__(self, created_at: datetime) -> ImmutableObjectInventory:
        payload = self._runner.capture_stdout(
            [
                "kubectl",
                "-n",
                self._config.namespace,
                "exec",
                "statefulset/loom-postgres",
                "--",
                "sh",
                "-ceu",
                'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                _INVENTORY_SQL,
            ],
            env=self._environment,
            timeout_seconds=_INVENTORY_TIMEOUT_SECONDS,
        )
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("object inventory query did not return valid JSON") from exc
        if not isinstance(document, dict) or set(document) != {
            "mutation_epoch",
            "schema_revision",
            "objects",
        }:
            raise ValueError("object inventory query schema is invalid")
        if (
            type(document["mutation_epoch"]) is not int
            or not isinstance(document["schema_revision"], str)
            or not isinstance(document["objects"], list)
            or not all(isinstance(item, dict) for item in document["objects"])
        ):
            raise ValueError("object inventory query authority is incomplete")
        objects = tuple(ImmutableObjectReference.from_dict(item) for item in document["objects"])
        return build_immutable_inventory(
            environment=self._config.environment,
            namespace=self._config.namespace,
            mutation_epoch=document["mutation_epoch"],
            schema_revision=document["schema_revision"],
            created_at=created_at,
            objects=objects,
        )


__all__ = ["KubernetesLifecycleInventoryProvider"]
