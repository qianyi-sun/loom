from __future__ import annotations

import asyncio
from typing import Any

from loom_service.readiness import probe_dependencies


class _Scalar:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class _Session:
    def __init__(
        self,
        *,
        values: tuple[int, ...] = (1, 8),
        error: Exception | None = None,
    ) -> None:
        self.values = list(values)
        self.error = error
        self.statements: list[str] = []

    async def execute(self, statement: Any) -> _Scalar:
        self.statements.append(str(statement))
        if self.error is not None:
            raise self.error
        return _Scalar(self.values.pop(0))


class _Minio:
    def __init__(self, *, failing: set[str] | None = None) -> None:
        self.failing = failing or set()
        self.calls: list[tuple[str, str]] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803 - boto3 API
        self.calls.append(("HEAD", Bucket))
        if Bucket in self.failing:
            raise RuntimeError("provider detail must be redacted")


def test_dependency_readiness_is_read_only_and_secret_free() -> None:
    session = _Session()
    minio = _Minio()

    result = asyncio.run(
        probe_dependencies(
            session,  # type: ignore[arg-type]
            minio_client=minio,
            buckets=("trajectories", "artifacts", "artifacts"),
            environment="staging",
            namespace="loom-staging",
        )
    )

    assert result.ready
    assert session.statements == [
        "SELECT 1",
        "SELECT epoch FROM staging_mutation_epochs WHERE environment = "
        "'staging' AND namespace = 'loom-staging'",
    ]
    assert minio.calls == [("HEAD", "artifacts"), ("HEAD", "trajectories")]
    assert result.to_dict()["blockers"] == []
    assert result.mutation_epoch == 8
    assert len(result.resource_digest) == 64


def test_dependency_readiness_reports_all_components_without_provider_details() -> None:
    session = _Session(error=RuntimeError("postgresql://secret"))
    minio = _Minio(failing={"artifacts", "trajectories"})

    result = asyncio.run(
        probe_dependencies(
            session,  # type: ignore[arg-type]
            minio_client=minio,
            buckets=("artifacts", "trajectories"),
            environment="staging",
            namespace="loom-staging",
        )
    )

    assert not result.ready
    assert result.blockers == (
        "object-store-bucket-unavailable:artifacts",
        "object-store-bucket-unavailable:trajectories",
        "postgres-unavailable",
    )
    assert "secret" not in str(result.to_dict())


def test_dependency_readiness_rejects_empty_bucket_authority() -> None:
    try:
        asyncio.run(
            probe_dependencies(
                _Session(),  # type: ignore[arg-type]
                minio_client=_Minio(),
                buckets=(),
                environment="staging",
                namespace="loom-staging",
            )
        )
    except ValueError as exc:
        assert str(exc) == "readiness bucket authority is invalid"
    else:  # pragma: no cover - defensive
        raise AssertionError("empty bucket authority was accepted")
