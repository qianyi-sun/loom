#!/usr/bin/env python3
"""Discover and reconcile every shared-fleet development environment once.

This is the single submit-host writer. It reads the durable management
registry, collects each instance database's task/capacity state, updates one
global lease ledger, then invokes the existing Slurm autoscaler for every
environment with its exact candidate/generation grant.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import shlex
import stat
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import DevInstance, SlurmWorkerJob, WorkerPoolAutoscalerPolicy
from loom.dev_instance import derive_identity
from loom.dev_instance_runtime import KubectlClient, fixture_database_url
from loom_control_plane.global_dev_fleet_autoscaler import (
    DevCapacityDemand,
    GlobalDevAutoscalerError,
    GlobalDevFleetAutoscaler,
    capacity_grants_from_report,
)
from loom_control_plane.global_execution_fence import (
    GlobalExecutionFenceError,
    GlobalExecutionWitness,
    assert_legacy_scale_up_allowed,
    load_global_execution_witness,
)
from loom_control_plane.shared_capacity_broker import (
    BrokerBudgets,
    BrokerError,
    LeaseObservation,
    RequestState,
    SharedCapacityBroker,
)
from loom_control_plane.slurm_worker_jobs import ACTIVE_STATES, slurm_cluster_for_pool
from loom_control_plane.worker_pool_autoscaler import (
    AutoscalerObservation,
    _load_observation,
    reconcile_worker_pool_autoscaler_once,
)


class GlobalDevExternalError(RuntimeError):
    """Bounded operator-facing failure with no credential detail."""


@dataclass(frozen=True, slots=True)
class InstanceSnapshot:
    name: str
    environment: str
    pool_name: str
    database: str
    deployment_generation: int
    candidate_sha: str
    operation_epoch: int
    status: str
    min_slots: int
    max_slots: int
    observation: AutoscalerObservation
    terminal_slots: int
    actuator_config: dict[str, Any]

    def demand(self, now: datetime) -> DevCapacityDemand:
        if self.status != "ready":
            requested = 0
            minimum = 0
        else:
            minimum = self.min_slots
            requested = min(
                self.max_slots,
                max(
                    minimum,
                    self.observation.active_slots + self.observation.pending_slots,
                    self.observation.occupied_slots + self.observation.queued_slots,
                ),
            )
        return DevCapacityDemand(
            environment=self.environment,
            deployment_generation=self.deployment_generation,
            candidate_sha=self.candidate_sha,
            pool_name=self.pool_name,
            min_slots=minimum,
            requested_slots=requested,
            observed_at=now,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the single registry-driven shared development autoscaler tick.",
        allow_abbrev=False,
    )
    parser.add_argument("--management-db-url-file", type=Path, required=True)
    parser.add_argument("--fixture-admin-db-url-file", type=Path, required=True)
    parser.add_argument("--state-db", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--global-budget", type=int, required=True)
    parser.add_argument("--global-pending-budget", type=int)
    parser.add_argument("--worker-env-dir", type=Path, required=True)
    parser.add_argument("--worker-minio-endpoint", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--kubectl", default="/usr/local/bin/kubectl")
    parser.add_argument("--kube-context", default="")
    parser.add_argument("--snapshot-freshness-seconds", type=int, default=120)
    parser.add_argument("--lease-ttl-seconds", type=int, default=300)
    parser.add_argument(
        "--global-execution-witness-json",
        type=Path,
        required=True,
        help="Authenticated manager witness; absence permits drain only.",
    )
    parser.add_argument("--manager-public-key", type=Path, required=True)
    parser.add_argument("--expected-manager-public-key-sha256", required=True)
    return parser


def _read_secret_file(path: Path, label: str) -> str:
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
            raise GlobalDevExternalError(f"{label} must be a regular file")
        if metadata.st_uid != os.geteuid():
            raise GlobalDevExternalError(f"{label} must be owned by the supervisor user")
        mode = stat.S_IMODE(metadata.st_mode)
        if mode & 0o077:
            raise GlobalDevExternalError(f"{label} must be owner-only")
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise GlobalDevExternalError(f"{label} is unavailable") from exc
    if not value:
        raise GlobalDevExternalError(f"{label} is empty")
    return value


def _sqlalchemy_url(value: str) -> str:
    if value.startswith("postgresql://"):
        return value.replace("postgresql://", "postgresql+psycopg://", 1)
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql+psycopg://", 1)
    if value.startswith("postgresql+psycopg://"):
        return value
    raise GlobalDevExternalError("database URL scheme is unsupported")


async def _registry_rows(url: str) -> list[DevInstance]:
    engine = create_async_engine(_sqlalchemy_url(url), pool_pre_ping=True)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            return list(
                (
                    await session.execute(
                        select(DevInstance)
                        .where(DevInstance.status != "deleted")
                        .order_by(DevInstance.name),
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()


async def _instance_snapshot(
    admin_url: str,
    row: DevInstance,
    *,
    freshness_sec: int,
) -> InstanceSnapshot | None:
    identity = derive_identity(row.name)
    database_url = fixture_database_url(admin_url, identity.database)
    engine = create_async_engine(_sqlalchemy_url(database_url), pool_pre_ping=True)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            policy = (
                await session.execute(
                    select(WorkerPoolAutoscalerPolicy).where(
                        WorkerPoolAutoscalerPolicy.environment == identity.runtime_environment,
                        WorkerPoolAutoscalerPolicy.pool_name == identity.worker_pool,
                    ),
                )
            ).scalar_one_or_none()
            if policy is None:
                if row.status == "ready":
                    raise GlobalDevExternalError("ready dev instance has no autoscaler policy")
                return None
            config = policy.actuator_config if isinstance(policy.actuator_config, dict) else {}
            if policy.actuator != "slurm" or config.get("external_runner") is not True:
                raise GlobalDevExternalError("dev instance policy is not external Slurm")
            observation = await _load_observation(
                session,
                policy,
                now=datetime.now(UTC),
                freshness_sec=freshness_sec,
            )
            terminal_slots = int(
                (
                    await session.execute(
                        select(
                            func.coalesce(func.sum(SlurmWorkerJob.requested_concurrency), 0),
                        ).where(
                            SlurmWorkerJob.environment == identity.runtime_environment,
                            SlurmWorkerJob.pool_name == identity.worker_pool,
                            ~SlurmWorkerJob.state.in_(ACTIVE_STATES),
                        ),
                    )
                ).scalar_one()
            )
            await session.rollback()
            return InstanceSnapshot(
                name=row.name,
                environment=identity.runtime_environment,
                pool_name=identity.worker_pool,
                database=identity.database,
                deployment_generation=row.deployment_generation,
                candidate_sha=row.candidate_sha,
                operation_epoch=row.operation_epoch,
                status=row.status,
                min_slots=policy.min_slots,
                max_slots=policy.max_slots,
                observation=observation,
                terminal_slots=terminal_slots,
                actuator_config=dict(config),
            )
    except GlobalDevExternalError:
        raise
    except Exception as exc:
        raise GlobalDevExternalError("dev instance database snapshot failed") from exc
    finally:
        await engine.dispose()


def _lease_observations(
    broker: SharedCapacityBroker,
    snapshots: list[InstanceSnapshot],
) -> tuple[LeaseObservation, ...]:
    by_scope = {(item.environment, item.pool_name): item for item in snapshots}
    observations: list[LeaseObservation] = []
    for raw in cast(list[dict[str, object]], broker.status().get("requests", [])):
        request = raw.get("request")
        lease = raw.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise GlobalDevExternalError("capacity ledger record is invalid")
        if request.get("state") == RequestState.TERMINAL.value:
            continue
        snapshot = by_scope.get((str(request.get("sandbox")), str(request.get("pool"))))
        if snapshot is None:
            # Removed environments cannot be asserted terminal without their
            # last database observation. The ledger keeps that capacity until
            # TTL expiry/operator recovery instead of guessing it is free.
            continue
        observation = snapshot.observation
        observations.append(
            LeaseObservation(
                request_id=str(request["id"]),
                lease_epoch=int(lease["lease_epoch"]),
                pending_slots=observation.pending_slots,
                active_slots=observation.active_slots,
                draining_slots=observation.draining_slots,
                terminal_slots=snapshot.terminal_slots,
            )
        )
    return tuple(observations)


def _safe_env_value(value: str) -> str:
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise GlobalDevExternalError("worker environment value is invalid")
    return shlex.quote(value)


def _atomic_write(path: Path, content: str) -> None:
    if not path.parent.is_dir():
        raise GlobalDevExternalError("worker environment directory is unavailable")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(descriptor)
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


def _validate_owner_only_directory(path: Path, label: str) -> None:
    if not path.is_absolute():
        raise GlobalDevExternalError(f"{label} must be an absolute path")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise GlobalDevExternalError(f"{label} is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink():
        raise GlobalDevExternalError(f"{label} must be a regular directory")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise GlobalDevExternalError(f"{label} must be owner-only")


def _worker_env_is_current(
    path: Path,
    snapshot: InstanceSnapshot,
    *,
    image_tag: str,
) -> bool:
    if not path.is_file():
        return False
    if path.is_symlink():
        raise GlobalDevExternalError("worker environment path must be a regular file")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise GlobalDevExternalError("worker environment file must be owner-only")
    required = {
        f"LOOM_DEV_DEPLOYMENT_GENERATION={snapshot.deployment_generation}",
        f"LOOM_DEV_OPERATION_EPOCH={snapshot.operation_epoch}",
        f"LOOM_WORKER_CANDIDATE_SHA={snapshot.candidate_sha}",
        f"LOOM_IMAGE_TAG={image_tag}",
    }
    return required <= set(path.read_text(encoding="utf-8").splitlines())


def _prune_worker_env_files(directory: Path, active_environments: set[str]) -> int:
    """Remove only derived Loom dev credential files absent from the registry."""
    if not directory.is_dir():
        raise GlobalDevExternalError("worker environment directory is unavailable")
    removed = 0
    for path in directory.glob("dev-*.env"):
        environment = path.name.removesuffix(".env")
        name = environment.removeprefix("dev-")
        try:
            identity = derive_identity(name)
        except ValueError:
            continue
        if identity.runtime_environment != environment or environment in active_environments:
            continue
        if path.is_symlink() or not path.is_file():
            raise GlobalDevExternalError("stale worker environment path is not a regular file")
        try:
            path.unlink()
        except OSError as exc:
            raise GlobalDevExternalError("stale worker environment cleanup failed") from exc
        removed += 1
    return removed


async def _ensure_worker_env(
    args: argparse.Namespace,
    kubectl: KubectlClient,
    snapshot: InstanceSnapshot,
) -> None:
    identity = derive_identity(snapshot.name)
    configured = str(snapshot.actuator_config.get("env_file") or "")
    expected = args.worker_env_dir / f"{identity.runtime_environment}.env"
    if Path(configured) != expected:
        raise GlobalDevExternalError("policy env_file is outside its derived protected path")
    if snapshot.candidate_sha[:7] not in args.image_tag:
        raise GlobalDevExternalError("worker image tag is not bound to the instance candidate")
    if _worker_env_is_current(expected, snapshot, image_tag=args.image_tag):
        return

    admin_secret = await kubectl.read_secret(identity.namespace, "loom-admin-secret")
    runtime_secret = await kubectl.read_secret(identity.namespace, "loom-secrets")
    try:
        admin_document = admin_secret["secrets.toml"].decode()
        token_line = next(
            line for line in admin_document.splitlines() if line.startswith("token = ")
        )
        admin_token = token_line.split('"', 2)[1]
        minio_access = runtime_secret["minio-access-key"].decode()
        minio_secret = runtime_secret["minio-secret-key"].decode()
    except (KeyError, StopIteration, UnicodeDecodeError, IndexError) as exc:
        raise GlobalDevExternalError("instance worker bootstrap secret is invalid") from exc

    cp_url = f"https://{identity.worker_control_plane_host}"
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(
            f"{cp_url}/admin/worker-tokens",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"expires_in_days": 30},
        )
    if response.status_code != 201:
        raise GlobalDevExternalError("instance worker token mint failed")
    body = response.json()
    worker_token = body.get("token") if isinstance(body, dict) else None
    if not isinstance(worker_token, str):
        raise GlobalDevExternalError("instance worker token response was invalid")

    values = {
        "LOOM_DEV_DEPLOYMENT_GENERATION": str(snapshot.deployment_generation),
        "LOOM_DEV_OPERATION_EPOCH": str(snapshot.operation_epoch),
        "LOOM_IMAGE_TAG": args.image_tag,
        "LOOM_WORKER_CONTROL_PLANE_URL": cp_url,
        "LOOM_WORKER_GATEWAY_URL": f"https://{identity.worker_gateway_host}",
        "LOOM_WORKER_TOKEN": worker_token,
        "LOOM_WORKER_MINIO_ENDPOINT": args.worker_minio_endpoint,
        "LOOM_WORKER_MINIO_ACCESS_KEY": minio_access,
        "LOOM_WORKER_MINIO_SECRET_KEY": minio_secret,
        "LOOM_WORKER_ARTIFACTS_BUCKET": identity.artifacts_bucket,
        "LOOM_WORKER_TRAJECTORIES_BUCKET": identity.trajectories_bucket,
        "LOOM_WORKER_POOL_NAME": identity.worker_pool,
        "LOOM_WORKER_CANDIDATE_SHA": snapshot.candidate_sha,
        "LOOM_WORKER_MAX_CONCURRENT": str(
            int(snapshot.actuator_config.get("requested_concurrency") or 1)
        ),
    }
    _atomic_write(
        expected,
        "\n".join(f"{key}={_safe_env_value(value)}" for key, value in values.items()) + "\n",
    )


async def _reconcile_instance(
    admin_url: str,
    snapshot: InstanceSnapshot,
    grants: dict[tuple[str, str], Any],
    *,
    global_execution_witness: GlobalExecutionWitness | None,
) -> None:
    url = fixture_database_url(admin_url, snapshot.database)
    engine = create_async_engine(_sqlalchemy_url(url), pool_pre_ping=True)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            await reconcile_worker_pool_autoscaler_once(
                session,
                environment=snapshot.environment,
                include_external_policies=True,
                external_only=True,
                pool_names=(snapshot.pool_name,),
                capacity_grants=grants,
                deployment_generation=snapshot.deployment_generation,
                global_execution_witness=global_execution_witness,
            )
            await session.commit()
    except Exception as exc:
        raise GlobalDevExternalError("dev instance Slurm reconcile failed") from exc
    finally:
        await engine.dispose()


def _write_report(path: Path, report: dict[str, object]) -> None:
    _atomic_write(path, json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")


async def _main(args: argparse.Namespace) -> dict[str, object]:
    if args.global_budget < 0:
        raise GlobalDevExternalError("global budget must be non-negative")
    pending_budget = (
        args.global_budget if args.global_pending_budget is None else args.global_pending_budget
    )
    if pending_budget < 0:
        raise GlobalDevExternalError("global pending budget must be non-negative")
    management_url = _read_secret_file(args.management_db_url_file, "management DB URL file")
    admin_url = _read_secret_file(args.fixture_admin_db_url_file, "fixture admin DB URL file")
    global_execution_witness = load_global_execution_witness(
        args.global_execution_witness_json,
        manager_public_key_path=args.manager_public_key,
        expected_manager_public_key_sha256=args.expected_manager_public_key_sha256,
    )
    _validate_owner_only_directory(args.worker_env_dir, "worker environment directory")
    _validate_owner_only_directory(args.output_json.parent, "grant report directory")
    rows = await _registry_rows(management_url)
    _prune_worker_env_files(
        args.worker_env_dir,
        {derive_identity(row.name).runtime_environment for row in rows},
    )
    snapshots: list[InstanceSnapshot] = []
    for row in rows:
        try:
            snapshot = await _instance_snapshot(
                admin_url,
                row,
                freshness_sec=args.snapshot_freshness_seconds,
            )
        except GlobalDevExternalError:
            if row.status == "ready":
                raise
            # A newly claimed instance may not have a database/schema yet; a
            # deleting instance may already have removed it. Omitting a
            # non-ready demand cancels/retains its old lease conservatively,
            # while one developer's lifecycle does not blind the ready cohort.
            continue
        if snapshot is not None:
            snapshots.append(snapshot)
    now = datetime.now(UTC)
    demands = tuple(snapshot.demand(now) for snapshot in snapshots)
    pools = {demand.pool_name for demand in demands}
    budgets = BrokerBudgets(
        global_slots=args.global_budget,
        pool_slots={pool: args.global_budget for pool in pools},
        global_pending_slots=pending_budget,
        pool_pending_slots={pool: pending_budget for pool in pools},
    )
    try:
        for pool_id in {slurm_cluster_for_pool(item.pool_name) for item in demands} or {"oldlab"}:
            assert_legacy_scale_up_allowed(
                global_execution_witness,
                expected_authority="global-capacity-manager",
                expected_pool_id=pool_id,
                now=now,
            )
        broker = SharedCapacityBroker(args.state_db)
        report = GlobalDevFleetAutoscaler(
            broker,
            snapshot_freshness_seconds=args.snapshot_freshness_seconds,
            lease_ttl_seconds=args.lease_ttl_seconds,
        ).reconcile(
            demands,
            budgets,
            observations=_lease_observations(broker, snapshots),
            execution_witness=global_execution_witness,
        )
    except GlobalExecutionFenceError:
        # Do not touch the legacy broker or mint worker credentials.  The
        # local policy path receives an empty grant set and its own reciprocal
        # fence, retaining the established drain-safe behavior for workers
        # already running under a prior legacy grant.
        report = {
            "schema_version": 1,
            "authority": "global-dev-fleet-autoscaler",
            "status": "fenced",
            "generated_at": now.isoformat().replace("+00:00", "Z"),
            "demands": [demand.public_dict() for demand in demands],
            "grants": [],
            "aggregate": {"legacy_scale_up_fenced": True},
        }
    _write_report(args.output_json, report)
    grants = capacity_grants_from_report(report) if report["status"] == "ok" else {}
    kubectl = KubectlClient(args.kubectl, context=args.kube_context)
    for snapshot in snapshots:
        # Provisioning/failed/deleting instances publish zero demand and must
        # never mint fresh worker credentials. Existing deleting credentials
        # stay on disk just long enough for the external actuator to drain and
        # are pruned once the registry row becomes deleted.
        if snapshot.status == "ready" and report["grants"]:
            await _ensure_worker_env(args, kubectl, snapshot)
        await _reconcile_instance(
            admin_url,
            snapshot,
            grants,
            global_execution_witness=global_execution_witness,
        )
    return {
        "authority": report["authority"],
        "aggregate": report["aggregate"],
        "instances": len(snapshots),
        "status": report["status"],
    }


def main() -> int:
    try:
        result = asyncio.run(_main(_parser().parse_args()))
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "ok" else 2
    except (BrokerError, GlobalDevAutoscalerError, GlobalDevExternalError):
        sys.stderr.write("error: global development fleet reconcile failed safely\n")
        return 2
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        sys.stderr.write("error: global development fleet reconcile failed safely\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
