"""Validate one isolated GB10 rehearsal policy without local Slurm authority."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from scripts.ops import task_image_builder_autoscaler_external_once as builder_once
from scripts.ops import worker_pool_autoscaler_external_once as external_once

from loom_cli.rollout.external_supervisor_readiness import REHEARSAL_KUBECONFIG
from loom_control_plane.global_execution_fence import (
    GlobalExecutionFenceError,
    assert_legacy_scale_up_allowed,
)

_GB10_CLUSTER = "trt-gb10"
_GB10_CONTROLLER = "gx10-01c7"


class RehearsalPolicyValidationError(external_once.ExternalAutoscalerError):
    """A bounded failure before the isolated read-only policy query."""


def _parser() -> argparse.ArgumentParser:
    parser = external_once._parser()
    parser.prog = "loom-rehearsal-external-supervisor-policy-validate"
    return parser


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    raw = list(sys.argv[1:] if argv is None else argv)
    selector = argparse.ArgumentParser(add_help=False)
    selector.add_argument("--pool-name", action="append")
    selected, _unknown = selector.parse_known_args(raw)
    if not selected.pool_name:
        return _parser().parse_args(raw)
    pool_names = external_once._scoped_pool_names(selected.pool_name)
    parser = (
        builder_once._parser()
        if pool_names == ("task-image-builder-gb10",)
        else _parser()
    )
    return parser.parse_args(raw)


async def _main_async(args: argparse.Namespace) -> dict[str, object]:
    pool_names = external_once._scoped_pool_names(args.pool_name)
    cluster = external_once._validated_slurm_identifier(
        args.expected_slurm_cluster_name,
        "--expected-slurm-cluster-name",
    )
    controller = external_once._validated_slurm_identifier(
        args.expected_slurm_controller_host,
        "--expected-slurm-controller-host",
    )
    if (
        args.validate_only is not True
        or args.namespace == "loom-staging"
        or not args.namespace.startswith("loom-rehearsal-")
        or args.kubeconfig != REHEARSAL_KUBECONFIG
        or pool_names not in {("gb10",), ("task-image-builder-gb10",)}
        or (cluster, controller) != (_GB10_CLUSTER, _GB10_CONTROLLER)
        or args.capacity_grants_json is not None
        or args.deployment_generation is not None
    ):
        raise RehearsalPolicyValidationError(
            "rehearsal external supervisor policy authority is invalid"
        )
    authority = external_once.SlurmPolicyAuthority(
        cluster_name=cluster,
        controller_host=controller,
    )
    if pool_names == ("gb10",):
        policies = await external_once._validate_external_policies_once(
            args,
            authority=authority,
        )
        result: dict[str, object] = {
            "database_reachable": True,
            "expected_slurm_authority": {
                "cluster_name": authority.cluster_name,
                "controller_host": authority.controller_host,
            },
            "mode": "rehearsal-policy-only",
            "pools": policies,
        }
    else:
        try:
            config = builder_once._load_enabled_builder_config(args)
            if (
                config.pool_name != "task-image-builder-gb10"
                or config.slurm_cluster_id != "gb10"
                or config.cpu_arch != "arm64"
                or config.exclusive is not True
                or config.requested_concurrency != 1
            ):
                raise ValueError("GB10 builder policy is not isolated")
            evidence = builder_once._rehearsal_validation_evidence(config)
        except (OSError, RuntimeError, ValueError) as exc:
            raise RehearsalPolicyValidationError(
                "rehearsal task-image builder policy is invalid"
            ) from exc
        result = {
            "expected_slurm_authority": {
                "cluster_name": authority.cluster_name,
                "controller_host": authority.controller_host,
            },
            "mode": "rehearsal-task-image-builder-policy-only",
            "policy": {
                "cpu_arch": config.cpu_arch,
                "exclusive": config.exclusive,
                "pool_name": config.pool_name,
                "requested_concurrency": config.requested_concurrency,
                **evidence,
            },
        }
    try:
        witness = external_once._load_current_global_execution_witness(
            args,
            pool_id="gb10",
        )
        assert_legacy_scale_up_allowed(
            witness,
            expected_authority="global-capacity-manager",
            expected_pool_id="gb10",
            now=datetime.now(UTC),
        )
    except GlobalExecutionFenceError as exc:
        raise RehearsalPolicyValidationError(
            "rehearsal global execution witness is unavailable"
        ) from exc
    return result


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = asyncio.run(_main_async(_parse_args(argv)))
    except (external_once.ExternalAutoscalerError, ValueError):
        sys.stderr.write("error: rehearsal external supervisor policy validation failed safely\n")
        return 1
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        sys.stderr.write("error: rehearsal external supervisor policy validation failed safely\n")
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["RehearsalPolicyValidationError", "main"]
