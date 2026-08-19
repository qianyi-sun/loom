"""Validate one isolated GB10 rehearsal policy without local Slurm authority."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

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
        or pool_names != ("gb10",)
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
    policies = await external_once._validate_external_policies_once(
        args,
        authority=authority,
    )
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
    return {
        "database_reachable": True,
        "expected_slurm_authority": {
            "cluster_name": authority.cluster_name,
            "controller_host": authority.controller_host,
        },
        "mode": "rehearsal-policy-only",
        "pools": policies,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = asyncio.run(_main_async(_parser().parse_args(argv)))
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
