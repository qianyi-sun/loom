from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.ops.task_image_builder_slurm_readback import (
    ReadbackError,
    parse_parsable2_row,
    verify_account,
    verify_association,
    verify_qos,
    verify_reservation,
)

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ops/task_image_builder_slurm_readback.py"

LIVE_EMPTY_TRES = "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00|\n"
SENTINEL_EMPTY_TRES = "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00||\n"
ROOTLESS_TRES = (
    "loom-task-image-builder-rootless-oldlab|DenyOnLimit|0|1|1|"
    "02:00:00|node=1,mem=32G,cpu=8|\n"
)


def _verify_legacy_qos(payload: str) -> dict[str, object] | None:
    return verify_qos(
        payload,
        name="loom-task-image-builder",
        flags=("DenyOnLimit",),
        priority=0,
        max_jobs_per_user=1,
        max_submit_jobs_per_user=1,
        max_wall="04:00:00",
        group_tres={},
        allow_absent=False,
    )


def _verify_rootless_qos(payload: str) -> dict[str, object] | None:
    return verify_qos(
        payload,
        name="loom-task-image-builder-rootless-oldlab",
        flags=("DenyOnLimit",),
        priority=0,
        max_jobs_per_user=1,
        max_submit_jobs_per_user=1,
        max_wall="02:00:00",
        group_tres={"cpu": 8, "memory_mib": 32_768, "nodes": 1},
        allow_absent=False,
    )


def test_live_empty_final_field_forms_have_one_canonical_qos() -> None:
    expected = {
        "name": "loom-task-image-builder",
        "flags": ["DenyOnLimit"],
        "priority": 0,
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "04:00:00",
        "group_tres": {},
    }

    assert _verify_legacy_qos(LIVE_EMPTY_TRES) == expected
    assert _verify_legacy_qos(SENTINEL_EMPTY_TRES) == expected


def test_rootless_tres_order_and_binary_memory_units_are_normalized() -> None:
    assert _verify_rootless_qos(ROOTLESS_TRES) == {
        "name": "loom-task-image-builder-rootless-oldlab",
        "flags": ["DenyOnLimit"],
        "priority": 0,
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "02:00:00",
        "group_tres": {"cpu": 8, "memory_mib": 32_768, "nodes": 1},
    }


@pytest.mark.parametrize(
    "payload",
    [
        LIVE_EMPTY_TRES + LIVE_EMPTY_TRES,
        "loom-task-image-builder|DenyOnLimit|0|1|1|\n",
        "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00|||extra\n",
    ],
)
def test_parsable2_row_rejects_wrong_row_or_field_count(payload: str) -> None:
    with pytest.raises(ReadbackError):
        parse_parsable2_row(
            payload,
            (
                "name",
                "flags",
                "priority",
                "max_jobs_per_user",
                "max_submit_jobs_per_user",
                "max_wall",
                "group_tres",
            ),
            allow_absent=False,
        )


@pytest.mark.parametrize(
    "payload",
    [
        ROOTLESS_TRES.replace("node=1,", "node=1,node=1,"),
        ROOTLESS_TRES.replace("node=1,", "node=1,gres/gpu=1,"),
        ROOTLESS_TRES.replace("mem=32G", "mem=32000M"),
        ROOTLESS_TRES.replace("DenyOnLimit", "DenyOnLimit,NoDecay"),
        ROOTLESS_TRES.replace("02:00:00", "08:00:00"),
    ],
)
def test_qos_semantic_drift_is_rejected(payload: str) -> None:
    with pytest.raises(ReadbackError):
        _verify_rootless_qos(payload)


def test_absent_row_is_allowed_only_when_requested() -> None:
    assert (
        verify_qos(
            "",
            name="loom-task-image-builder-rootless-oldlab",
            flags=("DenyOnLimit",),
            priority=0,
            max_jobs_per_user=1,
            max_submit_jobs_per_user=1,
            max_wall="02:00:00",
            group_tres={"cpu": 8, "memory_mib": 32_768, "nodes": 1},
            allow_absent=True,
        )
        is None
    )
    with pytest.raises(ReadbackError):
        _verify_rootless_qos("")


def test_account_is_exact_and_may_be_explicitly_absent() -> None:
    assert verify_account(
        "loom-task-builder|\n",
        name="loom-task-builder",
        allow_absent=False,
    ) == {"name": "loom-task-builder"}
    assert verify_account("", name="loom-task-builder", allow_absent=True) is None
    with pytest.raises(ReadbackError):
        verify_account("foreign|\n", name="loom-task-builder", allow_absent=False)


def test_association_qos_is_order_independent_but_exact() -> None:
    live = (
        "trt-oldlab|loom-staging|loom-rollout|"
        "loom-task-image-builder,normal|normal\n"
    )
    expected = {
        "cluster": "trt-oldlab",
        "account": "loom-staging",
        "user": "loom-rollout",
        "qos": ["loom-task-image-builder", "normal"],
        "default_qos": "normal",
    }

    assert verify_association(
        live,
        cluster="trt-oldlab",
        account="loom-staging",
        user="loom-rollout",
        partition=None,
        qos=("normal", "loom-task-image-builder"),
        default_qos="normal",
        allow_absent=False,
    ) == expected
    with pytest.raises(ReadbackError):
        verify_association(
            live.replace("|normal\n", ",debug|normal\n"),
            cluster="trt-oldlab",
            account="loom-staging",
            user="loom-rollout",
            partition=None,
            qos=("normal", "loom-task-image-builder"),
            default_qos="normal",
            allow_absent=False,
        )


def test_reservation_tokens_are_order_independent_and_exact() -> None:
    live = (
        "State=ACTIVE Flags=SPEC_NODES,IGNORE_JOBS Accounts=loom-staging "
        "Users=loom-rollout PartitionName=all NodeCnt=1 "
        "Nodes=trt-eai-oldlab-6 ReservationName=loom-task-image-builder\n"
    )

    assert verify_reservation(
        live,
        name="loom-task-image-builder",
        node="trt-eai-oldlab-6",
        node_count=1,
        partition="all",
        users=("loom-rollout",),
        accounts=("loom-staging",),
        state="ACTIVE",
        flags=("IGNORE_JOBS", "SPEC_NODES"),
    ) == {
        "name": "loom-task-image-builder",
        "node": "trt-eai-oldlab-6",
        "node_count": 1,
        "partition": "all",
        "users": ["loom-rollout"],
        "accounts": ["loom-staging"],
        "state": "ACTIVE",
        "flags": ["IGNORE_JOBS", "SPEC_NODES"],
    }

    for drifted in (
        live.replace("trt-eai-oldlab-6", "trt-eai-oldlab-5"),
        live.replace("State=ACTIVE", "State=INACTIVE"),
        live.replace("Users=loom-rollout", "Users=loom-rollout,foreign"),
        live.replace("SPEC_NODES,IGNORE_JOBS", "IGNORE_JOBS"),
    ):
        with pytest.raises(ReadbackError):
            verify_reservation(
                drifted,
                name="loom-task-image-builder",
                node="trt-eai-oldlab-6",
                node_count=1,
                partition="all",
                users=("loom-rollout",),
                accounts=("loom-staging",),
                state="ACTIVE",
                flags=("IGNORE_JOBS", "SPEC_NODES"),
            )


def test_reservation_ignores_unique_unrelated_metadata_values() -> None:
    live = (
        "ReservationName=loom-task-image-builder Nodes=trt-eai-oldlab-6 "
        "NodeCnt=1 PartitionName=all Users=loom-rollout Accounts=loom-staging "
        "State=ACTIVE Flags=IGNORE_JOBS,SPEC_NODES Comment='owner=operations'\n"
    )

    assert verify_reservation(
        live,
        name="loom-task-image-builder",
        node="trt-eai-oldlab-6",
        node_count=1,
        partition="all",
        users=("loom-rollout",),
        accounts=("loom-staging",),
        state="ACTIVE",
        flags=("IGNORE_JOBS", "SPEC_NODES"),
    )["name"] == "loom-task-image-builder"


def test_cli_prints_canonical_json_and_sanitizes_invalid_input() -> None:
    arguments = [
        sys.executable,
        str(SCRIPT),
        "qos",
        "--name",
        "loom-task-image-builder",
        "--flags",
        "DenyOnLimit",
        "--priority",
        "0",
        "--max-jobs",
        "1",
        "--max-submit",
        "1",
        "--max-wall",
        "04:00:00",
        "--group-tres",
        "",
    ]
    accepted = subprocess.run(
        arguments,
        input=LIVE_EMPTY_TRES,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert accepted.returncode == 0, accepted.stderr
    assert json.loads(accepted.stdout) == _verify_legacy_qos(LIVE_EMPTY_TRES)

    secret_like_raw = "private-row-value|bad\n"
    rejected = subprocess.run(
        arguments,
        input=secret_like_raw,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode == 1
    assert rejected.stderr == "error: Slurm readback is invalid\n"
    assert "private-row-value" not in rejected.stderr
