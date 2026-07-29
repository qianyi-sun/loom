from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
import threading
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.ops import developer_sandbox_slurm_policy as policy

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy/slurm/developer-sandboxes/oldlab.toml"
GB10_PROFILE = REPO_ROOT / "deploy/slurm/developer-sandboxes/gb10.toml"
TEST_GENERATION_ID = "0" * 64
SANDBOX = "qianyi"
TRANSACTION = {
    "transaction_id": "1" * 64,
    "generation": 1,
    "convergence_id": "2" * 64,
    "payload_sha256": "3" * 64,
}


def _generation_window() -> tuple[str, str]:
    now = datetime.now(UTC)
    return (
        (now - timedelta(minutes=1)).isoformat(),
        (now + timedelta(minutes=10)).isoformat(),
    )


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "root"
    (root / "etc/slurm").mkdir(parents=True)
    (root / "etc/docker").mkdir(parents=True)
    (root / "etc/slurm/slurm.conf").write_text(
        "\n".join(
            (
                "ClusterName=trt-oldlab",
                "ProctrackType=proctrack/linuxproc",
                "TaskPlugin=task/none",
                "PriorityType=priority/basic",
                "PriorityWeightFairshare=0",
                "",
            ),
        ),
        encoding="utf-8",
    )
    (root / "etc/slurm/cgroup.conf").write_text(
        "CgroupPlugin=autodetect\nConstrainCores=yes\nConstrainRAMSpace=no\n",
        encoding="utf-8",
    )
    (root / "etc/docker/daemon.json").write_text(
        json.dumps(
            {
                "features": {"containerd-snapshotter": False},
                "exec-opts": ["native.cgroupdriver=systemd"],
            },
        ),
        encoding="utf-8",
    )
    return root


def test_live_apply_revalidates_cluster_and_controller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    monkeypatch.setattr(policy, "verify_source_candidate", lambda _sha: None)
    monkeypatch.setattr(policy.os, "geteuid", lambda: 0)
    monkeypatch.setattr(policy, "_canonical_host", lambda: "trt-eai-oldlab-2")
    responses = {
        ("scontrol", "show", "config"): (
            "ClusterName = trt-oldlab\nSlurmctldHost = TRT-EAI-OLDLAB-1\n"
        ),
    }
    monkeypatch.setattr(policy, "_run", lambda argv: responses[tuple(argv)])

    assert policy._validate_live_apply(
        Path("/"),
        loaded,
        candidate_sha="a" * 40,
        restart=True,
        apply_accounting=False,
    ) == ("trt-eai-oldlab-2", "trt-EAI-OLDLAB-2")

    responses[("scontrol", "show", "config")] = (
        "ClusterName = wrong-cluster\nSlurmctldHost = TRT-EAI-OLDLAB-1\n"
    )
    with pytest.raises(policy.PolicyError, match="cluster identity"):
        policy._validate_live_apply(
            Path("/"),
            loaded,
            candidate_sha="a" * 40,
            restart=True,
            apply_accounting=False,
        )


def _mock_slurm_admission(
    monkeypatch: pytest.MonkeyPatch,
    *,
    initial_state: str,
    initial_reason: str,
) -> tuple[dict[str, str], list[tuple[str, ...]]]:
    admission = {"state": initial_state, "reason": initial_reason}
    commands: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        policy,
        "_slurm_node_admission",
        lambda _node: (admission["state"], admission["reason"]),
    )

    def run(argv: tuple[str, ...] | list[str]) -> str:
        command = tuple(argv)
        commands.append(command)
        if command[:2] != ("scontrol", "update"):
            raise AssertionError(command)
        state = next(item.split("=", 1)[1] for item in command if item.startswith("State="))
        if state == "DRAIN":
            admission["state"] = "IDLE+DRAIN"
            admission["reason"] = next(
                item.split("=", 1)[1] for item in command if item.startswith("Reason=")
            )
        elif state == "RESUME":
            admission["state"] = "IDLE"
            admission["reason"] = "None"
        else:
            raise AssertionError(command)
        return ""

    monkeypatch.setattr(policy, "_run", run)
    monkeypatch.setattr(policy, "_canonical_host", lambda: "gx10-0faf")
    return admission, commands


def test_slurm_node_admission_parses_exact_oneline_state_and_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        policy,
        "_run",
        lambda argv: (
            "NodeName=trt-gb10-7 Arch=aarch64 State=ALLOCATED+DRAIN "
            "CfgTRES=cpu=20 Reason=loom-sandbox-policy:aaaaaaaaaaaa:bbbbbbbbbbbbbbbb "
            "[root@2026-07-29T12:00:00]\n"
            if tuple(argv) == ("scontrol", "show", "node", "trt-gb10-7", "-o")
            else pytest.fail(str(tuple(argv)))
        ),
    )

    assert policy._slurm_node_admission("trt-gb10-7") == (
        "ALLOCATED+DRAIN",
        "loom-sandbox-policy:aaaaaaaaaaaa:bbbbbbbbbbbbbbbb [root@2026-07-29T12:00:00]",
    )


def test_slurm_node_admission_rejects_wrong_node_or_ambiguous_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = iter(
        (
            "NodeName=trt-gb10-8 State=IDLE Reason=None\n",
            "NodeName=trt-gb10-7 State=IDLE Reason=None\n"
            "NodeName=trt-gb10-7 State=IDLE Reason=None\n",
        ),
    )
    monkeypatch.setattr(policy, "_run", lambda _argv: next(outputs))

    with pytest.raises(policy.PolicyError, match="admission readback"):
        policy._slurm_node_admission("trt-gb10-7")
    with pytest.raises(policy.PolicyError, match="admission readback"):
        policy._slurm_node_admission("trt-gb10-7")


def test_restart_drain_is_candidate_owned_reused_after_crash_and_released(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    root = tmp_path / "root"
    admission, commands = _mock_slurm_admission(
        monkeypatch,
        initial_state="IDLE",
        initial_reason="None",
    )

    first = policy._acquire_restart_drain(
        root,
        loaded,
        slurm_node="trt-gb10-7",
        candidate_sha="a" * 40,
    )
    replayed = policy._acquire_restart_drain(
        root,
        loaded,
        slurm_node="trt-gb10-7",
        candidate_sha="a" * 40,
    )

    assert replayed == first
    assert first["owned"] is True
    assert first["prior_state"] == "IDLE"
    assert first["prior_reason"] == "None"
    assert admission["state"] == "IDLE+DRAIN"
    assert sum("State=DRAIN" in command for command in commands) == 1

    policy._release_restart_drain(root, loaded, replayed)

    assert admission == {"state": "IDLE", "reason": "None"}
    assert commands[-1] == (
        "scontrol",
        "update",
        "NodeName=trt-gb10-7",
        "State=RESUME",
    )
    persisted = policy._load_drain_journal(
        root,
        loaded,
        slurm_node="trt-gb10-7",
    )
    assert persisted is not None
    assert persisted["phase"] == "released"


@pytest.mark.parametrize(
    ("state", "reason"),
    (
        ("IDLE+DRAIN", "maintenance"),
        ("DOWN", "hardware"),
    ),
)
def test_restart_drain_defers_preexisting_foreign_drain_or_down(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    reason: str,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    root = tmp_path / "root"
    admission, commands = _mock_slurm_admission(
        monkeypatch,
        initial_state=state,
        initial_reason=reason,
    )

    with pytest.raises(policy.PolicyError, match="foreign DRAIN/DOWN"):
        policy._acquire_restart_drain(
            root,
            loaded,
            slurm_node="trt-gb10-7",
            candidate_sha="a" * 40,
        )

    assert admission == {"state": state, "reason": reason}
    assert commands == []
    assert not policy._drain_journal_path(root, loaded).exists()


def test_restart_drain_never_resumes_changed_or_down_owned_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    root = tmp_path / "root"
    admission, commands = _mock_slurm_admission(
        monkeypatch,
        initial_state="IDLE",
        initial_reason="None",
    )
    drain = policy._acquire_restart_drain(
        root,
        loaded,
        slurm_node="trt-gb10-7",
        candidate_sha="a" * 40,
    )
    admission["reason"] = "foreign-maintenance"

    with pytest.raises(policy.PolicyError, match="reason changed"):
        policy._release_restart_drain(root, loaded, drain)

    assert not any("State=RESUME" in command for command in commands)
    persisted = policy._load_drain_journal(
        root,
        loaded,
        slurm_node="trt-gb10-7",
    )
    assert persisted is not None
    assert persisted["phase"] == "release_failed"


def test_restart_quiescence_waits_for_slurm_docker_and_gpu_without_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    root = tmp_path / "root"
    _admission, commands = _mock_slurm_admission(
        monkeypatch,
        initial_state="IDLE",
        initial_reason="None",
    )
    drain = policy._acquire_restart_drain(
        root,
        loaded,
        slurm_node="trt-gb10-7",
        candidate_sha="a" * 40,
    )
    observations = iter(
        (
            {"slurm_jobs": True, "docker_containers": True, "gpu_processes": True},
            {"slurm_jobs": False, "docker_containers": False, "gpu_processes": False},
        ),
    )
    monkeypatch.setattr(policy, "_restart_activity", lambda *_args: next(observations))
    monkeypatch.setattr(policy.time, "sleep", lambda _seconds: None)

    policy._wait_for_restart_quiescence(root, loaded, drain)

    assert drain["phase"] == "quiesced"
    assert not any(command and command[0] in {"scancel", "docker"} for command in commands)


def test_restart_quiescence_timeout_keeps_owned_drain_for_safe_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    root = tmp_path / "root"
    admission, commands = _mock_slurm_admission(
        monkeypatch,
        initial_state="ALLOCATED",
        initial_reason="None",
    )
    drain = policy._acquire_restart_drain(
        root,
        loaded,
        slurm_node="trt-gb10-7",
        candidate_sha="a" * 40,
    )
    monkeypatch.setattr(
        policy,
        "_restart_activity",
        lambda *_args: {
            "slurm_jobs": True,
            "docker_containers": False,
            "gpu_processes": True,
        },
    )
    monkeypatch.setattr(policy, "_RESTART_QUIESCE_TIMEOUT_SECONDS", 0.0)

    with pytest.raises(policy.PolicyError, match="slurm_jobs, gpu_processes"):
        policy._wait_for_restart_quiescence(root, loaded, drain)

    assert admission["state"] == "IDLE+DRAIN"
    assert not any("State=RESUME" in command for command in commands)
    persisted = policy._load_drain_journal(
        root,
        loaded,
        slurm_node="trt-gb10-7",
    )
    assert persisted is not None
    assert persisted["phase"] == "drained"


def test_restart_activity_probes_gpu_only_for_gpu_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gb10 = policy.load_profile(GB10_PROFILE)
    oldlab = policy.load_profile(PROFILE)
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...] | list[str]) -> str:
        command = tuple(argv)
        calls.append(command)
        return {
            ("squeue", "-h", "-w", "trt-gb10-7"): "123\n",
            ("docker", "ps", "-q"): "container\n",
            (
                "nvidia-smi",
                "--query-compute-apps=pid",
                "--format=csv,noheader,nounits",
            ): "456\n",
            ("squeue", "-h", "-w", "oldlab-2"): "",
        }[command]

    monkeypatch.setattr(policy, "_run", run)

    assert policy._restart_activity(gb10, "trt-gb10-7") == {
        "slurm_jobs": True,
        "docker_containers": True,
        "gpu_processes": True,
    }
    assert policy._restart_activity(oldlab, "oldlab-2") == {
        "slurm_jobs": False,
        "docker_containers": True,
        "gpu_processes": False,
    }
    assert sum(command[0] == "nvidia-smi" for command in calls) == 1


def test_persistent_recovery_replays_exact_candidate_journal_and_releases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    offline_root = tmp_path / "offline"
    _admission, _commands = _mock_slurm_admission(
        monkeypatch,
        initial_state="IDLE",
        initial_reason="None",
    )
    drain = policy._acquire_restart_drain(
        offline_root,
        loaded,
        slurm_node="trt-gb10-7",
        candidate_sha="a" * 40,
        operation="apply",
        apply_accounting=False,
    )
    candidate_policy = tmp_path / "candidate/developer_sandbox_slurm_policy.py"
    candidate_profile = tmp_path / "candidate/gb10.toml"
    monkeypatch.setattr(policy.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        policy,
        "_load_journal",
        lambda path: drain if path.name == "trt-gb10.json" else None,
    )
    monkeypatch.setattr(
        policy,
        "_recovery_candidate",
        lambda _root, payload: (
            loaded,
            candidate_policy,
            candidate_profile,
        ),
    )
    replayed: list[dict[str, object]] = []
    released = dict(drain)

    def recover(
        policy_path: Path,
        profile_path: Path,
        payload: dict[str, object],
    ) -> None:
        assert policy_path == candidate_policy
        assert profile_path == candidate_profile
        replayed.append(dict(payload))
        released["phase"] = "released"

    monkeypatch.setattr(policy, "_run_recovery_candidate", recover)
    loads = iter((drain, released))
    monkeypatch.setattr(policy, "_load_drain_journal", lambda *_args, **_kwargs: next(loads))

    report = policy.recover_pending_drains()

    assert [(item["operation"], item["candidate_sha"]) for item in replayed] == [
        ("apply", "a" * 40),
    ]
    assert report["recovered"] == [
        {
            "cluster": "trt-gb10",
            "candidate_sha": "a" * 40,
            "operation": "apply",
            "phase": "released",
        },
    ]


def test_recovery_candidate_invocation_has_no_cancellation_or_override_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> object:
        calls.append((argv, kwargs))
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr(policy.subprocess, "run", run)
    bindings = policy._offline_candidate_bindings(
        policy.load_profile(GB10_PROFILE),
        "a" * 40,
    )
    policy._run_recovery_candidate(
        Path("/candidate/scripts/ops/developer_sandbox_slurm_policy.py"),
        Path("/candidate/deploy/slurm/developer-sandboxes/gb10.toml"),
        {
            "operation": "apply",
            "candidate_sha": "a" * 40,
            "candidate_bindings": bindings,
            "transaction_id": "1" * 64,
            "candidate_set_generation": 1,
            "candidate_set_convergence_id": "2" * 64,
            "candidate_set_payload_sha256": "3" * 64,
            "apply_accounting": True,
        },
    )

    argv, kwargs = calls[0]
    assert argv == [
        "/usr/bin/python3",
        "-I",
        "-B",
        "/candidate/scripts/ops/developer_sandbox_slurm_policy.py",
        "apply",
        "--profile",
        "/candidate/deploy/slurm/developer-sandboxes/gb10.toml",
        "--candidate-sha",
        "a" * 40,
        "--candidate-bindings-json",
        json.dumps(bindings, sort_keys=True, separators=(",", ":")),
        "--transaction-id",
        "1" * 64,
        "--candidate-set-generation",
        "1",
        "--candidate-set-convergence-id",
        "2" * 64,
        "--candidate-set-payload-sha256",
        "3" * 64,
        "--execute",
        "--restart",
        "--apply-accounting",
    ]
    assert kwargs["stdin"] is policy.subprocess.DEVNULL
    assert kwargs["stdout"] is policy.subprocess.DEVNULL
    assert all(token not in argv for token in ("scancel", "stop", "kill", "--root"))


def test_live_apply_busy_timeout_precedes_snapshot_and_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    drain = {"phase": "drained"}
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        policy,
        "desired_files",
        lambda *_args, **_kwargs: {Path("/etc/docker/daemon.json"): "{}\n"},
    )
    monkeypatch.setattr(policy, "_run", lambda _argv: "")
    monkeypatch.setattr(policy, "_acquire_restart_drain", lambda *_args, **_kwargs: drain)
    monkeypatch.setattr(
        policy,
        "_wait_for_restart_quiescence",
        lambda *_args: (_ for _ in ()).throw(policy.PolicyError("node remains busy")),
    )
    monkeypatch.setattr(
        policy,
        "_snapshot",
        lambda *_args: pytest.fail("busy retry must not create a snapshot"),
    )
    monkeypatch.setattr(
        policy,
        "_write_journal",
        lambda *_args: pytest.fail("busy retry must not create a transaction journal"),
    )
    monkeypatch.setattr(
        policy,
        "_release_restart_drain",
        lambda *_args: pytest.fail("busy timeout must retain its owned drain for timer retry"),
    )

    with pytest.raises(policy.PolicyError, match="node remains busy"):
        policy.apply(
            Path("/"),
            loaded,
            restart=True,
            apply_accounting=False,
            candidate_sha="a" * 40,
            candidate_bindings=policy._offline_candidate_bindings(loaded, "a" * 40),
            **TRANSACTION,
        )


def test_live_rollback_busy_timeout_precedes_snapshot_and_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    drain = {"phase": "drained"}
    target = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120000.000000Z",
    )
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        policy,
        "_load_policy_journal",
        lambda *_args, **_kwargs: {
            "phase": "committed",
            "operation": "apply",
            "snapshot": str(target),
                "accounting_snapshot": None,
                "candidate_set_generation": 1,
                "candidate_set_convergence_id": "2" * 64,
                "candidate_set_payload_sha256": "3" * 64,
        },
    )
    monkeypatch.setattr(policy, "_validate_snapshot_path", lambda *_args: target)
    monkeypatch.setattr(policy, "desired_files", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(policy, "_acquire_restart_drain", lambda *_args, **_kwargs: drain)
    monkeypatch.setattr(
        policy,
        "_wait_for_restart_quiescence",
        lambda *_args: (_ for _ in ()).throw(policy.PolicyError("node remains busy")),
    )
    monkeypatch.setattr(
        policy,
        "_snapshot",
        lambda *_args: pytest.fail("busy retry must not create a recovery snapshot"),
    )
    monkeypatch.setattr(
        policy,
        "_write_journal",
        lambda *_args: pytest.fail("busy retry must not replace the committed transaction"),
    )
    monkeypatch.setattr(
        policy,
        "_release_restart_drain",
        lambda *_args: pytest.fail("busy timeout must retain its owned drain for timer retry"),
    )

    with pytest.raises(policy.PolicyError, match="node remains busy"):
        policy.rollback(
            Path("/"),
            loaded,
            candidate_sha="a" * 40,
            candidate_bindings=policy._offline_candidate_bindings(loaded, "a" * 40),
            **TRANSACTION,
        )


def test_pretransaction_snapshot_failure_releases_owned_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    drain = {"phase": "quiesced"}
    released: list[dict[str, str]] = []
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        policy,
        "desired_files",
        lambda *_args, **_kwargs: {Path("/etc/docker/daemon.json"): "{}\n"},
    )
    monkeypatch.setattr(policy, "_run", lambda _argv: "")
    monkeypatch.setattr(policy, "_acquire_restart_drain", lambda *_args, **_kwargs: drain)
    monkeypatch.setattr(policy, "_wait_for_restart_quiescence", lambda *_args: None)
    monkeypatch.setattr(
        policy,
        "_snapshot",
        lambda *_args: (_ for _ in ()).throw(policy.PolicyError("snapshot failed")),
    )
    monkeypatch.setattr(
        policy,
        "_release_restart_drain",
        lambda _root, _profile, payload: released.append(payload),
    )
    monkeypatch.setattr(
        policy,
        "_write_journal",
        lambda *_args: pytest.fail("snapshot failure must not create a transaction journal"),
    )

    with pytest.raises(policy.PolicyError, match="snapshot failed"):
        policy.apply(
            Path("/"),
            loaded,
            restart=True,
            apply_accounting=False,
            candidate_sha="a" * 40,
            candidate_bindings=policy._offline_candidate_bindings(loaded, "a" * 40),
            **TRANSACTION,
        )
    assert released == [drain]


def test_committed_apply_transaction_replay_is_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "a" * 40)
    snapshot = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120000.000000Z",
    )
    committed = {
        "phase": "committed",
        "operation": "apply",
        "candidate_sha": "a" * 40,
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "transaction_id": TRANSACTION["transaction_id"],
        "candidate_set_generation": TRANSACTION["generation"],
        "candidate_set_convergence_id": TRANSACTION["convergence_id"],
        "candidate_set_payload_sha256": TRANSACTION["payload_sha256"],
        "snapshot": str(snapshot),
        "restart": True,
        "apply_accounting": False,
    }
    released: list[dict[str, object]] = []
    events: list[str] = []
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: committed)
    monkeypatch.setattr(policy, "desired_files", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(policy, "_validate_snapshot_path", lambda *_args: snapshot)
    monkeypatch.setattr(
        policy,
        "_live_readback_unlocked",
        lambda *_args, **_kwargs: events.append("readback") or {"converged": True},
    )
    monkeypatch.setattr(
        policy,
        "_release_committed_transaction_drain",
        lambda _root, _profile, **kwargs: (
            events.append("release"),
            released.append(dict(kwargs["journal"])),
        ),
    )
    monkeypatch.setattr(
        policy,
        "plan",
        lambda *_args, **_kwargs: {
            "schema_version": 1,
            "cluster": loaded.cluster,
            "file_plan": {"converged": True},
        },
    )
    monkeypatch.setattr(
        policy,
        "_acquire_restart_drain",
        lambda *_args, **_kwargs: pytest.fail("replay must not drain"),
    )
    monkeypatch.setattr(
        policy,
        "_snapshot",
        lambda *_args, **_kwargs: pytest.fail("replay must not snapshot"),
    )

    result = policy.apply(
        Path("/"),
        loaded,
        restart=True,
        apply_accounting=False,
        candidate_sha="a" * 40,
        candidate_bindings=bindings,
        **TRANSACTION,
    )

    assert result["replayed"] is True
    assert result["snapshot"] == str(snapshot)
    assert released == [committed]
    assert events == ["readback", "release"]

    released.clear()
    monkeypatch.setattr(
        policy,
        "_live_readback_unlocked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            policy.PolicyError("simulated replay drift"),
        ),
    )
    with pytest.raises(policy.PolicyError, match="simulated replay drift"):
        policy.apply(
            Path("/"),
            loaded,
            restart=True,
            apply_accounting=False,
            candidate_sha="a" * 40,
            candidate_bindings=bindings,
            **TRANSACTION,
        )
    assert released == []


def test_same_generation_different_transaction_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "a" * 40)
    committed = {
        "phase": "committed",
        "operation": "apply",
        "candidate_sha": "a" * 40,
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "transaction_id": "9" * 64,
        "candidate_set_generation": 1,
        "candidate_set_convergence_id": "8" * 64,
        "candidate_set_payload_sha256": "7" * 64,
    }
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: committed)
    monkeypatch.setattr(policy, "desired_files", lambda *_args, **_kwargs: {})

    with pytest.raises(policy.PolicyError, match="generation regressed or skipped"):
        policy.apply(
            Path("/"),
            loaded,
            restart=True,
            apply_accounting=False,
            candidate_sha="a" * 40,
            candidate_bindings=bindings,
            **TRANSACTION,
        )


def test_timer_completed_rollback_transaction_replay_is_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "a" * 40)
    recovery = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120001.000000Z",
    )
    restored = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120000.000000Z",
    )
    committed = {
        "phase": "committed",
        "operation": "rollback",
        "candidate_sha": "a" * 40,
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "transaction_id": TRANSACTION["transaction_id"],
        "candidate_set_generation": TRANSACTION["generation"],
        "candidate_set_convergence_id": TRANSACTION["convergence_id"],
        "candidate_set_payload_sha256": TRANSACTION["payload_sha256"],
        "snapshot": str(recovery),
        "rollback_target": str(restored),
        "restart": True,
        "apply_accounting": True,
    }
    released: list[dict[str, object]] = []
    accounting: list[Path] = []
    events: list[str] = []
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: committed)
    monkeypatch.setattr(
        policy,
        "_validate_snapshot_path",
        lambda _root, path: path,
    )
    monkeypatch.setattr(
        policy,
        "_snapshot_readback",
        lambda _root, path: (
            events.append("readback") or {"converged": True, "snapshot": str(path)}
        ),
    )
    monkeypatch.setattr(
        policy,
        "_release_committed_transaction_drain",
        lambda _root, _profile, **kwargs: (
            events.append("release"),
            released.append(dict(kwargs["journal"])),
        ),
    )
    monkeypatch.setattr(
        policy,
        "_validate_accounting_snapshot_path",
        lambda _root, _snapshot, path: path,
    )
    monkeypatch.setattr(
        policy,
        "_accounting_snapshot_matches",
        lambda _profile, path: (events.append("accounting"), accounting.append(path)),
    )
    monkeypatch.setattr(
        policy,
        "_acquire_restart_drain",
        lambda *_args, **_kwargs: pytest.fail("rollback replay must not drain"),
    )
    monkeypatch.setattr(
        policy,
        "_restore_snapshot",
        lambda *_args, **_kwargs: pytest.fail("rollback replay must not restore twice"),
    )

    result = policy.rollback(
        Path("/"),
        loaded,
        candidate_sha="a" * 40,
        candidate_bindings=bindings,
        **TRANSACTION,
    )

    assert result["replayed"] is True
    assert result["restored_snapshot"] == str(restored)
    assert result["recovery_snapshot"] == str(recovery)
    assert released == [committed]
    assert accounting == [restored / "accounting-cas.json"]
    assert events == ["readback", "accounting", "release"]

    released.clear()
    monkeypatch.setattr(
        policy,
        "_accounting_snapshot_matches",
        lambda *_args: (_ for _ in ()).throw(
            policy.PolicyError("simulated accounting drift"),
        ),
    )
    with pytest.raises(policy.PolicyError, match="simulated accounting drift"):
        policy.rollback(
            Path("/"),
            loaded,
            candidate_sha="a" * 40,
            candidate_bindings=bindings,
            **TRANSACTION,
        )
    assert released == []


def test_committed_replay_releases_only_its_exact_pending_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "a" * 40)
    journal: dict[str, object] = {
        "restart": True,
        "operation": "apply",
        "apply_accounting": False,
        "candidate_sha": "a" * 40,
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "transaction_id": TRANSACTION["transaction_id"],
        "candidate_set_generation": TRANSACTION["generation"],
        "candidate_set_convergence_id": TRANSACTION["convergence_id"],
        "candidate_set_payload_sha256": TRANSACTION["payload_sha256"],
    }
    drain = {**journal, "phase": "transacting"}
    released: list[dict[str, object]] = []
    monkeypatch.setattr(
        policy,
        "_load_drain_journal",
        lambda *_args, **_kwargs: drain,
    )

    def release(
        _root: Path,
        _profile: policy.Profile,
        payload: dict[str, object],
    ) -> None:
        released.append(dict(payload))
        payload["phase"] = "released"

    monkeypatch.setattr(policy, "_release_restart_drain", release)

    policy._release_committed_transaction_drain(
        Path("/"),
        loaded,
        slurm_node="trt-gb10-7",
        journal=journal,
    )

    assert released == [{**journal, "phase": "transacting"}]

    released.clear()
    journal["operation"] = "rollback"
    journal["apply_accounting"] = True
    drain.update(
        {
            "operation": "rollback",
            "apply_accounting": False,
            "transaction_id": TRANSACTION["transaction_id"],
            "phase": "transacting",
        },
    )
    policy._release_committed_transaction_drain(
        Path("/"),
        loaded,
        slurm_node="trt-gb10-7",
        journal=journal,
    )
    assert released == [{**drain, "phase": "transacting"}]

    drain["phase"] = "transacting"
    drain["transaction_id"] = "9" * 64
    with pytest.raises(policy.PolicyError, match="drain identity drifted"):
        policy._release_committed_transaction_drain(
            Path("/"),
            loaded,
            slurm_node="trt-gb10-7",
            journal=journal,
        )


def test_interrupted_rollback_recovery_can_retry_same_exact_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "a" * 40)
    target = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120000.000000Z",
    )
    recovered_snapshot = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120001.000000Z",
    )
    retry_snapshot = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120002.000000Z",
    )
    recovered = {
        "phase": "rolled_back",
        "operation": "rollback",
        "candidate_sha": "a" * 40,
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "transaction_id": TRANSACTION["transaction_id"],
        "candidate_set_generation": TRANSACTION["generation"],
        "candidate_set_convergence_id": TRANSACTION["convergence_id"],
        "candidate_set_payload_sha256": TRANSACTION["payload_sha256"],
        "snapshot": str(recovered_snapshot),
        "rollback_target": str(target),
        "restart": True,
        "apply_accounting": False,
        "accounting_snapshot": None,
    }
    drain = {"phase": "quiesced"}
    restored: list[Path] = []
    released: list[dict[str, object]] = []
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *_args, **_kwargs: ("gx10-0faf", "trt-gb10-7"),
    )
    monkeypatch.setattr(policy, "_domain_lock", lambda *_args: nullcontext())
    monkeypatch.setattr(policy, "_recover_orphan", lambda *_args, **_kwargs: recovered)
    monkeypatch.setattr(
        policy,
        "_load_policy_journal",
        lambda *_args, **_kwargs: pytest.fail("retry must use the recovered rollback target"),
    )
    monkeypatch.setattr(policy, "_validate_snapshot_path", lambda _root, path: path)
    monkeypatch.setattr(policy, "desired_files", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(policy, "_acquire_restart_drain", lambda *_args, **_kwargs: drain)
    monkeypatch.setattr(policy, "_wait_for_restart_quiescence", lambda *_args: None)
    monkeypatch.setattr(policy, "_snapshot", lambda *_args, **_kwargs: retry_snapshot)
    monkeypatch.setattr(policy, "_mark_restart_drain_transacting", lambda *_args: None)
    monkeypatch.setattr(policy, "_write_journal", lambda *_args: None)
    monkeypatch.setattr(
        policy,
        "_restore_snapshot",
        lambda _root, path: restored.append(path),
    )
    monkeypatch.setattr(policy, "_restore_services", lambda *_args: None)
    monkeypatch.setattr(
        policy,
        "_snapshot_readback",
        lambda _root, path: {"converged": True, "snapshot": str(path)},
    )
    monkeypatch.setattr(
        policy,
        "_advance_journal",
        lambda _path, payload, phase: payload.__setitem__("phase", phase),
    )
    monkeypatch.setattr(
        policy,
        "_release_restart_drain",
        lambda _root, _profile, payload: released.append(dict(payload)),
    )

    result = policy.rollback(
        Path("/"),
        loaded,
        candidate_sha="a" * 40,
        candidate_bindings=bindings,
        **TRANSACTION,
    )

    assert restored == [target]
    assert released == [drain]
    assert result["phase"] == "committed"
    assert result["restored_snapshot"] == str(target)
    assert result["recovery_snapshot"] == str(retry_snapshot)


def test_controller_rollback_orphan_recovery_keeps_drain_accounting_flag_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "a" * 40)
    recovery = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120001.000000Z",
    )
    target = Path(
        "/var/lib/loom-developer-sandbox-slurm-policy/"
        "snapshots/20260729T120000.000000Z",
    )
    journal = {
        "phase": "files_written",
        "operation": "rollback",
        "candidate_sha": "a" * 40,
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "transaction_id": TRANSACTION["transaction_id"],
        "candidate_set_generation": TRANSACTION["generation"],
        "candidate_set_convergence_id": TRANSACTION["convergence_id"],
        "candidate_set_payload_sha256": TRANSACTION["payload_sha256"],
        "snapshot": str(recovery),
        "accounting_snapshot": str(recovery / "accounting-cas.json"),
        "rollback_target": str(target),
        "restart": True,
        "apply_accounting": True,
    }
    acquired: list[dict[str, object]] = []
    drain = {"phase": "drained"}
    monkeypatch.setattr(policy, "_load_policy_journal", lambda *_args, **_kwargs: journal)
    monkeypatch.setattr(policy, "_validate_snapshot_path", lambda _root, path: path)
    monkeypatch.setattr(
        policy,
        "_validate_accounting_snapshot_path",
        lambda _root, _snapshot, path: path,
    )
    monkeypatch.setattr(policy, "_snapshot_manifest_rows", lambda *_args: [])
    monkeypatch.setattr(policy, "_validated_accounting_snapshot", lambda *_args: {})

    def acquire(*_args: object, **kwargs: object) -> dict[str, object]:
        acquired.append(dict(kwargs))
        return drain

    monkeypatch.setattr(policy, "_acquire_restart_drain", acquire)
    monkeypatch.setattr(policy, "_wait_for_restart_quiescence", lambda *_args: None)
    monkeypatch.setattr(policy, "_mark_restart_drain_transacting", lambda *_args: None)
    monkeypatch.setattr(policy, "_restore_snapshot", lambda *_args: None)
    monkeypatch.setattr(policy, "_restore_accounting", lambda *_args: None)
    monkeypatch.setattr(policy, "_restore_services", lambda *_args: None)
    monkeypatch.setattr(policy, "_snapshot_readback", lambda *_args: {})
    monkeypatch.setattr(policy, "_accounting_snapshot_matches", lambda *_args: None)
    monkeypatch.setattr(
        policy,
        "_advance_journal",
        lambda _path, payload, phase: payload.__setitem__("phase", phase),
    )
    monkeypatch.setattr(policy, "_release_restart_drain", lambda *_args: None)

    recovered = policy._recover_orphan(
        Path("/"),
        loaded,
        slurm_node="trt-gb10-1",
    )

    assert recovered is journal
    assert recovered["phase"] == "rolled_back"
    assert len(acquired) == 1
    assert acquired[0]["operation"] == "rollback"
    assert acquired[0]["apply_accounting"] is False


def test_terminal_legacy_policy_journal_is_archived_durably(tmp_path: Path) -> None:
    root = _root(tmp_path)
    loaded = policy.load_profile(PROFILE)
    files = policy.desired_files(root, loaded, candidate_sha="a" * 40)
    snapshot = policy._snapshot(root, files)
    path = policy._journal_path(root, loaded)
    now = datetime.now(UTC).isoformat()
    legacy = {
        "schema_version": 1,
        "operation": "apply",
        "cluster": loaded.cluster,
        "host": policy._canonical_host(),
        "slurm_node": None,
        "candidate_sha": "a" * 40,
        "snapshot": str(snapshot),
        "accounting_snapshot": None,
        "restart": False,
        "apply_accounting": False,
        "phase": "committed",
        "created_at": now,
        "updated_at": now,
    }
    policy._write_journal(path, legacy)

    assert policy._load_policy_journal(
        path,
        root=root,
        profile=loaded,
        slurm_node=None,
    ) is None
    assert not path.exists()
    archives = list((path.parent / "legacy").glob("*.json"))
    assert len(archives) == 1
    assert policy._load_journal(archives[0]) == legacy


def test_nonterminal_legacy_policy_journal_fails_closed(tmp_path: Path) -> None:
    root = _root(tmp_path)
    loaded = policy.load_profile(PROFILE)
    files = policy.desired_files(root, loaded, candidate_sha="a" * 40)
    snapshot = policy._snapshot(root, files)
    path = policy._journal_path(root, loaded)
    now = datetime.now(UTC).isoformat()
    legacy = {
        "schema_version": 1,
        "operation": "apply",
        "cluster": loaded.cluster,
        "host": policy._canonical_host(),
        "slurm_node": None,
        "candidate_sha": "a" * 40,
        "snapshot": str(snapshot),
        "accounting_snapshot": None,
        "restart": False,
        "apply_accounting": False,
        "phase": "prepared",
        "created_at": now,
        "updated_at": now,
    }
    policy._write_journal(path, legacy)

    with pytest.raises(policy.PolicyError, match="nonterminal legacy"):
        policy._load_policy_journal(
            path,
            root=root,
            profile=loaded,
            slurm_node=None,
        )


def test_released_legacy_drain_is_archived_only_after_idle_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _root(tmp_path)
    loaded = policy.load_profile(PROFILE)
    path = policy._drain_journal_path(root, loaded)
    now = datetime.now(UTC).isoformat()
    legacy = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.slurm-restart-drain",
        "cluster": loaded.cluster,
        "host": policy._canonical_host(),
        "slurm_node": "oldlab-2",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "candidate_root": "/shared_work/loom/candidates/sandboxes/qianyi/" + "a" * 40,
        "profile_relative": "deploy/slurm/developer-sandboxes/oldlab.toml",
        "operation": "apply",
        "apply_accounting": False,
        "ownership_token": "c" * 64,
        "ownership_reason": "loom-sandbox-policy:" + "a" * 12 + ":" + "c" * 16,
        "owned": True,
        "prior_state": "IDLE",
        "prior_reason": "",
        "phase": "released",
        "created_at": now,
        "updated_at": now,
    }
    policy._write_journal(path, legacy)
    monkeypatch.setattr(policy, "_slurm_node_admission", lambda _node: ("IDLE", ""))

    assert policy._load_drain_journal(
        root,
        loaded,
        slurm_node="oldlab-2",
    ) is None
    assert not path.exists()
    assert len(list((path.parent / "legacy").glob("*.json"))) == 1


def test_legacy_guard_status_is_versioned_and_fresh(tmp_path: Path) -> None:
    root = tmp_path / "root"
    loaded = policy.load_profile(PROFILE)
    config = {
        "schema_version": 1,
        "cluster": loaded.cluster,
        "controller": loaded.controller,
        "submit_host": loaded.submit_host,
        "allowed_nodes": list(loaded.allowed_nodes),
        "candidate_sha": "a" * 40,
        "pids_max": loaded.job_pids_max,
        "allowed_accounts": sorted(loaded.child_accounts),
        "poll_interval_seconds": 0.2,
        "require_gpu_probe": False,
    }
    config_sha256 = hashlib.sha256(
        (json.dumps(config, sort_keys=True) + "\n").encode(),
    ).hexdigest()
    status = root / policy._GUARD_STATUS_RELATIVE
    status.parent.mkdir(parents=True)
    status.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "timestamp": datetime.now(UTC).isoformat(),
                "candidate_sha": "a" * 40,
                "config_sha256": config_sha256,
                "scanned": 0,
                "verified": 0,
                "unrelated": 0,
                "failed": 0,
                "failures": [],
                "resource_probe": None,
            },
            sort_keys=True,
        )
        + "\n",
    )
    status.chmod(0o600)

    assert policy._legacy_guard_status_readback(
        root,
        loaded,
        config=config,
        expected_config_sha256=config_sha256,
    )["failed"] == 0


def test_profile_is_exact_three_sandbox_fairshare_contract() -> None:
    loaded = policy.load_profile(PROFILE)

    assert loaded.cluster == "trt-oldlab"
    assert loaded.infrastructure_nodes == loaded.allowed_nodes
    assert loaded.child_accounts == (
        "loom-dev-qianyi",
        "loom-dev-hongjian",
        "loom-dev-devansh",
    )
    assert loaded.users == (
        "loom-sandbox-qianyi",
        "loom-sandbox-hongjian",
        "loom-sandbox-devansh",
    )
    assert loaded.docker_cgroup_driver == "cgroupfs"
    assert loaded.slurm["accounting_storage_enforce"] == ("associations,limits,qos,safe")


def test_candidate_bindings_reject_colliding_job_label_prefixes() -> None:
    loaded = policy.load_profile(PROFILE)
    bindings = {
        "loom-dev-qianyi": {
            "sandbox": "qianyi",
            "service_user": "loom-sandbox-qianyi",
            "candidate_sha": "1" * 12 + "a" * 28,
            "candidate_tree": "a" * 40,
        },
        "loom-dev-hongjian": {
            "sandbox": "hongjian",
            "service_user": "loom-sandbox-hongjian",
            "candidate_sha": "1" * 12 + "b" * 28,
            "candidate_tree": "b" * 40,
        },
        "loom-dev-devansh": {
            "sandbox": "devansh",
            "service_user": "loom-sandbox-devansh",
            "candidate_sha": "2" * 40,
            "candidate_tree": "c" * 40,
        },
    }

    with pytest.raises(policy.PolicyError, match="pairwise distinct"):
        policy._candidate_bindings(loaded, bindings)


def test_profile_rejects_personal_login_users(tmp_path: Path) -> None:
    profile = tmp_path / "oldlab.toml"
    profile.write_text(
        PROFILE.read_text(encoding="utf-8").replace(
            '  "loom-sandbox-qianyi",\n  "loom-sandbox-hongjian",\n  "loom-sandbox-devansh",',
            '  "qianyi",\n  "hongjian",\n  "devansh",',
        ),
        encoding="utf-8",
    )

    with pytest.raises(policy.PolicyError, match="fixed sandbox service users"):
        policy.load_profile(profile)


def test_gb10_profile_maps_connection_aliases_to_canonical_hosts() -> None:
    loaded = policy.load_profile(GB10_PROFILE)

    assert policy._slurm_node_for_host(loaded, "gx10-01c7") == "trt-gb10-1"
    assert policy._slurm_node_for_host(loaded, "trt-gb10-1") is None
    assert loaded.infrastructure_nodes == tuple(f"trt-gb10-{index}" for index in range(1, 16))
    assert loaded.allowed_nodes == loaded.infrastructure_nodes
    assert loaded.host_aliases["trt-gb10-7"] == "gx10-0faf"
    assert policy._slurm_node_for_host(loaded, "gx10-0faf") == "trt-gb10-7"
    assert policy._allowed_host_aliases(loaded)["trt-gb10-7"] == "gx10-0faf"


def test_gb10_guard_and_capacity_artifacts_include_full_infrastructure_fleet(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    root = _root(tmp_path)

    rendered = policy.desired_files(root, loaded, candidate_sha="a" * 40)
    guard = json.loads(
        rendered[root / "etc/loom/slurm-job-cgroup-guard.json"],
    )
    plan = policy.plan(root, loaded, candidate_sha="a" * 40)

    assert "trt-gb10-7" in guard["allowed_nodes"]
    assert "gx10-0faf" in guard["allowed_nodes"]
    assert plan["infrastructure_nodes"] == list(loaded.infrastructure_nodes)
    assert plan["allowed_nodes"] == list(loaded.allowed_nodes)


def test_runtime_proof_inventory_and_capacity_include_full_gb10_fleet() -> None:
    assert len(policy._RUNTIME_FLEET_NODES) == 20
    assert "trt-gb10-7" in policy._RUNTIME_FLEET_NODES
    assert len(policy._RUNTIME_DOMAIN_HOSTS["gb10"]) == 15
    assert "gx10-0faf" in policy._RUNTIME_DOMAIN_HOSTS["gb10"]

    loaded = policy.load_profile(GB10_PROFILE)
    assert "trt-gb10-7" in loaded.allowed_nodes
    assert policy._allowed_host_aliases(loaded)["trt-gb10-7"] == "gx10-0faf"


def test_profile_rejects_capacity_node_outside_infrastructure_inventory(
    tmp_path: Path,
) -> None:
    profile = tmp_path / "gb10.toml"
    profile.write_text(
        GB10_PROFILE.read_text(encoding="utf-8").replace(
            '  "trt-gb10-1",\n',
            "",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(policy.PolicyError, match="subset of infrastructure_nodes"):
        policy.load_profile(profile)


def test_profile_requires_aliases_for_exact_infrastructure_inventory(
    tmp_path: Path,
) -> None:
    profile = tmp_path / "gb10.toml"
    profile.write_text(
        GB10_PROFILE.read_text(encoding="utf-8").replace(
            'trt-gb10-7 = "gx10-0faf"\n',
            "",
        ),
        encoding="utf-8",
    )

    with pytest.raises(policy.PolicyError, match="every infrastructure Slurm node"):
        policy.load_profile(profile)


def test_live_validation_accepts_infrastructure_only_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    monkeypatch.setattr(policy, "verify_source_candidate", lambda _sha: None)
    monkeypatch.setattr(policy.os, "geteuid", lambda: 0)
    monkeypatch.setattr(policy, "_canonical_host", lambda: "gx10-0faf")
    monkeypatch.setattr(
        policy,
        "_run",
        lambda argv: (
            "ClusterName = trt-gb10\nSlurmctldHost = trt-gb10-1\n"
            if tuple(argv) == ("scontrol", "show", "config")
            else ""
        ),
    )

    assert policy._validate_live_apply(
        Path("/"),
        loaded,
        candidate_sha="a" * 40,
        restart=False,
        apply_accounting=False,
    ) == ("gx10-0faf", "trt-gb10-7")


def test_render_preserves_unrelated_settings_and_removes_duplicate_keys(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    slurm = root / "etc/slurm/slurm.conf"
    slurm.write_text(
        slurm.read_text() + "TaskPlugin=task/affinity\n" + "SlurmctldHost=TRT-EAI-OLDLAB-1\n",
        encoding="utf-8",
    )

    rendered = policy.desired_files(root, loaded)[slurm]

    assert rendered.count("TaskPlugin=") == 1
    assert "TaskPlugin=task/cgroup,task/affinity" in rendered
    assert "SlurmctldHost=TRT-EAI-OLDLAB-1" in rendered
    assert "PriorityWeightFairshare=10000" in rendered


def test_daemon_merge_preserves_existing_keys_and_replaces_driver(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)

    rendered = policy.desired_files(root, loaded)[root / "etc/docker/daemon.json"]
    payload = json.loads(rendered)

    assert payload["features"] == {"containerd-snapshotter": False}
    assert payload["exec-opts"] == ["native.cgroupdriver=cgroupfs"]


def test_apply_to_offline_root_is_idempotent_and_snapshots(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)

    first = policy.apply(root, loaded, restart=False, apply_accounting=False)
    second = policy.apply(root, loaded, restart=False, apply_accounting=False)

    assert first["mutation_authorized"] is True
    assert second["mutation_authorized"] is True
    assert all(row["converged"] for row in second["files"])
    assert Path(first["snapshot"]).is_dir()
    assert Path(second["snapshot"]).is_dir()
    assert (root / "etc/slurm/cgroup.conf").read_text() == policy.render_cgroup_conf(loaded)


def test_rollback_restores_preinstall_absence_without_mixed_state(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original_slurm = (root / "etc/slurm/slurm.conf").read_text()
    candidate = "9" * 40
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha=candidate,
    )

    result = policy.rollback(root, loaded, candidate_sha=candidate)

    assert result["phase"] == "committed"
    assert (root / "etc/slurm/slurm.conf").read_text() == original_slurm
    assert not (root / "etc/loom/slurm-job-cgroup-guard.json").exists()
    assert not (root / "usr/libexec/loom-slurm-job-cgroup-guard").exists()


def test_snapshot_manifest_is_closed_and_binds_archive_content(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    snapshot = policy._snapshot(
        root,
        policy.desired_files(root, loaded, candidate_sha="7" * 40),
    )
    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))

    assert [row["path"] for row in manifest["files"]] == list(
        policy._SNAPSHOT_RELATIVE_PATHS,
    )
    assert all(set(row) == policy._SNAPSHOT_ROW_FIELDS for row in manifest["files"])
    present = next(row for row in manifest["files"] if row["present"])
    archived = snapshot / present["path"]
    archived.write_bytes(b"x" * present["size"])

    with pytest.raises(policy.PolicyError, match="content identity drifted"):
        policy._restore_snapshot(root, snapshot)


def test_snapshot_rejects_hardlinked_or_foreign_archive_content(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    snapshot = policy._snapshot(
        root,
        policy.desired_files(root, loaded, candidate_sha="6" * 40),
    )
    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    present = next(row for row in manifest["files"] if row["present"])
    archived = snapshot / present["path"]
    os.link(archived, snapshot / "foreign-hardlink")

    with pytest.raises(policy.PolicyError, match="metadata is unsafe"):
        policy._snapshot_manifest_rows(root, snapshot)


def test_orphan_recovery_rejects_open_or_foreign_journal_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    result = policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="5" * 40,
    )
    journal_path = Path(result["journal"])
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    journal["phase"] = "files_written"
    journal["foreign"] = True
    policy._write_journal(journal_path, journal)
    monkeypatch.setattr(
        policy,
        "_restore_snapshot",
        lambda *_args: pytest.fail("foreign journal reached restore"),
    )
    monkeypatch.setattr(
        policy,
        "_restore_services",
        lambda *_args: pytest.fail("foreign journal reached restart"),
    )

    with pytest.raises(policy.PolicyError, match="journal binding"):
        policy._recover_orphan(root, loaded, slurm_node=None)


def test_orphan_recovery_rejects_accounting_path_flag_mismatch_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    result = policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="4" * 40,
    )
    journal_path = Path(result["journal"])
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    journal["phase"] = "files_written"
    journal["accounting_snapshot"] = str(Path(result["snapshot"]) / "accounting-cas.json")
    policy._write_journal(journal_path, journal)
    monkeypatch.setattr(
        policy,
        "_restore_snapshot",
        lambda *_args: pytest.fail("mismatched accounting binding reached restore"),
    )

    with pytest.raises(policy.PolicyError, match="accounting path binding"):
        policy._recover_orphan(root, loaded, slurm_node=None)


def test_orphan_recovery_prevalidates_accounting_payload_before_file_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    result = policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="3" * 40,
    )
    snapshot = Path(result["snapshot"])
    accounting = snapshot / "accounting-cas.json"
    accounting.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": {"qos": {}, "accounts": {}, "associations": {}},
                "desired": {"foreign": {}},
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    accounting.chmod(0o600)
    journal_path = Path(result["journal"])
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    journal.update(
        {
            "phase": "files_written",
            "apply_accounting": True,
            "accounting_snapshot": str(accounting),
        },
    )
    policy._write_journal(journal_path, journal)
    monkeypatch.setattr(
        policy,
        "_restore_snapshot",
        lambda *_args: pytest.fail("invalid accounting snapshot reached file restore"),
    )

    offline_controller = replace(loaded, controller=None)
    with pytest.raises(policy.PolicyError, match="accounting CAS snapshot binding"):
        policy._recover_orphan(root, offline_controller, slurm_node=None)


def test_file_plan_rejects_guard_permission_drift(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    candidate = "8" * 40
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha=candidate,
    )
    config = root / "etc/loom/slurm-job-cgroup-guard.json"
    config.chmod(0o644)

    result = policy.plan(root, loaded, candidate_sha=candidate)
    row = next(item for item in result["files"] if item["path"] == str(config))

    assert row["live_sha256"] == row["desired_sha256"]
    assert row["live_mode"] == 0o644
    assert row["desired_mode"] == 0o600
    assert row["converged"] is False
    assert result["file_plan"]["converged"] is False


def test_invalid_or_weakened_profile_fails_closed(tmp_path: Path) -> None:
    raw = PROFILE.read_text(encoding="utf-8").replace(
        "constrain_devices = true",
        "constrain_devices = false",
    )
    invalid = tmp_path / "invalid.toml"
    invalid.write_text(raw, encoding="utf-8")

    with pytest.raises(policy.PolicyError, match="constrain_devices"):
        policy.load_profile(invalid)


def test_accounting_plan_has_parent_budget_and_one_child_per_user() -> None:
    loaded = policy.load_profile(PROFILE)

    commands = policy.accounting_commands(loaded)
    flattened = [" ".join(command) for command in commands]

    assert any(
        "account=loom-dev set Fairshare=1 GrpTRES=cpu=40,mem=160G" in row for row in flattened
    )
    for user in loaded.users:
        assert any(f"add user {user}" in row for row in flattened)
    for account in loaded.child_accounts:
        assert any(f"add account {account} Parent=loom-dev" in row for row in flattened)


@pytest.mark.parametrize(
    "crash_phase",
    ("files_written", "accounting_applied", "services_reconfigured", "verified"),
)
def test_next_apply_recovers_every_orphan_transaction_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_phase: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original_slurm = (root / "etc/slurm/slurm.conf").read_text()
    original_advance = policy._advance_journal

    def crash_after_durable_phase(
        path: Path,
        payload: dict[str, object],
        phase: str,
    ) -> None:
        original_advance(path, payload, phase)
        if phase == crash_phase:
            raise SystemExit("simulated process death")

    monkeypatch.setattr(policy, "_advance_journal", crash_after_durable_phase)
    with pytest.raises(SystemExit, match="simulated process death"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )

    journal = json.loads(policy._journal_path(root, loaded).read_text())
    assert journal["phase"] == crash_phase
    restored: list[Path] = []
    original_restore = policy._restore_snapshot

    def observe_restore(observed_root: Path, snapshot: Path) -> None:
        restored.append(snapshot)
        original_restore(observed_root, snapshot)

    monkeypatch.setattr(policy, "_advance_journal", original_advance)
    monkeypatch.setattr(policy, "_restore_snapshot", observe_restore)
    result = policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )

    assert restored
    assert result["phase"] == "committed"
    assert original_slurm != (root / "etc/slurm/slurm.conf").read_text()
    assert stat.S_IMODE(policy._journal_path(root, loaded).stat().st_mode) == 0o600


@pytest.mark.parametrize("field", ("snapshot", "accounting_snapshot"))
def test_apply_rejects_noncanonical_paths_from_committed_journal(
    tmp_path: Path,
    field: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )
    journal_path = policy._journal_path(root, loaded)
    journal = json.loads(journal_path.read_text())
    journal[field] = str(tmp_path / "outside-snapshot")
    policy._write_journal(journal_path, journal)

    with pytest.raises(policy.PolicyError, match=r"canonical|not canonical"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )


def test_apply_rejects_symlink_or_hardlinked_journal(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )
    journal = policy._journal_path(root, loaded)
    hardlink = tmp_path / "journal-hardlink"
    os.link(journal, hardlink)
    with pytest.raises(policy.PolicyError, match="journal is unsafe"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )
    hardlink.unlink()

    external = tmp_path / "external-journal"
    external.write_bytes(journal.read_bytes())
    external.chmod(0o600)
    journal.unlink()
    journal.symlink_to(external)
    with pytest.raises(policy.PolicyError, match="journal is unreadable"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )


def test_apply_rejects_canonical_snapshot_path_that_is_a_symlink(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha="a" * 40,
    )
    journal_path = policy._journal_path(root, loaded)
    journal = json.loads(journal_path.read_text())
    external = tmp_path / "external-snapshot"
    external.mkdir(mode=0o700)
    alias = policy._state_root(root) / "snapshots" / "20000101T000000.000000Z"
    alias.symlink_to(external, target_is_directory=True)
    journal["snapshot"] = str(alias)
    policy._write_journal(journal_path, journal)

    with pytest.raises(policy.PolicyError, match="ownership is unsafe"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=False,
            candidate_sha="a" * 40,
        )


def test_accounting_failure_uses_targeted_cas_without_cluster_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original = (root / "etc/slurm/slurm.conf").read_text()
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *args, **kwargs: ("trt-eai-oldlab-1", loaded.controller),
    )
    calls: list[tuple[str, ...]] = []

    def fake_run(argv: tuple[str, ...] | list[str]) -> str:
        command = tuple(argv)
        calls.append(command)
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)

    def fail_apply(
        _profile: policy.Profile,
        _snapshot: dict[str, object],
    ) -> None:
        raise policy.PolicyError("accounting mutation failed")

    monkeypatch.setattr(policy, "_apply_accounting", fail_apply)
    with pytest.raises(policy.PolicyError, match="accounting mutation failed"):
        policy.apply(
            root,
            loaded,
            restart=False,
            apply_accounting=True,
            candidate_sha="b" * 40,
        )

    assert (root / "etc/slurm/slurm.conf").read_text() == original
    assert not any("dump" in command or "load" in command for command in calls)
    journal = json.loads(policy._journal_path(root, loaded).read_text())
    assert journal["phase"] == "rolled_back"


def test_accounting_cas_ignores_unrelated_accounts_but_rejects_owned_field_drift() -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    before = json.loads(json.dumps(desired))
    before["accounts"][loaded.parent_account]["Fairshare"] = "2"
    current = json.loads(json.dumps(desired))
    current["accounts"]["unrelated-research"] = {
        "ParentName": "",
        "Fairshare": "99",
        "GrpTRES": "cpu=999",
    }

    policy._validate_accounting_cas(current, before, desired)

    current["accounts"][loaded.parent_account]["Fairshare"] = "777"
    with pytest.raises(policy.PolicyError, match="changed concurrently"):
        policy._validate_accounting_cas(current, before, desired)


def test_accounting_cas_refuses_to_delete_new_identity_with_external_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": {"qos": {}, "accounts": {}, "associations": {}},
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: desired)
    monkeypatch.setattr(
        policy,
        "_accounting_external_references",
        lambda _profile: {loaded.parent_account},
    )

    def unexpected_run(
        _argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        pytest.fail("accounting mutation ran before the external-reference gate")

    monkeypatch.setattr(policy, "_run", unexpected_run)
    with pytest.raises(policy.PolicyError, match="external references"):
        policy._restore_accounting(loaded, snapshot)


def test_accounting_restore_rejects_owned_drift_before_first_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    before = json.loads(json.dumps(desired))
    before["accounts"][loaded.parent_account]["Fairshare"] = "2"
    current = json.loads(json.dumps(desired))
    current["accounts"][loaded.parent_account]["Fairshare"] = "777"
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": before,
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: current)

    def unexpected_run(
        _argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        pytest.fail("accounting mutation ran before the CAS drift gate")

    monkeypatch.setattr(policy, "_run", unexpected_run)
    with pytest.raises(policy.PolicyError, match="changed concurrently"):
        policy._restore_accounting(loaded, snapshot)


def test_accounting_restore_deletes_only_exact_new_loom_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    empty = {"qos": {}, "accounts": {}, "associations": {}}
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": empty,
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: desired)
    monkeypatch.setattr(policy, "_accounting_external_references", lambda _profile: set())
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    monkeypatch.setattr(
        policy,
        "_require_accounting_state",
        lambda _profile, _expected, *, phase: None,
    )
    monkeypatch.setattr(
        policy,
        "_checked_accounting_transition",
        lambda _profile, command, _expected, next_expected: (
            commands.append(tuple(command)) or next_expected
        ),
    )
    policy._restore_accounting(loaded, snapshot)

    delete_commands = [command for command in commands if "delete" in command]
    for user, account in zip(loaded.users, loaded.child_accounts, strict=True):
        assert (
            "sacctmgr",
            "-i",
            "delete",
            "user",
            "where",
            f"name={user}",
            f"account={account}",
            f"cluster={loaded.cluster}",
        ) in delete_commands
    for account in (*loaded.child_accounts, loaded.parent_account):
        assert (
            "sacctmgr",
            "-i",
            "delete",
            "account",
            "where",
            f"account={account}",
            f"cluster={loaded.cluster}",
        ) in delete_commands
    assert (
        "sacctmgr",
        "-i",
        "delete",
        "qos",
        "where",
        f"name={loaded.qos}",
    ) in delete_commands
    assert len(delete_commands) == len(loaded.users) + len(loaded.child_accounts) + 2


def test_accounting_domain_lock_blocks_second_tool_transaction_between_gate_and_mutation(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    first_preflight = threading.Event()
    release_first = threading.Event()
    first_mutation = threading.Event()
    second_preflight = threading.Event()
    errors: list[BaseException] = []

    def first_transaction() -> None:
        try:
            with policy._domain_lock(root, loaded):
                first_preflight.set()
                if not release_first.wait(timeout=2):
                    raise RuntimeError("first accounting transaction was not released")
                first_mutation.set()
        except BaseException as exc:
            errors.append(exc)

    def second_transaction() -> None:
        try:
            if not first_preflight.wait(timeout=2):
                raise RuntimeError("first accounting transaction did not acquire the lock")
            with policy._domain_lock(root, loaded):
                second_preflight.set()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=first_transaction)
    second = threading.Thread(target=second_transaction)
    first.start()
    assert first_preflight.wait(timeout=2)
    second.start()
    assert not second_preflight.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert errors == []
    assert first_mutation.is_set()
    assert second_preflight.is_set()
    lock = policy._state_root(root) / "locks" / f"{loaded.cluster}.lock"
    metadata = lock.lstat()
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert metadata.st_nlink == 1


def test_accounting_domain_lock_rejects_symlink_and_hardlink(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    lock = policy._state_root(root) / "locks" / f"{loaded.cluster}.lock"
    lock.parent.mkdir(parents=True, mode=0o700)
    lock.parent.chmod(0o700)
    external = tmp_path / "external-lock"
    external.write_text("")
    external.chmod(0o600)
    lock.symlink_to(external)
    with pytest.raises(policy.PolicyError, match="lock could not be opened safely"):
        with policy._domain_lock(root, loaded):
            pytest.fail("symlink lock was acquired")

    lock.unlink()
    lock.write_text("")
    lock.chmod(0o600)
    hardlink = tmp_path / "lock-hardlink"
    os.link(lock, hardlink)
    with pytest.raises(policy.PolicyError, match="lock inode is unsafe"):
        with policy._domain_lock(root, loaded):
            pytest.fail("hardlinked lock was acquired")


def test_accounting_transition_detects_bypass_drift_after_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    expected = policy._accounting_desired_state(loaded)
    next_expected = json.loads(json.dumps(expected))
    next_expected["accounts"][loaded.parent_account]["Fairshare"] = "2"
    bypassed = json.loads(json.dumps(next_expected))
    bypassed["accounts"][loaded.parent_account]["Fairshare"] = "777"
    states = iter((expected, bypassed))
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: next(states))
    monkeypatch.setattr(policy, "_run", lambda _command: "")

    with pytest.raises(policy.PolicyError, match="after mutation"):
        policy._checked_accounting_transition(
            loaded,
            ("sacctmgr", "-i", "modify", "account"),
            expected,
            next_expected,
        )


def test_accounting_restore_rechecks_external_refs_immediately_before_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    desired = policy._accounting_desired_state(loaded)
    current = {
        "qos": {},
        "accounts": {
            loaded.parent_account: desired["accounts"][loaded.parent_account],
        },
        "associations": {},
    }
    snapshot = tmp_path / "accounting-cas.json"
    policy._atomic_write(
        snapshot,
        json.dumps(
            {
                "schema_version": 1,
                "cluster": loaded.cluster,
                "before": {"qos": {}, "accounts": {}, "associations": {}},
                "desired": desired,
            },
        )
        + "\n",
        mode=0o600,
    )
    monkeypatch.setattr(policy, "_accounting_state", lambda _profile: current)
    references = iter((set(), {loaded.parent_account}))
    monkeypatch.setattr(
        policy,
        "_accounting_external_references",
        lambda _profile: next(references),
    )
    monkeypatch.setattr(
        policy,
        "_run",
        lambda _command: pytest.fail("delete ran after an external reference appeared"),
    )

    with pytest.raises(policy.PolicyError, match="gained an external reference"):
        policy._restore_accounting(loaded, snapshot)


def test_service_reload_failure_restores_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    original = (root / "etc/docker/daemon.json").read_text()
    monkeypatch.setattr(
        policy,
        "_validate_live_apply",
        lambda *args, **kwargs: ("trt-eai-oldlab-1", loaded.controller),
    )
    calls = 0

    def restart(_profile: policy.Profile, _node: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise policy.PolicyError("daemon reload failed")

    monkeypatch.setattr(policy, "_restart_services", restart)
    monkeypatch.setattr(
        policy,
        "_restore_services",
        lambda _root, _profile, _node: restart(_profile, _node),
    )
    with pytest.raises(policy.PolicyError, match="daemon reload failed"):
        policy.apply(
            root,
            loaded,
            restart=True,
            apply_accounting=False,
            candidate_sha="c" * 40,
        )

    assert calls == 2
    assert (root / "etc/docker/daemon.json").read_text() == original


def test_restart_stops_guard_before_invalidating_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    events: list[tuple[str, ...]] = []

    def run_status(argv: tuple[str, ...]) -> tuple[int, str]:
        events.append(tuple(argv))
        return (3, "inactive") if argv[1] == "is-active" else (0, "")

    monkeypatch.setattr(policy, "_run_status", run_status)
    monkeypatch.setattr(
        policy,
        "_invalidate_guard_status",
        lambda _root: events.append(("invalidate-status",)),
    )
    monkeypatch.setattr(policy, "_run", lambda argv: events.append(tuple(argv)) or "")

    policy._restart_services(loaded, "trt-gb10-7")

    assert events[:3] == [
        ("systemctl", "stop", "loom-slurm-job-cgroup-guard.service"),
        ("systemctl", "is-active", "loom-slurm-job-cgroup-guard.service"),
        ("invalidate-status",),
    ]
    assert ("systemctl", "start", "loom-slurm-job-cgroup-guard.service") in events
    assert ("systemctl", "restart", "loom-slurm-job-cgroup-guard.service") not in events


def _live_outputs(profile: policy.Profile) -> dict[str, str]:
    slurm = "\n".join(
        f"{key} = {profile.slurm[field]}" for key, field in policy._SLURM_KEYS.items()
    )
    accounts = "\n".join(
        (
            "loom-dev||1|cpu=40,mem=160G",
            "loom-dev-qianyi|loom-dev|1|",
            "loom-dev-hongjian|loom-dev|1|",
            "loom-dev-devansh|loom-dev|1|",
        ),
    )
    associations = "\n".join(
        f"{user}|{account}|1|loom-dev|loom-dev"
        for user, account in zip(profile.users, profile.child_accounts, strict=True)
    )
    return {
        "scontrol": slurm,
        "docker": "cgroupfs\n",
        "enabled": "enabled\n",
        "active": "active\n",
        "qos": "loom-dev|0|02:00:00|10|20\n",
        "accounts": accounts,
        "associations": associations,
    }


def test_live_readback_rejects_effective_slurm_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    root = _root(tmp_path)
    candidate = "d" * 40
    policy.apply(
        root,
        loaded,
        restart=False,
        apply_accounting=False,
        candidate_sha=candidate,
    )
    desired = policy.desired_files(root, loaded, candidate_sha=candidate)
    bindings = policy._offline_candidate_bindings(loaded, candidate)
    candidate_set_sha256 = policy._candidate_set_sha256(bindings)
    guard_config = root / "etc/loom/slurm-job-cgroup-guard.json"
    status = {
        "schema_version": 2,
        "timestamp": datetime.now(UTC).isoformat(),
        "candidate_set_sha256": candidate_set_sha256,
        "config_sha256": policy._sha256(desired[guard_config].encode()),
        "scanned": 1,
        "verified": 1,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probes": {
            "loom-dev-qianyi": {
                "job_id": "123",
                "account": "loom-dev-qianyi",
                "sandbox": "qianyi",
                "service_user": "loom-sandbox-qianyi",
                "candidate_sha": candidate,
                "candidate_tree": candidate,
                "candidate_set_sha256": candidate_set_sha256,
                "observed_at": datetime.now(UTC).isoformat(),
                "cpu_max": "200000 100000",
                "memory_max": "8388608000",
                "pids_max": "32768",
                "gpu_tres": "not-required",
                "gpu_verified": True,
            },
        },
    }
    status_path = root / policy._GUARD_STATUS_RELATIVE
    policy._atomic_write(status_path, json.dumps(status) + "\n", mode=0o600)
    outputs = _live_outputs(loaded)

    def fake_run(argv: tuple[str, ...] | list[str]) -> str:
        command = tuple(argv)
        if command[:3] == ("scontrol", "show", "config"):
            return outputs["scontrol"]
        if command[:3] == ("docker", "info", "--format"):
            return outputs["docker"]
        if command[:2] == ("systemctl", "is-enabled"):
            return outputs["enabled"]
        if command[:2] == ("systemctl", "is-active"):
            return outputs["active"]
        joined = " ".join(command)
        if "show qos" in joined:
            return outputs["qos"]
        if "show account " in joined:
            return outputs["accounts"]
        if "show association " in joined:
            return outputs["associations"]
        raise AssertionError(command)

    monkeypatch.setattr(policy, "_run", fake_run)
    assert policy.live_readback(
        root,
        loaded,
        sandbox="qianyi",
        candidate_sha=candidate,
        candidate_bindings=bindings,
        require_probe=True,
    )["converged"]

    outputs["scontrol"] = outputs["scontrol"].replace(
        "TaskPlugin = task/cgroup,task/affinity",
        "TaskPlugin = task/none",
    )
    with pytest.raises(policy.PolicyError, match="TaskPlugin"):
        policy.live_readback(
            root,
            loaded,
            sandbox="qianyi",
            candidate_sha=candidate,
            candidate_bindings=bindings,
            require_probe=True,
        )


def test_guard_all_failed_status_is_not_accepted_as_live_health(tmp_path: Path) -> None:
    path = tmp_path / policy._GUARD_STATUS_RELATIVE
    loaded = policy.load_profile(PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "e" * 40)
    payload = {
        "schema_version": 2,
        "timestamp": datetime.now(UTC).isoformat(),
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "config_sha256": "f" * 64,
        "scanned": 3,
        "verified": 0,
        "unrelated": 0,
        "failed": 3,
        "failures": [{"job_id": "1", "reason": "readback failed"}],
        "resource_probes": {},
    }
    policy._atomic_write(path, json.dumps(payload) + "\n", mode=0o600)

    with pytest.raises(policy.PolicyError, match="failed or drifted"):
        policy._guard_status_readback(
            tmp_path,
            candidate_bindings=bindings,
            expected_config_sha256="f" * 64,
            require_probe=True,
            sandbox="qianyi",
        )


@pytest.mark.parametrize("future_field", ("status", "probe"))
def test_guard_readback_rejects_future_dated_status_or_probe(
    tmp_path: Path,
    future_field: str,
) -> None:
    path = tmp_path / policy._GUARD_STATUS_RELATIVE
    loaded = policy.load_profile(PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "e" * 40)
    candidate_set_sha256 = policy._candidate_set_sha256(bindings)
    now = datetime.now(UTC)
    future = now + policy._GUARD_MAX_CLOCK_SKEW + timedelta(minutes=1)
    payload = {
        "schema_version": 2,
        "timestamp": (future if future_field == "status" else now).isoformat(),
        "candidate_set_sha256": candidate_set_sha256,
        "config_sha256": "f" * 64,
        "scanned": 1,
        "verified": 1,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probes": {
            "loom-dev-qianyi": {
                "account": "loom-dev-qianyi",
                "sandbox": "qianyi",
                "service_user": "loom-sandbox-qianyi",
                "candidate_sha": bindings["loom-dev-qianyi"]["candidate_sha"],
                "candidate_tree": bindings["loom-dev-qianyi"]["candidate_tree"],
                "candidate_set_sha256": candidate_set_sha256,
                "observed_at": (future if future_field == "probe" else now).isoformat(),
            },
        },
    }
    policy._atomic_write(path, json.dumps(payload) + "\n", mode=0o600)

    with pytest.raises(policy.PolicyError, match="stale"):
        policy._guard_status_readback(
            tmp_path,
            candidate_bindings=bindings,
            expected_config_sha256="f" * 64,
            require_probe=True,
            sandbox="qianyi",
        )


def test_guard_readback_rejects_status_before_restart_boundary(tmp_path: Path) -> None:
    path = tmp_path / policy._GUARD_STATUS_RELATIVE
    loaded = policy.load_profile(PROFILE)
    bindings = policy._offline_candidate_bindings(loaded, "e" * 40)
    observed = datetime.now(UTC)
    payload = {
        "schema_version": 2,
        "timestamp": observed.isoformat(),
        "candidate_set_sha256": policy._candidate_set_sha256(bindings),
        "config_sha256": "f" * 64,
        "scanned": 0,
        "verified": 0,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probes": {},
    }
    policy._atomic_write(path, json.dumps(payload) + "\n", mode=0o600)

    with pytest.raises(policy.PolicyError, match="stale"):
        policy._guard_status_readback(
            tmp_path,
            candidate_bindings=bindings,
            expected_config_sha256="f" * 64,
            require_probe=False,
            not_before=observed + timedelta(microseconds=1),
        )


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repo), *args),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _trust_ambient_tmp_ancestors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_lstat = Path.lstat
    ambient_ancestors = frozenset(tmp_path.parents)

    def lstat_without_ambient_write_bits(path: Path) -> os.stat_result:
        metadata = actual_lstat(path)
        if path not in ambient_ancestors:
            return metadata
        fields = list(metadata)
        fields[0] &= ~0o022
        return os.stat_result(fields)

    monkeypatch.setattr(Path, "lstat", lstat_without_ambient_write_bits)


def _strict_candidate_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    attributes: str | None = None,
) -> tuple[Path, Path, str]:
    _trust_ambient_tmp_ancestors(tmp_path, monkeypatch)
    repository = tmp_path / "candidate"
    repository.mkdir(mode=0o755)
    _git(repository, "init", "-q")
    _git(repository, "config", "user.name", "Policy Test")
    _git(repository, "config", "user.email", "policy@example.com")
    (repository / "tracked.txt").write_text("candidate bytes\n")
    if attributes is not None:
        (repository / ".gitattributes").write_text(attributes)
    _git(repository, "add", ".")
    _git(repository, "commit", "-qm", "candidate")
    candidate = _git(repository, "rev-parse", "HEAD")
    env_dir = tmp_path / "private"
    env_dir.mkdir(mode=0o700)
    worker_env = env_dir / "worker.env"
    worker_env.write_text("LOOM_WORKER_TOKEN_FILE=/run/loom/token\n")
    worker_env.chmod(0o600)
    return repository, worker_env, candidate


def test_strict_candidate_binding_reads_raw_tree_and_private_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ambient = tmp_path / "ambient"
    ambient.mkdir()
    ambient.chmod(0o777)
    trusted_case_root = ambient / "trusted-case"
    trusted_case_root.mkdir()
    repository, worker_env, candidate = _strict_candidate_fixture(
        trusted_case_root,
        monkeypatch,
    )

    binding = policy.strict_candidate_binding(
        repository,
        worker_env,
        candidate_sha=candidate,
    )

    assert binding["repository"]["candidate_sha"] == candidate
    assert binding["repository"]["candidate_tree"] == _git(
        repository,
        "rev-parse",
        "HEAD^{tree}",
    )
    assert binding["worker_env"]["keys"] == ["LOOM_WORKER_TOKEN_FILE"]
    assert binding["worker_env"]["inode"] == worker_env.stat().st_ino
    assert binding["worker_env"]["uid"] == worker_env.stat().st_uid
    assert binding["worker_env"]["gid"] == worker_env.stat().st_gid


@pytest.mark.parametrize("flag", ("--skip-worktree", "--assume-unchanged"))
def test_strict_candidate_rejects_hidden_index_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flag: str,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path, monkeypatch)
    _git(repository, "update-index", flag, "tracked.txt")

    with pytest.raises(policy.PolicyError, match="skip-worktree or assume-unchanged"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


@pytest.mark.parametrize("mutation", ("extra", "raw-drift"))
def test_strict_candidate_rejects_extra_files_and_raw_byte_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path, monkeypatch)
    if mutation == "extra":
        (repository / "ignored-or-untracked.txt").write_text("extra\n")
        expected = "extra or missing"
    else:
        (repository / "tracked.txt").write_text("different raw bytes\n")
        expected = "raw tracked bytes"

    with pytest.raises(policy.PolicyError, match=expected):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_strict_candidate_rejects_clean_filter_interference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(
        tmp_path,
        monkeypatch,
        attributes="*.txt filter=unsafe-clean\n",
    )

    with pytest.raises(policy.PolicyError, match="interfering Git filter"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_strict_candidate_rejects_symlink_parent_and_nonprivate_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path, monkeypatch)
    alias = tmp_path / "candidate-alias"
    alias.symlink_to(repository, target_is_directory=True)

    with pytest.raises(policy.PolicyError, match="symlinks"):
        policy.strict_candidate_binding(
            alias,
            worker_env,
            candidate_sha=candidate,
        )

    worker_env.chmod(0o640)
    with pytest.raises(policy.PolicyError, match="0600"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )

    worker_env.chmod(0o600)
    with pytest.raises(policy.PolicyError, match="batch UID/GID"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
            expected_batch_uid=worker_env.stat().st_uid + 1,
            expected_batch_gid=worker_env.stat().st_gid,
        )


@pytest.mark.parametrize(
    "contents",
    (
        "LOOM_TOKEN=value\nLOOM_TOKEN=other\n",
        "lowercase=value\n",
        "LOOM_TOKEN\n",
    ),
)
def test_strict_candidate_rejects_duplicate_or_invalid_env_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    contents: str,
) -> None:
    repository, worker_env, candidate = _strict_candidate_fixture(tmp_path, monkeypatch)
    worker_env.write_text(contents)
    worker_env.chmod(0o600)

    with pytest.raises(policy.PolicyError, match=r"duplicate|invalid"):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_strict_candidate_rejects_writable_descendant_below_ambient_tmp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unsafe = tmp_path / "unsafe"
    unsafe.mkdir()
    unsafe.chmod(0o777)
    repository, worker_env, candidate = _strict_candidate_fixture(unsafe, monkeypatch)

    with pytest.raises(
        policy.PolicyError,
        match="trusted path chain must not be group/world writable",
    ):
        policy.strict_candidate_binding(
            repository,
            worker_env,
            candidate_sha=candidate,
        )


def test_slurm_profiles_keep_independent_controller_routes() -> None:
    oldlab = policy.load_profile(PROFILE)
    gb10 = policy.load_profile(GB10_PROFILE)

    assert (oldlab.cluster, oldlab.controller, oldlab.submit_host) == (
        "trt-oldlab",
        "TRT-EAI-OLDLAB-1",
        "trt-EAI-OLDLAB-2",
    )
    assert (gb10.cluster, gb10.controller, gb10.submit_host) == (
        "trt-gb10",
        "trt-gb10-1",
        "trt-gb10-1",
    )
    assert oldlab.cluster != gb10.cluster


def _allocation_inflight_payload(
    loaded: policy.Profile,
    candidate: str,
    *,
    job_id: str = "123",
) -> dict[str, object]:
    node = loaded.allowed_nodes[0]
    return {
        "schema_version": 1,
        "sandbox": SANDBOX,
        "created_at": datetime.now(UTC).isoformat(),
        "candidate_sha": candidate,
        "cluster": loaded.cluster,
        "controller": loaded.controller,
        "submit_host": loaded.submit_host,
        "node": node,
        "host": loaded.host_aliases[node],
        "user": loaded.users[0],
        "account": loaded.child_accounts[0],
        "job_id": job_id,
        "job_name": policy._allocation_job_name(
            SANDBOX,
            candidate,
            node,
            1,
            generation_id=TEST_GENERATION_ID,
        ),
        "generation_id": TEST_GENERATION_ID,
        "batch_uid": 501,
        "batch_gid": 20,
        "phase": "submitted",
    }


def _allocation_accounting_row(
    loaded: policy.Profile,
    payload: dict[str, object],
    state: str,
) -> list[str]:
    return [
        str(payload["job_id"]),
        str(payload["job_name"]),
        state,
        str(payload["node"]),
        "cpu=1,mem=256M",
        loaded.child_accounts[0],
        loaded.users[0],
        loaded.cluster,
        loaded.qos,
    ]


def _allocation_queue_row(
    loaded: policy.Profile,
    payload: dict[str, object],
    state: str = "RUNNING",
) -> list[str]:
    return [
        str(payload["job_id"]),
        str(payload["job_name"]),
        state,
        loaded.users[0],
        loaded.child_accounts[0],
        str(payload["node"]),
    ]


def test_allocation_probe_lock_serializes_candidate_transactions(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "6" * 40
    first_entered = threading.Event()
    release_first = threading.Event()
    second_attempting = threading.Event()
    second_submitted = threading.Event()
    errors: list[BaseException] = []

    def first_transaction() -> None:
        try:
            with policy._allocation_probe_lock(
                tmp_path,
                loaded,
                SANDBOX,
                candidate,
                enforce_root_ownership=False,
            ):
                first_entered.set()
                if not release_first.wait(timeout=2):
                    raise RuntimeError("first allocation transaction was not released")
        except BaseException as exc:
            errors.append(exc)

    def second_transaction() -> None:
        try:
            if not first_entered.wait(timeout=2):
                raise RuntimeError("first allocation transaction did not acquire the lock")
            second_attempting.set()
            with policy._allocation_probe_lock(
                tmp_path,
                loaded,
                SANDBOX,
                candidate,
                enforce_root_ownership=False,
            ):
                second_submitted.set()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=first_transaction)
    second = threading.Thread(target=second_transaction)
    first.start()
    assert first_entered.wait(timeout=2)
    second.start()
    assert second_attempting.wait(timeout=2)
    assert not second_submitted.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert second_submitted.is_set()
    lock = policy._allocation_lock_path(tmp_path, loaded, SANDBOX, candidate)
    metadata = lock.lstat()
    assert stat.S_ISREG(metadata.st_mode)
    assert stat.S_IMODE(metadata.st_mode) == 0o600
    assert (metadata.st_uid, metadata.st_gid) == (os.geteuid(), os.getegid())


def test_three_sandboxes_hold_independent_candidate_locks_and_bind_distinct_shas(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidates = {
        "qianyi": "1" * 40,
        "hongjian": "2" * 40,
        "devansh": "3" * 40,
    }
    entered = threading.Barrier(len(candidates) + 1)
    release = threading.Event()
    errors: list[BaseException] = []

    def hold(sandbox: str, candidate: str) -> None:
        try:
            with policy._allocation_probe_lock(
                tmp_path,
                loaded,
                sandbox,
                candidate,
                enforce_root_ownership=False,
            ):
                entered.wait(timeout=2)
                if not release.wait(timeout=2):
                    raise RuntimeError("sandbox allocation lock was not released")
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=hold, args=(sandbox, candidate))
        for sandbox, candidate in candidates.items()
    ]
    for thread in threads:
        thread.start()
    entered.wait(timeout=2)
    release.set()
    for thread in threads:
        thread.join(timeout=2)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    paths = {
        sandbox: policy._allocation_matrix_path(
            tmp_path,
            loaded,
            sandbox,
            candidate,
        )
        for sandbox, candidate in candidates.items()
    }
    assert len(set(paths.values())) == 3
    assert all(f"/{loaded.cluster}/{sandbox}/" in str(path) for sandbox, path in paths.items())
    proof_paths = {
        sandbox: (
            policy._runtime_proof_base(
                tmp_path,
                loaded,
                sandbox,
                candidate,
            ),
            policy._runtime_proof_high_water_path(tmp_path, loaded, sandbox),
            policy._runtime_proof_transaction_path(tmp_path, loaded, sandbox),
        )
        for sandbox, candidate in candidates.items()
    }
    assert len({path for rows in proof_paths.values() for path in rows}) == 9
    assert all(
        f"/{loaded.cluster}/{sandbox}" in str(path)
        for sandbox, rows in proof_paths.items()
        for path in rows
    )
    assert {policy._sandbox_account(loaded, sandbox) for sandbox in candidates} == set(
        loaded.child_accounts
    )
    for sandbox, service_user, account in zip(
        policy._SANDBOXES,
        loaded.users,
        loaded.child_accounts,
        strict=True,
    ):
        arguments = policy._allocation_probe_arguments(
            loaded,
            sandbox=sandbox,
            node=loaded.allowed_nodes[0],
            attempt=1,
            candidate_sha=candidates[sandbox],
            candidate_root=Path("/candidate"),
            worker_env=Path("/private/worker.env"),
            binding={
                "repository": {"candidate_tree": "a" * 40},
                "worker_env": {"inode": 1, "sha256": "b" * 64},
            },
            batch_uid=501,
            batch_gid=20,
            expected_pool="oldlab",
            expected_concurrency=1,
            result_path=Path(f"/private/{sandbox}.json"),
            generation_id=TEST_GENERATION_ID,
        )
        assert f"--uid={service_user}" in arguments
        assert f"--account={account}" in arguments
        assert f"--sandbox {sandbox}" in arguments[-1]
    assert (
        len(
            {
                policy._allocation_job_name(
                    sandbox,
                    candidate,
                    loaded.allowed_nodes[0],
                    1,
                    generation_id=TEST_GENERATION_ID,
                )
                for sandbox, candidate in candidates.items()
            },
        )
        == 3
    )


def test_cross_sandbox_runtime_and_legacy_recovery_bindings_fail_closed(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "4" * 40
    binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    with pytest.raises(policy.PolicyError, match="sandbox binding drifted"):
        policy._new_allocation_matrix(
            loaded,
            sandbox="hongjian",
            candidate_sha=candidate,
            binding=binding,
            runtime_attestation=runtime_attestation,
            batch_uid=501,
            batch_gid=20,
            expected_pool="oldlab",
            expected_concurrency=1,
        )
    with pytest.raises(policy.PolicyError, match="unavailable"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            sandbox="hongjian",
            candidate_sha=candidate,
            candidate_binding=binding,
            runtime_attestation={**runtime_attestation, "sandbox": "hongjian"},
            expected_pool="oldlab",
            expected_concurrency=1,
        )
    matrix = policy._load_allocation_state(
        policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate),
        enforce_root_ownership=False,
    )
    assert matrix is not None
    matrix["generation_id"] = str(runtime_attestation["receipt_sha256"])[:12]
    assert policy._legacy_allocation_generation_is_bound(
        matrix,
        sandbox=SANDBOX,
    )
    assert not policy._legacy_allocation_generation_is_bound(
        matrix,
        sandbox="hongjian",
    )
    with pytest.raises(policy.PolicyError, match="fixed sandbox labels"):
        policy._allocation_probe_path(tmp_path, loaded, "foreign", candidate)


@pytest.mark.parametrize(
    "command",
    ("materialize-runtime-proof", "allocation-probe", "check"),
)
def test_live_runtime_commands_require_explicit_sandbox(
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    result = policy.main(
        (
            command,
            "--profile",
            str(PROFILE),
            "--candidate-sha",
            "5" * 40,
            "--execute",
        ),
    )

    assert result == 1
    assert f"{command} requires --sandbox" in capsys.readouterr().err


def test_allocation_cleanup_archives_completed_job_without_scancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "3" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(
        policy,
        "_probe_accounting_rows",
        lambda _job_id, _profile: [
            _allocation_accounting_row(loaded, payload, "COMPLETED"),
        ],
    )

    def unexpected_run(
        _argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        pytest.fail("terminal allocation cleanup must not call scancel")

    monkeypatch.setattr(policy, "_run", unexpected_run)
    policy._cancel_allocation_job(
        path,
        payload,
        loaded,
        sandbox=SANDBOX,
        enforce_root_ownership=False,
    )

    assert not path.exists()
    history = path.parent / f"{candidate}.123.terminal.json"
    archived = json.loads(history.read_text())
    assert archived["terminal_state"] == "COMPLETED"
    assert archived["generation_id"] == TEST_GENERATION_ID


@pytest.mark.parametrize(
    ("message", "expected"),
    (
        ("allocation probe did not reach a terminal state before timeout", "timeout"),
        ("sacct failed safely with exit code 1", "sacct failed"),
    ),
)
def test_allocation_probe_poll_failure_cancels_and_waits_for_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    message: str,
    expected: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "e" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    poll_calls = 0
    commands: list[tuple[str, ...]] = []

    def fake_poll(
        job_id: str,
        profile: policy.Profile,
        *,
        timeout_seconds: float,
        poll_seconds: float = policy._ALLOCATION_POLL_SECONDS,
    ) -> list[list[str]]:
        nonlocal poll_calls
        del profile, timeout_seconds, poll_seconds
        poll_calls += 1
        if poll_calls == 1:
            raise policy.PolicyError(message)
        return [_allocation_accounting_row(loaded, payload, "CANCELLED")]

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_poll_probe_terminal", fake_poll)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id, _profile: [])
    monkeypatch.setattr(
        policy,
        "_queue_probe_rows",
        lambda _profile, **_kwargs: [_allocation_queue_row(loaded, payload)],
    )
    monkeypatch.setattr(policy, "_run", fake_run)
    with pytest.raises(policy.PolicyError, match=expected):
        policy._poll_allocation_or_cancel(
            path,
            payload,
            loaded,
            sandbox=SANDBOX,
            timeout_seconds=5,
            enforce_root_ownership=False,
        )

    assert poll_calls == 2
    assert commands == [("scancel", f"--clusters={loaded.cluster}", "123")]
    assert not path.exists()
    assert list(path.parent.glob(f"{candidate}.123.cancelled.json"))


def test_allocation_probe_recovers_crash_journal_before_new_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "f" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        command = tuple(argv)
        commands.append(command)
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id, _profile: [])
    monkeypatch.setattr(
        policy,
        "_queue_probe_rows",
        lambda _profile, **_kwargs: [_allocation_queue_row(loaded, payload)],
    )
    monkeypatch.setattr(policy, "_probe_named_accounting_rows", lambda *_args: [])
    monkeypatch.setattr(
        policy,
        "_poll_probe_terminal",
        lambda job_id, profile, **_kwargs: [
            _allocation_accounting_row(loaded, payload, "CANCELLED"),
        ],
    )
    with policy._allocation_probe_lock(
        tmp_path,
        loaded,
        SANDBOX,
        candidate,
        enforce_root_ownership=False,
    ):
        policy._recover_allocation_probe(
            path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            node=str(payload["node"]),
            job_name=str(payload["job_name"]),
            enforce_root_ownership=False,
        )

    assert commands == [("scancel", f"--clusters={loaded.cluster}", "123")]
    assert not path.exists()
    assert list(path.parent.glob(f"{candidate}.123.cancelled.json"))


def test_allocation_recovery_archives_terminal_job_without_scancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "4" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(
        policy,
        "_probe_accounting_rows",
        lambda job_id, _profile: [
            _allocation_accounting_row(loaded, payload, "COMPLETED"),
        ],
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    policy._recover_allocation_probe(
        path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        node=str(payload["node"]),
        job_name=str(payload["job_name"]),
        enforce_root_ownership=False,
    )

    assert commands == [
        (
            "squeue",
            f"--clusters={loaded.cluster}",
            "-h",
            "-n",
            payload["job_name"],
            "-o",
            "%A|%j|%T|%u|%a|%N",
        ),
        (
            "sacct",
            "-nP",
            f"--clusters={loaded.cluster}",
            f"--name={payload['job_name']}",
            "--starttime=now-1day",
            "--format=JobIDRaw,JobName,State,NodeList,AllocTRES,Account,User,Cluster,QOS",
        ),
    ]
    assert not path.exists()
    history = path.parent / f"{candidate}.123.terminal.json"
    assert json.loads(history.read_text())["terminal_state"] == "COMPLETED"


def test_unjournaled_foreign_same_name_job_is_never_cancelled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "a" * 40
    node = loaded.allowed_nodes[0]
    job_name = policy._allocation_job_name(
        SANDBOX,
        candidate,
        node,
        1,
        generation_id=TEST_GENERATION_ID,
    )
    path = policy._allocation_node_inflight_path(
        tmp_path,
        loaded,
        SANDBOX,
        candidate,
        node,
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        command = tuple(argv)
        commands.append(command)
        if command[0] == "squeue":
            return f"987|{job_name}|RUNNING|foreign-user|{loaded.child_accounts[0]}|{node}\n"
        pytest.fail(f"foreign job must not reach {command[0]}")

    monkeypatch.setattr(policy, "_run", fake_run)
    with pytest.raises(policy.PolicyError, match="identity drifted"):
        policy._recover_allocation_probe(
            path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            node=node,
            job_name=job_name,
            enforce_root_ownership=False,
        )

    assert [command[0] for command in commands] == ["squeue"]
    assert not path.exists()


def test_unjournaled_quick_terminal_job_is_recovered_without_resubmission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "b" * 40
    node = loaded.allowed_nodes[0]
    job_name = policy._allocation_job_name(
        SANDBOX,
        candidate,
        node,
        1,
        generation_id=TEST_GENERATION_ID,
    )
    path = policy._allocation_node_inflight_path(
        tmp_path,
        loaded,
        SANDBOX,
        candidate,
        node,
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        command = tuple(argv)
        commands.append(command)
        if command[0] == "squeue":
            return ""
        if command[0] == "sacct":
            base = (
                f"456|{job_name}|COMPLETED|{node}|cpu=1,mem=256M|"
                f"{loaded.child_accounts[0]}|{loaded.users[0]}|{loaded.cluster}|"
                f"{loaded.qos}\n"
            )
            if any(item.startswith("--name=") for item in command):
                return base
            return base + (
                f"456.0|srun|COMPLETED|{node}|cpu=1,mem=256M|"
                f"{loaded.child_accounts[0]}|{loaded.users[0]}|{loaded.cluster}|"
                f"{loaded.qos}\n"
            )
        pytest.fail(f"terminal recovery must not reach {command[0]}")

    monkeypatch.setattr(policy, "_run", fake_run)
    recovered_job = policy._recover_allocation_probe(
        path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        node=node,
        job_name=job_name,
        enforce_root_ownership=False,
    )

    assert [command[0] for command in commands] == ["squeue", "sacct", "sacct"]
    assert not path.exists()
    history = path.parent / f"{candidate}.456.terminal.json"
    recovered = json.loads(history.read_text())
    assert recovered["terminal_state"] == "COMPLETED"
    assert recovered["node"] == node
    assert recovered_job is not None
    assert recovered_job[0] == "456"
    assert [row[0] for row in recovered_job[1]] == ["456", "456.0"]


def test_allocation_cleanup_scancels_when_initial_sacct_is_temporarily_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "5" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id, _profile: [])
    monkeypatch.setattr(
        policy,
        "_queue_probe_rows",
        lambda _profile, **_kwargs: [_allocation_queue_row(loaded, payload)],
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del timeout
        commands.append(tuple(argv))
        return ""

    monkeypatch.setattr(policy, "_run", fake_run)
    monkeypatch.setattr(
        policy,
        "_poll_probe_terminal",
        lambda job_id, profile, **_kwargs: [
            _allocation_accounting_row(loaded, payload, "CANCELLED"),
        ],
    )
    policy._cancel_allocation_job(
        path,
        payload,
        loaded,
        sandbox=SANDBOX,
        enforce_root_ownership=False,
    )

    assert commands == [("scancel", f"--clusters={loaded.cluster}", "123")]
    assert not path.exists()
    assert list(path.parent.glob(f"{candidate}.123.cancelled.json"))


def test_allocation_probe_scancel_failure_remains_durably_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "1" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)

    def fail_scancel(
        argv: tuple[str, ...] | list[str],
        *,
        timeout: float = 60,
    ) -> str:
        del argv, timeout
        raise policy.PolicyError("scancel failed safely with exit code 1")

    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id, _profile: [])
    monkeypatch.setattr(
        policy,
        "_queue_probe_rows",
        lambda _profile, **_kwargs: [_allocation_queue_row(loaded, payload)],
    )
    monkeypatch.setattr(policy, "_run", fail_scancel)
    with pytest.raises(policy.PolicyError, match="scancel failed"):
        policy._cancel_allocation_job(
            path,
            payload,
            loaded,
            sandbox=SANDBOX,
            enforce_root_ownership=False,
        )

    assert path.exists()
    assert json.loads(path.read_text())["phase"] == "cancel_failed"


def test_allocation_probe_cancel_readback_failure_remains_durably_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "2" * 40
    path = policy._allocation_inflight_path(tmp_path, loaded, SANDBOX, candidate)
    payload = _allocation_inflight_payload(loaded, candidate)
    policy._write_allocation_state(path, payload, enforce_root_ownership=False)
    monkeypatch.setattr(policy, "_probe_accounting_rows", lambda _job_id, _profile: [])
    monkeypatch.setattr(
        policy,
        "_queue_probe_rows",
        lambda _profile, **_kwargs: [_allocation_queue_row(loaded, payload)],
    )
    monkeypatch.setattr(policy, "_run", lambda _argv, **_kwargs: "")

    def fail_readback(
        _job_id: str,
        _profile: policy.Profile,
        **_kwargs: float,
    ) -> list[list[str]]:
        raise policy.PolicyError("cancel readback timed out")

    monkeypatch.setattr(policy, "_poll_probe_terminal", fail_readback)

    with pytest.raises(policy.PolicyError, match="cancel readback timed out"):
        policy._cancel_allocation_job(
            path,
            payload,
            loaded,
            sandbox=SANDBOX,
            enforce_root_ownership=False,
        )

    assert path.exists()
    assert json.loads(path.read_text())["phase"] == "cancel_readback_failed"


def _allocation_matrix_fixture(
    tmp_path: Path,
    loaded: policy.Profile,
    candidate: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    collected_at, expires_at = _generation_window()
    binding: dict[str, object] = {
        "repository": {
            "path": "/candidate",
            "candidate_sha": candidate,
            "candidate_tree": "b" * 40,
            "tracked_files": 10,
        },
        "worker_env": {
            "path": "/private/worker.env",
            "device": 1,
            "inode": 2,
            "uid": 501,
            "gid": 20,
            "sha256": "c" * 64,
            "keys": ["LOOM_WORKER_TOKEN_FILE"],
        },
    }
    runtime_attestation: dict[str, object] = {
        "bundle_id": "a" * 64,
        "receipt_path": "/private/combined.json",
        "receipt_sha256": "d" * 64,
        "receipt_collected_at": collected_at,
        "receipt_expires_at": expires_at,
        "proof_expires_at": expires_at,
        "sandbox": "qianyi",
        "candidate_sha": candidate,
        "candidate_tree": "b" * 40,
        "domain": "oldlab",
        "domain_payload_sha256": "e" * 64,
        "domain_signature_sha256": "f" * 64,
        "domain_generation": 7,
        "domain_hosts": [loaded.host_aliases[node] for node in loaded.allowed_nodes],
        "fleet_payload_sha256": f"sha256:{'1' * 64}",
        "fleet_nodes": ["oldlab-1"],
    }
    matrix = policy._new_allocation_matrix(
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        binding=binding,
        runtime_attestation=runtime_attestation,
        batch_uid=501,
        batch_gid=20,
        expected_pool="oldlab",
        expected_concurrency=1,
    )
    evidence: list[dict[str, object]] = []
    generation_id = policy._allocation_generation_id(runtime_attestation)
    for index, node in enumerate(loaded.allowed_nodes, start=1):
        job_id = str(100 + index)
        item: dict[str, object] = {
            "sandbox": SANDBOX,
            "node": node,
            "host": loaded.host_aliases[node],
            "job_id": job_id,
            "job_name": policy._allocation_job_name(
                SANDBOX,
                candidate,
                node,
                1,
                generation_id=generation_id,
            ),
            "state": "COMPLETED",
            "account": loaded.child_accounts[0],
            "qos": loaded.qos,
            "alloc_tres": "cpu=1,mem=256M",
            "gpu_verified": True,
            "sbatch_verified": True,
            "srun_verified": True,
            "nonexclusive": True,
            "explicit_nodelist": node,
            "compute_check": {
                "schema_version": 1,
                "sandbox": SANDBOX,
                "account": loaded.child_accounts[0],
                "candidate_sha": candidate,
                "candidate_tree": "b" * 40,
                "host": loaded.host_aliases[node],
                "env_device": 1,
                "env_inode": 2,
                "env_sha256": "c" * 64,
                "pool": "oldlab",
                "concurrency": 1,
                "docker_cgroup_driver": loaded.docker_cgroup_driver,
                "job_id": job_id,
                "cgroup_parent": f"/sys/fs/cgroup/slurm/job_{job_id}",
                "cgroup_guard_verified": True,
                "compose_verified": True,
            },
            "batch_uid": 501,
            "batch_gid": 20,
            "command_sha256": "3" * 64,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        matrix["nodes"][node]["status"] = "completed"  # type: ignore[index]
        matrix["nodes"][node]["attempts"] = 1  # type: ignore[index]
        matrix["nodes"][node]["evidence"] = item  # type: ignore[index]
        evidence.append(item)
    matrix["phase"] = "completed"
    completed_at = datetime.now(UTC).isoformat()
    matrix["completed_at"] = completed_at
    payload: dict[str, object] = {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-allocation-matrix",
        "created_at": completed_at,
        "generation_id": generation_id,
        "generation_started_at": collected_at,
        "generation_expires_at": expires_at,
        "candidate_sha": candidate,
        "candidate_tree": "b" * 40,
        "sandbox": SANDBOX,
        "cluster": loaded.cluster,
        "controller": loaded.controller,
        "submit_host": loaded.submit_host,
        "submitting_host": "trt-eai-oldlab-2",
        "allowed_nodes": list(loaded.allowed_nodes),
        "host_aliases": dict(loaded.host_aliases),
        "batch_uid": 501,
        "batch_gid": 20,
        "account": loaded.child_accounts[0],
        "qos": loaded.qos,
        "expected_pool": "oldlab",
        "expected_concurrency": 1,
        "candidate_binding": binding,
        "runtime_attestation": runtime_attestation,
        "nodes": evidence,
        "closed_world_verified": True,
    }
    policy._write_allocation_state(
        policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate),
        matrix,
        enforce_root_ownership=False,
    )
    policy._write_allocation_state(
        policy._allocation_probe_path(tmp_path, loaded, SANDBOX, candidate),
        payload,
        enforce_root_ownership=False,
    )
    return binding, runtime_attestation, payload


def test_allocation_probe_readback_is_candidate_route_and_closed_set_bound(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "a" * 40
    binding, runtime_attestation, payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    observed = policy.allocation_probe_readback(
        tmp_path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        candidate_binding=binding,
        runtime_attestation=runtime_attestation,
        expected_pool="oldlab",
        expected_concurrency=1,
    )
    assert observed["cluster"] == "trt-oldlab"
    assert observed["generation_id"] == runtime_attestation["bundle_id"]
    assert len(str(observed["generation_id"])) == 64
    payload["controller"] = "trt-gb10-1"
    policy._write_allocation_state(
        policy._allocation_probe_path(tmp_path, loaded, SANDBOX, candidate),
        payload,
        enforce_root_ownership=False,
    )
    with pytest.raises(policy.PolicyError, match="binding drifted"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            candidate_binding=binding,
            runtime_attestation=runtime_attestation,
            expected_pool="oldlab",
            expected_concurrency=1,
        )


def test_allocation_matrix_sbatch_explicitly_targets_every_declared_host() -> None:
    loaded = policy.load_profile(GB10_PROFILE)
    candidate = "9" * 40
    binding = {
        "repository": {"candidate_tree": "8" * 40},
        "worker_env": {
            "device": 1,
            "inode": 2,
            "sha256": "7" * 64,
        },
    }

    commands = [
        policy._allocation_probe_arguments(
            loaded,
            sandbox=SANDBOX,
            node=node,
            attempt=1,
            candidate_sha=candidate,
            candidate_root=Path("/candidate"),
            worker_env=Path("/private/worker.env"),
            binding=binding,
            batch_uid=501,
            batch_gid=20,
            expected_pool="gb10",
            expected_concurrency=10,
            result_path=Path(f"/private/results/{node}.json"),
            generation_id=TEST_GENERATION_ID,
        )
        for node in loaded.allowed_nodes
    ]

    assert len(commands) == len(loaded.allowed_nodes)
    assert len({command[2] for command in commands}) == len(loaded.allowed_nodes)
    for node, command in zip(loaded.allowed_nodes, commands, strict=True):
        assert f"--nodelist={node}" in command
        assert "--oversubscribe" in command
        assert (
            f"--job-name=loom827-{SANDBOX}-{candidate[:12]}-{node.lower()}-"
            f"g{TEST_GENERATION_ID}-a1"
        ) in command
        assert f"--nodelist={node}" in command[-1]
        assert "/bin/sleep" not in command[-1]
        assert "allocation-node-check" in command[-1]


def test_allocation_node_check_binds_raw_inputs_docker_and_compose(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "8" * 40
    batch_uid = os.getuid()
    batch_gid = os.getgid()
    compose_calls: list[tuple[str, ...]] = []
    monkeypatch.setenv("SLURM_JOB_ID", "123")
    monkeypatch.setattr(policy.os, "geteuid", lambda: batch_uid)
    monkeypatch.setattr(policy.os, "getegid", lambda: batch_gid)

    def service_identity(user: str) -> object:
        assert user == "loom-sandbox-qianyi"
        return type("Identity", (), {"pw_uid": batch_uid, "pw_gid": batch_gid})()

    monkeypatch.setattr(policy.pwd, "getpwnam", service_identity)
    monkeypatch.setattr(policy, "_canonical_host", lambda: "trt-eai-oldlab-1")
    monkeypatch.setattr(
        policy,
        "_worker_capacity_contract",
        lambda *_args: ("oldlab", 4),
    )
    monkeypatch.setattr(
        policy,
        "strict_candidate_binding",
        lambda *_args, **_kwargs: {
            "repository": {"candidate_tree": "7" * 40},
            "worker_env": {
                "device": 1,
                "inode": 2,
                "sha256": "6" * 64,
            },
        },
    )
    monkeypatch.setattr(
        policy,
        "_read_exact_env_values",
        lambda *_args, **_kwargs: {
            "LOOM_WORKER_CANDIDATE_SHA": candidate,
            "LOOM_WORKER_POOL_NAME": "oldlab",
            "LOOM_WORKER_MAX_CONCURRENT": "4",
        },
    )
    monkeypatch.setattr(policy, "_run", lambda *_args, **_kwargs: "cgroupfs\n")

    def fake_subprocess_run(
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[bytes] | subprocess.CompletedProcess[str]:
        compose_calls.append(argv)
        if "slurm_job_cgroup.py" in " ".join(argv):
            return subprocess.CompletedProcess(
                argv,
                0,
                "/sys/fs/cgroup/slurm/job_123\n",
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "services": {
                        "worker": {
                            "cgroup_parent": "/sys/fs/cgroup/slurm/job_123",
                        },
                        "sandbox-link": {
                            "cgroup_parent": "/sys/fs/cgroup/slurm/job_123",
                        },
                    },
                },
            ).encode(),
            b"",
        )

    monkeypatch.setattr(policy.subprocess, "run", fake_subprocess_run)

    result = policy.allocation_node_check(
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        candidate_root=Path("/candidate"),
        worker_env=Path("/private/worker.env"),
        expected_tree="7" * 40,
        expected_env_inode=2,
        expected_env_sha256="6" * 64,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
        expected_host="trt-eai-oldlab-1",
        expected_pool="oldlab",
        expected_concurrency=4,
        result_path=tmp_path / "result.json",
    )

    assert result["compose_verified"] is True
    assert result["docker_cgroup_driver"] == "cgroupfs"
    assert len(compose_calls) == 2
    assert compose_calls[1][-3:] == ("config", "--format", "json")
    assert "docker-compose.remote-worker.sandbox-link.yml" in " ".join(compose_calls[1])
    assert json.loads((tmp_path / "result.json").read_text()) == result


def test_worker_capacity_binding_comes_from_checked_in_candidate_policy() -> None:
    repository = Path(__file__).resolve().parents[2]

    assert policy._worker_capacity_contract(
        policy.load_profile(PROFILE),
        repository,
    ) == ("oldlab", 4)
    assert policy._worker_capacity_contract(
        policy.load_profile(GB10_PROFILE),
        repository,
    ) == ("gb10", 8)

    with pytest.raises(policy.PolicyError, match="operator pool/concurrency assertion"):
        policy._require_worker_capacity_assertion(
            policy.load_profile(PROFILE),
            repository,
            expected_pool="oldlab",
            expected_concurrency=1,
        )


@pytest.mark.parametrize("mutation", ("foreign", "duplicate", "missing"))
def test_allocation_matrix_readback_rejects_nonclosed_node_set(
    tmp_path: Path,
    mutation: str,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "7" * 40
    binding, runtime_attestation, payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    nodes = payload["nodes"]
    assert isinstance(nodes, list)
    if mutation == "foreign":
        nodes[-1] = {**nodes[-1], "node": "foreign-node"}
    elif mutation == "duplicate":
        nodes[-1] = dict(nodes[0])
    else:
        nodes.pop()
    policy._write_allocation_state(
        policy._allocation_probe_path(tmp_path, loaded, SANDBOX, candidate),
        payload,
        enforce_root_ownership=False,
    )

    with pytest.raises(policy.PolicyError, match=r"binding drifted|node drifted"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            candidate_binding=binding,
            runtime_attestation=runtime_attestation,
            expected_pool="oldlab",
            expected_concurrency=1,
        )


def test_allocation_matrix_resume_selects_only_unfinished_nodes() -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "6" * 40
    binding = {
        "repository": {"candidate_tree": "5" * 40},
        "worker_env": {"uid": 501, "gid": 20},
    }
    collected_at, expires_at = _generation_window()
    runtime_attestation = {
        "bundle_id": "3" * 64,
        "sandbox": SANDBOX,
        "receipt_sha256": "4" * 64,
        "receipt_collected_at": collected_at,
        "receipt_expires_at": expires_at,
        "proof_expires_at": expires_at,
        "candidate_sha": candidate,
        "candidate_tree": "5" * 40,
    }
    matrix = policy._new_allocation_matrix(
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        binding=binding,
        runtime_attestation=runtime_attestation,
        batch_uid=501,
        batch_gid=20,
        expected_pool="oldlab",
        expected_concurrency=1,
    )
    matrix["nodes"][loaded.allowed_nodes[0]]["status"] = "completed"  # type: ignore[index]
    matrix["nodes"][loaded.allowed_nodes[1]]["status"] = "failed"  # type: ignore[index]

    unfinished = policy._unfinished_allocation_nodes(matrix, loaded)

    assert loaded.allowed_nodes[0] not in unfinished
    assert unfinished == loaded.allowed_nodes[1:]


def test_completed_crash_replays_original_attempt_before_deleting_result(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "c" * 40
    node = loaded.allowed_nodes[0]
    batch_uid = os.geteuid()
    batch_gid = os.getegid()
    worker_env = tmp_path / "private" / "worker.env"
    worker_env.parent.mkdir(mode=0o700)
    binding = {
        "repository": {
            "path": str(tmp_path / "candidate"),
            "candidate_sha": candidate,
            "candidate_tree": "d" * 40,
            "tracked_files": 10,
        },
        "worker_env": {
            "path": str(worker_env),
            "device": 1,
            "inode": 2,
            "uid": batch_uid,
            "gid": batch_gid,
            "sha256": "e" * 64,
            "keys": ["LOOM_WORKER_TOKEN_FILE"],
        },
    }
    collected_at, expires_at = _generation_window()
    runtime_attestation = {
        "bundle_id": "e" * 64,
        "sandbox": SANDBOX,
        "receipt_sha256": "f" * 64,
        "receipt_collected_at": collected_at,
        "receipt_expires_at": expires_at,
        "proof_expires_at": expires_at,
        "candidate_sha": candidate,
        "candidate_tree": "d" * 40,
    }
    matrix = policy._new_allocation_matrix(
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        binding=binding,
        runtime_attestation=runtime_attestation,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
        expected_pool="oldlab",
        expected_concurrency=1,
    )
    matrix["nodes"][node]["attempts"] = 1
    matrix_path = policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate)
    result_path = policy._allocation_result_path(
        worker_env,
        loaded,
        SANDBOX,
        candidate,
        node,
    )
    policy._prepare_allocation_result_path(
        result_path,
        worker_env=worker_env,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
    )
    job_id = "789"
    node_result = {
        "schema_version": 1,
        "sandbox": SANDBOX,
        "account": loaded.child_accounts[0],
        "candidate_sha": candidate,
        "candidate_tree": "d" * 40,
        "host": loaded.host_aliases[node],
        "env_device": 7,
        "env_inode": 2,
        "env_sha256": "e" * 64,
        "pool": "oldlab",
        "concurrency": 1,
        "docker_cgroup_driver": loaded.docker_cgroup_driver,
        "job_id": job_id,
        "cgroup_parent": f"/sys/fs/cgroup/slurm/job_{job_id}",
        "cgroup_guard_verified": True,
        "compose_verified": True,
    }
    policy._write_allocation_state(
        result_path,
        node_result,
        enforce_root_ownership=False,
    )
    job_name = policy._allocation_job_name(
        SANDBOX,
        candidate,
        node,
        1,
        generation_id=policy._allocation_generation_id(runtime_attestation),
    )
    rows = [
        [
            job_id,
            job_name,
            "COMPLETED",
            node,
            "cpu=1,mem=256M",
            loaded.child_accounts[0],
            loaded.users[0],
            loaded.cluster,
            loaded.qos,
        ],
        [
            f"{job_id}.0",
            "srun",
            "COMPLETED",
            node,
            "cpu=1,mem=256M",
            loaded.child_accounts[0],
            loaded.users[0],
            loaded.cluster,
            loaded.qos,
        ],
    ]

    policy._replay_completed_allocation_probe(
        matrix_path,
        matrix,
        loaded,
        sandbox=SANDBOX,
        node=node,
        candidate_sha=candidate,
        candidate_root=tmp_path / "candidate",
        worker_env=worker_env,
        binding=binding,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
        expected_pool="oldlab",
        expected_concurrency=1,
        job_id=job_id,
        recovered_rows=rows,
        enforce_root_ownership=False,
    )

    persisted = policy._load_allocation_state(
        matrix_path,
        enforce_root_ownership=False,
    )
    assert persisted is not None
    row = persisted["nodes"][node]
    assert row["status"] == "completed"
    assert row["attempts"] == 1
    assert row["job_id"] == job_id
    assert row["job_name"] == job_name
    assert row["evidence"]["compute_check"] == node_result
    assert not result_path.exists()


def test_allocation_matrix_one_node_failure_has_no_final_pass(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "5" * 40
    binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    target = policy._allocation_probe_path(tmp_path, loaded, SANDBOX, candidate)
    target.unlink()
    matrix_path = policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate)
    matrix = policy._load_allocation_state(
        matrix_path,
        enforce_root_ownership=False,
    )
    assert matrix is not None
    failed_node = loaded.allowed_nodes[-1]
    matrix["nodes"][failed_node]["status"] = "failed"
    matrix["nodes"][failed_node]["evidence"] = {
        "node": failed_node,
        "terminal": True,
        "failure": "bounded failure",
    }
    matrix["phase"] = "failed"
    policy._write_allocation_state(
        matrix_path,
        matrix,
        enforce_root_ownership=False,
    )

    with pytest.raises(policy.PolicyError, match="not complete"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            candidate_binding=binding,
            runtime_attestation=runtime_attestation,
            expected_pool="oldlab",
            expected_concurrency=1,
        )
    assert not target.exists()


def test_allocation_matrix_readback_rejects_runtime_receipt_drift(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "4" * 40
    binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    drifted = dict(runtime_attestation)
    drifted["domain_generation"] = 8

    with pytest.raises(policy.PolicyError, match="journal binding drifted"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            candidate_binding=binding,
            runtime_attestation=drifted,
            expected_pool="oldlab",
            expected_concurrency=1,
        )


def test_legacy_runtime_receipt_path_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate_sha = "2" * 40
    candidate_tree = "1" * 40
    candidate_root = Path("/shared_work/loom/candidates/sandboxes/qianyi") / candidate_sha
    worker_env = (
        Path("/shared_work/loom/runtime/sandboxes/qianyi") / candidate_sha / "worker-oldlab.env"
    )
    combined_root = tmp_path / "combined"
    domain_root = tmp_path / "domains"
    fleet_root = tmp_path / "fleet"
    monkeypatch.setattr(policy, "_COMBINED_RUNTIME_ATTESTATION_ROOT", combined_root)
    monkeypatch.setattr(policy, "_DOMAIN_RUNTIME_ATTESTATION_ROOT", domain_root)
    monkeypatch.setattr(policy, "_FLEET_ATTESTATION_ROOT", fleet_root)
    published_at = datetime.now(UTC)
    expires_at = published_at + timedelta(minutes=15)
    signature = b"domain-signature"
    manifest_path = domain_root / "qianyi" / candidate_sha / "oldlab.json"
    signature_path = manifest_path.with_suffix(".sig")
    manifest: dict[str, object] = {
        "candidate": {
            "sha": candidate_sha,
            "tree": candidate_tree,
            "path": str(candidate_root),
        },
        "runtime_env": {"path": str(worker_env)},
        "publisher": {
            "generation": 9,
            "published_at": published_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
        "eligible_peers": [
            {
                "hostname": loaded.host_aliases[node],
                "candidate_inode": 101,
                "env_inode": 202,
                "result": "verified",
            }
            for node in loaded.allowed_nodes
        ],
    }
    manifest["payload_sha256"] = hashlib.sha256(
        policy._canonical_json_bytes(manifest),
    ).hexdigest()
    policy._atomic_write(
        manifest_path,
        policy._canonical_json_bytes(manifest).decode() + "\n",
        mode=0o644,
    )
    policy._atomic_write(
        signature_path,
        base64.b64encode(signature).decode() + "\n",
        mode=0o644,
    )
    fleet_nodes = [
        "oldlab-1",
        "oldlab-2",
        "oldlab-3",
        "oldlab-4",
        "oldlab-5",
        "trt-gb10-1",
        "trt-gb10-2",
        "trt-gb10-3",
        "trt-gb10-4",
        "trt-gb10-5",
        "trt-gb10-6",
        "trt-gb10-8",
        "trt-gb10-9",
        "trt-gb10-10",
        "trt-gb10-11",
        "trt-gb10-12",
        "trt-gb10-13",
        "trt-gb10-14",
        "trt-gb10-15",
    ]
    fleet_path = fleet_root / "qianyi" / candidate_sha / "fleet.json"
    fleet: dict[str, object] = {
        "candidate_sha": candidate_sha,
        "generated_at": published_at.isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        "eligible_nodes": fleet_nodes,
        "nodes": {node: {"candidate_sha": candidate_sha} for node in fleet_nodes},
    }
    fleet["payload_sha256"] = (
        "sha256:"
        + hashlib.sha256(
            policy._canonical_json_bytes(fleet),
        ).hexdigest()
    )
    policy._atomic_write(
        fleet_path,
        policy._canonical_json_bytes(fleet).decode() + "\n",
        mode=0o600,
    )
    receipt_path = combined_root / "qianyi" / candidate_sha / "combined.json"
    receipt: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-combined-activation",
        "sandbox": "qianyi",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "collector": {
            "hostname": "trt-eai-oldlab-2",
            "collected_at": published_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
        "fleet_attestation": {
            "path": str(fleet_path),
            "payload_sha256": fleet["payload_sha256"],
            "generated_at": fleet["generated_at"],
            "expires_at": fleet["expires_at"],
        },
        "domains": {
            "oldlab": {
                "manifest_path": str(manifest_path),
                "signature_path": str(signature_path),
                "payload_sha256": manifest["payload_sha256"],
                "signature_sha256": hashlib.sha256(signature).hexdigest(),
                "key_id": "a" * 64,
                "generation": 9,
                "published_at": published_at.isoformat(),
                "expires_at": expires_at.isoformat(),
            },
            "gb10": {
                "manifest_path": str(domain_root / "qianyi" / candidate_sha / "gb10.json"),
                "signature_path": str(domain_root / "qianyi" / candidate_sha / "gb10.sig"),
                "payload_sha256": "b" * 64,
                "signature_sha256": "c" * 64,
                "key_id": "d" * 64,
                "generation": 7,
                "published_at": published_at.isoformat(),
                "expires_at": expires_at.isoformat(),
            },
        },
    }
    receipt["payload_sha256"] = hashlib.sha256(
        policy._canonical_json_bytes(receipt),
    ).hexdigest()
    policy._atomic_write(
        receipt_path,
        policy._canonical_json_bytes(receipt).decode() + "\n",
        mode=0o600,
    )

    with pytest.raises(policy.PolicyError, match="runtime attestation input is unavailable"):
        policy._runtime_attestation_binding(
            receipt_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            candidate_root=candidate_root,
            worker_env=worker_env,
            enforce_root_ownership=False,
        )


def _runtime_proof_source_bytes(
    *,
    candidate_sha: str,
    candidate_tree: str,
    combined_root: Path,
    domain_root: Path,
    fleet_root: Path,
    published_at: datetime,
    generations: dict[str, int],
    private_keys: dict[str, Ed25519PrivateKey],
) -> dict[Path, bytes]:
    expires_at = published_at + timedelta(minutes=15)
    fleet: dict[str, object] = {
        "candidate_sha": candidate_sha,
        "generated_at": published_at.isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        "eligible_nodes": list(policy._RUNTIME_FLEET_NODES),
        "nodes": {node: {"candidate_sha": candidate_sha} for node in policy._RUNTIME_FLEET_NODES},
    }
    fleet["payload_sha256"] = (
        "sha256:" + hashlib.sha256(policy._canonical_json_bytes(fleet)).hexdigest()
    )
    fleet_path = fleet_root / "qianyi" / candidate_sha / "fleet.json"
    fleet_reference = {
        "path": str(fleet_path),
        "payload_sha256": fleet["payload_sha256"],
        "generated_at": fleet["generated_at"],
        "expires_at": fleet["expires_at"],
    }
    sources = {fleet_path: policy._canonical_json_bytes(fleet) + b"\n"}
    domain_rows: dict[str, dict[str, object]] = {}
    for domain_name in ("oldlab", "gb10"):
        private_key = private_keys[domain_name]
        public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        manifest: dict[str, object] = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-domain-attestation",
            "domain": domain_name,
            "sandbox": "qianyi",
            "candidate": {
                "sha": candidate_sha,
                "tree": candidate_tree,
                "path": f"/shared_work/loom/candidates/sandboxes/qianyi/{candidate_sha}",
            },
            "runtime_env": {
                "path": (
                    f"/shared_work/loom/runtime/sandboxes/qianyi/{candidate_sha}/"
                    f"worker-{domain_name}.env"
                ),
            },
            "fleet_attestation": fleet_reference,
            "publisher": {
                "hostname": policy._RUNTIME_PROOF_SOURCES[domain_name][1],
                "generation": generations[domain_name],
                "published_at": published_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "signature_algorithm": "ed25519",
                "key_id": hashlib.sha256(public_key).hexdigest(),
            },
            "eligible_peers": [
                {
                    "hostname": hostname,
                    "candidate_inode": 101,
                    "env_inode": 202,
                    "result": "verified",
                }
                for hostname in policy._RUNTIME_DOMAIN_HOSTS[domain_name]
            ],
        }
        manifest["payload_sha256"] = hashlib.sha256(
            policy._canonical_json_bytes(manifest),
        ).hexdigest()
        manifest_bytes = policy._canonical_json_bytes(manifest) + b"\n"
        signature = private_key.sign(manifest_bytes)
        parent = domain_root / "qianyi" / candidate_sha
        manifest_path = parent / f"{domain_name}.json"
        signature_path = parent / f"{domain_name}.sig"
        key_path = Path(
            f"/etc/loom/developer-domain-runtime/attestation-keys/{domain_name}.pub",
        )
        sources[manifest_path] = manifest_bytes
        sources[signature_path] = base64.b64encode(signature) + b"\n"
        sources[key_path] = public_key
        domain_rows[domain_name] = {
            "manifest_path": str(manifest_path),
            "signature_path": str(signature_path),
            "payload_sha256": manifest["payload_sha256"],
            "signature_sha256": hashlib.sha256(signature).hexdigest(),
            "key_id": hashlib.sha256(public_key).hexdigest(),
            "generation": generations[domain_name],
            "published_at": published_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
    receipt: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-combined-activation",
        "sandbox": "qianyi",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "collector": {
            "hostname": "trt-eai-oldlab-2",
            "collected_at": published_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
        "fleet_attestation": fleet_reference,
        "domains": domain_rows,
    }
    receipt["payload_sha256"] = hashlib.sha256(
        policy._canonical_json_bytes(receipt),
    ).hexdigest()
    sources[combined_root / "qianyi" / candidate_sha / "combined.json"] = (
        policy._canonical_json_bytes(receipt) + b"\n"
    )
    return sources


def test_materialized_runtime_proof_verifies_both_signed_domains_and_closed_fleet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate_sha = "8" * 40
    candidate_tree = "9" * 40
    combined_root = tmp_path / "remote-combined"
    domain_root = tmp_path / "remote-domains"
    fleet_root = tmp_path / "remote-fleet"
    monkeypatch.setattr(policy, "_COMBINED_RUNTIME_ATTESTATION_ROOT", combined_root)
    monkeypatch.setattr(policy, "_DOMAIN_RUNTIME_ATTESTATION_ROOT", domain_root)
    monkeypatch.setattr(policy, "_FLEET_ATTESTATION_ROOT", fleet_root)
    publish_attempts = 0

    def publish(source: Path, target: Path) -> None:
        nonlocal publish_attempts
        publish_attempts += 1
        if publish_attempts == 1:
            raise policy.PolicyError("injected publication crash")
        source.rename(target)

    monkeypatch.setattr(policy, "_rename_noreplace", publish)
    published_at = datetime.now(UTC)
    expires_at = published_at + timedelta(minutes=15)
    fleet: dict[str, object] = {
        "candidate_sha": candidate_sha,
        "generated_at": published_at.isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        "eligible_nodes": list(policy._RUNTIME_FLEET_NODES),
        "nodes": {node: {"candidate_sha": candidate_sha} for node in policy._RUNTIME_FLEET_NODES},
    }
    fleet["payload_sha256"] = (
        "sha256:" + hashlib.sha256(policy._canonical_json_bytes(fleet)).hexdigest()
    )
    fleet_path = fleet_root / "qianyi" / candidate_sha / "fleet.json"
    fleet_reference = {
        "path": str(fleet_path),
        "payload_sha256": fleet["payload_sha256"],
        "generated_at": fleet["generated_at"],
        "expires_at": fleet["expires_at"],
    }
    fetched_by_path: dict[Path, bytes] = {
        fleet_path: policy._canonical_json_bytes(fleet) + b"\n",
    }
    domain_rows: dict[str, dict[str, object]] = {}
    for generation, domain_name in enumerate(("oldlab", "gb10"), start=11):
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        manifest: dict[str, object] = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-domain-attestation",
            "domain": domain_name,
            "sandbox": "qianyi",
            "candidate": {
                "sha": candidate_sha,
                "tree": candidate_tree,
                "path": f"/shared_work/loom/candidates/sandboxes/qianyi/{candidate_sha}",
            },
            "runtime_env": {
                "path": (
                    f"/shared_work/loom/runtime/sandboxes/qianyi/{candidate_sha}/"
                    f"worker-{domain_name}.env"
                ),
            },
            "fleet_attestation": fleet_reference,
            "publisher": {
                "hostname": policy._RUNTIME_PROOF_SOURCES[domain_name][1],
                "generation": generation,
                "published_at": published_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "signature_algorithm": "ed25519",
                "key_id": hashlib.sha256(public_key).hexdigest(),
            },
            "eligible_peers": [
                {
                    "hostname": hostname,
                    "candidate_inode": 101,
                    "env_inode": 202,
                    "result": "verified",
                }
                for hostname in policy._RUNTIME_DOMAIN_HOSTS[domain_name]
            ],
        }
        manifest["payload_sha256"] = hashlib.sha256(
            policy._canonical_json_bytes(manifest),
        ).hexdigest()
        manifest_bytes = policy._canonical_json_bytes(manifest) + b"\n"
        signature = private_key.sign(manifest_bytes)
        parent = domain_root / "qianyi" / candidate_sha
        manifest_path = parent / f"{domain_name}.json"
        signature_path = parent / f"{domain_name}.sig"
        key_path = Path(
            f"/etc/loom/developer-domain-runtime/attestation-keys/{domain_name}.pub",
        )
        fetched_by_path[manifest_path] = manifest_bytes
        fetched_by_path[signature_path] = base64.b64encode(signature) + b"\n"
        fetched_by_path[key_path] = public_key
        domain_rows[domain_name] = {
            "manifest_path": str(manifest_path),
            "signature_path": str(signature_path),
            "payload_sha256": manifest["payload_sha256"],
            "signature_sha256": hashlib.sha256(signature).hexdigest(),
            "key_id": hashlib.sha256(public_key).hexdigest(),
            "generation": generation,
            "published_at": published_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
    receipt: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-combined-activation",
        "sandbox": "qianyi",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "collector": {
            "hostname": "trt-eai-oldlab-2",
            "collected_at": published_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
        "fleet_attestation": fleet_reference,
        "domains": domain_rows,
    }
    receipt["payload_sha256"] = hashlib.sha256(
        policy._canonical_json_bytes(receipt),
    ).hexdigest()
    receipt_path = combined_root / "qianyi" / candidate_sha / "combined.json"
    fetched_by_path[receipt_path] = policy._canonical_json_bytes(receipt) + b"\n"

    def fetcher(
        _target: str,
        _hostname: str,
        artifact_id: str,
        *,
        expected_mode: int,
    ) -> bytes:
        assert expected_mode in {0o600, 0o644}
        name = artifact_id.rsplit("/", 1)[-1]
        return next(value for path, value in fetched_by_path.items() if path.name == name)

    with pytest.raises(policy.PolicyError, match="injected publication crash"):
        policy.materialize_runtime_proof(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            fetcher=fetcher,
        )
    result = policy.materialize_runtime_proof(
        tmp_path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        fetcher=fetcher,
    )
    repeated = policy.materialize_runtime_proof(
        tmp_path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        fetcher=fetcher,
    )
    binding = policy._runtime_attestation_binding(
        tmp_path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        candidate_root=Path("/candidate"),
        worker_env=Path("/worker.env"),
        enforce_root_ownership=False,
    )

    assert binding["bundle_id"] == result["bundle_id"]
    assert repeated["bundle_id"] == result["bundle_id"]
    assert set(binding["domains"]) == {"oldlab", "gb10"}
    assert binding["fleet_nodes"] == list(policy._RUNTIME_FLEET_NODES)
    proof = Path(result["proof_path"]).parent
    assert {item.name for item in proof.iterdir()} == policy._RUNTIME_PROOF_FILE_NAMES
    assert all(stat.S_IMODE(item.stat().st_mode) == 0o600 for item in proof.iterdir())
    policy._atomic_write(proof / "gb10.sig", "Zm9yZWlnbg==\n", mode=0o600)
    with pytest.raises(policy.PolicyError, match="local digest drifted"):
        policy._runtime_attestation_binding(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            candidate_root=Path("/candidate"),
            worker_env=Path("/worker.env"),
            enforce_root_ownership=False,
        )


def test_rotated_receipt_recovers_prior_crash_before_fetch_and_rename_preserves_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate_sha = "6" * 40
    candidate_tree = "7" * 40
    combined_root = tmp_path / "remote-combined"
    domain_root = tmp_path / "remote-domains"
    fleet_root = tmp_path / "remote-fleet"
    monkeypatch.setattr(policy, "_COMBINED_RUNTIME_ATTESTATION_ROOT", combined_root)
    monkeypatch.setattr(policy, "_DOMAIN_RUNTIME_ATTESTATION_ROOT", domain_root)
    monkeypatch.setattr(policy, "_FLEET_ATTESTATION_ROOT", fleet_root)
    monkeypatch.setattr(policy, "_rename_noreplace", lambda source, target: source.rename(target))
    keys = {
        "oldlab": Ed25519PrivateKey.generate(),
        "gb10": Ed25519PrivateKey.generate(),
    }
    started = datetime.now(UTC) - timedelta(seconds=3)
    current_sources = _runtime_proof_source_bytes(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        combined_root=combined_root,
        domain_root=domain_root,
        fleet_root=fleet_root,
        published_at=started,
        generations={"oldlab": 11, "gb10": 12},
        private_keys=keys,
    )

    def fetcher(
        _target: str,
        _hostname: str,
        artifact_id: str,
        *,
        expected_mode: int,
    ) -> bytes:
        assert expected_mode in {0o600, 0o644}
        name = artifact_id.rsplit("/", 1)[-1]
        return next(value for path, value in current_sources.items() if path.name == name)

    first = policy.materialize_runtime_proof(
        tmp_path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        fetcher=fetcher,
    )
    current_sources = _runtime_proof_source_bytes(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        combined_root=combined_root,
        domain_root=domain_root,
        fleet_root=fleet_root,
        published_at=started + timedelta(seconds=1),
        generations={"oldlab": 21, "gb10": 22},
        private_keys=keys,
    )

    def fail_publish(_source: Path, _target: Path) -> None:
        raise policy.PolicyError("injected rename failure")

    monkeypatch.setattr(policy, "_rename_noreplace", fail_publish)
    with pytest.raises(policy.PolicyError, match="injected rename failure"):
        policy.materialize_runtime_proof(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            fetcher=fetcher,
        )
    high_water_path = policy._runtime_proof_high_water_path(tmp_path, loaded, SANDBOX)
    after_failure = policy._load_canonical_attestation_json(
        high_water_path,
        expected_mode=0o600,
        enforce_root_ownership=False,
    )
    assert after_failure["bundle_id"] == first["bundle_id"]

    current_sources = _runtime_proof_source_bytes(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        combined_root=combined_root,
        domain_root=domain_root,
        fleet_root=fleet_root,
        published_at=started + timedelta(seconds=2),
        generations={"oldlab": 31, "gb10": 32},
        private_keys=keys,
    )
    monkeypatch.setattr(policy, "_rename_noreplace", lambda source, target: source.rename(target))
    rotated = policy.materialize_runtime_proof(
        tmp_path,
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        fetcher=fetcher,
    )
    recovered = policy._load_canonical_attestation_json(
        high_water_path,
        expected_mode=0o600,
        enforce_root_ownership=False,
    )

    assert rotated["bundle_id"] != first["bundle_id"]
    assert recovered["bundle_id"] == rotated["bundle_id"]
    assert not policy._path_exists_without_following(
        policy._runtime_proof_transaction_path(tmp_path, loaded, SANDBOX),
    )


def test_foreign_runtime_proof_journal_fails_before_fetch_without_cleanup(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate_sha = "4" * 40
    candidate_tree = "5" * 40
    bundle_id = "a" * 64
    foreign_stage = tmp_path / "foreign-stage"
    foreign_stage.mkdir(mode=0o700)
    marker = foreign_stage / "keep"
    marker.write_text("foreign\n", encoding="utf-8")
    marker.chmod(0o600)
    transaction_path = policy._runtime_proof_transaction_path(tmp_path, loaded, SANDBOX)
    policy._write_journal(
        transaction_path,
        {
            "schema_version": 1,
            "kind": "loom.developer-runtime-proof-transaction",
            "cluster": loaded.cluster,
            "submit_node": loaded.submit_host,
            "submit_hostname": loaded.host_aliases[loaded.submit_host],
            "sandbox": loaded.users[0],
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "bundle_id": bundle_id,
            "receipt_sha256": "b" * 64,
            "stage": str(foreign_stage),
            "final": str(tmp_path / "foreign-final"),
            "phase": "prepared",
        },
    )
    fetched = False

    def fetcher(*_args: object, **_kwargs: object) -> bytes:
        nonlocal fetched
        fetched = True
        raise AssertionError("foreign recovery must stop before fetch")

    with pytest.raises(policy.PolicyError, match="foreign runtime proof transaction"):
        policy.materialize_runtime_proof(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            fetcher=fetcher,
        )

    assert fetched is False
    assert marker.read_text(encoding="utf-8") == "foreign\n"
    assert policy._path_exists_without_following(transaction_path)


def test_runtime_receipt_cli_option_is_removed() -> None:
    with pytest.raises(SystemExit):
        policy._parser().parse_args(
            [
                "allocation-probe",
                "--profile",
                str(PROFILE),
                "--runtime-receipt",
                "/tmp/foreign.json",
            ],
        )


def test_runtime_proof_fetch_uses_fixed_node_authority_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: tuple[str, ...] = ()
    content = b'{"proof":"bounded"}\n'
    candidate_sha = "a" * 40
    candidate_tree = "b" * 40
    artifact_id = f"runtime-proof/v1/qianyi/{candidate_sha}/{candidate_tree}/artifact/oldlab.json"

    def bounded(
        argv: tuple[str, ...],
        *,
        input_bytes: bytes | None,
        timeout: float,
        max_bytes: int,
    ) -> bytes:
        nonlocal captured
        captured = argv
        assert timeout == 120
        assert max_bytes == 1536 * 1024
        assert input_bytes is not None
        request = json.loads(input_bytes)
        assert input_bytes == policy._canonical_json_bytes(request) + b"\n"
        assert request["action"] == "export-runtime-proof-artifact"
        assert request["node"] == "oldlab-1"
        assert base64.b64decode(request["payload_base64"], validate=True).decode() == artifact_id
        response = {
            "schema_version": 1,
            "request_id": request["request_id"],
            "status": "succeeded",
            "result": {
                "schema_version": 1,
                "operation": "export-runtime-proof-artifact",
                "artifact_id": artifact_id,
                "artifact_name": "oldlab.json",
                "node": "oldlab-1",
                "hostname": "trt-eai-oldlab-1",
                "domain": "oldlab",
                "sandbox": "qianyi",
                "candidate_sha": candidate_sha,
                "candidate_tree": candidate_tree,
                "content_size": len(content),
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "content_base64": base64.b64encode(content).decode(),
            },
        }
        return policy._canonical_json_bytes(response) + b"\n"

    monkeypatch.setattr(policy, "_run_bounded_stdout", bounded)

    fetched = policy._fetch_runtime_proof_source(
        "oldlab-1",
        "trt-eai-oldlab-1",
        artifact_id,
        expected_mode=0o644,
    )

    assert fetched == content
    assert captured == (
        "/usr/bin/python3",
        "-I",
        "/usr/local/libexec/loom-developer-sandbox-node-transport",
        "invoke",
        "--node",
        "oldlab-1",
        "--verb",
        "check",
    )
    source = Path(policy.__file__).read_text(encoding="utf-8")
    assert "runtime-proof-known-hosts" not in source
    assert "runtime-proof-fetch" not in source
    assert '"ssh"' not in source


def test_bounded_transport_runner_drops_ambient_python_and_ssh_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        captured["argv"] = argv
        captured["env"] = kwargs["env"]
        output = kwargs["stdout"]
        assert hasattr(output, "write")
        output.write(b"bounded\n")
        output.flush()
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setenv("PYTHONPATH", "/tmp/ambient")
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/agent")
    monkeypatch.setattr(policy.subprocess, "run", run)

    result = policy._run_bounded_stdout(
        ("/usr/bin/python3", "-I", "/fixed/transport"),
        input_bytes=b"{}\n",
        timeout=1,
        max_bytes=1024,
    )

    assert result == b"bounded\n"
    assert captured["env"] == {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }


def test_runtime_proof_fetch_rejects_unbound_artifact_id_before_transport() -> None:
    with pytest.raises(policy.PolicyError, match="artifact identity"):
        policy._fetch_runtime_proof_source(
            "oldlab-1",
            "trt-eai-oldlab-1",
            "/var/lib/foreign.json",
            expected_mode=0o644,
        )


def test_allocation_readback_waits_for_writer_and_never_returns_invalidated_artifact(
    tmp_path: Path,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "3" * 40
    binding, runtime_attestation, payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    target = policy._allocation_probe_path(tmp_path, loaded, SANDBOX, candidate)
    old_created_at = payload["created_at"]
    writer_invalidated = threading.Event()
    allow_writer_finish = threading.Event()
    reader_started = threading.Event()
    reader_finished = threading.Event()
    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            with policy._allocation_probe_lock(
                tmp_path,
                loaded,
                SANDBOX,
                candidate,
                enforce_root_ownership=False,
            ):
                target.unlink()
                policy._fsync_directory(target.parent)
                writer_invalidated.set()
                if not allow_writer_finish.wait(timeout=2):
                    raise RuntimeError("writer was not released")
                payload["created_at"] = datetime.now(UTC).isoformat()
                policy._write_allocation_state(
                    target,
                    payload,
                    enforce_root_ownership=False,
                )
        except BaseException as exc:
            errors.append(exc)

    def reader() -> None:
        try:
            if not writer_invalidated.wait(timeout=2):
                raise RuntimeError("writer did not invalidate the artifact")
            reader_started.set()
            results.append(
                policy.allocation_probe_readback(
                    tmp_path,
                    loaded,
                    sandbox=SANDBOX,
                    candidate_sha=candidate,
                    candidate_binding=binding,
                    runtime_attestation=runtime_attestation,
                    expected_pool="oldlab",
                    expected_concurrency=1,
                ),
            )
            reader_finished.set()
        except BaseException as exc:
            errors.append(exc)

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    writer_thread.start()
    assert writer_invalidated.wait(timeout=2)
    reader_thread.start()
    assert reader_started.wait(timeout=2)
    assert not reader_finished.wait(timeout=0.1)
    allow_writer_finish.set()
    writer_thread.join(timeout=2)
    reader_thread.join(timeout=2)

    assert not writer_thread.is_alive()
    assert not reader_thread.is_alive()
    assert errors == []
    assert len(results) == 1
    assert results[0]["created_at"] != old_created_at


def test_cancelled_by_uid_is_a_terminal_allocation_state() -> None:
    rows = [
        [
            "123",
            "probe",
            "CANCELLED by 501",
            "node-1",
            "cpu=1",
            "account",
            "user",
            "cluster",
            "qos",
        ],
    ]

    assert policy._base_job_state(rows, "123") == "CANCELLED"
    assert policy._base_job_state(rows, "123") in policy._TERMINAL_JOB_STATES


def test_pending_queue_reason_does_not_impersonate_an_allocated_node() -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "2" * 40
    payload = _allocation_inflight_payload(loaded, candidate)
    pending = _allocation_queue_row(loaded, payload, state="PENDING")
    pending[-1] = "(Resources)"

    policy._validate_probe_queue_row(pending, payload, loaded, sandbox=SANDBOX)

    pending[3] = "foreign-user"
    with pytest.raises(policy.PolicyError, match="identity drifted"):
        policy._validate_probe_queue_row(pending, payload, loaded, sandbox=SANDBOX)


def test_allocation_accounting_rejects_effective_qos_drift() -> None:
    loaded = policy.load_profile(PROFILE)
    payload = _allocation_inflight_payload(loaded, "1" * 40)
    row = _allocation_accounting_row(loaded, payload, "COMPLETED")
    row[8] = "foreign-qos"

    with pytest.raises(policy.PolicyError, match="identity drifted"):
        policy._validate_probe_base_row(row, payload, loaded, sandbox=SANDBOX)


def test_final_created_at_cannot_refresh_stale_completed_nodes(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "0" * 40
    binding, runtime_attestation, payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    matrix_path = policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate)
    matrix = policy._load_allocation_state(
        matrix_path,
        enforce_root_ownership=False,
    )
    assert matrix is not None
    stale = (datetime.now(UTC) - timedelta(minutes=16)).isoformat()
    first = loaded.allowed_nodes[0]
    matrix["nodes"][first]["evidence"]["completed_at"] = stale
    payload["nodes"][0]["completed_at"] = stale
    payload["created_at"] = datetime.now(UTC).isoformat()
    policy._write_allocation_state(
        matrix_path,
        matrix,
        enforce_root_ownership=False,
    )
    policy._write_allocation_state(
        policy._allocation_probe_path(tmp_path, loaded, SANDBOX, candidate),
        payload,
        enforce_root_ownership=False,
    )

    with pytest.raises(policy.PolicyError, match=r"generation window|stale"):
        policy.allocation_probe_readback(
            tmp_path,
            loaded,
            sandbox=SANDBOX,
            candidate_sha=candidate,
            candidate_binding=binding,
            runtime_attestation=runtime_attestation,
            expected_pool="oldlab",
            expected_concurrency=1,
        )


def test_receipt_rotation_archives_old_generation_and_resets_all_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "f" * 40
    binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    matrix_path = policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate)
    matrix = policy._load_allocation_state(
        matrix_path,
        enforce_root_ownership=False,
    )
    assert matrix is not None
    next_collected, next_expires = _generation_window()
    rotated = {
        **runtime_attestation,
        "bundle_id": "9" * 64,
        "receipt_sha256": "e" * 64,
        "receipt_collected_at": next_collected,
        "receipt_expires_at": next_expires,
        "proof_expires_at": next_expires,
    }
    assert policy._allocation_matrix_requires_reset(
        matrix,
        loaded,
        runtime_attestation=rotated,
        now=datetime.now(UTC),
    )
    monkeypatch.setattr(policy, "_require_root_private_directory", lambda _path: None)
    policy._archive_allocation_generation(
        tmp_path,
        loaded,
        SANDBOX,
        candidate,
        matrix,
    )

    assert not matrix_path.exists()
    assert not policy._allocation_probe_path(
        tmp_path,
        loaded,
        SANDBOX,
        candidate,
    ).exists()
    assert len(list(matrix_path.parent.glob("*.archived"))) == 2
    replacement = policy._new_allocation_matrix(
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        binding=binding,
        runtime_attestation=rotated,
        batch_uid=501,
        batch_gid=20,
        expected_pool="oldlab",
        expected_concurrency=1,
    )
    assert policy._unfinished_allocation_nodes(replacement, loaded) == loaded.allowed_nodes
    node = loaded.allowed_nodes[0]
    old_name = policy._allocation_job_name(
        SANDBOX,
        candidate,
        node,
        1,
        generation_id=str(matrix["generation_id"]),
    )
    new_name = policy._allocation_job_name(
        SANDBOX,
        candidate,
        node,
        1,
        generation_id=str(replacement["generation_id"]),
    )
    assert old_name != new_name


def test_allocation_generation_binds_full_bundle_not_receipt_prefix(tmp_path: Path) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "a" * 40
    binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    same_receipt_new_bundle = {
        **runtime_attestation,
        "bundle_id": "b" * 64,
    }

    assert policy._allocation_generation_id(runtime_attestation) == "a" * 64
    assert policy._allocation_generation_id(same_receipt_new_bundle) == "b" * 64
    matrix = policy._new_allocation_matrix(
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        binding=binding,
        runtime_attestation=runtime_attestation,
        batch_uid=501,
        batch_gid=20,
        expected_pool="oldlab",
        expected_concurrency=1,
    )
    assert policy._allocation_matrix_requires_reset(
        matrix,
        loaded,
        runtime_attestation=same_receipt_new_bundle,
        now=datetime.now(UTC),
    )


@pytest.mark.parametrize(
    "runtime_attestation",
    (
        {"receipt_sha256": "c" * 64},
        {"bundle_id": "d" * 12, "receipt_sha256": "c" * 64},
    ),
)
def test_allocation_generation_rejects_missing_or_truncated_bundle(
    runtime_attestation: dict[str, str],
) -> None:
    with pytest.raises(policy.PolicyError, match="bundle ID"):
        policy._allocation_generation_id(runtime_attestation)


def test_legacy_receipt_generation_is_only_archivable_when_bundle_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "b" * 40
    _binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    matrix_path = policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate)
    matrix = policy._load_allocation_state(matrix_path, enforce_root_ownership=False)
    assert matrix is not None
    matrix["generation_id"] = str(runtime_attestation["receipt_sha256"])[:12]
    policy._write_allocation_state(
        matrix_path,
        matrix,
        enforce_root_ownership=False,
    )
    monkeypatch.setattr(policy, "_require_root_private_directory", lambda _path: None)

    policy._archive_allocation_generation(
        tmp_path,
        loaded,
        SANDBOX,
        candidate,
        matrix,
    )

    assert not matrix_path.exists()
    assert len(list(matrix_path.parent.glob("*.archived"))) == 2
    foreign = dict(matrix)
    foreign["generation_id"] = "0" * 12
    with pytest.raises(policy.PolicyError, match="cannot be archived safely"):
        policy._archive_allocation_generation(
            tmp_path,
            loaded,
            SANDBOX,
            candidate,
            foreign,
        )


def test_allocation_rejects_proof_expiring_inside_timeout_margin() -> None:
    now = datetime.now(UTC)
    runtime_attestation = {
        "bundle_id": "e" * 64,
        "receipt_collected_at": (now - timedelta(seconds=1)).isoformat(),
        "proof_expires_at": (
            now
            + timedelta(seconds=180)
            + policy._ALLOCATION_PROOF_EXPIRY_MARGIN
            - timedelta(microseconds=1)
        ).isoformat(),
    }

    with pytest.raises(policy.PolicyError, match="timeout safety window"):
        policy._require_allocation_proof_freshness(
            runtime_attestation,
            timeout_seconds=180,
            now=now,
        )


def test_missing_completed_result_is_durably_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "e" * 40
    binding, runtime_attestation, _payload = _allocation_matrix_fixture(
        tmp_path,
        loaded,
        candidate,
    )
    matrix_path = policy._allocation_matrix_path(tmp_path, loaded, SANDBOX, candidate)
    matrix = policy._load_allocation_state(
        matrix_path,
        enforce_root_ownership=False,
    )
    assert matrix is not None
    node = loaded.allowed_nodes[0]
    matrix["nodes"][node]["status"] = "failed"
    matrix["nodes"][node]["evidence"] = None
    matrix["phase"] = "failed"
    policy._write_allocation_state(
        matrix_path,
        matrix,
        enforce_root_ownership=False,
    )
    monkeypatch.setattr(
        policy,
        "_replay_completed_allocation_probe",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            policy.PolicyError("allocation node result is unavailable"),
        ),
    )

    replayed = policy._replay_completed_or_mark_retry(
        matrix_path,
        matrix,
        loaded,
        sandbox=SANDBOX,
        node=node,
        candidate_sha=candidate,
        candidate_root=Path("/candidate"),
        worker_env=Path("/private/worker.env"),
        binding=binding,
        batch_uid=501,
        batch_gid=20,
        expected_pool="oldlab",
        expected_concurrency=1,
        job_id="101",
        recovered_rows=[],
        enforce_root_ownership=False,
    )

    assert replayed is False
    persisted = policy._load_allocation_state(
        matrix_path,
        enforce_root_ownership=False,
    )
    assert persisted is not None
    assert persisted["nodes"][node]["status"] == "pending"
    assert persisted["nodes"][node]["attempts"] == 1
    assert node in policy._unfinished_allocation_nodes(persisted, loaded)
    assert persisted["runtime_attestation"] == runtime_attestation


def test_allocation_writer_uses_sandbox_candidate_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    candidate = "d" * 40
    events: list[str] = []

    @contextmanager
    def candidate_lock(  # type: ignore[no-untyped-def]
        _root: Path,
        _profile: policy.Profile,
        _sandbox: str,
        _candidate: str,
    ):
        assert events == []
        events.append("candidate-enter")
        yield
        events.append("candidate-exit")

    monkeypatch.setattr(policy, "_allocation_probe_lock", candidate_lock)
    monkeypatch.setattr(policy.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        policy,
        "_run_allocation_probe_transaction",
        lambda *_args, **_kwargs: {"converged": True},
    )

    policy.run_allocation_probe(
        Path("/"),
        loaded,
        sandbox=SANDBOX,
        candidate_sha=candidate,
        candidate_root=Path("/candidate"),
        worker_env=Path("/private/worker.env"),
        batch_uid=501,
        batch_gid=20,
        expected_pool="oldlab",
        expected_concurrency=1,
    )

    assert events == [
        "candidate-enter",
        "candidate-exit",
    ]


def test_public_live_readback_holds_domain_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = policy.load_profile(PROFILE)
    events: list[str] = []

    @contextmanager
    def domain_lock(_root: Path, _profile: policy.Profile):  # type: ignore[no-untyped-def]
        events.append("domain-enter")
        yield
        events.append("domain-exit")

    def unlocked(*_args: object, **_kwargs: object) -> dict[str, object]:
        assert events == ["domain-enter"]
        events.append("readback")
        return {"converged": True}

    monkeypatch.setattr(policy, "_domain_lock", domain_lock)
    monkeypatch.setattr(policy, "_live_readback_unlocked", unlocked)

    assert policy.live_readback(
        tmp_path,
        loaded,
        sandbox=None,
        candidate_sha="c" * 40,
        candidate_bindings=policy._offline_candidate_bindings(loaded, "c" * 40),
        require_probe=False,
    ) == {"converged": True}
    assert events == ["domain-enter", "readback", "domain-exit"]
