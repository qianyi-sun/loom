from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import render_shared_capacity_supervisor_service as renderer
from scripts.ops import shared_capacity_supervisor as supervisor

from loom_control_plane.shared_capacity_broker import (
    BrokerBudgets,
    SandboxId,
    SharedCapacityBroker,
)

ROOT = Path(__file__).resolve().parents[2]
SHA_A = "a" * 40
SHA_B = "b" * 40
NOW = datetime(2026, 7, 28, 18, 0, tzinfo=UTC)


def _write(path: Path, payload: object, *, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(
        (payload if isinstance(payload, str) else json.dumps(payload, sort_keys=True) + "\n"),
        encoding="utf-8",
    )
    path.chmod(mode)


def _fixture(tmp_path: Path) -> tuple[supervisor.SupervisorConfig, Path]:
    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    config_path = tmp_path / "supervisor.toml"
    _write(
        config_path,
        "\n".join(
            (
                "schema_version = 1",
                f'state_db = "{authority / "broker.sqlite3"}"',
                f'handoff_dir = "{authority / "handoffs"}"',
                f'observation_dir = "{authority / "observations"}"',
                f'supervisor_state_path = "{authority / "supervisor-state.json"}"',
                f'audit_path = "{authority / "supervisor-audit.jsonl"}"',
                f'evidence_path = "{authority / "evidence/latest.json"}"',
                "global_slot_budget = 132",
                "global_pending_slot_budget = 34",
                "instances = [",
                '  "qianyi-gb10", "qianyi-oldlab",',
                '  "hongjian-gb10", "hongjian-oldlab",',
                '  "devansh-gb10", "devansh-oldlab",',
                "]",
                "[pool_slot_budgets]",
                "gb10 = 112",
                "oldlab = 20",
                "[pool_pending_slot_budgets]",
                "gb10 = 24",
                "oldlab = 10",
                "",
            ),
        ),
    )
    return supervisor.load_config(config_path), config_path


def _request(
    config: supervisor.SupervisorConfig,
    sandbox: SandboxId,
    *,
    pool: str,
    candidate_sha: str,
    target_slots: int,
    key: str,
) -> str:
    broker = SharedCapacityBroker(config.state_db, clock=lambda: NOW)
    request, _ = broker.request_capacity(
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        pool=pool,
        min_slots=0,
        target_slots=target_slots,
        ttl_seconds=7200,
        purpose="large-batch-runtime-validation",
        preemptible=True,
        idempotency_key=key,
    )
    return request.id


def _handoff(config: supervisor.SupervisorConfig, instance: str) -> dict[str, Any]:
    return json.loads((config.handoff_dir / "current" / f"{instance}.json").read_text())


def _assert_complete_generation(config: supervisor.SupervisorConfig) -> dict[str, Any]:
    current = config.handoff_dir / "current"
    manifest = json.loads((current / "manifest.json").read_text())
    assert set(manifest["instances"]) == set(supervisor._EXPECTED_INSTANCES)
    for instance, entry in manifest["instances"].items():
        path = current / f"{instance}.json"
        if entry["status"] == "absent":
            assert not path.exists()
        else:
            canonical = (
                json.dumps(
                    json.loads(path.read_text()),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
            assert hashlib.sha256(canonical.encode()).hexdigest() == entry["digest"]
    return manifest


def _observation(
    config: supervisor.SupervisorConfig,
    instance: str,
    *,
    handoff: dict[str, Any],
    pending: int = 0,
    active: int = 0,
    draining: int = 0,
    terminal: int = 0,
    sequence: int = 1,
    observed_at: datetime = NOW,
    capacity_lease_state: str | None = None,
) -> None:
    observation = {
        "sandbox": handoff["sandbox"],
        "pool_name": handoff["pool_name"],
        "candidate_sha": handoff["candidate_sha"],
        "request_id": handoff["request_id"],
        "lease_epoch": handoff["lease_epoch"],
        "capacity_lease_state": (
            capacity_lease_state or ("active" if handoff["enabled"] else "retiring")
        ),
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
        "observation_sequence": sequence,
        "pending_slots": pending,
        "active_slots": active,
        "draining_slots": draining,
        "terminal_slots": terminal,
    }
    observation["payload_sha256"] = hashlib.sha256(
        json.dumps(observation, sort_keys=True, separators=(",", ":")).encode(),
    ).hexdigest()
    _write(
        config.observation_dir / f"{instance}.json",
        [observation],
    )


def test_cycle_reconciles_once_and_publishes_exact_broker_handoff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _ = _fixture(tmp_path)
    request_id = _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-cycle",
    )
    original = SharedCapacityBroker.reconcile
    calls = 0

    def counted(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SharedCapacityBroker, "reconcile", counted)
    result = supervisor.run_once(config, now=NOW)

    assert calls == 1
    assert result["status"] == "reconciled"
    assert result["aggregate"]["granted_slots"] == 12
    published = _handoff(config, "qianyi-gb10")
    broker_handoff = next(
        item
        for item in SharedCapacityBroker(config.state_db).status()["handoffs"]
        if item["request_id"] == request_id
    )
    assert published == broker_handoff
    assert published["lease_epoch"] == 1
    assert published["max_slots"] == 12
    assert not (config.handoff_dir / "current" / "hongjian-gb10.json").exists()


def test_six_file_collection_accepts_two_exact_observations_in_one_transaction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-observe",
    )
    _request(
        config,
        SandboxId.HONGJIAN,
        pool="oldlab",
        candidate_sha=SHA_B,
        target_slots=8,
        key="hongjian-oldlab-observe",
    )
    supervisor.run_once(config, now=NOW)
    qianyi = _handoff(config, "qianyi-gb10")
    hongjian = _handoff(config, "hongjian-oldlab")
    _observation(
        config,
        "qianyi-gb10",
        handoff=qianyi,
        active=qianyi["max_slots"],
    )
    _observation(
        config,
        "hongjian-oldlab",
        handoff=hongjian,
        active=hongjian["max_slots"],
    )
    original = SharedCapacityBroker.reconcile
    calls = 0
    observed_count = 0

    def counted(self, budgets, *, observations=()):  # type: ignore[no-untyped-def]
        nonlocal calls, observed_count
        calls += 1
        observed_count = len(observations)
        return original(self, budgets, observations=observations)

    monkeypatch.setattr(SharedCapacityBroker, "reconcile", counted)
    result = supervisor.run_once(config, now=NOW)

    assert calls == 1
    assert observed_count == 2
    assert result["aggregate"]["active_slots"] == 20
    assert result["observations"]["qianyi-gb10"]["status"] == "accepted"
    assert result["observations"]["hongjian-oldlab"]["status"] == "accepted"


def test_restart_is_idempotent_and_audit_sequence_is_durable(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-restart",
    )
    first = supervisor.run_once(config, now=NOW)
    handoff_path = config.handoff_dir / "current" / "qianyi-gb10.json"
    first_inode = handoff_path.stat().st_ino
    first_bytes = handoff_path.read_bytes()

    second = supervisor.run_once(config, now=NOW)

    assert first["cycle_sequence"] == 1
    assert second["cycle_sequence"] == 2
    assert handoff_path.stat().st_ino == first_inode
    assert handoff_path.read_bytes() == first_bytes
    state = json.loads(config.supervisor_state_path.read_text())
    assert state["cycle_sequence"] == 2
    audit = [json.loads(line) for line in config.audit_path.read_text().splitlines()]
    assert [event["sequence"] for event in audit] == [1, 2]
    evidence = json.loads(config.evidence_path.read_text())
    assert evidence["cycle"]["sequence"] == 2


def test_interrupted_generation_flip_never_exposes_partial_six_instance_set(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _ = _fixture(tmp_path)
    request_id = _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-generation-interrupt",
    )
    supervisor.run_once(config, now=NOW)
    old_target = config.handoff_dir.joinpath("current").readlink()
    old_manifest = _assert_complete_generation(config)
    SharedCapacityBroker(config.state_db, clock=lambda: NOW).cancel(request_id)

    original_flip = supervisor._flip_current_generation

    def interrupted(*_args, **_kwargs) -> None:
        raise supervisor.SupervisorError("injected before atomic generation flip")

    monkeypatch.setattr(supervisor, "_flip_current_generation", interrupted)
    with pytest.raises(supervisor.SupervisorError, match="injected"):
        supervisor.run_once(config, now=NOW)

    assert config.handoff_dir.joinpath("current").readlink() == old_target
    assert _assert_complete_generation(config) == old_manifest

    monkeypatch.setattr(supervisor, "_flip_current_generation", original_flip)
    supervisor.run_once(config, now=NOW)
    assert config.handoff_dir.joinpath("current").readlink() != old_target
    new_manifest = _assert_complete_generation(config)
    assert new_manifest != old_manifest
    assert _handoff(config, "qianyi-gb10")["enabled"] is False


def test_unchanged_cycle_retains_current_and_previous_generations(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path)
    request_id = _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-retain-rollback",
    )
    first = supervisor.run_once(config, now=NOW)
    SharedCapacityBroker(config.state_db, clock=lambda: NOW).cancel(request_id)
    second = supervisor.run_once(config, now=NOW)
    third = supervisor.run_once(config, now=NOW)

    assert third["generation"] == second["generation"]
    assert third["generation"] != first["generation"]
    generations = {
        path.name
        for path in config.handoff_dir.iterdir()
        if supervisor._GENERATION_RE.fullmatch(path.name) is not None
    }
    assert generations == {first["generation"], second["generation"]}


def test_supervisor_rejects_overlapping_invocations(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)

    with supervisor._exclusive_supervisor_lock(config):
        with pytest.raises(supervisor.SupervisorError, match="already active"):
            supervisor.run_once(config, now=NOW)

    assert not config.handoff_dir.exists()
    assert not config.audit_path.exists()


def test_malformed_observation_fails_before_reconcile_or_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-malformed",
    )
    _write(config.observation_dir / "qianyi-gb10.json", {"bad": "shape"})
    calls = 0

    def forbidden(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        raise AssertionError("reconcile must not run")

    monkeypatch.setattr(SharedCapacityBroker, "reconcile", forbidden)

    with pytest.raises(supervisor.SupervisorError, match="exactly one"):
        supervisor.run_once(config, now=NOW)

    assert calls == 0
    assert not config.handoff_dir.exists()
    assert not config.audit_path.exists()


def test_ahead_epoch_observation_fails_closed(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-ahead",
    )
    supervisor.run_once(config, now=NOW)
    handoff = _handoff(config, "qianyi-gb10")
    handoff["lease_epoch"] += 1
    _observation(
        config,
        "qianyi-gb10",
        handoff=handoff,
        active=12,
    )
    previous_audit = config.audit_path.read_bytes()

    with pytest.raises(supervisor.SupervisorError, match="not current"):
        supervisor.run_once(config, now=NOW)

    assert config.audit_path.read_bytes() == previous_audit


def test_terminal_tombstone_is_ignored_without_blocking_new_request(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path)
    first_id = _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=4,
        key="qianyi-gb10-terminal-old",
    )
    supervisor.run_once(config, now=NOW)
    first_handoff = _handoff(config, "qianyi-gb10")
    broker = SharedCapacityBroker(config.state_db, clock=lambda: NOW)
    cancelled = broker.cancel(first_id)
    _observation(
        config,
        "qianyi-gb10",
        handoff={
            **first_handoff,
            "lease_epoch": cancelled["lease"]["lease_epoch"],
            "enabled": False,
        },
        terminal=4,
    )
    supervisor.run_once(config, now=NOW)
    second_id = _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_B,
        target_slots=4,
        key="qianyi-gb10-terminal-new",
    )

    result = supervisor.run_once(config, now=NOW)

    assert result["observations"]["qianyi-gb10"]["status"] == "terminal_tombstone_ignored"
    assert second_id != first_id
    assert _handoff(config, "qianyi-gb10")["request_id"] == second_id


def test_duplicate_observation_does_not_refresh_broker_liveness(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path)
    request_id = _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=4,
        key="qianyi-gb10-duplicate-observation",
    )
    supervisor.run_once(config, now=NOW)
    handoff = _handoff(config, "qianyi-gb10")
    _observation(
        config,
        "qianyi-gb10",
        handoff=handoff,
        active=4,
        sequence=7,
    )
    supervisor.run_once(config, now=NOW)
    first_lease = next(
        item["lease"]
        for item in SharedCapacityBroker(config.state_db).status()["requests"]
        if item["request"]["id"] == request_id
    )

    replay = supervisor.run_once(config, now=NOW + datetime.resolution)
    second_lease = next(
        item["lease"]
        for item in SharedCapacityBroker(config.state_db).status()["requests"]
        if item["request"]["id"] == request_id
    )

    assert replay["observations"]["qianyi-gb10"]["status"] == "duplicate_ignored"
    assert second_lease["last_observed_at"] == first_lease["last_observed_at"]
    assert second_lease["last_observation_sequence"] == 7


def test_stale_and_regressed_observations_fail_before_reconcile(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=4,
        key="qianyi-gb10-stale-observation",
    )
    supervisor.run_once(config, now=NOW)
    handoff = _handoff(config, "qianyi-gb10")
    _observation(
        config,
        "qianyi-gb10",
        handoff=handoff,
        active=4,
        sequence=2,
    )
    supervisor.run_once(config, now=NOW)
    previous_audit = config.audit_path.read_bytes()

    _observation(
        config,
        "qianyi-gb10",
        handoff=handoff,
        active=4,
        sequence=3,
        observed_at=NOW - supervisor._OBSERVATION_MAX_AGE - datetime.resolution,
    )
    with pytest.raises(supervisor.SupervisorError, match="stale"):
        supervisor.run_once(config, now=NOW)
    assert config.audit_path.read_bytes() == previous_audit

    _observation(
        config,
        "qianyi-gb10",
        handoff=handoff,
        active=4,
        sequence=1,
    )
    with pytest.raises(supervisor.SupervisorError, match="sequence regressed"):
        supervisor.run_once(config, now=NOW)
    assert config.audit_path.read_bytes() == previous_audit

    _observation(
        config,
        "qianyi-gb10",
        handoff=handoff,
        active=4,
        sequence=3,
        capacity_lease_state="retired",
    )
    with pytest.raises(supervisor.SupervisorError, match="binding is not current"):
        supervisor.run_once(config, now=NOW)
    assert config.audit_path.read_bytes() == previous_audit


def test_duplicate_nonterminal_instance_requests_fail_before_publication(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=4,
        key="qianyi-gb10-duplicate-a",
    )
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_B,
        target_slots=4,
        key="qianyi-gb10-duplicate-b",
    )

    with pytest.raises(supervisor.SupervisorError, match="multiple nonterminal"):
        supervisor.run_once(config, now=NOW)

    assert not config.handoff_dir.exists()


def test_independent_budget_validation_blocks_publication_after_transaction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _ = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-budget-tamper",
    )
    original = SharedCapacityBroker.reconcile

    def tampered(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        report = copy.deepcopy(original(self, *args, **kwargs))
        report["aggregate"]["committed_slots"] = 161
        return report

    monkeypatch.setattr(SharedCapacityBroker, "reconcile", tampered)

    with pytest.raises(supervisor.SupervisorError, match="aggregate"):
        supervisor.run_once(config, now=NOW)

    assert not config.handoff_dir.exists()
    assert not config.audit_path.exists()
    assert SharedCapacityBroker(config.state_db).status()["aggregate"]["committed_slots"] == 12


@pytest.mark.parametrize(
    ("config_replacement", "lease_updates", "aggregate_updates", "message"),
    [
        (
            ("global_slot_budget = 132", "global_slot_budget = 100"),
            {"pending_slots": 0, "active_slots": 101, "committed_slots": 101},
            {"pending_slots": 0, "active_slots": 101, "committed_slots": 101},
            "global budget",
        ),
        (
            None,
            {"pending_slots": 0, "active_slots": 113, "committed_slots": 113},
            {"pending_slots": 0, "active_slots": 113, "committed_slots": 113},
            "gb10 budget",
        ),
        (
            (
                "global_pending_slot_budget = 34",
                "global_pending_slot_budget = 20",
            ),
            {"pending_slots": 21, "committed_slots": 21},
            {"pending_slots": 21, "committed_slots": 21},
            "global pending",
        ),
        (
            None,
            {"pending_slots": 25, "committed_slots": 25},
            {"pending_slots": 25, "committed_slots": 25},
            "gb10 pending",
        ),
    ],
)
def test_budget_validation_checks_global_pool_and_pending_independently(
    tmp_path: Path,
    config_replacement: tuple[str, str] | None,
    lease_updates: dict[str, int],
    aggregate_updates: dict[str, int],
    message: str,
) -> None:
    config, config_path = _fixture(tmp_path)
    if config_replacement is not None:
        old, new = config_replacement
        _write(config_path, config_path.read_text().replace(old, new, 1))
        config = supervisor.load_config(config_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key=f"qianyi-gb10-independent-{message.replace(' ', '-')}",
    )
    report = SharedCapacityBroker(config.state_db, clock=lambda: NOW).reconcile(
        BrokerBudgets(
            global_slots=config.global_slot_budget,
            pool_slots=config.pool_slot_budgets,
            global_pending_slots=config.global_pending_slot_budget,
            pool_pending_slots=config.pool_pending_slot_budgets,
        ),
    )
    report["requests"][0]["lease"].update(lease_updates)
    report["aggregate"].update(aggregate_updates)

    with pytest.raises(supervisor.SupervisorError, match=message):
        supervisor._validate_report_budgets(report, config)


def test_restart_rejects_config_digest_change_while_capacity_is_committed(
    tmp_path: Path,
) -> None:
    config, config_path = _fixture(tmp_path)
    _request(
        config,
        SandboxId.QIANYI,
        pool="gb10",
        candidate_sha=SHA_A,
        target_slots=12,
        key="qianyi-gb10-config-drift",
    )
    supervisor.run_once(config, now=NOW)
    previous_audit = config.audit_path.read_bytes()
    _write(
        config_path,
        config_path.read_text().replace(
            "global_slot_budget = 132",
            "global_slot_budget = 131",
            1,
        ),
    )
    changed = supervisor.load_config(config_path)

    with pytest.raises(supervisor.SupervisorError, match="config changed"):
        supervisor.run_once(changed, now=NOW)

    assert config.audit_path.read_bytes() == previous_audit


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("global_slot_budget = 132", "global_slot_budget = 133", "global_slot"),
        (
            "global_pending_slot_budget = 34",
            "global_pending_slot_budget = 35",
            "global_pending",
        ),
        ("gb10 = 112", "gb10 = 113", "reviewed bound"),
    ],
)
def test_config_rejects_budgets_above_reviewed_bounds(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    _, config_path = _fixture(tmp_path)
    text = config_path.read_text().replace(old, new, 1)
    _write(config_path, text)

    with pytest.raises(supervisor.SupervisorError, match=message):
        supervisor.load_config(config_path)


def test_main_emits_only_generic_failure(tmp_path: Path, capsys) -> None:
    _, config_path = _fixture(tmp_path)
    config_path.unlink()

    rc = supervisor.main(["--config", str(config_path), "run"])

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"error":"shared-capacity-supervisor-failed-safely"}\n'


def test_checked_in_config_and_exact_candidate_service_renderer(tmp_path: Path) -> None:
    checked = ROOT / "deploy/developer-sandboxes/shared-capacity-supervisor/config.toml"
    installed = tmp_path / "supervisor.toml"
    _write(installed, checked.read_text())
    config = supervisor.load_config(installed)
    assert config.pool_slot_budgets == {"gb10": 112, "oldlab": 20}
    assert config.pool_pending_slot_budgets == {"gb10": 24, "oldlab": 10}
    assert set(config.instances) == set(supervisor._EXPECTED_INSTANCES)
    template = (
        ROOT / "deploy/developer-sandboxes/loom-shared-capacity-supervisor.service"
    ).read_text()

    rendered = renderer.render_service_unit(template, git_sha=SHA_A)

    assert "${GIT_SHA}" not in rendered
    assert rendered.count(SHA_A) == 4
    assert "PrivateNetwork=true" in rendered
    assert "ReadWritePaths=/var/lib/loom-shared-capacity" in rendered
    with pytest.raises(ValueError, match="40-character"):
        renderer.render_service_unit(template, git_sha="abc")
