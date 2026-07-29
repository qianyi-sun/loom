from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_staging_promotion as promotion

SHA = "a" * 40
TREE = "b" * 40
REQUEST_ID = "req-0123456789abcdef"
ROLLOUT_ID = "20260729t010000z-staging-aaaaaaa"


def _write(path: Path, value: object, *, canonical: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if canonical:
        payload = promotion._canonical_bytes(value)
    else:
        payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(payload)
    path.chmod(0o600)


def _layout(tmp_path: Path) -> promotion.Layout:
    return promotion.Layout(
        state_root=tmp_path / "var/lib/loom-staging-rollout",
        rollout_root=tmp_path / "data/loom-staging",
        candidate_root=tmp_path / "opt/loom-staging-runner/candidates",
        acceptance_root=tmp_path / "var/lib/loom-staging-rollout/acceptance",
        installed_program=tmp_path / "usr/local/libexec/loom-developer-sandbox-staging-promotion",
        installed_sudoers=tmp_path / "etc/sudoers.d/loom-developer-sandbox-staging-promotion",
        git=Path("/usr/bin/git"),
    )


def _time(index: int, *, base: int = 0) -> str:
    value = datetime(2026, 7, 29, 1, tzinfo=UTC) + timedelta(
        minutes=base,
        seconds=index,
    )
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _browser_report(
    *,
    request_id: str,
    attempt_number: int,
    envelope_sha256: str,
    candidate_sha: str,
) -> dict[str, object]:
    return {
        "schema_version": 4,
        "status": "pass",
        "deployment_identity": {
            "expected_deployed_sha": candidate_sha,
            "observed_deployed_sha": candidate_sha,
            "matched": True,
        },
        "route": "https://loom.example.test/staging",
        "request_id": request_id,
        "target": {"username": "qianyi", "user_id": "user-qianyi"},
        "audit_event_id": "audit-0123456789",
        "browser": {"name": "chromium", "version": "140.0"},
        "checks": {key: True for key in promotion._BROWSER_CHECKS},
        "cleanup": {"logout_status": 204, "auth_me_after_logout_status": 401},
        "failure_code": None,
        "rollout_binding": {
            "request_id": request_id,
            "attempt_number": attempt_number,
            "request_envelope_sha256": envelope_sha256,
            "resolved_sha": candidate_sha,
        },
    }


def _build_rollout(
    layout: promotion.Layout,
    *,
    request_id: str = REQUEST_ID,
    rollout_id: str = ROLLOUT_ID,
    candidate_sha: str = SHA,
    candidate_tree: str = TREE,
    time_base: int = 0,
) -> dict[str, Path]:
    uid = os.geteuid()
    request = {
        "request_id": request_id,
        "rollout_id": rollout_id,
        "caller": {"username": "qianyi", "uid": uid, "schema_version": 1},
        "candidate": {
            "remote_url": promotion.APPROVED_REMOTE_URL,
            "target_ref": promotion.APPROVED_TARGET_REF,
            "resolved_sha": candidate_sha,
            "image_tag": f"staging-{candidate_sha[:7]}",
            "fetched_at": _time(0, base=time_base),
            "schema_version": 1,
            "resolved_tree": candidate_tree,
        },
        "requested_at": _time(0, base=time_base),
        "runner_config_sha256": "1" * 64,
        "preflight_attestation_sha256": "2" * 64,
        "preflight_registry_sha256": "3" * 64,
        "preflight_coverage_sha256": "4" * 64,
        "command": "start",
        "status": "pending",
        "schema_version": 1,
    }
    request_path = layout.state_root / "requests" / request_id / "request.json"
    _write(request_path, request)
    envelope: dict[str, object] = {
        "schema_version": 1,
        "request_id": request_id,
        "rollout_id": rollout_id,
        "initiating_operator": "qianyi",
        "initiating_uid": uid,
        "attempt_number": 1,
        "attempt_operator": "qianyi",
        "attempt_uid": uid,
        "remote_url": promotion.APPROVED_REMOTE_URL,
        "target_ref": promotion.APPROVED_TARGET_REF,
        "resolved_sha": candidate_sha,
        "image_tag": f"staging-{candidate_sha[:7]}",
        "fetched_at": _time(0, base=time_base),
        "backup_manifest_path": "/var/lib/loom-staging-rollout/backup.json",
        "backup_manifest_sha256": "5" * 64,
        "runner_config_sha256": "1" * 64,
        "preflight_attestation_sha256": "2" * 64,
        "preflight_registry_sha256": "3" * 64,
        "preflight_coverage_sha256": "4" * 64,
        "cluster_name": "loom-staging",
        "namespace": "loom-staging",
        "environment": "staging",
        "cp_url": "http://127.0.0.1:18081",
        "cluster_config_path": str(
            layout.candidate_root / candidate_sha / "repo/deploy/environments/staging.cluster.toml",
        ),
        "rollout_root": str(layout.rollout_root),
        "admin_token_source": "file:/run/loom/admin",
        "worker_token_source": "file:/run/loom/worker",
        "service_token_source": "file:/run/loom/service",
        "expect_admin_token_fingerprint": "sha256:0123456789ab len=32",
        "smoke_on_behalf_username": "devansh",
        "smoke_on_behalf_team_id": "team-devansh",
        "scope": "current-gb10",
        "gb10_prep_concurrency": 2,
        "resume": False,
        "resolved_tree": candidate_tree,
    }
    envelope_path = layout.state_root / "requests" / request_id / "attempts/1/envelope.json"
    _write(envelope_path, envelope)
    envelope_sha256 = hashlib.sha256(envelope_path.read_bytes()).hexdigest()

    rollout_directory = layout.rollout_root / "rollouts" / rollout_id
    records: list[dict[str, object]] = []
    browser_path = (
        rollout_directory
        / "16-staging-admin-browser-acceptance"
        / "browser-output/staging-admin-browser-acceptance.json"
    )
    browser = _browser_report(
        request_id=request_id,
        attempt_number=1,
        envelope_sha256=envelope_sha256,
        candidate_sha=candidate_sha,
    )
    _write(browser_path, browser)
    for index, (number, name) in enumerate(promotion._STEPS, start=1):
        inputs_hash = hashlib.sha256(f"{request_id}:{number}".encode()).hexdigest()
        started_at = _time(index * 2, base=time_base)
        finished_at = _time(index * 2 + 1, base=time_base)
        artifacts: dict[str, object] = {}
        if number == 16:
            artifacts = {
                "browser_report": str(browser_path),
                "browser_report_sha256": hashlib.sha256(browser_path.read_bytes()).hexdigest(),
                "request_envelope_sha256": envelope_sha256,
            }
        result = {
            "number": number,
            "name": name,
            "state": "done",
            "inputs_hash": inputs_hash,
            "started_at": started_at,
            "finished_at": finished_at,
            "exit_code": 0,
            "error": None,
            "summary": f"{name} passed",
            "artifacts": artifacts,
        }
        _write(rollout_directory / f"{number:02d}-{name}/result.json", result)
        records.append(
            {
                "number": number,
                "name": name,
                "state": "done",
                "inputs_hash": inputs_hash,
                "started_at": started_at,
                "finished_at": finished_at,
                "error": None,
            },
        )
    state = {
        "version": 2,
        "rollout_id": rollout_id,
        "status": "done",
        "current_step": None,
        "driver": None,
        "request_id": request_id,
        "initiating_operator": "qianyi",
        "initiating_uid": uid,
        "attempt_number": 1,
        "attempt_operator": "qianyi",
        "attempt_uid": uid,
        "steps": records,
    }
    state_path = rollout_directory / "state.json"
    _write(state_path, state)
    repository = layout.candidate_root / candidate_sha / "repo"
    (repository / ".git").mkdir(parents=True)
    return {
        "request": request_path,
        "envelope": envelope_path,
        "state": state_path,
        "browser": browser_path,
        "browser_result": rollout_directory / "16-staging-admin-browser-acceptance/result.json",
    }


def _git_runner(
    candidate_sha: str = SHA,
    candidate_tree: str = TREE,
    *,
    remote: str = promotion.APPROVED_REMOTE_URL,
    dirty: str = "",
) -> promotion.GitRunner:
    def run(argv: Sequence[str], _git: Path) -> str:
        if argv[-3:] == ["rev-parse", "--verify", "HEAD"]:
            return candidate_sha
        if argv[-1].endswith("^{commit}"):
            return candidate_sha
        if argv[-1].endswith("^{tree}"):
            return candidate_tree
        if argv[-3:] == ["config", "--get", "remote.origin.url"]:
            return remote
        if argv[-3:] == ["status", "--porcelain=v1", "--untracked-files=no"]:
            return dirty
        raise AssertionError(argv)

    return run


def _produce(
    layout: promotion.Layout,
    *,
    request_id: str = REQUEST_ID,
    git_runner: promotion.GitRunner | None = None,
    failpoint: promotion.Failpoint = lambda _name: None,
) -> dict[str, object]:
    return promotion.produce(
        request_id,
        layout=layout,
        service_uid=os.geteuid(),
        owner_uid=os.geteuid(),
        hostname=promotion.SOURCE_HOST,
        now="2026-07-29T02:00:00Z",
        git_runner=git_runner or _git_runner(),
        failpoint=failpoint,
    )


def test_produces_closed_consumer_receipt_and_is_idempotent(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _build_rollout(layout)

    receipt = _produce(layout)

    assert receipt == {
        "schema_version": 1,
        "kind": "loom.staging-rollout.acceptance",
        "source_host": promotion.SOURCE_HOST,
        "rollout_id": ROLLOUT_ID,
        "candidate_sha": SHA,
        "candidate_tree": TREE,
        "result": "pass",
        "observed_at": _time(35),
    }
    assert layout.promotion.read_bytes() == promotion._canonical_bytes(receipt)
    assert stat_mode(layout.promotion) == 0o600
    state_before = layout.state.read_bytes()
    assert _produce(layout) == receipt
    assert layout.state.read_bytes() == state_before
    assert len(list(layout.receipts.iterdir())) == 1


def test_rejects_caller_forged_regression_success(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    paths = _build_rollout(layout)
    result = json.loads(paths["browser_result"].read_text())
    result["exit_code"] = 1
    _write(paths["browser_result"], result)

    with pytest.raises(promotion.PromotionError, match="canonical success"):
        _produce(layout)


def test_rejects_wrong_rollout_binding(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    paths = _build_rollout(layout)
    envelope = json.loads(paths["envelope"].read_text())
    envelope["rollout_id"] = "20260729t010001z-staging-foreign"
    _write(paths["envelope"], envelope)

    with pytest.raises(promotion.PromotionError, match="immutable request"):
        _produce(layout)


def test_rejects_wrong_request_tree(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    paths = _build_rollout(layout)
    request = json.loads(paths["request"].read_text())
    request["candidate"]["resolved_tree"] = "f" * 40
    _write(paths["request"], request)

    with pytest.raises(promotion.PromotionError, match="immutable request"):
        _produce(layout)


def test_rejects_forged_deployed_sha_readback(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    paths = _build_rollout(layout)
    report = json.loads(paths["browser"].read_text())
    report["deployment_identity"]["observed_deployed_sha"] = "f" * 40
    _write(paths["browser"], report)
    result = json.loads(paths["browser_result"].read_text())
    result["artifacts"]["browser_report_sha256"] = hashlib.sha256(
        paths["browser"].read_bytes(),
    ).hexdigest()
    _write(paths["browser_result"], result)

    with pytest.raises(promotion.PromotionError, match="did not pass"):
        _produce(layout)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("candidate_sha", "c" * 40, "git object/tree"),
        ("candidate_tree", "d" * 40, "git object/tree"),
        ("remote", "https://example.test/foreign.git", "git object/tree"),
        ("dirty", " M tracked.py", "git object/tree"),
    ),
)
def test_rejects_wrong_installed_candidate_readback(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    layout = _layout(tmp_path)
    _build_rollout(layout)
    kwargs = {field: value}

    with pytest.raises(promotion.PromotionError, match=message):
        _produce(layout, git_runner=_git_runner(**kwargs))


def test_rejects_wrong_source_host(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _build_rollout(layout)

    with pytest.raises(promotion.PromotionError, match="fixed source host"):
        promotion.produce(
            REQUEST_ID,
            layout=layout,
            service_uid=os.geteuid(),
            owner_uid=os.geteuid(),
            hostname="oldlab2",
        )


def test_rejects_replay_below_high_water(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _build_rollout(layout, time_base=20)
    _produce(layout)
    second_request = "req-fedcba9876543210"
    second_rollout = "20260729t000000z-staging-ccccccc"
    second_sha = "c" * 40
    second_tree = "d" * 40
    _build_rollout(
        layout,
        request_id=second_request,
        rollout_id=second_rollout,
        candidate_sha=second_sha,
        candidate_tree=second_tree,
        time_base=0,
    )

    with pytest.raises(promotion.PromotionError, match="high-water"):
        _produce(
            layout,
            request_id=second_request,
            git_runner=_git_runner(second_sha, second_tree),
        )


@pytest.mark.parametrize("kind", ("symlink", "hardlink"))
def test_rejects_link_attacks_on_fixed_source(
    tmp_path: Path,
    kind: str,
) -> None:
    layout = _layout(tmp_path)
    paths = _build_rollout(layout)
    target = tmp_path / "foreign.json"
    target.write_bytes(paths["request"].read_bytes())
    target.chmod(0o600)
    paths["request"].unlink()
    if kind == "symlink":
        paths["request"].symlink_to(target)
    else:
        os.link(target, paths["request"])

    with pytest.raises(promotion.PromotionError, match=r"authority|unavailable"):
        _produce(layout)


def test_detects_source_swap_during_transaction(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    paths = _build_rollout(layout)

    def mutate(name: str) -> None:
        if name == "after-audit":
            value = json.loads(paths["state"].read_text())
            value["status"] = "failed"
            _write(paths["state"], value)

    with pytest.raises(promotion.PromotionError, match="changed before publication"):
        _produce(layout, failpoint=mutate)


@pytest.mark.parametrize(
    "crash_at",
    ("after-pending", "after-audit", "after-promotion", "after-state"),
)
def test_recovers_every_crash_boundary(tmp_path: Path, crash_at: str) -> None:
    layout = _layout(tmp_path)
    promotion.install(
        layout=layout,
        owner_uid=os.geteuid(),
        program_payload=Path(promotion.__file__).read_bytes(),
    )
    _build_rollout(layout)

    def crash(name: str) -> None:
        if name == crash_at:
            raise SimulatedCrash(name)

    with pytest.raises(SimulatedCrash):
        _produce(layout, failpoint=crash)

    receipt = _produce(layout)
    assert receipt["candidate_sha"] == SHA
    assert not layout.pending.exists()
    state = promotion.check(layout=layout, owner_uid=os.geteuid())
    assert state is not None
    assert state["sequence"] == 1


def test_check_rejects_promotion_rollback(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    promotion.install(
        layout=layout,
        owner_uid=os.geteuid(),
        program_payload=Path(promotion.__file__).read_bytes(),
    )
    _build_rollout(layout)
    _produce(layout)
    receipt = json.loads(layout.promotion.read_text())
    receipt["candidate_sha"] = "f" * 40
    _write(layout.promotion, receipt, canonical=True)

    with pytest.raises(promotion.PromotionError, match="regressed"):
        promotion.check(layout=layout, owner_uid=os.geteuid())


def test_check_rejects_hardlinked_public_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    promotion.install(
        layout=layout,
        owner_uid=os.geteuid(),
        program_payload=Path(promotion.__file__).read_bytes(),
    )
    _build_rollout(layout)
    _produce(layout)
    foreign = tmp_path / "promotion-copy.json"
    layout.promotion.rename(foreign)
    os.link(foreign, layout.promotion)

    with pytest.raises(promotion.PromotionError, match="authority"):
        promotion.check(layout=layout, owner_uid=os.geteuid())


def test_pending_recovery_refuses_foreign_compare_and_swap(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _build_rollout(layout)

    def crash(name: str) -> None:
        if name == "after-pending":
            raise SimulatedCrash(name)

    with pytest.raises(SimulatedCrash):
        _produce(layout, failpoint=crash)
    foreign = {
        "schema_version": 1,
        "kind": "loom.staging-rollout.acceptance",
        "source_host": promotion.SOURCE_HOST,
        "rollout_id": "foreign-rollout",
        "candidate_sha": "f" * 40,
        "candidate_tree": "e" * 40,
        "result": "pass",
        "observed_at": "2026-07-29T01:00:01Z",
    }
    _write(layout.promotion, foreign, canonical=True)

    with pytest.raises(promotion.PromotionError, match="compare-and-swap"):
        _produce(layout)


def test_cli_has_no_caller_supplied_evidence_surface() -> None:
    with pytest.raises(SystemExit):
        promotion._parser().parse_args(
            [
                "produce",
                "--request-id",
                REQUEST_ID,
                "--result",
                "pass",
                "--execute",
            ],
        )


def test_install_is_persistent_and_checks_fixed_assets(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    payload = Path(promotion.__file__).read_bytes()

    promotion.install(
        layout=layout,
        owner_uid=os.geteuid(),
        program_payload=payload,
    )

    assert layout.installed_program.read_bytes() == payload
    assert stat_mode(layout.installed_program) == 0o755
    assert stat_mode(layout.installed_sudoers) == 0o440
    assert promotion.check(layout=layout, owner_uid=os.geteuid()) is None


class SimulatedCrash(BaseException):
    pass


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777
