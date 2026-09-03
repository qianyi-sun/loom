from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from uuid import UUID

import pytest

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.ledger import GuardLedger

GRANT = UUID("11111111-1111-1111-1111-111111111111")
REQUEST = UUID("22222222-2222-2222-2222-222222222222")
PROOF = UUID("33333333-3333-3333-3333-333333333333")
EXCHANGE = UUID("44444444-4444-4444-4444-444444444444")
SESSION = UUID("55555555-5555-5555-5555-555555555555")
DIGEST_A = "1" * 64
DIGEST_B = "2" * 64
BATCH = "/slurm/job_12345/step_batch/user/task_0"
PINS = "/sys/fs/bpf/loom-task-image-builder/11111111-1111-1111-1111-111111111111"


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "ledger"
    root.mkdir(mode=0o700)
    return root


def _ledger(tmp_path: Path, **changes: object) -> GuardLedger:
    values: dict[str, object] = {
        "root": _root(tmp_path),
        "maximum_entries": 8,
        "trusted_uid": os.geteuid(),
        "trusted_gid": os.getegid(),
    }
    values.update(changes)
    return GuardLedger(**values)  # type: ignore[arg-type]


def _intent(ledger: GuardLedger) -> None:
    ledger.create_intent(
        grant_id=GRANT,
        request_id=REQUEST,
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative=BATCH,
    )


def _request() -> dict[str, object]:
    return {
        "schema_version": 1,
        "request_id": str(REQUEST),
        "grant_id": str(GRANT),
        "observed_at": "2026-09-02T16:00:00Z",
        "node_name": "trt-gb10-1",
        "node_boot_id": "66666666-6666-6666-6666-666666666666",
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "12345",
        "supervisor_pid": 42100,
        "supervisor_uid": 993,
        "supervisor_gid": 980,
        "supervisor_executable_sha256": DIGEST_A,
        "cgroup_path": "/sys/fs/cgroup/slurm/job_12345/step_batch/user/task_0",
        "cgroup_inode": 987654,
        "submitting_identity": "loom-builder",
        "slurm_account": "loom-task-builder",
        "slurm_partition": "loom-task-builder",
        "slurm_qos": "loom-task-image-builder-rootless-gb10",
        "cpu_arch": "arm64",
        "slurm_request_sha256": DIGEST_B,
    }


def _challenge(request_sha256: str = DIGEST_A) -> dict[str, object]:
    return {
        "schema_version": 1,
        "request_id": str(REQUEST),
        "grant_id": str(GRANT),
        "request_sha256": request_sha256,
        "challenge_nonce": "77777777-7777-7777-7777-777777777777",
        "containment_policy_sha256": DIGEST_A,
        "resource_profile_sha256": DIGEST_B,
        "issued_at": "2026-09-02T16:00:01Z",
        "expires_at": "2026-09-02T16:00:50Z",
    }


def _proof() -> dict[str, object]:
    return {
        "schema_version": 1,
        "proof_id": str(PROOF),
        "grant_id": str(GRANT),
        "request_id": str(REQUEST),
        "request_sha256": DIGEST_A,
        "challenge_nonce": "77777777-7777-7777-7777-777777777777",
        "attachment": {
            "link_ids": list(range(101, 125)),
            "program_ids": list(range(201, 225)),
            "map_ids": list(range(301, 319)),
        },
    }


def _record_challenge(ledger: GuardLedger):
    request = _request()
    request_sha256 = ledger.document_sha256(request)
    challenge = _challenge(request_sha256)
    return ledger.record_challenge(
        GRANT,
        projection_request=request,
        projection_request_sha256=request_sha256,
        challenge=challenge,
        challenge_sha256=ledger.document_sha256(challenge),
    )


def test_persists_exact_replay_identity_with_atomic_root_owned_file(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    _record_challenge(ledger)
    ledger.close()

    reopened = GuardLedger(
        root=tmp_path / "ledger",
        maximum_entries=8,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    entry = reopened.get(GRANT)
    path = tmp_path / "ledger" / f"{GRANT}.json"

    assert entry is not None
    assert entry.state == "challenged"
    assert entry.document()["projection_request"] == _request()
    assert entry.document()["projection_request_sha256"] == ledger.document_sha256(_request())
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.stat().st_nlink == 1
    assert list((tmp_path / "ledger").iterdir()) == [path]
    assert path.read_bytes().endswith(b"\n")
    reopened.close()


def test_state_transitions_are_exactly_idempotent_and_reject_equivocation(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    first = _record_challenge(ledger)
    replay = _record_challenge(ledger)
    request_sha256 = ledger.document_sha256(_request())
    changed = _challenge(request_sha256) | {
        "challenge_nonce": "99999999-9999-9999-9999-999999999999"
    }

    assert first.raw == replay.raw
    with pytest.raises(GuardError) as caught:
        ledger.record_challenge(
            GRANT,
            projection_request=_request(),
            projection_request_sha256=request_sha256,
            challenge=changed,
            challenge_sha256=ledger.document_sha256(changed),
        )
    assert caught.value.code == "ledger_replay_conflict"
    ledger.close()


def test_records_only_public_attachment_exchange_and_attestation_bindings(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    _record_challenge(ledger)
    proof = _proof()
    ledger.record_attachment(
        GRANT,
        proof=proof,
        proof_sha256=ledger.document_sha256(proof),
        pin_path=PINS,
        link_ids=tuple(range(101, 125)),
        program_ids=tuple(range(201, 225)),
        map_ids=tuple(range(301, 319)),
        attestation_generation=1,
        attestation_sha256=DIGEST_B,
        attestation_expires_at="2026-09-02T16:00:50Z",
    )
    ledger.record_projection(
        GRANT,
        receipt_public_binding_sha256=DIGEST_B,
        bootstrap_token_sha256=DIGEST_A,
    )
    ledger.record_exchange(
        GRANT,
        exchange_id=EXCHANGE,
        exchange_public_binding_sha256=DIGEST_A,
        session_id=SESSION,
        session_public_binding_sha256=DIGEST_B,
        session_token_sha256=DIGEST_A,
        session_expires_at="2026-09-02T16:10:00Z",
    )
    ledger.record_attestation(
        GRANT,
        generation=2,
        attestation_sha256=DIGEST_A,
        expires_at="2026-09-02T16:01:00Z",
    )

    payload = (tmp_path / "ledger" / f"{GRANT}.json").read_bytes()
    document = json.loads(payload)

    assert document["state"] == "exchanged"
    assert document["attestation_generation"] == 2
    assert document["pin_path"] == PINS
    assert b"loom_tibp_" not in payload
    assert b"loom_tibs_" not in payload
    assert b"bootstrap_token\"" not in payload
    assert b"session_token\"" not in payload
    ledger.close()


def test_restarted_supervisor_reuses_persisted_request_identity(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    _record_challenge(ledger)

    replay = ledger.create_intent(
        grant_id=GRANT,
        request_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        peer_pid=42100,
        job_id="12345",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative=BATCH,
    )

    assert replay.state == "challenged"
    assert replay.request_id == REQUEST
    ledger.close()


def test_refuses_secret_values_and_preserves_prior_durable_state(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    request = _request() | {"raw": "loom_tibp_" + "S" * 64}

    with pytest.raises(GuardError) as caught:
        ledger.record_challenge(
            GRANT,
            projection_request=request,
            projection_request_sha256=DIGEST_A,
            challenge=_challenge(),
            challenge_sha256=DIGEST_B,
        )

    assert caught.value.code == "ledger_secret_forbidden"
    assert ledger.get(GRANT).state == "intent"  # type: ignore[union-attr]
    ledger.close()


def test_rejects_symlink_hardlink_unknown_file_and_crash_staging(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    ledger.close()
    path = tmp_path / "ledger" / f"{GRANT}.json"
    hardlink = tmp_path / "ledger" / f"{UUID('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')}.json"
    os.link(path, hardlink)

    reopened = GuardLedger(
        root=tmp_path / "ledger",
        maximum_entries=8,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    with pytest.raises(GuardError) as caught:
        reopened.load_all()
    assert caught.value.code == "ledger_file_invalid"
    reopened.close()

    hardlink.unlink()
    unknown = tmp_path / "ledger" / "notes.txt"
    unknown.write_text("unknown", encoding="ascii")
    unknown.chmod(0o600)
    reopened = GuardLedger(
        root=tmp_path / "ledger",
        maximum_entries=8,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    with pytest.raises(GuardError) as caught:
        reopened.load_all()
    assert caught.value.code == "ledger_inventory_ambiguous"
    reopened.close()

    unknown.rename(tmp_path / "ledger" / f"staging-{GRANT}-deadbeef")
    reopened = GuardLedger(
        root=tmp_path / "ledger",
        maximum_entries=8,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    with pytest.raises(GuardError) as caught:
        reopened.load_all()
    assert caught.value.code == "ledger_inventory_ambiguous"
    reopened.close()


def test_bounds_entry_count_and_entry_bytes(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, maximum_entries=1, maximum_entry_bytes=1024)
    _intent(ledger)
    with pytest.raises(GuardError) as caught:
        _record_challenge(ledger)
    assert caught.value.code == "ledger_entry_too_large"
    ledger.close()

    ledger = GuardLedger(
        root=tmp_path / "ledger",
        maximum_entries=1,
        maximum_entry_bytes=64 * 1024,
        trusted_uid=os.geteuid(),
        trusted_gid=os.getegid(),
    )
    _intent(ledger)
    with pytest.raises(GuardError) as caught:
        ledger.create_intent(
            grant_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            request_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            peer_pid=42101,
            job_id="12346",
            peer_executable_sha256=DIGEST_A,
            batch_cgroup_relative="/slurm/job_12346/step_batch/user/task_0",
        )
    assert caught.value.code == "ledger_capacity_exhausted"
    ledger.close()


def test_quarantine_preserves_attachment_and_remove_requires_terminal_empty(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    _intent(ledger)
    _record_challenge(ledger)
    proof = _proof()
    ledger.record_attachment(
        GRANT,
        proof=proof,
        proof_sha256=ledger.document_sha256(proof),
        pin_path=PINS,
        link_ids=tuple(range(101, 125)),
        program_ids=tuple(range(201, 225)),
        map_ids=tuple(range(301, 319)),
        attestation_generation=1,
        attestation_sha256=DIGEST_B,
        attestation_expires_at="2026-09-02T16:00:50Z",
    )
    ledger.record_projection(
        GRANT,
        receipt_public_binding_sha256=DIGEST_B,
        bootstrap_token_sha256=DIGEST_A,
    )
    quarantined = ledger.quarantine(GRANT, reason="pin_identity_ambiguous")

    assert quarantined.state == "quarantined"
    assert quarantined.document()["pin_path"] == PINS
    assert Path(PINS) not in ledger.removable_pin_paths(GRANT)
    with pytest.raises(GuardError) as caught:
        ledger.remove_terminal(GRANT, allocation_empty=True)
    assert caught.value.code == "ledger_not_terminal"

    with pytest.raises(GuardError) as caught:
        ledger.mark_terminal(GRANT, reason="slurm_completed")
    assert caught.value.code == "ledger_quarantined"

    terminal_grant = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    ledger.create_intent(
        grant_id=terminal_grant,
        request_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        peer_pid=42101,
        job_id="12346",
        peer_executable_sha256=DIGEST_A,
        batch_cgroup_relative="/slurm/job_12346/step_batch/user/task_0",
    )
    ledger.mark_terminal(terminal_grant, reason="slurm_completed")
    with pytest.raises(GuardError) as caught:
        ledger.remove_terminal(terminal_grant, allocation_empty=False)
    assert caught.value.code == "ledger_allocation_not_empty"
    assert ledger.get(terminal_grant) is not None
    ledger.remove_terminal(terminal_grant, allocation_empty=True)
    assert ledger.get(terminal_grant) is None
    ledger.close()
