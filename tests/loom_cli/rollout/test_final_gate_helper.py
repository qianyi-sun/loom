from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout import final_gate_helper as helper
from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlanStore
from loom_cli.rollout.operator.model import driver_envelope_bytes
from loom_cli.rollout.preflight_contract import CheckOperation
from tests.loom_cli.rollout.operator.test_final_gate_action_source import _authority
from tests.loom_cli.rollout.operator.test_final_gate_plan import _envelope
from tests.loom_cli.rollout.operator.test_final_gate_runner import _admission


def _prepared(tmp_path: Path, monkeypatch, *, with_resolved_tree: bool = False):
    source, attestation, _calls = _authority(tmp_path)
    envelope = _envelope(attestation)
    if with_resolved_tree:
        # A merged-dev candidate legitimately carries its derivable resolved tree.
        envelope = replace(envelope, resolved_tree=attestation.bindings.candidate_tree)
    source(envelope, attestation, 7, _admission(attestation))
    state = tmp_path / "state"
    path = state / "requests/req-alpha/attempts/1/final-gate-plan.json"
    envelope_path = path.with_name("envelope.json")
    envelope_path.write_bytes(driver_envelope_bytes(envelope))
    envelope_path.chmod(0o600)
    digest = FinalGatePlanStore(state, request_id="req-alpha", attempt_number=1).read().plan_digest
    monkeypatch.setattr(helper, "_STATE_ROOT", state)
    monkeypatch.setattr(helper, "_verify_checkpoint", lambda _plan: None)
    return attestation, path, digest


def test_final_gate_helper_loads_exact_plan_and_emits_strict_result(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    attestation, path, digest = _prepared(tmp_path, monkeypatch)

    def execute(check_id, operation, _plan):
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="a" * 40,
            attestation_digest=attestation.attestation_digest,
            observed_epoch=7,
            evidence_digest="e" * 64,
            protected_mutation=False,
            blockers={},
        )

    rc = helper.main(
        [
            "execute",
            "--check-id",
            "final.convergence",
            "--operation",
            "verify",
            "--plan",
            str(path),
            "--plan-sha256",
            digest,
        ],
        execute=execute,
    )

    assert rc == 0
    assert '"check_id":"final.convergence"' in capsys.readouterr().out


def test_final_gate_helper_fails_closed_without_executor_or_on_wrong_operation(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _attestation, path, digest = _prepared(tmp_path, monkeypatch)
    base = [
        "execute",
        "--check-id",
        "final.protected-apply",
        "--plan",
        str(path),
        "--plan-sha256",
        digest,
    ]

    assert helper.main([*base, "--operation", "apply"]) == 2
    assert helper.main([*base, "--operation", "verify"]) == 2
    assert capsys.readouterr().out == ""


def test_final_gate_helper_rejects_plan_digest_drift(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _attestation, path, _digest = _prepared(tmp_path, monkeypatch)

    assert (
        helper.main(
            [
                "execute",
                "--check-id",
                "final.summary",
                "--operation",
                CheckOperation.VERIFY.value,
                "--plan",
                str(path),
                "--plan-sha256",
                "f" * 64,
            ]
        )
        == 2
    )
    assert capsys.readouterr().out == ""


def test_final_gate_helper_accepts_merged_dev_resolved_tree(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    # Regression: a merged-dev candidate carries its resolved git tree, so the
    # helper must load the plan instead of rejecting the populated tree as
    # "driver envelope drifted" (which stalled every merged-dev protected apply).
    attestation, path, digest = _prepared(tmp_path, monkeypatch, with_resolved_tree=True)

    def execute(check_id, operation, _plan):
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="a" * 40,
            attestation_digest=attestation.attestation_digest,
            observed_epoch=7,
            evidence_digest="e" * 64,
            protected_mutation=False,
            blockers={},
        )

    rc = helper.main(
        [
            "execute",
            "--check-id",
            "final.convergence",
            "--operation",
            "verify",
            "--plan",
            str(path),
            "--plan-sha256",
            digest,
        ],
        execute=execute,
    )

    assert rc == 0
    assert '"check_id":"final.convergence"' in capsys.readouterr().out


def test_final_gate_helper_rejects_driver_envelope_byte_drift(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _attestation, path, digest = _prepared(tmp_path, monkeypatch)
    envelope_path = path.with_name("envelope.json")
    payload = envelope_path.read_bytes()
    envelope_path.write_bytes(payload.replace(b'"attempt_uid":501', b'"attempt_uid":502'))
    envelope_path.chmod(0o600)

    assert (
        helper.main(
            [
                "execute",
                "--check-id",
                "final.summary",
                "--operation",
                "verify",
                "--plan",
                str(path),
                "--plan-sha256",
                digest,
            ]
        )
        == 2
    )
    assert capsys.readouterr().out == ""


def test_final_gate_helper_requires_one_epoch_advance_for_successful_apply(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    attestation, path, digest = _prepared(tmp_path, monkeypatch)

    def execute(check_id, operation, _plan):
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="a" * 40,
            attestation_digest=attestation.attestation_digest,
            observed_epoch=7,
            evidence_digest="e" * 64,
            protected_mutation=True,
            blockers={},
        )

    assert (
        helper.main(
            [
                "execute",
                "--check-id",
                "final.protected-apply",
                "--operation",
                "apply",
                "--plan",
                str(path),
                "--plan-sha256",
                digest,
            ],
            execute=execute,
        )
        == 2
    )
    assert capsys.readouterr().out == ""


def test_final_gate_helper_accepts_live_smoke_inside_claimed_epoch(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    attestation, path, digest = _prepared(tmp_path, monkeypatch)

    def execute(check_id, operation, _plan):
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="a" * 40,
            attestation_digest=attestation.attestation_digest,
            observed_epoch=8,
            evidence_digest="e" * 64,
            protected_mutation=True,
            blockers={},
        )

    assert (
        helper.main(
            [
                "execute",
                "--check-id",
                "final.smoke",
                "--operation",
                "apply",
                "--plan",
                str(path),
                "--plan-sha256",
                digest,
            ],
            execute=execute,
        )
        == 0
    )
    assert '"check_id":"final.smoke"' in capsys.readouterr().out


def _activation_documents(tmp_path, path, *, native=False):
    from tests.loom_cli.rollout.operator.test_protected_execution_activation import fixture
    (tmp_path / "activation-fixture").mkdir()
    owner, _, _, _ = fixture(tmp_path / "activation-fixture", native=native)
    documents = {pool: request.document for pool, request in owner.requests.items()}
    payload = json.dumps({pool: document.model_dump(mode="json") for pool, document in documents.items()}).encode()
    source = path.with_name("execution-activation-documents.json")
    source.write_bytes(payload)
    source.chmod(0o600)
    return owner, documents, source, hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize("resume", [False, True])
def test_activation_command_uses_bound_documents_or_retained_recovery(tmp_path, monkeypatch, capsys, resume):
    _, path, digest = _prepared(tmp_path, monkeypatch)
    owner, documents, source, source_digest = _activation_documents(tmp_path, path)
    # Freshness belongs to the installed forward guard; recovery may need drain.
    monkeypatch.setattr(helper, "_verify_checkpoint", lambda _: pytest.fail("checkpoint must be checked by installed forward guard"))
    calls = []
    def activate(plan, *, documents):
        calls.append(documents)
        assert plan.plan_digest == digest
        return owner.expected
    args = ["activate-prepared", "--plan", str(path), "--plan-sha256", digest]
    if not resume:
        args += ["--documents", str(source), "--documents-sha256", source_digest]
    assert helper.main(args, activate=activate) == 0
    assert calls == [None if resume else documents]
    record = json.loads(capsys.readouterr().out)
    assert record["plan_digest"] == digest
    assert record["execution"]["execution_state"] == "active"


@pytest.mark.parametrize("drift", ["digest", "path", "mode", "duplicate", "missing-pool", "unpaired-digest"])
def test_activation_command_refuses_unbound_document_inputs(tmp_path, monkeypatch, capsys, drift):
    _, path, digest = _prepared(tmp_path, monkeypatch)
    _, _, source, source_digest = _activation_documents(tmp_path, path)
    if drift == "digest":
        source_digest = "f" * 64
    elif drift == "path":
        other = source.with_name("unbound.json")
        source.rename(other)
        source = other
    elif drift == "mode":
        source.chmod(0o644)
    elif drift in {"duplicate", "missing-pool"}:
        source.write_bytes(b'{"gb10":{},"gb10":{}}' if drift == "duplicate" else b'{"gb10":{}}')
        source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
    args = ["activate-prepared", "--plan", str(path), "--plan-sha256", digest,
        "--documents-sha256", source_digest]
    if drift != "unpaired-digest":
        args += ["--documents", str(source)]
    assert helper.main(args, activate=lambda *args, **kwargs: pytest.fail("invalid input reached executor")) == 2
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("drift", [None, "hash", "path", "mode", "symlink", "missing", "extra", "no-documents", "key"])
def test_activation_command_binds_private_native_material(tmp_path, monkeypatch, capsys, drift):
    _, path, digest = _prepared(tmp_path, monkeypatch)
    owner, _documents, source, source_digest = _activation_documents(tmp_path, path, native=True)
    material = {pool: request.native_delivery_material for pool, request in owner.requests.items()}
    payload = {pool: value.to_dict() for pool, value in material.items()}
    if drift == "missing":
        payload.pop("oldlab")
    elif drift == "extra":
        payload["unbound"] = payload["gb10"]
    elif drift == "key":
        from loom_cli.rollout.operator.protected_native_delivery_material import (
            NativeDeliveryMaterial,
        )
        payload["oldlab"] = NativeDeliveryMaterial(b"a" * 64, b"b" * 64, b"c" * 64).to_dict()
    material_path = path.with_name("execution-activation-native-material.json")
    raw = json.dumps(payload).encode()
    material_path.write_bytes(raw)
    material_path.chmod(0o600)
    material_digest = hashlib.sha256(raw).hexdigest()
    if drift == "hash":
        material_digest = "f" * 64
    elif drift in {"path", "symlink"}:
        other = material_path.with_name("unbound-material.json")
        material_path.rename(other)
        if drift == "path":
            material_path = other
        else:
            material_path.symlink_to(other)
    elif drift == "mode":
        material_path.chmod(0o644)
    calls = []
    def activate(plan, *, documents, native_material):
        calls.append(plan)
        assert native_material == material
        return owner.expected
    args = ["activate-prepared", "--plan", str(path), "--plan-sha256", digest,
        "--native-material", str(material_path), "--native-material-sha256", material_digest]
    if drift != "no-documents":
        args += ["--documents", str(source), "--documents-sha256", source_digest]
    assert helper.main(args, activate=activate) == (0 if drift is None else 2)
    assert len(calls) == (1 if drift is None else 0)
    captured = capsys.readouterr()
    assert all(value not in captured.out + captured.err for entry in payload.values() for value in entry.values())
