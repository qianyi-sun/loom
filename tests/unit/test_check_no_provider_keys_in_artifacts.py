"""scan_bytes + scan_paths in scripts/check_no_provider_keys_in_artifacts.py
(#78 Phase D PR-D2). No MinIO touch — that's exercised in CI.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts" / "check_no_provider_keys_in_artifacts.py"
)


@pytest.fixture(scope="module")
def audit_module():
    spec = importlib.util.spec_from_file_location("audit", _SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["audit"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_clean_buffer_yields_no_hits(audit_module) -> None:
    data = b"the LLM said hello world; the agent responded ok"
    assert list(audit_module.scan_bytes(data, source="x.txt")) == []


def test_anthropic_key_detected(audit_module) -> None:
    payload = b"prefix sk-ant-abc1234567890abcdefghij suffix"
    hits = list(audit_module.scan_bytes(payload, source="t.txt"))
    assert len(hits) == 1
    assert hits[0].provider == "anthropic"
    # Redacted snippet must show head+tail but hide the middle.
    assert "sk-ant" in hits[0].snippet
    assert "…" in hits[0].snippet


def test_openai_key_detected(audit_module) -> None:
    payload = b"OPENAI_API_KEY=sk-proj1234567890abcdefghij12345678901234567890XYZ"
    hits = list(audit_module.scan_bytes(payload, source="env"))
    assert len(hits) == 1
    assert hits[0].provider == "openai"


def test_openai_does_not_collide_with_anthropic(audit_module) -> None:
    # The openai regex must use a negative lookahead so an `sk-ant-`
    # token doesn't also fire it.
    payload = b"sk-ant-abcdefghijklmnopqrstu"
    hits = list(audit_module.scan_bytes(payload, source="t"))
    providers = {h.provider for h in hits}
    assert providers == {"anthropic"}


def test_google_aiza_detected(audit_module) -> None:
    payload = b"GOOGLE=AIzaSyABCDEFGHIJKLMNOPQRSTUVWXYZ0123456 end"
    hits = list(audit_module.scan_bytes(payload, source="env"))
    assert len(hits) == 1
    assert hits[0].provider == "google"


def test_replicate_token_detected(audit_module) -> None:
    payload = b"key=r8_abcdef0123456789abcdef0123456789abcdef"
    hits = list(audit_module.scan_bytes(payload, source="env"))
    assert any(h.provider == "replicate" for h in hits)


def test_loom_worker_token_detected(audit_module) -> None:
    payload = b"Authorization: Bearer loom_w_" + (b"a" * 64) + b"\n"
    hits = list(audit_module.scan_bytes(payload, source="trajectory.jsonl"))
    assert any(h.provider == "loom_worker" for h in hits)


def test_loom_worker_token_short_lookalike_not_detected(audit_module) -> None:
    # Prefix without 64 hex chars must not fire — narrow shape because
    # the runtime always mints exactly 64-hex tokens.
    payload = b"loom_w_short and loom_w_" + (b"a" * 63) + b" placeholder"
    hits = list(audit_module.scan_bytes(payload, source="t"))
    assert hits == []


def test_loom_batch_runner_token_detected(audit_module) -> None:
    payload = b"x-batch-runner-cp-token: loom_br_abcDEF0123-456789_xyz\n"
    hits = list(audit_module.scan_bytes(payload, source="env"))
    assert any(h.provider == "loom_batch_runner" for h in hits)


def test_short_lookalike_not_detected(audit_module) -> None:
    # `sk-ant-` is the prefix; without ≥20 trailing chars it shouldn't
    # fire. False positive rate matters because the script runs on
    # arbitrary trial outputs.
    payload = b"reference to sk-ant- somewhere"
    hits = list(audit_module.scan_bytes(payload, source="t"))
    assert hits == []


def test_redact_long_token(audit_module) -> None:
    redacted = audit_module._redact(b"sk-ant-XYZABC1234567890DEFGHI")
    assert redacted.startswith("sk-ant")
    assert "…" in redacted
    assert "GHI" in redacted  # tail visible


def test_redact_short_token(audit_module) -> None:
    redacted = audit_module._redact(b"verysecret")
    # The literal token bytes must NOT appear verbatim — only a
    # short prefix + the "(short)" marker label.
    assert "verysecret" not in redacted
    assert "ver" in redacted
    assert "(short)" in redacted


def test_scan_paths_walks_directory_tree(audit_module, tmp_path: Path) -> None:
    # Build a tree with one clean + one tainted file.
    (tmp_path / "clean.txt").write_bytes(b"hello world")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "tainted.txt").write_bytes(
        b"x AIzaSyABCDEFGHIJKLMNOPQRSTUVWXYZ0123456 y",
    )
    hits = list(audit_module.scan_paths([tmp_path]))
    assert len(hits) == 1
    assert hits[0].source.endswith("tainted.txt")
    assert hits[0].provider == "google"


def test_main_exits_zero_on_clean_path(
    audit_module, tmp_path: Path, capsys
) -> None:
    (tmp_path / "f.txt").write_bytes(b"nothing to see")
    rc = audit_module.main([str(tmp_path)])
    assert rc == 0
    captured = capsys.readouterr()
    assert '"leaks": false' in captured.out


def test_main_exits_one_on_tainted_path(
    audit_module, tmp_path: Path, capsys
) -> None:
    (tmp_path / "f.txt").write_bytes(
        b"key=AIzaSyABCDEFGHIJKLMNOPQRSTUVWXYZ0123456",
    )
    rc = audit_module.main([str(tmp_path)])
    assert rc == 1
    captured = capsys.readouterr()
    assert "SECRET LEAK DETECTED" in captured.err
    assert '"leaks": true' in captured.out
