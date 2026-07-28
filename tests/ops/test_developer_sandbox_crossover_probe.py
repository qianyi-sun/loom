from __future__ import annotations

import json
from pathlib import Path

from scripts.ops import developer_sandbox_crossover_probe as probe

ADMIN_A = "loom_admin_" + ("A" * 43)
ADMIN_B = "loom_admin_" + ("B" * 43)
WORKER_A = "loom_w_" + ("a" * 40)
WORKER_B = "loom_w_" + ("b" * 40)


def _write_admin(path: Path, token: str) -> None:
    path.write_text(
        f'[admin]\ntoken = "{token}"\ncreated_at = "2026-07-28T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    path.chmod(0o600)


def _write_worker_env(path: Path, token: str) -> None:
    path.write_text(f"LOOM_WORKER_TOKEN={token}\n", encoding="utf-8")
    path.chmod(0o600)


def test_dry_run_matrix_is_secret_free(tmp_path: Path) -> None:
    admin_a = tmp_path / "a-admin.toml"
    admin_b = tmp_path / "b-admin.toml"
    worker_a = tmp_path / "a-worker.env"
    worker_b = tmp_path / "b-worker.env"
    _write_admin(admin_a, ADMIN_A)
    _write_admin(admin_b, ADMIN_B)
    _write_worker_env(worker_a, WORKER_A)
    _write_worker_env(worker_b, WORKER_B)

    evidence_path = tmp_path / "evidence.json"
    code = probe.main(
        [
            "--qianyi-cp-url",
            "http://127.0.0.1:20080",
            "--qianyi-worker-token-file",
            str(worker_a),
            "--qianyi-admin-secret-file",
            str(admin_a),
            "--hongjian-cp-url",
            "http://127.0.0.1:21080",
            "--hongjian-worker-token-file",
            str(worker_b),
            "--hongjian-admin-secret-file",
            str(admin_b),
            "--write-evidence",
            str(evidence_path),
            "--json",
        ],
    )
    assert code == 0
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "dry-run"
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["total"] >= 4
    rendered = json.dumps(payload)
    assert ADMIN_A not in rendered
    assert WORKER_A not in rendered
    assert "loom_w_" not in rendered
    assert "loom_admin_" not in rendered
    fps = {
        row["source_worker_fingerprint"]
        for row in payload["results"]
        if row.get("source_worker_fingerprint")
    }
    assert any(fp and fp.startswith("sha256:") for fp in fps)


def test_directed_pairs_cover_six_edges() -> None:
    pairs = probe.directed_pairs(["qianyi", "hongjian", "devansh"])
    assert len(pairs) == 6
    assert ("qianyi", "hongjian") in pairs
    assert ("hongjian", "qianyi") in pairs
    assert ("qianyi", "qianyi") not in pairs


def test_cli_rejects_literal_token_argv(capsys) -> None:  # type: ignore[no-untyped-def]
    import sys

    old = sys.argv
    try:
        sys.argv = [
            "developer_sandbox_crossover_probe.py",
            "--qianyi-cp-url",
            "http://127.0.0.1:20080",
            WORKER_A,
        ]
        code = probe.main(
            ["--qianyi-cp-url", "http://127.0.0.1:20080", WORKER_A],
        )
    finally:
        sys.argv = old
    assert code == 2
    err = capsys.readouterr().err
    assert "refusing literal token" in err
