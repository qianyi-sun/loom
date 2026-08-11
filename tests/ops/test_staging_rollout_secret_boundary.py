from __future__ import annotations

import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest
from scripts.ops import verify_staging_rollout_secret_boundary as verifier


def test_exact_secret_match_fails_without_printing_value_or_context() -> None:
    secret = b"sensitive-token-fixture"
    artifacts = [
        verifier.Artifact("requests/events.jsonl", b'prefix "token": "' + secret + b'" suffix'),
        verifier.Artifact("rollouts/summary.json", b"clean"),
    ]
    stdout = io.StringIO()

    with redirect_stdout(stdout):
        rc = verifier.main(["--format", "json"], secrets=[secret], artifacts=artifacts)

    assert rc == 1
    output = stdout.getvalue()
    assert secret.decode() not in output
    assert "prefix" not in output and "suffix" not in output
    payload = json.loads(output)
    assert payload == [
        {"bytes_scanned": len(artifacts[0].payload), "match_count": 1, "path": artifacts[0].path},
        {"bytes_scanned": 5, "match_count": 0, "path": artifacts[1].path},
    ]


def test_clean_scan_exits_zero_and_reports_only_counts() -> None:
    stdout = io.StringIO()
    with redirect_stdout(stdout):
        rc = verifier.main(
            [],
            secrets=[b"configured-secret"],
            artifacts=[verifier.Artifact("journald:unit", b"safe metadata")],
        )
    assert rc == 0
    assert stdout.getvalue() == "journald:unit\tbytes=13\tmatches=0\n"


def test_load_configured_secrets_includes_tokens_catalog_values_and_private_key(
    tmp_path: Path,
) -> None:
    token_paths = [tmp_path / name for name in ("admin", "worker", "service")]
    for index, path in enumerate(token_paths):
        path.write_text(f"token-{index}\n", encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text(
        "\n".join(
            [
                f'admin_token_source = "file:{token_paths[0]}"',
                f'worker_token_source = "file:{token_paths[1]}"',
                f'service_token_source = "file:{token_paths[2]}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    catalog = tmp_path / "catalog.env"
    catalog.write_text(
        "PUBLIC=value\nSECRET='catalog-secret' # ignored comment\n", encoding="utf-8"
    )
    private_key = tmp_path / "key"
    private_key.write_text("private-key-line-one\nprivate-key-line-two\n", encoding="utf-8")
    taskset_token = tmp_path / "taskset-token"
    taskset_token.write_text("taskset-token-fixture\n", encoding="utf-8")
    worker_env = tmp_path / "worker.env"
    worker_env.write_text(
        "LOOM_WORKER_TOKEN=worker-env-token\n"
        "LOOM_WORKER_MINIO_ACCESS_KEY=worker-env-access\n"
        "LOOM_WORKER_MINIO_SECRET_KEY=worker-env-secret\n",
        encoding="utf-8",
    )

    secrets = verifier.load_configured_secrets(
        config_path=config,
        catalog_path=catalog,
        private_key_path=private_key,
        taskset_token_path=taskset_token,
        worker_env_path=worker_env,
    )

    assert secrets == (
        b"token-0",
        b"token-1",
        b"token-2",
        b"catalog-secret",
        b"worker-env-token",
        b"worker-env-access",
        b"worker-env-secret",
        b"taskset-token-fixture",
        b"private-key-line-one\nprivate-key-line-two",
        b"private-key-line-one",
        b"private-key-line-two",
    )


def test_configured_secret_reader_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_text("secret\n", encoding="utf-8")
    link = tmp_path / "link"
    link.symlink_to(target)
    config = tmp_path / "config.toml"
    config.write_text(
        "\n".join(
            [
                f'admin_token_source = "file:{link}"',
                f'worker_token_source = "file:{target}"',
                f'service_token_source = "file:{target}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(verifier.BoundaryScanError, match="unavailable"):
        verifier.load_configured_secrets(
            config_path=config,
            catalog_path=target,
            private_key_path=target,
            taskset_token_path=target,
            worker_env_path=target,
        )


def test_artifact_tree_symlink_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "requests"
    root.mkdir()
    target = tmp_path / "outside"
    target.write_text("clean\n", encoding="utf-8")
    (root / "linked-evidence").symlink_to(target)

    with pytest.raises(verifier.BoundaryScanError, match="symlink"):
        list(verifier._tree_artifacts(root))


def test_broken_artifact_tree_root_symlink_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "requests"
    root.symlink_to(tmp_path / "missing")

    with pytest.raises(verifier.BoundaryScanError, match="unsafe"):
        list(verifier._tree_artifacts(root))


def test_live_journal_scan_selects_user_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        calls.append(list(argv))
        return type("Completed", (), {"returncode": 0, "stdout": b""})()

    monkeypatch.setattr(verifier.subprocess, "run", fake_run)
    monkeypatch.setattr(verifier, "REQUEST_ROOT", Path("/nonexistent-request-root"))
    monkeypatch.setattr(verifier, "ROLLOUT_ROOT", Path("/nonexistent-rollout-root"))

    list(verifier.live_artifacts())

    assert "--user-unit=loom-staging-rollout-*" in calls[0]
    assert all(not argument.startswith("--unit=") for argument in calls[0])


def test_live_journal_scan_fails_closed_on_any_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        del argv, kwargs
        return type("Completed", (), {"returncode": 1, "stdout": b""})()

    monkeypatch.setattr(verifier.subprocess, "run", fake_run)

    with pytest.raises(verifier.BoundaryScanError, match="journald export"):
        list(verifier.live_artifacts())


def test_scan_error_does_not_echo_secret() -> None:
    stderr = io.StringIO()
    with redirect_stderr(stderr):
        rc = verifier.main([], secrets=[], artifacts=[])
    assert rc == 2
    assert "no configured secrets" in stderr.getvalue()
