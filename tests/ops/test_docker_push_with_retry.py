from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/docker_push_with_retry.sh"
TARGET = "ghcr.io/qianyi-sun/loom-api:build-test-arm64"
DIGEST = "a" * 64


def _write_executable(path: Path, body: str) -> None:
    path.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{body}", encoding="utf-8")
    path.chmod(0o755)


def _run_push(tmp_path: Path, docker_body: str) -> subprocess.CompletedProcess[str]:
    assert SCRIPT.is_file(), "the Docker push retry helper must exist"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    attempt_file = tmp_path / "attempts"
    sleep_log = tmp_path / "sleeps"
    _write_executable(
        fake_bin / "docker",
        """
if (( $# != 2 )) || [[ "$1" != push || "$2" != "$EXPECTED_TARGET" ]]; then
  printf 'unexpected Docker arguments:' >&2
  printf ' <%s>' "$@" >&2
  printf '\\n' >&2
  exit 99
fi
"""
        + docker_body,
    )
    _write_executable(fake_bin / "sleep", 'printf "%s\\n" "$1" >> "$SLEEP_LOG"\n')
    env = os.environ.copy()
    env.update(
        {
            "ATTEMPT_FILE": str(attempt_file),
            "EXPECTED_TARGET": TARGET,
            "PATH": f"{fake_bin}:{env['PATH']}",
            "SLEEP_LOG": str(sleep_log),
        },
    )
    return subprocess.run(
        [str(SCRIPT), TARGET],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def test_push_retries_transient_failures_and_returns_only_success_output(
    tmp_path: Path,
) -> None:
    result = _run_push(
        tmp_path,
        f"""
attempt=$(($(cat "$ATTEMPT_FILE" 2>/dev/null || printf '0') + 1))
printf '%s' "$attempt" > "$ATTEMPT_FILE"
if (( attempt < 3 )); then
  echo "transient push failure $attempt" >&2
  exit 42
fi
echo "build: digest: sha256:{DIGEST} size: 1234"
""",
    )

    assert result.returncode == 0
    assert result.stdout == f"build: digest: sha256:{DIGEST} size: 1234\n"
    assert "docker push attempt 1/3 failed with exit 42" in result.stderr
    assert "transient push failure 1" in result.stderr
    assert "retrying Docker push in 5 seconds" in result.stderr
    assert "docker push attempt 2/3 failed with exit 42" in result.stderr
    assert "transient push failure 2" in result.stderr
    assert "retrying Docker push in 15 seconds" in result.stderr
    assert (tmp_path / "attempts").read_text(encoding="utf-8") == "3"
    assert (tmp_path / "sleeps").read_text(encoding="utf-8").splitlines() == ["5", "15"]


def test_push_stops_after_bound_and_prints_every_failed_attempt(tmp_path: Path) -> None:
    result = _run_push(
        tmp_path,
        """
attempt=$(($(cat "$ATTEMPT_FILE" 2>/dev/null || printf '0') + 1))
printf '%s' "$attempt" > "$ATTEMPT_FILE"
echo "registry failure $attempt" >&2
exit 17
""",
    )

    assert result.returncode == 17
    assert result.stdout == ""
    for attempt in range(1, 4):
        assert f"docker push attempt {attempt}/3 failed with exit 17" in result.stderr
        assert f"registry failure {attempt}" in result.stderr
    assert result.stderr.count("retrying Docker push") == 2
    assert (tmp_path / "attempts").read_text(encoding="utf-8") == "3"
    assert (tmp_path / "sleeps").read_text(encoding="utf-8").splitlines() == ["5", "15"]
