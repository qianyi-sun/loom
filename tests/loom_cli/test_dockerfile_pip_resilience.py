"""Dockerfile pip resilience checks (#199).

Ensures every rollout-critical Dockerfile that runs `pip install` also sets
`PIP_RETRIES` and `PIP_DEFAULT_TIMEOUT` env vars so a single transient PyPI
ReadTimeoutError (like the one that aborted `staging-92f0090`) doesn't
kill the rollout build. These env vars are honored by pip itself with no
per-invocation flag surgery.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY_DIR = _REPO_ROOT / "deploy"

# Rollout-critical service images. These are the ones the staging driver
# builds + kind-loads before every rollout. Sandbox images (agent, gateway)
# are built out-of-band and get the same treatment for consistency, but the
# blast radius of a sandbox build failing during a rollout is lower.
_ROLLOUT_CRITICAL_DOCKERFILES = (
    "Dockerfile.control-plane",
    "Dockerfile.gateway",
    "Dockerfile.service",
    "Dockerfile.worker",
    "Dockerfile.egress-xds",
)

# Sandbox / auxiliary Dockerfiles. Still nice-to-have but not release-gating.
_AUXILIARY_DOCKERFILES = (
    "Dockerfile.agent-sandbox",
    "Dockerfile.gateway-sandbox",
    "Dockerfile.web",  # multi-stage; only build-stage runs npm not pip.
)


def _read_dockerfile(name: str) -> str:
    return (_DEPLOY_DIR / name).read_text()


def _has_pip_install(text: str) -> bool:
    """True if the Dockerfile actually runs pip install anywhere."""
    return bool(re.search(r"\bpip\s+install\b", text)) or bool(
        re.search(r"\bpython\s+-m\s+pip\s+install\b", text)
    )


def _env_declares(text: str, *keys: str) -> dict[str, str | None]:
    """Return each key -> its value if present in an ENV line, else None."""
    out: dict[str, str | None] = {k: None for k in keys}
    # ENV lines can be `ENV K=V K=V ...` or `ENV K V` (legacy form).
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("ENV "):
            continue
        payload = line[4:].strip()
        # Split on whitespace, handling `K=V` and `K V` forms.
        if "=" in payload.split()[0]:
            # `ENV K=V K=V ...`
            for pair in payload.split():
                if "=" not in pair:
                    continue
                k, v = pair.split("=", 1)
                if k in out:
                    out[k] = v
        else:
            # Legacy `ENV K V` — only one key/value pair per line.
            parts = payload.split(None, 1)
            if len(parts) == 2 and parts[0] in out:
                out[parts[0]] = parts[1]
    return out


class TestRolloutCriticalPipResilience:
    """Every rollout-critical Dockerfile must set pip retry + timeout."""

    @pytest.mark.parametrize("dockerfile", _ROLLOUT_CRITICAL_DOCKERFILES)
    def test_pip_retries_and_timeout_env_set(self, dockerfile: str) -> None:
        text = _read_dockerfile(dockerfile)
        # Sanity: these Dockerfiles must actually run pip install.
        assert _has_pip_install(text), (
            f"expected {dockerfile} to run pip install; if it doesn't, "
            "remove it from _ROLLOUT_CRITICAL_DOCKERFILES."
        )
        env = _env_declares(text, "PIP_RETRIES", "PIP_DEFAULT_TIMEOUT")
        assert env["PIP_RETRIES"] is not None, (
            f"{dockerfile} must declare PIP_RETRIES so a single transient "
            "PyPI ReadTimeout doesn't fail the whole rollout build (#199)"
        )
        assert env["PIP_DEFAULT_TIMEOUT"] is not None, (
            f"{dockerfile} must declare PIP_DEFAULT_TIMEOUT"
        )
        # Values should be sensible for a rollout: at least 5 retries and
        # 30s timeout. Higher is fine.
        retries = int(env["PIP_RETRIES"])
        timeout = int(env["PIP_DEFAULT_TIMEOUT"])
        assert retries >= 5, (
            f"{dockerfile}: PIP_RETRIES={retries} is too low; require >=5"
        )
        assert timeout >= 30, (
            f"{dockerfile}: PIP_DEFAULT_TIMEOUT={timeout} is too low; "
            "require >=30 (a single wheel fetch under a slow link can "
            "take longer than pip's 15s default)"
        )


class TestAuxiliaryDockerfileNoRegression:
    """Sandbox/auxiliary Dockerfiles keep working; we don't require them
    to set the env but if they do the parser must not error out."""

    @pytest.mark.parametrize("dockerfile", _AUXILIARY_DOCKERFILES)
    def test_dockerfile_parses(self, dockerfile: str) -> None:
        # Parser exercise; guards against a syntax mistake in an auxiliary file
        # that would trip up ci/lint.
        _read_dockerfile(dockerfile)
