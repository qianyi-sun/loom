"""Fixed private credential paths consumed by installed deep preflight."""

from pathlib import Path

PREFLIGHT_CREDENTIAL_ROOT = Path("/var/lib/loom-staging-rollout/credentials")
READONLY_KUBECONFIG_PATH = PREFLIGHT_CREDENTIAL_ROOT / "readonly-kubeconfig"
READONLY_TOKEN_PATH = PREFLIGHT_CREDENTIAL_ROOT / "readonly-probe-token"
REHEARSAL_KUBECONFIG_PATH = PREFLIGHT_CREDENTIAL_ROOT / "rehearsal-kubeconfig"

__all__ = [
    "PREFLIGHT_CREDENTIAL_ROOT",
    "READONLY_KUBECONFIG_PATH",
    "READONLY_TOKEN_PATH",
    "REHEARSAL_KUBECONFIG_PATH",
]
