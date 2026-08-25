"""Fail-closed security policy for Daytona service-mode trials.

Hosted sandboxes receive only task data plus short-lived Loom Gateway step
credentials. Provider keys, worker credentials, public egress, arbitrary CIDRs,
and unreviewed domains are outside the first supported cloud-burst slice.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from urllib.parse import urlsplit

from loom.models.networking import Allowlist, NetworkPolicy, NoNetwork, Public
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.security.redaction import redact_environment_mapping, redact_text

_IN_PROCESS_AGENTS = frozenset(
    {"oracle", "direct-completion", "litellm", "terminus-2"}
)


class DaytonaSecurityError(ValueError):
    """Secret-safe Daytona admission failure with a stable reason code."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        super().__init__(f"{code}: {detail}")


@dataclass(frozen=True)
class DaytonaTrialSecurity:
    baseline_network_policy: NetworkPolicy
    allowed_network_domains: frozenset[str]
    sandbox_gateway_url: str | None


def _gateway_hostname(raw_url: str | None) -> str:
    if raw_url is None:
        raise DaytonaSecurityError(
            "daytona_gateway_url_required",
            "subprocess agents require LOOM_WORKER_SUBPROCESS_GATEWAY_URL",
        )
    try:
        parsed = urlsplit(raw_url)
        port = parsed.port
    except ValueError:
        raise DaytonaSecurityError(
            "daytona_gateway_url_unsafe",
            "sandbox Gateway URL is malformed",
        ) from None
    host = (parsed.hostname or "").rstrip(".").lower()
    if (
        parsed.scheme != "https"
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or "." not in host
        or host.endswith((".local", ".internal"))
        or parsed.query
        or parsed.fragment
    ):
        raise DaytonaSecurityError(
            "daytona_gateway_url_unsafe",
            "sandbox Gateway URL must be a credential-free public HTTPS origin on port 443",
        )
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise DaytonaSecurityError(
            "daytona_gateway_url_unsafe",
            "sandbox Gateway URL must use a reviewed DNS hostname",
        )
    return host


def _validate_network_policy(
    policy: NetworkPolicy,
    *,
    allowed_domains: frozenset[str],
    field: str,
) -> None:
    if isinstance(policy, NoNetwork):
        return
    if isinstance(policy, Public):
        raise DaytonaSecurityError(
            "daytona_public_egress_denied",
            f"{field} cannot request public internet access",
        )
    if isinstance(policy, Allowlist):
        normalized = frozenset(domain.rstrip(".").lower() for domain in policy.domains)
        if policy.cidrs or not normalized.issubset(allowed_domains):
            raise DaytonaSecurityError(
                "daytona_network_allowlist_denied",
                f"{field} may allow only the reviewed Loom Gateway hostname",
            )
        return
    raise DaytonaSecurityError(
        "daytona_network_policy_unsupported",
        f"{field} uses an unsupported network policy",
    )


def build_daytona_trial_security(
    *,
    task_config: TaskConfig,
    trial_config: TrialConfig,
    sandbox_gateway_url: str | None,
) -> DaytonaTrialSecurity:
    """Validate one trial and return the exact sandbox network authority."""

    sensitive_names = sorted(
        redact_text(entry.name)
        for entry in redact_environment_mapping(task_config.environment.environment)
        if entry.sensitive
    )
    if sensitive_names:
        raise DaytonaSecurityError(
            "daytona_secret_environment_denied",
            "task environment contains secret-bearing variable names: "
            + ", ".join(sensitive_names),
        )
    if task_config.environment.extra_hosts or task_config.environment.dns:
        raise DaytonaSecurityError(
            "daytona_network_override_denied",
            "custom host aliases and DNS servers are not allowed in hosted sandboxes",
        )

    gateway_url: str | None = None
    allowed_domains: frozenset[str] = frozenset()
    secure_default: NetworkPolicy = NoNetwork()
    if trial_config.agent_name not in _IN_PROCESS_AGENTS:
        host = _gateway_hostname(sandbox_gateway_url)
        gateway_url = sandbox_gateway_url
        allowed_domains = frozenset({host})
        secure_default = Allowlist(domains=(host,))

    requested_baseline = trial_config.baseline_network_policy_override
    if requested_baseline is None and not isinstance(
        task_config.environment.baseline_network_policy,
        Public,
    ):
        requested_baseline = task_config.environment.baseline_network_policy
    baseline = requested_baseline or secure_default
    _validate_network_policy(
        baseline,
        allowed_domains=allowed_domains,
        field="baseline network policy",
    )

    for step in task_config.steps:
        if step.network is None:
            continue
        for phase_name, policy in (
            ("agent", step.network.agent_phase),
            ("verifier", step.network.verifier_phase),
        ):
            if policy is not None:
                _validate_network_policy(
                    policy,
                    allowed_domains=allowed_domains,
                    field=(
                        f"step {redact_text(step.name)!r} {phase_name} network policy"
                    ),
                )

    return DaytonaTrialSecurity(
        baseline_network_policy=baseline,
        allowed_network_domains=allowed_domains,
        sandbox_gateway_url=gateway_url,
    )
