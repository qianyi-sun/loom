"""License metadata helpers.

`Task.license` and `license_execution_policy` remain catalog/import metadata.
They are informational for research evaluation and must not block task
selection or trial submission.
"""

from __future__ import annotations

from collections.abc import Mapping

LICENSE_EXECUTION_POLICY_TAG = "license_execution_policy"
LICENSE_POLICY_ALLOWLIST = "allowlist"
LICENSE_POLICY_NOTICE = "notice"

_VALID_POLICIES = frozenset({LICENSE_POLICY_ALLOWLIST, LICENSE_POLICY_NOTICE})


def normalize_license_execution_policy(policy: str | None) -> str:
    if policy is None or policy == "":
        return LICENSE_POLICY_ALLOWLIST
    if policy not in _VALID_POLICIES:
        raise ValueError(
            f"unknown license execution policy {policy!r}; expected one of "
            f"{sorted(_VALID_POLICIES)}",
        )
    return policy


def tags_with_license_execution_policy(
    tags: Mapping[str, str] | None,
    policy: str | None,
) -> dict[str, str]:
    """Merge adapter-level policy into task tags.

    Instance tags are untrusted metadata from upstream rows, so an existing
    `license_execution_policy` key is replaced by the adapter/catalog policy.
    """
    merged = dict(tags or {})
    merged.pop(LICENSE_EXECUTION_POLICY_TAG, None)
    normalized = normalize_license_execution_policy(policy)
    if normalized != LICENSE_POLICY_ALLOWLIST:
        merged[LICENSE_EXECUTION_POLICY_TAG] = normalized
    return merged
