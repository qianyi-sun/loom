"""License execution policy helpers.

`Task.license` keeps the source/license metadata. The execution policy says
whether a non-allowlisted license is a hard submit blocker or a launch-time
notice for public benchmark mirrors that remain acceptable for internal
research evaluation.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

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


def is_license_allowed_for_submit(
    *,
    task_license: str | None,
    allowlist: Iterable[str],
    tags: Mapping[str, str] | None,
) -> bool:
    if task_license is None:
        return True
    if task_license in set(allowlist):
        return True
    return (tags or {}).get(LICENSE_EXECUTION_POLICY_TAG) == LICENSE_POLICY_NOTICE
