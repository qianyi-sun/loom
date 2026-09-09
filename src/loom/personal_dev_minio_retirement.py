"""Monotonic IAM retirement for incarnation-bound MinIO tenants.

Denied users and their Deny policies are tombstones, not cleanup garbage.
This prevents new authenticated requests, not already-authorized S3 effects.
"""

from __future__ import annotations

import base64
import json
import secrets
import shlex
from typing import Any

from loom.dev_instance import DevInstanceIdentity
from loom.dev_instance_runtime import DevInstanceRuntimeError, KubectlMinioTenantProvisioner
from loom.personal_dev_incarnation_storage import validate_personal_dev_storage_identity
from loom_capacity_manager.contracts import canonical_digest


def _deny_name(identity: DevInstanceIdentity) -> str:
    validate_personal_dev_storage_identity(identity)
    if identity.storage_binding is None:
        raise ValueError("IAM retirement requires bound storage")
    return "loom-retired-v1-" + canonical_digest(identity.storage_binding)


async def _execute(tenant: KubectlMinioTenantProvisioner, script: str, *, stdin: str | None = None) -> str:
    prelude = '\n'.join((
        'test -n "${MINIO_ROOT_USER:-}"', 'test -n "${MINIO_ROOT_PASSWORD:-}"',
        'export MC_HOST_fixture="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"',
    )) + '\n'
    result = await tenant.kubectl.runner.run(tenant.kubectl._argv(
        "exec", "--namespace", tenant.namespace, "--container", tenant.container,
        "--stdin", tenant.pod, "--", "/bin/sh", "-eu", "-c", prelude + script,
    ), stdin=stdin, timeout_seconds=60)
    return result.stdout


def _result(payload: str) -> dict[str, Any]:
    try:
        if len(payload.encode()) > 64 * 1024:
            raise ValueError
        result = json.loads(payload)
        if not isinstance(result, dict) or result.get("status") not in {"success", "error"}:
            raise ValueError
        return result
    except (ValueError, TypeError, RecursionError):
        raise DevInstanceRuntimeError("MinIO authority readback is invalid") from None


def _error_code(result: dict[str, Any]) -> str | None:
    try:
        value = result["error"]["cause"]["error"]["Code"]
        return value if isinstance(value, str) else None
    except (KeyError, TypeError):
        return None


async def _lookup(tenant: KubectlMinioTenantProvisioner, kind: str, name: str) -> dict[str, Any] | None:
    result = _result(await _execute(tenant,
        f"mc admin {kind} info fixture {shlex.quote(name)} --json || true",
    ))
    if result["status"] == "error":
        if _error_code(result) == {"policy": "XMinioAdminNoSuchPolicy", "user": "XMinioAdminNoSuchUser"}[kind]:
            return None
        raise DevInstanceRuntimeError("MinIO authority lookup failed")
    return result


def _policy_shape(policy: Any) -> str:
    try:
        normalized = json.loads(json.dumps(policy, allow_nan=False))
        if not isinstance(normalized, dict) or not isinstance(normalized["Statement"], list):
            raise ValueError
        for statement in normalized["Statement"]:
            for field in ("Action", "Resource"):
                values = statement[field]
                if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                    raise ValueError
                # The release-pinned MinIO serializes an all-S3-resource '*'
                # as '**'. This equivalence is exercised against actual IAM.
                statement[field] = sorted("**" if field == "Resource" and value == "*" else value for value in values)
        normalized["Statement"].sort(key=lambda value: json.dumps(value, sort_keys=True))
        return json.dumps(normalized, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError, KeyError, AttributeError):
        raise DevInstanceRuntimeError("MinIO policy shape is invalid") from None


def _verify_policy(observed: dict[str, Any] | None, name: str, desired: dict[str, Any]) -> None:
    try:
        if observed is None or observed["policy"] != name or observed["policyInfo"]["PolicyName"] != name:
            raise ValueError
        if _policy_shape(observed["policyInfo"]["Policy"]) != _policy_shape(desired):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise DevInstanceRuntimeError("MinIO owned policy differs from its immutable intent") from None


async def _ensure_policy(tenant: KubectlMinioTenantProvisioner, name: str, policy: dict[str, Any]) -> None:
    observed = await _lookup(tenant, "policy", name)
    if observed is None:
        payload = base64.b64encode(json.dumps(policy, sort_keys=True).encode()).decode()
        try:
            await _execute(tenant, '\n'.join((
                "umask 077", "policy_file=$(mktemp)",
                "trap 'rm -f -- \"$policy_file\"' EXIT HUP INT TERM",
                f'printf %s {shlex.quote(payload)} | base64 -d >"$policy_file"',
                f'mc admin policy create fixture {shlex.quote(name)} "$policy_file" >/dev/null',
            )))
        except DevInstanceRuntimeError:
            observed = await _lookup(tenant, "policy", name)
            if observed is None:
                raise
        else:
            observed = await _lookup(tenant, "policy", name)
    _verify_policy(observed, name, policy)


def _user_policies(observed: dict[str, Any] | None, access: str) -> set[str]:
    if observed is None:
        return set()
    if (observed.get("accessKey") != access or observed.get("userStatus") not in {"enabled", "disabled"}
        or observed.get("memberOf") or not isinstance(observed.get("policyName", ""), str)):
        raise DevInstanceRuntimeError("MinIO tenant identity is invalid")
    return set(filter(None, observed.get("policyName", "").split(",")))


async def _credentials(tenant: KubectlMinioTenantProvisioner, access: str, secret: str) -> None:
    await _execute(tenant, '\n'.join((
        "IFS= read -r access_key", "IFS= read -r secret_key",
        f'test "$access_key" = {shlex.quote(access)}',
        'printf "%s\\n%s\\n" "$access_key" "$secret_key" | mc admin user add fixture >/dev/null',
    )), stdin=f"{access}\n{secret}\n")


def _deny_policy(name: str) -> dict[str, Any]:
    return {"Version": "2012-10-17", "Statement": [{
        "Sid": name, "Effect": "Deny", "Action": ["s3:*"], "Resource": ["*"],
    }]}


async def _assert_not_retired(tenant: KubectlMinioTenantProvisioner, name: str) -> None:
    observed = await _lookup(tenant, "policy", name)
    if observed is not None:
        _verify_policy(observed, name, _deny_policy(name))
        raise DevInstanceRuntimeError("MinIO storage incarnation is permanently retired")


async def converge_bound_tenant(tenant: KubectlMinioTenantProvisioner, identity: DevInstanceIdentity) -> None:
    deny_name = _deny_name(identity)
    # The immutable policy is the durable retirement record, even if the user
    # or its mapping is absent. Its creation starts retirement; attaching Deny
    # and verifying denial are still required before retirement is complete.
    await _assert_not_retired(tenant, deny_name)
    access, allow_name = tenant._names(identity)
    policies = _user_policies(await _lookup(tenant, "user", access), access)
    if deny_name in policies:
        raise DevInstanceRuntimeError("MinIO storage incarnation is permanently retired")
    if policies - {allow_name}:
        raise DevInstanceRuntimeError("MinIO tenant has foreign policy authority")
    expected_access, secret = await tenant.vault.object_credentials(identity)
    if expected_access != access:
        raise DevInstanceRuntimeError("MinIO tenant credential identity differs")
    await _ensure_policy(tenant, allow_name, tenant._policy(identity))
    await _credentials(tenant, access, secret)
    await _execute(tenant, f"mc admin policy attach fixture {shlex.quote(allow_name)} --user {shlex.quote(access)} >/dev/null")
    policies = _user_policies(await _lookup(tenant, "user", access), access)
    if policies != {allow_name}:
        raise DevInstanceRuntimeError("MinIO tenant authority changed or was retired")
    await _assert_not_retired(tenant, deny_name)


async def retire_bound_tenant(tenant: KubectlMinioTenantProvisioner, identity: DevInstanceIdentity) -> None:
    deny_name = _deny_name(identity)
    access, _ = tenant._names(identity)
    policy = _deny_policy(deny_name)
    await _ensure_policy(tenant, deny_name, policy)
    # Also creates an authority-free principal if retirement preceded first
    # provisioning. Updating credentials preserves attached Deny policies.
    secret = secrets.token_urlsafe(48)
    await _credentials(tenant, access, secret)
    await _execute(tenant, f"mc admin policy attach fixture {shlex.quote(deny_name)} --user {shlex.quote(access)} >/dev/null")
    _verify_policy(await _lookup(tenant, "policy", deny_name), deny_name, policy)
    if deny_name not in _user_policies(await _lookup(tenant, "user", access), access):
        raise DevInstanceRuntimeError("MinIO retirement mapping was not acknowledged")
    challenge = _result(await _execute(tenant, '\n'.join((
        "IFS= read -r access_key", "IFS= read -r secret_key",
        'export MC_HOST_retired="http://${access_key}:${secret_key}@127.0.0.1:9000"',
        f"mc ls {shlex.quote('retired/' + identity.task_bucket)} --json || true",
    )), stdin=f"{access}\n{secret}\n"))
    if challenge["status"] != "error" or _error_code(challenge) != "AccessDenied":
        raise DevInstanceRuntimeError("MinIO retirement denial was not verified")
