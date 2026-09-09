"""Ambiguous IAM readbacks never authorize policy or credential changes."""

import json
from types import SimpleNamespace

import pytest

from loom.dev_instance_runtime import CommandResult, DevInstanceRuntimeError, KubectlClient
from loom.personal_dev_minio_retirement import _lookup, _policy_shape, _result, _user_policies, _verify_policy


@pytest.mark.parametrize("payload", ("", "{}", "[]", "null", "not-json", '{"status":"unknown"}',
                                     '{"status":"error"}\n{"status":"error"}', "x" * 65537))
def test_invalid_or_multirecord_iam_response_is_rejected(payload):
    with pytest.raises(DevInstanceRuntimeError):
        _result(payload)


@pytest.mark.parametrize("kind,code,absent", (
    ("policy", "XMinioAdminNoSuchPolicy", True),
    ("user", "XMinioAdminNoSuchUser", True),
    ("policy", "AccessDenied", False),
    ("user", "AccessDenied", False),
    ("policy", "XMinioAdminNoSuchUser", False),
    ("user", "XMinioAdminNoSuchPolicy", False),
    ("policy", "InternalError", False),
))
async def test_only_exact_missing_resource_error_means_absence(kind, code, absent):
    class Runner:
        async def run(self, argv, **kwargs):
            assert f"mc admin {kind} info" in argv[-1]
            assert "create" not in argv[-1] and "user add" not in argv[-1]
            return CommandResult(json.dumps({"status": "error", "error": {"cause": {"error": {"Code": code}}}}), "")

    tenant = SimpleNamespace(kubectl=KubectlClient("kubectl", runner=Runner()), namespace="fixture", pod="fixture", container="admin")
    if absent:
        assert await _lookup(tenant, kind, "fixture") is None
    else:
        with pytest.raises(DevInstanceRuntimeError):
            await _lookup(tenant, kind, "fixture")


@pytest.mark.parametrize("change", ("missing", "name", "body", "condition"))
def test_policy_readback_must_match_immutable_intent(change):
    policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Action": ["s3:*"], "Resource": ["*"]}]}
    observed = {"policy": "fixture", "policyInfo": {"PolicyName": "fixture", "Policy": json.loads(json.dumps(policy))}}
    if change == "missing":
        observed = None
    elif change == "name":
        observed["policyInfo"]["PolicyName"] = "foreign"
    elif change == "body":
        observed["policyInfo"]["Policy"]["Statement"][0]["Effect"] = "Allow"
    else:
        observed["policyInfo"]["Policy"]["Statement"][0]["Condition"] = {"StringEquals": {"aws:username": "someone-else"}}
    with pytest.raises(DevInstanceRuntimeError):
        _verify_policy(observed, "fixture", policy)


def test_pinned_minio_wildcard_readback_does_not_ignore_other_policy_changes():
    policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Action": ["s3:*"], "Resource": ["*"]}]}
    normalized = json.loads(json.dumps(policy))
    normalized["Statement"][0]["Resource"] = ["**"]
    assert _policy_shape(policy) == _policy_shape(normalized)
    normalized["Statement"][0]["Resource"] = ["arn:aws:s3:::one-bucket/*"]
    assert _policy_shape(policy) != _policy_shape(normalized)


@pytest.mark.parametrize("change", ({"accessKey": "foreign"}, {"userStatus": "unknown"},
                                     {"memberOf": ["foreign-group"]}, {"policyName": ["policy"]}))
def test_user_readback_rejects_wrong_identity_group_or_invalid_shape(change):
    with pytest.raises(DevInstanceRuntimeError):
        _user_policies({"accessKey": "fixture", "userStatus": "enabled", **change}, "fixture")
