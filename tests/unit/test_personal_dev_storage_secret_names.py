"""Personal workload credentials must never alias across namespace lifetimes."""

from dataclasses import replace
from importlib import import_module
from uuid import uuid4

import pytest

from loom.dev_instance import derive_identity
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim

_PURPOSES = (
    "loom-secrets", "loom-admin-secret", "loom-protected-worker-runtime",
    "loom-capacity-agent", "loom-capacity-agent-credentials",
)


@pytest.mark.parametrize("purpose", _PURPOSES)
def test_secret_names_preserve_legacy_and_use_full_bound_incarnation(purpose):
    resolve = import_module("loom.personal_dev_incarnation_storage").personal_dev_secret_name
    binding = _bound_claim().operation.storage_binding
    first = binding.identity
    successor = binding.model_copy(update={"subject_incarnation": uuid4()}).identity
    assert resolve(derive_identity(first.name), purpose) == purpose
    assert resolve(first, purpose) == f"{purpose}-{binding.subject_incarnation.hex}"
    assert resolve(first, purpose) != resolve(successor, purpose)
    # Secret names use the 253-character DNS-subdomain bound, not the
    # 63-character label/volume-name bound. Logical volume names stay stable.
    assert 1 <= len(resolve(first, purpose)) <= 253
    assert first.namespace == successor.namespace == f"loom-dev-{first.name}"


@pytest.mark.parametrize("defect", ("missing_binding", "wrong_database", "wrong_owner_name"))
def test_secret_name_rejects_noncanonical_identity(defect):
    resolve = import_module("loom.personal_dev_incarnation_storage").personal_dev_secret_name
    identity = _bound_claim().operation.storage_binding.identity
    if defect == "missing_binding":
        identity = replace(identity, storage_binding=None)
    elif defect == "wrong_database":
        identity = replace(identity, database="forged")
    else:
        identity = replace(identity, name="another")
    with pytest.raises(ValueError):
        resolve(identity, "loom-secrets")


@pytest.mark.parametrize("purpose", ("", "foreign", "loom-secrets-other", "../loom-secrets"))
def test_secret_names_reject_unknown_purposes(purpose):
    resolve = import_module("loom.personal_dev_incarnation_storage").personal_dev_secret_name
    with pytest.raises(ValueError):
        resolve(_bound_claim().operation.storage_binding.identity, purpose)
