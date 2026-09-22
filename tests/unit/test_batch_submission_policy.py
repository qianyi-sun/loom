"""Malformed JSON input must be rejected before batch admission."""
import pytest
from fastapi import HTTPException

from loom_service.routes.batches import _reject_invalid_workspace_staging_policy_name


@pytest.mark.parametrize("value", [[], {}, ["tb21"], True, 1, "unknown"])
def test_invalid_workspace_policy_returns_bad_request(value) -> None:
    with pytest.raises(HTTPException) as error:
        _reject_invalid_workspace_staging_policy_name({"workspace_staging_policy_name": value})
    assert error.value.status_code == 400
    assert "must be 'tb21' or 'none'" in error.value.detail


@pytest.mark.parametrize("value", [None, "tb21", "none"])
def test_supported_workspace_policy_is_accepted(value) -> None:
    _reject_invalid_workspace_staging_policy_name({"workspace_staging_policy_name": value})
