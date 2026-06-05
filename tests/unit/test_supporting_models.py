from pathlib import PurePosixPath

import pytest
from pydantic import ValidationError

from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.skill import SkillRef, SkillSource
from loom.models.types import ModelSpec


def test_healthcheck_defaults():
    h = HealthcheckSpec(command="test -d /workspace")
    assert h.start_period_sec == 0
    assert h.interval_sec == 5
    assert h.timeout_sec == 3
    assert h.retries == 6


def test_healthcheck_rejects_negative():
    with pytest.raises(ValidationError):
        HealthcheckSpec(command="x", retries=-1)


def test_model_spec_minimum():
    m = ModelSpec(provider="anthropic", name="claude-opus-4-7")
    assert m.provider == "anthropic"
    assert m.tier is None


def test_skill_ref_local_source():
    s = SkillRef(name="search", source=SkillSource(kind="local", path=PurePosixPath("/skills/search")))
    assert s.source.kind == "local"


def test_exec_result_truncated_flag():
    r = ExecResult(return_code=0, stdout=b"a" * 100, stderr=b"", truncated=True, duration_sec=1.5)
    assert r.truncated is True
    assert r.return_code == 0


def test_exec_result_immutable():
    r = ExecResult(return_code=0, stdout=b"", stderr=b"", truncated=False, duration_sec=0.1)
    with pytest.raises(ValidationError):
        r.return_code = 1  # type: ignore[misc]
