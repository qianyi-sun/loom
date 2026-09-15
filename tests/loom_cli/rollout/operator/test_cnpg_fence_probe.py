"""A failed kubectl command alone is not admission-fence enforcement evidence."""

import subprocess
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner


def _denial(suffix):
    name = "loom-cnpg-fence-" + "a" * 24 + "-" + suffix
    return ("Error from server (Forbidden): ValidatingAdmissionPolicy '" + name +
            "' with binding '" + name + "' denied request: loom-cnpg-fence: "
            "protected handoff input is frozen\n").encode()


def test_probe_requires_all_exact_policy_denials_and_only_uses_server_dry_run(monkeypatch):
    runner = SubprocessProtectedApplyCommandRunner()
    suffixes = iter(("cluster", "dependents", "scale", "credentials", "credentials", "monitoring"))
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        assert "--dry-run=server" in argv
        assert kwargs["env"] == runner.environment
        assert kwargs["env"]["KUBECTL_KUBERC"] == "false"
        assert kwargs["timeout"] == 30
        return SimpleNamespace(returncode=1, stdout=b"", stderr=_denial(next(suffixes)))

    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", run)
    assert runner.probe_cnpg_input_fence(intent_digest="a" * 64, target_pooler_names=()) is True
    assert len(calls) == 6
    assert not any("exec" in argv or "delete" in argv for argv, _ in calls)


def test_successful_dry_run_is_not_enforcement(monkeypatch):
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
                        lambda *a, **k: SimpleNamespace(returncode=0, stdout=b"{}", stderr=b""))
    assert SubprocessProtectedApplyCommandRunner().probe_cnpg_input_fence(
        intent_digest="a" * 64, target_pooler_names=()) is False


@pytest.mark.parametrize("stderr", [
    b"Error from server (Forbidden): User cannot patch clusters; raw-secret-value",
    b"connection refused raw-secret-value",
    _denial("dependents"),  # Correct fence, wrong policy for this request.
    _denial("cluster").replace(b"with binding 'loom", b"with binding 'foreign-loom"),
    _denial("cluster").replace(b"(Forbidden)", b"(Invalid)"),
])
def test_unrelated_command_failure_is_never_positive_evidence(monkeypatch, stderr):
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
                        lambda *a, **k: SimpleNamespace(returncode=1, stdout=b"", stderr=stderr))
    with pytest.raises(RuntimeError, match=r"CNPG.*probe") as error:
        SubprocessProtectedApplyCommandRunner().probe_cnpg_input_fence(
            intent_digest="a" * 64, target_pooler_names=())
    assert "raw-secret-value" not in str(error.value)


@pytest.mark.parametrize("failure", [OSError("raw-secret-value"),
                                   subprocess.TimeoutExpired("raw-secret-value", 30)])
def test_probe_transport_failures_are_sanitized(monkeypatch, failure):
    def run(*args, **kwargs):
        raise failure
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", run)
    with pytest.raises(RuntimeError, match=r"CNPG.*probe") as error:
        SubprocessProtectedApplyCommandRunner().probe_cnpg_input_fence(
            intent_digest="a" * 64, target_pooler_names=())
    assert "raw-secret-value" not in str(error.value)


@pytest.mark.parametrize("returncode,stdout,stderr", [
    (2, b"", _denial("cluster")), (-9, b"", _denial("cluster")),
    (1, b"unexpected-output", _denial("cluster")),
    (1, b"x" * 4097, _denial("cluster")),
    (1, b"", _denial("cluster") + b"x" * 4097),
])
def test_malformed_or_oversized_probe_results_refuse(monkeypatch, returncode, stdout, stderr):
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
                        lambda *a, **k: SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr))
    with pytest.raises(RuntimeError, match=r"CNPG.*probe"):
        SubprocessProtectedApplyCommandRunner(max_output_bytes=4096).probe_cnpg_input_fence(
            intent_digest="a" * 64, target_pooler_names=())


@pytest.mark.parametrize("overrides", [{"intent_digest": "a" * 63},
                                     {"target_pooler_names": ["mutable"]},
                                     {"target_pooler_names": ("--help",)}])
def test_invalid_probe_identity_never_executes(monkeypatch, overrides):
    def run(*args, **kwargs):
        pytest.fail("invalid probe reached subprocess")
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", run)
    with pytest.raises(ValueError, match="CNPG"):
        SubprocessProtectedApplyCommandRunner().probe_cnpg_input_fence(**{
            "intent_digest": "a" * 64, "target_pooler_names": (), **overrides,
        })
