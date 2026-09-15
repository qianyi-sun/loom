"""Only the exact permanent policy denial is runtime fence evidence."""

from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner


def denial(suffix):
    name = "loom-legacy-writer-" + "a" * 24 + "-" + suffix
    return (f"Error from server (Forbidden): ValidatingAdmissionPolicy '{name}' with binding '{name}' "
        "denied request: loom-legacy-writer-retirement: legacy writer cannot resume\n").encode()


def test_all_six_permanent_fences_are_probed_without_mutations(monkeypatch):
    runner = SubprocessProtectedApplyCommandRunner()
    suffixes = iter(("scale", "deployments", "replicasets", "replicaset-scale", "pods", "lifecycle"))
    calls = []
    def run(argv, **kwargs):
        assert "--dry-run=server" in argv and kwargs["env"] == runner.environment
        calls.append(argv)
        return SimpleNamespace(returncode=1, stdout=b"", stderr=denial(next(suffixes)))
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", run)
    assert runner.probe_legacy_writer_fence(intent_digest="a" * 64, replica_set_name="loom-service-abcdef")
    assert len(calls) == 6


@pytest.mark.parametrize("failure", ["accepted", "foreign", "authentication", "timeout", "oversized"])
def test_other_failures_never_prove_retirement(monkeypatch, failure):
    runner = SubprocessProtectedApplyCommandRunner(max_output_bytes=4096)
    def run(argv, **kwargs):
        if failure == "timeout":
            raise OSError("private-detail")
        return SimpleNamespace(returncode=0 if failure == "accepted" else 1, stdout=b"",
            stderr=denial("deployments") if failure == "foreign" else
            b"Error from server (Forbidden): private-detail" + (b"x" * 4096 if failure == "oversized" else b""))
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run", run)
    if failure == "accepted":
        assert not runner.probe_legacy_writer_fence(intent_digest="a" * 64, replica_set_name="loom-service-abcdef")
    else:
        with pytest.raises(RuntimeError, match="legacy writer fence probe") as error:
            runner.probe_legacy_writer_fence(intent_digest="a" * 64, replica_set_name="loom-service-abcdef")
        assert "private-detail" not in str(error.value)


@pytest.mark.parametrize("name", ["--help", "foreign-abcdef", "loom-service-", "loom-service-abc/scale"])
def test_probe_never_accepts_unbound_replica_set_names(monkeypatch, name):
    monkeypatch.setattr("loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
        lambda *args, **kwargs: pytest.fail("invalid name reached transport"))
    with pytest.raises(ValueError, match="legacy writer fence"):
        SubprocessProtectedApplyCommandRunner().probe_legacy_writer_fence(intent_digest="a" * 64, replica_set_name=name)
