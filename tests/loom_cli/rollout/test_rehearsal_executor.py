from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan, RehearsalResources
from loom_cli.rollout.rehearsal_executor import IsolatedRehearsalExecutor, _default_stream_run


def _plan() -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        checkpoint_request_id="req-abcdefgh",
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_path=Path("/data/loom-staging/backups/exact/backup-manifest.json"),
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=8,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={
            "loom-control-plane": "sha256:" + "8" * 64,
            "loom-rehearsal-postgres": "sha256:" + "9" * 64,
            "loom-service": "sha256:" + "1" * 64,
        },
        image_tag="staging-aaaaaaaa",
        image_artifact_sha256="2" * 64,
        artifact_bundle_sha256="6" * 64,
        artifact_descriptor_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/artifact.json"
        ),
        rendered_manifest_path=Path(
            "/var/lib/loom-staging-rollout/preflight-artifacts/" + "6" * 64 + "/rendered.yaml"
        ),
        manifest_artifact_sha256="7" * 64,
        rendered_manifest_sha256="8" * 64,
        migration_plan_sha256="3" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://staging.example.test/dev",
        ),
    )


def test_namespace_uses_fixed_scoped_apply_and_exact_readback() -> None:
    calls: list[tuple[tuple[str, ...], bytes | None, int]] = []
    records: dict[str, object] = {}

    def run(argv, payload, timeout):
        calls.append((tuple(argv), payload, timeout))
        if payload is not None:
            record = json.loads(payload)
            records[str(record["kind"])] = record
        elif "rolebinding" in argv:
            record = records["RoleBinding"]
        elif "networkpolicy" in argv:
            record = records["NetworkPolicy"]
        else:
            record = records["Namespace"]
        return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")

    plan = _plan()
    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.namespace", plan)

    assert outcome.passed
    assert len(calls) == 6
    assert calls[0][0] == (
        "kubectl",
        "--kubeconfig",
        "/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig",
        "apply",
        "--server-side=true",
        "--field-manager=loom-staging-preflight",
        "--request-timeout=30s",
        "-f",
        "-",
        "-o",
        "json",
    )
    assert calls[1][0][3:6] == ("--namespace", plan.resources.namespace, "apply")
    assert calls[2][0][3:6] == ("--namespace", plan.resources.namespace, "apply")
    assert calls[3][0][3:6] == ("get", "namespace", plan.resources.namespace)
    assert calls[4][0][3:6] == ("--namespace", plan.resources.namespace, "get")
    assert calls[5][0][3:6] == ("--namespace", plan.resources.namespace, "get")
    namespace = json.loads(calls[0][1] or b"{}")
    assert namespace["metadata"]["annotations"]["loom.openai.dev/plan-sha256"] == plan.plan_digest
    assert namespace["metadata"]["labels"]["loom.openai.dev/authority"] == "staging-preflight"
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce-version"] == "latest"
    network_policy = json.loads(calls[1][1] or b"{}")
    assert network_policy["spec"] == {
        "podSelector": {},
        "policyTypes": ["Ingress", "Egress"],
    }


def test_namespace_returns_normalized_blockers_without_command_output() -> None:
    plan = _plan()
    failed = IsolatedRehearsalExecutor(
        run=lambda argv, payload, timeout: subprocess.CompletedProcess(
            argv, 1, "", "token=must-not-leak"
        )
    ).execute("rehearsal.namespace", plan)
    assert failed.blockers == {"namespace": "apply-failed"}
    assert "token" not in str(failed)

    calls = 0

    def drift(argv, payload, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(argv, 0, (payload or b"{}").decode(), "")
        return subprocess.CompletedProcess(argv, 0, '{"apiVersion":"v1","kind":"Namespace"}', "")

    blocked = IsolatedRehearsalExecutor(run=drift).execute("rehearsal.namespace", plan)
    assert blocked.blockers == {"namespace": "network-policy-failed"}


def test_database_streams_exact_checkpoint_into_restricted_pod() -> None:
    plan = _plan()
    calls: list[tuple[tuple[str, ...], bytes | None, int]] = []
    streams: list[tuple[tuple[str, ...], Path, int]] = []
    pod: dict[str, object] = {}
    restored = False

    def run(argv, payload, timeout):
        nonlocal pod
        calls.append((tuple(argv), payload, timeout))
        if payload is not None:
            pod = json.loads(payload)
            return subprocess.CompletedProcess(argv, 0, json.dumps(pod), "")
        if "wait" in argv:
            return subprocess.CompletedProcess(argv, 0, "ready\n", "")
        if "get" in argv and "pod" in argv:
            observed = {
                **pod,
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "imageID": "docker://" + plan.image_digests["loom-rehearsal-postgres"],
                            "name": "postgres",
                            "ready": True,
                        },
                        {
                            "imageID": "docker://" + plan.image_digests["loom-control-plane"],
                            "name": "migration",
                            "ready": True,
                        },
                    ],
                },
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(observed), "")
        if "psql" in argv:
            command = next(item for item in argv if item.startswith("--command="))
            if "to_regclass" in command:
                record = {"database": plan.resources.database, "restored": restored}
            else:
                record = {"schema_revision": plan.schema_revision}
            return subprocess.CompletedProcess(argv, 0, json.dumps(record) + "\n", "")
        raise AssertionError(argv)

    def stream(argv, source, timeout):
        nonlocal restored
        streams.append((tuple(argv), source, timeout))
        restored = True
        return subprocess.CompletedProcess(argv, 0, "", "")

    outcome = IsolatedRehearsalExecutor(run=run, stream_run=stream).execute(
        "rehearsal.db-clone", plan
    )

    assert outcome.passed
    assert outcome.details == {
        "database": plan.resources.database,
        "schema-revision": plan.schema_revision,
        "status": "restored",
    }
    assert len(streams) == 1
    assert streams[0][1] == plan.checkpoint_manifest_path.parent / "postgres" / "loom.dump"
    assert streams[0][2] == 1800
    manifest = json.loads(calls[0][1] or b"{}")
    assert manifest["spec"]["automountServiceAccountToken"] is False
    assert manifest["spec"]["containers"][0]["imagePullPolicy"] == "Never"
    assert manifest["spec"]["containers"][0]["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    assert manifest["spec"]["containers"][1]["image"] == ("loom-control-plane:" + plan.image_tag)
    assert manifest["spec"]["containers"][1]["command"] == ["/bin/sleep", "infinity"]


def test_database_rejects_missing_exact_image_before_kubernetes() -> None:
    plan = _plan()
    plan = RehearsalPlan.from_record(
        {
            **plan.to_record(),
            "image_digests": {"loom-service": "sha256:" + "1" * 64},
        }
    )
    calls: list[object] = []

    outcome = IsolatedRehearsalExecutor(
        run=lambda *_args: calls.append(object()),  # type: ignore[arg-type,return-value]
    ).execute("rehearsal.db-clone", plan)

    assert outcome.blockers == {"database": "image-authority-missing"}
    assert calls == []


def test_migration_runs_exact_candidate_against_restored_database() -> None:
    plan = _plan()
    revision = plan.schema_revision
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        nonlocal revision
        command = tuple(argv)
        calls.append(command)
        if "psql" in command:
            sql = next(item for item in command if item.startswith("--command="))
            record = (
                {"database": plan.resources.database, "restored": True}
                if "to_regclass" in sql
                else {"schema_revision": revision}
            )
            return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")
        if "alembic" in command:
            revision = plan.migration_target_revision
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.migration", plan)

    assert outcome.passed
    assert outcome.details == {
        "plan-sha256": plan.migration_plan_sha256,
        "schema-revision": plan.migration_target_revision,
        "status": "migrated",
    }
    migration = next(command for command in calls if "alembic" in command)
    assert "--container" in migration
    assert migration[migration.index("--container") + 1] == "migration"
    db_url = next(item for item in migration if item.startswith("LOOM_DB_URL="))
    assert plan.resources.database in db_url
    assert "loom-staging" not in db_url


def test_migration_is_idempotent_and_rejects_unexpected_baseline() -> None:
    plan = _plan()
    revision = plan.migration_target_revision
    calls: list[tuple[str, ...]] = []

    def run(argv, _payload, _timeout):
        command = tuple(argv)
        calls.append(command)
        if "psql" not in command:
            raise AssertionError(argv)
        sql = next(item for item in command if item.startswith("--command="))
        record = (
            {"database": plan.resources.database, "restored": True}
            if "to_regclass" in sql
            else {"schema_revision": revision}
        )
        return subprocess.CompletedProcess(argv, 0, json.dumps(record), "")

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.migration", plan)
    assert outcome.passed
    assert all("alembic" not in command for command in calls)

    revision = "unexpected"
    blocked = IsolatedRehearsalExecutor(run=run).execute("rehearsal.migration", plan)
    assert blocked.blockers == {"migration": "database-baseline-drift"}

    record = plan.to_record()
    record["migration_target_revision"] = "head"
    with pytest.raises(ValueError, match="identity"):
        RehearsalPlan.from_record(record)


def test_systemd_launch_uses_exact_isolated_transient_unit_and_budget() -> None:
    plan = _plan()
    calls: list[tuple[str, ...]] = []
    active = False

    def run(argv, _payload, _timeout):
        nonlocal active
        command = tuple(argv)
        calls.append(command)
        if command[0] == "systemd-run":
            active = True
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:3] == ("systemctl", "--user", "show"):
            if not active:
                return subprocess.CompletedProcess(argv, 4, "", "unit absent")
            description = f"Loom isolated rehearsal {plan.plan_digest}"
            output = "\n".join(
                (
                    "LoadState=loaded",
                    "ActiveState=active",
                    "SubState=exited",
                    "Type=oneshot",
                    "Result=success",
                    "ExecMainStatus=0",
                    "NeedDaemonReload=no",
                    "Transient=yes",
                    f"Description={description}",
                )
            )
            return subprocess.CompletedProcess(argv, 0, output + "\n", "")
        raise AssertionError(argv)

    clock = iter((10.0, 10.125))
    outcome = IsolatedRehearsalExecutor(
        run=run,
        monotonic=lambda: next(clock),
    ).execute("rehearsal.systemd-launch", plan)

    assert outcome.passed
    assert outcome.details == {
        "latency-ms": "125",
        "status": "active",
        "unit": plan.resources.systemd_unit,
    }
    activation = next(call for call in calls if call[0] == "systemd-run")
    assert f"--unit={plan.resources.systemd_unit}" in activation
    assert "loom-staging-rollout.service" not in activation
    assert activation[-2:] == ("--", "/usr/bin/true")


def test_systemd_launch_rejects_existing_drift_and_latency_overrun() -> None:
    plan = _plan()

    def drift(argv, _payload, _timeout):
        if argv[0] == "systemctl":
            return subprocess.CompletedProcess(argv, 0, "LoadState=loaded\n", "")
        raise AssertionError("drifted unit must not be replaced")

    outcome = IsolatedRehearsalExecutor(run=drift).execute("rehearsal.systemd-launch", plan)
    assert outcome.blockers == {"systemd": "existing-unit-drift"}

    active = False

    def slow(argv, _payload, _timeout):
        nonlocal active
        if argv[0] == "systemd-run":
            active = True
            return subprocess.CompletedProcess(argv, 0, "", "")
        if not active:
            return subprocess.CompletedProcess(argv, 4, "", "")
        description = f"Loom isolated rehearsal {plan.plan_digest}"
        output = (
            "LoadState=loaded\nActiveState=active\nSubState=exited\nType=oneshot\n"
            "Result=success\nExecMainStatus=0\nNeedDaemonReload=no\nTransient=yes\n"
            f"Description={description}\n"
        )
        return subprocess.CompletedProcess(argv, 0, output, "")

    clock = iter((1.0, 6.001))
    outcome = IsolatedRehearsalExecutor(
        run=slow,
        monotonic=lambda: next(clock),
    ).execute("rehearsal.systemd-launch", plan)
    assert outcome.blockers == {"systemd": "activation-readback-drift"}


def test_cleanup_deletes_only_exact_unit_and_namespace_with_preconditions() -> None:
    plan = _plan()
    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            **json.loads(
                json.dumps(
                    {
                        "annotations": {
                            "loom.openai.dev/candidate-sha": plan.candidate_sha,
                            "loom.openai.dev/candidate-tree": plan.candidate_tree,
                            "loom.openai.dev/mutation-epoch": str(plan.mutation_epoch),
                            "loom.openai.dev/plan-sha256": plan.plan_digest,
                        },
                        "labels": {
                            "loom.openai.dev/authority": "staging-preflight",
                            "loom.openai.dev/isolation": plan.resources.namespace.removeprefix(
                                "loom-rehearsal-"
                            ),
                            "pod-security.kubernetes.io/audit": "restricted",
                            "pod-security.kubernetes.io/enforce": "restricted",
                            "pod-security.kubernetes.io/enforce-version": "latest",
                            "pod-security.kubernetes.io/warn": "restricted",
                        },
                        "name": plan.resources.namespace,
                    }
                )
            ),
            "resourceVersion": "42",
            "uid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        },
    }
    unit_active = True
    namespace_present = True
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv, payload, _timeout):
        nonlocal unit_active, namespace_present
        command = tuple(argv)
        calls.append((command, payload))
        if command[:3] == ("systemctl", "--user", "show"):
            if command[-1] == "--value":
                return subprocess.CompletedProcess(
                    argv, 0, "loaded\n" if unit_active else "not-found\n", ""
                )
            if not unit_active:
                return subprocess.CompletedProcess(argv, 4, "", "")
            description = f"Loom isolated rehearsal {plan.plan_digest}"
            output = (
                "LoadState=loaded\nActiveState=active\nSubState=exited\nType=oneshot\n"
                "Result=success\nExecMainStatus=0\nNeedDaemonReload=no\nTransient=yes\n"
                f"Description={description}\n"
            )
            return subprocess.CompletedProcess(argv, 0, output, "")
        if command[:3] == ("systemctl", "--user", "stop"):
            unit_active = False
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:3] == ("systemctl", "--user", "reset-failed"):
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "get" in command and "namespace" in command:
            if not namespace_present:
                return subprocess.CompletedProcess(argv, 1, "", "not found")
            return subprocess.CompletedProcess(argv, 0, json.dumps(namespace), "")
        if "delete" in command and "--raw" in command:
            options = json.loads(payload or b"{}")
            assert options["preconditions"] == {
                "resourceVersion": "42",
                "uid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            }
            namespace_present = False
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        if "wait" in command:
            return subprocess.CompletedProcess(argv, 0, "deleted\n", "")
        raise AssertionError(argv)

    outcome = IsolatedRehearsalExecutor(run=run).execute("rehearsal.cleanup", plan)

    assert outcome.passed and outcome.cleanup_verified
    assert outcome.details["status"] == "absent"
    delete = next(command for command, _payload in calls if "--raw" in command)
    assert delete[-2:] == ("-f", "-")
    assert plan.resources.namespace in delete[-3]


def test_cleanup_refuses_unknown_unit_or_namespace_identity() -> None:
    plan = _plan()

    def unit_drift(argv, _payload, _timeout):
        if argv[:3] == ("systemctl", "--user", "show"):
            return subprocess.CompletedProcess(argv, 0, "LoadState=loaded\n", "")
        raise AssertionError("unknown unit must not be deleted")

    unit = IsolatedRehearsalExecutor(run=unit_drift).execute("rehearsal.cleanup", plan)
    assert unit.blockers == {"cleanup": "systemd-identity-drift"}

    def namespace_drift(argv, _payload, _timeout):
        if argv[:3] == ("systemctl", "--user", "show"):
            if argv[-1] == "--value":
                return subprocess.CompletedProcess(argv, 0, "not-found\n", "")
            return subprocess.CompletedProcess(argv, 4, "", "")
        if "get" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"other"}}',
                "",
            )
        raise AssertionError("unknown namespace must not be deleted")

    namespace = IsolatedRehearsalExecutor(run=namespace_drift).execute("rehearsal.cleanup", plan)
    assert namespace.blockers == {"cleanup": "namespace-identity-drift"}


def test_stream_runner_reads_private_file_without_following_parent_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    source = real / "loom.dump"
    source.write_bytes(b"exact-dump")
    source.chmod(0o600)
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    observed: list[bytes] = []

    def run(*_args, stdin, **_kwargs):
        observed.append(stdin.read())
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr("loom_cli.rollout.rehearsal_executor.subprocess.run", run)

    result = _default_stream_run(("consumer",), source, 30)
    assert result.returncode == 0
    assert observed == [b"exact-dump"]
    with pytest.raises(OSError):
        _default_stream_run(("consumer",), linked / "loom.dump", 30)


def test_stream_runner_rejects_mode_and_read_time_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "loom.dump"
    source.write_bytes(b"exact-dump")
    source.chmod(0o644)
    with pytest.raises(RuntimeError, match="authority"):
        _default_stream_run(("consumer",), source, 30)

    source.chmod(0o600)

    def drift(*_args, stdin, **_kwargs):
        stdin.read()
        source.write_bytes(b"changed")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr("loom_cli.rollout.rehearsal_executor.subprocess.run", drift)
    with pytest.raises(RuntimeError, match="changed"):
        _default_stream_run(("consumer",), source, 30)

    assert os.stat(source).st_mode & 0o777 == 0o600


def test_unimplemented_rehearsal_steps_remain_fail_closed() -> None:
    outcome = IsolatedRehearsalExecutor().execute("rehearsal.release", _plan())
    assert outcome.blockers == {"executor": "isolated-action-not-implemented"}
