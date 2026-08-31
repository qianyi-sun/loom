from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import py_compile
import re
import runpy
import shlex
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_manifest import _config as _builder_config

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import personal_dev_preparation_manifest_documents
from loom.personal_dev_builder_manifest import personal_dev_builder_manifest_documents

_ROOT = Path(__file__).resolve().parents[2]


def _synthetic_checkout_env(**overrides: str) -> dict[str, str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith("COV_CORE_"):
            del env[key]
    env.update(overrides)
    return env


def _read(relative: str) -> str:
    return (_ROOT / relative).read_text(encoding="utf-8")


def _document_sha256(relative: str) -> str:
    return hashlib.sha256((_ROOT / relative).read_bytes()).hexdigest()


def _indented_shell_function(document: str, name: str) -> str:
    marker = f"    {name}() {{"
    assert marker in document, f"missing shell function: {name}"
    start = document.index(marker)
    end_match = re.search(r"^    }$", document[start:], flags=re.MULTILINE)
    assert end_match is not None
    end = start + end_match.end()
    return textwrap.dedent(document[start:end])


def _fenced_shell_function(document: str, name: str) -> str:
    marker = f"\n{name}() {{"
    assert marker in document, f"missing shell function: {name}"
    start = document.index(marker) + 1
    end_match = re.search(r"^}$", document[start:], flags=re.MULTILINE)
    assert end_match is not None
    end = start + end_match.end()
    return document[start:end]


def _shell_command_block(document: str, command: str, output: str) -> str:
    lines = document.splitlines()
    start = next(index for index, line in enumerate(lines) if command in line)
    end = next(index for index, line in enumerate(lines[start:], start=start) if output in line)
    return textwrap.dedent("\n".join(lines[start : end + 1]))


def _fenced_bash(document: str) -> str:
    blocks = re.findall(r"^```bash\n(.*?)^```$", document, flags=re.DOTALL | re.MULTILINE)
    assert blocks
    return "\n".join(blocks)


def _run_schema_transition_orchestration(
    tmp_path: Path,
    *,
    fail_at: str = "",
    persistent_fail_at: str = "",
    fail_after: int = 0,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "quiesce_management",
            "apply_target_transition",
            "restore_predecessor",
            "run_schema_transition",
        )
    )
    call_log = tmp_path / "calls.txt"
    failure_marker = tmp_path / "failed-once"
    operation_names = (
        "assert_transition_interlocks",
        "assert_predecessor_recovery_interlocks",
        "assert_predecessor_restore_artifacts",
        "assert_live_predecessor_state",
        "scale_management_to_zero",
        "wait_for_no_management_pods",
        "assert_migration_job_absent",
        "apply_reviewed_migration_job",
        "wait_for_reviewed_migration_job",
        "assert_live_target_head",
        "apply_reviewed_target_shadow",
        "wait_for_target_shadow",
        "assert_target_shadow_ready",
        "stop_target_migration",
        "assert_only_restore_connection",
        "restore_predecessor_database",
        "apply_reviewed_predecessor_shadow",
        "remove_forward_only_web",
        "wait_for_predecessor_shadow",
        "assert_predecessor_shadow_ready",
    )
    stubs = "\n".join(
        f"{name}() {{ record_call {shlex.quote(name)}; }}" for name in operation_names
    )
    program = (
        "set -euo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        f"failure_marker={shlex.quote(str(failure_marker))}\n"
        f"fail_at={shlex.quote(fail_at)}\n"
        f"persistent_fail_at={shlex.quote(persistent_fail_at)}\n"
        f"fail_after={fail_after}\n"
        "declare -A call_counts=()\n"
        "record_call() {\n"
        '  printf \'%s\\n\' "$1" >> "$call_log"\n'
        "  call_counts[$1]=$(( ${call_counts[$1]:-0} + 1 ))\n"
        '  if test "$1" = "$persistent_fail_at" &&\n'
        '     test "${call_counts[$1]}" -gt "$fail_after"; then\n'
        "    return 1\n"
        "  fi\n"
        '  if test "$1" = "$fail_at" && test ! -e "$failure_marker"; then\n'
        '    : > "$failure_marker"\n'
        "    return 1\n"
        "  fi\n"
        "  return 0\n"
        "}\n"
        f"{stubs}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        f"{functions}\n"
        "run_schema_transition\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )
    calls = call_log.read_text(encoding="utf-8").splitlines()
    return result, calls


def _write_backup_schema_head_test_repo(
    root: Path,
    *,
    heads: tuple[object, ...],
) -> Path:
    repo = root.resolve()
    package = repo / "src" / "loom"
    migrations = repo / "migrations"
    (package / "db").mkdir(parents=True)
    migrations.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "db" / "__init__.py").write_text("", encoding="utf-8")
    (migrations / "alembic.ini").write_text("[alembic]\n", encoding="utf-8")
    module = textwrap.dedent(
        f"""
        import os
        from pathlib import Path

        _EXPECTED_ALEMBIC_INI = Path({str(migrations / "alembic.ini")!r}).resolve()
        _HEADS = {heads!r}

        def service_schema_heads(alembic_ini=None):
            if alembic_ini is None:
                return frozenset(("wrong-default-head",))
            if Path(alembic_ini).resolve() != _EXPECTED_ALEMBIC_INI:
                return frozenset(("wrong-config-head",))
            return frozenset(_HEADS)

        if override := os.environ.get("LOOM_TEST_SCHEMA_MODULE_FILE"):
            __file__ = override
        """
    )
    (package / "db" / "schema_startup.py").write_text(module, encoding="utf-8")
    return repo


def _run_backup_schema_head_resolver(
    tmp_path: Path,
    *,
    heads: tuple[object, ...],
    schema_module_override: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    runbook = _read("docs/runbooks/personal-dev-backup-restore-evidence.md")
    function_source = _fenced_shell_function(
        runbook,
        "resolve_expected_service_schema_head",
    )
    selected_repo = _write_backup_schema_head_test_repo(
        tmp_path / "selected",
        heads=heads,
    )
    stale_repo = _write_backup_schema_head_test_repo(
        tmp_path / "stale-editable",
        heads=("stale_editable_head",),
    )
    program = (
        "set -euo pipefail\n"
        f"{function_source}\n"
        f"repo={shlex.quote(str(selected_repo))}\n"
        f"python_cli={shlex.quote(sys.executable)}\n"
        "resolve_expected_service_schema_head\n"
    )
    env = _synthetic_checkout_env(PYTHONPATH=str(stale_repo / "src"))
    if schema_module_override is not None:
        env["LOOM_TEST_SCHEMA_MODULE_FILE"] = str(schema_module_override)
    return subprocess.run(
        ["bash", "-c", program],
        cwd=_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _run_backup_selected_source_binding(
    tmp_path: Path,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    runbook = _read("docs/runbooks/personal-dev-backup-restore-evidence.md")
    start = runbook.index('repo="$(pwd -P)"')
    end_marker = 'profile="$repo/deploy/dev-fleet/personal-dev-control-plane.toml"'
    end = runbook.index(end_marker, start) + len(end_marker)
    bootstrap_source = runbook[start:end]
    selected_repo = _write_backup_schema_head_test_repo(
        tmp_path / "selected",
        heads=("selected_head",),
    )
    stale_repo = _write_backup_schema_head_test_repo(
        tmp_path / "stale-editable",
        heads=("stale_head",),
    )
    program = (
        "set -euo pipefail\n"
        f"{bootstrap_source}\n"
        f"python_cli={shlex.quote(sys.executable)}\n"
        "\"$python_cli\" - <<'PY'\n"
        "from pathlib import Path\n"
        "import loom\n"
        "print(Path(loom.__file__).resolve())\n"
        "PY\n"
    )
    result = subprocess.run(
        ["bash", "-c", program],
        cwd=selected_repo,
        env=_synthetic_checkout_env(PYTHONPATH=str(stale_repo / "src")),
        check=False,
        capture_output=True,
        text=True,
    )
    return result, (selected_repo / "src" / "loom" / "__init__.py").resolve()


def _run_backup_schema_head_comparison(
    tmp_path: Path,
    *,
    phase: str,
    observed_head: str,
) -> subprocess.CompletedProcess[str]:
    runbook = _read("docs/runbooks/personal-dev-backup-restore-evidence.md")
    function_source = _fenced_shell_function(
        runbook,
        "resolve_expected_service_schema_head",
    )
    selected_repo = _write_backup_schema_head_test_repo(
        tmp_path / "selected",
        heads=("future_schema_head",),
    )
    stale_repo = _write_backup_schema_head_test_repo(
        tmp_path / "stale-editable",
        heads=("stale_editable_head",),
    )
    if phase == "live":
        start = runbook.index('expected_service_schema_head="$(')
        end = runbook.index("\nsuffix=", start)
        comparison_source = runbook[start:end]
        command_stub = f"kubectl() {{ printf '%s\\n' {shlex.quote(observed_head)}; }}"
    elif phase == "restored":
        start = runbook.index('restored_schema_head="$(docker')
        end = runbook.index("\ncmp -s", start)
        comparison_source = runbook[start:end]
        command_stub = f"docker() {{ printf '%s\\n' {shlex.quote(observed_head)}; }}"
    else:
        raise AssertionError(f"unknown schema comparison phase: {phase}")
    program = (
        "set -euo pipefail\n"
        f"{function_source}\n"
        f"repo={shlex.quote(str(selected_repo))}\n"
        f"python_cli={shlex.quote(sys.executable)}\n"
        "kubeconfig=unused\n"
        "postgres_pod=unused\n"
        "postgres_restore=unused\n"
        f"{command_stub}\n"
    )
    if phase == "restored":
        program += 'expected_service_schema_head="$(resolve_expected_service_schema_head)"\n'
    program += comparison_source + "\n"
    return subprocess.run(
        ["bash", "-c", program],
        cwd=_ROOT,
        env=_synthetic_checkout_env(PYTHONPATH=str(stale_repo / "src")),
        check=False,
        capture_output=True,
        text=True,
    )


def _run_schema_transition_predecessor_checkout_head(
    tmp_path: Path,
    *,
    schema_module_override: Path | None = None,
    malicious_bytecode_head: str | None = None,
) -> subprocess.CompletedProcess[str]:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    start = runbook.index('predecessor_checkout_schema_head="$(')
    end = runbook.index('\ntest "$predecessor_checkout_schema_head"', start)
    resolver_source = runbook[start:end]
    predecessor_repo = _write_backup_schema_head_test_repo(
        tmp_path / "predecessor",
        heads=("selected_predecessor_head",),
    )
    stale_repo = _write_backup_schema_head_test_repo(
        tmp_path / "stale-editable",
        heads=("stale_editable_head",),
    )
    if malicious_bytecode_head is not None:
        selected_module = predecessor_repo / "src/loom/db/schema_startup.py"
        malicious_source = tmp_path / "malicious_schema_startup.py"
        malicious_source.write_text(
            "def service_schema_heads(alembic_ini=None):\n"
            f"    return frozenset(({malicious_bytecode_head!r},))\n",
            encoding="utf-8",
        )
        bytecode = Path(importlib.util.cache_from_source(str(selected_module)))
        bytecode.parent.mkdir(parents=True, exist_ok=True)
        py_compile.compile(
            str(malicious_source),
            cfile=str(bytecode),
            dfile=str(selected_module),
            doraise=True,
            invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
        )
    program = (
        "set -euo pipefail\n"
        f"predecessor_repo={shlex.quote(str(predecessor_repo))}\n"
        f"python_cli={shlex.quote(sys.executable)}\n"
        f"{resolver_source}\n"
        'printf "%s\\n" "$predecessor_checkout_schema_head"\n'
    )
    environment = _synthetic_checkout_env(PYTHONPATH=str(stale_repo / "src"))
    if schema_module_override is not None:
        schema_module_override.parent.mkdir(parents=True)
        schema_module_override.write_text("# wrong module\n", encoding="ascii")
        environment["LOOM_TEST_SCHEMA_MODULE_FILE"] = str(schema_module_override)
    return subprocess.run(
        ["/bin/bash", "-c", program],
        cwd=_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _run_predecessor_status_compatibility_guard(
    tmp_path: Path,
    *,
    tamper_kubeconfig: bool,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_open_owner_only_sha256",
            "assert_predecessor_release_compat_path",
            "assert_predecessor_kubeconfig_compat_path",
            "run_predecessor_shadow_status",
        )
    )
    reviewed_kubeconfig = tmp_path / "reviewed-kubeconfig"
    reviewed_kubeconfig.write_text("reviewed-kubeconfig\n", encoding="ascii")
    reviewed_kubeconfig.chmod(0o600)
    predecessor_release = tmp_path / "predecessor-release.json"
    predecessor_release.write_text("{}", encoding="ascii")
    predecessor_release.chmod(0o600)
    predecessor_repo = tmp_path / "predecessor"
    (predecessor_repo / "src").mkdir(parents=True)
    predecessor_profile = predecessor_repo / "profile.toml"
    predecessor_profile.write_text("profile\n", encoding="ascii")
    invocation_log = tmp_path / "invocation.log"
    status_path = tmp_path / "predecessor.status.json"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    kubectl = fake_bin / "kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'test "$1" = --kubeconfig\n'
        'test "$2" = "$EXPECTED_PINNED_KUBECONFIG"\n'
        'test "$3" = config\n'
        'test "$4" = current-context\n'
        'printf "%s\\n" "$EXPECTED_KUBE_CONTEXT"\n',
        encoding="ascii",
    )
    kubectl.chmod(0o700)
    predecessor_cli = tmp_path / "predecessor-loom"
    predecessor_cli.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'observed=""\n'
        'while test "$#" -gt 0; do\n'
        '  if test "$1" = --kubeconfig; then observed="$2"; shift 2; else shift; fi\n'
        "done\n"
        'test "$observed" = "$EXPECTED_KUBECONFIG"\n'
        'printf "%s\\n" "$observed" > "$INVOCATION_LOG"\n'
        'if test "$TAMPER_KUBECONFIG" = 1; then\n'
        '  replacement="${EXPECTED_KUBECONFIG}.replacement"\n'
        '  printf "%s\\n" tampered > "$replacement"\n'
        '  chmod 0600 "$replacement"\n'
        '  mv "$replacement" "$EXPECTED_KUBECONFIG"\n'
        "fi\n"
        "printf '%s\\n' '{\"ready\":true}'\n",
        encoding="ascii",
    )
    predecessor_cli.chmod(0o700)
    kubeconfig_sha256 = hashlib.sha256(reviewed_kubeconfig.read_bytes()).hexdigest()
    release_sha256 = hashlib.sha256(predecessor_release.read_bytes()).hexdigest()
    program = (
        "set -euo pipefail\n"
        f"{functions}\n"
        f"kubeconfig_source={shlex.quote(str(reviewed_kubeconfig))}\n"
        f"kubeconfig_sha256={kubeconfig_sha256}\n"
        "expected_kube_context=reviewed-context\n"
        f"predecessor_release_source={shlex.quote(str(predecessor_release))}\n"
        f"predecessor_release_sha256={release_sha256}\n"
        f"predecessor_repo={shlex.quote(str(predecessor_repo))}\n"
        f"predecessor_profile={shlex.quote(str(predecessor_profile))}\n"
        f"predecessor_loom_cli={shlex.quote(str(predecessor_cli))}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        "exec 31<\"$predecessor_release_source\"\n"
        "exec 36<\"$kubeconfig_source\"\n"
        "predecessor_release=/proc/self/fd/31\n"
        "kubeconfig=/proc/self/fd/36\n"
        f"run_predecessor_shadow_status {shlex.quote(str(status_path))}\n"
    )
    result = subprocess.run(
        ["/bin/bash", "-c", program],
        cwd=_ROOT,
        env=_synthetic_checkout_env(
            EXPECTED_KUBECONFIG=str(reviewed_kubeconfig),
            EXPECTED_KUBE_CONTEXT="reviewed-context",
            EXPECTED_PINNED_KUBECONFIG="/proc/self/fd/36",
            INVOCATION_LOG=str(invocation_log),
            PATH=f"{fake_bin}:{os.environ['PATH']}",
            TAMPER_KUBECONFIG="1" if tamper_kubeconfig else "0",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    return result, status_path, invocation_log


def test_predecessor_status_uses_guarded_regular_kubeconfig_path(tmp_path: Path) -> None:
    result, status_path, invocation_log = _run_predecessor_status_compatibility_guard(
        tmp_path,
        tamper_kubeconfig=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(status_path.read_bytes()) == {"ready": True}
    assert invocation_log.read_text(encoding="utf-8").strip() == str(
        tmp_path / "reviewed-kubeconfig"
    )


def test_predecessor_status_discards_output_after_kubeconfig_drift(tmp_path: Path) -> None:
    result, status_path, _invocation_log = _run_predecessor_status_compatibility_guard(
        tmp_path,
        tamper_kubeconfig=True,
    )

    assert result.returncode != 0
    assert not status_path.exists()


def _run_target_status_compatibility_guard(
    tmp_path: Path,
    *,
    reject_after_observation: bool,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_open_owner_only_sha256",
            "assert_target_status_compat_paths",
            "run_target_shadow_status",
        )
    )
    invocation_log = tmp_path / "invocation.log"
    status_path = tmp_path / "target.status.json"
    profile = tmp_path / "profile.toml"
    profile.write_text("profile\n", encoding="ascii")
    trusted_release = tmp_path / "trusted-release.json"
    trusted_release.write_text("{}", encoding="ascii")
    trusted_release.chmod(0o600)
    kubeconfig_source = tmp_path / "reviewed-kubeconfig"
    kubeconfig_source.write_text("reviewed kubeconfig\n", encoding="ascii")
    kubeconfig_source.chmod(0o600)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    kubectl = fake_bin / "kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'test "$1" = --kubeconfig\n'
        'test "$2" = "$EXPECTED_PINNED_KUBECONFIG"\n'
        'test "$3" = config\n'
        'test "$4" = current-context\n'
        'printf "%s\\n" "$EXPECTED_KUBE_CONTEXT"\n',
        encoding="ascii",
    )
    kubectl.chmod(0o700)
    target_cli = tmp_path / "target-loom"
    target_cli.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "observed_kubeconfig=\n"
        "observed_release=\n"
        'while test "$#" -gt 0; do\n'
        '  case "$1" in\n'
        '    --kubeconfig) observed_kubeconfig="$2"; shift 2 ;;\n'
        '    --trusted-release-file) observed_release="$2"; shift 2 ;;\n'
        "    *) shift ;;\n"
        "  esac\n"
        "done\n"
        'test "$observed_kubeconfig" = "$EXPECTED_KUBECONFIG"\n'
        'test "$observed_release" = "$EXPECTED_TRUSTED_RELEASE"\n'
        'printf \'%s\\n%s\\n\' "$observed_kubeconfig" "$observed_release" '
        '> "$INVOCATION_LOG"\n'
        'if test "$TAMPER_KUBECONFIG" = 1; then\n'
        '  replacement="${EXPECTED_KUBECONFIG}.replacement"\n'
        '  printf "%s\\n" tampered > "$replacement"\n'
        '  chmod 0600 "$replacement"\n'
        '  mv "$replacement" "$EXPECTED_KUBECONFIG"\n'
        "fi\n"
        "printf '%s\\n' '{\"ready\":true}'\n",
        encoding="ascii",
    )
    target_cli.chmod(0o700)
    trusted_release_sha256 = hashlib.sha256(trusted_release.read_bytes()).hexdigest()
    kubeconfig_sha256 = hashlib.sha256(kubeconfig_source.read_bytes()).hexdigest()
    program = (
        "set -euo pipefail\n"
        f"{functions}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        f"profile={shlex.quote(str(profile))}\n"
        f"kubeconfig_source={shlex.quote(str(kubeconfig_source))}\n"
        f"kubeconfig_sha256={kubeconfig_sha256}\n"
        "expected_kube_context=reviewed-context\n"
        "trusted_release=/proc/self/fd/30\n"
        f"trusted_release_sha256={trusted_release_sha256}\n"
        "repo=/unused/target\n"
        f"loom_cli={shlex.quote(str(target_cli))}\n"
        f"exec 30< {shlex.quote(str(trusted_release))}\n"
        'exec 36<"$kubeconfig_source"\n'
        "kubeconfig=/proc/self/fd/36\n"
        f"run_target_shadow_status {shlex.quote(str(status_path))}\n"
    )
    result = subprocess.run(
        ["/bin/bash", "-c", program],
        cwd=_ROOT,
        env=_synthetic_checkout_env(
            EXPECTED_KUBECONFIG=str(kubeconfig_source),
            EXPECTED_KUBE_CONTEXT="reviewed-context",
            EXPECTED_PINNED_KUBECONFIG="/proc/self/fd/36",
            EXPECTED_TRUSTED_RELEASE="/proc/self/fd/30",
            INVOCATION_LOG=str(invocation_log),
            PATH=f"{fake_bin}:{os.environ['PATH']}",
            TAMPER_KUBECONFIG="1" if reject_after_observation else "0",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    return result, status_path, invocation_log


def test_target_status_uses_guarded_regular_kubeconfig_path(tmp_path: Path) -> None:
    result, status_path, invocation_log = _run_target_status_compatibility_guard(
        tmp_path,
        reject_after_observation=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert json.loads(status_path.read_bytes()) == {"ready": True}
    assert invocation_log.read_text(encoding="ascii").splitlines() == [
        str(tmp_path / "reviewed-kubeconfig"),
        "/proc/self/fd/30",
    ]


def test_target_status_discards_output_after_compatibility_drift(tmp_path: Path) -> None:
    result, status_path, _invocation_log = _run_target_status_compatibility_guard(
        tmp_path,
        reject_after_observation=True,
    )

    assert result.returncode != 0
    assert not status_path.exists()


def test_schema_transition_target_ready_uses_guarded_status_runner(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_target_shadow_ready")
    call_log = tmp_path / "calls.txt"
    status = json.dumps(
        {
            "blockers": [],
            "components": [{"name": "personal-workers", "observed": 0}],
            "manager_ceiling": 0,
            "mode": "shadow",
            "ready": True,
            "worker_available": False,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    program = (
        "set -euo pipefail\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        'record() { printf "%s\\n" "$1" >> '
        f"{shlex.quote(str(call_log))}; }}\n"
        "run_target_shadow_status() {\n"
        "  record guarded-target-status\n"
        '  test "$1" = "$evidence_dir/target-shadow.status.json"\n'
        f"  printf '%s\\n' {shlex.quote(status)} > \"$1\"\n"
        "}\n"
        "direct_status() {\n"
        "  record unguarded-target-status\n"
        f"  printf '%s\\n' {shlex.quote(status)}\n"
        "}\n"
        "repo=/unused/target\n"
        "loom_cli=direct_status\n"
        "profile=/unused/profile\n"
        "kubeconfig=/proc/self/fd/36\n"
        "trusted_release=/proc/self/fd/30\n"
        f"trusted_release_sha256={'a' * 64}\n"
        f"{function}\n"
        "assert_target_shadow_ready\n"
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert call_log.read_text(encoding="ascii").splitlines() == ["guarded-target-status"]


_WEB_V2_COMPONENT_NAMES = (
    "cluster-resources",
    "manager",
    "namespaced-resources",
    "namespaces",
    "personal-workers",
    "runtime-class",
    "web",
)


def _web_v2_rollback_status(mode: str, mutation: str) -> dict[str, object]:
    ready = dict.fromkeys(_WEB_V2_COMPONENT_NAMES, True)
    blockers: list[str] = []

    if mutation == "allowed-web-failure":
        blockers = ["web_not_ready"]
        if mode == "acceptance":
            blockers.insert(0, "acceptance_window_expired")
        ready["namespaced-resources"] = False
        ready["web"] = False
    elif mutation == "acceptance-window-expired":
        blockers = ["acceptance_window_expired"]
    elif mutation == "non-web-blocker":
        blockers = ["namespace_missing"]
        ready["namespaces"] = False
    elif mutation == "non-web-component":
        blockers = ["resource_inventory_drift"]
        ready["namespaced-resources"] = False
    elif mutation == "missing-component":
        ready.pop("runtime-class")
    elif mutation == "duplicate-component":
        pass
    elif mutation == "extra-component":
        pass
    elif mutation == "web-blocker-with-ready-web":
        blockers = ["web_not_ready"]
        ready["namespaced-resources"] = False
    elif mutation == "web-blocker-with-ready-namespaced-resources":
        blockers = ["web_not_ready"]
        ready["web"] = False
    elif mutation == "web-unready-without-blocker":
        ready["namespaced-resources"] = False
        ready["web"] = False
    elif mutation != "all-ready":
        raise AssertionError(f"unknown status mutation: {mutation}")

    observed = {
        "cluster-resources": 10,
        "manager": 1,
        "namespaced-resources": 35,
        "namespaces": 1,
        "personal-workers": 0,
        "runtime-class": 1,
        "web": 1,
    }
    components = [
        {"name": name, "observed": observed[name], "ready": is_ready}
        for name, is_ready in ready.items()
    ]
    if mutation == "duplicate-component":
        components.append(dict(components[-1]))
    if mutation == "extra-component":
        components.append({"name": "unexpected", "observed": 1, "ready": True})

    plan_key = f"{mode}_plan_sha256"
    status: dict[str, object] = {
        "application_ready": not blockers,
        "blockers": blockers,
        "capacity_publication_ready": True,
        "components": components,
        "input_sha256": "c" * 64,
        "manager_ceiling": 0,
        "mode": mode,
        plan_key: "a" * 64,
        "ready": not blockers,
        "release_sha256": "b" * 64,
        "schema": "loom-personal-dev-control-plane-status-v1",
        "worker_available": False,
    }
    return status


def _run_web_v2_rollback_interlocks(
    tmp_path: Path,
    relative: str,
    status: dict[str, object],
) -> subprocess.CompletedProcess[str]:
    runbook = _read(relative)
    durable = "durable" in relative
    function_names = (
        ("assert_operational_manager_boundary", "assert_rollback_interlocks")
        if durable
        else ("assert_rollback_interlocks",)
    )
    function_reader = _fenced_shell_function if durable else _indented_shell_function
    functions = "\n".join(function_reader(runbook, name) for name in function_names)
    status_command = "status-operational" if durable else "status-acceptance"
    plan_variable = "operational" if durable else "acceptance"
    stubs = (
        "assert_current_artifacts assert_reviewed_kubeconfig "
        "assert_secret_key_inventory assert_storage_and_migration "
        "assert_no_dynamic_namespaces assert_no_dynamic_namespaces_or_workers "
        "assert_runtime_scanner_release_bindings"
    )
    program = (
        "set -euo pipefail\n"
        + functions
        + "\n"
        + "\n".join(f"{name}() {{ :; }}" for name in stubs.split())
        + "\nassert_rollback_window() { :; }\n"
        + "assert_canonical_json_line() { :; }\n"
        + f"status_json={shlex.quote(json.dumps(status))}\n"
        + "loom_cli_stub() {\n"
        + f'  case " $* " in *" {status_command} "*) ;; *) return 91 ;; esac\n'
        + "  printf '%s\\n' \"$status_json\"\n"
        + "  return 1\n"
        + "}\n"
        + "loom_cli=loom_cli_stub\n"
        + f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        + "kubeconfig=/unused/kubeconfig\nprofile=/unused/profile\n"
        + "trusted_release=/unused/release\ntrusted_release_sha256="
        + "b" * 64
        + "\n"
        + f"{plan_variable}_plan=/unused/plan\n"
        + f"{plan_variable}_plan_sha256="
        + "a" * 64
        + "\n"
        + f"{plan_variable}_evidence_args=()\n"
        + (
            "rollback_pre_status=" + shlex.quote(str(tmp_path / "rollback.json")) + "\n"
            if not durable
            else ""
        )
        + "assert_rollback_interlocks\n"
    )
    return subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )


def _executable_calls_are_ordered(
    document: str,
    *,
    start: str,
    apply: str,
    calls: tuple[str, ...],
) -> bool:
    bash = _fenced_bash(document)
    try:
        start_index = bash.index(start)
        apply_index = bash.index(apply, start_index)
    except ValueError:
        return False
    executable = [
        line.strip()
        for line in bash[start_index:apply_index].splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    positions: list[int] = []
    for call in calls:
        try:
            positions.append(executable.index(call))
        except ValueError:
            return False
    return positions == sorted(positions) and len(set(positions)) == len(calls)


def _sidecar_status(**changes: str) -> bytes:
    fields = {
        "Uid": "1000\t1000\t1000\t1000",
        "Gid": "1000\t1000\t1000\t1000",
        "CapInh": "0000000000000000",
        "CapPrm": "0000000000000000",
        "CapEff": "0000000000000000",
        "CapBnd": "00000000000000c0",
        "CapAmb": "0000000000000000",
        "Seccomp": "0",
    }
    fields.update(changes)
    return "".join(f"{name}:\t{value}\n" for name, value in fields.items()).encode("ascii")


def _sidecar_launcher() -> dict[str, object]:
    return runpy.run_path(
        str(_ROOT / "deploy/personal-dev-builder/loom-personal-dev-buildkitd"),
        run_name="loom_personal_dev_buildkitd_test",
    )


def test_shadow_package_is_pure_render_only_and_has_no_legacy_extension() -> None:
    source = (_ROOT / "src/loom/personal_dev_control_plane_render.py").read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert "import kubernetes" not in source
    assert "slurm" not in source.casefold()
    assert "def apply" not in source
    assert "def activate" not in source
    assert "loom-dev-shared" not in source
    assert '"cidr": "0.0.0.0/0"' not in source


def test_shadow_package_pins_the_exact_acme_http01_solver_boundary() -> None:
    profile = _read("deploy/dev-fleet/personal-dev-control-plane.toml")
    source = _read("src/loom/personal_dev_control_plane_render.py")

    assert "acme_http01_solver_port = 8089" in profile
    assert '"loom-personal-dev-acme-http01-ingress"' in source
    assert '"acme.cert-manager.io/http01-solver": "true"' in source
    assert "profile.network.acme_http01_solver_port" in source


def test_dynamic_personal_and_builder_namespaces_bind_read_authority_locally() -> None:
    personal = personal_dev_preparation_manifest_documents(
        derive_identity("alice"),
        _immutable_config(),
    )
    builder = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=_builder_config(),
    )

    for documents in (personal, builder):
        binding = next(
            item
            for item in documents
            if item["kind"] == "RoleBinding"
            and item["metadata"]["name"] == "loom-personal-dev-management"
        )
        assert binding["roleRef"]["name"] == "loom-personal-dev-managed-namespace"
        assert binding["subjects"] == [
            {
                "kind": "ServiceAccount",
                "name": "loom-personal-dev-management",
                "namespace": "loom-dev",
            }
        ]


def test_personal_dev_builder_image_binds_rootless_sidecar_prerequisites() -> None:
    dockerfile = _read("deploy/Dockerfile.personal-dev-builder")
    ownership = _read("config/component-ownership.toml")

    assert "rootlesskit version 3.0.1" in dockerfile
    assert "rootlesskit/v3@v3.0.1" in dockerfile
    assert "golang.org/x/crypto@v0.55.0" in dockerfile
    assert (
        "COPY --from=rootlesskit-builder /rootlesskit-fixed /usr/bin/rootlesskit"
        in dockerfile
    )
    assert "ARG TARGETARCH" in dockerfile
    assert "/usr/bin/buildctl" in dockerfile
    assert "qemu_path=/usr/bin/buildkit-qemu-aarch64" in dockerfile
    assert "qemu_path=/usr/bin/buildkit-qemu-x86_64" in dockerfile
    assert 'test -x "$qemu_path"' in dockerfile
    assert "/bin/setpriv" in dockerfile
    assert "/usr/bin/newuidmap" in dockerfile
    assert "0100000280000000000000000000000000000000" in dockerfile
    assert "/usr/bin/newgidmap" in dockerfile
    assert "0100000240000000000000000000000000000000" in dockerfile
    assert (
        "COPY --chown=0:0 --chmod=0555 "
        "deploy/personal-dev-builder/loom-personal-dev-buildkitd" in dockerfile
    )
    assert '"deploy/personal-dev-builder/loom-personal-dev-buildkitd"' in ownership


def test_rootless_sidecar_launcher_execs_only_the_fixed_buildkit_command(
    tmp_path: Path,
) -> None:
    launcher = _sidecar_launcher()
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status())
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    recorded: list[tuple[str, list[str], dict[str, str]]] = []

    def execve(path: str, argv: list[str], environment: dict[str, str]) -> None:
        recorded.append((path, argv, environment))

    def accept_private_directory(path: Path, *, uid: int, gid: int) -> None:
        assert path in {state / "home", tmp_path / "runtime"}
        assert (uid, gid) == (1000, 1000)

    launcher["_main"].__globals__["_ensure_private_directory"] = accept_private_directory
    with pytest.raises(RuntimeError, match="returned"):
        launcher["_main"](
            gvisor_marker=marker,
            status_file=status,
            uid=1000,
            gid=1000,
            no_new_privs=0,
            home=state / "home",
            runtime_directory=tmp_path / "runtime",
            execve=execve,
        )

    assert recorded == [
        (
            "/usr/bin/rootlesskit",
            [
                "/usr/bin/rootlesskit",
                "/bin/setpriv",
                "--nnp",
                "/usr/local/bin/loom-personal-dev-buildkitd",
                "--buildkit-child",
            ],
            {
                "HOME": str(state / "home"),
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "TMPDIR": "/tmp",
                "USER": "user",
                "XDG_RUNTIME_DIR": str(tmp_path / "runtime"),
            },
        )
    ]


def test_rootless_sidecar_native_tcp_launcher_enters_rootlesskit_before_child(
    tmp_path: Path,
) -> None:
    launcher = _sidecar_launcher()
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status())
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    recorded: list[tuple[str, list[str], dict[str, str]]] = []

    def execve(path: str, argv: list[str], environment: dict[str, str]) -> None:
        recorded.append((path, argv, environment))

    def accept_private_directory(path: Path, *, uid: int, gid: int) -> None:
        assert path in {state / "home", tmp_path / "runtime"}
        assert (uid, gid) == (1000, 1000)

    launcher["_main_native_tcp_outer"].__globals__["_ensure_private_directory"] = (
        accept_private_directory
    )
    with pytest.raises(RuntimeError, match="returned"):
        launcher["_main_native_tcp_outer"](
            gvisor_marker=marker,
            status_file=status,
            uid=1000,
            gid=1000,
            no_new_privs=0,
            home=state / "home",
            runtime_directory=tmp_path / "runtime",
            execve=execve,
        )

    assert recorded == [
        (
            "/usr/bin/rootlesskit",
            [
                "/usr/bin/rootlesskit",
                "/bin/setpriv",
                "--nnp",
                "/usr/local/bin/loom-personal-dev-buildkitd",
                "--native-tcp-buildkit-child",
            ],
            {
                "HOME": str(state / "home"),
                "LOOM_PERSONAL_DEV_BUILDKIT_CHILD_MODE": "native-tcp-v1",
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "TMPDIR": "/tmp",
                "USER": "user",
                "XDG_RUNTIME_DIR": str(tmp_path / "runtime"),
            },
        )
    ]


def test_rootless_sidecar_launcher_clears_setgid_inherited_by_created_directory(
    tmp_path: Path,
) -> None:
    launcher = _sidecar_launcher()
    state = tmp_path / "state"
    state.mkdir()
    state.chmod(0o2770)
    home = state / "home"

    launcher["_ensure_private_directory"](
        home,
        uid=os.getuid(),
        gid=os.getgid(),
    )

    metadata = home.lstat()
    assert stat.S_ISDIR(metadata.st_mode)
    assert stat.S_IMODE(metadata.st_mode) == 0o700
    assert metadata.st_uid == os.getuid()
    assert metadata.st_gid == os.getgid()


def test_rootless_sidecar_launcher_rejects_preexisting_setgid_directory(
    tmp_path: Path,
) -> None:
    launcher = _sidecar_launcher()
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    home.chmod(0o2700)

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_ensure_private_directory"](
            home,
            uid=os.getuid(),
            gid=os.getgid(),
        )
    assert stat.S_IMODE(home.lstat().st_mode) == 0o2700


@pytest.mark.parametrize(
    ("field", "value", "uid", "gid"),
    [
        ("Uid", "1001\t1001\t1001\t1001", 1000, 1000),
        ("Gid", "1001\t1001\t1001\t1001", 1000, 1000),
        ("CapInh", "00000000000000c0", 1000, 1000),
        ("CapPrm", "00000000000000c0", 1000, 1000),
        ("CapEff", "00000000000000c0", 1000, 1000),
        ("CapBnd", "0000000000000000", 1000, 1000),
        ("CapAmb", "00000000000000c0", 1000, 1000),
        ("Seccomp", "2", 1000, 1000),
        ("Uid", "1000\t1000\t1000\t1000", 1001, 1000),
        ("Gid", "1000\t1000\t1000\t1000", 1000, 1001),
    ],
)
def test_rootless_sidecar_launcher_rejects_each_identity_drift(
    tmp_path: Path,
    field: str,
    value: str,
    uid: int,
    gid: int,
) -> None:
    launcher = _sidecar_launcher()
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status(**{field: value}))

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_verify_preflight"](
            gvisor_marker=marker,
            status_file=status,
            uid=uid,
            gid=gid,
            no_new_privs=0,
        )


def test_rootless_sidecar_launcher_rejects_no_new_privs_drift(
    tmp_path: Path,
) -> None:
    launcher = _sidecar_launcher()
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status())

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_verify_preflight"](
            gvisor_marker=marker,
            status_file=status,
            uid=1000,
            gid=1000,
            no_new_privs=1,
        )


@pytest.mark.parametrize("result", [-1, 2])
def test_rootless_sidecar_launcher_rejects_invalid_prctl_result(result: int) -> None:
    launcher = _sidecar_launcher()

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_read_no_new_privs"](
            prctl=lambda option, arg2, arg3, arg4, arg5: result,
        )


def test_rootless_sidecar_launcher_reads_no_new_privs_with_exact_prctl() -> None:
    launcher = _sidecar_launcher()
    calls: list[tuple[int, int, int, int, int]] = []

    def prctl(option: int, arg2: int, arg3: int, arg4: int, arg5: int) -> int:
        calls.append((option, arg2, arg3, arg4, arg5))
        return 0

    assert launcher["_read_no_new_privs"](prctl=prctl) == 0
    assert calls == [(39, 0, 0, 0, 0)]


def test_rootless_sidecar_buildkit_child_requires_nnp_and_execs_fixed_daemon(
    capsys: pytest.CaptureFixture[str],
) -> None:
    launcher = _sidecar_launcher()
    recorded: list[tuple[str, list[str], dict[str, str]]] = []

    def execve(path: str, argv: list[str], environment: dict[str, str]) -> None:
        recorded.append((path, argv, environment))

    with pytest.raises(RuntimeError, match="returned"):
        launcher["_main_buildkit_child"](
            no_new_privs=1,
            environment={"HOME": "/var/lib/loom-buildkit/home"},
            execve=execve,
        )

    assert recorded == [
        (
            "/usr/bin/buildkitd",
            [
                "/usr/bin/buildkitd",
                "--addr=unix:///var/run/loom-buildkit/buildkitd.sock",
                "--oci-worker-no-process-sandbox",
                "--oci-worker-snapshotter=native",
            ],
            {"HOME": "/var/lib/loom-buildkit/home"},
        )
    ]
    assert capsys.readouterr().out == "loom-buildkitd-child-preflight nnp=1\n"

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_main_buildkit_child"](
            no_new_privs=0,
            environment={},
            execve=execve,
        )


def test_rootless_sidecar_native_tcp_child_execs_only_fixed_daemon(
    capsys: pytest.CaptureFixture[str],
) -> None:
    launcher = _sidecar_launcher()
    recorded: list[tuple[str, list[str], dict[str, str]]] = []

    def execve(path: str, argv: list[str], environment: dict[str, str]) -> None:
        recorded.append((path, argv, environment))

    with pytest.raises(RuntimeError, match="returned"):
        launcher["_main_native_tcp_buildkit_child"](
            no_new_privs=1,
            environment={
                "HOME": "/var/lib/loom-buildkit/home",
                "LOOM_PERSONAL_DEV_BUILDKIT_CHILD_MODE": "native-tcp-v1",
            },
            execve=execve,
        )

    assert recorded == [
        (
            "/usr/bin/buildkitd",
            [
                "/usr/bin/buildkitd",
                "--addr=tcp://0.0.0.0:1234",
                "--oci-worker-no-process-sandbox",
                "--oci-worker-snapshotter=native",
            ],
            {"HOME": "/var/lib/loom-buildkit/home"},
        )
    ]
    assert capsys.readouterr().out == "loom-buildkitd-native-child-preflight nnp=1\n"

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_main_native_tcp_buildkit_child"](
            no_new_privs=0,
            environment={},
            execve=execve,
        )


def test_rootless_sidecar_launcher_dispatches_only_fixed_modes() -> None:
    launcher = _sidecar_launcher()
    calls: list[str] = []

    def outer() -> None:
        calls.append("outer")

    def child() -> None:
        calls.append("child")

    def native_outer() -> None:
        calls.append("native-outer")

    def native_child() -> None:
        calls.append("native-child")

    launcher["_dispatch"]([], outer=outer, child=child)
    launcher["_dispatch"](["--buildkit-child"], outer=outer, child=child)
    launcher["_dispatch"](
        ["--native-tcp-buildkit-child"],
        outer=outer,
        child=child,
        native_outer=native_outer,
        native_child=native_child,
        environment={},
    )
    launcher["_dispatch"](
        ["--native-tcp-buildkit-child"],
        outer=outer,
        child=child,
        native_outer=native_outer,
        native_child=native_child,
        environment={"LOOM_PERSONAL_DEV_BUILDKIT_CHILD_MODE": "native-tcp-v1"},
    )
    assert calls == ["outer", "child", "native-outer", "native-child"]

    for arguments in (
        ["--unknown"],
        ["--native-tcp-buildkit-child", "tcp://127.0.0.1:1234"],
        ["--native-tcp-buildkit-child=tcp://127.0.0.1:1234"],
    ):
        with pytest.raises(RuntimeError, match="preflight"):
            launcher["_dispatch"](arguments, outer=outer, child=child)

    for arguments in ([], ["--buildkit-child"], ["--native-tcp-buildkit-child"]):
        with pytest.raises(RuntimeError, match="preflight"):
            launcher["_dispatch"](
                arguments,
                outer=outer,
                child=child,
                environment={"LOOM_PERSONAL_DEV_BUILDKIT_CHILD_MODE": "changed"},
            )


def test_rootless_sidecar_launcher_requires_gvisor_marker(tmp_path: Path) -> None:
    launcher = _sidecar_launcher()
    status = tmp_path / "status"
    status.write_bytes(_sidecar_status())

    with pytest.raises(RuntimeError, match="preflight"):
        launcher["_verify_preflight"](
            gvisor_marker=tmp_path / "missing",
            status_file=status,
            uid=1000,
            gid=1000,
            no_new_privs=0,
        )


def test_dev_fleet_package_never_creates_a_second_shared_namespace() -> None:
    for path in (_ROOT / "deploy/dev-fleet").glob("**/*"):
        if path.is_file():
            assert "loom-dev-shared" not in path.read_text(encoding="utf-8", errors="ignore")


def test_personal_management_shadow_runbook_has_exact_bounded_rehearsal() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    normalized = " ".join(runbook.split())

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "management-shadow/<approved-window-id>" in runbook
    assert 'test ! -e "$shadow_render"' in runbook
    assert 'test ! -e "$render_evidence"' in runbook
    assert 'install -d -m 0700 "$evidence_dir"' in runbook
    assert 'test "$(stat -c %u "$trusted_release")" = "$(id -u)"' in runbook
    assert 'test "$(stat -c %a "$trusted_release")" = 600' in runbook
    assert 'test "$(stat -c %h "$trusted_release")" = 1' in runbook
    assert 'test "$(stat -c %u "$kubeconfig")" = "$(id -u)"' in runbook
    assert 'test "$(stat -c %a "$kubeconfig")" = 600' in runbook
    assert 'test "$(stat -c %h "$kubeconfig")" = 1' in runbook
    assert 'test -z "$(git status --porcelain=v1 --untracked-files=all)"' in runbook
    assert 'repository_source_sha="$(git rev-parse HEAD)"' in runbook
    assert 'repository_source_tree="$(git rev-parse HEAD^{tree})"' in runbook
    assert "export PYTHONPATH=src:." in runbook
    assert '$(jq -r .source_sha "$trusted_release")' in runbook
    assert '$(jq -r .source_tree "$trusted_release")' in runbook
    assert 'test "$(stat -c %u "$profile")" = "$(id -u)"' in runbook
    assert 'test "$(stat -c %h "$profile")" = 1' in runbook
    assert "loom admin personal-dev-control-plane render" in normalized
    assert '--file "$profile"' in normalized
    assert '--trusted-release-file "$trusted_release"' in normalized
    assert '--trusted-release-sha256 "$trusted_release_sha256"' in normalized
    assert 'sha256sum "$shadow_render"' in runbook
    assert "python -m loom.personal_dev_storage_lineage_guard" in normalized
    assert '--current "$shadow_render"' in normalized
    assert '--previous "$previous_shadow_render"' in normalized
    assert '--live-inventory "$live_storage_inventory"' in normalized
    storage_command_start = runbook.index(
        'live_storage_inventory="$(mktemp',
    )
    storage_command_end = runbook.index(
        "    --namespace loom-dev",
        storage_command_start,
    )
    storage_inventory_command = runbook[storage_command_start:storage_command_end]
    assert "," not in storage_inventory_command
    for resource in (
        "statefulset.apps/loom-dev-postgres",
        "statefulset.apps/loom-dev-minio",
        "persistentvolumeclaim/data-loom-dev-postgres-0",
        "persistentvolumeclaim/data-loom-dev-minio-0",
        "persistentvolumeclaim/loom-personal-dev-scanner-cache",
    ):
        assert resource in storage_inventory_command
    storage_calls = [
        match.start()
        for match in re.finditer(
            r"^assert_forward_storage_lineage_contract$",
            runbook,
            flags=re.MULTILINE,
        )
    ]
    diff_calls = [
        match.start()
        for match in re.finditer(
            r'^kubectl --kubeconfig "\$kubeconfig" diff --server-side',
            runbook,
            flags=re.MULTILINE,
        )
    ]
    apply_calls = [
        match.start()
        for match in re.finditer(
            r'^kubectl --kubeconfig "\$kubeconfig" apply --server-side',
            runbook,
            flags=re.MULTILINE,
        )
    ]
    assert len(storage_calls) == 4
    assert len(diff_calls) == len(apply_calls) == 2
    assert (
        storage_calls[0]
        < diff_calls[0]
        < storage_calls[1]
        < apply_calls[0]
        < storage_calls[2]
        < diff_calls[1]
        < storage_calls[3]
        < apply_calls[1]
    )
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert "diff_status=0" in runbook
    assert "|| diff_status=$?" in runbook
    assert 'test "$diff_status" -eq 0 || test "$diff_status" -eq 1' in runbook
    assert '"$evidence_dir/server-side-diff.txt"' in runbook
    assert "--field-manager=loom-personal-dev-control-plane" in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "issue #1280 shadow window" in runbook.casefold()
    assert "rollout status statefulset/loom-dev-postgres" in normalized
    assert "rollout status statefulset/loom-dev-minio" in normalized
    assert "--for=condition=complete" in normalized
    assert "rollout status deployment/loom-personal-dev-management" in normalized
    assert "loom admin personal-dev-control-plane status" in normalized
    assert '"schema":"loom-personal-dev-control-plane-status-v1"' in runbook
    assert '"manager_ceiling":0' in runbook
    assert '"ready":true' in runbook
    assert 'previous_shadow_render="$evidence_dir/previous-reviewed-shadow.yaml"' in runbook
    assert "previous_shadow_sha256='<previous-reviewed-64-lowercase-hex>'" in runbook
    assert (
        'test "$(sha256sum "$previous_shadow_render" | awk \'{print $1}\')" = '
        '"$previous_shadow_sha256"'
    ) in normalized
    assert "reapply the previous reviewed shadow" in runbook.casefold()
    assert "stop if any `loom-dev-<owner>` namespace exists" in runbook.casefold()
    assert "stop if any `loom-build-*` namespace exists" in runbook.casefold()
    assert "schema-compatible" in runbook.casefold()
    assert 'rollback_status_evidence="$evidence_dir/rollback-shadow.status.json"' in runbook
    assert '--file "$previous_profile"' in normalized
    assert '--trusted-release-file "$previous_trusted_release"' in normalized
    assert '--trusted-release-sha256 "$previous_trusted_release_sha256"' in normalized
    assert 'cmp -s "$previous_shadow_render_tmp" "$previous_shadow_render"' in runbook
    assert "yaml.safe_load_all" in runbook
    assert "render_identity_set" in runbook
    assert "forward-current-identities" in runbook
    assert "forward-previous-identities" in runbook
    assert 'kind == "Job" and migration_name.fullmatch(name)' in runbook
    assert "live_identity_set" in runbook
    assert 'test ! -s "$live_identities"' in runbook
    assert "assert_live_identity_delta" in runbook
    assert "loom-personal-dev-acme-http01-ingress" in runbook
    assert "loom-personal-dev-capacity-manager-ingress" in runbook
    assert "loom-personal-dev-web-ingress" in runbook
    assert 'test ! -s "$live_unexpected_identities"' in runbook
    assert 'comm -23 \\\n      "$live_missing_identities" "$allowed_missing_identities"' in runbook
    assert "remove_forward_only_web_resources_for_rollback" in runbook
    assert "deployment.apps/loom-personal-dev-web" in runbook
    assert "rollback_diff_status=0" in runbook
    assert "|| rollback_diff_status=$?" in runbook
    assert 'test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1' in runbook
    assert 'sha256sum "$rollback_status_evidence"' in runbook


@pytest.mark.parametrize(
    ("live_variant", "expected_returncode"),
    [
        ("exact", 0),
        ("all-reviewed-missing", 0),
        ("one-reviewed-missing", 0),
        ("unreviewed-missing", 1),
        ("live-only", 1),
    ],
)
def test_personal_management_identity_delta_allows_only_reviewed_missing_subsets(
    tmp_path: Path,
    live_variant: str,
    expected_returncode: int,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    start = runbook.index("assert_live_identity_delta() {")
    end = runbook.index("\n}\n\nassert_forward_identity_contract", start) + 2
    function_source = runbook[start:end]
    base = '["v1","Service","loom-dev","loom-personal-dev-management"]'
    reviewed = [
        '["apps/v1","Deployment","loom-dev","loom-personal-dev-web"]',
        '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-acme-http01-ingress"]',
        '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-capacity-manager-ingress"]',
        '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-web-ingress"]',
        '["v1","Service","loom-dev","loom-personal-dev-web"]',
    ]
    previous_values = sorted([base, *reviewed])
    live_values = list(previous_values)
    if live_variant == "all-reviewed-missing":
        live_values = [base]
    elif live_variant == "one-reviewed-missing":
        live_values = sorted([base, reviewed[0]])
    elif live_variant == "unreviewed-missing":
        live_values = list(reviewed)
    elif live_variant == "live-only":
        live_values = sorted([*previous_values, '["v1","Secret","loom-dev","unexpected"]'])

    paths = {
        name: tmp_path / f"{name}.txt"
        for name in ("previous", "live", "allowed", "missing", "unexpected")
    }
    paths["previous"].write_text("\n".join(previous_values) + "\n", encoding="utf-8")
    paths["live"].write_text("\n".join(live_values) + "\n", encoding="utf-8")
    paths["allowed"].write_text("\n".join(reviewed) + "\n", encoding="utf-8")

    result = subprocess.run(
        [
            "bash",
            "-c",
            function_source + '\nassert_live_identity_delta "$1" "$2" "$3" "$4" "$5"',
            "identity-delta-test",
            *(str(paths[name]) for name in paths),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == expected_returncode, result.stderr


@pytest.mark.parametrize(
    ("previous_web_identity_count", "expected_returncode", "expected_delete"),
    [(0, 0, True), (1, 1, False), (2, 1, False), (3, 0, False)],
)
def test_personal_management_rollback_removes_only_complete_forward_web_set(
    tmp_path: Path,
    previous_web_identity_count: int,
    expected_returncode: int,
    expected_delete: bool,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    start = runbook.index("remove_forward_only_web_resources_for_rollback() {")
    end = runbook.index("\n}\n\nassert_forward_storage_lineage_contract", start) + 2
    function_source = runbook[start:end]
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    previous = tmp_path / "previous-identities.txt"
    web_identities = [
        '["apps/v1","Deployment","loom-dev","loom-personal-dev-web"]',
        '["networking.k8s.io/v1","NetworkPolicy","loom-dev","loom-personal-dev-web-ingress"]',
        '["v1","Service","loom-dev","loom-personal-dev-web"]',
    ]
    previous.write_text(
        "\n".join(web_identities[:previous_web_identity_count]) + "\n",
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    kubectl_log = tmp_path / "kubectl.log"
    fake_kubectl = fake_bin / "kubectl"
    fake_kubectl.write_text(
        '#!/bin/sh\nset -eu\nprintf "%s\\n" "$*" > "$KUBECTL_LOG"\n',
        encoding="utf-8",
    )
    fake_kubectl.chmod(0o755)
    script = (
        "set -euo pipefail\n"
        + function_source
        + "\n"
        + 'render_identity_set() { cat "$1"; }\n'
        + 'evidence_dir="$1"\n'
        + 'previous_shadow_render="$2"\n'
        + 'kubeconfig="$3"\n'
        + "remove_forward_only_web_resources_for_rollback\n"
    )
    result = subprocess.run(
        [
            "bash",
            "-c",
            script,
            "rollback-web-test",
            str(evidence_dir),
            str(previous),
            str(tmp_path / "kubeconfig"),
        ],
        check=False,
        capture_output=True,
        env={
            **os.environ,
            "KUBECTL_LOG": str(kubectl_log),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
        text=True,
    )

    assert result.returncode == expected_returncode, result.stderr
    assert kubectl_log.exists() is expected_delete
    if expected_delete:
        command = kubectl_log.read_text(encoding="utf-8")
        assert "deployment.apps/loom-personal-dev-web" in command
        assert "service/loom-personal-dev-web" in command
        assert "networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress" in command
        assert "--ignore-not-found --wait=true --timeout=300s" in command


@pytest.mark.parametrize(
    ("kubectl_payload", "kubectl_status", "expected_items", "expected_returncode"),
    [
        pytest.param("", 0, 0, 0, id="all-five-absent"),
        pytest.param(
            '{"apiVersion":"v1","items":[{"sentinel":true}],"kind":"List"}',
            0,
            1,
            0,
            id="nonempty-inventory",
        ),
        pytest.param("", 1, 0, 1, id="kubectl-failure"),
    ],
)
def test_personal_management_storage_inventory_producer_handles_kubectl_output(
    tmp_path: Path,
    kubectl_payload: str,
    kubectl_status: int,
    expected_items: int,
    expected_returncode: int,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    function_start = runbook.index("assert_forward_storage_lineage_contract() {")
    function_end = runbook.index("\n}\n\njq -e", function_start) + 2
    function_source = runbook[function_start:function_end]
    fake_bin = tmp_path / "bin"
    evidence_dir = tmp_path / "evidence"
    fake_bin.mkdir()
    evidence_dir.mkdir()
    kubeconfig = tmp_path / "reviewed.kubeconfig"
    shadow_render = tmp_path / "shadow.yaml"
    previous_render = tmp_path / "absent-previous.yaml"
    kubeconfig.write_text("reviewed\n", encoding="utf-8")
    shadow_render.write_text("reviewed\n", encoding="utf-8")
    fake_kubectl = fake_bin / "kubectl"
    fake_kubectl.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'test "$*" = "$EXPECTED_KUBECTL_ARGS"\n'
        'printf %s "$KUBECTL_PAYLOAD"\n'
        'exit "$KUBECTL_STATUS"\n',
        encoding="utf-8",
    )
    fake_kubectl.chmod(0o755)
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "live_inventory=\n"
        'while test "$#" -gt 0; do\n'
        '  if test "$1" = --live-inventory; then\n'
        "    shift\n"
        '    live_inventory="$1"\n'
        "  fi\n"
        "  shift\n"
        "done\n"
        'test -n "$live_inventory"\n'
        'jq -e --argjson expected "$EXPECTED_ITEMS" '
        '\'.apiVersion == "v1" and .kind == "List" and '
        '(.items | length) == $expected\' "$live_inventory" >/dev/null\n',
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    expected_arguments = " ".join(
        (
            "--kubeconfig",
            str(kubeconfig),
            "--request-timeout=10s",
            "get",
            "statefulset.apps/loom-dev-postgres",
            "statefulset.apps/loom-dev-minio",
            "persistentvolumeclaim/data-loom-dev-postgres-0",
            "persistentvolumeclaim/data-loom-dev-minio-0",
            "persistentvolumeclaim/loom-personal-dev-scanner-cache",
            "--namespace",
            "loom-dev",
            "--ignore-not-found",
            "--output=json",
        )
    )
    script = (
        "set -euo pipefail\n"
        f"{function_source}\n"
        'evidence_dir="$1"\n'
        'kubeconfig="$2"\n'
        'shadow_render="$3"\n'
        'previous_shadow_render="$4"\n'
        "previous_shadow_sha256=" + "0" * 64 + "\n"
        "assert_forward_storage_lineage_contract\n"
    )
    environment = {
        **os.environ,
        "EXPECTED_ITEMS": str(expected_items),
        "EXPECTED_KUBECTL_ARGS": expected_arguments,
        "KUBECTL_PAYLOAD": kubectl_payload,
        "KUBECTL_STATUS": str(kubectl_status),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
    }

    result = subprocess.run(
        [
            "bash",
            "-c",
            script,
            "runbook-storage-producer",
            str(evidence_dir),
            str(kubeconfig),
            str(shadow_render),
            str(previous_render),
        ],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == expected_returncode, result.stderr


def test_personal_management_shadow_runbook_preserves_authority_boundaries() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    lowered = runbook.casefold()

    assert "loom-dev-shared" not in runbook
    assert "kubectl create secret" not in lowered
    assert "--from-literal" not in lowered
    assert "loom_svc_dev_instances_enabled=true" not in lowered
    assert "loom_svc_personal_dev_builder_enabled=true" not in lowered
    assert "kubectl scale" not in lowered
    assert "kubectl delete pvc" not in lowered
    assert "loom service up" not in lowered
    assert "loom admin capacity-control-plane render" not in lowered
    assert "systemctl" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered
    assert "approved secret channel" in lowered
    assert "never delete pvcs" in lowered
    assert "physical capacity unchanged" in lowered
    assert "flattened and self-contained" in lowered


def test_personal_management_shadow_runbook_rechecks_interlocks_at_each_apply() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")

    forward_diff = runbook.index('test "$diff_status" -eq 0 || test "$diff_status" -eq 1')
    forward_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        forward_diff,
    )
    forward_interlock = runbook[forward_diff:forward_apply]
    rollback_diff = runbook.index(
        'test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1'
    )
    rollback_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        rollback_diff,
    )
    rollback_interlock = runbook[rollback_diff:rollback_apply]

    for interlock in (forward_interlock, rollback_interlock):
        assert "assert_reviewed_kubeconfig" in interlock
        assert "assert_forward_identity_contract" in interlock
        assert "assert_no_dynamic_namespaces" in interlock
        assert "assert_zero_capacity" in interlock
    assert "assert_current_shadow_artifacts" in forward_interlock
    assert "assert_previous_shadow_artifacts" in rollback_interlock


def test_personal_management_shadow_embedded_identity_tools_execute(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    programs = re.findall(r"<<'PY'\n(.*?)\nPY\n", runbook, flags=re.DOTALL)

    assert len(programs) == 3
    for program in programs:
        compile(program, "personal-dev-management-plane-shadow.md", "exec")

    checked_profile = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
    prepared_profile = tmp_path / "prepared-profile.toml"
    prepared_profile.write_text(
        checked_profile.read_text(encoding="utf-8").replace(
            """\
[native_builder]
prepared = false
agent_instance_id = ""
agent_key_id = ""
public_key_sha256 = ""
host_name = ""
runtime_profile_sha256 = ""
public_store_origin = ""
public_store_endpoint_cidrs = []
""",
            """\
[native_builder]
prepared = true
agent_instance_id = "11111111-1111-4111-8111-111111111111"
agent_key_id = "gb10-native-builder-test-v1"
public_key_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
host_name = "gx10-01c7"
runtime_profile_sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
public_store_origin = "https://objects.dev.yylx.world"
public_store_endpoint_cidrs = ["207.35.188.227/32"]
""",
        ),
        encoding="utf-8",
    )
    for profile, expected_count in (
        (checked_profile, "38\n"),
        (prepared_profile, "40\n"),
    ):
        count = subprocess.run(
            [sys.executable, "-", str(profile)],
            input=programs[0],
            text=True,
            capture_output=True,
            check=True,
        )
        assert count.stdout == expected_count

    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """\
apiVersion: v1
kind: Namespace
metadata:
  name: loom-dev
---
apiVersion: batch/v1
kind: Job
metadata:
  name: loom-personal-dev-migrate-1111111111111111-2222222222222222
  namespace: loom-dev
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-personal-dev-management
  namespace: loom-dev
""",
        encoding="utf-8",
    )
    rendered = subprocess.run(
        [sys.executable, "-", str(manifest)],
        input=programs[1],
        text=True,
        capture_output=True,
        check=True,
    )
    assert rendered.stdout.splitlines() == [
        '["apps/v1","Deployment","loom-dev","loom-personal-dev-management"]'
    ]

    managed_labels = {"app.kubernetes.io/managed-by": "loom-personal-dev-control-plane"}
    namespaced = tmp_path / "namespaced.json"
    namespaced.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {
                            "name": "loom-personal-dev-management",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                    {
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "metadata": {
                            "name": ("loom-personal-dev-migrate-1111111111111111-2222222222222222"),
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "PersistentVolumeClaim",
                        "metadata": {
                            "name": "data-loom-dev-postgres-0",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "derived-pod",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                            "ownerReferences": [{"controller": True, "kind": "ReplicaSet"}],
                        },
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "unexpected-top-level-pod",
                            "namespace": "loom-dev",
                            "labels": managed_labels,
                        },
                    },
                ],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    cluster = tmp_path / "cluster.json"
    cluster.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    {
                        "apiVersion": "rbac.authorization.k8s.io/v1",
                        "kind": "ClusterRole",
                        "metadata": {
                            "name": "loom-personal-dev-management-mutation",
                            "labels": managed_labels,
                        },
                    }
                ],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    live = subprocess.run(
        [sys.executable, "-", str(namespaced), str(cluster)],
        input=programs[2],
        text=True,
        capture_output=True,
        check=True,
    )
    assert live.stdout.splitlines() == [
        '["apps/v1","Deployment","loom-dev","loom-personal-dev-management"]',
        '["rbac.authorization.k8s.io/v1","ClusterRole","","loom-personal-dev-management-mutation"]',
        '["v1","Pod","loom-dev","unexpected-top-level-pod"]',
    ]


def test_dev_fleet_readme_coordinates_both_shadow_readiness_gates() -> None:
    readme = _read("deploy/dev-fleet/README.md")
    normalized = " ".join(readme.split())

    assert "personal-dev-management-plane-shadow.md" in readme
    assert "executable-global-capacity-bridge-rehearsal.md" in readme
    assert (
        "Both the personal-management shadow and the global-capacity zero-ceiling "
        "shadow must report ready before the later acceptance interlock"
    ) in normalized


def test_personal_shadow_runbook_is_indexed_and_architecture_linked() -> None:
    runbook_index = _read("docs/runbooks/README.md")
    architecture = _read("docs/architecture/multi-dev-environments.md")

    assert "personal-dev-management-plane-shadow.md" in runbook_index
    assert "personal-dev-management-plane-shadow.md" in architecture
    assert "personal-dev-management-plane-deployment.md" in architecture


def test_approved_solo_owner_acceptance_runbook_is_byte_preserved() -> None:
    assert (
        _document_sha256("docs/runbooks/personal-dev-zero-capacity-acceptance.md")
        == "743a75eb27d536aa61e929e605f060ea3367e3d80c40f6ef23a4ef1a88a40a24"
    )


def test_zero_capacity_acceptance_runbook_has_exact_single_owner_workflow() -> None:
    runbook = _read("docs/runbooks/personal-dev-zero-capacity-acceptance.md")
    normalized = " ".join(runbook.split())

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "personal-dev-trusted-release-run-<run>-attempt-<attempt>" in runbook
    assert 'trusted_release="$trusted_release_artifact/trusted-release.json"' in runbook
    assert 'repo="$(pwd -P)"' in runbook
    assert 'loom_cli="$repo/.venv/bin/loom"' in runbook
    assert 'python_cli="$repo/.venv/bin/python"' in runbook
    assert 'test -x "$loom_cli"' in runbook
    assert 'test -x "$python_cli"' in runbook
    assert (
        'scanner_cache_lock="$repo/deploy/dev-fleet/personal-dev-scanner-cache-lock.json"'
    ) in runbook
    assert "/home/hongjian/loom/.venv" not in runbook
    assert 'scanner_cache_lock_sha256="$(sha256sum "$scanner_cache_lock"' in runbook
    assert '$(jq -r .scanner.lock_sha256 "$trusted_release")' in runbook
    assert '.images.personal_dev_scanner_cache "$trusted_release"' in runbook
    assert '.release.images.personal_dev_scanner_cache "$acceptance_plan"' in runbook
    assert '.scanner.cache_identity_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_cache_identity_sha256 "$acceptance_plan"' in runbook
    assert '.scanner.database_metadata_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_database_metadata_sha256 "$acceptance_plan"' in runbook
    assert '.scanner.java_database_metadata_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_java_database_metadata_sha256 "$acceptance_plan"' in runbook
    assert 'acceptance_plan="<absolute-owner-only-acceptance-plan.json>"' in runbook
    assert 'test "$(stat -c %a "$acceptance_plan")" = 600' in runbook
    assert 'test "$(stat -c %h "$acceptance_plan")" = 1' in runbook
    assert 'acceptance_plan_sha256="$(sha256sum "$acceptance_plan"' in runbook
    assert "personal-dev-control-plane render-acceptance" in normalized
    assert "personal-dev-control-plane status-acceptance" in normalized
    assert runbook.count("personal-dev-control-plane status-acceptance") >= 3
    assert '"application_ready":true' in runbook
    assert '"capacity_publication_ready":true' in runbook
    assert '"worker_available":false' in runbook
    assert '"manager_ceiling":0' in runbook

    assert 'owner_xdg="<absolute-mode-0700-acceptance-owner-xdg-config-root>"' in runbook
    assert 'primary_name="<owner-primary-personal-name>"' in runbook
    assert 'retained_name="<owner-retained-personal-name>"' in runbook
    assert 'XDG_CONFIG_HOME="$owner_xdg"' in runbook
    assert runbook.count("loom auth whoami") == 1
    assert "primary_deploy_pid=$!" in runbook
    assert "retained_deploy_pid=$!" in runbook
    assert 'wait "$primary_deploy_pid"' in runbook
    assert 'wait "$retained_deploy_pid"' in runbook
    assert "primary_update_pid=$!" in runbook
    assert "retained_update_pid=$!" in runbook
    assert runbook.count("--min-slots 0") >= 5
    assert '--source-root "$primary_source_v1"' in normalized
    assert '--source-root "$retained_source_v1"' in normalized
    assert '--source-root "$primary_source_v2"' in normalized
    assert '--source-root "$retained_source_v2"' in normalized
    assert ".acceptance_owner.user_id" in runbook
    assert ".acceptance_owner.team_id" in runbook
    assert 'loom dev status "$retained_name"' in normalized
    assert 'loom dev status "$primary_name"' in normalized
    assert 'loom dev destroy "$primary_name" --format json' in normalized
    assert 'loom dev destroy "$retained_name" --keep-data --format json' in normalized
    assert 'loom service up --environment "dev-$retained_name"' in normalized
    assert 'jq -e --arg previous "$retained_subject"' in normalized
    assert ".subject_id != $previous" in runbook

    assert 'shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"' in runbook
    assert ".release.shadow_manifest_sha256" in runbook
    assert 'cmp -s "$shadow_render_recheck" "$shadow_render"' in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "--field-manager=loom-personal-dev-control-plane" in normalized
    assert runbook.count("rollout status deployment/loom-personal-dev-management") >= 2
    assert "capture_scanner_cache_init_status" in runbook
    assert runbook.count("personal-dev-scanner-cache-init") >= 2


def test_concurrent_owner_zero_capacity_acceptance_runbook_has_exact_two_owner_workflow() -> None:
    runbook = _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
    normalized = " ".join(runbook.split())

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "personal-dev-trusted-release-run-<run>-attempt-<attempt>" in runbook
    assert 'trusted_release="$trusted_release_artifact/trusted-release.json"' in runbook
    repo_binding = 'repo="$(validated_repository_root "$(pwd -P)")"'
    assert repo_binding in runbook
    assert runbook.index(repo_binding) < runbook.index('prepare_new_evidence_dir "$evidence_dir"')
    assert 'loom_cli="$repo/.venv/bin/loom"' in runbook
    assert 'python_cli="$repo/.venv/bin/python"' in runbook
    assert 'test -x "$loom_cli"' in runbook
    assert 'test -x "$python_cli"' in runbook
    assert (
        'scanner_cache_lock="$repo/deploy/dev-fleet/personal-dev-scanner-cache-lock.json"'
    ) in runbook
    assert "/home/hongjian/loom/.venv" not in runbook
    assert 'scanner_cache_lock_sha256="$(sha256sum "$scanner_cache_lock"' in runbook
    assert '$(jq -r .scanner.lock_sha256 "$trusted_release")' in runbook
    assert '.images.personal_dev_scanner_cache "$trusted_release"' in runbook
    assert '.release.images.personal_dev_scanner_cache "$acceptance_plan"' in runbook
    assert '.scanner.cache_identity_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_cache_identity_sha256 "$acceptance_plan"' in runbook
    assert '.scanner.database_metadata_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_database_metadata_sha256 "$acceptance_plan"' in runbook
    assert '.scanner.java_database_metadata_sha256 "$trusted_release"' in runbook
    assert '.builder.scanner_java_database_metadata_sha256 "$acceptance_plan"' in runbook
    assert 'acceptance_plan="<absolute-owner-only-acceptance-plan.json>"' in runbook
    assert 'test "$(stat -c %a "$acceptance_plan")" = 600' in runbook
    assert 'test "$(stat -c %h "$acceptance_plan")" = 1' in runbook
    assert 'acceptance_plan_sha256="$(sha256sum "$acceptance_plan"' in runbook
    assert "personal-dev-control-plane render-acceptance" in normalized
    assert "personal-dev-control-plane status-acceptance" in normalized
    assert runbook.count("personal-dev-control-plane status-acceptance") >= 3
    assert '"application_ready":true' in runbook
    assert '"capacity_publication_ready":true' in runbook
    assert '"worker_available":false' in runbook
    assert '"manager_ceiling":0' in runbook

    assert 'owner_0_xdg="<absolute-mode-0700-owner-0-xdg-config-root>"' in runbook
    assert 'owner_1_xdg="<absolute-mode-0700-owner-1-xdg-config-root>"' in runbook
    assert 'owner_0_name="<owner-0-personal-name>"' in runbook
    assert 'owner_1_name="<owner-1-personal-name>"' in runbook
    assert 'XDG_CONFIG_HOME="$owner_0_xdg" loom auth whoami --format json' in runbook
    assert 'XDG_CONFIG_HOME="$owner_1_xdg" loom auth whoami --format json' in runbook
    assert runbook.count("loom auth whoami --format json") == 4
    exact_acceptance_credential = (
        '.auth_kind == "bearer" and '
        '.credential_type == "user_owned_api_token" and '
        '.principal_type == "team" and .role == null'
    )
    assert runbook.count(exact_acceptance_credential) == 2
    assert ".acceptance_owners[0].user_id" in runbook
    assert ".acceptance_owners[0].team_id" in runbook
    assert ".acceptance_owners[1].user_id" in runbook
    assert ".acceptance_owners[1].team_id" in runbook
    assert 'test "$(realpath -e "$owner_0_xdg")" != "$(realpath -e "$owner_1_xdg")"' in runbook
    assert 'test "$owner_0_config_identity" != "$owner_1_config_identity"' in runbook
    assert 'owner_0_principal="$(jq -cS \'{user_id,team_id}\' "$owner_0_whoami")"' in runbook
    assert 'owner_1_principal="$(jq -cS \'{user_id,team_id}\' "$owner_1_whoami")"' in runbook
    principal_inequality = 'test "$owner_0_principal" != "$owner_1_principal"'
    assert principal_inequality in runbook
    assert runbook.index(principal_inequality) < runbook.index(
        '( XDG_CONFIG_HOME="$owner_0_xdg" loom service up'
    )
    assert "assert_owner_sessions()" in runbook

    source_root_validation = """
    declare -A source_real_paths=()
    for source_root in "$owner_0_source_v1" "$owner_0_source_v2" "$owner_1_source_v1" "$owner_1_source_v2"; do
      test -d "$source_root"
      test ! -L "$source_root"
      source_real_path="$(realpath -e "$source_root")"
      test "$source_real_path" = "$source_root"
      test "$source_real_path" != "$repo"
      source_real_paths["$source_real_path"]=1
    done
    test "${#source_real_paths[@]}" -eq 4
    """.strip()
    assert source_root_validation in runbook
    assert 'source_real_paths="$(for source_root in' not in runbook

    for owner in (0, 1):
        assert f"owner_{owner}_deploy_pid=$!" in runbook
        assert f'wait "$owner_{owner}_deploy_pid"' in runbook
        assert f"owner_{owner}_update_pid=$!" in runbook
        assert f'wait "$owner_{owner}_update_pid"' in runbook
        for version in (1, 2):
            assert (
                f'owner_{owner}_source_v{version}="<absolute-owner-{owner}-source-v{version}>"'
                in runbook
            )
            assert f'--source-root "$owner_{owner}_source_v{version}"' in normalized
    assert runbook.index("owner_1_deploy_pid=$!") < runbook.index('wait "$owner_0_deploy_pid"')
    assert runbook.index("owner_1_update_pid=$!") < runbook.index('wait "$owner_0_update_pid"')
    assert runbook.count("--min-slots 0") >= 5
    assert "--max-slots 2" in runbook
    assert "--max-slots 3" in runbook
    assert "--max-slots 4" in runbook

    assert "probe_cross_owner_denial()" in runbook
    assert 'local actor_xdg="$1"' in runbook
    assert 'local actor_candidate="$2"' in runbook
    assert 'local target_xdg="$3"' in runbook
    assert 'local target_name="$4"' in runbook
    assert 'local actor_index="$5"' in runbook
    assert 'local target_index="$6"' in runbook
    assert 'local operation="$7"' in runbook
    assert (
        'XDG_CONFIG_HOME="$target_xdg" loom dev status "$target_name" --format json' in normalized
    )
    assert 'XDG_CONFIG_HOME="$actor_xdg" loom dev status "$target_name" --format json' in normalized
    assert '--candidate "$actor_candidate"' in normalized
    assert "--expected-operation-epoch 1" in normalized
    assert "--expected-operation-epoch 0" not in normalized
    assert "--min-slots 0" in normalized
    assert "--quiet" in normalized
    assert (
        'XDG_CONFIG_HOME="$actor_xdg" loom dev destroy "$target_name" --format json' in normalized
    )
    assert normalized.count("--expected-hidden-denial") == 3
    assert (
        'target_epoch="$(jq -er ".operation_epoch | '
        'select(type == \\"number\\" and . > 0)" "$before")"'
    ) in normalized
    assert '--expected-operation-epoch "$target_epoch"' in normalized
    assert 'test "$rc" -eq 1' in runbook
    assert 'test ! -s "$stdout"' in runbook
    assert 'cmp -s "$stderr" "$expected_stderr"' in runbook
    assert 'cmp -s "$before" "$after"' in runbook
    assert 'assert_live_acceptance "$interlock_status"' in runbook
    assert '--arg stdout_sha256 "$(sha256sum "$stdout" | awk \'{print $1}\')"' in runbook
    assert '--arg stderr_sha256 "$(sha256sum "$stderr" | awk \'{print $1}\')"' in runbook
    assert "stderr contents" not in runbook.casefold()
    denial_calls = re.findall(
        r'^    probe_cross_owner_denial "\$owner_([01])_xdg" '
        r'"\$owner_\1_candidate" "\$owner_([01])_xdg" '
        r'"\$owner_\2_name" ([01]) ([01]) (read|update|destroy)$',
        runbook,
        flags=re.MULTILINE,
    )
    assert denial_calls == [
        ("0", "1", "0", "1", "read"),
        ("0", "1", "0", "1", "update"),
        ("0", "1", "0", "1", "destroy"),
        ("1", "0", "1", "0", "read"),
        ("1", "0", "1", "0", "update"),
        ("1", "0", "1", "0", "destroy"),
    ]

    assert 'loom dev destroy "$owner_0_name" --format json' in normalized
    assert 'loom dev destroy "$owner_1_name" --keep-data --format json' in normalized
    assert 'retained_subject_id="$(jq -r .subject_id "$owner_1_updated")"' in runbook
    assert 'retained_incarnation="$(jq -r .subject_incarnation "$owner_1_updated")"' in runbook
    assert (
        'owner_1_redeploy_epoch="$(jq -er ".operation_epoch | '
        'select(type == \\"number\\" and . > 0)" "$owner_1_destroyed")"'
    ) in normalized
    assert '--expected-operation-epoch "$owner_1_redeploy_epoch"' in normalized
    assert ".subject_id == $subject" in runbook
    assert ".subject_incarnation != $incarnation" in runbook
    assert 'loom dev destroy "$owner_1_name" --format json' in normalized
    assert "assert_no_dynamic_namespaces" in runbook
    assert "loom-personal-dev-zero-capacity-acceptance-result-v2" in runbook
    assert "jq -cS" in runbook
    assert "verify-acceptance-result" in normalized
    assert '--acceptance-result-file "$acceptance_result"' in normalized
    assert '--acceptance-result-sha256 "$acceptance_result_sha256"' in normalized
    assert '--rollback-shadow-manifest-file "$shadow_render"' in normalized
    assert '--rollback-shadow-status-file "$rollback_status"' in normalized
    assert ".rollback_shadow_status_sha256 == $rollback_shadow_status_sha256" in runbook

    for forbidden in (
        "owner_xdg",
        "primary_name",
        "retained_name",
        ".subject_id != $previous",
    ):
        assert forbidden not in runbook
    assert re.search(r"\.acceptance_owner(?:[^s]|$)", runbook) is None

    assert 'shadow_render="$evidence_dir/reviewed-rollback-shadow.yaml"' in runbook
    assert ".release.shadow_manifest_sha256" in runbook
    assert 'cmp -s "$shadow_render_recheck" "$shadow_render"' in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "--field-manager=loom-personal-dev-control-plane" in normalized
    assert runbook.count("rollout status deployment/loom-personal-dev-management") >= 2
    assert "capture_scanner_cache_init_status" in runbook
    assert runbook.count("personal-dev-scanner-cache-init") >= 2


def test_native_builder_provider_and_runbooks_are_indexed() -> None:
    architecture = _read("docs/architecture/README.md")
    runbooks = _read("docs/runbooks/README.md")
    fleet = _read("deploy/dev-fleet/README.md")

    assert "personal-dev-native-builder-provider.md" in architecture
    assert "personal-dev-native-builder-runtime.md" in runbooks
    assert "personal-dev-native-builder-acceptance.md" in runbooks
    assert "personal-dev-native-builder-runtime.md" in fleet
    assert "personal-dev-native-builder-acceptance.md" in fleet


def test_concurrent_owner_denial_probe_canonicalizes_pretty_cli_status(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
    canonical_function = _indented_shell_function(
        runbook,
        "assert_canonical_json_line",
    )
    denial_function = _indented_shell_function(runbook, "probe_cross_owner_denial")
    acceptance_plan = tmp_path / "acceptance-plan.json"
    acceptance_plan.write_text(
        json.dumps(
            {
                "acceptance_owners": [
                    {"team_id": "team-0", "user_id": "user-0"},
                    {"team_id": "team-1", "user_id": "user-1"},
                ]
            },
            separators=(",", ":"),
        ),
        encoding="ascii",
    )
    denials_jsonl = tmp_path / "cross-owner-denials.jsonl"
    program = (
        "set -euo pipefail\n"
        + canonical_function
        + "\n"
        + denial_function
        + "\n"
        + f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        + f"acceptance_plan={shlex.quote(str(acceptance_plan))}\n"
        + f"denials_jsonl={shlex.quote(str(denials_jsonl))}\n"
        + r"""
loom() {
  if [[ " $* " == *" --expected-hidden-denial "* ]]; then
    printf '%s\n' '{"error_code":"resource_hidden","http_method":"GET","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_read"}' >&2
    return 1
  fi
  printf '%s\n' '{' '  "operation_epoch": 7,' '  "name": "target"' '}'
}
assert_live_acceptance() {
  printf '%s\n' '{}' > "$1"
}
: > "$denials_jsonl"
probe_cross_owner_denial actor-xdg actor-candidate target-xdg target 0 1 read
"""
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    before = tmp_path / "denial-0-1-read.before.json"
    after = tmp_path / "denial-0-1-read.after.json"
    assert before.read_bytes() == b'{"name":"target","operation_epoch":7}\n'
    assert after.read_bytes() == before.read_bytes()
    denial = json.loads(denials_jsonl.read_text(encoding="ascii"))
    assert denial["target_before_sha256"] == denial["target_after_sha256"]


def test_concurrent_owner_update_denial_probe_uses_noncreating_sentinel_epoch(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
    canonical_function = _indented_shell_function(
        runbook,
        "assert_canonical_json_line",
    )
    denial_function = _indented_shell_function(runbook, "probe_cross_owner_denial")
    acceptance_plan = tmp_path / "acceptance-plan.json"
    acceptance_plan.write_text(
        '{"acceptance_owners":[{"team_id":"team-0","user_id":"user-0"},'
        '{"team_id":"team-1","user_id":"user-1"}]}',
        encoding="ascii",
    )
    denials_jsonl = tmp_path / "cross-owner-denials.jsonl"
    calls = tmp_path / "loom-calls.txt"
    program = (
        "set -euo pipefail\n"
        + canonical_function
        + "\n"
        + denial_function
        + "\n"
        + f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        + f"acceptance_plan={shlex.quote(str(acceptance_plan))}\n"
        + f"denials_jsonl={shlex.quote(str(denials_jsonl))}\n"
        + f"calls={shlex.quote(str(calls))}\n"
        + r"""
loom() {
  printf '%s\n' "$*" >> "$calls"
  if [[ " $* " == *" --expected-hidden-denial "* ]]; then
    printf '%s\n' '{"error_code":"resource_hidden","http_method":"PUT","schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,"target_phase":"target_update"}' >&2
    return 1
  fi
  printf '%s\n' '{"name":"target","operation_epoch":7}'
}
assert_live_acceptance() {
  printf '%s\n' '{}' > "$1"
}
: > "$denials_jsonl"
probe_cross_owner_denial actor-xdg actor-candidate target-xdg target 0 1 update
"""
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    update_call = next(
        line
        for line in calls.read_text(encoding="utf-8").splitlines()
        if line.startswith("service up ")
    )
    assert "--expected-operation-epoch 1" in update_call
    assert "--expected-operation-epoch 0" not in update_call


@pytest.mark.parametrize(
    ("owner_0_server", "owner_1_server", "expected_returncode"),
    [
        (
            "https://loom-service.dev.yylx.world",
            "https://loom-service.dev.yylx.world",
            0,
        ),
        (
            "https://loom-service.dev.yylx.world",
            "https://unrelated.example",
            1,
        ),
        ("https://unrelated.example", "https://unrelated.example", 1),
    ],
)
def test_concurrent_owner_sessions_share_the_reviewed_management_plane(
    tmp_path: Path,
    owner_0_server: str,
    owner_1_server: str,
    expected_returncode: int,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
    origin_function = _indented_shell_function(runbook, "reviewed_public_origin")
    server_function = _indented_shell_function(
        runbook,
        "assert_reviewed_owner_servers",
    )
    whoami_template = {
        "auth_kind": "bearer",
        "credential_type": "user_owned_api_token",
        "expires_at": None,
        "principal_type": "team",
        "role": None,
        "scopes": ["read:own", "submit"],
        "team_id": "team",
        "user_id": "user",
    }
    owner_0_whoami = tmp_path / "owner-0.whoami.json"
    owner_1_whoami = tmp_path / "owner-1.whoami.json"
    owner_0_whoami.write_text(
        json.dumps(
            {**whoami_template, "server": owner_0_server},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="ascii",
    )
    owner_1_whoami.write_text(
        json.dumps(
            {
                **whoami_template,
                "server": owner_1_server,
                "team_id": "other-team",
                "user_id": "other-user",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="ascii",
    )
    program = (
        "set -euo pipefail\n"
        + origin_function
        + "\n"
        + server_function
        + "\n"
        + f"python_cli={shlex.quote(sys.executable)}\n"
        + f"profile={shlex.quote(str(_ROOT / 'deploy/dev-fleet/personal-dev-control-plane.toml'))}\n"
        + f"owner_0_whoami={shlex.quote(str(owner_0_whoami))}\n"
        + f"owner_1_whoami={shlex.quote(str(owner_1_whoami))}\n"
        + 'reviewed_server="$(reviewed_public_origin "$profile")"\n'
        + "assert_reviewed_owner_servers\n"
        + "printf '%s\\n' \"$reviewed_server\"\n"
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == expected_returncode, result.stderr
    if expected_returncode == 0:
        assert result.stdout == "https://loom-service.dev.yylx.world\n"


def test_concurrent_owner_zero_capacity_acceptance_runbook_preserves_stop_and_authority_boundaries() -> (
    None
):
    runbook = _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
    lowered = runbook.casefold()

    assert "separately reviewed concurrent-owner certification window" in lowered
    assert "1280" not in lowered
    assert "personal-dev-multi-owner-durable-launch.md" in runbook

    for phrase in (
        "credential or kubeconfig drift",
        "acceptance window",
        "runtimeclass or scanner drift",
        "secret key inventory drift",
        "migration or storage drift",
        "manager identity, configuration-epoch regression, execution epoch/state, or",
        "namespace ownership drift",
        "personal worker deployment",
    ):
        assert phrase in lowered
    assert "assert_pre_apply_interlocks" in runbook
    assert "assert_live_acceptance" in runbook
    assert "assert_rollback_interlocks" in runbook
    assert "backup_restore_evidence_sha256" in runbook
    assert "runtime_profile_sha256" in runbook
    assert "scanner_finding_policy_sha256" in runbook
    assert "approved secret channel" in lowered
    assert "exact key inventory" in lowered
    assert "two pinned user-owned bearer credentials" in lowered
    assert "arbitrary committed, modified, and untracked source" in lowered
    assert "architecture-specific and architecture-neutral tasks are out of scope" in lowered
    assert "issue #906" in lowered
    assert "never delete pvcs" in lowered
    assert "never delete a namespace directly" in lowered
    assert "physical capacity remains unchanged" in lowered

    for legacy_input in (
        "scanner_binary=",
        "scanner_database=",
        "scanner_java_database=",
    ):
        assert legacy_input not in runbook
    for legacy_reference in (
        '"$scanner_binary"',
        '"$scanner_database"',
        '"$scanner_java_database"',
    ):
        assert legacy_reference not in runbook
    assert "kubectl cp" not in lowered
    assert "--download-db-only" not in lowered
    assert "--download-java-db-only" not in lowered
    assert "hostpath" not in lowered
    assert "temporary egress" not in lowered

    assert "loom-dev-shared" not in runbook
    assert "kubectl create secret" not in lowered
    assert "--from-literal" not in lowered
    assert "kubectl delete" not in lowered
    assert "kubectl scale" not in lowered
    assert "loom admin capacity-control-plane" not in lowered
    assert "systemctl" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered

    forward_diff = runbook.index("acceptance_diff_status=0")
    forward_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        forward_diff,
    )
    assert "assert_pre_apply_interlocks" in runbook[forward_diff:forward_apply]
    rollback_diff = runbook.index("rollback_diff_status=0")
    rollback_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        rollback_diff,
    )
    assert "assert_rollback_interlocks" in runbook[rollback_diff:rollback_apply]


def test_zero_capacity_acceptance_runbook_preserves_stop_and_authority_boundaries() -> None:
    runbook = _read("docs/runbooks/personal-dev-zero-capacity-acceptance.md")
    lowered = runbook.casefold()

    for phrase in (
        "credential or kubeconfig drift",
        "acceptance window",
        "runtimeclass or scanner drift",
        "secret key inventory drift",
        "migration or storage drift",
        "manager identity, configuration-epoch regression, execution epoch/state, or",
        "namespace ownership drift",
        "personal worker deployment",
    ):
        assert phrase in lowered
    assert "assert_pre_apply_interlocks" in runbook
    assert "assert_live_acceptance" in runbook
    assert "assert_rollback_interlocks" in runbook
    assert "backup_restore_evidence_sha256" in runbook
    assert "runtime_profile_sha256" in runbook
    assert "scanner_finding_policy_sha256" in runbook
    assert "approved secret channel" in lowered
    assert "exact key inventory" in lowered
    assert "single authenticated acceptance-owner session" in lowered
    assert "arbitrary committed, modified, and untracked source" in lowered
    assert "architecture-specific and architecture-neutral tasks are out of scope" in lowered
    assert "issue #906" in lowered
    assert "never delete pvcs" in lowered
    assert "never delete a namespace directly" in lowered
    assert "physical capacity remains unchanged" in lowered

    for legacy_input in (
        "scanner_binary=",
        "scanner_database=",
        "scanner_java_database=",
    ):
        assert legacy_input not in runbook
    for legacy_reference in (
        '"$scanner_binary"',
        '"$scanner_database"',
        '"$scanner_java_database"',
    ):
        assert legacy_reference not in runbook
    assert "kubectl cp" not in lowered
    assert "--download-db-only" not in lowered
    assert "--download-java-db-only" not in lowered
    assert "hostpath" not in lowered
    assert "temporary egress" not in lowered

    assert "loom-dev-shared" not in runbook
    assert "kubectl create secret" not in lowered
    assert "--from-literal" not in lowered
    assert "kubectl delete" not in lowered
    assert "kubectl scale" not in lowered
    assert "loom admin capacity-control-plane" not in lowered
    assert "systemctl" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered

    forward_diff = runbook.index("acceptance_diff_status=0")
    forward_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        forward_diff,
    )
    assert "assert_pre_apply_interlocks" in runbook[forward_diff:forward_apply]
    rollback_diff = runbook.index("rollback_diff_status=0")
    rollback_apply = runbook.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side',
        rollback_diff,
    )
    assert "assert_rollback_interlocks" in runbook[rollback_diff:rollback_apply]


def test_zero_capacity_acceptance_runbook_is_indexed_and_current() -> None:
    runbook_index = _read("docs/runbooks/README.md")
    fleet = _read("deploy/dev-fleet/README.md")
    architecture = _read("docs/architecture/multi-dev-environments.md")
    deployment_architecture = _read("docs/architecture/personal-dev-management-plane-deployment.md")

    required = (
        "personal-dev-zero-capacity-acceptance.md",
        "personal-dev-durable-launch.md",
        "personal-dev-concurrent-owner-zero-capacity-acceptance.md",
        "personal-dev-multi-owner-durable-launch.md",
    )
    for path in required:
        assert path in runbook_index
        assert path in architecture
        assert path in deployment_architecture
    assert "personal-dev-zero-capacity-acceptance.md" in fleet
    assert "render-acceptance" in fleet
    assert "status-acceptance" in fleet
    for document in (architecture, deployment_architecture):
        normalized = " ".join(document.casefold().split())
        assert "sole-owner/two-environment" in normalized
        assert "before a second person is onboarded" in normalized
        assert "verified schema-v2 result" in normalized
    assert "worker_available=false" in architecture


def test_approved_solo_owner_durable_launch_is_byte_preserved() -> None:
    assert (
        _document_sha256("docs/runbooks/personal-dev-durable-launch.md")
        == "f31575e320499624438250b9fec20cef22cc56628e56f107251c7457ae9875ef"
    )


@pytest.mark.parametrize(
    ("relative", "rollback_manifest_variable", "rollback_status_variable"),
    [
        (
            "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
            "shadow_render",
            "rollback_status",
        ),
        (
            "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
            "acceptance_rollback_manifest",
            "rollback_evidence",
        ),
    ],
)
def test_v2_runbooks_pass_bound_rollback_shadow_to_read_only_verifier(
    tmp_path: Path,
    relative: str,
    rollback_manifest_variable: str,
    rollback_status_variable: str,
) -> None:
    command = _shell_command_block(
        _read(relative),
        '"$loom_cli" admin personal-dev-control-plane verify-acceptance-result',
        '> "$acceptance_verification"',
    )
    arguments = tmp_path / "arguments"
    loom_cli = tmp_path / "loom"
    loom_cli.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'printf \'%s\\0\' "$@" > "$ARGUMENTS"\n'
        "printf '{}\\n'\n",
        encoding="ascii",
    )
    loom_cli.chmod(0o700)
    plan = tmp_path / "acceptance-plan.json"
    result_file = tmp_path / "acceptance-result.json"
    rollback_manifest = tmp_path / "rollback-shadow.yaml"
    rollback = tmp_path / "rollback-shadow.status.json"
    verification = tmp_path / "acceptance-verification.json"
    program = (
        "set -euo pipefail\n"
        + f"loom_cli={shlex.quote(str(loom_cli))}\n"
        + f"acceptance_plan={shlex.quote(str(plan))}\n"
        + f"acceptance_plan_sha256={'1' * 64}\n"
        + f"acceptance_result={shlex.quote(str(result_file))}\n"
        + f"acceptance_result_sha256={'2' * 64}\n"
        + f"acceptance_manifest_sha256={'3' * 64}\n"
        + f"acceptance_render_sha256={'3' * 64}\n"
        + f"{rollback_manifest_variable}={shlex.quote(str(rollback_manifest))}\n"
        + f"{rollback_status_variable}={shlex.quote(str(rollback))}\n"
        + f"acceptance_verification={shlex.quote(str(verification))}\n"
        + command
        + "\n"
    )
    environment = os.environ.copy()
    environment["ARGUMENTS"] = str(arguments)

    completed = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr
    assert arguments.read_bytes().split(b"\0")[:-1] == [
        b"admin",
        b"personal-dev-control-plane",
        b"verify-acceptance-result",
        b"--acceptance-plan-file",
        os.fsencode(plan),
        b"--acceptance-plan-sha256",
        b"1" * 64,
        b"--acceptance-result-file",
        os.fsencode(result_file),
        b"--acceptance-result-sha256",
        b"2" * 64,
        b"--acceptance-manifest-sha256",
        b"3" * 64,
        b"--rollback-shadow-manifest-file",
        os.fsencode(rollback_manifest),
        b"--rollback-shadow-status-file",
        os.fsencode(rollback),
    ]


def test_durable_launch_uses_the_exact_checkout_cli() -> None:
    runbook = _read("docs/runbooks/personal-dev-durable-launch.md")

    assert 'loom_cli="$repo/.venv/bin/loom"' in runbook
    assert 'test -x "$loom_cli"' in runbook
    assert runbook.count('"$loom_cli" admin personal-dev-control-plane') >= 7
    assert "operational_evidence_args=(" in runbook
    assert '--source-root "$repo"' in runbook
    assert '--trusted-launcher-profile-file "$trusted_launcher_profile"' in runbook
    assert '--scanner-finding-policy-file "$scanner_finding_policy"' in runbook
    assert '--backup-restore-evidence-file "$backup_restore_evidence"' in runbook
    assert runbook.count('"${operational_evidence_args[@]}"') == 3
    assert 'test "$solver_port" = 8089' in runbook
    assert "networkpolicy/loom-personal-dev-acme-http01-ingress" in runbook
    assert "networkpolicy/loom-personal-dev-management-ingress" in runbook
    assert '{"acme.cert-manager.io/http01-solver":"true"}' in runbook
    assert '{"app":"loom-personal-dev-management"}' in runbook
    assert '(.spec | has("egress") | not)' in runbook
    assert runbook.count('(.spec.ingress[0] | has("from") | not)') >= 2
    assert '.spec.ingress == [{ports:[{port:8090,protocol:"TCP"}]}]' in runbook
    assert "--proto '=https' --tlsv1.2" in runbook
    assert "/home/hongjian/loom/.venv" not in runbook


def test_durable_launch_bounds_every_public_web_probe_and_checks_both_auth_spas() -> None:
    runbook = _read("docs/runbooks/personal-dev-durable-launch.md")
    curl_commands = re.findall(r"(?m)^\s*curl(?:[^\n]|\\\n)*", runbook)

    assert curl_commands
    assert all("--disable" in command for command in curl_commands)
    assert all("--connect-timeout 10" in command for command in curl_commands)
    assert all("--max-time 30" in command for command in curl_commands)
    assert '"https://$management_host/auth/reset"' in runbook
    assert '"https://$management_host/auth/setup"' in runbook


def test_multi_owner_durable_launch_requires_verified_v2_result() -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")

    assert 'loom_cli="$repo/.venv/bin/loom"' in runbook
    assert 'test -x "$loom_cli"' in runbook
    assert runbook.count('"$loom_cli" admin personal-dev-control-plane') >= 7
    assert "operational_evidence_args=(" in runbook
    assert '--source-root "$repo"' in runbook
    assert '--trusted-launcher-profile-file "$trusted_launcher_profile"' in runbook
    assert '--scanner-finding-policy-file "$scanner_finding_policy"' in runbook
    assert '--backup-restore-evidence-file "$backup_restore_evidence"' in runbook
    assert runbook.count('"${operational_evidence_args[@]}"') == 5
    assert 'test "$solver_port" = 8089' in runbook
    assert "networkpolicy/loom-personal-dev-acme-http01-ingress" in runbook
    assert "networkpolicy/loom-personal-dev-management-ingress" in runbook
    assert '{"acme.cert-manager.io/http01-solver":"true"}' in runbook
    assert '{"app":"loom-personal-dev-management"}' in runbook
    assert '(.spec | has("egress") | not)' in runbook
    assert runbook.count('(.spec.ingress[0] | has("from") | not)') >= 2
    assert '.spec.ingress == [{ports:[{port:8090,protocol:"TCP"}]}]' in runbook
    assert "--proto '=https' --tlsv1.2" in runbook
    assert "/home/hongjian/loom/.venv" not in runbook
    assert "schema-v1 single-owner record remains historical compatibility" in runbook
    assert "final multi-person launch requires a verified schema-v2 result" in runbook
    assert "separately reviewed multi-owner durable-launch window" in runbook
    repo_binding = 'repo="$(validated_repository_root "$repo")"'
    assert repo_binding in runbook
    assert runbook.index(repo_binding) < runbook.index('prepare_new_evidence_dir "$evidence_dir"')
    assert "#1280" not in runbook
    assert "personal-dev-concurrent-owner-zero-capacity-acceptance.md" in runbook
    assert 'acceptance_plan="<absolute-owner-only-acceptance-plan.json>"' in runbook
    assert 'acceptance_result="<absolute-owner-only-acceptance-result-v2.json>"' in runbook
    assert (
        'acceptance_rollback_manifest="<absolute-owner-only-acceptance-rollback-shadow-manifest>"'
        in runbook
    )
    verify = runbook.index("verify-acceptance-result")
    render = runbook.index(" render-operational ")
    assert verify < render
    assert '--acceptance-plan-file "$acceptance_plan"' in runbook
    assert '--acceptance-plan-sha256 "$acceptance_plan_sha256"' in runbook
    assert '--acceptance-result-file "$acceptance_result"' in runbook
    assert '--acceptance-result-sha256 "$acceptance_result_sha256"' in runbook
    assert '--acceptance-manifest-sha256 "$acceptance_manifest_sha256"' in runbook
    assert '--rollback-shadow-manifest-file "$acceptance_rollback_manifest"' in runbook
    assert '--rollback-shadow-status-file "$rollback_evidence"' in runbook
    assert ".rollback_shadow_status_sha256 == $rollback_shadow_status_sha256" in runbook
    assert ".owner_count == 2" in runbook
    assert ".cross_owner_denial_count == 6" in runbook


@pytest.mark.parametrize(
    ("mutation", "expected_returncode"),
    [
        ("shadow-binding", 0),
        ("operational-binding", 0),
        ("acceptance-binding", 1),
        ("unexpected-loom-annotation", 1),
        ("ingress-snippet", 1),
        ("ingress-default-backend", 1),
    ],
)
def test_multi_owner_durable_preflight_and_final_route_contract_share_ingress_bindings(
    tmp_path: Path,
    mutation: str,
    expected_returncode: int,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    preflight = _shell_command_block(
        runbook,
        "ingress/loom-personal-dev-management -o json \\",
        '"$evidence_dir/management.ingress.json" >/dev/null',
    )
    preflight = 'kubectl --kubeconfig "$kubeconfig" --namespace loom-dev get \\\n' + preflight
    final_route_contract = _fenced_shell_function(runbook, "assert_web_api_route_contract")
    annotations = {
        "cert-manager.io/cluster-issuer": "letsencrypt-prod",
        "loom.dev/render-input-sha256": "a" * 64,
        "loom.dev/trusted-release-sha256": "b" * 64,
        "nginx.ingress.kubernetes.io/proxy-body-size": "512m",
        "nginx.ingress.kubernetes.io/proxy-read-timeout": "300",
    }
    if mutation == "operational-binding":
        annotations["loom.dev/operational-plan-sha256"] = "c" * 64
    elif mutation == "acceptance-binding":
        annotations["loom.dev/acceptance-plan-sha256"] = "c" * 64
    elif mutation == "unexpected-loom-annotation":
        annotations["loom.dev/unexpected"] = "c" * 64
    elif mutation == "ingress-snippet":
        annotations["nginx.ingress.kubernetes.io/server-snippet"] = "deny all;"

    ingress: dict[str, object] = {
        "metadata": {"annotations": annotations},
        "spec": {
            "ingressClassName": "nginx",
            "rules": [
                {
                    "host": "loom-service.dev.yylx.world",
                    "http": {
                        "paths": [
                            {
                                "backend": {
                                    "service": {
                                        "name": "loom-personal-dev-management",
                                        "port": {"number": 8090},
                                    }
                                },
                                "path": "/api",
                                "pathType": "Prefix",
                            },
                            {
                                "backend": {
                                    "service": {
                                        "name": "loom-personal-dev-web",
                                        "port": {"number": 80},
                                    }
                                },
                                "path": "/",
                                "pathType": "Prefix",
                            },
                        ]
                    },
                }
            ],
            "tls": [
                {
                    "hosts": ["loom-service.dev.yylx.world"],
                    "secretName": "loom-personal-dev-management-tls",
                }
            ],
        },
    }
    if mutation == "ingress-default-backend":
        ingress["spec"] = {
            **ingress["spec"],
            "defaultBackend": {"service": {"name": "wrong"}},
        }

    network_policy = {
        "spec": {
            "podSelector": {"matchLabels": {"app": "loom-personal-dev-web"}},
            "policyTypes": ["Ingress"],
            "ingress": [{"ports": [{"port": 8080, "protocol": "TCP"}]}],
        }
    }
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    program = (
        "set -u\n"
        + final_route_contract
        + "\n"
        + f"ingress={shlex.quote(json.dumps(ingress))}\n"
        + f"network_policy={shlex.quote(json.dumps(network_policy))}\n"
        + f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
        + "kubeconfig=/unused/kubeconfig\n"
        + "management_host=loom-service.dev.yylx.world\n"
        + "reviewed_server=https://loom-service.dev.yylx.world\n"
        + "kubectl() {\n"
        + '  case "$*" in\n'
        + "    *networkpolicy/loom-personal-dev-web-ingress*) printf '%s\\n' \"$network_policy\" ;;\n"
        + "    *ingress/loom-personal-dev-management*) printf '%s\\n' \"$ingress\" ;;\n"
        + "    *) return 91 ;;\n"
        + "  esac\n"
        + "}\n"
        + "curl() {\n"
        + '  case "${!#}" in\n'
        + "    */api/v1/health) printf '%s\\n' '{\"status\":\"ok\"}' ;;\n"
        + '    */loom-frontend-config.json) printf \'%s\\n\' \'{"environment":"development","routePath":"","apiBase":"","apiRouteBase":"https://loom-service.dev.yylx.world/api"}\' ;;\n'
        + "    */auth/reset|*/auth/setup) printf '%s\\n' '<div id=\"root\"></div>' ;;\n"
        + "    *) return 92 ;;\n"
        + "  esac\n"
        + "}\n"
        + "preflight_status=0\n"
        + "if ! (\n"
        + preflight
        + "\n); then preflight_status=1; fi\n"
        + "final_status=0\n"
        + "if ! assert_web_api_route_contract; then final_status=1; fi\n"
        + 'printf \'preflight=%s final=%s\\n\' "$preflight_status" "$final_status"\n'
        + f'test "$preflight_status" -eq {expected_returncode}\n'
        + f'test "$final_status" -eq {expected_returncode}\n'
    )

    completed = subprocess.run(["bash"], input=program, text=True, capture_output=True, check=False)

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == f"preflight={expected_returncode} final={expected_returncode}\n"


@pytest.mark.parametrize(
    "relative",
    [
        "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
        "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
    ],
)
@pytest.mark.parametrize(
    ("mutation", "expected_returncode"),
    [
        ("exact", 0),
        ("acceptance-binding", 0),
        ("operational-binding", 0),
        ("web-port", 1),
        ("web-selector", 1),
        ("ingress-backend", 1),
        ("ingress-default-backend", 1),
        ("ingress-snippet", 1),
        ("frontend-config", 1),
        ("reset-route", 1),
        ("setup-route", 1),
        ("api-health", 1),
        ("api-health-payload", 1),
    ],
)
def test_multi_owner_runbooks_execute_exact_web_and_api_route_contract(
    tmp_path: Path,
    relative: str,
    mutation: str,
    expected_returncode: int,
) -> None:
    runbook = _read(relative)
    if "concurrent-owner" in relative:
        function = _indented_shell_function(runbook, "assert_web_api_route_contract")
    else:
        function = _fenced_shell_function(runbook, "assert_web_api_route_contract")

    web_port = 80 if mutation == "web-port" else 8080
    api_service = (
        "loom-personal-dev-web"
        if mutation == "ingress-backend"
        else ("loom-personal-dev-management")
    )
    frontend_api = (
        "https://wrong.example/api"
        if mutation == "frontend-config"
        else "https://loom-service.dev.yylx.world/api"
    )
    reset_page = "<html></html>" if mutation == "reset-route" else '<div id="root"></div>'
    setup_page = "<html></html>" if mutation == "setup-route" else '<div id="root"></div>'
    web_selector: dict[str, object] = {"matchLabels": {"app": "loom-personal-dev-web"}}
    if mutation == "web-selector":
        web_selector["matchExpressions"] = [{"key": "loom.dev/never", "operator": "Exists"}]
    network_policy = {
        "spec": {
            "podSelector": web_selector,
            "policyTypes": ["Ingress"],
            "ingress": [{"ports": [{"port": web_port, "protocol": "TCP"}]}],
        }
    }
    ingress = {
        "metadata": {
            "annotations": {
                "cert-manager.io/cluster-issuer": "letsencrypt-prod",
                "nginx.ingress.kubernetes.io/proxy-body-size": "512m",
                "nginx.ingress.kubernetes.io/proxy-read-timeout": "300",
                "loom.dev/render-input-sha256": "a" * 64,
                "loom.dev/trusted-release-sha256": "b" * 64,
            }
        },
        "spec": {
            "ingressClassName": "nginx",
            "rules": [
                {
                    "host": "loom-service.dev.yylx.world",
                    "http": {
                        "paths": [
                            {
                                "backend": {
                                    "service": {
                                        "name": api_service,
                                        "port": {"number": 8090},
                                    }
                                },
                                "path": "/api",
                                "pathType": "Prefix",
                            },
                            {
                                "backend": {
                                    "service": {
                                        "name": "loom-personal-dev-web",
                                        "port": {"number": 80},
                                    }
                                },
                                "path": "/",
                                "pathType": "Prefix",
                            },
                        ]
                    },
                }
            ],
            "tls": [
                {
                    "hosts": ["loom-service.dev.yylx.world"],
                    "secretName": "loom-personal-dev-management-tls",
                }
            ],
        },
    }
    if mutation == "ingress-default-backend":
        ingress["spec"]["defaultBackend"] = {"service": {"name": "wrong"}}
    if mutation == "ingress-snippet":
        ingress["metadata"]["annotations"]["nginx.ingress.kubernetes.io/server-snippet"] = (
            "deny all;"
        )
    if mutation == "acceptance-binding":
        ingress["metadata"]["annotations"]["loom.dev/acceptance-plan-sha256"] = "c" * 64
    if mutation == "operational-binding":
        ingress["metadata"]["annotations"]["loom.dev/operational-plan-sha256"] = "c" * 64
    if mutation in {"acceptance-binding", "operational-binding"}:
        expected_returncode = int(
            (mutation == "acceptance-binding") != ("concurrent-owner" in relative)
        )
    frontend = {
        "apiBase": "",
        "apiRouteBase": frontend_api,
        "environment": "development",
        "environmentLabel": "Personal development",
        "routePath": "",
    }
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    program = (
        "set -euo pipefail\n"
        + function
        + "\n"
        + f"network_policy={shlex.quote(json.dumps(network_policy))}\n"
        + f"ingress={shlex.quote(json.dumps(ingress))}\n"
        + f"frontend={shlex.quote(json.dumps(frontend))}\n"
        + f"reset_page={shlex.quote(reset_page)}\n"
        + f"setup_page={shlex.quote(setup_page)}\n"
        + f"health_rc={22 if mutation == 'api-health' else 0}\n"
        + f"health_payload={shlex.quote(json.dumps({'status': 'unavailable' if mutation == 'api-health-payload' else 'ok'}))}\n"
        + f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
        + "kubeconfig=/unused/kubeconfig\n"
        + "management_host=loom-service.dev.yylx.world\n"
        + "reviewed_server=https://loom-service.dev.yylx.world\n"
        + "kubectl() {\n"
        + '  case "$*" in\n'
        + "    *networkpolicy/loom-personal-dev-web-ingress*) printf '%s\\n' \"$network_policy\" ;;\n"
        + "    *ingress/loom-personal-dev-management*) printf '%s\\n' \"$ingress\" ;;\n"
        + "    *) return 91 ;;\n"
        + "  esac\n"
        + "}\n"
        + "curl() {\n"
        + '  test "$1" = --disable || return 93\n'
        + '  case " $* " in\n'
        + "    *' --connect-timeout 10 --max-time 30 '*) ;;\n"
        + "    *) return 94 ;;\n"
        + "  esac\n"
        + '  local url="${!#}"\n'
        + '  case "$url" in\n'
        + '    */api/v1/health) test "$health_rc" -eq 0 || return "$health_rc"; printf \'%s\\n\' "$health_payload" ;;\n'
        + "    */loom-frontend-config.json) printf '%s\\n' \"$frontend\" ;;\n"
        + "    */auth/reset) printf '%s\\n' \"$reset_page\" ;;\n"
        + "    */auth/setup) printf '%s\\n' \"$setup_page\" ;;\n"
        + "    *) return 92 ;;\n"
        + "  esac\n"
        + "}\n"
        + "assert_web_api_route_contract\n"
    )

    completed = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == expected_returncode, completed.stderr


def test_multi_owner_runbooks_bind_web_release_render_and_rollout() -> None:
    for relative in (
        "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
        "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
    ):
        runbook = _read(relative)
        assert 'test "$(jq -r .schema_version "$trusted_release")" = 3' in runbook
        assert ".resource_count == 38" in runbook
        assert runbook.count("deployment/loom-personal-dev-web --timeout=300s") >= 2
        assert runbook.count("assert_web_api_route_contract") >= 3


def test_shadow_runbook_binds_resource_count_to_the_validated_profile() -> None:
    runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")

    assert "load_personal_dev_control_plane_profile" in runbook
    assert "expected_shadow_resource_count" in runbook
    assert "profile.native_builder is not None and profile.native_builder.prepared" in runbook
    assert '--argjson resource_count "$expected_shadow_resource_count"' in runbook
    assert ".resource_count == $resource_count" in runbook
    assert ".resource_count == 38" not in runbook
    assert '"namespaced-resources","observed":36' in runbook


@pytest.mark.parametrize(
    "relative",
    [
        "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
        "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
    ],
)
def test_multi_owner_rollback_does_not_depend_on_public_route_availability(
    relative: str,
) -> None:
    runbook = _read(relative)
    rollback_interlocks = (
        _indented_shell_function(runbook, "assert_rollback_interlocks")
        if "concurrent-owner" in relative
        else _fenced_shell_function(runbook, "assert_rollback_interlocks")
    )

    assert "assert_web_api_route_contract" not in rollback_interlocks
    assert "assert_dns_tls_ingress" not in rollback_interlocks
    rollback_apply = runbook.index('> "$evidence_dir/rollback.server-side-apply.txt"')
    web_rollout = runbook.index(
        "deployment/loom-personal-dev-web --timeout=300s",
        rollback_apply,
    )
    route_contract = runbook.index("assert_web_api_route_contract", web_rollout)

    assert rollback_apply < web_rollout < route_contract


@pytest.mark.parametrize(
    ("relative", "mode", "expected_returncode"),
    [
        (
            "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
            "acceptance",
            0,
        ),
        (
            "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
            "operational",
            0,
        ),
    ],
)
@pytest.mark.parametrize(
    "mutation",
    [
        "all-ready",
        "allowed-web-failure",
    ],
)
def test_web_v2_rollback_accepts_only_consistent_web_unavailability(
    tmp_path: Path,
    relative: str,
    mode: str,
    expected_returncode: int,
    mutation: str,
) -> None:
    result = _run_web_v2_rollback_interlocks(
        tmp_path,
        relative,
        _web_v2_rollback_status(mode, mutation),
    )

    assert result.returncode == expected_returncode, result.stderr


@pytest.mark.parametrize(
    ("relative", "mode", "expected_returncode"),
    [
        (
            "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
            "acceptance",
            0,
        ),
        (
            "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
            "operational",
            1,
        ),
    ],
)
def test_web_v2_rollback_acceptance_window_expiry_is_acceptance_only(
    tmp_path: Path,
    relative: str,
    mode: str,
    expected_returncode: int,
) -> None:
    result = _run_web_v2_rollback_interlocks(
        tmp_path,
        relative,
        _web_v2_rollback_status(mode, "acceptance-window-expired"),
    )

    assert result.returncode == expected_returncode, result.stderr


@pytest.mark.parametrize(
    "mutation",
    [
        "non-web-blocker",
        "non-web-component",
        "missing-component",
        "duplicate-component",
        "extra-component",
        "web-blocker-with-ready-web",
        "web-blocker-with-ready-namespaced-resources",
        "web-unready-without-blocker",
    ],
)
@pytest.mark.parametrize(
    ("relative", "mode"),
    [
        (
            "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
            "acceptance",
        ),
        (
            "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
            "operational",
        ),
    ],
)
def test_web_v2_rollback_rejects_non_web_or_inconsistent_statuses(
    tmp_path: Path,
    relative: str,
    mode: str,
    mutation: str,
) -> None:
    result = _run_web_v2_rollback_interlocks(
        tmp_path,
        relative,
        _web_v2_rollback_status(mode, mutation),
    )

    assert result.returncode == 1, result.stderr


@pytest.mark.parametrize(
    "relative",
    [
        "docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md",
        "docs/runbooks/personal-dev-multi-owner-durable-launch.md",
    ],
)
def test_multi_owner_runbook_curl_is_bounded_and_ignores_user_config(
    relative: str,
) -> None:
    runbook = _read(relative)
    curl_lines = [line.strip() for line in runbook.splitlines() if re.search(r"\bcurl\s", line)]
    required = (
        "curl --disable --fail --silent --show-error --proto '=https' "
        "--tlsv1.2 --connect-timeout 10 --max-time 30"
    )

    assert curl_lines
    assert all(required in line for line in curl_lines), curl_lines


@pytest.mark.parametrize(
    ("start", "apply", "calls"),
    [
        (
            'test "$diff_status" -eq 0 || test "$diff_status" -eq 1',
            'kubectl --kubeconfig "$kubeconfig" apply --server-side',
            (
                'assert_server_side_diff_unchanged "$operational_render" '
                '"$evidence_dir/operational.server-side-diff.txt" operational',
                "assert_operational_interlocks",
                "assert_operational_render_binding",
            ),
        ),
        (
            'test "$rollback_diff_status" -eq 0 || test "$rollback_diff_status" -eq 1',
            'kubectl --kubeconfig "$kubeconfig" apply --server-side',
            (
                'assert_server_side_diff_unchanged "$shadow_render" '
                '"$evidence_dir/rollback.server-side-diff.txt" rollback',
                "assert_rollback_interlocks",
                "assert_shadow_render_binding",
            ),
        ),
    ],
)
def test_multi_owner_durable_launch_executes_pre_apply_calls_in_order_and_mutations_fail(
    start: str,
    apply: str,
    calls: tuple[str, ...],
) -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    assert _executable_calls_are_ordered(
        runbook,
        start=start,
        apply=apply,
        calls=calls,
    )

    for call in calls:
        executable_line = f"\n{call}\n"
        assert executable_line in runbook
        removed = runbook.replace(executable_line, "\n", 1)
        assert not _executable_calls_are_ordered(
            removed,
            start=start,
            apply=apply,
            calls=calls,
        )
        commented = runbook.replace(executable_line, f"\n# {call}\n", 1)
        assert not _executable_calls_are_ordered(
            commented,
            start=start,
            apply=apply,
            calls=calls,
        )
        displaced = runbook.replace(executable_line, "\n", 1) + f"\n```bash\n{call}\n```\n"
        assert not _executable_calls_are_ordered(
            displaced,
            start=start,
            apply=apply,
            calls=calls,
        )


def test_multi_owner_durable_launch_captures_and_immediately_revalidates_render_bindings() -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    render = runbook.index('operational_render_sha256="$(sha256sum')
    forward_diff = runbook.index("diff_status=0", render)
    for call in (
        'capture_file_binding "$operational_render" 16777216',
        'capture_file_binding "$operational_render_evidence" 4194304',
        'capture_file_binding "$shadow_render" 16777216',
        'capture_file_binding "$shadow_render_evidence" 4194304',
    ):
        assert call in runbook[render:forward_diff]
    assert 'cmp -s "$shadow_recheck_evidence" "$shadow_render_evidence"' in runbook
    assert 'assert_file_binding "$operational_render"' in _fenced_shell_function(
        runbook,
        "assert_operational_render_binding",
    )
    assert 'assert_file_binding "$operational_render_evidence"' in _fenced_shell_function(
        runbook,
        "assert_operational_render_binding",
    )
    assert 'assert_file_binding "$shadow_render"' in _fenced_shell_function(
        runbook,
        "assert_shadow_render_binding",
    )
    assert 'assert_file_binding "$shadow_render_evidence"' in _fenced_shell_function(
        runbook,
        "assert_shadow_render_binding",
    )


def test_multi_owner_durable_launch_file_binding_helpers_detect_byte_or_identity_drift(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in ("assert_owner_only_file", "capture_file_binding", "assert_file_binding")
    )
    bound = tmp_path / "bound.json"
    bound.write_bytes(b"reviewed-bytes")
    bound.chmod(0o600)
    program = (
        "set -euo pipefail\n"
        "declare -A reviewed_file_identity=()\n"
        "declare -A reviewed_file_sha256=()\n"
        "declare -A reviewed_file_maximum=()\n"
        + functions
        + "\n"
        + f"bound={shlex.quote(str(bound))}\n"
        + 'capture_file_binding "$bound" 64\n'
        + 'assert_file_binding "$bound"\n'
        + 'printf changed-bytes > "$bound"\n'
        + 'if assert_file_binding "$bound"; then exit 90; fi\n'
        + "printf 'drift-detected\\n'\n"
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "drift-detected\n"


def test_multi_owner_durable_launch_diff_recheck_helper_requires_exact_reviewed_bytes(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    function = _fenced_shell_function(runbook, "assert_server_side_diff_unchanged")
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(mode=0o700)
    manifest = evidence_dir / "reviewed.yaml"
    manifest.write_text("manifest\n", encoding="ascii")
    reviewed = evidence_dir / "reviewed.diff"
    reviewed.write_text("reviewed diff\n", encoding="ascii")
    reviewed.chmod(0o600)
    arguments = evidence_dir / "kubectl.arguments"
    program = (
        "set -euo pipefail\n"
        "umask 077\n"
        + function
        + "\n"
        + f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
        + f"kubeconfig={shlex.quote(str(tmp_path / 'kubeconfig'))}\n"
        + f"arguments={shlex.quote(str(arguments))}\n"
        + "kubectl() {\n"
        + '  printf \'%s\\n\' "$*" > "$arguments"\n'
        + "  printf '%s\\n' \"$fake_diff\"\n"
        + "  return 1\n"
        + "}\n"
        + f"manifest={shlex.quote(str(manifest))}\n"
        + f"reviewed={shlex.quote(str(reviewed))}\n"
        + "fake_diff='reviewed diff'\n"
        + 'assert_server_side_diff_unchanged "$manifest" "$reviewed" operational\n'
        + "fake_diff='changed diff'\n"
        + 'if assert_server_side_diff_unchanged "$manifest" "$reviewed" operational; '
        + "then exit 90; fi\n"
        + "printf 'drift-detected\\n'\n"
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "drift-detected\n"
    assert arguments.read_text() == (
        f"--kubeconfig {tmp_path / 'kubeconfig'} diff --server-side "
        f"--field-manager=loom-personal-dev-control-plane -f {manifest}\n"
    )


@pytest.mark.parametrize(
    ("launch_server", "expected_returncode"),
    [
        ("https://loom-service.dev.yylx.world", 0),
        ("https://unrelated.example", 1),
    ],
)
def test_multi_owner_launch_smoke_uses_exact_cli_and_reviewed_server(
    tmp_path: Path,
    launch_server: str,
    expected_returncode: int,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    origin_function = _fenced_shell_function(runbook, "reviewed_public_origin")
    cli_function = _fenced_shell_function(runbook, "launch_owner_cli")
    server_function = _fenced_shell_function(runbook, "assert_launch_owner_server")
    exact_marker = tmp_path / "exact-cli.marker"
    bare_marker = tmp_path / "bare-cli.marker"
    exact_cli = tmp_path / "reviewed-loom"
    exact_cli.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            test "$XDG_CONFIG_HOME" = "$EXPECTED_XDG"
            test "$*" = "auth whoami --format json"
            printf '%s|%s\\n' "$XDG_CONFIG_HOME" "$*" > "$EXACT_MARKER"
            printf '{"auth_kind":"bearer","credential_type":"user_owned_api_token","expires_at":null,"principal_type":"team","role":null,"scopes":["read:own","submit"],"server":"%s","team_id":"team","user_id":"user"}\\n' "$FAKE_SERVER"
            """
        ),
        encoding="ascii",
    )
    exact_cli.chmod(0o700)
    path_dir = tmp_path / "path"
    path_dir.mkdir()
    bare_cli = path_dir / "loom"
    bare_cli.write_text(
        "#!/usr/bin/env bash\nprintf '%s\\n' invoked > \"$BARE_MARKER\"\nexit 99\n",
        encoding="ascii",
    )
    bare_cli.chmod(0o700)
    launch_xdg = tmp_path / "xdg"
    launch_xdg.mkdir()
    launch_owner_whoami = tmp_path / "launch-owner.whoami.json"
    program = (
        "set -euo pipefail\n"
        + origin_function
        + "\n"
        + cli_function
        + "\n"
        + server_function
        + "\n"
        + f"python_cli={shlex.quote(sys.executable)}\n"
        + f"profile={shlex.quote(str(_ROOT / 'deploy/dev-fleet/personal-dev-control-plane.toml'))}\n"
        + f"loom_cli={shlex.quote(str(exact_cli))}\n"
        + f"launch_xdg_config_root={shlex.quote(str(launch_xdg))}\n"
        + f"launch_owner_whoami={shlex.quote(str(launch_owner_whoami))}\n"
        + 'reviewed_server="$(reviewed_public_origin "$profile")"\n'
        + 'launch_owner_cli auth whoami --format json | jq -cS . > "$launch_owner_whoami"\n'
        + "assert_launch_owner_server\n"
    )
    environment = os.environ.copy()
    environment.update(
        {
            "BARE_MARKER": str(bare_marker),
            "EXACT_MARKER": str(exact_marker),
            "EXPECTED_XDG": str(launch_xdg),
            "FAKE_SERVER": launch_server,
            "PATH": f"{path_dir}:{environment['PATH']}",
        }
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert result.returncode == expected_returncode, result.stderr
    assert exact_marker.read_text(encoding="ascii") == (f"{launch_xdg}|auth whoami --format json\n")
    assert not bare_marker.exists()
    if expected_returncode == 0:
        whoami = json.loads(launch_owner_whoami.read_text(encoding="ascii"))
        assert whoami["server"] == "https://loom-service.dev.yylx.world"


def test_multi_owner_launch_requires_a_new_canonical_evidence_directory(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-multi-owner-durable-launch.md")
    repository_function = _fenced_shell_function(runbook, "validated_repository_root")
    parent_function = _fenced_shell_function(
        runbook,
        "assert_owner_controlled_evidence_parent",
    )
    prepare_function = _fenced_shell_function(runbook, "prepare_new_evidence_dir")
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)

    def prepare(
        path: Path,
        *,
        cwd: Path = repo,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash"],
            input=(
                "set -euo pipefail\n"
                "umask 077\n"
                + repository_function
                + "\n"
                + parent_function
                + "\n"
                + prepare_function
                + "\n"
                + f"repo={shlex.quote(str(repo))}\n"
                + 'repo="$(validated_repository_root "$repo")"\n'
                + f"prepare_new_evidence_dir {shlex.quote(str(path))}\n"
            ),
            text=True,
            capture_output=True,
            check=False,
            cwd=cwd,
        )

    fresh = tmp_path / "fresh"
    fresh_result = prepare(fresh)
    assert fresh_result.returncode == 0, fresh_result.stderr
    assert fresh.is_dir()
    assert not fresh.is_symlink()
    assert stat.S_IMODE(fresh.stat().st_mode) == 0o700
    assert list(fresh.iterdir()) == []

    existing = tmp_path / "existing"
    existing.mkdir()
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    (nonempty / "stale.json").write_text("{}\n", encoding="ascii")
    symlink = tmp_path / "symlink"
    symlink.symlink_to(existing, target_is_directory=True)
    symlink_parent = tmp_path / "symlink-parent"
    symlink_parent.symlink_to(existing, target_is_directory=True)
    ignored_parent = repo / "ignored"
    ignored_parent.mkdir()
    writable_parent = tmp_path / "writable-parent"
    writable_parent.mkdir()
    writable_parent.chmod(0o777)
    writable_ancestor = tmp_path / "writable-ancestor"
    writable_ancestor.mkdir()
    writable_ancestor.chmod(0o777)
    controlled_parent = writable_ancestor / "controlled-parent"
    controlled_parent.mkdir(mode=0o700)

    try:
        unsafe_paths = (
            existing,
            nonempty,
            symlink,
            symlink_parent / "child",
            ignored_parent / "evidence",
            writable_parent / "child",
            controlled_parent / "child",
            tmp_path / "noncanonical-parent" / ".." / "noncanonical",
            Path("relative-evidence"),
        )
        for unsafe in unsafe_paths:
            assert prepare(unsafe).returncode != 0
        assert not (ignored_parent / "evidence").exists()
        assert not (writable_parent / "child").exists()
        assert not (controlled_parent / "child").exists()
    finally:
        writable_parent.chmod(0o700)
        writable_ancestor.chmod(0o700)


def test_concurrent_acceptance_requires_a_new_canonical_outside_evidence_directory(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-concurrent-owner-zero-capacity-acceptance.md")
    repository_function = _indented_shell_function(
        runbook,
        "validated_repository_root",
    )
    parent_function = _indented_shell_function(
        runbook,
        "assert_owner_controlled_evidence_parent",
    )
    prepare_function = _indented_shell_function(runbook, "prepare_new_evidence_dir")
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)

    def prepare(
        path: Path,
        *,
        cwd: Path = repo,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash"],
            input=(
                "set -euo pipefail\n"
                "umask 077\n"
                + repository_function
                + "\n"
                + parent_function
                + "\n"
                + prepare_function
                + "\n"
                + 'repo="$(validated_repository_root "$(pwd -P)")"\n'
                + f"prepare_new_evidence_dir {shlex.quote(str(path))}\n"
            ),
            text=True,
            capture_output=True,
            check=False,
            cwd=cwd,
        )

    fresh = tmp_path / "fresh"
    fresh_result = prepare(fresh)
    assert fresh_result.returncode == 0, fresh_result.stderr
    assert fresh.is_dir()
    assert not fresh.is_symlink()
    assert fresh.stat().st_uid == os.getuid()
    assert stat.S_IMODE(fresh.stat().st_mode) == 0o700
    assert list(fresh.iterdir()) == []

    existing = tmp_path / "existing"
    existing.mkdir()
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    (nonempty / "stale.json").write_text("{}\n", encoding="ascii")
    leaf_symlink = tmp_path / "leaf-symlink"
    leaf_symlink.symlink_to(existing, target_is_directory=True)
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    symlink_parent = tmp_path / "symlink-parent"
    symlink_parent.symlink_to(real_parent, target_is_directory=True)
    ignored_parent = repo / "ignored"
    ignored_parent.mkdir()
    repo_subdir = repo / "subdir"
    repo_subdir.mkdir()
    writable_parent = tmp_path / "writable-parent"
    writable_parent.mkdir()
    writable_parent.chmod(0o777)
    writable_ancestor = tmp_path / "writable-ancestor"
    writable_ancestor.mkdir()
    writable_ancestor.chmod(0o777)
    controlled_parent = writable_ancestor / "controlled-parent"
    controlled_parent.mkdir(mode=0o700)

    try:
        unsafe_paths = (
            existing,
            nonempty,
            leaf_symlink,
            symlink_parent / "child",
            repo / "contained",
            ignored_parent / "evidence",
            real_parent / ".." / "noncanonical",
            writable_parent / "child",
            controlled_parent / "child",
            Path("relative-evidence"),
        )
        for unsafe in unsafe_paths:
            assert prepare(unsafe).returncode != 0
        from_subdir = ignored_parent / "from-subdir"
        assert prepare(from_subdir, cwd=repo_subdir).returncode != 0
        assert not from_subdir.exists()
        assert not (repo / "contained").exists()
        assert not (ignored_parent / "evidence").exists()
        assert not (writable_parent / "child").exists()
        assert not (controlled_parent / "child").exists()
    finally:
        writable_parent.chmod(0o700)
        writable_ancestor.chmod(0o700)


def test_acceptance_evidence_is_source_derived_and_bound_to_every_command() -> None:
    runbook = _read("docs/runbooks/personal-dev-zero-capacity-acceptance.md")

    assert "render-trusted-launcher-profile" in runbook
    assert "render-scanner-finding-policy" in runbook
    assert "acceptance_evidence_args=(" in runbook
    assert '--source-root "$repo"' in runbook
    assert '--trusted-launcher-profile-file "$trusted_launcher_profile"' in runbook
    assert '--scanner-finding-policy-file "$scanner_finding_policy"' in runbook
    assert '--backup-restore-evidence-file "$backup_restore_evidence"' in runbook
    assert runbook.count('"${acceptance_evidence_args[@]}"') == 4
    assert "<absolute-owner-only-trusted-launcher-profile>" not in runbook
    assert "<absolute-owner-only-scanner-finding-policy>" not in runbook


def test_backup_restore_runbook_performs_complete_isolated_api_readback(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-backup-restore-evidence.md")
    runbook_index = _read("docs/runbooks/README.md")
    fleet = _read("deploy/dev-fleet/README.md")
    architecture = _read("docs/architecture/personal-dev-management-plane-deployment.md")

    for document in (runbook_index, fleet, architecture):
        assert "personal-dev-backup-restore-evidence.md" in document
    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "trap cleanup_restore_resources EXIT" in runbook
    assert "trap - EXIT" in runbook
    assert 'rm -f "$restore_env"' in runbook
    assert 'test ! -e "$restore_env"' in runbook
    assert 'docker run --detach --network none --name "$postgres_restore"' in runbook
    assert '"$postgres_image"' in runbook
    assert "pg_restore -U postgres -d postgres" in runbook
    assert 'cmp -s "$postgres_source_state" "$postgres_restored_state"' in runbook
    assert 'docker network create --internal "$restore_network"' in runbook
    assert "--network-alias minio-restore" in runbook
    assert '"$minio_image" server /data' in runbook
    assert '--trusted-release-file "$trusted_release"' in runbook
    assert '--trusted-release-sha256 "$trusted_release_sha256"' in runbook
    assert "capture-minio-backup" in runbook
    assert "restore-minio-backup" in runbook
    assert "ready restore" in runbook
    assert runbook.index("ready restore") < runbook.index(
        '"$loom_cli" admin personal-dev-control-plane restore-minio-backup'
    )
    assert '"$minio_client_image" -euc' in runbook
    assert '--payload-root "$minio_payload_root"' in runbook
    assert '--source-manifest-file "$minio_source_manifest"' in runbook
    assert '--restored-manifest-file "$minio_restored_manifest"' in runbook
    assert '--minio-payload-root "$minio_payload_root"' in runbook
    assert runbook.index("capture-minio-backup") > runbook.index(
        '> "$evidence_dir/storage-inventory.json"'
    )
    assert runbook.index("cleanup_restore_resources\ntrap - EXIT") < runbook.index(
        "render-backup-restore-evidence"
    )

    loom_cli = tmp_path / "loom"
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    variables = {
        "loom_cli": loom_cli,
        "kubeconfig": tmp_path / "kubeconfig",
        "trusted_release": tmp_path / "trusted-release.json",
        "trusted_release_sha256": "a" * 64,
        "minio_source_manifest": evidence_dir / "minio.source-manifest.json",
        "minio_payload_root": evidence_dir / "minio" / "payloads",
        "minio_restored_manifest": evidence_dir / "minio.restored-manifest.json",
        "restore_env": evidence_dir / "minio-restore.env",
        "minio_restore": "loom-personal-dev-minio-restore-aaaaaaaaaaaa",
        "restore_network": "loom-personal-dev-restore-aaaaaaaaaaaa",
        "evidence_dir": evidence_dir,
        "profile": tmp_path / "profile.toml",
        "started_at": "2026-08-29T00:00:00Z",
        "completed_at": "2026-08-29T00:01:00Z",
        "postgres_dump": evidence_dir / "postgres.dump",
        "postgres_source_state": evidence_dir / "postgres.source.tsv",
        "postgres_restored_state": evidence_dir / "postgres.restored.tsv",
        "source_schema_head": "0112",
        "restored_schema_head": "0112",
        "secret_inventory": evidence_dir / "secret-key-inventory.json",
        "postgres_restore": "loom-personal-dev-pg-restore-aaaaaaaaaaaa",
        "result_tmp": evidence_dir / "backup-restore.json",
        "result_render_evidence": evidence_dir / "backup-restore.render.json",
    }

    def assert_documented_invocation(
        command: str,
        output: str,
        operation: str,
        expected_arguments: tuple[str, ...],
    ) -> None:
        invocation = _shell_command_block(runbook, command, output)
        arguments = tmp_path / f"{len(expected_arguments)}-arguments"
        loom_cli.write_text(
            f"#!/bin/sh\nprintf '%s\\n' \"$@\" > {shlex.quote(str(arguments))}\n",
            encoding="utf-8",
        )
        loom_cli.chmod(0o700)
        program = "set -euo pipefail\n" + "\n".join(
            f"{name}={shlex.quote(str(value))}" for name, value in variables.items()
        )
        result = subprocess.run(
            ["bash", "-c", f"{program}\n{invocation}\n"],
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        captured = tuple(arguments.read_text(encoding="utf-8").splitlines())
        assert captured[:3] == ("admin", "personal-dev-control-plane", operation)
        for index in range(0, len(expected_arguments), 2):
            option = expected_arguments[index]
            assert captured[captured.index(option) : captured.index(option) + 2] == (
                option,
                expected_arguments[index + 1],
            )

    assert_documented_invocation(
        '"$loom_cli" admin personal-dev-control-plane capture-minio-backup',
        '> "$evidence_dir/minio.capture.json"',
        "capture-minio-backup",
        (
            "--source-manifest-file",
            str(variables["minio_source_manifest"]),
            "--payload-root",
            str(variables["minio_payload_root"]),
        ),
    )
    assert_documented_invocation(
        '"$loom_cli" admin personal-dev-control-plane restore-minio-backup',
        '> "$evidence_dir/minio.restore.json"',
        "restore-minio-backup",
        (
            "--source-manifest-file",
            str(variables["minio_source_manifest"]),
            "--payload-root",
            str(variables["minio_payload_root"]),
            "--restored-manifest-file",
            str(variables["minio_restored_manifest"]),
        ),
    )
    assert_documented_invocation(
        '"$loom_cli" admin personal-dev-control-plane render-backup-restore-evidence',
        '> "$result_tmp" 2> "$result_render_evidence"',
        "render-backup-restore-evidence",
        (
            "--minio-payload-root",
            str(variables["minio_payload_root"]),
        ),
    )

    assert 'mc_live cat "local/$bucket/$key"' not in runbook
    assert "mc_live()" not in runbook
    assert "mc_restore()" not in runbook
    assert "test ! -s" not in runbook
    assert "$bucket" not in runbook
    assert "$key" not in runbook
    assert 'mc_restore pipe "restore/$bucket/$key"' not in runbook
    assert 'mc_restore cat "restore/$bucket/$key"' not in runbook
    assert 'cmp -s "$minio_source_manifest" "$minio_restored_manifest"' in runbook
    assert "render-backup-restore-evidence" in runbook
    assert '--pre-shadow-status-file "$evidence_dir/pre-backup.shadow-status.json"' in runbook
    assert '--post-shadow-status-file "$evidence_dir/post-restore.shadow-status.json"' in runbook
    assert '--storage-inventory-file "$evidence_dir/storage-inventory.json"' in runbook
    assert '--isolated-postgres-name "$postgres_restore"' in runbook
    assert '--isolated-minio-name "$minio_restore"' in runbook
    assert '--isolated-network-name "$restore_network"' in runbook
    assert "Use the identical bucket/object loop" not in runbook
    assert 'test -f "$minio_restored_manifest"' not in runbook
    assert "kubectl cp" not in runbook.casefold()


def test_backup_restore_runbook_fenced_bash_is_syntactically_valid() -> None:
    runbook = _read("docs/runbooks/personal-dev-backup-restore-evidence.md")
    assert "```bash\n```bash" not in runbook
    syntax = subprocess.run(
        ["bash", "-n"],
        input=_fenced_bash(runbook),
        text=True,
        capture_output=True,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stderr


@pytest.mark.parametrize(
    "runbook_path",
    (
        "docs/runbooks/personal-dev-backup-restore-evidence.md",
        "docs/runbooks/personal-dev-schema-transition.md",
    ),
)
def test_isolated_postgres_waits_for_final_pid1_before_readiness(
    tmp_path: Path,
    runbook_path: str,
) -> None:
    runbook = _read(runbook_path)
    function = _fenced_shell_function(runbook, "wait_for_final_postgres")
    call_log = tmp_path / "calls.txt"
    program = (
        "set -euo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        "pid1_checks=0\n"
        "sleep() { :; }\n"
        "docker() {\n"
        "  test \"$1\" = exec || return 97\n"
        "  case \"$*\" in\n"
        "    *'read -r comm </proc/1/comm; test \"$comm\" = postgres'*)\n"
        "      pid1_checks=$((pid1_checks + 1))\n"
        "      if test \"$pid1_checks\" -eq 1; then\n"
        "        printf 'bootstrap-pid1\\n' >> \"$call_log\"\n"
        "        return 1\n"
        "      fi\n"
        "      printf 'final-pid1\\n' >> \"$call_log\"\n"
        "      return 0\n"
        "      ;;\n"
        "    *pg_isready*)\n"
        "      printf 'pg-isready\\n' >> \"$call_log\"\n"
        "      test \"$pid1_checks\" -ge 2\n"
        "      ;;\n"
        "    *) return 98 ;;\n"
        "  esac\n"
        "}\n"
        f"{function}\n"
        "wait_for_final_postgres isolated-postgres\n"
    )

    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert call_log.read_text(encoding="utf-8").splitlines() == [
        "bootstrap-pid1",
        "final-pid1",
        "pg-isready",
    ]


def test_backup_restore_schema_head_is_bound_to_selected_source_and_graph(
    tmp_path: Path,
) -> None:
    result = _run_backup_schema_head_resolver(
        tmp_path,
        heads=("future_schema_head",),
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "future_schema_head\n"


def test_backup_restore_binds_every_python_entrypoint_to_selected_source(
    tmp_path: Path,
) -> None:
    result, expected_module = _run_backup_selected_source_binding(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout == f"{expected_module}\n"


@pytest.mark.parametrize(
    ("heads", "error"),
    (
        ((), "expected exactly one service schema head"),
        (("head_one", "head_two"), "expected exactly one service schema head"),
        (("",), "expected one valid service schema head"),
        (("invalid head",), "expected one valid service schema head"),
        ((123,), "expected one valid service schema head"),
    ),
)
def test_backup_restore_schema_head_fails_closed(
    tmp_path: Path,
    heads: tuple[object, ...],
    error: str,
) -> None:
    result = _run_backup_schema_head_resolver(tmp_path, heads=heads)

    assert result.returncode != 0
    assert error in result.stderr


def test_backup_restore_schema_head_rejects_wrong_loaded_module(
    tmp_path: Path,
) -> None:
    result = _run_backup_schema_head_resolver(
        tmp_path,
        heads=("future_schema_head",),
        schema_module_override=tmp_path / "outside" / "schema_startup.py",
    )

    assert result.returncode != 0
    assert "selected repository source" in result.stderr


@pytest.mark.parametrize("phase", ("live", "restored"))
def test_backup_restore_compares_each_database_to_source_derived_head(
    tmp_path: Path,
    phase: str,
) -> None:
    matching = _run_backup_schema_head_comparison(
        tmp_path / "matching",
        phase=phase,
        observed_head="future_schema_head",
    )
    stale = _run_backup_schema_head_comparison(
        tmp_path / "stale",
        phase=phase,
        observed_head="stale_editable_head",
    )

    assert matching.returncode == 0, matching.stderr
    assert stale.returncode != 0


def test_schema_transition_runbook_is_executable_and_discoverable() -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    runbook_index = _read("docs/runbooks/README.md")
    shadow_runbook = _read("docs/runbooks/personal-dev-management-plane-shadow.md")
    architecture = _read("docs/architecture/personal-dev-management-plane-deployment.md")

    for document in (runbook_index, shadow_runbook, architecture):
        assert "personal-dev-schema-transition.md" in document
    assert "~~~" not in runbook
    assert "Use capture_live_postgres_state" not in runbook
    assert "Then stream the reviewed dump" not in runbook
    shell = _fenced_bash(runbook)
    syntax = subprocess.run(
        ["bash", "-n"],
        input=shell,
        text=True,
        capture_output=True,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stderr
    for name in (
        "capture_docker_postgres_state",
        "capture_live_postgres_state",
        "prepare_transition_evidence_dir",
        "assert_transition_artifacts",
        "assert_predecessor_restore_artifacts",
        "restore_rehearsal_predecessor",
        "restore_predecessor_database",
        "quiesce_management",
        "apply_target_transition",
        "restore_predecessor",
        "run_schema_transition",
    ):
        assert f"{name}() {{" in shell


def test_schema_transition_predecessor_head_uses_selected_checkout(
    tmp_path: Path,
) -> None:
    result = _run_schema_transition_predecessor_checkout_head(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "selected_predecessor_head\n"


def test_schema_transition_predecessor_head_rejects_wrong_loaded_module(
    tmp_path: Path,
) -> None:
    result = _run_schema_transition_predecessor_checkout_head(
        tmp_path,
        schema_module_override=tmp_path / "outside" / "schema_startup.py",
    )

    assert result.returncode != 0
    assert "predecessor source binding failed" in result.stderr


def test_schema_transition_predecessor_head_ignores_repository_bytecode(
    tmp_path: Path,
) -> None:
    result = _run_schema_transition_predecessor_checkout_head(
        tmp_path,
        malicious_bytecode_head="malicious_unchecked_head",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "selected_predecessor_head\n"


def test_schema_transition_rehearsal_exports_explicit_postgres_identity(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "start_rehearsal_postgres")
    call_log = tmp_path / "docker-run-arguments.bin"
    program = (
        "set -euo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        "rehearsal_network=rehearsal-network\n"
        "rehearsal_postgres=rehearsal-postgres\n"
        "postgres_image=postgres-image\n"
        "docker() {\n"
        '  test "$1" = run\n'
        '  printf \'%s\\0\' "$@" > "$call_log"\n'
        "}\n"
        "wait_for_final_postgres() { :; }\n"
        f"{function}\n"
        "start_rehearsal_postgres\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    arguments = call_log.read_bytes().split(b"\0")
    assert arguments[0] == b"run"
    assert arguments[-2:] == [b"postgres-image", b""]
    environment = [
        arguments[index + 1]
        for index, argument in enumerate(arguments)
        if argument == b"--env"
    ]
    assert len(environment) == 3
    assert set(environment) == {
        b"POSTGRES_HOST_AUTH_METHOD=trust",
        b"POSTGRES_USER=postgres",
        b"POSTGRES_DB=postgres",
    }


@pytest.mark.parametrize(
    ("function_name", "command_name"),
    (
        ("restore_rehearsal_predecessor", "docker"),
        ("restore_predecessor_database", "kubectl"),
    ),
)
def test_schema_transition_restore_recreates_database_before_loading_archive(
    tmp_path: Path,
    function_name: str,
    command_name: str,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, function_name)
    call_log = tmp_path / "calls.txt"
    dump = tmp_path / "postgres.dump"
    dump.write_bytes(b"opaque dump")
    program = (
        "set -uo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        f"postgres_dump={shlex.quote(str(dump))}\n"
        "rehearsal_postgres=rehearsal-postgres\n"
        "postgres_pod=live-postgres\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"{command_name}() {{\n"
        '  case "$*" in\n'
        '    *dropdb*createdb*) printf "reset\\n" >> "$call_log" ;;\n'
        '    *pg_restore*) printf "pg_restore\\n" >> "$call_log" ;;\n'
        "    *) return 1 ;;\n"
        "  esac\n"
        "}\n"
        f"{function}\n"
        f"{function_name}\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = call_log.read_text(encoding="utf-8").splitlines()
    assert calls == ["reset", "pg_restore"]
    assert runbook.count("start_rehearsal_postgres") == 2
    assert runbook.count('docker rm --force "$rehearsal_postgres"') == 1
    assert runbook.count("restore_rehearsal_predecessor") == 2


def test_schema_transition_evidence_directory_must_be_new_absolute_and_owner_only(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "prepare_transition_evidence_dir")
    target_repo = tmp_path / "target-repo"
    predecessor_repo = tmp_path / "predecessor-repo"
    target_repo.mkdir()
    predecessor_repo.mkdir()
    bindings = (
        f"repo={shlex.quote(str(target_repo))}\n"
        f"predecessor_repo={shlex.quote(str(predecessor_repo))}\n"
    )
    valid = tmp_path / "valid"
    valid_result = subprocess.run(
        ["bash"],
        input=(
            f"set -euo pipefail\n{bindings}{function}\n"
            f"prepare_transition_evidence_dir {shlex.quote(str(valid))}\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert valid_result.returncode == 0, valid_result.stderr
    assert valid.is_dir()
    assert stat.S_IMODE(valid.stat().st_mode) == 0o700

    existing = tmp_path / "existing"
    existing.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    for invalid in (
        Path("relative"),
        existing,
        linked,
        target_repo / "evidence",
        predecessor_repo / "evidence",
    ):
        result = subprocess.run(
            ["bash"],
            input=(
                "set -euo pipefail\n"
                f"{bindings}"
                f"{function}\n"
                f"prepare_transition_evidence_dir {shlex.quote(str(invalid))}\n"
            ),
            cwd=tmp_path,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode != 0


def test_schema_transition_evidence_directory_propagates_inventory_failure(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "prepare_transition_evidence_dir")
    target_repo = tmp_path / "target-repo"
    predecessor_repo = tmp_path / "predecessor-repo"
    target_repo.mkdir()
    predecessor_repo.mkdir()
    evidence = tmp_path / "evidence"
    program = (
        "set -uo pipefail\n"
        f"repo={shlex.quote(str(target_repo))}\n"
        f"predecessor_repo={shlex.quote(str(predecessor_repo))}\n"
        "find() { return 23; }\n"
        f"{function}\n"
        f"prepare_transition_evidence_dir {shlex.quote(str(evidence))}\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def _write_schema_transition_git_fixture(root: Path) -> tuple[str, str]:
    root.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    (root / "tracked.txt").write_text("reviewed\n", encoding="ascii")
    subprocess.run(["git", "-C", str(root), "add", "tracked.txt"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Loom tests",
            "-c",
            "user.email=loom-tests@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return commit, tree


def _write_owner_only_artifact(path: Path, payload: bytes) -> str:
    path.write_bytes(payload)
    path.chmod(0o600)
    return hashlib.sha256(payload).hexdigest()


def _schema_transition_artifact_program(
    tmp_path: Path,
) -> tuple[str, dict[str, Path]]:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_exact_source_repository",
            "assert_transition_artifacts",
            "assert_predecessor_recovery_artifacts",
        )
    )
    target_repo = tmp_path / "target-repo"
    predecessor_repo = tmp_path / "predecessor-repo"
    target_commit, target_tree = _write_schema_transition_git_fixture(target_repo)
    predecessor_commit, predecessor_tree = _write_schema_transition_git_fixture(predecessor_repo)
    artifacts = {
        name: tmp_path / name
        for name in (
            "trusted-release.json",
            "predecessor-release.json",
            "predecessor-shadow.yaml",
            "backup-evidence.json",
            "transition-plan.json",
            "transition-job.json",
            "target-shadow.yaml",
        )
    }
    payloads = {name: f"{name}\n".encode("ascii") for name in artifacts}
    payloads["predecessor-release.json"] = json.dumps(
        {"source_sha": predecessor_commit, "source_tree": predecessor_tree},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    payloads["backup-evidence.json"] = b'{"postgres":{"source_schema_head":"0112"}}'
    digests = {
        name: _write_owner_only_artifact(path, payloads[name]) for name, path in artifacts.items()
    }
    variables = {
        "repo": target_repo,
        "predecessor_repo": predecessor_repo,
        "trusted_release": artifacts["trusted-release.json"],
        "predecessor_release": artifacts["predecessor-release.json"],
        "predecessor_shadow": artifacts["predecessor-shadow.yaml"],
        "backup_evidence": artifacts["backup-evidence.json"],
        "transition_plan": artifacts["transition-plan.json"],
        "transition_job": artifacts["transition-job.json"],
        "target_shadow": artifacts["target-shadow.yaml"],
    }
    assignments = "\n".join(
        f"{name}={shlex.quote(str(value))}" for name, value in variables.items()
    )
    assignments += "\n" + "\n".join(
        (
            f"target_source_commit={target_commit}",
            f"target_source_tree={target_tree}",
            f"predecessor_source_commit={predecessor_commit}",
            f"predecessor_source_tree={predecessor_tree}",
            "predecessor_head=0112",
            f"trusted_release_sha256={digests['trusted-release.json']}",
            f"predecessor_release_sha256={digests['predecessor-release.json']}",
            f"predecessor_shadow_sha256={digests['predecessor-shadow.yaml']}",
            f"backup_evidence_sha256={digests['backup-evidence.json']}",
            f"plan_sha256={digests['transition-plan.json']}",
            f"migration_job_sha256={digests['transition-job.json']}",
            f"target_shadow_sha256={digests['target-shadow.yaml']}",
        )
    )
    pinned = "\n".join(
        (
            'exec 30< "$trusted_release"',
            'exec 31< "$predecessor_release"',
            'exec 32< "$predecessor_shadow"',
            'exec 33< "$backup_evidence"',
            'exec 37< "$transition_job"',
            'exec 38< "$transition_plan"',
            'exec 39< "$target_shadow"',
        )
    )
    return f"set -uo pipefail\n{functions}\n{assignments}\n{pinned}\n", artifacts


@pytest.mark.parametrize(
    "artifact_name",
    (
        "trusted-release.json",
        "predecessor-release.json",
        "predecessor-shadow.yaml",
        "backup-evidence.json",
        "transition-plan.json",
        "transition-job.json",
        "target-shadow.yaml",
    ),
)
def test_schema_transition_revalidates_every_bound_artifact(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    program, artifacts = _schema_transition_artifact_program(tmp_path)
    valid = subprocess.run(
        ["bash"],
        input=program + "assert_transition_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert valid.returncode == 0, valid.stderr

    artifacts[artifact_name].write_bytes(b"tampered\n")
    artifacts[artifact_name].chmod(0o600)
    tampered = subprocess.run(
        ["bash"],
        input=program + "assert_transition_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert tampered.returncode != 0


def test_schema_transition_target_artifact_drift_cannot_block_recovery_validation(
    tmp_path: Path,
) -> None:
    program, artifacts = _schema_transition_artifact_program(tmp_path)
    target_repo = tmp_path / "target-repo"
    (target_repo / "untracked-target-drift.txt").write_text("target drift\n", encoding="ascii")
    for artifact_name in (
        "trusted-release.json",
        "transition-job.json",
        "transition-plan.json",
        "target-shadow.yaml",
    ):
        artifacts[artifact_name].write_bytes(b"target artifact drift\n")
        artifacts[artifact_name].chmod(0o600)

    result = subprocess.run(
        ["/bin/bash"],
        input=(
            program
            + "assert_predecessor_recovery_artifacts || exit 41\n"
            + "if assert_transition_artifacts; then exit 42; fi\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_schema_transition_pinned_artifacts_are_inherited_by_external_consumers(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_open_owner_only_sha256",
        )
    )
    pin_start = runbook.index('trusted_release_source="$trusted_release"')
    pin_end_marker = "transition_plan=/proc/self/fd/38"
    pin_end = runbook.index(pin_end_marker, pin_start) + len(pin_end_marker)
    primary_pin_block = runbook[pin_start:pin_end]
    target_start = runbook.index('target_shadow_source="$target_shadow"')
    target_end_marker = "target_shadow=/proc/self/fd/39"
    target_end = runbook.index(target_end_marker, target_start) + len(target_end_marker)
    target_pin_block = runbook[target_start:target_end]
    artifact_variables = (
        "trusted_release",
        "predecessor_release",
        "predecessor_shadow",
        "backup_evidence",
        "postgres_dump",
        "postgres_source_state",
        "kubeconfig",
        "transition_job",
        "transition_plan",
        "target_shadow",
    )
    digest_variables = (
        "trusted_release_sha256",
        "predecessor_release_sha256",
        "predecessor_shadow_sha256",
        "backup_evidence_sha256",
        "postgres_dump_sha256",
        "postgres_state_sha256",
        "kubeconfig_sha256",
        "migration_job_sha256",
        "plan_sha256",
        "target_shadow_sha256",
    )
    assignments: list[str] = []
    child_arguments: list[str] = []
    source_variables: list[str] = []
    for index, (artifact_variable, digest_variable) in enumerate(
        zip(artifact_variables, digest_variables, strict=True),
        start=1,
    ):
        path = tmp_path / f"artifact-{index}"
        digest = _write_owner_only_artifact(
            path,
            f"reviewed artifact {index}\n".encode("ascii"),
        )
        assignments.extend(
            (
                f"{artifact_variable}={shlex.quote(str(path))}",
                f"{digest_variable}={digest}",
            )
        )
        child_arguments.extend((f'"${artifact_variable}"', f'"${digest_variable}"'))
        source_variables.append(f'"${artifact_variable}_source"')
    program = (
        "set -euo pipefail\n"
        f"python_cli={shlex.quote(sys.executable)}\n"
        f"{functions}\n"
        + "\n".join(assignments)
        + "\n"
        + primary_pin_block
        + "\n"
        + target_pin_block
        + "\n"
        + "for source_path in "
        + " ".join(source_variables)
        + "; do\n"
        + '  replacement="$source_path.replacement"\n'
        + "  printf 'pathname replacement\\n' > \"$replacement\"\n"
        + '  /usr/bin/chmod 0600 "$replacement"\n'
        + '  /usr/bin/mv -f -- "$replacement" "$source_path"\n'
        + "done\n"
        + '"$python_cli" - '
        + " ".join(child_arguments)
        + " <<'PY'\n"
        + "import hashlib\n"
        + "import sys\n"
        + "for path, expected in zip(sys.argv[1::2], sys.argv[2::2], strict=True):\n"
        + "    if not path.startswith('/proc/self/fd/'):\n"
        + "        raise SystemExit(f'unpinned path: {path}')\n"
        + "    if hashlib.sha256(open(path, 'rb').read()).hexdigest() != expected:\n"
        + "        raise SystemExit(f'descriptor drift: {path}')\n"
        + "PY\n"
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_schema_transition_rejects_source_drift_and_unsafe_artifact_metadata(
    tmp_path: Path,
) -> None:
    dirty_program, dirty_artifacts = _schema_transition_artifact_program(tmp_path / "dirty")
    target_repo = tmp_path / "dirty" / "target-repo"
    (target_repo / "untracked.txt").write_text("drift\n", encoding="ascii")
    dirty = subprocess.run(
        ["bash"],
        input=dirty_program + "assert_transition_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert dirty.returncode != 0
    assert dirty_artifacts

    mode_program, mode_artifacts = _schema_transition_artifact_program(tmp_path / "mode")
    mode_artifacts["transition-job.json"].chmod(0o644)
    unsafe_mode = subprocess.run(
        ["bash"],
        input=mode_program + "assert_transition_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert unsafe_mode.returncode != 0

    link_program, link_artifacts = _schema_transition_artifact_program(tmp_path / "link")
    os.link(link_artifacts["target-shadow.yaml"], tmp_path / "link" / "second-link")
    unsafe_link = subprocess.run(
        ["bash"],
        input=link_program + "assert_transition_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert unsafe_link.returncode != 0


@pytest.mark.parametrize("index_option", ("--skip-worktree", "--assume-unchanged"))
def test_schema_transition_revalidation_rejects_hidden_index_state(
    tmp_path: Path,
    index_option: str,
) -> None:
    program, _ = _schema_transition_artifact_program(tmp_path)
    target_repo = tmp_path / "target-repo"
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(target_repo),
            "update-index",
            index_option,
            "tracked.txt",
        ],
        check=True,
    )
    (target_repo / "tracked.txt").write_text("hidden drift\n", encoding="ascii")

    result = subprocess.run(
        ["/bin/bash"],
        input=program + "assert_transition_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_schema_transition_source_revalidation_ignores_caller_git_environment(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_exact_source_repository")
    repository = tmp_path / "repo"
    commit, tree = _write_schema_transition_git_fixture(repository)
    program = (
        "set -euo pipefail\n"
        f"repository={shlex.quote(str(repository))}\n"
        "export PATH=/definitely/missing\n"
        f"export GIT_DIR={shlex.quote(str(tmp_path / 'wrong-git-dir'))}\n"
        f"export GIT_INDEX_FILE={shlex.quote(str(tmp_path / 'wrong-index'))}\n"
        f"export GIT_WORK_TREE={shlex.quote(str(tmp_path / 'wrong-work-tree'))}\n"
        f"{function}\n"
        f'assert_exact_source_repository "$repository" {commit} {tree}\n'
    )
    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_schema_transition_source_revalidation_does_not_execute_clean_filter(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_exact_source_repository")
    repository = tmp_path / "repo"
    _write_schema_transition_git_fixture(repository)
    marker = tmp_path / "filter-executed"
    (repository / ".gitattributes").write_text("tracked.txt filter=side-effect\n", encoding="ascii")
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(repository),
            "config",
            "filter.side-effect.clean",
            f"/bin/sh -c '/usr/bin/touch {marker}; /bin/cat'",
        ],
        check=True,
    )
    subprocess.run(
        ["/usr/bin/git", "-C", str(repository), "add", ".gitattributes"],
        check=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(repository),
            "-c",
            "user.name=Loom tests",
            "-c",
            "user.email=loom-tests@example.invalid",
            "commit",
            "-qm",
            "configure clean filter",
        ],
        check=True,
    )
    marker.unlink(missing_ok=True)
    tracked = repository / "tracked.txt"
    metadata = tracked.stat()
    os.utime(tracked, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000))
    commit = subprocess.run(
        ["/usr/bin/git", "-C", str(repository), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["/usr/bin/git", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    program = (
        "set -euo pipefail\n"
        f"repository={shlex.quote(str(repository))}\n"
        f"{function}\n"
        f'assert_exact_source_repository "$repository" {commit} {tree}\n'
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert not marker.exists()


def test_schema_transition_source_revalidation_rejects_non_repository(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_exact_source_repository")
    repository = tmp_path / "repo"
    repository.mkdir()
    program = (
        "set -uo pipefail\n"
        f"repository={shlex.quote(str(repository))}\n"
        f"{function}\n"
        'assert_exact_source_repository "$repository" commit tree\n'
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


@pytest.mark.parametrize("artifact_name", ("postgres.dump", "postgres.state.tsv"))
def test_schema_transition_revalidates_restore_artifacts_at_restore_boundaries(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_predecessor_restore_artifacts",
        )
    )
    dump = tmp_path / "postgres.dump"
    state = tmp_path / "postgres.state.tsv"
    dump_sha256 = _write_owner_only_artifact(dump, b"opaque dump\n")
    state_sha256 = _write_owner_only_artifact(state, b"opaque state\n")
    program = (
        "set -uo pipefail\n"
        f"{functions}\n"
        f"postgres_dump={shlex.quote(str(dump))}\n"
        f"postgres_source_state={shlex.quote(str(state))}\n"
        f"postgres_dump_sha256={dump_sha256}\n"
        f"postgres_state_sha256={state_sha256}\n"
        'exec 34< "$postgres_dump"\n'
        'exec 35< "$postgres_source_state"\n'
    )
    valid = subprocess.run(
        ["bash"],
        input=program + "assert_predecessor_restore_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert valid.returncode == 0, valid.stderr

    (tmp_path / artifact_name).write_bytes(b"tampered\n")
    (tmp_path / artifact_name).chmod(0o600)
    tampered = subprocess.run(
        ["bash"],
        input=program + "assert_predecessor_restore_artifacts\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert tampered.returncode != 0


def test_schema_transition_success_orders_quiesce_migration_and_target(
    tmp_path: Path,
) -> None:
    result, calls = _run_schema_transition_orchestration(tmp_path)

    assert result.returncode == 0, result.stderr
    assert calls == [
        "assert_transition_interlocks",
        "assert_predecessor_restore_artifacts",
        "assert_live_predecessor_state",
        "scale_management_to_zero",
        "wait_for_no_management_pods",
        "assert_live_predecessor_state",
        "assert_transition_interlocks",
        "assert_migration_job_absent",
        "apply_reviewed_migration_job",
        "wait_for_reviewed_migration_job",
        "assert_live_target_head",
        "assert_transition_interlocks",
        "apply_reviewed_target_shadow",
        "wait_for_target_shadow",
        "assert_target_shadow_ready",
        "assert_transition_interlocks",
    ]


def test_schema_transition_failure_restores_before_predecessor_restart(
    tmp_path: Path,
) -> None:
    result, calls = _run_schema_transition_orchestration(
        tmp_path,
        fail_at="wait_for_reviewed_migration_job",
    )

    assert result.returncode == 1
    assert calls[-17:] == [
        "assert_predecessor_recovery_interlocks",
        "scale_management_to_zero",
        "wait_for_no_management_pods",
        "assert_predecessor_recovery_interlocks",
        "stop_target_migration",
        "assert_predecessor_recovery_interlocks",
        "assert_predecessor_restore_artifacts",
        "assert_only_restore_connection",
        "restore_predecessor_database",
        "assert_predecessor_restore_artifacts",
        "assert_live_predecessor_state",
        "assert_predecessor_recovery_interlocks",
        "apply_reviewed_predecessor_shadow",
        "remove_forward_only_web",
        "wait_for_predecessor_shadow",
        "assert_predecessor_shadow_ready",
        "assert_predecessor_recovery_interlocks",
    ]
    restore = calls.index("restore_predecessor_database")
    verify = calls.index("assert_live_predecessor_state", restore)
    restart = calls.index("apply_reviewed_predecessor_shadow")
    assert restore < verify < restart


def test_schema_transition_recovery_validates_boundary_before_every_mutation_phase(
    tmp_path: Path,
) -> None:
    result, calls = _run_schema_transition_orchestration(
        tmp_path,
        fail_at="wait_for_reviewed_migration_job",
    )

    assert result.returncode == 1
    failure = calls.index("wait_for_reviewed_migration_job")
    recovery = calls[failure + 1 :]
    assert recovery[:6] == [
        "assert_predecessor_recovery_interlocks",
        "scale_management_to_zero",
        "wait_for_no_management_pods",
        "assert_predecessor_recovery_interlocks",
        "stop_target_migration",
        "assert_predecessor_recovery_interlocks",
    ]
    assert recovery.index("assert_predecessor_recovery_interlocks") < recovery.index(
        "scale_management_to_zero"
    )
    assert recovery.index("stop_target_migration") < recovery.index("restore_predecessor_database")
    assert recovery[recovery.index("stop_target_migration") + 1] == (
        "assert_predecessor_recovery_interlocks"
    )


@pytest.mark.parametrize(
    ("rollback_deletions", "expected_returncode", "expected_delete"),
    [
        ([], 0, False),
        (
            [
                "deployment.apps/loom-personal-dev-web",
                "networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress",
                "service/loom-personal-dev-web",
            ],
            0,
            True,
        ),
        (["deployment.apps/loom-personal-dev-web"], 1, False),
    ],
)
def test_schema_transition_rollback_removes_only_planned_forward_web_resources(
    tmp_path: Path,
    rollback_deletions: list[str],
    expected_returncode: int,
    expected_delete: bool,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    start = runbook.index("\nremove_forward_only_web() {") + 1
    end = runbook.index("\n}\nwait_for_predecessor_shadow() {", start) + 2
    function = runbook[start:end]
    call_log = tmp_path / "calls.txt"
    transition_plan = tmp_path / "schema-transition-plan.json"
    transition_plan.write_text(
        json.dumps(
            {"rollback": {"delete_after_predecessor_apply": rollback_deletions}},
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="ascii",
    )
    program = (
        "set -uo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        f"transition_plan={shlex.quote(str(transition_plan))}\n"
        "kubeconfig=/unused/kubeconfig\n"
        "predecessor_shadow=/unused/predecessor.yaml\n"
        'kubectl() { printf "%s\\n" "$*" >> "$call_log"; }\n'
        f"{function}\n"
        "remove_forward_only_web\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    if expected_returncode == 0:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0
    assert call_log.exists() is expected_delete
    if expected_delete:
        calls = call_log.read_text(encoding="utf-8").splitlines()
        assert len(calls) == 1
        command = calls[0].split()
        assert command.count("delete") == 1
        assert {
            "deployment/loom-personal-dev-web",
            "networkpolicy/loom-personal-dev-web-ingress",
            "service/loom-personal-dev-web",
        }.issubset(command)


def test_schema_transition_recovery_preserves_succeeded_predecessor_migration(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "stop_target_migration")
    predecessor_name = "predecessor-migration"
    target_name = "target-migration"
    predecessor_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "labels": {"app": "loom-personal-dev-migration"},
            "name": predecessor_name,
        },
        "status": {"active": 0, "failed": 0, "succeeded": 1},
    }
    predecessor_pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "labels": {
                "app": "loom-personal-dev-migration",
                "batch.kubernetes.io/job-name": predecessor_name,
            },
            "name": "predecessor-migration-pod",
        },
        "status": {"phase": "Succeeded"},
    }
    target_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "labels": {"app": "loom-personal-dev-migration"},
            "name": target_name,
        },
        "status": {"active": 1, "failed": 0, "succeeded": 0},
    }
    target_pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "labels": {
                "app": "loom-personal-dev-migration",
                "batch.kubernetes.io/job-name": target_name,
            },
            "name": "target-migration-pod",
        },
        "status": {"phase": "Running"},
    }
    historical_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "labels": {"app": "loom-personal-dev-migration"},
            "name": "historical-migration",
        },
        "status": {"active": 0, "failed": 0, "succeeded": 1},
    }
    historical_pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "labels": {
                "app": "loom-personal-dev-migration",
                "batch.kubernetes.io/job-name": "historical-migration",
            },
            "name": "historical-migration-pod",
        },
        "status": {"phase": "Succeeded"},
    }
    inventory_before = json.dumps(
        {
            "items": [
                predecessor_job,
                predecessor_pod,
                target_job,
                target_pod,
                historical_job,
                historical_pod,
            ]
        },
        separators=(",", ":"),
    )
    inventory_after = json.dumps(
        {
            "items": [
                predecessor_job,
                predecessor_pod,
                historical_job,
                historical_pod,
            ]
        },
        separators=(",", ":"),
    )
    empty_inventory = '{"items":[]}'
    call_log = tmp_path / "calls.txt"
    program = (
        "set -euo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"predecessor_migration_job_name={predecessor_name}\n"
        f"migration_job_name={target_name}\n"
        "predecessor_present=1\n"
        "target_present=1\n"
        "target_pod_present=1\n"
        "kubectl() {\n"
        '  printf "%s\\n" "$*" >> "$call_log"\n'
        '  case " $* " in\n'
        '    *" get jobs,pods "*" -o json "*)\n'
        '      if test "$predecessor_present" -eq 0; then\n'
        f"        printf '%s\\n' {shlex.quote(empty_inventory)}\n"
        '      elif test "$target_present" -eq 0 &&\n'
        '           test "$target_pod_present" -eq 0; then\n'
        f"        printf '%s\\n' {shlex.quote(inventory_after)}\n"
        "      else\n"
        f"        printf '%s\\n' {shlex.quote(inventory_before)}\n"
        "      fi\n"
        "      ;;\n"
        f'    *" delete job {target_name} "*) target_present=0 ;;\n'
        f'    *" delete pods "*"batch.kubernetes.io/job-name={target_name}"*)\n'
        "      target_pod_present=0\n"
        "      ;;\n"
        '    *" delete jobs "*" app=loom-personal-dev-migration "*)\n'
        "      predecessor_present=0\n"
        "      target_present=0\n"
        "      target_pod_present=0\n"
        "      ;;\n"
        "    *) return 91 ;;\n"
        "  esac\n"
        "}\n"
        f"{function}\n"
        "stop_target_migration\n"
        'printf "%s:%s:%s\\n" "$predecessor_present" '
        '"$target_present" "$target_pod_present"\n'
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "1:0:0\n"


def test_schema_transition_recovery_rejects_orphan_migration_pod(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "stop_target_migration")
    inventory = json.dumps(
        {
            "items": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "labels": {"app": "loom-personal-dev-migration"},
                        "name": "predecessor-migration",
                    },
                    "status": {"succeeded": 1},
                },
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "labels": {
                            "app": "loom-personal-dev-migration",
                            "batch.kubernetes.io/job-name": "unknown-migration",
                        },
                        "name": "orphan-migration",
                    },
                    "status": {"phase": "Running"},
                },
            ]
        },
        separators=(",", ":"),
    )
    program = (
        "set -euo pipefail\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        "kubeconfig=/unused/kubeconfig\n"
        "predecessor_migration_job_name=predecessor-migration\n"
        "migration_job_name=target-migration\n"
        "kubectl() {\n"
        '  case "$*" in\n'
        f'    *"get jobs,pods"*"-o json"*) printf \'%s\\n\' {shlex.quote(inventory)} ;;\n'
        "    *) return 91 ;;\n"
        "  esac\n"
        "}\n"
        f"{function}\n"
        "stop_target_migration\n"
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


@pytest.mark.parametrize(
    "mutation",
    (
        "active-job",
        "failed-job",
        "running-pod",
        "deleting-job",
        "deleting-pod",
    ),
)
def test_schema_transition_recovery_rejects_unsafe_historical_migration(
    tmp_path: Path,
    mutation: str,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "stop_target_migration")
    predecessor_name = "predecessor-migration"
    target_name = "target-migration"
    historical_name = "historical-migration"

    def job(name: str, *, active: int, succeeded: int) -> dict[str, object]:
        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "labels": {"app": "loom-personal-dev-migration"},
                "name": name,
            },
            "status": {"active": active, "failed": 0, "succeeded": succeeded},
        }

    def pod(name: str, owner: str, *, phase: str) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "labels": {
                    "app": "loom-personal-dev-migration",
                    "batch.kubernetes.io/job-name": owner,
                },
                "name": name,
            },
            "status": {"phase": phase},
        }

    historical_job = job(historical_name, active=0, succeeded=1)
    historical_pod = pod("historical-pod", historical_name, phase="Succeeded")
    if mutation == "active-job":
        historical_job["status"] = {"active": 1, "failed": 0, "succeeded": 0}
    elif mutation == "failed-job":
        historical_job["status"] = {"active": 0, "failed": 1, "succeeded": 0}
    elif mutation == "running-pod":
        historical_pod["status"] = {"phase": "Running"}
    elif mutation == "deleting-job":
        historical_job["metadata"]["deletionTimestamp"] = "2026-08-30T00:00:00Z"  # type: ignore[index]
    elif mutation == "deleting-pod":
        historical_pod["metadata"]["deletionTimestamp"] = "2026-08-30T00:00:00Z"  # type: ignore[index]
    inventory = json.dumps(
        {
            "items": [
                job(predecessor_name, active=0, succeeded=1),
                pod("predecessor-pod", predecessor_name, phase="Succeeded"),
                job(target_name, active=1, succeeded=0),
                pod("target-pod", target_name, phase="Running"),
                historical_job,
                historical_pod,
            ]
        },
        separators=(",", ":"),
    )
    call_log = tmp_path / "calls.txt"
    program = (
        "set -euo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"predecessor_migration_job_name={predecessor_name}\n"
        f"migration_job_name={target_name}\n"
        "kubectl() {\n"
        '  printf "%s\\n" "$*" >> "$call_log"\n'
        '  case " $* " in\n'
        '    *" get jobs,pods "*" -o json "*)\n'
        f"      printf '%s\\n' {shlex.quote(inventory)}\n"
        "      ;;\n"
        '    *" delete job "*|*" delete pods "*) ;;\n'
        "    *) return 91 ;;\n"
        "  esac\n"
        "}\n"
        f"{function}\n"
        "stop_target_migration\n"
    )
    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert call_log.read_text(encoding="ascii").splitlines() == [
        "--kubeconfig /unused/kubeconfig --namespace loom-dev get jobs,pods "
        "-l app=loom-personal-dev-migration -o json"
    ]


@pytest.mark.parametrize(
    "fail_at",
    (
        "scale_management_to_zero",
        "wait_for_no_management_pods",
        "assert_migration_job_absent",
        "apply_reviewed_migration_job",
        "wait_for_reviewed_migration_job",
        "assert_live_target_head",
        "apply_reviewed_target_shadow",
        "wait_for_target_shadow",
        "assert_target_shadow_ready",
    ),
)
def test_schema_transition_every_post_quiesce_failure_runs_full_restore(
    tmp_path: Path,
    fail_at: str,
) -> None:
    result, calls = _run_schema_transition_orchestration(tmp_path, fail_at=fail_at)

    assert result.returncode == 1
    assert calls[-17:] == [
        "assert_predecessor_recovery_interlocks",
        "scale_management_to_zero",
        "wait_for_no_management_pods",
        "assert_predecessor_recovery_interlocks",
        "stop_target_migration",
        "assert_predecessor_recovery_interlocks",
        "assert_predecessor_restore_artifacts",
        "assert_only_restore_connection",
        "restore_predecessor_database",
        "assert_predecessor_restore_artifacts",
        "assert_live_predecessor_state",
        "assert_predecessor_recovery_interlocks",
        "apply_reviewed_predecessor_shadow",
        "remove_forward_only_web",
        "wait_for_predecessor_shadow",
        "assert_predecessor_shadow_ready",
        "assert_predecessor_recovery_interlocks",
    ]


def test_schema_transition_target_drift_cannot_block_predecessor_recovery(
    tmp_path: Path,
) -> None:
    result, calls = _run_schema_transition_orchestration(
        tmp_path,
        persistent_fail_at="assert_transition_interlocks",
        fail_after=1,
    )

    assert result.returncode == 1
    failure = calls.index("assert_transition_interlocks", 1)
    recovery = calls.index("assert_predecessor_recovery_interlocks", failure)
    restart = calls.index("apply_reviewed_predecessor_shadow", recovery)
    assert failure < recovery < restart
    assert calls[-1] == "assert_predecessor_recovery_interlocks"


def _write_schema_transition_cli_stub(
    path: Path,
    *,
    call_log: Path,
    output: str,
    returncode: int = 0,
) -> None:
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' {shlex.quote(path.name)} >> {shlex.quote(str(call_log))}\n"
        f"printf '%s\\n' {shlex.quote(output)}\n"
        f"exit {returncode}\n",
        encoding="ascii",
    )
    path.chmod(0o755)


def test_schema_transition_predecessor_capacity_uses_predecessor_cli(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_zero_capacity")
    call_log = tmp_path / "calls.txt"
    target_cli = tmp_path / "target-loom"
    predecessor_cli = tmp_path / "predecessor-loom"
    _write_schema_transition_cli_stub(
        target_cli,
        call_log=call_log,
        output="target launcher must not run",
        returncode=23,
    )
    _write_schema_transition_cli_stub(
        predecessor_cli,
        call_log=call_log,
        output='{"executable_new_capacity_ceiling":0,"status":"ready"}',
    )
    predecessor_repo = tmp_path / "predecessor"
    (predecessor_repo / "src").mkdir(parents=True)
    program = (
        "set -euo pipefail\n"
        f"repo={shlex.quote(str(tmp_path / 'target'))}\n"
        f"predecessor_repo={shlex.quote(str(predecessor_repo))}\n"
        f"loom_cli={shlex.quote(str(target_cli))}\n"
        f"predecessor_loom_cli={shlex.quote(str(predecessor_cli))}\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"{function}\n"
        'assert_zero_capacity "$predecessor_repo"\n'
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert call_log.read_text(encoding="ascii").splitlines() == ["predecessor-loom"]


def test_schema_transition_predecessor_compat_path_rejects_pathname_replacement(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_open_owner_only_sha256",
            "assert_predecessor_release_compat_path",
        )
    )
    release = tmp_path / "predecessor-release.json"
    replacement = tmp_path / "replacement.json"
    payload = b'{"schema_version":3}\n'
    digest = _write_owner_only_artifact(release, payload)
    _write_owner_only_artifact(replacement, payload)
    program = (
        "set -uo pipefail\n"
        f"predecessor_release_source={shlex.quote(str(release))}\n"
        f"predecessor_release_sha256={digest}\n"
        f"replacement={shlex.quote(str(replacement))}\n"
        f"{functions}\n"
        'exec 31< "$predecessor_release_source"\n'
        'mv -f -- "$replacement" "$predecessor_release_source"\n'
        "assert_predecessor_release_compat_path\n"
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_schema_transition_predecessor_ready_uses_guarded_status_runner(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_predecessor_shadow_ready")
    call_log = tmp_path / "calls.txt"
    status = json.dumps(
        {
            "blockers": [],
            "components": [{"name": "personal-workers", "observed": 0}],
            "manager_ceiling": 0,
            "mode": "shadow",
            "ready": True,
            "worker_available": False,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    program = (
        "set -euo pipefail\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        'record() { printf "%s\\n" "$1" >> '
        f"{shlex.quote(str(call_log))}; }}\n"
        "run_predecessor_shadow_status() {\n"
        "  record guarded-predecessor-status\n"
        '  test "$1" = "$evidence_dir/rollback-predecessor.status.json"\n'
        f"  printf '%s\\n' {shlex.quote(status)} > \"$1\"\n"
        "}\n"
        f"{function}\n"
        "assert_predecessor_shadow_ready\n"
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert call_log.read_text(encoding="ascii").splitlines() == [
        "guarded-predecessor-status"
    ]


def test_schema_transition_pre_quiesce_failure_does_not_mutate_recovery(
    tmp_path: Path,
) -> None:
    result, calls = _run_schema_transition_orchestration(
        tmp_path,
        fail_at="assert_transition_interlocks",
    )

    assert result.returncode == 1
    assert calls == ["assert_transition_interlocks"]


@pytest.mark.parametrize(
    ("function_name", "arguments"),
    (
        ("capture_docker_postgres_state", 'fake "$evidence_dir/state.tsv"'),
        ("capture_live_postgres_state", '"$evidence_dir/state.tsv"'),
        ("assert_no_dynamic_namespaces", ""),
        ("assert_migration_job_absent", ""),
        ("apply_reviewed_migration_job", ""),
        ("apply_reviewed_target_shadow", ""),
        ("restore_predecessor_database", ""),
        ("apply_reviewed_predecessor_shadow", ""),
        ("remove_forward_only_web", ""),
    ),
)
def test_schema_transition_critical_helpers_propagate_command_failure(
    tmp_path: Path,
    function_name: str,
    arguments: str,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, function_name)
    program = (
        "set -uo pipefail\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        "kubeconfig=/unused/kubeconfig\n"
        "postgres_pod=postgres\n"
        f"postgres_dump={shlex.quote(str(tmp_path / 'postgres.dump'))}\n"
        "transition_job=/unused/migration.json\n"
        "target_shadow=/unused/target.yaml\n"
        "predecessor_shadow=/unused/predecessor.yaml\n"
        "migration_job_name=migration\n"
        "kubectl() { return 23; }\n"
        "docker() { return 23; }\n"
        f"{function}\n"
        ': > "$postgres_dump"\n'
        f"{function_name} {arguments}\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_schema_transition_interlock_propagates_first_failed_check() -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_transition_interlocks")
    program = (
        "set -uo pipefail\n"
        "assert_transition_artifacts() { return 23; }\n"
        "assert_no_dynamic_namespaces() { return 0; }\n"
        "assert_zero_capacity() { return 0; }\n"
        "assert_reviewed_kubeconfig() { return 0; }\n"
        "kubectl() { printf '0'; }\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"{function}\n"
        "assert_transition_interlocks\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_schema_transition_interlock_binds_kubeconfig_before_cluster_reads(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_transition_interlocks")
    call_log = tmp_path / "calls.txt"
    program = (
        "set -uo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        'record() { printf "%s\\n" "$1" >> "$call_log"; }\n'
        "assert_reviewed_kubeconfig() { record kubeconfig; return 23; }\n"
        "assert_transition_artifacts() { record artifacts; }\n"
        "assert_no_dynamic_namespaces() { record namespaces; }\n"
        "assert_zero_capacity() { record capacity; }\n"
        'kubectl() { record kubectl; printf "0"; }\n'
        "kubeconfig=/unused/kubeconfig\n"
        f"{function}\n"
        "assert_transition_interlocks\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert call_log.read_text(encoding="utf-8").splitlines() == ["kubeconfig"]


def test_schema_transition_binds_kubeconfig_before_first_live_status_read(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    status_start = runbook.index('predecessor_status="$evidence_dir/predecessor.status.json"')
    start = runbook.rindex("assert_reviewed_kubeconfig\n", 0, status_start)
    end = runbook.index("\n```", start)
    live_preflight = runbook[start:end]
    call_log = tmp_path / "calls.txt"
    program = (
        "set -euo pipefail\n"
        f"call_log={shlex.quote(str(call_log))}\n"
        'record() { printf "%s\\n" "$1" >> "$call_log"; }\n'
        "assert_reviewed_kubeconfig() { record kubeconfig; return 23; }\n"
        "assert_no_dynamic_namespaces() { record namespaces; }\n"
        "assert_zero_capacity() { record capacity; }\n"
        "predecessor_cli() { record predecessor-status; printf '%s\\n' "
        + shlex.quote(
            '{"blockers":[],"components":[{"name":"personal-workers",'
            '"observed":0}],"manager_ceiling":0,"mode":"shadow","ready":true,'
            '"worker_available":false}'
        )
        + "; }\n"
        'kubectl() { record kubectl; printf "0"; }\n'
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        "predecessor_repo=/unused/predecessor\n"
        "predecessor_loom_cli=predecessor_cli\n"
        "predecessor_profile=/unused/profile\n"
        "predecessor_release=/unused/release\n"
        f"predecessor_release_sha256={'a' * 64}\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"{live_preflight}\n"
    )

    result = subprocess.run(
        ["/bin/bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert call_log.read_text(encoding="ascii").splitlines() == ["kubeconfig"]


def test_schema_transition_reviewed_kubeconfig_is_content_bound(tmp_path: Path) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    functions = "\n".join(
        _fenced_shell_function(runbook, name)
        for name in (
            "assert_owner_only_sha256",
            "assert_pinned_owner_only_sha256",
            "assert_reviewed_kubeconfig",
        )
    )
    kubeconfig = tmp_path / "kubeconfig"
    digest = _write_owner_only_artifact(kubeconfig, b"reviewed kubeconfig\n")
    program = (
        "set -uo pipefail\n"
        f"kubeconfig={shlex.quote(str(kubeconfig))}\n"
        f"kubeconfig_sha256={digest}\n"
        "expected_kube_context=reviewed-context\n"
        'kubectl() { printf "reviewed-context\\n"; }\n'
        f"{functions}\n"
        'exec 36< "$kubeconfig"\n'
        "kubeconfig=/proc/self/fd/36\n"
    )
    valid = subprocess.run(
        ["bash"],
        input=program + "assert_reviewed_kubeconfig\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert valid.returncode == 0, valid.stderr

    kubeconfig.write_bytes(b"redirected kubeconfig\n")
    kubeconfig.chmod(0o600)
    redirected = subprocess.run(
        ["bash"],
        input=program + "assert_reviewed_kubeconfig\n",
        text=True,
        capture_output=True,
        check=False,
    )
    assert redirected.returncode != 0


@pytest.mark.parametrize(
    ("namespace", "expected_returncode"),
    (("loom-dev", 0), ("loom-dev-alice", 1), ("loom-build-candidate", 1)),
)
def test_schema_transition_dynamic_namespace_guard_executes(
    namespace: str,
    expected_returncode: int,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_no_dynamic_namespaces")
    namespace_json = json.dumps({"items": [{"metadata": {"name": namespace}}]})
    program = (
        "set -uo pipefail\n"
        f"namespace_json={shlex.quote(namespace_json)}\n"
        "kubectl() { printf '%s\\n' \"$namespace_json\"; }\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"{function}\n"
        "assert_no_dynamic_namespaces\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == expected_returncode, result.stderr


def test_schema_transition_dynamic_namespace_guard_rejects_malformed_inventory() -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_no_dynamic_namespaces")
    program = (
        "set -uo pipefail\n"
        "kubectl() { printf 'not-json\\n'; }\n"
        "kubeconfig=/unused/kubeconfig\n"
        f"{function}\n"
        "assert_no_dynamic_namespaces\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_schema_transition_predecessor_check_propagates_capture_failure(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-schema-transition.md")
    function = _fenced_shell_function(runbook, "assert_live_predecessor_state")
    expected = tmp_path / "expected.tsv"
    observed = tmp_path / "live.test.tsv"
    expected.write_text("same\n", encoding="utf-8")
    observed.write_text("same\n", encoding="utf-8")
    program = (
        "set -uo pipefail\n"
        f"evidence_dir={shlex.quote(str(tmp_path))}\n"
        f"postgres_source_state={shlex.quote(str(expected))}\n"
        "predecessor_head=0112\n"
        "capture_live_postgres_state() { return 23; }\n"
        "live_schema_head() { printf '0112\\n'; }\n"
        f"{function}\n"
        "assert_live_predecessor_state test\n"
    )
    result = subprocess.run(
        ["bash"],
        input=program,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0


def test_personal_dev_builder_runtime_runbook_is_exact_and_inert() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    normalized = " ".join(runbook.split())
    lowered = runbook.casefold()

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert 'test "$(id -u)" != 0' in runbook
    assert "&& test ! -L" not in runbook
    assert 'test ! -e "$archive" &&' not in runbook
    assert "< <(" not in runbook
    assert "assert_pod_continuity()" in runbook
    assert runbook.count("assert_pod_continuity") >= 4
    assert "capture_runtime_node_state()" in runbook
    assert runbook.count("capture_runtime_node_state") >= 3
    assert "assert_node_cordon_state()" in runbook
    assert runbook.count("assert_node_cordon_state") >= 8
    assert 'get "node/$node" -o json > "$evidence_dir/$node.node-after.json"' not in normalized
    assert 'test -z "$(git status' not in runbook
    assert "<absolute-existing-owner-only-evidence-root-outside-repository>" in runbook
    assert "artifacts/personal-dev" not in runbook
    assert 'test "$(stat -c %a "$evidence_root")" = 700' in runbook
    assert "deploy/dev-fleet/personal-dev-builder-runtime-profile.json" in runbook
    assert "deploy/dev-fleet/personal-dev-builder-runtime-class.yaml" in runbook
    assert "scripts/ops/install_personal_dev_builder_runtime.py" in runbook
    assert "release-20260810.0" in runbook
    assert (
        "3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe"
        "33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62"
    ) in runbook
    assert "trt-eai-oldlab-2" in runbook
    assert "trt-eai-oldlab-3" in runbook
    assert "trt-eai-oldlab-4" in runbook
    assert "trt-eai-oldlab-5" in runbook
    assert "control-plane node is never eligible" in lowered
    assert '"node-role.kubernetes.io/control-plane"' in runbook
    assert "expected_agent_names" in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" cordon "$node"' in normalized
    assert normalized.count('kubectl --kubeconfig "$kubeconfig" cordon "$node"') >= 3
    assert "do not drain" in lowered
    assert "a failure on one node stops the fleet rollout" in lowered
    assert "verify-staged" in runbook
    assert "verify-active" in runbook
    assert " remove " in normalized
    assert "systemctl restart k3s-agent" in normalized
    assert ("loom.dev/personal-dev-runtime-profile-a=6ee2c283e5bf0783e192787522ea9550") in runbook
    assert ("loom.dev/personal-dev-runtime-profile-b=caadff4131590cc0a26dbf7dd2a6869b") in runbook
    assert (
        "loom.dev/personal-dev-runtime-profile-sha256="
        "6ee2c283e5bf0783e192787522ea9550caadff4131590cc0a26dbf7dd2a6869b"
    ) in runbook
    assert "/proc/gvisor/kernel_is_gvisor" in runbook
    assert "rootless buildkit" in lowered
    assert "buildkit-qemu-aarch64" in runbook
    assert "linux/amd64" in runbook
    assert "linux/arm64" in runbook
    assert 'kubectl --kubeconfig "$kubeconfig" diff --server-side' in normalized
    assert 'kubectl --kubeconfig "$kubeconfig" apply --server-side' in normalized
    assert "executable-new-capacity ceiling remains exactly `0`" in runbook
    assert "no `loom-dev-<owner>` namespace" in runbook
    assert "no `loom-build-*` namespace" in runbook
    assert "loom-runtime-smoke" in runbook
    assert 'runtimeClassName: "loom-personal-dev-builder"' in runbook
    assert "automountServiceAccountToken: false" in runbook
    assert "build-egress" in runbook
    assert "_load_safe_kubeconfig" in runbook
    assert "render_runtime_class" in runbook
    assert "sudo -n --" in runbook
    assert "personal-dev-runtime\\.[A-Za-z0-9]{8}" in runbook
    assert "/root/loom-personal-dev-builder-runtime-rollout" in runbook
    assert "installer_sha256" in runbook
    assert "profile_module_sha256" in runbook
    assert (
        "/usr/bin/install -o root -g root -m 0600 '$remote_stage/gvisor-release-20260810.0.tar.bz2'"
    ) in normalized
    assert '"regular file:0:0:600:1"' in runbook
    assert "PYTHONPATH='$remote_stage'" not in runbook
    assert "cd '$root_stage' && sudo" not in runbook
    assert "assert_no_runtimeclass_consumers" in runbook
    assert runbook.count("assert_no_runtimeclass_consumers") >= 5
    assert "assert_runtimeclass_absent" in runbook
    assert runbook.count("assert_runtimeclass_absent") >= 6
    runtimeclass_absence_check = runbook.split("assert_runtimeclass_absent() {", 1)[1].split(
        "\n}", 1
    )[0]
    assert "--ignore-not-found -o name" in " ".join(runtimeclass_absence_check.split())
    assert "> /dev/null 2>&1" not in runtimeclass_absence_check
    assert "assert_node_staging()" in runbook
    assert runbook.count('assert_node_staging "$node"') >= 4
    assert "assert_remote_staging()" in runbook
    assert runbook.count('assert_remote_staging "$node"') >= 2
    assert "assert_runtime_receipt()" in runbook
    assert runbook.count("assert_runtime_receipt") >= 10
    assert 'test ! -e "$root_stage_base"' in runbook
    assert "InvocationID" in runbook
    assert "await_fresh_node_lease()" in runbook
    assert runbook.count("await_fresh_node_lease") >= 4
    assert "kube-node-lease" in runbook
    assert "/usr/bin/env LC_ALL=C /usr/bin/stat" in normalized
    assert runbook.count("k3s-invocation.before.txt") >= 3
    assert runbook.count("k3s-invocation.after.txt") >= 3
    assert "runtime_class_sha256" in runbook
    assert "remove_node_runtime_identity" in runbook
    assert runbook.count('remove_node_runtime_identity "$node"') >= 2
    assert "loom.dev/personal-dev-runtime-profile-a-" not in runbook
    assert "loom.dev/personal-dev-runtime-profile-b-" not in runbook
    assert "loom.dev/personal-dev-runtime-profile-sha256-" not in runbook
    assert "--type=merge" in runbook
    assert "reviewed_runtime_diff_sha256='<reviewed-runtime-diff-sha256>'" in runbook
    assert 'test "$runtime_diff_status" -eq 1' in runbook
    assert "<ssh-user>@$node" not in runbook
    assert "assert_smoke_namespace_owned" in runbook
    assert runbook.count("assert_smoke_namespace_owned") >= 3
    assert 'delete "pod/$pod"' not in runbook
    assert "assert_smoke_namespace_absent" in runbook
    assert runbook.count("assert_smoke_namespace_absent") >= 4
    assert 'create -f "$smoke_namespace_manifest"' in normalized
    assert "--field-manager=loom-personal-dev-runtime-smoke" not in runbook
    assert 'test -z "$(kubectl' not in runbook
    assert "loom.dev/runtime-rollout-source-sha" in runbook
    assert runbook.count("loom-personal-dev-runtime-smoke") >= 8
    assert runbook.count(".spec.nodeSelector == {") >= 2
    assert runbook.count(".scheduling.nodeSelector == {") >= 3
    assert '"pod-security.kubernetes.io/enforce": "privileged"' in runbook
    assert '"pod-security.kubernetes.io/enforce-version": "v1.36"' in runbook
    assert '"pod-security.kubernetes.io/audit": "restricted"' in runbook
    assert 'restartPolicy: "Always"' in runbook
    assert 'command: ["/usr/local/bin/loom-personal-dev-buildkitd"]' in runbook
    assert 'capabilities: {drop: ["ALL"], add: ["SETGID", "SETUID"]}' in runbook
    assert 'seccompProfile: {type: "Unconfined"}' in runbook
    assert "enableServiceLinks: false" in runbook
    assert "shareProcessNamespace: false" in runbook
    assert "/usr/bin/buildctl" in runbook
    assert "/usr/bin/buildctl-daemonless.sh" not in runbook
    assert "loom-nnp=1" in runbook
    assert 'configMap: {name: "buildkit-conformance", defaultMode: 292}' in runbook
    assert "get secrets -o name" in normalized
    assert "services,secrets" not in normalized
    assert "apply --server-side --force-conflicts" not in normalized
    assert "|| true" not in runbook
    assert "PYTHONPATH='$root_stage'" in runbook
    assert runbook.count(" remove --profile ") >= 2

    assert "loom-dev-shared" not in runbook
    assert "kubectl drain" not in lowered
    assert "kubectl create namespace" not in lowered
    assert "loom service up" not in lowered
    for command in ("salloc", "sbatch", "scancel", "scontrol", "sinfo", "squeue"):
        assert command not in lowered


def test_personal_dev_builder_runtime_embedded_programs_parse() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    bash_blocks = re.findall(r"^```bash\n(.*?)^```$", runbook, flags=re.DOTALL | re.MULTILINE)
    python_programs = re.findall(r"<<'PY'\n(.*?)\nPY\n", runbook, flags=re.DOTALL)
    jq_filters = re.findall(r"\bjq\b[^']*'(.*?)'", runbook, flags=re.DOTALL)

    assert bash_blocks
    bash = subprocess.run(
        ["bash", "-n"],
        input="\n".join(bash_blocks),
        text=True,
        capture_output=True,
        check=False,
    )
    assert bash.returncode == 0, bash.stderr
    assert python_programs
    for program in python_programs:
        compile(program, "personal-dev-builder-runtime.md", "exec")
    assert jq_filters
    for jq_filter in jq_filters:
        jq = subprocess.run(
            ["jq", "-n", f"def candidate: {jq_filter}; empty"],
            text=True,
            capture_output=True,
            check=False,
        )
        assert jq.returncode == 0, jq.stderr

    node_filters = [
        value for value in jq_filters if "node-role.kubernetes.io/control-plane" in value
    ]
    assert len(node_filters) == 1
    agent_names = [f"trt-eai-oldlab-{number}" for number in range(2, 6)]

    def node(name: str, *, control_plane: bool = False) -> dict[str, object]:
        labels = {"node-role.kubernetes.io/control-plane": ""} if control_plane else {}
        return {
            "metadata": {"annotations": {}, "labels": labels, "name": name},
            "status": {
                "conditions": [
                    {"status": "True", "type": "Ready"},
                    {"status": "False", "type": "DiskPressure"},
                ]
            },
        }

    healthy_nodes = {
        "items": [node("control", control_plane=True)] + [node(name) for name in agent_names]
    }

    def evaluate_nodes(payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "jq",
                "-e",
                "--argjson",
                "agents",
                json.dumps(agent_names),
                node_filters[0],
            ],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )

    healthy = evaluate_nodes(healthy_nodes)
    assert healthy.returncode == 0, healthy.stderr
    drifted_nodes = json.loads(json.dumps(healthy_nodes))
    drifted_control_labels = drifted_nodes["items"][0]["metadata"]["labels"]
    drifted_control_labels["loom.dev/personal-dev-runtime-profile-a"] = "unexpected"
    assert evaluate_nodes(drifted_nodes).returncode != 0

    network_policy_filters = [
        value for value in jq_filters if 'kind: "List"' in value and 'name: "build-egress"' in value
    ]
    assert len(network_policy_filters) == 1
    smoke_policies = json.loads(
        subprocess.run(
            [
                "jq",
                "-n",
                "--arg",
                "namespace",
                "loom-runtime-smoke",
                "--arg",
                "source",
                "2" * 40,
                network_policy_filters[0],
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout
    )
    policies = {item["metadata"]["name"]: item for item in smoke_policies["items"]}
    assert policies["default-deny"]["spec"] == {
        "podSelector": {},
        "policyTypes": ["Ingress", "Egress"],
    }
    assert policies["build-egress"]["spec"]["egress"] == [
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                    },
                    "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                }
            ],
            "ports": [
                {"protocol": "UDP", "port": 53},
                {"protocol": "TCP", "port": 53},
            ],
        },
        {
            "to": [
                {
                    "ipBlock": {
                        "cidr": "0.0.0.0/0",
                        "except": [
                            "0.0.0.0/8",
                            "10.0.0.0/8",
                            "100.64.0.0/10",
                            "127.0.0.0/8",
                            "169.254.0.0/16",
                            "172.16.0.0/12",
                            "192.0.0.0/24",
                            "192.0.2.0/24",
                            "192.88.99.0/24",
                            "192.168.0.0/16",
                            "198.18.0.0/15",
                            "198.51.100.0/24",
                            "203.0.113.0/24",
                            "224.0.0.0/4",
                            "240.0.0.0/4",
                        ],
                    }
                },
                {
                    "ipBlock": {
                        "cidr": "2000::/3",
                        "except": [
                            "2001::/23",
                            "2001:db8::/32",
                            "2002::/16",
                            "3fff::/20",
                        ],
                    }
                },
            ],
            "ports": [{"protocol": "TCP", "port": 443}],
        },
    ]

    smoke_filters = [value for value in jq_filters if "kernel_is_gvisor" in value]
    assert len(smoke_filters) == 1
    rendered = subprocess.run(
        [
            "jq",
            "-n",
            "--arg",
            "namespace",
            "loom-runtime-smoke",
            "--arg",
            "node",
            "trt-eai-oldlab-2",
            "--arg",
            "pod",
            "gvisor-smoke-2",
            "--arg",
            "image",
            "example.invalid/smoke@sha256:" + "1" * 64,
            "--arg",
            "source",
            "2" * 40,
            smoke_filters[0],
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    command = json.loads(rendered.stdout)["spec"]["containers"][0]["args"][0]
    shell = subprocess.run(
        ["/bin/sh", "-n", "-c", command],
        text=True,
        capture_output=True,
        check=False,
    )
    assert shell.returncode == 0, shell.stderr

    buildkit_filters = [
        value
        for value in jq_filters
        if 'restartPolicy: "Always"' in value and 'name: "buildkitd"' in value
    ]
    assert len(buildkit_filters) == 1
    buildkit_pod = json.loads(
        subprocess.run(
            [
                "jq",
                "-n",
                "--arg",
                "namespace",
                "loom-runtime-smoke",
                "--arg",
                "image",
                "example.invalid/builder@sha256:" + "3" * 64,
                "--arg",
                "base",
                "example.invalid/base@sha256:" + "4" * 64,
                "--arg",
                "source",
                "5" * 40,
                buildkit_filters[0],
            ],
            text=True,
            capture_output=True,
            check=True,
        ).stdout
    )
    spec = buildkit_pod["spec"]
    assert spec["enableServiceLinks"] is False
    assert spec["shareProcessNamespace"] is False
    assert len(spec["containers"]) == 1
    assert len(spec["initContainers"]) == 1
    client = spec["containers"][0]
    sidecar = spec["initContainers"][0]
    assert client["name"] == "conformance"
    assert sidecar["name"] == "buildkitd"
    client_mounts = {mount["name"] for mount in client["volumeMounts"]}
    sidecar_mounts = {mount["name"] for mount in sidecar["volumeMounts"]}
    assert client_mounts == {"buildkit-run", "script", "tmp-client", "workspace"}
    assert sidecar_mounts == {"buildkit-run", "buildkit-state", "tmp-buildkit"}
    assert not {"attempt-capability", "contract", "script", "workspace"} & (sidecar_mounts)
    assert not {"buildkit-state", "tmp-buildkit"} & client_mounts


def test_personal_dev_builder_runtime_configmap_rendering_executes(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    start = runbook.index('buildkit_configmap="$evidence_dir/buildkit-conformance.configmap.json"')
    end_marker = 'chmod 0600 "$buildkit_configmap"'
    end = runbook.index(end_marker, start) + len(end_marker)
    rendering = runbook[start:end]

    buildkit_script = tmp_path / "run.sh"
    buildkit_script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    merged_source_sha = "0123456789abcdef0123456789abcdef01234567"
    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            "kubeconfig=/dev/null\n"
            "smoke_namespace=loom-runtime-smoke\n"
            f"buildkit_script={shlex.quote(str(buildkit_script))}\n"
            f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
            f"merged_source_sha={merged_source_sha}\n"
            "kubectl() {\n"
            '  test "$*" = "--kubeconfig /dev/null --namespace '
            "loom-runtime-smoke create configmap buildkit-conformance "
            '--from-file=run.sh=$buildkit_script --dry-run=client -o json"\n'
            "  printf '%s\\n' "
            '\'{"apiVersion":"v1","data":{"run.sh":"#!/bin/sh\\nexit 0\\n"},'
            '"kind":"ConfigMap","metadata":{"creationTimestamp":null,'
            '"name":"buildkit-conformance","namespace":'
            '"loom-runtime-smoke"}}\'\n'
            "}\n"
            f"{rendering}\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr

    rendered = json.loads(
        (evidence_dir / "buildkit-conformance.configmap.json").read_text(encoding="utf-8")
    )
    assert rendered["metadata"]["namespace"] == "loom-runtime-smoke"
    assert rendered["metadata"]["labels"] == {
        "app.kubernetes.io/managed-by": "loom-personal-dev-runtime-smoke"
    }
    assert rendered["metadata"]["annotations"] == {
        "loom.dev/runtime-rollout-source-sha": merged_source_sha
    }
    assert rendered["data"] == {"run.sh": "#!/bin/sh\nexit 0\n"}


def test_personal_dev_builder_runtime_hostname_identity_uses_dns_case_rules() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    marker = "assert_dns_hostname_identity() {"
    start = runbook.index(marker)
    function = runbook[start:].split("\n}", 1)[0] + "\n}"
    assert runbook.count('assert_dns_hostname_identity "$observed_hostname" "$node"') == 2

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            function
            + "\n"
            + "assert_dns_hostname_identity "
            + "trt-EAI-OLDLAB-2 trt-eai-oldlab-2\n"
            + "if assert_dns_hostname_identity "
            + "trt-eai-oldlab-3 trt-eai-oldlab-2; then exit 1; fi\n"
            + "if assert_dns_hostname_identity "
            + "trt-eai-oldlab-2. trt-eai-oldlab-2; then exit 1; fi\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_separates_ssh_stdin_from_scp_options() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    options = re.search(
        r"^ssh_options=\([^\n]+\)\nssh_run_options=\([^\n]+\)$",
        runbook,
        flags=re.MULTILINE,
    )
    assert options is not None

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            options.group(0)
            + "\n"
            + "ssh() {\n"
            + '  test "$1" = -n\n'
            + "}\n"
            + "scp() {\n"
            + '  for argument in "$@"; do test "$argument" != -n; done\n'
            + "}\n"
            + 'ssh "${ssh_run_options[@]}" target /bin/true\n'
            + 'scp "${ssh_options[@]}" -- source target:/tmp/\n'
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr
    assert 'ssh "${ssh_options[@]}"' not in runbook
    assert 'scp "${ssh_run_options[@]}"' not in runbook


def test_personal_dev_builder_runtime_remote_staging_directories_are_private(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    command = re.search(
        r'^ssh "\$\{ssh_run_options\[@\]\}" "\$target" \\\n'
        r'  "/usr/bin/install -d -m 0700 [^"]+"$',
        runbook,
        flags=re.MULTILINE,
    )
    assert command is not None

    remote_stage = tmp_path / "remote-stage"
    remote_stage.mkdir(mode=0o700)
    remote_stage.chmod(0o700)
    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            "umask 022\n"
            f"remote_stage={shlex.quote(str(remote_stage))}\n"
            "ssh_run_options=()\n"
            "target=target\n"
            "ssh() {\n"
            '  test "$#" -eq 2\n'
            '  test "$1" = target\n'
            '  /bin/bash -ceu -- "$2"\n'
            "}\n" + command.group(0) + "\n" + 'for directory in "$remote_stage" '
            '"$remote_stage/scripts" "$remote_stage/scripts/ops"; do\n'
            + '  mode="$(stat -c %a "$directory")"\n'
            + '  test "$mode" = 700 || { '
            'printf "%s mode=%s\\n" "$directory" "$mode" >&2; exit 1; }\n' + "done\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_root_staging_directories_are_private(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    root_stage_evidence = 'printf \'%s\\n\' "$root_stage" > "$evidence_dir/$node.root-stage.txt"'
    command_start = runbook.index(
        'ssh "${ssh_run_options[@]}" "$target" \\\n',
        runbook.index(root_stage_evidence),
    )
    command_end = runbook.index("\nassert_node_staging", command_start)
    command = runbook[command_start:command_end]

    remote_stage = tmp_path / "remote-stage"
    remote_ops = remote_stage / "scripts" / "ops"
    remote_ops.mkdir(parents=True)
    for relative in (
        "personal-dev-builder-runtime-profile.json",
        "gvisor-release-20260810.0.tar.bz2",
        "scripts/ops/install_personal_dev_builder_runtime.py",
        "scripts/ops/personal_dev_builder_runtime_profile.py",
    ):
        (remote_stage / relative).write_bytes(b"test fixture\n")
    root_stage_base = tmp_path / "root-stage"
    root_stage_parent = root_stage_base / "source-sha"
    root_stage = root_stage_parent / "trt-eai-oldlab-2"
    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            "umask 022\n"
            f"remote_stage={shlex.quote(str(remote_stage))}\n"
            f"root_stage_base={shlex.quote(str(root_stage_base))}\n"
            f"root_stage_parent={shlex.quote(str(root_stage_parent))}\n"
            f"root_stage={shlex.quote(str(root_stage))}\n"
            "ssh_run_options=()\n"
            "target=target\n"
            "sudo() {\n"
            '  test "$1" = -n\n'
            '  test "$2" = --\n'
            "  shift 2\n"
            '  if test "$1" != /usr/bin/install; then "$@"; return; fi\n'
            "  shift\n"
            "  install_arguments=()\n"
            '  while test "$#" -gt 0; do\n'
            '    case "$1" in\n'
            "      -o|-g) shift 2 ;;\n"
            '      *) install_arguments+=("$1"); shift ;;\n'
            "    esac\n"
            "  done\n"
            '  /usr/bin/install "${install_arguments[@]}"\n'
            "}\n"
            "ssh() {\n"
            '  test "$#" -eq 2\n'
            '  test "$1" = target\n'
            '  eval "$2"\n'
            "}\n" + command + "\n" + 'for directory in "$root_stage_base" '
            '"${root_stage%/*}" "$root_stage" '
            '"$root_stage/scripts" "$root_stage/scripts/ops"; do\n'
            + '  mode="$(stat -c %a "$directory")"\n'
            + '  test "$mode" = 700 || { '
            'printf "%s mode=%s\\n" "$directory" "$mode" >&2; exit 1; }\n' + "done\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_staging_cleanup_handles_live_siblings(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    marker = "cleanup_node_staging() {"
    cleanup_function = runbook[runbook.index(marker) :].split("\n}", 1)[0] + "\n}"
    source_sha = "a" * 40
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    actual_root_base = tmp_path / "root-stage"
    actual_root_parent = actual_root_base / source_sha
    for number in (2, 3):
        (actual_root_parent / f"trt-eai-oldlab-{number}").mkdir(parents=True)

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            f"{cleanup_function}\n"
            f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
            f"merged_source_sha={source_sha}\n"
            f"actual_root_base={shlex.quote(str(actual_root_base))}\n"
            "hard_root_base=/root/loom-personal-dev-builder-runtime-rollout\n"
            "ssh_run_options=()\n"
            "declare -A ssh_targets=(\n"
            "  [trt-eai-oldlab-2]=target\n"
            "  [trt-eai-oldlab-3]=target\n"
            ")\n"
            "assert_remote_staging() { :; }\n"
            "assert_node_staging() { :; }\n"
            "sudo() {\n"
            '  test "$1" = -n\n'
            '  test "$2" = --\n'
            "  shift 2\n"
            '  "$@"\n'
            "}\n"
            "ssh() {\n"
            '  test "$1" = target\n'
            '  command="$2"\n'
            '  command="${command//$hard_root_base/$actual_root_base}"\n'
            '  eval "$command"\n'
            "}\n"
            "for number in 2 3; do\n"
            '  node="trt-eai-oldlab-$number"\n'
            '  remote_stage="$(mktemp -d /tmp/loom-personal-dev-runtime.XXXXXXXX)"\n'
            '  printf "%s\\n" "$remote_stage" > "$evidence_dir/$node.remote-stage.txt"\n'
            '  printf "/root/loom-personal-dev-builder-runtime-rollout/%s/%s\\n" '
            '"$merged_source_sha" "$node" > "$evidence_dir/$node.root-stage.txt"\n'
            '  cleanup_node_staging "$node"\n'
            '  test ! -e "$remote_stage"\n'
            '  test ! -e "$actual_root_base/$merged_source_sha/$node"\n'
            '  if test "$number" = 2; then\n'
            '    test -d "$actual_root_base/$merged_source_sha/trt-eai-oldlab-3"\n'
            "  fi\n"
            "done\n"
            'test ! -e "$actual_root_base"\n'
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert behavior.returncode == 0, behavior.stderr


def test_personal_dev_builder_runtime_captures_logs_before_failed_conformance_stops(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    start = runbook.index("conformance_wait_status=0")
    end_marker = 'test "$conformance_wait_status" -eq 0'
    end = runbook.index(end_marker, start) + len(end_marker)
    failure_path = runbook[start:end]
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
            "kubeconfig=/dev/null\n"
            "smoke_namespace=loom-runtime-smoke\n"
            "builder_image=example.invalid/builder@sha256:"
            + "3" * 64
            + "\n"
            + "merged_source_sha="
            + "5" * 40
            + "\n"
            + "profile_label_a="
            + "6" * 32
            + "\n"
            + "profile_label_b="
            + "7" * 32
            + "\n"
            + "kubectl() {\n"
            + '  case " $* " in\n'
            + '    *" wait "*) return 1 ;;\n'
            + '    *" get pod/buildkit-conformance "*) '
            + 'printf \'%s\\n\' \'{"status":{"phase":"Failed"}}\' ;;\n'
            + '    *" logs "*" -c conformance "*) printf "client failure\\n" ;;\n'
            + '    *" logs "*" -c buildkitd "*) printf "sidecar failure\\n" ;;\n'
            + "    *) return 1 ;;\n"
            + "  esac\n"
            + "}\n"
            + failure_path
            + "\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert behavior.returncode != 0
    assert (evidence_dir / "buildkit-conformance.client.log").read_text(
        encoding="utf-8"
    ) == "client failure\n"
    assert (evidence_dir / "buildkit-conformance.sidecar.log").read_text(
        encoding="utf-8"
    ) == "sidecar failure\n"


def test_personal_dev_builder_runtime_captures_log_before_failed_gvisor_probe_stops(
    tmp_path: Path,
) -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    loop = runbook.index('for node in "${nodes[@]}"; do', runbook.index("## 6."))
    start = runbook.index('kubectl --kubeconfig "$kubeconfig" create -f "$manifest"', loop)
    end = runbook.index("  assert_smoke_namespace_owned", start)
    failure_path = runbook[start:end]
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()

    behavior = subprocess.run(
        ["bash", "-seu", "--"],
        input=(
            f"evidence_dir={shlex.quote(str(evidence_dir))}\n"
            "kubeconfig=/dev/null\n"
            "smoke_namespace=loom-runtime-smoke\n"
            "manifest=/dev/null\n"
            "pod=gvisor-smoke-2\n"
            "node=trt-eai-oldlab-2\n"
            "smoke_image=example.invalid/smoke@sha256:"
            + "3" * 64
            + "\n"
            + "merged_source_sha="
            + "5" * 40
            + "\n"
            + "profile_label_a="
            + "6" * 32
            + "\n"
            + "profile_label_b="
            + "7" * 32
            + "\n"
            + "kubectl() {\n"
            + '  case " $* " in\n'
            + '    *" create "*) return 0 ;;\n'
            + '    *" wait "*) return 1 ;;\n'
            + '    *" get pod/gvisor-smoke-2 "*) '
            + 'printf \'%s\\n\' \'{"status":{"phase":"Failed"}}\' ;;\n'
            + '    *" logs "*) printf "gvisor failure\\n" ;;\n'
            + "    *) return 1 ;;\n"
            + "  esac\n"
            + "}\n"
            + failure_path
            + "\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert behavior.returncode != 0
    assert (evidence_dir / "gvisor-smoke-2.log").read_text(encoding="utf-8") == "gvisor failure\n"


def test_personal_dev_builder_runtime_longhorn_health_requires_live_readiness() -> None:
    runbook = _read("docs/runbooks/personal-dev-builder-runtime.md")
    jq_filters = re.findall(r"\bjq\b[^']*'(.*?)'", runbook, flags=re.DOTALL)
    controller_filters = [
        value
        for value in jq_filters
        if 'select(.kind == "Deployment")' in value and "desiredNumberScheduled" in value
    ]
    pod_filters = [
        value
        for value in jq_filters
        if '.status.phase != "Succeeded"' in value
        and '.status.phase != "Failed"' in value
        and "containerStatuses" in value
    ]
    assert len(controller_filters) == 1
    assert len(pod_filters) == 1

    def evaluate(jq_filter: str, payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["jq", "-e", jq_filter],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )

    healthy_controllers: dict[str, object] = {
        "items": [
            {
                "kind": "Deployment",
                "metadata": {"generation": 4},
                "spec": {"replicas": 3},
                "status": {
                    "availableReplicas": 3,
                    "observedGeneration": 4,
                    "readyReplicas": 3,
                    "updatedReplicas": 3,
                },
            },
            {
                "kind": "DaemonSet",
                "metadata": {"generation": 7},
                "status": {
                    "desiredNumberScheduled": 5,
                    "numberAvailable": 5,
                    "numberReady": 5,
                    "observedGeneration": 7,
                    "updatedNumberScheduled": 5,
                },
            },
        ]
    }
    assert evaluate(controller_filters[0], healthy_controllers).returncode == 0
    deployment_only = {"items": healthy_controllers["items"][:1]}
    assert evaluate(controller_filters[0], deployment_only).returncode != 0
    daemon_set_only = {"items": healthy_controllers["items"][1:]}
    assert evaluate(controller_filters[0], daemon_set_only).returncode != 0
    degraded_controllers = json.loads(json.dumps(healthy_controllers))
    degraded_controllers["items"][0]["status"]["readyReplicas"] = 2
    assert evaluate(controller_filters[0], degraded_controllers).returncode != 0
    degraded_daemon_set = json.loads(json.dumps(healthy_controllers))
    degraded_daemon_set["items"][1]["status"]["numberReady"] = 4
    assert evaluate(controller_filters[0], degraded_daemon_set).returncode != 0

    ready_with_history: dict[str, object] = {
        "items": [
            {
                "status": {
                    "containerStatuses": [{"ready": True}],
                    "phase": "Running",
                }
            },
            {"status": {"phase": "Failed", "reason": "Evicted"}},
            {"status": {"phase": "Succeeded"}},
        ]
    }
    assert evaluate(pod_filters[0], ready_with_history).returncode == 0
    not_ready = json.loads(json.dumps(ready_with_history))
    not_ready["items"][0]["status"]["containerStatuses"][0]["ready"] = False
    assert evaluate(pod_filters[0], not_ready).returncode != 0
    pending_with_ready = json.loads(json.dumps(ready_with_history))
    pending_with_ready["items"].append({"status": {"phase": "Pending"}})
    assert evaluate(pod_filters[0], pending_with_ready).returncode != 0
    terminal_only = {"items": ready_with_history["items"][1:]}
    assert evaluate(pod_filters[0], terminal_only).returncode != 0


def test_personal_dev_builder_runtime_runbook_is_indexed_and_architecture_linked() -> None:
    for document in (
        _read("docs/runbooks/README.md"),
        _read("deploy/dev-fleet/README.md"),
        _read("docs/architecture/personal-dev-management-plane-deployment.md"),
    ):
        assert "personal-dev-builder-runtime.md" in document


def test_release_bound_scanner_cache_is_architecture_linked_and_inert() -> None:
    management = _read("docs/architecture/personal-dev-management-plane-deployment.md")
    environments = _read("docs/architecture/multi-dev-environments.md")
    fleet = _read("deploy/dev-fleet/README.md")

    for document in (management, environments, fleet):
        lowered = document.casefold()
        assert "release-bound scanner cache" in lowered
        assert "personal_dev_scanner_cache" in document
        assert "ceiling" in lowered and "zero" in lowered
    for document in (management, environments):
        assert "personal-dev-scanner-cache-preparation.md" in document
    assert "remain separate operational gates" in " ".join(management.casefold().split())
    assert "remain separate operational gates" in " ".join(fleet.casefold().split())


def test_scanner_cache_design_states_the_pod_scoped_network_boundary() -> None:
    design = _read("docs/architecture/personal-dev-scanner-cache-preparation.md")
    normalized = " ".join(design.split())

    assert "Kubernetes NetworkPolicy is Pod-scoped" in normalized
    assert "does not have a separate network namespace" in normalized
    assert "performs no runtime network operation" in normalized
    assert "no network authority" not in design
