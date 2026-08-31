from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "docs/runbooks/personal-dev-native-builder-runtime.md"
ACCEPTANCE = ROOT / "docs/runbooks/personal-dev-native-builder-acceptance.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _shell(document: str) -> str:
    return "\n".join(re.findall(r"```bash\n(.*?)\n```", document, flags=re.DOTALL))


def _normalized(document: str) -> str:
    return " ".join(document.split())


def _shell_function(document: str, name: str) -> str:
    shell = _shell(document)
    marker = f"{name}() {{\n"
    start = shell.index(marker)
    end = shell.index("\n}\n", start) + 2
    return shell[start:end]


def test_native_authority_transport_pins_root_ssh_and_preserves_client_frame(
    tmp_path: Path,
) -> None:
    """Catches a root transport that trusts repository SSH config or alters its client frame."""
    request = _shell_function(_read(RUNTIME), "native_authority_request")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sudo_log = tmp_path / "sudo"
    client_args = tmp_path / "client.args"
    ssh_args = tmp_path / "ssh.args"
    ssh_stdin = tmp_path / "ssh.stdin"
    fake_sudo = fake_bin / "sudo"
    fake_sudo.write_text(
        "#!/bin/sh\n"
        'log="$(mktemp "$AUTHORITY_SUDO_LOG.XXXXXX")"\n'
        'printf "%s\\n" "$@" > "$log"\n'
        'test "$1" = -n && shift\n'
        'test "$1" = -- && shift\n'
        'if test "$1" = /usr/bin/ssh; then shift; exec "$AUTHORITY_FAKE_SSH" "$@"; fi\n'
        'exec "$@"\n',
        encoding="utf-8",
    )
    fake_sudo.chmod(0o700)
    fake_client = fake_bin / "client"
    fake_client.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$@" > "$AUTHORITY_CLIENT_ARGS"\n'
        "printf 'LOOMNBR1client-frame'\n",
        encoding="utf-8",
    )
    fake_client.chmod(0o700)
    fake_ssh = fake_bin / "ssh"
    fake_ssh.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$@" > "$AUTHORITY_SSH_ARGS"\n'
        'cat > "$AUTHORITY_SSH_STDIN"\n'
        'request_id="$(sed -n \'/--request-id/{n;p;q;}\' "$AUTHORITY_CLIENT_ARGS")"\n'
        "printf '%s' '{\"state_sha256\":\"\",\"state\":null,\"schema\":\"loom-personal-dev-native-builder-runtime-authority-receipt.v1\",\"runtime_profile_sha256\":\"'\"$AUTHORITY_PROFILE\"'\",\"request_id\":\"'\"$request_id\"'\",\"phase\":\"inert\",\"operation\":\"status\",\"nft_table\":\"absent\",\"managed_networks\":null,\"managed_containers\":0,\"host_name\":\"gx10-01c7\",\"executable_new_capacity\":0,\"dockerd_service\":\"inactive\",\"authority_source_tree\":\"'\"$AUTHORITY_TREE\"'\",\"authority_source_sha\":\"'\"$AUTHORITY_SHA\"'\",\"architecture\":\"aarch64\",\"agent_service\":\"inactive\"}'\n",
        encoding="utf-8",
    )
    fake_ssh.chmod(0o700)
    receipt = tmp_path / "status.json"
    source_sha = "a" * 40
    source_tree = "b" * 40
    profile_sha = "c" * 64
    request_id = "123e4567-e89b-42d3-a456-426614174000"
    behavior = subprocess.run(
        ["bash", "-seu", "--", str(fake_client), str(receipt), request_id],
        input=(
            f'PATH="{fake_bin}:$PATH"\n'
            f'AUTHORITY_SUDO_LOG="{sudo_log}"\n'
            f'AUTHORITY_FAKE_SSH="{fake_ssh}"\n'
            f'AUTHORITY_CLIENT_ARGS="{client_args}"\n'
            f'AUTHORITY_SSH_ARGS="{ssh_args}"\n'
            f'AUTHORITY_SSH_STDIN="{ssh_stdin}"\n'
            f'AUTHORITY_SHA="{source_sha}"\n'
            f'AUTHORITY_TREE="{source_tree}"\n'
            f'AUTHORITY_PROFILE="{profile_sha}"\n'
            "export AUTHORITY_SUDO_LOG AUTHORITY_FAKE_SSH AUTHORITY_CLIENT_ARGS "
            "AUTHORITY_SSH_ARGS AUTHORITY_SSH_STDIN AUTHORITY_SHA AUTHORITY_TREE "
            "AUTHORITY_PROFILE\n"
            'native_authority_client=("$1")\n'
            f'authority_source_sha="{source_sha}"\n'
            f'authority_source_tree="{source_tree}"\n'
            f'runtime_profile_sha256="{profile_sha}"\n'
            + request
            + "\nprintf 'caller-stdin-must-not-replace-the-client-frame' | "
            + 'native_authority_request status "$3" "$2"\n'
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert behavior.returncode == 0, behavior.stderr
    assert behavior.stdout == ""
    assert ssh_stdin.read_bytes() == b"LOOMNBR1client-frame"
    assert ssh_args.read_text(encoding="utf-8").splitlines() == [
        "-F",
        "/dev/null",
        "-o",
        "HostName=192.168.20.12",
        "-o",
        "Port=22",
        "-o",
        "User=qianyi",
        "-o",
        "IdentityFile=/var/lib/loom-staging-rollout/gb10-deploy-ed25519",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "PubkeyAuthentication=yes",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "KbdInteractiveAuthentication=no",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "UpdateHostKeys=no",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ConnectTimeout=10",
        "-o",
        'ProxyCommand=/usr/bin/ssh -F /dev/null -o HostName=207.35.188.227 -o Port=2221 -o User=qianyi -o IdentityFile=/var/lib/loom-staging-rollout/gb10-deploy-ed25519 -o IdentitiesOnly=yes -o PubkeyAuthentication=yes -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts -o GlobalKnownHostsFile=/dev/null -o UpdateHostKeys=no -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -W "[%h]:%p" trt-gb10-1',
        "trt-gb10-2",
        "sudo -n -- /usr/local/libexec/loom-personal-dev-native-builder-runtime-authority",
    ]
    assert client_args.read_text(encoding="utf-8").splitlines() == [
        "status",
        "--authority-source-sha",
        source_sha,
        "--authority-source-tree",
        source_tree,
        "--request-id",
        request_id,
        "--runtime-profile-sha256",
        profile_sha,
        "--schema-version",
        "1",
    ]
    sudo_invocations = sorted(tmp_path.glob("sudo.*"))
    assert len(sudo_invocations) == 1
    assert sudo_invocations[0].read_text(encoding="utf-8").splitlines()[0:3] == [
        "-n",
        "--",
        "/usr/bin/ssh",
    ]
    expected_receipt = {
        "agent_service": "inactive",
        "architecture": "aarch64",
        "authority_source_sha": source_sha,
        "authority_source_tree": source_tree,
        "dockerd_service": "inactive",
        "executable_new_capacity": 0,
        "host_name": "gx10-01c7",
        "managed_containers": 0,
        "managed_networks": None,
        "nft_table": "absent",
        "operation": "status",
        "phase": "inert",
        "request_id": request_id,
        "runtime_profile_sha256": profile_sha,
        "schema": "loom-personal-dev-native-builder-runtime-authority-receipt.v1",
        "state": None,
        "state_sha256": "",
    }
    assert receipt.read_text(encoding="utf-8") == json.dumps(
        expected_receipt, sort_keys=True, separators=(",", ":")
    )


def test_native_builder_acceptance_rejects_lifecycle_requests_before_transport(
    tmp_path: Path,
) -> None:
    """Catches the acceptance-only status boundary growing a broker mutation route."""
    request = _shell_function(_read(ACCEPTANCE), "native_authority_request")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    client_invoked = tmp_path / "client-invoked"
    ssh_invoked = tmp_path / "ssh-invoked"
    fake_sudo = fake_bin / "sudo"
    fake_sudo.write_text(
        "#!/bin/sh\n"
        'touch "$AUTHORITY_SSH_INVOKED"\n'
        "cat >/dev/null\n",
        encoding="utf-8",
    )
    fake_sudo.chmod(0o700)
    request_id = "123e4567-e89b-42d3-a456-426614174000"
    behavior = subprocess.run(
        ["bash", "-su", "--", str(client_invoked), str(ssh_invoked), request_id],
        input=(
            f'PATH="{fake_bin}:$PATH"\n'
            'AUTHORITY_CLIENT_INVOKED="$1"\n'
            'AUTHORITY_SSH_INVOKED="$2"\n'
            "export AUTHORITY_CLIENT_INVOKED AUTHORITY_SSH_INVOKED\n"
            "native_authority_client=(/bin/sh -c 'touch \"$AUTHORITY_CLIENT_INVOKED\"')\n"
            'authority_source_sha="' + "a" * 40 + '"\n'
            'authority_source_tree="' + "b" * 40 + '"\n'
            'runtime_profile_sha256="' + "c" * 64 + '"\n'
            + request
            + '\nnative_authority_request prepare "$3" /dev/null\n'
            + "request_status=$?\n"
            + 'test "$request_status" -ne 0\n'
            + 'test ! -e "$AUTHORITY_CLIENT_INVOKED"\n'
            + 'test ! -e "$AUTHORITY_SSH_INVOKED"\n'
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert behavior.returncode == 0, behavior.stderr


def test_native_authority_transport_preflight_validates_checked_in_gb10_topology() -> None:
    """Catches omitting the checked-in GB10 topology check before root SSH runs."""
    validators = [
        _shell_function(_read(runbook), "validate_native_authority_transport_config")
        for runbook in (RUNTIME, ACCEPTANCE)
    ]
    assert validators[0] == validators[1]
    for validator in validators:
        behavior = subprocess.run(
            ["bash", "-seu", "--", str(ROOT)],
            input=(
                'repository_root="$1"\n'
                + validator
                + "\nvalidate_native_authority_transport_config\n"
            ),
            text=True,
            capture_output=True,
            check=False,
        )
        assert behavior.returncode == 0, behavior.stderr


def test_native_builder_runbooks_leave_no_direct_gb10_privilege_or_scp_path() -> None:
    """Catches a privileged GB10 shell, SCP, or Docker escape around the fixed broker."""
    combined = _read(RUNTIME) + "\n" + _read(ACCEPTANCE)
    shell = _shell(combined)

    assert re.search(r"(?im)^\s*scp\b", shell) is None
    assert 'ssh_run "$gb10_target"' not in shell
    assert re.search(
        r'(?im)^\s*ssh_run\b[^\n]*(?:sudo|systemctl|nft|docker|python)',
        shell,
    ) is None


def test_native_builder_runbooks_bind_cli_to_exact_checkout(tmp_path: Path) -> None:
    runtime = _read(RUNTIME)
    acceptance = _read(ACCEPTANCE)
    runtime_cli = _shell_function(runtime, "loom_cli")
    acceptance_cli = _shell_function(acceptance, "loom_cli")
    assert runtime_cli == acceptance_cli

    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s|%s|' \"$PYTHONPATH\" \"$PYTHONNOUSERSITE\"\n"
        "printf '<%s>' \"$@\"\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o700)
    behavior = subprocess.run(
        ["bash", "-seu", "--", str(fake_python)],
        input=(
            'repository_root="/exact/release"\n'
            'loom_python="$1"\n'
            + runtime_cli
            + "\n"
            + "loom_cli admin personal-dev-control-plane status\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert behavior.returncode == 0, behavior.stderr
    assert behavior.stdout == (
        "/exact/release/src|1|<-m><loom_cli><admin>"
        "<personal-dev-control-plane><status>"
    )
    for runbook in (runtime, acceptance):
        assert "/.venv/bin/loom" not in _shell(runbook)


def test_native_builder_runbooks_reject_cli_module_provenance_drift(
    tmp_path: Path,
) -> None:
    verify_source = _shell_function(_read(RUNTIME), "verify_loom_cli_source")
    assert verify_source == _shell_function(
        _read(ACCEPTANCE), "verify_loom_cli_source"
    )
    release = tmp_path / "release"
    poison = tmp_path / "poison"
    for root in (release, poison):
        for package in ("loom", "loom_cli"):
            package_root = root / "src" / package
            package_root.mkdir(parents=True, exist_ok=True)
            (package_root / "__init__.py").write_text("", encoding="utf-8")

    accepted = subprocess.run(
        ["bash", "-seu", "--", str(release), str(poison)],
        input=(
            'repository_root="$1"\n'
            f'loom_python="{sys.executable}"\n'
            'export PYTHONPATH="$2/src"\n'
            + verify_source
            + "\nverify_loom_cli_source\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert accepted.returncode == 0, accepted.stderr

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "__init__.py").write_text("", encoding="utf-8")
    (release / "src" / "loom_cli" / "__init__.py").unlink()
    (release / "src" / "loom_cli" / "__init__.py").symlink_to(
        outside / "__init__.py"
    )
    rejected = subprocess.run(
        ["bash", "-seu", "--", str(release)],
        input=(
            'repository_root="$1"\n'
            f'loom_python="{sys.executable}"\n'
            + verify_source
            + "\nverify_loom_cli_source\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert rejected.returncode != 0


def test_native_builder_runtime_binds_exact_release_and_owner_only_evidence() -> None:
    runbook = _read(RUNTIME)

    assert "set -euo pipefail" in runbook
    assert "umask 077" in runbook
    assert "merged_source_sha='<merged-40-lowercase-hex>'" in runbook
    assert "trusted_release_sha256='<trusted-release-64-lowercase-hex>'" in runbook
    assert "previous_trusted_release_sha256='<previous-trusted-release-64-lowercase-hex-or-empty>'" in runbook
    assert "c193873a276ace659a27ff9318d4b8322b487f83a68f5d100d18bc6935eb477d" in runbook
    assert (
        "dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff5292"
        "d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7"
    ) in runbook
    assert 'test "$(git rev-parse HEAD)" = "$merged_source_sha"' in runbook
    assert 'test -z "$(git status --porcelain=v1 --untracked-files=all)"' in runbook
    assert 'sha256sum "$trusted_release"' in runbook
    assert 'sha256sum "$previous_trusted_release"' in runbook
    assert "previous_trusted_release_sha256:$previous_release" in runbook
    assert runbook.index('test ! -L "$path"') < runbook.index(
        'jq -er .source_sha "$trusted_release"'
    )
    assert runbook.index('test ! -L "$previous_trusted_release"') < runbook.index(
        'jq -er .source_sha "$previous_trusted_release"'
    )
    assert 'test "$(stat -c %a "$evidence_root")" = 700' in runbook
    assert 'case "$evidence_root/" in' in runbook
    assert '"$repository_root"/*) exit 1 ;;' in runbook
    assert 'install -d -m 0700 "$evidence_dir"' in runbook
    assert runbook.count('> "$evidence_dir/immutable-inputs.json"') == 1


def test_native_builder_runtime_validates_protected_material_as_root() -> None:
    runbook = _read(RUNTIME)
    try:
        validator = _shell_function(runbook, "validate_protected_material_metadata")
    except ValueError:
        validator = ""

    assert validator.startswith("validate_protected_material_metadata() {")
    assert "sudo /bin/sh -euc" in validator
    assert 'test "$(stat -c %u "$key")" = 0' in validator
    assert 'test "$(stat -c %g "$key")" = 0' in validator
    assert 'test "$(stat -c %a "$key")" = 400' in validator
    assert 'test "$(stat -c %s "$key")" = 32' in validator
    assert 'test "$(stat -c %h "$key")" = 1' in validator
    assert 'test "$(stat -c %u "$ca")" = 0' in validator
    assert 'test "$(stat -c %g "$ca")" = 0' in validator
    assert 'test "$(stat -c %a "$ca")" = 444' in validator
    assert 'test "$(stat -c %h "$ca")" = 1' in validator
    assert 'sh "$agent_private_key" "$service_ca"' in validator


def test_native_builder_runtime_orders_receipt_bound_inert_stage_before_activation() -> None:
    """Catches a runbook that activates before the broker has sealed the inert stages."""
    runbook = _read(RUNTIME)
    normalized = _normalized(runbook)

    milestones = (
        "runtime-prepare.json",
        "agent-stage.json",
        "native-builder-public-key.apply.txt",
        "runtime-activate.json",
        "after-host.json",
        "signed-zero-grant-readiness",
    )
    offsets = [runbook.index(milestone) for milestone in milestones]
    assert offsets == sorted(offsets)
    assert '.phase == "prepared" and .agent_service == "inactive"' in runbook
    assert '.phase == "staged" and .agent_service == "inactive"' in runbook
    assert '.phase == "active" and .agent_service == "active"' in runbook
    assert "current/previous image convergence" in runbook
    assert "docker image prune" not in normalized
    assert "docker system prune" not in normalized


def test_native_builder_runtime_captures_exact_read_only_boundaries() -> None:
    runbook = _read(RUNTIME)
    normalized = _normalized(runbook)

    for evidence in (
        "before-host.json",
        "after-host.json",
        "before-slurm.json",
        "after-slurm.json",
        "before-database-counts.json",
        "after-database-counts.json",
        "before-namespaces.json",
        "after-namespaces.json",
        "before-capacity.status.json",
        "after-capacity.status.json",
        "public-store-dns.json",
    ):
        assert evidence in runbook
    assert "scontrol show nodes --json" in normalized
    assert "squeue --json" in normalized
    assert "personal_dev_native_build_grants" in runbook
    assert "workers" in runbook
    assert "tasks" in runbook
    assert "getent ahostsv4" in normalized or "getent ahostsv6" in normalized
    assert "public_store_endpoint_cidrs" in runbook
    assert 'test "$observed_public_store_cidrs" = "$reviewed_public_store_cidrs"' in runbook
    assert '"manager_ceiling":0' in runbook
    assert '"worker_available":false' in runbook


def test_native_builder_runtime_normalizes_public_dns_cidrs() -> None:
    runbook = _read(RUNTIME)
    try:
        normalizer = _shell_function(runbook, "normalize_public_store_cidrs")
    except ValueError:
        normalizer = ""
    behavior = subprocess.run(
        ["bash", "-seu"],
        input=(
            normalizer
            + "\nprintf '%s\\n' '207.35.188.227' "
            + "'::ffff:207.35.188.227' '2606:4700:4700::1111' "
            + "'2606:4700:4700::1111' | normalize_public_store_cidrs\n"
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert behavior.returncode == 0, behavior.stderr
    assert behavior.stdout == (
        "207.35.188.227/32\n2606:4700:4700::1111/128\n"
    )


def test_native_builder_runtime_rejects_non_global_dns_addresses() -> None:
    normalizer = _shell_function(
        _read(RUNTIME), "normalize_public_store_cidrs"
    )

    for address in ("10.0.0.1", "::ffff:10.0.0.1", "fd00::1"):
        behavior = subprocess.run(
            ["bash", "-seu", "--", address],
            input=(
                normalizer
                + "\nprintf '%s\\n' \"$1\" | normalize_public_store_cidrs\n"
            ),
            text=True,
            capture_output=True,
            check=False,
        )

        assert behavior.returncode != 0, address


def test_native_builder_runtime_delegates_fixed_kvm_gvisor_conformance_to_broker() -> None:
    """Catches returning an operator-controlled Docker conformance shell to the runbook."""
    runbook = _read(RUNTIME)
    normalized = _normalized(runbook)

    assert "linux/arm64" in runbook
    assert "fixed KVM-gVisor conformance asset" in runbook
    assert "two-container conformance asset" in runbook
    assert "two-container-conformance" not in runbook
    assert "docker_native=" not in runbook
    assert "docker_primary=" not in runbook
    assert "ssh_run \"$gb10_target\"" not in runbook
    assert "no qemu" in normalized.casefold()
    assert "no runc fallback" in normalized.casefold()
    assert "tonistiigi/binfmt" not in normalized.casefold()


def test_native_builder_runbooks_contain_no_forbidden_mutation_or_secret_capture() -> None:
    combined = _read(RUNTIME) + "\n" + _read(ACCEPTANCE)
    shell = _shell(combined)
    normalized = _normalized(shell).casefold()

    forbidden = (
        r"kubectl\s+[^\n]*delete\s+(?:ns|namespace)",
        r"\bsbatch\b",
        r"\bscancel\b",
        r"\bsalloc\b",
        r"\bsrun\b",
        r"\bscontrol\s+(?:update|create|delete|reconfigure|hold|release|suspend|resume|requeue)",
        r"\bloom\s+run\b",
        r"\bloom\s+eval\b",
        r"\bloom\s+batch\b",
        r"docker\s+(?:image|system|builder|network|container|volume)?\s*prune",
        r"systemctl\s+(?:restart|stop)\s+docker(?:\.service)?",
        r"kubectl\s+[^\n]*get\s+secret[^\n]*(?:-o|--output)[= ](?:json|yaml)",
        r"(?:cat|base64|xxd|hexdump)\s+[^\n]*(?:private|secret|token|credential|kubeconfig)",
    )
    for pattern in forbidden:
        assert re.search(pattern, shell, flags=re.IGNORECASE) is None, pattern
    assert "no task submission" in combined.casefold()
    assert "no slurm mutation" in combined.casefold()
    assert "secret values" in combined.casefold()
    assert "--token" not in normalized
    assert "authorization:" not in normalized


def test_native_builder_acceptance_proves_concurrent_native_platforms_and_routes() -> None:
    runbook = _read(ACCEPTANCE)
    normalized = _normalized(runbook)

    assert "owner_0_xdg='<absolute-mode-0700-owner-0-xdg-config-root>'" in runbook
    assert "owner_1_xdg='<absolute-mode-0700-owner-1-xdg-config-root>'" in runbook
    assert "owner_0_source='<absolute-owner-0-source-root>'" in runbook
    assert "owner_1_source='<absolute-owner-1-source-root>'" in runbook
    assert 'test "$(realpath -e "$owner_0_xdg")" != "$(realpath -e "$owner_1_xdg")"' in runbook
    assert "owner_0_deploy_pid=$!" in runbook
    assert "owner_1_deploy_pid=$!" in runbook
    assert runbook.index("owner_1_deploy_pid=$!") < runbook.index('wait "$owner_0_deploy_pid"')
    assert 'wait "$owner_1_deploy_pid"' in runbook
    assert "linux/amd64" in runbook
    assert "linux/arm64" in runbook
    assert "two simultaneous amd64 Jobs" in runbook
    assert "two simultaneous arm64 grants" in normalized
    assert "runsc-personal-dev" in runbook
    assert "runsc-personal-dev-native" in runbook
    assert "docker buildx imagetools inspect" in normalized
    assert "application/vnd.oci.image.index.v1+json" in runbook
    assert "curl --fail" in normalized
    assert "probe_cross_owner_denial" in runbook
    assert '[[ "$owner_0_candidate" =~ ^[0-9a-f]{64}$ ]]' in runbook
    assert '[[ "$owner_1_candidate" =~ ^[0-9a-f]{64}$ ]]' in runbook
    assert '[[ "$route_host" =~ ^[a-z0-9]' in runbook
    assert "Run section 7 of the runtime runbook" in runbook


def test_native_builder_acceptance_activates_after_agent_and_cleans_up_through_owner_api() -> None:
    runbook = _read(ACCEPTANCE)
    normalized = _normalized(runbook)

    agent_active = runbook.index("agent-active-pre-management.json")
    management_apply = runbook.index('kubectl --kubeconfig "$kubeconfig" apply --server-side')
    readiness = runbook.index("signed-zero-grant-readiness")
    assert agent_active < management_apply < readiness
    assert 'XDG_CONFIG_HOME="$owner_0_xdg" loom_cli dev destroy "$owner_0_name"' in normalized
    assert 'XDG_CONFIG_HOME="$owner_1_xdg" loom_cli dev destroy "$owner_1_name"' in normalized
    assert "--format json" in normalized
    assert 'cmp -s "$shadow_recheck_after" "$rollback_shadow_manifest"' in runbook
    assert runbook.index('cmp -s "$shadow_recheck_after" "$rollback_shadow_manifest"') < (
        runbook.index("verify-acceptance-result")
    )
    assert "final-zero-grants.json" in runbook
    assert "final-zero-namespaces.json" in runbook
    assert "final-zero-workers.json" in runbook
    assert "final-zero-tasks.json" in runbook
    assert "final-capacity.status.json" in runbook
    assert '"manager_ceiling":0' in runbook
    assert '"worker_available":false' in runbook
    assert "executable-new-capacity ceiling remains exactly `0`" in runbook


def test_native_builder_acceptance_orders_temporary_authority_before_durable_operations() -> None:
    runbook = _read(ACCEPTANCE)

    milestones = (
        "agent-active-pre-management.json",
        "render-acceptance",
        "native-acceptance.server-side-apply.txt",
        "signed-zero-grant-readiness",
        "owner_0_deploy_pid=$!",
        "owner_0_update_pid=$!",
        "probe_cross_owner_denial \"$owner_1_xdg\" \"$owner_1_candidate\" \"$owner_0_xdg\" \"$owner_0_name\" 1 0 destroy",
        "owner-1.final-destroyed.json",
        "rollback.server-side-apply.txt",
        "rollback-shadow.status.json",
        'schema:"loom-personal-dev-zero-capacity-acceptance-result-v3"',
        "verify-acceptance-result",
        'operational_plan="$evidence_dir/native-operational-plan.json"',
        "render-operational",
        "native-operational.server-side-apply.txt",
        "final-operational.status.json",
    )
    offsets = [runbook.index(milestone) for milestone in milestones]
    assert offsets == sorted(offsets)
    verification_offset = runbook.index("verify-acceptance-result")
    assert "render-operational" not in runbook[:verification_offset]
    assert "native-operational.server-side-apply.txt" not in runbook[:verification_offset]
    assert runbook.count("jq -cS -j") >= 2
    assert ".native == true" in runbook


def test_native_builder_acceptance_canonicalizes_loader_inputs_without_newlines() -> None:
    runbook = _read(ACCEPTANCE)

    raw_status = runbook.index('rollback_status_raw="$evidence_dir/rollback-shadow.status.raw.json"')
    canonical_status = runbook.index(
        'jq -cS -j . "$rollback_status_raw" > "$rollback_status"'
    )
    verification = runbook.index("verify-acceptance-result")
    assert raw_status < canonical_status < verification
    assert 'assert_canonical_json "$rollback_status"' in runbook
    assert 'assert_canonical_json_line "$acceptance_verification"' in runbook


def test_native_builder_runbooks_seal_sanitized_evidence_and_exact_rollback() -> None:
    runtime = _read(RUNTIME)
    acceptance = _read(ACCEPTANCE)

    assert "rollback to the exact inert shadow" in runtime.casefold()
    assert "rollback-shadow.status.json" in runtime
    assert 'cmp -s "$rollback_shadow_recheck" "$rollback_shadow_manifest"' in runtime
    assert "fixed `remove` transition stops the agent before the dedicated daemon" in runtime
    assert "removes only byte-identical managed runtime files" in _normalized(runtime).casefold()
    assert "dedicated image cache and system identities are retained" in _normalized(
        runtime
    ).casefold()
    assert runtime.index('remove_request_id="$(new_native_authority_request_id)"') > runtime.index(
        'kubectl --kubeconfig "$kubeconfig" apply --server-side'
    )
    assert acceptance.index("verify-acceptance-result") < acceptance.index("render-operational")
    for runbook in (runtime, acceptance):
        assert "evidence-index.sha256" in runbook
        assert "sha256sum" in runbook
        assert "LC_ALL=C sort" in runbook
        assert "secret values are never" in runbook.casefold()
