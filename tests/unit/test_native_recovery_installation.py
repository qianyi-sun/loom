"""Restricted node transport has no ordinary SSH or sudo command authority."""

from importlib import import_module

import pytest


def spec(module, **changes):
    return module.NativeRecoveryEndpointV1(**(dict(address="192.0.2.10", port=2222,
        release_root="/opt/loom-native-recovery/releases/" + "a" * 64,
        python="/opt/loom-native/python/bin/python3", policy="/etc/loom-native-recovery/policy.json",
        policy_sha256="b" * 64, host_key="/etc/loom-native-recovery/ssh_host_ed25519_key",
        authorized_keys="/etc/loom-native-recovery/authorized_keys") | changes))


def test_endpoint_renders_separate_fixed_sshd_and_exact_noarg_sudo():
    module = import_module("loom_capacity_executor.native_recovery_installation")
    configured = spec(module)
    assets = module.render_native_recovery_endpoint(configured)
    sshd = assets.sshd_config.decode()
    for line in ("AllowUsers loom-native-recovery", "AuthenticationMethods publickey", "PasswordAuthentication no",
        "KbdInteractiveAuthentication no", "PermitRootLogin no", "PermitUserEnvironment no", "PermitUserRC no",
        "DisableForwarding yes", "PermitTTY no", "UsePAM no", "MaxSessions 1"):
        assert line in sshd.splitlines()
    assert "Include " not in sshd and "AcceptEnv " not in sshd and "Subsystem " not in sshd
    assert "ForceCommand " + configured.release_root + "/ssh-entry" in sshd
    gate = assets.ssh_entry.decode()
    assert 'SSH_ORIGINAL_COMMAND' in gate and 'exec /usr/bin/sudo -n -- ' + configured.release_root + '/helper' in gate
    assert 'NOSETENV:' in assets.sudoers.decode() and configured.release_root + '/helper ""' in assets.sudoers.decode()
    helper = assets.helper.decode()
    # Root must not create bytecode in the closed protected import tree.
    assert helper.startswith("#!" + configured.python + " -IB\n")
    assert configured.policy_sha256 in helper and "run_native_recovery_helper" in helper
    compile(helper, "fixed-node-helper", "exec")


@pytest.mark.parametrize("changes", [dict(address="example.com"), dict(address="0.0.0.0"), dict(port=22),
    dict(release_root="/opt/test;command"), dict(python="/a b/python"), dict(policy="/etc/a\nForceCommand evil"),
    dict(host_key="relative"), dict(authorized_keys="/etc/../keys")])
def test_endpoint_rejects_injection_and_shared_ssh_port(changes):
    module = import_module("loom_capacity_executor.native_recovery_installation")
    with pytest.raises(ValueError):
        spec(module, **changes)


def test_generated_assets_parse_with_installed_shell_sudo_and_sshd(tmp_path):
    import shutil
    import subprocess

    paths = [shutil.which(name) for name in ("sshd", "ssh-keygen", "visudo")]
    if any(path is None for path in paths):
        pytest.skip("OS SSH/sudo syntax tools unavailable; real endpoint remains an installation check")
    sshd, keygen, visudo = paths
    module = import_module("loom_capacity_executor.native_recovery_installation")
    host_key = tmp_path / "host_key"
    subprocess.run([keygen, "-q", "-t", "ed25519", "-N", "", "-f", str(host_key)], check=True, timeout=5)
    assets = module.render_native_recovery_endpoint(spec(module, host_key=str(host_key)))
    config, gate, sudoers = tmp_path / "sshd_config", tmp_path / "entry", tmp_path / "sudoers"
    config.write_bytes(assets.sshd_config)
    gate.write_bytes(assets.ssh_entry)
    sudoers.write_bytes(assets.sudoers)
    subprocess.run(["/bin/sh", "-n", str(gate)], check=True, timeout=5, capture_output=True)
    result = subprocess.run([visudo, "-cf", str(sudoers)], timeout=5, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    result = subprocess.run([sshd, "-T", "-f", str(config)], timeout=5, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    effective = result.stdout.splitlines()
    for expected in ("permitrootlogin no", "permituserenvironment no", "disableforwarding yes", "permittty no",
        "passwordauthentication no", "kbdinteractiveauthentication no", "authenticationmethods publickey"):
        assert expected in effective
