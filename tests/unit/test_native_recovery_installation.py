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
    assert helper.startswith("#!" + configured.python + " -I\n")
    assert configured.policy_sha256 in helper and "run_native_recovery_helper" in helper
    compile(helper, "fixed-node-helper", "exec")


@pytest.mark.parametrize("changes", [dict(address="example.com"), dict(address="0.0.0.0"), dict(port=22),
    dict(release_root="/opt/test;command"), dict(python="/a b/python"), dict(policy="/etc/a\nForceCommand evil"),
    dict(host_key="relative"), dict(authorized_keys="/etc/../keys")])
def test_endpoint_rejects_injection_and_shared_ssh_port(changes):
    module = import_module("loom_capacity_executor.native_recovery_installation")
    with pytest.raises(ValueError):
        spec(module, **changes)
