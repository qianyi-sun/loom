"""The Protocol itself isn't directly testable, but we can verify constant
values and helper structures live in the right place."""

from loom.driver.base import (
    MAX_EXEC_STREAM_BYTES,
    Driver,
    StartOptions,
)


def test_max_exec_stream_bytes_default():
    """Spec §2.2 + §4.9: 10 MB default."""
    assert MAX_EXEC_STREAM_BYTES == 10 * 1024 * 1024


def test_start_options_defaults():
    o = StartOptions()
    assert o.force_build is False
    assert o.pull is True


def test_driver_protocol_method_surface():
    """Driver is @runtime_checkable; verify the documented method set is exposed."""
    expected_methods = {
        "start", "stop", "exec", "upload", "download",
        "set_network_policy", "run_healthcheck",
    }
    assert expected_methods.issubset(set(dir(Driver)))
