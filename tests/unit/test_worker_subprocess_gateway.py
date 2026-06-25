from loom_worker.main_loop import _host_cpu_arch, _sandbox_extra_hosts_for_url


def test_host_cpu_arch_normalizes_arm64_names(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("loom_worker.main_loop.platform.machine", lambda: "aarch64")
    assert _host_cpu_arch() == "arm64"

    monkeypatch.setattr("loom_worker.main_loop.platform.machine", lambda: "arm64")
    assert _host_cpu_arch() == "arm64"


def test_host_cpu_arch_defaults_to_x86_64(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("loom_worker.main_loop.platform.machine", lambda: "x86_64")
    assert _host_cpu_arch() == "x86_64"


def test_host_docker_internal_subprocess_gateway_adds_host_gateway_alias() -> None:
    assert _sandbox_extra_hosts_for_url(
        "http://host.docker.internal:30443/openai/v1"
    ) == (("host.docker.internal", "host-gateway"),)


def test_regular_subprocess_gateway_url_needs_no_extra_hosts() -> None:
    assert _sandbox_extra_hosts_for_url("http://10.0.0.5:30443/openai/v1") == ()
    assert _sandbox_extra_hosts_for_url(None) == ()
