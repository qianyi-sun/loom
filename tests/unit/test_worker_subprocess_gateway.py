from loom_worker.main_loop import _sandbox_extra_hosts_for_url


def test_host_docker_internal_subprocess_gateway_adds_host_gateway_alias() -> None:
    assert _sandbox_extra_hosts_for_url(
        "http://host.docker.internal:30443/openai/v1"
    ) == (("host.docker.internal", "host-gateway"),)


def test_regular_subprocess_gateway_url_needs_no_extra_hosts() -> None:
    assert _sandbox_extra_hosts_for_url("http://10.0.0.5:30443/openai/v1") == ()
    assert _sandbox_extra_hosts_for_url(None) == ()
