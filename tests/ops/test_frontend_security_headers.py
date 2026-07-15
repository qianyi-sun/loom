from __future__ import annotations

from pathlib import Path

import pytest
from scripts.ops.frontend_security_headers import (
    CSP_POLICY,
    HSTS_VALUE,
    WEB_ORIGIN_HEADERS,
    HttpResponse,
    Probe,
    _parse_args,
    route_probes,
    run_probes,
    validate_probe_response,
    validate_security_headers,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SECURITY_INCLUDE = "/etc/nginx/loom-spa-security-headers.conf"


def _headers(*, include_hsts: bool = True) -> dict[str, str]:
    headers = dict(WEB_ORIGIN_HEADERS)
    if include_hsts:
        headers["strict-transport-security"] = HSTS_VALUE
    return headers


def test_exact_combined_security_policy_passes() -> None:
    response = HttpResponse(status=200, headers=_headers())

    assert validate_security_headers(response, require_hsts=True) == []


@pytest.mark.parametrize(
    ("header_name", "replacement", "error"),
    [
        (
            "content-security-policy",
            (CSP_POLICY, CSP_POLICY),
            "expected exactly one content-security-policy header",
        ),
        (
            "permissions-policy",
            "camera=(self), microphone=(), geolocation=()",
            "unexpected permissions-policy policy",
        ),
        (
            "strict-transport-security",
            None,
            "expected exactly one strict-transport-security header",
        ),
    ],
)
def test_policy_rejects_duplicate_mismatched_or_missing_headers(
    header_name: str,
    replacement: str | tuple[str, ...] | None,
    error: str,
) -> None:
    headers: dict[str, str | tuple[str, ...]] = _headers()
    if replacement is None:
        headers.pop(header_name)
    else:
        headers[header_name] = replacement

    assert error in validate_security_headers(
        HttpResponse(status=200, headers=headers),
        require_hsts=True,
    )


def test_web_origin_mode_does_not_claim_ingress_owned_hsts() -> None:
    response = HttpResponse(status=500, headers=_headers(include_hsts=False))

    assert validate_security_headers(response, require_hsts=False) == []
    assert "expected exactly one strict-transport-security header" in (
        validate_security_headers(response, require_hsts=True)
    )


def test_redirect_probe_requires_exact_status_location_and_policy() -> None:
    probe = Probe(
        label="staging_redirect",
        url="https://yylx.world/dev?next=%2Fmonitor&x=1",
        expected_status=308,
        expected_location="/dev/?next=%2Fmonitor&x=1",
    )
    response = HttpResponse(
        status=308,
        headers={**_headers(), "location": "/dev/?next=%2Fmonitor&x=1"},
    )

    assert validate_probe_response(probe, response, require_hsts=True) == []

    errors = validate_probe_response(
        probe,
        HttpResponse(
            status=200,
            headers={**_headers(), "location": "/dev/"},
        ),
        require_hsts=True,
    )
    assert "expected HTTP 308, received HTTP 200" in errors
    assert "unexpected redirect location" in errors


def test_route_expands_to_200_308_and_404_coverage() -> None:
    probes = route_probes("staging", "https://yylx.world/dev")

    assert [(probe.label, probe.expected_status) for probe in probes] == [
        ("staging_redirect", 308),
        ("staging_shell", 200),
        ("staging_config", 200),
        ("staging_missing_asset", 404),
    ]
    assert probes[0].url == "https://yylx.world/dev?next=%2Fmonitor&x=1"
    assert probes[0].expected_location == "/dev/?next=%2Fmonitor&x=1"
    assert probes[-1].url.endswith("/dev/assets/loom-security-header-smoke-missing.js")


def test_probe_report_never_contains_response_headers_or_body() -> None:
    probe = Probe(label="shell", url="https://example.test/dev/", expected_status=200)

    def fetcher(url: str, *, timeout: float) -> HttpResponse:
        assert url == probe.url
        assert timeout == 3
        return HttpResponse(status=200, headers=_headers())

    results = run_probes(
        [probe],
        timeout=3,
        require_hsts=True,
        fetcher=fetcher,
    )

    assert results[0].status == "pass"
    assert not hasattr(results[0], "headers")
    assert not hasattr(results[0], "body")


def test_web_origin_mode_is_limited_to_explicit_loopback_probes() -> None:
    args = _parse_args(
        [
            "--web-origin-only",
            "--probe",
            "web_500=500=http://127.0.0.1:18082/dev/security-header-5xx-probe",
        ],
    )

    assert args.web_origin_only is True

    with pytest.raises(SystemExit, match="2"):
        _parse_args(
            [
                "--web-origin-only",
                "--probe",
                "web_500=500=http://example.test/dev/security-header-5xx-probe",
            ],
        )


def test_nginx_policy_is_exact_and_uses_always() -> None:
    policy_path = REPO_ROOT / "deploy/nginx-spa-security-headers.conf"
    directives = [
        line.strip()
        for line in policy_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert directives == [
        f'add_header Content-Security-Policy "{CSP_POLICY}" always;',
        'add_header X-Content-Type-Options "nosniff" always;',
        'add_header Referrer-Policy "no-referrer" always;',
        'add_header Permissions-Policy "camera=(), microphone=(), geolocation=()" always;',
    ]
    assert "'unsafe-inline'" not in CSP_POLICY
    assert "'unsafe-eval'" not in CSP_POLICY
    assert "*" not in CSP_POLICY
    assert "http:" not in CSP_POLICY
    assert "https:" not in CSP_POLICY


def test_every_cache_header_location_reincludes_the_security_policy() -> None:
    config = (REPO_ROOT / "deploy/nginx-spa.conf").read_text(encoding="utf-8")
    include_directive = f"include {SECURITY_INCLUDE};"
    cache_location_blocks = [
        segment.split("\n    }", 1)[0]
        for segment in config.split("location ")[1:]
        if 'add_header Cache-Control "' in segment.split("\n    }", 1)[0]
    ]

    assert len(cache_location_blocks) == 6
    assert all(include_directive in block for block in cache_location_blocks)
    assert config.count(include_directive) == len(cache_location_blocks) + 1
    assert config.index(include_directive) < config.index("location = /dev")


def test_web_image_installs_the_policy_for_unprivileged_nginx() -> None:
    dockerfile = (REPO_ROOT / "deploy/Dockerfile.web").read_text(encoding="utf-8")

    assert (
        "COPY deploy/nginx-spa-security-headers.conf /etc/nginx/loom-spa-security-headers.conf"
    ) in dockerfile
    assert "chmod 644 /etc/nginx/loom-spa-security-headers.conf" in dockerfile


def test_frontend_has_no_third_party_font_or_avoidable_inline_fallback() -> None:
    index_html = (REPO_ROOT / "web/index.html").read_text(encoding="utf-8")
    index_css = (REPO_ROOT / "web/src/index.css").read_text(encoding="utf-8")
    tailwind = (REPO_ROOT / "web/tailwind.config.js").read_text(encoding="utf-8")
    main = (REPO_ROOT / "web/src/main.tsx").read_text(encoding="utf-8")
    recovery_sources = "\n".join(
        (REPO_ROOT / path).read_text(encoding="utf-8")
        for path in (
            "web/src/bootstrap/FrontendBootstrap.tsx",
            "web/src/components/RecoveryPanel.tsx",
            "web/src/components/RootErrorBoundary.tsx",
            "web/src/lib/errorReporting.ts",
        )
    )
    combined = "\n".join((index_html, index_css, tailwind))

    assert "fonts.googleapis.com" not in combined
    assert "fonts.gstatic.com" not in combined
    assert "DM Sans" not in combined
    assert "JetBrains Mono" not in combined
    assert "<style" not in index_html.lower()
    assert "style=" not in index_html.lower()
    assert ".innerHTML" not in "\n".join((main, recovery_sources))
    assert '<main role="status" aria-live="polite" aria-busy="true">' in index_html
    assert "Starting Loom" in index_html
    assert "ReactDOM.createRoot(rootElement).render(" in main
