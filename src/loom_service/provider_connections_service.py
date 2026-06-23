"""Provider-connection service layer.

The route handlers are thin orchestrators; this module owns the
business logic:

- URL parsing + upstream-host derivation.
- DNS resolution + SSRF IP classification (RFC1918, ULA, loopback,
  link-local, multicast, broadcast, unspecified). The team-level
  `allow_private_endpoints` flag relaxes RFC1918 + ULA; loopback +
  link-local stay rejected unconditionally because no legitimate
  provider hosts on those.
- Pricing-source validation (the `operator-supplied` source requires
  `pricing_data` with numeric `{input_usd_per_1m, output_usd_per_1m}`).

Separated from the routes so each piece is testable without a
FastAPI request fixture; the routes file is a thin orchestrator.
"""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import quote, urlparse

from loom.security.redaction import redact_text

# Public IP classifications that are always SSRF targets — rejected
# even when team.allow_private_endpoints=True. These are network
# constructs that are never legitimate provider hosts on any topology.
_UNCONDITIONALLY_REJECTED_IPS = (
    # IPv4
    ipaddress.IPv4Network("0.0.0.0/8"),       # this network / unspecified
    ipaddress.IPv4Network("127.0.0.0/8"),     # loopback
    ipaddress.IPv4Network("169.254.0.0/16"),  # link-local
    ipaddress.IPv4Network("224.0.0.0/4"),     # multicast
    ipaddress.IPv4Network("240.0.0.0/4"),     # reserved (incl. broadcast)
    # IPv6
    ipaddress.IPv6Network("::1/128"),         # loopback
    ipaddress.IPv6Network("::/128"),          # unspecified
    ipaddress.IPv6Network("fe80::/10"),       # link-local
    ipaddress.IPv6Network("ff00::/8"),        # multicast
)

# Private IP classifications — rejected by default but permitted when
# team.allow_private_endpoints=True. Operators with on-prem providers
# (vLLM in the lab, hosted Llama on internal subnet) need the opt-in.
_PRIVATE_IPS = (
    # IPv4 RFC1918
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
    # IPv4 CGNAT (RFC6598)
    ipaddress.IPv4Network("100.64.0.0/10"),
    # IPv6 ULA
    ipaddress.IPv6Network("fc00::/7"),
)


class InvalidBaseUrlError(Exception):
    """Base URL fails parsing, scheme check, or has no resolvable host."""


class SsrfRejectedError(Exception):
    """The resolved IPs include addresses the SSRF policy forbids."""

    def __init__(self, message: str, *, ips: list[str]) -> None:
        super().__init__(message)
        self.ips = ips


class InvalidPricingError(Exception):
    """pricing_source / pricing_data combination is invalid."""


@dataclass(frozen=True)
class ResolvedUpstream:
    """A `base_url` validated + resolved for provider_connection storage."""

    base_url: str
    upstream_host: str
    resolved_ips: list[str]


def derive_upstream_host(base_url: str) -> str:
    """Parse `base_url` and return the hostname portion.

    Used both for the `provider_connections.upstream_host` column (the
    string the egress proxy validates SNI against) and for the DNS
    resolution that populates `resolved_egress_ips`. Raises on missing
    scheme or host.
    """
    if not base_url:
        raise InvalidBaseUrlError("base_url must be non-empty")
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        raise InvalidBaseUrlError(
            f"base_url scheme must be http or https; got {parsed.scheme!r}",
        )
    if not parsed.hostname:
        raise InvalidBaseUrlError(
            f"base_url has no hostname: {base_url!r}",
        )
    return parsed.hostname


def classify_ip(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    *,
    allow_private: bool,
) -> str | None:
    """Return None if `ip` is allowed; otherwise a human-readable reason
    naming the rejection category. The `allow_private` flag relaxes the
    `_PRIVATE_IPS` set; the `_UNCONDITIONALLY_REJECTED_IPS` set stays
    rejected.
    """
    for net in _UNCONDITIONALLY_REJECTED_IPS:
        if ip.version == net.version and ip in net:
            return f"address {ip} is in {net} (never legitimate as a provider host)"
    if not allow_private:
        for net in _PRIVATE_IPS:
            if ip.version == net.version and ip in net:
                return (
                    f"address {ip} is in private range {net}; set "
                    f"team.allow_private_endpoints=True to permit on-prem providers"
                )
    return None


def resolve_and_validate(
    base_url: str, *, allow_private: bool,
    _resolver: object = None,
) -> ResolvedUpstream:
    """Parse + DNS-resolve `base_url`; validate every resolved IP against
    the SSRF policy.

    `_resolver`: optional override for testability. Must be a callable
    matching `socket.getaddrinfo(host, port=None)` — tests inject a
    stub to avoid hitting real DNS. Default uses stdlib `socket`.
    """
    upstream_host = derive_upstream_host(base_url)
    resolver = _resolver if _resolver is not None else socket.getaddrinfo

    try:
        # type=SOCK_STREAM filters down to TCP results so we don't see
        # the same address twice (UDP + TCP duplicates).
        infos = resolver(  # type: ignore[operator]
            upstream_host, None, type=socket.SOCK_STREAM,
        )
    except socket.gaierror as e:
        raise InvalidBaseUrlError(
            f"DNS resolution failed for {upstream_host!r}: {e}",
        ) from e

    ips: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen: set[str] = set()
    for info in infos:
        sockaddr = info[4]
        ip_str = sockaddr[0]
        # IPv6 results can include zone-id suffix (e.g. "fe80::1%eth0");
        # strip before parsing.
        ip_str_clean = ip_str.split("%", 1)[0]
        if ip_str_clean in seen:
            continue
        seen.add(ip_str_clean)
        ips.append(ipaddress.ip_address(ip_str_clean))

    if not ips:
        raise InvalidBaseUrlError(
            f"DNS resolution for {upstream_host!r} returned no addresses",
        )

    rejected_reasons: list[str] = []
    for ip in ips:
        reason = classify_ip(ip, allow_private=allow_private)
        if reason is not None:
            rejected_reasons.append(reason)
    if rejected_reasons:
        raise SsrfRejectedError(
            "; ".join(rejected_reasons),
            ips=[str(ip) for ip in ips],
        )

    return ResolvedUpstream(
        base_url=base_url,
        upstream_host=upstream_host,
        resolved_ips=[str(ip) for ip in ips],
    )


def validate_pricing(
    pricing_source: str, pricing_data: dict[str, float] | None,
) -> None:
    """Enforce the cost-source × pricing_data contract.

    - `rate-card` / `tokens-only`: pricing_data MUST be NULL (any value
      is ignored at cost-compute time, so accepting it would be a UX
      footgun — operators expect what they set to be used).
    - `operator-supplied`: pricing_data MUST contain non-negative
      numeric `input_usd_per_1m` AND `output_usd_per_1m`.
    """
    if pricing_source not in {"rate-card", "tokens-only", "operator-supplied"}:
        raise InvalidPricingError(
            f"pricing_source must be one of "
            f"{{'rate-card', 'tokens-only', 'operator-supplied'}}; "
            f"got {pricing_source!r}",
        )
    if pricing_source == "operator-supplied":
        if not isinstance(pricing_data, dict):
            raise InvalidPricingError(
                "pricing_source='operator-supplied' requires pricing_data "
                "with input_usd_per_1m + output_usd_per_1m",
            )
        for key in ("input_usd_per_1m", "output_usd_per_1m"):
            value = pricing_data.get(key)
            if value is None:
                raise InvalidPricingError(
                    f"pricing_data.{key} is required for "
                    f"pricing_source='operator-supplied'",
                )
            if not isinstance(value, int | float) or isinstance(value, bool):
                raise InvalidPricingError(
                    f"pricing_data.{key} must be a number; got {type(value).__name__}",
                )
            if value < 0:
                raise InvalidPricingError(
                    f"pricing_data.{key} must be >= 0; got {value}",
                )
    elif pricing_data is not None:
        raise InvalidPricingError(
            f"pricing_data must be NULL for pricing_source={pricing_source!r}; "
            f"got {pricing_data!r}",
        )


def default_pricing_source_for(provider_type: str) -> str:
    """Per cluster-deploy.md §Cost computation:
    - anthropic, google → rate-card
    - openai-compatible, custom → tokens-only
    """
    if provider_type in ("anthropic", "google"):
        return "rate-card"
    return "tokens-only"


def default_rate_card_provider_for(provider_type: str) -> str | None:
    """Default rate-card provider namespace for new connections.

    OpenAI-compatible endpoints are protocol-compatible with many hosted
    providers. The default preserves the common OpenAI-hosted path, and
    operators can override it per connection for Together, Fireworks, or
    other OpenAI-compatible upstreams.
    """
    if provider_type == "anthropic":
        return "anthropic"
    if provider_type == "google":
        return "google"
    if provider_type == "openai-compatible":
        return "openai"
    return None


# ──────────────────────────────────────────────────────────────────────
# Connection probe (used by /test route)
# ──────────────────────────────────────────────────────────────────────

# Probe timeout — short on purpose; legitimate provider models endpoints
# answer in under a second from anywhere on the public internet. Stretching
# this only makes the route easier to use as a slow-loris vector against
# loom_service itself.
_PROBE_TIMEOUT_SEC = 5.0


def _redact_secret(s: str, *secrets: str) -> str:
    """Replace every occurrence of each non-empty secret in `s` with
    ``[REDACTED]``. Used on upstream-body excerpts + URLs before they
    land in DB columns (``last_validation_error``) or response bodies
    that downstream operators can read.

    Real concern: some upstreams echo the auth header / `?key=` query
    parameter in 4xx error bodies (debug pages, dev OpenAI-compatible
    servers). Without redaction, a single bad probe leaks the key
    into `provider_connections.last_validation_error`, where any
    team-scoped GET surfaces it.

    Two-character minimum on each secret avoids the degenerate case
    where an empty or 1-char secret would replace half the string.
    """
    out = s
    for secret in secrets:
        if secret and len(secret) >= 4:
            out = out.replace(secret, "[REDACTED]")
    return redact_text(out)


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of a connection probe. `http_status` is None when the
    request never reached a server (DNS/connect/timeout); otherwise the
    upstream's response code."""

    status: str  # 'valid' | 'invalid'
    http_status: int | None
    error: str | None


@dataclass(frozen=True)
class ModelPreflightResult:
    """Outcome of a single model entitlement probe.

    `status='valid'` means the provider accepted a minimal non-streaming
    generation request for this exact model id. `status='failed'` means the
    connection/key/model combination could not be used. Error fields are safe
    to persist and return to operators; secrets are redacted before return.
    """

    status: str  # 'valid' | 'failed'
    http_status: int | None
    error_code: str | None
    error_message: str | None


def _probe_url_and_headers(
    provider_type: str, base_url: str, api_key: str,
) -> tuple[str, dict[str, str]]:
    """Map (type, base_url, api_key) → (probe URL, headers). Every probe
    is GET; bodies stay zero-byte so a misconfigured provider can't bill
    us for tokens.

    - openai-compatible / custom: `GET <base>/models` with Bearer
    - anthropic: `GET <base>/models` with `x-api-key` + version header
    - google: `GET <base>/models?key=<API_KEY>` (Google API style)
    """
    base = base_url.rstrip("/")
    if provider_type == "anthropic":
        return (
            f"{base}/models",
            {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
        )
    if provider_type == "google":
        return (
            f"{base}/models?key={api_key}",
            {},
        )
    # openai-compatible + custom (defensible default)
    return (
        f"{base}/models",
        {"Authorization": f"Bearer {api_key}"},
    )


def _preflight_url_headers_body(
    provider_type: str, base_url: str, api_key: str, model_id: str,
) -> tuple[str, dict[str, str], dict[str, object]]:
    """Map a provider connection + model id to a minimal generation call.

    Discovery uses `/models`; preflight intentionally hits the generation
    surface so entitlement failures such as 403 "model not enabled" are
    caught before a batch is submitted.
    """
    base = base_url.rstrip("/")
    if provider_type == "anthropic":
        return (
            f"{base}/messages",
            {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            {
                "model": model_id,
                "messages": [{"role": "user", "content": "ping"}],
                "max_tokens": 1,
            },
        )
    if provider_type == "google":
        quoted_model = quote(model_id, safe="")
        return (
            f"{base}/models/{quoted_model}:generateContent?key={api_key}",
            {},
            {
                "contents": [{"parts": [{"text": "ping"}]}],
                "generationConfig": {"maxOutputTokens": 1},
            },
        )
    return (
        f"{base}/chat/completions",
        {"Authorization": f"Bearer {api_key}"},
        {
            "model": model_id,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
        },
    )


async def probe_connection(
    provider_type: str, base_url: str, api_key: str,
    *, _client_factory: object = None,
) -> ProbeResult:
    """Probe a provider connection's reachability + credential validity.

    Issues a single GET to the provider's `/models` listing endpoint
    with a short timeout, no redirects (a 3xx to an internal host
    would re-introduce SSRF). 2xx → valid; anything else → invalid
    with the http_status + a 200-char body excerpt for diagnosis.

    `_client_factory` is a test seam — must be a callable returning an
    `httpx.AsyncClient`-like object exposing `.get(url, headers=...)`.
    Default constructs a fresh AsyncClient per call so settings stay
    explicit (timeout + no redirects).
    """
    import httpx

    url, headers = _probe_url_and_headers(provider_type, base_url, api_key)

    if _client_factory is None:
        client_cm = httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC, follow_redirects=False,
        )
    else:
        client_cm = _client_factory()  # type: ignore[operator]

    try:
        async with client_cm as client:
            try:
                resp = await client.get(url, headers=headers)
            except httpx.TimeoutException as e:
                return ProbeResult(
                    status="invalid", http_status=None,
                    error=f"timeout after {_PROBE_TIMEOUT_SEC}s: {e}",
                )
            except httpx.RequestError as e:
                return ProbeResult(
                    status="invalid", http_status=None,
                    error=f"{type(e).__name__}: {e}",
                )
    except Exception as e:
        # Some test doubles raise at __aenter__ instead of .get; surface
        # as invalid rather than 500 the route.
        return ProbeResult(
            status="invalid", http_status=None,
            error=f"{type(e).__name__}: {e}",
        )

    if 200 <= resp.status_code < 300:
        return ProbeResult(
            status="valid", http_status=resp.status_code, error=None,
        )
    excerpt = (resp.text or "")[:200]
    # Redact: upstream may echo the auth header / `?key=` query param
    # in 4xx debug pages, and this error string is persisted to
    # `provider_connections.last_validation_error` (readable via
    # GET /provider-connections/{id}).
    safe_url = _redact_secret(url, api_key)
    safe_excerpt = _redact_secret(excerpt, api_key)
    return ProbeResult(
        status="invalid",
        http_status=resp.status_code,
        error=(
            f"HTTP {resp.status_code} from {safe_url}; "
            f"body excerpt: {safe_excerpt!r}"
        ),
    )


async def preflight_model(
    provider_type: str, base_url: str, api_key: str, model_id: str,
    *, _client_factory: object = None,
) -> ModelPreflightResult:
    """Probe whether this connection/key can call one concrete model.

    This is deliberately single-model and bounded: no catalog-wide probing,
    no streaming, one token max. Any 2xx counts as valid; provider-specific
    response validation is intentionally out of scope because entitlement is
    already proven when the upstream accepts the generation request.
    """
    import httpx

    url, headers, body = _preflight_url_headers_body(
        provider_type, base_url, api_key, model_id,
    )
    if _client_factory is None:
        client_cm = httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC, follow_redirects=False,
        )
    else:
        client_cm = _client_factory()  # type: ignore[operator]

    try:
        async with client_cm as client:
            try:
                resp = await client.post(url, headers=headers, json=body)
            except httpx.TimeoutException as e:
                return ModelPreflightResult(
                    status="failed",
                    http_status=None,
                    error_code="timeout",
                    error_message=_redact_secret(
                        f"timeout after {_PROBE_TIMEOUT_SEC}s: {e}",
                        api_key,
                    ),
                )
            except httpx.RequestError as e:
                return ModelPreflightResult(
                    status="failed",
                    http_status=None,
                    error_code="request-error",
                    error_message=_redact_secret(f"{type(e).__name__}: {e}", api_key),
                )
    except Exception as e:
        return ModelPreflightResult(
            status="failed",
            http_status=None,
            error_code="unexpected-error",
            error_message=_redact_secret(f"{type(e).__name__}: {e}", api_key),
        )

    if 200 <= resp.status_code < 300:
        return ModelPreflightResult(
            status="valid",
            http_status=resp.status_code,
            error_code=None,
            error_message=None,
        )

    excerpt = (resp.text or "")[:200]
    safe_url = _redact_secret(url, api_key)
    safe_excerpt = _redact_secret(excerpt, api_key)
    error_code = (
        "access-denied" if resp.status_code in (401, 403)
        else "upstream-http-error"
    )
    return ModelPreflightResult(
        status="failed",
        http_status=resp.status_code,
        error_code=error_code,
        error_message=(
            f"HTTP {resp.status_code} from {safe_url}; "
            f"body excerpt: {safe_excerpt!r}"
        ),
    )


# ──────────────────────────────────────────────────────────────────────
# Upstream models fetch (used by /models/refresh route)
# ──────────────────────────────────────────────────────────────────────


class UpstreamModelFetchError(Exception):
    """Upstream `/models` listing call failed or returned an unparseable
    shape. Includes the http_status when one was received."""

    def __init__(
        self, message: str, *, http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status


def _parse_upstream_models(
    provider_type: str, payload: object,
) -> list[str]:
    """Map a provider's /models response body to a flat list of model
    ids. Shapes covered:

    - openai-compatible / custom / anthropic: `{"data": [{"id": "..."}]}`
      (Anthropic's /models endpoint matches the OpenAI shape since 2024)
    - google: `{"models": [{"name": "models/<id>"}]}` — strip the
      `models/` prefix (Google's convention; the user-visible id is
      the suffix).

    Unknown shapes raise UpstreamModelFetchError so the route returns
    a 502-ish error to the operator rather than a silently-empty cache.
    """
    if not isinstance(payload, dict):
        raise UpstreamModelFetchError(
            f"upstream /models returned non-object: {type(payload).__name__}",
        )
    ids: list[str] = []
    if provider_type == "google":
        raw_list = payload.get("models")
        if not isinstance(raw_list, list):
            raise UpstreamModelFetchError(
                "google /models response missing top-level `models` array",
            )
        for entry in raw_list:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not isinstance(name, str):
                continue
            # Google returns `name: "models/<id>"`. Strip the prefix
            # so the stored id matches what users type in --model.
            ids.append(name.removeprefix("models/"))
    else:
        # openai-compatible / custom / anthropic
        raw_list = payload.get("data")
        if not isinstance(raw_list, list):
            raise UpstreamModelFetchError(
                f"{provider_type} /models response missing top-level "
                "`data` array",
            )
        for entry in raw_list:
            if not isinstance(entry, dict):
                continue
            mid = entry.get("id")
            if isinstance(mid, str) and mid:
                ids.append(mid)
    if not ids:
        raise UpstreamModelFetchError(
            "upstream /models returned no model entries we could parse",
        )
    # Dedup preserving order — operators see the upstream's preferred
    # ordering, but the same id repeated would be a UNIQUE-violation
    # at upsert.
    seen: set[str] = set()
    out: list[str] = []
    for mid in ids:
        if mid not in seen:
            seen.add(mid)
            out.append(mid)
    return out


async def fetch_upstream_models(
    provider_type: str, base_url: str, api_key: str,
    *, _client_factory: object = None,
) -> list[str]:
    """Issue the same `/models` GET that probe_connection uses, then
    parse the response body into a flat list of model ids.

    Raises UpstreamModelFetchError on network errors, non-2xx
    responses, or unparseable bodies. Successful return is always
    a non-empty list (parser raises if zero ids).
    """
    import httpx

    url, headers = _probe_url_and_headers(provider_type, base_url, api_key)
    if _client_factory is None:
        client_cm = httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC, follow_redirects=False,
        )
    else:
        client_cm = _client_factory()  # type: ignore[operator]

    try:
        async with client_cm as client:
            try:
                resp = await client.get(url, headers=headers)
            except httpx.TimeoutException as e:
                raise UpstreamModelFetchError(
                    f"timeout after {_PROBE_TIMEOUT_SEC}s: {e}",
                ) from e
            except httpx.RequestError as e:
                raise UpstreamModelFetchError(
                    f"{type(e).__name__}: {e}",
                ) from e
    except UpstreamModelFetchError:
        raise
    except Exception as e:
        raise UpstreamModelFetchError(
            f"{type(e).__name__}: {e}",
        ) from e

    if not (200 <= resp.status_code < 300):
        excerpt = (resp.text or "")[:200]
        # Same redaction rationale as probe_connection — this string
        # surfaces via the /models/refresh 502 detail body which the
        # CLI prints back to operators.
        safe_url = _redact_secret(url, api_key)
        safe_excerpt = _redact_secret(excerpt, api_key)
        raise UpstreamModelFetchError(
            f"HTTP {resp.status_code} from {safe_url}; "
            f"body excerpt: {safe_excerpt!r}",
            http_status=resp.status_code,
        )
    try:
        payload = resp.json()
    except ValueError as e:
        raise UpstreamModelFetchError(
            f"upstream /models returned non-JSON body: {e}",
            http_status=resp.status_code,
        ) from e
    return _parse_upstream_models(provider_type, payload)
