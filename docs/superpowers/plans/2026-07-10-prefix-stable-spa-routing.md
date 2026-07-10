# Prefix-Stable SPA Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/dev`, `/prod`, and every prefixed deep link load one immutable Loom web build with canonical redirects, correctly prefixed assets, and executable ingress/browser smoke coverage.

**Architecture:** Preserve the Vite build output as a read-only `index.html.template`, then atomically derive the served `index.html` from the validated runtime prefix on every container start. Route slash-prefixed API and SPA traffic through the existing rewrite Ingress, route the anchored exact prefix through a separate no-rewrite Ingress, and let the web Nginx return the query-preserving 308 before any SPA shell. Extend the public smoke from metadata-only validation to shell and same-origin asset validation, then exercise the generated Ingress against the repository-pinned ingress-nginx controller.

**Tech Stack:** POSIX shell, Nginx, Vite, Python 3.11/pytest, Jinja2 Kubernetes templates, ingress-nginx, GitHub Actions.

## Global Constraints

- One immutable web image must serve local, `/dev`, and `/prod`; environment-specific image rebuilds are forbidden.
- `/dev` and `/prod` must return HTTP 308 before SPA HTML, preserve the query string, and point to the slash-suffixed canonical route.
- Exact, one-segment, multi-segment, and detail routes must reference `/dev/assets/*` or `/prod/assets/*`, never root `/assets/*` or nested pseudo-assets.
- Every referenced same-origin module or stylesheet must return HTTP 200 with JavaScript or CSS MIME, never the HTML fallback.
- Root asset exposure remains closed at the public prefixed Ingress even though the web pod stores build assets at `/assets/*` internally.
- Runtime generation must be atomic, idempotent across restarts, and incapable of retaining or double-applying a prior environment prefix.
- The rendered manifest test must be supplemented by a real ingress-nginx kind smoke; neither one substitutes for the other.
- No bearer token, session cookie, CSRF value, password, provider key, or signed URL may enter logs or evidence.

---

## File Structure

- `deploy/web-runtime-config.sh`: validate runtime route metadata and derive both public JSON config and served HTML atomically.
- `deploy/Dockerfile.web`: retain the immutable Vite shell template in the final image.
- `deploy/nginx-spa.conf`: own exact-prefix 308 behavior and internal static/SPА fallback behavior.
- `src/loom_cli/templates/k8s/ingress.yaml.j2`: render separate rewrite and exact-prefix Ingress resources.
- `tests/ops/test_frontend_route_smoke.py`: cover runtime HTML derivation and pure HTTP response validation.
- `tests/loom_cli/test_cluster_render.py`: lock the generated Ingress resources and regex boundaries.
- `scripts/ops/frontend_route_smoke.py`: validate redirect, shell, runtime config, referenced assets, MIME, and cache behavior.
- `.github/workflows/staging-smoke.yml`: run the canonical/deep-route checks through the real ingress-nginx controller.
- `docs/runbooks/operator-runbook.md`: document canonical public URLs and the supported validation command.

### Task 1: Atomically derive a prefix-stable SPA shell

**Files:**
- Modify: `tests/ops/test_frontend_route_smoke.py`
- Modify: `deploy/web-runtime-config.sh`
- Modify: `deploy/Dockerfile.web`

**Interfaces:**
- Consumes: `LOOM_FRONTEND_ROUTE_PATH` validated as `""`, `/dev`, or `/prod`.
- Produces: `LOOM_FRONTEND_INDEX_TEMPLATE_PATH` and `LOOM_FRONTEND_INDEX_PATH` test seams; a served shell whose `./assets/` HTML references are either preserved for root deployment or replaced with `<route_path>/assets/`.

- [ ] **Step 1: Write failing runtime-shell tests**

Add a helper and three tests to `tests/ops/test_frontend_route_smoke.py`:

```python
def _run_runtime_config(
    tmp_path: Path,
    *,
    environment: str,
    route_path: str,
) -> tuple[dict[str, object], str]:
    config_path = tmp_path / "loom-frontend-config.json"
    template_path = tmp_path / "index.html.template"
    index_path = tmp_path / "index.html"
    template_path.write_text(
        '<link rel="stylesheet" href="./assets/index.css">'
        '<script type="module" src="./assets/index.js"></script>',
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "LOOM_FRONTEND_CONFIG_PATH": str(config_path),
        "LOOM_FRONTEND_INDEX_TEMPLATE_PATH": str(template_path),
        "LOOM_FRONTEND_INDEX_PATH": str(index_path),
        "LOOM_FRONTEND_ENVIRONMENT": environment,
        "LOOM_FRONTEND_ENVIRONMENT_LABEL": environment.title(),
        "LOOM_FRONTEND_ROUTE_PATH": route_path,
        "LOOM_FRONTEND_API_BASE": route_path,
        "LOOM_FRONTEND_PUBLIC_ORIGIN": "https://yylx.world",
    }
    subprocess.run(
        ["sh", "deploy/web-runtime-config.sh"],
        check=True,
        env=env,
        text=True,
        capture_output=True,
    )
    return json.loads(config_path.read_text(encoding="utf-8")), index_path.read_text(
        encoding="utf-8",
    )


def test_web_runtime_config_prefixes_dev_assets(tmp_path: Path) -> None:
    _, html = _run_runtime_config(tmp_path, environment="staging", route_path="/dev")
    assert 'href="/dev/assets/index.css"' in html
    assert 'src="/dev/assets/index.js"' in html
    assert '="./assets/' not in html


def test_web_runtime_config_preserves_root_asset_contract(tmp_path: Path) -> None:
    _, html = _run_runtime_config(tmp_path, environment="local", route_path="")
    assert 'href="./assets/index.css"' in html
    assert 'src="./assets/index.js"' in html


def test_web_runtime_config_restart_never_retains_or_doubles_prefix(tmp_path: Path) -> None:
    _run_runtime_config(tmp_path, environment="staging", route_path="/dev")
    _run_runtime_config(tmp_path, environment="production", route_path="/prod")
    _, html = _run_runtime_config(tmp_path, environment="staging", route_path="/dev")
    assert html.count("/dev/assets/") == 2
    assert "/dev/dev/assets/" not in html
    assert "/prod/assets/" not in html
```

Import `Path` from `pathlib`. Replace the existing
`test_web_runtime_config_script_writes_public_metadata` setup with a call to
`_run_runtime_config(tmp_path, environment="production", route_path="/prod")`
and retain its exact JSON assertion; this ensures every script invocation uses
an explicit immutable template instead of depending on the image filesystem.

- [ ] **Step 2: Run the focused tests and confirm the missing shell output fails**

Run:

```bash
uv run pytest tests/ops/test_frontend_route_smoke.py \
  -k 'prefixes_dev_assets or preserves_root_asset_contract or restart_never' -q
```

Expected: FAIL because `web-runtime-config.sh` does not write `LOOM_FRONTEND_INDEX_PATH`.

- [ ] **Step 3: Implement atomic shell derivation**

In `deploy/web-runtime-config.sh`, define the paths next to `config_path`:

```sh
index_template_path="${LOOM_FRONTEND_INDEX_TEMPLATE_PATH:-/usr/share/nginx/html/index.html.template}"
index_path="${LOOM_FRONTEND_INDEX_PATH:-/usr/share/nginx/html/index.html}"
```

After route validation and before JSON generation, add:

```sh
if [ ! -f "${index_template_path}" ]; then
  echo "frontend index template not found: ${index_template_path}" >&2
  exit 1
fi

tmp_index_path="${index_path}.tmp"
if [ -n "${route_path}" ]; then
  sed \
    -e "s|src=\"\./assets/|src=\"${route_path}/assets/|g" \
    -e "s|href=\"\./assets/|href=\"${route_path}/assets/|g" \
    "${index_template_path}" > "${tmp_index_path}"
else
  cp "${index_template_path}" "${tmp_index_path}"
fi
if grep -Eq '(src|href)="\./assets/' "${tmp_index_path}"; then
  echo "frontend shell retained a relative build asset" >&2
  rm -f "${tmp_index_path}"
  exit 1
fi
if grep -Eq '/(dev|prod)/(dev|prod)/assets/' "${tmp_index_path}"; then
  echo "frontend shell contains a double or stale route prefix" >&2
  rm -f "${tmp_index_path}"
  exit 1
fi
mv "${tmp_index_path}" "${index_path}"
```

Apply the two validation checks only inside the non-empty `route_path` branch;
root deployment intentionally retains Vite's `./assets/` contract.

In the final stage of `deploy/Dockerfile.web`, retain the build shell before dropping privileges:

```dockerfile
RUN cp /usr/share/nginx/html/index.html /usr/share/nginx/html/index.html.template \
    && chmod 755 /docker-entrypoint.d/40-loom-frontend-config.sh \
    && chmod 644 /etc/nginx/conf.d/default.conf \
    && chown -R 101:0 /usr/share/nginx/html \
    && chown root:root /usr/share/nginx/html/index.html.template \
    && chmod 444 /usr/share/nginx/html/index.html.template
```

- [ ] **Step 4: Run runtime tests and shell syntax validation**

Run:

```bash
sh -n deploy/web-runtime-config.sh
uv run pytest tests/ops/test_frontend_route_smoke.py -q
```

Expected: shell syntax succeeds and all route-smoke unit tests pass.

- [ ] **Step 5: Commit the runtime-shell slice**

```bash
git add deploy/Dockerfile.web deploy/web-runtime-config.sh \
  tests/ops/test_frontend_route_smoke.py
git commit -m "fix(web): derive prefix-stable runtime shell (#772)"
```

### Task 2: Split exact-prefix canonicalization from slash-route rewriting

**Files:**
- Modify: `tests/loom_cli/test_cluster_render.py`
- Modify: `src/loom_cli/templates/k8s/ingress.yaml.j2`
- Modify: `deploy/nginx-spa.conf`

**Interfaces:**
- Consumes: `frontend_route_path` of `/dev` or `/prod`.
- Produces: `loom-ingress` with slash-only rewrite regexes and `loom-frontend-prefix-redirect` with an end-anchored exact regex and no rewrite annotation.

- [ ] **Step 1: Replace the prefixed-render expectation with two Ingress contracts**

In `test_render_profile_ingress_routes_api_and_spa_under_frontend_prefix`, select by name and assert:

```python
ingresses = {d["metadata"]["name"]: d for d in docs if d["kind"] == "Ingress"}
assert set(ingresses) == {"loom-ingress", "loom-frontend-prefix-redirect"}

ingress = ingresses["loom-ingress"]
annotations = ingress["metadata"]["annotations"]
assert annotations["nginx.ingress.kubernetes.io/use-regex"] == "true"
assert annotations["nginx.ingress.kubernetes.io/rewrite-target"] == "/$1"
paths = ingress["spec"]["rules"][0]["http"]["paths"]
assert [(p["path"], p["pathType"], p["backend"]["service"]["name"]) for p in paths] == [
    (f"{route_path}/(api/v1(/.*)?)$", "ImplementationSpecific", "loom-service"),
    (f"{route_path}/(.*)$", "ImplementationSpecific", "loom-web"),
]

redirect = ingresses["loom-frontend-prefix-redirect"]
assert "nginx.ingress.kubernetes.io/rewrite-target" not in redirect["metadata"]["annotations"]
redirect_path = redirect["spec"]["rules"][0]["http"]["paths"][0]
assert redirect_path["path"] == f"{route_path}$"
assert redirect_path["pathType"] == "ImplementationSpecific"
assert redirect_path["backend"]["service"]["name"] == "loom-web"
```

Extend `test_render_ingress_redirect_hosts_bind_tls_and_redirect_to_canonical`
to assert only `loom-ingress` carries `from-to-www-redirect: "true"`, the
exact-prefix resource does not duplicate the host redirect annotation, and
both resources use the same TLS hosts/secret.

- [ ] **Step 2: Run the renderer tests and confirm the one-Ingress implementation fails**

Run:

```bash
uv run pytest tests/loom_cli/test_cluster_render.py \
  -k 'frontend_prefix or redirect_hosts_bind' -q
```

Expected: FAIL because only `loom-ingress` exists and it still admits the exact prefix into the rewrite regex.

- [ ] **Step 3: Render slash-only rewrite paths and a no-rewrite exact Ingress**

Change the prefixed `loom-ingress` annotation to `nginx.ingress.kubernetes.io/rewrite-target: /$1` and paths to:

```yaml
          - path: {{ frontend_route_path }}/(api/v1(/.*)?)$
            pathType: ImplementationSpecific
            backend:
              service:
                name: loom-service
                port:
                  number: 8090
          - path: {{ frontend_route_path }}/(.*)$
            pathType: ImplementationSpecific
            backend:
              service:
                name: loom-web
                port:
                  number: 80
```

Append this conditional second resource to the Jinja template:

```yaml
{% if frontend_prefixed_route %}
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: loom-frontend-prefix-redirect
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "100m"
    nginx.ingress.kubernetes.io/use-regex: "true"
spec:
  ingressClassName: {{ ingress_class_name }}
  tls:
{% if ingress_host_is_ip %}
    - secretName: {{ ingress_tls_secret_name }}
{% else %}
    - hosts:
        - {{ ingress_host }}
{% for host in ingress_redirect_hosts %}
        - {{ host }}
{% endfor %}
      secretName: {{ ingress_tls_secret_name }}
{% endif %}
  rules:
{% if ingress_host_is_ip %}
    - http:
{% else %}
    - host: {{ ingress_host }}
      http:
{% endif %}
        paths:
          - path: {{ frontend_route_path }}$
            pathType: ImplementationSpecific
            backend:
              service:
                name: loom-web
                port:
                  number: 80
{% endif %}
```

The second resource deliberately omits `rewrite-target`,
`from-to-www-redirect`, and the cert-manager issuer. Host redirect generation
and certificate issuance remain single-owner responsibilities of
`loom-ingress`; the exact resource only reuses its TLS secret.

- [ ] **Step 4: Return canonical 308 responses before the SPA fallback**

Add these exact locations above the prefixed regex location in `deploy/nginx-spa.conf`:

```nginx
    # Keep redirects origin-relative behind TLS-terminating ingress. Without
    # this, nginx can expose its internal http://host:8080 listener.
    absolute_redirect off;

    location = /dev {
        return 308 /dev/$is_args$args;
    }

    location = /prod {
        return 308 /prod/$is_args$args;
    }
```

- [ ] **Step 5: Run renderer, boundary, and template checks**

Run:

```bash
uv run pytest tests/loom_cli/test_cluster_render.py \
  tests/loom_cli/test_cluster_boundary.py -q
docker run --rm -v "$PWD/deploy/nginx-spa.conf:/etc/nginx/conf.d/default.conf:ro" \
  nginxinc/nginx-unprivileged:1.27-alpine nginx -t
```

Expected: all pytest cases pass and Nginx reports `syntax is ok` and `test is successful`.

- [ ] **Step 6: Commit the canonical-route slice**

```bash
git add deploy/nginx-spa.conf src/loom_cli/templates/k8s/ingress.yaml.j2 \
  tests/loom_cli/test_cluster_render.py
git commit -m "fix(web): canonicalize prefixed ingress routes (#772)"
```

### Task 3: Validate the executable shell and its same-origin assets

**Files:**
- Modify: `tests/ops/test_frontend_route_smoke.py`
- Modify: `scripts/ops/frontend_route_smoke.py`

**Interfaces:**
- Consumes: a public HTTPS route without a trailing slash.
- Produces: evidence for the exact redirect, canonical shell, runtime config, referenced modules/stylesheets, MIME, and absence of nested pseudo-assets.

- [ ] **Step 1: Add pure-response validation tests**

Define `HttpResponse` in the smoke module and test its validator without network access:

```python
@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
```

Add tests that construct a shell containing `/dev/assets/index.js` and `/dev/assets/index.css`, pass matching JavaScript/CSS responses, and expect `[]`; then replace the JavaScript MIME/body with `text/html`/`<div id="root"></div>` and assert the error contains `asset returned HTML fallback`.

- [ ] **Step 2: Run the focused validator tests and confirm the validator is absent**

```bash
uv run pytest tests/ops/test_frontend_route_smoke.py -k executable_shell -q
```

Expected: FAIL on the missing `validate_executable_shell` import or symbol.

- [ ] **Step 3: Implement bounded HTML reference extraction and response checks**

Add these contracts to `scripts/ops/frontend_route_smoke.py`:

```python
ASSET_REF_RE = re.compile(
    rb'''(?:src|href)=["']([^"']+\.(?:js|css))["']''',
    re.IGNORECASE,
)
EXPECTED_ASSET_MIME = {".js": "javascript", ".css": "text/css"}


def extract_asset_urls(shell_url: str, body: bytes) -> list[str]:
    return sorted({urljoin(shell_url, match.decode("utf-8")) for match in ASSET_REF_RE.findall(body)})


def validate_executable_shell(
    *, route_url: str, shell: HttpResponse, assets: list[HttpResponse]
) -> list[str]:
    errors: list[str] = []
    route_path = urlparse(route_url).path.rstrip("/")
    if shell.status != 200:
        errors.append(f"canonical shell returned HTTP {shell.status}")
    if "text/html" not in shell.headers.get("content-type", "").lower():
        errors.append("canonical shell must return text/html")
    expected_prefix = f"{route_path}/assets/"
    refs = extract_asset_urls(shell.url, shell.body)
    if not refs:
        errors.append("canonical shell contains no module or stylesheet assets")
    for ref in refs:
        if urlparse(ref).path.startswith(f"{route_path}/") is False:
            errors.append(f"asset is outside route prefix: {ref}")
        if expected_prefix not in urlparse(ref).path:
            errors.append(f"asset does not use canonical asset prefix: {ref}")
    for response in assets:
        suffix = ".css" if urlparse(response.url).path.endswith(".css") else ".js"
        content_type = response.headers.get("content-type", "").lower()
        if response.status != 200:
            errors.append(f"asset returned HTTP {response.status}: {response.url}")
        elif EXPECTED_ASSET_MIME[suffix] not in content_type:
            if "text/html" in content_type:
                errors.append(f"asset returned HTML fallback: {response.url}")
            else:
                errors.append(f"asset has unexpected MIME {content_type}: {response.url}")
    return errors
```

Use a no-redirect opener for the exact route so status 308 and `Location` can be inspected. Fetch the canonical slash route, `/monitor`, `/batches/example-id`, and each shell asset; include status/MIME errors in `RouteCheck.errors`. Preserve the existing redacted JSON output shape while adding checked URLs, statuses, and content types only.

- [ ] **Step 4: Run smoke unit tests and CLI help**

```bash
uv run pytest tests/ops/test_frontend_route_smoke.py -q
uv run python scripts/ops/frontend_route_smoke.py --help >/dev/null
```

Expected: tests pass and CLI help exits zero.

- [ ] **Step 5: Commit the executable-smoke slice**

```bash
git add scripts/ops/frontend_route_smoke.py tests/ops/test_frontend_route_smoke.py
git commit -m "test(web): verify prefixed shell assets (#772)"
```

### Task 4: Exercise the contract through ingress-nginx in kind

**Files:**
- Modify: `.github/workflows/staging-smoke.yml`
- Modify: `scripts/plan_ci_validations.py`
- Modify: `tests/unit/test_plan_ci_validations.py`

**Interfaces:**
- Consumes: the rendered staging profile and kind host-port mapping.
- Produces: a required staging-smoke failure if exact redirects, deep shells, asset MIME, or root asset isolation regress.

- [ ] **Step 1: Make frontend runtime/Ingress paths select staging smoke**

Add failing planner cases asserting that each of these paths selects `staging_smoke=True`:

```python
@pytest.mark.parametrize(
    "path",
    [
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/web-runtime-config.sh",
        "src/loom_cli/templates/k8s/ingress.yaml.j2",
        "scripts/ops/frontend_route_smoke.py",
    ],
)
def test_frontend_route_contract_selects_staging_smoke(path: str) -> None:
    assert plan_for_paths([path]).staging_smoke is True
```

Use the test file's existing planner helper name if it differs; do not create a second planner implementation.

- [ ] **Step 2: Run planner tests and confirm any missing path fails**

```bash
uv run pytest tests/unit/test_plan_ci_validations.py -q
```

Expected: at least `scripts/ops/frontend_route_smoke.py` fails selection before the exact set is added.

- [ ] **Step 3: Add the exact paths to `staging_exact`**

In `scripts/plan_ci_validations.py`, ensure the set contains:

```python
staging_exact = {
    ".github/workflows/staging-smoke.yml",
    "deploy/Dockerfile.web",
    "deploy/nginx-spa.conf",
    "deploy/web-runtime-config.sh",
    "scripts/ops/frontend_route_smoke.py",
    "src/loom_cli/templates/k8s/ingress.yaml.j2",
}
```

Retain every unrelated existing selector entry.

- [ ] **Step 4: Configure the kind candidate with the real `/dev` profile**

In `.github/workflows/staging-smoke.yml`, generate:

```toml
namespace = "loom"
image_tag = "${IMAGE_TAG}"
ingress_host = "yylx.world"
frontend_environment = "staging"
frontend_environment_label = "Development / staging"
frontend_route_path = "/dev"
frontend_api_base_path = "/dev"
[replicas]
service = 1
control_plane = 1
gateway = 1
web = 1
worker = 0
```

After component readiness, invoke the route smoke through the kind HTTPS
mapping with `--resolve yylx.world:443:127.0.0.1` semantics and a dedicated
`--insecure-for-kind` flag. That flag may disable certificate verification
only for this disposable kind invocation; ordinary CLI runs and live staging
must reject invalid TLS. Assert 308 and a `Location` ending `/dev/`, query
preservation with `?from=smoke`, canonical/deep shell HTML, and 200 with the
expected MIME for every `/dev/assets/*.js|css` reference. Also assert
`/assets/<same-name>` is not 200 and `/devil`, `/devapi`, and `/prodfoo` do not
match either prefixed Ingress.

Apply the `ci:images`, `cluster-smoke`, and `staging-smoke` labels to the PR so
the controller-level route contract is exercised before merge.

- [ ] **Step 5: Run all local non-cluster coverage**

```bash
uv run pytest tests/unit/test_plan_ci_validations.py \
  tests/loom_cli/test_cluster_render.py \
  tests/loom_cli/test_cluster_boundary.py \
  tests/ops/test_frontend_route_smoke.py -q
uv run python -m loom_cli cluster render \
  --config deploy/environments/staging.cluster.toml >/tmp/loom-772-rendered.yaml
```

Expected: all tests pass; the rendered file contains both `loom-ingress` and `loom-frontend-prefix-redirect`.

- [ ] **Step 6: Commit the ingress-nginx smoke slice**

```bash
git add .github/workflows/staging-smoke.yml scripts/plan_ci_validations.py \
  tests/unit/test_plan_ci_validations.py
git commit -m "ci(web): gate prefixed routes in ingress smoke (#772)"
```

### Task 5: Verify the complete #772 acceptance contract and document it

**Files:**
- Modify: `docs/runbooks/operator-runbook.md`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: reproducible operator instructions and merge evidence linked to #772.

- [ ] **Step 1: Document canonical URLs and the public smoke**

Add a runbook section containing these exact commands:

```bash
uv run python scripts/ops/frontend_route_smoke.py \
  --route staging=https://yylx.world/dev=https://yylx.world/dev/api \
  --json

curl -fsSI 'https://yylx.world/dev?from=operator'
curl -fsS 'https://yylx.world/dev/batches/example-id' >/dev/null
```

State that the first curl must be 308 to `/dev/?from=operator`, and that the smoke fails on HTML returned for JS/CSS.

- [ ] **Step 2: Build and test the web image**

```bash
docker build -f deploy/Dockerfile.web -t loom-web:issue-772 .
docker run --rm loom-web:issue-772 nginx -t
```

Expected: image build completes and Nginx reports a successful config test.

- [ ] **Step 3: Run the full relevant local suite**

```bash
uv run pytest tests/ops/test_frontend_route_smoke.py \
  tests/loom_cli/test_cluster_render.py \
  tests/loom_cli/test_cluster_boundary.py \
  tests/unit/test_plan_ci_validations.py -q
uv run ruff check scripts/ops/frontend_route_smoke.py \
  tests/ops/test_frontend_route_smoke.py \
  tests/loom_cli/test_cluster_render.py
git diff --check
```

Expected: all tests and Ruff pass; `git diff --check` is empty.

- [ ] **Step 4: Commit documentation**

```bash
git add docs/runbooks/operator-runbook.md
git commit -m "docs(web): record canonical route validation (#772)"
```

- [ ] **Step 5: Open the PR and run live staging acceptance after merge**

Open a PR targeting `dev`, enable squash auto-merge, and keep #772 open as `[Needs validation]` until the merged commit is rolled. Against that fixed staging image, record only safe evidence from:

```bash
uv run python scripts/ops/frontend_route_smoke.py \
  --route staging=https://yylx.world/dev=https://yylx.world/dev/api \
  --json
```

Close #772 only when `/dev`, `/dev/`, `/dev/monitor`, `/dev/batches`, and `/dev/batches/<id>` mount without console/resource failures and all shell assets have correct status/MIME.
