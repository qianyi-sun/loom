# Environment domain topology

> Archived decision record. Current routing behavior is documented in
> `docs/architecture/env-naming-convention.md` and the operator runbook.

- **Status:** Accepted
- **Date:** 2026-07-31
- **Decision owner:** Qianyi Sun
- **Tracking:** #857, #873, #880, #894

## Context

Loom exposes development, staging, and production through one public origin.
Earlier design discussion considered moving each environment to a separate
subdomain. The product contract instead keeps one `yylx.world` origin and uses
an explicit path prefix for every environment.

## Decision

The canonical public routes are:

| Environment | Frontend route | API base |
| --- | --- | --- |
| Development | `https://yylx.world/dev` | `https://yylx.world/dev/api` |
| Staging | `https://yylx.world/staging` | `https://yylx.world/staging/api` |
| Production | `https://yylx.world/prod` | `https://yylx.world/prod/api` |

Environment-specific frontend subdomains such as `dev.yylx.world`,
`staging.yylx.world`, and `prod.yylx.world` are not product entrypoints and
must not appear in configuration, acceptance evidence, or user documentation.
The shared origin and TLS/ingress common-mode risk are accepted deliberately.

Path identity is part of the environment boundary, not a cosmetic redirect:

- runtime frontend configuration must report the exact environment, route
  prefix, and matching API base;
- ingress must route each prefix only to its matching environment and must not
  fall through or redirect across environment prefixes;
- database, object storage, credentials, namespaces, worker identities, and
  rollout evidence remain distinct even though the public origin is shared;
- cookies or other browser state used by an environment must have an
  environment-specific name and the narrowest practical `Path`; server-side
  authorization must never infer environment identity from a cookie alone;
- a shared frontend artifact is allowed only when runtime configuration binds
  it to one environment and no build-time URL can cross that binding;
- release gates must validate all three route/API pairs and fail on duplicate,
  crossed, root-host, or environment-subdomain inputs.

## Alternatives considered

### One subdomain per environment

Rejected. Separate origins give stronger browser-storage isolation, but add
DNS, certificate, ingress, CORS, redirect, and operational surfaces that are
not required for the chosen product. Loom instead enforces the three path
prefixes and the mitigations above.

### Hybrid host and path routing

Rejected. Supporting both models would create two canonical entrypoints per
environment and weaken route, evidence, and rollback checks.

## Consequences

- `/dev`, `/staging`, and `/prod` are final addresses, not migration aliases.
- Historical evidence may mention staging on `/dev`; it remains historical and
  must not be reused as current route acceptance.
- DNS and certificates cover `yylx.world`; no wildcard environment certificate
  is required by this decision.
- #894 owns documentation and mitigation completion, not a future topology
  choice or subdomain rollout.
