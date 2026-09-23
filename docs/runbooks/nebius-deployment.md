# Independent Nebius deployment

Nebius is the image publication path for both development and production.
`nebius-candidate` publishes the seven manifest-owned AMD64 images from an exact
`dev` commit. Production promotes those same immutable digest references through
`release-promotion-gate`, then the protected `dev` to `main` pull request and
`main-promotion-gate`. A `main` push does not rebuild or publish images to GHCR.
Use the candidate publication output as the source of image references; never
substitute a mutable branch tag during promotion.

Production still requires separately reviewed environment inputs, release-owner
and Production Environment approval. Run
`scripts/ops/verify_production_release_gate.sh` from the promoted `main` checkout
with the approved candidate, image selector and release-gate run before applying
manifests. Checked-in examples are not production deployment authorization.
`nebius-rollout` automates the independent integration environment only; it does
not turn a `main` merge into an automatic production rollout.

Before applying production, protect each of the seven approved image digests
with the release manifest's unused SemVer `prod_tag` in the same Nebius image
repository. Keep these tags for running production and retained rollback versions;
never move an existing release tag to another digest. Integration retention reads
integration workloads, not production workloads, so a candidate-only tag can
otherwise expire after production stops sharing integration's current version.
Its existing policy preserves images with non-candidate tags, including SemVer
release tags.

After the production evidence verifier succeeds, use the operator's configured
registry credentials for each approved `IMAGE_REF` (`repository@sha256:...`) and
`PROD_TAG` from the release evidence:

```sh
skopeo copy --preserve-digests \
  "docker://${IMAGE_REF}" "docker://${IMAGE_REF%@*}:${PROD_TAG}"
test "$(skopeo inspect --format '{{.Digest}}' \
  "docker://${IMAGE_REF%@*}:${PROD_TAG}")" = "${IMAGE_REF##*@}"
```

Do not apply production until all seven release tags read back the approved
digests. Render and deploy the original digest references, not the tags. Adding
these retention tags does not rebuild the images or create a second publisher.

`scripts/ops/deploy_nebius_platform.py` plans or applies the output of
`render_nebius_platform.py`. Its default is a read-only cluster preflight; cloud
mutation requires explicit `--apply`. This is the supported hosted deployment entrypoint. `loom cluster up` is
limited to disposable development targets; the shared-cluster rollout broker
and its CLI command are retired.

### Protected read-only installation inventory

Before qualifying a managed multi-person installation, dispatch the existing
protected Nebius workflow from `dev` with the explicit inspection operation:

```bash
gh workflow run nebius-rollout.yml --repo qianyi-sun/loom --ref dev -f operation=inspect
```

This uses the same protected `nebius-integration` environment, pinned SSH host and
cluster identity checks as rollout, but cannot select the rollout job. It does
not require automatic rollout to be enabled and performs no Kubernetes, database,
DNS or cloud mutation. It shares rollout concurrency so the two workflow modes
do not race each other.

Download `nebius-inspect-RUN_ID-ATTEMPT` for the sanitized
`management-preflight.json` artifact. It contains the configured candidate,
namespace identities, node allocatable resources, declared Pod requests including
init containers and overhead, services/ingress, PVC sizes and storage classes.
It excludes Secret values, Pod environment/commands, annotations, kubeconfig and
configuration payloads. Failed or incomplete inventory fails the command rather
than being treated as an empty cluster.

For up to three newest failed bootstrap Pods in the selected platform namespace,
inspection checks the exact Job owner UID, terminal failure and expected bootstrap
command before reading at most 50 log lines / 16 KiB per Pod. The artifact adds
`failed_bootstrap_jobs`: Job/Pod identities plus allowlisted phase/error type and,
for configuration HTTP errors, method, status and a fixed operation category.
No raw logs, arbitrary reason strings, route identifiers, SQL or credential values
are exported. Missing/unsupported logs are explicitly `unavailable`; they do not
turn a failed Job into success. This diagnosis grants no Job retry, dispatch-unpause
or rollback authority.

`observed` means inventory succeeded, not that personal environments are ready.
The configured candidate is read from the platform ConfigMap, which rollout can
update before migrations and workload replacement complete. It is not proof of
the running workload versions; use successful candidate-bound rollout evidence
and workload readback to qualify that separately.
No child capacity allowance is inferred from a naive request sum. Wildcard
DNS/TLS, provisioning IAM, management installation, live Nebius quota and pool
limits, and installed concurrent-owner acceptance still require qualification.
Keep this evidence outside the repository.

### Shared HTTPS installation boundary

The shared-controller renderer is `loom.nebius_shared_ingress.render_shared_ingress`.
Its `SharedIngressInstallation` input uses schema
`loom.nebius-shared-ingress.v1`, a non-nil `installation_id`, `foundation`,
digest-pinned native-registry `image`, and a separate `tls_secret_name`. Foundation
ingress namespace must equal the existing standalone namespace and its controller
label must be `loom-shared-ingress`. Use a certificate covering both the configured
child wildcard and the separate management hostname. Private keys stay in the
platform namespace; do not reuse the standalone Caddy key or publish Secret data.

Rendering does not install or switch traffic. Before enabling the standalone
`shared_ingress_enabled` flag through the protected Nebius workflow, qualify the
image and certificate, existing Service UID/allocation/ports, exact resource
ownership, controller readiness, capacity and legacy-host HTTPS/TLS-ALPN routing.
Preserve the original selector and protected configuration as rollback evidence.
The selector change and persisted flag must share the rollout concurrency boundary;
ordinary rollout rechecks the selected mode after acquiring its guard, before any
backup or resource mutation. Never remove `loom-web-tls`
or replace the LoadBalancer to perform this migration. No protected shared-ingress
install operation is supplied yet; do not use an ad-hoc `kubectl` cutover.

The renderer uses Traefik 3.7.13 features and receives trusted read-only Secret
discovery across the cluster. Budget 200m CPU, 256 MiB memory and 128 MiB ephemeral
storage including its rolling surge. Uploads and responses stream directly;
there is no uniform request-byte or global request-concurrency limit. Existing
application validation is not a pre-auth request-exhaustion defense. Qualify
bounded application request reception before public management activation; do
not treat the removed nginx body-size annotation as enforced. The controller has no certificate
issuance credentials; renewal and the corresponding safe reload still require
the protected installation workflow. Disposable routing evidence is not live
DNS/TLS or installed multi-owner acceptance.

### Certificate DNS-01 hooks

`scripts/ops/nebius_dns_challenge.py` implements the narrow GoDaddy **v3 bearer
PAT** provider boundary for Certbot manual authentication/cleanup hooks. Install
the locked `cluster` extra for `httpx` and `dnspython`. The hook does not implement
ACME, install certificates, change Kubernetes, or expose a public management
endpoint. Its presence is not permission to perform an ad-hoc ingress cutover.
The protected certificate operation below supplies the pinned ACME client,
private account state and SAN/expiry qualification. Safe Kubernetes Secret
delivery, controller reload and scheduled renewal remain installation boundaries.

The caller supplies `auth` or `cleanup`, `--zone`, `--certificate-domain`,
`--credential-file` and `--state-dir`; Certbot supplies `CERTBOT_DOMAIN` and
`CERTBOT_VALIDATION`. The certificate domain must be an exact child of the
selected zone, and the hook accepts only that domain (or its wildcard). It
derives the one `_acme-challenge` TXT name; it never replaces a recordset or
modifies A, CNAME, NS or other records. Existing standalone Caddy TLS remains
independent.

The owner-only regular credential file contains `token` and ISO-date
`expires_on`; mode 0600 and a nonexpired token are required. Provision and renew
the PAT through the existing protected operator route, never in ingress or a
personal namespace. File paths and secret values are not emitted in hook output.
Keep the journal directory private (0700); its 0600 files must persist across
renewals and interrupted operations. Use one shared protected journal and the
installation workflow's exclusion boundary, not copies on parallel hosts.

Authentication durably records intent and the bounded scoped record-ID inventory
before POST (including the parent entry on first journal creation), and persists
the returned record ID before reporting success. It checks the exact TXT value directly against
every discovered authoritative IPv4 DNS address; recursive resolver or provider
API success alone is insufficient. Aliased/delegated challenge routes are
rejected. Propagation waits at most ten minutes. Failure preserves the journal
and created TXT for retry or exact cleanup. No uncertain POST is retried.

A `pending` journal means the provider write outcome is uncertain. Stop and
reconcile its private intent with the provider's exact record inventory; do not
erase the journal, guess an ID or blindly repeat authentication. Cleanup reads
the recorded ID back and requires matching name/type/value/TTL before deleting
only that record. Other TXT values are preserved. An absent recorded ID completes
cleanup idempotently. GoDaddy supplies no conditional delete in this API: the
protected caller must exclude competing management of its recorded challenge
IDs between readback and DELETE. Arbitrary external DNS administration is not
made transactional by these hooks.

### Protected certificate qualification

`nebius-rollout` supports the manual `certificate` operation on protected `dev`:

```bash
gh workflow run nebius-rollout.yml --repo qianyi-sun/loom --ref dev -f operation=certificate
```

It does not require enabling automatic application rollout and cannot select
application deployment. The same workflow concurrency group serializes it with
rollout and inspection. There is **no renewal schedule yet**: recurring issuance
without corresponding Secret delivery/reload would give false confidence.

Configure `NEBIUS_CERTIFICATE_INSTALLATION_JSON` in the protected
`nebius-integration` GitHub Environment. This is non-secret installation metadata,
not the DNS token or private key. Its exact fields are:

```json
{
  "schema": "loom.nebius-certificate-installation.v1",
  "installation_id": "024cfbfb-a7e8-4d85-9c60-c1d838730f9a",
  "zone": "example.test",
  "child_domain": "dev.example.test",
  "management_host": "management.example.test",
  "credential_file": "/home/operator/.loom/private/godaddy.json",
  "state_dir": "/home/operator/.loom/nebius-certificates/state",
  "email": "operator@example.test"
}
```

These are examples, not provisioned names or an installation identity to reuse.
Choose a non-nil installation UUID; the state root must be a private owned
`nebius-certificates/state` below an existing trusted parent. Both subjects must
be below the selected DNS zone, and management must be outside the personal
child zone. The only requested SANs are the child wildcard and exact management
host. Use an operator contact email, or explicitly set `email` to `null` for an
ACME account without email. Arrange PAT rotation before its recorded expiry;
certificate lifetime does not extend credential lifetime.

The protected runner sends only two reviewed scripts, deterministic configuration,
the pinned uv executable and hash-locked wheel requirements. Certbot 5.8.0 is
installed in an isolated gateway virtual environment. DNS credentials, ACME
account keys, certificate keys, journals and private logs stay on the gateway;
they are not workflow artifacts or ingress mounts. It neither changes the old
operator environment nor runs hooks from personal source. At most eight
content-addressed tooling releases are retained; further new versions stop until
an operator retires exact obsolete tooling. State/accounts are never deleted as
part of tooling installation.

Issuance holds an owner-local lock and records durable intent. Existing pending,
created or unknown DNS journals block a new issuance, even if Certbot would use
a different challenge value. Failure retains its intent and previous selected
certificate. Do not delete `issuance.json` or retry until private process state,
ACME outcome and exact DNS record ownership have been reconciled. Automated
recovery of ambiguous issuance is not provided by this operation.

Successful issuance validates the leaf/key pair, exact two SANs, public trust
chain, server authentication, non-CA leaf and at least seven days of remaining
validity. A private generation is fsynced before atomic `selected.json` publication;
the prior generation remains available. Certbot's original account/lineage and
the challenge journal also remain private for recovery. Public evidence contains
only installation ID, generation/fingerprint, SANs, expiry and a fixed status.

`qualified` means a recoverable certificate exists on the gateway. It does **not**
mean public DNS, Kubernetes TLS Secret delivery, safe reload, shared ingress or
management is installed. Preserve standalone Caddy, its certificate/PVC and the
existing public LoadBalancer. The protected ingress installer must verify exact
Secret ownership/UID and delivered fingerprint before activation, and connect
renewal to verified reload before enabling a schedule.

### Render management manifests

Management HTTP requests default to a 1 MiB body limit, eight in-flight requests
per process and a 30-second total body-reception deadline, enforced before
parsing/authentication. Overload returns 503 with Retry-After rather than queuing;
oversize requests return 413. Response streaming is not buffered. Configure via
`LOOM_SVC_MANAGEMENT_HTTP_MAX_BODY_BYTES`,
`LOOM_SVC_MANAGEMENT_HTTP_MAX_INFLIGHT` and
`LOOM_SVC_MANAGEMENT_HTTP_BODY_TIMEOUT_SEC` only after accounting for raw bodies,
copies and parsing/application memory in the Pod envelope. These controls are
management-only and do not establish personal-upload or installed readiness.

Prepare the protected `loom.nebius-management-deployment.v1` JSON described in
[the management architecture](../architecture/nebius-primary-platform.md#management-deployment-manifests)
outside the repository. Its nested `installation.provider_runtime` is mandatory
for this deployment. Set its Kubernetes endpoint to the foundation's exact API
origin, `ca_file` to `/var/run/loom-management-kubernetes/ca.crt`,
`credentials_file` to `/var/run/loom-management-kubernetes/credentials.json`, and
`cloud_credentials_file` to `/var/run/loom-management-cloud/credentials.json`.
These are explicit projected-file paths, not an ambient operator login.

```bash
uv run --no-sync python scripts/ops/render_nebius_management.py \
  --deployment /secure/management-deployment.json \
  --candidate /secure/publication/candidate.json \
  --runtime-profile /secure/publication/runtime-profile.json \
  --output /secure/management-render
```

The output directory must not already exist. It and its files are private; errors
do not echo input values. `rendered-not-installed` reports the manifest list,
candidate, revision and fixed platform-resource envelope. Rendering validates
source identity, registry/digest binding and configuration, not successful remote
CI/publication or installed readiness. Select a protected `dev` publication, never
a personal snapshot or the retired integration branch.

All referenced Secrets belong only to the management namespace:
`loom-platform-db` (admin/service credentials and database CA),
`loom-management-db-tls`, `loom-platform-auth` (secret-store master key),
`loom-admin-secret`, `loom-management-publications` (`token`, read-only GitHub),
`loom-management-kubernetes` (`ca.crt`, `credentials.json`),
`loom-management-cloud` (`credentials.json`), and `loom-platform-storage`
(`backup-access-key`, `backup-secret-key`). Identical names in another namespace
do not authorize copying that namespace's values. Preserve generated keys and
their recovery material; Kubernetes/cloud identities must be separately scoped.

Do not pass this render to the standalone platform deployer or apply it manually.
Management ownership/readback, initial Secret delivery, HTTPS ingress/TLS,
off-node backup and the protected management rollout must be qualified before
activation. No `nebius-rollout` management-install operation is introduced by the
render-only command, and it purchases no capacity or storage.

## Before the first application

Use the independently configured Terraform platform state and its cluster ID/API
endpoint to fill the environment configuration. Create the dedicated integration
nodes and the two namespaces, then provision referenced secrets into their owning
namespace. This secret bootstrap is separate: the renderer never writes secret
values. The deployer checks names of nonempty keys through a Kubernetes template;
it never requests or records the values. Provision public DNS and TLS certificates
for the configured hostname and a private PostgreSQL certificate/CA for the exact
service hostname. Configure access to Nebius Registry and the independent backup
bucket. A renderer success does not prove these dependencies exist.

Render the published image references, review the Kubernetes output, then deploy
from an operator checkout. First prepare the
[locked operator environment](operator-runbook.md#locked-operator-environment).
Use `--no-sync` for the deployment command so it cannot implicitly modify that
environment. The deployment machine does not need the image source commit
checked out:

```sh
uv run --no-sync python scripts/ops/deploy_nebius_platform.py \
  --render-dir /secure/nebius-render \
  --kubeconfig /secure/nebius-kubeconfig \
  --expected-cluster-id mk8scluster-EXACT_ID \
  --evidence-dir /secure/nebius-deployment-evidence
```

Repeat the same command with `--apply` after reviewing the target and preflight.
Serialize deployments of this environment through the caller's workflow concurrency
group; do not run independent operator deployments simultaneously. This script
does not introduce another lock broker or silently steal an active deployment.

## Validation and phases

Historical Nebius databases at ambiguous revisions `0133`–`0136` require the
[qualified lineage conversion](nebius-lineage-conversion.md) before their first
dev migration. Preserve the backup and writer-quiescence requirements in that
runbook; ordinary deployment does not infer or stamp the historical lineage.

The deployer reads the known phase YAML once, taking environment settings from
the application ConfigMap. It checks
that resources belong to the two integration namespaces (plus their dedicated
collector RBAC). Reviewed replica/resource tuning and operator notes beside the
rendered files do not require re-signing, rehashing or a fresh Git checkout.
There is no hash inventory, candidate signature wrapper or deterministic rerender
gate. The existing control plane verifies runtime image admission at its own
boundary; the deployment scripts only transport that configuration.
The kubeconfig server must equal the separately configured API endpoint, its
cluster name must identify the expected Nebius cluster, TLS verification must use
the cluster CA, and provisioned integration nodes must have Nebius provider IDs.
The current observed Nebius kubeconfig convention is a name ending in
`cluster-<id suffix>` and Node provider IDs use `nebius://computeinstance-...`.

For an existing database that needs a new migration or candidate/configuration,
the first mutation is a new Job from the existing backup CronJob. That Job must
finish successfully before configuration, PostgreSQL, migrations or services are
changed. The backup uploader verifies the stored object's byte count and checksum
metadata before reporting success. A missing backup CronJob blocks the upgrade.

For a fresh database, the order is namespaces/configuration, PostgreSQL and
readiness, backup CronJob installation, the candidate migration Job, services and
readiness, catalog/capacity configuration, execution actuator, public entry, then
HTTPS API/frontend smoke. Installing backup immediately after database readiness
allows a partially completed first installation to resume with backup protection.

Completed candidate Jobs are reused. Pending Jobs are waited for. Failed Jobs
require a diagnosed fix followed by explicit `--retry-failed-jobs`; only the failed
Jobs belonging to this rendered candidate can then be replaced. The deployer never
deletes namespaces, PVCs, healthy Jobs, or unrelated resources. A retained database
PVC without its StatefulSet blocks application and requires an explicit restore.

Every attempt leaves a separate sanitized JSON phase record, including the candidate version, target
and failed phase. Evidence excludes kubeconfig material,
API endpoint details and secret values. A failed attempt is not automatically
rolled back: restore and database-version compatibility must be reviewed against
the retained backup before a downgrade. Reapplying a known candidate is not proof
that a database restore or schema downgrade is safe.

HTTPS health and frontend routing smoke are deployment checks only. User login,
real workload execution, result integrity and fault-recovery acceptance remain the
separate pure-Nebius E2E milestone.

**Verifying the deployed version (#2009):** confirm the rendered candidate SHA
actually reached the cluster by comparing it against what the running app
reports, not just this deployment's own logs. Open the target URL, click the
version entry at the bottom of the sidebar, and check the frontend commit
matches the candidate; separately curl `<target>/api/v1/version` (or open it
in the details) for the backend's own commit, keeping in mind one response is
evidence for the responding instance only — for a multi-replica rollout, check
more than one before declaring the rollout complete. Record this comparison
(URL, expected candidate, observed frontend/backend commits) as deployment
evidence; a green deploy script run or merged PR is not by itself live
version acceptance.
