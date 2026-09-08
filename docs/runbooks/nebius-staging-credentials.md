# Persistent staging identities and PostgreSQL trust

This is the staging-only credential workflow for the existing canonical Loom
database and object store. It is not a second control plane, a development
migration, or authority to take over another rollout. Infrastructure creation,
root installation, canonical rollout and runtime acceptance remain separate
authorized steps. Use the protected merged candidate and installed staging
broker; never run the development apply helper against staging.

## Mechanism and ownership

| Credential | Persistent source | Reconciliation |
| --- | --- | --- |
| Gateway/actuator DB passwords | Two canonical basic-auth Secrets | CNPG `spec.managed.roles` reads the labelled Secrets; separate LOGIN roles inherit existing `loom` privileges, with no calendar expiry or new database |
| Canonical input-store identity | Canonical `loom-nebius-staging-canonical-inputs` | Bootstrap creates the dedicated MinIO user and read-only artifacts policy; root credentials never leave the bootstrap process |
| Source spool | Optional Terraform `staging_spool` | Dedicated bucket/service account/non-expiring access key; protected EXPLICIT GetSecret retrieval, never state/evidence credentials |
| Collector token | Canonical `loom-nebius-staging-collector` | Stable token is persisted first, then its hash is registered idempotently in canonical staging with worker-only `execution:capacity:observe` scope and no expiry |
| Nebius observer and Kubernetes access | Authorized service-account keys | SDK/CLI exchange short-lived tokens automatically; no browser login or cached personal token |
| Database CA | Existing canonical `loom-postgres-ca/ca.crt` | Timer copies current CA into the remote directory-mounted Secret; no `subPath` or one-time CA copy |
| Gateway signing/master/local provider settings | Converged, running canonical Gateway | Timer copies active process settings, not newer unactivated Secret values |

Four generated identity Secrets are **create-only**. Retries reuse their bytes;
partial/foreign Secrets, revoked/expired collector tokens and disabled MinIO
users are errors, not requests to regenerate or silently re-enable authority.
Collector registration uses an automated, bounded transaction in the existing
staging CP; it is not a per-minute token-mint call or manual database operation.
No calendar deadline is introduced for these service identities. Deliberate
revocation, provider key replacement and release changes still require the
owning supported reconciliation; the timer does not override revocations or
promise that a third-party model provider will never disable a key.

## Database TLS binding

Native PostgreSQL TLS is passed through the private TCP listener unchanged.
The HTTPS certificate used for CP/MinIO is **not** the database certificate.
Add this to the existing private staging attachment:

```json
"database_tls": {
  "server_name": "loom-postgres-rw.loom-staging.svc.cluster.local",
  "ca_secret": {"name": "loom-nebius-staging-db-ca", "key": "ca.crt"}
}
```

Gateway/actuator resolve that name to the reserved gateway private address and
connect to port 15432. The generated `postgresql+psycopg` DSNs use the existing
database `loom`, the dedicated component role, `sslmode=verify-full` and
`sslrootcert=/var/run/loom/postgres-tls/ca.crt`. The Pod also explicitly sets
`PGSSLMODE`/`PGSSLROOTCERT`. CA mounts are read-only directories, readable by
UID 65532, and update with the Secret. Collector gets no DB alias or CA mount.
Omitting `database_tls` preserves the prior renderer output, but the persistent
credential CLI requires it. Never replace this with `sslmode=require`, disabled
verification, or the HTTPS name to make a handshake pass.

## Protected installation and input contract

The authorized installer installs the exact reviewed source under
`/opt/loom-nebius-staging-credentials/source` and its locked Python environment
(including Loom's cluster dependencies) under
`/opt/loom-nebius-staging-credentials/.venv`. Keep both root-owned and not
developer-writable. Install `kubectl`, Nebius CLI and the tested `mc` on its PATH;
do not download binaries at each timer invocation. Tested mc:
`RELEASE.2025-08-13T08-35-41Z`, SHA256
`a877fd0c183409da9f20f9d6e1811987298bbbca1aa03428eebdffba79fb9445`.

Use root-owned mode-0600 JSON at `/etc/loom/nebius-staging-credentials.json`:

```json
{
  "schema_version": "loom.nebius-staging-credentials.v1",
  "canonical_kubeconfig": "/etc/rancher/k3s/k3s.yaml",
  "canonical_namespace_uid": "<read-back loom-staging namespace UID>",
  "nebius_kubeconfig": "/etc/loom/nebius-staging.kubeconfig",
  "nebius_namespace_uid": "<read-back loom-nebius-staging namespace UID>",
  "nebius_cluster_id": "<approved mk8scluster ID>",
  "nebius_config": "/etc/loom/nebius-service-account.yaml",
  "nebius_profile": "<authorized service-account profile>",
  "attachment_file": "/etc/loom/nebius-staging-attachment.json",
  "terraform_spool_output": "/etc/loom/nebius-staging-spool-output.json",
  "observer_credentials_file": "/etc/loom/nebius-observer.json",
  "runtime_profile_file": "/etc/loom/nebius-runtime-profile.json",
  "admission_keyring_file": "/etc/loom/nebius-admission-keyring.json",
  "canonical_minio_endpoint": "https://<verified canonical private origin>:19443",
  "canonical_minio_address": "<canonical host WireGuard RFC1918 address>",
  "mc_binary": "/usr/local/bin/mc"
}
```

All referenced inputs are owner-only regular files. `terraform_spool_output`
is the protected result of `terraform output -json staging_spool` for the
reviewed applied spool, not a whole Terraform state dump. It contains key IDs,
not raw secret bytes; bootstrap retrieves GetSecret internally and checks the
returned AWS access-key ID. Runtime profile and keyring pass the application's
actual schema, image coverage and signature validators **before any write**.
Neither illustrative paths nor placeholders above form a deploy-ready package.

On the canonical host, use its own WireGuard listener address for
`canonical_minio_address`, **not** the Nebius VPC alias. Bootstrap passes a
per-command mc `--resolve HOST:PORT=IP` binding while preserving the HTTPS
hostname/SNI and certificate validation. This avoids depending on public DNS
or the known-unreachable host-to-ClusterIP path; it changes no global DNS or
`/etc/hosts`. Verify that exact private listener route after canonical rollout.

The source kubeconfig must be the live k3s-managed file, not an expiring copy.
The Nebius config must explicitly select an `auth-type: service account`
profile with service-account/public-key/private-key bindings. The `configure`
command generates the target kubeconfig from the approved cluster, sets the
explicit config/profile and `--no-browser`, and makes its exec plugin
noninteractive. Reconcile rejects static tokens/client certificates, human
profiles, overriding exec environments and ambiguous profile arguments.

## Ordered activation

Use the installed command below for each phase (replace only the phase word):

```bash
sudo env PYTHONPATH=/opt/loom-nebius-staging-credentials/source/src \
  /opt/loom-nebius-staging-credentials/.venv/bin/python \
  /opt/loom-nebius-staging-credentials/source/scripts/ops/nebius_staging_credentials.py \
  seed --config /etc/loom/nebius-staging-credentials.json
```

1. Coordinate the current staging request owner. Through the separately
   authorized root installation, configure the protected input package, apply
   the reviewed optional spool Terraform plan, and run `configure` then `seed`.
   Seed persists the four generated identities and source/runtime/admission
   inputs, but creates no SQL roles and makes no MinIO/database registration
   calls. It does not patch canonical Deployments or launch execution nodes.
2. Use a **new supported broker request**, not another initiator's request, for
   the exact canonical release/profile. This applies CNPG managed roles, the
   private entry and source configuration. Wait for role reconciliation, the
   intended schema, and canonical service health. Verify native DB TLS login
   and the approved private HTTPS endpoint. No bypass of a failed broker gate.
3. Run `bootstrap` once; retries are supported. It reuses seeds, configures the
   dedicated MinIO identity and registers/verifies the collector token. Root
   MinIO credentials are passed in an environment variable and user passwords
   via stdin; captured diagnostics never enter logs. It changes no default
   users, buckets, existing application passwords, or old collector tokens.
4. Establish the exact target namespace through the approved runtime manifest
   workflow, read its UID into the protected package, then run `reconcile`.
   It checks source and destination namespace identities, source CP attachment
   revision and converged canonical Gateway state before mapping Secrets.
   DSNs, spool binding, current CA, gateway identities, local providers and
   observer/collector credentials are derived automatically; no manual copying.
5. Apply the reviewed remote attachment and run `reconcile` again. Existing
   workloads must have the approved configuration revision before any Secret
   is changed. After every Secret succeeds, the helper patches only the three
   Nebius workload Pod templates with a separate credential revision. Gateway
   retains its drain hook/grace period. The collector's next scheduled Job
   reads the new projected credentials; running Jobs are not killed.
6. Install the two reviewed files under `deploy/systemd/` through the authorized
   installer, `systemctl daemon-reload`, then enable
   `loom-nebius-staging-credentials.timer`. It retries after completion every
   60 seconds (up to 5 seconds jitter) and starts after reboot. Confirm a
   successful oneshot and subsequent no-change invocation in the journal.

An unavailable API/network or partial write fails the current pass without
deleting resources; the next timer invocation resumes. Existing matching
Secrets and Pod annotations are no-ops. CA changes trigger Secret update and
graceful remote reconvergence; new database connections load the current CA.
Only aggregate counters and bounded error reasons are printed. Counter success
is **not** Pod readiness, a successful model call, or live acceptance.

For a new attachment/release or coordinated source-credential rotation, stop
this one timer during the protected upgrade, update the input package/shared
`configuration_revision`, seed and roll canonical consumers through the broker,
then reconcile and resume the timer. Do not overwrite the shared configuration
revision from the recurring job. The timer changes only the derived remote
credential revision and never creates a new canonical rollout autonomously.
It follows active canonical Gateway identities instead of racing an unactivated
master/signing-key Secret change. The non-expiring spool avoids periodic manual
renewal; deliberate spool rotation must update both consumers coherently.

To pause or roll back the synchronizer, disable/stop only
`loom-nebius-staging-credentials.timer` and wait for its oneshot to finish.
Preserve all canonical identity Secrets, CA, namespace and Terraform state.
Do not delete generated identities, revoke unrelated tokens or remove the
spool to roll back a code deployment. Roll back workload/config revisions only
through their supported deployment paths.

## Verification and remaining live boundary

Focused tests cover seed-before-register, retry identity reuse, partial-write
recovery, strict destination revision checks, source binding failures,
renewable authentication, malformed released inputs and secret-safe errors.
Disposable PostgreSQL verifies actual psycopg/SQLAlchemy TLS, bad hostname/CA,
server-certificate and CA replacement, existing connections, and stable
collector registration/revocation behavior. Disposable MinIO verifies real mc
bootstrap, repeated execution, input Get/List success, and Put/other-bucket
denial. The tested MinIO image matches the inspected live image; this is not a
claim that the bootstrap or timer has run on that live environment.

Reproduce TLS smoke with `python scripts/ops/smoke_nebius_database_tls.py`.
The two explicitly enabled disposable-backend tests and their environment
contracts are in `tests/ops/test_nebius_staging_identity_bootstrap.py`; never
point them at shared staging because they deliberately test revoke/disable.
Then perform separately authorized real namespace/CA propagation, restart and
ordinary-user full-artifact smoke, followed by the maximum current-quota batch
and scale-to-zero checks under #1765/#1538. Local tests do not close those issues.
