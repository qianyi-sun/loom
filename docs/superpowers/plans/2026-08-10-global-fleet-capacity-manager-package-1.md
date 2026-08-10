# Global Fleet Capacity Manager Package 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the versioned central management contracts, PostgreSQL shadow
ledger, topology-aware deterministic allocator simulation, and read-only
operator surfaces for all Loom environments and both physical pools.

**Architecture:** Add a new `loom_capacity_manager` package and an independent
Alembic migration tree backed by the management PostgreSQL database; no
environment database becomes global truth. Strict versioned contracts feed a
pure allocator, and a serializable writer stores complete shadow epochs with
the executable new-capacity ceiling constrained to zero in both configuration
and the database.

**Tech Stack:** Python 3.11+, Pydantic 2, SQLAlchemy 2 async, PostgreSQL 16,
Alembic 1.13+, FastAPI, Prometheus client, pytest 8, pytest-asyncio,
Hypothesis 6, Ruff, and strict mypy.

## Global Constraints

- The approved specification is
  `docs/superpowers/specs/2026-08-07-global-fleet-capacity-manager-design.md`.
- This package has no executable grant, admission, worker-token, launch-permit,
  Slurm submission, Slurm cancellation, or capacity-release path.
- `capacity_authority_state.executable_new_capacity_ceiling` is exactly `0`;
  a database check constraint rejects every other value.
- All contracts use `schema_version = 1`, reject unknown fields and versions,
  and use canonical nonnegative integer units; booleans, floats, overflow, and
  lossy conversion are invalid numeric input.
- `min_slots` defaults to `0`; `0 <= min_slots <= max_slots`, and every
  `max_slots` is finite.
- Priority order is exactly `production > staging > development`.
- Development fairness is hierarchical by immutable owner account and then
  subject; creating environments cannot create top-level fair shares.
- Users cannot configure capacity-account weights or physical-pool weights.
- Existing, proposed, accepted, pending, active, draining, stale, unknown, and
  quarantined commitments are charged until fenced release evidence exists.
- Demand reports use management-database receipt time for freshness; source
  clocks are diagnostic only.
- One immutable fleet-state generation owns controller, partition, node,
  resource-domain, and envelope data for each physical pool. Environment
  profiles may reference or narrow it but cannot redefine it.
- Every allocation result is deterministic for the same canonical input.
- Pure allocation contracts contain no wall-clock timestamp, random UUID, or
  database sequence; the store adds receipt/commit metadata outside the
  canonical allocator-output digest.
- Malformed or incomplete global configuration/topology, arithmetic overflow,
  or allocator timeout prevents a new epoch and records a bounded, secret-free
  audit reason. A stale, missing, invalid, or
  equivocal subject/pool report instead freezes increases only for that exact
  subject/pool, retains all known commitments, and cannot provide release
  evidence; independently valid domains may still receive a complete epoch.
- The existing `global_dev_fleet_autoscaler.py` and
  `shared_capacity_broker.py` remain legacy authorities during this package;
  this package neither reads their output as a grant nor changes their live
  behavior.

---

## Plan Series and Package Boundary

The approved design already decomposes the work into five dependency-ordered
packages. This document implements Package 1 only:

1. Management contracts and shadow ledger: this plan produces inert central
   contracts and shadow epochs.
2. Protected environment admission: the next plan consumes report/config
   contracts and remains fail-closed.
3. Grant and pool-executor protocol: this follows Package 2, consumes accepted
   allocations, and retains the zero executable ceiling.
4. Development lifecycle and candidate isolation: this follows the shared
   binding contracts and adds user lifecycle without capacity activation.
5. Fleet migration and activation: the last plan removes legacy writers and
   alone may authorize activation.

Package 1 deliberately excludes protected environment tables, claim guards,
reservation acceptance, launch permits, pool-executor mutation, candidate
builds, dynamic environment operations, cutover, and rollback. Their contract
identifiers appear in Package 1 only where needed to make inputs immutable and
forward-compatible.

## File Structure

### New runtime package

- `src/loom_capacity_manager/__init__.py`: public package version and exports.
- `src/loom_capacity_manager/contracts.py`: strict schema-v1 Pydantic models,
  canonical JSON, checked arithmetic, and digests.
- `src/loom_capacity_manager/fleet_state.py`: fleet-state and subject-state
  loading, immutable-generation validation, and legacy topology inventory
  comparison without merging the two authorities.
- `src/loom_capacity_manager/models.py`: ORM records for the independent
  management database.
- `src/loom_capacity_manager/schema_startup.py`: validate only the independent
  capacity Alembic head and emit the correct remediation command.
- `src/loom_capacity_manager/store.py`: serializable configuration/report
  ingestion, writer fencing, shadow-epoch commit, and audit queries.
- `src/loom_capacity_manager/topology.py`: bounded deterministic physical
  topology packing and witnesses.
- `src/loom_capacity_manager/allocator.py`: pure strict-tier, hierarchical
  progressive-filling shadow allocator.
- `src/loom_capacity_manager/reconciler.py`: complete-input validation and one
  fenced shadow reconciliation transaction.
- `src/loom_capacity_manager/metrics.py`: bounded-label shadow health metrics.
- `src/loom_capacity_manager/auth.py`: owner-only hashed principal registry,
  constant-time bearer verification, and subject/pool binding.
- `src/loom_capacity_manager/api.py`: authenticated configuration/report
  ingestion and read-only status routes; no grant route.
- `src/loom_capacity_manager/config.py`: management DB, auth, freshness, and
  bounded-computation settings.
- `src/loom_capacity_manager/__main__.py`: FastAPI service entry point.

### Independent management database

- `capacity_migrations/alembic.ini`: Alembic configuration using only
  `LOOM_CAPACITY_DB_URL`.
- `capacity_migrations/env.py`: metadata and online/offline migration runner.
- `capacity_migrations/script.py.mako`: repository-standard revision template.
- `capacity_migrations/versions/0001_shadow_management_schema.py`: initial
  management tables and shadow-only database constraints.

### Fleet configuration and operations

- `deploy/fleet-state/README.md`: ownership, publication, drift, and
  non-activation rules.
- `deploy/fleet-state/schema-v1.example.toml`: non-live example using synthetic
  nodes; no unresolved production topology is silently chosen.
- `scripts/ops/global_fleet_capacity_shadow_once.py`: one-shot shadow tick for
  deterministic evidence; it cannot emit a consumable grant.

### Tests

- `tests/unit/test_capacity_contracts.py`: strict parsing, canonical encoding,
  checked arithmetic, and version rejection.
- `tests/capacity_fixtures.py`: canonical builders shared by capacity unit,
  property, integration, API, and operations tests.
- `tests/unit/test_capacity_fleet_state.py`: authority, immutable generation,
  narrowing, and legacy-drift validation.
- `tests/unit/test_capacity_topology.py`: topology packing and witness tests.
- `tests/property/test_capacity_allocator.py`: invariant/property scenarios.
- `tests/unit/test_capacity_allocator.py`: deterministic priority, fairness,
  minima, stable placement, and tie-break examples.
- `tests/integration/test_capacity_management_migrations.py`: clean PostgreSQL
  migration and database constraints.
- `tests/integration/conftest.py`: create a separate temporary management
  database and async sessions without sharing the environment-test database.
- `tests/integration/test_capacity_management_store.py`: fencing,
  idempotency, equivocation, freshness, and atomic epoch commit.
- `tests/integration/test_capacity_manager_api.py`: authorization, strict
  payloads, status, and absence of executable routes.
- `tests/integration/test_capacity_manager_mtls.py`: real loopback TLS
  handshakes with trusted, missing, and untrusted client certificates.
- `tests/unit/test_capacity_auth.py`: owner-only registry validation,
  constant-time token checks, scopes, and identity binding.
- `tests/ops/test_global_fleet_capacity_shadow_once.py`: safe one-shot output.
- `tests/fixtures/capacity/fleet-v1.toml`: synthetic two-pool fleet.
- `tests/fixtures/capacity/subjects-v1.toml`: synthetic static subject policies
  and profile references.
- `tests/fixtures/capacity/snapshot-v1.json`: canonical multi-tier input.

### Documentation and CI

- `docs/architecture/global-fleet-capacity-manager.md`: link the approved spec,
  package state, data flow, and explicit non-activation statement.
- `.github/workflows/ci.yml`: collect the new unit/property/integration tests
  in the existing Python jobs; do not add a deployment job.

---

### Task 1: Strict Schema-v1 Contracts and Canonical Arithmetic

**Files:**

- Create: `src/loom_capacity_manager/__init__.py`
- Create: `src/loom_capacity_manager/contracts.py`
- Create: `tests/capacity_fixtures.py`
- Create: `tests/unit/test_capacity_contracts.py`

**Interfaces:**

- Consumes: only Pydantic and Python standard-library types.
- Produces: `ResourceVectorV1`, `ResourceDomainV1`, `WorkerShapeV1`,
  `FleetManifestV1`, `SubjectConfigurationV1`, `ConfigurationSnapshotV1`,
  `ConfigurationActivationV1`, `DemandSnapshotV1`, `PoolObservationV1`,
  `ProfileReferenceV1`,
  `FairnessCursorV1`, `AllocationInputV1`, `ShadowAllocationV1`,
  `ShadowEpochV1`, `CapacityContractError`, `MAX_QUANTITY`,
  `canonical_bytes(model) -> bytes`, `canonical_digest(model) -> str`, and
  `checked_add(left: int, right: int) -> int`.
- Produces test-only builders in `tests.capacity_fixtures`: `resource_vector`,
  `resource_vector_payload`, `node`, `shape`, `fleet_payload`, `fleet_manifest`,
  `subject_configuration`, `demand_snapshot`, `pool_observation`,
  `profile_reference`, `valid_profile_payload`, `configuration_activation`,
  `allocation_input`, `packing_request`, `shadow_epoch`, and the named
  `input_with_*`/`symmetric_owner_input` scenario builders used below,
  `BlockingAllocator`, `ChangingInputAllocator`, `TimeoutAllocator`, plus fixed
  UUID/SHA/time constants and `operator_token`/`reporter_token` auth-header
  builders. Every later test imports these names instead of defining
  incompatible local types.

- [ ] **Step 1: Write failing strict-contract tests**

```python
def test_unknown_version_extra_field_float_and_bool_fail_closed() -> None:
    valid = resource_vector_payload()
    for patch in (
        {"schema_version": 2},
        {"unexpected": 1},
        {"cpu_millicores": 1.5},
        {"memory_bytes": True},
    ):
        with pytest.raises(ValidationError):
            ResourceVectorV1.model_validate(valid | patch)


def test_canonical_digest_ignores_mapping_insertion_order() -> None:
    left = ResourceVectorV1.model_validate(
        resource_vector_payload(generic={"fpga": 1, "scratch_bytes": 4096})
    )
    right = ResourceVectorV1.model_validate(
        resource_vector_payload(generic={"scratch_bytes": 4096, "fpga": 1})
    )
    assert canonical_digest(left) == canonical_digest(right)


def test_checked_add_rejects_uint63_overflow() -> None:
    with pytest.raises(CapacityContractError, match="overflow"):
        checked_add(MAX_QUANTITY, 1)


def test_multi_node_shape_requires_exact_per_node_sum() -> None:
    with pytest.raises(ValidationError, match="node resources"):
        shape(
            concurrency_slots=2,
            total=resource_vector(cpu_millicores=4_000),
            per_node=(resource_vector(cpu_millicores=3_000),),
        )


def test_user_weight_fields_are_not_part_of_any_contract() -> None:
    with pytest.raises(ValidationError, match="pool_weight"):
        FleetManifestV1.model_validate(fleet_payload() | {"pool_weight": 2})
```

- [ ] **Step 2: Run the contract tests and confirm they fail**

Run:

```bash
uv run --frozen pytest tests/unit/test_capacity_contracts.py -q
```

Expected: collection fails because `loom_capacity_manager.contracts` does not
exist.

- [ ] **Step 3: Implement strict base types, canonical encoding, and checked arithmetic**

Use one frozen strict base model and exact integer bounds:

```python
SCHEMA_VERSION = 1
MAX_QUANTITY = (1 << 63) - 1
MAX_POOLS = 32
MAX_DOMAINS_PER_POOL = 128
MAX_NODES_PER_DOMAIN = 4_096
MAX_SUBJECTS = 10_000
MAX_SHAPES_PER_PROFILE = 256
MAX_DEMAND_BUCKETS_PER_REPORT = 2_048
MAX_ASSIGNMENTS_PER_REPORT = 10_000
MAX_FIXED_CLAIMS_PER_REPORT = 10_000
MAX_CONTRACT_BYTES = 8 * 1024 * 1024


class StrictV1Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[1] = 1


class ResourceVectorV1(StrictV1Model):
    slots: int = Field(ge=0, le=MAX_QUANTITY)
    cpu_millicores: int = Field(ge=0, le=MAX_QUANTITY)
    memory_bytes: int = Field(ge=0, le=MAX_QUANTITY)
    gpu_count: int = Field(ge=0, le=MAX_QUANTITY)
    generic: dict[str, int] = Field(default_factory=dict)


def canonical_bytes(model: StrictV1Model) -> bytes:
    payload = model.model_dump(mode="json", exclude_none=False)
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def canonical_digest(model: StrictV1Model) -> str:
    return hashlib.sha256(canonical_bytes(model)).hexdigest()
```

Define UUID/incarnation fencing fields and finite bounded tuple fields on all
report and allocation models. Normalize timestamps to UTC on validation but do
not use source timestamps for admission or release decisions. Validate generic
resource keys with `^[a-z][a-z0-9_.-]{0,62}$`, sort them in canonical output,
and reject duplicate IDs in every manifest.
Validators sort semantically unordered tuples by their stable identity before
digesting: pools, domains, nodes, accounts, subjects, profiles, shapes,
capabilities, features, assignments, claims, and observations. Ordered policy
fields—tier priority, local task-priority band, fairness cursor, and launch
rank—retain their declared order and validate uniqueness explicitly.

Keep configuration authorities distinct in the contracts:

- `FleetManifestV1` contains the zero executable ceiling, strict tier
  policies, static service-account policies and dynamic
  account-policy templates, immutable pool/resource-domain generations, and
  protocol generations. Each pool has one trusted read-only pool-reporter
  incarnation binding. It contains no subject display name, candidate, or
  environment worker profile.
- `SubjectConfigurationV1` contains one immutable subject UUID/incarnation,
  display name, account and tier references, finite min/max/surge values,
  lifecycle/candidate/deployment generation, and required worker profiles.
  It also binds the one trusted demand-reporter incarnation for that subject.
  Every profile references fleet-owned pool/protocol generations by ID,
  generation, and digest and may contain only domain-narrowing fields plus its
  exact worker-shape catalog.
- `ConfigurationSnapshotV1` contains one fleet generation/digest plus a sorted
  tuple of subject generation/digest references. It is the immutable composed
  configuration used by one allocation input.

Define the remaining structural types in `contracts.py` exactly as follows:

```python
class NodeEnvelopeV1(StrictV1Model):
    node_id: str
    allocatable: ResourceVectorV1
    features: tuple[str, ...] = ()


class ResourceDomainV1(StrictV1Model):
    domain_id: str
    architecture: Literal["x86_64", "arm64"]
    partition: str
    nodes: tuple[NodeEnvelopeV1, ...]
    topology_constraints: dict[str, str] = Field(default_factory=dict)


class WorkerShapeV1(StrictV1Model):
    shape_id: str
    concurrency_slots: int = Field(gt=0, le=MAX_QUANTITY)
    total_resources: ResourceVectorV1
    node_resources: tuple[ResourceVectorV1, ...]
    compatible_domain_ids: tuple[str, ...]
    capabilities: tuple[str, ...]
    placement_constraints: dict[str, str] = Field(default_factory=dict)
    warm_approved: bool = False


class ProfileReferenceV1(StrictV1Model):
    pool_id: str
    pool_generation: int = Field(gt=0, le=MAX_QUANTITY)
    pool_digest: str
    protocol_generation: int = Field(gt=0, le=MAX_QUANTITY)
    protocol_digest: str
    eligible_resource_domains: tuple[str, ...]
    worker_shapes: tuple[WorkerShapeV1, ...]
```

Require `1 <= len(node_resources) <= MAX_NODES_PER_DOMAIN` and validate with
checked arithmetic that their component-wise sum exactly equals
`total_resources`. The shape's node count is `len(node_resources)`; neither the
allocator nor executor may divide a total vector and invent per-node costs.
Apply the listed collection bounds to every nested manifest/report and reject
canonical encoded payloads above `MAX_CONTRACT_BYTES` before persistence.

Also define `DemandBucketV1`, `CurrentAssignmentV1`, `FixedClaimV1`,
`ObservedCommitmentV1`, `PackingRequestV1`, and `PackingWitnessV1` with stable
identities and exact subject, incarnation, candidate, deployment, pool,
profile, demand/pool reporter, sequence, resource-vector, and state bindings. A
demand snapshot carries pending-unassigned buckets, current assignments, and
fixed claims as three separate tuples. A shadow allocation carries desired
shape instances, protected-claim slots, retained physical commitments, drains,
placement allowances, a claim-to-worker-slot witness, and the joint
assignment/allowance matching witness. `AllocationInputV1` also carries
the last committed two-level fairness cursors, existing pending slot/job
commitments, and configured rate limits. `ShadowEpochV1` carries the next
cursors, bounded hypothetical launch rank, and explicit pending blockers. Each
ranked entry says `rate_state="unavailable_package_1"`; these diagnostics are
not permits and cannot be consumed. Package 3 alone adds durable DB-time rate
buckets and eligibility.

Represent each subject and pool input with an explicit
`InputFreshnessV1 = valid | stale | missing | invalid | equivocal` state and its
last accepted canonical payload/digest. Only `valid` demand can request an
increase or prove a release. Every other state retains and charges its last
known fixed/physical commitments and emits a scoped blocker without supplying
new demand.

- [ ] **Step 4: Run focused tests, lint, and type checking**

```bash
uv run --frozen pytest tests/unit/test_capacity_contracts.py -q
uv run --frozen ruff check \
  src/loom_capacity_manager/contracts.py \
  tests/unit/test_capacity_contracts.py
uv run --frozen mypy src/loom_capacity_manager/contracts.py
```

Expected: all commands pass.

- [ ] **Step 5: Commit the contract slice**

```bash
git add \
  src/loom_capacity_manager/__init__.py \
  src/loom_capacity_manager/contracts.py \
  tests/capacity_fixtures.py \
  tests/unit/test_capacity_contracts.py
git commit -m "feat: define global capacity contracts"
```

---

### Task 2: Single-Source Fleet-State Validation and Drift Inventory

**Files:**

- Create: `src/loom_capacity_manager/fleet_state.py`
- Create: `deploy/fleet-state/README.md`
- Create: `deploy/fleet-state/schema-v1.example.toml`
- Create: `tests/fixtures/capacity/fleet-v1.toml`
- Create: `tests/fixtures/capacity/subjects-v1.toml`
- Create: `tests/unit/test_capacity_fleet_state.py`

**Interfaces:**

- Consumes: `FleetManifestV1`, `ResourceDomainV1`, and `WorkerShapeV1` from
  Task 1.
- Produces: `load_fleet_manifest(path: Path) -> FleetManifestV1`,
  `load_subject_configuration(path: Path) -> tuple[SubjectConfigurationV1, ...]`,
  `validate_profile_narrowing(manifest, profile) -> None`,
  `TopologyInventoryReport`, `FleetStateError`, and
  `inventory_legacy_topology(paths: Sequence[Path]) -> TopologyInventoryReport`.

- [ ] **Step 1: Write failing topology-authority tests**

```python
def test_environment_profile_can_narrow_but_not_redefine_pool() -> None:
    manifest = load_fleet_manifest(FIXTURES / "fleet-v1.toml")
    validate_profile_narrowing(
        manifest,
        profile_reference(manifest, eligible_resource_domains=("gb10-arm",)),
    )
    with pytest.raises(ValidationError, match="controller"):
        ProfileReferenceV1.model_validate(
            valid_profile_payload(manifest)
            | {"controller": "different-controller"}
        )


def test_current_legacy_replacement_node_drift_is_reported() -> None:
    report = inventory_legacy_topology(
        (
            Path("deploy/environment-state/development.toml"),
            Path("deploy/environment-state/staging.toml"),
            Path("deploy/environment-state/production.toml"),
        )
    )
    assert not report.clean
    assert {conflict.pool_id for conflict in report.conflicts} == {"gb10", "oldlab"}
```

- [ ] **Step 2: Run the fleet-state tests and confirm they fail**

```bash
uv run --frozen pytest tests/unit/test_capacity_fleet_state.py -q
```

Expected: import fails because `fleet_state.py` does not exist.

- [ ] **Step 3: Implement manifest loading and single-source validation**

`load_fleet_manifest` must parse TOML, validate with `FleetManifestV1`, compute
the manifest and nested generation digests from canonical JSON, and reject a
digest supplied by the file when it differs. Require the exact tier order and
both initial pools. `load_subject_configuration` parses a separate document and
requires every subject profile to reference a fleet pool and protocol by
`(ID, generation, digest)`; it permits only `eligible_resource_domains` to
narrow fleet topology. Loading either document alone has no side effect.

The legacy inventory parser is diagnostic only. It extracts controller,
partition, allowed nodes, requested vectors, and packing fields from every
legacy `worker_pool_autoscaler_policies` row, compares values by pool, and
returns structured conflicts without selecting a winner. The checked-in
example uses synthetic node names and begins with:

```toml
schema_version = 1
executable_new_capacity_ceiling = 0

[[tiers]]
id = "production"
priority = 0

[[tiers]]
id = "staging"
priority = 1

[[tiers]]
id = "development"
priority = 2
```

Do not add a live fleet manifest while the current GB10/OLDLAB copies conflict.
The README must say a reviewed operator reconciliation creates that file; the
validator never copies one environment's topology over another.

- [ ] **Step 4: Run tests and verify the diagnostic against current files**

```bash
uv run --frozen pytest tests/unit/test_capacity_fleet_state.py -q
uv run --frozen python -m loom_capacity_manager.fleet_state \
  inventory-legacy \
  deploy/environment-state/development.toml \
  deploy/environment-state/staging.toml \
  deploy/environment-state/production.toml
```

Expected: tests pass; the command exits `2` and prints bounded JSON with GB10
and OLDLAB conflicts and no credentials.

- [ ] **Step 5: Commit the fleet-state slice**

```bash
git add \
  src/loom_capacity_manager/fleet_state.py \
  deploy/fleet-state \
  tests/fixtures/capacity/fleet-v1.toml \
  tests/fixtures/capacity/subjects-v1.toml \
  tests/unit/test_capacity_fleet_state.py
git commit -m "feat: validate global fleet state"
```

---

### Task 3: Independent PostgreSQL Shadow Schema

**Files:**

- Create: `src/loom_capacity_manager/models.py`
- Create: `src/loom_capacity_manager/schema_startup.py`
- Create: `capacity_migrations/alembic.ini`
- Create: `capacity_migrations/env.py`
- Create: `capacity_migrations/script.py.mako`
- Create: `capacity_migrations/versions/0001_shadow_management_schema.py`
- Modify: `tests/integration/conftest.py`
- Create: `tests/integration/test_capacity_management_migrations.py`

**Interfaces:**

- Consumes: canonical JSON payloads and digests from Task 1.
- Produces: an independent management schema at Alembic revision
  `capacity_0001`, imported as `loom_capacity_manager.models.Base.metadata`,
  `assert_capacity_schema_at_head(engine) -> int`,
  plus `capacity_postgres_url`, `empty_capacity_engine`,
  `capacity_session_factory`, and `capacity_session` pytest fixtures.

- [ ] **Step 1: Write a failing migration/constraint integration test**

```python
def test_shadow_schema_has_zero_execution_guard(capacity_postgres_url: str) -> None:
    engine = create_engine(capacity_postgres_url)
    with engine.begin() as connection:
        tables = set(inspect(connection).get_table_names())
        assert EXPECTED_TABLES <= tables
        with pytest.raises(IntegrityError):
            connection.execute(
                text(
                    "UPDATE capacity_authority_state "
                    "SET executable_new_capacity_ceiling = 1 WHERE singleton_id = 1"
                )
            )


async def test_capacity_schema_error_never_names_environment_migrations(
    empty_capacity_engine: AsyncEngine,
) -> None:
    with pytest.raises(CapacitySchemaNotAtHeadError) as caught:
        await assert_capacity_schema_at_head(empty_capacity_engine)
    assert "capacity_migrations/alembic.ini" in str(caught.value)
    assert "migrations/alembic.ini" not in str(caught.value).replace(
        "capacity_migrations/alembic.ini", ""
    )
```

Set `EXPECTED_TABLES` to the exact table names listed in Step 3.

- [ ] **Step 2: Run the migration test and confirm it fails**

```bash
uv run --frozen pytest tests/integration/test_capacity_management_migrations.py -q
```

Expected: the Alembic configuration or initial revision is missing.

- [ ] **Step 3: Create the independent schema and immutable constraints**

Create these tables with UUID primary keys unless a key is shown:

- `capacity_authority_state`: `singleton_id=1`, authority UUID, writer epoch,
  schema version, recovery and increase-freeze state/reason, zero executable
  ceiling, global pending/rate ceilings, and timestamps.
- `capacity_config_generations`: `fleet|subject` scope, nullable
  subject/incarnation binding, scope generation, immutable digest/payload,
  `proposed|active|retired` state, actor, idempotency key, and creation time.
- `capacity_configuration_epochs`: monotonic configuration epoch, active fleet
  generation/digest, sorted complete subject-generation manifest, canonical
  digest, and database creation time.
- `capacity_tiers`: config generation, tier ID, exact priority, slot/resource
  ceilings, pending ceilings, and unique generation/tier and priority keys.
- `capacity_account_policies`: immutable account/template ID, kind, optional
  owner UUID, min/max/surge/pending/lifecycle/build/artifact quotas, and no
  weight column.
- `capacity_fairness_state`: `mode='shadow'`, tier/account or account/subject
  scope, minimum/demand cursor IDs, last shadow epoch, unique scope, and no
  weight column.
- `capacity_pools`: pool generation/digest, controller/partition/association
  identity, topology, envelopes, health, pending/rate limits, and unique
  pool-generation binding.
- `capacity_subjects`: subject UUID/incarnation, display name, account/tier,
  finite min/max/surge, lifecycle, candidate/deployment/config generations;
  display name is not the primary key.
- `capacity_candidates`: immutable candidate digest and
  source/artifact/architecture/launcher/attestation/protocol payloads.
- `capacity_deployment_generations`: subject/incarnation/generation, candidate
  digest, required profile set, readiness/lifecycle/cutover fields, and unique
  exact binding.
- `capacity_worker_profiles`: subject/deployment/pool/profile generation and
  digest, exact shape catalog, and narrowing constraints; no global topology
  copy.
- `capacity_demand_reporters`: subject/incarnation/reporter UUID, high-water,
  state, current binding, last receipt time, and digest.
- `capacity_demand_snapshots`: reporter/sequence, canonical digest/payload,
  database receipt time, validity/acknowledgement, and unique sequence.
- `capacity_pool_reporters`: pool/reporter incarnation, high-water, state,
  current pool-generation binding, last receipt time, and digest.
- `capacity_pool_observations`: pool/reporter incarnation/sequence, canonical
  digest/payload, database receipt time, and validity; no mutation instruction.
- `capacity_observed_commitments`: stable claim or physical identity, exact
  source/binding/vector, `observed|unknown|quarantined` state, and first/last
  reporter high-water/receipt time. Package 1 has no released state.
- `capacity_allocation_epochs`: allocation/writer/config epochs, input digest,
  `shadow|failed` status, failure reason, complete payload, timestamps, and
  `executable=false`.
- `capacity_allocations`: epoch and exact subject/pool/deployment binding,
  desired shapes/resources, commitments, drains, allowances, witness,
  `mode='shadow'`, `executable=false`, and unique epoch/subject/pool key.
- `capacity_audit_events`: monotonic bigint ID, actor kind/ID, event kind,
  object binding, bounded JSON detail, and database timestamp.

Use `ON DELETE RESTRICT` for immutable history. Add checks for nonnegative
integers, strict tier names, finite `min <= max`, nonempty IDs, SHA-256
digests, and the two independent zero-execution guards:

```python
sa.CheckConstraint(
    "executable_new_capacity_ceiling = 0",
    name="capacity_authority_shadow_only_check",
)
sa.CheckConstraint(
    "mode = 'shadow' AND executable = false",
    name="capacity_allocations_shadow_only_check",
)
sa.CheckConstraint(
    "mode = 'shadow'",
    name="capacity_fairness_state_shadow_only_check",
)
```

The migration inserts exactly one authority row with a migration-generated
UUID, writer epoch zero, recovery state `shadow`, increase freeze true, and
executable ceiling zero. Fleet configuration cannot replace that UUID; only a
future separately fenced recovery procedure may change the authority
incarnation.

The capacity Alembic environment must read only `LOOM_CAPACITY_DB_URL`; it must
not fall back to `LOOM_DB_URL` or `LOOM_CP_DB_URL`.
`assert_capacity_schema_at_head` reads only
`capacity_migrations/alembic.ini`; its error tells the operator to export the
owner-only runtime URL as `LOOM_CAPACITY_DB_URL` and run
`alembic -c capacity_migrations/alembic.ini upgrade head`. It never mentions or
loads `migrations/alembic.ini`.

Extend `tests/integration/conftest.py` with a session-scoped fixture that uses
the existing PostgreSQL container's administrative connection to create a
uniquely named migrated database `f"loom_capacity_test_{os.getpid()}"` and an
empty startup-check database `f"loom_capacity_empty_{os.getpid()}"`. Rewrite
each DSN with `sqlalchemy.engine.URL.set(database=database_name)`, run only
`capacity_migrations/alembic.ini` against the migrated database, and drop both
after every capacity engine is disposed. The management and environment test
schemas must never share an `alembic_version` table.

- [ ] **Step 4: Run migration, downgrade/upgrade, and model checks**

```bash
uv run --frozen pytest tests/integration/test_capacity_management_migrations.py -q
uv run --frozen ruff check \
  src/loom_capacity_manager/models.py \
  src/loom_capacity_manager/schema_startup.py \
  capacity_migrations \
  tests/integration/test_capacity_management_migrations.py
uv run --frozen mypy src/loom_capacity_manager/models.py
```

Expected: schema creation, zero-execution rejection, one downgrade, and a
second upgrade all pass.

- [ ] **Step 5: Commit the database slice**

```bash
git add \
  src/loom_capacity_manager/models.py \
  src/loom_capacity_manager/schema_startup.py \
  capacity_migrations \
  tests/integration/conftest.py \
  tests/integration/test_capacity_management_migrations.py
git commit -m "feat: add shadow capacity management schema"
```

---

### Task 4: Fenced Configuration and Monotonic Report Store

**Files:**

- Create: `src/loom_capacity_manager/store.py`
- Create: `tests/integration/test_capacity_management_store.py`

**Interfaces:**

- Consumes: Task 1 contracts and Task 3 ORM models.
- Produces the `CapacityManagementStore` methods
  `propose_fleet_configuration`, `propose_subject_configuration`,
  `activate_configuration`, `register_writer`, `ingest_demand_snapshot`,
  `ingest_pool_observation`, `load_allocation_input`, `commit_shadow_epoch`,
  and `status` with the signatures below.

- [ ] **Step 1: Write failing transaction and fencing tests**

Define the test-local `reporter_state`, `allocation_epoch_count`, and
`active_configuration_epoch` helpers as direct SQLAlchemy selects against the
Task 3 models; they must not call the store method under test. Define
`registered_writer` by reading the seeded authority UUID and calling
`register_writer` once for the test transaction.

```python
async def test_exact_report_replay_is_idempotent_but_equivocation_fences(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    report = demand_snapshot(sequence=7)
    first = await store.ingest_demand_snapshot(capacity_session, report, actor="dev-a")
    replay = await store.ingest_demand_snapshot(capacity_session, report, actor="dev-a")
    assert first.snapshot_id == replay.snapshot_id
    assert replay.replayed

    changed = report.model_copy(update={"pending_unassigned": ()})
    with pytest.raises(ReportEquivocationError):
        await store.ingest_demand_snapshot(capacity_session, changed, actor="dev-a")
    assert await reporter_state(capacity_session, report.reporter_incarnation) == "fenced"


async def test_stale_writer_cannot_commit_complete_shadow_epoch(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    old = await store.register_writer(capacity_session, authority_uuid(), expected_epoch=0)
    new = await store.register_writer(
        capacity_session, authority_uuid(), expected_epoch=old.writer_epoch
    )
    with pytest.raises(StaleWriterError):
        await store.commit_shadow_epoch(capacity_session, old, shadow_epoch())
    assert await allocation_epoch_count(capacity_session) == 0
    assert new.writer_epoch == old.writer_epoch + 1


async def test_incompatible_configuration_proposals_never_become_partly_active(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(pool_generation=2),
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    subject = await store.propose_subject_configuration(
        capacity_session,
        subject_configuration(pool_generation=1),
        actor="environment-state",
        idempotency_key=CONFIG_KEY_B,
    )
    with pytest.raises(ConfigurationConflictError, match="pool generation"):
        await store.activate_configuration(
            capacity_session,
            configuration_activation(fleet=fleet, subjects=(subject,)),
            actor="fleet-operator",
            idempotency_key=ACTIVATION_KEY,
        )
    assert await active_configuration_epoch(capacity_session) == 0


async def test_newer_report_omission_cannot_release_observed_commitment(
    capacity_session: AsyncSession,
    registered_writer: WriterFence,
) -> None:
    store = CapacityManagementStore()
    first = demand_snapshot(sequence=1, fixed_claim_ids=("claim-a",))
    second = demand_snapshot(sequence=2, fixed_claim_ids=())
    await store.ingest_demand_snapshot(capacity_session, first, actor="dev-a")
    await store.ingest_demand_snapshot(capacity_session, second, actor="dev-a")
    allocation = await store.load_allocation_input(
        capacity_session,
        registered_writer,
    )
    assert allocation.observed_commitment_ids == ("claim-a",)
```

- [ ] **Step 2: Run the store tests and confirm they fail**

```bash
uv run --frozen pytest tests/integration/test_capacity_management_store.py -q
```

Expected: import fails because `CapacityManagementStore` is missing.

- [ ] **Step 3: Implement serializable, digest-bound store operations**

Every mutating method starts a PostgreSQL `SERIALIZABLE` transaction, locks
the singleton authority row, and writes the state change plus an audit event in
the same transaction. Implement these exact public interfaces:

- `propose_fleet_configuration(session, manifest, *, actor, idempotency_key)`
  returns `ProposedConfiguration`.
- `propose_subject_configuration(session, subject, *, actor, idempotency_key)`
  returns `ProposedConfiguration`.
- `activate_configuration(session, proposal, *, actor, idempotency_key)`
  returns `ActivatedConfiguration`.
- `register_writer(session, authority_incarnation, *, expected_epoch)` returns
  `WriterFence`.
- `ingest_demand_snapshot(session, report, *, actor)` and
  `ingest_pool_observation(session, report, *, actor)` return `IngestResult`.
- `load_allocation_input(session, writer)` returns `AllocationInputV1`.
- `commit_shadow_epoch(session, writer, epoch)` returns
  `CommittedShadowEpoch`.
- `status(session, *, cursor, limit)` returns `CapacityStatusPageV1` and
  enforces `1 <= limit <= 500`.

`register_writer` accepts only the current durable authority incarnation and
the exact current writer epoch, then increments the writer epoch once. An
authority-incarnation mismatch is a recovery error, not ordinary writer
takeover. Package 1 exposes no authority-incarnation mutation endpoint.

The two proposal methods insert immutable scoped generations but never change
the active configuration. A fleet proposal cannot modify a subject payload; a
subject proposal cannot modify pool, tier, account-template, or protocol
payloads. `ConfigurationActivationV1` contains the exact expected active
configuration epoch, one proposed or retained fleet generation/digest, and the
complete sorted proposed/retained subject generation/digest manifest.
`activate_configuration` validates every cross-reference, account/subject
aggregate, required dual-pool profile, protocol, topology narrowing, finite
limit, and zero executable ceiling under a serializable lock, then appends one
`capacity_configuration_epochs` row and changes proposal states atomically.
It registers the exact configured demand and pool reporter incarnations and
fences their predecessors in that same transaction; a report cannot create or
rotate an incarnation.
Package 1 cannot delete a subject: every active subject must be retained or
replaced by an exact successor proposal in the complete manifest. Stale
expected epoch, a missing subject, or one incompatible reference changes
nothing. There is no generic merged payload write.

Report receipt time is always PostgreSQL `clock_timestamp()` and is not a
method argument. A reporter sequence below high-water is stale; equal sequence
plus equal canonical digest is replay; equal sequence plus different digest is
equivocation and fences the incarnation. A newly registered incarnation fences
its predecessor and begins at sequence zero. `load_allocation_input` includes
the active immutable composed configuration, current freshness/equivocation
state plus last accepted complete report for every enabled subject and pool,
and every reported commitment. It never infers release from a missing or
invalid report. It also locks and loads the current tier/account and
account/subject fairness cursors. A successful
`commit_shadow_epoch` advances those cursors with the epoch; a failed or stale
epoch advances none.

Report ingestion monotonically inserts or refreshes
`capacity_observed_commitments`. A missing identity, source-reported terminal
state, reporter rotation, or newer snapshot cannot delete or release one;
conflicting identity/binding/vector evidence adds a quarantined charge. Package
1 intentionally has no store method, model state, or API that marks an observed
commitment released. Protected/physical release transitions arrive only with
Packages 2 and 3.

These cursors are explicitly shadow simulation state. No later executable
package may adopt them as service history; activation initializes or imports a
separately reviewed executable fairness state from the cutover snapshot.

`commit_shadow_epoch` compares writer epoch, config generation, input digest,
every demand/pool reporter high-water, and freshness against a new PostgreSQL
`clock_timestamp()` read in one transaction. A report that crosses its
freshness boundary during allocation rejects the stale input and reruns with a
scoped freeze. The store inserts the complete epoch and all child allocations
or inserts none. It never updates a legacy broker, environment DB, grant file,
or Slurm record.

- [ ] **Step 4: Run store tests including concurrent serializable races**

```bash
uv run --frozen pytest tests/integration/test_capacity_management_store.py -q
uv run --frozen mypy src/loom_capacity_manager/store.py
```

Expected: exact replay, stale/equivocal reports, two-writer races, proposal
idempotency, incompatible/stale activation rejection, immutable generations,
and all-or-nothing epoch commit pass.

- [ ] **Step 5: Commit the store slice**

```bash
git add src/loom_capacity_manager/store.py tests/integration/test_capacity_management_store.py
git commit -m "feat: persist fenced capacity shadow state"
```

---

### Task 5: Deterministic Topology Packer

**Files:**

- Create: `src/loom_capacity_manager/topology.py`
- Create: `tests/unit/test_capacity_topology.py`

**Interfaces:**

- Consumes: resource domains, fixed commitments, and worker shapes from Task 1.
- Produces: `pack_topology(request: PackingRequestV1) -> PackingWitnessV1` and
  `TopologyInfeasible`, `TopologySearchLimit` exceptions.

- [ ] **Step 1: Write failing fragmentation, pin, and determinism tests**

```python
def test_aggregate_resources_do_not_hide_per_node_infeasibility() -> None:
    request = packing_request(
        nodes=(node("a", cpu=8, memory=16), node("b", cpu=8, memory=16)),
        shapes=(shape("wide", count=1, cpu=12, memory=8),),
    )
    with pytest.raises(TopologyInfeasible):
        pack_topology(request)


def test_same_input_has_byte_identical_witness_across_orderings() -> None:
    first = pack_topology(fragmented_request(reverse=False))
    second = pack_topology(fragmented_request(reverse=True))
    assert canonical_bytes(first) == canonical_bytes(second)


def test_old_commitment_above_new_envelope_is_charged_over_limit() -> None:
    witness = pack_topology(request_with_old_generation_commitment_over_limit())
    assert witness.over_limit_slots == 2
    assert witness.new_placement_allowed is False
    assert witness.charged_commitment_ids == ("old-worker-a",)
```

- [ ] **Step 2: Run topology tests and confirm they fail**

```bash
uv run --frozen pytest tests/unit/test_capacity_topology.py -q
```

Expected: import fails because `topology.py` does not exist.

- [ ] **Step 3: Implement conservative bounded packing**

Validate and charge fixed commitments first under their immutable historical
pool/profile vectors. When their node identity still exists in the current
generation, subtract the exact node/domain vector. When it does not, or their
total exceeds a lowered envelope, record an over-limit/unknown witness and
prohibit new placement in the affected scope; never drop or reinterpret the
commitment and never raise `TopologyInfeasible` merely because old capacity is
over-limit. Sort new shapes by fewest compatible domains, descending dominant
resource fraction, descending node count, then stable shape identity. Search
canonical nodes by residual vector with symmetry elimination and memoize
`(shape_index, sorted_residual_vectors)`. The injected `SearchBudget` enforces
both a state limit and monotonic deadline:

If a reported historical commitment cannot map uniquely to an approved shape,
charge the largest compatible shape. If no finite mapping is provable, charge
the pool's entire remaining envelope. Conflicting accepted/observed bindings
are charged separately and marked quarantined until a later fenced recovery
proves one physical identity; the packer never merges them by name or timing.

```python
@dataclass(frozen=True, slots=True)
class SearchBudget:
    max_states: int = 250_000
    deadline_seconds: float = 0.5


def pack_topology(
    request: PackingRequestV1,
    *,
    budget: SearchBudget = SearchBudget(),
    monotonic: Callable[[], float] = time.monotonic,
) -> PackingWitnessV1:
    return _PackingSearch(request, budget=budget, monotonic=monotonic).solve()
```

The float deadline is process control, never allocatable capacity. A search
limit raises `TopologySearchLimit`; it never returns a partial witness. The
witness lists each stable shape instance, exact node/domain placement, and
post-placement residual vector so it can be independently replayed.

- [ ] **Step 4: Run focused and generated topology checks**

```bash
uv run --frozen pytest tests/unit/test_capacity_topology.py -q
uv run --frozen ruff check \
  src/loom_capacity_manager/topology.py \
  tests/unit/test_capacity_topology.py
uv run --frozen mypy src/loom_capacity_manager/topology.py
```

Expected: all checks pass, including deadline and state-limit failure cases.

- [ ] **Step 5: Commit the topology slice**

```bash
git add src/loom_capacity_manager/topology.py tests/unit/test_capacity_topology.py
git commit -m "feat: add bounded capacity topology packing"
```

---

### Task 6: Pure Hierarchical Shadow Allocator

**Files:**

- Create: `src/loom_capacity_manager/allocator.py`
- Create: `tests/unit/test_capacity_allocator.py`
- Create: `tests/property/test_capacity_allocator.py`

**Interfaces:**

- Consumes: `AllocationInputV1` from Task 1 and `pack_topology` from Task 5.
- Produces: `allocate_shadow(input: AllocationInputV1) -> ShadowEpochV1`.

- [ ] **Step 1: Write failing priority, owner-fairness, and accounting examples**

```python
def test_environment_splitting_does_not_multiply_owner_share() -> None:
    result = allocate_shadow(
        allocation_input(
            development={
                "owner-a": {"dev-a1": 8, "dev-a2": 8, "dev-a3": 8},
                "owner-b": {"dev-b1": 8},
            },
            pool_slots=8,
        )
    )
    assert result.account_slots("owner-a") == 4
    assert result.account_slots("owner-b") == 4


def test_assigned_attempt_is_not_counted_twice() -> None:
    result = allocate_shadow(
        input_with_one_assigned_attempt_and_one_pending_attempt(pool_slots=2)
    )
    assert result.subject("dev-a").requested_slots == 2
    assert result.subject("dev-a").new_allowance_slots == 1


def test_claim_and_backing_worker_consume_one_physical_shape() -> None:
    result = allocate_shadow(input_with_one_claim_on_one_live_worker())
    assert result.protected_claim_slots == 1
    assert result.physical_committed_shape_slots == 1
    assert result.pool("oldlab").charged_physical_slots == 1


def test_higher_tier_reclaims_only_compatible_resource_domain() -> None:
    result = allocate_shadow(input_with_prod_x86_and_development_arm_demand())
    assert result.subject("production").oldlab_slots == 1
    assert result.subject("development").gb10_slots == 1


def test_odd_residual_slot_rotates_between_equal_owner_accounts() -> None:
    first = allocate_shadow(symmetric_owner_input(pool_slots=3, cursor="owner-a"))
    second = allocate_shadow(
        symmetric_owner_input(pool_slots=3, cursor=first.next_account_cursor)
    )
    assert first.account_slots("owner-a") == 2
    assert second.account_slots("owner-b") == 2


def test_pending_shape_job_ceiling_blocks_only_hypothetical_increase() -> None:
    result = allocate_shadow(input_at_pending_job_ceiling_with_live_demand())
    assert result.fixed_commitment_slots > 0
    assert result.hypothetical_launch_rank == ()
    assert result.blockers == ("pool_pending_job_ceiling",)


def test_warm_minimum_uses_only_approved_shape_and_best_normalized_headroom() -> None:
    result = allocate_shadow(input_with_zero_demand_and_two_warm_pool_choices())
    subject = result.subject("development")
    assert subject.requested_slots == 1
    assert subject.desired_shapes == (("gb10-warm-one", 1),)


def test_constrained_demand_is_placed_before_neutral_demand() -> None:
    result = allocate_shadow(input_with_one_x86_pin_and_one_neutral_task())
    assert result.assignment_pool("attempt-x86") == "oldlab"
    assert result.assignment_pool("attempt-neutral") == "gb10"


def test_overlapping_allowances_have_one_joint_matching_witness() -> None:
    result = allocate_shadow(input_with_overlapping_compatible_shape_sets())
    assert result.allowance_slots == result.matching_witness.matched_slots
    assert len(set(result.matching_witness.shape_instance_ids)) == (
        result.matching_witness.matched_slots
    )


def test_stale_subject_retains_commitment_while_valid_subject_can_grow() -> None:
    result = allocate_shadow(input_with_one_stale_and_one_valid_subject())
    assert result.subject("dev-stale").retained_commitment_slots == 2
    assert result.subject("dev-stale").hypothetical_launch_rank == ()
    assert result.subject("dev-valid").desired_slots == 2
```

- [ ] **Step 2: Write failing property invariants**

Generate bounded fleets of two to four tiers, one to eight accounts, one to
sixteen subjects, two pools, up to four domains per pool, and integer vectors
up to the contract limit. For every successful result assert:

```python
assert every_fixed_commitment_is_charged(result)
assert every_claim_matches_one_worker_slot_or_conservative_reserve(result)
assert matched_claims_do_not_duplicate_physical_shape_charge(result)
assert no_pool_tier_account_or_subject_limit_is_exceeded(result)
assert no_resource_vector_or_node_witness_is_exceeded(result)
assert allocation_is_identical_after_input_permutation(case)
assert splitting_one_owner_into_more_subjects_does_not_increase_account_service(case)
assert symmetric_long_run_account_service_diff_is_at_most_one(case)
assert assigned_attempts_receive_no_duplicate_allowance(result)
assert every_allowance_and_assignment_has_one_distinct_matching_slot(result)
assert lower_tier_service_uses_only_resource_local_headroom(result)
assert no_surge_slot_exists_without_distinct_draining_old_shape_backing(result)
assert no_ranked_item_exceeds_pending_slot_or_job_limits(result)
assert every_ranked_item_has_unavailable_rate_state_and_no_permit(result)
```

- [ ] **Step 3: Run allocator tests and confirm they fail**

```bash
uv run --frozen pytest \
  tests/unit/test_capacity_allocator.py \
  tests/property/test_capacity_allocator.py \
  -q
```

Expected: import fails because `allocator.py` does not exist.

- [ ] **Step 4: Implement the lexicographic allocation phases**

Implement one pure function with these explicit phases:

1. Canonicalize and validate all bindings. Charge each physical worker/shape
   commitment once under its immutable vector, then match every protected claim
   to one exact concurrency slot on that physical identity. Claims consume
   subject task ceilings but do not duplicate the backing physical vector.
   An unmatched or conflicting claim receives the conservative largest-shape
   or remaining-envelope reserve and quarantine treatment from Task 5.
   Charge every proposed/unknown/quarantined physical commitment separately.
   For stale/missing/invalid/equivocal subject or pool input, retain known
   commitments, mark the exact scope increase-frozen, and exclude its demand
   from increase eligibility without blocking unrelated valid scopes.
2. Derive each subject target exactly as
   `min(max_slots, max(min_slots, fixed_claim_slots + runnable_unclaimed_slots))`.
3. Preserve feasible accepted placements and current valid assignments.
4. For each resource domain, serve tiers in priority order.
5. Within a tier, progressively fill one minimum slot per account turn and one
   environment turn from the persisted cursors; repeat for task-backed demand
   from its separate cursors.
6. Within the selected environment, serve local priority bands, then fewer
   compatible shape/domain choices, oldest submission, and stable IDs.
7. On each tentative slot, synthesize the smallest deterministic approved
   worker-shape multiset whose concurrency sum equals the target exactly, and
   require a complete topology witness from Task 5. The mandatory one-slot
   shape makes every integer target representable without over-granting.
8. Preserve all existing pending slot/job commitments, then select a bounded
   hypothetical launch rank that stays within global, tier, account, subject,
   and pool pending slot/job ceilings. Mark every ranked item rate-ineligible
   because Package 1 has no durable rate bucket. Do not issue or persist a
   consumable token or permit.
9. Add rollout surge only from remaining headroom and only against distinct
   nonterminal old-shape backing.
10. Derive drains from retained commitments minus desired commitments; never
   mark physical release.

Use explicit state objects rather than mutating input models:

```python
def allocate_shadow(value: AllocationInputV1) -> ShadowEpochV1:
    state = AllocationState.from_input(value)
    state.charge_fixed_commitments()
    state.preserve_feasible_placements()
    for tier in state.tiers_by_priority():
        state.progressive_fill(tier=tier, phase="minimum")
        state.progressive_fill(tier=tier, phase="demand")
    state.rank_hypothetical_launches_within_pending_limits()
    state.place_rollout_surge_from_headroom()
    return state.build_shadow_epoch()
```

There is no account weight or pool weight parameter. When a search is
infeasible or exceeds its budget, raise a bounded allocator error and produce
no result.

- [ ] **Step 5: Run allocator, property, lint, and type checks**

```bash
uv run --frozen pytest \
  tests/unit/test_capacity_allocator.py \
  tests/property/test_capacity_allocator.py \
  -q
uv run --frozen ruff check \
  src/loom_capacity_manager/allocator.py \
  tests/unit/test_capacity_allocator.py \
  tests/property/test_capacity_allocator.py
uv run --frozen mypy src/loom_capacity_manager/allocator.py
```

Expected: example and generated invariants pass with deterministic output.

- [ ] **Step 6: Commit the allocator slice**

```bash
git add \
  src/loom_capacity_manager/allocator.py \
  tests/unit/test_capacity_allocator.py \
  tests/property/test_capacity_allocator.py
git commit -m "feat: simulate hierarchical fleet allocation"
```

---

### Task 7: Fenced Shadow Reconciliation and Failure Freeze

**Files:**

- Create: `src/loom_capacity_manager/reconciler.py`
- Modify: `tests/integration/test_capacity_management_store.py`

**Interfaces:**

- Consumes: `CapacityManagementStore` and `allocate_shadow`.
- Produces `reconcile_shadow_once` with `session_factory`, `writer`,
  `allocator=allocate_shadow`, and `max_attempts=3`; it returns
  `ShadowRunResult`.

- [ ] **Step 1: Write failing complete-epoch and failure tests**

In the same test module, define `registered_writer` with
`CapacityManagementStore.register_writer`, make `publish_newer_report` ingest
sequence two after the fixture seeded sequence one, and implement the three
count/audit helpers with explicit SQLAlchemy `select(func.count())`/ordered
audit queries against Task 3 models.

```python
async def test_input_change_during_allocation_rejects_whole_epoch(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    allocator = BlockingAllocator()
    task = asyncio.create_task(
        reconcile_shadow_once(
            capacity_session_factory,
            registered_writer,
            allocator=allocator,
        )
    )
    await allocator.started.wait()
    await publish_newer_report(capacity_session_factory)
    allocator.release.set()
    result = await task
    assert result.status == "committed"
    assert result.attempt_count == 2
    assert await committed_shadow_epoch_count(capacity_session_factory) == 1


async def test_allocator_timeout_records_failure_without_partial_allocations(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        allocator=TimeoutAllocator(),
    )
    assert result.status == "failed"
    assert await allocation_row_count(capacity_session_factory) == 0
    assert await latest_audit_kind(capacity_session_factory) == (
        "shadow_allocation_timeout"
    )


async def test_continuous_input_churn_preserves_prior_epoch(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    allocator = ChangingInputAllocator(capacity_session_factory)
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        allocator=allocator,
        max_attempts=3,
    )
    assert result.status == "input-contention"
    assert result.attempt_count == 3
    assert await committed_shadow_epoch_count(capacity_session_factory) == 0
```

- [ ] **Step 2: Run the reconciliation tests and confirm they fail**

```bash
uv run --frozen pytest tests/integration/test_capacity_management_store.py -q -k shadow
```

Expected: `reconcile_shadow_once` is missing.

- [ ] **Step 3: Implement one complete fenced shadow tick**

`reconcile_shadow_once` must:

1. load one input plus demand-reporter/pool-reporter/config high-water manifest;
2. run the pure allocator outside the transaction under the configured bound;
3. reopen one serializable transaction and compare every high-water and writer
   epoch;
4. commit all allocations and the complete epoch atomically on equality;
5. on mismatch, discard the result and reload/recalculate for at most three
   total attempts; after the third mismatch, return `input-contention` without changing the
   prior allocation epoch or advancing shadow fairness cursors;
6. record a writer-fenced whole-epoch failure audit on invalid global input,
   arithmetic failure, or timeout and leave the
   global increase freeze set; scoped report failure remains a committed
   increase-freeze blocker inside an otherwise complete epoch;
7. assert the authority and every row still have executable ceiling/flag zero.

A complete successful epoch clears the writer-fenced increase freeze in its
same transaction. Although Package 1 has no increase path, exercising this
state now prevents a later package from treating an operator acknowledgement
as allocation authority.

The result is diagnostic only:

```python
@dataclass(frozen=True, slots=True)
class ShadowRunResult:
    status: Literal["committed", "input-contention", "failed"]
    allocation_epoch: int | None
    input_digest: str
    reason: str | None
    attempt_count: int
```

Do not serialize `ShadowEpochV1` into the existing dev grant-report format and
do not call `capacity_grants_from_report`, `SharedCapacityBroker`, or a worker
autoscaler.

- [ ] **Step 4: Run transaction-race and failure tests**

```bash
uv run --frozen pytest tests/integration/test_capacity_management_store.py -q
uv run --frozen mypy src/loom_capacity_manager/reconciler.py
```

Expected: input races, writer races, timeout, invalid topology, DB rollback,
and successful complete epochs pass.

- [ ] **Step 5: Commit the reconciliation slice**

```bash
git add src/loom_capacity_manager/reconciler.py tests/integration/test_capacity_management_store.py
git commit -m "feat: reconcile fenced capacity shadow epochs"
```

---

### Task 8: Authenticated Ingestion, Read-Only Status, and Bounded Metrics

**Files:**

- Create: `src/loom_capacity_manager/config.py`
- Create: `src/loom_capacity_manager/auth.py`
- Create: `src/loom_capacity_manager/api.py`
- Create: `src/loom_capacity_manager/metrics.py`
- Create: `src/loom_capacity_manager/__main__.py`
- Create: `tests/unit/test_capacity_auth.py`
- Create: `tests/integration/test_capacity_manager_api.py`
- Create: `tests/integration/test_capacity_manager_mtls.py`

**Interfaces:**

- Consumes: Task 1 contracts, Task 4 store, and Task 7 reconciler.
- Produces: FastAPI endpoints for configuration publication, report ingestion,
  reconciliation trigger, health, status, audit, and metrics; no executable
  handoff endpoint. `CapacityPrincipalVerifier.from_file(path)` loads the
  owner-only principal registry, and `verify_bearer(header) -> CapacityPrincipal`
  returns the exact scoped subject/pool binding or rejects it.
  `build_uvicorn_kwargs(settings) -> dict[str, object]` always enables client
  certificate verification for a real server process.

- [ ] **Step 1: Write failing authorization and route-surface tests**

Define `hold_reconciliation_open` in the API test with a blocking injected
allocator and `threading.Event` entry/release signals; it must exercise the
real route lock rather than monkeypatching the response.

```python
def test_shadow_api_has_no_grant_or_executor_mutation_route(app: FastAPI) -> None:
    routes = {(route.path, tuple(sorted(route.methods or ()))) for route in app.routes}
    assert not any("grant" in path or "launch-permit" in path for path, _ in routes)
    assert not any(path.endswith("/execute") for path, _ in routes)


def test_reporter_cannot_publish_config_or_impersonate_subject(client: TestClient) -> None:
    reporter = reporter_token(subject_id=SUBJECT_A)
    assert client.put(
        "/v1/config-proposals/fleet", headers=reporter, json=fleet_payload()
    ).status_code == 403
    response = client.put(
        f"/v1/reports/demand/{SUBJECT_B}",
        headers=reporter,
        json=demand_payload(subject_id=SUBJECT_B),
    )
    assert response.status_code == 403


def test_real_server_requires_mutual_tls(settings: CapacityManagerSettings) -> None:
    options = build_uvicorn_kwargs(settings)
    assert options["ssl_cert_reqs"] == ssl.CERT_REQUIRED
    assert options["ssl_ca_certs"] == str(settings.tls_client_ca_file)


def test_concurrent_reconciliation_trigger_is_rejected(client: TestClient) -> None:
    with hold_reconciliation_open(client):
        response = client.post(
            "/v1/shadow-reconciliations",
            headers=operator_token(),
        )
    assert response.status_code == 409
    assert response.json() == {"detail": "shadow reconciliation already running"}
```

In `test_capacity_manager_mtls.py`, generate an ephemeral CA, server
certificate, trusted client certificate, and unrelated client certificate with
the existing `cryptography` dependency. Start Uvicorn on loopback with
`build_uvicorn_kwargs`, then assert `httpx` without a client certificate and
with the unrelated certificate raises `ConnectError`, while the trusted client
receives `200` from `/healthz`. Use a dynamically bound port and a bounded
startup/shutdown timeout.

- [ ] **Step 2: Run API tests and confirm they fail**

```bash
uv run --frozen pytest tests/integration/test_capacity_manager_api.py -q
```

Expected: the app module does not exist.

- [ ] **Step 3: Implement the hashed principal verifier**

The principal file has a strict schema and stores only SHA-256 token hashes:

```json
{
  "schema_version": 1,
  "principals": [
    {
      "principal_id": "fleet-operator",
      "token_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "scopes": [
        "capacity:configure:fleet",
        "capacity:configure:subject",
        "capacity:configure:activate",
        "capacity:reconcile",
        "capacity:read"
      ],
      "subject_id": null,
      "subject_incarnation": null,
      "demand_reporter_incarnation": null,
      "pool_id": null,
      "pool_reporter_incarnation": null
    }
  ]
}
```

Require a regular, nonsymlink, current-UID-owned file with mode `0600`. Reject
duplicate IDs/hashes, unknown scopes/fields, incomplete subject or pool
bindings, and a registry with no operator. Hash the presented bearer token and
compare all same-length candidate hashes with `hmac.compare_digest`; return one
immutable principal and never reveal whether an ID, token, scope, or binding
was the failed component.

- [ ] **Step 4: Implement the minimal shadow-only API**

Expose exactly these routes:

```text
GET  /healthz
PUT  /v1/config-proposals/fleet
PUT  /v1/config-proposals/subjects/{subject_id}
POST /v1/config-activations
PUT  /v1/reports/demand/{subject_id}
PUT  /v1/reports/pools/{pool_id}
POST /v1/shadow-reconciliations
GET  /v1/status
GET  /v1/status/subjects
GET  /v1/status/pools
GET  /v1/shadow-epochs/{allocation_epoch}
GET  /v1/shadow-epochs/{allocation_epoch}/allocations
GET  /v1/audit-events
GET  /metrics
```

Configuration routes require the matching `capacity:configure:fleet`,
`capacity:configure:subject`, or `capacity:configure:activate` scope; shadow
reconciliation requires `capacity:reconcile`. Reporter routes require a
configured identity bound to
the exact subject/incarnation/reporter or pool/pool-reporter incarnation. The application
uses `CapacityPrincipalVerifier` by default and permits an injected verifier
only through `create_app(settings, verifier=test_verifier)` for tests. Production
configuration requires `LOOM_CAPACITY_PRINCIPALS_FILE`,
`LOOM_CAPACITY_DB_URL_FILE`, `LOOM_CAPACITY_EXPECTED_AUTHORITY_INCARNATION`,
`LOOM_CAPACITY_TLS_CERT_FILE`, `LOOM_CAPACITY_TLS_KEY_FILE`, and
`LOOM_CAPACITY_TLS_CLIENT_CA_FILE`. `__main__.py` starts Uvicorn with
`ssl.CERT_REQUIRED`, so bearer identity binding is always carried inside
mutually authenticated TLS outside injected in-process tests. Never log
authorization headers or raw report payloads.

The lifespan reads the owner-only DB URL file, verifies the database schema at
`capacity_0001`, and claims one writer epoch against the exact configured
authority incarnation before accepting reconciliation requests. A mismatched
database/authority or stale writer keeps health in not-ready state and never
falls back to an environment database.

Guard the reconciliation route with one app-lifetime `asyncio.Lock`; return the
bounded `409` response above instead of queueing concurrent ticks. Writer/high-
water fencing remains the cross-process safety boundary.

The aggregate status response includes authority/writer epochs, active config
digest, report/observation freshness counts, shadow allocation epoch/digest,
account/tier/pool totals, blocker counts, and
`executable_new_capacity_ceiling: 0`. Exact subject rows, allocation rows, and
audit events use stable `(created_at, UUID)` or numeric-ID cursors with
`1 <= limit <= 500`; they never return an unbounded collection. Pool status is
bounded by `MAX_POOLS`. Prometheus labels are restricted to configured pool,
tier, state, and bounded reason enums; subject IDs and environment names are
not metric labels.

Install a streaming request-body limiter that rejects more than
`MAX_CONTRACT_BYTES` whether or not `Content-Length` is present. Audit detail is
canonical JSON capped at 16 KiB; truncate only diagnostic strings at their
contract boundary, never a fencing identity, count, digest, or state value.

- [ ] **Step 5: Run API, security, metrics, lint, and type checks**

```bash
uv run --frozen pytest \
  tests/unit/test_capacity_auth.py \
  tests/integration/test_capacity_manager_api.py \
  tests/integration/test_capacity_manager_mtls.py \
  tests/unit/test_metrics_enumeration.py \
  -q
uv run --frozen ruff check src/loom_capacity_manager tests/integration/test_capacity_manager_api.py
uv run --frozen mypy src/loom_capacity_manager
```

Expected: authorization isolation, strict payload rejection, secret-free error
responses, bounded metrics, and route absence tests pass.

- [ ] **Step 6: Commit the service surface**

```bash
git add \
  src/loom_capacity_manager/config.py \
  src/loom_capacity_manager/auth.py \
  src/loom_capacity_manager/api.py \
  src/loom_capacity_manager/metrics.py \
  src/loom_capacity_manager/__main__.py \
  tests/unit/test_capacity_auth.py \
  tests/integration/test_capacity_manager_api.py \
  tests/integration/test_capacity_manager_mtls.py \
  tests/unit/test_metrics_enumeration.py
git commit -m "feat: expose shadow capacity manager status"
```

---

### Task 9: One-Shot Evidence Driver, End-to-End Gate, and Documentation

**Files:**

- Create: `scripts/ops/global_fleet_capacity_shadow_once.py`
- Create: `tests/fixtures/capacity/snapshot-v1.json`
- Create: `tests/ops/test_global_fleet_capacity_shadow_once.py`
- Create: `docs/architecture/global-fleet-capacity-manager.md`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**

- Consumes: the Task 8 API/store boundary and synthetic fleet/report fixtures.
- Produces: reproducible shadow evidence JSON and the Package 1 verification
  gate; it produces no grant file.

- [ ] **Step 1: Write failing one-shot safety tests**

```python
def test_shadow_once_emits_diagnostic_not_grant(tmp_path: Path) -> None:
    output = tmp_path / "shadow.json"
    result = run_shadow_once(
        fleet=FIXTURES / "fleet-v1.toml",
        subjects=FIXTURES / "subjects-v1.toml",
        snapshot=FIXTURES / "snapshot-v1.json",
        output=output,
    )
    document = json.loads(output.read_text())
    assert result == 0
    assert document["mode"] == "shadow"
    assert document["executable"] is False
    assert document["executable_new_capacity_ceiling"] == 0
    assert "grants" not in document
    assert "launch_permits" not in document


def test_output_is_owner_only_and_atomic(tmp_path: Path) -> None:
    output = tmp_path / "shadow.json"
    run_shadow_once(
        fleet=FIXTURES / "fleet-v1.toml",
        subjects=FIXTURES / "subjects-v1.toml",
        snapshot=FIXTURES / "snapshot-v1.json",
        output=output,
    )
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(f".{output.name}.*"))
```

- [ ] **Step 2: Run the operations tests and confirm they fail**

```bash
uv run --frozen pytest tests/ops/test_global_fleet_capacity_shadow_once.py -q
```

Expected: the one-shot driver is missing.

- [ ] **Step 3: Implement the deterministic evidence driver**

Accept only `--fleet`, `--subjects`, `--snapshot`, `--output`, and allocator
search-bound arguments. Validate all three versioned inputs, compose the same
`ConfigurationSnapshotV1` used by the store, call the pure allocator, and
write canonical JSON atomically with mode `0600`. Reject relative output paths,
missing parent directories, symlinks, unknown fields, and live environment
database or Slurm arguments. Exit `0` for a valid shadow result and `2` for a
bounded validation/allocation failure.

- [ ] **Step 4: Document the Package 1 boundary and CI commands**

The architecture page must identify the independent management DB, show the
config/report -> shadow allocator -> status flow, link the approved spec and
this plan, list the current legacy topology conflicts as blockers, and state:

```text
Package 1 is not capacity activation. It cannot authorize a task claim,
worker launch, Slurm mutation, or physical release. The database rejects a
non-zero executable ceiling.
```

Add the new tests to existing Python CI jobs without introducing credentials,
external Slurm access, a deployment job, or a live fleet manifest.

- [ ] **Step 5: Run the complete Package 1 verification gate**

```bash
uv run --frozen pytest \
  tests/unit/test_capacity_contracts.py \
  tests/unit/test_capacity_fleet_state.py \
  tests/unit/test_capacity_topology.py \
  tests/unit/test_capacity_allocator.py \
  tests/unit/test_capacity_auth.py \
  tests/property/test_capacity_allocator.py \
  tests/integration/test_capacity_management_migrations.py \
  tests/integration/test_capacity_management_store.py \
  tests/integration/test_capacity_manager_api.py \
  tests/integration/test_capacity_manager_mtls.py \
  tests/ops/test_global_fleet_capacity_shadow_once.py \
  -q
uv run --frozen ruff check \
  src/loom_capacity_manager \
  capacity_migrations \
  scripts/ops/global_fleet_capacity_shadow_once.py \
  tests/capacity_fixtures.py \
  tests/unit/test_capacity_contracts.py \
  tests/unit/test_capacity_fleet_state.py \
  tests/unit/test_capacity_topology.py \
  tests/unit/test_capacity_allocator.py \
  tests/unit/test_capacity_auth.py \
  tests/property/test_capacity_allocator.py \
  tests/integration/test_capacity_management_migrations.py \
  tests/integration/test_capacity_management_store.py \
  tests/integration/test_capacity_manager_api.py \
  tests/integration/test_capacity_manager_mtls.py \
  tests/ops/test_global_fleet_capacity_shadow_once.py
uv run --frozen mypy src/loom_capacity_manager scripts/ops/global_fleet_capacity_shadow_once.py
git diff --check
```

Expected: all tests, Ruff, mypy, and whitespace checks pass.

- [ ] **Step 6: Re-run the no-execution audit**

```bash
rg -n \
  -e "SharedCapacityBroker|capacity_grants_from_report" \
  -e "sbatch|scancel|worker:claim|launch_permit" \
  src/loom_capacity_manager \
  scripts/ops/global_fleet_capacity_shadow_once.py
```

Expected: no matches. Also query PostgreSQL and prove both the authority
ceiling and every allocation executable flag are zero/false.

- [ ] **Step 7: Commit the completed Package 1 gate**

```bash
git add \
  scripts/ops/global_fleet_capacity_shadow_once.py \
  tests/fixtures/capacity/snapshot-v1.json \
  tests/ops/test_global_fleet_capacity_shadow_once.py \
  docs/architecture/global-fleet-capacity-manager.md \
  .github/workflows/ci.yml
git commit -m "docs: gate global capacity shadow package"
```

---

## Package 1 Acceptance Checklist

- [ ] The management database is distinct from every environment database and
  starts only at `capacity_0001`.
- [ ] All schema-v1 contracts reject unknown versions, fields, floats,
  booleans-as-integers, overflow, and noncanonical resource names.
- [ ] A live fleet manifest is absent until legacy topology conflicts are
  explicitly reconciled; the diagnostic reports all conflicts without choosing
  an environment copy.
- [ ] Demand/pool reporter sequence replay, equivocation, incarnation fencing,
  database receipt time, and immutable binding checks pass.
- [ ] One serializable writer commits either a complete shadow epoch or no
  allocation rows.
- [ ] Existing and ambiguous commitments remain charged; missing reports never
  free capacity.
- [ ] Reporter rotation, omission, and source-reported terminality cannot move
  an observed commitment to released because Package 1 has no released state.
- [ ] Topology packing is resource-domain and per-node aware, deterministic,
  bounded, and fail-closed on search limits.
- [ ] Strict tier order, owner-safe hierarchical fairness, local task priority,
  constrained-before-flexible placement, stable assignments, and `min_slots=0`
  behavior pass example and property tests.
- [ ] There are no user-configurable pool or global account weights.
- [ ] Status and audit output are bounded and secret-free; metric labels cannot
  grow with dynamic environment names.
- [ ] No API or file has the existing grant-report shape, and no Package 1 code
  imports or invokes Slurm or local worker-autoscaler mutation.
- [ ] The database, API status, one-shot evidence, and documentation all prove
  the executable new-capacity ceiling is zero.
- [ ] Existing legacy capacity behavior is unchanged; live activation remains
  gated by Packages 2-5, issue #896 containment evidence, re-scoped issue #906
  activation evidence, and the specification's Activation Boundary.
