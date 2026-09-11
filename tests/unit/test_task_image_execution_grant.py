"""Signed complete execution evidence must not become offline start authority."""

import copy
import hashlib
import importlib
import json
from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest
import rfc8785
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom.task_image_materialization import task_image_materialization_key
from tests.unit.test_task_image_build_plan_versions import strong_payload
from tests.unit.test_task_image_materialization import _task_config
from tests.unit.test_task_image_publication_keyset import _b64, _sign, _time
from tests.unit.test_task_image_publication_signing import DOMAIN, NOW, setup_signing

GRANT_DOMAIN = b"loom-task-image-execution-grant-v2\x00"
IDENTITY = "11111111-1111-4111-8111-111111111111"


def module():
    name = "loom_task_image_authority.execution_grant"
    assert importlib.util.find_spec(name) is not None, "signed execution-grant verifier missing"
    return importlib.import_module(name)


def sign_grant(payload, private, *, domain=GRANT_DOMAIN):
    raw = rfc8785.dumps(payload)
    return rfc8785.dumps(
        dict(
            canonical_grant=raw.decode(),
            grant_sha256=hashlib.sha256(raw).hexdigest(),
            key_id="execution-1",
            algorithm="Ed25519",
            signature=_b64(private.sign(domain + raw)),
        )
    )


def fixture(
    *,
    arch="arm64",
    kind="legacy",
    purpose="production",
    sidecar_only=False,
    plan_change=None,
    publication_change=None,
):
    m = module()
    c, s, private, key, state, distribution, original, _ = setup_signing()
    task = _task_config(
        cpu_arch=arch,
        dockerfile="Dockerfile",
        sidecars=[
            {"name": "db", "dockerfile": "db/Dockerfile", "docker_build_context": "db"},
        ],
    ).model_dump(mode="json")
    plan = strong_payload()
    plan.update(
        task_id=task["task"]["id"],
        task_checksum=original.task_checksum,
        materialization_id=original.materialization_id,
        grant_id=original.grant_id,
        session_id=original.original_claim_session_id,
        session_generation=original.original_claim_session_generation,
        builder_id="rootless:" + UUID(original.original_claim_session_id).hex,
        cpu_arch=arch,
        platform="linux/arm64" if arch == "arm64" else "linux/amd64",
        authorization_expires_at=_time(NOW - timedelta(days=1)),
        components=[
            dict(
                name="task",
                dockerfile_path="Dockerfile",
                context_path=".",
                oci_output_path="oci/0000.tar",
            ),
            dict(
                name="sidecar:db",
                dockerfile_path="db/Dockerfile",
                context_path="db",
                oci_output_path="oci/0001.tar",
            ),
        ],
    )
    if sidecar_only:
        task["environment"]["dockerfile"] = None
        task["environment"]["docker_image"] = "registry.example/prebuilt@sha256:" + "e" * 64
        plan["components"] = [dict(plan["components"][1], oci_output_path="oci/0000.tar")]
    if plan_change is not None:
        plan_change(plan)
    plan_wire = rfc8785.dumps(plan)
    materialization_key = task_image_materialization_key(
        task_id=plan["task_id"],
        task_checksum=plan["task_checksum"],
        cpu_arch=arch,
        bundle_content_manifest_sha256=plan["bundle_content_manifest_sha256"],
    )
    execution = Ed25519PrivateKey.generate()
    root = m.ExecutionGrantTrustRoot(
        key_id="execution-1",
        environment="production",
        public_key=execution.public_key().public_bytes_raw(),
        activated_at=NOW - timedelta(days=1),
        expires_at=NOW + timedelta(days=1),
    )
    keyset = _sign(
        dict(
            schema="loom.task-image-publication-keyset/v1",
            environment="production",
            keyset_version=3,
            revocation_epoch=2,
            issued_at=_time(NOW - timedelta(seconds=10)),
            expires_at=_time(NOW + timedelta(minutes=5)),
            keys=[
                dict(
                    key_id=key.key_id,
                    public_key=_b64(key.public_key),
                    status="active",
                    activated_at=_time(key.activated_at),
                )
            ],
        ),
        execution,
    )
    wires, components = [], []
    for name in ("sidecar:db",) if sidecar_only else ("task", "sidecar:db"):
        unsigned = original.model_dump(mode="json", by_alias=True, exclude_none=True)
        unsigned.update(
            component=name,
            task_id=plan["task_id"],
            materialization_key=materialization_key,
            frozen_plan_sha256=hashlib.sha256(plan_wire).hexdigest(),
            platform=plan["platform"],
            slurm_cluster_id="gb10" if arch == "arm64" else "oldlab",
        )
        from loom_task_image_authority.registry_token import publication_repository

        unsigned["repository"] = publication_repository(
            purpose="production",
            shadow_campaign_id=None,
            cpu_arch=arch,
            attempt_id=UUID(original.attempt_id),
            component=name,
        )
        if purpose == "shadow":
            unsigned.update(purpose="shadow", shadow_campaign_id=IDENTITY)
            unsigned["repository"] = unsigned["repository"].replace(
                "loom-task-image-attempts/",
                f"loom-task-image-shadow/{IDENTITY}/",
            )
        if publication_change is not None:
            publication_change(unsigned)
        statement = s.prepare_publication_statement(
            c.decode_unsigned_input(rfc8785.dumps(unsigned)),
            key=key,
            state=state,
            distribution=distribution,
            signer_now=NOW,
        )
        canonical = c.canonical_publication_bytes(statement)
        wire = rfc8785.dumps(
            dict(
                canonical_statement=canonical.decode(),
                statement_sha256=hashlib.sha256(canonical).hexdigest(),
                key_id=key.key_id,
                algorithm="Ed25519",
                signature=_b64(private.sign(DOMAIN + canonical)),
            )
        )
        wires.append(wire)
        components.append(
            dict(
                component=name,
                envelope_sha256=hashlib.sha256(wire).hexdigest(),
                image=f"registry.example/{unsigned['repository']}@{unsigned['manifest']['digest']}",
            )
        )
    claim = dict(
        kind=kind,
        trial_id=IDENTITY,
        team_id=IDENTITY,
        worker_id=IDENTITY,
        worker_lease_epoch=2,
        trial_attempt_count=3,
    )
    if kind == "protected":
        claim.update(receipt_sha256="a" * 64, worker_incarnation=IDENTITY, claim_high_water=7)
    else:
        claim["claim_id"] = "33333333-3333-4333-8333-333333333333"
    payload = dict(
        schema="loom.task-image-execution-grant/v2",
        grant_id=IDENTITY,
        revision=1,
        claim=claim,
        environment="production",
        purpose=purpose,
        materialization_id=plan["materialization_id"],
        materialization_key=materialization_key,
        task_checksum=plan["task_checksum"],
        cpu_arch=arch,
        canonical_task_config=rfc8785.dumps(task).decode(),
        task_source=f"s3://{plan['bundle_bucket']}/{plan['bundle_prefix']}",
        canonical_source_provenance=rfc8785.dumps(
            dict(
                bundle_content_manifest_sha256=plan["bundle_content_manifest_sha256"],
                bundle_file_metadata_sha256="sha256:" + plan["bundle_file_metadata_sha256"],
            )
        ).decode(),
        frozen_plan_sha256=hashlib.sha256(plan_wire).hexdigest(),
        components=components,
        keyset_sha256=hashlib.sha256(keyset).hexdigest(),
        keyset_version=3,
        revocation_epoch=2,
        issued_at=_time(NOW),
        expires_at=_time(NOW + timedelta(minutes=2)),
    )
    if purpose == "shadow":
        payload["shadow_campaign_id"] = IDENTITY
    kwargs = dict(
        wire=sign_grant(payload, execution),
        plan_wire=plan_wire,
        publication_wires=tuple(wires),
        keyset_wire=keyset,
        trust_root=root,
        expected_claim=m.decode_execution_claim(rfc8785.dumps(claim)),
        expected_purpose=purpose,
        expected_shadow_campaign_id=IDENTITY if purpose == "shadow" else None,
        now=NOW,
    )
    return payload, execution, kwargs


@pytest.mark.parametrize("arch", ["arm64", "x86_64"])
@pytest.mark.parametrize("kind", ["legacy", "protected"])
def test_complete_signed_evidence_accepts_historical_build_and_returns_no_start(arch, kind):
    payload, _, kwargs = fixture(arch=arch, kind=kind)
    result = module().verify_execution_grant(**kwargs)
    assert result.registry_images == tuple(
        (item["component"], item["image"]) for item in payload["components"]
    )
    assert result.grant.claim.kind == kind
    assert result.envelope_sha256 == hashlib.sha256(kwargs["wire"]).hexdigest()
    assert not hasattr(result, "start_authorization")
    assert not hasattr(result, "ready")
    with pytest.raises(ValueError):
        result.grant.revision = 2


@pytest.mark.parametrize(
    "field", ["trial_id", "team_id", "worker_id", "worker_lease_epoch", "trial_attempt_count", "claim_id"]
)
def test_signed_claim_cannot_replace_independent_claim_authority(field):
    payload, key, kwargs = fixture()
    payload["claim"][field] = (
        9 if field.endswith(("epoch", "count")) else "22222222-2222-4222-8222-222222222222"
    )
    kwargs["wire"] = sign_grant(payload, key)
    with pytest.raises(ValueError):
        module().verify_execution_grant(**kwargs)


@pytest.mark.parametrize(
    "change",
    [
        "signature",
        "domain",
        "key-id",
        "environment",
        "purpose",
        "expired",
        "future",
        "long-life",
        "keyset-coverage",
        "keyset-pin",
        "keyset-version",
        "epoch",
        "plan-pin",
        "plan-v1",
        "plan-source",
        "plan-path",
        "plan-claim",
        "source",
        "manifest",
        "metadata",
        "legacy-source",
        "task-id",
        "components",
        "image",
        "envelope-pin",
        "publication-signature",
        "publication-order",
        "publication-missing",
        "noncanonical",
        "unknown",
        "null",
        "bool",
        "duplicate-json",
        "canonical-task",
        "protected-high-water",
        "root-expired",
        "wrong-root",
        "plan-noncanonical",
    ],
)
def test_signed_or_wire_substitutions_are_refused(change):
    payload, key, kwargs = fixture(kind="protected")
    if change in {"signature", "domain", "key-id", "noncanonical", "duplicate-json"}:
        if change == "domain":
            kwargs["wire"] = sign_grant(payload, key, domain=b"wrong\x00")
        else:
            envelope = json.loads(kwargs["wire"])
            if change == "signature":
                envelope["signature"] = _b64(b"\x00" * 64)
            if change == "key-id":
                envelope["key_id"] = "other"
            kwargs["wire"] = rfc8785.dumps(envelope)
            if change == "noncanonical":
                kwargs["wire"] += b" "
            if change == "duplicate-json":
                kwargs["wire"] = b'{"key_id":"execution-1",' + kwargs["wire"][1:]
    else:
        if change == "environment":
            payload["environment"] = "staging"
        elif change == "purpose":
            payload.update(purpose="shadow", shadow_campaign_id=IDENTITY)
        elif change == "expired":
            kwargs["now"] = NOW + timedelta(minutes=2)
        elif change == "future":
            payload["issued_at"] = _time(NOW + timedelta(seconds=1))
        elif change == "long-life":
            payload["expires_at"] = _time(NOW + timedelta(hours=1))
        elif change == "keyset-coverage":
            payload["expires_at"] = _time(NOW + timedelta(minutes=6))
        elif change == "keyset-pin":
            payload["keyset_sha256"] = "f" * 64
        elif change == "keyset-version":
            payload["keyset_version"] += 1
        elif change == "epoch":
            payload["revocation_epoch"] += 1
        elif change == "plan-pin":
            payload["frozen_plan_sha256"] = "f" * 64
        elif change.startswith("plan-"):
            plan = json.loads(kwargs["plan_wire"])
            if change == "plan-v1":
                plan["schema_version"] = "loom.task-image-build-plan.v1"
                del plan["bundle_content_manifest_sha256"]
            if change == "plan-source":
                plan["bundle_bucket"] = "other-bucket"
            if change == "plan-path":
                plan["components"][0]["dockerfile_path"] = "other/Dockerfile"
            if change == "plan-claim":
                plan["grant_id"] = IDENTITY
            kwargs["plan_wire"] = rfc8785.dumps(plan)
            if change == "plan-noncanonical":
                kwargs["plan_wire"] += b" "
            payload["frozen_plan_sha256"] = hashlib.sha256(kwargs["plan_wire"]).hexdigest()
        elif change == "source":
            payload["task_source"] = payload["task_source"].replace("s3://loom", "s3://other")
        elif change in {"manifest", "metadata", "legacy-source"}:
            provenance = json.loads(payload["canonical_source_provenance"])
            if change == "manifest":
                provenance["bundle_content_manifest_sha256"] = "f" * 64
            if change == "metadata":
                provenance["bundle_file_metadata_sha256"] = "sha256:" + "f" * 64
            if change == "legacy-source":
                del provenance["bundle_content_manifest_sha256"]
            payload["canonical_source_provenance"] = rfc8785.dumps(provenance).decode()
        elif change in {"task-id", "canonical-task"}:
            task = json.loads(payload["canonical_task_config"])
            if change == "task-id":
                task["task"]["id"] = "other/task"
            payload["canonical_task_config"] = rfc8785.dumps(task).decode() + (
                " " if change == "canonical-task" else ""
            )
        elif change == "components":
            payload["components"] = payload["components"][::-1]
        elif change == "image":
            payload["components"][0]["image"] = "registry.example/other@sha256:" + "1" * 64
        elif change == "envelope-pin":
            payload["components"][0]["envelope_sha256"] = "f" * 64
        elif change == "publication-signature":
            envelope = json.loads(kwargs["publication_wires"][0])
            envelope["signature"] = _b64(b"\x00" * 64)
            wire = rfc8785.dumps(envelope)
            kwargs["publication_wires"] = (wire, kwargs["publication_wires"][1])
            payload["components"][0]["envelope_sha256"] = hashlib.sha256(wire).hexdigest()
        elif change == "publication-order":
            kwargs["publication_wires"] = kwargs["publication_wires"][::-1]
        elif change == "publication-missing":
            kwargs["publication_wires"] = kwargs["publication_wires"][:1]
        elif change == "unknown":
            payload["start_authorized"] = True
        elif change == "null":
            payload["shadow_campaign_id"] = None
        elif change == "bool":
            payload["revision"] = True
        elif change == "protected-high-water":
            payload["claim"]["claim_high_water"] += 1
        elif change == "root-expired":
            kwargs["trust_root"] = replace(kwargs["trust_root"], expires_at=NOW)
        elif change == "wrong-root":
            kwargs["trust_root"] = replace(
                kwargs["trust_root"],
                public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw(),
            )
        kwargs["wire"] = sign_grant(payload, key)
    with pytest.raises(ValueError):
        module().verify_execution_grant(**kwargs)


@pytest.mark.parametrize("field", ["wire", "plan_wire", "keyset_wire", "publication_wires"])
def test_untrusted_wires_are_bounded_before_decoding(field):
    _, _, kwargs = fixture()
    huge = b"x" * (512 * 1024 + 1)
    kwargs[field] = (huge,) * 129 if field == "publication_wires" else huge
    with pytest.raises(ValueError):
        module().verify_execution_grant(**kwargs)


def test_caller_constructed_claim_cannot_bypass_validation():
    _, _, kwargs = fixture()
    claim = copy.copy(kwargs["expected_claim"])
    object.__setattr__(claim, "worker_lease_epoch", True)
    kwargs["expected_claim"] = claim
    with pytest.raises(ValueError):
        module().verify_execution_grant(**kwargs)


def test_legacy_claim_requires_nonrefundable_identity():
    # node_setup_health refunds attempt_count before requeue; the same worker
    # intended retry may reuse every field below. None identifies that new
    # claim. The scheduler must persist a separate ID atomically. This is wire
    # coverage, not an end-to-end claim/refund/reclaim regression.
    ambiguous = dict(
        kind="legacy", trial_id=IDENTITY, team_id=IDENTITY, worker_id=IDENTITY,
        worker_lease_epoch=2, trial_attempt_count=3,
    )
    with pytest.raises(ValueError, match="claim_id"):
        module().decode_execution_claim(rfc8785.dumps(ambiguous))


def test_old_grant_cannot_authorize_refunded_attempt_reclaim():
    payload, _, kwargs = fixture()
    successor = dict(payload["claim"], claim_id="44444444-4444-4444-8444-444444444444")
    kwargs["expected_claim"] = module().decode_execution_claim(rfc8785.dumps(successor))
    with pytest.raises(ValueError, match="claim, lifetime or attachment binding"):
        module().verify_execution_grant(**kwargs)


@pytest.mark.parametrize("claim_id", [None, "", "0" * 32, "00000000-0000-0000-0000-000000000000", 7])
def test_legacy_claim_id_must_be_nonzero_canonical_uuid(claim_id):
    payload, _, _ = fixture()
    payload["claim"]["claim_id"] = claim_id
    with pytest.raises(ValueError, match="claim_id"):
        module().decode_execution_claim(rfc8785.dumps(payload["claim"]))


@pytest.mark.parametrize(
    "field,value",
    [
        ("receipt_sha256", "b" * 64),
        ("worker_incarnation", "22222222-2222-4222-8222-222222222222"),
    ],
)
def test_protected_receipt_and_process_identity_are_independently_bound(field, value):
    payload, private, kwargs = fixture(kind="protected")
    payload["claim"][field] = value
    kwargs["wire"] = sign_grant(payload, private)
    with pytest.raises(ValueError, match="claim, lifetime or attachment binding"):
        module().verify_execution_grant(**kwargs)


def test_shadow_evidence_verifies_only_with_independent_shadow_expectation():
    _, _, kwargs = fixture(purpose="shadow")
    result = module().verify_execution_grant(**kwargs)
    assert result.grant.purpose == "shadow"
    assert all(f"/loom-task-image-shadow/{IDENTITY}/" in ref for _, ref in result.registry_images)
    kwargs.update(expected_purpose="production", expected_shadow_campaign_id=None)
    with pytest.raises(ValueError, match="claim, lifetime or attachment binding"):
        module().verify_execution_grant(**kwargs)


def test_sidecar_only_build_does_not_require_a_primary_publication():
    _, _, kwargs = fixture(sidecar_only=True)
    result = module().verify_execution_grant(**kwargs)
    assert tuple(name for name, _ in result.registry_images) == ("sidecar:db",)


def test_mutating_snapshot_copies_does_not_change_verified_authority():
    payload, _, kwargs = fixture()
    result = module().verify_execution_grant(**kwargs)
    task, provenance = result.grant.snapshots()
    task["environment"]["sidecars"][0]["dockerfile"] = "changed"
    provenance["bundle_content_manifest_sha256"] = "f" * 64
    assert result.grant.snapshots() == (
        json.loads(payload["canonical_task_config"]),
        json.loads(payload["canonical_source_provenance"]),
    )


@pytest.mark.parametrize(
    "field,value", [("dockerfile_path", "db/Otherfile"), ("context_path", ".")]
)
def test_plan_path_mismatch_is_refused_even_when_all_publications_pin_that_exact_plan(field, value):
    def mutate(plan):
        plan["components"][1][field] = value

    _, _, kwargs = fixture(plan_change=mutate)
    with pytest.raises(ValueError, match="frozen source, task or component plan differs"):
        module().verify_execution_grant(**kwargs)


@pytest.mark.parametrize(
    "field,value",
    [
        ("grant_id", IDENTITY),
        ("original_claim_session_id", IDENTITY),
        ("original_claim_session_generation", 4),
    ],
)
def test_matching_signed_publication_pins_do_not_replace_plan_claim_bindings(field, value):
    _, _, kwargs = fixture(publication_change=lambda unsigned: unsigned.update({field: value}))
    with pytest.raises(ValueError, match="publication build authority differs"):
        module().verify_execution_grant(**kwargs)
