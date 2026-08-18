"""POST /admin/step-tokens (Plan 9 Task 4)."""

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.orm import sessionmaker

from loom.auth import verify_step_jwt
from loom.db.schema import (
    ProviderConnection,
    ProviderConnectionShare,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


def _set_cp_env(monkeypatch: pytest.MonkeyPatch, postgres_url: str) -> None:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def seed(postgres_url: str) -> Iterator[dict]:
    """Seed a worker token + one team + one trial (so the step-token mint
    can verify the trial exists and belongs to the team)."""
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    raw = f"loom_w_{uuid4().hex}"
    family_raw = f"loom_fo_{uuid4().hex}"
    team_id = uuid4()
    trial_id = uuid4()
    with session_local() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="worker",
                scopes=["worker:report"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(family_raw.encode()).digest(),
                type="family_orchestrator",
                scopes=["family:evolve"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="t",
                config={},
                requires_caps={},
                state="running",
            )
        )
        s.commit()
    try:
        yield {
            "token": raw,
            "family_token": family_raw,
            "team_id": team_id,
            "trial_id": trial_id,
        }
    finally:
        with session_local() as s:
            s.execute(delete(Trial))
            # Tests in this file may seed a ProviderConnection to
            # exercise issue #72; clean it up before Team to satisfy
            # the FK.
            s.execute(delete(ProviderConnectionShare))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def worker_token(seed: dict) -> str:
    return seed["token"]


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, worker_token: str):
    _set_cp_env(monkeypatch, postgres_url)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_issue_step_token_returns_verifiable_jwt(app, seed):  # type: ignore[no-untyped-def]
    worker_token = seed["token"]
    team_id = seed["team_id"]
    trial_id = seed["trial_id"]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["token"].startswith("loom_step_")
        # The minted token verifies against the same signing key.
        signing_key = os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"]
        ctx = verify_step_jwt(body["token"], signing_key=signing_key)
        assert ctx.team_id == team_id
        assert ctx.trial_id == trial_id
        assert ctx.step_id == "main"


def test_issue_step_token_rejects_missing_scope(app, postgres_url):  # type: ignore[no-untyped-def]
    """A team token (scope=submit, no worker:report) should be rejected."""
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    raw = f"team_{uuid4().hex}"
    with session_local() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.commit()
    engine.dispose()
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "team_id": str(uuid4()),
                "trial_id": str(uuid4()),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 403


def test_issue_step_token_rejects_unauthenticated(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            json={
                "team_id": str(uuid4()),
                "trial_id": str(uuid4()),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 403


@pytest.mark.parametrize(
    "path",
    (
        "/tasks/t/bundle",
        "/trials/{trial_id}",
        "/trials/{trial_id}/llm-calls",
        "/trials/{trial_id}/trajectory",
    ),
)
def test_family_orchestrator_token_is_rejected_outside_step_exchange(
    app,  # type: ignore[no-untyped-def]
    seed: dict,
    path: str,
) -> None:
    request_path = path.format(trial_id=seed["trial_id"])
    with TestClient(app) as client:
        response = client.get(
            request_path,
            headers={"Authorization": f"Bearer {seed['family_token']}"},
        )

    assert response.status_code == 401


def test_issue_step_token_accepts_effective_trial_timeout_ttl(
    app,
    seed,  # type: ignore[no-untyped-def]
) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 9300,
            },
        )

    assert response.status_code == 201, response.text


def test_issue_step_token_validates_ttl(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "team_id": str(uuid4()),
                "trial_id": str(uuid4()),
                "step_id": "main",
                "ttl_sec": 30_001,
            },
        )
        assert r.status_code == 422


def test_issue_step_token_rejects_unknown_trial(app, seed):  # type: ignore[no-untyped-def]
    """Plan 9 audit fix: trial_id must exist in trials. Otherwise a
    worker could mint tokens for fictional trials and llm_calls would
    accumulate orphan rows."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(uuid4()),  # fictional
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 404


def test_issue_step_token_rejects_team_id_mismatch(app, seed):  # type: ignore[no-untyped-def]
    """Plan 9 audit fix: team_id MUST match trial.team_id."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(uuid4()),  # not the trial's owner
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 400


def test_round_trip_with_jwt_can_be_decoded(app, seed):  # type: ignore[no-untyped-def]
    """End-to-end smoke: mint a token, decode raw JWT body, verify claims."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "phase-2",
                "ttl_sec": 30,
            },
        )
        assert r.status_code == 201
        token = r.json()["token"]
        body = token[len("loom_step_") :]
        signing_key = os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"]
        claims = jwt.decode(body, signing_key, algorithms=["HS256"])
        assert claims["sub"] == "step-session"
        assert claims["step_id"] == "phase-2"
        assert claims["scopes"] == ["llm:call"]


def test_execution_attempt_token_freezes_live_dispatch_authority(
    app,
    seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    worker_id, run_id, stage_id, attempt_id, claim_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    worker_hash = hashlib.sha256(seed["token"].encode()).digest()
    lease_token = "lease-" + "x" * 40
    binding_digest = "sha256:" + "b" * 64
    execution_spec = {
        "container_node": {
            "network_profile": "gateway",
            "timeout_seconds": 300,
        },
        "control_binding_snapshots": [{"snapshot_sha256": binding_digest}],
    }
    spec_digest = canonical_digest(execution_spec)
    authorization = {"schema_version": "terminalgen.authoring-grant.v1"}
    authorization_digest = canonical_digest(authorization)
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO workers (
                    id,hostname,version,capabilities,supported_work_kinds,
                    auth_token_hash,registered_at,last_seen_at,status
                ) VALUES (
                    :id,'step-token-worker','test','[]'::jsonb,
                    ARRAY['trial','execution_attempt']::text[],
                    :token,now(),now(),'active'
                )
            """),
            {"id": worker_id, "token": worker_hash},
        )
        connection.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key,state,started_at
                ) VALUES (
                    :id,:team,'ordinary','step-token-test',1,:digest,'{}'::jsonb,:digest,
                    '{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,:digest,:key,'running',now()
                )
            """),
            {
                "id": run_id,
                "team": seed["team_id"],
                "digest": "sha256:" + "a" * 64,
                "key": f"step-token-{run_id}",
            },
        )
        connection.execute(
            text("""
                INSERT INTO pipeline_stage_runs (
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resolved_execution_spec_json,resolved_execution_spec_bytes,
                    execution_spec_digest,resolved_input_bindings_json,
                    resolved_input_bindings_digest,resource_profile_json,
                    resource_profile_digest,failure_policy,ready_at,claimed_at
                ) VALUES (
                    :id,:run,'generate_card_00','singleton','container','claimed',
                    CAST(:spec AS jsonb),:spec_bytes,:spec_digest,'[]'::jsonb,
                    :bindings_digest,'{}'::jsonb,:profile_digest,'fail_run',now(),now()
                )
            """),
            {
                "id": stage_id,
                "run": run_id,
                "spec": json.dumps(execution_spec),
                "spec_bytes": canonical_document(execution_spec),
                "spec_digest": spec_digest,
                "bindings_digest": canonical_digest([]),
                "profile_digest": "sha256:" + "c" * 64,
            },
        )
        connection.execute(
            text("""
                INSERT INTO execution_attempts (
                    id,stage_run_id,attempt_number,state,worker_id,claim_id,
                    lease_epoch,lease_token_digest,lease_expires_at,
                    execution_authorization_json,execution_authorization_bytes,
                    execution_authorization_digest,queued_at,claimed_at
                ) VALUES (
                    :id,:stage,1,'claimed',:worker,:claim,4,:lease_digest,:expires,
                    CAST(:authorization AS jsonb),:authorization_bytes,
                    :authorization_digest,now(),now()
                )
            """),
            {
                "id": attempt_id,
                "stage": stage_id,
                "worker": worker_id,
                "claim": claim_id,
                "lease_digest": hashlib.sha256(lease_token.encode()).hexdigest(),
                "expires": datetime.now(UTC) + timedelta(minutes=10),
                "authorization": json.dumps(authorization),
                "authorization_bytes": canonical_document(authorization),
                "authorization_digest": authorization_digest,
            },
        )
    try:
        with TestClient(app) as client:
            response = client.post(
                "/admin/step-tokens",
                headers={
                    "Authorization": f"Bearer {seed['token']}",
                    "X-Loom-Claim-Id": str(claim_id),
                    "X-Loom-Lease-Epoch": "4",
                    "X-Loom-Lease-Token": lease_token,
                },
                json={
                    "team_id": str(seed["team_id"]),
                    "execution_attempt_id": str(attempt_id),
                    "step_id": "generate_card_00",
                    "ttl_sec": 600,
                },
            )
        assert response.status_code == 201, response.text
        ctx = verify_step_jwt(
            response.json()["token"],
            signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
        )
        assert ctx.execution_attempt_id == attempt_id
        assert ctx.step_jwt_id is not None
        assert ctx.execution_attempt_lease_epoch == 4
        assert ctx.execution_spec_digest == spec_digest
        assert ctx.control_binding_snapshot_digest == binding_digest
        assert ctx.execution_authorization_digest == authorization_digest
        assert ctx.provider_connection_id_bound is True
        with engine.connect() as connection:
            stored_jti = connection.execute(
                text("SELECT step_jwt_id FROM execution_attempts WHERE id=:id"),
                {"id": attempt_id},
            ).scalar_one()
        assert stored_jti == ctx.step_jwt_id
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM execution_attempts WHERE id=:id"), {"id": attempt_id}
            )
            connection.execute(
                text("DELETE FROM pipeline_stage_runs WHERE id=:id"), {"id": stage_id}
            )
            connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
            connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
        engine.dispose()


# ──────────────────────────────────────────────────────────────────────
# Issue #72 — JWT scope carries provider_connection_id from Trial row
# ──────────────────────────────────────────────────────────────────────


def test_step_token_omits_provider_connection_id_when_trial_has_none(
    app,
    seed,
):  # type: ignore[no-untyped-def]
    """The ordinary worker path retains its legacy unbound null shape."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 30,
            },
        )
        assert r.status_code == 201
        ctx = verify_step_jwt(
            r.json()["token"],
            signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
        )
        assert ctx.provider_connection_id is None
        assert ctx.provider_connection_id_bound is False


def test_step_token_carries_trial_provider_connection_id(
    app,
    seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    """Issue #72: CP pulls provider_connection_id from the Trial row at
    mint time (defense against a compromised worker forging a different
    connection_id). Verify the JWT scope contains the right id."""
    from sqlalchemy import update as sa_update

    from loom.db.schema import ProviderConnection

    conn_id = uuid4()
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as s:
        # FK trial → provider_connections requires the connection row
        # to exist. Seed a minimal one for this team.
        s.execute(
            insert(ProviderConnection).values(
                id=conn_id,
                team_id=seed["team_id"],
                provider_type="openai-compatible",
                display_name=f"test-conn-{conn_id}",
                base_url="https://api.openai.com/v1",
                upstream_host="api.openai.com",
                encrypted_api_key_ref=f"loom://team:{seed['team_id']}/{conn_id}",
                created_by="admin:fixture",
            )
        )
        s.execute(
            sa_update(Trial)
            .where(Trial.id == seed["trial_id"])
            .values(provider_connection_id=conn_id),
        )
        s.commit()
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 30,
            },
        )
        assert r.status_code == 201
        ctx = verify_step_jwt(
            r.json()["token"],
            signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
        )
        assert ctx.provider_connection_id == conn_id


def test_worker_step_token_does_not_accept_provider_connection_id_in_payload(
    app,
    seed,
):  # type: ignore[no-untyped-def]
    """Ordinary workers cannot select an evolver provider connection."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "family_evolver",
                "ttl_sec": 30,
                # Attempt at forgery — trial has NULL connection_id.
                "provider_connection_id": str(uuid4()),
            },
        )
        assert r.status_code == 403


def _insert_provider(
    postgres_url: str,
    *,
    owner_team_id,
    target_team_id=None,
):
    conn_id = uuid4()
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as s:
        if s.get(Team, owner_team_id) is None:
            s.execute(insert(Team).values(id=owner_team_id, name=f"owner-{owner_team_id}"))
            s.execute(insert(TeamQuota).values(team_id=owner_team_id))
        s.execute(
            insert(ProviderConnection).values(
                id=conn_id,
                team_id=owner_team_id,
                provider_type="openai-compatible",
                display_name=f"evolver-{conn_id}",
                base_url="https://provider.example/v1",
                upstream_host="provider.example",
                encrypted_api_key_ref=f"loom://team:{owner_team_id}/{conn_id}",
                created_by="test:family-evolver",
            )
        )
        if target_team_id is not None and target_team_id != owner_team_id:
            s.execute(
                insert(ProviderConnectionShare).values(
                    provider_connection_id=conn_id,
                    target_team_id=target_team_id,
                    created_by_actor="test:family-evolver-share",
                )
            )
        s.commit()
    engine.dispose()
    return conn_id


@pytest.mark.parametrize("token_type", ["team", "worker"])
@pytest.mark.parametrize("resources_exist", [False, True])
def test_family_evolver_rejects_wrong_principal_before_resource_lookup(
    app,
    seed,
    postgres_url,
    token_type,
    resources_exist,
):  # type: ignore[no-untyped-def]
    principal_team_id = uuid4()
    raw = f"loom_{token_type}_{uuid4().hex}"
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as s:
        s.execute(
            insert(Team).values(
                id=principal_team_id,
                name=f"wrong-principal-{principal_team_id}",
            )
        )
        s.execute(insert(TeamQuota).values(team_id=principal_team_id))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type=token_type,
                scopes=["family:evolve"],
                team_id=principal_team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.commit()
    engine.dispose()

    trial_id = seed["trial_id"] if resources_exist else uuid4()
    provider_connection_id = (
        _insert_provider(postgres_url, owner_team_id=seed["team_id"])
        if resources_exist
        else uuid4()
    )
    with TestClient(app) as client:
        response = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(trial_id),
                "step_id": "family_evolver",
                "ttl_sec": 60,
                "provider_connection_id": str(provider_connection_id),
            },
        )

    assert response.status_code == 403
    assert response.json() == {
        "detail": (
            "family_evolver step tokens require the dedicated principal and an explicit "
            "provider_connection_id field"
        )
    }


@pytest.mark.parametrize("shared", [False, True])
def test_family_evolver_token_binds_owned_or_shared_provider(
    app,
    seed,
    postgres_url,
    shared,
):  # type: ignore[no-untyped-def]
    owner_team_id = uuid4() if shared else seed["team_id"]
    conn_id = _insert_provider(
        postgres_url,
        owner_team_id=owner_team_id,
        target_team_id=seed["team_id"] if shared else None,
    )
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['family_token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "family_evolver",
                "ttl_sec": 60,
                "provider_connection_id": str(conn_id),
            },
        )
    assert r.status_code == 201, r.text
    ctx = verify_step_jwt(
        r.json()["token"],
        signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
    )
    assert ctx.team_id == seed["team_id"]
    assert ctx.trial_id == seed["trial_id"]
    assert ctx.provider_connection_id == conn_id


def test_family_evolver_explicit_null_binds_platform_provider(
    app,
    seed,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['family_token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "family_evolver",
                "ttl_sec": 60,
                "provider_connection_id": None,
            },
        )
    assert r.status_code == 201, r.text
    ctx = verify_step_jwt(
        r.json()["token"],
        signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
    )
    assert ctx.provider_connection_id is None
    assert ctx.provider_connection_id_bound is True


def test_family_evolver_rejects_unshared_cross_team_provider(
    app,
    seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    conn_id = _insert_provider(postgres_url, owner_team_id=uuid4())
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['family_token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "family_evolver",
                "ttl_sec": 60,
                "provider_connection_id": str(conn_id),
            },
        )
    assert r.status_code == 404
    assert r.json()["detail"] == "provider_connection not found"
