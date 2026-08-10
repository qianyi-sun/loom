from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom_control_plane.execution_attempt_fencing import (
    AttemptFenceError,
    verify_attempt_claim,
)


async def test_attempt_fence_checks_worker_claim_epoch_token_and_expiry(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, worker_id, run_id, stage_id, attempt_id, claim_id = (
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    )
    raw_token = "lease-" + "x" * 40
    lease_digest = hashlib.sha256(raw_token.encode()).hexdigest()
    worker_token_hash = b"z" * 32
    digest = "sha256:" + "d" * 64
    async with sessions() as session, session.begin():
        for statement, params in (
            ("INSERT INTO teams(id,name) VALUES (:id,:name)", {"id": team_id, "name": f"fence-{team_id}"}),
            ("INSERT INTO team_quotas(team_id) VALUES (:id)", {"id": team_id}),
            ("""INSERT INTO workers(id,hostname,version,capabilities,auth_token_hash,
                    registered_at,last_seen_at,status)
                    VALUES (:id,'worker','test','[]'::jsonb,:token,now(),now(),'active')""",
             {"id": worker_id, "token": worker_token_hash}),
            ("""INSERT INTO pipeline_runs(id,team_id,submission_policy,recipe_name,recipe_version,
                    recipe_digest,graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key)
                    VALUES (:id,:team,'ordinary','fence-test',1,:digest,'{}'::jsonb,:digest,
                    '{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,:digest,:key)""",
             {"id": run_id, "team": team_id, "digest": digest, "key": f"fence-{run_id}"}),
            ("""INSERT INTO pipeline_stage_runs(id,pipeline_run_id,node_key,shard_key,node_kind,
                    state,resource_profile_json,resource_profile_digest,failure_policy)
                    VALUES (:id,:run,'stage','singleton','container','blocked','{}'::jsonb,:digest,
                    'fail_run')""",
             {"id": stage_id, "run": run_id, "digest": digest}),
            ("""INSERT INTO execution_attempts(id,stage_run_id,attempt_number,state,worker_id,
                    claim_id,lease_epoch,lease_token_digest,lease_expires_at,queued_at,claimed_at)
                    VALUES (:id,:stage,1,'claimed',:worker,:claim,2,:token,:expires,now(),now())""",
             {"id": attempt_id, "stage": stage_id, "worker": worker_id, "claim": claim_id,
              "token": lease_digest, "expires": datetime.now(UTC) + timedelta(minutes=1)}),
        ):
            await session.execute(text(statement), params)
    ctx = AuthContext(
        token_hash=worker_token_hash,
        type="worker",
        scopes=["worker:report"],
        team_id=None,
        expires_at=None,
    )
    async with sessions() as session:
        attempt = await verify_attempt_claim(
            session,
            attempt_id=attempt_id,
            auth=ctx,
            claim_id=claim_id,
            lease_epoch=2,
            lease_token=raw_token,
            require_live_lease=True,
            lock=False,
        )
        assert attempt.id == attempt_id
        with pytest.raises(AttemptFenceError, match="claim_fenced"):
            await verify_attempt_claim(
                session,
                attempt_id=attempt_id,
                auth=ctx,
                claim_id=claim_id,
                lease_epoch=2,
                lease_token="wrong-" + "y" * 40,
                require_live_lease=True,
                lock=False,
            )
    async with engine.begin() as connection:
        await connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
        await connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
        await connection.execute(text("DELETE FROM team_quotas WHERE team_id=:id"), {"id": team_id})
        await connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
    await engine.dispose()
