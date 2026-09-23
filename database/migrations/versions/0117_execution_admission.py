"""Add race-safe hybrid execution admission ceilings and reservations.

Revision ID: 0117
Revises: 0116
"""

from alembic import op

revision = "0117"
down_revision = "0116"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE execution_admission_policies (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          scope_kind TEXT NOT NULL,
          scope_key TEXT NOT NULL,
          max_concurrent INTEGER NOT NULL,
          active_count INTEGER NOT NULL DEFAULT 0,
          counter_updated_at TIMESTAMPTZ,
          enabled BOOLEAN NOT NULL DEFAULT false,
          reason TEXT,
          version BIGINT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT execution_admission_policies_scope_kind_check CHECK (
            scope_kind IN ('global','environment','region','team','batch',
                           'execution_class','pool')
          ),
          CONSTRAINT execution_admission_policies_scope_key_check CHECK (
            length(trim(scope_key)) BETWEEN 1 AND 120
          ),
          CONSTRAINT execution_admission_policies_max_concurrent_check CHECK (
            max_concurrent > 0
          ),
          CONSTRAINT execution_admission_policies_active_count_check CHECK (
            active_count >= 0
          ),
          CONSTRAINT execution_admission_policies_global_key_check CHECK (
            (scope_kind = 'global' AND scope_key = '*') OR scope_kind <> 'global'
          ),
          CONSTRAINT execution_admission_policies_scope_uidx UNIQUE (scope_kind, scope_key)
        );

        CREATE TABLE execution_admission_reservations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          trial_id UUID NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
          attempt INTEGER NOT NULL,
          execution_role TEXT NOT NULL,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          batch_id UUID REFERENCES batches(id) ON DELETE RESTRICT,
          environment TEXT,
          region TEXT,
          execution_class_id TEXT,
          pool_id TEXT NOT NULL,
          owner_kind TEXT NOT NULL,
          owner_id UUID NOT NULL,
          state TEXT NOT NULL DEFAULT 'active',
          acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          released_at TIMESTAMPTZ,
          release_reason TEXT,
          CONSTRAINT execution_admission_reservations_attempt_check CHECK (attempt > 0),
          CONSTRAINT execution_admission_reservations_role_check CHECK (
            execution_role IN ('attempt','verifier')
          ),
          CONSTRAINT execution_admission_reservations_owner_kind_check CHECK (
            owner_kind IN ('legacy_worker_claim','service_execution_lease')
          ),
          CONSTRAINT execution_admission_reservations_state_check CHECK (
            state IN ('active','released')
          ),
          CONSTRAINT execution_admission_reservations_release_group_check CHECK (
            (state = 'active' AND released_at IS NULL AND release_reason IS NULL) OR
            (state = 'released' AND released_at IS NOT NULL
             AND length(trim(release_reason)) > 0)
          ),
          CONSTRAINT execution_admission_reservations_trial_attempt_role_uidx
            UNIQUE (trial_id, attempt, execution_role)
        );
        CREATE INDEX execution_admission_reservations_active_scope_idx
          ON execution_admission_reservations
             (state, pool_id, environment, team_id);

        CREATE FUNCTION loom_execution_admission_blocker(
          p_team_id UUID,
          p_batch_id UUID,
          p_environment TEXT,
          p_region TEXT,
          p_execution_class_id TEXT,
          p_pool_id TEXT
        ) RETURNS TABLE (
          scope_kind TEXT,
          scope_key TEXT,
          max_concurrent INTEGER,
          active_count BIGINT
        ) LANGUAGE plpgsql AS $$
        BEGIN
          RETURN QUERY
          SELECT policy.scope_kind,
                 policy.scope_key,
                 policy.max_concurrent,
                 policy.active_count::bigint
            FROM execution_admission_policies policy
           WHERE policy.enabled
             AND CASE policy.scope_kind
                   WHEN 'global' THEN policy.scope_key = '*'
                   WHEN 'environment' THEN p_environment = policy.scope_key
                   WHEN 'region' THEN p_region = policy.scope_key
                   WHEN 'team' THEN p_team_id::text = policy.scope_key
                   WHEN 'batch' THEN p_batch_id::text = policy.scope_key
                   WHEN 'execution_class' THEN p_execution_class_id = policy.scope_key
                   WHEN 'pool' THEN p_pool_id = policy.scope_key
                   ELSE false
                 END
             AND policy.active_count >= policy.max_concurrent
           ORDER BY CASE policy.scope_kind
                      WHEN 'global' THEN 1
                      WHEN 'environment' THEN 2
                      WHEN 'region' THEN 3
                      WHEN 'team' THEN 4
                      WHEN 'batch' THEN 5
                      WHEN 'execution_class' THEN 6
                      WHEN 'pool' THEN 7
                      ELSE 8
                    END,
                    policy.scope_key
           LIMIT 1;
        END;
        $$;

        CREATE FUNCTION loom_execution_admission_available(
          p_team_id UUID,
          p_batch_id UUID,
          p_environment TEXT,
          p_region TEXT,
          p_execution_class_id TEXT,
          p_pool_id TEXT
        ) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
        DECLARE
          blocked RECORD;
        BEGIN
          SELECT * INTO blocked
            FROM loom_execution_admission_blocker(
              p_team_id, p_batch_id, p_environment, p_region,
              p_execution_class_id, p_pool_id
            );
          RETURN NOT FOUND;
        END;
        $$;

        CREATE FUNCTION loom_execution_admission_reserve(
          p_trial_id UUID,
          p_attempt INTEGER,
          p_execution_role TEXT,
          p_team_id UUID,
          p_batch_id UUID,
          p_environment TEXT,
          p_region TEXT,
          p_execution_class_id TEXT,
          p_pool_id TEXT,
          p_owner_kind TEXT,
          p_owner_id UUID,
          p_acquired_at TIMESTAMPTZ
        ) RETURNS UUID LANGUAGE plpgsql AS $$
        DECLARE
          reservation_id UUID;
          policy_row RECORD;
        BEGIN
          -- Policy mutations take the matching exclusive advisory lock. The
          -- shared lock keeps those rare mutations out of the reservation
          -- critical section without serializing reservations with each other.
          PERFORM pg_advisory_xact_lock_shared(
            hashtextextended('execution-admission-policy-mutation', 1552)
          );
          -- Lock every matching ceiling in one canonical order. Overlapping
          -- admissions serialize on the shared scope rows; disjoint pools and
          -- teams remain concurrent. FOR UPDATE observes the latest committed
          -- counter after a waiter acquires the row lock.
          FOR policy_row IN
            SELECT policy.scope_kind, policy.scope_key,
                   policy.max_concurrent, policy.active_count
              FROM execution_admission_policies policy
             WHERE policy.enabled
               AND CASE policy.scope_kind
                     WHEN 'global' THEN policy.scope_key = '*'
                     WHEN 'environment' THEN p_environment = policy.scope_key
                     WHEN 'region' THEN p_region = policy.scope_key
                     WHEN 'team' THEN p_team_id::text = policy.scope_key
                     WHEN 'batch' THEN p_batch_id::text = policy.scope_key
                     WHEN 'execution_class' THEN p_execution_class_id = policy.scope_key
                     WHEN 'pool' THEN p_pool_id = policy.scope_key
                     ELSE false
                   END
             ORDER BY CASE policy.scope_kind
                        WHEN 'global' THEN 1
                        WHEN 'environment' THEN 2
                        WHEN 'region' THEN 3
                        WHEN 'team' THEN 4
                        WHEN 'batch' THEN 5
                        WHEN 'execution_class' THEN 6
                        WHEN 'pool' THEN 7
                        ELSE 8
                      END,
                      policy.scope_key
             FOR UPDATE
          LOOP
            IF policy_row.active_count >= policy_row.max_concurrent THEN
              RETURN NULL;
            END IF;
          END LOOP;
          INSERT INTO execution_admission_reservations (
            trial_id, attempt, execution_role, team_id, batch_id,
            environment, region, execution_class_id, pool_id,
            owner_kind, owner_id, acquired_at
          ) VALUES (
            p_trial_id, p_attempt, p_execution_role, p_team_id, p_batch_id,
            p_environment, p_region, p_execution_class_id, p_pool_id,
            p_owner_kind, p_owner_id, p_acquired_at
          )
          ON CONFLICT (trial_id, attempt, execution_role) DO NOTHING
          RETURNING id INTO reservation_id;
          IF reservation_id IS NULL THEN
            RETURN NULL;
          END IF;
          UPDATE execution_admission_policies policy
             SET active_count = policy.active_count + 1,
                 counter_updated_at = NOW()
           WHERE policy.enabled
             AND CASE policy.scope_kind
                   WHEN 'global' THEN policy.scope_key = '*'
                   WHEN 'environment' THEN p_environment = policy.scope_key
                   WHEN 'region' THEN p_region = policy.scope_key
                   WHEN 'team' THEN p_team_id::text = policy.scope_key
                   WHEN 'batch' THEN p_batch_id::text = policy.scope_key
                   WHEN 'execution_class' THEN p_execution_class_id = policy.scope_key
                   WHEN 'pool' THEN p_pool_id = policy.scope_key
                   ELSE false
                 END;
          RETURN reservation_id;
        END;
        $$;

        CREATE FUNCTION loom_release_legacy_execution_admission()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          released RECORD;
        BEGIN
          PERFORM pg_advisory_xact_lock_shared(
            hashtextextended('execution-admission-policy-mutation', 1552)
          );
          IF OLD.state IN ('claimed','running')
             AND NEW.state NOT IN ('claimed','running') THEN
            UPDATE execution_admission_reservations
               SET state = 'released', released_at = NOW(),
                   release_reason = 'trial_left_active_state'
             WHERE trial_id = NEW.id
               AND attempt = NEW.attempt_count
               AND execution_role = 'attempt'
               AND owner_kind = 'legacy_worker_claim'
               AND state = 'active'
            RETURNING * INTO released;
            IF FOUND THEN
              UPDATE execution_admission_policies policy
                 SET active_count = GREATEST(0, policy.active_count - 1),
                     counter_updated_at = NOW()
               WHERE CASE policy.scope_kind
                       WHEN 'global' THEN policy.scope_key = '*'
                       WHEN 'environment' THEN released.environment = policy.scope_key
                       WHEN 'region' THEN released.region = policy.scope_key
                       WHEN 'team' THEN released.team_id::text = policy.scope_key
                       WHEN 'batch' THEN released.batch_id::text = policy.scope_key
                       WHEN 'execution_class' THEN
                         released.execution_class_id = policy.scope_key
                       WHEN 'pool' THEN released.pool_id = policy.scope_key
                       ELSE false
                     END;
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_admission_release_legacy_trigger
          AFTER UPDATE OF state ON trials
          FOR EACH ROW EXECUTE FUNCTION loom_release_legacy_execution_admission();

        CREATE FUNCTION loom_release_service_execution_admission()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          released RECORD;
        BEGIN
          PERFORM pg_advisory_xact_lock_shared(
            hashtextextended('execution-admission-policy-mutation', 1552)
          );
          IF NEW.observed_state IN ('cancelled','timed_out','failed','finalized','deleted')
             AND OLD.observed_state IS DISTINCT FROM NEW.observed_state THEN
            UPDATE execution_admission_reservations
               SET state = 'released', released_at = NOW(),
                   release_reason = 'execution_lease_terminal'
             WHERE owner_kind = 'service_execution_lease'
               AND owner_id = NEW.id
               AND state = 'active'
            RETURNING * INTO released;
            IF FOUND THEN
              UPDATE execution_admission_policies policy
                 SET active_count = GREATEST(0, policy.active_count - 1),
                     counter_updated_at = NOW()
               WHERE CASE policy.scope_kind
                       WHEN 'global' THEN policy.scope_key = '*'
                       WHEN 'environment' THEN released.environment = policy.scope_key
                       WHEN 'region' THEN released.region = policy.scope_key
                       WHEN 'team' THEN released.team_id::text = policy.scope_key
                       WHEN 'batch' THEN released.batch_id::text = policy.scope_key
                       WHEN 'execution_class' THEN
                         released.execution_class_id = policy.scope_key
                       WHEN 'pool' THEN released.pool_id = policy.scope_key
                       ELSE false
                     END;
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_admission_release_service_trigger
          AFTER UPDATE OF observed_state ON execution_leases
          FOR EACH ROW EXECUTE FUNCTION loom_release_service_execution_admission();

        CREATE FUNCTION loom_execution_admission_reservation_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.trial_id, NEW.attempt, NEW.execution_role, NEW.team_id,
                 NEW.batch_id, NEW.environment, NEW.region,
                 NEW.execution_class_id, NEW.pool_id, NEW.owner_kind,
                 NEW.owner_id, NEW.acquired_at)
             IS DISTINCT FROM
             ROW(OLD.trial_id, OLD.attempt, OLD.execution_role, OLD.team_id,
                 OLD.batch_id, OLD.environment, OLD.region,
                 OLD.execution_class_id, OLD.pool_id, OLD.owner_kind,
                 OLD.owner_id, OLD.acquired_at) THEN
            RAISE EXCEPTION 'execution admission reservation identity is immutable';
          END IF;
          IF OLD.state = 'released' AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'released execution admission reservation is immutable';
          END IF;
          RETURN NEW;
        END;
        $$;
        CREATE TRIGGER execution_admission_reservation_immutable_trigger
          BEFORE UPDATE ON execution_admission_reservations
          FOR EACH ROW EXECUTE FUNCTION loom_execution_admission_reservation_immutable();

        CREATE FUNCTION loom_execution_admission_reservation_delete()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock_shared(
            hashtextextended('execution-admission-policy-mutation', 1552)
          );
          IF OLD.state = 'active' THEN
            UPDATE execution_admission_policies policy
               SET active_count = GREATEST(0, policy.active_count - 1),
                   counter_updated_at = NOW()
             WHERE CASE policy.scope_kind
                     WHEN 'global' THEN policy.scope_key = '*'
                     WHEN 'environment' THEN OLD.environment = policy.scope_key
                     WHEN 'region' THEN OLD.region = policy.scope_key
                     WHEN 'team' THEN OLD.team_id::text = policy.scope_key
                     WHEN 'batch' THEN OLD.batch_id::text = policy.scope_key
                     WHEN 'execution_class' THEN OLD.execution_class_id = policy.scope_key
                     WHEN 'pool' THEN OLD.pool_id = policy.scope_key
                     ELSE false
                   END;
          END IF;
          RETURN OLD;
        END;
        $$;
        CREATE TRIGGER execution_admission_reservation_delete_trigger
          BEFORE DELETE ON execution_admission_reservations
          FOR EACH ROW EXECUTE FUNCTION loom_execution_admission_reservation_delete();
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DROP TRIGGER IF EXISTS execution_admission_reservation_delete_trigger
          ON execution_admission_reservations;
        DROP FUNCTION IF EXISTS loom_execution_admission_reservation_delete();
        DROP TRIGGER IF EXISTS execution_admission_reservation_immutable_trigger
          ON execution_admission_reservations;
        DROP FUNCTION IF EXISTS loom_execution_admission_reservation_immutable();
        DROP TRIGGER IF EXISTS execution_admission_release_service_trigger ON execution_leases;
        DROP FUNCTION IF EXISTS loom_release_service_execution_admission();
        DROP TRIGGER IF EXISTS execution_admission_release_legacy_trigger ON trials;
        DROP FUNCTION IF EXISTS loom_release_legacy_execution_admission();
        DROP FUNCTION IF EXISTS loom_execution_admission_reserve(
          UUID, INTEGER, TEXT, UUID, UUID, TEXT, TEXT, TEXT, TEXT, TEXT, UUID, TIMESTAMPTZ
        );
        DROP FUNCTION IF EXISTS loom_execution_admission_available(
          UUID, UUID, TEXT, TEXT, TEXT, TEXT
        );
        DROP FUNCTION IF EXISTS loom_execution_admission_blocker(
          UUID, UUID, TEXT, TEXT, TEXT, TEXT
        );
        DROP TABLE IF EXISTS execution_admission_reservations;
        DROP TABLE IF EXISTS execution_admission_policies;
        """
    )
