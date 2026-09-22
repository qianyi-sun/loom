"""Allow beta-mixture plans alongside K1/K2 model_switch_plans.

Revision ID: 0104
Revises: 0103
"""

from alembic import op

revision = "0104"
down_revision = "0103"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE model_switch_plans
          ADD COLUMN mix_mode TEXT NOT NULL DEFAULT 'student_teacher_student',
          ADD COLUMN beta DOUBLE PRECISION,
          ALTER COLUMN k1 DROP NOT NULL,
          ALTER COLUMN k2 DROP NOT NULL,
          ALTER COLUMN teacher_episodes DROP NOT NULL;

        ALTER TABLE model_switch_plans
          DROP CONSTRAINT IF EXISTS model_switch_plans_k1_check,
          DROP CONSTRAINT IF EXISTS model_switch_plans_k2_check,
          DROP CONSTRAINT IF EXISTS model_switch_plans_teacher_episodes_check;

        ALTER TABLE model_switch_plans
          ADD CONSTRAINT model_switch_plans_mix_mode_check
            CHECK (mix_mode IN ('student_teacher_student', 'beta_mixture')),
          ADD CONSTRAINT model_switch_plans_schedule_xor_beta_check
            CHECK (
              (
                mix_mode = 'student_teacher_student'
                AND k1 IS NOT NULL AND k1 >= 2
                AND k2 IS NOT NULL AND k2 > k1
                AND teacher_episodes IS NOT NULL AND teacher_episodes >= 1
                AND beta IS NULL
              )
              OR (
                mix_mode = 'beta_mixture'
                AND beta IS NOT NULL AND beta >= 0 AND beta <= 1
                AND k1 IS NULL AND k2 IS NULL AND teacher_episodes IS NULL
              )
            );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM model_switch_plans WHERE mix_mode = 'beta_mixture';
        ALTER TABLE model_switch_plans
          DROP CONSTRAINT IF EXISTS model_switch_plans_schedule_xor_beta_check,
          DROP CONSTRAINT IF EXISTS model_switch_plans_mix_mode_check;
        ALTER TABLE model_switch_plans
          ALTER COLUMN k1 SET NOT NULL,
          ALTER COLUMN k2 SET NOT NULL,
          ALTER COLUMN teacher_episodes SET NOT NULL,
          DROP COLUMN IF EXISTS beta,
          DROP COLUMN IF EXISTS mix_mode;
        ALTER TABLE model_switch_plans
          ADD CONSTRAINT model_switch_plans_k1_check CHECK (k1 >= 2),
          ADD CONSTRAINT model_switch_plans_k2_check CHECK (k2 > k1),
          ADD CONSTRAINT model_switch_plans_teacher_episodes_check
            CHECK (teacher_episodes >= 1);
        """
    )
