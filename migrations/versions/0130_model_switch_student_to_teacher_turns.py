"""Allow student_to_teacher_turns mix plans (turn-grain rising beta + latch).

Revision ID: 0130
Revises: 0129
"""

from alembic import op

revision = "0130"
down_revision = "0129"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE model_switch_plans
          DROP CONSTRAINT IF EXISTS model_switch_plans_mix_mode_check,
          DROP CONSTRAINT IF EXISTS model_switch_plans_schedule_xor_beta_check;

        ALTER TABLE model_switch_plans
          ADD CONSTRAINT model_switch_plans_mix_mode_check
            CHECK (mix_mode IN (
              'student_teacher_student',
              'beta_mixture',
              'student_to_teacher_turns'
            )),
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
              OR (
                mix_mode = 'student_to_teacher_turns'
                AND k1 IS NOT NULL AND k1 >= 2
                AND k2 IS NOT NULL AND k2 > k1
                AND teacher_episodes IS NULL
                AND beta IS NULL
              )
            );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM model_switch_plans
          WHERE mix_mode = 'student_to_teacher_turns';

        ALTER TABLE model_switch_plans
          DROP CONSTRAINT IF EXISTS model_switch_plans_mix_mode_check,
          DROP CONSTRAINT IF EXISTS model_switch_plans_schedule_xor_beta_check;

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
