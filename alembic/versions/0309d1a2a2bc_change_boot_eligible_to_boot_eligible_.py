"""change boot_eligible to boot_eligible_under_rdaa_threshold

Revision ID: 0309d1a2a2bc
Revises:
Create Date: 2020-05-03 16:59:30.719808

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0309d1a2a2bc'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('plate_lookups',
                    column_name='boot_eligible',
                    new_column_name='boot_eligible_under_rdaa_threshold',
                    existing_type=sa.Boolean(),
                    existing_nullable=False)


def downgrade():
    op.alter_column('plate_lookups_under_rdaa_threshold',
                    column_name='boot_eligible',
                    new_column_name='boot_eligible',
                    existing_type=sa.Boolean(),
                    existing_nullable=False)
