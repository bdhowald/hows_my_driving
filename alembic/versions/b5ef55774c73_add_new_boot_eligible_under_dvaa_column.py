"""add new boot_eligible_under_dvaa column

Revision ID: b5ef55774c73
Revises: 0309d1a2a2bc
Create Date: 2020-05-03 18:58:48.013852

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b5ef55774c73'
down_revision = '0309d1a2a2bc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('plate_lookups',
                  sa.Column('boot_eligible_under_dvaa_threshold',
                  sa.Boolean(), nullable=False, server_default=sa.false()))
    op.create_index('index_boot_eligible_under_dvaa_threshold',
                    'plate_lookups', ['boot_eligible_under_dvaa_threshold'])


def downgrade():
    op.drop_index('index_boot_eligible_under_dvaa_threshold', 'plate_lookups')
    op.drop_column('plate_lookups', 'boot_eligible_under_dvaa_threshold')

