"""add migration for COVID-19 repeat offenders table

Revision ID: e28150ce7c3d
Revises: eba9e08708b9
Create Date: 2025-12-08 21:51:23.354331

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = 'e28150ce7c3d'
down_revision = 'eba9e08708b9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'covid_19_camera_offenders',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('plate_id', sa.String(12), nullable=False),
        sa.Column('state', sa.String(2), nullable=False),
        sa.Column('red_light_camera_violations', mysql.SMALLINT(unsigned=True) , nullable=False),
        sa.Column('speed_camera_violations', mysql.SMALLINT(unsigned=True) , nullable=False),
        sa.Column('count_as_queried', sa.Boolean(), nullable=False),
    )


def downgrade():
    op.drop_table('covid_19_camera_offenders')
