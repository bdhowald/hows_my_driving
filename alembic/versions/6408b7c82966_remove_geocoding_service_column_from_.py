"""Remove geocoding_service column from geocodes.

Revision ID: 6408b7c82966
Revises: 742f54768280
Create Date: 2025-03-10 00:20:19.887670

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6408b7c82966'
down_revision = '742f54768280'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("geocodes", "geocoding_service")


def downgrade():
    op.add_column(
        "geocodes",
        sa.Column(
            "geocoding_service",
            sa.String(255),
            nullable=False,
            server_default="google",
        )
    )