"""create geocoder table

Revision ID: ac4075934a0f
Revises: fd47e0d56aaf
Create Date: 2025-03-09 13:38:16.783308

"""
from alembic import op
import sqlalchemy as sa

from traffic_violations.models import geocoder

# revision identifiers, used by Alembic.
revision = "ac4075934a0f"
down_revision = "fd47e0d56aaf"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "geocoder",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(32), nullable=False),
    )

    google = geocoder.Geocoder(name="Google")
    
    geocoder.Geocoder.query.session.add(google)
    geocoder.Geocoder.query.session.commit()


def downgrade():
    op.drop_table("geocoder")
