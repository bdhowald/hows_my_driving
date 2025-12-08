"""add geocoder column and foreign key to geocodes records

Revision ID: 742f54768280
Revises: ac4075934a0f
Create Date: 2025-03-09 14:00:07.366151

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "742f54768280"
down_revision = "ac4075934a0f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "geocodes",
        sa.Column(
            "geocoder_id",
            sa.Integer(),
            nullable=False,
            server_default="1",
        )
    )

    op.create_foreign_key(
        "geocodes_geocoder_id_fk_1",
        "geocodes",
        "geocoder",
        ["geocoder_id"],
        ["id"],
    )


def downgrade():
    op.drop_constraint("geocodes_geocoder_id_fk_1", 'geocodes', type_='foreignkey')
    op.drop_column("geocodes", "geocoder_id")
