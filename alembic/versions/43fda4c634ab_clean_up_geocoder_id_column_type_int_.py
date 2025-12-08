"""clean up geocoder.id column type (int -> tinyint)

Revision ID: 43fda4c634ab
Revises: 6408b7c82966
Create Date: 2025-12-07 23:52:50.813065

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from traffic_violations.models import geocoder


# revision identifiers, used by Alembic.
revision = '43fda4c634ab'
down_revision = '6408b7c82966'
branch_labels = None
depends_on = None


def upgrade():
    # drop fk constraint
    op.drop_constraint("geocodes_geocoder_id_fk_1", 'geocodes', type_='foreignkey')

    # drop fk column
    op.drop_column("geocodes", "geocoder_id")

    # # drop geocoder table
    op.drop_table("geocoder")

    # re-add table with TINYINT referenced column
    op.create_table(
        "geocoder",
        sa.Column("id", mysql.TINYINT, primary_key=True),
        sa.Column("name", sa.String(32), nullable=False),
    )

    google = geocoder.Geocoder(name="Google")

    geocoder.Geocoder.query.session.add(google)
    geocoder.Geocoder.query.session.commit()

    # re-add fk column with TINYINT
    op.add_column(
        "geocodes",
        sa.Column(
            "geocoder_id",
            mysql.TINYINT,
            nullable=False,
            server_default="1",
        )
    )

    # re-add fk constraint
    op.create_foreign_key(
        "geocodes_geocoder_id_fk_1",
        "geocodes",
        "geocoder",
        ["geocoder_id"],
        ["id"],
    )


def downgrade():
    # drop fk constraint
    op.drop_constraint("geocodes_geocoder_id_fk_1", 'geocodes', type_='foreignkey')

    # drop fk column
    op.drop_column("geocodes", "geocoder_id")

    # drop referenced column
    op.drop_table("geocoder")

    # re-add table with Integer referenced column
    op.create_table(
        "geocoder",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(32), nullable=False),
    )

    google = geocoder.Geocoder(name="Google")

    geocoder.Geocoder.query.session.add(google)
    geocoder.Geocoder.query.session.commit()

    # re-add fk column with Integer
    op.add_column(
        "geocodes",
        sa.Column(
            "geocoder_id",
            sa.Integer(),
            nullable=False,
            server_default="1",
        )
    )

    # re-add fk constraint
    op.create_foreign_key(
        "geocodes_geocoder_id_fk_1",
        "geocodes",
        "geocoder",
        ["geocoder_id"],
        ["id"],
    )
