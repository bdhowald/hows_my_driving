"""store geocode latitude, longitude, location type, and location names

Revision ID: eba9e08708b9
Revises: 43fda4c634ab
Create Date: 2025-12-06 16:37:42.631885

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


from traffic_violations.models import geocoder_location_type


# revision identifiers, used by Alembic.
revision = 'eba9e08708b9'
down_revision = '43fda4c634ab'
branch_labels = None
depends_on = None


def upgrade():
    # geocoder_location_types

    op.create_table(
        'geocoder_location_types',
        sa.Column('id', mysql.TINYINT, primary_key=True),
        sa.Column('geocoder_id', mysql.TINYINT),
        sa.Column('name', sa.String(20), nullable=False),
    )

    op.create_foreign_key(
        'geocoder_location_types_geocoder_id_fk_1',
        'geocoder_location_types',
        'geocoder',
        ['geocoder_id'],
        ['id'],
    )

    approximate = geocoder_location_type.GeocoderLocationType(
        geocoder_id=1, # Google
        name='APPROXIMATE',
    )
    geometric_center = geocoder_location_type.GeocoderLocationType(
        geocoder_id=1, # Google
        name='GEOMETRIC_CENTER',
    )
    range_interpolated = geocoder_location_type.GeocoderLocationType(
        geocoder_id=1, # Google
        name='RANGE_INTERPOLATED',
    )
    rooftop = geocoder_location_type.GeocoderLocationType(
        geocoder_id=1, # Google
        name='ROOFTOP',
    )

    geocoder_location_type.GeocoderLocationType.query.session.add(approximate)
    geocoder_location_type.GeocoderLocationType.query.session.add(geometric_center)
    geocoder_location_type.GeocoderLocationType.query.session.add(range_interpolated)
    geocoder_location_type.GeocoderLocationType.query.session.add(rooftop)

    geocoder_location_type.GeocoderLocationType.query.session.commit()


    # geocodes.latitude

    op.add_column(
        'geocodes',
        sa.Column(
            'latitude',
            sa.Numeric(
                precision=9,
                scale=6,
                asdecimal=True,
            ),
            nullable=True,
        )
    )

    op.create_check_constraint(
        'geocodes_ck_latitude',
        'geocodes',
        sa.text(f'latitude is null or latitude >= -90 and latitude <= 90'),
        schema=None
    )

    # geocodes.longitude

    op.add_column(
        'geocodes',
        sa.Column(
            'longitude',
            sa.Numeric(
                precision=9,
                scale=6,
                asdecimal=True,
            ),
            nullable=True,
        )
    )

    op.create_check_constraint(
        'geocodes_ck_longitude',
        'geocodes',
        sa.text(f'longitude is null or longitude >= -180 and latitude <= 180'),
        schema=None
    )


    # geocodes.location_type

    op.add_column(
        'geocodes',
        sa.Column(
            'geocoder_location_type_id',
            mysql.TINYINT,
            nullable=True,
        )
    )

    op.create_foreign_key(
        'geocodes_geocoder_location_type_id_fk_1',
        'geocodes',
        'geocoder_location_types',
        ['geocoder_location_type_id'],
        ['id'],
    )

    # geocodes.full_name

    op.add_column(
        'geocodes',
        sa.Column(
            'full_name',
            sa.String(255),
            nullable=True,
        )
    )

    # geocodes.short_name

    op.add_column(
        'geocodes',
        sa.Column(
            'short_name',
            sa.String(255),
            nullable=True,
        )
    )


def downgrade():
    op.drop_column('geocodes', 'short_name')
    op.drop_column('geocodes', 'full_name')

    op.drop_constraint(
        'geocodes_geocoder_location_type_id_fk_1',
        'geocodes',
        type_='foreignkey'
    )

    op.drop_column('geocodes', 'geocoder_location_type_id')

    op.drop_constraint(
        'geocodes_ck_longitude',
        'geocodes',
        type_='check',
        schema=None,
    )

    op.drop_column('geocodes', 'longitude')

    op.drop_constraint(
        'geocodes_ck_latitude',
        'geocodes',
        type_='check',
        schema=None,
    )

    op.drop_column('geocodes', 'latitude')

    op.drop_table('geocoder_location_types')
