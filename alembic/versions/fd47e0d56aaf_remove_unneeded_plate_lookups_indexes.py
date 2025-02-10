"""Remove unneeded plate_lookups indexes.

Revision ID: fd47e0d56aaf
Revises: b5ef55774c73
Create Date: 2025-02-09 17:49:20.782025

Remove low-cardinality indexes.

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fd47e0d56aaf'
down_revision = 'b5ef55774c73'
branch_labels = None
depends_on = None


def upgrade():
    # geocodes
    op.drop_index('index_borough', 'geocodes')
    op.drop_index('index_geocoding_service', 'geocodes')

    # plate_lookups
    op.drop_index('index_boot_eligible', 'plate_lookups')
    op.drop_index('index_boot_eligible_under_dvaa_threshold', 'plate_lookups')
    op.drop_index('index_count_towards_frequency', 'plate_lookups')
    op.drop_index('index_observed', 'plate_lookups')
    op.drop_index('index_responded_to', 'plate_lookups')

    # repeat_camera_offenders
    op.drop_index('red_light_camera_violations_index', 'repeat_camera_offenders')
    op.drop_index('speed_camera_violations_index', 'repeat_camera_offenders')
    op.drop_index('state_index', 'repeat_camera_offenders')
    op.drop_index('times_featured_index', 'repeat_camera_offenders')
    op.drop_index('total_camera_violations_index', 'repeat_camera_offenders')


def downgrade():
    # repeat_camera_offenders
    op.create_index('total_camera_violations_index', 'repeat_camera_offenders', ['total_camera_violations'])
    op.create_index('times_featured_index', 'repeat_camera_offenders', ['times_featured'])
    op.create_index('state_index', 'repeat_camera_offenders', ['state'])
    op.create_index('speed_camera_violations_index', 'repeat_camera_offenders', ['speed_camera_violations'])
    op.create_index('red_light_camera_violations_index', 'repeat_camera_offenders', ['red_light_camera_violations'])

    # plate_lookups
    op.create_index('index_responded_to', 'plate_lookups', ['responded_to'])
    op.create_index('index_observed', 'plate_lookups', ['observed'])
    op.create_index('index_count_towards_frequency', 'plate_lookups', ['count_towards_frequency'])
    op.create_index('index_boot_eligible_under_dvaa_threshold', 'plate_lookups', ['boot_eligible_under_dvaa_threshold'])
    op.create_index('index_boot_eligible', 'plate_lookups', ['boot_eligible_under_rdaa_threshold'])

    # geocodes
    op.create_index('index_geocoding_service', 'geocodes', ['geocoding_service'])
    op.create_index('index_borough', 'geocodes', ['borough'])