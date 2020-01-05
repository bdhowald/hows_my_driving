import argparse
import math

import os
import logging

from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
from typing import List, Tuple

from traffic_violations.constants import L10N

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse

from traffic_violations.services.apis.open_data_service import OpenDataService
from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter

LOG = logging.getLogger(__name__)


class RecklessDriverRetrospectiveJob(BaseJob):
    """ Tweet out a previously-repored reckless driver. """

    CAMERA_VIOLATIONS = ['Bus Lane Violation',
                         'Failure To Stop At Red Light',
                         'School Zone Speed Camera Violation']

    def perform(self, *args, **kwargs):
        is_dry_run: bool = kwargs.get('is_dry_run') or False

        now = datetime.now()

        top_of_the_hour_last_year = now.replace(
            microsecond=0,
            minute=0,
            second=0) - relativedelta(years=1)

        top_of_the_next_hour_last_year = (
            top_of_the_hour_last_year + relativedelta(hours=1))

        recent_plate_lookup_ids: List[Tuple[int]] = PlateLookup.query.session.query(
            func.max(PlateLookup.id).label('most_recent_vehicle_lookup')
        ).filter(
            and_(PlateLookup.created_at >= top_of_the_hour_last_year,
                 PlateLookup.created_at < top_of_the_next_hour_last_year,
                 PlateLookup.boot_eligible == True,
                 PlateLookup.count_towards_frequency == True)
        ).group_by(
            PlateLookup.plate,
            PlateLookup.state
        ).all()

        lookup_ids_to_update: List[int] = [id[0] for id in recent_plate_lookup_ids]

        lookups_to_update: List[PlateLookup] = PlateLookup.get_all_in(
            id=lookup_ids_to_update)

        for previous_lookup in lookups_to_update:

            plate_query: PlateQuery=PlateQuery(created_at=now,
                                                 message_source=previous_lookup.message_source,
                                                 plate=previous_lookup.plate,
                                                 plate_types=previous_lookup.plate_types,
                                                 state=previous_lookup.state)

            nyc_open_data_service: OpenDataService=OpenDataService()
            open_data_response: OpenDataServiceResponse=nyc_open_data_service.lookup_vehicle(
                plate_query=plate_query)

            open_data_plate_lookup: OpenDataServicePlateLookup=open_data_response.data

            new_bus_lane_camera_violations=None
            new_speed_camera_violations=None
            new_red_light_camera_violations=None

            for violation_type_summary in open_data_plate_lookup.violations:
                if violation_type_summary['title'] in self.CAMERA_VIOLATIONS:
                    violation_count=violation_type_summary['count']

                    if violation_type_summary['title'] == 'Bus Lane Violation':
                        new_bus_lane_camera_violations=(violation_count -
                            previous_lookup.bus_lane_camera_violations)
                    if violation_type_summary['title'] == 'Failure To Stop At Red Light':
                        new_speed_camera_violations=(violation_count -
                            previous_lookup.red_light_camera_violations)
                    elif violation_type_summary['title'] == 'School Zone Speed Camera Violation':
                        new_red_light_camera_violations=(violation_count -
                            previous_lookup.speed_camera_violations)

            if not new_bus_lane_camera_violations:
                new_bus_lane_camera_violations=previous_lookup.bus_lane_camera_violations

            if not new_speed_camera_violations:
                new_speed_camera_violations=previous_lookup.red_light_camera_violations

            if not new_red_light_camera_violations:
                new_red_light_camera_violations=previous_lookup.speed_camera_violations

            print(
                f'Update for {previous_lookup.state}:{previous_lookup.plate} :\n'
                f'New bus lane camera tickets: {new_bus_lane_camera_violations}\n'
                f'New speed camera tickets: {new_speed_camera_violations}\n'
                f'New red light camera tickets: {new_red_light_camera_violations}\n')

def parse_args():
    parser=argparse.ArgumentParser(
        description='Perform lookups to collect yesterday\'s statistics.')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't tweet results")

    return parser.parse_args()


if __name__ == '__main__':
    arguments=parse_args()

    job=RecklessDriverRetrospectiveJob()
    job.run(is_dry_run=arguments.dry_run)
