import argparse
import math

import os
import logging
import threading

from datetime import datetime, time, timedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
from typing import List, Optional

from traffic_violations.constants import L10N

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse

from traffic_violations.services.apis.open_data_service import OpenDataService
from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter

from traffic_violations.utils import string_utils, twitter_utils

LOG = logging.getLogger(__name__)


class RecklessDriverRetrospectiveJob(BaseJob):
    """ Tweet out a previously-repored reckless driver. """

    CAMERA_VIOLATIONS = ['Bus Lane Violation',
                         'Failure To Stop At Red Light',
                         'School Zone Speed Camera Violation']

    def perform(self, *args, **kwargs):
        new_tickets_rates: List[Tuple[int, int]] = []

        plate_lookups = PlateLookup.get_all_by(
            boot_eligible=True,
            count_towards_frequency=True)

        threads = []
        num_threads = 10
        chunk_length = math.ceil(len(plate_lookups)/num_threads)

        for n in range(0, num_threads):
            chunk_begin = n * chunk_length
            chunk_plate_lookups = plate_lookups[chunk_begin:(chunk_begin + chunk_length)]
            print(f'this thread will handle lookups: {[l.id for l in chunk_plate_lookups]}')
            threads.append(threading.Thread(target=self.update_lookups, args=(chunk_plate_lookups,)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        PlateLookup.query.session.commit()

    def update_lookups(self, lookups: List[PlateLookup]):

        for previous_lookup in lookups:
            now = datetime.utcnow()
            plate_query: PlateQuery = PlateQuery(created_at=now,
                                                 message_source=previous_lookup.message_source,
                                                 plate=previous_lookup.plate,
                                                 plate_types=previous_lookup.plate_types,
                                                 state=previous_lookup.state)

            nyc_open_data_service: OpenDataService = OpenDataService()
            open_data_response: OpenDataServiceResponse = nyc_open_data_service.lookup_vehicle(
                plate_query=plate_query,
                until=previous_lookup.created_at)

            open_data_plate_lookup: OpenDataServicePlateLookup = open_data_response.data

            for violation_type_summary in open_data_plate_lookup.violations:
                if violation_type_summary['title'] in self.CAMERA_VIOLATIONS:
                    violation_count = violation_type_summary['count']

                    if violation_type_summary['title'] == 'Bus Lane Violation':
                        previous_lookup.bus_lane_camera_violations = violation_count
                    if violation_type_summary['title'] == 'Failure To Stop At Red Light':
                        previous_lookup.red_light_camera_violations = violation_count
                    elif violation_type_summary['title'] == 'School Zone Speed Camera Violation':
                        previous_lookup.speed_camera_violations = violation_count

            if not previous_lookup.bus_lane_camera_violations:
                previous_lookup.bus_lane_camera_violations = 0

            if not previous_lookup.red_light_camera_violations:
                previous_lookup.red_light_camera_violations = 0

            if not previous_lookup.speed_camera_violations:
                previous_lookup.speed_camera_violations = 0

            LOG.debug(f'updating lookup {previous_lookup.id}')
            print(f'updating lookup {previous_lookup.id}')


            # camera_streak_data: CameraStreakData = open_data_plate_lookup.camera_streak_data

            # new_lookup = PlateLookup(
            #     boot_eligible=camera_streak_data.max_streak >= 5 if camera_streak_data else False,
            #     created_at=plate_query.created_at,
            #     message_id=plate_query.message_id,
            #     message_source=plate_query.message_source,
            #     num_tickets=open_data_plate_lookup.num_violations,
            #     plate=plate_query.plate,
            #     plate_types=plate_query.plate_types,
            #     state=plate_query.state,
            #     username=plate_query.username)

            # tickets_since: int = new_lookup.num_tickets - previous_lookup.num_tickets
            # days_since: int = (now - previous_lookup.created_at).days

            # new_tickets_rates.append((tickets_since, days_since,))

            # print((tickets_since, days_since,))


def parse_args():
    parser = argparse.ArgumentParser(
        description='Perform lookups to collect yesterday\'s statistics.')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't save results")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_args()

    job = RecklessDriverRetrospectiveJob()
    job.run(is_dry_run=arguments.dry_run)
