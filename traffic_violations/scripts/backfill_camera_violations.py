import argparse
import math

import os
import logging
import threading

from datetime import datetime
from typing import List

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse

from traffic_violations.services.apis.open_data_service import OpenDataService

LOG = logging.getLogger(__name__)


class BackfillCameraViolationsJob(BaseJob):
    """ Backfill camera violations for old lookups """

    CAMERA_VIOLATIONS = ['Bus Lane Violation',
                         'Failure To Stop At Red Light',
                         'School Zone Speed Camera Violation']

    def perform(self, *args, **kwargs):
        all_vehicles: bool = kwargs.get('all_vehicles') or False
        is_dry_run: bool = kwargs.get('is_dry_run') or False

        if all_vehicles:
            plate_lookups = PlateLookup.get_all_by(
                count_towards_frequency=True)
        else:
            plate_lookups = PlateLookup.get_all_by(
                boot_eligible_under_rdaa_threshold=True,
                count_towards_frequency=True)

        threads = []
        num_threads = 100
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

        if not is_dry_run:
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
            open_data_response: OpenDataServiceResponse = nyc_open_data_service.look_up_vehicle(
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



def parse_args():
    parser = argparse.ArgumentParser(
        description='Job that backfills camera violations for old lookups.')

    parser.add_argument(
        '--all-vehicles',
        '-a',
        action='store_true',
        help="Backfill all valid lookups.")

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't save results")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_args()

    job = BackfillCameraViolationsJob()
    job.run(
        all_vehicles=arguments.all_vehicles,
        is_dry_run=arguments.dry_run)
