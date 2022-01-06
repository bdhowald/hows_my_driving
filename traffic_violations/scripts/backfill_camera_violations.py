import argparse
import math

import os
import logging
import threading

from traffic_violations.constants import thresholds

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.campaign import Campaign
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
                boot_eligible_under_dvaa_threshold=False,
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

    def update_lookups(self, lookups: list[PlateLookup]):

        for previous_lookup in lookups:
            plate_query: PlateQuery = PlateQuery(created_at=previous_lookup.created_at,
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

            if not previous_lookup.boot_eligible_under_dvaa_threshold or True:
                camera_streak_data = open_data_plate_lookup.camera_streak_data

                red_light_camera_streak_data = camera_streak_data.get('Failure to Stop at Red Light')
                speed_camera_streak_data = camera_streak_data.get('School Zone Speed Camera Violation')

                eligible_to_be_booted_for_red_light_violations_under_dvaa = (
                    red_light_camera_streak_data is not None and red_light_camera_streak_data.max_streak >=
                        thresholds.DANGEROUS_VEHICLE_ABATEMENT_ACT_RED_LIGHT_CAMERA_THRESHOLD)

                eligible_to_be_booted_for_speed_camera_violations_under_dvaa = (
                    speed_camera_streak_data is not None and speed_camera_streak_data.max_streak >=
                        thresholds.DANGEROUS_VEHICLE_ABATEMENT_ACT_SCHOOL_ZONE_SPEED_CAMERA_THRESHOLD)

                previous_lookup.boot_eligible_under_dvaa_threshold = (
                  eligible_to_be_booted_for_red_light_violations_under_dvaa or
                      eligible_to_be_booted_for_speed_camera_violations_under_dvaa)

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
