import argparse
import logging
import math
import random
import pytz

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from traffic_violations.constants import L10N
from traffic_violations.constants.lookup_sources import LookupSource

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse
from traffic_violations.models.special_purpose.covid_19_camera_offender import (
    Covid19CameraOffender)

from traffic_violations.services.apis.open_data_service import OpenDataService

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter


LOG = logging.getLogger(__name__)


class Covid19CameraOffenderJob(BaseJob):
    """ Tweet out a reckless driver during COVID-19. """

    SECONDS_IN_A_DAY = 86400

    RED_LIGHT_CAMERA_VIOLATION_DESCRIPTION = 'Failure To Stop At Red Light'
    SPEED_CAMERA_VIOLATION_DESCRIPTION = 'School Zone Speed Camera Violation'

    CAMERA_VIOLATIONS = [RED_LIGHT_CAMERA_VIOLATION_DESCRIPTION,
                         SPEED_CAMERA_VIOLATION_DESCRIPTION]

    COVID_19_OPEN_STREETS_TWEETS: List[str] = [
        'https://twitter.com/JSadikKhan/status/1248675348268621826',
        'https://twitter.com/StreetsblogDen/status/1247535862843236355',
        'https://twitter.com/RegineGuenther/status/1247870521234075652?s=20',
        'https://twitter.com/BrentToderian/status/1248076792683888640',
        'https://twitter.com/ashk4n/status/1248442190017122304',
        'https://twitter.com/MayorHancock/status/1246193113611280386?s=20',
        'https://twitter.com/maxnesterak/status/1243300381301641217',
        'https://twitter.com/MikeLydon/status/1248400819642220545',
        'https://twitter.com/DianaUrge/status/1248402038649544706',
    ]

    def perform(self, *args, **kwargs):
        is_dry_run: bool = kwargs.get('is_dry_run') or False

        start_date = datetime(2020, 3, 10, 0, 0, 0, 0)
        end_date = datetime(2020, 5, 11, 23, 59, 59, 999999)

        days_in_period = math.ceil((end_date - start_date).total_seconds() / self.SECONDS_IN_A_DAY)
        days_in_year = 366.0
        periods_in_year = days_in_year / days_in_period

        tweeter = TrafficViolationsTweeter()

        nyc_open_data_service: OpenDataService = OpenDataService()
        covid_19_camera_offender_raw_data: List[Dict[str, str]] = nyc_open_data_service.lookup_covid_19_camera_violations()

        for vehicle in covid_19_camera_offender_raw_data:
            plate = vehicle['plate']
            state = vehicle['state']

            offender: Optional[Covid19CameraOffender] = Covid19CameraOffender.get_by(
                plate_id=plate,
                state=state,
                count_as_queried=True)

            if offender:
                LOG.debug(f'COVID-19 speeder - {L10N.VEHICLE_HASHTAG.format(state, plate)} '
                      f"with {vehicle['count']} camera violations has been seen before.")
                continue

            LOG.debug(f'COVID-19 speeder - {L10N.VEHICLE_HASHTAG.format(state, plate)} '
                      f"with {vehicle['count']} camera violations has not been seen before.")

            plate_query: PlateQuery = PlateQuery(created_at=datetime.now(),
                                                    message_source=LookupSource.API,
                                                    plate=plate,
                                                    plate_types=None,
                                                    state=state)

            response: OpenDataServiceResponse = nyc_open_data_service.look_up_vehicle(
                plate_query=plate_query,
                since=start_date,
                until=end_date)

            plate_lookup: OpenDataServicePlateLookup = response.data

            red_light_camera_violations = 0
            speed_camera_violations = 0

            for violation_type_summary in plate_lookup.violations:
                if violation_type_summary['title'] in self.CAMERA_VIOLATIONS:
                    violation_count = violation_type_summary['count']

                    if violation_type_summary['title'] == self.RED_LIGHT_CAMERA_VIOLATION_DESCRIPTION:
                        red_light_camera_violations = violation_count
                    if violation_type_summary['title'] == self.SPEED_CAMERA_VIOLATION_DESCRIPTION:
                        speed_camera_violations = violation_count

            total_camera_violations = (red_light_camera_violations +
                speed_camera_violations)

            vehicle_hashtag = L10N.VEHICLE_HASHTAG.format(
                state, plate)

            red_light_camera_violations_string = (
                f'{red_light_camera_violations} | Red Light Camera Violations\n'
                if red_light_camera_violations > 0 else '')

            speed_camera_violations_string = (
                f'{speed_camera_violations} | Speed Safety Camera Violations\n'
                if speed_camera_violations > 0 else '')

            covid_19_reckless_driver_string = (
                f"From {start_date.strftime('%B %-d, %Y')} to "
                f"{end_date.strftime('%B %-d, %Y')}, {vehicle_hashtag} "
                f'received {total_camera_violations} camera '
                f'violations:\n\n'
                f'{red_light_camera_violations_string}'
                f'{speed_camera_violations_string}')

            dval_string = (
                'At this rate, this vehicle will receive '
                f'{round(periods_in_year * total_camera_violations)} '
                'speed safety camera violations over '
                'a year, qualifying it for towing or booting under '
                '@bradlander\'s Dangerous Vehicle Abatement Law and '
                'requiring its driver to take a course on the consequences '
                'of reckless driving.')

            speeding_string = (
                'With such little traffic, many drivers are speeding '
                'regularly, putting New Yorkers at increased risk of '
                'ending up in a hospital at a time our hospitals are '
                'stretched to their limits. It\'s also hard to practice '
                'social distancing when walking on our narrow sidewalks.')

            open_streets_string = (
                'Other cities are eating our lunch, @NYCMayor:\n\n'
                f'{random.choice(self.COVID_19_OPEN_STREETS_TWEETS)}')

            # messages: List[str] = [
            #     covid_19_reckless_driver_string,
            #     dval_string,
            #     [speeding_string, open_streets_string]]

            messages: List[str] = [covid_19_reckless_driver_string, dval_string]

            if not is_dry_run:
                success: bool = tweeter.send_status(
                    message_parts=messages,
                    on_error_message=(
                        f'Error printing COVID-19 reckless driver update. '
                        f'Tagging @bdhowald.'))

                if success:
                    offender = Covid19CameraOffender(plate_id=plate,
                                                        state=state,
                                                        red_light_camera_violations=red_light_camera_violations,
                                                        speed_camera_violations=speed_camera_violations)

                    Covid19CameraOffender.query.session.add(offender)
                    try:
                        Covid19CameraOffender.query.session.commit()

                        LOG.debug('COVID-19 Reckless driver retrospective job '
                            'ran successfully.')
                    except:
                        tweeter.send_status(message_parts=[(
                            f'Error printing COVID-19 reckless driver update. '
                            f'Tagging @bdhowald.')])

                    # Only do one at a time.
                    break

            else:
                print(covid_19_reckless_driver_string)
                print(dval_string)
                # print(speeding_string)
                # print(open_streets_string)
                break


def parse_args():
    parser = argparse.ArgumentParser(
        description='Present a featured COVID-19 repeat reckless driver.')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't tweet results")

    return parser.parse_args()

if __name__ == '__main__':
    arguments = parse_args()

    job = Covid19CameraOffenderJob()
    job.run(is_dry_run=arguments.dry_run)
