import argparse
import logging
import pytz

from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
from typing import List, Optional, Tuple

from traffic_violations.constants import L10N, lookup_sources

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse

from traffic_violations.services.apis.open_data_service import OpenDataService
from traffic_violations.services.apis.tweet_detection_service import \
    TweetDetectionService
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

        tweet_detection_service = TweetDetectionService()
        tweeter = TrafficViolationsTweeter()

        eastern = pytz.timezone('US/Eastern')
        utc = pytz.timezone('UTC')

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

        if not lookups_to_update:
            LOG.debug(f'No vehicles for which to perform retrospective job '
                      f'between {top_of_the_hour_last_year} and '
                      f'and {top_of_the_next_hour_last_year}.')

        for previous_lookup in lookups_to_update:

            LOG.debug(f'Performing retrospective job for '
                      f'{L10N.VEHICLE_HASHTAG.format(previous_lookup.state, previous_lookup.plate)} ')

            plate_query: PlateQuery = PlateQuery(created_at=now,
                                                 message_source=previous_lookup.message_source,
                                                 plate=previous_lookup.plate,
                                                 plate_types=previous_lookup.plate_types,
                                                 state=previous_lookup.state)

            nyc_open_data_service: OpenDataService = OpenDataService()
            open_data_response: OpenDataServiceResponse = nyc_open_data_service.lookup_vehicle(
                plate_query=plate_query)

            open_data_plate_lookup: OpenDataServicePlateLookup = open_data_response.data

            camera_streak_data: CameraStreakData = open_data_plate_lookup.camera_streak_data

            new_bus_lane_camera_violations: Optional[int] = None
            new_speed_camera_violations: Optional[int] = None
            new_red_light_camera_violations: Optional[int] = None

            for violation_type_summary in open_data_plate_lookup.violations:
                if violation_type_summary['title'] in self.CAMERA_VIOLATIONS:
                    violation_count = violation_type_summary['count']

                    if violation_type_summary['title'] == 'Bus Lane Violation':
                        new_bus_lane_camera_violations = (violation_count -
                                                          previous_lookup.bus_lane_camera_violations)
                    if violation_type_summary['title'] == 'Failure To Stop At Red Light':
                        new_red_light_camera_violations = (violation_count -
                                                       previous_lookup.red_light_camera_violations)
                    elif violation_type_summary['title'] == 'School Zone Speed Camera Violation':
                        new_speed_camera_violations = (violation_count -
                                                           previous_lookup.speed_camera_violations)

            if new_bus_lane_camera_violations is None:
                new_bus_lane_camera_violations = previous_lookup.bus_lane_camera_violations

            if new_speed_camera_violations is None:
                new_speed_camera_violations = previous_lookup.red_light_camera_violations

            if new_red_light_camera_violations is None:
                new_red_light_camera_violations = previous_lookup.speed_camera_violations

            new_boot_eligible_violations = (new_red_light_camera_violations +
                                            new_speed_camera_violations)

            if new_boot_eligible_violations > 0:
                vehicle_hashtag = L10N.VEHICLE_HASHTAG.format(
                    previous_lookup.state, previous_lookup.plate)
                previous_lookup_created_at = utc.localize(
                    previous_lookup.created_at)
                previous_lookup_date = previous_lookup_created_at.astimezone(eastern).strftime(
                    L10N.REPEAT_LOOKUP_DATE_FORMAT)
                previous_lookup_time = previous_lookup_created_at.astimezone(eastern).strftime(
                    L10N.REPEAT_LOOKUP_TIME_FORMAT)

                red_light_camera_violations_string = (
                    f'{new_red_light_camera_violations} | Red Light Camera Violations\n'
                    if new_red_light_camera_violations > 0 else '')

                speed_camera_violations_string = (
                    f'{new_speed_camera_violations} | Speed Safety Camera Violations\n'
                    if new_speed_camera_violations > 0 else '')

                reckless_driver_summary_string = (
                    f'{vehicle_hashtag} was originally '
                    f'queried on {previous_lookup_date} '
                    f'at {previous_lookup_time}')

                # assume we can't link
                can_link_tweet = False

                # Where did this come from?
                if previous_lookup.message_source == lookup_sources.LookupSource.STATUS.value:
                    # Determine if tweet is still visible:
                    if tweet_detection_service.tweet_exists(id=previous_lookup.message_id,
                                                            username=previous_lookup.username):
                        can_link_tweet = True

                if can_link_tweet:
                    reckless_driver_summary_string += L10N.PREVIOUS_LOOKUP_STATUS_STRING.format(
                        previous_lookup.username,
                        previous_lookup.username,
                        previous_lookup.message_id)
                else:
                    reckless_driver_summary_string += '.'

                reckless_driver_update_string = (
                    f'From {camera_streak_data.min_streak_date} to '
                    f'{camera_streak_data.max_streak_date}, this vehicle '
                    f'received {camera_streak_data.max_streak} camera '
                    f'violations. Over the past 12 months, this vehicle '
                    f'received {new_boot_eligible_violations} new camera violation'
                    f"{'' if new_boot_eligible_violations == 1 else 's'}: \n\n"
                    f'{red_light_camera_violations_string}'
                    f'{speed_camera_violations_string}')

                advocacy_string = (
                    f'Please contact your council members and ask them to support '
                    f'@bradlander\'s Reckless Driver Accountability Act.')

                messages: List[str] = [
                    reckless_driver_summary_string,
                    reckless_driver_update_string,
                    advocacy_string]

                if not is_dry_run:
                    success: bool = tweeter.send_status(
                        message_parts=messages,
                        on_error_message=(
                            f'Error printing reckless driver update. '
                            f'Tagging @bdhowald.'))

                    if success:
                        LOG.debug('Reckless driver retrospective job '
                                  'ran successfully.')
                else:
                    print(f'{reckless_driver_update_string}')
                    print('\n\n')
                    print(f'{advocacy_string}')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Tweet out an update for a reckless driver.')

    parser.add_argument(
        '--dry-run',
        '-d',
        action='store_true',
        help="Don't tweet results")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_args()

    job = RecklessDriverRetrospectiveJob()
    job.run(is_dry_run=arguments.dry_run)
