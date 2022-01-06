import argparse
import logging

from datetime import datetime, time, timedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
from typing import Optional

from traffic_violations.constants import L10N

from traffic_violations.jobs.base_job import BaseJob

from traffic_violations.models.special_purpose.repeat_camera_offender import (
    RepeatCameraOffender)

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter

from traffic_violations.utils import string_utils, twitter_utils

LOG = logging.getLogger(__name__)


class FeaturedPlateJob(BaseJob):
    """ Tweet out a particular reckless driver. """

    def perform(self, *args, **kwargs):
        is_dry_run: bool = kwargs.get('is_dry_run') or False

        tweeter = TrafficViolationsTweeter()

        repeat_camera_offender: Optional[RepeatCameraOffender] = RepeatCameraOffender.query.filter(
            and_(RepeatCameraOffender.times_featured == 0,
                 RepeatCameraOffender.total_camera_violations >= 25)).order_by(
            func.random()).first()

        if repeat_camera_offender:

            # get the number of vehicles that have the same number
            # of violations
            tied_with = RepeatCameraOffender.query.filter(
                RepeatCameraOffender.total_camera_violations ==
                repeat_camera_offender.total_camera_violations).count()

            # since the vehicles are in descending order of violations,
            # we find the record that has the same number of violations
            # and the lowest id...
            min_id = RepeatCameraOffender.query.session.query(
                func.min(RepeatCameraOffender.id)
            ).filter(
                RepeatCameraOffender.total_camera_violations ==
                repeat_camera_offender.total_camera_violations
            ).one()[0]

            # nth place is simply the sum of the two values minus one.
            nth_place = tied_with + min_id - 1

            red_light_camera_violations = \
                repeat_camera_offender.red_light_camera_violations
            speed_camera_violations = \
                repeat_camera_offender.speed_camera_violations

            vehicle_hashtag: str = L10N.VEHICLE_HASHTAG.format(
                repeat_camera_offender.state,
                repeat_camera_offender.plate_id)

            # one of 'st', 'nd', 'rd', 'th'
            suffix: str = string_utils.determine_ordinal_indicator(nth_place)

            # how bad is this vehicle?
            worst_substring: str = (
                f'{nth_place}{suffix}-worst' if nth_place > 1 else 'worst')

            tied_substring: str = ' tied for' if tied_with != 1 else ''

            spaces_needed: int = twitter_utils.padding_spaces_needed(
                red_light_camera_violations, speed_camera_violations)

            featured_string = L10N.REPEAT_CAMERA_OFFENDER_STRING.format(
                vehicle_hashtag,
                repeat_camera_offender.total_camera_violations,
                str(red_light_camera_violations).ljust(
                    spaces_needed - len(str(red_light_camera_violations))),
                str(speed_camera_violations).ljust(
                    spaces_needed - len(str(speed_camera_violations))),
                vehicle_hashtag,
                tied_substring,
                worst_substring)

            messages: list[str] = [featured_string]

            if not is_dry_run:
                success: bool = tweeter.send_status(
                    message_parts=messages,
                    on_error_message=(
                        f'Error printing featured plate. '
                        f'Tagging @bdhowald.'))

                if success:
                    repeat_camera_offender.times_featured += 1
                    RepeatCameraOffender.query.session.commit()

                    LOG.debug('Featured job plate ran successfully.')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Present a featured repeat reckless driver.')

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Don't tweet results")

    return parser.parse_args()

if __name__ == '__main__':
    arguments = parse_args()

    job = FeaturedPlateJob()
    job.run(is_dry_run=arguments.dry_run)