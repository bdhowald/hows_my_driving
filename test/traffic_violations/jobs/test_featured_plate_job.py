import ddt
import mock
import random
import unittest

from unittest.mock import call, MagicMock

from traffic_violations.jobs.featured_plate_job import FeaturedPlateJob

from traffic_violations.models.special_purpose.repeat_camera_offender import (
    RepeatCameraOffender)


@ddt.ddt
class TestFeaturedPlateJob(unittest.TestCase):

    @ddt.data(
        {
            'dry_run': True
        },
        {
            'dry_run': False
        }
    )
    @mock.patch(
        'traffic_violations.jobs.featured_plate_job.RepeatCameraOffender.query')
    @mock.patch(
        'traffic_violations.jobs.featured_plate_job.TrafficViolationsTweeter')
    @ddt.unpack
    def test_print_featured_plate(self,
                                  mocked_traffic_violations_tweeter,
                                  mocked_repeat_camera_offender_query,
                                  dry_run):

        job = FeaturedPlateJob()

        plate = 'ABC1234'
        state = 'NY'
        total_camera_violations = random.randint(1, 100)
        red_light_camera_violations = total_camera_violations - \
            random.randint(1, total_camera_violations)
        speed_camera_violations = total_camera_violations - red_light_camera_violations
        times_featured = 0

        index = random.randint(1, 3000)
        tied_with = random.randint(0, 3)
        min_id = random.randint(
            max(index - (tied_with if tied_with == 0 else (tied_with - 1)), 1), index)
        nth_place = min_id + tied_with - 1

        repeat_camera_offender: RepeatCameraOffender = RepeatCameraOffender(
            plate_id=plate,
            state=state,
            total_camera_violations=total_camera_violations,
            red_light_camera_violations=red_light_camera_violations,
            speed_camera_violations=speed_camera_violations,
            times_featured=0)

        mocked_repeat_camera_offender_query.filter(
        ).order_by().first.return_value = repeat_camera_offender

        mocked_repeat_camera_offender_query.filter(
        ).count.return_value = tied_with

        mocked_repeat_camera_offender_query.session.query().filter(
        ).one.return_value = [min_id]

        vehicle_hashtag = f'#{state}_{plate}'
        suffix = 'st' if (nth_place % 10 == 1 and nth_place % 100 != 11) else ('nd' if (
            nth_place % 10 == 2 and nth_place % 100 != 12) else ('rd' if (nth_place % 10 == 3 and nth_place % 100 != 13) else 'th'))
        worst_substring = f'{nth_place}{suffix}-worst' if nth_place > 1 else "worst"
        tied_substring = ' tied for' if tied_with != 1 else ''

        max_count_length = len(
            str(max(red_light_camera_violations, speed_camera_violations)))
        spaces_needed = (max_count_length * 2) + 1

        featured_string = (
            f'Featured #RepeatCameraOffender:\n\n'
            f'{vehicle_hashtag} has received {total_camera_violations} camera violations:\n\n'
            f'{str(red_light_camera_violations).ljust(spaces_needed - len(str(red_light_camera_violations)))} | Red Light Camera Violations\n'
            f'{str(speed_camera_violations).ljust(spaces_needed - len(str(speed_camera_violations)))} | Speed Safety Camera Violations\n\n'
            f'This makes {vehicle_hashtag}{tied_substring} the {worst_substring} camera violator in New York City.')

        job.run(is_dry_run=dry_run)

        if not dry_run:
            mocked_traffic_violations_tweeter().send_status.assert_called_with(
                message_parts=[featured_string],
                on_error_message=(
                    f'Error printing featured plate. '
                    f'Tagging @bdhowald.'))
