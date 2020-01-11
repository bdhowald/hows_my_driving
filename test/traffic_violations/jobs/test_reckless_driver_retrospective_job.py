import ddt
import mock
import pytz
import random
import unittest

from datetime import datetime
from unittest.mock import call, MagicMock

from traffic_violations.jobs.reckless_driver_retrospective_job \
    import RecklessDriverRetrospectiveJob

from traffic_violations.models.camera_streak_data import \
    CameraStreakData
from traffic_violations.models.fine_data import FineData
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse


@ddt.ddt
class TestRecklessDriverRetrospectiveJob(unittest.TestCase):

    @ddt.data(
        {
            'dry_run': True
        },
        {
            'dry_run': False
        },
        {
            'new_camera_violations_string': '1 new camera violation',
            'new_red_light_camera_tickets': 8,
            'new_speed_camera_tickets': 13,
            'old_red_light_camera_tickets': 8,
            'old_speed_camera_tickets': 12,
        },
        {
            'new_red_light_camera_tickets': 8,
            'new_speed_camera_tickets': 12,
            'old_red_light_camera_tickets': 8,
            'old_speed_camera_tickets': 12,
        },
        {
            'can_link_tweet': True,
        }
    )
    @mock.patch(
        'traffic_violations.jobs.reckless_driver_retrospective_job.OpenDataService.lookup_vehicle')
    @mock.patch(
        'traffic_violations.jobs.reckless_driver_retrospective_job.PlateLookup.query')
    @mock.patch(
        'traffic_violations.jobs.reckless_driver_retrospective_job.PlateLookup.get_all_in')
    @mock.patch(
        'traffic_violations.jobs.reckless_driver_retrospective_job.TrafficViolationsTweeter')
    @mock.patch(
        'traffic_violations.jobs.reckless_driver_retrospective_job.TweetDetectionService.tweet_exists')
    @ddt.unpack
    def test_print_reckless_driver_retrospective(self,
                                 mocked_tweet_detection_service_tweet_exists,
                                 mocked_traffic_violations_tweeter,
                                 mocked_plate_lookup_get_all_in,
                                 mocked_plate_lookup_query,
                                 mocked_open_data_service_lookup_vehicle,
                                 can_link_tweet=False,
                                 dry_run=False,
                                 new_camera_violations_string='10 new camera violations',
                                 new_red_light_camera_tickets=11,
                                 new_speed_camera_tickets=19,
                                 old_red_light_camera_tickets=8,
                                 old_speed_camera_tickets=12):

        job = RecklessDriverRetrospectiveJob()

        # mocked_plate_lookup_query.session.query(
        # ).filter().group_by().all.return_value = [1,2,3]

        mocked_tweet_detection_service_tweet_exists.return_value = can_link_tweet

        plate = 'ABCDEFG'
        plate_types = 'COM,PAS'
        state = 'NY'

        now = datetime.now()
        previous_message_id = random.randint(1000000000000000000, 2000000000000000000)
        previous_message_source = 'status'
        previous_num_tickets = 123
        previous_username = 'BarackObama'

        max_streak = 20
        min_streak_date = datetime(2018, 11, 26, 0, 0, 0).strftime('%B %-d, %Y')
        max_streak_date = datetime(2019, 11, 22, 0, 0, 0).strftime('%B %-d, %Y')

        plate_lookups = [
            PlateLookup(
                bus_lane_camera_violations=2,
                created_at=datetime(2020, 1, 3, 14, 37, 12),
                message_id=previous_message_id,
                message_source=previous_message_source,
                num_tickets=previous_num_tickets,
                plate=plate,
                plate_types=plate_types,
                red_light_camera_violations=old_red_light_camera_tickets,
                speed_camera_violations=old_speed_camera_tickets,
                state=state,
                username=previous_username)]

        mocked_plate_lookup_get_all_in.return_value = plate_lookups

        open_data_plate_lookup = OpenDataServicePlateLookup(
            boroughs=[],
            camera_streak_data=CameraStreakData(
                min_streak_date=min_streak_date,
                max_streak=max_streak,
                max_streak_date=max_streak_date),
            fines=FineData(),
            num_violations=150,
            plate=plate,
            plate_types=plate_types,
            state=state,
            violations=[
                {'count': new_red_light_camera_tickets,
                 'title': 'Failure To Stop At Red Light'},
                {'count': new_speed_camera_tickets, 'title':
                 'School Zone Speed Camera Violation'}],
            years=[])

        open_data_response = OpenDataServiceResponse(
            data=open_data_plate_lookup,
            success=True)

        mocked_open_data_service_lookup_vehicle.return_value = open_data_response

        can_link_tweet_string = (
          f' by @BarackObama: '
          f'https://twitter.com/BarackObama/status/{previous_message_id}'
          if can_link_tweet else '')

        red_light_camera_tickets_diff = (new_red_light_camera_tickets -
            old_red_light_camera_tickets)
        speed_camera_tickets_diff = (new_speed_camera_tickets -
            old_speed_camera_tickets)

        red_light_camera_tickets_diff_string = (
            f'{red_light_camera_tickets_diff} | Red Light Camera Violations\n'
            if red_light_camera_tickets_diff else '')

        speed_camera_tickets_diff_string = (
            f'{speed_camera_tickets_diff} | Speed Safety Camera Violations\n'
            if speed_camera_tickets_diff else '')

        summary_string = (
            f'#NY_ABCDEFG was originally queried on January 3, 2020 at 09:37AM'
            f'{can_link_tweet_string}.')

        update_string = (
            f'From November 26, 2018 to November 22, 2019, this vehicle '
            f'received 20 camera violations. Over the past 12 months, this '
            f'vehicle received {new_camera_violations_string}: \n\n'
            f'{red_light_camera_tickets_diff_string}'
            f'{speed_camera_tickets_diff_string}')

        reckless_string = (
            f"Please contact your council members and ask them to support "
            f"@bradlander's Reckless Driver Accountability Act.")

        job_should_be_run = ((new_red_light_camera_tickets -
            old_red_light_camera_tickets) + (new_speed_camera_tickets -
            old_speed_camera_tickets)) > 0

        job.run(is_dry_run=dry_run)

        if not dry_run and job_should_be_run:
            mocked_traffic_violations_tweeter().send_status.assert_called_with(
                message_parts=[summary_string, update_string, reckless_string],
                on_error_message=(
                    f'Error printing reckless driver update. '
                    f'Tagging @bdhowald.'))
