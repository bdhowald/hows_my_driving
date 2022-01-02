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
            'red_light_camera_tickets_after_previous_lookup': 0,
            'red_light_camera_tickets_before_previous_lookup': 3,
            'speed_camera_tickets_after_previous_lookup': 1,
            'speed_camera_tickets_before_previous_lookup': 12,
            'use_dvaa_thresholds': True
        },
        {
            'new_camera_violations_string': '1 new camera violation',
            'red_light_camera_tickets_after_previous_lookup': 0,
            'red_light_camera_tickets_before_previous_lookup': 5,
            'speed_camera_tickets_after_previous_lookup': 1,
            'speed_camera_tickets_before_previous_lookup': 15,
            'use_dvaa_thresholds': True
        },
        {
            'red_light_camera_tickets_after_previous_lookup': 0,
            'red_light_camera_tickets_before_previous_lookup': 5,
            'speed_camera_tickets_after_previous_lookup': 0,
            'speed_camera_tickets_before_previous_lookup': 15,
        },
        {
            'can_link_tweet': False,
            'red_light_camera_tickets_after_previous_lookup': 0,
            'red_light_camera_tickets_before_previous_lookup': 5,
            'speed_camera_tickets_after_previous_lookup': 1,
            'speed_camera_tickets_before_previous_lookup': 15,
            'use_dvaa_thresholds': True,
            'use_only_visible_tweets': True
        },
        {
            'can_link_tweet': False,
            'red_light_camera_tickets_after_previous_lookup': 0,
            'red_light_camera_tickets_before_previous_lookup': 5,
            'speed_camera_tickets_after_previous_lookup': 0,
            'speed_camera_tickets_before_previous_lookup': 15,
            'use_dvaa_thresholds': True,
            'use_only_visible_tweets': False
        },
        {
            'can_link_tweet': False,
        }
    )
    @mock.patch(
        'traffic_violations.jobs.reckless_driver_retrospective_job.OpenDataService.look_up_vehicle')
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
                                 mocked_open_data_service_look_up_vehicle,
                                 can_link_tweet=True,
                                 dry_run=False,
                                 new_camera_violations_string='10 new camera violations',
                                 red_light_camera_tickets_after_previous_lookup=3,
                                 red_light_camera_tickets_before_previous_lookup=5,
                                 speed_camera_tickets_after_previous_lookup=7,
                                 speed_camera_tickets_before_previous_lookup=15,
                                 use_dvaa_thresholds: bool = False,
                                 use_only_visible_tweets: bool = False):

        job = RecklessDriverRetrospectiveJob()

        mocked_tweet_detection_service_tweet_exists.return_value = can_link_tweet

        plate = 'ABCDEFG'
        plate_types = 'COM,PAS'
        state = 'NY'

        now = datetime.now()
        previous_message_id = random.randint(1000000000000000000, 2000000000000000000)
        previous_message_source = 'status'
        previous_num_tickets = 123
        previous_username = 'BarackObama'

        rdaa_max_streak = (red_light_camera_tickets_before_previous_lookup +
            speed_camera_tickets_before_previous_lookup)
        rdaa_min_streak_date = datetime(2018, 11, 26, 0, 0, 0).strftime('%B %-d, %Y')
        rdaa_max_streak_date = datetime(2019, 11, 22, 0, 0, 0).strftime('%B %-d, %Y')

        dvaa_red_light_camera_max_streak = red_light_camera_tickets_before_previous_lookup
        dvaa_red_light_camera_min_streak_date = datetime(2018, 9, 14, 0, 0, 0).strftime('%B %-d, %Y')
        dvaa_red_light_camera_max_streak_date = datetime(2019, 12, 17, 0, 0, 0).strftime('%B %-d, %Y')

        dvaa_speed_camera_max_streak = speed_camera_tickets_before_previous_lookup
        dvaa_speed_camera_min_streak_date = datetime(2018, 11, 26, 0, 0, 0).strftime('%B %-d, %Y')
        dvaa_speed_camera_max_streak_date = datetime(2019, 11, 22, 0, 0, 0).strftime('%B %-d, %Y')

        plate_lookup_to_return = PlateLookup(
            bus_lane_camera_violations=2,
            created_at=datetime(2020, 1, 3, 14, 37, 12),
            message_id=previous_message_id,
            message_source=previous_message_source,
            num_tickets=previous_num_tickets,
            plate=plate,
            plate_types=plate_types,
            red_light_camera_violations=red_light_camera_tickets_before_previous_lookup,
            speed_camera_violations=speed_camera_tickets_before_previous_lookup,
            state=state,
            username=previous_username)

        below_dvaa_thresholds = (dvaa_red_light_camera_max_streak < 5 and
            dvaa_speed_camera_max_streak < 15)

        new_camera_violations_exist = (red_light_camera_tickets_after_previous_lookup +
            speed_camera_tickets_after_previous_lookup) > 0

        if use_dvaa_thresholds and below_dvaa_thresholds:
            # We don't expect a lookup here.
            expect_response = False
            plate_lookups = []

        elif use_only_visible_tweets and not can_link_tweet:
            # We don't expect a lookup here.
            expect_response = False
            plate_lookups = [plate_lookup_to_return]

        elif not new_camera_violations_exist:
            # We don't expect a lookup here.
            expect_response = False
            plate_lookups = [plate_lookup_to_return]

        elif dry_run:
            # We don't expect a lookup here.
            expect_response = False
            plate_lookups = [plate_lookup_to_return]

        else:
            expect_response = True
            plate_lookups = [plate_lookup_to_return]


        mocked_plate_lookup_get_all_in.return_value = plate_lookups

        open_data_plate_lookup_before_previous_lookup = OpenDataServicePlateLookup(
            boroughs=[],
            camera_streak_data={
                'Failure to Stop at Red Light': CameraStreakData(
                    min_streak_date=dvaa_red_light_camera_min_streak_date,
                    max_streak=dvaa_red_light_camera_max_streak,
                    max_streak_date=dvaa_red_light_camera_max_streak_date),
                'Mixed': CameraStreakData(
                    min_streak_date=rdaa_min_streak_date,
                    max_streak=rdaa_max_streak,
                    max_streak_date=rdaa_max_streak_date),
                'School Zone Speed Camera Violation': CameraStreakData(
                    min_streak_date=dvaa_speed_camera_min_streak_date,
                    max_streak=dvaa_speed_camera_max_streak,
                    max_streak_date=dvaa_speed_camera_max_streak_date),
            },
            fines=FineData(),
            num_violations=150,
            plate=plate,
            plate_types=plate_types,
            state=state,
            violations=[
                {'count': red_light_camera_tickets_before_previous_lookup,
                 'title': 'Failure To Stop At Red Light'},
                {'count': speed_camera_tickets_before_previous_lookup, 'title':
                 'School Zone Speed Camera Violation'}],
            years=[])

        open_data_response_before_previous_lookup = OpenDataServiceResponse(
            data=open_data_plate_lookup_before_previous_lookup,
            success=True)


        open_data_plate_lookup_after_previous_lookup = OpenDataServicePlateLookup(
            boroughs=[],
            camera_streak_data={
                'Failure to Stop at Red Light': CameraStreakData(
                    min_streak_date=dvaa_red_light_camera_min_streak_date,
                    max_streak=dvaa_red_light_camera_max_streak,
                    max_streak_date=dvaa_red_light_camera_max_streak_date),
                'Mixed': CameraStreakData(
                    min_streak_date=rdaa_min_streak_date,
                    max_streak=rdaa_max_streak,
                    max_streak_date=rdaa_max_streak_date),
                'School Zone Speed Camera Violation': CameraStreakData(
                    min_streak_date=dvaa_speed_camera_min_streak_date,
                    max_streak=dvaa_speed_camera_max_streak,
                    max_streak_date=dvaa_speed_camera_max_streak_date),
            },
            fines=FineData(),
            num_violations=150,
            plate=plate,
            plate_types=plate_types,
            state=state,
            violations=[
                {'count': red_light_camera_tickets_after_previous_lookup,
                 'title': 'Failure To Stop At Red Light'},
                {'count': speed_camera_tickets_after_previous_lookup, 'title':
                 'School Zone Speed Camera Violation'}],
            years=[])

        open_data_response_after_previous_lookup = OpenDataServiceResponse(
            data=open_data_plate_lookup_after_previous_lookup,
            success=True)

        mocked_open_data_service_look_up_vehicle.side_effect = [
            open_data_response_before_previous_lookup, open_data_response_after_previous_lookup]

        can_link_tweet_string = (
          f' by @BarackObama: '
          f'https://twitter.com/BarackObama/status/{previous_message_id}'
          if can_link_tweet else '')
        red_light_camera_tickets_diff_string = (
            f'{red_light_camera_tickets_after_previous_lookup} | Red Light Camera Violations\n'
            if red_light_camera_tickets_after_previous_lookup else '')

        speed_camera_tickets_diff_string = (
            f'{speed_camera_tickets_after_previous_lookup} | Speed Safety Camera Violations\n'
            if speed_camera_tickets_after_previous_lookup else '')

        summary_string = (
            f'#NY_ABCDEFG was originally queried on January 3, 2020 at 09:37AM'
            f'{can_link_tweet_string}.')

        update_string = (
            f'From November 26, 2018 to November 22, 2019, this vehicle '
            f'received 20 camera violations. Over the past 12 months, this '
            f'vehicle received {new_camera_violations_string}: \n\n'
            f'{red_light_camera_tickets_diff_string}'
            f'{speed_camera_tickets_diff_string}')

        job.run(is_dry_run=dry_run, use_only_visible_tweets=use_only_visible_tweets)

        if expect_response:
            mocked_traffic_violations_tweeter().send_status.assert_called_with(
                message_parts=[summary_string, update_string],
                on_error_message=(
                    f'Error printing reckless driver update. '
                    f'Tagging @bdhowald.'))
        else:
            mocked_traffic_violations_tweeter().send_status.assert_not_called()
