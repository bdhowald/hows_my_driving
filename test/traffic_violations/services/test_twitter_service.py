import mock
import os
import pytz
import random
import statistics
import unittest

from datetime import datetime, timezone, time, timedelta

from unittest.mock import call, MagicMock

from traffic_violations.models.twitter_event import TwitterEvent
from traffic_violations.models.repeat_camera_offender import \
    RepeatCameraOffender
from traffic_violations.models.repeat_camera_offender import \
    RepeatCameraOffender

from traffic_violations.reply_argument_builder import \
    AccountActivityAPIDirectMessage, AccountActivityAPIStatus

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter


def inc(part, in_reply_to_status_id):
    int_mock = MagicMock(name='api')
    int_mock.id = (in_reply_to_status_id + 1)
    return int_mock


class TestTrafficViolationsTweeter(unittest.TestCase):

    def setUp(self):
        self.tweeter = TrafficViolationsTweeter()

    def test_find_messages_to_respond_to(self):
        twitter_events_mock = MagicMock(
            name='find_and_respond_to_twitter_events')
        self.tweeter._find_and_respond_to_twitter_events = twitter_events_mock

        self.tweeter._find_messages_to_respond_to()

        twitter_events_mock.assert_called_with()

    @mock.patch(
        'traffic_violations.services.twitter_service.TwitterEvent')
    def test_find_and_respond_to_twitter_events(self, twitter_event_mock):
        db_id = 1
        random_id = random.randint(10000000000000000000, 20000000000000000000)
        event_type = 'status'
        user_handle = 'bdhowald'
        user_id = random.randint(1000000000, 2000000000)
        event_text = '@HowsMyDrivingNY abc1234:ny'
        timestamp = random.randint(1500000000000, 1700000000000)
        in_reply_to_message_id = random.randint(
            10000000000000000000, 20000000000000000000)
        location = 'Queens, NY'
        responded_to = 0
        user_mentions = '@HowsMyDrivingNY'

        twitter_event = TwitterEvent(
            id=db_id,
            created_at=timestamp,
            event_id=random_id,
            event_text=event_text,
            event_type=event_type,
            in_reply_to_message_id=in_reply_to_message_id,
            location=location,
            responded_to=responded_to,
            user_handle=user_handle,
            user_id=user_id,
            user_mentions=user_mentions)

        lookup_request = AccountActivityAPIDirectMessage(
            message=TwitterEvent(
                id=1,
                created_at=timestamp,
                event_id=random_id,
                event_text='@howsmydrivingny ny:hme6483',
                event_type='direct_message',
                user_handle=user_handle,
                user_id=30139847),
            message_source='api')

        twitter_event_mock.get_all_by.return_value = [twitter_event]

        initiate_reply_mock = MagicMock(name='initiate_reply')
        self.tweeter.aggregator.initiate_reply = initiate_reply_mock

        build_reply_data_mock = MagicMock(name='build_reply_data')
        build_reply_data_mock.return_value = lookup_request
        self.tweeter.reply_argument_builder.build_reply_data = build_reply_data_mock

        self.tweeter._find_and_respond_to_twitter_events()

        self.tweeter.aggregator.initiate_reply.assert_called_with(
            lookup_request)

    def test_is_production(self):
        self.assertEqual(self.tweeter._is_production(),
                         (os.environ.get('ENV') == 'production'))

    @mock.patch(
        'traffic_violations.services.twitter_service.PlateLookup.query')
    def test_print_daily_summary(self, mocked_plate_lookup_query):
        utc = pytz.timezone('UTC')
        eastern = pytz.timezone('US/Eastern')

        today = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(days=1)).astimezone(utc)

        message_id = random.randint(10000000000000000000, 20000000000000000000)

        plate_lookups = []
        for _ in range(random.randint(5, 20)):
            lookup = MagicMock()
            lookup.num_tickets = random.randint(1, 200)
            lookup.boot_eligible = random.random() >= 0.5
            plate_lookups.append(lookup)

        num_lookups = len(plate_lookups)
        ticket_counts = [
            plate_lookup.num_tickets for plate_lookup in plate_lookups]
        total_tickets = sum(ticket_counts)
        num_empty_lookups = len([
            lookup for lookup in plate_lookups if lookup.num_tickets == 0])
        num_reckless_drivers = len([
            lookup for lookup in plate_lookups if lookup.boot_eligible == True])

        mocked_plate_lookup_query.join().all.return_value = plate_lookups

        total_reckless_drivers = random.randint(10, 10000)

        mocked_plate_lookup_query.session.query(
        ).distinct().filter().count.return_value = total_reckless_drivers

        median = statistics.median(
            plate_lookup.num_tickets for plate_lookup in plate_lookups)

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        message_mock = MagicMock(name='message')
        message_mock.id = message_id

        update_status_mock = MagicMock(name='update_status')
        update_status_mock.return_value = message_mock

        api_mock = MagicMock(name='api')
        api_mock.update_status = update_status_mock

        self.tweeter._is_production = is_production_mock
        self.tweeter.api = api_mock

        lookup_str = (
            f"On {midnight_yesterday.strftime('%A, %B %-d, %Y')}, users requested {num_lookups} lookup{'' if num_lookups == 1 else 's'}. "
            f"{'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have'} received {'{:,}'.format(total_tickets)} ticket{'' if total_tickets == 1 else 's'} "
            f"for an average of {round(total_tickets / num_lookups, 2)} ticket{'' if total_tickets == 1 else 's'} "
            f"and a median of {median} ticket{'' if median == 1 else 's'} per vehicle. "
            f"{num_empty_lookups} lookup{'' if num_empty_lookups == 1 else 's'} returned no tickets.")

        reckless_str = (
            f"{num_reckless_drivers} {'vehicle was' if num_reckless_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under @bradlander's proposed legislation "
            f"({total_reckless_drivers} such lookups since June 6, 2018).")

        self.tweeter._print_daily_summary()

        calls = [call(lookup_str), call(
            reckless_str, in_reply_to_status_id=message_id)]

        update_status_mock.assert_has_calls(calls)

    @mock.patch(
        'traffic_violations.services.twitter_service.RepeatCameraOffender.query')
    def test_print_featured_plate(self, mocked_repeat_camera_offender_query):
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

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        message_mock = MagicMock(name='message')
        # message_mock.id = message_id

        update_status_mock = MagicMock(name='update_status')
        update_status_mock.return_value = message_mock

        api_mock = MagicMock(name='api')
        api_mock.update_status = update_status_mock

        self.tweeter._is_production = is_production_mock
        self.tweeter.api = api_mock

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

        self.tweeter._print_featured_plate()

        calls = [call(featured_string)]

        update_status_mock.assert_has_calls(calls)

    def test_process_response_direct_message(self):
        """ Test direct message and new format """

        username = 'bdhowald'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        lookup_request = AccountActivityAPIDirectMessage(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text='@howsmydrivingny ny:hme6483',
                event_type='direct_message',
                user_handle=username,
                user_id=30139847),
            message_source='direct_message')

        combined_message = "@bdhowald #NY_HME6483 has been queried 1 time.\n\nTotal parking and camera violation tickets: 15\n\n4 | No Standing - Day/Time Limits\n3 | No Parking - Street Cleaning\n1 | Failure To Display Meter Receipt\n1 | No Violation Description Available\n1 | Bus Lane Violation\n\n@bdhowald Parking and camera violation tickets for #NY_HME6483, cont'd:\n\n1 | Failure To Stop At Red Light\n1 | No Standing - Commercial Meter Zone\n1 | Expired Meter\n1 | Double Parking\n1 | No Angle Parking\n\n@bdhowald Violations by year for #NY_HME6483:\n\n10 | 2017\n15 | 2018\n\n@bdhowald Known fines for #NY_HME6483:\n\n$200.00 | Fined\n$125.00 | Outstanding\n$75.00   | Paid\n"

        reply_event_args = {
            'error_on_lookup': False,
            'request_object': lookup_request,
            'response_parts': [[combined_message]],
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        send_direct_message_mock = MagicMock('send_direct_message_mock')

        api_mock = MagicMock(name='api')
        api_mock.send_direct_message_new = send_direct_message_mock

        self.tweeter._is_production = is_production_mock
        self.tweeter.api = api_mock

        self.tweeter._process_response(reply_event_args)

        send_direct_message_mock.assert_called_with({
            'event': {
                'type': 'message_create',
                'message_create': {
                    'target': {
                        'recipient_id': 30139847
                    },
                    'message_data': {
                        'text': combined_message
                    }
                }
            }
        })

    @mock.patch(
        'traffic_violations.services.twitter_service.TrafficViolationsTweeter._recursively_process_status_updates')
    def test_process_response_status_legacy_format(self,
                                                   recursively_process_status_updates_mock):
        """ Test status and old format """

        username = 'BarackObama'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        response_parts = [['@BarackObama #PA_GLF7467 has been queried 1 time.\n\nTotal parking and camera violation tickets: 49\n\n17 | No Parking - Street Cleaning\n6   | Expired Meter\n5   | No Violation Description Available\n3   | Fire Hydrant\n3   | No Parking - Day/Time Limits\n', "@BarackObama Parking and camera violation tickets for #PA_GLF7467, cont'd:\n\n3   | Failure To Display Meter Receipt\n3   | School Zone Speed Camera Violation\n2   | No Parking - Except Authorized Vehicles\n2   | Bus Lane Violation\n1   | Failure To Stop At Red Light\n",
                           "@BarackObama Parking and camera violation tickets for #PA_GLF7467, cont'd:\n\n1   | No Standing - Day/Time Limits\n1   | No Standing - Except Authorized Vehicle\n1   | Obstructing Traffic Or Intersection\n1   | Double Parking\n", '@BarackObama Known fines for #PA_GLF7467:\n\n$1,000.00 | Fined\n$225.00     | Outstanding\n$775.00     | Paid\n']]

        lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text='@howsmydrivingny plate:glf7467 state:pa',
                event_type='status',
                user_handle=username,
                user_id=30139847),
            message_source='status')

        reply_event_args = {
            'error_on_lookup': False,
            'request_object': lookup_request,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        create_favorite_mock = MagicMock(name='is_production')
        create_favorite_mock.return_value = True

        api_mock = MagicMock(name='api')
        api_mock.create_favorite = create_favorite_mock

        self.tweeter._is_production = is_production_mock
        self.tweeter.api = api_mock

        reply_event_args['username'] = username

        self.tweeter._process_response(reply_event_args)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts, message_id, username)

    @mock.patch(
        'traffic_violations.services.twitter_service.TrafficViolationsTweeter._recursively_process_status_updates')
    def test_process_response_campaign_only_lookup(self,
                                                   recursively_process_status_updates_mock):
        """ Test campaign-only lookup """

        username = 'NYCMayorsOffice'
        message_id = random.randint(1000000000000000000, 2000000000000000000)
        campaign_hashtag = '#SaferSkillman'
        campaign_tickets = random.randint(1000, 2000)
        campaign_vehicles = random.randint(100, 200)

        response_parts = [['@' + username + ' ' + str(campaign_vehicles) + ' vehicles with a total of ' + str(
            campaign_tickets) + ' tickets have been tagged with ' + campaign_hashtag + '.\n\n']]

        lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text=f'@howsmydrivingny {campaign_hashtag}',
                event_type='status',
                user_handle=username,
                user_id=30139847),
            message_source='status')

        reply_event_args = {
            'error_on_lookup': False,
            'request_object': lookup_request,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        reply_event_args['username'] = username

        self.tweeter._process_response(reply_event_args)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts, message_id, username)

    @mock.patch(
        'traffic_violations.services.twitter_service.TrafficViolationsTweeter._recursively_process_status_updates')
    def test_process_response_with_search_status(self,
                                                 recursively_process_status_updates_mock):
        """ Test plateless lookup """

        username = 'NYC_DOT'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        response_parts = [
            ['@' + username + ' I’d be happy to look that up for you!\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234']]

        lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text='@howsmydrivingny plate dkr9364 state ny',
                event_type='status',
                user_handle=username,
                user_id=30139847),
            message_source='status')

        reply_event_args = {
            'error_on_lookup': False,
            'request_object': lookup_request,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        reply_event_args['username'] = username

        self.tweeter._process_response(reply_event_args)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts, message_id, username)

    @mock.patch(
        'traffic_violations.services.twitter_service.TrafficViolationsTweeter._recursively_process_status_updates')
    def test_process_response_with_direct_message_api_direct_message(self,
                                                                     recursively_process_status_updates_mock):
        """ Test plateless lookup """

        username = 'NYCDDC'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        response_parts = [
            ['@' + username + " I think you're trying to look up a plate, but can't be sure.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"]]

        lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text='@howsmydrivingny the state is ny',
                event_type='status',
                user_handle=username,
                user_id=30139847),
            message_source='status')

        reply_event_args = {
            'error_on_lookup': False,
            'request_object': lookup_request,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        reply_event_args['username'] = username

        self.tweeter._process_response(reply_event_args)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts, message_id, username)

    @mock.patch(
        'traffic_violations.services.twitter_service.TrafficViolationsTweeter._recursively_process_status_updates')
    def test_process_response_with_error(self,
                                         recursively_process_status_updates_mock):
        """ Test error handling """

        username = 'BarackObama'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text='@howsmydrivingny plate:glf7467 state:pa',
                event_type='status',
                user_handle=username,
                user_id=30139847),
            message_source='status'
        )

        response_parts = [
            ['@' + username + " Sorry, I encountered an error. Tagging @bdhowald."]]

        reply_event_args = {
            'error_on_lookup': False,
            'request_object': lookup_request,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        reply_event_args['username'] = username

        self.tweeter._process_response(reply_event_args)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts, message_id, username)

    def test_recursively_process_direct_messages(self):
        str1 = 'Some stuff\n'
        str2 = 'Some other stuff\nSome more Stuff'
        str3 = 'Yet more stuff'

        response_parts = [
            [str1], str2, str3
        ]

        result_str = "\n".join([str1, str2, str3])

        self.assertEqual(self.tweeter._recursively_process_direct_messages(
            response_parts), result_str)

    def test_recursively_process_status_updates(self):
        str1 = 'Some stuff\n'
        str2 = 'Some other stuff\nSome more Stuff'
        str3 = 'Yet more stuff'

        original_id = 1

        response_parts = [
            [str1], str2, str3
        ]

        api_mock = MagicMock(name='api')
        api_mock.update_status = inc

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        self.tweeter.api = api_mock
        self.tweeter._is_production = is_production_mock

        self.assertEqual(self.tweeter._recursively_process_status_updates(
            response_parts, original_id, 'BarackObama'), original_id + len(response_parts))

        self.assertEqual(self.tweeter._recursively_process_status_updates(
            response_parts, original_id, 'BarackObama'), original_id + len(response_parts))
