import ddt
import mock
import os
import pytz
import random
import statistics
import unittest

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from typing import List
from typing import Optional

from unittest.mock import call, MagicMock

from traffic_violations.models.twitter_event import TwitterEvent

from traffic_violations.reply_argument_builder import \
    AccountActivityAPIDirectMessage, AccountActivityAPIStatus

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter


def inc(status, in_reply_to_status_id: int, auto_populate_reply_metadata: bool, exclude_reply_user_ids: bool):
    int_mock = MagicMock(name='api')
    int_mock.id = (in_reply_to_status_id + 1)
    return int_mock

@ddt.ddt
class TestTrafficViolationsTweeter(unittest.TestCase):

    def setUp(self):
        self.tweeter = TrafficViolationsTweeter()

        self.tweeter._app_api = MagicMock(name='app_api')
        self.tweeter._client_api = MagicMock(name='client_api')

        # mock followers ids to be empty so that asking if the
        # requesting user is a follower always returns True
        self.tweeter._client_api.get_follower_ids.return_value = []
        self.log_patcher = mock.patch(
            'traffic_violations.services.twitter_service.LOG')

        self.mocked_log = self.log_patcher.start()

    def tearDown(self):
        self.log_patcher.stop()

    def test_find_and_respond_to_requests(self):
        direct_messages_mock = MagicMock(
            name='_find_and_respond_to_missed_direct_messages')
        statuses_mock = MagicMock(
            name='_find_and_respond_to_missed_statuses')
        twitter_events_mock = MagicMock(
            name='_find_and_respond_to_twitter_events')
        self.tweeter._find_and_respond_to_twitter_events = twitter_events_mock
        self.tweeter._find_and_respond_to_missed_direct_messages = direct_messages_mock
        self.tweeter._find_and_respond_to_missed_statuses = statuses_mock

        self.tweeter.find_and_respond_to_requests()

        direct_messages_mock.assert_called_with()
        statuses_mock.assert_called_with()
        twitter_events_mock.assert_called_with()

    @mock.patch(
        'traffic_violations.services.twitter_service.TwitterEvent')
    def test_find_and_respond_to_missed_direct_messages(self, twitter_event_mock):
        db_id = 1
        event_id = random.randint(10000000000000000000, 20000000000000000000)
        event_text = 'abc1234:ny'
        event_type = 'direct_message'
        timestamp = random.randint(1500000000000, 1700000000000)
        user_handle = 'bdhowald'
        user_id = random.randint(1000000000, 2000000000)

        new_twitter_event = TwitterEvent(
            created_at=timestamp + 1,
            detected_via_account_activity_api=False,
            event_id=event_id + 1,
            event_text=event_text + '!',
            event_type=event_type,
            in_reply_to_message_id=None,
            location=None,
            responded_to=False,
            user_handle=user_handle,
            user_id=user_id,
            user_mentions=[])

        message_needing_event = MagicMock(
            id=event_id + 1,
            created_timestamp = timestamp + 1,
            message_create={
              'message_data': {
                'entities': {
                  'user_mentions': [
                    {
                      'id': 123,
                      'id_str': '123',
                      'screen_name': user_handle
                    },
                    {
                      'id': 456,
                      'id_str': '456',
                      'screen_name': 'OtherUser'
                    },
                    {
                      'id': 789,
                      'id_str': '456',
                      'screen_name': 'SomeOtherUser'
                    }
                  ]
                },
                'text': event_text + '!'
              },
              'sender_id': f'{user_id}'
            },
            name='message_needing_event')

        message_not_needing_event = MagicMock(
            id=event_id - 1,
            created_timestamp = timestamp - 1,
            message_create={
                'message_data': {
                    'entities': {
                        'user_mentions': [
                            {
                                'id': 123,
                                'id_str': '123',
                                'screen_name': user_handle
                            },
                            {
                                'id': 456,
                                'id_str': '456',
                                'screen_name': 'OtherUser'
                            },
                            {
                                'id': 789,
                                'id_str': '456',
                                'screen_name': 'SomeOtherUser'
                            }
                        ]
                    },
                  'text': event_text,
                },
                'sender_id': f'{user_id}'
            },
            name='message_not_needing_event')

        sender = MagicMock(
            id=user_id,
            id_str=f'{user_id}',
            name='sender',
            screen_name=user_handle)

        twitter_event_mock.return_value = new_twitter_event
        twitter_event_mock.query.filter.return_value.first.side_effect = [
            None, message_not_needing_event]

        client_api_mock = MagicMock(name='client_api')
        client_api_mock.get_direct_messages.return_value = [
          message_needing_event, message_not_needing_event]
        client_api_mock.lookup_users.return_value = [sender]
        self.tweeter._client_api = client_api_mock

        self.tweeter._find_and_respond_to_missed_direct_messages()

        twitter_event_mock.query.session.add.assert_called_once_with(new_twitter_event)
        twitter_event_mock.query.session.commit.assert_called_once_with()

        self.mocked_log.debug.assert_called_with('Found 1 direct message that was previously undetected.')

        self.tweeter.terminate_lookups()

    @mock.patch(
        'traffic_violations.services.twitter_service.TwitterEvent')
    def test_find_and_respond_to_missed_statuses(self, twitter_event_mock):
        db_id = 1
        event_id = random.randint(10000000000000000000, 20000000000000000000)
        event_text = '@HowsMyDrivingNY @bdhowald abc1234:ny'
        event_type = 'status'
        in_reply_to_message_id = random.randint(
            10000000000000000000, 20000000000000000000)
        location = 'Queens, NY'
        now = datetime.utcnow()
        place = MagicMock(
            full_name=location,
            name='place')

        user_id = random.randint(1000000000, 2000000000)
        user = MagicMock(
            id=user_id,
            name='user')
        user_handle = 'bdhowald'

        older_twitter_event = TwitterEvent(
            id=db_id,
            created_at=now.replace(tzinfo=pytz.timezone('UTC')).timestamp() * 1000,
            detected_via_account_activity_api=False,
            event_id=event_id,
            event_text=event_text,
            event_type=event_type,
            in_reply_to_message_id=in_reply_to_message_id,
            location=location,
            responded_to=True,
            user_handle=user_handle,
            user_id=user_id,
            user_mention_ids=[user_id],
            user_mentions=[
                {
                    'id': 123,
                    'id_str': '123',
                    'screen_name': user_handle
                },
                {
                    'id': 456,
                    'id_str': '456',
                    'screen_name': 'OtherUser'
                },
                {
                    'id': 789,
                    'id_str': '456',
                    'screen_name': 'SomeOtherUser'
                }
            ])

        new_twitter_event = TwitterEvent(
            created_at=now.replace(tzinfo=pytz.timezone('UTC')).timestamp() * 1000,
            detected_via_account_activity_api=False,
            event_id=event_id + 1,
            event_text=event_text + '!',
            event_type=event_type,
            in_reply_to_message_id=in_reply_to_message_id,
            location=location,
            responded_to=False,
            user_handle=user_handle,
            user_id=user_id,
            user_mention_ids=[user_id],
            user_mentions=user_handle)

        status_needing_event = MagicMock(
            id=event_id + 1,
            created_at = now,
            entities={
                'user_mentions': [
                    {
                        'id': 123,
                        'id_str': '123',
                        'screen_name': user_handle
                    },
                    {
                        'id': 456,
                        'id_str': '456',
                        'screen_name': 'OtherUser'
                    },
                    {
                        'id': 789,
                        'id_str': '456',
                        'screen_name': 'SomeOtherUser'
                    }
                ]
            },
            full_text=event_text + '!',
            in_reply_to_message_id=in_reply_to_message_id,
            name='status_needing_event',
            place=place,
            user=user)

        status_not_needing_event = MagicMock(
            id=event_id - 1,
            created_at = now - relativedelta(minutes=1),
            entities={
                'user_mentions': [
                    {
                        'id': 123,
                        'id_str': '123',
                        'screen_name': user_handle
                    },
                    {
                        'id': 456,
                        'id_str': '456',
                        'screen_name': 'OtherUser'
                    },
                    {
                        'id': 789,
                        'id_str': '456',
                        'screen_name': 'SomeOtherUser'
                    }
                ]
            },
            full_text=event_text,
            in_reply_to_message_id=in_reply_to_message_id,
            name='status_not_needing_event',
            place=place,
            user=user)

        twitter_event_mock.return_value = new_twitter_event
        twitter_event_mock.query.filter().order_by().first.return_value = older_twitter_event
        twitter_event_mock.query.filter().first.side_effect = [
            None, status_not_needing_event]

        client_api_mock = MagicMock(name='client_api')
        client_api_mock.mentions_timeline.side_effect = [[
            status_needing_event, status_not_needing_event], []]
        self.tweeter._client_api = client_api_mock

        self.tweeter._find_and_respond_to_missed_statuses()

        twitter_event_mock.query.session.add.assert_called_once_with(new_twitter_event)
        twitter_event_mock.query.session.commit.assert_called_once_with()

        self.mocked_log.debug.assert_called_with('Found 1 status that was previously undetected.')

        self.tweeter.terminate_lookups()

    @mock.patch(
        'traffic_violations.services.twitter_service.TwitterEvent')
    def test_find_and_respond_to_missed_statuses_with_no_undetected_events(self, twitter_event_mock):
        twitter_event_mock.query.filter().order_by().first.return_value = None

        self.tweeter._find_and_respond_to_missed_statuses()

        twitter_event_mock.query.session().add.assert_not_called()
        twitter_event_mock.query.session().commit.assert_not_called()

        self.tweeter.terminate_lookups()

    @ddt.data({
        'event_type': 'direct_message',
        'is_follower': True
    }, {
        'event_type': 'direct_message',
        'is_follower': False,
        'response_parts': [
            'If you would like to look up plates via direct message, '
            'please follow @HowsMyDrivingNY and try again.']
    }, {
        'event_type': 'status',
        'is_follower': False,
        'response_parts': [
            "It appears that you don't follow @HowsMyDrivingNY.\n\n"
            'No worries, simply like this tweet to perform a query '
            'or visit https://howsmydrivingny.nyc.']
    })
    @ddt.unpack
    @mock.patch(
        'traffic_violations.services.twitter_service.TwitterEvent')
    def test_find_and_respond_to_twitter_events(self,
                                                twitter_event_mock: MagicMock,
                                                event_type: str,
                                                is_follower: bool,
                                                response_parts: Optional[List[str]] = None):
        db_id = 1
        random_id = random.randint(1000000000000000000, 2000000000000000000)
        user_handle = 'bdhowald'
        user_id = random.randint(1000000000, 2000000000)
        event_text = '@HowsMyDrivingNY abc1234:ny'
        timestamp = random.randint(1500000000000, 1700000000000)
        in_reply_to_message_id = random.randint(
            10000000000000000000, 20000000000000000000)
        location = 'Queens, NY'
        responded_to = 0
        user_mentions = [
            {
                'id': 123,
                'id_str': '123',
                'screen_name': 'HowsMyDrivingNY'
            },
            {
                'id': 456,
                'id_str': '456',
                'screen_name': 'OtherUser'
            },
            {
                'id': 789,
                'id_str': '456',
                'screen_name': 'SomeOtherUser'
            }
        ]

        twitter_event = TwitterEvent(
            id=db_id,
            created_at=timestamp,
            event_id=random_id,
            event_text=event_text,
            event_type=event_type,
            in_reply_to_message_id=in_reply_to_message_id,
            last_failed_at_time=None,
            location=location,
            num_times_failed=0,
            responded_to=responded_to,
            user_handle=user_handle,
            user_id=user_id,
            user_mentions=user_mentions)

        lookup_request = AccountActivityAPIDirectMessage(
            message=TwitterEvent(
                id=1,
                created_at=timestamp,
                event_id=random_id,
                event_text=event_text,
                event_type=event_type,
                user_handle=user_handle,
                user_id=user_id,
                user_favorited_non_follower_reply=False),
            message_source=event_type)

        twitter_event_mock.get_all_by.return_value = [twitter_event]
        twitter_event_mock.query.filter_by().filter().count.return_value = 0

        initiate_reply_mock = MagicMock(name='initiate_reply')
        self.tweeter.aggregator.initiate_reply = initiate_reply_mock

        build_reply_data_mock = MagicMock(name='build_reply_data')
        build_reply_data_mock.return_value = lookup_request
        self.tweeter.reply_argument_builder.build_reply_data = build_reply_data_mock

        application_api_mock = MagicMock(name='application_api')
        application_api_mock.get_follower_ids.return_value = ([
            user_id if is_follower else (user_id + 1)], (123, 0))
        self.tweeter._app_api = application_api_mock

        process_response_mock = MagicMock(name='process_response')
        process_response_mock.return_value = random_id
        self.tweeter._process_response = process_response_mock

        self.tweeter._find_and_respond_to_twitter_events()

        if is_follower:
            self.tweeter.aggregator.initiate_reply.assert_called_with(
                lookup_request=lookup_request)
        else:
            self.tweeter.aggregator.initiate_reply.assert_not_called()
            self.tweeter._process_response.assert_called_with(
                request_object=lookup_request,
                response_parts=response_parts)

        self.tweeter.terminate_lookups()

    @ddt.data({
        'event_type': 'direct_message',
        'expect_called': True,
        'num_times_failed': 0
    }, {
        'event_type': 'direct_message',
        'expect_called': True,
        'num_times_failed': 0
    }, {
        'event_type': 'status',
        'expect_called': False,
        'num_times_failed': 0,
        'tweet_exists': False,
    }, {
        'event_type': 'status',
        'expect_called': False,
        'last_failed_time': timedelta(minutes=4),
        'num_times_failed': 1
    }, {
        'event_type': 'status',
        'expect_called': True,
        'last_failed_time': timedelta(minutes=6),
        'num_times_failed': 1
    }, {
        'event_type': 'status',
        'expect_called': False,
        'last_failed_time': timedelta(minutes=59),
        'num_times_failed': 2
    }, {
        'event_type': 'status',
        'expect_called': True,
        'last_failed_time': timedelta(minutes=61),
        'num_times_failed': 2
    }, {
        'event_type': 'status',
        'expect_called': False,
        'last_failed_time': timedelta(hours=2),
        'num_times_failed': 3
    }, {
        'event_type': 'status',
        'expect_called': True,
        'last_failed_time': timedelta(hours=4),
        'num_times_failed': 3
    }, {
        'event_type': 'status',
        'expect_called': False,
        'last_failed_time': timedelta(hours=23),
        'num_times_failed': 4
    }, {
        'event_type': 'status',
        'expect_called': True,
        'last_failed_time': timedelta(hours=25),
        'num_times_failed': 4
    }, {
        'event_type': 'status',
        'expect_called': False,
        'last_failed_time': timedelta(hours=25),
        'num_times_failed': 5
    })
    @ddt.unpack
    @mock.patch(
        'traffic_violations.services.twitter_service.TwitterEvent')
    def test_find_and_respond_to_failed_twitter_events(self,
                                                       twitter_event_mock: MagicMock,
                                                       event_type: str,
                                                       expect_called: bool,
                                                       num_times_failed: int,
                                                       last_failed_time: timedelta = timedelta(minutes=0),
                                                       tweet_exists: bool = True):
        db_id = 1
        random_id = random.randint(1000000000000000000, 2000000000000000000)
        user_handle = 'bdhowald'
        user_id = random.randint(1000000000, 2000000000)
        event_text = '@HowsMyDrivingNY abc1234:ny'
        timestamp = random.randint(1500000000000, 1700000000000)
        in_reply_to_message_id = random.randint(
            10000000000000000000, 20000000000000000000)
        location = 'Queens, NY'
        responded_to = 0
        user_mentions = [
            {
                'id': 123,
                'id_str': '123',
                'screen_name': 'HowsMyDrivingNY'
            },
            {
                'id': 456,
                'id_str': '456',
                'screen_name': 'OtherUser'
            },
            {
                'id': 789,
                'id_str': '456',
                'screen_name': 'SomeOtherUser'
            }
        ]


        twitter_event = TwitterEvent(
            id=db_id,
            created_at=timestamp,
            event_id=random_id,
            event_text=event_text,
            event_type=event_type,
            in_reply_to_message_id=in_reply_to_message_id,
            last_failed_at_time=(datetime.utcnow() - last_failed_time),
            location=location,
            num_times_failed=num_times_failed,
            responded_to=responded_to,
            user_handle=user_handle,
            user_id=user_id,
            user_mentions=user_mentions)

        direct_message_lookup_request = AccountActivityAPIDirectMessage(
            message=TwitterEvent(
                id=1,
                created_at=timestamp,
                event_id=random_id,
                event_text=event_text,
                event_type=event_type,
                user_handle=user_handle,
                user_id=user_id,
                user_favorited_non_follower_reply=False),
            message_source=event_type)

        status_lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=timestamp,
                event_id=random_id,
                event_text=event_text,
                event_type=event_type,
                user_handle=user_handle,
                user_id=user_id,
                user_favorited_non_follower_reply=False),
            message_source=event_type)

        lookup_request = (direct_message_lookup_request if
            event_type == 'direct_message' else status_lookup_request)

        twitter_event_mock.get_all_by.side_effect = [[], [twitter_event]]
        twitter_event_mock.query.filter_by().filter().count.return_value = 0

        tweet_exists_mock = MagicMock(name='tweet_exists')
        tweet_exists_mock.return_value = True if tweet_exists else False
        self.tweeter.tweet_detection_service.tweet_exists = tweet_exists_mock

        initiate_reply_mock = MagicMock(name='initiate_reply')
        self.tweeter.aggregator.initiate_reply = initiate_reply_mock

        build_reply_data_mock = MagicMock(name='build_reply_data')
        build_reply_data_mock.return_value = lookup_request
        self.tweeter.reply_argument_builder.build_reply_data = build_reply_data_mock

        application_api_mock = MagicMock(name='application_api')
        application_api_mock.get_follower_ids.return_value = ([user_id], (123, 0))
        self.tweeter._app_api = application_api_mock

        process_response_mock = MagicMock(name='process_response')
        process_response_mock.return_value = random_id
        self.tweeter._process_response = process_response_mock

        self.tweeter._find_and_respond_to_twitter_events()

        if expect_called:
            self.tweeter.aggregator.initiate_reply.assert_called_with(
                lookup_request=lookup_request)
        else:
            self.tweeter.aggregator.initiate_reply.assert_not_called()

        self.tweeter.terminate_lookups()


    @ddt.data({
        'expect_called': True,
        'minutes_ago': 20
    }, {
        'expect_called': False,
        'minutes_ago': 10
    })
    @ddt.unpack
    def test_get_follower_ids(self, expect_called: bool, minutes_ago: int):
        application_api_mock = MagicMock(name='application_api')
        application_api_mock.get_follower_ids.return_value = ([1], (0, 0))
        self.tweeter._app_api = application_api_mock

        self.tweeter._follower_ids_last_fetched = datetime.utcnow() - timedelta(
            minutes=minutes_ago)

        self.tweeter._get_follower_ids()

        if expect_called:
            application_api_mock.get_follower_ids.assert_called_with(cursor=-1)
        else:
            application_api_mock.get_follower_ids.assert_not_called()

    @mock.patch(
        'traffic_violations.services.twitter_service.TrafficViolationsTweeter._is_production')
    def test_process_response_direct_message(self, mocked_is_production):
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

        mocked_is_production.return_value = True

        send_direct_message_mock = MagicMock('send_direct_message_mock')

        application_api_mock = MagicMock(name='application_api')
        application_api_mock.send_direct_message = send_direct_message_mock
        self.tweeter._app_api = application_api_mock

        self.tweeter._process_response(
            request_object=lookup_request,
            response_parts=[[combined_message]],
            successful_lookup=True)

        send_direct_message_mock.assert_called_with(
          recipient_id=30139847,
          text=combined_message)

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
                user_id=30139847,
                user_mention_ids='813286,19834403,1230933768342528001,37687633,66379182',
                user_mentions='@BarackObama @NYCMayor @GoodNYCMayor @NYC_DOT @NYCTSubway'
            ),
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

        application_api_mock = MagicMock(name='application_api')
        application_api_mock.create_favorite = create_favorite_mock

        self.tweeter._is_production = is_production_mock
        self.tweeter._app_api = application_api_mock

        reply_event_args['username'] = username

        self.tweeter._process_response(
            request_object=lookup_request,
            response_parts=response_parts,
            successful_lookup=True)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts=response_parts,
            message_id=message_id,
            user_mention_ids=[
                '813286',
                '19834403',
                '1230933768342528001',
                '37687633',
                '66379182'
            ])

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

        response_parts = [[(f"@{username} {'{:,}'.format(campaign_vehicles)} vehicles with a total of "
            f"{'{:,}'.format(campaign_tickets)} tickets have been tagged with {campaign_hashtag}.\n\n")]]

        lookup_request = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1_000_000_000_000, 2_000_000_000_000),
                event_id=message_id,
                event_text=f'@howsmydrivingny {campaign_hashtag}',
                event_type='status',
                user_handle=username,
                user_id=30139847,
                user_mention_ids='813286,19834403,1230933768342528001,37687633,66379182',
                user_mentions='@BarackObama @NYCMayor @GoodNYCMayor @NYC_DOT @NYCTSubway'
            ),
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

        self.tweeter._process_response(
            request_object=lookup_request,
            response_parts=response_parts,
            successful_lookup=True)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts=response_parts,
            message_id=message_id,
            user_mention_ids=[
                '813286',
                '19834403',
                '1230933768342528001',
                '37687633',
                '66379182'
            ])

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
                user_id=30139847,
                user_mention_ids='813286,19834403,1230933768342528001,37687633,66379182',
                user_mentions='@BarackObama @NYCMayor @GoodNYCMayor @NYC_DOT @NYCTSubway'
            ),
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

        self.tweeter._process_response(
            request_object=lookup_request,
            response_parts=response_parts,
            successful_lookup=True)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts=response_parts,
            message_id=message_id,
            user_mention_ids=[
                '813286',
                '19834403',
                '1230933768342528001',
                '37687633',
                '66379182'
            ])

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
                user_id=30139847,
                user_mention_ids='813286,19834403,1230933768342528001,37687633,66379182',
                user_mentions='@BarackObama @NYCMayor @GoodNYCMayor @NYC_DOT @NYCTSubway'
            ),
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

        self.tweeter._process_response(
            request_object=lookup_request,
            response_parts=response_parts,
            successful_lookup=True)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts=response_parts,
            message_id=message_id,
            user_mention_ids=[
                '813286',
                '19834403',
                '1230933768342528001',
                '37687633',
                '66379182'
            ])

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
                user_id=30139847,
                user_mention_ids='813286,19834403,1230933768342528001,37687633,66379182',
                user_mentions='@BarackObama @NYCMayor @GoodNYCMayor @NYC_DOT @NYCTSubway'
            ),
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

        self.tweeter._process_response(
            request_object=lookup_request,
            response_parts=response_parts,
            successful_lookup=True)

        recursively_process_status_updates_mock.assert_called_with(
            response_parts=response_parts,
            message_id=message_id,
            user_mention_ids=[
                '813286',
                '19834403',
                '1230933768342528001',
                '37687633',
                '66379182'
            ])

    def test_recursively_compile_direct_messages(self):
        str1 = 'Some stuff\n'
        str2 = 'Some other stuff\nSome more Stuff'
        str3 = 'Yet more stuff'

        response_parts = [
            [str1], str2, str3
        ]

        result_str = "\n".join([str1, str2, str3])

        self.assertEqual(self.tweeter._recursively_compile_direct_messages(
            response_parts), result_str)

    @ddt.data({
        'response_parts': [
            [
                'Some stuff\n'
            ],
            'Some other stuff\nSome more Stuff',
            'Yet more stuff'
        ]
    }, {
        'response_parts': [
            'Some stuff\n',
            'Some other stuff\nSome more Stuff',
            'Yet more stuff'
        ]
    })
    @ddt.unpack
    @mock.patch(
        'traffic_violations.services.twitter_service.LOG')
    def test_recursively_process_status_updates(
        self,
        mocked_log: MagicMock,
        response_parts: List[str]
    ) -> None:
        original_id = 1

        user_mention_ids = ['1','2','3']

        update_status_mock = MagicMock(name='update_status')
        update_status_mock.side_effect = [
            MagicMock(id=123), MagicMock(id=456), MagicMock(id=789)
        ]

        application_api_mock = MagicMock(name='application_api')
        application_api_mock.update_status = update_status_mock

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        self.tweeter._app_api = application_api_mock
        self.tweeter._is_production = is_production_mock

        self.tweeter._recursively_process_status_updates(
            response_parts, original_id, user_mention_ids)

        mocked_log.debug.assert_has_calls([
            call("message_id: 123"),
            call("message_id: 456"),
            call("message_id: 789")
        ])

        excluded_reply_user_ids = ','.join(user_mention_ids)

        update_status_mock.assert_has_calls([
            call(
                status='Some stuff\n',
                in_reply_to_status_id=original_id,
                exclude_reply_user_ids=excluded_reply_user_ids
            ),
            call(
                status='Some other stuff\nSome more Stuff',
                in_reply_to_status_id=123,
                exclude_reply_user_ids=excluded_reply_user_ids
            ),
            call(
                status='Yet more stuff',
                in_reply_to_status_id=456,
                exclude_reply_user_ids=excluded_reply_user_ids
            )
        ])