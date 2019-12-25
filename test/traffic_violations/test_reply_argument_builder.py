import random
import unittest

from datetime import datetime
from unittest.mock import MagicMock

from traffic_violations.reply_argument_builder import (
    AccountActivityAPIDirectMessage,
    AccountActivityAPIStatus,
    DirectMessageAPIDirectMessage,
    HowsMyDrivingAPIRequest,
    ReplyArgumentBuilder,
    SearchStatus,
    StreamExtendedStatus,
    StreamingDirectMessage,
    StreamingStatus
)

from traffic_violations.constants.lookup_sources import LookupSources
from traffic_violations.constants.twitter import TwitterMessageTypes


class TestReplyArgumentBuilder(unittest.TestCase):

    CREATED_AT_MIN = 100_000_000
    CREATED_AT_MAX = 200_000_000

    CREATED_AT_TIMESTAMP_MIN = 1_000_000_000_000
    CREATED_AT_TIMESTAMP_MAX = 2_000_000_000_000

    EVENT_ID_MIN = 100_000_000_000
    EVENT_ID_MAX = 200_000_000_000

    USER_ID_MIN = 100_000_000_000
    USER_ID_MAX = 200_000_000_000

    HMDNY_TWITTER_HANDLE = 'HowsMyDrivingNY'
    OTHER_TWITTER_HANDLE = 'BarackObama'

    def setUp(self):
        recipient_mock = MagicMock(name='recipient_mock')
        recipient_mock.screen_name = self.HMDNY_TWITTER_HANDLE

        sender_mock = MagicMock(name='sender_mock')
        sender_mock.screen_name = self.OTHER_TWITTER_HANDLE

        api = MagicMock(name='api_mock')
        api.get_user.side_effect = [recipient_mock, sender_mock]
        self.reply_argument_builder = ReplyArgumentBuilder(api)

    def test_build_account_activity_api_direct_message(self):
        account_activity_api_direct_message = {
            'created_at': random.randint(self.CREATED_AT_MIN, self.CREATED_AT_MAX),
            'event_id': random.randint(self.EVENT_ID_MIN, self.EVENT_ID_MAX),
            'event_text': 'hi there',
            'event_type': 'direct_message',
            'full_text': 'abc',
            'user_handle': self.OTHER_TWITTER_HANDLE,
            'user_id': random.randint(self.USER_ID_MIN, self.USER_ID_MAX),
            'user_mentions': self.HMDNY_TWITTER_HANDLE
        }

        req = self.reply_argument_builder.build_reply_data(account_activity_api_direct_message,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.DIRECT_MESSAGE)

        self.assertIsInstance(req, AccountActivityAPIDirectMessage)

    def test_build_account_activity_api_status(self):
        account_activity_api_status = {
            'created_at': random.randint(self.CREATED_AT_MIN, self.CREATED_AT_MAX),
            'event_id': random.randint(self.EVENT_ID_MIN, self.EVENT_ID_MAX),
            'event_text': 'hi there',
            'event_type': 'status',
            'full_text': 'abc',
            'user_handle': self.OTHER_TWITTER_HANDLE,
            'user_id': random.randint(self.USER_ID_MIN, self.USER_ID_MAX),
            'user_mentions': self.HMDNY_TWITTER_HANDLE
        }

        req = self.reply_argument_builder.build_reply_data(account_activity_api_status,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.STATUS)

        self.assertIsInstance(req, AccountActivityAPIStatus)

    def test_build_direct_message_api_direct_message(self):
        direct_message_api_direct_message = MagicMock(
            spec=['direct_message_api_direct_message'])
        direct_message_api_direct_message.created_timestamp = str(
            random.randint(
                self.CREATED_AT_TIMESTAMP_MIN, self.CREATED_AT_TIMESTAMP_MAX))
        direct_message_api_direct_message.id = random.randint(
            self.EVENT_ID_MIN, self.EVENT_ID_MAX)
        direct_message_api_direct_message.message_create = {
            'entities': {
                'user_mentions': [
                    {
                        'screen_name': self.HMDNY_TWITTER_HANDLE
                    }
                ]
            },
            'full_text': 'abc',
            'message_data': {
                'text': 'hi there'
            },
            'sender_id': random.randint(self.USER_ID_MIN, self.USER_ID_MAX),
            'target': {
                'recipient_id': random.randint(self.USER_ID_MIN, self.USER_ID_MAX)
            }
        }

        req = self.reply_argument_builder.build_reply_data(direct_message_api_direct_message,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.DIRECT_MESSAGE)

        self.assertIsInstance(req, DirectMessageAPIDirectMessage)

    def test_build_hows_my_driving_api_request(self):
        hows_my_driving_api_request = {
            'created_at': datetime.now().strftime('%a %b %d %H:%M:%S %z %Y'),
            'event_id': random.randint(
                self.EVENT_ID_MIN, self.EVENT_ID_MAX),
            'event_text': 'abc',
            'username': self.OTHER_TWITTER_HANDLE
        }

        req = self.reply_argument_builder.build_reply_data(hows_my_driving_api_request,
                                                           LookupSources.API,
                                                           None)

        self.assertIsInstance(req, HowsMyDrivingAPIRequest)

    def test_build_stream_extended_status(self):
        stream_extended_status = MagicMock(name='stream_extended_status')
        stream_extended_status.created_at = datetime.now()
        stream_extended_status.extended_tweet = {
            'entities': {
                'user_mentions': [
                    {
                        'screen_name': self.HMDNY_TWITTER_HANDLE
                    }
                ]
            },
            'full_text': 'abc'
        }

        req = self.reply_argument_builder.build_reply_data(stream_extended_status,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.STATUS)

        self.assertIsInstance(req, StreamExtendedStatus)

    def test_build_streaming_direct_message(self):
        streaming_direct_message = MagicMock(spec=['streaming_direct_message'])
        streaming_direct_message.direct_message = {
            'created_at': random.randint(self.CREATED_AT_MIN, self.CREATED_AT_MAX),
            'event_type': 'direct_message',
            'id': random.randint(self.EVENT_ID_MIN, self.EVENT_ID_MAX),
            'recipient': {
                'screen_name': self.HMDNY_TWITTER_HANDLE
            },
            'sender': {
                'id': random.randint(self.USER_ID_MIN, self.USER_ID_MAX),
                'screen_name': self.OTHER_TWITTER_HANDLE
            },
            'text': 'hi there',
            'user_handle': self.OTHER_TWITTER_HANDLE,
            'user_id': random.randint(self.USER_ID_MIN, self.USER_ID_MAX),
            'user_mentions': self.HMDNY_TWITTER_HANDLE
        }

        req = self.reply_argument_builder.build_reply_data(streaming_direct_message,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.DIRECT_MESSAGE)

        self.assertIsInstance(req, StreamingDirectMessage)

    def test_build_streaming_status(self):
        user_mock = MagicMock(name='user')
        user_mock.id = random.randint(self.USER_ID_MIN, self.USER_ID_MAX)
        user_mock.screen_name = self.OTHER_TWITTER_HANDLE

        streaming_status = MagicMock(spec=['entities'])
        streaming_status.created_at = datetime.now()
        streaming_status.entities = {
            'user_mentions': [
                {
                    'screen_name': self.HMDNY_TWITTER_HANDLE
                }
            ]
        }
        streaming_status.id = random.randint(
            self.EVENT_ID_MIN, self.EVENT_ID_MAX)
        streaming_status.text = 'hi there'
        streaming_status.user = user_mock

        req = self.reply_argument_builder.build_reply_data(streaming_status,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.STATUS)

        self.assertIsInstance(req, StreamingStatus)

    def test_build_search_status(self):
        user_mock = MagicMock(name='user')
        user_mock.id = random.randint(self.USER_ID_MIN, self.USER_ID_MAX)
        user_mock.screen_name = self.OTHER_TWITTER_HANDLE

        search_status = MagicMock(spec=['entities'])
        search_status.full_text = 'hi there'
        search_status.created_at = datetime.now()
        search_status.entities = {
            'user_mentions': [
                {
                    'screen_name': self.HMDNY_TWITTER_HANDLE
                }
            ]
        }
        search_status.id = random.randint(
            self.EVENT_ID_MIN, self.EVENT_ID_MAX)
        search_status.user = user_mock

        req = self.reply_argument_builder.build_reply_data(search_status,
                                                           LookupSources.TWITTER,
                                                           TwitterMessageTypes.STATUS)

        self.assertIsInstance(req, SearchStatus)
