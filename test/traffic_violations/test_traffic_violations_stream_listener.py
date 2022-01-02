import json
import mock
import unittest
import tweepy

from unittest.mock import MagicMock

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter

from traffic_violations.traffic_violations_stream_listener import \
    TrafficViolationsStreamListener


class TestTrafficViolationsStreamListener(unittest.TestCase):

    def setUp(self):
        self.listener = TrafficViolationsStreamListener(
            TrafficViolationsTweeter())

        self.log_patcher = mock.patch(
            'traffic_violations.traffic_violations_stream_listener.LOG')
        self.mocked_log = self.log_patcher.start()

    def tearDown(self):
        self.log_patcher.stop()

    def test_on_data(self):
        direct_message_data = '{"direct_message": "stuff"}'

        parse_mock = MagicMock(name='parse')
        parse_mock.return_value = 123

        status_mock = MagicMock(name='status')
        status_mock.parse = parse_mock

        tweepy.models.Status = status_mock

        initiate_reply_mock = MagicMock(name='initiate_reply')

        self.listener.tweeter.aggregator.initiate_reply = initiate_reply_mock

        self.listener.on_data(direct_message_data)

        self.listener.tweeter.aggregator.initiate_reply.assert_called_with(
            123, 'direct_message')
        parse_mock.assert_called_with(None, json.loads(direct_message_data))

        event_data = '{"event": "stuff"}'

        self.listener.on_data(event_data)

        parse_mock.assert_called_with(None, json.loads(event_data))

        in_reply_to_status_id_data = '{"in_reply_to_status_id": "stuff"}'

        self.listener.on_data(in_reply_to_status_id_data)

        self.listener.tweeter.aggregator.initiate_reply.assert_called_with(
            123, 'status')
        parse_mock.assert_called_with(None, json.loads(in_reply_to_status_id_data))

    # @mock.patch('')
    def test_on_direct_message(self):
        status_mock = MagicMock(name='status')

        self.listener.on_direct_message(status_mock)

        self.mocked_log.debug.assert_called_with(
            f'on_direct_message: {status_mock}')

    def test_on_error(self):
        status_mock = MagicMock(name='status')

        self.listener.on_error(status_mock)

        self.mocked_log.debug.assert_called_with(f'on_error: {status_mock}')

    def test_on_event(self):
        status_mock = MagicMock(name='status')

        self.listener.on_event(status_mock)

        self.mocked_log.debug.assert_called_with(f'on_event: {status_mock}')

    def test_on_status(self):
        status_mock = MagicMock(name='status')

        self.listener.on_status(status_mock)

        self.mocked_log.debug.assert_called_with(
            f'on_status: {status_mock.text}')
