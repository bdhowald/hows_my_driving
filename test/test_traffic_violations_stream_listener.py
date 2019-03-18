import json
import unittest
import tweepy

# from mock import MagicMock
from unittest.mock import MagicMock

from traffic_violations.traffic_violations_stream_listener import TrafficViolationsStreamListener
from twitter_service import TrafficViolationsTweeter


class TestTrafficViolationsStreamListener(unittest.TestCase):

    def setUp(self):
        self.listener = TrafficViolationsStreamListener(
            TrafficViolationsTweeter())

    def test_on_data(self):
        direct_message_data = '{"direct_message": "stuff"}'

        parse_mock = MagicMock(name='parse')
        parse_mock.return_value = 123

        status_mock = MagicMock(name='status')
        status_mock.parse = parse_mock

        tweepy.Status = status_mock

        initiate_reply_mock = MagicMock(name='initiate_reply')

        self.listener.tweeter.aggregator.initiate_reply = initiate_reply_mock

        self.listener.on_data(direct_message_data)

        self.listener.tweeter.aggregator.initiate_reply.assert_called_with(
            123, 'direct_message')
        parse_mock.assert_called_with(
            self.listener.api, json.loads(direct_message_data))

        event_data = '{"event": "stuff"}'

        self.listener.on_data(event_data)

        parse_mock.assert_called_with(
            self.listener.api, json.loads(event_data))

        in_reply_to_status_id_data = '{"in_reply_to_status_id": "stuff"}'

        self.listener.on_data(in_reply_to_status_id_data)

        self.listener.tweeter.aggregator.initiate_reply.assert_called_with(
            123, 'status')
        parse_mock.assert_called_with(
            self.listener.api, json.loads(in_reply_to_status_id_data))

    def test_on_direct_message(self):
        status_mock = MagicMock(name='status')

        debug_mock = MagicMock(name='debug')
        logger_mock = MagicMock(name='logger')
        logger_mock.debug = debug_mock

        self.listener.logger = logger_mock

        self.listener.on_direct_message(status_mock)

        debug_mock.assert_called_with("on_direct_message: %s", status_mock)

    def test_on_error(self):
        status_mock = MagicMock(name='status')

        debug_mock = MagicMock(name='debug')
        logger_mock = MagicMock(name='logger')
        logger_mock.debug = debug_mock

        self.listener.logger = logger_mock

        self.listener.on_error(status_mock)

        debug_mock.assert_called_with("on_error: %s", status_mock)

    def test_on_event(self):
        status_mock = MagicMock(name='status')

        debug_mock = MagicMock(name='debug')
        logger_mock = MagicMock(name='logger')
        logger_mock.debug = debug_mock

        self.listener.logger = logger_mock

        self.listener.on_event(status_mock)

        debug_mock.assert_called_with("on_event: %s", status_mock)

    def test_on_status(self):
        status_mock = MagicMock(name='status')
        status_mock.text = 'Here is some text!'

        debug_mock = MagicMock(name='debug')
        logger_mock = MagicMock(name='logger')
        logger_mock.debug = debug_mock

        self.listener.logger = logger_mock

        self.listener.on_status(status_mock)

        debug_mock.assert_called_with(
            "\n\n\non_status: %s\n\n\n", status_mock.text)
