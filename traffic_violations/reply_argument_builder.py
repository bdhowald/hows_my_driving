import logging
import pytz
import re

from datetime import datetime, timezone
from typing import Type

from traffic_violations.constants.lookup_sources import LookupSource
from traffic_violations.constants.twitter import TwitterAPIAttribute, \
    TwitterMessageType

from traffic_violations.models.lookup_requests import BaseLookupRequest, \
    AccountActivityAPIDirectMessage, AccountActivityAPIStatus, \
    DirectMessageAPIDirectMessage,  HowsMyDrivingAPIRequest, SearchStatus, \
    StreamExtendedStatus, StreamingDirectMessage, StreamingStatus

LOG = logging.getLogger(__name__)


class ReplyArgumentBuilder:

    def __init__(self, api):
        self.api = api

    def build_reply_data(self,
                         message: any,
                         message_source: LookupSource):

        LOG.info(
            f'args for reply data:\n'
            f'message: {message}\n'
            f'message_source: {message_source}\n')

        lookup_request: Type[BaseLookupRequest] = None

        # why doesn't python have switch statements
        if message_source == LookupSource.STATUS:

            # Using old streaming service for a tweet longer than 140
            # characters

            if hasattr(message, TwitterAPIAttribute.EXTENDED_TWEET.value):
                LOG.debug('We have an extended tweet')

                lookup_request = StreamExtendedStatus(
                    message, TwitterMessageType.STATUS.value)

            # Using tweet api search endpoint

            elif hasattr(message, TwitterAPIAttribute.FULL_TEXT.value) and (not hasattr(message, TwitterAPIAttribute.RETWEETED_STATUS.value)):
                LOG.debug(
                    'We have a tweet from the search api endpoint')

                lookup_request = SearchStatus(
                    message, TwitterMessageType.STATUS.value)

            # Using old streaming service for a tweet of 140 characters or
            # fewer

            elif hasattr(message, TwitterAPIAttribute.ENTITIES.value) and (not hasattr(message, TwitterAPIAttribute.RETWEETED_STATUS.value)):

                LOG.debug(
                    'We are dealing with a tweet of '
                    '140 characters or fewer')

                lookup_request = StreamingStatus(
                    message, TwitterMessageType.STATUS.value)

            # Using new account api service by way of SQL table for events

            elif hasattr(message, TwitterAPIAttribute.EVENT_TYPE.value):

                LOG.debug(
                    'We are dealing with account activity api object')

                lookup_request = AccountActivityAPIStatus(
                    message, TwitterMessageType.STATUS.value)

        elif message_source == LookupSource.DIRECT_MESSAGE:

            # Using old streaming service for a direct message
            if hasattr(message, TwitterAPIAttribute.DIRECT_MESSAGE.value):

                LOG.debug(
                    'We have a direct message from the streaming service')

                lookup_request = StreamingDirectMessage(
                    message, TwitterMessageType.DIRECT_MESSAGE.value)

            # Using new direct message api endpoint

            elif hasattr(message, TwitterAPIAttribute.MESSAGE_CREATE.value):

                LOG.debug(
                    'We have a direct message from the direct message api')

                lookup_request = DirectMessageAPIDirectMessage(
                    message, TwitterMessageType.DIRECT_MESSAGE.value, self.api)

            # Using account activity api endpoint

            elif hasattr(message, TwitterAPIAttribute.EVENT_TYPE.value):

                LOG.debug(
                    'We are dealing with an account activity api object')

                lookup_request = AccountActivityAPIDirectMessage(
                    message, TwitterMessageType.DIRECT_MESSAGE.value)

        elif message_source == LookupSource.API:

            LOG.debug(
                'We are dealing with a HowsMyDrivingNY API request')

            lookup_request = HowsMyDrivingAPIRequest(message, LookupSource.API)


        if not lookup_request:

            LOG.debug('Unrecognized request type')

            lookup_request = BaseLookupRequest('unknown', 'unknown')

        return lookup_request
