import logging
import pytz
import re

from datetime import datetime, timezone

from traffic_violations.models.lookup_requests import BaseLookupRequest, \
    AccountActivityAPIDirectMessage, AccountActivityAPIStatus, DirectMessageAPIDirectMessage, \
    HowsMyDrivingAPIRequest, SearchStatus, StreamExtendedStatus, StreamingDirectMessage, StreamingStatus


class ReplyArgumentBuilder:

    def __init__(self, api):
        self.logger = logging.getLogger('hows_my_driving')
        self.api = api

    def build_reply_data(self, message, message_source, message_type):

        # Print args
        self.logger.info('args:')
        self.logger.info('message: %s', message)
        self.logger.info('message_source: %s', message_source)
        self.logger.info('message_type: %s', message_type)

        lookup_request = None

        # why doesn't python have switch statements
        if message_source == "twitter":

            if message_type == 'status':

                # Using old streaming service for a tweet longer than 140
                # characters

                if hasattr(message, 'extended_tweet'):
                    self.logger.debug('\n\nWe have an extended tweet\n\n')

                    lookup_request = StreamExtendedStatus(
                        message, message_source, message_type)

                # Using tweet api search endpoint

                elif hasattr(message, 'full_text') and (not hasattr(message, 'retweeted_status')):
                    self.logger.debug(
                        '\n\nWe have a tweet from the search api endpoint\n\n')

                    lookup_request = SearchStatus(
                        message, message_source, message_type)

                # Using old streaming service for a tweet of 140 characters or
                # fewer

                elif hasattr(message, 'entities') and (not hasattr(message, 'retweeted_status')):

                    self.logger.debug(
                        '\n\nWe are dealing with a tweet of 140 characters or fewer\n\n')

                    lookup_request = StreamingStatus(
                        message, message_source, message_type)

                # Using new account api service by way of SQL table for events

                elif type(message) == dict and 'event_type' in message:

                    self.logger.debug(
                        '\n\nWe are dealing with account activity api object\n\n')

                    lookup_request = AccountActivityAPIStatus(
                        message, message_source, message_type)

            elif message_type == 'direct_message':

                # Using old streaming service for a direct message

                if hasattr(message, 'direct_message'):

                    self.logger.debug(
                        '\n\nWe have a direct message from the streaming service\n\n')

                    lookup_request = StreamingDirectMessage(
                        message, message_source, message_type)

                # Using new direct message api endpoint

                elif hasattr(message, 'message_create'):

                    self.logger.debug(
                        '\n\nWe have a direct message from the direct message api\n\n')

                    lookup_request = DirectMessageAPIDirectMessage(
                        message, message_source, message_type, self.api)

                # Using account activity api endpoint

                elif 'event_type' in message:

                    self.logger.debug(
                        '\n\nWe are dealing with an account activity api object\n\n')

                    lookup_request = AccountActivityAPIDirectMessage(
                        message, message_source, message_type)

        elif message_source == 'api':

            self.logger.debug(
                '\n\nWe are dealing with a HowsMyDrivingNY API request\n\n')

            lookup_request = HowsMyDrivingAPIRequest(
                message, message_source, message_type)

        else:

            self.logger.debug('\n\nUnrecognized request type\n\n')

            lookup_request = BaseLookupRequest(None, 'unknown', 'unknown')

        return lookup_request
