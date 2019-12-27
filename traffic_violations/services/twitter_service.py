import logging
import os
import pytz
import threading
import tweepy

from typing import List, Optional, Union

from traffic_violations.constants.lookup_sources import LookupSource

from traffic_violations.models.twitter_event import TwitterEvent
from traffic_violations.reply_argument_builder import ReplyArgumentBuilder
from traffic_violations.traffic_violations_aggregator import \
    TrafficViolationsAggregator

LOG = logging.getLogger(__name__)


class TrafficViolationsTweeter:

    def __init__(self):

        # Set up Twitter auth
        self.auth = tweepy.OAuthHandler(
            os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
        self.auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ[
                                   'TWITTER_ACCESS_TOKEN_SECRET'])

        # keep reference to twitter api
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                              retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))

        # create reply argument_builder
        self.reply_argument_builder = ReplyArgumentBuilder(self.api)

        # create new aggregator
        self.aggregator = TrafficViolationsAggregator()

        # Log how many times we've called the apis
        self.direct_messages_iteration = 0
        self.events_iteration = 0
        self.statuses_iteration = 0

    def send_status(self,
                    message_parts: Union[List[any], List[str]],
                    on_error_message: str) -> bool:
        try:
            self._recursively_process_status_updates(message_parts)

            return True
        except Exception as e:
            LOG.error(e)
            self._recursively_process_status_updates(on_error_message)

            return False

    def _find_and_respond_to_twitter_events(self):
        interval = 3.0 if self._is_production() else 3000.0

        self.events_iteration += 1
        LOG.debug(
            f'Looking up twitter events on iteration {self.events_iteration}')

        # start timer
        threading.Timer(
            interval, self._find_and_respond_to_twitter_events).start()

        try:
            events: Optional[List[TwitterEvent]] = TwitterEvent.get_all_by(
                responded_to=0, response_begun=0)

            LOG.debug(f'events: {events}')

            for event in events:

                LOG.debug(f'Beginning response for event: {event.id}')

                event.response_begun = True

                try:
                    message_source = LookupSource(event.event_type)

                    # build request
                    lookup_request: Type[BaseLookupRequest] = self.reply_argument_builder.build_reply_data(
                        message=event,
                        message_source=message_source)

                    # Reply to the event.
                    reply_event = self.aggregator.initiate_reply(lookup_request)
                    success = reply_event.get('success', False)

                    if success:
                        # Need username for statuses
                        reply_event['username'] = event.user_handle

                        self._process_response(reply_event)

                        # We've responded!
                        event.responded_to = 1

                        if reply_event.get('error_on_lookup'):
                            event.error_on_lookup = True

                    TwitterEvent.query.session.commit()

                except ValueError as e:
                    LOG.error(
                        f'Encountered unknown event type. '
                        f'Response is not possible.')

        except Exception as e:

            LOG.error(e)
            LOG.error(str(e))
            LOG.error(e.args)
            logging.exception("stack trace")

        finally:
            TwitterEvent.query.session.close()

    def _find_messages_to_respond_to(self):
        self._find_and_respond_to_twitter_events()

    def _is_production(self):
        return os.environ.get('ENV') == 'production'

    def _process_response(self, reply_event_args):
        request_object = reply_event_args.get('request_object')

        message_source = request_object.message_source if request_object else None
        message_id = request_object.external_id() if request_object else None

        # Respond to user
        if message_source == LookupSource.DIRECT_MESSAGE.value:

            LOG.debug('responding as direct message')

            combined_message = self._recursively_process_direct_messages(
                reply_event_args.get('response_parts', {}))

            LOG.debug(f'combined_message: {combined_message}')

            self._is_production() and self.api.send_direct_message(
                recipient_id=request_object.user_id if request_object else None,
                text=combined_message)

        elif message_source == LookupSource.STATUS.value:
            # If we have at least one successful lookup, favorite the status
            if reply_event_args.get('successful_lookup', False):

                # Favorite every look-up from a status
                try:
                    self._is_production() and self.api.create_favorite(message_id)

                # But don't crash on error
                except tweepy.error.TweepError as te:
                    # There's no easy way to know if this status has already
                    # been favorited
                    pass

            LOG.debug('responding as status update')

            self._recursively_process_status_updates(reply_event_args.get(
                'response_parts', {}), message_id, reply_event_args['username'])

        else:
            LOG.error('Unkown message source. Cannot respond.')

    def _recursively_process_direct_messages(self, response_parts):

        return_message = []

        # Iterate through all response parts
        for part in response_parts:
            if isinstance(part, list):
                return_message.append(
                    self._recursively_process_direct_messages(part))
            else:
                return_message.append(part)

        return '\n'.join(return_message)

    def _recursively_process_status_updates(self,
                                            response_parts: Union[List[any], List[str]],
                                            message_id: Optional[int] = None,
                                            username: Optional[str] = None):

        # Iterate through all response parts
        for part in response_parts:
            # Some may be lists themselves
            if isinstance(part, list):
                message_id = self._recursively_process_status_updates(
                    part, message_id, username)
            else:
                if self._is_production():
                    new_message = self.api.update_status(
                        '@' + username + ' ' + part, in_reply_to_status_id=message_id)
                    message_id = new_message.id

                    LOG.debug(f'message_id: {message_id}')
                else:
                    LOG.debug(
                        "This is where 'self.api.update_status(part, in_reply_to_status_id = message_id)' would be called in production.")

        return message_id
