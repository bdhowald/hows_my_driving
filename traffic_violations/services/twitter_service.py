import logging
import os
import pytz
import threading
import tweepy

from sqlalchemy import and_
from typing import List, Optional, Union

from traffic_violations.constants.lookup_sources import LookupSource
from traffic_violations.constants.twitter import HMDNY_TWITTER_USER_ID, TwitterMessageType

from traffic_violations.models.twitter_event import TwitterEvent
from traffic_violations.reply_argument_builder import ReplyArgumentBuilder
from traffic_violations.traffic_violations_aggregator import \
    TrafficViolationsAggregator

LOG = logging.getLogger(__name__)


class TrafficViolationsTweeter:

    DEVELOPMENT_TIME_INTERVAL = 3000.0
    MILLISECONDS_PER_SECOND = 1000.0

    MAX_DIRECT_MESSAGES_RETURNED = 50


    def __init__(self):

        # Set up application-based Twitter auth
        self.app_auth = tweepy.OAuthHandler(
            os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
        self.app_auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ[
                                   'TWITTER_ACCESS_TOKEN_SECRET'])

        # Keep reference to twitter app api
        self.app_api = tweepy.API(self.app_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                              retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))


        # Set up user-based Twitter auth
        self.client_auth = tweepy.OAuthHandler(
            os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
        self.client_auth.set_access_token(os.environ['TWITTER_CLIENT_ACCESS_TOKEN'], os.environ[
            'TWITTER_CLIENT_ACCESS_TOKEN_SECRET'])

        # Keep reference to twitter app api
        self.client_api = tweepy.API(self.client_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                              retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))

        # Create reply argument_builder
        self.reply_argument_builder = ReplyArgumentBuilder(self.app_api)

        # Create new aggregator
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

    def _add_twitter_events_for_missed_direct_messages(self, messages: List[tweepy.Status]) -> None:
        """Creates TwitterEvent objects when the Account Activity API fails to send us
        direct message events via webhooks to have them created by the HowsMyDrivingNY API.

        :param messages: List[tweepy.DirectMessage]: The direct messages returned via the
                                                     Twitter Search API via Tweepy.
        """

        undetected_messages = 0

        sender_ids = set(int(message.message_create['sender_id']) for message in messages)

        sender_objects = self.client_api.lookup_users(sender_ids)
        senders = {sender.id_str:sender for sender in sender_objects}

        for message in messages:

            existing_event: Optional[TwitterEvent] = TwitterEvent.query.filter(TwitterEvent.event_id == message.id).first()

            if not existing_event:
                undetected_messages += 1

                sender = senders[message.message_create['sender_id']]

                event: TwitterEvent = TwitterEvent(
                    event_type=TwitterMessageType.DIRECT_MESSAGE.value,
                    event_id=message.id,
                    user_handle=sender.screen_name,
                    user_id=sender.id,
                    event_text=message.message_create['message_data']['text'],
                    created_at=message.created_timestamp,
                    in_reply_to_message_id=None,
                    location=None,
                    user_mentions=' '.join([user['screen_name'] for user in message.message_create['message_data']['entities']['user_mentions']]),
                    detected_via_account_activity_api=False)

                TwitterEvent.query.session.add(event)

        TwitterEvent.query.session.commit()

        LOG.debug(
            f"Found {undetected_messages} direct message{'' if undetected_messages == 1 else 's'} that "
            f"{'was' if undetected_messages == 1 else 'were'} previously undetected.")


    def _add_twitter_events_for_missed_statuses(self, messages: List[tweepy.Status]):
        """Creates TwitterEvent objects when the Account Activity API fails to send us
        status events via webhooks to have them created by the HowsMyDrivingNY API.

        :param messages: List[tweepy.Status]: The statuses returned via the Twitter
                                              Search API via Tweepy.
        """

        undetected_messages = 0

        for message in messages:
            existing_event: Optional[TwitterEvent] = TwitterEvent.query.filter(TwitterEvent.event_id == message.id).first()

            if not existing_event:
                undetected_messages += 1

                event: TwitterEvent = TwitterEvent(
                    event_type=TwitterMessageType.STATUS.value,
                    event_id=message.id,
                    user_handle=message.user.screen_name,
                    user_id=message.user.id,
                    event_text=message.full_text,
                    created_at=message.created_at.replace(tzinfo=pytz.timezone('UTC')).timestamp() * self.MILLISECONDS_PER_SECOND,
                    in_reply_to_message_id=message.in_reply_to_status_id,
                    location=message.place and message.place.full_name,
                    user_mentions=' '.join([user['screen_name'] for user in message.entities['user_mentions']]),
                    detected_via_account_activity_api=False)

                TwitterEvent.query.session.add(event)

        TwitterEvent.query.session.commit()

        LOG.debug(
            f"Found {undetected_messages} status{'' if undetected_messages == 1 else 'es'} that "
            f"{'was' if undetected_messages == 1 else 'were'} previously undetected.")

    def _find_and_respond_to_missed_direct_messages(self) -> None:
        """Uses Tweepy to call the Twitter Search API to find direct messages to/from
        HowsMyDrivingNY. It then passes this data to a function that creates
        TwitterEvent objects when those direct message events have not already been recorded.
        """

        interval = 300.0 if self._is_production() else self.DEVELOPMENT_TIME_INTERVAL

        self.statuses_iteration += 1
        LOG.debug(
            f'Looking up missed direct messages on iteration {self.direct_messages_iteration}')

        # start timer
        threading.Timer(
            interval, self._find_and_respond_to_missed_direct_messages).start()

        try:
            # most_recent_undetected_twitter_event = TwitterEvent.query.filter(
            #     and_(TwitterEvent.detected_via_account_activity_api == False,
            #          TwitterEvent.event_type == TwitterMessageType.DIRECT_MESSAGE.value)
            # ).order_by(TwitterEvent.event_id.desc()).first()

            # Tweepy bug with cursors prevents us from searching for more than 50 events
            # at a time until 3.9, so it'll have to do.

            direct_messages_since_last_twitter_event = self.client_api.list_direct_messages(
                    count=self.MAX_DIRECT_MESSAGES_RETURNED)

            received_messages = [message for message in direct_messages_since_last_twitter_event if
                int(message.message_create['sender_id']) != HMDNY_TWITTER_USER_ID]

            self._add_twitter_events_for_missed_direct_messages(messages=received_messages)

        except Exception as e:
            LOG.error(e)
            LOG.error(str(e))
            LOG.error(e.args)
            logging.exception("stack trace")

        finally:
            TwitterEvent.query.session.close()

    def _find_and_respond_to_missed_statuses(self):
        """Uses Tweepy to call the Twitter Search API to find statuses mentioning
        HowsMyDrivingNY. It then passes this data to a function that creates
        TwitterEvent objects when those status events have not already been recorded.
        """

        interval = 300.0 if self._is_production() else self.DEVELOPMENT_TIME_INTERVAL

        self.statuses_iteration += 1
        LOG.debug(
            f'Looking up missed statuses on iteration {self.statuses_iteration}')

        # start timer
        threading.Timer(
            interval, self._find_and_respond_to_missed_statuses).start()

        try:
            # Find most recent undetected twitter status event, and then
            # search for recent events until we can find no more.
            most_recent_undetected_twitter_event = TwitterEvent.query.filter(
                and_(TwitterEvent.detected_via_account_activity_api == False,
                     TwitterEvent.event_type == TwitterMessageType.STATUS.value)
            ).order_by(TwitterEvent.event_id.desc()).first()

            if most_recent_undetected_twitter_event:

                statuses_since_last_twitter_event: List[tweepy.Status] = []
                max_status_id: Optional[int] = None

                while max_status_id is None or statuses_since_last_twitter_event:
                    statuses_since_last_twitter_event = self.client_api.mentions_timeline(
                        max_id=max_status_id, since_id=most_recent_undetected_twitter_event.event_id, tweet_mode='extended')

                    self._add_twitter_events_for_missed_statuses(statuses_since_last_twitter_event)

                    if statuses_since_last_twitter_event:
                        max_status_id = statuses_since_last_twitter_event[-1].id - 1
                    else:
                        max_status_id = most_recent_undetected_twitter_event.event_id - 1

        except Exception as e:
            LOG.error(e)
            LOG.error(str(e))
            LOG.error(e.args)
            logging.exception("stack trace")

        finally:
            TwitterEvent.query.session.close()

    def _find_and_respond_to_twitter_events(self):
        """Looks for TwitterEvent objects that have not yet been responded to and
        begins the process of creating a response. Additionally, failed events are
        rerun to provide a correct response, particularly useful in cases where
        external apis are down for maintenance.
        """

        interval = 3.0 if self._is_production() else self.DEVELOPMENT_TIME_INTERVAL

        self.events_iteration += 1
        LOG.debug(
            f'Looking up twitter events on iteration {self.events_iteration}')

        # start timer
        threading.Timer(
            interval, self._find_and_respond_to_twitter_events).start()

        try:
            new_events: [List[TwitterEvent]] = TwitterEvent.get_all_by(
                is_duplicate=False,
                responded_to=False,
                response_in_progress=False)

            LOG.debug(f'new events: {new_events}')

            failed_events: [List[TwitterEvent]] = TwitterEvent.get_all_by(
                is_duplicate=False,
                error_on_lookup=True,
                responded_to=True,
                response_in_progress=False)

            LOG.debug(f'failed events: {failed_events}')

            events_to_respond_to: [List[TwitterEvent]] = new_events + failed_events

            LOG.debug(f'events to respond to: {events_to_respond_to}')

            for event in events_to_respond_to:

                LOG.debug(f'Beginning response for event: {event.id}')

                # search for duplicates
                is_event_duplicate: bool = TwitterEvent.query.filter_by(
                    event_type=event.event_type,
                    event_id=event.event_id,
                    user_handle=event.user_handle,
                    responded_to=True
                ).filter(
                    TwitterEvent.id != event.id).count() > 0

                if is_event_duplicate:
                    event.is_duplicate = True

                    TwitterEvent.query.session.commit()

                    LOG.info(f'Event {event.id} is a duplicate, skipping.')

                else:

                    event.response_in_progress = True
                    TwitterEvent.query.session.commit()

                    try:
                        message_source = LookupSource(event.event_type)

                        # build request
                        lookup_request: Type[BaseLookupRequest] = self.reply_argument_builder.build_reply_data(
                            message=event,
                            message_source=message_source)

                        # Reply to the event.
                        reply_event = self.aggregator.initiate_reply(
                            lookup_request=lookup_request)
                        success = reply_event.get('success', False)

                        if success:
                            # There's need to tell people that there was an error more than once
                            if not (reply_event.get(
                                    'error_on_lookup') and event.error_on_lookup):

                                try:
                                    self._process_response(reply_event)
                                except tweepy.error.TweepError as e:
                                    reply_event['error_on_lookup'] = True

                            # We've responded!
                            event.response_in_progress = False
                            event.responded_to = True

                            # Update error status
                            if reply_event.get('error_on_lookup'):
                                event.error_on_lookup = True
                            else:
                                event.error_on_lookup = False

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

    def _find_and_respond_to_requests(self):
        """Convenience method to collect the different ways TwitterEvent
        objects are created, found, and responded to and begin the process
        of calling these methods at process start.
        """
        self._find_and_respond_to_missed_direct_messages()
        self._find_and_respond_to_missed_statuses()
        self._find_and_respond_to_twitter_events()

    def _is_production(self):
        """Determines if we are running in production to see if we can create
        direct message and status responses.

        TODO: Come up with a better way to determine production environment.
        """
        return os.environ.get('ENV') == 'production'

    def _process_response(self, reply_event_args):
        """Directs the response to a Twitter message, depending on whether
        or not the event is a direct message or a status. Statuses that mention
        HowsMyDrivingNY earn a favorite/like.
        """

        request_object = reply_event_args.get('request_object')

        message_source = request_object.message_source if request_object else None
        message_id = request_object.external_id() if request_object else None

        # Respond to user
        if message_source == LookupSource.DIRECT_MESSAGE.value:

            LOG.debug('responding as direct message')

            combined_message = self._recursively_process_direct_messages(
                reply_event_args.get('response_parts', {}))

            LOG.debug(f'combined_message: {combined_message}')

            self._is_production() and self.app_api.send_direct_message(
                recipient_id=request_object.user_id if request_object else None,
                text=combined_message)

        elif message_source == LookupSource.STATUS.value:
            # If we have at least one successful lookup, favorite the status
            if reply_event_args.get('successful_lookup', False):

                # Favorite every look-up from a status
                try:
                    self._is_production() and self.app_api.create_favorite(message_id)

                # But don't crash on error
                except tweepy.error.TweepError as te:
                    # There's no easy way to know if this status has already
                    # been favorited
                    pass

            LOG.debug('responding as status update')

            self._recursively_process_status_updates(
                response_parts=reply_event_args.get('response_parts', {}),
                message_id=message_id)

        else:
            LOG.error('Unkown message source. Cannot respond.')

    def _recursively_process_direct_messages(self, response_parts):
        """Direct message responses from the aggregator return lists
        of chunked information (by violation type, by borough, by year, etc.).
        Data look like:

        [
          [data_type_1_part_1, data_type_1_part_2, data_type_1_part_3],
          [data_type_2_part_1, data_type_2_part_2],
          [data_type_3_part_1, data_type_3_part_2, data_type_3_part_3],
        ]

        This ensures that the data is grouped with like data. Using recursion,
        the final message is built into one large message.
        """

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
                                            message_id: Optional[int] = None):

        """Status responses from the aggregator return lists
        of chunked information (by violation type, by borough, by year, etc.).
        Data look like:

        [
          [data_type_1_part_1, data_type_1_part_2, data_type_1_part_3],
          [data_type_2_part_1, data_type_2_part_2],
          [data_type_3_part_1, data_type_3_part_2, data_type_3_part_3],
        ]

        This ensures that the data is grouped with like data. Using recursion,
        response statuses are created, then their message ids are saved to be
        used as the in_reply_to_status_id for the next status.
        """

        # Iterate through all response parts
        for part in response_parts:
            # Some may be lists themselves
            if isinstance(part, list):
                message_id = self._recursively_process_status_updates(
                    response_parts=part,
                    message_id=message_id)
            else:
                if self._is_production():
                    new_message = self.app_api.update_status(
                        status=part,
                        in_reply_to_status_id=message_id,
                        auto_populate_reply_metadata=True)
                    message_id = new_message.id

                    LOG.debug(f'message_id: {message_id}')
                else:
                    LOG.debug(
                        "This is where 'self.app_api.update_status(part, in_reply_to_status_id = message_id)' would be called in production.")

        return message_id
