import logging
import os
import pytz
import threading
import tweepy

from datetime import datetime
from sqlalchemy import and_
from typing import Any, List, Optional, Type, Union

from traffic_violations.constants import L10N
from traffic_violations.constants.lookup_sources import LookupSource
from traffic_violations.constants.time import (MILLISECONDS_PER_SECOND,
    SECONDS_PER_MINUTE)
from traffic_violations.constants.twitter import HMDNY_TWITTER_USER_ID, TwitterMessageType

from traffic_violations.models.lookup_requests import BaseLookupRequest
from traffic_violations.models.non_follower_reply import NonFollowerReply
from traffic_violations.models.twitter_event import TwitterEvent
from traffic_violations.reply_argument_builder import ReplyArgumentBuilder
from traffic_violations.traffic_violations_aggregator import \
    TrafficViolationsAggregator

LOG = logging.getLogger(__name__)


class TrafficViolationsTweeter:

    PRODUCTION_APP_RATE_LIMITING_INTERVAL_IN_SECONDS = 3.0

    DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS = 3000.0
    PRODUCTION_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS = 300.0

    FOLLOWERS_RATE_LIMITING_INTERVAL_IN_SECONDS = 15 * SECONDS_PER_MINUTE

    MAX_DIRECT_MESSAGES_RETURNED = 50


    def __init__(self):

        self._app_api = None
        self._client_api = None

        # Create reply argument_builder
        self.reply_argument_builder = ReplyArgumentBuilder(self._get_twitter_application_api())

        # Create new aggregator
        self.aggregator = TrafficViolationsAggregator()

        # Log how many times we've called the apis
        self.direct_messages_iteration = 0
        self.events_iteration = 0
        self.statuses_iteration = 0

        # Initialize cached values to None
        self._follower_ids: Optional[List[int]] = None
        self._follower_ids_last_fetched: Optional[datetime] = None

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

        sender_objects = self._get_twitter_client_api().lookup_users(sender_ids)
        senders = {sender.id_str:sender for sender in sender_objects}

        for message in messages:

            existing_event: Optional[TwitterEvent] = TwitterEvent.query.filter(TwitterEvent.event_id == message.id).first()

            if not existing_event and int(message.message_create['sender_id']) != HMDNY_TWITTER_USER_ID:
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

            if not existing_event and message.user.id != HMDNY_TWITTER_USER_ID:
                undetected_messages += 1

                event: TwitterEvent = TwitterEvent(
                    event_type=TwitterMessageType.STATUS.value,
                    event_id=message.id,
                    user_handle=message.user.screen_name,
                    user_id=message.user.id,
                    event_text=message.full_text,
                    created_at=message.created_at.replace(tzinfo=pytz.timezone('UTC')).timestamp() * MILLISECONDS_PER_SECOND,
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

        interval = (self.PRODUCTION_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS if self._is_production()
            else self.DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS)

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

            direct_messages_since_last_twitter_event = self._get_twitter_client_api(
                ).list_direct_messages(
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

        interval = (self.PRODUCTION_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS if self._is_production()
            else self.DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS)

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
                    statuses_since_last_twitter_event = self._get_twitter_client_api(
                        ).mentions_timeline(
                            max_id=max_status_id,
                            since_id=most_recent_undetected_twitter_event.event_id,
                            tweet_mode='extended')

                    if statuses_since_last_twitter_event:
                        self._add_twitter_events_for_missed_statuses(statuses_since_last_twitter_event)
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

        interval = (
            self.PRODUCTION_APP_RATE_LIMITING_INTERVAL_IN_SECONDS if self._is_production()
            else self.DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS)

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
                self._process_twitter_event(event=event)

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

    def _get_twitter_application_api(self):
        """Set the application (non-client) api connection for this instance"""

        if not self._app_api:
            # Set up application-based Twitter auth
            app_auth = tweepy.OAuthHandler(
                os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
            app_auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ[
                                    'TWITTER_ACCESS_TOKEN_SECRET'])

            # Keep reference to twitter app api
            self._app_api = tweepy.API(app_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                                retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))

        return self._app_api

    def _get_twitter_client_api(self):
        """Set the client (non-client) api connection for this instance"""

        if not self._client_api:
            # Set up user-based Twitter auth
            client_auth = tweepy.OAuthHandler(
                os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
            client_auth.set_access_token(os.environ['TWITTER_CLIENT_ACCESS_TOKEN'], os.environ[
                'TWITTER_CLIENT_ACCESS_TOKEN_SECRET'])

            # Keep reference to twitter app api
            self._client_api = tweepy.API(client_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                                retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))

        return self._client_api


    def _get_follower_ids(self):
        """Get list of followers from Twitter every 15 minutes.

        This is used to determine who should be prompted to like
        the reply tweet in order to trigger a response.

        If the cached value is older than 15 minutes, refetch.
        """
        now = datetime.utcnow()

        # If cache is empty or stale, refetch.
        if not self._follower_ids_last_fetched or (
            ((now - self._follower_ids_last_fetched).seconds
                > self.FOLLOWERS_RATE_LIMITING_INTERVAL_IN_SECONDS)):

            follower_ids = []
            next_cursor: int = -1

            while next_cursor:
                results, cursors = self._get_twitter_application_api().followers_ids(cursor=next_cursor)
                next_cursor = cursors[1]
                follower_ids += results

            # set follower_ids
            self._follower_ids = follower_ids

            # cache current time
            self._follower_ids_last_fetched = now

        # Return cached or fetched value.
        return self._follower_ids

    def _is_production(self):
        """Determines if we are running in production to see if we can create
        direct message and status responses.

        TODO: Come up with a better way to determine production environment.
        """
        return os.environ.get('ENV') == 'production'

    def _process_response(self,
        request_object: Type[BaseLookupRequest],
        response_parts: List[Any],
        successful_lookup: bool = False) -> Optional[int]:

        """Directs the response to a Twitter message, depending on whether
        or not the event is a direct message or a status. Statuses that mention
        HowsMyDrivingNY earn a favorite/like.
        """

        message_source = request_object.message_source if request_object else None
        message_id = request_object.external_id() if request_object else None

        # Respond to user
        if message_source == LookupSource.DIRECT_MESSAGE.value:

            LOG.debug('responding as direct message')

            combined_message = self._recursively_compile_direct_messages(
                response_parts)

            LOG.debug(f'combined_message: {combined_message}')

            return self._send_direct_message(message=combined_message,
                                             recipient_id=request_object.user_id)

        elif message_source == LookupSource.STATUS.value:
            # If we have at least one successful lookup, favorite the status
            if successful_lookup:

                # Favorite every look-up from a status
                try:
                    self._is_production() and self._get_twitter_application_api(
                        ).create_favorite(message_id)

                # But don't crash on error
                except tweepy.error.TweepError as te:
                    # There's no easy way to know if this status has already
                    # been favorited
                    pass

            LOG.debug('responding as status update')

            return self._recursively_process_status_updates(
                response_parts=response_parts,
                message_id=message_id)

        else:
            LOG.error('Unkown message source. Cannot respond.')

    def _process_twitter_event(self, event: TwitterEvent):
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
                message_source: str = LookupSource(event.event_type)

                # build request
                lookup_request: Type[BaseLookupRequest] = self.reply_argument_builder.build_reply_data(
                    message=event,
                    message_source=message_source)

                user_is_follower = lookup_request.requesting_user_is_follower(
                    follower_ids=self._get_follower_ids())

                perform_lookup_for_user: bool = (user_is_follower or
                    event.user_favorited_non_follower_reply)

                if self.aggregator.lookup_has_valid_plates(
                    lookup_request=lookup_request) and not perform_lookup_for_user:

                    response_parts: List[Any]

                    if lookup_request.is_direct_message():
                        response_parts = [L10N.NON_FOLLOWER_DIRECT_MESSAGE_REPLY_STRING]
                    elif lookup_request.is_status():
                        response_parts = [L10N.NON_FOLLOWER_TWEET_REPLY_STRING]

                    try:
                        reply_message_id = self._process_response(
                            request_object=lookup_request,
                            response_parts=response_parts)

                        # Save the reply id, so that when the user favorites it,
                        # we can trigger the search.
                        non_follower_reply = NonFollowerReply(
                            created_at=(int(datetime.utcnow().timestamp() *
                                MILLISECONDS_PER_SECOND)),
                            event_type=event.event_type,
                            event_id=reply_message_id,
                            in_reply_to_message_id=event.event_id,
                            user_handle=event.user_handle,
                            user_id=event.user_id)

                        NonFollowerReply.query.session.add(
                            non_follower_reply)
                        NonFollowerReply.query.session.commit()

                    except tweepy.error.TweepError as e:
                        event.error_on_lookup = True
                else:
                    # Reply to the event.
                    reply_to_event = self.aggregator.initiate_reply(
                        lookup_request=lookup_request)

                    success = reply_to_event['success']

                    if success:
                        # There's no need to tell people that
                        # there was an error more than once.
                        if not (reply_to_event[
                            'error_on_lookup'] and event.error_on_lookup):

                            try:
                                self._process_response(
                                    request_object=reply_to_event['request_object'],
                                    response_parts=reply_to_event['response_parts'],
                                    successful_lookup=reply_to_event.get('successful_lookup'))
                            except tweepy.error.TweepError as e:
                                reply_to_event['error_on_lookup'] = True

                    # Update error status
                    if reply_to_event['error_on_lookup']:
                        event.error_on_lookup = True
                    else:
                        event.error_on_lookup = False

                # We've responded!
                event.response_in_progress = False
                event.responded_to = True

                TwitterEvent.query.session.commit()

            except ValueError as e:
                LOG.error(
                    f'Encountered unknown event type. '
                    f'Response is not possible.')

    def _recursively_compile_direct_messages(self, response_parts):
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
                    self._recursively_compile_direct_messages(part))
            else:
                return_message.append(part)

        return '\n'.join(return_message)

    def _recursively_process_status_updates(self,
                                            response_parts: Union[List[any], List[str]],
                                            message_id: Optional[int] = None) -> Optional[int]:

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
                    new_message = self._get_twitter_application_api(
                        ).update_status(
                            status=part,
                            in_reply_to_status_id=message_id,
                            auto_populate_reply_metadata=True)
                    message_id = new_message.id

                    LOG.debug(f'message_id: {message_id}')
                else:
                    LOG.debug(
                        "This is where 'self._get_twitter_application_api()"
                        ".update_status(status=part, in_reply_to_status_id=message_id, "
                        "auto_populate_reply_metadata=True)' would be called in production.")
                    return None

        return message_id

    def _send_direct_message(self, message: str, recipient_id: int) -> Optional[int]:
        """Send a direct message to a Twitter user."""

        if self._is_production():
            new_message = self._get_twitter_application_api().send_direct_message(
                recipient_id=recipient_id,
                text=message)
            return new_message.id
        else:
            LOG.debug(
                "This is where 'self._get_twitter_application_api()"
                ".send_direct_message(recipient_id=recipient_id, "
                "text=message)' would be called in production.")
            return None