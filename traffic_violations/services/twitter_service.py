import logging
import os
import pytz
import threading
import time
import tweepy

from datetime import datetime, timedelta
from sqlalchemy import and_
from typing import Any, List, Optional, Tuple, Type, Union

from traffic_violations import settings
from traffic_violations.constants import L10N
from traffic_violations.constants.lookup_sources import LookupSource
from traffic_violations.constants.time import (MILLISECONDS_PER_SECOND,
    SECONDS_PER_MINUTE)
from traffic_violations.constants.twitter import HMDNY_TWITTER_USER_ID, TwitterMessageType

from traffic_violations.models.lookup_requests import BaseLookupRequest
from traffic_violations.models.non_follower_reply import NonFollowerReply
from traffic_violations.models.twitter_event import TwitterEvent
from traffic_violations.reply_argument_builder import ReplyArgumentBuilder
from traffic_violations.services.apis.tweet_detection_service import (
    TweetDetectionService)
from traffic_violations.services.apis import twitter_api_wrapper
from traffic_violations.traffic_violations_aggregator import (
    TrafficViolationsAggregator)

LOG = logging.getLogger(__name__)


class TrafficViolationsTweeter:

    PRODUCTION_APP_RATE_LIMITING_INTERVAL_IN_SECONDS = 30.0

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

        # Need to find if unanswered statuses have been deleted
        self.tweet_detection_service = TweetDetectionService()

        # Log how many times we've called the apis
        self._direct_messages_iteration = 0
        self._events_iteration = 0
        self._statuses_iteration = 0

        self._lookup_threads = []

        # Initialize cached values to None
        self._follower_ids: Optional[List[int]] = None
        self._follower_ids_last_fetched: Optional[datetime] = None


    def find_and_respond_to_requests(self) -> None:
        """Convenience method to collect the different ways TwitterEvent
        objects are created, found, and responded to and begin the process
        of calling these methods at process start.
        """
        self._find_and_respond_to_missed_direct_messages()
        self._find_and_respond_to_missed_statuses()
        self._find_and_respond_to_twitter_events()


    def send_status(self,
                    message_parts: Union[List[any], List[str]],
                    on_error_message: str) -> bool:
        """Send statuses from @HowsMyDrivingNY"""
        try:
            self._recursively_process_status_updates(message_parts)

            return True
        except Exception as e:
            LOG.error(e)
            self._recursively_process_status_updates(on_error_message)

            return False

    def terminate_lookups(self) -> None:
        """Stop looking for twitter events, statuses, or direct messages to respond to."""
        for thread in self._lookup_threads:
            thread.cancel()

    def _add_twitter_events_for_missed_direct_messages(self, messages: List[tweepy.models.Status]) -> None:
        """Creates TwitterEvent objects when the Account Activity API fails to send us
        direct message events via webhooks to have them created by the HowsMyDrivingNY API.

        :param messages: List[tweepy.DirectMessage]: The direct messages returned via the
                                                     Twitter Search API via Tweepy.
        """

        undetected_messages = 0

        sender_ids = set(int(message.message_create['sender_id']) for message in messages)

        sender_objects = self._get_twitter_client_api().lookup_users(user_id=sender_ids)
        senders = {sender.id_str:sender for sender in sender_objects}

        messages_in_chronological_order = sorted(messages, key=lambda m: m.id)

        for message in messages_in_chronological_order:

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
                    user_mention_ids=','.join([user['id_str'] for user in message.message_create['message_data']['entities']['user_mentions']]),
                    user_mentions=' '.join([user['screen_name'] for user in message.message_create['message_data']['entities']['user_mentions']]),
                    detected_via_account_activity_api=False)

                TwitterEvent.query.session.add(event)

        TwitterEvent.query.session.commit()

        LOG.debug(
            f"Found {undetected_messages} direct message{'' if undetected_messages == 1 else 's'} that "
            f"{'was' if undetected_messages == 1 else 'were'} previously undetected.")


    def _add_twitter_events_for_missed_statuses(self, messages: List[tweepy.models.Status]) -> None:
        """Creates TwitterEvent objects when the Account Activity API fails to send us
        status events via webhooks to have them created by the HowsMyDrivingNY API.

        :param messages: List[tweepy.models.Status]: The statuses returned via the Twitter
                                                     Search API via Tweepy.
        """

        undetected_messages = 0

        messages_in_chronological_order = sorted(messages, key=lambda m: m.id)

        for message in messages_in_chronological_order:
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
                    user_mention_ids=','.join([user['id_str'] for user in message.entities['user_mentions']]),
                    user_mentions=' '.join([user['screen_name'] for user in message.entities['user_mentions']]),
                    detected_via_account_activity_api=False)

                TwitterEvent.query.session.add(event)

        TwitterEvent.query.session.commit()

        LOG.debug(
            f"Found {undetected_messages} status{'' if undetected_messages == 1 else 'es'} that "
            f"{'was' if undetected_messages == 1 else 'were'} previously undetected.")


    def _filter_failed_twitter_events(self, failed_events: List[TwitterEvent]) -> List[TwitterEvent]:
        failed_events_that_need_response: List[TwitterEvent] = []

        for failed_event in failed_events:
            # If this event has failed five or more times, give up.
            if failed_event.num_times_failed >= 5:
                failed_event.error_on_lookup = False
                failed_event.query.session.commit()

                continue

            LOG.info(f'failed_event.event_type: {failed_event.event_type}')

            # If event is a tweet, but can no longer be found, there's nothing we can do.
            if (failed_event.event_type == TwitterMessageType.STATUS.value and
                not self.tweet_detection_service.tweet_exists(id=failed_event.event_id,
                                                          username=failed_event.user_handle)):

                LOG.info(f'Status of failed reply cannot be found.')

                failed_event.error_on_lookup = False
                failed_event.num_times_failed = 0
                failed_event.last_failed_at_time = None

                failed_event.query.session.commit()

                continue

            if failed_event.num_times_failed == 0:
                failed_events_that_need_response.append(failed_event)

            elif failed_event.num_times_failed == 1:
                time_to_retry = failed_event.last_failed_at_time + timedelta(minutes=5)
                if time_to_retry <= datetime.utcnow():
                    failed_events_that_need_response.append(failed_event)

            elif failed_event.num_times_failed == 2:
                time_to_retry = failed_event.last_failed_at_time + timedelta(hours=1)
                if time_to_retry <= datetime.utcnow():
                    failed_events_that_need_response.append(failed_event)

            elif failed_event.num_times_failed == 3:
                time_to_retry = failed_event.last_failed_at_time + timedelta(hours=3)
                if time_to_retry <= datetime.utcnow():
                    failed_events_that_need_response.append(failed_event)

            elif failed_event.num_times_failed == 4:
                time_to_retry = failed_event.last_failed_at_time + timedelta(days=1)
                if time_to_retry <= datetime.utcnow():
                    failed_events_that_need_response.append(failed_event)

            else:
                LOG.debug(f'Event response cannot be retried automatically.')


        LOG.debug(f'IDs of failed events to retry: {[event.id for event in failed_events_that_need_response]}')

        return failed_events_that_need_response


    def _find_and_respond_to_missed_direct_messages(self) -> None:
        """Uses Tweepy to call the Twitter Search API to find direct messages to/from
        HowsMyDrivingNY. It then passes this data to a function that creates
        TwitterEvent objects when those direct message events have not already been recorded.
        """

        interval = (self.PRODUCTION_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS if self._is_production()
            else self.DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS)

        self._direct_messages_iteration += 1
        LOG.debug(
            f'Looking up missed direct messages on iteration {self._direct_messages_iteration}')

        # set up timer
        direct_message_thread = threading.Timer(
            interval, self._find_and_respond_to_missed_direct_messages)
        self._lookup_threads.append(direct_message_thread)

        # start timer
        direct_message_thread.start()

        try:
            # most_recent_undetected_twitter_event = TwitterEvent.query.filter(
            #     and_(TwitterEvent.detected_via_account_activity_api == False,
            #          TwitterEvent.event_type == TwitterMessageType.DIRECT_MESSAGE.value)
            # ).order_by(TwitterEvent.event_id.desc()).first()

            # Tweepy bug with cursors prevents us from searching for more than 50 events
            # at a time until 3.9, so it'll have to do.

            direct_messages_since_last_twitter_event = self._get_twitter_client_api(
                ).get_direct_messages(
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

    def _find_and_respond_to_missed_statuses(self) -> None:
        """Uses Tweepy to call the Twitter Search API to find statuses mentioning
        HowsMyDrivingNY. It then passes this data to a function that creates
        TwitterEvent objects when those status events have not already been recorded.
        """

        interval = (self.PRODUCTION_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS if self._is_production()
            else self.DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS)

        self._statuses_iteration += 1
        LOG.debug(
            f'Looking up missed statuses on iteration {self._statuses_iteration}')

        # set up timer
        statuses_thread = threading.Timer(
            interval, self._find_and_respond_to_missed_statuses)
        self._lookup_threads.append(statuses_thread)

        # start timer
        statuses_thread.start()

        try:
            # Find most recent undetected twitter status event, and then
            # search for recent events until we can find no more.
            most_recent_undetected_twitter_event = TwitterEvent.query.filter(
                and_(TwitterEvent.detected_via_account_activity_api == False,
                     TwitterEvent.event_type == TwitterMessageType.STATUS.value)
            ).order_by(TwitterEvent.event_id.desc()).first()

            if most_recent_undetected_twitter_event:
                LOG.info(
                    f"Most recent undetected twitter event: "
                    f"{most_recent_undetected_twitter_event.event_id}"
                )

                statuses_since_last_twitter_event: List[tweepy.models.Status] = []
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

    def _find_and_respond_to_twitter_events(self) -> None:
        """Looks for TwitterEvent objects that have not yet been responded to and
        begins the process of creating a response. Additionally, failed events are
        rerun to provide a correct response, particularly useful in cases where
        external apis are down for maintenance.
        """

        interval = (
            self.PRODUCTION_APP_RATE_LIMITING_INTERVAL_IN_SECONDS if self._is_production()
            else self.DEVELOPMENT_CLIENT_RATE_LIMITING_INTERVAL_IN_SECONDS)

        self._events_iteration += 1
        LOG.debug(
            f'Looking up twitter events on iteration {self._events_iteration}')

        # set up timer
        twitter_events_thread = threading.Timer(
            interval, self._find_and_respond_to_twitter_events)
        self._lookup_threads.append(twitter_events_thread)

        # start timer
        twitter_events_thread.start()

        try:
            new_events: List[TwitterEvent] = TwitterEvent.get_all_by(
                is_duplicate=False,
                responded_to=False,
                response_in_progress=False)

            LOG.debug(f'IDs of new events: {[event.event_id for event in new_events]}')

            failed_events: List[TwitterEvent] = TwitterEvent.get_all_by(
                is_duplicate=False,
                error_on_lookup=True,
                responded_to=True,
                response_in_progress=False)

            failed_events_that_need_response: List[TwitterEvent] = self._filter_failed_twitter_events(failed_events)

            events_to_respond_to: [List[TwitterEvent]] = new_events + failed_events_that_need_response

            LOG.debug(f'IDs of events to respond to: {[event.event_id for event in events_to_respond_to]}')

            for event in events_to_respond_to:
                LOG.debug(f'Processing event with id {event.id} and event_id: {event.event_id}')
                self._process_twitter_event(event=event)

        except Exception as e:

            LOG.error(e)
            LOG.error(str(e))
            LOG.error(e.args)
            logging.exception("stack trace")

        finally:
            TwitterEvent.query.session.close()

    def _get_twitter_application_api(self) -> twitter_api_wrapper.TwitterApplicationApiWrapper:
        """Set the application (non-client) api connection for this instance"""

        if not self._app_api:
            self._app_api = twitter_api_wrapper.TwitterApplicationApiWrapper().get_connection()

        return self._app_api

    def _get_twitter_client_api(self) -> twitter_api_wrapper.TwitterClientApiWrapper:
        """Set the client api connection for this instance"""

        if not self._client_api:
            self._client_api = twitter_api_wrapper.TwitterClientApiWrapper().get_connection()

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
                results, cursors = self._get_twitter_application_api().get_follower_ids(cursor=next_cursor)
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
        return os.getenv('ENV') == 'production'

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
                except tweepy.errors.TweepyException as te:
                    # There's no easy way to know if this status has already
                    # been favorited
                    pass

            LOG.debug('responding as status update')

            user_mention_ids = (request_object.mentioned_user_ids if
                request_object.mentioned_user_ids else None)

            user_mention_ids = [
                x for x in user_mention_ids if x != str(HMDNY_TWITTER_USER_ID)]

            response = self._recursively_process_status_updates(
                response_parts=response_parts,
                message_id=message_id,
                user_mention_ids=user_mention_ids)

            if response:
                # tuple(message_id, ...)
                return response[0]

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
            LOG.info(f'Event {event.id} is a new event, setting `response_in_progress=True`')

            event.response_in_progress = True
            TwitterEvent.query.session.commit()

            try:
                message_source: str = LookupSource(event.event_type)

                # build request
                lookup_request: Type[BaseLookupRequest] = self.reply_argument_builder.build_reply_data(
                    message=event,
                    message_source=message_source)

                user_is_follower: bool = lookup_request.requesting_user_is_follower(
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

                    except tweepy.errors.TweepyException as e:
                        LOG.error(e)
                        event.error_on_lookup = True
                        event.num_times_failed += 1
                        event.last_failed_at_time = datetime.utcnow()

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
                            except tweepy.errors.TweepyException as e:
                                reply_to_event['error_on_lookup'] = True

                    # Update error status
                    if reply_to_event['error_on_lookup']:
                        event.error_on_lookup = True
                        event.num_times_failed += 1
                        event.last_failed_at_time = datetime.utcnow()
                    else:
                        event.error_on_lookup = False
                        event.num_times_failed = 0
                        event.last_failed_at_time = None

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
                                            message_id: Optional[int] = None,
                                            user_mention_ids: Optional[List[str]] = None,
                                            has_sent_first_reply: Optional[bool] = False
    ) -> Optional[Tuple[int, bool]]:

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
                message_id, has_sent_first_reply = self._recursively_process_status_updates(
                    response_parts=part,
                    message_id=message_id,
                    user_mention_ids=user_mention_ids,
                    has_sent_first_reply=has_sent_first_reply)
            else:
                excluded_reply_user_ids = ','.join(
                    user_mention_ids) if user_mention_ids else None

                if self._is_production():
                    should_auto_populate_reply_metadata = not has_sent_first_reply
                    new_message = self._get_twitter_application_api(
                        ).update_status(
                            auto_populate_reply_metadata=should_auto_populate_reply_metadata,
                            exclude_reply_user_ids=excluded_reply_user_ids,
                            in_reply_to_status_id=message_id,
                            status=part)
                    
                    if not has_sent_first_reply:
                        has_sent_first_reply = True

                    message_id = new_message.id

                    LOG.debug(f'message_id: {message_id}')
                else:
                    LOG.debug(
                        "This is where 'self._get_twitter_application_api()"
                        ".update_status(status=part, in_reply_to_status_id=message_id, "
                        "exclude_reply_user_ids=user_mention_ids)' "
                        "would be called in production.")
                    return None

        return (message_id, has_sent_first_reply)

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
