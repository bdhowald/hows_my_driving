import logging
import optparse
import os
import pytz
import statistics
import sys
import threading
import tweepy

from datetime import datetime, time, timedelta
from sqlalchemy import and_
from sqlalchemy.sql.expression import func
from typing import List, Optional

from traffic_violations.constants import L10N
from traffic_violations.constants.lookup_sources import LookupSource

from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.repeat_camera_offender import \
    RepeatCameraOffender
from traffic_violations.models.twitter_event import TwitterEvent
from traffic_violations.reply_argument_builder import ReplyArgumentBuilder
from traffic_violations.traffic_violations_aggregator import \
    TrafficViolationsAggregator

from traffic_violations.utils import string_utils, twitter_utils

LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}

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

    def _run(self):
        print('Setting up logging')
        parser = optparse.OptionParser()
        parser.add_option('-l', '--logging-level', help='Logging level')
        parser.add_option('-f', '--logging-file', help='Logging file name')
        (options, args) = parser.parse_args()
        logging_level = LOGGING_LEVELS.get(
            options.logging_level, logging.NOTSET)
        logging.basicConfig(level=logging_level, filename=options.logging_file,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

        self._find_messages_to_respond_to()

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

    def _print_daily_summary(self):
        """ Tweet out daily summary of yesterday's lookups """

        utc = pytz.timezone('UTC')
        eastern = pytz.timezone('US/Eastern')

        today = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(days=1)).astimezone(utc)
        end_of_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(seconds=1)).astimezone(utc)

        # find all of yesterday's lookups, using only the most
        # recent of yesterday's queries for a vehicle.
        subquery = PlateLookup.query.session.query(
            PlateLookup.plate, PlateLookup.state, func.max(
                PlateLookup.id).label('most_recent_vehicle_lookup')
        ).filter(
            and_(PlateLookup.created_at >= midnight_yesterday,
                 PlateLookup.created_at <= end_of_yesterday,
                 PlateLookup.count_towards_frequency == True)
        ).group_by(
            PlateLookup.plate,
            PlateLookup.state
        ).subquery('subquery')

        full_query = PlateLookup.query.join(subquery,
                                            (PlateLookup.id == subquery.c.most_recent_vehicle_lookup))

        yesterdays_lookups: List[PlateLookup] = full_query.all()

        num_lookups: int = len(yesterdays_lookups)
        ticket_counts: int = [
            lookup.num_tickets for lookup in yesterdays_lookups]
        total_tickets: int = sum(ticket_counts)
        num_empty_lookups: int = len([
            lookup for lookup in yesterdays_lookups if lookup.num_tickets == 0])
        num_reckless_drivers: int = len([
            lookup for lookup in yesterdays_lookups if lookup.boot_eligible == True])

        total_reckless_drivers = PlateLookup.query.session.query(
            PlateLookup.plate, PlateLookup.state
        ).distinct().filter(
            and_(PlateLookup.boot_eligible == True,
                 PlateLookup.count_towards_frequency)).count()

        lookups_summary_string = (
            f'On {midnight_yesterday.strftime("%A, %B %-d, %Y")}, '
            f"users requested {num_lookups} lookup{L10N.pluralize(num_lookups)}. ")

        if num_lookups > 0:

            median = statistics.median(ticket_counts)

            lookups_summary_string += (
                f"{'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have'} "
                f"received {'{:,}'.format(total_tickets)} ticket{L10N.pluralize(total_tickets)} "
                f"for an average of {round(total_tickets / num_lookups, 2)} ticket{L10N.pluralize(total_tickets / num_lookups)} "
                f"and a median of {median} ticket{L10N.pluralize(median)} per vehicle. "
                f"{num_empty_lookups} lookup{L10N.pluralize(num_empty_lookups)} returned no tickets.")

        reckless_drivers_summary_string = (
            f"{num_reckless_drivers} {'vehicle was' if num_reckless_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under @bradlander's "
            f"proposed legislation ({total_reckless_drivers} such lookups "
            f"since June 6, 2018).")

        if self._is_production():
            try:
                message = self.api.update_status(lookups_summary_string)
                self.api.update_status(
                    reckless_drivers_summary_string,
                    in_reply_to_status_id=message.id)

            except tweepy.error.TweepError as te:
                print(te)
                self.api.update_status(
                    "Error printing daily summary. Tagging @bdhowald.")

        else:
            print(lookups_summary_string)
            print(reckless_drivers_summary_string)

    def _print_featured_plate(self):
        """ Tweet out repeat camera offenders """

        repeat_camera_offender: Optional[RepeatCameraOffender] = RepeatCameraOffender.query.filter(
            and_(RepeatCameraOffender.times_featured == 0,
                 RepeatCameraOffender.total_camera_violations >= 25)).order_by(
            func.random()).first()

        if repeat_camera_offender:

            # get the number of vehicles that have the same number
            # of violations
            tied_with = RepeatCameraOffender.query.filter(
                RepeatCameraOffender.total_camera_violations ==
                repeat_camera_offender.total_camera_violations).count()

            # since the vehicles are in descending order of violations,
            # we find the record that has the same number of violations
            # and the lowest id...
            min_id = RepeatCameraOffender.query.session.query(
                func.min(RepeatCameraOffender.id)
            ).filter(
                RepeatCameraOffender.total_camera_violations ==
                repeat_camera_offender.total_camera_violations
            ).one()[0]

            # nth place is simply the sum of the two values minus one.
            nth_place = tied_with + min_id - 1

            red_light_camera_violations = \
                repeat_camera_offender.red_light_camera_violations
            speed_camera_violations = \
                repeat_camera_offender.speed_camera_violations

            vehicle_hashtag: str = L10N.VEHICLE_HASHTAG.format(
                repeat_camera_offender.state,
                repeat_camera_offender.plate_id)

            # one of 'st', 'nd', 'rd', 'th'
            suffix: str = string_utils.determine_ordinal_indicator(nth_place)

            # how bad is this vehicle?
            worst_substring: str = (
                f'{nth_place}{suffix}-worst' if nth_place > 1 else 'worst')

            tied_substring: str = ' tied for' if tied_with != 1 else ''

            spaces_needed: int = twitter_utils.padding_spaces_needed(
                red_light_camera_violations, speed_camera_violations)

            featured_string = L10N.REPEAT_CAMERA_OFFENDER_STRING.format(
                vehicle_hashtag,
                repeat_camera_offender.total_camera_violations,
                str(red_light_camera_violations).ljust(
                    spaces_needed - len(str(red_light_camera_violations))),
                str(speed_camera_violations).ljust(
                    spaces_needed - len(str(speed_camera_violations))),
                vehicle_hashtag,
                tied_substring,
                worst_substring)

            if self._is_production():
                try:
                    message = self.api.update_status(featured_string)

                    repeat_camera_offender.times_featured += 1
                    RepeatCameraOffender.query.session.commit()

                except tweepy.error.TweepError as te:
                    print(te)
                    self.api.update_status(
                        f'Error printing featured plate. '
                        f'Tagging @bdhowald.')

            else:
                print(featured_string)

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

            event = {
                "event": {
                    "type": "message_create",
                    "message_create": {
                        "target": {
                            "recipient_id": request_object.user_id if request_object else None
                        },
                        "message_data": {
                            "text": combined_message
                        }
                    }
                }
            }

            # self._is_production() and self.api.send_direct_message(screen_name
            # = username, text = combined_message)
            self._is_production() and self.api.send_direct_message_new(event)

        else:
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

    def _recursively_process_status_updates(self, response_parts, message_id, username):

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

if __name__ == '__main__':
    tweeter = TrafficViolationsTweeter()

    if sys.argv[-1] == 'print_daily_summary':
        tweeter._print_daily_summary()
    elif sys.argv[-1] == 'print_featured_plate':
        tweeter._print_featured_plate()
    else:
        tweeter._run()
