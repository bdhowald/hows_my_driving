import logging
import optparse
import os
import pytz
import sys
import threading
import tweepy

from datetime import datetime, time, timedelta

from common.db_service import DbService

from traffic_violations.reply_argument_builder import ReplyArgumentBuilder
from traffic_violations.traffic_violations_aggregator import TrafficViolationsAggregator

LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}


class TrafficViolationsTweeter:

    def __init__(self):

        # Create a logger
        self.logger = logging.getLogger('hows_my_driving')

        # Set up Twitter auth
        self.auth = tweepy.OAuthHandler(
            os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
        self.auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ[
                                   'TWITTER_ACCESS_TOKEN_SECRET'])

        # keep reference to twitter api
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                              retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))

        # get instance of db service
        self.db_service = DbService(self.logger)

        # create reply argument_builder
        self.reply_argument_builder = ReplyArgumentBuilder(self.api)

        # create new aggregator
        self.aggregator = TrafficViolationsAggregator(self.logger)

        # Log how many times we've called the apis
        self.direct_messages_iteration = 0
        self.events_iteration = 0
        self.statuses_iteration = 0

    def run(self):
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

        self.find_messages_to_respond_to()

    def find_and_respond_to_twitter_events(self):
        interval = 3.0 if self.is_production() else 3000.0

        self.events_iteration += 1
        self.logger.debug(
            f'Looking up twitter events on iteration {self.events_iteration}')

        # start timer
        threading.Timer(
            interval, self.find_and_respond_to_twitter_events).start()

        # Instantiate a connection.
        conn = self.db_service.get_connection()

        try:

            events_query = conn.execute(
                """ select * from twitter_events where responded_to = 0 and response_begun = 0 """)
            events = [dict(zip(tuple(events_query.keys()), i))
                      for i in events_query.cursor]

            self.logger.debug(f'events: {events}')

            # Note that we began the response.
            if events:
                self.logger.debug(f'updating response_begun = 1 for events {",".join([str(event["id"]) for event in events])}')
                conn.execute(
                    """ update twitter_events set response_begun = 1 where id IN (%s) """ % ','.join(['%s'] * len(events)), [event['id'] for event in events])

            for event in events:

                self.logger.debug(f'handling event: {event}')

                # build request
                lookup_request = self.reply_argument_builder.build_reply_data(
                    event, 'twitter', event['event_type'])

                # Reply to the event.
                reply_event = self.aggregator.initiate_reply(lookup_request)
                success = reply_event.get('success', False)

                if success:
                    # Need username for statuses
                    reply_event['username'] = event['user_handle']

                    self.process_response(reply_event)

                    conn.execute(
                        """ update twitter_events set responded_to = 1 where id = %s and responded_to = 0 """, (event['id']))

                    if reply_event.get('error_on_lookup'):
                        conn.execute(
                            """ update twitter_events set error_on_lookup = 1 where id = %s and error_on_lookup = 0 """, (event['id']))


        except Exception as e:

            self.logger.error(e)
            self.logger.error(str(e))
            self.logger.error(e.args)
            logging.exception("stack trace")

        finally:
            # Close the connection
            conn.close()

    def find_messages_to_respond_to(self):
        self.find_and_respond_to_twitter_events()

    def is_production(self):
        return os.environ.get('ENV') == 'production'

    def print_daily_summary(self):
        conn = self.db_service.get_connection()

        utc = pytz.timezone('UTC')
        eastern = pytz.timezone('US/Eastern')

        today = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(days=1)).astimezone(utc)
        end_of_yesterday = (eastern.localize(datetime.combine(
            today, time.min)) - timedelta(seconds=1)).astimezone(utc)

        # num lookups
        daily_lookup_query_string = """
            select count(t1.id) as lookups,
                   ifnull(sum(num_tickets), 0) as total_tickets,
                   count(case when num_tickets = 0 then 1 end) as num_empty_lookups,
                   count(case when boot_eligible = 1 then 1 end) as num_reckless_drivers
              from plate_lookups t1
             where count_towards_frequency = 1
               and t1.created_at =
                 (select MAX(t2.created_at)
                    from plate_lookups t2
                   where t2.plate = t1.plate
                     and t2.state = t1.state
                     and created_at between %s
                     and %s);
        """

        daily_lookup_query = conn.execute(daily_lookup_query_string.replace('\n', ''), (midnight_yesterday.strftime(
            '%Y-%m-%d %H:%M:%S'), end_of_yesterday.strftime('%Y-%m-%d %H:%M:%S'))).fetchone()

        # num tickets
        daily_tickets_query_string = """
            select num_tickets
              from plate_lookups t1
             where count_towards_frequency = 1
               and t1.created_at =
                 (select MAX(t2.created_at)
                    from plate_lookups t2
                   where t2.plate = t1.plate
                     and t2.state = t1.state
                     and created_at between %s
                     and %s);
        """

        daily_tickets_query = conn.execute(daily_tickets_query_string.replace(
            '\n', ''), (midnight_yesterday.strftime('%Y-%m-%d %H:%M:%S'), end_of_yesterday.strftime('%Y-%m-%d %H:%M:%S')))

        # num reckless drivers
        boot_eligible_query_string = """
            select count(distinct plate, state) as boot_eligible_count
              from plate_lookups
             where boot_eligible = 1;
        """

        boot_eligible_query = conn.execute(
            boot_eligible_query_string.replace('\n', '')).fetchone()

        num_lookups = daily_lookup_query[0]
        num_tickets = daily_lookup_query[1]
        num_empty_lookups = daily_lookup_query[2]
        num_reckless_drivers = daily_lookup_query[3]

        lookups_summary_string = (
            f'On {midnight_yesterday.strftime("%A, %B %-d, %Y")}, '
            f"users requested {num_lookups} lookup{'' if num_lookups == 1 else 's'}. ")

        if num_lookups > 0:

            tickets = sorted([i[0] for i in daily_tickets_query])

            median = tickets[int(len(tickets) / 2)] if num_lookups % 2 == 1 else (
                (tickets[int(len(tickets) / 2)] + tickets[int((len(tickets) / 2) - 1)]) / 2.0)

            lookups_summary_string += (
                f"{'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have'} "
                f"received {'{:,}'.format(num_tickets)} ticket{'' if num_tickets == 1 else 's'} "
                f"for an average of {round(num_tickets / num_lookups, 2)} ticket{'' if (num_tickets / num_lookups) == 1 else 's'} "
                f"and a median of {median} ticket{'' if median == 1 else 's'} per vehicle. "
                f"{num_empty_lookups} lookup{'' if num_empty_lookups == 1 else 's'} returned no tickets.")

        total_reckless_drivers = boot_eligible_query[0]

        reckless_drivers_summary_string = (
            f"{num_reckless_drivers} {'vehicle was' if num_reckless_drivers == 1 else 'vehicles were'} "
            f"eligible to be booted or impounded under @bradlander's proposed legislation "
            f"({total_reckless_drivers} such lookups since June 6, 2018).")

        if self.is_production():
            try:
                message = self.api.update_status(lookups_summary_string)
                self.api.update_status(
                    reckless_drivers_summary_string, in_reply_to_status_id=message.id)

            except tweepy.error.TweepError as te:
                print(te)
                self.api.update_status(
                    "Error printing daily summary. Tagging @bdhowald.")

        else:
            print(lookups_summary_string)
            print(reckless_drivers_summary_string)

        # Close the connection
        conn.close()

    def print_featured_plate(self):
        # Instantiate a connection.
        conn = self.db_service.get_connection()

        random_repeat_offender_query = """
            select *
              from repeat_camera_offenders
             where total_camera_violations >= 25
               and times_featured = 0
          order by rand()
             limit 1

        """

        random_repeat_offender_query = conn.execute(
            random_repeat_offender_query.replace('\n', '')).fetchone()

        rco_id = random_repeat_offender_query[0]
        plate = random_repeat_offender_query[1]
        state = random_repeat_offender_query[2]
        total_camera_violations = random_repeat_offender_query[3]
        red_light_camera_violations = random_repeat_offender_query[4]
        speed_camera_violations = random_repeat_offender_query[5]
        times_featured = random_repeat_offender_query[6]

        nth_worst_violator_query = """
            select id
                ,  (
                       select count(*)
                         from repeat_camera_offenders t2
                        where total_camera_violations = t1.total_camera_violations
                   ) as tied_with
                ,  (
                      select min(id)
                         from repeat_camera_offenders t2
                        where total_camera_violations = t1.total_camera_violations
                   ) as min_id
              from repeat_camera_offenders t1
             where plate_id = %s
               and state = %s
        """

        worst_violator_results = conn.execute(
            nth_worst_violator_query.replace('\n', ''), plate, state).fetchone()

        nth_place = worst_violator_results[1] + worst_violator_results[2] - 1
        tied_with = worst_violator_results[1]

        if nth_place:
            vehicle_hashtag = f'#{state}_{plate}'
            suffix = 'st' if (nth_place % 10 == 1 and nth_place % 100 != 11) else ('nd' if (
                nth_place % 10 == 2 and nth_place % 100 != 12) else ('rd' if (nth_place % 10 == 3 and nth_place % 100 != 13) else 'th'))
            worst_substring = f'{nth_place}{suffix}-worst' if nth_place > 1 else 'worst'
            tied_substring = ' tied for' if tied_with != 1 else ''

            max_count_length = len(
                str(max(red_light_camera_violations, speed_camera_violations)))
            spaces_needed = (max_count_length * 2) + 1

            featured_string = (
                f'Featured #RepeatCameraOffender:\n\n'
                f'{vehicle_hashtag} has received {total_camera_violations} camera violations:\n\n'
                f'{str(red_light_camera_violations).ljust(spaces_needed - len(str(red_light_camera_violations)))} | Red Light Camera Violations\n'
                f'{str(speed_camera_violations).ljust(spaces_needed - len(str(speed_camera_violations)))} | Speed Safety Camera Violations\n\n'
                f'This makes {vehicle_hashtag}{tied_substring} the {worst_substring} camera violator in New York City.')

            if self.is_production():
                try:
                    message = self.api.update_status(featured_string)

                    # update record so that we don't feature it again
                    conn.execute(
                        """ update repeat_camera_offenders set times_featured = %s where id = %s """, times_featured + 1, rco_id)

                except tweepy.error.TweepError as te:
                    print(te)
                    self.api.update_status(
                        "Error printing featured plate. Tagging @bdhowald.")

            else:
                print(
                    f'\n'
                    f'update repeat_camera_offenders set times_featured = {times_featured + 1} where id = {rco_id}\n')
                print(featured_string)

        # Close the connection
        conn.close()

    def process_response(self, reply_event_args):
        request_object = reply_event_args.get('request_object')

        message_type = request_object.message_type() if request_object else None
        message_id = request_object.external_id() if request_object else None

        # Respond to user
        if message_type == 'direct_message':

            self.logger.debug('responding as direct message')

            combined_message = self.recursively_process_direct_messages(
                reply_event_args.get('response_parts', {}))

            self.logger.debug('combined_message: %s', combined_message)

            event = {
                "event": {
                    "type": "message_create",
                    "message_create": {
                        "target": {
                            "recipient_id": request_object.user_id() if request_object else None
                        },
                        "message_data": {
                            "text": combined_message
                        }
                    }
                }
            }

            # self.is_production() and self.api.send_direct_message(screen_name
            # = username, text = combined_message)
            self.is_production() and self.api.send_direct_message_new(event)

        else:
            # If we have at least one successful lookup, favorite the status
            if reply_event_args.get('successful_lookup', False):

                # Favorite every look-up from a status
                try:
                    self.is_production() and self.api.create_favorite(message_id)

                # But don't crash on error
                except tweepy.error.TweepError as te:
                    # There's no easy way to know if this status has already
                    # been favorited
                    pass

            self.logger.debug('responding as status update')

            self.recursively_process_status_updates(reply_event_args.get(
                'response_parts', {}), message_id, reply_event_args['username'])

    def recursively_process_direct_messages(self, response_parts):

        return_message = []

        # Iterate through all response parts
        for part in response_parts:
            if isinstance(part, list):
                return_message.append(
                    self.recursively_process_direct_messages(part))
            else:
                return_message.append(part)

        return '\n'.join(return_message)

    def recursively_process_status_updates(self, response_parts, message_id, username):

        # Iterate through all response parts
        for part in response_parts:
            # Some may be lists themselves
            if isinstance(part, list):
                message_id = self.recursively_process_status_updates(
                    part, message_id, username)
            else:
                if self.is_production():
                    new_message = self.api.update_status(
                        '@' + username + ' ' + part, in_reply_to_status_id=message_id)
                    message_id = new_message.id

                    self.logger.debug("message_id: %s", str(message_id))
                else:
                    self.logger.debug(
                        "This is where 'self.api.update_status(part, in_reply_to_status_id = message_id)' would be called in production.")

        return message_id


if __name__ == '__main__':
    tweeter = TrafficViolationsTweeter()

    if sys.argv[-1] == 'print_daily_summary':
        tweeter.print_daily_summary()
    elif sys.argv[-1] == 'print_featured_plate':
        tweeter.print_featured_plate()
    else:
        tweeter.run()
