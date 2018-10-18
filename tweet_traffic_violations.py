# Imports
import getpass
import logging
import json
import optparse
import os
import pdb
import pytz
import sys
import threading
import tweepy


from datetime import datetime, timezone, time, timedelta

from db_service import DbService
from traffic_violations_aggregator import TrafficViolationsAggregator
# from traffic_violations_stream_listener import TrafficViolationStreamListener


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
        self.auth = tweepy.OAuthHandler(os.environ['TWITTER_API_KEY'], os.environ['TWITTER_API_SECRET'])
        self.auth.set_access_token(os.environ['TWITTER_ACCESS_TOKEN'], os.environ['TWITTER_ACCESS_TOKEN_SECRET'])

        # keep reference to twitter api
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))

        google_api_key = os.environ['GOOGLE_API_KEY'] if os.environ.get('GOOGLE_API_KEY') else ''

        # get instance of db service
        self.db_service = DbService(self.logger)

        # create new aggregator
        self.aggregator = TrafficViolationsAggregator(self.db_service, self.logger, google_api_key)

        # Log how many times we've called the apis
        self.direct_messages_iteration = 0
        self.events_iteration          = 0
        self.statuses_iteration        = 0



    def run(self):
        print('Setting up logging')
        parser = optparse.OptionParser()
        parser.add_option('-l', '--logging-level', help='Logging level')
        parser.add_option('-f', '--logging-file', help='Logging file name')
        (options, args) = parser.parse_args()
        logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
        logging.basicConfig(level=logging_level, filename=options.logging_file,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')



        # twitterStream = tweepy.Stream(self.auth, TrafficViolationStreamListener(self))
        # userstream = twitterStream.userstream()

        # deprecatedStream = tweepy.Stream(self.auth, TrafficViolationStreamListener(self))
        # deprecatedStream.filter(track=['howsmydrivingny'])

        self.find_messages_to_respond_to()



    def find_and_respond_to_direct_messages(self):
        self.direct_messages_iteration += 1
        self.logger.debug('Looking up direct messages on iteration {}'.format(self.direct_messages_iteration))

        # start timer
        threading.Timer(120.0, self.find_and_respond_to_direct_messages).start()

        # Instantiate a connection.
        with self.db_service as conn:

            try:

                # Find last status to which we have responded.
                max_responded_to_id = conn.execute(""" select max(message_id) from ( select max(message_id) as message_id from plate_lookups where lookup_source = 'direct_message' and responded_to = 1 union select max(message_id) as message_id from failed_plate_lookups fpl where lookup_source = 'direct_message' and responded_to = 1 ) a """).fetchone()[0]

                # Query for us.
                messages = self.api.direct_messages(count=50, full_text=True, since_id=max_responded_to_id)

                # Grab message ids.
                message_ids = [int(message.id) for message in messages if int(message.message_create['sender_id']) != 976593574732222465]

                # Figure out which don't need response.
                already_responded_message_ids       = conn.execute(""" select message_id from plate_lookups where message_id in (%s) and responded_to = 1 """ % ','.join(['%s'] * len(message_ids)), message_ids)

                failed_plate_lookup_ids             = conn.execute(""" select message_id from failed_plate_lookups where message_id in (%s) and responded_to = 1 """ % ','.join(['%s'] * len(message_ids)), message_ids)

                message_ids_that_dont_need_response = [i[0] for i in already_responded_message_ids] + [i[0] for i in failed_plate_lookup_ids]

                # Subtract the second from the first.
                message_ids_that_need_response      = set(message_ids) - set(message_ids_that_dont_need_response)

                self.logger.debug("messages that need response: %s", message_ids_that_need_response)

                if message_ids_that_need_response:

                    for message in [message for message in messages if int(message.id) in message_ids_that_need_response]:

                        self.logger.debug("Responding to message: %s - %s", message.id, message)

                        self.aggregator.initiate_reply(message, 'direct_message')


            except Exception as e:
                self.logger.error('"Error in querying tweets')
                self.logger.error(e)
                self.logger.error(str(e))
                self.logger.error(e.args)
                logging.exception("stack trace")



    def find_and_respond_to_statuses(self):
        self.statuses_iteration += 1
        self.logger.debug('Looking up statuses on iteration {}'.format(self.statuses_iteration))

        # start timer
        threading.Timer(120.0, self.find_and_respond_to_statuses).start()

        # Instantiate a connection.
        with self.db_service as conn:

            try:

                # Find last status to which we have responded.
                max_responded_to_id = conn.execute(""" select max(message_id) from ( select max(message_id) as message_id from plate_lookups where lookup_source = 'status' and responded_to = 1 union select max(message_id) as message_id from failed_plate_lookups fpl where lookup_source = 'status' and responded_to = 1 ) a """).fetchone()[0]

                message_pages = 0

                # Query for us.
                messages = self.api.search(q='@HowsMyDrivingNY', count=100, result_type='recent', since_id=max_responded_to_id, tweet_mode='extended')

                # Grab message ids.
                message_ids = [int(message.id) for message in messages]

                while messages:

                    message_pages += 1
                    self.logger.debug('message_page: {}'.format(message_pages))

                    # Figure out which don't need response.
                    already_responded_message_ids       = conn.execute(""" select message_id from plate_lookups where message_id in (%s) and responded_to = 1 """ % ','.join(['%s'] * len(message_ids)), message_ids)

                    failed_plate_lookup_ids             = conn.execute(""" select message_id from failed_plate_lookups where message_id in (%s) and responded_to = 1 """ % ','.join(['%s'] * len(message_ids)), message_ids)

                    message_ids_that_dont_need_response = [i[0] for i in already_responded_message_ids] + [i[0] for i in failed_plate_lookup_ids]

                    # Subtract the second from the first.
                    message_ids_that_need_response      = set(message_ids) - set(message_ids_that_dont_need_response)

                    self.logger.debug("messages that need response: %s", messages)


                    for message in messages:

                        if int(message.id) in message_ids_that_need_response:

                            self.logger.debug("Responding to mesasge: %s - %s", message.id, message)

                            self.aggregator.initiate_reply(message, 'status')

                        else:

                            self.logger.debug("recent message that appears to need response, but did not: %s - %s", message.id, message)

                    # search for next set
                    message_ids.sort()

                    min_id = message_ids[0]

                    messages = self.api.search(q='@HowsMyDrivingNY', count=100, result_type='recent', tweet_mode='extended', since_id=max_responded_to_id, max_id=min_id - 1)


            except Exception as e:

                self.logger.error('"Error in querying tweets')
                self.logger.error(e)
                self.logger.error(str(e))
                self.logger.error(e.args)
                logging.exception("stack trace")



    def find_and_respond_to_twitter_events(self):
        self.events_iteration += 1
        self.logger.debug('Looking up twitter events on iteration {}'.format(self.events_iteration))

        # start timer
        threading.Timer(3.0, self.find_and_respond_to_twitter_events).start()

        # Instantiate a connection.
        with self.db_service as conn:

            try:

                events_query = conn.execute(""" select * from twitter_events where responded_to = 0 and response_begun = 0 """)
                events       = [dict(zip(tuple (events_query.keys()), i)) for i in events_query.cursor]

                self.logger.debug('events: {}'.format(events))

                for event in events:

                    self.logger.debug('handling event: {}'.format(event))

                    # Note that we began the response.
                    conn.execute(""" update twitter_events set response_begun = 1 where id = %s """, (event['id']))

                    # Reply to the event.
                    reply_event = self.aggregator.initiate_reply(event, event['event_type'])
                    success     = reply_event.get('success', False)

                    if success:
                        self.process_response(reply_event)
                        conn.execute(""" update twitter_events set responded_to = 1 where id = %s and responded_to = 0 """, (event['id']))


            except Exception as e:

                self.logger.error(e)
                self.logger.error(str(e))
                self.logger.error(e.args)
                logging.exception("stack trace")



    def find_messages_to_respond_to(self):
        self.find_and_respond_to_twitter_events()

        # time.sleep(30)

        # # Until I get access to account activity API,
        # # just search for statuses to which we haven't responded.
        # self.find_and_respond_to_statuses()
        # self.find_and_respond_to_direct_messages()


    def is_production(self):
        return True if getpass.getuser() == 'safestreets' else False



    def print_daily_summary(self):
        # Instantiate a connection.
        with self.db_service as conn:

            utc           = pytz.timezone('UTC')
            eastern       = pytz.timezone('US/Eastern')

            today         = datetime.now(eastern).date()

            midnight_yesterday = (eastern.localize(datetime.combine(today, time.min)) - timedelta(days=1)).astimezone(utc)
            end_of_yesterday   = (eastern.localize(datetime.combine(today, time.min)) - timedelta(seconds=1)).astimezone(utc)

            daily_lookup_query_string = """
                select count(t1.id) as lookups,
                       ifnull(sum(num_tickets), 0) as total_tickets,
                       count(case when num_tickets = 0 then 1 end) as empty_lookups,
                       count(case when boot_eligible = 1 then 1 end) as reckless_drivers
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


            daily_lookup_query = conn.execute(daily_lookup_query_string.replace('\n', ''), (midnight_yesterday.strftime('%Y-%m-%d %H:%M:%S'), end_of_yesterday.strftime('%Y-%m-%d %H:%M:%S'))).fetchone()

            num_lookups      = daily_lookup_query[0]
            num_tickets      = daily_lookup_query[1]
            empty_lookups    = daily_lookup_query[2]
            reckless_drivers = daily_lookup_query[3]


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

            daily_tickets_query = conn.execute(daily_tickets_query_string.replace('\n', ''), (midnight_yesterday.strftime('%Y-%m-%d %H:%M:%S'), end_of_yesterday.strftime('%Y-%m-%d %H:%M:%S')))

            tickets = sorted([i[0] for i in daily_tickets_query])
            median  = tickets[int(len(tickets)/2)] if num_lookups % 2 == 1 else ((tickets[int(len(tickets)/2)] + tickets[int((len(tickets)/2) - 1)])/2.0)


            boot_eligible_query_string = """
                select count(distinct plate, state) as boot_eligible_count
                  from plate_lookups
                 where boot_eligible = 1;
            """

            boot_eligible_query = conn.execute(boot_eligible_query_string.replace('\n', '')).fetchone()

            total_reckless_drivers = boot_eligible_query[0]


            if num_lookups > 0:
                lookups_summary_string = "On {}, users requested {} {}. {} received {} {} for an average of {} {} and a median of {} {} per vehicle. {} {} returned no tickets.".format(midnight_yesterday.strftime('%A, %B %-d, %Y'), num_lookups, 'lookup' if num_lookups == 1 else 'lookups', 'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have', "{:,}".format(num_tickets), 'ticket' if num_tickets == 1 else 'tickets', round(num_tickets/num_lookups, 2), 'ticket' if (num_tickets/num_lookups) == 1 else 'tickets', median, 'ticket' if median == 1 else 'tickets', empty_lookups, 'lookup' if empty_lookups == 1 else 'lookups')

                reckless_drivers_summary_string = "{} {} eligible to be booted or impounded under @bradlander's proposed legislation ({} such lookups since June 6, 2018).".format(reckless_drivers, 'vehicle was' if reckless_drivers == 1 else 'vehicles were', total_reckless_drivers)

                if self.is_production():
                    try:
                        message = self.api.update_status(lookups_summary_string)
                        self.api.update_status(reckless_drivers_summary_string, in_reply_to_status_id = message.id)

                    except tweepy.error.TweepError as te:
                        print(te)
                        self.api.update_status("Error printing daily summary. Tagging @bdhowald.")

                else:
                    print(lookups_summary_string)
                    print(reckless_drivers_summary_string)



    def print_featured_plate(self):
        # Instantiate a connection.
        with self.db_service as conn:

            random_repeat_offender_query = """
                select *
                  from repeat_camera_offenders
                 where total_camera_violations >= 25
                   and times_featured = 0
              order by rand()
                 limit 1

            """

            random_repeat_offender_query = conn.execute(random_repeat_offender_query.replace('\n', '')).fetchone()

            rco_id                      = random_repeat_offender_query[0]
            plate                       = random_repeat_offender_query[1]
            state                       = random_repeat_offender_query[2]
            total_camera_violations     = random_repeat_offender_query[3]
            red_light_camera_violations = random_repeat_offender_query[4]
            speed_camera_violations     = random_repeat_offender_query[5]
            times_featured              = random_repeat_offender_query[6]


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

            worst_violator_results = conn.execute(nth_worst_violator_query.replace('\n', ''), plate, state).fetchone()

            nth_place = worst_violator_results[1] + worst_violator_results[2] - 1
            tied_with = worst_violator_results[1]


            if nth_place:
                vehicle_hashtag  = "#{}_{}".format(state, plate)
                suffix           = 'st' if (nth_place % 10 == 1 and nth_place % 100 != 11) else ('nd' if (nth_place % 10 == 2 and nth_place % 100 != 12) else ('rd' if (nth_place % 10 == 3 and nth_place % 100 != 13) else 'th'))
                worst_substring  = "{}{}-worst".format(nth_place, suffix) if nth_place > 1 else "worst"
                tied_substring   = ' tied for' if tied_with != 1 else ''

                max_count_length = len(str(max( red_light_camera_violations, speed_camera_violations )))
                spaces_needed    = (max_count_length * 2) + 1


                featured_string = "Featured #RepeatCameraOffender:\n\n{} has received {} camera violations:\n\n{} | Red Light Camera Violations\n{} | Speed Safety Camera Violations\n\nThis makes {}{} the {} camera violator in New York City.".format(vehicle_hashtag, total_camera_violations, str(red_light_camera_violations).ljust(spaces_needed - len(str(red_light_camera_violations))), str(speed_camera_violations).ljust(spaces_needed - len(str(speed_camera_violations))), vehicle_hashtag, tied_substring, worst_substring)

                if self.is_production():
                    try:
                        message = self.api.update_status(featured_string)

                        # update record so that we don't feature it again
                        conn.execute(""" update repeat_camera_offenders set times_featured = %s where id = %s """, times_featured + 1, rco_id)

                    except tweepy.error.TweepError as te:
                        print(te)
                        self.api.update_status("Error printing featured plate. Tagging @bdhowald.")

                else:
                    print("\nupdate repeat_camera_offenders set times_featured = {} where id = {}\n".format(times_featured + 1, rco_id))
                    print(featured_string)



    def process_response(self, reply_event_args):

        message_type = reply_event_args.get('response_args', {}).get('type', None)
        message_id   = reply_event_args.get('response_args', {}).get('id', None)

        # Respond to user
        if message_type == 'direct_message':

            self.logger.debug('responding as direct message')

            combined_message = self.recursively_process_direct_messages(reply_event_args.get('response_parts', {}))

            self.logger.debug('combined_message: %s', combined_message)

            event = {
              "event": {
                "type": "message_create",
                "message_create": {
                  "target": {
                    "recipient_id": reply_event_args.get('response_args', {}).get('user_id', None)
                  },
                  "message_data": {
                    "text": combined_message
                  }
                }
              }
            }

            # self.is_production() and self.api.send_direct_message(screen_name = username, text = combined_message)
            self.is_production() and self.api.send_direct_message_new(event)

        else:
            # If we have at least one successful lookup, favorite the status
            if reply_event_args.get('successful_lookup', False):

                # Favorite every look-up from a status
                try:
                    self.is_production() and self.api.create_favorite(message_id)

                # But don't crash on error
                except tweepy.error.TweepError as te:
                    # There's no easy way to know if this status has already been favorited
                    pass

            self.logger.debug('responding as status update')

            self.recursively_process_status_updates(reply_event_args.get('response_parts', {}), message_id)



    def recursively_process_direct_messages(self, response_parts):

        return_message = []

        # Iterate through all response parts
        for part in response_parts:
            if isinstance(part, list):
                return_message.append(self.recursively_process_direct_messages(part))
            else:
                return_message.append(part)

        return '\n'.join(return_message)



    def recursively_process_status_updates(self, response_parts, message_id):

        # Iterate through all response parts
        for part in response_parts:
            # Some may be lists themselves
            if isinstance(part, list):
                message_id = self.recursively_process_status_updates(part, message_id)
            else:
                if self.is_production():
                    new_message = self.api.update_status(part, in_reply_to_status_id = message_id)
                    message_id  = new_message.id

                    self.logger.debug("message_id: %s", str(message_id))
                else:
                    self.logger.debug("This is where 'self.api.update_status(part, in_reply_to_status_id = message_id)' would be called in production.")

        return message_id


if __name__ == '__main__':
    tweeter = TrafficViolationsTweeter()

    if sys.argv[-1] == 'print_daily_summary':
        tweeter.print_daily_summary()
    elif sys.argv[-1] == 'print_featured_plate':
        tweeter.print_featured_plate()
    else:
        tweeter.run()
        # app.run()