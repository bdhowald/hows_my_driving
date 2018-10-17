# test_tweet_traffic_violations.py

# import pytest
import random
import unittest

# from mock import MagicMock
from unittest.mock import call, MagicMock, patch

# import class
from tweet_traffic_violations import TrafficViolationsTweeter


import pdb

import getpass
import json
import os
import requests
import requests_futures.sessions
import pytz
import sys


from datetime import datetime, timezone, time, timedelta



def inc(part, in_reply_to_status_id):
    int_mock = MagicMock(name='api')
    int_mock.id = (in_reply_to_status_id + 1)
    return int_mock



class TestTrafficViolationsTweeter(unittest.TestCase):

    def setUp(self):
        self.tweeter = TrafficViolationsTweeter()


    def test_find_and_respond_to_direct_messages(self):
      random_id = random.randint(10000000000000000000, 20000000000000000000)

      connect_mock        = MagicMock(name='connect_mock')
      execute_mock        = MagicMock(name='execute_mock')

      execute_mock.fetchone.return_value = [random_id]
      connect_mock.execute.return_value = execute_mock

      engine_mock         = MagicMock(name='engine_mock')
      engine_mock.connect.return_value = connect_mock

      api_mock            = MagicMock(name='api_mock')
      direct_message_mock = MagicMock(name='direct_message')

      api_mock.direct_messages = direct_message_mock

      self.tweeter.api    = api_mock

      self.tweeter.db_service = connect_mock
      self.tweeter.db_service.__enter__.return_value = connect_mock

      self.tweeter.find_and_respond_to_direct_messages()

      direct_message_mock.assert_called_with(count=50, full_text=True, since_id=random_id)


    def test_find_messages_to_respond_to(self):
      status_mock         = MagicMock(name='find_and_respond_to_statuses')
      direct_message_mock = MagicMock(name='find_and_respond_to_direct_messages')
      twitter_events_mock = MagicMock(name='find_and_respond_to_twitter_events')

      self.tweeter.find_and_respond_to_statuses        = status_mock
      self.tweeter.find_and_respond_to_direct_messages = direct_message_mock
      self.tweeter.find_and_respond_to_twitter_events  = twitter_events_mock

      self.tweeter.find_messages_to_respond_to()

      twitter_events_mock.assert_called_with()
      # status_mock.assert_called_with()
      # direct_message_mock.assert_called_with()


    def test_find_and_respond_to_statuses(self):
      random_id = random.randint(10000000000000000000, 20000000000000000000)

      connect_mock        = MagicMock(name='connect_mock')
      execute_mock        = MagicMock(name='execute_mock')

      execute_mock.fetchone.return_value = [random_id]
      connect_mock.execute.return_value = execute_mock

      api_mock            = MagicMock(name='api_mock')
      status_mock         = MagicMock(name='status')
      status_mock.return_value = []

      api_mock.search     = status_mock

      self.tweeter.api    = api_mock

      self.tweeter.db_service = connect_mock
      self.tweeter.db_service.__enter__.return_value = connect_mock

      self.tweeter.find_and_respond_to_statuses()

      status_mock.assert_called_with(q='@HowsMyDrivingNY', count=100, result_type='recent', since_id=random_id, tweet_mode='extended')


    def test_find_and_respond_to_twitter_events(self):
      db_id                  = 1
      random_id              = random.randint(10000000000000000000, 20000000000000000000)
      event_type             = 'status'
      user_handle            = 'bdhowald'
      user_id                = random.randint(1000000000, 2000000000)
      event_text             = '@HowsMyDrivingNY abc1234:ny'
      timestamp              = random.randint(1500000000000, 1700000000000)
      in_reply_to_message_id = random.randint(10000000000000000000, 20000000000000000000)
      location               = 'Queens, NY'
      responded_to           = 0
      user_mentions          = '@HowsMyDrivingNY'

      event_obj = {
        'id': db_id,
        'event_type': event_type,
        'event_id': random_id,
        'user_handle': user_handle,
        'user_id': user_id,
        'event_text': event_text,
        'created_at': timestamp,
        'in_reply_to_message_id': in_reply_to_message_id,
        'location': location,
        'responded_to': responded_to,
        'user_mentions': user_mentions
      }

      cursor_tuple = [tuple(d.values()) for d in [event_obj]]

      events_mock = MagicMock(name='events')

      events_mock.keys.return_value = tuple(event_obj.keys())
      events_mock.cursor = (cursor_tuple[0],)

      connect_mock = MagicMock(name='connect')
      connect_mock.execute.return_value = events_mock

      self.tweeter.db_service = connect_mock
      self.tweeter.db_service.__enter__.return_value = connect_mock

      initiate_reply_mock = MagicMock(name='initiate_reply')
      self.tweeter.aggregator.initiate_reply = initiate_reply_mock

      self.tweeter.find_and_respond_to_twitter_events()

      self.tweeter.aggregator.initiate_reply.assert_called_with(event_obj, event_obj['event_type'])



    def test_is_production(self):
        username = getpass.getuser()

        self.assertEqual(self.tweeter.is_production(), (username == 'safestreets'))



    def test_print_daily_summary(self):
        utc           = pytz.timezone('UTC')
        eastern       = pytz.timezone('US/Eastern')

        today         = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(today, time.min)) - timedelta(days=1)).astimezone(utc)
        end_of_yesterday   = (eastern.localize(datetime.combine(today, time.min)) - timedelta(seconds=1)).astimezone(utc)

        num_lookups            = random.randint(1, 10000)
        num_tickets            = random.randint(1, 1000000)
        empty_lookups          = random.randint(1, 100)
        reckless_drivers       = random.randint(1, 250)
        total_reckless_drivers = random.randint(1, 1000)

        message_id             = random.randint(10000000000000000000, 20000000000000000000)


        cursor_mock = MagicMock(name='cursor')
        # cursor_mock.fetchone.return_value = (num_lookups, num_tickets, empty_lookups, reckless_drivers)
        cursor_mock.fetchone.side_effect = [[num_lookups, num_tickets, empty_lookups, reckless_drivers], [total_reckless_drivers]]

        execute_mock = MagicMock(name='execute')
        execute_mock.execute.return_value = cursor_mock
        # tweeter.
        connect_mock = MagicMock(name='connect')
        connect_mock.return_value = execute_mock

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        message_mock = MagicMock(name='message')
        message_mock.id = message_id

        update_status_mock = MagicMock(name='update_status')
        update_status_mock.return_value = message_mock

        api_mock = MagicMock(name='api')
        api_mock.update_status = update_status_mock

        self.tweeter.db_service = connect_mock
        self.tweeter.db_service.__enter__ = connect_mock

        self.tweeter.is_production = is_production_mock
        self.tweeter.api = api_mock

        lookup_str   = "On {}, users requested {} {}. {} received {} {}. {} {} returned no tickets.".format(midnight_yesterday.strftime('%A, %B %-d, %Y'), num_lookups, 'lookup' if num_lookups == 1 else 'lookups', 'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have', "{:,}".format(num_tickets), 'ticket' if num_tickets == 1 else 'tickets', empty_lookups, 'lookup' if empty_lookups == 1 else 'lookups')
        reckless_str = "{} {} eligible to be booted or impounded under @bradlander's proposed legislation ({} such lookups since June 6, 2018).".format(reckless_drivers, 'vehicle was' if reckless_drivers == 1 else 'vehicles were', total_reckless_drivers)


        self.tweeter.print_daily_summary()

        calls = [call(lookup_str), call(reckless_str, in_reply_to_status_id = message_id)]

        update_status_mock.assert_has_calls(calls)


    def test_print_featured_plate(self):
        rco_id                      = 123
        plate                       = 'ABC1234'
        state                       = 'NY'
        total_camera_violations     = random.randint(1, 100)
        red_light_camera_violations = total_camera_violations - random.randint(1, total_camera_violations)
        speed_camera_violations     = total_camera_violations - red_light_camera_violations
        times_featured              = 0

        index                       = random.randint(1,3000)
        tied_with                   = random.randint(0,3)
        nth_place                   = index + tied_with - 1


        cursor_mock = MagicMock(name='cursor')
        # cursor_mock.fetchone.return_value = (num_lookups, num_tickets, empty_lookups, reckless_drivers)
        cursor_mock.fetchone.side_effect = [[rco_id, plate, state, total_camera_violations, red_light_camera_violations, speed_camera_violations, times_featured], [index, tied_with]]

        connect_mock = MagicMock(name='connect')
        connect_mock.execute.return_value = cursor_mock

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        message_mock = MagicMock(name='message')
        # message_mock.id = message_id

        update_status_mock = MagicMock(name='update_status')
        update_status_mock.return_value = message_mock

        api_mock = MagicMock(name='api')
        api_mock.update_status = update_status_mock

        self.tweeter.db_service = connect_mock
        self.tweeter.db_service.__enter__.return_value = connect_mock

        self.tweeter.is_production = is_production_mock
        self.tweeter.api = api_mock


        vehicle_hashtag  = "#{}_{}".format(state, plate)
        suffix           = 'st' if (nth_place % 10 == 1 and nth_place % 100 != 11) else ('nd' if (nth_place % 10 == 2 and nth_place % 100 != 12) else ('rd' if (nth_place % 10 == 3 and nth_place % 100 != 13) else 'th'))
        worst_substring  = "{}{}-worst".format(nth_place, suffix) if nth_place > 1 else "worst"
        tied_substring   = ' tied for' if tied_with != 1 else ''

        max_count_length = len(str(max( red_light_camera_violations, speed_camera_violations )))
        spaces_needed    = (max_count_length * 2) + 1


        featured_string ="Featured #RepeatCameraOffender:\n\n{} has received {} camera violations:\n\n{} | Red Light Camera Violations\n{} | Speed Safety Camera Violations\n\nThis makes {}{} the {} camera violator in New York City.".format(vehicle_hashtag, total_camera_violations, str(red_light_camera_violations).ljust(spaces_needed - len(str(red_light_camera_violations))), str(speed_camera_violations).ljust(spaces_needed - len(str(speed_camera_violations))), vehicle_hashtag, tied_substring, worst_substring)

        self.tweeter.print_featured_plate()

        calls = [call(featured_string)]

        update_status_mock.assert_has_calls(calls)


    def test_process_response(self):
        now           = datetime.now()
        previous_time = now - timedelta(minutes=10)
        utc           = pytz.timezone('UTC')
        eastern       = pytz.timezone('US/Eastern')
        utc_time      = utc.localize(now).astimezone(timezone.utc)
        # eastern_time  = utc.localize(now).astimezone(eastern)
        previous_utc  = utc.localize(previous_time).astimezone(timezone.utc)



        ######################################
        # Test direct message and new format #
        ######################################

        username1   = 'bdhowald'
        message_id  = random.randint(1000000000000000000, 2000000000000000000)
        response_args1 = {
          'created_at': utc_time.strftime('%a %b %d %H:%M:%S %z %Y'),
          'id': message_id,
          'legacy_string_parts': ['@howsmydrivingny',
                                  'ny:hme6483'],
          'string_parts': ['@howsmydrivingny',
                           'ny:hme6483'],
          'type': 'direct_message',
          'username': username1,
          'user_id': 30139847
        }

        combined_message = "@bdhowald #NY_HME6483 has been queried 1 time.\n\nTotal parking and camera violation tickets: 15\n\n4 | No Standing - Day/Time Limits\n3 | No Parking - Street Cleaning\n1 | Failure To Display Meter Receipt\n1 | No Violation Description Available\n1 | Bus Lane Violation\n\n@bdhowald Parking and camera violation tickets for #NY_HME6483, cont'd:\n\n1 | Failure To Stop At Red Light\n1 | No Standing - Commercial Meter Zone\n1 | Expired Meter\n1 | Double Parking\n1 | No Angle Parking\n\n@bdhowald Violations by year for #NY_HME6483:\n\n10 | 2017\n15 | 2018\n\n@bdhowald Known fines for #NY_HME6483:\n\n$200.00 | Fined\n$125.00 | Outstanding\n$75.00   | Paid\n"

        reply_event_args1 = {
          'error_on_lookup'  : False,
          'response_args'    : response_args1,
          'response_parts'   : [[combined_message]],
          'success'          : True,
          'successful_lookup': True
        }

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        send_direct_message_mock = MagicMock('send_direct_message_mock')

        api_mock = MagicMock(name='api')
        api_mock.send_direct_message_new  = send_direct_message_mock

        self.tweeter.is_production = is_production_mock
        self.tweeter.api = api_mock

        self.tweeter.process_response(reply_event_args1)

        send_direct_message_mock.assert_called_with({
          'event': {
            'type': 'message_create',
            'message_create': {
              'target': {
                'recipient_id': 30139847
              },
              'message_data': {
                'text': combined_message
              }
            }
          }
        })



        ##############################
        # Test status and old format #
        ##############################

        username2      = 'BarackObama'
        response_args2 = {
          'created_at': utc_time.strftime('%a %b %d %H:%M:%S %z %Y'),
          'id': message_id,
          'legacy_string_parts': ['@howsmydrivingny',
                                  'plate:glf7467',
                                  'state:pa'],
          'string_parts': ['@howsmydrivingny',
                           'plate:glf7467'
                           'state:pa'],
          'type': 'status',
          'username': username2
        }

        response_parts2 = [['@BarackObama #PA_GLF7467 has been queried 1 time.\n\nTotal parking and camera violation tickets: 49\n\n17 | No Parking - Street Cleaning\n6   | Expired Meter\n5   | No Violation Description Available\n3   | Fire Hydrant\n3   | No Parking - Day/Time Limits\n', "@BarackObama Parking and camera violation tickets for #PA_GLF7467, cont'd:\n\n3   | Failure To Display Meter Receipt\n3   | School Zone Speed Camera Violation\n2   | No Parking - Except Authorized Vehicles\n2   | Bus Lane Violation\n1   | Failure To Stop At Red Light\n", "@BarackObama Parking and camera violation tickets for #PA_GLF7467, cont'd:\n\n1   | No Standing - Day/Time Limits\n1   | No Standing - Except Authorized Vehicle\n1   | Obstructing Traffic Or Intersection\n1   | Double Parking\n", '@BarackObama Known fines for #PA_GLF7467:\n\n$1,000.00 | Fined\n$225.00     | Outstanding\n$775.00     | Paid\n']]

        reply_event_args2 = {
          'error_on_lookup'  : False,
          'response_args'    : response_args2,
          'response_parts'   : response_parts2,
          'success'          : True,
          'successful_lookup': True
        }

        create_favorite_mock = MagicMock(name='is_production')
        create_favorite_mock.return_value = True

        recursively_process_status_updates_mock = MagicMock('recursively_process_status_updates')

        api_mock.create_favorite = create_favorite_mock

        self.tweeter.recursively_process_status_updates = recursively_process_status_updates_mock

        self.tweeter.process_response(reply_event_args2)

        recursively_process_status_updates_mock.assert_called_with(response_parts2, message_id)



        #############################
        # Test campaign-only lookup #
        #############################

        username3        = 'NYCMayorsOffice'
        campaign_hashtag = '#SaferSkillman'
        response_args3   = {
          'created_at': utc_time.strftime('%a %b %d %H:%M:%S %z %Y'),
          'id': message_id,
          'legacy_string_parts': ['@howsmydrivingny',
                                  campaign_hashtag],
          'string_parts': ['@howsmydrivingny',
                           campaign_hashtag],
          'type': 'status',
          'username': username3
        }


        campaign_tickets  = random.randint(1000, 2000)
        campaign_vehicles = random.randint(100, 200)

        campaign_result = {
          'included_campaigns': [
            {
              'campaign_hashtag': campaign_hashtag,
              'campaign_tickets': campaign_tickets,
              'campaign_vehicles': campaign_vehicles
            }
          ]
        }

        included_campaigns = [(1, campaign_hashtag)]

        response_parts3 = [['@' + username3 + ' ' + str(campaign_vehicles) + ' vehicles with a total of ' + str(campaign_tickets) + ' tickets have been tagged with ' + campaign_hashtag + '.\n\n']]

        reply_event_args3 = {
          'error_on_lookup'  : False,
          'response_args'    : response_args3,
          'response_parts'   : response_parts3,
          'success'          : True,
          'successful_lookup': True
        }

        self.tweeter.process_response(reply_event_args3)

        recursively_process_status_updates_mock.assert_called_with(response_parts3, message_id)



        #########################
        # Test plateless lookup #
        #########################

        username4        = 'NYC_DOT'
        response_args4   = {
          'created_at': utc_time.strftime('%a %b %d %H:%M:%S %z %Y'),
          'id': message_id,
          'legacy_string_parts': ['@howsmydrivingny',
                                  'plate',
                                  'dkr9364',
                                  'state',
                                  'ny'],
          'string_parts': ['@howsmydrivingny',
                           'plate',
                           'dkr9364',
                           'state',
                           'ny'],
          'type': 'status',
          'username': username4
        }

        response_parts4 = [['@' + username4 + ' I’d be happy to look that up for you!\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234']]

        reply_event_args4 = {
          'error_on_lookup'  : False,
          'response_args'    : response_args4,
          'response_parts'   : response_parts4,
          'success'          : True,
          'successful_lookup': True
        }

        self.tweeter.process_response(reply_event_args4)

        recursively_process_status_updates_mock.assert_called_with(response_parts4, message_id)


        username5        = 'NYCDDC'
        response_args5   = {
          'created_at': utc_time.strftime('%a %b %d %H:%M:%S %z %Y'),
          'id': message_id,
          'legacy_string_parts': ['@howsmydrivingny',
                                  'the',
                                  'state',
                                  'is',
                                  'ny'],
          'string_parts': ['@howsmydrivingny',
                           'the',
                           'state',
                           'is',
                           'ny'],
          'type': 'status',
          'username': username5
        }

        response_parts5 = [['@' + username5 + " I think you're trying to look up a plate, but can't be sure.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"]]

        reply_event_args5 = {
          'error_on_lookup'  : False,
          'response_args'    : response_args5,
          'response_parts'   : response_parts5,
          'success'          : True,
          'successful_lookup': True
        }

        self.tweeter.process_response(reply_event_args5)

        recursively_process_status_updates_mock.assert_called_with(response_parts5, message_id)



        #######################
        # Test error handling #
        #######################

        response_parts6 = [['@' + username2 + " Sorry, I encountered an error. Tagging @bdhowald."]]

        reply_event_args6 = {
          'error_on_lookup'  : False,
          'response_args'    : response_args2,
          'response_parts'   : response_parts6,
          'success'          : True,
          'successful_lookup': True
        }

        self.tweeter.process_response(reply_event_args6)

        recursively_process_status_updates_mock.assert_called_with(response_parts6, message_id)


    def test_recursively_process_direct_messages(self):
        str1 = 'Some stuff\n'
        str2 = 'Some other stuff\nSome more Stuff'
        str3 = 'Yet more stuff'

        response_parts = [
          [str1], str2, str3
        ]

        result_str = "\n".join([str1, str2, str3])

        self.assertEqual(self.tweeter.recursively_process_direct_messages(response_parts), result_str)


    def test_recursively_process_status_updates(self):
        str1 = 'Some stuff\n'
        str2 = 'Some other stuff\nSome more Stuff'
        str3 = 'Yet more stuff'

        original_id = 1

        response_parts = [
          [str1], str2, str3
        ]

        api_mock = MagicMock(name='api')
        api_mock.update_status = inc

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        self.tweeter.api = api_mock
        self.tweeter.is_production = is_production_mock

        self.assertEqual(self.tweeter.recursively_process_status_updates(response_parts, original_id), original_id + len(response_parts))

        new_id             = 1
        new_response_parts = [[[[str1]]]]

        self.assertEqual(self.tweeter.recursively_process_status_updates(response_parts, original_id), original_id + len(response_parts))


