# test_tweet_traffic_violations.py

# import pytest
import random
import unittest

# from mock import MagicMock
from unittest.mock import patch, MagicMock

# import class
from tweet_traffic_violations import TrafficViolationsTweeter

import pdb

import getpass
import requests
import pytz
from datetime import datetime, timezone, time, timedelta






def inc(part, in_reply_to_status_id):
    int_mock = MagicMock(name='api')
    int_mock.id = (in_reply_to_status_id + 1)
    return int_mock


class TestTrafficViolationsTweeter(unittest.TestCase):

    def setUp(self):
        self.tweeter = TrafficViolationsTweeter()

    # def tearDown(self):
    #     self.tweeter.dispose()

    # @pytest.fixture
    # def tweeter(self):
    #     '''Returns a TrafficViolationsTweeter instance'''
    #     return TrafficViolationsTweeter()


    def test_detect_campaign_hashtags(self):
        cursor_mock = MagicMock(name='cursor')
        cursor_mock.cursor = [[6, '#TestCampaign']]

        execute_mock = MagicMock(name='execute')
        execute_mock.execute.return_value = cursor_mock
        # tweeter.
        connect_mock = MagicMock(name='connect')
        connect_mock.connect.return_value = execute_mock

        self.tweeter.engine = connect_mock

        assert self.tweeter.detect_campaign_hashtags(['#TestCampaign'])[0][1] == '#TestCampaign'


    def test_detect_state(self):
        str     = '99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QB|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT'
        regions = str.split('|')

        for region in regions:
            assert self.tweeter.detect_state(region) == True
            assert self.tweeter.detect_state(region + 'XX') == False


    def test_find_potential_vehicles(self):

      string_parts1       = ['@HowsMyDrivingNY', 'I', 'found', 'some', 'more', 'ny:123abcd', 'ca:6vmd948', 'xx:7kvj935', 'state:fl', 'plate:d4kdm4']
      string_parts2       = ['@HowsMyDrivingNY', 'I', 'love', 'you', 'very', 'much!']
      potential_vehicles1 = [
        {'original_string':'ny:123abcd', 'state': 'ny', 'plate': '123abcd', 'valid_plate': True},
        {'original_string':'ca:6vmd948', 'state': 'ca', 'plate': '6vmd948', 'valid_plate': True},
        {'original_string':'xx:7kvj935', 'valid_plate': False}
      ]
      potential_vehicles2 = [{'state': 'fl', 'plate': 'd4kdm4', 'valid_plate': True}]

      self.assertEqual(self.tweeter.find_potential_vehicles(string_parts1), potential_vehicles1)
      self.assertEqual(self.tweeter.find_potential_vehicles(string_parts1, True), potential_vehicles2)
      self.assertEqual(self.tweeter.find_potential_vehicles(string_parts2, True), [])


    def test_infer_plate_and_state_data(self):
        plate_tuples = [['ny', '123abcd'], ['ca', ''], ['xx', 'pxk3819'], ['99', '1234']]

        result = [
          {'original_string':'ny:123abcd', 'state': 'ny', 'plate': '123abcd', 'valid_plate': True},
          {'original_string': 'ca:', 'valid_plate': False},
          {'original_string': 'xx:pxk3819', 'valid_plate': False},
          {'original_string':'99:1234', 'state': '99', 'plate': '1234', 'valid_plate': True}
        ]

        self.assertEqual(self.tweeter.infer_plate_and_state_data(plate_tuples),result)


        plate_tuples = []

        self.assertEqual(self.tweeter.infer_plate_and_state_data(plate_tuples),[])


        plate_tuples = [['ny', 'ny']]

        self.assertEqual(self.tweeter.infer_plate_and_state_data(plate_tuples),[{'original_string':'ny:ny', 'state': 'ny', 'plate': 'ny', 'valid_plate': True}])


    def test_form_campaign_lookup_response_parts(self):
        campaign_data1 = {'included_campaigns': [{'campaign_hashtag': '#SaferSkillman', 'campaign_tickets': 71, 'campaign_vehicles': 6}]}
        campaign_data2 = {'included_campaigns': [{'campaign_hashtag': '#BetterPresident', 'campaign_tickets': 1, 'campaign_vehicles': 1}]}
        campaign_data3 = {'included_campaigns': [{'campaign_hashtag': '#BusTurnaround', 'campaign_tickets': 0, 'campaign_vehicles': 1}]}

        self.assertEqual(self.tweeter.form_campaign_lookup_response_parts(campaign_data1, '@bdhowald'), ['@bdhowald 6 vehicles with a total of 71 tickets have been tagged with #SaferSkillman.\n'])
        self.assertEqual(self.tweeter.form_campaign_lookup_response_parts(campaign_data2, '@BarackObama'), ['@BarackObama 1 vehicle with 1 ticket has been tagged with #BetterPresident.\n'])
        self.assertEqual(self.tweeter.form_campaign_lookup_response_parts(campaign_data3, '@FixQueensBlvd'), ['@FixQueensBlvd 1 vehicle with 0 tickets has been tagged with #BusTurnaround.\n'])


    def test_form_plate_lookup_response_parts(self):
        previous_time = datetime.now() - timedelta(minutes=10)
        utc           = pytz.timezone('UTC')
        eastern       = pytz.timezone('US/Eastern')

        adjusted_time = utc.localize(previous_time).astimezone(eastern)


        plate_lookup = {
          'plate': 'HME6483',
          'state': 'NY',
          'violations': [
            {'name': 'No Standing - Day/Time Limits', 'count': 14},
            {'name': 'No Parking - Street Cleaning', 'count': 3},
            {'name': 'Failure To Display Meter Receipt', 'count': 1},
            {'name': 'No Violation Description Available', 'count': 1},
            {'name': 'Bus Lane Violation', 'count': 1},
            {'name': 'Failure To Stop At Red Light', 'count': 1},
            {'name': 'No Standing - Commercial Meter Zone', 'count': 1},
            {'name': 'Expired Meter', 'count': 1}, {'name': 'Double Parking', 'count': 1},
            {'name': 'No Angle Parking', 'count': 1}
          ],
          'previous_result': {
            'num_tickets': 23,
            'created_at': previous_time
          },
          'frequency': 8
        }

        response_parts = [
          'bdhowald #NY_HME6483 has been queried 8 times.\n'
          '\n'
          'Since the last time the vehicle was queried (May 26, 2018 at ' + adjusted_time.strftime('%I:%M%p') + '), '
          '#NY_HME6483 has received 2 new tickets.\n'
          '\n'
          'Total parking and camera violation tickets: 25\n'
          '\n'
          '14 | No Standing - Day/Time Limits\n'
          '3   | No Parking - Street Cleaning\n',
          "bdhowald Parking and camera violation tickets for #NY_HME6483, cont'd:\n"
          '\n'
          '1   | Failure To Display Meter Receipt\n'
          '1   | No Violation Description Available\n'
          '1   | Bus Lane Violation\n'
          '1   | Failure To Stop At Red Light\n'
          '1   | No Standing - Commercial Meter Zone\n'
          '1   | Expired Meter\n',
          "bdhowald Parking and camera violation tickets for #NY_HME6483, cont'd:\n"
          '\n'
          '1   | Double Parking\n'
          '1   | No Angle Parking\n'
        ]

        self.assertEqual(self.tweeter.form_plate_lookup_response_parts(plate_lookup, 'bdhowald'), response_parts)


    def test_initiate_reply(self):
        rand_int = random.randint(10000000000000000000, 20000000000000000000)
        now      = datetime.now()
        utc      = pytz.timezone('UTC')
        now_str  = utc.localize(now).astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y')

        direct_message_mock = MagicMock(spec=[u'a'])
        direct_message_mock.direct_message = {
          'created_at': now,
          'id': rand_int,
          'recipient': {
            'screen_name': 'HowsMyDrivingNY'
          },
          'sender': {
            'screen_name': 'bdhowald'
          },
          'text': '@HowsMyDrivingNY ny:123abcd ca:cad4534 ny:456efgh'
        }

        direct_message_args_for_response = {
          'created_at': now,
          'id': rand_int,
          'legacy_string_parts': [
            '@howsmydrivingny',
            'ny:123abcd',
            'ca:cad4534',
            'ny:456efgh'
          ],
          'string_parts': [
            '@howsmydrivingny',
            'ny:123abcd',
            'ca:cad4534',
            'ny:456efgh'
          ],
          'username': 'bdhowald'
        }


        entities_mock = MagicMock(spec=[u'b'])
        entities_mock.created_at      = now
        entities_mock.entities        = {
          'user_mentions': [
            {
              'screen_name': 'HowsMyDrivingNY'
            },
            {
              'screen_name': 'BarackObama'
            }
          ]
        }
        entities_mock.id             = rand_int
        entities_mock.text           = '@HowsMyDrivingNY ny:123abcd:abc ca:cad4534:zyx ny:456efgh bex:az:1234567'

        screen_name_mock             = MagicMock('screen_name')
        screen_name_mock.screen_name = 'bdhowald'

        entities_mock.user           = screen_name_mock

        entities_args_for_response   = {
          'created_at': now_str,
          'id': rand_int,
          'legacy_string_parts': [
            '@howsmydrivingny',
            'ny:123abcd:abc',
            'ca:cad4534:zyx',
            'ny:456efgh',
            'bex:az:1234567'
          ],
          'mentioned_users': [
            'howsmydrivingny',
            'barackobama'
          ],
          'string_parts': [
            '@howsmydrivingny',
            'ny:123abcd:abc',
            'ca:cad4534:zyx',
            'ny:456efgh',
            'bex:az:1234567'
          ],
          'username': 'bdhowald'
        }


        extended_tweet_mock = MagicMock(spec=[u'c'])
        extended_tweet_mock.created_at      = now
        extended_tweet_mock.extended_tweet = {
          'entities': {
            'user_mentions': [
              {
                'screen_name': 'HowsMyDrivingNY'
              },
              {
                'screen_name': 'BarackObama'
              }
            ]
          },
          'full_text': '@HowsMyDrivingNY ny:ny ca:1234567'
        }
        extended_tweet_mock.id             = rand_int

        # screen_name_mock             = MagicMock('screen_name')
        # screen_name_mock.screen_name = 'bdhowald'

        extended_tweet_mock.user           = screen_name_mock

        extended_tweet_args_for_response   = {
          'created_at': now_str,
          'id': rand_int,
          'legacy_string_parts': [
            '@howsmydrivingny',
            'ny:ny',
            'ca:1234567'
          ],
          'mentioned_users': [
            'howsmydrivingny',
            'barackobama'
          ],
          'string_parts': [
            '@howsmydrivingny',
            'ny:ny',
            'ca:1234567'
          ],
          'username': 'bdhowald'
        }



        process_response_message_mock = MagicMock(name='process_response_message')

        self.tweeter.process_response_message = process_response_message_mock



        self.tweeter.initiate_reply(direct_message_mock)

        process_response_message_mock.assert_called_with(direct_message_mock, direct_message_args_for_response)


        self.tweeter.initiate_reply(entities_mock)

        process_response_message_mock.assert_called_with(entities_mock, entities_args_for_response)


        self.tweeter.initiate_reply(extended_tweet_mock)

        process_response_message_mock.assert_called_with(extended_tweet_mock, extended_tweet_args_for_response)


    def test_is_production(self):
        username = getpass.getuser()

        self.assertEqual(self.tweeter.is_production(), (username == 'safestreets'))


    def test_perform_campaign_lookup(self):
        included_campaigns = [
          (1, '#SaferSkillman')
        ]

        result = {
          'included_campaigns': [
            {
              'campaign_hashtag': '#SaferSkillman',
              'campaign_tickets': 7167,
              'campaign_vehicles': 152
            }
          ]
        }

        cursor_mock = MagicMock(name='cursor')
        cursor_mock.fetchone.return_value = (152, 7167)

        execute_mock = MagicMock(name='execute')
        execute_mock.execute.return_value = cursor_mock
        # tweeter.
        connect_mock = MagicMock(name='connect')
        connect_mock.connect.return_value = execute_mock

        self.tweeter.engine = connect_mock

        self.assertEqual(self.tweeter.perform_campaign_lookup(included_campaigns), result)



    def test_perform_plate_lookup(self):
        rand_int = random.randint(10000000000000000000, 20000000000000000000)
        now      = datetime.now()
        previous = now - timedelta(minutes=10)
        utc      = pytz.timezone('UTC')
        now_str  = utc.localize(now).astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y')

        args = {
          'created_at': now_str,
          'message_id': rand_int,
          'message_type': 'direct_message',
          'included_campaigns': [(87, '#BetterPresident')],
          'plate': 'hme6483',
          'state': 'ny',
          'username': 'bdhowald'
        }

        violations = [
          {
            'amount_due': '0',
            'county': 'NY',
            'fine_amount': '65',
            'interest_amount': '0',
            'issue_date': '01/19/2017',
            'issuing_agency': 'TRAFFIC',
            'license_type': 'PAS',
            'payment_amount': '0',
            'penalty_amount': '0',
            'plate': 'HME6483',
            'precinct': '005',
            'reduction_amount': '65',
            'state': 'NY',
            'summons_image': 'http://nycserv.nyc.gov/NYCServWeb/ShowImage?searchID=VDBSVmQwNVVaekZOZWxreFQxRTlQUT09&locationName=_____________________',
            'summons_image_description': 'View Summons',
            'summons_number': '8505853659',
            'violation_status': 'HEARING HELD-NOT GUILTY',
            'violation_time': '08:39P'
          },
          {
            'amount_due': '0',
            'county': 'NY',
            'fine_amount': '65',
            'interest_amount': '0',
            'issue_date': '01/20/2017',
            'issuing_agency': 'TRAFFIC',
            'license_type': 'PAS',
            'payment_amount': '0',
            'penalty_amount': '0',
            'plate': 'HME6483',
            'precinct': '005',
            'reduction_amount': '65',
            'state': 'NY',
            'summons_image': 'http://nycserv.nyc.gov/NYCServWeb/ShowImage?searchID=VDBSVmQwNVVaekZOZWxreFQxRTlQUT09&locationName=_____________________',
            'summons_image_description': 'View Summons',
            'summons_number': '8505853660',
            'violation': 'FAIL TO DSPLY MUNI METER RECPT',
            'violation_description': 'FAIL TO DSPLY MUNI METER RECPT',
            'violation_status': 'HEARING HELD-NOT GUILTY',
            'violation_time': '08:40P'
          }
        ]

        result = {
          'plate': 'HME6483',
          'state': 'NY',
          'violations': [
            {
              'count': 1,
              'name': 'No Violation Description Available'
            },
            {
              'count': 1,
              'name': 'Fail To Dsply Muni Meter Recpt'
            }
          ],
          'previous_result': {},
          'frequency': 2
        }

        violations_mock = MagicMock(name='violations')
        violations_mock.json.return_value = violations

        get_mock = MagicMock(name='get')
        get_mock.return_value = violations_mock

        requests.get = get_mock

        cursor_mock = MagicMock(name='cursor')
        cursor_mock.cursor = [{'num_tickets': 1, 'created_at': previous}]
        cursor_mock.fetchone.return_value = (1,)

        execute_mock = MagicMock(name='execute')
        execute_mock.execute.return_value = cursor_mock
        # tweeter.
        connect_mock = MagicMock(name='connect')
        connect_mock.connect.return_value = execute_mock

        self.tweeter.engine = connect_mock

        self.assertEqual(self.tweeter.perform_plate_lookup(args), result)


    def test_print_daily_summary(self):
        utc           = pytz.timezone('UTC')
        eastern       = pytz.timezone('US/Eastern')

        today         = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(today, time.min)) - timedelta(days=1)).astimezone(utc)
        end_of_yesterday   = (eastern.localize(datetime.combine(today, time.min)) - timedelta(seconds=1)).astimezone(utc)

        num_lookups   = 123
        num_tickets   = 456
        empty_lookups = 0


        cursor_mock = MagicMock(name='cursor')
        cursor_mock.fetchone.return_value = (num_lookups, num_tickets, empty_lookups)

        execute_mock = MagicMock(name='execute')
        execute_mock.execute.return_value = cursor_mock
        # tweeter.
        connect_mock = MagicMock(name='connect')
        connect_mock.connect.return_value = execute_mock

        is_production_mock = MagicMock(name='is_production')
        is_production_mock.return_value = True

        update_status_mock = MagicMock(name='update_status')

        api_mock = MagicMock(name='api')
        api_mock.update_status = update_status_mock

        self.tweeter.engine = connect_mock
        self.tweeter.is_production = is_production_mock
        self.tweeter.api = api_mock

        result_str = 'On {}, users requested {} {}. {} received {} {}. {} {} returned no tickets.'.format(midnight_yesterday.strftime('%A, %B %-d, %Y'), num_lookups, 'lookup' if num_lookups == 1 else 'lookups', 'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have', "{:,}".format(num_tickets), 'ticket' if num_tickets == 1 else 'tickets', empty_lookups, 'lookup' if empty_lookups == 1 else 'lookups')


        self.tweeter.print_daily_summary()

        update_status_mock.assert_called_with(result_str)


    # def test_process_response_message(self):
    #     1+1


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














