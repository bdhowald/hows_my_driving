import ddt
import logging
import mock
import pytz
import random
import requests
import requests_futures.sessions
import unittest

from datetime import datetime, timezone, timedelta

from typing import Dict, List

from unittest.mock import MagicMock

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.campaign import Campaign
from traffic_violations.models.fine_data import FineData
from traffic_violations.models.lookup_requests import \
    AccountActivityAPIDirectMessage
from traffic_violations.models.plate_lookup import PlateLookup
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.vehicle import Vehicle
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse
from traffic_violations.models.response.traffic_violations_aggregator_response \
    import TrafficViolationsAggregatorResponse
from traffic_violations.models.twitter_event import TwitterEvent

from traffic_violations.reply_argument_builder import \
    AccountActivityAPIStatus, DirectMessageAPIDirectMessage, \
    HowsMyDrivingAPIRequest, SearchStatus
from traffic_violations.traffic_violations_aggregator \
    import TrafficViolationsAggregator


@ddt.ddt
class TestTrafficViolationsAggregator(unittest.TestCase):

    previous_time = datetime.now() - timedelta(minutes=10)
    utc = pytz.timezone('UTC')
    eastern = pytz.timezone('US/Eastern')

    adjusted_time = utc.localize(previous_time).astimezone(eastern)

    def setUp(self):
        self.aggregator = TrafficViolationsAggregator()

    @ddt.data(
        {
            'campaigns': Campaign.get_all_in(hashtag=('#SaferSkillman',)),
            'hashtags': ['#SaferSkillman']
        },
        {
            'campaigns': Campaign.get_all_in(hashtag=('#SaferSkillman',)),
            'hashtags': ['#SaferSkillman,']
        },
        {
            'campaigns': Campaign.get_all_in(
                hashtag=('#FixQueensBlvd', '#SaferSkillman')),
            'hashtags': ['#FixQueensBlvd', '#SaferSkillman']
        }
    )
    @ddt.unpack
    def test_detect_campaigns(self, campaigns, hashtags):
        self.assertEqual(self.aggregator._detect_campaigns(
            hashtags), campaigns)

    def test_detect_plate_types(self):
        plate_types_str = (
            f'AGC|AGR|AMB|APP|ARG|ATD|ATV|AYG|BOB|BOT|CBS|CCK|CHC|CLG|CMB|CME|'
            f'CMH|COM|CSP|DLR|FAR|FPW|GAC|GSM|HAC|HAM|HIR|HIS|HOU|HSM|IRP|ITP|'
            f'JCA|JCL|JSC|JWV|LMA|LMB|LMC|LOC|LTR|LUA|MCD|MCL|MED|MOT|NLM|NYA|'
            f'NYC|NYS|OMF|OML|OMO|OMR|OMS|OMT|OMV|ORC|ORG|ORM|PAS|PHS|PPH|PSD|'
            f'RGC|RGL|SCL|SEM|SNO|SOS|SPC|SPO|SRF|SRN|STA|STG|SUP|THC|TOW|TRA|'
            f'TRC|TRL|USC|USS|VAS|VPL|WUG')
        types = plate_types_str.split('|')

        for type in types:
            self.assertEqual(self.aggregator._detect_plate_types(type), True)
            self.assertEqual(
                self.aggregator._detect_plate_types(type + 'XX'), False)

        self.assertEqual(self.aggregator._detect_plate_types(
            f'{types[random.randrange(0, len(types))]},XXX'), True)

    def test_detect_state(self):
        str = (
            f'99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|'
            f'ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|'
            f'NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QC|RI|SC|SD|SK|'
            f'TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT')
        regions = str.split('|')

        for region in regions:
            self.assertEqual(self.aggregator._detect_state(region), True)
            self.assertEqual(
                self.aggregator._detect_state(region + 'XX'), False)

        self.assertEqual(self.aggregator._detect_state(None), False)

    @ddt.data(
      {
          'vehicles': [
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY'),
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY')
          ],
          'result': [
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY')
          ]
      },
      {
          'vehicles': [
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY'),
              Vehicle(
                  valid_plate=True,
                  original_string='NY:ABC1234',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY')
          ],
          'result': [
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY')
          ]
      },
      {
          'vehicles': [
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY'),
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types='COM',
                  state='NY')
          ],
          'result': [
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types=None,
                  state='NY'),
              Vehicle(
                  valid_plate=True,
                  original_string='ABC1234:NY',
                  plate='ABC1234',
                  plate_types='COM',
                  state='NY')
          ]
      }
    )
    @ddt.unpack
    def test_ensure_unique_plates(self,
                                       vehicles,
                                       result):

        self.assertEqual(self.aggregator._ensure_unique_plates(
            vehicles), result)

    @ddt.data(
        {
            'potential_vehicle_data': [
                {'original_string': 'ny:123abcd', 'state': 'ny',
                 'plate': '123abcd', 'valid_plate': True},
                {'original_string': 'ca:6vmd948', 'state': 'ca',
                 'plate': '6vmd948', 'valid_plate': True},
                {'original_string': 'xx:7kvj935', 'valid_plate': False},
                {'original_string': '79217:ny:med', 'valid_plate': True,
                 'plate': '79217', 'state': 'ny', 'plate_types': 'med'},
                {'original_string': 'ny:med', 'valid_plate': True,
                 'plate': 'med', 'state': 'ny'}
            ],
            'string_parts': ['@HowsMyDrivingNY', 'I', 'found', 'some', 'more',
                             'ny:123abcd', 'ca:6vmd948', 'xx:7kvj935',
                             'state:fl', 'plate:d4kdm4', '79217:ny:med']
        },
        {
            'potential_vehicle_data': [
                {'original_string': 'morning:NY', 'plate': 'morning',
                 'state': 'NY', 'valid_plate': True},
                {'original_string': 'NY:HJY3401', 'plate': 'HJY3401',
                 'state': 'NY', 'valid_plate': True}
            ],
            'string_parts': [
                'The', 'fact', 'that', 'red', 'light', 'camera', 'tickets',
                'are', 'only', '$50', '(and', 'the', 'fact', 'that,', 'I',
                'assume,', 'they', 'are', 'relatively', 'sparse', 'throughout',
                'the', 'city)', 'explains', 'a', 'lot.', 'From', 'this',
                'morning:', 'NY:HJY3401', '@HowsMyDrivingNY']
        },
        {
            'potential_vehicle_data': [
                {'original_string': 'NY:HJY3401', 'plate': 'HJY3401',
                    'state': 'NY', 'valid_plate': True}
            ],
            'string_parts': [
                'The', 'fact', 'that', 'red', 'light', 'camera', 'tickets',
                'are', 'only', '$50', '(and', 'the', 'fact', 'that,', 'I',
                'assume,', 'they', 'are', 'relatively', 'sparse', 'throughout',
                'the', 'city)', 'explains', 'a', 'lot.', 'From', 'this',
                'morning:', 'check', 'NY:HJY3401', '@HowsMyDrivingNY']
        },
        {
            'potential_vehicle_data': [
                {'original_string': 'check:ny', 'plate': 'check',
                    'state': 'ny', 'valid_plate': True},
                {'original_string': 'ny:123abcd', 'plate': '123abcd',
                    'state': 'ny', 'valid_plate': True}
            ],
            'string_parts': ['@HowsMyDrivingNY', 'check:', 'ny:', '123abcd']
        }
    )
    @ddt.unpack
    def test_find_potential_vehicles(self,
                                     potential_vehicle_data, string_parts):

        potential_vehicles: List[Vehicle] = [
            Vehicle(**data) for data in potential_vehicle_data]

        self.assertEqual(self.aggregator._find_potential_vehicles(
            string_parts), potential_vehicles)

    @ddt.data(
        {
            'potential_vehicles': [
                Vehicle(state='fl', plate='d4kdm4', valid_plate=True)],
            'string_parts': [
                '@HowsMyDrivingNY', 'I', 'found', 'some', 'more', 'ny:123abcd',
                'ca:6vmd948', 'xx:7kvj935', 'state:fl', 'plate:d4kdm4']
        },
        {
            'potential_vehicles': [],
            'string_parts': ['@HowsMyDrivingNY',
                             'I', 'love', 'you', 'very', 'much!']
        },
        {
            'potential_vehicles': [
                Vehicle(state='fl', plate='d4kdm4', valid_plate=True,
                        plate_types='pas,com')],
            'string_parts': [
                '@HowsMyDrivingNY', 'I', 'found', 'some', 'more', 'state:fl',
                'plate:d4kdm4', 'types:pas,com']
        }
    )
    @ddt.unpack
    def test_find_potential_vehicles_using_legacy_logic(self,
                                                        potential_vehicles,
                                                        string_parts):
        self.assertEqual(
            self.aggregator._find_potential_vehicles_using_legacy_logic(
                string_parts), potential_vehicles)

    @ddt.data(
        {
            'data': [('#SaferSkillman', 6, 71)],
            'results': [
                (f'6 vehicles with a total of 71 tickets have been '
                 f'tagged with #SaferSkillman.\n\n')
            ],
            'username': '@bdhowald'
        },
        {
            'data': [('#BetterPresident', 1, 1)],
            'results': [
                (f'1 vehicle with 1 ticket has been '
                 f'tagged with #BetterPresident.\n\n')
            ],
            'username': '@BarackObama'
        },
        {
            'data': [('#BusTurnaround', 1, 0)],
            'results': [
                (f'1 vehicle with 0 tickets has been '
                 f'tagged with #BusTurnaround.\n\n')
            ],
            'username': '@FixQueensBlvd'
        }
    )
    @ddt.unpack
    def test_form_campaign_lookup_response_parts(self,
                                                 data: {},
                                                 results: [],
                                                 username):
        self.assertEqual(
            self.aggregator._form_campaign_lookup_response_parts(
                data, username), results)

    @ddt.data(
        {
            'data': {
                'plate': 'HME6483',
                'plate_types': 'pas',
                'state': 'NY',
                'violations': [
                    {'title': 'No Standing - Day/Time Limits', 'count': 14},
                    {'title': 'No Parking - Street Cleaning', 'count': 3},
                    {'title': 'Failure To Display Meter Receipt', 'count': 1},
                    {'title': 'No Violation Description Available',
                        'count': 1},
                    {'title': 'Bus Lane Violation', 'count': 1},
                    {'title': 'Failure To Stop At Red Light', 'count': 1},
                    {'title': 'No Standing - Commercial Meter Zone',
                        'count': 1},
                    {'title': 'Expired Meter', 'count': 1},
                    {'title': 'Double Parking', 'count': 1},
                    {'title': 'No Angle Parking', 'count': 1}
                ],
                'years': [
                    {'title': '2016', 'count': 2},
                    {'title': '2017', 'count': 8},
                    {'title': '2018', 'count': 13}
                ],
                'previous_result': PlateLookup(
                    created_at=previous_time,
                    message_id=12345678901234567890,
                    message_source='direct_message',
                    num_tickets=23,
                    username='BarackObama'
                ),
                'frequency': 8,
                'boroughs': [
                    {'count': 1, 'title': 'Bronx'},
                    {'count': 7, 'title': 'Brooklyn'},
                    {'count': 2, 'title': 'Queens'},
                    {'count': 13, 'title': 'Staten Island'}
                ],
                'fines': FineData(**{'fined': 180.0, 'reduced': 50.0,
                                     'paid': 100.0, 'outstanding': 30.0}),
                'camera_streak_data':
                    CameraStreakData(**{'min_streak_date': 'September 7, 2015',
                                        'max_streak': 4,
                                        'max_streak_date': 'November 5, 2015'})
            },
            'response': [
                '#NY_HME6483 (types: pas) has been queried 8 times.\n'
                '\n'
                'This vehicle was last queried on ' + adjusted_time.strftime(
                    '%B %-d, %Y') + ' at ' +
                adjusted_time.strftime('%I:%M%p') + '. '
                'Since then, #NY_HME6483 has received 2 new tickets.\n'
                '\n'
                'Total parking and camera violation tickets: 25\n'
                '\n'
                '14 | No Standing - Day/Time Limits\n',
                'Parking and camera violation tickets for '
                '#NY_HME6483, cont\'d:\n'
                '\n'
                '3   | No Parking - Street Cleaning\n'
                '1   | Failure To Display Meter Receipt\n'
                '1   | No Violation Description Available\n'
                '1   | Bus Lane Violation\n'
                '1   | Failure To Stop At Red Light\n',
                'Parking and camera violation tickets for '
                '#NY_HME6483, cont\'d:\n'
                '\n'
                '1   | No Standing - Commercial Meter Zone\n'
                '1   | Expired Meter\n'
                '1   | Double Parking\n'
                '1   | No Angle Parking\n',
                'Violations by year for #NY_HME6483:\n'
                '\n'
                '2   | 2016\n'
                '8   | 2017\n'
                '13 | 2018\n',
                'Violations by borough for #NY_HME6483:\n'
                '\n'
                '1   | Bronx\n'
                '7   | Brooklyn\n'
                '2   | Queens\n'
                '13 | Staten Island\n',
                'Known fines for #NY_HME6483:\n'
                '\n'
                '$180.00 | Fined\n'
                '$50.00   | Reduced\n'
                '$100.00 | Paid\n'
                '$30.00   | Outstanding\n'
            ],
            'username': 'bdhowald'
        },
        {
            'data': {
                'plate': 'HME6483',
                'plate_types': None,
                'state': 'NY',
                'violations': [
                    {'title': 'No Standing - Day/Time Limits', 'count': 14},
                    {'title': 'No Parking - Street Cleaning', 'count': 3},
                    {'title': 'Failure To Display Meter Receipt', 'count': 1},
                    {'title': 'No Violation Description Available',
                        'count': 1},
                    {'title': 'Bus Lane Violation', 'count': 1},
                    {'title': 'Failure To Stop At Red Light', 'count': 1},
                    {'title': 'No Standing - Commercial Meter Zone',
                        'count': 1},
                    {'title': 'Expired Meter', 'count': 1},
                    {'title': 'Double Parking', 'count': 1},
                    {'title': 'No Angle Parking', 'count': 1}
                ],
                'years': [
                    {'title': '2016', 'count': 2},
                    {'title': '2017', 'count': 8},
                    {'title': '2018', 'count': 13}
                ],
                'previous_result': PlateLookup(
                    created_at=previous_time,
                    message_id=12345678901234567890,
                    message_source='status',
                    num_tickets=23,
                    username='BarackObama'
                ),
                'frequency': 8,
                'boroughs': [
                    {'count': 1, 'title': 'Bronx'},
                    {'count': 7, 'title': 'Brooklyn'},
                    {'count': 2, 'title': 'Queens'},
                    {'count': 13, 'title': 'Staten Island'}
                ],
                'fines': FineData(**{'fined': 0.0, 'reduced': 0.0,
                                     'paid': 0.0, 'outstanding': 0.0}),
                'camera_streak_data':
                    CameraStreakData(**{'min_streak_date': 'September 7, 2015',
                                        'max_streak': 5,
                                        'max_streak_date': 'November 5, 2015'})
            },
            'response': [
                '#NY_HME6483 has been queried 8 times.\n'
                '\n'
                'This vehicle was last queried on ' + adjusted_time.strftime(
                    '%B %-d, %Y') + ' at ' + adjusted_time.strftime('%I:%M%p') +
                ' by @BarackObama: '
                'https://twitter.com/BarackObama/status/12345678901234567890. ' +
                'Since then, #NY_HME6483 has received 2 new tickets.\n'
                '\n'
                'Total parking and camera violation tickets: 25\n'
                '\n',
                'Parking and camera violation tickets for '
                '#NY_HME6483, cont\'d:\n'
                '\n'
                '14 | No Standing - Day/Time Limits\n'
                '3   | No Parking - Street Cleaning\n'
                '1   | Failure To Display Meter Receipt\n'
                '1   | No Violation Description Available\n'
                '1   | Bus Lane Violation\n',
                'Parking and camera violation tickets for '
                '#NY_HME6483, cont\'d:\n'
                '\n'
                '1   | Failure To Stop At Red Light\n'
                '1   | No Standing - Commercial Meter Zone\n'
                '1   | Expired Meter\n'
                '1   | Double Parking\n'
                '1   | No Angle Parking\n',
                'Violations by year for #NY_HME6483:\n'
                '\n'
                '2   | 2016\n'
                '8   | 2017\n'
                '13 | 2018\n',
                'Violations by borough for #NY_HME6483:\n'
                '\n'
                '1   | Bronx\n'
                '7   | Brooklyn\n'
                '2   | Queens\n'
                '13 | Staten Island\n',
                "Under @bradlander's proposed legislation, this vehicle could "
                "have been booted or impounded due to its 5 camera violations "
                "(>= 5/year) from September 7, 2015 to November 5, 2015.\n",
            ],
            'username': 'bdhowald'
        }
    )
    @ddt.unpack
    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TweetDetectionService.tweet_exists')
    def test_form_plate_lookup_response_parts(self,
                                              mocked_tweet_exists,
                                              data: Dict[str, any],
                                              response: List[str],
                                              username: str):

        mocked_tweet_exists.return_value = True

        self.assertEqual(self.aggregator._form_plate_lookup_response_parts(
            borough_data=data['boroughs'],
            camera_streak_data=data['camera_streak_data'],
            fine_data=data['fines'],
            frequency=data['frequency'],
            plate=data['plate'],
            plate_types=data['plate_types'],
            previous_lookup=data['previous_result'],
            state=data['state'],
            username=username,
            violations=data['violations'],
            year_data=data['years']), response)

    def test_form_summary_string(self):

        vehicle1_fined = random.randint(10, 20000)
        vehicle1_reduced = random.randint(0, vehicle1_fined)
        vehicle1_paid = random.randint(0, vehicle1_fined - vehicle1_reduced)
        vehicle1_fine_data = FineData(
            fined=vehicle1_fined,
            outstanding=(vehicle1_fined - vehicle1_reduced - vehicle1_paid),
            paid=vehicle1_paid,
            reduced=vehicle1_reduced)

        vehicle1_mock = MagicMock(name='vehicle1')
        vehicle1_mock.fines = vehicle1_fine_data
        vehicle1_mock.violations = [{} for _ in range(random.randint(10, 20))]

        vehicle2_fined = random.randint(10, 20000)
        vehicle2_reduced = random.randint(0, vehicle2_fined)
        vehicle2_paid = random.randint(0, vehicle2_fined - vehicle2_reduced)
        vehicle2_fine_data = FineData(
            fined=vehicle2_fined,
            outstanding=(vehicle2_fined - vehicle2_reduced - vehicle2_paid),
            paid=vehicle2_paid,
            reduced=vehicle2_reduced)
        vehicle2_mock = MagicMock(name='vehicle2')
        vehicle2_mock.fines = vehicle2_fine_data
        vehicle2_mock.violations = [{} for _ in range(random.randint(10, 20))]

        vehicle3_mock = MagicMock(name='vehicle3')
        vehicle3_mock.fines = FineData(
            fined=0, outstanding=0,
            paid=0, reduced=0)
        vehicle3_mock.violations = []

        total_fined = vehicle1_fined + vehicle2_fined
        total_paid = vehicle1_paid + vehicle2_paid
        total_reduced = vehicle1_reduced + vehicle2_reduced

        summary: TrafficViolationsAggregatorResponse = TrafficViolationsAggregatorResponse(
            plate_lookups=[vehicle1_mock, vehicle2_mock, vehicle3_mock])

        total_tickets = sum(len(lookup.violations)
                            for lookup in summary.plate_lookups)

        self.assertEqual(
            self.aggregator._form_summary_string(summary), [
                f"You queried 3 vehicles, of which 2 vehicles have "
                f"collectively received {total_tickets} tickets with at least "
                f"{'${:,.2f}'.format(total_fined - total_reduced)} in fines, "
                f"of which {'${:,.2f}'.format(total_paid)} "
                f"has been paid.\n\n"])

    def test_handle_response_part_formation(self):

        plate = 'HME6483'
        state = 'NY'
        username = '@NYC_DOT'

        collection = [
            {'title': '2017', 'count': 1},
            {'title': '2018', 'count': 1}
        ]

        keys = {
            'count': 'count',
            'continued_format_string':
                f"Violations by year for #{state}_{plate}:, cont'd\n\n",
            'cur_string': '',
            'description': 'title',
            'default_description': 'No Year Available',
            'prefix_format_string':
                f'Violations by year for #{state}_{plate}:\n\n',
            'result_format_string': '{}| {}\n',
            'username': username
        }

        result = [(keys['prefix_format_string']).format(
            state, plate) + '1 | 2017\n1 | 2018\n']

        self.assertEqual(self.aggregator._handle_response_part_formation(
            collection=collection,
            count='count',
            continued_format_string=f"Violations by year for #{state}_{plate}:, cont'd\n\n",
            description='title',
            default_description='No Year Available',
            prefix_format_string=f'Violations by year for #{state}_{plate}:\n\n',
            result_format_string='{}| {}\n',
            username=username), result)

    @mock.patch('traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._create_response')
    def test_initiate_reply(self, mocked_create_response):

        direct_message_mock = MagicMock(name='direct_message')
        direct_message_mock.requires_response.return_value = True

        self.aggregator.initiate_reply(direct_message_mock)

        direct_message_mock.requires_response.return_value = False

        self.aggregator.initiate_reply(direct_message_mock)

        mocked_create_response.assert_called_once_with(direct_message_mock)

    @ddt.data(
        {
            'plate_tuples': [
                ['ny', '123abcd'], ['ca', ''],
                ['xx', 'pxk3819'], ['99', '1234'],
                ['ny', 't327sd', 'pas,agr'], ['79217', 'ny', 'med'],
                ['ny', 'med']
            ],
            'potential_vehicle_data': [
                {'original_string': 'ny:123abcd', 'state': 'ny',
                    'plate': '123abcd', 'valid_plate': True},
                {'original_string': 'ca:', 'valid_plate': False},
                {'original_string': 'xx:pxk3819', 'valid_plate': False},
                {'original_string': '99:1234', 'state': '99',
                    'plate': '1234', 'valid_plate': True},
                {'original_string': 'ny:t327sd:pas,agr', 'state': 'ny',
                    'plate': 't327sd', 'plate_types': 'pas,agr', 'valid_plate': True},
                {'original_string': '79217:ny:med', 'state': 'ny',
                    'plate': '79217', 'plate_types': 'med', 'valid_plate': True},
                {'original_string': 'ny:med', 'state': 'ny',
                    'plate': 'med', 'valid_plate': True},
            ]
        },
        {
            'plate_tuples': [],
            'potential_vehicle_data': []
        },
        {
            'plate_tuples': [['ny', 'ny']],
            'potential_vehicle_data': [
                {
                    'original_string': 'ny:ny',
                    'state': 'ny',
                    'plate': 'ny',
                    'valid_plate': True
                }
            ]
        }
    )
    @ddt.unpack
    def test_infer_plate_and_state_data(self, plate_tuples, potential_vehicle_data):
        self.assertEqual(
            self.aggregator._infer_plate_and_state_data(plate_tuples),
            [Vehicle(**data) for data in potential_vehicle_data])

    @mock.patch('traffic_violations.traffic_violations_aggregator.Campaign')
    @mock.patch('traffic_violations.traffic_violations_aggregator.PlateLookup')
    def test_perform_campaign_lookup(self,
                                     mocked_plate_lookup_class,
                                     mocked_campaign_class):

        campaign = MagicMock(name='campaign')
        campaign_hashtag = '#SaferSkillman'
        campaign.hashtag = campaign_hashtag

        included_campaigns = [campaign]

        plate_lookups = []
        for _ in range(random.randint(5, 20)):
            lookup = MagicMock()
            lookup.num_tickets = random.randint(1, 200)
            plate_lookups.append(lookup)

        mocked_plate_lookup_class.campaigns.any.return_value = True
        mocked_plate_lookup_class.query.join().order_by().all.return_value = plate_lookups

        result = [
            (campaign_hashtag,
             len(plate_lookups),
             sum(plate_lookup.num_tickets for plate_lookup in plate_lookups))]

        self.assertEqual(self.aggregator._perform_campaign_lookup(
            included_campaigns), result)

    @mock.patch('traffic_violations.traffic_violations_aggregator.PlateLookup.get_by')
    def test_perform_plate_lookup(self, mocked_plate_lookup_get_by):

        rand_int = random.randint(1000000000000000000, 2000000000000000000)
        now = datetime.now()
        previous_time = now - timedelta(minutes=10)
        utc = pytz.timezone('UTC')
        now_str = utc.localize(now).astimezone(
            timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        plate = 'ABCDEFG'
        plate_types = 'COM,PAS'
        state = 'NY'

        previous_message_id = rand_int + 1
        previous_message_source = 'status'
        previous_num_tickets = 1
        previous_username = 'BarackObama'

        campaigns = Campaign.get_all_in(
            hashtag=('#FixQueensBlvd', '#SaferSkillman',))

        plate_query = PlateQuery(
            created_at=now_str,
            message_id=rand_int,
            message_source='direct_message',
            plate=plate,
            plate_types=plate_types,
            state=state,
            username='@bdhowald')

        previous_lookup = PlateLookup(
            created_at=previous_time,
            message_id=previous_message_id,
            message_source=previous_message_source,
            num_tickets=previous_num_tickets,
            plate=plate,
            plate_types=plate_types,
            state=state,
            username=previous_username)

        mocked_plate_lookup_get_by.return_value = previous_lookup

        violations = [
            {
                'amount_due': '100',
                'county': 'NY',
                'fine_amount': '100',
                'interest_amount': '0',
                'issue_date': '01/19/2017',
                'issuing_agency': 'TRAFFIC',
                'license_type': 'PAS',
                'payment_amount': '0',
                'penalty_amount': '0',
                'plate': 'HME6483',
                'precinct': '005',
                'reduction_amount': '0',
                'state': 'NY',
                'summons_image': (f'http://nycserv.nyc.gov/NYCServWeb/'
                                  f'ShowImage?searchID=VDBSVmQwNVVaekZ'
                                  f'OZWxreFQxRTlQUT09&locationName='
                                  f'_____________________'),
                'summons_image_description': 'View Summons',
                'summons_number': '8505853659',
                'violation_status': 'HEARING HELD-NOT GUILTY',
                'violation_time': '08:39P'
            },
            {
                'amount_due': '30',
                'county': 'NY',
                'fine_amount': '50',
                'interest_amount': '0',
                'issue_date': '01/20/2018',
                'issuing_agency': 'TRAFFIC',
                'license_type': 'PAS',
                'payment_amount': '0',
                'penalty_amount': '0',
                'plate': 'HME6483',
                'precinct': '005',
                'reduction_amount': '20',
                'state': 'NY',
                'summons_image': (f'http://nycserv.nyc.gov/NYCServWeb/'
                                  f'ShowImage?searchID=VDBSVmQwNVVaekZ'
                                  f'OZWxreFQxRTlQUT09&locationName='
                                  f'_____________________'),
                'summons_image_description': 'View Summons',
                'summons_number': '8505853660',
                'violation': 'FAIL TO DSPLY MUNI METER RECPT',
                'violation_description': 'FAIL TO DSPLY MUNI METER RECPT',
                'violation_status': 'HEARING HELD-NOT GUILTY',
                'violation_time': '08:40P'
            },
            {
                'amount_due': '50',
                'county': 'MN',
                'fine_amount': '50',
                'interest_amount': '0',
                'issue_date': '01/27/2018',
                'issuing_agency': 'DEPARTMENT OF TRANSPORTATION',
                'license_type': 'SRF',
                'payment_amount': '50',
                'penalty_amount': '0',
                'plate': 'HME6483',
                'precinct': '0',
                'reduction_amount': '0',
                'state': 'NY',
                'summons_image': (f'http://nycserv.nyc.gov/NYCServWeb/'
                                  f'ShowImage?searchID=VDBSVmQwNVVaekZ'
                                  f'OZWxreFQxRTlQUT09&locationName='
                                  f'_____________________'),
                'summons_image_description': 'View Summons',
                'summons_number': '23958567421',
                'violation': 'PHTO SCHOOL ZN SPEED VIOLATION',
                'violation_time': '07:33P'
            },
            {
                'amount_due': '0',
                'county': 'MN',
                'fine_amount': '50',
                'interest_amount': '0',
                'issue_date': '01/27/2018',
                'issuing_agency': 'DEPARTMENT OF TRANSPORTATION',
                'license_type': 'SRF',
                'payment_amount': '75',
                'penalty_amount': '25',
                'plate': 'HME6483',
                'precinct': '0',
                'reduction_amount': '0',
                'state': 'NY',
                'summons_image': (f'http://nycserv.nyc.gov/NYCServWeb/'
                                  f'ShowImage?searchID=VDBSVmQwNVVaekZ'
                                  f'OZWxreFQxRTlQUT09&locationName='
                                  f'_____________________'),
                'summons_image_description': 'View Summons',
                'summons_number': '2398572382',
                'violation': 'FAILURE TO STOP AT RED LIGHT',
                'violation_time': '10:25A'
            }
        ]

        lookup_data = {
            'boroughs': [
                {'count': 4, 'title': 'Manhattan'}
            ],
            'camera_streak_data': CameraStreakData(
                **{'max_streak': 2,
                   'min_streak_date': 'January 27, 2018',
                   'max_streak_date': 'January 27, 2018'}),
            'fines': FineData(**{'fined': 275.0, 'reduced': 20.0,
                                 'paid': 125.0, 'outstanding': 180.0}),
            'num_violations': 4,
            'plate': 'ABCDEFG',
            'plate_types': 'COM,PAS',
            'state': 'NY',
            'violations': [
                {
                    'count': 1,
                    'title': 'No Violation Description Available'
                },
                {
                    'count': 1,
                    'title': 'Fail To Dsply Muni Meter Recpt'
                },
                {
                    'count': 1,
                    'title': 'School Zone Speed Camera Violation'
                },
                {
                    'count': 1,
                    'title': 'Failure To Stop At Red Light'
                }
            ],
            'years': [
                {'count': 1, 'title': '2017'},
                {'count': 3, 'title': '2018'}
            ],
        }

        lookup = OpenDataServicePlateLookup(**lookup_data)

        result = OpenDataServiceResponse(
            data=lookup,
            success=True)

        violations_mock = MagicMock(name='violations')
        violations_mock.json.return_value = violations
        violations_mock.status_code = 200

        result_mock = MagicMock(name='result')
        result_mock.result.return_value = violations_mock

        get_mock = MagicMock(name='get')
        get_mock.return_value = result_mock

        session_mock = MagicMock(name='session_object')
        session_mock.get = get_mock

        session_object_mock = MagicMock(name='session_object')
        session_object_mock.return_value = session_mock

        requests_futures.sessions.FuturesSession = session_object_mock

        self.assertEqual(self.aggregator._perform_plate_lookup(
            campaigns=campaigns, plate_query=plate_query), result)

        # Try again with a forced error.
        violations_mock.status_code = 503

        result = self.aggregator._perform_plate_lookup(
            campaigns=[], plate_query=plate_query)

        self.assertIsInstance(result, OpenDataServiceResponse)
        self.assertRegex(str(result.message), 'server error when accessing')

    @ddt.data({
        'plate': 'HME6483',
        'returned_plate': 'HME6483',
        'state': 'NY'
    }, {
        'plate': '8A23',
        'returned_plate': '8A23B',
        'state': 'NY'
    })
    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._perform_plate_lookup')
    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._query_for_lookup_frequency')
    @ddt.unpack
    def test_create_response_direct_message(self,
                                            mocked_query_for_lookup_frequency,
                                            mocked_perform_plate_lookup,
                                            plate,
                                            returned_plate,
                                            state):
        """ Test direct message and new format """

        username = 'bdhowald'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        num_tickets = 15

        twitter_event = TwitterEvent(
            id=1,
            created_at=random.randint(1500000000000, 1600000000000),
            event_id=message_id,
            event_text=f'@howsmydrivingny {state.lower()}:{plate.lower()}',
            event_type='direct_message',
            user_handle=username,
            user_id=random.randint(100000000, 1000000000))

        direct_message_request_object = AccountActivityAPIDirectMessage(
            message=twitter_event,
            message_source='direct_message')

        plate_lookup_data = {
            'boroughs': [
                {'count': 1, 'title': 'Bronx'},
                {'count': 2, 'title': 'Brooklyn'},
                {'count': 3, 'title': 'Manhattan'},
                {'count': 4, 'title': 'Queens'},
                {'count': 5, 'title': 'Staten Island'}
            ],
            'fines': FineData(**{'fined': 200.0, 'paid': 75.0,
                                 'outstanding': 125.0}),
            'num_violations': num_tickets,
            'plate': returned_plate,
            'plate_types': None,
            'state': state,
            'violations': [{'count': 4,
                            'title': 'No Standing - Day/Time Limits'},
                           {'count': 3,
                            'title': 'No Parking - Street Cleaning'},
                           {'count': 1,
                            'title': 'Failure To Display Meter Receipt'},
                           {'count': 1,
                            'title': 'No Violation Description Available'},
                           {'count': 1,
                            'title': 'Bus Lane Violation'},
                           {'count': 1,
                            'title': 'Failure To Stop At Red Light'},
                           {'count': 1,
                            'title': 'No Standing - Commercial Meter Zone'},
                           {'count': 1,
                            'title': 'Expired Meter'},
                           {'count': 1,
                            'title': 'Double Parking'},
                           {'count': 1,
                            'title': 'No Angle Parking'}],
            'years': [
                {'title': '2017', 'count': 10},
                {'title': '2018', 'count': 15}
            ]
        }

        lookup = OpenDataServicePlateLookup(**plate_lookup_data)

        plate_lookup = OpenDataServiceResponse(
            data=lookup,
            success=True)

        response = {
            'error_on_lookup': False,
            'request_object': direct_message_request_object,
            'response_parts': [
                [
                    f'#{state}_{returned_plate} has been queried 1 time.\n'
                    '\n'
                    'Total parking and camera violation tickets: 15\n'
                    '\n'
                    '4 | No Standing - Day/Time Limits\n'
                    '3 | No Parking - Street Cleaning\n'
                    '1 | Failure To Display Meter Receipt\n'
                    '1 | No Violation Description Available\n'
                    '1 | Bus Lane Violation\n',
                    'Parking and camera violation tickets for '
                    f'#{state}_{returned_plate}, cont\'d:\n'
                    '\n'
                    '1 | Failure To Stop At Red Light\n'
                    '1 | No Standing - Commercial Meter Zone\n'
                    '1 | Expired Meter\n'
                    '1 | Double Parking\n'
                    '1 | No Angle Parking\n',
                    f'Violations by year for #{state}_{returned_plate}:\n'
                    '\n'
                    '10 | 2017\n'
                    '15 | 2018\n',
                    f'Violations by borough for #{state}_{returned_plate}:\n'
                    '\n'
                    '1 | Bronx\n'
                    '2 | Brooklyn\n'
                    '3 | Manhattan\n'
                    '4 | Queens\n'
                    '5 | Staten Island\n',
                    f'Known fines for #{state}_{returned_plate}:\n'
                    '\n'
                    '$200.00 | Fined\n'
                    '$0.00     | Reduced\n'
                    '$75.00   | Paid\n'
                    '$125.00 | Outstanding\n'
                ]
            ],
            'success': True,
            'successful_lookup': True,
            'username': username
        }

        mocked_perform_plate_lookup.return_value = plate_lookup
        mocked_query_for_lookup_frequency.return_value = 1

        self.assertEqual(self.aggregator._create_response(
            direct_message_request_object), response)

    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._perform_plate_lookup')
    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._query_for_lookup_frequency')
    def test_create_response_status_legacy_format(self,
                                                  mocked_query_for_lookup_frequency,
                                                  mocked_perform_plate_lookup):
        """ Test status and old format """

        username = 'BarackObama'
        message_id = random.randint(1000000000000000000, 2000000000000000000)

        request_object = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1500000000000, 1600000000000),
                event_id=message_id,
                event_text='@howsmydrivingny plate:glf7467 state:pa',
                event_type='status',
                user_handle=username,
                user_id=random.randint(100000000, 1000000000)
            ),
            message_source='status'
        )
        plate_lookup_data = {
            'boroughs': [],
            'fines': FineData(**{'fined': 1000.0, 'reduced': 0.0,
                                 'paid': 775.0, 'outstanding': 225.0}),
            'num_violations': 44,
            'plate': 'GLF7467',
            'plate_types': None,
            'state': 'PA',
            'violations': [{'count': 17,
                            'title': 'No Parking - Street Cleaning'},
                           {'count': 6,
                            'title': 'Expired Meter'},
                           {'count': 5,
                            'title': 'No Violation Description Available'},
                           {'count': 3,
                            'title': 'Fire Hydrant'},
                           {'count': 3,
                            'title': 'No Parking - Day/Time Limits'},
                           {'count': 3,
                            'title': 'Failure To Display Meter Receipt'},
                           {'count': 3,
                            'title': 'School Zone Speed Camera Violation'},
                           {'count': 2,
                            'title': 'No Parking - Except Authorized Vehicles'},
                           {'count': 2,
                            'title': 'Bus Lane Violation'},
                           {'count': 1,
                            'title': 'Failure To Stop At Red Light'},
                           {'count': 1,
                            'title': 'No Standing - Day/Time Limits'},
                           {'count': 1,
                            'title': 'No Standing - Except Authorized Vehicle'},
                           {'count': 1,
                            'title': 'Obstructing Traffic Or Intersection'},
                           {'count': 1,
                            'title': 'Double Parking'}],
            'years': []
        }

        lookup = OpenDataServicePlateLookup(**plate_lookup_data)

        plate_lookup = OpenDataServiceResponse(
            data=lookup,
            success=True)

        response_parts = [['#PA_GLF7467 has been queried 2 times.\n\n'
                           'Total parking and camera violation tickets: 49\n\n'
                           '17 | No Parking - Street Cleaning\n'
                           '6   | Expired Meter\n'
                           '5   | No Violation Description Available\n'
                           '3   | Fire Hydrant\n'
                           '3   | No Parking - Day/Time Limits\n',
                           'Parking and camera violation tickets for '
                           '#PA_GLF7467, cont\'d:\n\n'
                           '3   | Failure To Display Meter Receipt\n'
                           '3   | School Zone Speed Camera Violation\n'
                           '2   | No Parking - Except Authorized Vehicles\n'
                           '2   | Bus Lane Violation\n'
                           '1   | Failure To Stop At Red Light\n',
                           'Parking and camera violation tickets for '
                           '#PA_GLF7467, cont\'d:\n\n'
                           '1   | No Standing - Day/Time Limits\n'
                           '1   | No Standing - Except Authorized Vehicle\n'
                           '1   | Obstructing Traffic Or Intersection\n'
                           '1   | Double Parking\n',
                           'Known fines for #PA_GLF7467:\n\n'
                           '$1,000.00 | Fined\n'
                           '$0.00         | Reduced\n'
                           '$775.00     | Paid\n'
                           '$225.00     | Outstanding\n']]

        response = {
            'error_on_lookup': False,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': request_object.username()
        }

        mocked_perform_plate_lookup.return_value = plate_lookup
        mocked_query_for_lookup_frequency.return_value = 2

        self.assertEqual(self.aggregator._create_response(
            request_object), response)

    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._perform_campaign_lookup')
    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._detect_campaigns')
    def test_create_response_campaign_only_lookup(self,
                                                  mocked_detect_campaigns,
                                                  mocked_perform_campaign_lookup):
        """ Test campaign-only lookup """

        message_id = random.randint(1000000000000000000, 2000000000000000000)
        username = 'NYCMayorsOffice'
        campaign_hashtag = '#SaferSkillman'

        request_object = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1500000000000, 1600000000000),
                event_id=message_id,
                event_text=f'@howsmydrivingny {campaign_hashtag}',
                event_type='status',
                user_handle=username,
                user_id=random.randint(100000000, 1000000000)
            ),
            message_source='status'
        )

        campaign_tickets = random.randint(1000, 2000)
        campaign_vehicles = random.randint(100, 200)

        campaign_result = [(
            campaign_hashtag, campaign_vehicles, campaign_tickets)]

        included_campaigns = [(1, campaign_hashtag)]

        response_parts = [[(f"{'{:,}'.format(campaign_vehicles)} vehicles with a total of "
            f"{'{:,}'.format(campaign_tickets)} tickets have been tagged with {campaign_hashtag}.\n\n")]]

        response = {
            'error_on_lookup': False,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': True,
            'username': request_object.username()
        }

        mocked_detect_campaigns.return_value = included_campaigns
        mocked_perform_campaign_lookup.return_value = campaign_result

        self.assertEqual(self.aggregator._create_response(
            request_object), response)

        mocked_perform_campaign_lookup.assert_called_with(included_campaigns)

    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._detect_campaigns')
    def test_create_response_with_search_status(self,
                                                mocked_detect_campaigns):
        """ Test plateless lookup """

        now = datetime.now()

        message_id = random.randint(1000000000000000000, 2000000000000000000)
        user_handle = '@NYC_DOT'

        message_object = MagicMock(name='message')
        message_object.created_at = now
        message_object.entities = {}
        message_object.entities['user_mentions'] = [
            {'screen_name': 'HowsMyDrivingNY'}]
        message_object.id = message_id
        message_object.full_text = '@howsmydrivingny plate dkr9364 state ny'
        message_object.user.screen_name = user_handle

        request_object = SearchStatus(
            message=message_object,
            message_source='status'
        )

        response_parts = [
            [f"I’d be happy to look that up for you!\n\nJust a reminder, "
             f"the format is <state|province|territory>:<plate>, "
             f"e.g. NY:abc1234"]]

        response = {
            'error_on_lookup': False,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': False,
            'username': request_object.username()
        }

        mocked_detect_campaigns.return_value = []

        self.assertEqual(self.aggregator._create_response(
            request_object), response)

    def test_create_response_with_direct_message_api_direct_message(self):
        """ Test plateless lookup """

        now = datetime.now()
        previous_time = now - timedelta(minutes=10)
        utc = pytz.timezone('UTC')
        utc_time = utc.localize(now).astimezone(timezone.utc)

        message_id = random.randint(1000000000000000000, 2000000000000000000)
        message_text = '@howsmydrivingny plate dkr9364'
        username = 'NYCDDC'

        recipient = MagicMock(name='recipient')
        recipient.screen_name = 'HowsMyDrivingNY'

        sender = MagicMock(name='sender')
        sender.screen_name = username
        sender.id = random.randint(100000000, 1000000000)

        mock_api = MagicMock(name='api')
        mock_api.get_user.side_effect = [recipient, sender]

        message_object = MagicMock(name='message')
        message_object.created_timestamp = random.randint(
            1500000000000, 1600000000000)
        message_object.id = message_id
        message_object.message_create = {}
        message_object.message_create['message_data'] = {}
        message_object.message_create['message_data']['text'] = message_text
        message_object.message_create['sender_id'] = '123'
        message_object.message_create['target'] = {}
        message_object.message_create['target']['recipient_id'] = '456'

        request_object = DirectMessageAPIDirectMessage(
            api=mock_api,
            message=message_object,
            message_source='direct_message'
        )

        response_parts = [
            [f"I think you're trying to look up a plate, but can't be sure.\n\n"
             f"Just a reminder, the format is "
             f"<state|province|territory>:<plate>, e.g. NY:abc1234"]]

        response = {
            'error_on_lookup': False,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': False,
            'username': request_object.username()
        }

        self.assertEqual(self.aggregator._create_response(
            request_object), response)

    @mock.patch(
        'traffic_violations.traffic_violations_aggregator.TrafficViolationsAggregator._perform_plate_lookup')
    def test_create_response_with_error(self,
                                        mocked_perform_plate_lookup):
        """ Test error handling """

        message_id = random.randint(1000000000000000000, 2000000000000000000)
        username = 'BarackObama'

        response_parts = [
            ["Sorry, I encountered an error. Tagging @bdhowald."]]

        # mock an error
        mocked_perform_plate_lookup.side_effect = ValueError('generic error')

        request_object = AccountActivityAPIStatus(
            message=TwitterEvent(
                id=1,
                created_at=random.randint(1500000000000, 1600000000000),
                event_id=message_id,
                event_text='@howsmydrivingny plate:glf7467 state:pa',
                event_type='status',
                user_handle=username,
                user_id=random.randint(100000000, 1000000000)
            ),
            message_source='status'
        )

        self.aggregator._create_response(request_object)

        response = {
            'error_on_lookup': True,
            'request_object': request_object,
            'response_parts': response_parts,
            'success': True,
            'successful_lookup': False,
            'username': request_object.username()
        }

        self.assertEqual(self.aggregator._create_response(
            request_object), response)
