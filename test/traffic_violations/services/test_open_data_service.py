import copy
import ddt
import math
import mock
import random
import unittest

from collections import Counter
from datetime import datetime, timedelta

from traffic_violations.constants import regexps as regexp_constants
from traffic_violations.constants.borough_codes import BOROUGH_CODES
from traffic_violations.constants.open_data.endpoints import \
    FISCAL_YEAR_DATABASE_ENDPOINTS, MEDALLION_ENDPOINT, \
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT
from traffic_violations.constants.open_data.violations import \
    HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS, \
    HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS
from traffic_violations.constants.precincts import PRECINCTS_BY_BOROUGH

from traffic_violations.models.camera_streak_data import CameraStreakData
from traffic_violations.models.fine_data import FineData
from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup
from traffic_violations.models.response.open_data_service_response \
    import OpenDataServiceResponse

from traffic_violations.services.apis.open_data_service import OpenDataService


@ddt.ddt
class TestOpenDataService(unittest.TestCase):

    def setUp(self):
        self.open_data_service = OpenDataService()

    def test_find_max_camera_streak(self):
        list_of_camera_times = [
            datetime(2015, 9, 18, 0, 0),
            datetime(2015, 10, 16, 0, 0),
            datetime(2015, 11, 2, 0, 0),
            datetime(2015, 11, 5, 0, 0),
            datetime(2015, 11, 12, 0, 0),
            datetime(2016, 2, 2, 0, 0),
            datetime(2016, 2, 25, 0, 0),
            datetime(2016, 5, 31, 0, 0),
            datetime(2016, 9, 8, 0, 0),
            datetime(2016, 10, 17, 0, 0),
            datetime(2016, 10, 24, 0, 0),
            datetime(2016, 10, 26, 0, 0),
            datetime(2016, 11, 21, 0, 0),
            datetime(2016, 12, 18, 0, 0),
            datetime(2016, 12, 22, 0, 0),
            datetime(2017, 1, 5, 0, 0),
            datetime(2017, 2, 13, 0, 0),
            datetime(2017, 5, 10, 0, 0),
            datetime(2017, 5, 24, 0, 0),
            datetime(2017, 6, 27, 0, 0),
            datetime(2017, 6, 27, 0, 0),
            datetime(2017, 9, 14, 0, 0),
            datetime(2017, 11, 6, 0, 0),
            datetime(2018, 1, 28, 0, 0)
        ]

        result = CameraStreakData(
            min_streak_date='September 8, 2016',
            max_streak=13,
            max_streak_date='June 27, 2017')

        self.assertEqual(self.open_data_service._find_max_camera_violations_streak(
            list_of_camera_times), result)

    @ddt.data(
        {
            'plate': 'ABC1234'
        },
        {
            'medallion_query_result': [
                {'dmv_license_plate_number': '8A23B'}
            ],
            'plate': '8A23',
            'state': 'NY',
        },
    )
    @ddt.unpack
    @mock.patch(
        f'traffic_violations.services.apis.open_data_service.'
        f'OpenDataService._perform_query')
    def test_look_up_vehicle_with_violations(self,
                                            mocked_perform_query,
                                            plate,
                                            state=None,
                                            medallion_query_result=None):

        random_state_index = random.randint(
            0, len(regexp_constants.STATE_ABBREVIATIONS) - 1)
        random_plate_type_index = random.randint(
            0, len(regexp_constants.PLATE_TYPES) - 1)

        final_plate = plate if medallion_query_result is None else medallion_query_result[0][
            'dmv_license_plate_number']
        plate_type = regexp_constants.PLATE_TYPES[random_plate_type_index]
        state = state if state else regexp_constants.STATE_ABBREVIATIONS[random_state_index]

        all_borough_codes = [code for borough_list in list(
            BOROUGH_CODES.values()) for code in borough_list]
        all_precincts = [precinct for sublist in list(
            PRECINCTS_BY_BOROUGH.values()) for precinct in sublist]

        summons_numbers = set()

        fiscal_year_databases_violations = []
        for year, _ in FISCAL_YEAR_DATABASE_ENDPOINTS.items():
            fy_violations = []
            for _ in range(random.randint(10, 20)):
                date = datetime(
                    year,
                    random.randint(1, 12),
                    random.randint(1, 28))

                random_borough_code_index = random.randint(
                    0, len(all_borough_codes) - 1)
                random_precinct_index = random.randint(
                    0, len(all_precincts) - 1)
                random_violation_string_index = random.randint(
                    0, len(HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.keys())-1)

                summons_number = random.randint(
                    1000000000,
                    9999999999)
                while summons_number in summons_numbers:
                    summons_number = random.randint(
                        1000000000,
                        9999999999)

                summons_numbers.add(summons_number)

                fy_violations.append({
                    'date_first_observed': date.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                    'feet_from_curb': '0 ft',
                    'from_hours_in_effect': '0800',
                    'house_number': '123',
                    'intersecting_street': '',
                    'issue_date': date.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                    'issuer_code': '43',
                    'issuer_command': 'C',
                    'issuer_precinct': all_precincts[random_precinct_index],
                    'issuing_agency': 'TRAFFIC',
                    'law_section': '123 ABC',
                    'plate_id': plate,
                    'plate_type': plate_type,
                    'registration_state': state,
                    'street_code1': '123',
                    'street_code2': '456',
                    'street_code3': '789',
                    'street_name': 'Fake Street',
                    'sub_division': '23',
                    'summons_image': '',
                    'summons_number': summons_number,
                    'to_hours_in_effect': '2200',
                    'vehicle_body_type': 'SUV',
                    'vehicle_color': 'BLACK',
                    'vehicle_expiration_date': date + timedelta(days=180),
                    'vehicle_make': 'FORD',
                    'vehicle_year': '2017',
                    'violation_code': list(HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.keys())[random_violation_string_index],
                    'violation_county': all_borough_codes[random_borough_code_index],
                    'violation_in_front_of_or_opposite': '',
                    'violation_legal_code': '4-08',
                    'violation_location': '',
                    'violation_post_code': '12345',
                    'violation_precinct': all_precincts[random_precinct_index]})

            fiscal_year_databases_violations.append(fy_violations)

        open_parking_and_camera_violations = []
        for _ in range(random.randint(10, 20)):
            fine_amount = random.randint(10, 200)
            interest_amount = round(float(random.randint(
                0, math.floor(fine_amount/2)) + random.random()), 2)
            penalty_amount = round(float(random.randint(
                0, math.floor(fine_amount/2)) + random.random()), 2)
            reduction_amount = round(float(random.randint(
                0, math.floor(fine_amount/2)) + random.random()), 2)

            payment_amount = round(float(random.randint(0, math.floor(
                fine_amount + interest_amount + penalty_amount - reduction_amount))), 2)
            amount_due = fine_amount + interest_amount + \
                penalty_amount - reduction_amount - payment_amount

            date = datetime(
                random.randint(1999, 2020),
                random.randint(1, 12),
                random.randint(1, 28))

            random_borough_code_index = random.randint(
                0, len(all_borough_codes) - 1)
            random_precinct_index = random.randint(
                0, len(all_precincts) - 1)
            random_violation_string_index = random.randint(
                0, len(HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS.keys())-1)

            summons_number = random.randint(1000000000, 9999999999)
            summons_numbers.add(summons_number)

            open_parking_and_camera_violations.append({
                'amount_due': amount_due,
                'county': all_borough_codes[random_borough_code_index],
                'fine_amount': fine_amount,
                'interest_amount': interest_amount,
                'issue_date': date.strftime('%m/%d/%Y'),
                'issuing_agency': 'TRAFFIC',
                'license_type': plate_type,
                'payment_amount': payment_amount,
                'penalty_amount': penalty_amount,
                'plate': plate,
                'precinct': all_precincts[random_precinct_index],
                'reduction_amount': reduction_amount,
                'state': state,
                'summons': (f'http://nycserv.nyc.gov/NYCServWeb/'
                            f'ShowImage?searchID=VDBSVmQwNVVaekZ'
                            f'OZWxreFQxRTlQUT09&locationName='
                            f'_____________________'),
                'summons_image_description': 'View Summons',
                'summons_number': summons_number,
                'violation': list(
                    HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS.keys())[
                        random_violation_string_index],
                'violation_status': 'HEARING HELD-GUILTY',
                'violation_time': f'0{random.randint(1,9)}:{random.randint(10,59)}P'
            })

        side_effects = []

        if medallion_query_result:
            side_effects.append({'data': medallion_query_result})

        side_effects.append({'data': copy.deepcopy(
            open_parking_and_camera_violations)})

        for violations_list in fiscal_year_databases_violations:
            side_effects.append({'data': copy.deepcopy(violations_list)})

        mocked_perform_query.side_effect = side_effects

        open_parking_and_camera_violations_dict = {}
        for summons in open_parking_and_camera_violations:
            summons['borough'] = [boro for boro, precincts in PRECINCTS_BY_BOROUGH.items(
            ) if int(summons['precinct']) in precincts][0]
            summons['has_date'] = True
            summons['issue_date'] = datetime.strptime(
                summons['issue_date'], '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%S.%f')
            summons['violation'] = HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS[summons['violation']]
            open_parking_and_camera_violations_dict[summons['summons_number']] = summons

        fiscal_year_databases_violations_dict = {}
        for violation_list in fiscal_year_databases_violations:
            for summons in violation_list:
                summons['borough'] = [boro for boro, precincts in PRECINCTS_BY_BOROUGH.items(
                ) if int(summons['violation_precinct']) in precincts][0]
                summons['has_date'] = True
                summons['violation'] = HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS[summons['violation_code']]
                fiscal_year_databases_violations_dict[summons['summons_number']] = summons

        merged_violations = {**open_parking_and_camera_violations_dict,
                             **fiscal_year_databases_violations_dict}

        for key, value in merged_violations.items():
            if key in open_parking_and_camera_violations_dict and key in fiscal_year_databases_violations_dict:
                merged_violations[key] = {**open_parking_and_camera_violations_dict[key], **fiscal_year_databases_violations_dict[key]}

        fines_dict = {}
        for prefix in ['fine', 'interest', 'penalty', 'reduction', 'payment']:
            fines_dict[prefix] = sum(v[f'{prefix}_amount'] for v in merged_violations.values(
            ) if v.get(f'{prefix}_amount'))

        fined = round(sum([fines_dict.get('fine', 0), fines_dict.get(
            'interest', 0), fines_dict.get('penalty', 0)]), 2)
        reduced = round(fines_dict.get('reduction', 0), 2)
        paid = round(fines_dict.get('payment'), 2)
        amount_due = round(fined - reduced - paid, 2)

        fines: FineData = FineData(
            fined=fined,
            reduced=reduced,
            paid=paid,
            outstanding=amount_due)

        tickets: List[Tuple[str, int]] = Counter([v['violation'].title() for v in merged_violations.values(
        ) if v.get('violation')]).most_common()

        years: List[Tuple[str, int]] = Counter([datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y') if v.get(
            'has_date') else 'No Year Available' for v in merged_violations.values()]).most_common()

        boroughs = Counter([v['borough'].title()
                            for v in merged_violations.values()]).most_common()

        violation_times = sorted(
            [datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f') for v in merged_violations.values(
            ) if v.get('violation') and v['violation'] in ['Failure to Stop at Red Light',
                                                           'School Zone Speed Camera Violation']])

        camera_streak_data = None
        if violation_times:
            max_streak = 0
            min_streak_date = None
            max_streak_date = None

            for date in violation_times:

                year_later = date + \
                    (datetime(date.year + 1, 1, 1) - datetime(date.year, 1, 1))

                year_long_tickets = [
                    comp_date for comp_date in violation_times if date <= comp_date < year_later]
                this_streak = len(year_long_tickets)

                if this_streak > max_streak:

                    max_streak = this_streak
                    min_streak_date = year_long_tickets[0]
                    max_streak_date = year_long_tickets[-1]

            camera_streak_data = CameraStreakData(
                min_streak_date=min_streak_date.strftime('%B %-d, %Y'),
                max_streak=max_streak,
                max_streak_date=max_streak_date.strftime('%B %-d, %Y'))

        plate_query = PlateQuery(
            created_at='Tue Dec 31 19:28:12 -0500 2019',
            message_id=random.randint(
                1000000000000000000,
                2000000000000000000),
            message_source='status',
            plate=plate,
            plate_types=plate_type,
            state=state,
            username='@bdhowald')

        result = OpenDataServiceResponse(
            data=OpenDataServicePlateLookup(
                boroughs=[{'count': v, 'title': k.title()}
                          for k, v in boroughs],
                camera_streak_data=camera_streak_data,
                fines=fines,
                num_violations=len(merged_violations),
                plate=final_plate,
                plate_types=plate_query.plate_types,
                state=plate_query.state,
                violations=[{'count': v, 'title': k.title()}
                            for k, v in tickets],
                years=sorted([{'count': v, 'title': k.title()}
                              for k, v in years], key=lambda k: k['title'])),
            success=True)

        self.assertEqual(self.open_data_service.look_up_vehicle(plate_query),
                         result)

    @mock.patch(
        f'traffic_violations.services.apis.open_data_service.'
        f'OpenDataService._perform_query')
    def test_look_up_vehicle_with_no_violations(self,
                                            mocked_perform_query):

        plate = 'ABC1234'
        plate_types = 'PAS'
        state = 'NY'

        mocked_perform_query.side_effects = [{'data': {}} for _ in range(0, 8)]

        plate_query = PlateQuery(
            created_at='Tue Dec 31 19:28:12 -0500 2019',
            message_id=random.randint(
                1000000000000000000,
                2000000000000000000),
            message_source='status',
            plate=plate,
            plate_types=plate_types,
            state=state,
            username='@bdhowald')

        result = OpenDataServiceResponse(
            data=OpenDataServicePlateLookup(
                boroughs=[],
                camera_streak_data=None,
                fines=FineData(fined=0, outstanding=0, paid=0, reduced=0),
                num_violations=0,
                plate=plate,
                plate_types=plate_types,
                state=state,
                violations=[],
                years=[]),
            success=True)

        self.assertEqual(self.open_data_service.look_up_vehicle(plate_query),
                         result)
