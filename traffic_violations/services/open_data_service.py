import logging
import os
import re
import requests
import requests_futures.sessions

from collections import Counter
from datetime import datetime
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Any, Dict, List

from traffic_violations.constants.borough_codes import BOROUGH_CODES
from traffic_violations.constants.open_data.endpoints import FISCAL_YEAR_DATABASE_ENDPOINTS, \
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT
from traffic_violations.constants.open_data.needed_fields import \
    FISCAL_YEAR_DATABASE_NEEDED_FIELDS, OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS, \
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS
from traffic_violations.constants.open_data.violations import \
    HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS, \
    HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS
from traffic_violations.constants.precincts import PRECINCTS_BY_BOROUGH
from traffic_violations.services.location_service import LocationService
from traffic_violations.models.plate_lookup import PlateLookup



class OpenDataService:

    OUTPUT_FINE_KEYS = ['fined', 'paid', 'reduced', 'outstanding']

    MAX_RESULTS = 10_000

    MEDALLION_PATTERN = re.compile(r'^[0-9][A-Z][0-9]{2}$')

    OPEN_DATA_TOKEN = os.environ['NYC_OPEN_DATA_TOKEN']

    def __init__(self, logger):
        # Set up retry ability
        s_req = requests_futures.sessions.FuturesSession(max_workers=9)

        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[403, 500, 502, 503, 504],
                        raise_on_status=False)

        s_req.mount('https://', HTTPAdapter(max_retries=retries))

        self.api = s_req

        self.logger = logger
        self.location_service = LocationService(logger)


    def lookup_vehicle(self, plate_lookup: PlateLookup) -> Dict[str, Any]:
        return self._perform_all_queries(plate_lookup=plate_lookup)


    def _add_fine_data_for_open_parking_and_camera_violations_summons(self, summons) -> Dict[str, Any]:
        for output_key in self.OUTPUT_FINE_KEYS:
            summons[output_key] = 0

        for fine_key in OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS:
            if fine_key in summons:
                try:
                    amount = float(summons[fine_key])

                    if fine_key in ['fine_amount', 'interest_amount', 'penalty_amount']:
                        summons['fined'] += amount

                    elif fine_key == 'reduction_amount':
                        summons['reduced'] += amount

                    elif fine_key == 'amount_due':
                        summons['outstanding'] += amount

                    elif fine_key == 'payment_amount':
                        summons['paid'] += amount

                except ValueError as ve:

                    self.logger.error('Error parsing value into float')
                    self.logger.error(e)
                    self.logger.error(str(e))
                    self.logger.error(e.args)
                    logging.exception("stack trace")

                    pass

        return summons

    def _add_query_limit_and_token(self, url: str) -> str:
        return f'{url}&$limit={self.MAX_RESULTS}&$$app_token={self.OPEN_DATA_TOKEN}'

    def _calculate_aggregate_data(self, plate_lookup, violations) -> Dict[str, Any]:
        # Marshal all ticket data into form.
        fines = [
            ('fined',       sum(v['fined']
                                for v in violations.values() if v.get('fined'))),
            ('reduced',     sum(v['reduced']
                                for v in violations.values() if v.get('reduced'))),
            ('paid',        sum(v['paid']
                                for v in violations.values() if v.get('paid'))),
            ('outstanding', sum(v['outstanding']
                                for v in violations.values() if v.get('outstanding')))
        ]

        tickets = Counter([v['violation'] for v in violations.values(
        ) if v.get('violation')]).most_common()

        years = Counter([datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y') if v.get(
            'has_date') else 'No Year Available' for v in violations.values()]).most_common()

        boroughs = Counter([v['borough'] for v in violations.values(
        ) if v.get('borough')]).most_common()


        camera_violations = ['Failure to Stop at Red Light', 'School Zone Speed Camera Violation']

        camera_streak_data = self._find_max_camera_violations_streak(sorted([datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f') for v in violations.values(
        ) if v.get('violation') and v['violation'] in camera_violations]))

        result = {
            'boroughs': [{'title': k.title(), 'count': v} for k, v in boroughs],
            'fines': fines,
            'num_violations': len(violations),
            'plate': plate_lookup.plate,
            'plate_types': plate_lookup.plate_types,
            'state': plate_lookup.state,
            'violations': [{'title': k.title(), 'count': v} for k, v in tickets],
            'years': sorted([{'title': k.title(), 'count': v} for k, v in years], key=lambda k: k['title'])
        }

        # No need to add streak data if it doesn't exist
        if camera_streak_data:
            result['camera_streak_data'] = camera_streak_data

        return result


    def _find_max_camera_violations_streak(self, list_of_violation_times) -> Dict[str, Any]:
        if list_of_violation_times:
            max_streak = 0
            min_streak_date = None
            max_streak_date = None

            for date in list_of_violation_times:

                self.logger.debug("date: %s", date)

                year_later = date + \
                    (datetime(date.year + 1, 1, 1) - datetime(date.year, 1, 1))
                self.logger.debug("year_later: %s", year_later)

                year_long_tickets = [
                    comp_date for comp_date in list_of_violation_times if date <= comp_date < year_later]
                this_streak = len(year_long_tickets)

                if this_streak > max_streak:

                    max_streak = this_streak
                    min_streak_date = year_long_tickets[0]
                    max_streak_date = year_long_tickets[-1]

            return {
                'min_streak_date': min_streak_date.strftime('%B %-d, %Y'),
                'max_streak': max_streak,
                'max_streak_date': max_streak_date.strftime('%B %-d, %Y')
            }

        return {}


    def _normalize_fiscal_year_database_summons(self, summons) -> Dict[str, Any]:
        # get human readable ticket type name
        if summons.get('violation_description') is None:
            if summons.get('violation_code') and HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(summons['violation_code']):
                summons['violation'] = HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(
                    summons['violation_code'])
        else:
            if HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(summons['violation_description']):
                summons['violation'] = HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(
                    summons['violation_description'])
            else:
                summons['violation'] = re.sub(
                    '[0-9]*-', '', summons['violation_description'])

        if summons.get('issue_date') is None:
            summons['has_date'] = False
        else:
            try:
                summons['issue_date'] = datetime.strptime(
                    summons['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')
                summons['has_date'] = True
            except ValueError as ve:
                summons['has_date'] = False

        if summons.get('violation_precinct'):
            boros = [boro for boro, precincts in PRECINCTS_BY_BOROUGH.items() if int(
                summons['violation_precinct']) in precincts]
            if boros:
                summons['borough'] = boros[0]
            else:
                if summons.get('violation_county'):
                    boros = [name.replace(" ", "_") for name, codes in BOROUGH_CODES.items(
                    ) if summons.get('violation_county') in codes]
                    if boros:
                        summons['borough'] = boros[0]
                else:
                    if summons.get('street_name'):
                        street_name = summons.get('street_name')
                        intersecting_street = summons.get(
                            'intersecting_street') or ''

                        geocoded_borough = self.location_service.get_borough_from_location_strings(
                            [street_name, intersecting_street])
                        if geocoded_borough:
                            summons['borough'] = geocoded_borough.lower()

        return summons


    def _normalize_open_parking_and_camera_violations_summons(self, summons) -> Dict[str, Any]:
        # get human readable ticket type name
        if summons.get('violation'):
            summons['violation'] = HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS[
                summons['violation']]

        # normalize the date
        if summons.get('issue_date') is None:
            summons['has_date'] = False

        else:
            try:
                summons['issue_date'] = datetime.strptime(
                    summons['issue_date'], '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%S.%f')
                summons['has_date'] = True
            except ValueError as ve:
                summons['has_date'] = False

        if summons.get('precinct'):
            boros = [boro for boro, precincts in PRECINCTS_BY_BOROUGH.items() if int(
                summons['precinct']) in precincts]
            if boros:
                summons['borough'] = boros[0]
            else:
                if summons.get('county'):
                    boros = [name for name, codes in BOROUGH_CODES.items(
                    ) if summons.get('county') in codes]
                    if boros:
                        summons['borough'] = boros[0]

        summons = self._add_fine_data_for_open_parking_and_camera_violations_summons(summons=summons)

        return summons


    def _perform_all_queries(self, plate_lookup: PlateLookup) -> Dict[str, Any]:
        # set up return data structure
        violations = {}


        result: Dict[str, bool] = self._perform_medallion_query(plate_lookup=plate_lookup)

        if result.get('error'):
            return result


        result = self._perform_open_parking_and_camera_violations_query(
            plate_lookup=plate_lookup, violations=violations)

        if result.get('error'):
            return result


        result = self._perform_fiscal_year_database_queries(
            plate_lookup=plate_lookup, violations=violations)

        if result.get('error'):
            return result


        for record in violations.values():
            if record.get('violation') is None:
                record['violation'] = "No Violation Description Available"

            if record.get('borough') is None:
                record['borough'] = 'No Borough Available'

        return self._calculate_aggregate_data(plate_lookup=plate_lookup,
            violations=violations)


    def _perform_fiscal_year_database_queries(self, plate_lookup: PlateLookup, violations) -> Dict[str, bool]:
        """Grab data from each of the fiscal year violation datasets"""

        # iterate through the endpoints
        for year, endpoint in FISCAL_YEAR_DATABASE_ENDPOINTS.items():

            fiscal_year_database_query_string: str = (
                f"{endpoint}?"
                f"plate_id={plate_lookup.plate}&"
                f"registration_state={plate_lookup.state}"
                f"{'&$where=plate_type%20in(' + ','.join(['%27' + type + '%27' for type in plate_lookup.plate_types.split(',')]) + ')' if plate_lookup.plate_types is not None else ''}")

            fiscal_year_database_response: Dict[str, Any] = self._perform_query(
                  query_string=fiscal_year_database_query_string)

            if fiscal_year_database_response.get('error'):
                return fiscal_year_database_response

            if fiscal_year_database_response.get('data'):
                fiscal_year_database_data : List[str, str] = fiscal_year_database_response.get('data')

                self.logger.debug(
                    f'Fiscal year data for {plate_lookup.state}:{plate_lookup.plate}'
                    f'{":" + plate_lookup.plate_types if plate_lookup.plate_types else ""} for {year}: '
                    f'{fiscal_year_database_data}')

                for record in fiscal_year_database_data:
                    record = self._normalize_fiscal_year_database_summons(summons=record)

                    # structure response and only use the data we need
                    new_data: Dict[str, Any] = {needed_field: record.get(needed_field) for needed_field in FISCAL_YEAR_DATABASE_NEEDED_FIELDS}

                    if violations.get(record['summons_number']) is None:
                        violations[record['summons_number']] = new_data
                    else:
                        # Merge records together, treating fiscal year data as
                        # authoritative.
                        return_record = violations[record['summons_number']] = {**violations.get(record['summons_number']), **new_data}

                        # If we still don't have a violation (description) after merging records,
                        # record it as blank
                        if return_record.get('violation') is None:
                            return_record[
                                'violation'] = "No Violation Description Available"
                        if return_record.get('borough') is None:
                            record['borough'] = 'No Borough Available'

        return {'success': True}


    def _perform_medallion_query(self, plate_lookup: PlateLookup) -> Dict[str, bool]:
      if self.MEDALLION_PATTERN.search(plate_lookup.plate) != None:

          medallion_query_string: str = (
              f'{self.MEDALLION_ENDPOINT}?'
              f'license_number={plate_lookup.plate}')

          medallion_response: Dict[str, Any] = self._perform_query(query_string=medallion_query_string)

          if medallion_response.get('error'):
              return medallion_response

          if medallion_response.get('data'):
              medallion_data : List[str, Any] = medallion_response.get('data')

              self.logger.debug(
                  f'Medallion data for {plate_lookup.state}:{plate_lookup.plate}'
                  f'{medallion_data}')

              sorted_list: Dict[str, Any] = sorted(
                  set([res['dmv_license_plate_number'] for res in medallion_data]))
              plate_lookup.plate = sorted_list[-1] if sorted_list else plate_lookup.plate

      return {'success': True}


    def _perform_open_parking_and_camera_violations_query(self, plate_lookup, violations) -> Dict[str, bool]:
        """Grab data from 'Open Parking and Camera Violations'"""

        # response from city open data portal
        open_parking_and_camera_violations_query_string: str = (
            f'{OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT}?'
            f'plate={plate_lookup.plate}&'
            f'state={plate_lookup.state}'
            f"{'&$where=license_type%20in(' + ','.join(['%27' + type + '%27' for type in plate_lookup.plate_types.split(',')]) + ')' if plate_lookup.plate_types is not None else ''}")

        open_parking_and_camera_violations_response: Dict[str, Any] = self._perform_query(
            query_string=open_parking_and_camera_violations_query_string)

        if open_parking_and_camera_violations_response.get('error'):
            return open_parking_and_camera_violations_response

        if open_parking_and_camera_violations_response.get('data'):
            open_parking_and_camera_violations_data : List[str, str] = \
                open_parking_and_camera_violations_response.get('data')

            self.logger.debug(
                f'Open Parking and Camera Violations data for {plate_lookup.state}:{plate_lookup.plate}'
                f'{":" + plate_lookup.plate_types if plate_lookup.plate_types else ""}: '
                f'{open_parking_and_camera_violations_data}')

            # only data we're looking for
            opacv_desired_keys = OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS

            # add violation if it's missing
            for record in open_parking_and_camera_violations_data:

                record = self._normalize_open_parking_and_camera_violations_summons(summons=record)

                violations[record['summons_number']] = {
                    needed_field: record.get(needed_field) for needed_field in opacv_desired_keys}

        return {'success': True}


    def _perform_query(self, query_string: str) -> Dict[str, Any]:
        full_url: str = f'{self._add_query_limit_and_token(query_string)}'
        response = self.api.get(full_url)

        result = response.result()

        if result.status_code in range(200, 300):
            # Only attempt to read json on a successful response.
            return {'data': result.json()}
        elif result.status_code in range(300, 400):
            return {'error': 'redirect', 'url': full_url}
        elif result.status_code in range(400, 500):
            return {'error': 'user error', 'url': full_url}
        elif result.status_code in range(500, 600):
            return {'error': 'server error', 'url': full_url}
        else:
            return {'error': 'unknown error', 'url': full_url}
